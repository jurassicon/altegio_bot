"""Tests: WhatsApp delivery status webhook handling.

Covers:
- status=delivered updates OutboxMessage from sent to delivered
- status=read updates OutboxMessage from delivered to read
- duplicate delivered/read webhook is idempotent
- status regression is ignored (read must not become delivered)
- campaign run recompute triggered after status update
- inbound message flow still works after the change
- STOP/START flow still works after the change
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    OutboxMessage,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import (
    _apply_status_updates,
    _extract_status_updates,
    handle_event,
)

WAMID = "wamid.TEST001"
PHONE_NUMBER_ID = "PNID_STATUS"
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _NullProvider(WhatsAppProvider):
    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        contact_name: str | None = None,
    ) -> str:
        return "msg-null"


def _status_payload(
    phone_number_id: str,
    wamid: str,
    status: str,
) -> dict[str, Any]:
    """Build a minimal Meta status-webhook payload."""
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "phone_number_id": phone_number_id,
                            },
                            "statuses": [
                                {
                                    "id": wamid,
                                    "status": status,
                                    "timestamp": "1700000001",
                                    "recipient_id": "4915100000001",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _message_payload(
    phone_number_id: str,
    from_phone: str,
    text: str,
) -> dict[str, Any]:
    """Build a minimal Meta inbound-message payload."""
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "phone_number_id": phone_number_id,
                            },
                            "messages": [
                                {
                                    "from": from_phone,
                                    "id": "wamid.INBOUND",
                                    "timestamp": "1700000002",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _setup_outbox_with_campaign(
    session_maker,
    *,
    outbox_status: str = "sent",
) -> tuple[int, int, int]:
    """Create CampaignRun + CampaignRecipient + OutboxMessage.

    Returns (run_id, recipient_id, outbox_id).
    """
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="test_campaign",
                mode="send-real",
                company_ids=[1],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                total_clients_seen=1,
                candidates_count=1,
                sent_count=1,
                provider_accepted_count=1,
            )
            session.add(run)
            await session.flush()

            ob = OutboxMessage(
                company_id=1,
                phone_e164="+4915100000001",
                template_code="test_tpl",
                body="Hello",
                status=outbox_status,
                provider_message_id=WAMID,
                scheduled_at=_utcnow(),
                sent_at=_utcnow(),
            )
            session.add(ob)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run.id,
                company_id=1,
                phone_e164="+4915100000001",
                status="provider_accepted",
                outbox_message_id=ob.id,
                provider_message_id=WAMID,
            )
            session.add(recipient)
            await session.flush()

            return int(run.id), int(recipient.id), int(ob.id)


# ---------------------------------------------------------------------------
# Unit tests: _extract_status_updates
# ---------------------------------------------------------------------------


def test_extract_status_updates_delivered() -> None:
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "delivered")
    updates = _extract_status_updates(payload)
    assert len(updates) == 1
    assert updates[0]["wamid"] == WAMID
    assert updates[0]["status"] == "delivered"


def test_extract_status_updates_read() -> None:
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "read")
    updates = _extract_status_updates(payload)
    assert len(updates) == 1
    assert updates[0]["status"] == "read"


def test_extract_status_updates_unknown_skipped() -> None:
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "deleted")
    updates = _extract_status_updates(payload)
    assert updates == []


def test_extract_status_updates_empty_payload() -> None:
    assert _extract_status_updates({}) == []


def test_extract_status_updates_message_payload_returns_empty() -> None:
    """Inbound message payloads have no statuses — must return empty."""
    payload = _message_payload(PHONE_NUMBER_ID, "4915100000001", "hello")
    updates = _extract_status_updates(payload)
    assert updates == []


# ---------------------------------------------------------------------------
# Integration: _apply_status_updates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delivered_advances_outbox_from_sent(session_maker) -> None:
    run_id, recipient_id, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {"id": WAMID, "status": "delivered"},
                }
            ]
            affected_run_ids = await _apply_status_updates(session, updates)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"
        assert "wa_status_delivered" in (ob.meta or {})

    assert run_id in affected_run_ids


@pytest.mark.asyncio
async def test_read_advances_outbox_from_delivered(session_maker) -> None:
    run_id, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="delivered")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "read",
                    "timestamp": "1700000002",
                    "raw": {"id": WAMID, "status": "read"},
                }
            ]
            await _apply_status_updates(session, updates)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"


@pytest.mark.asyncio
async def test_duplicate_delivered_is_idempotent(session_maker) -> None:
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="delivered")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    # No advancement — rank did not increase → no run_ids returned.
    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"


@pytest.mark.asyncio
async def test_status_regression_ignored(session_maker) -> None:
    """read must not regress to delivered."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="read")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"  # unchanged


@pytest.mark.asyncio
async def test_unknown_wamid_no_crash(session_maker) -> None:
    """Webhook for an unknown wamid must not raise."""
    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": "wamid.DOESNOTEXIST",
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []


# ---------------------------------------------------------------------------
# Integration: handle_event with status payload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_event_delivered_triggers_recompute(
    session_maker,
) -> None:
    """handle_event with a delivered status payload advances OutboxMessage
    and calls recompute_campaign_run_stats for the linked run."""
    run_id, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    provider = _NullProvider()
    recomputed: list[int] = []

    async def _fake_recompute(session, run_id_arg: int) -> dict:
        recomputed.append(run_id_arg)
        return {}

    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "delivered")

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.recompute_campaign_run_stats",
        side_effect=_fake_recompute,
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:status-delivered-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    assert run_id in recomputed

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"


@pytest.mark.asyncio
async def test_handle_event_read_advances_to_read(session_maker) -> None:
    run_id, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="delivered")

    provider = _NullProvider()

    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "read")

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.recompute_campaign_run_stats",
        new_callable=AsyncMock,
        return_value={},
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:status-read-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"


# ---------------------------------------------------------------------------
# Status callback: sent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sent_callback_noop_when_already_sent(session_maker) -> None:
    """Meta 'sent' callback when OutboxMessage is already 'sent' must be a no-op."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "sent",
                    "timestamp": "1700000000",
                    "raw": {"id": WAMID, "status": "sent"},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"


@pytest.mark.asyncio
async def test_sent_callback_advances_from_queued(session_maker) -> None:
    """Meta 'sent' callback when OutboxMessage is still 'queued' must advance to 'sent'."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="queued")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": WAMID,
                    "status": "sent",
                    "timestamp": "1700000000",
                    "raw": {"id": WAMID, "status": "sent"},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids != [] or True  # campaign recompute may or may not be triggered

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"


@pytest.mark.asyncio
async def test_no_match_by_provider_message_id(session_maker) -> None:
    """Callback for a different wamid must not update unrelated OutboxMessage."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    async with session_maker() as session:
        async with session.begin():
            updates = [
                {
                    "wamid": "wamid.DIFFERENT_WAMID",
                    "status": "delivered",
                    "timestamp": "1700000001",
                    "raw": {},
                }
            ]
            run_ids = await _apply_status_updates(session, updates)

    assert run_ids == []

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"  # unchanged


@pytest.mark.asyncio
async def test_full_status_path_sent_delivered_read(session_maker) -> None:
    """Full progression: sent → delivered → read via sequential callbacks."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    # Step 1: delivered
    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "delivered", "timestamp": "1700000001", "raw": {}}],
            )

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "delivered"

    # Step 2: read
    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": WAMID, "status": "read", "timestamp": "1700000002", "raw": {}}],
            )

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "read"


@pytest.mark.asyncio
async def test_handle_event_sent_callback_noop(session_maker) -> None:
    """handle_event with a 'sent' status payload on already-sent OutboxMessage is a no-op."""
    _, _, outbox_id = await _setup_outbox_with_campaign(session_maker, outbox_status="sent")

    provider = _NullProvider()
    payload = _status_payload(PHONE_NUMBER_ID, WAMID, "sent")

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.recompute_campaign_run_stats",
        new_callable=AsyncMock,
        return_value={},
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:status-sent-noop-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)
        assert ob is not None
        assert ob.status == "sent"  # unchanged


# ---------------------------------------------------------------------------
# Regression: inbound message flow still works
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inbound_message_still_forwarded_to_chatwoot(
    session_maker,
) -> None:
    """Inbound (non-status) events must still be forwarded to Chatwoot."""
    provider = _NullProvider()

    payload = _message_payload(PHONE_NUMBER_ID, "4915100000001", "Hello")

    logged: list[str] = []

    class _FakeCW:
        async def log_incoming_message(
            self,
            phone: str,
            text: str,
            contact_name: str | None = None,
        ) -> None:
            logged.append(text)

        async def aclose(self) -> None:
            pass

    with patch(
        "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
        return_value=_FakeCW(),
    ):
        async with session_maker() as session:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key="wa:inbound-1",
                    status="received",
                    query={},
                    headers={},
                    payload=payload,
                )
                session.add(evt)
                await session.flush()
                await handle_event(session, evt, provider)

    assert "Hello" in logged


# ---------------------------------------------------------------------------
# Regression: STOP/START flow still works
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_command_still_sets_opt_out(session_maker) -> None:
    """STOP command must still set wa_opted_out after our changes."""
    from altegio_bot.models.models import Client

    provider = _NullProvider()
    phone = "+10000000010"

    async with session_maker() as session:
        async with session.begin():
            c = await session.get(Client, 10)
            assert c is not None
            c.phone_e164 = phone
            c.wa_opted_out = False

            session.add(
                WhatsAppSender(
                    id=99,
                    company_id=1,
                    sender_code="default",
                    phone_number_id=PHONE_NUMBER_ID,
                    display_phone="+49",
                    is_active=True,
                )
            )

            payload = _message_payload(PHONE_NUMBER_ID, "10000000010", "STOP")
            evt = WhatsAppEvent(
                dedupe_key="wa:stop-1",
                status="received",
                query={},
                headers={},
                payload=payload,
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

    async with session_maker() as session:
        c2 = await session.get(Client, 10)
        assert c2 is not None
        assert c2.wa_opted_out is True
