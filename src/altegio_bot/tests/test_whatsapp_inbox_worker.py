"""Tests for Chatwoot-origin mirror event guard in whatsapp_inbox_worker.

Real-world case: customer +4915207156150 sent STOP, which produced:
  event_id=4971  dedupe_key=wa:...           origin=meta    body=STOP
  event_id=4972  dedupe_key=chatwoot:395:3388 origin=chatwoot body=STOP

Both were processed as wa_cmd=stop, sending the ack twice.

Fix: Chatwoot-origin events must not execute inbound commands.

Covers:
 1. Meta-origin STOP → command executed, ack sent, OutboxMessage created.
 2. Chatwoot-origin mirrored STOP → command skipped, no ack, no OutboxMessage.
 3. Real-world sequence: exactly one wa_cmd_stop OutboxMessage total.
 4. Operator relay (Chatwoot-origin with _chatwoot_operator_relay) → still sent.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    Client,
    OutboxMessage,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import _apply_status_updates, handle_event


class _CaptureProvider(WhatsAppProvider):
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
    ) -> str:
        self.sent.append({"sender_id": sender_id, "phone_e164": phone_e164, "text": text})
        return "wamid.CAPTURE"

    async def send_template(self, *args: Any, **kwargs: Any) -> str:
        return "wamid.CAPTURE_TPL"


def _meta_stop_payload(phone_number_id: str, from_phone: str) -> dict[str, Any]:
    """Standard Meta-origin STOP payload — no _chatwoot key."""
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "from": from_phone,
                                    "type": "text",
                                    "text": {"body": "STOP"},
                                    "id": "wamid.META_STOP",
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }


def _chatwoot_mirror_stop_payload(
    phone_number_id: str,
    from_phone: str,
    *,
    conversation_id: int = 395,
    message_id: int = 3388,
) -> dict[str, Any]:
    """Chatwoot-mirrored STOP payload — same message, but _chatwoot marker present."""
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "from": from_phone,
                                    "type": "text",
                                    "text": {"body": "STOP"},
                                    "id": "cw-msg-001",
                                }
                            ],
                        }
                    }
                ]
            }
        ],
        "_chatwoot": {
            "conversation_id": conversation_id,
            "message_id": message_id,
            "account_id": 1,
        },
    }


# ---------------------------------------------------------------------------
# Test 1: Meta-origin STOP executes the command
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_origin_stop_executes_command(session_maker, monkeypatch) -> None:
    """Meta-origin STOP: ack sent, wa_cmd_stop OutboxMessage created."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _CaptureProvider()
    phone = "+4915207156150"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    company_id=1,
                    altegio_client_id=9001,
                    display_name="Seren",
                    phone_e164=phone,
                    raw={},
                )
            )
            session.add(
                WhatsAppSender(
                    id=900,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_META_STOP",
                    display_phone="+49",
                    is_active=True,
                )
            )
            evt = WhatsAppEvent(
                dedupe_key="wa:meta:stop:001",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_meta_stop_payload("PNID_META_STOP", "4915207156150"),
                chatwoot_conversation_id=None,
            )
            session.add(evt)
            await session.flush()

            await handle_event(session, evt, provider)

    assert len(provider.sent) == 1
    assert provider.sent[0]["phone_e164"] == phone
    assert "abgemeldet" in provider.sent[0]["text"].lower()

    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_cmd_stop"))
        assert result.scalar_one_or_none() is not None


# ---------------------------------------------------------------------------
# Test 2: Chatwoot-origin mirrored STOP is silently skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chatwoot_mirror_stop_skips_command(session_maker, monkeypatch) -> None:
    """Chatwoot-mirror STOP: no ack, no OutboxMessage, event processed without error."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _CaptureProvider()
    phone = "+4915207156151"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    company_id=1,
                    altegio_client_id=9002,
                    display_name="Seren Mirror",
                    phone_e164=phone,
                    raw={},
                )
            )
            session.add(
                WhatsAppSender(
                    id=901,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_CW_MIRROR",
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Matches real-world event_id=4972.
            evt = WhatsAppEvent(
                dedupe_key="chatwoot:395:3388",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_chatwoot_mirror_stop_payload("PNID_CW_MIRROR", "4915207156151"),
                chatwoot_conversation_id=395,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            await handle_event(session, evt, provider)

    assert len(provider.sent) == 0

    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_cmd_stop"))
        assert result.scalar_one_or_none() is None

    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is None


# ---------------------------------------------------------------------------
# Test 3: Real-world sequence — exactly one OutboxMessage total
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_stop_then_chatwoot_mirror_creates_single_outbox(session_maker, monkeypatch) -> None:
    """Meta STOP (event 4971) then Chatwoot mirror (event 4972) → exactly one wa_cmd_stop."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _CaptureProvider()
    phone = "+4915207156152"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    company_id=1,
                    altegio_client_id=9003,
                    display_name="Seren Seq",
                    phone_e164=phone,
                    raw={},
                )
            )
            session.add(
                WhatsAppSender(
                    id=902,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_SEQ",
                    display_phone="+49",
                    is_active=True,
                )
            )
            # event_id=4971 equivalent: real Meta-origin STOP
            meta_evt = WhatsAppEvent(
                dedupe_key="wa:meta:stop:seq",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_meta_stop_payload("PNID_SEQ", "4915207156152"),
                chatwoot_conversation_id=None,
            )
            session.add(meta_evt)
            # event_id=4972 equivalent: Chatwoot mirror of the same STOP
            mirror_evt = WhatsAppEvent(
                dedupe_key="chatwoot:395:3389",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_chatwoot_mirror_stop_payload(
                    "PNID_SEQ", "4915207156152", conversation_id=395, message_id=3389
                ),
                chatwoot_conversation_id=395,
            )
            session.add(mirror_evt)
            await session.flush()

            await handle_event(session, meta_evt, provider)
            await handle_event(session, mirror_evt, provider)

    # Only one ack sent (from the Meta event).
    assert len(provider.sent) == 1

    # Only one wa_cmd_stop OutboxMessage.
    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.template_code == "wa_cmd_stop"))
        rows = result.scalars().all()
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# Test 4: Operator relay (Chatwoot-origin) is NOT blocked by the guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_relay_not_blocked_by_chatwoot_guard(session_maker, monkeypatch) -> None:
    """Operator relay must still be sent even though it's Chatwoot-origin.

    The relay check (section 0) runs before the Chatwoot-origin guard
    (section 2), so relay events are handled correctly and never dropped.
    """
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _CaptureProvider()
    phone = "+4915207156153"

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=903,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_RELAY_GUARD",
                    display_phone="+49",
                    is_active=True,
                )
            )
            # Inbound window event so the 24h customer service window is open.
            now = datetime.now(timezone.utc)
            session.add(
                WhatsAppEvent(
                    dedupe_key="wa:inbound:relay:guard",
                    received_at=now - timedelta(hours=1),
                    status="processed",
                    query={},
                    headers={},
                    payload={
                        "entry": [
                            {
                                "changes": [
                                    {
                                        "value": {
                                            "messages": [
                                                {
                                                    "from": "4915207156153",
                                                    "type": "text",
                                                    "text": {"body": "Hallo"},
                                                    "id": "wamid.win",
                                                }
                                            ],
                                            "metadata": {"phone_number_id": "PNID_RELAY_GUARD"},
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    chatwoot_conversation_id=None,
                )
            )
            relay_evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:6000:7000",
                status="received",
                error=None,
                query={},
                headers={},
                payload={
                    "_chatwoot_operator_relay": {
                        "recipient_phone": phone,
                        "text": "Hallo von Operator",
                        "conversation_id": 6000,
                        "message_id": 7000,
                        "phone_number_id": "PNID_RELAY_GUARD",
                        "agent_name": "Test",
                        "agent_id": 1,
                    }
                },
                chatwoot_conversation_id=6000,
            )
            session.add(relay_evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, relay_evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig

    assert len(provider.sent) == 1
    assert provider.sent[0]["phone_e164"] == phone


# ---------------------------------------------------------------------------
# Test 5: _apply_status_updates resolves campaign_run_id via followup_outbox_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_status_updates_resolves_run_id_via_followup_outbox_id(
    session_maker,
) -> None:
    """Delivery status for a follow-up OutboxMessage must resolve campaign_run_id.

    CampaignRecipient links the follow-up message via followup_outbox_id
    (not outbox_message_id), so _apply_status_updates must query both columns.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                status="completed",
                company_ids=[758285],
                location_id=1,
                period_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
                followup_enabled=True,
                followup_delay_days=14,
                followup_policy="unread_only",
                followup_template_name="newsletter_new_clients_followup",
                completed_at=now,
                meta={},
            )
            session.add(run)
            await session.flush()

            fu_outbox = OutboxMessage(
                company_id=758285,
                record_id=None,
                client_id=None,
                job_id=None,
                sender_id=None,
                phone_e164="+49111222333",
                template_code="newsletter_new_clients_followup",
                language="de",
                body="Follow-up text",
                status="sent",
                error=None,
                provider_message_id="wamid.FOLLOWUP-TEST-001",
                scheduled_at=now,
                sent_at=now,
                meta={},
            )
            session.add(fu_outbox)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run.id,
                company_id=758285,
                altegio_client_id=9001,
                phone_e164="+49111222333",
                followup_status="queued",
                followup_outbox_id=fu_outbox.id,
                followup_sent_at=now,
                status="sent",
            )
            session.add(recipient)
            await session.flush()

            run_id = run.id

    async with session_maker() as session:
        async with session.begin():
            resolved = await _apply_status_updates(
                session,
                [
                    {
                        "wamid": "wamid.FOLLOWUP-TEST-001",
                        "status": "delivered",
                        "timestamp": "1234567890",
                        "raw": {},
                    }
                ],
            )

    assert run_id in resolved, f"campaign_run_id={run_id} must be resolved via followup_outbox_id; got {resolved}"


# ---------------------------------------------------------------------------
# Helpers shared by P1 followup_status webhook tests
# ---------------------------------------------------------------------------


async def _create_fu_webhook_fixtures(
    session_maker,
    *,
    wamid: str,
    outbox_status: str = "sent",
    followup_status_init: str = "sent",
    use_primary_link: bool = False,
) -> tuple[int, int, int]:
    """Create CampaignRun + OutboxMessage + CampaignRecipient for webhook tests.

    Returns (run_id, outbox_id, recipient_id).
    When *use_primary_link* is True the recipient is linked via outbox_message_id
    instead of followup_outbox_id (simulates primary campaign message).
    """
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                status="completed",
                company_ids=[758285],
                location_id=1,
                period_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
                followup_enabled=True,
                followup_delay_days=14,
                followup_policy="unread_only",
                followup_template_name="newsletter_new_clients_followup",
                completed_at=now,
                meta={},
            )
            session.add(run)
            await session.flush()

            outbox = OutboxMessage(
                company_id=758285,
                record_id=None,
                client_id=None,
                job_id=None,
                sender_id=None,
                phone_e164="+49999000111",
                template_code="newsletter_new_clients_followup",
                language="de",
                body="text",
                status=outbox_status,
                error=None,
                provider_message_id=wamid,
                scheduled_at=now,
                sent_at=now,
                meta={},
            )
            session.add(outbox)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run.id,
                company_id=758285,
                altegio_client_id=9002,
                phone_e164="+49999000111",
                followup_status=followup_status_init if not use_primary_link else None,
                followup_outbox_id=outbox.id if not use_primary_link else None,
                followup_sent_at=now if not use_primary_link else None,
                outbox_message_id=outbox.id if use_primary_link else None,
                status="sent",
            )
            session.add(recipient)
            await session.flush()

            return run.id, outbox.id, recipient.id


# ---------------------------------------------------------------------------
# Test 6: followup outbox delivered → followup_status advances to 'delivered'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_status_updates_advances_followup_status_to_delivered(
    session_maker,
) -> None:
    """Delivered webhook for follow-up outbox must set followup_status='delivered'."""
    run_id, outbox_id, recipient_id = await _create_fu_webhook_fixtures(
        session_maker,
        wamid="wamid.FU-DEL-001",
        outbox_status="sent",
        followup_status_init="sent",
    )

    async with session_maker() as session:
        async with session.begin():
            resolved = await _apply_status_updates(
                session,
                [{"wamid": "wamid.FU-DEL-001", "status": "delivered", "timestamp": "t", "raw": {}}],
            )

    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)
        outbox = await session.get(OutboxMessage, outbox_id)

    assert run_id in resolved
    assert outbox is not None and outbox.status == "delivered"
    assert recipient is not None and recipient.followup_status == "delivered", (
        f"followup_status should be 'delivered', got {recipient.followup_status!r}"
    )


# ---------------------------------------------------------------------------
# Test 7: followup outbox read → followup_status advances to 'read'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_status_updates_advances_followup_status_to_read(
    session_maker,
) -> None:
    """Read webhook for follow-up outbox must set followup_status='read'."""
    run_id, outbox_id, recipient_id = await _create_fu_webhook_fixtures(
        session_maker,
        wamid="wamid.FU-READ-001",
        outbox_status="delivered",
        followup_status_init="delivered",
    )

    async with session_maker() as session:
        async with session.begin():
            resolved = await _apply_status_updates(
                session,
                [{"wamid": "wamid.FU-READ-001", "status": "read", "timestamp": "t", "raw": {}}],
            )

    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)
        outbox = await session.get(OutboxMessage, outbox_id)

    assert run_id in resolved
    assert outbox is not None and outbox.status == "read"
    assert recipient is not None and recipient.followup_status == "read", (
        f"followup_status should be 'read', got {recipient.followup_status!r}"
    )


# ---------------------------------------------------------------------------
# Test 8: no downgrade — followup_status='read' must not revert to 'delivered'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_status_updates_does_not_downgrade_read_to_delivered(
    session_maker,
) -> None:
    """Delivered webhook must not downgrade followup_status from 'read' to 'delivered'."""
    run_id, outbox_id, recipient_id = await _create_fu_webhook_fixtures(
        session_maker,
        wamid="wamid.FU-NODOWN-001",
        outbox_status="sent",
        followup_status_init="read",
    )

    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(
                session,
                [{"wamid": "wamid.FU-NODOWN-001", "status": "delivered", "timestamp": "t", "raw": {}}],
            )

    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)

    assert recipient is not None and recipient.followup_status == "read", (
        f"followup_status must stay 'read', got {recipient.followup_status!r}"
    )


# ---------------------------------------------------------------------------
# Test 9: primary outbox path — run_id returned, followup fields untouched
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_status_updates_primary_path_does_not_touch_followup_status(
    session_maker,
) -> None:
    """Delivery webhook via outbox_message_id must return run_id and leave followup_status alone."""
    run_id, outbox_id, recipient_id = await _create_fu_webhook_fixtures(
        session_maker,
        wamid="wamid.PRIMARY-001",
        outbox_status="sent",
        followup_status_init="sent",
        use_primary_link=True,
    )

    async with session_maker() as session:
        async with session.begin():
            resolved = await _apply_status_updates(
                session,
                [{"wamid": "wamid.PRIMARY-001", "status": "delivered", "timestamp": "t", "raw": {}}],
            )

    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)

    assert run_id in resolved
    assert recipient is not None and recipient.followup_status is None, (
        f"followup_status should be None (primary path), got {recipient.followup_status!r}"
    )
