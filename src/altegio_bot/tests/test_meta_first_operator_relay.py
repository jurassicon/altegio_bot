"""Tests for Meta-first operator relay architecture.

Covers all required scenarios:

 1. Chatwoot outgoing operator message that MUST be sent to Meta.
 2. Chatwoot incoming customer message that must NOT be re-sent to Meta.
 3. Private note / internal activity that must NOT go to Meta.
 4. Loop prevention: mirrored bot traffic is never re-sent to Meta.
 5. DB persistence of operator-originated outbound OutboxMessage.
 6. Matching a subsequent Meta status webhook with the outbound record.
 7. Idempotency / duplicate webhook handling.
 8. Migration compatibility: OutboxMessage inserts without message_source
    succeed (server_default='bot') and existing rows read back correctly.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from altegio_bot.models.models import OutboxMessage, WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.workers.whatsapp_inbox_worker import (
    _apply_status_updates,
    _is_operator_relay,
    _resolve_relay_sender,
    handle_event,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeProvider(WhatsAppProvider):
    """Provider that records calls and returns a fixed wamid."""

    def __init__(self, wamid: str = "wamid.OPERATOR001") -> None:
        self.wamid = wamid
        self.sent: list[dict[str, Any]] = []

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
    ) -> str:
        self.sent.append(
            {
                "sender_id": sender_id,
                "phone_e164": phone_e164,
                "text": text,
            }
        )
        return self.wamid

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
        fallback_text: str = "",
        *,
        contact_name: str | None = None,
    ) -> str:
        return self.wamid


class _ErrorProvider(WhatsAppProvider):
    """Provider that always raises on send."""

    async def send(self, *args: Any, **kwargs: Any) -> str:
        raise RuntimeError("meta api unavailable")

    async def send_template(self, *args: Any, **kwargs: Any) -> str:
        raise RuntimeError("meta api unavailable")


def _operator_relay_payload(
    recipient_phone: str = "+49111222333",
    text: str = "Hello from operator",
    conversation_id: int = 10,
    message_id: int = 20,
    phone_number_id: str = "PNID_OP",
    agent_name: str = "Anna",
) -> dict[str, Any]:
    """Payload as produced by _ingest_operator_outgoing in chatwoot.py."""
    return {
        "_chatwoot_operator_relay": {
            "recipient_phone": recipient_phone,
            "text": text,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "phone_number_id": phone_number_id,
            "agent_name": agent_name,
            "agent_id": 5,
        },
    }


def _customer_incoming_payload(
    phone_number_id: str,
    from_phone: str,
    text: str,
) -> dict[str, Any]:
    """Payload produced by _ingest_incoming in chatwoot.py."""
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {
                                "phone_number_id": phone_number_id,
                            },
                            "messages": [
                                {
                                    "from": from_phone,
                                    "type": "text",
                                    "text": {"body": text},
                                    "id": "cw-msg-001",
                                    "timestamp": "1700000000",
                                }
                            ],
                        }
                    }
                ]
            }
        ],
        "_chatwoot": {
            "conversation_id": 99,
            "message_id": 1,
            "account_id": 2,
        },
    }


async def _make_sender(
    session: Any,
    *,
    sender_id: int = 1,
    company_id: int = 1,
    phone_number_id: str = "PNID_OP",
) -> WhatsAppSender:
    sender = WhatsAppSender(
        id=sender_id,
        company_id=company_id,
        sender_code="default",
        phone_number_id=phone_number_id,
        display_phone="+49000000000",
        is_active=True,
    )
    session.add(sender)
    await session.flush()
    return sender


# ---------------------------------------------------------------------------
# Test 1: operator outgoing message MUST be sent to Meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_outgoing_sent_to_meta(session_maker) -> None:
    """When relay is enabled, operator message must be sent via provider."""
    provider = _FakeProvider(wamid="wamid.OP_TEST_001")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, phone_number_id="PNID_OP")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:10:20",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(),
                chatwoot_conversation_id=10,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = original

    assert len(provider.sent) == 1
    assert provider.sent[0]["phone_e164"] == "+49111222333"
    assert provider.sent[0]["text"] == "Hello from operator"


# ---------------------------------------------------------------------------
# Test 2: incoming customer message must NOT be re-sent to Meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_incoming_customer_not_sent_to_meta(session_maker) -> None:
    """An incoming customer message (chatwoot-origin) must never be relayed."""
    provider = _FakeProvider()

    payload = _customer_incoming_payload(
        phone_number_id="PNID_IN",
        from_phone="49987654321",
        text="Hello bot",
    )

    # _is_operator_relay must return False for customer incoming payloads.
    assert not _is_operator_relay(payload)

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=2, phone_number_id="PNID_IN")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot:99:1",
                status="received",
                error=None,
                query={},
                headers={},
                payload=payload,
                chatwoot_conversation_id=99,
            )
            session.add(evt)
            await session.flush()

            mock_cw_class = MagicMock()
            mock_cw = MagicMock()
            mock_cw.log_incoming_message = AsyncMock(return_value=None)
            mock_cw.aclose = AsyncMock(return_value=None)
            mock_cw_class.return_value = mock_cw

            with patch(
                "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                mock_cw_class,
            ):
                await handle_event(session, evt, provider)

    # Provider send must NOT have been called for a customer incoming message.
    assert len(provider.sent) == 0


# ---------------------------------------------------------------------------
# Test 3: private note must NOT go to Meta
# ---------------------------------------------------------------------------


def test_private_note_payload_not_operator_relay() -> None:
    """A private note payload must not be identified as operator relay."""
    # The webhook handler rejects private notes before creating an event,
    # so by the time a payload reaches the worker it cannot be private.
    # This test verifies that a mistakenly crafted payload without the
    # _chatwoot_operator_relay key is correctly classified.
    payload_no_relay: dict[str, Any] = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": "X"},
                            "messages": [],
                        }
                    }
                ]
            }
        ],
        "_chatwoot": {"conversation_id": 5, "message_id": 6},
    }
    assert not _is_operator_relay(payload_no_relay)


@pytest.mark.asyncio
async def test_webhook_private_note_skipped(session_maker) -> None:
    """Chatwoot webhook: private=True outgoing must return skipped."""
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _s

    original_session_local = cw_module.SessionLocal
    original_relay = _s.chatwoot_operator_relay_enabled

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        _s.chatwoot_operator_relay_enabled = True

        payload = {
            "event": "message_created",
            "id": 77,
            "content": "Internal note",
            "message_type": 1,
            "private": True,
            "content_type": "text",
            "sender": {"id": 5, "name": "Agent", "type": "agent"},
            "conversation": {
                "id": 3,
                "meta": {
                    "sender": {
                        "phone_number": "+49123000000",
                        "name": "Customer",
                    }
                },
            },
            "account": {"id": 1},
        }

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )

        assert resp.status_code == 200
        assert resp.json().get("skipped") == "private_note"

    finally:
        cw_module.SessionLocal = original_session_local
        _s.chatwoot_operator_relay_enabled = original_relay


@pytest.mark.asyncio
async def test_webhook_activity_content_type_skipped(session_maker) -> None:
    """Chatwoot webhook: content_type=activity must return skipped."""
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _s

    original_session_local = cw_module.SessionLocal
    original_relay = _s.chatwoot_operator_relay_enabled

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        _s.chatwoot_operator_relay_enabled = True

        payload = {
            "event": "message_created",
            "id": 88,
            "content": "Conversation was assigned to Anna",
            "message_type": 1,
            "private": False,
            "content_type": "activity",
            "sender": {"id": 5, "name": "System", "type": "agent"},
            "conversation": {
                "id": 4,
                "meta": {
                    "sender": {
                        "phone_number": "+49123000000",
                    }
                },
            },
            "account": {"id": 1},
        }

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )

        assert resp.status_code == 200
        assert resp.json().get("skipped") == "content_type=activity"

    finally:
        cw_module.SessionLocal = original_session_local
        _s.chatwoot_operator_relay_enabled = original_relay


# ---------------------------------------------------------------------------
# Test 4: loop prevention — bot mirrored traffic must not re-enter Meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loop_prevention_bot_outgoing_not_relayed(
    session_maker,
) -> None:
    """Outgoing messages with sender_type='agent_bot' must be skipped."""
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _s

    original_session_local = cw_module.SessionLocal
    original_relay = _s.chatwoot_operator_relay_enabled

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        _s.chatwoot_operator_relay_enabled = True

        # agent_bot — this is the bot's own outgoing message mirrored back.
        payload = {
            "event": "message_created",
            "id": 101,
            "content": "Ihr Termin wurde bestätigt.",
            "message_type": 1,
            "private": False,
            "content_type": "text",
            "sender": {"id": 99, "name": "altegio_bot", "type": "agent_bot"},
            "conversation": {
                "id": 5,
                "meta": {
                    "sender": {
                        "phone_number": "+49999888777",
                    }
                },
            },
            "account": {"id": 1},
        }

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )

        data = resp.json()
        assert resp.status_code == 200
        # Must be skipped — not stored as operator relay.
        assert data.get("skipped") == "message_type=1"

    finally:
        cw_module.SessionLocal = original_session_local
        _s.chatwoot_operator_relay_enabled = original_relay


# ---------------------------------------------------------------------------
# Test 5: DB persistence of operator outbound OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_relay_outbox_persisted(session_maker) -> None:
    """Operator relay must create an OutboxMessage with source='operator'."""
    provider = _FakeProvider(wamid="wamid.OP_PERSIST")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=3, company_id=2, phone_number_id="PNID_OP2")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:30:40",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49777888999",
                    text="Wir erwarten Sie morgen.",
                    conversation_id=30,
                    message_id=40,
                    phone_number_id="PNID_OP2",
                    agent_name="Boris",
                ),
                chatwoot_conversation_id=30,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = original

    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.OP_PERSIST")
        )
        outbox = result.scalar_one_or_none()

    assert outbox is not None
    assert outbox.message_source == "operator"
    assert outbox.phone_e164 == "+49777888999"
    assert outbox.body == "Wir erwarten Sie morgen."
    assert outbox.template_code == "operator_relay"
    assert outbox.status == "sent"
    assert outbox.meta.get("agent_name") == "Boris"
    assert outbox.meta.get("chatwoot_conversation_id") == 30


# ---------------------------------------------------------------------------
# Test 6: Meta status webhook matches the operator outbound record
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_status_matched_to_operator_outbox(session_maker) -> None:
    """Meta delivery webhook must update the OutboxMessage created by relay."""
    wamid = "wamid.STATUS_MATCH"

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=1,
                client_id=None,
                record_id=None,
                job_id=None,
                sender_id=None,
                phone_e164="+49100200300",
                template_code="operator_relay",
                language="de",
                body="Auf Wiedersehen",
                status="sent",
                provider_message_id=wamid,
                scheduled_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                sent_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                message_source="operator",
                meta={},
            )
            session.add(outbox)
            await session.flush()
            outbox_id = outbox.id

    status_updates = [{"wamid": wamid, "status": "delivered", "timestamp": "1700000001", "raw": {}}]

    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(session, status_updates)

    async with session_maker() as session:
        updated = await session.get(OutboxMessage, outbox_id)

    assert updated is not None
    assert updated.status == "delivered"
    assert "wa_status_delivered" in updated.meta


# ---------------------------------------------------------------------------
# Test 7: Idempotency — duplicate operator relay webhook
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_relay_duplicate_dedupe(session_maker) -> None:
    """Sending the same operator outgoing webhook twice must deduplicate."""
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _s

    original_session_local = cw_module.SessionLocal
    original_relay = _s.chatwoot_operator_relay_enabled

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        _s.chatwoot_operator_relay_enabled = True

        payload = {
            "event": "message_created",
            "id": 555,
            "content": "Duplicate test",
            "message_type": 1,
            "private": False,
            "content_type": "text",
            "sender": {"id": 5, "name": "Agent", "type": "agent"},
            "conversation": {
                "id": 200,
                "meta": {
                    "sender": {
                        "phone_number": "+49500600700",
                    }
                },
            },
            "account": {"id": 1},
        }

        body = json.dumps(payload).encode()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp1 = await tc.post(
                "/webhook/chatwoot",
                content=body,
                headers={"Content-Type": "application/json"},
            )
            resp2 = await tc.post(
                "/webhook/chatwoot",
                content=body,
                headers={"Content-Type": "application/json"},
            )

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        assert resp1.json()["duplicate"] is False
        assert resp2.json()["duplicate"] is True

    finally:
        cw_module.SessionLocal = original_session_local
        _s.chatwoot_operator_relay_enabled = original_relay


# ---------------------------------------------------------------------------
# Test 7b: Idempotency — duplicate Meta status webhook
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_status_idempotent(session_maker) -> None:
    """Applying the same Meta status twice must not regress the status."""
    wamid = "wamid.IDEMPOTENT"

    async with session_maker() as session:
        async with session.begin():
            outbox = OutboxMessage(
                company_id=1,
                client_id=None,
                record_id=None,
                job_id=None,
                sender_id=None,
                phone_e164="+49000111222",
                template_code="operator_relay",
                language="de",
                body="Test",
                status="sent",
                provider_message_id=wamid,
                scheduled_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                sent_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                message_source="operator",
                meta={},
            )
            session.add(outbox)
            await session.flush()
            outbox_id = outbox.id

    delivered_update = [{"wamid": wamid, "status": "delivered", "timestamp": "1", "raw": {}}]
    read_update = [{"wamid": wamid, "status": "read", "timestamp": "2", "raw": {}}]
    # Apply read first, then delivered — delivered must NOT overwrite read.
    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(session, read_update)

    async with session_maker() as session:
        async with session.begin():
            await _apply_status_updates(session, delivered_update)

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)

    assert ob is not None
    assert ob.status == "read", "delivered must not regress from read"


# ---------------------------------------------------------------------------
# Test 8: Migration compatibility — OutboxMessage without message_source
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_without_message_source_uses_default(
    session_maker,
) -> None:
    """OutboxMessage created without message_source gets server_default='bot'."""
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            # Do NOT set message_source — simulates old code path.
            outbox = OutboxMessage(
                company_id=1,
                client_id=None,
                record_id=None,
                job_id=None,
                sender_id=None,
                phone_e164="+49legacy",
                template_code="reminder_24h",
                language="de",
                body="Reminder",
                status="sent",
                provider_message_id="wamid.LEGACY",
                scheduled_at=now,
                sent_at=now,
                meta={},
            )
            session.add(outbox)
            await session.flush()
            outbox_id = outbox.id

    async with session_maker() as session:
        ob = await session.get(OutboxMessage, outbox_id)

    assert ob is not None
    # server_default='bot' — must be 'bot' when not explicitly set.
    assert ob.message_source == "bot"


# ---------------------------------------------------------------------------
# Test 8b: Feature flag OFF — operator relay event stored but not forwarded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_relay_disabled_by_flag(session_maker) -> None:
    """When chatwoot_operator_relay_enabled=False, relay events are no-op."""
    provider = _FakeProvider()

    async with session_maker() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:999:888",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(conversation_id=999, message_id=888),
                chatwoot_conversation_id=999,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = False
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = original

    # Provider must NOT have been called.
    assert len(provider.sent) == 0


# ---------------------------------------------------------------------------
# Test: webhook accepted when relay flag on and sender type is 'agent'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_operator_outgoing_accepted(session_maker) -> None:
    """Valid operator outgoing message must be stored as event when flag on."""
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _s

    original_session_local = cw_module.SessionLocal
    original_relay = _s.chatwoot_operator_relay_enabled

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        _s.chatwoot_operator_relay_enabled = True

        payload = {
            "event": "message_created",
            "id": 300,
            "content": "Guten Tag, wir bestätigen Ihren Termin.",
            "message_type": 1,
            "private": False,
            "content_type": "text",
            "sender": {"id": 7, "name": "Maria", "type": "agent"},
            "conversation": {
                "id": 400,
                "meta": {
                    "sender": {
                        "phone_number": "+4912312312300",
                        "name": "Customer",
                    }
                },
            },
            "account": {"id": 1},
        }

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data.get("duplicate") is False

        # Verify WhatsAppEvent was created with operator relay marker.
        async with session_maker() as session:
            result = await session.execute(
                select(WhatsAppEvent).where(WhatsAppEvent.dedupe_key == "chatwoot_out:400:300")
            )
            evt = result.scalar_one_or_none()

        assert evt is not None
        assert "_chatwoot_operator_relay" in evt.payload
        relay = evt.payload["_chatwoot_operator_relay"]
        assert relay["recipient_phone"] == "+4912312312300"
        assert relay["text"] == "Guten Tag, wir bestätigen Ihren Termin."

    finally:
        cw_module.SessionLocal = original_session_local
        _s.chatwoot_operator_relay_enabled = original_relay


# ---------------------------------------------------------------------------
# Tests: ambiguous sender routing (pre-merge hardening)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ambiguous_sender_blocks_relay(session_maker) -> None:
    """Two active senders for the same phone_number_id → relay blocked.

    OutboxMessage must NOT be created and event.error must describe the
    ambiguity so operators can diagnose the misconfiguration.
    """
    provider = _FakeProvider(wamid="wamid.SHOULD_NOT_APPEAR")

    async with session_maker() as session:
        async with session.begin():
            # Two active senders, same phone_number_id, different company_ids.
            session.add(
                WhatsAppSender(
                    id=50,
                    company_id=10,
                    sender_code="default",
                    phone_number_id="PNID_SHARED",
                    display_phone="+49000000000",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=51,
                    company_id=20,
                    sender_code="default",
                    phone_number_id="PNID_SHARED",
                    display_phone="+49000000000",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:700:800",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49700800900",
                    phone_number_id="PNID_SHARED",
                    conversation_id=700,
                    message_id=800,
                ),
                chatwoot_conversation_id=700,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = original

    # Provider must NOT have been called.
    assert len(provider.sent) == 0

    # event.error must describe the ambiguity.
    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "ambiguous" in reloaded.error
    assert "PNID_SHARED" in reloaded.error

    # No OutboxMessage created.
    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.SHOULD_NOT_APPEAR")
        )
        assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_ambiguous_sender_no_outbox_created(session_maker) -> None:
    """Verify zero OutboxMessage rows exist after ambiguous relay attempt."""
    provider = _FakeProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=60,
                    company_id=30,
                    sender_code="a",
                    phone_number_id="PNID_AMB2",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=61,
                    company_id=40,
                    sender_code="b",
                    phone_number_id="PNID_AMB2",
                    display_phone="+49",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:800:900",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    phone_number_id="PNID_AMB2",
                    conversation_id=800,
                    message_id=900,
                ),
                chatwoot_conversation_id=800,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = original

    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.template_code == "operator_relay"))
        rows = result.scalars().all()

    assert len(rows) == 0


@pytest.mark.asyncio
async def test_resolve_relay_sender_zero(session_maker) -> None:
    """_resolve_relay_sender returns error when no active sender exists."""
    async with session_maker() as session:
        sid, cid, err = await _resolve_relay_sender(session, "PNID_NONE")
    assert sid is None
    assert cid is None
    assert err is not None
    assert "no active sender" in err


@pytest.mark.asyncio
async def test_resolve_relay_sender_one(session_maker) -> None:
    """_resolve_relay_sender succeeds when exactly one active sender exists."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=70,
                    company_id=99,
                    sender_code="solo",
                    phone_number_id="PNID_SOLO",
                    display_phone="+49",
                    is_active=True,
                )
            )

        sid, cid, err = await _resolve_relay_sender(session, "PNID_SOLO")

    assert err is None
    assert sid == 70
    assert cid == 99


@pytest.mark.asyncio
async def test_resolve_relay_sender_many(session_maker) -> None:
    """_resolve_relay_sender returns ambiguous error for >1 active senders."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=80,
                    company_id=11,
                    sender_code="x",
                    phone_number_id="PNID_MULTI",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=81,
                    company_id=22,
                    sender_code="y",
                    phone_number_id="PNID_MULTI",
                    display_phone="+49",
                    is_active=True,
                )
            )

        sid, cid, err = await _resolve_relay_sender(session, "PNID_MULTI")

    assert sid is None
    assert cid is None
    assert err is not None
    assert "ambiguous" in err
    assert "PNID_MULTI" in err


@pytest.mark.asyncio
async def test_resolve_relay_sender_inactive_not_counted(
    session_maker,
) -> None:
    """Inactive senders must not cause a false ambiguity error."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=90,
                    company_id=55,
                    sender_code="active",
                    phone_number_id="PNID_MIXED",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=91,
                    company_id=66,
                    sender_code="inactive",
                    phone_number_id="PNID_MIXED",
                    display_phone="+49",
                    is_active=False,  # inactive — must be ignored
                )
            )

        sid, cid, err = await _resolve_relay_sender(session, "PNID_MIXED")

    assert err is None
    assert sid == 90
    assert cid == 55
