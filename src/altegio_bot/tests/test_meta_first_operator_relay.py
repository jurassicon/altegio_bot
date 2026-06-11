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
 9. Session-aware: default mode (private_note_only) + window open → sends text.
10. Session-aware: window open + any mode → sends text.
11. Session-aware: private_note_only + window closed → blocks send, private note, canceled outbox.
12. Session-aware: reopen_template + window closed → sends template.
13. Session-aware: reopen template send failure handling.
14. P1: recipient_phone without '+' is normalized before send.
15. P1: recipient_phone with spaces is normalized before send.
16. P1: invalid recipient_phone sets event.error and skips send.
17. P2: private note failure surfaced in event.error / outbox.error / meta.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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
        self.templates_sent: list[dict[str, Any]] = []

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        *,
        contact_name: str | None = None,
        reply_to_provider_message_id: str | None = None,
    ) -> str:
        self.sent.append(
            {
                "sender_id": sender_id,
                "phone_e164": phone_e164,
                "text": text,
                "reply_to_provider_message_id": reply_to_provider_message_id,
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
        header_image_url: str | None = None,
    ) -> str:
        self.templates_sent.append(
            {
                "sender_id": sender_id,
                "phone_e164": phone_e164,
                "template_name": template_name,
                "language": language,
                "params": params,
            }
        )
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
    reply_to_chatwoot_message_id: int | None = None,
    content_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Payload as produced by _ingest_operator_outgoing in chatwoot.py."""
    relay: dict[str, Any] = {
        "recipient_phone": recipient_phone,
        "text": text,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "phone_number_id": phone_number_id,
        "agent_name": agent_name,
        "agent_id": 5,
    }
    if reply_to_chatwoot_message_id is not None:
        relay["reply_to_chatwoot_message_id"] = reply_to_chatwoot_message_id
    if content_attributes is not None:
        relay["content_attributes"] = content_attributes
    return {"_chatwoot_operator_relay": relay}


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


def _meta_inbound_event(
    session: Any,
    *,
    phone: str,
    dedupe_key: str,
    hours_ago: float = 1.0,
) -> WhatsAppEvent:
    """Create and add a Meta-origin inbound event that opens the 24h window."""
    now = datetime.now(timezone.utc)
    evt = WhatsAppEvent(
        dedupe_key=dedupe_key,
        received_at=now - timedelta(hours=hours_ago),
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
                                        "from": phone,
                                        "type": "text",
                                        "text": {"body": "x"},
                                        "id": f"wamid.{dedupe_key}",
                                    }
                                ],
                                "metadata": {"phone_number_id": "PNID_WIN_HELPER"},
                            }
                        }
                    ]
                }
            ]
        },
        chatwoot_conversation_id=None,
    )
    session.add(evt)
    return evt


def _forwarded_inbound_event(
    session: Any,
    *,
    phone: str,
    dedupe_key: str = "wa:reply-context:inbound",
    chatwoot_message_id: int = 4960,
    chatwoot_conversation_id: int = 230,
    whatsapp_message_id: str = "wamid.INBOUND",
    hours_ago: float = 1.0,
) -> WhatsAppEvent:
    """Create a Meta-origin inbound event already forwarded to Chatwoot."""
    now = datetime.now(timezone.utc)
    evt = WhatsAppEvent(
        dedupe_key=dedupe_key,
        received_at=now - timedelta(hours=hours_ago),
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
                                        "from": phone,
                                        "type": "text",
                                        "text": {"body": "Message test"},
                                        "id": whatsapp_message_id,
                                    }
                                ],
                                "metadata": {"phone_number_id": "PNID_REPLY_CTX"},
                            }
                        }
                    ]
                }
            ]
        },
        chatwoot_message_id=chatwoot_message_id,
        forwarded_chatwoot_conversation_id=chatwoot_conversation_id,
        whatsapp_message_id=whatsapp_message_id,
    )
    session.add(evt)
    return evt


# ---------------------------------------------------------------------------
# Test 1: operator outgoing message MUST be sent to Meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_outgoing_sent_to_meta(session_maker, monkeypatch) -> None:
    """When relay is enabled, operator message must be sent via provider."""
    # Ensure safe_send does not short-circuit on WHATSAPP_PROVIDER env var.
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)

    provider = _FakeProvider(wamid="wamid.OP_TEST_001")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, phone_number_id="PNID_OP")
            _meta_inbound_event(session, phone="+49111222333", dedupe_key="meta:inbound:op:001")

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
async def test_operator_relay_outbox_persisted(session_maker, monkeypatch) -> None:
    """Operator relay must create an OutboxMessage with source='operator'."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.OP_PERSIST")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=3, company_id=2, phone_number_id="PNID_OP2")
            _meta_inbound_event(session, phone="+49777888999", dedupe_key="meta:inbound:op2:001")

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
    # Indexed copies for the native-reply lookup (PR1).
    assert outbox.chatwoot_conversation_id == 30
    assert outbox.chatwoot_message_id == 40


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


# ---------------------------------------------------------------------------
# Tests: same-company multi-sender (Fix 1 — no over-blocking)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_relay_sender_same_company_prefers_default(
    session_maker,
) -> None:
    """Two active senders in the same company: sender_code='default' wins."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=100,
                    company_id=77,
                    sender_code="english",
                    phone_number_id="PNID_SC1",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=101,
                    company_id=77,
                    sender_code="default",
                    phone_number_id="PNID_SC1",
                    display_phone="+49",
                    is_active=True,
                )
            )

        sid, cid, err = await _resolve_relay_sender(session, "PNID_SC1")

    assert err is None
    assert cid == 77
    assert sid == 101  # sender_code='default' preferred


@pytest.mark.asyncio
async def test_resolve_relay_sender_same_company_no_default_picks_min_id(
    session_maker,
) -> None:
    """Two active senders, same company, no 'default' code → min id chosen."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=200,
                    company_id=88,
                    sender_code="english",
                    phone_number_id="PNID_SC2",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=201,
                    company_id=88,
                    sender_code="german",
                    phone_number_id="PNID_SC2",
                    display_phone="+49",
                    is_active=True,
                )
            )

        sid, cid, err = await _resolve_relay_sender(session, "PNID_SC2")

    assert err is None
    assert cid == 88
    assert sid == 200  # min id when no 'default' sender_code


@pytest.mark.asyncio
async def test_relay_same_company_multi_sender_sends_successfully(session_maker, monkeypatch) -> None:
    """Relay must succeed (not block) when two senders share same company."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.SAMECOMPANY")

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=110,
                    company_id=50,
                    sender_code="english",
                    phone_number_id="PNID_SC3",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=111,
                    company_id=50,
                    sender_code="default",
                    phone_number_id="PNID_SC3",
                    display_phone="+49",
                    is_active=True,
                )
            )

            _meta_inbound_event(session, phone="+49111222333", dedupe_key="meta:inbound:sc3:001")
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:600:700",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    phone_number_id="PNID_SC3",
                    conversation_id=600,
                    message_id=700,
                ),
                chatwoot_conversation_id=600,
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

    # Must have sent exactly once, using the 'default' sender (id=111).
    assert len(provider.sent) == 1
    # OutboxMessage created with company_id=50.
    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.SAMECOMPANY")
        )
        ob = result.scalar_one_or_none()
    assert ob is not None
    assert ob.company_id == 50
    assert ob.sender_id == 111


# ---------------------------------------------------------------------------
# Test: send failure path (Fix 3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_relay_send_failure(session_maker, monkeypatch) -> None:
    """When provider.send raises, event.error is set, no OutboxMessage created."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _ErrorProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=120,
                    company_id=99,
                    sender_code="default",
                    phone_number_id="PNID_ERR",
                    display_phone="+49",
                    is_active=True,
                )
            )
            _meta_inbound_event(session, phone="+49900100200", dedupe_key="meta:inbound:err:001")
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:900:1000",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49900100200",
                    phone_number_id="PNID_ERR",
                    conversation_id=900,
                    message_id=1000,
                ),
                chatwoot_conversation_id=900,
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

    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "send failed" in reloaded.error

    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.phone_e164 == "+49900100200"))
        assert result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Test: WARNING log on ambiguous routing (Fix 4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ambiguous_sender_logs_warning(session_maker, caplog) -> None:
    """Ambiguous routing must emit a WARNING to whatsapp_inbox_worker logger."""
    import logging

    provider = _FakeProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=130,
                    company_id=11,
                    sender_code="default",
                    phone_number_id="PNID_WARN",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=131,
                    company_id=22,
                    sender_code="default",
                    phone_number_id="PNID_WARN",
                    display_phone="+49",
                    is_active=True,
                )
            )
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:1100:1200",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    phone_number_id="PNID_WARN",
                    conversation_id=1100,
                    message_id=1200,
                ),
                chatwoot_conversation_id=1100,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            with caplog.at_level(logging.WARNING, logger="whatsapp_inbox_worker"):
                try:
                    await handle_event(session, evt, provider)
                finally:
                    _s.chatwoot_operator_relay_enabled = original

    warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("ambiguous" in m for m in warning_messages), f"expected ambiguous warning, got: {warning_messages}"
    assert any("PNID_WARN" in m for m in warning_messages), (
        f"expected phone_number_id in warning, got: {warning_messages}"
    )


# ---------------------------------------------------------------------------
# Test: operator relay must NOT mirror to Chatwoot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_relay_no_chatwoot_mirror(session_maker, monkeypatch) -> None:
    """When a ChatwootHybridProvider is passed, operator relay must use _primary
    directly and never call mirror_outbound_as_note, because the operator's
    message is already visible in Chatwoot."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)

    primary = _FakeProvider(wamid="wamid.NO_MIRROR")
    mirror_calls: list[str] = []

    class _HybridLike:
        """Simulates ChatwootHybridProvider with _primary and a tracked mirror."""

        _supports_mirror_kwargs: bool = True
        _primary = primary

        async def send(self, sender_id: int, phone_e164: str, text: str, **kwargs: Any) -> str:
            # If this is called instead of _primary.send, that's a bug.
            mirror_calls.append(phone_e164)
            return "wamid.SHOULD_NOT_APPEAR"

        async def send_template(self, *args: Any, **kwargs: Any) -> str:
            return "wamid.SHOULD_NOT_APPEAR"

    hybrid = _HybridLike()

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=200, company_id=1, phone_number_id="PNID_NM")
            _meta_inbound_event(session, phone="+49500600700", dedupe_key="meta:inbound:nm:001")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:500:600",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49500600700",
                    text="No mirror please",
                    conversation_id=500,
                    message_id=600,
                    phone_number_id="PNID_NM",
                ),
                chatwoot_conversation_id=500,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            original = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, hybrid)  # type: ignore[arg-type]
            finally:
                _s.chatwoot_operator_relay_enabled = original

    # Must have sent via _primary, not via hybrid.send (which tracks to mirror_calls).
    assert len(primary.sent) == 1, "meta_provider.send must have been called once"
    assert primary.sent[0]["phone_e164"] == "+49500600700"
    assert mirror_calls == [], "ChatwootHybridProvider.send (mirror path) must NOT be called"


# ---------------------------------------------------------------------------
# Tests: inbox_company_map routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_relay_sender_with_company_hint(session_maker) -> None:
    """company_id_hint resolves ambiguous phone_number_id to correct company."""
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=140,
                    company_id=758285,
                    sender_code="default",
                    phone_number_id="PNID_HINT",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=141,
                    company_id=1271200,
                    sender_code="default",
                    phone_number_id="PNID_HINT",
                    display_phone="+49",
                    is_active=True,
                )
            )

        sid, cid, err = await _resolve_relay_sender(session, "PNID_HINT", company_id_hint=758285)

    assert err is None
    assert cid == 758285
    assert sid == 140


@pytest.mark.asyncio
async def test_relay_with_inbox_company_map(session_maker, monkeypatch) -> None:
    """CHATWOOT_INBOX_COMPANY_MAP disambiguates relay for two company_ids."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.INBOX_MAP")

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=150,
                    company_id=758285,
                    sender_code="default",
                    phone_number_id="PNID_MAP",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=151,
                    company_id=1271200,
                    sender_code="default",
                    phone_number_id="PNID_MAP",
                    display_phone="+49",
                    is_active=True,
                )
            )

            _meta_inbound_event(session, phone="+49123000000", dedupe_key="meta:inbound:map:001")
            relay_payload: dict[str, Any] = {
                "_chatwoot_operator_relay": {
                    "recipient_phone": "+49123000000",
                    "text": "Hello via inbox map",
                    "conversation_id": 1001,
                    "message_id": 2001,
                    "phone_number_id": "PNID_MAP",
                    "chatwoot_inbox_id": 8,
                    "agent_name": "Test",
                    "agent_id": 1,
                },
            }
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:1001:2001",
                status="received",
                error=None,
                query={},
                headers={},
                payload=relay_payload,
                chatwoot_conversation_id=1001,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_map = _s.chatwoot_inbox_company_map
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_inbox_company_map = '{"8": 758285, "7": 1271200}'
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_inbox_company_map = orig_map

    assert len(provider.sent) == 1
    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.INBOX_MAP")
        )
        ob = result.scalar_one_or_none()
    assert ob is not None
    assert ob.company_id == 758285
    assert ob.sender_id == 150


@pytest.mark.asyncio
async def test_relay_inbox_not_in_map_fail_closed(session_maker) -> None:
    """inbox_id present, map configured but inbox_id absent → fail-closed."""
    provider = _FakeProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=160,
                    company_id=758285,
                    sender_code="default",
                    phone_number_id="PNID_NOMAP",
                    display_phone="+49",
                    is_active=True,
                )
            )

            relay_payload: dict[str, Any] = {
                "_chatwoot_operator_relay": {
                    "recipient_phone": "+49111000000",
                    "text": "Missing inbox",
                    "conversation_id": 1002,
                    "message_id": 2002,
                    "phone_number_id": "PNID_NOMAP",
                    "chatwoot_inbox_id": 99,
                    "agent_name": "Test",
                    "agent_id": 1,
                },
            }
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:1002:2002",
                status="received",
                error=None,
                query={},
                headers={},
                payload=relay_payload,
                chatwoot_conversation_id=1002,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_map = _s.chatwoot_inbox_company_map
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_inbox_company_map = '{"8": 758285}'  # 99 absent
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_inbox_company_map = orig_map

    assert len(provider.sent) == 0
    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "fail-closed" in reloaded.error
    assert "99" in reloaded.error


@pytest.mark.asyncio
async def test_relay_ambiguous_without_map_still_blocks(
    session_maker,
) -> None:
    """Safety guard intact: no map + ambiguous phone_number_id → still blocked."""
    provider = _FakeProvider()

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=170,
                    company_id=11,
                    sender_code="default",
                    phone_number_id="PNID_NOMAP2",
                    display_phone="+49",
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    id=171,
                    company_id=22,
                    sender_code="default",
                    phone_number_id="PNID_NOMAP2",
                    display_phone="+49",
                    is_active=True,
                )
            )

            relay_payload: dict[str, Any] = {
                "_chatwoot_operator_relay": {
                    "recipient_phone": "+49000111222",
                    "text": "No hint",
                    "conversation_id": 1003,
                    "message_id": 2003,
                    "phone_number_id": "PNID_NOMAP2",
                    "agent_name": "Test",
                    "agent_id": 1,
                },
            }
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:1003:2003",
                status="received",
                error=None,
                query={},
                headers={},
                payload=relay_payload,
                chatwoot_conversation_id=1003,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_map = _s.chatwoot_inbox_company_map
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_inbox_company_map = "{}"  # not configured
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_inbox_company_map = orig_map

    assert len(provider.sent) == 0
    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "ambiguous" in reloaded.error


# ---------------------------------------------------------------------------
# Tests: session-aware operator relay (24h window)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_private_note_only_window_open_sends_text(session_maker, monkeypatch) -> None:
    """Default mode (private_note_only) + window open → sends free-form text."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.PNO_OPEN")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=300, company_id=1, phone_number_id="PNID_FD")
            _meta_inbound_event(session, phone="+49300400500", dedupe_key="meta:inbound:pno:open")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:3000:4000",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49300400500",
                    phone_number_id="PNID_FD",
                    conversation_id=3000,
                    message_id=4000,
                ),
                chatwoot_conversation_id=3000,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    # Must send text, must NOT send template.
    assert len(provider.sent) == 1
    assert len(provider.templates_sent) == 0

    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.PNO_OPEN")
        )
        ob = result.scalar_one_or_none()

    assert ob is not None
    assert ob.template_code == "operator_relay"
    assert ob.message_source == "operator"
    assert ob.meta.get("send_type") == "text"
    assert ob.meta.get("wa_window_open") is True


@pytest.mark.asyncio
async def test_reopen_mode_window_open_sends_text(session_maker, monkeypatch) -> None:
    """mode=reopen_template + window open → sends free-form text (not template)."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.WIN_OPEN")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=301, company_id=1, phone_number_id="PNID_WO")
            _meta_inbound_event(session, phone="+49111222001", dedupe_key="meta:inbound:wo:001")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:3100:4100",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49111222001",
                    phone_number_id="PNID_WO",
                    conversation_id=3100,
                    message_id=4100,
                ),
                chatwoot_conversation_id=3100,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_mode = _s.chatwoot_operator_closed_window_mode
            orig_name = _s.chatwoot_operator_reopen_template_name
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_operator_closed_window_mode = "reopen_template"
            _s.chatwoot_operator_reopen_template_name = "test_reopen_tpl"
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_operator_closed_window_mode = orig_mode
                _s.chatwoot_operator_reopen_template_name = orig_name

    assert len(provider.sent) == 1
    assert len(provider.templates_sent) == 0

    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.WIN_OPEN")
        )
        ob = result.scalar_one_or_none()

    assert ob is not None
    assert ob.template_code == "operator_relay"
    assert ob.message_source == "operator"
    assert ob.meta.get("send_type") == "text"
    assert ob.meta.get("wa_window_open") is True


@pytest.mark.asyncio
async def test_operator_reply_to_inbound_uses_native_whatsapp_context(session_maker, monkeypatch) -> None:
    """Chatwoot Reply to inbound customer message sends Meta context.message_id."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.OP_REPLY_NEW")
    phone = "+381638400431"

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=700, company_id=1, phone_number_id="PNID_REPLY_CTX")
            _forwarded_inbound_event(
                session,
                phone="381638400431",
                dedupe_key="wa:reply-context:inbound:happy",
                chatwoot_message_id=4960,
                chatwoot_conversation_id=230,
                whatsapp_message_id="wamid.INBOUND",
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:230:4961",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone=phone,
                    text="Reply from Chatwoot",
                    conversation_id=230,
                    message_id=4961,
                    phone_number_id="PNID_REPLY_CTX",
                    reply_to_chatwoot_message_id=4960,
                    content_attributes={"in_reply_to": 4960},
                ),
                chatwoot_conversation_id=230,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 1
    assert provider.sent[0]["reply_to_provider_message_id"] == "wamid.INBOUND"

    async with session_maker() as session:
        outbox = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.OP_REPLY_NEW")
        )

    assert outbox is not None
    assert outbox.provider_message_id == "wamid.OP_REPLY_NEW"
    assert outbox.chatwoot_message_id == 4961
    assert outbox.meta["reply_to_chatwoot_message_id"] == 4960
    assert outbox.meta["reply_to_provider_message_id"] == "wamid.INBOUND"
    assert outbox.meta["reply_context_source"] == "whatsapp_event"
    assert outbox.meta["reply_context_native"] is True
    assert outbox.meta["content_attributes"] == {"in_reply_to": 4960}


@pytest.mark.asyncio
async def test_operator_reply_to_previous_operator_message_uses_outbox_context(
    session_maker,
    monkeypatch,
) -> None:
    """Chatwoot Reply to a prior operator message resolves via OutboxMessage."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.OP_SECOND")
    phone = "+381638400431"
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=701, company_id=1, phone_number_id="PNID_REPLY_CTX_OP")
            _meta_inbound_event(session, phone=phone, dedupe_key="wa:reply-context:window:operator")
            session.add(
                OutboxMessage(
                    company_id=1,
                    sender_id=701,
                    phone_e164=phone,
                    template_code="operator_relay",
                    language="de",
                    body="First operator message",
                    status="sent",
                    provider_message_id="wamid.OPERATOR",
                    scheduled_at=now,
                    sent_at=now,
                    message_source="operator",
                    chatwoot_conversation_id=230,
                    chatwoot_message_id=4964,
                    meta={},
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:230:4965",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone=phone,
                    text="Second operator message",
                    conversation_id=230,
                    message_id=4965,
                    phone_number_id="PNID_REPLY_CTX_OP",
                    reply_to_chatwoot_message_id=4964,
                    content_attributes={"in_reply_to": "4964"},
                ),
                chatwoot_conversation_id=230,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 1
    assert provider.sent[0]["reply_to_provider_message_id"] == "wamid.OPERATOR"

    async with session_maker() as session:
        outbox = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.OP_SECOND")
        )

    assert outbox is not None
    assert outbox.meta["reply_to_chatwoot_message_id"] == 4964
    assert outbox.meta["reply_to_provider_message_id"] == "wamid.OPERATOR"
    assert outbox.meta["reply_context_source"] == "outbox_operator"
    assert outbox.meta["reply_context_native"] is True


@pytest.mark.asyncio
async def test_operator_reply_missing_mapping_sends_plain_text(session_maker, monkeypatch) -> None:
    """Missing Chatwoot→wamid mapping must not block operator relay."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.OP_PLAIN_MISSING")
    phone = "+381638400431"

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=702, company_id=1, phone_number_id="PNID_REPLY_CTX_MISS")
            _meta_inbound_event(session, phone=phone, dedupe_key="wa:reply-context:window:missing")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:230:4970",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone=phone,
                    conversation_id=230,
                    message_id=4970,
                    phone_number_id="PNID_REPLY_CTX_MISS",
                    reply_to_chatwoot_message_id=9999,
                ),
                chatwoot_conversation_id=230,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 1
    assert provider.sent[0]["reply_to_provider_message_id"] is None

    async with session_maker() as session:
        outbox = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.OP_PLAIN_MISSING")
        )

    assert outbox is not None
    assert outbox.meta["reply_to_chatwoot_message_id"] == 9999
    assert outbox.meta["reply_to_provider_message_id"] is None
    assert outbox.meta["reply_context_source"] is None
    assert outbox.meta["reply_context_native"] is False


@pytest.mark.asyncio
async def test_operator_reply_cross_conversation_sends_plain_text(session_maker, monkeypatch) -> None:
    """A mapping in another Chatwoot conversation must not become Meta context."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.OP_PLAIN_CROSS")
    phone = "+381638400431"

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=703, company_id=1, phone_number_id="PNID_REPLY_CTX_CROSS")
            _forwarded_inbound_event(
                session,
                phone=phone,
                dedupe_key="wa:reply-context:inbound:cross",
                chatwoot_message_id=4960,
                chatwoot_conversation_id=999,
                whatsapp_message_id="wamid.OTHER_CONV",
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:230:4971",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone=phone,
                    conversation_id=230,
                    message_id=4971,
                    phone_number_id="PNID_REPLY_CTX_CROSS",
                    reply_to_chatwoot_message_id=4960,
                ),
                chatwoot_conversation_id=230,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 1
    assert provider.sent[0]["reply_to_provider_message_id"] is None

    async with session_maker() as session:
        outbox = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.OP_PLAIN_CROSS")
        )

    assert outbox is not None
    assert outbox.meta["reply_context_native"] is False
    assert outbox.meta["reply_to_provider_message_id"] is None


@pytest.mark.asyncio
async def test_private_note_only_window_closed(session_maker, monkeypatch) -> None:
    """Default mode (private_note_only) + window closed → blocks send, private note, canceled outbox."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.SHOULD_NOT_APPEAR_PNO")

    mock_cw_class = MagicMock()
    mock_cw = MagicMock()
    mock_cw.send_message = AsyncMock(return_value=99)
    mock_cw.aclose = AsyncMock(return_value=None)
    mock_cw_class.return_value = mock_cw

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=303, company_id=1, phone_number_id="PNID_PNO")

            # No inbound events → window closed.
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:3400:4400",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49111222004",
                    text="Ihr Termin morgen um 11 Uhr",
                    phone_number_id="PNID_PNO",
                    conversation_id=3400,
                    message_id=4400,
                    agent_name="Klaus",
                    reply_to_chatwoot_message_id=4960,
                    content_attributes={"in_reply_to": 4960},
                ),
                chatwoot_conversation_id=3400,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_mode = _s.chatwoot_operator_closed_window_mode
            orig_note = _s.chatwoot_operator_reopen_private_note_enabled
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_operator_closed_window_mode = "private_note_only"
            _s.chatwoot_operator_reopen_private_note_enabled = True
            try:
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    mock_cw_class,
                ):
                    await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_operator_closed_window_mode = orig_mode
                _s.chatwoot_operator_reopen_private_note_enabled = orig_note

    # Must NOT have sent anything to Meta.
    assert len(provider.sent) == 0
    assert len(provider.templates_sent) == 0

    # Private note must have been sent to Chatwoot.
    mock_cw.send_message.assert_called_once()
    assert mock_cw.send_message.call_args.kwargs.get("private") is True

    # OutboxMessage created with status='canceled'.
    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.phone_e164 == "+49111222004"))
        ob = result.scalar_one_or_none()

    assert ob is not None
    assert ob.template_code == "operator_relay"
    assert ob.status == "canceled"
    assert ob.message_source == "operator"
    assert ob.body == "Ihr Termin morgen um 11 Uhr"
    assert ob.provider_message_id is None
    assert ob.meta["wa_window_open"] is False
    assert ob.meta["cancel_reason"] == "customer_service_window_closed"
    assert ob.meta["agent_name"] == "Klaus"
    assert ob.meta.get("send_type") == "none"
    assert ob.meta.get("attempted_send_type") == "text"
    assert ob.meta.get("private_note_status") == "sent"
    assert ob.meta.get("reply_to_chatwoot_message_id") == 4960
    assert ob.meta.get("reply_to_provider_message_id") is None
    assert ob.meta.get("reply_context_native") is False


@pytest.mark.asyncio
async def test_reopen_template_window_closed(session_maker, monkeypatch) -> None:
    """mode=reopen_template + window closed → sends reopen template, not text."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.REOPEN_TPL")

    mock_cw_class = MagicMock()
    mock_cw = MagicMock()
    mock_cw.send_message = AsyncMock(return_value=99)
    mock_cw.aclose = AsyncMock(return_value=None)
    mock_cw_class.return_value = mock_cw

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=302, company_id=1, phone_number_id="PNID_CLOSED")

            # No inbound events → window is closed.
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:3200:4200",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49111222002",
                    text="Ihr Termin morgen um 10 Uhr",
                    phone_number_id="PNID_CLOSED",
                    conversation_id=3200,
                    message_id=4200,
                    agent_name="Maria",
                    reply_to_chatwoot_message_id=4960,
                    content_attributes={"in_reply_to": 4960},
                ),
                chatwoot_conversation_id=3200,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_mode = _s.chatwoot_operator_closed_window_mode
            orig_name = _s.chatwoot_operator_reopen_template_name
            orig_note = _s.chatwoot_operator_reopen_private_note_enabled
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_operator_closed_window_mode = "reopen_template"
            _s.chatwoot_operator_reopen_template_name = "kitilash_reopen"
            _s.chatwoot_operator_reopen_private_note_enabled = True
            try:
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    mock_cw_class,
                ):
                    await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_operator_closed_window_mode = orig_mode
                _s.chatwoot_operator_reopen_template_name = orig_name
                _s.chatwoot_operator_reopen_private_note_enabled = orig_note

    # Must NOT have sent text; MUST have sent template.
    assert len(provider.sent) == 0
    assert len(provider.templates_sent) == 1
    assert provider.templates_sent[0]["template_name"] == "kitilash_reopen"
    assert provider.templates_sent[0]["phone_e164"] == "+49111222002"

    # Private note must have been sent to Chatwoot.
    mock_cw.send_message.assert_called_once()
    assert mock_cw.send_message.call_args.kwargs.get("private") is True

    # OutboxMessage must reflect template send, not text.
    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.REOPEN_TPL")
        )
        ob = result.scalar_one_or_none()

    assert ob is not None
    assert ob.template_code == "operator_reopen_template"
    assert ob.message_source == "operator"
    assert ob.body == "Ihr Termin morgen um 10 Uhr"
    assert ob.meta["send_type"] == "template"
    assert ob.meta["template"] == "kitilash_reopen"
    assert ob.meta["original_operator_text"] == "Ihr Termin morgen um 10 Uhr"
    assert ob.meta["wa_window_open"] is False
    assert ob.meta["reopen_reason"] == "customer_service_window_closed"
    assert ob.meta["agent_name"] == "Maria"
    assert ob.meta["reply_to_chatwoot_message_id"] == 4960
    assert ob.meta["reply_to_provider_message_id"] is None
    assert ob.meta["reply_context_native"] is False


@pytest.mark.asyncio
async def test_reopen_template_send_failure_sets_error(session_maker, monkeypatch) -> None:
    """When template send fails, event.error is set and no OutboxMessage is created."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _ErrorProvider()

    mock_cw_class = MagicMock()
    mock_cw = MagicMock()
    mock_cw.send_message = AsyncMock(return_value=99)
    mock_cw.aclose = AsyncMock(return_value=None)
    mock_cw_class.return_value = mock_cw

    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    id=310,
                    company_id=1,
                    sender_code="default",
                    phone_number_id="PNID_ERR_TPL",
                    display_phone="+49",
                    is_active=True,
                )
            )

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:3300:4300",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49111222003",
                    phone_number_id="PNID_ERR_TPL",
                    conversation_id=3300,
                    message_id=4300,
                ),
                chatwoot_conversation_id=3300,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_mode = _s.chatwoot_operator_closed_window_mode
            orig_name = _s.chatwoot_operator_reopen_template_name
            orig_note = _s.chatwoot_operator_reopen_private_note_enabled
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_operator_closed_window_mode = "reopen_template"
            _s.chatwoot_operator_reopen_template_name = "kitilash_reopen"
            _s.chatwoot_operator_reopen_private_note_enabled = True
            try:
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    mock_cw_class,
                ):
                    await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_operator_closed_window_mode = orig_mode
                _s.chatwoot_operator_reopen_template_name = orig_name
                _s.chatwoot_operator_reopen_private_note_enabled = orig_note

    # event.error must be set.
    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "reopen template failed" in reloaded.error

    # Failure note must have been attempted in Chatwoot.
    mock_cw.send_message.assert_called_once()

    # No successful OutboxMessage should exist.
    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.phone_e164 == "+49111222003"))
        assert result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Tests: P1 — phone normalization in operator relay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_relay_phone_without_plus_normalized(session_maker, monkeypatch) -> None:
    """recipient_phone without leading '+' is normalized; send uses E.164 form."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.NORM_NOPLUS")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=400, company_id=1, phone_number_id="PNID_NORM1")
            # Inbound event has phone WITH '+'.
            _meta_inbound_event(session, phone="+49111222333", dedupe_key="meta:inbound:norm1:001")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:5000:6000",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="49111222333",  # no leading +
                    phone_number_id="PNID_NORM1",
                    conversation_id=5000,
                    message_id=6000,
                ),
                chatwoot_conversation_id=5000,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 1
    assert provider.sent[0]["phone_e164"] == "+49111222333"

    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.NORM_NOPLUS")
        )
        ob = result.scalar_one_or_none()
    assert ob is not None
    assert ob.phone_e164 == "+49111222333"


@pytest.mark.asyncio
async def test_relay_phone_with_spaces_normalized(session_maker, monkeypatch) -> None:
    """recipient_phone with spaces is normalized to compact E.164 before send."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.NORM_SPACES")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=401, company_id=1, phone_number_id="PNID_NORM2")
            _meta_inbound_event(session, phone="+49111222333", dedupe_key="meta:inbound:norm2:001")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:5100:6100",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49 111 222 333",  # spaces
                    phone_number_id="PNID_NORM2",
                    conversation_id=5100,
                    message_id=6100,
                ),
                chatwoot_conversation_id=5100,
            )
            session.add(evt)
            await session.flush()

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 1
    assert provider.sent[0]["phone_e164"] == "+49111222333"

    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.NORM_SPACES")
        )
        ob = result.scalar_one_or_none()
    assert ob is not None
    assert ob.phone_e164 == "+49111222333"


@pytest.mark.asyncio
async def test_relay_invalid_phone_sets_error(session_maker, monkeypatch) -> None:
    """recipient_phone='abc' (no digits) → event.error set, no send, no OutboxMessage."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.SHOULD_NOT_APPEAR_BADPHONE")

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=402, company_id=1, phone_number_id="PNID_NORM3")

            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:5200:6200",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="abc",  # invalid — no digits
                    phone_number_id="PNID_NORM3",
                    conversation_id=5200,
                    message_id=6200,
                ),
                chatwoot_conversation_id=5200,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            _s.chatwoot_operator_relay_enabled = True
            try:
                await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay

    assert len(provider.sent) == 0
    assert len(provider.templates_sent) == 0

    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "invalid recipient_phone" in reloaded.error

    async with session_maker() as session:
        result = await session.execute(
            select(OutboxMessage).where(OutboxMessage.provider_message_id == "wamid.SHOULD_NOT_APPEAR_BADPHONE")
        )
        assert result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Test: P2 — private note failure surfaced in error fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_private_note_only_note_failure_surfaced(session_maker, monkeypatch) -> None:
    """private_note_only + window closed + note send raises → error surfaced in event, outbox, meta."""
    monkeypatch.delenv("WHATSAPP_PROVIDER", raising=False)
    provider = _FakeProvider(wamid="wamid.SHOULD_NOT_APPEAR_NOTE_FAIL")

    mock_cw_class = MagicMock()
    mock_cw = MagicMock()
    mock_cw.send_message = AsyncMock(side_effect=RuntimeError("chatwoot unavailable"))
    mock_cw.aclose = AsyncMock(return_value=None)
    mock_cw_class.return_value = mock_cw

    async with session_maker() as session:
        async with session.begin():
            await _make_sender(session, sender_id=500, company_id=1, phone_number_id="PNID_NOTE_FAIL")

            # No inbound events → window closed.
            evt = WhatsAppEvent(
                dedupe_key="chatwoot_out:7000:8000",
                status="received",
                error=None,
                query={},
                headers={},
                payload=_operator_relay_payload(
                    recipient_phone="+49777000001",
                    text="Note will fail",
                    phone_number_id="PNID_NOTE_FAIL",
                    conversation_id=7000,
                    message_id=8000,
                    agent_name="Fail",
                ),
                chatwoot_conversation_id=7000,
            )
            session.add(evt)
            await session.flush()
            evt_id = evt.id

            from altegio_bot.settings import settings as _s

            orig_relay = _s.chatwoot_operator_relay_enabled
            orig_mode = _s.chatwoot_operator_closed_window_mode
            orig_note = _s.chatwoot_operator_reopen_private_note_enabled
            _s.chatwoot_operator_relay_enabled = True
            _s.chatwoot_operator_closed_window_mode = "private_note_only"
            _s.chatwoot_operator_reopen_private_note_enabled = True
            try:
                with patch(
                    "altegio_bot.workers.whatsapp_inbox_worker.ChatwootClient",
                    mock_cw_class,
                ):
                    await handle_event(session, evt, provider)
            finally:
                _s.chatwoot_operator_relay_enabled = orig_relay
                _s.chatwoot_operator_closed_window_mode = orig_mode
                _s.chatwoot_operator_reopen_private_note_enabled = orig_note

    # Must NOT have sent anything to Meta.
    assert len(provider.sent) == 0
    assert len(provider.templates_sent) == 0

    # event.error must mention the private note failure.
    async with session_maker() as session:
        reloaded = await session.get(WhatsAppEvent, evt_id)
    assert reloaded is not None
    assert reloaded.error is not None
    assert "private note failed" in reloaded.error

    # Canceled OutboxMessage must exist with failure metadata.
    async with session_maker() as session:
        result = await session.execute(select(OutboxMessage).where(OutboxMessage.phone_e164 == "+49777000001"))
        ob = result.scalar_one_or_none()

    assert ob is not None
    assert ob.status == "canceled"
    assert ob.error is not None
    assert "private note failed" in ob.error
    assert ob.meta.get("private_note_status") == "failed"
    assert ob.meta.get("private_note_error") is not None
