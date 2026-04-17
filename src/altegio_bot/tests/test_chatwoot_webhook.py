"""Tests for Chatwoot webhook handler."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from httpx import ASGITransport, AsyncClient


def _cw_payload(
    phone: str = "+4915112345678",
    content: str = "Hello",
    conversation_id: int = 1,
    message_id: int = 1,
    message_type: int | str = 0,  # 0 is incoming in Chatwoot
) -> dict:
    """Build a minimal Chatwoot message_created webhook payload."""
    return {
        "event": "message_created",
        "id": message_id,
        "content": content,
        "message_type": message_type,
        "created_at": 1234567890,
        "conversation": {
            "id": conversation_id,
        },
        "sender": {
            "phone_number": phone,
        },
        "account": {
            "id": 2,
        },
    }


@pytest.mark.asyncio
async def test_incoming_message_saved(session_maker) -> None:
    """Incoming message webhook should create a WhatsAppEvent with chatwoot_conversation_id."""
    # Patch SessionLocal to use the test session_maker
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app

    original_session_local = cw_module.SessionLocal

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            payload = _cw_payload(phone="+49123456789", content="STOP", conversation_id=99)
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True
            assert data.get("duplicate") is False  # FIX: Now returns duplicate field

            # Verify event was saved
            async with session_maker() as session:
                from sqlalchemy import select

                from altegio_bot.models.models import WhatsAppEvent

                stmt = select(WhatsAppEvent).where(WhatsAppEvent.dedupe_key == "chatwoot:99:1")
                result = await session.execute(stmt)
                event = result.scalar_one_or_none()
                assert event is not None
                assert event.payload["_chatwoot"]["conversation_id"] == 99

    finally:
        cw_module.SessionLocal = original_session_local


@pytest.mark.asyncio
async def test_outgoing_message_skipped(session_maker) -> None:
    """Outgoing messages from the bot should be skipped."""
    import altegio_bot.webhooks.chatwoot as cw_module

    original_session_local = cw_module.SessionLocal

    from altegio_bot.main import app

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            payload = _cw_payload(message_type=1)  # 1 — это outgoing
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("skipped") == "message_type=1"

    finally:
        cw_module.SessionLocal = original_session_local


@pytest.mark.asyncio
async def test_duplicate_skipped(session_maker) -> None:
    """Sending the same payload twice should return duplicate=True on
    second call.
    """
    import altegio_bot.webhooks.chatwoot as cw_module

    original_session_local = cw_module.SessionLocal

    from altegio_bot.main import app

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        payload = _cw_payload(conversation_id=77)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp1 = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            resp2 = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
        assert resp1.json()["duplicate"] is False
        assert resp2.json()["duplicate"] is True

    finally:
        cw_module.SessionLocal = original_session_local


@pytest.mark.asyncio
async def test_signature_rejected_when_secret_set() -> None:
    """When chatwoot_webhook_secret is set, bad/missing signature → 403."""
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _settings

    original_secret = _settings.chatwoot_webhook_secret
    try:
        _settings.chatwoot_webhook_secret = "super-secret"
        payload = _cw_payload()
        body = json.dumps(payload).encode()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            # Bad signature
            resp = await tc.post(
                "/webhook/chatwoot",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Chatwoot-Signature": "badhex",
                },
            )
            assert resp.status_code == 403  # FIX: Now correctly rejects

            # No signature
            resp = await tc.post(
                "/webhook/chatwoot",
                content=body,
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 403

            # Valid signature
            sig = hmac.new(b"super-secret", body, hashlib.sha256).hexdigest()
            resp = await tc.post(
                "/webhook/chatwoot",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Chatwoot-Signature": sig,
                },
            )
            assert resp.status_code == 200

    finally:
        _settings.chatwoot_webhook_secret = original_secret


@pytest.mark.asyncio
async def test_operator_relay_user_sender_type(session_maker) -> None:
    """API inbox agent messages (sender.type='user') should be relayed when relay enabled."""
    import altegio_bot.webhooks.chatwoot as cw_module
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _settings

    original_session_local = cw_module.SessionLocal
    original_relay = _settings.chatwoot_operator_relay_enabled
    original_phone_id = _settings.meta_wa_phone_number_id

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        _settings.chatwoot_operator_relay_enabled = True
        _settings.meta_wa_phone_number_id = "123456789"

        payload = {
            "event": "message_created",
            "id": 999,
            "content": "Hello from agent",
            "message_type": 1,
            "private": False,
            "content_type": "text",
            "conversation": {
                "id": 50,
                "meta": {
                    "sender": {"phone_number": "+49111222333"},
                },
            },
            "sender": {"type": "user", "name": "Test Agent", "id": 7},
            "account": {"id": 2},
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
        assert data.get("dedupe_key") == "chatwoot_out:50:999"

        async with session_maker() as session:
            from sqlalchemy import select

            from altegio_bot.models.models import WhatsAppEvent

            stmt = select(WhatsAppEvent).where(WhatsAppEvent.dedupe_key == "chatwoot_out:50:999")
            result = await session.execute(stmt)
            event = result.scalar_one_or_none()
            assert event is not None
            relay = event.payload.get("_chatwoot_operator_relay") or {}
            assert relay["recipient_phone"] == "+49111222333"
            assert relay["text"] == "Hello from agent"

    finally:
        cw_module.SessionLocal = original_session_local
        _settings.chatwoot_operator_relay_enabled = original_relay
        _settings.meta_wa_phone_number_id = original_phone_id
