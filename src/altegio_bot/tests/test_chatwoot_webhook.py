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
    source_id: str | None = None,
    status: str | None = None,
) -> dict:
    """Build a minimal Chatwoot message_created webhook payload."""
    payload: dict = {
        "event": "message_created",
        "id": message_id,
        "content": content,
        "message_type": message_type,
        "created_at": 1234567890,
        "conversation": {
            "id": conversation_id,
            "meta": {
                "sender": {
                    "phone_number": phone,
                },
            },
        },
        "sender": {
            "phone_number": phone,
        },
        "account": {
            "id": 2,
        },
    }
    if source_id is not None:
        payload["source_id"] = source_id
    if status is not None:
        payload["status"] = status
    return payload


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
    """Outgoing messages without source_id (system messages) should be skipped."""
    import altegio_bot.webhooks.chatwoot as cw_module

    original_session_local = cw_module.SessionLocal

    from altegio_bot.main import app

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            payload = _cw_payload(message_type=1)  # 1 — outgoing, no source_id
            resp = await tc.post(
                "/webhook/chatwoot",
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("skipped") == "outgoing_no_source_id"

    finally:
        cw_module.SessionLocal = original_session_local


@pytest.mark.asyncio
async def test_outgoing_delivery_status_saved(session_maker) -> None:
    """Outgoing messages with source_id should be saved as delivery status events."""
    import altegio_bot.webhooks.chatwoot as cw_module

    original_session_local = cw_module.SessionLocal

    from altegio_bot.main import app

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        source_id = "wamid.HBgLNzkyNjgyMDI5ODIVAgARGBJBNTFBMkI3MTQyRTc2RUNBRTgA"
        payload = _cw_payload(
            phone="+79268202982",
            message_type=1,
            message_id=126,
            conversation_id=11,
            source_id=source_id,
            status="read",
        )

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
            assert data.get("dedupe_key") == "chatwoot_delivery:126"

        # Verify the event was saved with delivery status format
        async with session_maker() as session:
            from sqlalchemy import select

            from altegio_bot.models.models import WhatsAppEvent

            stmt = select(WhatsAppEvent).where(WhatsAppEvent.dedupe_key == "chatwoot_delivery:126")
            result = await session.execute(stmt)
            event = result.scalar_one_or_none()
            assert event is not None
            status_entry = event.payload["entry"][0]["changes"][0]["value"]["statuses"][0]
            assert status_entry["id"] == source_id
            assert status_entry["status"] == "read"
            assert status_entry["recipient_id"] == "79268202982"
            assert event.payload["_chatwoot"]["conversation_id"] == 11
            assert event.payload["_chatwoot"]["message_id"] == 126

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
