"""Tests for the /webhook/chatwoot endpoint."""
from __future__ import annotations

import json
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from altegio_bot.models.models import WhatsAppEvent


def _cw_payload(
    phone: str = '+49123456789',
    content: str = 'STOP',
    conversation_id: int = 42,
    event: str = 'message_created',
    message_type: str = 'incoming',
) -> dict[str, Any]:
    return {
        'event': event,
        'message_type': message_type,
        'content': content,
        'conversation': {
            'id': conversation_id,
            'meta': {
                'sender': {
                    'phone_number': phone,
                }
            },
        },
        'inbox': {'id': 1},
    }


@pytest.mark.asyncio
async def test_incoming_message_saved(session_maker) -> None:
    """Incoming message webhook should create a WhatsAppEvent with chatwoot_conversation_id."""
    import os
    os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://localhost/test')
    os.environ.setdefault('ALTEGIO_WEBHOOK_SECRET', 'test')

    from altegio_bot.main import app

    # Patch SessionLocal to use the test session_maker
    import altegio_bot.webhooks.chatwoot as cw_module
    original_session_local = cw_module.SessionLocal

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as tc:
            payload = _cw_payload(phone='+49123456789', content='STOP', conversation_id=99)
            resp = await tc.post(
                '/webhook/chatwoot',
                content=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data['ok'] is True
            assert data.get('duplicate') is False

        # Verify DB row
        async with session_maker() as session:
            res = await session.execute(
                select(WhatsAppEvent).order_by(WhatsAppEvent.id.desc()).limit(1)
            )
            evt = res.scalar_one_or_none()
            assert evt is not None
            assert evt.chatwoot_conversation_id == 99
            assert evt.status == 'received'

    finally:
        cw_module.SessionLocal = original_session_local  # type: ignore[assignment]


@pytest.mark.asyncio
async def test_outgoing_message_skipped(session_maker) -> None:
    """Outgoing messages from the bot should be skipped (not saved as events)."""
    import altegio_bot.webhooks.chatwoot as cw_module
    original_session_local = cw_module.SessionLocal

    from altegio_bot.main import app

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as tc:
            payload = _cw_payload(message_type='outgoing')
            resp = await tc.post(
                '/webhook/chatwoot',
                content=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data.get('skipped') is True

        # No events should be created
        async with session_maker() as session:
            res = await session.execute(
                select(WhatsAppEvent).where(
                    WhatsAppEvent.chatwoot_conversation_id.is_not(None)
                )
            )
            evts = res.scalars().all()
            assert evts == []
    finally:
        cw_module.SessionLocal = original_session_local  # type: ignore[assignment]


@pytest.mark.asyncio
async def test_duplicate_skipped(session_maker) -> None:
    """Sending the same payload twice should return duplicate=True on second call."""
    import altegio_bot.webhooks.chatwoot as cw_module
    original_session_local = cw_module.SessionLocal

    from altegio_bot.main import app

    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]

        payload = _cw_payload(conversation_id=77)
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as tc:
            resp1 = await tc.post(
                '/webhook/chatwoot',
                content=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
            )
            resp2 = await tc.post(
                '/webhook/chatwoot',
                content=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
            )
        assert resp1.json()['duplicate'] is False
        assert resp2.json()['duplicate'] is True
    finally:
        cw_module.SessionLocal = original_session_local  # type: ignore[assignment]


@pytest.mark.asyncio
async def test_invalid_json_returns_400(session_maker) -> None:
    """Bad JSON body should return HTTP 400."""
    from altegio_bot.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as tc:
        resp = await tc.post(
            '/webhook/chatwoot',
            content=b'not-json',
            headers={'Content-Type': 'application/json'},
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_signature_rejected_when_secret_set() -> None:
    """When chatwoot_webhook_secret is set, bad/missing signature → 403."""
    from altegio_bot.main import app
    from altegio_bot.settings import settings as _settings

    original_secret = _settings.chatwoot_webhook_secret
    try:
        _settings.chatwoot_webhook_secret = 'super-secret'
        payload = _cw_payload()
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as tc:
            resp = await tc.post(
                '/webhook/chatwoot',
                content=json.dumps(payload),
                headers={
                    'Content-Type': 'application/json',
                    'X-Chatwoot-Signature': 'badhex',
                },
            )
        assert resp.status_code == 403
    finally:
        _settings.chatwoot_webhook_secret = original_secret
