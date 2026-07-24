"""Chatwoot webhook now reuses the shared safe metadata helpers.

These pin the hardening without touching Chatwoot's HMAC scheme, response codes,
dedupe or payload handling: only the *stored* copy of query/headers is sanitised,
and the signature header is dropped from storage (it is verified live, upstream).
"""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from starlette.requests import Request

import altegio_bot.webhooks.chatwoot as cw_module
from altegio_bot.main import app
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import safe_headers

URL = "/webhook/chatwoot"


def _incoming(conversation_id: int = 501, message_id: int = 5001) -> dict:
    return {
        "event": "message_created",
        "id": message_id,
        "content": "hello",
        "message_type": 0,
        "created_at": 1234567890,
        "conversation": {"id": conversation_id},
        "sender": {"phone_number": "+4915112345678"},
        "account": {"id": 2},
    }


async def _rows(session_maker) -> list[WhatsAppEvent]:
    async with session_maker() as session:
        result = await session.execute(select(WhatsAppEvent).order_by(WhatsAppEvent.id))
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_valid_webhook_still_stored(session_maker) -> None:
    original = cw_module.SessionLocal
    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                URL,
                content=json.dumps(_incoming()),
                headers={"Content-Type": "application/json"},
            )
    finally:
        cw_module.SessionLocal = original

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    rows = await _rows(session_maker)
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_query_secret_is_masked_and_sensitive_headers_dropped(session_maker) -> None:
    original = cw_module.SessionLocal
    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?token=CW-QUERY-SECRET&foo=bar",
                content=json.dumps(_incoming()),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer nope",
                    "Cookie": "session=nope",
                    "X-Chatwoot-Signature": "deadbeef",
                },
            )
    finally:
        cw_module.SessionLocal = original

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.query["token"] == "***"
    assert row.query["foo"] == "bar"
    stored = {k.lower() for k in row.headers}
    assert "authorization" not in stored
    assert "cookie" not in stored
    assert "x-chatwoot-signature" not in stored


@pytest.mark.asyncio
async def test_nul_in_query_does_not_break_commit(session_maker) -> None:
    original = cw_module.SessionLocal
    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?ref=a%00b",
                content=json.dumps(_incoming()),
                headers={"Content-Type": "application/json"},
            )
    finally:
        cw_module.SessionLocal = original

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert "\x00" not in json.dumps(row.query)


def test_safe_headers_strips_nul_from_header_values() -> None:
    """A NUL in a header value must not reach Postgres.

    uvicorn's parser rejects NUL on the wire, so this can only be exercised by
    building the request directly — which is exactly the app-layer guard the
    Chatwoot route relies on via the shared helper.
    """
    scope = {
        "type": "http",
        "method": "POST",
        "path": URL,
        "headers": [
            (b"x-custom", b"a\x00b"),
            (b"authorization", b"secret"),
        ],
        "query_string": b"",
    }
    request = Request(scope)
    headers = safe_headers(request, extra_deny={cw_module._CHATWOOT_SIGNATURE_HEADER})
    assert "authorization" not in headers
    assert "\x00" not in headers["x-custom"]


@pytest.mark.asyncio
async def test_invalid_hmac_signature_still_403(session_maker, monkeypatch) -> None:
    """HMAC verification is unchanged: a bad signature is still rejected."""
    monkeypatch.setattr(settings, "chatwoot_webhook_secret", "hmac-secret")
    original = cw_module.SessionLocal
    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                URL,
                content=json.dumps(_incoming()),
                headers={"Content-Type": "application/json", "X-Chatwoot-Signature": "wrong"},
            )
    finally:
        cw_module.SessionLocal = original

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_valid_hmac_signature_is_accepted(session_maker, monkeypatch) -> None:
    """A correct signature over the raw body still authenticates as before."""
    secret = "hmac-secret"
    monkeypatch.setattr(settings, "chatwoot_webhook_secret", secret)
    body = json.dumps(_incoming()).encode()
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    original = cw_module.SessionLocal
    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                URL,
                content=body,
                headers={"Content-Type": "application/json", "X-Chatwoot-Signature": signature},
            )
    finally:
        cw_module.SessionLocal = original

    assert resp.status_code == 200
    assert len(await _rows(session_maker)) == 1
