"""WhatsApp (Meta) webhook: shared metadata sanitization + unchanged auth/dedupe.

Before this hardening the endpoint stored ``dict(request.query_params)`` and
``dict(request.headers)`` verbatim, so secrets and auth headers landed in the DB
and a NUL in metadata could break the commit. These tests pin the new behaviour
AND the parts that must not move: Meta HMAC is still computed over the raw body,
the verify handshake is untouched, and dedupe still collapses repeats.
"""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.webhooks.whatsapp as wa_module
from altegio_bot.main import app
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings

URL = "/webhook/whatsapp"
APP_SECRET = "meta-app-secret"
NUL = chr(0)


def _meta_payload(text: str = "hello", msg_id: str = "wamid.TEST1") -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": "PNI1"},
                            "messages": [
                                {
                                    "from": "4915112345678",
                                    "id": msg_id,
                                    "type": "text",
                                    "text": {"body": text},
                                    "timestamp": "1700000000",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _sign(body: bytes, secret: str = APP_SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


async def _rows(session_maker) -> list[WhatsAppEvent]:
    async with session_maker() as session:
        result = await session.execute(select(WhatsAppEvent).order_by(WhatsAppEvent.id))
        return list(result.scalars().all())


async def _post(session_maker, *, url: str = URL, body: bytes, headers: dict) -> object:
    original = wa_module.SessionLocal
    try:
        wa_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            return await tc.post(url, content=body, headers=headers)
    finally:
        wa_module.SessionLocal = original


# ---------------------------------------------------------------------------
# Signature semantics — must be unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_webhook_is_stored(session_maker) -> None:
    body = json.dumps(_meta_payload()).encode()
    resp = await _post(session_maker, body=body, headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_valid_signature_is_accepted(session_maker, monkeypatch) -> None:
    """HMAC is still computed over the raw body, before any sanitization."""
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
    body = json.dumps(_meta_payload()).encode()

    resp = await _post(
        session_maker,
        body=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": _sign(body)},
    )

    assert resp.status_code == 200
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_invalid_signature_is_rejected(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
    body = json.dumps(_meta_payload()).encode()

    resp = await _post(
        session_maker,
        body=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": "sha256=deadbeef"},
    )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_signature_over_payload_with_nul_still_verifies(session_maker, monkeypatch) -> None:
    """Sanitization happens on the stored copy only — it must not break HMAC."""
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
    body = json.dumps(_meta_payload(text=f"a{NUL}b")).encode()
    # The NUL travels escaped on the wire and survives json.loads, so the
    # signature is computed over bytes that still contain it.
    assert b"\\u0000" in body

    resp = await _post(
        session_maker,
        body=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": _sign(body)},
    )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert NUL not in json.dumps(rows[0].payload)


# ---------------------------------------------------------------------------
# Stored metadata sanitization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sensitive_headers_are_not_stored(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
    body = json.dumps(_meta_payload()).encode()

    resp = await _post(
        session_maker,
        body=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer must-not-be-stored",
            "Cookie": "session=must-not-be-stored",
            "X-Hub-Signature-256": _sign(body),
        },
    )

    assert resp.status_code == 200
    stored = {k.lower() for k in (await _rows(session_maker))[0].headers}
    assert "authorization" not in stored
    assert "cookie" not in stored
    assert "x-hub-signature-256" not in stored
    # A harmless header is still kept for diagnostics.
    assert "content-type" in stored


@pytest.mark.asyncio
async def test_query_secrets_masked_and_plain_values_kept(session_maker) -> None:
    body = json.dumps(_meta_payload()).encode()

    resp = await _post(
        session_maker,
        url=f"{URL}?token=WA-QUERY-SECRET&secret=ANOTHER&source=meta",
        body=body,
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 200
    query = (await _rows(session_maker))[0].query
    assert query["token"] == "***"
    assert query["secret"] == "***"
    assert query["source"] == "meta"


@pytest.mark.asyncio
async def test_nul_in_query_does_not_break_commit(session_maker) -> None:
    body = json.dumps(_meta_payload()).encode()

    resp = await _post(
        session_maker,
        url=f"{URL}?ref=a%00b",
        body=body,
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert "\x00" not in json.dumps(rows[0].query)


# ---------------------------------------------------------------------------
# Dedupe semantics — must be unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_delivery_is_collapsed(session_maker) -> None:
    body = json.dumps(_meta_payload()).encode()
    headers = {"Content-Type": "application/json"}

    first = await _post(session_maker, body=body, headers=headers)
    second = await _post(session_maker, body=body, headers=headers)

    assert first.status_code == 200
    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_dedupe_key_is_computed_from_the_original_payload(session_maker) -> None:
    """Sanitizing the stored copy must not shift the dedupe key."""
    payload = _meta_payload()
    body = json.dumps(payload).encode()
    expected = wa_module._payload_dedupe_key(payload)

    resp = await _post(session_maker, body=body, headers={"Content-Type": "application/json"})

    assert resp.json()["dedupe_key"] == expected
    assert (await _rows(session_maker))[0].dedupe_key == expected


# ---------------------------------------------------------------------------
# GET verify handshake — must be unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verify_returns_challenge(monkeypatch) -> None:
    monkeypatch.setattr(settings, "whatsapp_webhook_verify_token", "verify-me")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
        resp = await tc.get(f"{URL}?hub.mode=subscribe&hub.verify_token=verify-me&hub.challenge=12345")

    assert resp.status_code == 200
    assert resp.text == "12345"


@pytest.mark.asyncio
async def test_verify_rejects_wrong_token(monkeypatch) -> None:
    monkeypatch.setattr(settings, "whatsapp_webhook_verify_token", "verify-me")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
        resp = await tc.get(f"{URL}?hub.mode=subscribe&hub.verify_token=wrong&hub.challenge=12345")

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_verify_rejects_bad_mode() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
        resp = await tc.get(f"{URL}?hub.mode=unsubscribe&hub.challenge=12345")

    assert resp.status_code == 400
