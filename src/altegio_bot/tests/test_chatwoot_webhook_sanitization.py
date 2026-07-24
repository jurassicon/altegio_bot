"""Chatwoot webhook now reuses the shared safe metadata helpers.

These pin the hardening without touching Chatwoot's HMAC scheme, response codes,
dedupe or payload handling: only the *stored* copy of query/headers is sanitised,
and the signature header is dropped from storage (it is verified live, upstream).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging

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
NUL = chr(0)
LONE_SURROGATE = chr(0xD800)
LINE_SEP = chr(0x2028)
PARA_SEP = chr(0x2029)


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


def _operator_payload(*, agent_name: str = "Agent", contact_name: str = "Customer", text: str = "reply") -> dict:
    return {
        "event": "message_created",
        "id": 7001,
        "content": text,
        "message_type": 1,
        "private": False,
        "content_type": "text",
        "conversation": {
            "id": 701,
            "inbox_id": 123,
            "meta": {"sender": {"phone_number": "+4915112345678", "name": contact_name}},
        },
        "sender": {"type": "agent", "name": agent_name, "id": 9},
        "account": {"id": 2},
    }


async def _post_raw(session_maker, body: bytes, headers: dict | None = None):
    original = cw_module.SessionLocal
    try:
        cw_module.SessionLocal = session_maker  # type: ignore[assignment]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            return await tc.post(URL, content=body, headers=headers or {"Content-Type": "application/json"})
    finally:
        cw_module.SessionLocal = original


# ---------------------------------------------------------------------------
# Persisted payload must survive PostgreSQL JSONB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_customer_content_with_nul_is_stored(session_maker) -> None:
    """A NUL inside message content passes json.loads but JSONB rejects it."""
    payload = _incoming()
    payload["content"] = f"a{NUL}b"
    body = json.dumps(payload).encode()
    assert b"\\u0000" in body

    resp = await _post_raw(session_maker, body)

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    stored = json.dumps(rows[0].payload)
    assert NUL not in stored
    # Sanitised, not dropped: the surrounding text is still there.
    text_body = rows[0].payload["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"]
    assert text_body.startswith("a")
    assert text_body.endswith("b")


@pytest.mark.asyncio
async def test_customer_content_with_nul_still_passes_hmac(session_maker, monkeypatch) -> None:
    """HMAC is computed over the original body; sanitization is storage-only."""
    monkeypatch.setattr(settings, "chatwoot_webhook_secret", "hmac-secret")
    payload = _incoming()
    payload["content"] = f"x{NUL}y"
    body = json.dumps(payload).encode()
    signature = hmac.new(b"hmac-secret", body, hashlib.sha256).hexdigest()

    resp = await _post_raw(
        session_maker,
        body,
        headers={"Content-Type": "application/json", "X-Chatwoot-Signature": signature},
    )

    assert resp.status_code == 200
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_agent_and_contact_names_with_surrogate_are_stored(session_maker, monkeypatch) -> None:
    """Lone surrogates in operator metadata must not break the commit."""
    monkeypatch.setattr(settings, "chatwoot_operator_relay_enabled", True)
    payload = _operator_payload(
        agent_name=f"Anna{LONE_SURROGATE}",
        contact_name=f"Bob{LONE_SURROGATE}",
    )
    body = json.dumps(payload).encode()

    resp = await _post_raw(session_maker, body)

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    # Round-trips through JSONB without raising.
    json.dumps(rows[0].payload).encode("utf-8")


@pytest.mark.asyncio
async def test_clean_payload_is_stored_unchanged(session_maker) -> None:
    payload = _incoming()
    payload["content"] = "ganz normaler Text"
    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    text_body = row.payload["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"]
    assert text_body == "ganz normaler Text"


# ---------------------------------------------------------------------------
# Application log hygiene
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile_event",
    [
        "message_created\n2026-07-24 INFO forged",
        "message_created\rforged",
        "message_created\x1b[31mred",
        "message_created" + LINE_SEP + "forged",
        "message_created" + PARA_SEP + "forged",
        "m" * 5000,
    ],
)
@pytest.mark.asyncio
async def test_hostile_event_name_cannot_inject_a_log_line(session_maker, caplog, hostile_event) -> None:
    payload = _incoming()
    payload["event"] = hostile_event

    with caplog.at_level(logging.INFO, logger="chatwoot_webhook"):
        resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    records = [r for r in caplog.records if r.name == "chatwoot_webhook"]
    assert len(records) == 1
    message = records[0].getMessage()
    for ch in ("\n", "\r", "\x1b", LINE_SEP, PARA_SEP):
        assert ch not in message
    # Bounded: a 5000-char event name cannot flood the log.
    assert len(message) < 300


@pytest.mark.asyncio
async def test_phone_and_message_text_never_reach_the_logs(session_maker, caplog) -> None:
    payload = _incoming()
    payload["content"] = "STRENG GEHEIMER KUNDENTEXT"
    payload["sender"]["phone_number"] = "+4915199999999"

    with caplog.at_level(logging.DEBUG):
        resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "STRENG GEHEIMER KUNDENTEXT" not in logged
    assert "+4915199999999" not in logged
    # Technical metadata is still there for diagnostics.
    assert "conv_id" in logged


@pytest.mark.asyncio
async def test_dedupe_semantics_unchanged(session_maker, caplog) -> None:
    body = json.dumps(_incoming()).encode()

    first = await _post_raw(session_maker, body)
    second = await _post_raw(session_maker, body)

    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True
    assert len(await _rows(session_maker)) == 1


# ---------------------------------------------------------------------------
# Non-finite JSON and malformed scalar ids
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.asyncio
async def test_non_finite_json_is_persisted_safely(session_maker, literal: str) -> None:
    body = (
        '{"event":"message_created","id":5001,"content":"hi","message_type":0,'
        '"created_at":1234567890,"conversation":{"id":501},'
        '"sender":{"phone_number":"+4915112345678"},"account":{"id":2},'
        '"score":' + literal + "}"
    ).encode()

    resp = await _post_raw(session_maker, body)

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    json.dumps(rows[0].payload, allow_nan=False)


@pytest.mark.asyncio
async def test_non_finite_json_still_passes_hmac(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "chatwoot_webhook_secret", "hmac-secret")
    body = (
        '{"event":"message_created","id":5001,"content":"hi","message_type":0,'
        '"created_at":1234567890,"conversation":{"id":501},'
        '"sender":{"phone_number":"+4915112345678"},"account":{"id":2},'
        '"score":NaN}'
    ).encode()
    signature = hmac.new(b"hmac-secret", body, hashlib.sha256).hexdigest()

    resp = await _post_raw(
        session_maker,
        body,
        headers={"Content-Type": "application/json", "X-Chatwoot-Signature": signature},
    )

    assert resp.status_code == 200
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_nul_in_message_id_does_not_break_insert(session_maker) -> None:
    payload = _incoming()
    payload["id"] = f"x{NUL}y"

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    # Non-numeric id → NULL BIGINT projection, key still safe and bounded.
    assert row.chatwoot_message_id is None
    assert NUL not in row.dedupe_key
    assert len(row.dedupe_key) <= 128


@pytest.mark.asyncio
async def test_surrogate_in_message_id_does_not_break_insert(session_maker) -> None:
    payload = _incoming()
    payload["id"] = f"x{LONE_SURROGATE}y"

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    row.dedupe_key.encode("utf-8")
    assert row.chatwoot_message_id is None


@pytest.mark.asyncio
async def test_very_long_message_id_is_bounded(session_maker) -> None:
    payload = _incoming()
    payload["id"] = "9" * 500

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert len(row.dedupe_key) <= 128


@pytest.mark.asyncio
async def test_non_numeric_conversation_id_becomes_null(session_maker) -> None:
    payload = _incoming()
    payload["conversation"]["id"] = "not-an-integer"

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.chatwoot_conversation_id is None


@pytest.mark.asyncio
async def test_out_of_range_ids_become_null(session_maker) -> None:
    payload = _incoming()
    payload["conversation"]["id"] = 2**70
    payload["id"] = 2**70

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.chatwoot_conversation_id is None
    assert row.chatwoot_message_id is None


@pytest.mark.asyncio
async def test_valid_ids_keep_the_historical_dedupe_key(session_maker) -> None:
    payload = _incoming(conversation_id=99, message_id=1)

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.dedupe_key == "chatwoot:99:1"
    assert row.chatwoot_conversation_id == 99
    assert row.chatwoot_message_id == 1


# ---------------------------------------------------------------------------
# Responses must never reflect sender-controlled content
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile_event",
    [LONE_SURROGATE, f"a{NUL}b", "e" * 5000, "some_other_event"],
)
@pytest.mark.asyncio
async def test_unsupported_event_returns_a_stable_reason(session_maker, hostile_event: str) -> None:
    payload = _incoming()
    payload["event"] = hostile_event

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    data = resp.json()
    assert data == {"ok": True, "skipped": "unsupported_event"}
    assert hostile_event[:20] not in resp.text


@pytest.mark.parametrize("hostile_type", [LONE_SURROGATE, f"a{NUL}b", "t" * 5000, 99])
@pytest.mark.asyncio
async def test_unsupported_message_type_returns_a_stable_reason(session_maker, hostile_type) -> None:
    payload = _incoming()
    payload["message_type"] = hostile_type

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "skipped": "unsupported_message_type"}


@pytest.mark.asyncio
async def test_unsupported_content_type_returns_a_stable_reason(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "chatwoot_operator_relay_enabled", True)
    payload = _operator_payload()
    payload["content_type"] = "activity"

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "skipped": "unsupported_content_type"}
