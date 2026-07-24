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
LONE_SURROGATE = chr(0xD800)


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


# ---------------------------------------------------------------------------
# Non-finite JSON
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.asyncio
async def test_non_finite_json_is_persisted_safely(session_maker, literal: str) -> None:
    body = (
        '{"object":"whatsapp_business_account","score":' + literal + ","
        '"entry":[{"id":"WABA1","changes":[{"field":"messages","value":'
        '{"metadata":{"phone_number_id":"PNI1"},"messages":[]}}]}]}'
    ).encode()

    resp = await _post(session_maker, body=body, headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].payload["score"] is None
    json.dumps(rows[0].payload, allow_nan=False)


@pytest.mark.asyncio
async def test_non_finite_json_still_passes_signature(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
    body = (
        '{"object":"whatsapp_business_account","score":NaN,'
        '"entry":[{"id":"WABA1","changes":[{"field":"messages","value":'
        '{"metadata":{"phone_number_id":"PNI1"},"messages":[]}}]}]}'
    ).encode()

    resp = await _post(
        session_maker,
        body=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": _sign(body)},
    )

    assert resp.status_code == 200
    assert len(await _rows(session_maker)) == 1


# ---------------------------------------------------------------------------
# Responses must serialize regardless of body content
# ---------------------------------------------------------------------------


def _payload_with_pni(pni: str) -> dict:
    payload = _meta_payload()
    payload["entry"][0]["changes"][0]["value"]["metadata"]["phone_number_id"] = pni
    return payload


@pytest.mark.parametrize(
    "hostile_pni",
    [LONE_SURROGATE, f"a{NUL}b", "9" * 5000, "PNI-normal"],
)
@pytest.mark.asyncio
async def test_response_serializes_with_hostile_phone_number_id(session_maker, hostile_pni: str) -> None:
    """The raw phone_number_id must never be reflected back into the response.

    Reflecting it meant an un-encodable value produced a 500 AFTER the row was
    committed, and every retry hit the same failure on the duplicate branch.
    """
    body = json.dumps(_payload_with_pni(hostile_pni)).encode()

    resp = await _post(session_maker, body=body, headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    data = resp.json()
    assert "phone_number_id" not in data
    assert data["ok"] is True
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_duplicate_response_serializes_with_hostile_phone_number_id(session_maker) -> None:
    body = json.dumps(_payload_with_pni(LONE_SURROGATE)).encode()
    headers = {"Content-Type": "application/json"}

    first = await _post(session_maker, body=body, headers=headers)
    second = await _post(session_maker, body=body, headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert "phone_number_id" not in second.json()
    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_ignored_response_serializes_with_hostile_phone_number_id(session_maker, monkeypatch) -> None:
    """The `ignored` branch also writes pni into the error column."""
    monkeypatch.setattr(settings, "whatsapp_allowed_phone_number_ids", "ALLOWED_ONLY")
    body = json.dumps(_payload_with_pni(f"bad{LONE_SURROGATE}")).encode()

    resp = await _post(session_maker, body=body, headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    assert resp.json()["ignored"] is True
    row = (await _rows(session_maker))[0]
    assert row.status == "ignored"
    # The error column must be encodable too.
    (row.error or "").encode("utf-8")


# ---------------------------------------------------------------------------
# ensure_ascii=True is load-bearing for the dedupe key
# ---------------------------------------------------------------------------


def test_dedupe_key_survives_a_lone_surrogate() -> None:
    payload = _meta_payload()
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"] = LONE_SURROGATE

    key = wa_module._payload_dedupe_key(payload)

    assert key.startswith("wa:")
    # Deterministic across calls.
    assert key == wa_module._payload_dedupe_key(payload)


def test_dedupe_key_uses_ensure_ascii_true() -> None:
    """ensure_ascii=False would raise UnicodeEncodeError on this payload."""
    payload = _meta_payload()
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"] = LONE_SURROGATE

    with pytest.raises(UnicodeEncodeError):
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

    wa_module._payload_dedupe_key(payload)  # does not raise


@pytest.mark.asyncio
async def test_lone_surrogate_delivery_dedupes_and_stores_safely(session_maker) -> None:
    payload = _meta_payload()
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"] = LONE_SURROGATE
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}

    first = await _post(session_maker, body=body, headers=headers)
    second = await _post(session_maker, body=body, headers=headers)

    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True
    rows = await _rows(session_maker)
    assert len(rows) == 1
    json.dumps(rows[0].payload).encode("utf-8")


# ---------------------------------------------------------------------------
# Structural validation of the JSON root
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", [b"[]", b'"text"', b"123", b"null", b"[1,2]"])
@pytest.mark.asyncio
async def test_non_object_json_root_is_rejected(session_maker, raw: bytes) -> None:
    """Never ack a payload no worker can process.

    Storing it and answering 200 would tell Meta the delivery succeeded, so it
    would never retry — while the row silently ends up unprocessed.
    """
    resp = await _post(session_maker, body=raw, headers={"Content-Type": "application/json"})

    assert resp.status_code == 400
    assert resp.json()["detail"] == "JSON payload must be an object"
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_bad_signature_wins_over_structural_validation(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)

    resp = await _post(
        session_maker,
        body=b"[]",
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": "sha256=deadbeef"},
    )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_valid_signature_over_non_object_root_is_400(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(settings, "meta_app_secret", APP_SECRET)
    body = b"[]"

    resp = await _post(
        session_maker,
        body=body,
        headers={"Content-Type": "application/json", "X-Hub-Signature-256": _sign(body)},
    )

    assert resp.status_code == 400
    assert await _rows(session_maker) == []


@pytest.mark.parametrize("bad", ["[]", '"bad"', "123", "null"])
@pytest.mark.asyncio
async def test_malformed_nested_meta_structure_does_not_500(session_maker, bad: str) -> None:
    """A valid object root with a malformed `entry` must still be handled."""
    body = ('{"object":"whatsapp_business_account","entry":' + bad + "}").encode()

    resp = await _post(session_maker, body=body, headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    assert len(await _rows(session_maker)) == 1
