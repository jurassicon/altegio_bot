"""Regression tests for the Altegio webhook endpoint after security hardening.

Two behaviours are pinned here:
  * the shared secret is compared in constant time and a non-ASCII token must
    not raise (a raise would surface as 500 and distinguish "wrong secret" from
    "server error");
  * the stored ``AltegioEvent.query`` is masked, while authentication and the
    dedupe key keep using the original unmasked query.

Historical rows written before this change still contain the plaintext secret;
cleaning them up is a separate operation with a backup, not part of these tests.
"""

from __future__ import annotations

import hashlib
import json
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.main as main_module
from altegio_bot.main import _make_dedupe_key, app
from altegio_bot.models.models import AltegioEvent
from altegio_bot.settings import settings

SECRET = "altegio-webhook-secret"
URL = "/webhooks/altegio"


def _payload() -> dict:
    return {
        "company_id": 1,
        "resource": "record",
        "resource_id": 42,
        "status": "create",
        "data": {"last_change_date": "2026-07-10T14:00:00+0000"},
    }


async def _rows(session_maker) -> list[AltegioEvent]:
    async with session_maker() as session:
        result = await session.execute(select(AltegioEvent).order_by(AltegioEvent.id))
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_valid_secret_is_accepted_and_masked_in_storage(session_maker) -> None:
    payload = _payload()

    with patch.object(settings, "altegio_webhook_secret", SECRET):
        with patch.object(main_module, "SessionLocal", session_maker):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                resp = await tc.post(
                    f"{URL}?secret={SECRET}",
                    content=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    rows = await _rows(session_maker)
    assert len(rows) == 1
    # Only the persisted copy is masked; the payload itself is untouched.
    assert rows[0].query["secret"] == "***"
    assert rows[0].payload == payload
    assert rows[0].company_id == 1
    assert rows[0].resource_id == 42


@pytest.mark.asyncio
async def test_dedupe_still_collapses_identical_deliveries(session_maker) -> None:
    """Masking must not touch the dedupe key: a repeat still yields one row."""
    body = json.dumps(_payload())

    with patch.object(settings, "altegio_webhook_secret", SECRET):
        with patch.object(main_module, "SessionLocal", session_maker):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                for _ in range(2):
                    resp = await tc.post(
                        f"{URL}?secret={SECRET}",
                        content=body,
                        headers={"Content-Type": "application/json"},
                    )
                    assert resp.status_code == 200

    assert len(await _rows(session_maker)) == 1


@pytest.mark.asyncio
async def test_wrong_secret_is_rejected(session_maker) -> None:
    with patch.object(settings, "altegio_webhook_secret", SECRET):
        with patch.object(main_module, "SessionLocal", session_maker):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                resp = await tc.post(
                    f"{URL}?secret=wrong",
                    content=json.dumps(_payload()),
                    headers={"Content-Type": "application/json"},
                )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_missing_secret_is_rejected(session_maker) -> None:
    """A None query param must be a plain 403, not an AttributeError-driven 500."""
    with patch.object(settings, "altegio_webhook_secret", SECRET):
        with patch.object(main_module, "SessionLocal", session_maker):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                resp = await tc.post(
                    URL,
                    content=json.dumps(_payload()),
                    headers={"Content-Type": "application/json"},
                )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_non_ascii_secret_does_not_500(session_maker) -> None:
    """hmac.compare_digest raises TypeError on non-ASCII str — we compare bytes."""
    with patch.object(settings, "altegio_webhook_secret", SECRET):
        with patch.object(main_module, "SessionLocal", session_maker):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                resp = await tc.post(
                    f"{URL}?secret=пароль",
                    content=json.dumps(_payload()),
                    headers={"Content-Type": "application/json"},
                )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


NUL = chr(0)
LONE_SURROGATE = chr(0xD800)


def _old_dedupe_key(payload: dict, query: dict) -> str:
    """The pre-refactor algorithm, reimplemented verbatim.

    `_make_dedupe_key` was switched from a local json.dumps+sha256 to the shared
    `canonical_json_hash`. Proving the new code agrees with itself is not enough
    — it has to agree with the algorithm that produced every key already in the
    database, or historical rows would stop deduplicating.
    """
    company_id = payload.get("company_id")
    resource = payload.get("resource") or payload.get("type")
    resource_id = payload.get("resource_id")
    event_status = payload.get("status")
    last_change = (payload.get("data") or {}).get("last_change_date")
    secret = query.get("secret") or query.get("userGuid")

    main_fields = [company_id, resource, resource_id, event_status]
    if any(x is None for x in main_fields):
        old_canon = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        old_digest = hashlib.sha256(old_canon.encode("utf-8")).hexdigest()
        base = f"fallback:{old_digest}"
    else:
        base = f"{company_id}:{resource}:{resource_id}:{event_status}:{last_change}:{secret}"

    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Historical dedupe compatibility
# ---------------------------------------------------------------------------


def test_structured_branch_matches_the_old_algorithm() -> None:
    payload = _payload()
    query = {"secret": SECRET}
    assert _make_dedupe_key(payload, query) == _old_dedupe_key(payload, query)


def test_fallback_branch_matches_the_old_algorithm() -> None:
    """Drop a required field so the payload-hash fallback branch is taken."""
    payload = _payload()
    del payload["resource_id"]
    query = {"secret": SECRET}

    key = _make_dedupe_key(payload, query)
    assert key == _old_dedupe_key(payload, query)


def test_fallback_key_is_key_order_independent() -> None:
    a = {"company_id": 1, "status": "create", "extra": {"x": 1, "y": 2}}
    b = {"extra": {"y": 2, "x": 1}, "status": "create", "company_id": 1}
    query = {"secret": SECRET}

    assert _make_dedupe_key(a, query) == _make_dedupe_key(b, query)
    assert _make_dedupe_key(a, query) == _old_dedupe_key(a, query)


def test_clean_unicode_keeps_the_old_key() -> None:
    payload = _payload()
    payload["resource"] = "gr\u00fc\u00dfe"
    payload["data"]["note"] = "\u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435"
    query = {"secret": SECRET}

    assert _make_dedupe_key(payload, query) == _old_dedupe_key(payload, query)


def test_lone_surrogate_no_longer_raises() -> None:
    """New behaviour on purpose: the old algorithm raised UnicodeEncodeError.

    A hostile payload used to become an unhandled 500 here, so there is no
    historical key to stay compatible with — only the requirement not to crash.
    """
    payload = _payload()
    del payload["resource_id"]  # force the fallback branch
    payload["data"]["note"] = LONE_SURROGATE
    query = {"secret": SECRET}

    with pytest.raises(UnicodeEncodeError):
        _old_dedupe_key(payload, query)

    key = _make_dedupe_key(payload, query)
    assert isinstance(key, str) and len(key) == 64
    assert key == _make_dedupe_key(payload, query)  # deterministic


# ---------------------------------------------------------------------------
# Hostile content must not break persistence
# ---------------------------------------------------------------------------


async def _post_raw(session_maker, body: bytes):
    with patch.object(settings, "altegio_webhook_secret", SECRET):
        with patch.object(main_module, "SessionLocal", session_maker):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                return await tc.post(
                    f"{URL}?secret={SECRET}",
                    content=body,
                    headers={"Content-Type": "application/json"},
                )


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.asyncio
async def test_non_finite_json_is_persisted_safely(session_maker, literal: str) -> None:
    body = (
        '{"company_id":1,"resource":"record","resource_id":42,"status":"create","data":{"amount":' + literal + "}}"
    ).encode()

    resp = await _post_raw(session_maker, body)

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    # Non-finite became NULL and the row round-trips through JSONB.
    assert rows[0].payload["data"]["amount"] is None
    json.dumps(rows[0].payload, allow_nan=False)


@pytest.mark.asyncio
async def test_nul_and_surrogate_in_scalar_strings(session_maker) -> None:
    payload = _payload()
    payload["resource"] = f"rec{NUL}ord"
    payload["status"] = f"cre{LONE_SURROGATE}ate"

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert NUL not in (row.resource or "")
    (row.event_status or "").encode("utf-8")


@pytest.mark.asyncio
async def test_overlong_scalar_strings_are_truncated(session_maker) -> None:
    payload = _payload()
    payload["resource"] = "r" * 200
    payload["status"] = "s" * 200

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert len(row.resource) == 32
    assert len(row.event_status) == 32


@pytest.mark.asyncio
async def test_non_numeric_company_id_becomes_null(session_maker) -> None:
    payload = _payload()
    payload["company_id"] = "not-an-integer"

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.company_id is None
    # The full payload is still recoverable from JSONB.
    assert row.payload["company_id"] == "not-an-integer"


@pytest.mark.asyncio
async def test_out_of_range_ids_become_null(session_maker) -> None:
    payload = _payload()
    payload["company_id"] = 2**40  # valid BIGINT, too big for INTEGER
    payload["resource_id"] = 2**70  # beyond BIGINT

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.company_id is None
    assert row.resource_id is None


@pytest.mark.asyncio
async def test_valid_payload_scalars_are_preserved(session_maker) -> None:
    payload = _payload()

    resp = await _post_raw(session_maker, json.dumps(payload).encode())

    assert resp.status_code == 200
    row = (await _rows(session_maker))[0]
    assert row.company_id == 1
    assert row.resource == "record"
    assert row.resource_id == 42
    assert row.event_status == "create"
    assert row.dedupe_key == _old_dedupe_key(payload, {"secret": SECRET})
