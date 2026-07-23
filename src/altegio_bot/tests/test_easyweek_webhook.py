"""Tests for the research-grade EasyWeek webhook capture endpoint.

Pattern: httpx ASGITransport + the conftest ``session_maker`` fixture, as in
test_chatwoot_webhook.py. Settings are overridden with ``patch.object`` on the
shared singleton (the repo-wide idiom); ``SessionLocal`` is rebound on the
easyweek module so writes hit the test database.

The payload factory is shaped "as if EasyWeek", but no test may depend on that
structure beyond "the payload is stored verbatim" — the real schema is unknown,
which is the whole point of this capture layer.
"""

from __future__ import annotations

import json
from contextlib import ExitStack
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.webhooks.easyweek as ew_module
from altegio_bot.main import app
from altegio_bot.models.models import EasyWeekEvent
from altegio_bot.settings import settings

SECRET = "capture-secret-value"
URL = "/webhooks/easyweek"


def _payload() -> dict:
    return {
        "id": 1811630,
        "uid": "ac1500000000deadbeef",
        "customer_phone": "+4915000000000",
        "booking_date_start": "2026-07-10T14:00:00+0000",
    }


def _capture_env(session_maker, *, enabled: bool = True, secret: str = SECRET) -> ExitStack:
    """Enable capture, point the router at the test DB. Use as a context manager."""
    stack = ExitStack()
    stack.enter_context(patch.object(settings, "easyweek_enabled", enabled))
    stack.enter_context(patch.object(settings, "easyweek_webhook_secret", secret))
    stack.enter_context(patch.object(ew_module, "SessionLocal", session_maker))
    return stack


async def _rows(session_maker) -> list[EasyWeekEvent]:
    async with session_maker() as session:
        result = await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_happy_path_captures_row(session_maker) -> None:
    """Valid query token → 200 and exactly one fully-populated row."""
    payload = _payload()

    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=json.dumps(payload),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer must-not-be-stored",
                    "Cookie": "session=must-not-be-stored",
                    "X-Altegio-Token": SECRET,
                },
            )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    rows = await _rows(session_maker)
    assert len(rows) == 1
    row = rows[0]
    assert row.event_hint == "booking-created"
    assert row.auth_via == "query"
    assert row.status == "captured"
    assert row.payload == payload
    assert row.payload_hash is not None
    assert len(row.payload_hash) == 64
    assert row.body_text is None
    assert row.body_truncated is False
    assert row.content_type == "application/json"

    # Secrets masked in query, sensitive headers dropped entirely.
    assert row.query["token"] == "***"
    assert row.query["event"] == "booking-created"
    stored_headers = {k.lower() for k in row.headers}
    assert "authorization" not in stored_headers
    assert "cookie" not in stored_headers
    assert "x-altegio-token" not in stored_headers


@pytest.mark.asyncio
async def test_duplicate_delivery_creates_second_row(session_maker) -> None:
    """Retry/Resend must produce a SECOND row — dedupe is intentionally absent."""
    body = json.dumps(_payload())

    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            for _ in range(2):
                resp = await tc.post(
                    f"{URL}?event=booking-created&token={SECRET}",
                    content=body,
                    headers={"Content-Type": "application/json"},
                )
                assert resp.status_code == 200

    rows = await _rows(session_maker)
    assert len(rows) == 2
    assert rows[0].payload_hash == rows[1].payload_hash
    assert rows[0].id != rows[1].id


@pytest.mark.asyncio
async def test_wrong_query_token_is_forbidden(session_maker) -> None:
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token=wrong",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_missing_token_is_forbidden(session_maker) -> None:
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_header_token_is_accepted_when_query_token_absent(session_maker) -> None:
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-updated",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json", "X-Altegio-Token": SECRET},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].auth_via == "header"
    assert "x-altegio-token" not in {k.lower() for k in rows[0].headers}


@pytest.mark.asyncio
async def test_wrong_query_token_does_not_fall_back_to_header(session_maker) -> None:
    """A present-but-wrong query token must NOT be rescued by a valid header.

    Falling back would silently mask a rotated/mistyped secret in the webhook URL.
    """
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token=wrong",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json", "X-Altegio-Token": SECRET},
            )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_empty_secret_rejects_empty_token(session_maker) -> None:
    """Fail-closed: an empty configured secret closes the endpoint unconditionally."""
    with _capture_env(session_maker, secret=""):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token=",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 403
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_disabled_flag_returns_404(session_maker) -> None:
    """The surface stays hidden before go-live: 404, not 403."""
    with _capture_env(session_maker, enabled=False):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 404
    assert await _rows(session_maker) == []


@pytest.mark.asyncio
async def test_non_json_body_is_captured_as_text(session_maker) -> None:
    """Garbage body still gets a 200 — a 4xx would count as a failed delivery."""
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=b"not json at all <<<",
                headers={"Content-Type": "text/plain"},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].payload == {}
    assert rows[0].payload_hash is None
    assert rows[0].body_text == "not json at all <<<"
    assert rows[0].body_truncated is False


@pytest.mark.asyncio
async def test_oversized_body_is_truncated_and_not_parsed(session_maker) -> None:
    """>128KB: recorded, truncated, never parsed (CPU/RAM bound)."""
    limit = ew_module._MAX_BODY_BYTES
    body = b"a" * (limit + 1000)

    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=body,
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].body_truncated is True
    assert rows[0].payload_hash is None
    assert rows[0].payload == {}
    # ASCII filler: one byte decodes to one character, so the decoded length
    # equals the byte limit exactly.
    assert len(rows[0].body_text) == limit


@pytest.mark.asyncio
async def test_non_dict_json_root_is_wrapped(session_maker) -> None:
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=b"[1, 2]",
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].payload == {"_non_dict_payload": [1, 2]}
    assert rows[0].payload_hash is not None
    assert rows[0].body_text is None


@pytest.mark.asyncio
async def test_nan_json_falls_back_to_body_text(session_maker) -> None:
    """NaN parses in Python but Postgres JSONB rejects it — store it as text.

    Persisting the parsed value would 500 on commit and lose the delivery on
    every retry.
    """
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=b'{"amount": NaN}',
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].payload == {}
    assert rows[0].payload_hash is None
    assert rows[0].body_text == '{"amount": NaN}'


@pytest.mark.asyncio
async def test_event_hint_is_stored_verbatim_and_truncated(session_maker) -> None:
    """Capture does not validate trigger names — that is the PR-4 normalizer's job."""
    long_event = "x" * 50

    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=totally-unknown-trigger&token={SECRET}",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200

            resp = await tc.post(
                f"{URL}?event={long_event}&token={SECRET}",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200

    rows = await _rows(session_maker)
    assert len(rows) == 2
    assert rows[0].event_hint == "totally-unknown-trigger"
    assert rows[1].event_hint == "x" * 32
