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
import logging
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

import altegio_bot.webhooks.easyweek as ew_module
from altegio_bot.main import app
from altegio_bot.models.models import EasyWeekEvent
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import read_bounded_body

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
    # Raw bytes are kept even for a perfectly parseable JSON body: JSONB is only
    # a projection of them.
    assert row.body_raw == json.dumps(payload).encode()
    assert row.body_size_bytes == len(json.dumps(payload).encode())

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
    assert rows[0].body_raw == b"not json at all <<<"
    assert rows[0].body_size_bytes == len(b"not json at all <<<")


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
    # Only the bounded prefix is stored, but the true size is recorded.
    assert len(rows[0].body_raw) == limit
    assert rows[0].body_size_bytes == len(body)


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
    assert rows[0].body_raw == b'{"amount": NaN}'


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


# ---------------------------------------------------------------------------
# Raw-byte capture: JSONB is a projection, body_raw is the source of truth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_bytes_preserve_formatting_and_key_order(session_maker) -> None:
    """Whitespace and key order survive in body_raw even though JSONB loses them."""
    body = b'{\n  "z": 1,\n\t"a":   2.50\n}'

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
    assert rows[0].body_raw == body
    assert rows[0].payload == {"z": 1, "a": 2.5}
    assert rows[0].payload_hash is not None
    assert rows[0].body_size_bytes == len(body)


@pytest.mark.asyncio
async def test_duplicate_json_keys_survive_in_raw_bytes(session_maker) -> None:
    """A repeated JSON key is lost by any parser — that is why raw bytes are kept.

    The test deliberately makes no claim about which value JSONB ends up with;
    the recoverable record is body_raw.
    """
    body = b'{"a":1,"a":2}'

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
    assert rows[0].body_raw == body
    assert rows[0].body_size_bytes == len(body)
    assert set(rows[0].payload) == {"a"}


@pytest.mark.asyncio
async def test_invalid_utf8_body_is_captured(session_maker) -> None:
    """Invalid UTF-8 must not 500: raw bytes verbatim, text projection repaired."""
    body = b'{"name": "\xff\xfe broken"}'

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
    assert rows[0].body_raw == body
    assert rows[0].payload == {}
    assert rows[0].payload_hash is None
    assert "�" in rows[0].body_text


# ---------------------------------------------------------------------------
# PostgreSQL-hostile content (NUL) — regression tests against real Postgres
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_json_with_nul_string_is_captured_as_text(session_maker) -> None:
    """JSON carrying an escaped NUL parses in Python but JSONB rejects it.

    Storing the parsed value would fail at commit and lose the delivery on every
    retry, so it falls back to the text projection.
    """
    body = b'{"comment":"a\\u0000b"}'

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
    assert rows[0].body_raw == body
    assert rows[0].payload == {}
    assert rows[0].payload_hash is None
    assert rows[0].body_text is not None
    assert "\x00" not in rows[0].body_text


@pytest.mark.asyncio
async def test_raw_nul_byte_body_is_captured(session_maker) -> None:
    """A raw 0x00 byte is valid UTF-8, so errors="replace" does not remove it."""
    body = b"abc\x00def"

    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=booking-created&token={SECRET}",
                content=body,
                headers={"Content-Type": "text/plain"},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert rows[0].body_raw == b"abc\x00def"
    assert "\x00" not in rows[0].body_text


@pytest.mark.asyncio
async def test_nul_in_query_value_does_not_break_commit(session_maker) -> None:
    """NUL in ?event= must not turn an authenticated delivery into a 500."""
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
            resp = await tc.post(
                f"{URL}?event=book%00ing&token={SECRET}",
                content=json.dumps(_payload()),
                headers={"Content-Type": "application/json"},
            )

    assert resp.status_code == 200
    rows = await _rows(session_maker)
    assert len(rows) == 1
    assert "\x00" not in rows[0].event_hint
    assert "\x00" not in json.dumps(rows[0].query)


@pytest.mark.asyncio
async def test_lone_surrogate_json_falls_back_to_text(session_maker) -> None:
    """A lone surrogate parses in Python but cannot be encoded to UTF-8 for Postgres."""
    body = b'{"name":"\\ud800"}'

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
    assert rows[0].payload == {}
    assert rows[0].payload_hash is None
    assert rows[0].body_raw == body


# ---------------------------------------------------------------------------
# Persistence failures: 503, never a 200 over an unwritten row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persistence_failure_returns_503(session_maker, caplog) -> None:
    """A DB failure must not be acked with 200 — there is no durable spool."""

    class _FailingSession:
        """Wraps a real session but fails the commit like a dead connection would."""

        def __init__(self, inner) -> None:
            self._inner = inner

        async def __aenter__(self):
            await self._inner.__aenter__()
            return self

        async def __aexit__(self, *exc_info):
            return await self._inner.__aexit__(*exc_info)

        def add(self, obj) -> None:
            pass

        async def commit(self) -> None:
            # The driver puts bound parameters (i.e. payload/PII) into the
            # exception — the handler must never log its text.
            raise OperationalError(
                "INSERT INTO easyweek_events ...",
                {"phone": "+4915000000000"},
                Exception("connection closed"),
            )

        async def rollback(self) -> None:
            pass

    def _failing_maker():
        return _FailingSession(session_maker())

    with _capture_env(session_maker):
        with patch.object(ew_module, "SessionLocal", _failing_maker):
            with caplog.at_level(logging.INFO):
                async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                    resp = await tc.post(
                        f"{URL}?event=booking-created&token={SECRET}",
                        content=json.dumps(_payload()),
                        headers={"Content-Type": "application/json"},
                    )

    assert resp.status_code == 503
    # No internal error text leaks to the caller.
    assert "OperationalError" not in resp.text
    assert "INSERT" not in resp.text

    assert await _rows(session_maker) == []

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "error_type=OperationalError" in logged
    assert "INSERT INTO" not in logged
    assert "+4915000000000" not in logged
    assert SECRET not in logged


# ---------------------------------------------------------------------------
# Logging hygiene
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logs_contain_no_secret_query_or_payload(session_maker, caplog) -> None:
    payload = _payload()

    with _capture_env(session_maker):
        with caplog.at_level(logging.INFO):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as tc:
                resp = await tc.post(
                    f"{URL}?event=booking-created&token={SECRET}",
                    content=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                )

    assert resp.status_code == 200
    logged = "\n".join(record.getMessage() for record in caplog.records)

    # Nothing sender-controlled reaches the logs.
    assert SECRET not in logged
    assert "booking-created" not in logged  # the ?event= value is not validated
    assert payload["customer_phone"] not in logged
    assert payload["uid"] not in logged
    assert "customer_phone" not in logged

    # Safe metadata is present, and the access line carries the path without query.
    assert "easyweek capture stored id=" in logged
    assert "auth_via=query" in logged
    assert 'method=POST path="/webhooks/easyweek" status=200' in logged
    assert "token=" not in logged


def test_uvicorn_and_compose_disable_query_bearing_access_log() -> None:
    """Production entrypoints must not run uvicorn's default access log.

    It writes the full request target, which carries ?token=<secret>.
    """
    root = Path(__file__).resolve().parents[3]

    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "--no-access-log" in dockerfile

    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
    api_block = compose.split("altegio-api:", 1)[1].split("altegio-inbox-worker:", 1)[0]
    assert "--no-access-log" in api_block
    # The published port must not be world-reachable by default (an operator can
    # still opt out explicitly via API_BIND_HOST when the proxy is remote).
    assert "${API_BIND_HOST:-127.0.0.1}:8000:8000" in api_block


# ---------------------------------------------------------------------------
# read_bounded_body: memory is bounded by the limit, not by the request size
# ---------------------------------------------------------------------------


class _ChunkedRequest:
    """Minimal Request stand-in that yields a fixed chunk sequence."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.consumed: list[int] = []

    async def stream(self):
        for chunk in self._chunks:
            self.consumed.append(len(chunk))
            yield chunk


@pytest.mark.asyncio
async def test_read_bounded_body_keeps_only_the_prefix() -> None:
    """Chunks past the limit are counted but never accumulated."""
    request = _ChunkedRequest([b"a" * 4, b"b" * 4, b"c" * 4])

    prefix, total, truncated = await read_bounded_body(request, limit=6)

    assert prefix == b"aaaabb"
    assert len(prefix) == 6
    assert total == 12
    assert truncated is True
    # The stream is drained to the end even after the limit is reached.
    assert request.consumed == [4, 4, 4]


@pytest.mark.asyncio
async def test_read_bounded_body_small_body_is_complete() -> None:
    request = _ChunkedRequest([b"12", b"34"])

    prefix, total, truncated = await read_bounded_body(request, limit=1024)

    assert prefix == b"1234"
    assert total == 4
    assert truncated is False


@pytest.mark.asyncio
async def test_read_bounded_body_exact_limit_is_not_truncated() -> None:
    """A body of exactly `limit` bytes fits — truncation is a strict overflow."""
    request = _ChunkedRequest([b"x" * 8, b"y" * 8])

    prefix, total, truncated = await read_bounded_body(request, limit=16)

    assert len(prefix) == 16
    assert total == 16
    assert truncated is False


@pytest.mark.asyncio
async def test_read_bounded_body_empty_stream() -> None:
    request = _ChunkedRequest([])

    prefix, total, truncated = await read_bounded_body(request, limit=16)

    assert prefix == b""
    assert total == 0
    assert truncated is False
