"""Tests for the query-free, injection-safe access log middleware.

Two layers are covered:
  * ``safe_log_path`` in isolation — it is exactly what the middleware writes for
    the path field, so asserting on its output is asserting on the log content;
  * the middleware end to end, driven as raw ASGI, so a crafted ``scope["path"]``
    with a newline proves it cannot forge a second log line.
"""

from __future__ import annotations

import logging

import pytest

from altegio_bot.main import AccessLogMiddleware, safe_log_path
from altegio_bot.webhooks.common import safe_log_value

LINE_SEP = chr(0x2028)
PARA_SEP = chr(0x2029)


# ---------------------------------------------------------------------------
# safe_log_path: escaping and bounding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "/webhooks/easyweek\nInjected: fake",  # %0A newline
        "/path\rInjected",  # %0D carriage return
        "/path\x1b[31mred",  # %1B ANSI escape
        "/path" + LINE_SEP + "x",  # U+2028 line separator
        "/path" + PARA_SEP + "x",  # U+2029 paragraph separator
    ],
)
def test_safe_log_path_has_no_raw_control_or_line_breaks(raw: str) -> None:
    out = safe_log_path(raw)
    # No character a log viewer would treat as a new line / control byte.
    for ch in ("\n", "\r", "\x1b", LINE_SEP, PARA_SEP):
        assert ch not in out
    # Everything is ASCII after json.dumps(ensure_ascii=True).
    assert out.isascii()


def test_safe_log_path_bounds_length() -> None:
    out = safe_log_path("/" + "a" * 10_000, limit=2048)
    # json.dumps adds two surrounding quotes; the payload itself is capped.
    assert len(out) <= 2048 + 2


def test_safe_log_path_keeps_a_normal_path_readable() -> None:
    assert safe_log_path("/webhooks/easyweek") == '"/webhooks/easyweek"'


# ---------------------------------------------------------------------------
# Middleware as raw ASGI
# ---------------------------------------------------------------------------


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


async def _drive(scope) -> list:
    sent: list = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await AccessLogMiddleware(_ok_app)(scope, receive, send)
    return sent


def _http_scope(*, path: str, method: str = "POST", query: bytes = b"") -> dict:
    return {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "path": path,
        "raw_path": path.encode("utf-8", "surrogatepass"),
        "query_string": query,
        "headers": [],
    }


def _access_message(caplog) -> str:
    return next(r.getMessage() for r in caplog.records if r.name == "altegio_bot.access")


@pytest.mark.asyncio
async def test_newline_in_path_does_not_forge_a_log_line(caplog) -> None:
    scope = _http_scope(path="/webhooks/easyweek\nmethod=GET path=/forged status=200")

    with caplog.at_level(logging.INFO, logger="altegio_bot.access"):
        await _drive(scope)

    records = [r for r in caplog.records if r.name == "altegio_bot.access"]
    # Exactly one record — the injected newline did not split the log.
    assert len(records) == 1
    message = records[0].getMessage()
    # The newline is neutralised: no raw byte, present only as the escape "\n".
    assert "\n" not in message
    assert "\\n" in message
    # The whole injected string stays inside the single quoted path field.
    assert message.startswith('method=POST path="/webhooks/easyweek\\n')


@pytest.mark.asyncio
async def test_query_string_is_never_logged(caplog) -> None:
    scope = _http_scope(
        path="/webhooks/easyweek",
        query=b"event=booking-created&token=SUPER-SECRET-MARKER",
    )

    with caplog.at_level(logging.INFO, logger="altegio_bot.access"):
        sent = await _drive(scope)

    message = _access_message(caplog)
    assert "SUPER-SECRET-MARKER" not in message
    assert "token=" not in message
    assert "status=200" in message
    assert 'path="/webhooks/easyweek"' in message
    # Middleware is transparent to the response.
    assert sent[0]["status"] == 200


@pytest.mark.asyncio
async def test_status_500_is_logged_when_handler_raises(caplog) -> None:
    async def _boom(scope, receive, send):
        raise RuntimeError("handler exploded")

    scope = _http_scope(path="/health", method="GET")

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        pass

    with caplog.at_level(logging.INFO, logger="altegio_bot.access"):
        with pytest.raises(RuntimeError):
            await AccessLogMiddleware(_boom)(scope, receive, send)

    message = _access_message(caplog)
    assert "status=500" in message
    # The exception text must not leak into the access line.
    assert "handler exploded" not in message


@pytest.mark.asyncio
async def test_unknown_method_is_not_logged_verbatim(caplog) -> None:
    scope = _http_scope(path="/health", method="EVIL\nmethod=GET")

    with caplog.at_level(logging.INFO, logger="altegio_bot.access"):
        await _drive(scope)

    message = _access_message(caplog)
    assert "method=INVALID" in message
    assert "\n" not in message


@pytest.mark.asyncio
async def test_non_http_scope_is_passed_through() -> None:
    """Lifespan/websocket scopes must not be touched by the access logger."""
    seen = {}

    async def _lifespan_app(scope, receive, send):
        seen["type"] = scope["type"]

    await AccessLogMiddleware(_lifespan_app)({"type": "lifespan"}, None, None)
    assert seen["type"] == "lifespan"


# ---------------------------------------------------------------------------
# Honest output limits: the cap applies to the ESCAPED result
# ---------------------------------------------------------------------------

ASTRAL = chr(0x1F600)  # emoji — two \uXXXX escapes (12 chars) per character


def test_ascii_path_under_limit_is_returned_whole() -> None:
    out = safe_log_path("/webhooks/easyweek")
    assert out == '"/webhooks/easyweek"'
    assert "truncated" not in out


def test_10000_ascii_chars_respect_the_limit() -> None:
    out = safe_log_path("/" + "a" * 10_000, limit=2048)
    assert len(out) <= 2048
    assert out.endswith('<truncated>"')


def test_2048_astral_chars_respect_the_limit() -> None:
    """Regression: capping the INPUT let 2048 emoji expand to ~24KB of log."""
    out = safe_log_path("/" + ASTRAL * 2048, limit=2048)
    assert len(out) <= 2048
    assert out.isascii()
    assert out.endswith('<truncated>"')


def test_truncation_never_splits_an_escape_sequence() -> None:
    out = safe_log_path("/" + ASTRAL * 2048, limit=2048)
    # Strip quotes and the marker, then confirm the remainder is whole escapes.
    inner = out[1:-1]
    assert inner.endswith("...<truncated>")
    body = inner[: -len("...<truncated>")]
    escapes = body.lstrip("/")
    assert len(escapes) % 6 == 0  # each \uXXXX is exactly 6 chars
    assert escapes.count("\\u") == len(escapes) // 6


def test_control_characters_are_escaped_within_the_limit() -> None:
    out = safe_log_path("/x" + "\n\r\x1b" * 1000, limit=256)
    assert len(out) <= 256
    for ch in ("\n", "\r", "\x1b"):
        assert ch not in out


def test_safe_log_value_uses_the_same_length_semantics() -> None:
    out = safe_log_value(ASTRAL * 1000, limit=128)
    assert len(out) <= 128
    assert out.isascii()
    assert out.endswith('<truncated>"')


def test_safe_log_value_keeps_short_identifiers_readable() -> None:
    assert safe_log_value("message_created") == '"message_created"'
    assert safe_log_value(12345) == '"12345"'
