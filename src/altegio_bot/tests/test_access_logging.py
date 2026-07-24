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
