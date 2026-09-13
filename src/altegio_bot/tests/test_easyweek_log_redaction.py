"""Keeping EasyWeek request URLs out of the logs (§36.11).

`httpx` writes one INFO line per request containing the full URL, and the Ops
test-recipient endpoint fetches `/customers/{uuid}`. The web application runs at
INFO, so that line names one human being in a container log — a thing that
cannot be taken back once it is there.

These tests drive the REAL `EasyWeekClient` over a `MockTransport` with
`httpx` and `httpcore` deliberately set to INFO first, so a fix that only works
because the test configured logging quietly would fail here.

Every identifier is synthetic.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_log_redaction import (
    URL_LOGGING_NAMESPACES,
    EasyWeekUrlLogFilter,
    redact_easyweek_url_logging,
)

CUSTOMER_UUID = "77777777-4444-4888-8aaa-777777777777"
PHONE = "+4915100000001"
FIRST_NAME = "Synthetic Testperson"
API_KEY = "SYNTHETIC-API-KEY-zzz999"
WORKSPACE = "synthetic-workspace-slug"
# The client pins this origin, so a test URL cannot be invented. Nothing leaves
# the process: every request here is answered by a MockTransport.
BASE_URL = "https://my.easyweek.io/api/public/v2"


@pytest.fixture
def noisy_transport_logging():
    """Force the transport loggers to INFO, the way the web app leaves them."""
    saved: dict[str, tuple[int, list[logging.Filter]]] = {}
    for name in URL_LOGGING_NAMESPACES:
        target = logging.getLogger(name)
        saved[name] = (target.level, list(target.filters))
        target.setLevel(logging.INFO)
        for existing in list(target.filters):
            target.removeFilter(existing)
    yield
    for name, (level, filters) in saved.items():
        target = logging.getLogger(name)
        target.setLevel(level)
        for existing in list(target.filters):
            target.removeFilter(existing)
        for original in filters:
            target.addFilter(original)


def _client(handler) -> EasyWeekClient:
    return EasyWeekClient(
        api_key=API_KEY,
        workspace_slug=WORKSPACE,
        base_url=BASE_URL,
        transport=httpx.MockTransport(handler),
        sleep=_no_sleep,
        max_attempts=2,
    )


async def _no_sleep(_seconds: float) -> None:
    return None


def _assert_clean(caplog: pytest.LogCaptureFixture) -> None:
    """Nothing identifying, and nothing that could authenticate as us."""
    surface = "\n".join([caplog.text, *(record.getMessage() for record in caplog.records)])
    for secret in (CUSTOMER_UUID, PHONE, FIRST_NAME, API_KEY, WORKSPACE, BASE_URL, "/customers/"):
        assert secret not in surface, f"{secret!r} reached the logs"


@pytest.mark.asyncio
async def test_a_successful_customer_read_logs_no_identifier(caplog, noisy_transport_logging) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith(CUSTOMER_UUID)
        return httpx.Response(200, json={"uuid": CUSTOMER_UUID, "phone": PHONE, "first_name": FIRST_NAME})

    redact_easyweek_url_logging()
    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            payload = await client.get_customer(CUSTOMER_UUID)

    assert payload["uuid"] == CUSTOMER_UUID
    _assert_clean(caplog)


@pytest.mark.asyncio
async def test_a_transport_error_logs_no_identifier(caplog, noisy_transport_logging) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    redact_easyweek_url_logging()
    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            with pytest.raises(Exception):
                await client.get_customer(CUSTOMER_UUID)

    _assert_clean(caplog)


@pytest.mark.asyncio
async def test_a_malformed_response_logs_no_identifier(caplog, noisy_transport_logging) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>not json at all</html>")

    redact_easyweek_url_logging()
    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            with pytest.raises(Exception):
                await client.get_customer(CUSTOMER_UUID)

    _assert_clean(caplog)


@pytest.mark.asyncio
async def test_an_error_status_logs_no_identifier(caplog, noisy_transport_logging) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "not found"})

    redact_easyweek_url_logging()
    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            with pytest.raises(Exception):
                await client.get_customer(CUSTOMER_UUID)

    _assert_clean(caplog)


# ---------------------------------------------------------------------------
# The filter itself, for the day somebody lowers the level again
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        pytest.param(f'HTTP Request: GET {BASE_URL}/customers/{CUSTOMER_UUID} "HTTP/1.1 200 OK"', id="uuid_path"),
        pytest.param(f"HTTP Request: GET {BASE_URL}/customers?phone=%2B4915100000001", id="phone_query"),
        pytest.param(f"connecting with Authorization: Bearer {API_KEY}", id="authorization"),
        pytest.param(f"x-api-key={API_KEY} workspace_slug={WORKSPACE}", id="credentials"),
        pytest.param(f"dialling {PHONE}", id="bare_phone"),
    ],
)
def test_the_filter_scrubs_a_line_that_still_gets_emitted(message: str) -> None:
    """Belt and braces: raising the level is the first layer, not the only one."""
    record = logging.LogRecord("httpx", logging.WARNING, __file__, 1, message, None, None)

    assert EasyWeekUrlLogFilter().filter(record) is True

    scrubbed = record.getMessage()
    for secret in (CUSTOMER_UUID, API_KEY, WORKSPACE, "4915100000001"):
        assert secret not in scrubbed
    # Scrubbed, not dropped: the shape of the line survives, so an operator can
    # still see that something happened and what kind of thing it was.
    assert "<redacted>" in scrubbed
    assert len(scrubbed) > len("<redacted>")


def test_redaction_is_idempotent_and_attaches_one_filter() -> None:
    redact_easyweek_url_logging()
    redact_easyweek_url_logging()

    for name in URL_LOGGING_NAMESPACES:
        target = logging.getLogger(name)
        attached = [f for f in target.filters if isinstance(f, EasyWeekUrlLogFilter)]
        assert len(attached) == 1
        assert target.level >= logging.WARNING


@pytest.mark.asyncio
async def test_the_ops_add_path_redacts_before_the_client_is_built(monkeypatch, noisy_transport_logging) -> None:
    """The endpoint must arrange this before constructing `EasyWeekClient`.

    A client can log while it connects, so "before the request" is too late.
    """
    import altegio_bot.ops.campaigns_api as campaigns_api

    order: list[str] = []

    def recording_redact() -> None:
        order.append("redact")

    class _Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            order.append("client")

        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

    monkeypatch.setattr(campaigns_api, "redact_easyweek_url_logging", recording_redact)
    monkeypatch.setattr(campaigns_api, "EasyWeekClient", _Client)

    async def fake_add(*args: Any, **kwargs: Any):
        from altegio_bot.campaigns.easyweek_voucher_delivery.test_recipient import TestRecipientOutcome

        return TestRecipientOutcome(False, "test_recipient_run_not_editable")

    monkeypatch.setattr(campaigns_api, "add_test_recipient_to_preview", fake_add)

    with pytest.raises(Exception):
        await campaigns_api._add_easyweek_test_recipient(1, campaigns_api.AddRecipientRequest(phone=PHONE))

    assert order == ["redact", "client"]
