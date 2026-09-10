"""Transport tests for the single non-persistent voucher calculate POST.

Every HTTP interaction goes through ``httpx.MockTransport``: nothing here
touches the network. Sentinel strings stand in for the API key, the workspace
slug, a response body and a server error message, and are then asserted absent
from logs, exceptions and reprs.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any

import httpx
import pytest

from altegio_bot import easyweek_voucher_calculation as calculation_module
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekClient,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_voucher_calculation import (
    VOUCHER_QUANTITY,
    EasyWeekCalculationUncertain,
    EasyWeekVoucherCalculationClient,
)
from altegio_bot.tests.easyweek_voucher_evidence_fixtures import (
    code_without_docstrings,
    imported_modules,
)

KEY = "SENTINEL_CALCKEY_aaa111"
SLUG = "SENTINEL_CALCSLUG_aaa222"
BODY_MARKER = "SENTINEL_CALCBODY_aaa333"
ERROR_MARKER = "SENTINEL_CALCERROR_aaa444"
ALL_SENTINELS = (KEY, SLUG, BODY_MARKER, ERROR_MARKER)

BASE = "https://my.easyweek.io/api/public/v2"
CALCULATE_PATH = "/api/public/v2/orders/calculate"
LOCATION_UUID = "8395fab6-7ee8-4702-88d9-fd78f92539c1"
TEMPLATE_UUID = "49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677"
PRICE = 1500

CANONICAL_INVOICE: dict[str, Any] = {
    "base_amount": 1500,
    "base_price": 1500,
    "subtotal": 1500,
    "total": 1500,
    "amount_due": 1500,
    "discount_amount": 0,
    "amount_paid": 0,
    "voucher_paid_amount": 0,
    "account_paid_amount": -1500,
    "order_uuid": None,
    "status": None,
}
CANONICAL_RESPONSE: dict[str, Any] = {"invoice": dict(CANONICAL_INVOICE)}


def _client(handler, **kwargs) -> EasyWeekVoucherCalculationClient:
    return EasyWeekVoucherCalculationClient(
        api_key=KEY,
        workspace_slug=SLUG,
        base_url=BASE,
        transport=httpx.MockTransport(handler),
        **kwargs,
    )


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json=CANONICAL_RESPONSE)


# ---------------------------------------------------------------------------
# The exact request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_only_post_is_the_exact_calculate_path() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return _ok(request)

    async with _client(handler) as client:
        await client.calculate_single_voucher(
            location_uuid=LOCATION_UUID,
            voucher_template_uuid=TEMPLATE_UUID,
            price_minor=PRICE,
        )

    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].url.host == "my.easyweek.io"
    assert seen[0].url.path == CALCULATE_PATH
    assert not seen[0].url.query


@pytest.mark.asyncio
async def test_the_body_is_exactly_one_voucher_line() -> None:
    import json

    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content.decode()))
        return _ok(request)

    async with _client(handler) as client:
        await client.calculate_single_voucher(
            location_uuid=LOCATION_UUID,
            voucher_template_uuid=TEMPLATE_UUID,
            price_minor=PRICE,
        )

    assert seen == [
        {
            "location_uuid": LOCATION_UUID,
            "vouchers": [
                {
                    "voucher_template_uuid": TEMPLATE_UUID,
                    "price": PRICE,
                    "quantity": 1,
                }
            ],
        }
    ]
    # An exact int, not True and not 1.0.
    assert type(seen[0]["vouchers"][0]["quantity"]) is int


@pytest.mark.asyncio
async def test_quantity_is_not_a_caller_parameter() -> None:
    signature = inspect.signature(EasyWeekVoucherCalculationClient.calculate_single_voucher)
    assert set(signature.parameters) == {"self", "location_uuid", "voucher_template_uuid", "price_minor"}
    assert VOUCHER_QUANTITY == 1


@pytest.mark.asyncio
async def test_headers_carry_the_pinned_auth_and_workspace() -> None:
    seen: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers)
        return _ok(request)

    async with _client(handler) as client:
        await client.calculate_single_voucher(
            location_uuid=LOCATION_UUID,
            voucher_template_uuid=TEMPLATE_UUID,
            price_minor=PRICE,
        )

    assert seen[0]["Authorization"] == f"Bearer {KEY}"
    assert seen[0]["Workspace"] == SLUG
    assert seen[0]["Content-Type"] == "application/json"


# ---------------------------------------------------------------------------
# Pre-wire refusals — nothing reaches the network
# ---------------------------------------------------------------------------


def _refusing_handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
    raise AssertionError("the request must be refused before the wire")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_location",
    [
        "not-a-uuid",
        "8395FAB6-7EE8-4702-88D9-FD78F92539C1",
        " 8395fab6-7ee8-4702-88d9-fd78f92539c1 ",
        "{8395fab6-7ee8-4702-88d9-fd78f92539c1}",
        "urn:uuid:8395fab6-7ee8-4702-88d9-fd78f92539c1",
        "8395fab67ee8470288d9fd78f92539c1",
        "",
        None,
        1,
    ],
)
async def test_noncanonical_location_uuid_never_reaches_the_wire(bad_location) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.calculate_single_voucher(
                location_uuid=bad_location,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_template",
    [
        "not-a-uuid",
        "49BC000C-C3A6-47C7-BDFD-B8CCD3AE2677",
        "1234567",
        "",
        None,
    ],
)
async def test_noncanonical_template_uuid_never_reaches_the_wire(bad_template) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=bad_template,
                price_minor=PRICE,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_price", [0, -1, -1500, True, False, 1500.0, "1500", None])
async def test_unusable_price_never_reaches_the_wire(bad_price) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=bad_price,
            )


# ---------------------------------------------------------------------------
# Surface: exactly one operation, no escape hatch, no mutation neighbours
# ---------------------------------------------------------------------------


def test_the_client_exposes_no_generic_or_mutating_operation() -> None:
    forbidden = (
        "post",
        "put",
        "patch",
        "delete",
        "request",
        "send",
        "create_order",
        "pay_order",
        "refund_order",
        "create_voucher",
        "list_orders",
        "get_order",
        "list_accounts",
        "list_staffers",
    )
    for name in forbidden:
        assert not hasattr(EasyWeekVoucherCalculationClient, name), name

    public = {
        name
        for name in vars(EasyWeekVoucherCalculationClient)
        if not name.startswith("_") and callable(getattr(EasyWeekVoucherCalculationClient, name))
    }
    assert public == {"calculate_single_voucher", "aclose"}


def test_the_module_builds_no_write_endpoint() -> None:
    # Docstrings are stripped first: this module deliberately NAMES the write
    # endpoints it refuses to implement, and that prose must not read as code.
    code = code_without_docstrings(calculation_module)
    for forbidden in ("orders/{", "/pay", "/refund", "Idempotency-Key"):
        assert forbidden not in code, forbidden


def test_the_module_does_not_import_the_migration_write_client() -> None:
    imported = imported_modules(calculation_module)
    assert not any(name.startswith("altegio_bot.easyweek_migration") for name in imported), imported


@pytest.mark.asyncio
async def test_a_redirect_is_not_followed() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(302, headers={"Location": "https://evil.example.com/api/public/v2/orders/calculate"})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    assert len(calls) == 1
    assert "evil.example.com" not in calls[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_base", ["http://my.easyweek.io/api/public/v2", "https://evil.example.com/api/public/v2"])
async def test_only_the_canonical_origin_is_accepted(bad_base) -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekVoucherCalculationClient(
            api_key=KEY,
            workspace_slug=SLUG,
            base_url=bad_base,
            transport=httpx.MockTransport(_refusing_handler),
        )


# ---------------------------------------------------------------------------
# Permanent failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expected",
    [
        (401, EasyWeekAuthError),
        (403, EasyWeekAuthError),
        (404, EasyWeekNotFoundError),
        (400, EasyWeekPermanentError),
        (409, EasyWeekPermanentError),
        (422, EasyWeekPermanentError),
    ],
)
async def test_permanent_statuses_are_typed_and_posted_once(status, expected) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(status, json={"message": ERROR_MARKER})

    async with _client(handler) as client:
        with pytest.raises(expected) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    assert len(calls) == 1
    assert ERROR_MARKER not in str(excinfo.value)
    assert excinfo.value.retryable is False


@pytest.mark.asyncio
async def test_a_422_names_only_fields_we_actually_sent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={
                "message": ERROR_MARKER,
                "errors": {
                    "vouchers.0.price": [ERROR_MARKER],
                    "customer_uuid": [ERROR_MARKER],
                    "0": [ERROR_MARKER],
                },
            },
        )

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    text = str(excinfo.value)
    assert "price" in text
    assert "vouchers" in text
    # Not a field this client can send, so it is not echoed back.
    assert "customer_uuid" not in text
    assert ERROR_MARKER not in text


# ---------------------------------------------------------------------------
# Uncertainty: exactly one POST, never repeated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 599])
async def test_rate_limit_and_server_errors_are_uncertain_after_one_post(status) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(status, json={"message": ERROR_MARKER}, headers={"Retry-After": "1"})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekCalculationUncertain) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    assert len(calls) == 1
    assert excinfo.value.attempts == 1
    assert ERROR_MARKER not in str(excinfo.value)


@pytest.mark.asyncio
async def test_an_uncertain_outcome_is_not_a_generic_retryable_error() -> None:
    # A caller sweeping for "retryable" must not pick this up and repeat it.
    assert not issubclass(EasyWeekCalculationUncertain, EasyWeekRetryableError)
    assert EasyWeekCalculationUncertain("x").retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("timeout"),
        httpx.ConnectTimeout("timeout"),
        httpx.ConnectError("refused"),
        httpx.RemoteProtocolError("disconnect"),
    ],
)
async def test_timeout_and_transport_failure_post_once_and_stay_unknown(failure) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise failure

    async with _client(handler) as client:
        with pytest.raises(EasyWeekCalculationUncertain):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    assert len(calls) == 1


# ---------------------------------------------------------------------------
# Malformed successes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_factory",
    [
        lambda: httpx.Response(200, text="<html>" + BODY_MARKER + "</html>"),
        lambda: httpx.Response(200, json=[{"invoice": {}}]),
        lambda: httpx.Response(200, json="ok"),
        lambda: httpx.Response(200, json={"totals": {"total": 1500}}),
        lambda: httpx.Response(200, json={"data": {"totals": {"total": 1500}}}),
    ],
)
async def test_a_2xx_without_a_readable_invoice_is_a_protocol_failure(response_factory) -> None:
    async with _client(lambda request: response_factory()) as client:
        with pytest.raises(EasyWeekProtocolError) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )
    assert BODY_MARKER not in str(excinfo.value)


@pytest.mark.asyncio
async def test_a_data_envelope_is_unwrapped() -> None:
    async with _client(lambda request: httpx.Response(200, json={"data": CANONICAL_RESPONSE})) as client:
        result = await client.calculate_single_voucher(
            location_uuid=LOCATION_UUID,
            voucher_template_uuid=TEMPLATE_UUID,
            price_minor=PRICE,
        )
    assert result.http_status == 200
    assert result.payload["invoice"]["total"] == 1500


# ---------------------------------------------------------------------------
# Hygiene
# ---------------------------------------------------------------------------


def test_repr_carries_no_key_slug_or_header() -> None:
    client = EasyWeekVoucherCalculationClient(
        api_key=KEY,
        workspace_slug=SLUG,
        base_url=BASE,
        transport=httpx.MockTransport(_refusing_handler),
    )
    for text in (repr(client), str(client)):
        assert KEY not in text
        assert SLUG not in text
        assert "Authorization" not in text


@pytest.mark.asyncio
async def test_no_sentinel_ever_reaches_a_log_record(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"message": ERROR_MARKER, "note": BODY_MARKER})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekCalculationUncertain):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    recorded = "\n".join(record.getMessage() for record in caplog.records)
    for sentinel in ALL_SENTINELS:
        assert sentinel not in recorded
    assert "my.easyweek.io" not in recorded


# ---------------------------------------------------------------------------
# The GET-only client is untouched
# ---------------------------------------------------------------------------


def test_the_read_client_stays_get_only() -> None:
    for name in ("post", "put", "patch", "delete", "request", "calculate_single_voucher"):
        assert not hasattr(EasyWeekClient, name), name


@pytest.mark.asyncio
async def test_the_read_client_still_retries_its_reads() -> None:
    calls: list[int] = []
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"ping": "pong"})

    async def sleep(delay: float) -> None:
        slept.append(delay)

    async with EasyWeekClient(
        api_key=KEY,
        workspace_slug=SLUG,
        base_url=BASE,
        transport=httpx.MockTransport(handler),
        sleep=sleep,
    ) as client:
        assert await client.ping() == {"ping": "pong"}

    assert len(calls) == 3
    assert len(slept) == 2
