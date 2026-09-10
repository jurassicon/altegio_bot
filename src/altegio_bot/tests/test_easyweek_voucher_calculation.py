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
    VoucherCalculationResult,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.tests.easyweek_voucher_evidence_fixtures import (
    canonical_response,
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
LOCATION_UUID = KARLSRUHE_LOCATION_UUID
TEMPLATE_UUID = EASYWEEK_VOUCHER_TEMPLATE_UUID
PRICE = SUPPORTED_VOUCHER_PRICE_MINOR

CANONICAL_RESPONSE: dict[str, Any] = canonical_response()

# Syntactically perfect UUIDs that this transport still may not send: a real
# UUID for the wrong branch or the wrong product is not a smaller mistake than
# a malformed string.
OTHER_LOCATION_UUID = "11111111-2222-4333-8444-555555555555"
OTHER_TEMPLATE_UUID = "00000000-0000-0000-0000-000000000000"


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
        # Not a UUID at all.
        "not-a-uuid",
        "",
        None,
        1,
        # Canonical-looking variants of the right branch: still not the literal.
        "8395FAB6-7EE8-4702-88D9-FD78F92539C1",
        " 8395fab6-7ee8-4702-88d9-fd78f92539c1 ",
        "{8395fab6-7ee8-4702-88d9-fd78f92539c1}",
        "urn:uuid:8395fab6-7ee8-4702-88d9-fd78f92539c1",
        "8395fab67ee8470288d9fd78f92539c1",
        # A perfectly valid UUID for some OTHER branch.
        OTHER_LOCATION_UUID,
    ],
)
async def test_only_the_confirmed_location_reaches_the_wire(bad_location) -> None:
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
        # A perfectly valid UUID for some OTHER product.
        OTHER_TEMPLATE_UUID,
    ],
)
async def test_only_the_confirmed_template_reaches_the_wire(bad_template) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=bad_template,
                price_minor=PRICE,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_price",
    [
        0,
        -1,
        -1500,
        True,
        False,
        1500.0,
        "1500",
        None,
        # The two prices production proved the API happily accepts. Neither is
        # the supported nominal, and neither may leave this process.
        1499,
        1501,
        150000,
    ],
)
async def test_only_the_supported_nominal_reaches_the_wire(bad_price) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=bad_price,
            )


@pytest.mark.asyncio
async def test_a_refused_scope_never_names_the_offending_value() -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=OTHER_LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )
    assert OTHER_LOCATION_UUID not in str(excinfo.value)


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


def test_the_constructor_cannot_be_handed_a_client() -> None:
    """No ``http_client``: a caller could otherwise re-enable redirects.

    An ``httpx.AsyncClient(follow_redirects=True)`` handed in here would let a
    307/308 answer replay this POST — method, headers and body preserved —
    against the persistent order endpoint. The parameter is gone, and the client
    is always built and owned internally.
    """
    parameters = inspect.signature(EasyWeekVoucherCalculationClient.__init__).parameters
    assert "http_client" not in parameters
    assert set(parameters) == {"self", "api_key", "workspace_slug", "base_url", "timeout", "transport"}


@pytest.mark.asyncio
async def test_the_owned_client_never_follows_redirects() -> None:
    client = _client(_refusing_handler)
    try:
        assert client._client.follow_redirects is False
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_a_307_to_the_persistent_orders_endpoint_is_refused_after_one_post() -> None:
    """The exact escape this fix closes: 307 preserves method AND body."""
    seen: list[tuple[str, str, bytes, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.method,
                request.url.path,
                request.content,
                request.headers.get("Authorization"),
            )
        )
        return httpx.Response(
            307,
            headers={"Location": "https://my.easyweek.io/api/public/v2/orders"},
        )

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    # Exactly one request, and it was the calculate path.
    assert len(seen) == 1
    method, path, content, authorization = seen[0]
    assert method == "POST"
    assert path == CALCULATE_PATH
    # The persistent endpoint was never touched, directly or via Location.
    assert not any(entry[1].endswith("/orders") for entry in seen)
    # The Authorization header and the body left exactly once.
    assert authorization == f"Bearer {KEY}"
    assert content
    assert excinfo.value.status_code == 307
    assert "my.easyweek.io" not in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_every_redirect_status_is_a_typed_fail_closed_refusal(status) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            status,
            headers={"Location": "https://evil.example.com/api/public/v2/orders"},
        )

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await client.calculate_single_voucher(
                location_uuid=LOCATION_UUID,
                voucher_template_uuid=TEMPLATE_UUID,
                price_minor=PRICE,
            )

    assert len(calls) == 1
    assert calls[0].url.host == "my.easyweek.io"
    assert calls[0].url.path == CALCULATE_PATH
    assert excinfo.value.retryable is False
    assert "evil.example.com" not in str(excinfo.value)


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
async def test_a_data_envelope_is_handed_back_whole() -> None:
    """The outer object survives: dropping it would lose outer-level signals."""
    body = {"data": CANONICAL_RESPONSE, "order_uuid": None, "status": None}

    async with _client(lambda request: httpx.Response(200, json=body)) as client:
        result = await client.calculate_single_voucher(
            location_uuid=LOCATION_UUID,
            voucher_template_uuid=TEMPLATE_UUID,
            price_minor=PRICE,
        )

    assert result.http_status == 200
    # Not unwrapped to `data`: both levels are still reachable.
    assert set(result.envelope) == {"data", "order_uuid", "status"}
    assert result.envelope["data"]["invoice"]["total"] == 1500


@pytest.mark.asyncio
async def test_an_outer_persistence_field_is_not_discarded_by_the_transport() -> None:
    body = {
        "order_uuid": "11111111-2222-4333-8444-555555555555",
        "status": "open",
        "data": CANONICAL_RESPONSE,
    }

    async with _client(lambda request: httpx.Response(200, json=body)) as client:
        result = await client.calculate_single_voucher(
            location_uuid=LOCATION_UUID,
            voucher_template_uuid=TEMPLATE_UUID,
            price_minor=PRICE,
        )

    # The transport does not judge it — but it must not throw it away either.
    assert result.envelope["order_uuid"] == "11111111-2222-4333-8444-555555555555"
    assert result.envelope["status"] == "open"


# ---------------------------------------------------------------------------
# Hygiene
# ---------------------------------------------------------------------------


def test_client_repr_carries_no_key_slug_header_or_url() -> None:
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
        # A repr lands in tracebacks and incident tickets; the endpoint is not
        # something either of those needs.
        assert "my.easyweek.io" not in text
        assert BASE not in text
        assert "http" not in text


def test_result_repr_never_prints_the_response_body() -> None:
    """A dataclass repr reaches tracebacks, pytest output and log records."""
    envelope = {
        "invoice": {"total": 1500, "comment": BODY_MARKER},
        "customer": {"name": BODY_MARKER},
        "voucher_code": BODY_MARKER,
        "public_url": "https://example.invalid/" + BODY_MARKER,
    }
    result = VoucherCalculationResult(http_status=200, envelope=envelope)

    for text in (repr(result), str(result), f"{result}"):
        assert BODY_MARKER not in text
        assert "invoice" not in text
        assert "voucher_code" not in text
    # The status is still useful and still there.
    assert "200" in repr(result)
    # The payload itself remains available to the domain projection.
    assert result.envelope is envelope


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
