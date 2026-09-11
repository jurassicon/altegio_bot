"""Transport tests for the three canary mutation endpoints (§35).

Every HTTP interaction goes through ``httpx.MockTransport``: nothing here opens
a socket. Sentinel strings stand in for the API key, the workspace slug, a
response body, a server message and a voucher artifact, and are then asserted
absent from logs, exceptions, reprs and reports.
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any

import httpx
import pytest

from altegio_bot import easyweek_voucher_mutation as mutation_module
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekClient,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_voucher_calculation import EasyWeekVoucherCalculationClient
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.easyweek_voucher_mutation import (
    EasyWeekVoucherMutationClient,
    EasyWeekVoucherMutationUnknown,
    VoucherMutationResponse,
)
from altegio_bot.tests.easyweek_voucher_canary_fixtures import (
    ACCOUNT_UUID,
    CUSTOMER_UUID,
    ORDER_UUID,
    OTHER_UUID,
    STAFFER_UUID,
    code_without_docstrings,
    imported_modules,
)

KEY = "SENTINEL_MUTKEY_ddd111"
SLUG = "SENTINEL_MUTSLUG_ddd222"
BODY_MARKER = "SENTINEL_MUTBODY_ddd333"
ERROR_MARKER = "SENTINEL_MUTERROR_ddd444"
ALL_SENTINELS = (KEY, SLUG, BODY_MARKER, ERROR_MARKER)

BASE = "https://my.easyweek.io/api/public/v2"
ORDERS_PATH = "/api/public/v2/orders"
PAY_PATH = f"/api/public/v2/orders/{ORDER_UUID}/pay"
REFUND_PATH = f"/api/public/v2/orders/{ORDER_UUID}/refund"
MARKER = "ewvc1-000000000000"

OK_BODY: dict[str, Any] = {"uuid": ORDER_UUID, "status": "open"}


def _client(handler) -> EasyWeekVoucherMutationClient:
    return EasyWeekVoucherMutationClient(
        api_key=KEY,
        workspace_slug=SLUG,
        base_url=BASE,
        transport=httpx.MockTransport(handler),
    )


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json=OK_BODY)


def _refusing_handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
    raise AssertionError("the request must be refused before the wire")


async def _create(client: EasyWeekVoucherMutationClient, **changes: Any) -> VoucherMutationResponse:
    kwargs: dict[str, Any] = {
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "customer_uuid": CUSTOMER_UUID,
        "staffer_uuid": STAFFER_UUID,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "price_minor": SUPPORTED_VOUCHER_PRICE_MINOR,
        "marker": MARKER,
    }
    kwargs.update(changes)
    return await client.create_voucher_order(**kwargs)


# ---------------------------------------------------------------------------
# The exact create request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_posts_exactly_one_voucher_line_to_the_orders_path() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return _ok(request)

    async with _client(handler) as client:
        await _create(client)

    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].url.host == "my.easyweek.io"
    assert seen[0].url.path == ORDERS_PATH
    assert not seen[0].url.query

    body = json.loads(seen[0].content.decode())
    assert body == {
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "customer_uuid": CUSTOMER_UUID,
        "staffer_uuid": STAFFER_UUID,
        "comment": MARKER,
        "vouchers": [
            {
                "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
                "price": 1500,
                "quantity": 1,
            }
        ],
    }
    # Exact ints, not booleans and not floats.
    assert type(body["vouchers"][0]["price"]) is int
    assert type(body["vouchers"][0]["quantity"]) is int


@pytest.mark.asyncio
async def test_the_create_body_carries_no_service_good_discount_or_promocode() -> None:
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content.decode()))
        return _ok(request)

    async with _client(handler) as client:
        await _create(client)

    flat = json.dumps(seen[0])
    for forbidden in ("service", "good", "discount", "promocode", "promo_code", "bulk"):
        assert forbidden not in flat, forbidden
    assert set(seen[0]) == {"location_uuid", "customer_uuid", "staffer_uuid", "comment", "vouchers"}


@pytest.mark.asyncio
async def test_create_headers_carry_the_pinned_auth_and_workspace() -> None:
    seen: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers)
        return _ok(request)

    async with _client(handler) as client:
        await _create(client)

    assert seen[0]["Authorization"] == f"Bearer {KEY}"
    assert seen[0]["Workspace"] == SLUG
    assert seen[0]["Content-Type"] == "application/json"


def test_the_create_signature_offers_no_line_item_parameters() -> None:
    parameters = set(inspect.signature(EasyWeekVoucherMutationClient.create_voucher_order).parameters)
    assert parameters == {
        "self",
        "location_uuid",
        "customer_uuid",
        "staffer_uuid",
        "voucher_template_uuid",
        "price_minor",
        "marker",
    }


# ---------------------------------------------------------------------------
# Pay and refund request contracts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pay_posts_exactly_one_account_uuid_and_nothing_else() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return _ok(request)

    async with _client(handler) as client:
        await client.pay_voucher_order(order_uuid=ORDER_UUID, account_uuid=ACCOUNT_UUID)

    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].url.path == PAY_PATH
    # The documented endpoint takes no amount, and neither do we: the sum is
    # already fixed by the exact open order and its one voucher line.
    body = json.loads(seen[0].content.decode())
    assert body == {"account_uuid": ACCOUNT_UUID}
    assert list(body) == ["account_uuid"]


@pytest.mark.asyncio
async def test_refund_posts_the_documented_empty_body_to_the_refund_path() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return _ok(request)

    async with _client(handler) as client:
        await client.refund_voucher_order(order_uuid=ORDER_UUID)

    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].url.path == REFUND_PATH
    # Truly no body: `json=None` sends nothing, so there is no field of ours,
    # no amount and no reason string in a real financial record.
    assert seen[0].content == b""


def test_pay_takes_no_amount() -> None:
    parameters = set(inspect.signature(EasyWeekVoucherMutationClient.pay_voucher_order).parameters)
    assert parameters == {"self", "order_uuid", "account_uuid"}
    assert "amount" not in mutation_module.PAY_REQUEST_FIELDS


def test_refund_takes_no_amount_or_reason() -> None:
    parameters = set(inspect.signature(EasyWeekVoucherMutationClient.refund_voucher_order).parameters)
    assert parameters == {"self", "order_uuid"}


# ---------------------------------------------------------------------------
# Pre-wire refusals
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_location", ["not-a-uuid", "", None, OTHER_UUID, KARLSRUHE_LOCATION_UUID.upper()])
async def test_only_the_confirmed_location_reaches_the_wire(bad_location) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await _create(client, location_uuid=bad_location)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_template", ["not-a-uuid", "", None, OTHER_UUID])
async def test_only_the_confirmed_template_reaches_the_wire(bad_template) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await _create(client, voucher_template_uuid=bad_template)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_price", [0, -1, 1499, 1501, True, False, 1500.0, "1500", None])
async def test_only_the_supported_nominal_reaches_the_wire(bad_price) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await _create(client, price_minor=bad_price)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_uuid",
    ["not-a-uuid", "", None, CUSTOMER_UUID.upper(), f" {CUSTOMER_UUID} ", "{" + CUSTOMER_UUID + "}"],
)
async def test_a_noncanonical_runtime_uuid_never_reaches_the_wire(bad_uuid) -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await _create(client, customer_uuid=bad_uuid)
        with pytest.raises(EasyWeekPermanentError):
            await _create(client, staffer_uuid=bad_uuid)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_marker",
    [
        "",
        None,
        "has space",
        "Straße",
        "marker;DROP",
        "x" * 65,
        "+490000000000",
        "name@example.invalid",
    ],
)
async def test_a_marker_that_could_carry_free_text_is_refused(bad_marker) -> None:
    """The comment is the only human-read field this canary writes."""
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await _create(client, marker=bad_marker)


@pytest.mark.asyncio
async def test_a_refused_scope_never_names_the_offending_value() -> None:
    async with _client(_refusing_handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await _create(client, location_uuid=OTHER_UUID)
    assert OTHER_UUID not in str(excinfo.value)


# ---------------------------------------------------------------------------
# Surface
# ---------------------------------------------------------------------------


def test_the_client_exposes_only_the_three_mutations() -> None:
    forbidden = (
        "post",
        "put",
        "patch",
        "delete",
        "request",
        "send",
        "get_order",
        "list_orders",
        "cancel_order",
        "create_booking",
        "update_voucher_template",
    )
    for name in forbidden:
        assert not hasattr(EasyWeekVoucherMutationClient, name), name

    public = {
        name
        for name in vars(EasyWeekVoucherMutationClient)
        if not name.startswith("_") and callable(getattr(EasyWeekVoucherMutationClient, name))
    }
    assert public == {"create_voucher_order", "pay_voucher_order", "refund_voucher_order", "aclose"}


def test_the_constructor_cannot_be_handed_a_client() -> None:
    parameters = set(inspect.signature(EasyWeekVoucherMutationClient.__init__).parameters)
    assert "http_client" not in parameters
    assert parameters == {"self", "api_key", "workspace_slug", "base_url", "timeout", "transport"}


@pytest.mark.asyncio
async def test_the_owned_client_never_follows_redirects() -> None:
    client = _client(_refusing_handler)
    try:
        assert client._client.follow_redirects is False
    finally:
        await client.aclose()


def test_the_module_does_not_reuse_the_migration_write_client() -> None:
    imported = imported_modules(mutation_module)
    assert not any(name.startswith("altegio_bot.easyweek_migration") for name in imported), imported


def test_the_module_builds_no_undocumented_endpoint() -> None:
    code = code_without_docstrings(mutation_module)
    for forbidden in ("cancel", "Idempotency-Key", "DELETE", "PATCH", "voucher-templates"):
        assert forbidden not in code, forbidden


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_base", ["http://my.easyweek.io/api/public/v2", "https://evil.example.com/api/public/v2"])
async def test_only_the_canonical_origin_is_accepted(bad_base) -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekVoucherMutationClient(
            api_key=KEY,
            workspace_slug=SLUG,
            base_url=bad_base,
            transport=httpx.MockTransport(_refusing_handler),
        )


# ---------------------------------------------------------------------------
# Redirects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("operation", ["create", "pay", "refund"])
async def test_every_redirect_is_one_request_and_an_unknown_outcome(status, operation) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, headers={"Location": "https://evil.example.com/api/public/v2/orders"})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekVoucherMutationUnknown) as excinfo:
            if operation == "create":
                await _create(client)
            elif operation == "pay":
                await client.pay_voucher_order(order_uuid=ORDER_UUID, account_uuid=ACCOUNT_UUID)
            else:
                await client.refund_voucher_order(order_uuid=ORDER_UUID)

    assert len(calls) == 1
    assert calls[0].url.host == "my.easyweek.io"
    assert "evil.example.com" not in str(excinfo.value)
    assert excinfo.value.retryable is False


# ---------------------------------------------------------------------------
# Unknown outcomes — one request, never repeated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500, 501, 502, 503, 504, 599])
@pytest.mark.parametrize("operation", ["create", "pay", "refund"])
async def test_rate_limit_and_every_server_error_are_unknown_after_one_request(status, operation) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(status, json={"message": ERROR_MARKER}, headers={"Retry-After": "1"})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekVoucherMutationUnknown) as excinfo:
            if operation == "create":
                await _create(client)
            elif operation == "pay":
                await client.pay_voucher_order(order_uuid=ORDER_UUID, account_uuid=ACCOUNT_UUID)
            else:
                await client.refund_voucher_order(order_uuid=ORDER_UUID)

    assert len(calls) == 1
    assert excinfo.value.attempts == 1
    assert ERROR_MARKER not in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("timeout"),
        httpx.ConnectTimeout("timeout"),
        httpx.ConnectError("refused"),
        httpx.RemoteProtocolError("disconnect"),
        httpx.WriteError("broken pipe"),
    ],
)
@pytest.mark.parametrize("operation", ["create", "pay", "refund"])
async def test_timeout_and_transport_failure_are_unknown_after_one_request(failure, operation) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise failure

    async with _client(handler) as client:
        with pytest.raises(EasyWeekVoucherMutationUnknown):
            if operation == "create":
                await _create(client)
            elif operation == "pay":
                await client.pay_voucher_order(order_uuid=ORDER_UUID, account_uuid=ACCOUNT_UUID)
            else:
                await client.refund_voucher_order(order_uuid=ORDER_UUID)

    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_factory",
    [
        lambda: httpx.Response(200, text="<html>" + BODY_MARKER + "</html>"),
        lambda: httpx.Response(200, json=[{"uuid": ORDER_UUID}]),
        lambda: httpx.Response(201, json="created"),
        lambda: httpx.Response(204),
    ],
)
@pytest.mark.parametrize("operation", ["create", "pay", "refund"])
async def test_a_malformed_2xx_is_unknown_not_success(response_factory, operation) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return response_factory()

    async with _client(handler) as client:
        with pytest.raises(EasyWeekVoucherMutationUnknown) as excinfo:
            if operation == "create":
                await _create(client)
            elif operation == "pay":
                await client.pay_voucher_order(order_uuid=ORDER_UUID, account_uuid=ACCOUNT_UUID)
            else:
                await client.refund_voucher_order(order_uuid=ORDER_UUID)

    assert len(calls) == 1
    assert BODY_MARKER not in str(excinfo.value)


def test_an_unknown_outcome_is_not_a_generic_retryable_error() -> None:
    assert not issubclass(EasyWeekVoucherMutationUnknown, EasyWeekRetryableError)
    assert EasyWeekVoucherMutationUnknown("x").retryable is False


# ---------------------------------------------------------------------------
# Permanent 4xx — safe refusals
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expected",
    [
        (400, EasyWeekPermanentError),
        (401, EasyWeekAuthError),
        (403, EasyWeekAuthError),
        (404, EasyWeekNotFoundError),
        (409, EasyWeekPermanentError),
        (422, EasyWeekPermanentError),
    ],
)
async def test_permanent_statuses_are_typed_and_sent_once(status, expected) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(status, json={"message": ERROR_MARKER, "errors": {"secret": [ERROR_MARKER]}})

    async with _client(handler) as client:
        with pytest.raises(expected) as excinfo:
            await _create(client)

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
                    "discount_amount": [ERROR_MARKER],
                    "customer": [ERROR_MARKER],
                },
            },
        )

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await _create(client)

    text = str(excinfo.value)
    assert "price" in text
    assert "vouchers" in text
    # Never sent by this client, so never echoed back.
    assert "discount_amount" not in text
    assert ERROR_MARKER not in text


@pytest.mark.asyncio
async def test_a_refund_4xx_names_no_field_at_all() -> None:
    """The refund sends no fields, so it has none to name back."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"errors": {"amount": [ERROR_MARKER]}})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as excinfo:
            await client.refund_voucher_order(order_uuid=ORDER_UUID)

    assert "amount" not in str(excinfo.value)
    assert ERROR_MARKER not in str(excinfo.value)


# ---------------------------------------------------------------------------
# Hygiene
# ---------------------------------------------------------------------------


def test_client_repr_carries_no_key_slug_header_or_url() -> None:
    client = EasyWeekVoucherMutationClient(
        api_key=KEY,
        workspace_slug=SLUG,
        base_url=BASE,
        transport=httpx.MockTransport(_refusing_handler),
    )
    for text in (repr(client), str(client)):
        for forbidden in (KEY, SLUG, BASE, "my.easyweek.io", "Authorization", "Bearer", "http"):
            assert forbidden not in text, forbidden


def test_result_repr_never_prints_the_response_body() -> None:
    envelope = {
        "uuid": ORDER_UUID,
        "voucher_code": BODY_MARKER,
        "customer": {"name": BODY_MARKER},
        "public_url": "https://example.invalid/" + BODY_MARKER,
    }
    result = VoucherMutationResponse(http_status=200, envelope=envelope)

    for text in (repr(result), str(result), f"{result}"):
        assert BODY_MARKER not in text
        assert "voucher_code" not in text
        assert ORDER_UUID not in text
    assert "200" in repr(result)
    assert result.envelope is envelope


@pytest.mark.asyncio
async def test_this_module_logs_no_sentinel_identity_url_or_body(caplog) -> None:
    """Only this module's own records.

    ``httpx`` logs every request at INFO as a full URL. Silencing a global
    logger belongs to the entry point, not to a library module, so the CLI does
    it and proves it in its own test; what is asserted here is that nothing this
    module writes itself carries a secret, an identity, a URL or a body.
    """
    caplog.set_level(logging.DEBUG)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"message": ERROR_MARKER, "note": BODY_MARKER})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekVoucherMutationUnknown):
            await _create(client)

    ours = [record for record in caplog.records if record.name == "easyweek_voucher_mutation"]
    assert ours, "the module should say something about a lost mutation"
    recorded = "\n".join(record.getMessage() for record in ours)
    for sentinel in (*ALL_SENTINELS, CUSTOMER_UUID, STAFFER_UUID, MARKER, "my.easyweek.io", "Bearer"):
        assert sentinel not in recorded, sentinel


# ---------------------------------------------------------------------------
# The other two clients are untouched
# ---------------------------------------------------------------------------


def test_the_read_client_stays_get_only() -> None:
    for name in (
        "post",
        "put",
        "patch",
        "delete",
        "request",
        "create_voucher_order",
        "pay_voucher_order",
        "refund_voucher_order",
    ):
        assert not hasattr(EasyWeekClient, name), name


def test_the_calculate_client_still_owns_only_calculate() -> None:
    public = {
        name
        for name in vars(EasyWeekVoucherCalculationClient)
        if not name.startswith("_") and callable(getattr(EasyWeekVoucherCalculationClient, name))
    }
    assert public == {"calculate_single_voucher", "aclose"}
