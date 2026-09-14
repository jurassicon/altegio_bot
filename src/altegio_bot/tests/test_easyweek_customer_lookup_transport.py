"""The production `GET /customers` the manual add path actually uses (§37.1).

The manual endpoint resolves a phone number through the ordinary
`EasyWeekClient`. A test that swaps that client for a hand-written fake proves
the *caller* works and says nothing about whether the real one can make the
request at all — which is exactly the gap this file closes: the real class, a
`MockTransport`, and assertions about the wire.

What matters on the wire is as much about what is absent as what is present. A
`location_uuid` or a `staffer_uuid` would narrow a workspace-wide search and
turn a proven absence into a wrong one; a caller-controlled `per_page` would
turn one lookup into an export.

Every identifier here is synthetic and nothing leaves the process.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from altegio_bot.easyweek_client import (
    CUSTOMER_LOOKUP_PER_PAGE,
    EasyWeekClient,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)

PHONE = "+4915100000042"
CUSTOMER_UUID = "aaaa1111-2222-4333-8444-bbbbbbbbbbbb"
API_KEY = "SYNTHETIC-API-KEY-zzz999"
WORKSPACE = "synthetic-workspace-slug"
BASE_URL = "https://my.easyweek.io/api/public/v2"


def _page(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": len(rows)},
    }


async def _no_sleep(_seconds: float) -> None:
    return None


def _client(handler) -> EasyWeekClient:
    return EasyWeekClient(
        api_key=API_KEY,
        workspace_slug=WORKSPACE,
        base_url=BASE_URL,
        transport=httpx.MockTransport(handler),
        sleep=_no_sleep,
        max_attempts=2,
    )


@pytest.mark.asyncio
async def test_the_request_is_a_workspace_wide_customers_read() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_page([{"uuid": CUSTOMER_UUID, "phone": PHONE, "first_name": "T"}]))

    async with _client(handler) as client:
        payload = await client.list_customers(params={"phone": PHONE, "page": 1})

    assert payload["meta"]["total"] == 1
    [request] = seen
    assert request.method == "GET"
    assert request.url.path.endswith("/customers")
    assert request.url.params["phone"] == PHONE
    assert request.url.params["page"] == "1"
    # Fixed by the client, not by the caller.
    assert request.url.params["per_page"] == str(CUSTOMER_LOOKUP_PER_PAGE)
    # Workspace-wide: narrowing it would make an absence unprovable.
    for forbidden in ("location_uuid", "staffer_uuid", "created_at", "date_from", "date_to"):
        assert forbidden not in request.url.params


@pytest.mark.asyncio
async def test_every_requested_page_reaches_the_wire() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.params["page"])
        return httpx.Response(200, json=_page([]))

    async with _client(handler) as client:
        await client.list_customers(params={"phone": PHONE, "page": 1})
        await client.list_customers(params={"phone": PHONE, "page": 3})

    assert seen == ["1", "3"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"phone": PHONE, "page": 1, "per_page": 500}, id="per_page"),
        pytest.param({"phone": PHONE, "page": 1, "location_uuid": CUSTOMER_UUID}, id="location"),
        pytest.param({"phone": PHONE, "page": 1, "staffer_uuid": CUSTOMER_UUID}, id="staffer"),
        pytest.param({"phone": PHONE, "search": "anything"}, id="search"),
    ],
)
async def test_an_unknown_query_parameter_is_refused_before_the_request(params: dict[str, Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("a refused query must not reach the wire")

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.list_customers(params=params)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"phone": "nonsense", "page": 1}, id="unparseable"),
        pytest.param({"phone": "015112345678", "page": 1}, id="not_canonical"),
        pytest.param({"phone": f" {PHONE} ", "page": 1}, id="padded"),
        pytest.param({"page": 1}, id="absent"),
        pytest.param({"phone": PHONE, "page": 0}, id="page_zero"),
        pytest.param({"phone": PHONE, "page": -1}, id="page_negative"),
        pytest.param({"phone": PHONE, "page": "1"}, id="page_not_int"),
    ],
)
async def test_an_unusable_filter_is_refused_before_the_request(params: dict[str, Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError("a refused filter must not reach the wire")

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.list_customers(params=params)


@pytest.mark.asyncio
async def test_a_response_that_is_not_an_object_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"uuid": CUSTOMER_UUID}])

    async with _client(handler) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.list_customers(params={"phone": PHONE, "page": 1})


@pytest.mark.asyncio
async def test_a_malformed_body_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>not json</html>")

    async with _client(handler) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.list_customers(params={"phone": PHONE, "page": 1})


@pytest.mark.asyncio
async def test_a_transport_failure_is_retryable_and_never_a_result() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with _client(handler) as client:
        with pytest.raises(EasyWeekRetryableError):
            await client.list_customers(params={"phone": PHONE, "page": 1})


@pytest.mark.asyncio
async def test_the_lookup_never_writes() -> None:
    """Structural on the wire: one GET, and nothing else."""
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(200, json=_page([]))

    async with _client(handler) as client:
        await client.list_customers(params={"phone": PHONE, "page": 1})

    assert methods == ["GET"]


def test_the_client_does_not_import_the_mutating_migration_surface() -> None:
    """A runtime read must not drag a write client in behind it."""
    import ast

    tree = ast.parse(open("src/altegio_bot/easyweek_client.py").read())
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}

    for forbidden in (
        "altegio_bot.easyweek_migration.write_client",
        "altegio_bot.easyweek_migration.apply",
        "altegio_bot.easyweek_voucher_mutation",
    ):
        assert forbidden not in imported
