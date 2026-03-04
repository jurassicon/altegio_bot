"""Tests for AltegioLoyaltyClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient

BASE = "https://api.alteg.io/api/v1"


@pytest.fixture()
def client() -> AltegioLoyaltyClient:
    return AltegioLoyaltyClient(base_url=BASE)


@pytest.mark.asyncio
async def test_get_card_types_returns_list(client: AltegioLoyaltyClient) -> None:
    with respx.mock:
        respx.get(f"{BASE}/loyalty/card_types/salon/123").mock(
            return_value=httpx.Response(
                200,
                json={"data": [{"id": "1", "title": "Gold"}]},
            )
        )
        result = await client.get_card_types(123)

    assert result == [{"id": "1", "title": "Gold"}]
    await client.aclose()


@pytest.mark.asyncio
async def test_get_card_types_bare_list(client: AltegioLoyaltyClient) -> None:
    """When API returns a plain list (no 'data' wrapper) it is handled."""
    with respx.mock:
        respx.get(f"{BASE}/loyalty/card_types/salon/123").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "2", "title": "Silver"}],
            )
        )
        result = await client.get_card_types(123)

    assert result == [{"id": "2", "title": "Silver"}]
    await client.aclose()


@pytest.mark.asyncio
async def test_issue_card_returns_card_dict(client: AltegioLoyaltyClient) -> None:
    with respx.mock:
        respx.post(f"{BASE}/loyalty/cards/123").mock(
            return_value=httpx.Response(
                200,
                json={"data": {"id": 99, "loyalty_card_number": "0074454347287392"}},
            )
        )
        result = await client.issue_card(
            123,
            loyalty_card_number="0074454347287392",
            loyalty_card_type_id="1",
            phone=4915112345678,
        )

    assert result["loyalty_card_number"] == "0074454347287392"
    await client.aclose()


@pytest.mark.asyncio
async def test_issue_card_raises_on_http_error(
    client: AltegioLoyaltyClient,
) -> None:
    with respx.mock:
        respx.post(f"{BASE}/loyalty/cards/123").mock(return_value=httpx.Response(400, json={"error": "bad request"}))
        with pytest.raises(httpx.HTTPStatusError):
            await client.issue_card(
                123,
                loyalty_card_number="0000",
                loyalty_card_type_id="1",
                phone=4915100000000,
            )
    await client.aclose()


@pytest.mark.asyncio
async def test_delete_card_calls_delete(client: AltegioLoyaltyClient) -> None:
    with respx.mock:
        route = respx.delete(f"{BASE}/loyalty/cards/123/99").mock(return_value=httpx.Response(204))
        await client.delete_card(123, 99)

    assert route.called
    await client.aclose()


@pytest.mark.asyncio
async def test_delete_card_raises_on_error(
    client: AltegioLoyaltyClient,
) -> None:
    with respx.mock:
        respx.delete(f"{BASE}/loyalty/cards/123/99").mock(return_value=httpx.Response(404, json={"error": "not found"}))
        with pytest.raises(httpx.HTTPStatusError):
            await client.delete_card(123, 99)
    await client.aclose()
