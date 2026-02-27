"""Altegio Loyalty Cards API client.

Endpoints used:
  GET  /loyalty/card_types/salon/{location_id}
  POST /loyalty/cards/{location_id}
  DELETE /loyalty/cards/{location_id}/{card_id}

Authorization: Bearer {partner_token},{user_token}
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


def _auth_header() -> str:
    return f'Bearer {settings.altegio_partner_token},{settings.altegio_user_token}'


class AltegioLoyaltyClient:
    """Thin async HTTP wrapper for Altegio loyalty card endpoints."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout_sec: float = 20.0,
    ) -> None:
        self._base_url = (base_url or settings.altegio_api_base_url).rstrip('/')
        self._client = httpx.AsyncClient(timeout=timeout_sec)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {
            'Authorization': _auth_header(),
            'Accept': settings.altegio_api_accept,
            'Content-Type': 'application/json',
        }

    async def get_card_types(self, location_id: int) -> list[dict[str, Any]]:
        """Return loyalty card types available for *location_id*."""
        url = f'{self._base_url}/loyalty/card_types/salon/{location_id}'
        res = await self._client.get(url, headers=self._headers())
        res.raise_for_status()
        data: Any = res.json()
        # Altegio wraps results in {"data": [...]}
        if isinstance(data, dict) and 'data' in data:
            items = data['data']
            return items if isinstance(items, list) else []
        if isinstance(data, list):
            return data
        return []

    async def issue_card(
        self,
        location_id: int,
        *,
        loyalty_card_number: str,
        loyalty_card_type_id: str,
        phone: int,
    ) -> dict[str, Any]:
        """Issue a loyalty card for *phone* and return the created card dict."""
        url = f'{self._base_url}/loyalty/cards/{location_id}'
        payload = {
            'loyalty_card_number': loyalty_card_number,
            'loyalty_card_type_id': loyalty_card_type_id,
            'phone': phone,
        }
        res = await self._client.post(
            url, headers=self._headers(), json=payload
        )
        res.raise_for_status()
        data: Any = res.json()
        if isinstance(data, dict) and 'data' in data:
            return data['data']  # type: ignore[return-value]
        return data  # type: ignore[return-value]

    async def delete_card(self, location_id: int, card_id: int) -> None:
        """Delete loyalty card *card_id* from *location_id*."""
        url = f'{self._base_url}/loyalty/cards/{location_id}/{card_id}'
        res = await self._client.delete(url, headers=self._headers())
        res.raise_for_status()
