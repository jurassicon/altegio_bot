"""Altegio Records API client.

Endpoint used:
  GET /records/{company_id}

A visit is counted as attended when ``attendance == 1`` or
``visit_attendance == 1`` in the API response.  This corresponds to
the "Пришёл" (came) status shown in the Altegio UI.

The local ``records`` table must NOT be used as source of truth for
visit-count decisions.  It only contains a partial sync of the
client history: webhook events that arrived before the bot was
deployed are absent, causing systematic under-counting.

Authorization: Bearer {partner_token},{user_token}
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

_PAGE_SIZE = 200


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


async def count_attended_client_visits(
    *,
    company_id: int,
    altegio_client_id: int,
    timeout_sec: float = 15.0,
) -> int:
    """Return attended visit count for a client from the Altegio API.

    Paginates through all records for the client (page size
    ``_PAGE_SIZE``) and counts those where ``attendance == 1`` or
    ``visit_attendance == 1``.

    Raises ``httpx.HTTPError`` on network or HTTP errors instead of
    returning a fallback of 0.  Callers must handle the exception
    and choose the safe action (cancel job, fail for retry, etc.).
    Never treat an API failure as "0 visits" — that would wrongly
    qualify every unreachable client for a review request.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/records/{company_id}"
    total = 0
    page = 1

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        while True:
            params: dict[str, Any] = {
                "client_id": altegio_client_id,
                "count": _PAGE_SIZE,
                "page": page,
            }
            resp = await client.get(url, headers=_headers(), params=params)
            resp.raise_for_status()

            payload: Any = resp.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Unexpected Altegio response type: {type(payload)}")

            data = payload.get("data")
            if not isinstance(data, list):
                raise ValueError(f"Expected list in data, got: {type(data)}")

            for rec in data:
                att = rec.get("attendance") or 0
                v_att = rec.get("visit_attendance") or 0
                if att == 1 or v_att == 1:
                    total += 1

            if len(data) < _PAGE_SIZE:
                break

            logger.debug(
                "count_attended_client_visits page=%d company_id=%d altegio_client_id=%d partial=%d",
                page,
                company_id,
                altegio_client_id,
                total,
            )
            page += 1

    return total
