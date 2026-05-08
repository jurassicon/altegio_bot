"""Altegio promo loyalty card issuance for WhatsApp promo leads.

Orchestrates: find-or-create Altegio client → issue loyalty card.
Reuses AltegioLoyaltyClient from altegio_loyalty for card HTTP transport.

TODO: endpoint paths for client lookup/creation are assumed from Altegio
REST conventions — verify against Altegio API docs before enabling
promo_issue_loyalty_card_enabled=True in production:
  - GET  {base}/clients/{company_id}?phone={digits}&count=1  — find client
  - POST {base}/clients/{company_id}  {"phone": digits, "name": ""}    — create
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.campaigns.loyalty_cleanup import make_card_number
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


class AltegioLoyaltyError(Exception):
    """Raised when Altegio client or loyalty card API call fails."""


@dataclass
class LoyaltyCardResult:
    altegio_client_id: int
    loyalty_card_id: str
    loyalty_card_number: str
    card_type_id: str


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}",
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


async def get_or_create_altegio_client(
    http_client: httpx.AsyncClient,
    *,
    company_id: int,
    phone_e164: str,
) -> int:
    """Find existing Altegio client by phone, or create one.

    Returns altegio_client_id.

    TODO: verify exact endpoint and phone param format against Altegio API docs.
    Current assumptions:
      GET  {base}/clients/{company_id}?phone={digits}&count=1
      POST {base}/clients/{company_id}  {"phone": digits, "name": ""}
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/clients/{company_id}"
    phone_digits = phone_e164.lstrip("+")

    try:
        resp = await http_client.get(
            url,
            headers=_headers(),
            params={"phone": phone_digits, "count": 1},
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        raise AltegioLoyaltyError(f"client lookup failed company={company_id}: {exc}") from exc

    items: list = []
    if isinstance(data, dict):
        raw = data.get("data")
        items = raw if isinstance(raw, list) else []
    elif isinstance(data, list):
        items = data

    if items:
        first = items[0] if items else {}
        if isinstance(first, dict):
            client_id = first.get("id")
            if isinstance(client_id, int) and client_id > 0:
                logger.info(
                    "promo_loyalty: found client altegio_client_id=%d company=%d",
                    client_id,
                    company_id,
                )
                return client_id

    # Not found — create.
    # TODO: verify exact POST payload for /clients/{company_id}
    try:
        resp = await http_client.post(
            url,
            headers=_headers(),
            json={"phone": phone_digits, "name": ""},
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        raise AltegioLoyaltyError(f"client create failed company={company_id}: {exc}") from exc

    created: dict = {}
    if isinstance(data, dict):
        inner = data.get("data")
        created = inner if isinstance(inner, dict) else data
    if isinstance(created, dict):
        client_id = created.get("id")
        if isinstance(client_id, int) and client_id > 0:
            logger.info(
                "promo_loyalty: created client altegio_client_id=%d company=%d",
                client_id,
                company_id,
            )
            return client_id

    raise AltegioLoyaltyError(f"client create returned no valid id: company={company_id} resp={data!r}")


async def issue_promo_loyalty_card(
    *,
    company_id: int,
    phone_e164: str,
    location_id: int,
    card_type_id: str,
) -> LoyaltyCardResult:
    """Find or create Altegio client, then issue a loyalty card.

    Creates and manages internal HTTP clients.

    Returns LoyaltyCardResult on success.
    Raises AltegioLoyaltyError on any failure.

    TODO: verify POST /loyalty/cards/{location_id} payload with Altegio API docs.
    """
    async with httpx.AsyncClient(timeout=20.0) as http_client:
        altegio_client_id = await get_or_create_altegio_client(
            http_client,
            company_id=company_id,
            phone_e164=phone_e164,
        )

    card_number = make_card_number(phone_e164)
    phone_int = int(phone_e164.lstrip("+"))

    loyalty = AltegioLoyaltyClient()
    try:
        card = await loyalty.issue_card(
            location_id,
            loyalty_card_number=card_number,
            loyalty_card_type_id=card_type_id,
            phone=phone_int,
        )
    except Exception as exc:
        raise AltegioLoyaltyError(f"card issuance failed location={location_id}: {exc}") from exc
    finally:
        await loyalty.aclose()

    issued_id = str(card.get("id") or card.get("loyalty_card_id") or "")
    issued_number = str(card.get("loyalty_card_number") or card_number)

    if not issued_id:
        raise AltegioLoyaltyError(f"card issuance returned no id: location={location_id} resp={card!r}")

    logger.info(
        "promo_loyalty: card issued card_id=%s card_number=%s phone=%s location=%d",
        issued_id,
        issued_number,
        phone_e164,
        location_id,
    )
    return LoyaltyCardResult(
        altegio_client_id=altegio_client_id,
        loyalty_card_id=issued_id,
        loyalty_card_number=issued_number,
        card_type_id=card_type_id,
    )
