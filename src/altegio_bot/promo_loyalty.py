"""Altegio promo loyalty card issuance for WhatsApp promo leads.

Confirmed Altegio API endpoints (verified from OpenAPI spec):
  POST   /loyalty/cards/{location_id}           — Issue a loyalty card
  DELETE /loyalty/cards/{location_id}/{card_id} — Delete a loyalty card
  GET    /loyalty/card_types/salon/{location_id} — List card types at location

Response field note (confirmed from spec):
  - Card ID is returned as 'id' (int32), NOT 'card_id' or 'loyalty_card_id'.
  - Card number is returned as 'number' (str), NOT 'loyalty_card_number'.
  - Card type is returned as 'type_id' (int32).
  - Response does NOT include client_id.

Unconfirmed endpoints — NOT used in issue_promo_loyalty_card():
  GET  {base}/clients/{company_id}?phone=...  — find client by phone
  POST {base}/clients/{company_id}             — create client
  These require promo_altegio_client_api_verified=True.
  get_or_create_altegio_client() enforces this at call time and raises
  AltegioLoyaltyError if the flag is False.

Card issuance endpoint requires promo_loyalty_card_api_verified=True (separate flag).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import httpx

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.campaigns.loyalty_cleanup import make_card_number
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


class AltegioLoyaltyError(Exception):
    """Raised when Altegio client or loyalty card API call fails."""


@dataclass
class LoyaltyCardResult:
    loyalty_card_id: str
    loyalty_card_number: str
    card_type_id: str
    # Not returned by card issuance endpoint; None until a confirmed client
    # lookup path is available and promo_altegio_client_api_verified=True.
    altegio_client_id: int | None = field(default=None)


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}",
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


def _phone_digits(phone_e164: str) -> str:
    """Strip all non-digit characters from a phone string.

    Raises AltegioLoyaltyError if the result is empty.
    """
    digits = re.sub(r"\D+", "", phone_e164)
    if not digits:
        raise AltegioLoyaltyError(f"invalid phone_e164, no digits found: {phone_e164!r}")
    return digits


async def get_or_create_altegio_client(
    http_client: httpx.AsyncClient,
    *,
    company_id: int,
    phone_e164: str,
) -> int:
    """Find existing Altegio client by phone, or create one.

    Returns altegio_client_id.

    REQUIRES promo_altegio_client_api_verified=True before calling.
    Raises AltegioLoyaltyError immediately if the flag is False.

    Endpoint paths are assumed from Altegio REST conventions and have NOT been
    verified against the loyalty API OpenAPI spec.

    TODO: verify exact endpoint and payload with Altegio API docs:
      GET  {base}/clients/{company_id}?phone={digits}&count=1
      POST {base}/clients/{company_id}  {"phone": digits, "name": ""}
    """
    if not settings.promo_altegio_client_api_verified:
        raise AltegioLoyaltyError(
            "get_or_create_altegio_client: promo_altegio_client_api_verified=False"
            " — client API endpoints not verified, refusing to call"
        )

    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/clients/{company_id}"
    phone_digits = _phone_digits(phone_e164)

    try:
        resp = await http_client.get(
            url,
            headers=_headers(),
            params={"phone": phone_digits, "count": 1},
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise AltegioLoyaltyError(f"client lookup failed company={company_id}: {exc}") from exc

    try:
        data = resp.json()
    except Exception as exc:
        raise AltegioLoyaltyError(f"client lookup returned invalid JSON: company={company_id}: {exc}") from exc

    items: list = []
    if isinstance(data, dict):
        raw = data.get("data")
        items = raw if isinstance(raw, list) else []
    elif isinstance(data, list):
        items = data
    else:
        raise AltegioLoyaltyError(
            f"client lookup returned unexpected shape {type(data).__name__}: company={company_id}"
        )

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
    except httpx.HTTPError as exc:
        raise AltegioLoyaltyError(f"client create failed company={company_id}: {exc}") from exc

    try:
        data = resp.json()
    except Exception as exc:
        raise AltegioLoyaltyError(f"client create returned invalid JSON: company={company_id}: {exc}") from exc

    created: dict = {}
    if isinstance(data, dict):
        inner = data.get("data")
        created = inner if isinstance(inner, dict) else data
    elif not isinstance(data, dict):
        raise AltegioLoyaltyError(
            f"client create returned unexpected shape {type(data).__name__}: company={company_id}"
        )

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
    phone_e164: str,
    location_id: int,
    card_type_id: str,
) -> LoyaltyCardResult:
    """Issue a loyalty card via confirmed Altegio API endpoint.

    Uses POST /loyalty/cards/{location_id} (confirmed from OpenAPI spec).
    Does NOT call client lookup/creation endpoints (unconfirmed).
    altegio_client_id is NOT returned by this endpoint — stays None.

    Returns LoyaltyCardResult on success.
    Raises AltegioLoyaltyError on any failure.
    """
    digits = _phone_digits(phone_e164)
    phone_int = int(digits)
    card_number = make_card_number(phone_e164)

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

    if not isinstance(card, dict):
        raise AltegioLoyaltyError(f"card issuance returned unexpected type {type(card).__name__}: {card!r}")

    issued_id = str(card.get("id") or card.get("loyalty_card_id") or "")
    # Confirmed field name from API spec is 'number'; fallback covers older compat.
    issued_number = str(card.get("number") or card.get("loyalty_card_number") or card_number)

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
        loyalty_card_id=issued_id,
        loyalty_card_number=issued_number,
        card_type_id=card_type_id,
        altegio_client_id=None,
    )
