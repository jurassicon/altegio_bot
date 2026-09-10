"""Read-only EasyWeek gift-card evidence for campaign readiness (PR-13).

No value returned by this module is a customer message or a write instruction.
The confirmed API UUID is the only voucher-template identity kept in code.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, Protocol
from urllib.parse import urlsplit

# Re-exported from the single literal source so the transport and the
# calculation evidence pin themselves to exactly these values without importing
# this readiness module. Readiness semantics are unchanged.
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    KARLSRUHE_LOCATION_UUID,
)

GIFT_CARD_TEMPLATE_UNPROVEN: Final = "gift_card_template_unproven"
GIFT_CARD_WORKSPACE_MISMATCH: Final = "gift_card_workspace_mismatch"
GIFT_CARD_BRANCH_SCOPE_MISMATCH: Final = "gift_card_branch_scope_mismatch"
GIFT_CARD_DISABLED: Final = "gift_card_disabled"
GIFT_CARD_ONLINE_SALES_DISABLED: Final = "gift_card_online_sales_disabled"
GIFT_CARD_PUBLIC_URL_UNPROVEN: Final = "gift_card_public_url_unproven"
GIFT_CARD_SEMANTICS_UNPROVEN: Final = "gift_card_semantics_unproven"
GIFT_CARD_ISSUE_CONTRACT_UNPROVEN: Final = "gift_card_issue_contract_unproven"


class GiftCardReadClient(Protocol):
    async def get_workspace(self) -> dict[str, Any]: ...

    async def list_locations(self) -> list[dict[str, Any]]: ...

    async def list_voucher_templates(self) -> list[dict[str, Any]]: ...

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class GiftCardReadiness:
    ready: bool
    reasons: tuple[str, ...]
    template_uuid: str
    workspace_uuid: str
    location_uuid: str
    currency: str | None
    monetary_value: str | None
    validity: str
    branch_scope: str
    customer_purchase_url: str | None


def _object(payload: object) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _exact_int(value: object) -> int | None:
    return value if type(value) is int else None


def _explicit_customer_url(template: dict[str, Any]) -> str | None:
    """Accept only an explicit public URL from the API; never derive one."""
    value = template.get("public_purchase_url")
    if not isinstance(value, str) or value != value.strip() or not value:
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        return None
    # EasyWeek's authenticated admin host is never customer-facing evidence.
    if parsed.hostname.casefold() == "my.easyweek.io":
        return None
    return value


def evaluate_gift_card_readiness(
    *,
    workspace_payload: object,
    locations_payload: object,
    template_payload: object,
    required_location_uuid: str | None = None,
) -> GiftCardReadiness:
    """Project read-only evidence into stable, PII-free readiness reasons."""
    workspace = _object(workspace_payload)
    template = _object(template_payload)
    reasons: list[str] = []

    workspace_uuid = workspace.get("uuid")
    workspace_slug = workspace.get("slug")
    if workspace_uuid != EASYWEEK_WORKSPACE_UUID or workspace_slug != EASYWEEK_WORKSPACE_SLUG:
        reasons.append(GIFT_CARD_WORKSPACE_MISMATCH)

    if template.get("uuid") != EASYWEEK_VOUCHER_TEMPLATE_UUID:
        reasons.append(GIFT_CARD_TEMPLATE_UNPROVEN)

    locations = locations_payload
    if isinstance(locations, dict):
        locations = locations.get("data")
    location_rows = locations if isinstance(locations, list) else []
    location_uuids = {
        item.get("uuid") for item in location_rows if isinstance(item, dict) and isinstance(item.get("uuid"), str)
    }
    if KARLSRUHE_LOCATION_UUID not in location_uuids:
        reasons.append(GIFT_CARD_BRANCH_SCOPE_MISMATCH)

    all_branches = template.get("is_connected_all_branches") is True
    branches_count = _exact_int(template.get("branches_count"))
    all_branches_count = _exact_int(template.get("all_branches_count"))
    branch_scope = "all_branches" if all_branches else "restricted_or_unproven"
    if required_location_uuid is not None:
        # An all-branches product is explicitly not proof of a Karlsruhe-only one.
        if required_location_uuid != KARLSRUHE_LOCATION_UUID or all_branches:
            reasons.append(GIFT_CARD_BRANCH_SCOPE_MISMATCH)
    elif not all_branches or branches_count is None or branches_count != all_branches_count:
        reasons.append(GIFT_CARD_BRANCH_SCOPE_MISMATCH)

    if template.get("is_enabled") is not True:
        reasons.append(GIFT_CARD_DISABLED)
    if template.get("is_online") is not True:
        reasons.append(GIFT_CARD_ONLINE_SALES_DISABLED)

    customer_url = _explicit_customer_url(template)
    if customer_url is None:
        reasons.append(GIFT_CARD_PUBLIC_URL_UNPROVEN)

    currency = workspace.get("currency") if isinstance(workspace.get("currency"), str) else None
    cost = _exact_int(template.get("cost"))
    value = _exact_int(template.get("value"))
    monetary_value: str | None = None
    if currency == "EUR" and cost == value == 1500:
        monetary_value = str(Decimal(value) / Decimal(100))
    # A title containing a percentage is not machine-readable discount proof.
    if monetary_value is None or template.get("value_type") != "monetary":
        reasons.append(GIFT_CARD_SEMANTICS_UNPROVEN)

    # Read-only counts, including a future non-zero value, cannot prove that
    # this application may issue/sell a voucher safely or reconcile an unknown
    # write result. PR-13 has no write contract, so the blocker is unconditional.
    reasons.append(GIFT_CARD_ISSUE_CONTRACT_UNPROVEN)

    validity = "unproven_or_unlimited_per_api" if template.get("validity") is None else "api_value_present"
    unique_reasons = tuple(dict.fromkeys(reasons))
    return GiftCardReadiness(
        ready=not unique_reasons,
        reasons=unique_reasons,
        template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        workspace_uuid=EASYWEEK_WORKSPACE_UUID,
        location_uuid=KARLSRUHE_LOCATION_UUID,
        currency=currency,
        monetary_value=monetary_value,
        validity=validity,
        branch_scope=branch_scope,
        customer_purchase_url=customer_url,
    )


async def probe_gift_card_readiness(
    client: GiftCardReadClient,
    *,
    required_location_uuid: str | None = None,
) -> GiftCardReadiness:
    """Collect evidence using only the four reviewed GET methods."""
    workspace = await client.get_workspace()
    locations = await client.list_locations()
    templates = await client.list_voucher_templates()
    listed = next(
        (row for row in templates if row.get("uuid") == EASYWEEK_VOUCHER_TEMPLATE_UUID),
        None,
    )
    if listed is None:
        template: dict[str, Any] = {}
    else:
        template = await client.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    return evaluate_gift_card_readiness(
        workspace_payload=workspace,
        locations_payload=locations,
        template_payload=template,
        required_location_uuid=required_location_uuid,
    )
