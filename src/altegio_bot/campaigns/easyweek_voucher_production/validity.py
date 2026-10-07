"""Fail closed until issued-voucher validity has an evidenced API contract.

Template ``validity=1`` states the product's term, not this voucher's dates.
The historical observed issued artifact proves code/template/value/price only.
The public Get POS order example has ``vouchers=[]`` and documents no voucher
activation or expiry fields. Consequently neither payment time nor approval
time can establish validity. There is deliberately no configurable bypass.

Candidate date fields below can only REFUSE a send. Their mere presence, or a
future timestamp, cannot authorize it. Supporting a positive proof requires
reviewed read-only provider evidence and an explicit implementation change.
"""

from __future__ import annotations

from datetime import datetime, timezone

from altegio_bot.easyweek_voucher_canary.orders import order_object

VALIDITY_UNPROVEN = "voucher_production_validity_unproven"
VOUCHER_EXPIRED = "voucher_production_voucher_expired"


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def issued_voucher_validity_reason(payload: object, *, now: datetime) -> str:
    """No positive inference from an undocumented issued-artifact date shape."""
    moment = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    order = order_object(payload)
    if order is None:
        return VALIDITY_UNPROVEN
    nodes = order.get("vouchers")
    if not isinstance(nodes, list):
        nodes = [order.get("voucher")]
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("is_expired") is True or node.get("status") == "expired":
            return VOUCHER_EXPIRED
        for name in ("expires_at", "valid_until"):
            date = _timestamp(node.get(name))
            if date is not None and date <= moment:
                return VOUCHER_EXPIRED
    return VALIDITY_UNPROVEN
