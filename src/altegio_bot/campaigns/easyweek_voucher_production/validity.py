"""Who answers for an issued voucher's term, and what this module still refuses.

The provider owns the term (§45.4)
----------------------------------
EasyWeek controls a voucher's validity, its remaining balance and its
redemption. The approved product states the term — single use, one month from
activation, unused balance forfeited — the product baseline proves those product
facts, and the approved message states the same conditions to the recipient.
That is the promise the customer gets, and EasyWeek is what enforces it.

So this module does NOT prove the activation instant or the expiry boundary of
one issued code, and nothing it feeds into a report ever claims that it did. The
provider publishes no evidenced field for either: the official Get POS order
example carries ``vouchers=[]`` and documents neither, and inventing a semantics
for an undocumented field would be a worse answer than not answering.

An artifact that simply carries no activation or expiry date is therefore not a
refusal. It is the ordinary shape of a correct issued voucher under this
contract, and it blocks neither CREATE, nor PAY, nor DELIVER, and it does not
make a slot reconciliation-required.

What it still refuses
---------------------
The supported, unambiguous statements that a voucher is NOT usable: an explicit
``is_expired``, an explicit ``expired`` status, and an ``expires_at`` or
``valid_until`` already in the past. Those are the provider saying so, which is a
different fact from the provider saying nothing — and ignoring them would send a
code EasyWeek has already written off.

What it deliberately does not answer
------------------------------------
Whether the order and the voucher line could be read at all. That is not a
question about a term, and answering it here would collapse "EasyWeek returned
something unreadable" into "this voucher has no optional date". The runner's own
order, payment, artifact and binding checks refuse an unreadable or unbound
artifact on their own, both when the plan is built and again before the send
claim; this module speaks only where the provider spoke.

What may not replace it
-----------------------
Nothing substitutes a term here: not the age of a preview or a batch, not the
CREATE or PAY instant, not the approval TTL — which stays a check on how fresh a
permission is and is never read as a voucher's term — and not an arbitrary limit
in hours or days. An external API failure is not positive evidence either.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Final

from altegio_bot.easyweek_voucher_canary.orders import order_object

VOUCHER_EXPIRED = "voucher_production_voucher_expired"

# How a report names who answers for the term of this contract's vouchers.
# Deliberately a string and not a boolean: a ``true`` here would read as "this
# application proved each issued voucher's dates", which is exactly what §45.4
# says it does not do and must not claim.
PROVIDER_MANAGED_VALIDITY: Final = "provider_managed"


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def issued_voucher_validity_reason(payload: object, *, now: datetime) -> str | None:
    """A supported signal that this voucher is unusable, or ``None``.

    ``None`` means "nothing the provider published here says this voucher is
    invalid". It never means "this application proved the voucher is valid", and
    no caller may report it as such.

    An unreadable payload answers ``None`` for the same reason: this function has
    nothing to say about an artifact it cannot see, and the caller refuses such an
    artifact through the order and binding checks instead.
    """
    moment = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    order = order_object(payload)
    if order is None:
        return None
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
    return None
