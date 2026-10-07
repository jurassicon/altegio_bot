"""Fail closed until issued-voucher validity has an evidenced API contract.

Two different questions, deliberately kept apart
------------------------------------------------
(a) **Can this code prove the term of ANY issued voucher at all?** That is a
    question about the implementation and the provider evidence behind it, and
    today the answer is no — see :data:`ISSUED_VALIDITY_PROOF_IMPLEMENTED`.
(b) **Is THIS already-issued voucher still valid?** That is a question about one
    artifact, and it is what :func:`issued_voucher_validity_reason` answers.

Conflating them is how real money gets spent on a mailing that was never going
to be deliverable. (b) can only ever be asked about a voucher that already
exists, which means after a CREATE and a PAY; (a) can be asked before either,
for free, which is where the refusal belongs. So a new contract whose (a) is
unanswered refuses CREATE and PAY outright — the caller does not get to buy
first and discover at DELIVER that nothing may be sent.

Why (a) is a constant and not a setting
---------------------------------------
Template ``validity=1`` states the product's term, not this voucher's dates.
The historical observed issued artifact proves code/template/value/price only.
The public Get POS order example has ``vouchers=[]`` and documents no voucher
activation or expiry fields. Consequently neither payment time nor approval
time can establish validity. There is deliberately no configurable bypass:
answering (a) means reviewed read-only provider evidence plus an implementation
change in this module, not an environment variable somebody can flip.

Candidate date fields below can only REFUSE a send. Their mere presence, or a
future timestamp, cannot authorize it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Final

from altegio_bot.easyweek_voucher_canary.orders import order_object

VALIDITY_UNPROVEN = "voucher_production_validity_unproven"
VOUCHER_EXPIRED = "voucher_production_voucher_expired"
# Question (a), unanswered: not "this voucher's term is unknown" but "nothing
# here can establish any voucher's term". Its own code, because an operator
# reading a refusal before the first CREATE must not be told that a voucher
# that does not exist yet has an unproven date.
VALIDITY_CAPABILITY_UNPROVEN = "voucher_production_validity_capability_unproven"

# Question (a). ``False`` until a reviewed implementation reads the real
# activation instant, timezone and expiry boundary of one issued voucher out of
# evidenced provider fields. Flipping it is a code change that comes with that
# implementation and its evidence; it is not a flag, a setting or a fixture.
ISSUED_VALIDITY_PROOF_IMPLEMENTED: Final = False


def issued_validity_capability_reason() -> str | None:
    """Why no issued voucher's term could be proven at all, or ``None``.

    Answered without a payload, a network call or a voucher, which is the whole
    point: it is the one validity question that can be asked while the answer is
    still free.
    """
    return None if ISSUED_VALIDITY_PROOF_IMPLEMENTED else VALIDITY_CAPABILITY_UNPROVEN


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
