"""Is the ORIGINAL voucher still something a repeat message may point at? (§46.6)

Why the stored columns are not the answer
-----------------------------------------
A slot carries a keyed MAC, the key's identifier and the order uuid the voucher
was issued on. Those three being present proves that a binding was once made.
They do not prove that the binding still verifies, and they say nothing at all
about what has happened to the artifact since — EasyWeek owns the voucher's
validity, its balance and its redemption (§45.4), and a voucher refunded or
written off after the original delivery leaves the slot's ``delivered`` exactly
as it was.

§46.6 is explicit: «заново читается и доказывается именно исходный artifact,
проверяются HMAC/key ID и контракт исходной schema». So this module reads that
one order and proves it.

What it proves, with the checks this project already has
-------------------------------------------------------
Nothing is reimplemented. The §42 stage preconditions already know how to prove
an order is the one a slot names, and the same proofs are used here:

* one exact GET of ``target_order_uuid``, with the body required to describe
  that order — a 200 is not proof that it is about what was asked;
* the order's ``comment`` equal to the slot's own reconciliation marker, which
  is how this phase recognises its own orders;
* ``observe_artifact`` against the ORIGINAL schema's contract: the customer the
  order is bound to, the voucher line's shape and price, and whether a refund
  is observed. Schema 3 and schema 4 are different contracts — the paid €10
  product and the free gift certificate — and the slot's own schema decides
  which one applies, never the one a new mailing would use today;
* the documented validity signals of §45.4: an explicit ``is_expired``, an
  ``expired`` status, or an ``expires_at``/``valid_until`` already past. The
  ABSENCE of individual dates is the ordinary shape of a correct voucher under
  this contract and blocks nothing;
* SETTLEMENT and the amounts, through the same ``paid_order_amounts_proven``
  the primary delivery gate applies to this schema. A documented paid status
  AND every published total at both levels equal to what the ORIGINAL schema's
  contract says, with ``amount_due`` zero. An order that is merely ``open`` is
  refused: a voucher line in the response and a MAC that verifies prove a
  binding was made, not that the order behind it was ever settled. For a free
  schema-4 issue the zero totals are not themselves the proof — an unsettled
  draft has the same ones — so its documented zero-amount adjustments are
  required as well;
* the real MAC check. The code is read out of the order through the same proof
  the payment gate uses and compared against the slot's stored binding by
  :func:`...ledger.binding_matches`, which also pins the batch, the slot and the
  template uuid. A rotated key, a different order or a different code all fail
  it, and the MAC never leaves the ledger.

What it refuses to do
---------------------
It makes no mutation of any kind: no CREATE, no PAY, no refund, no replacement
code, no extended term, and no redemption tracking. It promises no balance and
no expiry date it has not been shown. It invents no semantics for undocumented
fields. An unreadable answer, a timeout or a mismatch is ``unproven`` — never
"probably still valid" — and the code it reads in memory is never stored,
logged or returned.

Locks
-----
The order read takes no database handle. The MAC check afterwards opens its own
short read session, which is why the two are separate steps here rather than
one convenient function holding a transaction across HTTP.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    ENTITLEMENT_INVALID,
    ENTITLEMENT_SETTLEMENT_UNPROVEN,
    ENTITLEMENT_UNPROVEN,
    ENTITLEMENT_UNREADABLE,
)
from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger
from altegio_bot.campaigns.easyweek_voucher_production.validity import issued_voucher_validity_reason
from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_normalizer import canonical_booking_uuid  # noqa: F401 - re-exported shape helper
from altegio_bot.easyweek_voucher_canary.artifact import observe_artifact
from altegio_bot.easyweek_voucher_canary.orders import (
    ORDER_CANCELLED,
    ORDER_MALFORMED,
    ORDER_PAID,
    ORDER_REFUNDED,
    ORDER_UNKNOWN,
    classify_order,
    order_object,
    paid_order_amounts_proven,
    prove_voucher_line,
)
from altegio_bot.easyweek_voucher_production_contract import ProductionVoucherContract, production_contract

logger = logging.getLogger("easyweek_voucher_followup_artifact")

__all__ = ["ArtifactVerdict", "prove_source_artifact"]

# The order states that say the voucher is gone rather than unclear. Reported as
# an exclusion with its own reason; everything else unclear is unproven.
_INVALID_ORDER_STATES = frozenset({ORDER_REFUNDED, ORDER_CANCELLED})


@dataclass(frozen=True)
class ArtifactVerdict:
    """Whether the original artifact is still referable, and why not when it is not."""

    proven: bool
    reason: str | None = None
    # Shape facts only, for the stored snapshot and the operator's screen. No
    # code, no customer value, no raw provider response.
    observations: dict[str, Any] = field(default_factory=dict)

    @property
    def unknown(self) -> bool:
        """Is this "we could not tell" rather than "this voucher is gone"?"""
        return not self.proven and self.reason in {
            ENTITLEMENT_UNPROVEN,
            ENTITLEMENT_UNREADABLE,
            ENTITLEMENT_SETTLEMENT_UNPROVEN,
        }


def _canonical(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


async def prove_source_artifact(
    reader: Any,
    *,
    session_maker: async_sessionmaker[AsyncSession],
    batch_id: int,
    slot: int,
    schema_version: str,
    target_order_uuid: object,
    reconciliation_marker: str,
    easyweek_customer_uuid: object,
    now: datetime,
) -> ArtifactVerdict:
    """Read and prove ONE slot's original order. Read-only, no session held.

    Returns ``proven`` only when every check above passed. The three negative
    answers are kept apart because an operator acts on them differently: the
    voucher is gone (``followup_entitlement_invalid``), the answer could not be
    read (``followup_entitlement_unreadable``), or it was read and does not
    prove what it must (``followup_entitlement_unproven``).
    """
    order_uuid = _canonical(target_order_uuid)
    if order_uuid is None:
        return ArtifactVerdict(False, ENTITLEMENT_UNPROVEN)
    if reader is None:
        # No configured read, so nothing about the artifact is established.
        return ArtifactVerdict(False, ENTITLEMENT_UNREADABLE, {"source": "reader_unavailable"})

    contract: ProductionVoucherContract = production_contract(str(schema_version))

    try:
        payload = await reader.get_order(order_uuid)
    except EasyWeekError:
        return ArtifactVerdict(False, ENTITLEMENT_UNREADABLE, {"source": "provider_error"})
    except Exception:  # noqa: BLE001 - an unreadable answer proves nothing
        return ArtifactVerdict(False, ENTITLEMENT_UNREADABLE, {"source": "unreadable"})

    order = order_object(payload)
    if order is None or _canonical(order.get("uuid")) != order_uuid:
        # A 200 is not proof the body describes the order we asked about.
        return ArtifactVerdict(False, ENTITLEMENT_UNREADABLE, {"source": "order_identity"})
    if _canonical(order.get("comment")) != _canonical(reconciliation_marker):
        # Somebody else's order, or this phase's marker rewritten. Either way
        # it is not the artifact this slot names.
        return ArtifactVerdict(False, ENTITLEMENT_UNPROVEN, {"source": "marker_mismatch"})

    observation = observe_artifact(
        payload,
        stage="followup_preview_readback",
        expected_customer_uuid=str(easyweek_customer_uuid or ""),
        expected_template_uuid=contract.template_uuid,
        expected_price_minor=contract.issue_price_minor,
        expected_value_minor=contract.face_value_minor,
    )
    facts: dict[str, Any] = {"schema_version": str(schema_version), **observation.as_safe_dict()}

    state, _payment = classify_order(payload, expected_price_minor=contract.issue_price_minor)
    facts["order_state"] = state
    if observation.refund_observed or state in _INVALID_ORDER_STATES:
        return ArtifactVerdict(False, ENTITLEMENT_INVALID, facts)
    if state in {ORDER_UNKNOWN, ORDER_MALFORMED}:
        return ArtifactVerdict(False, ENTITLEMENT_UNPROVEN, facts)

    expiry = issued_voucher_validity_reason(payload, now=now)
    if expiry is not None:
        facts["validity"] = expiry
        return ArtifactVerdict(False, ENTITLEMENT_INVALID, facts)

    if not observation.order_customer_binding_proven or not observation.voucher_line_proven:
        return ArtifactVerdict(False, ENTITLEMENT_UNPROVEN, facts)

    # SETTLEMENT, with the proof the primary delivery already applies to this
    # same schema. An order that is merely OPEN was never settled: the voucher
    # may exist in the response and its MAC may verify, and neither says the
    # order behind it was paid. The amounts are proven in the same step and at
    # both published levels, so a paid status beside a total that contradicts the
    # contract is refused rather than trusted — and a free schema-4 issue needs
    # its documented zero-amount adjustments, because zero totals alone are also
    # what an unsettled draft looks like. The historical schema's own price and
    # conditions decide, never a newer mailing's.
    facts["order_paid"] = state == ORDER_PAID
    settled = state == ORDER_PAID and paid_order_amounts_proven(
        payload, expected_price_minor=contract.issue_price_minor
    )
    facts["settlement_proven"] = bool(settled)
    if not settled:
        return ArtifactVerdict(False, ENTITLEMENT_SETTLEMENT_UNPROVEN, facts)

    code = _issued_code(order, contract=contract)
    if code is None:
        return ArtifactVerdict(False, ENTITLEMENT_UNPROVEN, facts)

    # The real MAC check, in its own short session with nothing external in
    # flight. ``binding_matches`` pins the batch, the slot, the template uuid
    # and the order, so a code from another slot cannot verify this one.
    matched = await production_ledger.binding_matches(
        session_maker,
        batch_id=int(batch_id),
        slot=int(slot),
        voucher_code=code,
        target_order_uuid=order_uuid,
    )
    facts["binding_verified"] = bool(matched)
    if not matched:
        return ArtifactVerdict(False, ENTITLEMENT_UNPROVEN, facts)
    return ArtifactVerdict(True, None, facts)


def _issued_code(order: dict[str, Any], *, contract: ProductionVoucherContract) -> str | None:
    """The one issued code of this order, in memory, or ``None``.

    Read through the same §35 proof the payment gate uses, so a body this phase
    would refuse to pay for is also a body it refuses to read a code out of. The
    value is used for the MAC comparison and nothing else: it is never stored,
    returned, logged or put in a report.
    """
    proof = prove_voucher_line(
        order,
        expected_template_uuid=contract.template_uuid,
        expected_price_minor=contract.issue_price_minor,
        expected_value_minor=contract.face_value_minor,
    )
    if not proof.proven:
        return None
    vouchers = order.get("vouchers")
    if isinstance(vouchers, list) and len(vouchers) == 1 and isinstance(vouchers[0], dict):
        code = vouchers[0].get("code")
        return code if isinstance(code, str) and code else None
    return None
