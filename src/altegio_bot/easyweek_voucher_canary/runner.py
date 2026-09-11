"""Stage orchestration and unknown-result reconciliation for the canary (§35).

Each public entry point here is ONE operator stage. There is deliberately no
function that runs create, pay and refund in sequence: the stop between them is
the control, not an inconvenience, and a human deciding "yes, go on" after
seeing what the last stage actually did is the whole safety model.

Every stage follows the same shape:

1. rebuild the plan live from GETs and refuse unless it still authorises this
   exact stage (digest, phrase, freshness, no drift);
2. re-read the template and refuse on any freeze or counter change;
3. claim in PostgreSQL and COMMIT the claim;
4. send at most one request;
5. record the outcome, then verify it by reading.

An unknown outcome is never retried. It is resolved by looking: the create by a
bounded, complete, marker-scoped walk of this customer's orders in this branch,
the pay and the refund by reading the one exact order the ledger names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.artifact import ArtifactObservation, observe_artifact
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_API_UNAVAILABLE,
    CANARY_API_UNCERTAIN,
    CREATE_WINDOW_AFTER,
    CREATE_WINDOW_BEFORE,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
    CanaryPlan,
    CanaryReader,
    RuntimeIdentity,
    build_plan,
    verify_plan_authorisation,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.easyweek_voucher_mutation import (
    EasyWeekVoucherMutationUnknown,
    VoucherMutationResponse,
)
from altegio_bot.utils import utcnow

# Outcomes. A closed vocabulary the CLI maps to exit codes, so a wrapper can act
# on the result without parsing prose.
OUTCOME_PROVEN: Final = "proven"
OUTCOME_REFUSED: Final = "refused"
OUTCOME_UNKNOWN_MUTATION: Final = "unknown_mutation_result"
OUTCOME_CONTRACT_MISMATCH: Final = "contract_mismatch"
OUTCOME_AMBIGUOUS: Final = "ambiguous_reconciliation"
OUTCOME_MANUAL_CLEANUP: Final = "manual_cleanup_required"
OUTCOME_ROLLBACK_UNPROVEN: Final = "final_rollback_unproven"

# Stable reason codes owned by this module.
REASON_MUTATION_UNKNOWN: Final = "canary_mutation_unknown"
REASON_MUTATION_REJECTED: Final = "canary_mutation_rejected"
REASON_RECONCILE_UNRESOLVED: Final = "canary_reconciliation_unresolved"
REASON_RECONCILE_AMBIGUOUS: Final = "canary_reconciliation_ambiguous"
REASON_LEDGER_STATE_INVALID: Final = "canary_ledger_state_invalid"
REASON_ORDER_SHAPE_UNPROVEN: Final = "canary_order_shape_unproven"
REASON_MANUAL_CLEANUP_REQUIRED: Final = "canary_manual_cleanup_required"
REASON_ROLLBACK_UNPROVEN: Final = "canary_rollback_unproven"
REASON_TEMPLATE_DRIFT: Final = "canary_template_drift"

# Order classifications, decided from documented fields only.
ORDER_OPEN: Final = "open"
ORDER_PAID: Final = "paid"
ORDER_REFUNDED: Final = "refunded"
ORDER_CANCELLED: Final = "cancelled"
ORDER_MALFORMED: Final = "malformed"

# How a payment was proven. `account_paid_amount` is deliberately NOT one of
# these: it is an opaque bookkeeping figure and proves nothing about an order.
PAYMENT_PROOF_STATUS: Final = "documented_status_flag"
PAYMENT_PROOF_AMOUNTS: Final = "settled_order_amounts"
PAYMENT_PROOF_NONE: Final = "none"

_MAX_ORDER_PAGES: Final = 50


class VoucherMutator(Protocol):
    """The three-endpoint mutation surface, and nothing wider."""

    async def create_voucher_order(
        self,
        *,
        location_uuid: str,
        customer_uuid: str,
        staffer_uuid: str,
        voucher_template_uuid: str,
        price_minor: int,
        marker: str,
    ) -> VoucherMutationResponse: ...

    async def pay_voucher_order(
        self, *, order_uuid: str, account_uuid: str, amount_minor: int
    ) -> VoucherMutationResponse: ...

    async def refund_voucher_order(self, *, order_uuid: str) -> VoucherMutationResponse: ...


@dataclass
class StageReport:
    """The PII-free result of one operator stage. Safe to print, always."""

    stage: str
    outcome: str
    reasons: list[str] = field(default_factory=list)
    external_mutation_attempted: bool = False
    reconciliation_required: bool = False
    manual_cleanup_required: bool = False
    ledger: dict[str, Any] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    template_counters: dict[str, int] | None = None
    order_state: str | None = None
    payment_proof: str | None = None
    remote_rollback_proven: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_canary_stage",
            "mutation_stage": self.stage,
            "outcome": self.outcome,
            "reasons": list(dict.fromkeys(self.reasons)),
            "external_mutation_attempted": self.external_mutation_attempted,
            "reconciliation_required": self.reconciliation_required,
            "manual_cleanup_required": self.manual_cleanup_required,
            "order_state": self.order_state,
            "payment_proof": self.payment_proof,
            "remote_rollback_proven": self.remote_rollback_proven,
            "template_counters": dict(self.template_counters) if self.template_counters is not None else None,
            "observations": list(self.observations),
            "ledger": dict(self.ledger),
            # Repeated verbatim on every stage, success included. A green stage
            # is a research result, never a permission.
            "campaign_send_authorized": False,
            "customer_message_sent": False,
            "ready_for_send": False,
            "raw_identifiers_omitted": True,
        }


# ---------------------------------------------------------------------------
# Order shape reading — documented fields only
# ---------------------------------------------------------------------------


def _order_object(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    inner = payload.get("data")
    if isinstance(inner, dict):
        return inner
    return payload


def _rows(payload: object) -> list[Any]:
    rows = payload.get("data") if isinstance(payload, dict) else payload
    return rows if isinstance(rows, list) else []


def _true(value: object) -> bool:
    return value is True


def classify_order(payload: object) -> tuple[str, str]:
    """``(order_state, payment_proof)`` from documented fields only.

    Refund/cancellation is decided before payment: an order that was paid and
    then reverted is *refunded*, and reporting it as paid would hide exactly the
    rollback the canary has to prove.
    """
    order = _order_object(payload)
    if order is None or not isinstance(order.get("uuid"), str):
        return ORDER_MALFORMED, PAYMENT_PROOF_NONE

    status = order.get("status")
    status_slug = status.casefold() if isinstance(status, str) else ""

    if _true(order.get("is_reverted")) or _true(order.get("is_refunded")) or status_slug in {"refunded", "reverted"}:
        return ORDER_REFUNDED, PAYMENT_PROOF_NONE
    if _true(order.get("is_canceled")) or _true(order.get("is_cancelled")) or status_slug in {"canceled", "cancelled"}:
        return ORDER_CANCELLED, PAYMENT_PROOF_NONE

    if _true(order.get("is_paid")) or status_slug in {"paid", "completed", "closed"}:
        return ORDER_PAID, PAYMENT_PROOF_STATUS

    # A settled invoice is the second documented way to see a payment. The
    # opaque `account_paid_amount` is never consulted.
    invoice = order.get("invoice")
    invoice = invoice if isinstance(invoice, dict) else order
    amount_due = invoice.get("amount_due")
    amount_paid = invoice.get("amount_paid")
    if (
        type(amount_due) is int
        and amount_due == 0
        and type(amount_paid) is int
        and amount_paid == SUPPORTED_VOUCHER_PRICE_MINOR
    ):
        return ORDER_PAID, PAYMENT_PROOF_AMOUNTS

    return ORDER_OPEN, PAYMENT_PROOF_NONE


def _structurally_matches(order: dict[str, Any], *, identity: RuntimeIdentity, marker: str) -> bool:
    """A candidate order is ours only if EVERY expected fact holds.

    Time and customer alone are never enough: two operators testing on the same
    day would both match. The marker is what makes the match ours, and the
    location, the customer and the voucher line are what make it the order we
    meant to create.
    """
    if order.get("comment") != marker:
        return False
    if order.get("location_uuid") != KARLSRUHE_LOCATION_UUID:
        return False

    customer = order.get("customer")
    customer_ok = order.get("customer_uuid") == identity.customer_uuid or (
        isinstance(customer, dict) and customer.get("uuid") == identity.customer_uuid
    )
    if not customer_ok:
        return False

    staffer = order.get("staffer")
    staffer_ok = order.get("staffer_uuid") == identity.staffer_uuid or (
        isinstance(staffer, dict) and staffer.get("uuid") == identity.staffer_uuid
    )
    if not staffer_ok:
        return False

    vouchers = order.get("vouchers")
    if not isinstance(vouchers, list) or len(vouchers) != 1 or not isinstance(vouchers[0], dict):
        return False
    line = vouchers[0]
    return (
        line.get("voucher_template_uuid") == EASYWEEK_VOUCHER_TEMPLATE_UUID
        and type(line.get("price")) is int
        and line.get("price") == SUPPORTED_VOUCHER_PRICE_MINOR
        and type(line.get("quantity")) is int
        and line.get("quantity") == 1
    )


def _within_window(order: dict[str, Any], *, start: datetime, end: datetime) -> bool:
    raw = order.get("created_at")
    if not isinstance(raw, str) or not raw:
        return False
    try:
        created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if created.tzinfo is None:
        return False
    return start <= created <= end


@dataclass(frozen=True)
class CreateMatch:
    """What a complete marker-scoped walk found."""

    count: int
    order_uuid: str | None
    complete: bool


async def find_marker_order(
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
    marker: str,
    window_start: datetime,
    window_end: datetime,
) -> CreateMatch:
    """Walk this customer's orders in this branch, completely, and match ours.

    ``complete`` is false when the walk hit its page ceiling. An incomplete walk
    that found nothing is UNRESOLVED, never "it was not created": the whole
    reason this function exists is that the create may have landed unseen.

    No candidate UUID is printed or logged; only the one match, and only into the
    ledger where a refund needs it.
    """
    matches: list[str] = []
    for page in range(1, _MAX_ORDER_PAGES + 1):
        payload = await reader.list_location_orders(
            location_uuid=KARLSRUHE_LOCATION_UUID,
            customer_uuid=identity.customer_uuid,
            page=page,
        )
        rows = _rows(payload)
        for row in rows:
            if not isinstance(row, dict):
                continue
            if not _structurally_matches(row, identity=identity, marker=marker):
                continue
            if not _within_window(row, start=window_start, end=window_end):
                continue
            found = row.get("uuid")
            if isinstance(found, str) and found:
                matches.append(found)
        if not rows:
            unique = list(dict.fromkeys(matches))
            return CreateMatch(count=len(unique), order_uuid=unique[0] if len(unique) == 1 else None, complete=True)
    unique = list(dict.fromkeys(matches))
    return CreateMatch(count=len(unique), order_uuid=unique[0] if len(unique) == 1 else None, complete=False)


# ---------------------------------------------------------------------------
# Shared stage preamble
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Authorisation:
    """A freshly recomputed plan that still authorises one exact stage."""

    plan: CanaryPlan
    reasons: tuple[str, ...]

    @property
    def granted(self) -> bool:
        return not self.reasons


async def authorise(
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
    enabled: bool,
    stage: str,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    now: datetime | None = None,
) -> Authorisation:
    """Rebuild the plan from live GETs and check it still authorises *stage*.

    Runs before every claim, every time. This is the drift check: a template
    whose price moved, a staffer who left, an account that vanished or a marker
    order that already exists all change the snapshot, so they all change the
    digest, so they all stop the stage before anything is claimed.
    """
    plan = await build_plan(reader, identity=identity, enabled=enabled, now=now)
    reasons = verify_plan_authorisation(
        plan,
        stage=stage,
        supplied_digest=plan_digest,
        supplied_issued_at=plan_issued_at,
        supplied_phrase=confirmation_phrase,
        now=now,
    )
    return Authorisation(plan=plan, reasons=reasons)


def _refusal(stage: str, reasons: tuple[str, ...] | list[str], ledger: dict[str, Any]) -> StageReport:
    return StageReport(
        stage=stage,
        outcome=OUTCOME_REFUSED,
        reasons=list(reasons),
        external_mutation_attempted=False,
        ledger=ledger,
    )


# ---------------------------------------------------------------------------
# Stage: create
# ---------------------------------------------------------------------------


async def run_create(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    mutator: VoucherMutator,
    *,
    identity: RuntimeIdentity,
    enabled: bool,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
) -> StageReport:
    """Create the ONE open POS order, having committed the claim first."""
    authorisation = await authorise(
        reader,
        identity=identity,
        enabled=enabled,
        stage=STAGE_CREATE,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
    )
    snapshot = await ledger_module.load(session_maker)
    if not authorisation.granted:
        return _refusal(STAGE_CREATE, authorisation.reasons, snapshot.as_safe_dict())

    plan = authorisation.plan
    now = utcnow()
    claim = await ledger_module.claim_create(
        session_maker,
        plan_digest=plan.digest,
        template_snapshot_digest=plan.template_snapshot_digest,
        customer_fingerprint=identity.fingerprints["customer"],
        staffer_fingerprint=identity.fingerprints["staffer"],
        account_fingerprint=identity.fingerprints["account"],
        reconciliation_marker=plan.marker,
        create_window_start=now - CREATE_WINDOW_BEFORE,
        create_window_end=now + CREATE_WINDOW_AFTER,
    )
    if not claim.granted:
        report = _refusal(STAGE_CREATE, [claim.reason], (await ledger_module.load(session_maker)).as_safe_dict())
        report.reconciliation_required = claim.status in ledger_module.UNRESOLVED_STATUSES
        return report

    # The claim is committed. From here on, an interruption reads as "the
    # request may have gone out", which is exactly what it is.
    try:
        response = await mutator.create_voucher_order(
            location_uuid=KARLSRUHE_LOCATION_UUID,
            customer_uuid=identity.customer_uuid,
            staffer_uuid=identity.staffer_uuid,
            voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
            marker=plan.marker,
        )
    except EasyWeekVoucherMutationUnknown:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_UNKNOWN,
            reason_code=REASON_MUTATION_UNKNOWN,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=ledger.as_safe_dict(),
            template_counters=plan.counters,
        )
    except EasyWeekError:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_REJECTED,
            reason_code=REASON_MUTATION_REJECTED,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[REASON_MUTATION_REJECTED],
            external_mutation_attempted=True,
            ledger=ledger.as_safe_dict(),
            template_counters=plan.counters,
        )

    observation = observe_artifact(
        response.envelope,
        stage="create_response",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    order = _order_object(response.envelope) or {}
    order_uuid = order.get("uuid")
    if not isinstance(order_uuid, str) or not order_uuid:
        # A 2xx we cannot tie to an order is not a success we can record: there
        # is nothing to read back, refund or cancel.
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_UNKNOWN,
            reason_code=REASON_ORDER_SHAPE_UNPROVEN,
            evidence={"create_response": observation.as_safe_dict()},
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_ORDER_SHAPE_UNPROVEN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=ledger.as_safe_dict(),
            observations=[observation.as_safe_dict()],
            template_counters=plan.counters,
        )

    readback, readback_observation, counters = await _read_back(
        reader, order_uuid=order_uuid, identity=identity, stage="create_readback"
    )
    ledger = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        target_order_uuid=order_uuid,
        verified_field="create_verified_at",
        evidence={
            "create_response": observation.as_safe_dict(),
            "create_readback": readback_observation.as_safe_dict() if readback_observation else None,
        },
    )
    state, _ = classify_order(readback) if readback is not None else (ORDER_MALFORMED, PAYMENT_PROOF_NONE)
    return StageReport(
        stage=STAGE_CREATE,
        outcome=OUTCOME_PROVEN,
        external_mutation_attempted=True,
        manual_cleanup_required=True,
        ledger=ledger.as_safe_dict(),
        observations=[
            observation.as_safe_dict(),
            *([readback_observation.as_safe_dict()] if readback_observation else []),
        ],
        template_counters=counters or plan.counters,
        order_state=state,
    )


async def _read_back(
    reader: CanaryReader,
    *,
    order_uuid: str,
    identity: RuntimeIdentity,
    stage: str,
) -> tuple[dict[str, Any] | None, ArtifactObservation | None, dict[str, int] | None]:
    """Read the exact order and the template again, projecting safe facts only."""
    from altegio_bot.easyweek_voucher_canary.plan import template_counters

    try:
        order = await reader.get_order(order_uuid)
    except EasyWeekError:
        return None, None, None
    observation = observe_artifact(
        order,
        stage=stage,
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    try:
        template = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except EasyWeekError:
        return order, observation, None
    return order, observation, template_counters(template)


# ---------------------------------------------------------------------------
# Stage: pay
# ---------------------------------------------------------------------------


async def run_pay(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    mutator: VoucherMutator,
    *,
    identity: RuntimeIdentity,
    enabled: bool,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
) -> StageReport:
    """Pay the ONE created order, once, on the approved account."""
    authorisation = await authorise(
        reader,
        identity=identity,
        enabled=enabled,
        stage=STAGE_PAY,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
    )
    snapshot = await ledger_module.load(session_maker)
    if not authorisation.granted:
        return _refusal(STAGE_PAY, authorisation.reasons, snapshot.as_safe_dict())
    if snapshot.target_order_uuid is None:
        report = _refusal(STAGE_PAY, [REASON_LEDGER_STATE_INVALID], snapshot.as_safe_dict())
        report.reconciliation_required = snapshot.status in ledger_module.UNRESOLVED_STATUSES
        return report

    plan = authorisation.plan
    target = snapshot.target_order_uuid
    claim = await ledger_module.claim_pay(
        session_maker,
        plan_digest=plan.digest,
        template_snapshot_digest=plan.template_snapshot_digest,
    )
    if not claim.granted:
        report = _refusal(STAGE_PAY, [claim.reason], (await ledger_module.load(session_maker)).as_safe_dict())
        report.reconciliation_required = claim.status in ledger_module.UNRESOLVED_STATUSES
        return report

    try:
        response = await mutator.pay_voucher_order(
            order_uuid=target,
            account_uuid=identity.account_uuid,
            amount_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
        )
    except EasyWeekVoucherMutationUnknown:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAY_UNKNOWN,
            reason_code=REASON_MUTATION_UNKNOWN,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=ledger.as_safe_dict(),
            template_counters=plan.counters,
        )
    except EasyWeekError:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAY_REJECTED,
            reason_code=REASON_MUTATION_REJECTED,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[REASON_MUTATION_REJECTED],
            external_mutation_attempted=True,
            manual_cleanup_required=True,
            ledger=ledger.as_safe_dict(),
            template_counters=plan.counters,
        )

    pay_observation = observe_artifact(
        response.envelope,
        stage="pay_response",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    readback, readback_observation, counters = await _read_back(
        reader, order_uuid=target, identity=identity, stage="paid_readback"
    )
    state, proof = classify_order(readback) if readback is not None else (ORDER_MALFORMED, PAYMENT_PROOF_NONE)
    evidence = {
        "pay_response": pay_observation.as_safe_dict(),
        "paid_readback": readback_observation.as_safe_dict() if readback_observation else None,
        "payment_proof": proof,
    }

    if state != ORDER_PAID:
        # The POST answered 2xx but the order does not read as paid. That is an
        # unknown, not a failure: the refund path stays open precisely because
        # the artifact may be unreadable while the payment is real.
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAY_UNKNOWN,
            reason_code=REASON_RECONCILE_UNRESOLVED,
            evidence=evidence,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_RECONCILE_UNRESOLVED],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=ledger.as_safe_dict(),
            observations=[pay_observation.as_safe_dict()],
            template_counters=counters or plan.counters,
            order_state=state,
            payment_proof=proof,
        )

    ledger = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_PAID,
        verified_field="pay_verified_at",
        evidence=evidence,
    )
    return StageReport(
        stage=STAGE_PAY,
        outcome=OUTCOME_PROVEN,
        external_mutation_attempted=True,
        ledger=ledger.as_safe_dict(),
        observations=[
            pay_observation.as_safe_dict(),
            *([readback_observation.as_safe_dict()] if readback_observation else []),
        ],
        template_counters=counters or plan.counters,
        order_state=state,
        payment_proof=proof,
    )


# ---------------------------------------------------------------------------
# Stage: refund
# ---------------------------------------------------------------------------


async def run_refund(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    mutator: VoucherMutator,
    *,
    identity: RuntimeIdentity,
    enabled: bool,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
) -> StageReport:
    """Refund the ONE paid order, once.

    Deliberately independent of whether the artifact investigation succeeded. An
    unreadable voucher is a research disappointment; an un-refunded real payment
    is a finance problem. Cleanup wins.
    """
    authorisation = await authorise(
        reader,
        identity=identity,
        enabled=enabled,
        stage=STAGE_REFUND,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
    )
    snapshot = await ledger_module.load(session_maker)
    if not authorisation.granted:
        return _refusal(STAGE_REFUND, authorisation.reasons, snapshot.as_safe_dict())
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_REFUND, [REASON_LEDGER_STATE_INVALID], snapshot.as_safe_dict())

    plan = authorisation.plan
    target = snapshot.target_order_uuid
    claim = await ledger_module.claim_refund(
        session_maker,
        plan_digest=plan.digest,
        template_snapshot_digest=plan.template_snapshot_digest,
    )
    if not claim.granted:
        report = _refusal(STAGE_REFUND, [claim.reason], (await ledger_module.load(session_maker)).as_safe_dict())
        report.reconciliation_required = claim.status in ledger_module.UNRESOLVED_STATUSES
        return report

    try:
        response = await mutator.refund_voucher_order(order_uuid=target)
    except EasyWeekVoucherMutationUnknown:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUND_UNKNOWN,
            reason_code=REASON_MUTATION_UNKNOWN,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=ledger.as_safe_dict(),
            template_counters=plan.counters,
        )
    except EasyWeekError:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUND_REJECTED,
            reason_code=REASON_MUTATION_REJECTED,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_ROLLBACK_UNPROVEN,
            reasons=[REASON_MUTATION_REJECTED, REASON_ROLLBACK_UNPROVEN],
            external_mutation_attempted=True,
            ledger=ledger.as_safe_dict(),
            template_counters=plan.counters,
        )

    refund_observation = observe_artifact(
        response.envelope,
        stage="refund_response",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    readback, readback_observation, counters = await _read_back(
        reader, order_uuid=target, identity=identity, stage="refunded_readback"
    )
    state, _ = classify_order(readback) if readback is not None else (ORDER_MALFORMED, PAYMENT_PROOF_NONE)
    evidence = {
        "refund_response": refund_observation.as_safe_dict(),
        "refunded_readback": readback_observation.as_safe_dict() if readback_observation else None,
    }

    if state != ORDER_REFUNDED:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUND_UNKNOWN,
            reason_code=REASON_ROLLBACK_UNPROVEN,
            evidence=evidence,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_ROLLBACK_UNPROVEN,
            reasons=[REASON_ROLLBACK_UNPROVEN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=ledger.as_safe_dict(),
            observations=[refund_observation.as_safe_dict()],
            template_counters=counters,
            order_state=state,
        )

    ledger = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_REFUNDED,
        verified_field="refund_verified_at",
        evidence=evidence,
    )
    return StageReport(
        stage=STAGE_REFUND,
        outcome=OUTCOME_PROVEN,
        external_mutation_attempted=True,
        ledger=ledger.as_safe_dict(),
        observations=[
            refund_observation.as_safe_dict(),
            *([readback_observation.as_safe_dict()] if readback_observation else []),
        ],
        template_counters=counters,
        order_state=state,
        remote_rollback_proven=True,
    )


# ---------------------------------------------------------------------------
# Reconcile and status — reads only, safe to repeat
# ---------------------------------------------------------------------------


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
) -> StageReport:
    """Resolve whatever is unresolved, by READING. Never sends a mutation.

    Safe to run as often as an operator likes: every path here is a GET plus a
    normalised write of what that GET showed.
    """
    snapshot = await ledger_module.load(session_maker)
    if not snapshot.exists or snapshot.status is None:
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_REFUSED,
            reasons=[REASON_LEDGER_STATE_INVALID],
            ledger=snapshot.as_safe_dict(),
        )

    try:
        if snapshot.status in {ledger_module.STATUS_CREATE_CLAIMED, ledger_module.STATUS_CREATE_UNKNOWN}:
            return await _reconcile_create(session_maker, reader, identity=identity, snapshot=snapshot)
        if snapshot.target_order_uuid is None:
            return StageReport(
                stage="reconcile",
                outcome=OUTCOME_REFUSED,
                reasons=[REASON_LEDGER_STATE_INVALID],
                ledger=snapshot.as_safe_dict(),
            )
        return await _reconcile_order(session_maker, reader, identity=identity, snapshot=snapshot)
    except EasyWeekError as exc:
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[CANARY_API_UNCERTAIN if exc.retryable else CANARY_API_UNAVAILABLE],
            reconciliation_required=True,
            ledger=snapshot.as_safe_dict(),
        )


async def _reconcile_create(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
    snapshot: ledger_module.LedgerSnapshot,
) -> StageReport:
    assert snapshot.reconciliation_marker is not None
    assert snapshot.create_window_start is not None and snapshot.create_window_end is not None

    match = await find_marker_order(
        reader,
        identity=identity,
        marker=snapshot.reconciliation_marker,
        window_start=snapshot.create_window_start,
        window_end=snapshot.create_window_end,
    )

    if match.count > 1:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_AMBIGUOUS,
            reason_code=REASON_RECONCILE_AMBIGUOUS,
            evidence={"create_reconcile_matches": match.count},
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_AMBIGUOUS,
            reasons=[REASON_RECONCILE_AMBIGUOUS],
            manual_cleanup_required=True,
            ledger=ledger.as_safe_dict(),
        )

    if match.count == 0 or match.order_uuid is None:
        # Zero is UNRESOLVED, never "it was not created". A complete walk that
        # found nothing is still a walk of a system that may not have caught up.
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_RECONCILE_UNRESOLVED],
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=snapshot.as_safe_dict(),
            observations=[{"create_reconcile_walk_complete": match.complete, "matches": match.count}],
        )

    readback, observation, counters = await _read_back(
        reader, order_uuid=match.order_uuid, identity=identity, stage="create_reconcile_readback"
    )
    ledger = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        target_order_uuid=match.order_uuid,
        verified_field="create_verified_at",
        evidence={"create_reconcile_readback": observation.as_safe_dict() if observation else None},
    )
    state, _ = classify_order(readback) if readback is not None else (ORDER_MALFORMED, PAYMENT_PROOF_NONE)
    return StageReport(
        stage="reconcile",
        outcome=OUTCOME_PROVEN,
        ledger=ledger.as_safe_dict(),
        observations=[observation.as_safe_dict()] if observation else [],
        template_counters=counters,
        order_state=state,
        manual_cleanup_required=state == ORDER_OPEN,
    )


async def _reconcile_order(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
    snapshot: ledger_module.LedgerSnapshot,
) -> StageReport:
    """Resolve a pay or refund by reading the one exact order the ledger names."""
    assert snapshot.target_order_uuid is not None
    readback, observation, counters = await _read_back(
        reader, order_uuid=snapshot.target_order_uuid, identity=identity, stage="reconcile_readback"
    )
    if readback is None:
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_RECONCILE_UNRESOLVED],
            reconciliation_required=True,
            ledger=snapshot.as_safe_dict(),
        )

    state, proof = classify_order(readback)
    evidence = {
        "reconcile_readback": observation.as_safe_dict() if observation else None,
        "payment_proof": proof,
    }

    if state == ORDER_REFUNDED:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUNDED,
            verified_field="refund_verified_at" if snapshot.stage_timestamps.get("refund_attempted_at") else None,
            evidence=evidence,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_PROVEN,
            ledger=ledger.as_safe_dict(),
            observations=[observation.as_safe_dict()] if observation else [],
            template_counters=counters,
            order_state=state,
            remote_rollback_proven=True,
        )

    if state == ORDER_CANCELLED:
        # Somebody closed the draft in the dashboard. That is OBSERVED, never
        # attributed to this tool: it did not perform the rollback.
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_MANUALLY_CLEANED,
            evidence=evidence,
            manual_cleanup_observed=True,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_PROVEN,
            ledger=ledger.as_safe_dict(),
            observations=[observation.as_safe_dict()] if observation else [],
            template_counters=counters,
            order_state=state,
            remote_rollback_proven=False,
        )

    if state == ORDER_PAID:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAID,
            verified_field="pay_verified_at" if snapshot.stage_timestamps.get("pay_attempted_at") else None,
            evidence=evidence,
        )
        outcome = (
            OUTCOME_PROVEN if snapshot.status != ledger_module.STATUS_REFUND_UNKNOWN else OUTCOME_ROLLBACK_UNPROVEN
        )
        return StageReport(
            stage="reconcile",
            outcome=outcome,
            reasons=[REASON_ROLLBACK_UNPROVEN] if outcome == OUTCOME_ROLLBACK_UNPROVEN else [],
            ledger=ledger.as_safe_dict(),
            observations=[observation.as_safe_dict()] if observation else [],
            template_counters=counters,
            order_state=state,
            payment_proof=proof,
        )

    if state == ORDER_OPEN:
        ledger = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATED,
            evidence=evidence,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_MANUAL_CLEANUP,
            reasons=[REASON_MANUAL_CLEANUP_REQUIRED],
            manual_cleanup_required=True,
            ledger=ledger.as_safe_dict(),
            observations=[observation.as_safe_dict()] if observation else [],
            template_counters=counters,
            order_state=state,
        )

    return StageReport(
        stage="reconcile",
        outcome=OUTCOME_UNKNOWN_MUTATION,
        reasons=[REASON_ORDER_SHAPE_UNPROVEN],
        reconciliation_required=True,
        ledger=snapshot.as_safe_dict(),
        observations=[observation.as_safe_dict()] if observation else [],
        order_state=state,
    )


async def run_status(session_maker: async_sessionmaker[AsyncSession]) -> StageReport:
    """Database only. No network, no mutation, safe at any moment."""
    snapshot = await ledger_module.load(session_maker)
    manual_cleanup = snapshot.status in {
        ledger_module.STATUS_CREATED,
        ledger_module.STATUS_CREATE_UNKNOWN,
        ledger_module.STATUS_CREATE_CLAIMED,
        ledger_module.STATUS_AMBIGUOUS,
    }
    return StageReport(
        stage="status",
        outcome=OUTCOME_PROVEN,
        reconciliation_required=snapshot.status in ledger_module.UNRESOLVED_STATUSES,
        manual_cleanup_required=bool(manual_cleanup),
        ledger=snapshot.as_safe_dict(),
        remote_rollback_proven=snapshot.status == ledger_module.STATUS_REFUNDED,
    )
