"""Stage orchestration and unknown-result reconciliation for the canary (§35).

Each public entry point here is ONE operator stage. There is deliberately no
function that runs create, pay and refund in sequence: the stop between them is
the control, and a human deciding "yes, go on" after seeing what the last stage
actually did is the whole safety model.

Every mutation stage follows the same shape:

1. rebuild THIS STAGE's plan live from GETs and refuse unless it still
   authorises this exact stage (digest, phrase, freshness, ledger state, no
   configuration drift);
2. claim in PostgreSQL and COMMIT the claim;
3. send at most one request;
4. record the outcome as a compare-and-set, then verify it by reading.

A create is not "proven" because a 2xx came back
------------------------------------------------
A 2xx with an order UUID is a claim, not a proof. Before the ledger says
``created`` the order has to be READ BACK and be the order we meant: canonical
UUID, our marker, our customer, still open, with whatever voucher facts it
carries recorded rather than demanded — and the frozen template configuration
still unchanged. Anything short of that is ``create_unknown`` with the candidate
UUID kept for a later read, never a green result.

An unknown outcome is never retried. It is resolved by looking: the create by a
bounded, complete, marker-scoped walk of this customer's orders in this branch,
the pay and the refund by reading the one exact order the ledger names — and
every one of those readings is written monotonically, so a reconciliation can
never hand the same POST back to be claimed again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.artifact import ArtifactObservation, observe_artifact
from altegio_bot.easyweek_voucher_canary.orders import (
    ORDER_CANCELLED,
    ORDER_MALFORMED,
    ORDER_OPEN,
    ORDER_PAID,
    ORDER_REFUNDED,
    PAYMENT_PROOF_NONE,
    classify_order,
    find_marker_orders,
    order_object,
)
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_API_UNAVAILABLE,
    CANARY_API_UNCERTAIN,
    CREATE_WINDOW_AFTER,
    CREATE_WINDOW_BEFORE,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
    CanaryReader,
    RuntimeIdentity,
    StagePlan,
    build_stage_plan,
    canonical_order_uuid,
    immutable_template_digest,
    template_counters,
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
REASON_LEDGER_STATE_CONFLICT: Final = "canary_ledger_state_conflict"
REASON_ORDER_SHAPE_UNPROVEN: Final = "canary_order_shape_unproven"
REASON_ORDER_UUID_UNCANONICAL: Final = "canary_order_uuid_uncanonical"
REASON_ORDER_MARKER_UNPROVEN: Final = "canary_order_marker_unproven"
REASON_ORDER_CUSTOMER_UNPROVEN: Final = "canary_order_customer_unproven"
REASON_ORDER_NOT_OPEN: Final = "canary_order_not_open"
REASON_ORDER_READBACK_FAILED: Final = "canary_order_readback_failed"
REASON_TEMPLATE_READBACK_FAILED: Final = "canary_template_readback_failed"
REASON_TEMPLATE_CONFIG_DRIFT: Final = "canary_template_configuration_drift"
REASON_MANUAL_CLEANUP_REQUIRED: Final = "canary_manual_cleanup_required"
REASON_ROLLBACK_UNPROVEN: Final = "canary_rollback_unproven"


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

    async def pay_voucher_order(self, *, order_uuid: str, account_uuid: str) -> VoucherMutationResponse: ...

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
# Reading one exact order back, strictly
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderVerification:
    """What an exact read of one candidate order actually established."""

    proven: bool
    reasons: tuple[str, ...]
    order_uuid: str | None
    order_state: str | None
    payment_proof: str
    observation: ArtifactObservation | None
    counters: dict[str, int] | None

    @property
    def safe_observation(self) -> dict[str, Any] | None:
        return self.observation.as_safe_dict() if self.observation is not None else None


async def verify_created_order(
    reader: CanaryReader,
    *,
    candidate_uuid: object,
    identity: RuntimeIdentity,
    marker: str,
    expected_config_digest: str,
    stage: str,
) -> OrderVerification:
    """Prove that a candidate order really is the one this canary created.

    A 2xx carrying an order UUID proves that the server answered, not that it
    answered about our order. Everything below has to hold before the ledger is
    allowed to say ``created``; anything short of it leaves the canary unresolved
    with the candidate kept for a later read.

    The one deliberate non-requirement is the voucher shape. That is what the
    canary is here to learn, so an unfamiliar voucher body is recorded as an
    observation and never treated as "this order is somebody else's".
    """
    reasons: list[str] = []
    canonical = canonical_order_uuid(candidate_uuid)
    if canonical is None:
        return OrderVerification(
            proven=False,
            reasons=(REASON_ORDER_UUID_UNCANONICAL,),
            order_uuid=None,
            order_state=None,
            payment_proof=PAYMENT_PROOF_NONE,
            observation=None,
            counters=None,
        )

    try:
        payload = await reader.get_order(canonical)
    except EasyWeekError:
        return OrderVerification(
            proven=False,
            reasons=(REASON_ORDER_READBACK_FAILED,),
            order_uuid=canonical,
            order_state=None,
            payment_proof=PAYMENT_PROOF_NONE,
            observation=None,
            counters=None,
        )

    order = order_object(payload) or {}
    state, payment_proof = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage=stage,
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )

    if state == ORDER_MALFORMED:
        reasons.append(REASON_ORDER_SHAPE_UNPROVEN)
    if order.get("comment") != marker:
        reasons.append(REASON_ORDER_MARKER_UNPROVEN)
    if not observation.order_customer_binding_proven:
        reasons.append(REASON_ORDER_CUSTOMER_UNPROVEN)
    if state != ORDER_OPEN:
        reasons.append(REASON_ORDER_NOT_OPEN)

    counters: dict[str, int] | None = None
    try:
        template = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except EasyWeekError:
        reasons.append(REASON_TEMPLATE_READBACK_FAILED)
    else:
        counters = template_counters(template)
        if immutable_template_digest(template) != expected_config_digest:
            reasons.append(REASON_TEMPLATE_CONFIG_DRIFT)

    unique = tuple(dict.fromkeys(reasons))
    return OrderVerification(
        proven=not unique,
        reasons=unique,
        order_uuid=canonical,
        order_state=state,
        payment_proof=payment_proof,
        observation=observation,
        counters=counters,
    )


async def _read_order_and_counters(
    reader: CanaryReader,
    *,
    order_uuid: str,
    identity: RuntimeIdentity,
    stage: str,
) -> tuple[dict[str, Any] | None, ArtifactObservation | None, dict[str, int] | None]:
    """Read the exact order and the template again, projecting safe facts only."""
    try:
        payload = await reader.get_order(order_uuid)
    except EasyWeekError:
        return None, None, None
    observation = observe_artifact(
        payload,
        stage=stage,
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    try:
        template = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except EasyWeekError:
        return payload, observation, None
    return payload, observation, template_counters(template)


# ---------------------------------------------------------------------------
# Shared stage preamble
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Authorisation:
    """A freshly recomputed stage plan that still authorises its own stage."""

    plan: StagePlan
    reasons: tuple[str, ...]

    @property
    def granted(self) -> bool:
        return not self.reasons


async def authorise(
    reader: CanaryReader,
    *,
    stage: str,
    identity: RuntimeIdentity,
    enabled: bool,
    ledger: ledger_module.LedgerSnapshot,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    now: datetime | None = None,
) -> Authorisation:
    """Rebuild THIS stage's plan from live GETs and check it still authorises it.

    Runs before every claim, every time. This is the drift check: a template
    whose configuration moved, a staffer who left, an account that vanished, a
    ledger that is not where this stage requires it to be, or a target order that
    is no longer in the expected state all change the snapshot, so they all
    change the digest, so they all stop the stage before anything is claimed.
    """
    plan = await build_stage_plan(
        reader,
        stage=stage,
        identity=identity,
        enabled=enabled,
        ledger_status=ledger.status,
        target_order_uuid=ledger.target_order_uuid,
        create_window_start=ledger.create_window_start,
        create_window_end=ledger.create_window_end,
        now=now,
    )
    reasons = verify_plan_authorisation(
        plan,
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
    snapshot = await ledger_module.load(session_maker)
    authorisation = await authorise(
        reader,
        stage=STAGE_CREATE,
        identity=identity,
        enabled=enabled,
        ledger=snapshot,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
    )
    if not authorisation.granted:
        return _refusal(STAGE_CREATE, authorisation.reasons, snapshot.as_safe_dict())

    plan = authorisation.plan
    now = utcnow()
    claim = await ledger_module.claim_create(
        session_maker,
        create_plan_digest=plan.digest,
        template_config_digest=plan.immutable_template_digest,
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
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_UNKNOWN,
            expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
            reason_code=REASON_MUTATION_UNKNOWN,
            stage_counters={"create_plan": plan.counters_observed},
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=result.snapshot.as_safe_dict(),
            template_counters=plan.counters_observed,
        )
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_REJECTED,
            expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
            reason_code=REASON_MUTATION_REJECTED,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[REASON_MUTATION_REJECTED],
            external_mutation_attempted=True,
            ledger=result.snapshot.as_safe_dict(),
            template_counters=plan.counters_observed,
        )

    response_observation = observe_artifact(
        response.envelope,
        stage="create_response",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    order = order_object(response.envelope) or {}
    verification = await verify_created_order(
        reader,
        candidate_uuid=order.get("uuid"),
        identity=identity,
        marker=plan.marker,
        expected_config_digest=plan.immutable_template_digest,
        stage="create_readback",
    )
    observations = [response_observation.as_safe_dict()]
    if verification.safe_observation is not None:
        observations.append(verification.safe_observation)
    evidence = {
        "create_response": response_observation.as_safe_dict(),
        "create_readback": verification.safe_observation,
    }
    counters = {"create_plan": plan.counters_observed, "create_after": verification.counters}

    if not verification.proven:
        # A 2xx we cannot tie to OUR order, in the state we expect, is not a
        # success we can record. The candidate UUID is kept only because a later
        # read needs something to read.
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_UNKNOWN,
            expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
            reason_code=REASON_ORDER_SHAPE_UNPROVEN,
            target_order_uuid=verification.order_uuid,
            evidence=evidence,
            stage_counters=counters,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=list(verification.reasons),
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=observations,
            template_counters=verification.counters or plan.counters_observed,
            order_state=verification.order_state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
        target_order_uuid=verification.order_uuid,
        verified_field="create_verified_at",
        evidence=evidence,
        stage_counters=counters,
    )
    if not result.applied:
        return _refusal(STAGE_CREATE, [REASON_LEDGER_STATE_CONFLICT], result.snapshot.as_safe_dict())
    return StageReport(
        stage=STAGE_CREATE,
        outcome=OUTCOME_PROVEN,
        external_mutation_attempted=True,
        manual_cleanup_required=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=observations,
        template_counters=verification.counters or plan.counters_observed,
        order_state=verification.order_state,
    )


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
    """Pay the ONE created order, once, on the approved account.

    The request carries no amount: the sum is already fixed by the exact open
    order and its one voucher line.
    """
    snapshot = await ledger_module.load(session_maker)
    authorisation = await authorise(
        reader,
        stage=STAGE_PAY,
        identity=identity,
        enabled=enabled,
        ledger=snapshot,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
    )
    if not authorisation.granted:
        report = _refusal(STAGE_PAY, authorisation.reasons, snapshot.as_safe_dict())
        report.reconciliation_required = snapshot.status in ledger_module.UNRESOLVED_STATUSES
        return report
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_PAY, [REASON_LEDGER_STATE_INVALID], snapshot.as_safe_dict())

    plan = authorisation.plan
    target = snapshot.target_order_uuid
    claim = await ledger_module.claim_pay(
        session_maker,
        pay_plan_digest=plan.digest,
        template_config_digest=plan.immutable_template_digest,
    )
    if not claim.granted:
        report = _refusal(STAGE_PAY, [claim.reason], (await ledger_module.load(session_maker)).as_safe_dict())
        report.reconciliation_required = claim.status in ledger_module.UNRESOLVED_STATUSES
        return report

    try:
        response = await mutator.pay_voucher_order(order_uuid=target, account_uuid=identity.account_uuid)
    except EasyWeekVoucherMutationUnknown:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAY_UNKNOWN,
            expected_statuses=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
            reason_code=REASON_MUTATION_UNKNOWN,
            stage_counters={"pay_plan": plan.counters_observed},
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=result.snapshot.as_safe_dict(),
            template_counters=plan.counters_observed,
        )
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAY_REJECTED,
            expected_statuses=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
            reason_code=REASON_MUTATION_REJECTED,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[REASON_MUTATION_REJECTED],
            external_mutation_attempted=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            template_counters=plan.counters_observed,
        )

    pay_observation = observe_artifact(
        response.envelope,
        stage="pay_response",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    readback, readback_observation, counters = await _read_order_and_counters(
        reader, order_uuid=target, identity=identity, stage="paid_readback"
    )
    state, proof = classify_order(readback) if readback is not None else (ORDER_MALFORMED, PAYMENT_PROOF_NONE)
    evidence = {
        "pay_response": pay_observation.as_safe_dict(),
        "paid_readback": readback_observation.as_safe_dict() if readback_observation else None,
        "payment_proof": proof,
    }
    stage_counters = {"pay_plan": plan.counters_observed, "pay_after": counters}

    if state != ORDER_PAID:
        # The POST answered 2xx but the order does not read as paid. That is an
        # unknown, not a failure: the refund path stays open precisely because
        # the artifact may be unreadable while the payment is real.
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_PAY_UNKNOWN,
            expected_statuses=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
            reason_code=REASON_RECONCILE_UNRESOLVED,
            evidence=evidence,
            stage_counters=stage_counters,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_RECONCILE_UNRESOLVED],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[pay_observation.as_safe_dict()],
            template_counters=counters or plan.counters_observed,
            order_state=state,
            payment_proof=proof,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_PAID,
        expected_statuses=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
        verified_field="pay_verified_at",
        evidence=evidence,
        stage_counters=stage_counters,
    )
    if not result.applied:
        return _refusal(STAGE_PAY, [REASON_LEDGER_STATE_CONFLICT], result.snapshot.as_safe_dict())
    return StageReport(
        stage=STAGE_PAY,
        outcome=OUTCOME_PROVEN,
        external_mutation_attempted=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[
            pay_observation.as_safe_dict(),
            *([readback_observation.as_safe_dict()] if readback_observation else []),
        ],
        template_counters=counters or plan.counters_observed,
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

    Deliberately independent of whether the artifact investigation succeeded, and
    of whether the voucher counters moved. An unreadable voucher is a research
    disappointment; an un-refunded real payment is a finance problem.
    """
    snapshot = await ledger_module.load(session_maker)
    authorisation = await authorise(
        reader,
        stage=STAGE_REFUND,
        identity=identity,
        enabled=enabled,
        ledger=snapshot,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
    )
    if not authorisation.granted:
        return _refusal(STAGE_REFUND, authorisation.reasons, snapshot.as_safe_dict())
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_REFUND, [REASON_LEDGER_STATE_INVALID], snapshot.as_safe_dict())

    plan = authorisation.plan
    target = snapshot.target_order_uuid
    claim = await ledger_module.claim_refund(
        session_maker,
        refund_plan_digest=plan.digest,
        template_config_digest=plan.immutable_template_digest,
    )
    if not claim.granted:
        report = _refusal(STAGE_REFUND, [claim.reason], (await ledger_module.load(session_maker)).as_safe_dict())
        report.reconciliation_required = claim.status in ledger_module.UNRESOLVED_STATUSES
        return report

    try:
        response = await mutator.refund_voucher_order(order_uuid=target)
    except EasyWeekVoucherMutationUnknown:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUND_UNKNOWN,
            expected_statuses=frozenset({ledger_module.STATUS_REFUND_CLAIMED}),
            reason_code=REASON_MUTATION_UNKNOWN,
            stage_counters={"refund_plan": plan.counters_observed},
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=[REASON_MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=result.snapshot.as_safe_dict(),
            template_counters=plan.counters_observed,
        )
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUND_REJECTED,
            expected_statuses=frozenset({ledger_module.STATUS_REFUND_CLAIMED}),
            reason_code=REASON_MUTATION_REJECTED,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_ROLLBACK_UNPROVEN,
            reasons=[REASON_MUTATION_REJECTED, REASON_ROLLBACK_UNPROVEN],
            external_mutation_attempted=True,
            ledger=result.snapshot.as_safe_dict(),
            template_counters=plan.counters_observed,
        )

    refund_observation = observe_artifact(
        response.envelope,
        stage="refund_response",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    readback, readback_observation, counters = await _read_order_and_counters(
        reader, order_uuid=target, identity=identity, stage="refunded_readback"
    )
    state, _ = classify_order(readback) if readback is not None else (ORDER_MALFORMED, PAYMENT_PROOF_NONE)
    evidence = {
        "refund_response": refund_observation.as_safe_dict(),
        "refunded_readback": readback_observation.as_safe_dict() if readback_observation else None,
    }
    stage_counters = {"refund_plan": plan.counters_observed, "refund_after": counters}

    if state != ORDER_REFUNDED:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_REFUND_UNKNOWN,
            expected_statuses=frozenset({ledger_module.STATUS_REFUND_CLAIMED}),
            reason_code=REASON_ROLLBACK_UNPROVEN,
            evidence=evidence,
            stage_counters=stage_counters,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_ROLLBACK_UNPROVEN,
            reasons=[REASON_ROLLBACK_UNPROVEN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[refund_observation.as_safe_dict()],
            template_counters=counters,
            order_state=state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_REFUNDED,
        expected_statuses=frozenset({ledger_module.STATUS_REFUND_CLAIMED}),
        verified_field="refund_verified_at",
        evidence=evidence,
        stage_counters=stage_counters,
    )
    if not result.applied:
        return _refusal(STAGE_REFUND, [REASON_LEDGER_STATE_CONFLICT], result.snapshot.as_safe_dict())
    return StageReport(
        stage=STAGE_REFUND,
        outcome=OUTCOME_PROVEN,
        external_mutation_attempted=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[
            refund_observation.as_safe_dict(),
            *([readback_observation.as_safe_dict()] if readback_observation else []),
        ],
        template_counters=counters,
        order_state=state,
        remote_rollback_proven=True,
    )


# ---------------------------------------------------------------------------
# Reconcile and status — reads only, safe to repeat, monotonic
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Transition:
    """What one remote reading is allowed to change the ledger to."""

    status: str | None
    expected_from: frozenset[str] = frozenset()
    verified_field: str | None = None
    manual_cleanup_observed: bool = False


# The monotonic reconciliation table. `None` means "observe and say so, but do
# not touch the ledger" — which is what stops a read taken while a POST is still
# in flight from handing that POST back to be claimed again.
_RECONCILE_TRANSITIONS: Final[dict[tuple[str, str], _Transition]] = {
    # A refunded order is the most advanced state there is; any reading of it
    # may be recorded, and only a claimed refund gets a verification stamp.
    (ledger_module.STATUS_REFUND_CLAIMED, ORDER_REFUNDED): _Transition(
        ledger_module.STATUS_REFUNDED,
        frozenset({ledger_module.STATUS_REFUND_CLAIMED}),
        "refund_verified_at",
    ),
    (ledger_module.STATUS_REFUND_UNKNOWN, ORDER_REFUNDED): _Transition(
        ledger_module.STATUS_REFUNDED,
        frozenset({ledger_module.STATUS_REFUND_UNKNOWN}),
        "refund_verified_at",
    ),
    (ledger_module.STATUS_PAID, ORDER_REFUNDED): _Transition(
        ledger_module.STATUS_REFUNDED, frozenset({ledger_module.STATUS_PAID})
    ),
    (ledger_module.STATUS_CREATED, ORDER_REFUNDED): _Transition(
        ledger_module.STATUS_REFUNDED, frozenset({ledger_module.STATUS_CREATED})
    ),
    # A payment is recordable from a claimed or unknown pay, and from `created`
    # when somebody settled the draft in the dashboard. Never from a refund
    # stage: that would walk the state back to somewhere refund is claimable.
    (ledger_module.STATUS_PAY_CLAIMED, ORDER_PAID): _Transition(
        ledger_module.STATUS_PAID, frozenset({ledger_module.STATUS_PAY_CLAIMED}), "pay_verified_at"
    ),
    (ledger_module.STATUS_PAY_UNKNOWN, ORDER_PAID): _Transition(
        ledger_module.STATUS_PAID, frozenset({ledger_module.STATUS_PAY_UNKNOWN}), "pay_verified_at"
    ),
    (ledger_module.STATUS_CREATED, ORDER_PAID): _Transition(
        ledger_module.STATUS_PAID, frozenset({ledger_module.STATUS_CREATED})
    ),
    # A pay whose POST is still in flight reads the order as open. That is not
    # evidence the payment failed — it becomes `pay_unknown`, never `created`.
    (ledger_module.STATUS_PAY_CLAIMED, ORDER_OPEN): _Transition(
        status=ledger_module.STATUS_PAY_UNKNOWN,
        expected_from=frozenset({ledger_module.STATUS_PAY_CLAIMED}),
    ),
    # Somebody closed the draft by hand. Observed, never attributed to us.
    (ledger_module.STATUS_CREATED, ORDER_CANCELLED): _Transition(
        ledger_module.STATUS_MANUALLY_CLEANED,
        frozenset({ledger_module.STATUS_CREATED}),
        manual_cleanup_observed=True,
    ),
    (ledger_module.STATUS_PAY_REJECTED, ORDER_CANCELLED): _Transition(
        ledger_module.STATUS_MANUALLY_CLEANED,
        frozenset({ledger_module.STATUS_PAY_REJECTED}),
        manual_cleanup_observed=True,
    ),
}


def reconcile_transition(current_status: str, remote_state: str) -> _Transition:
    """The ledger change one remote reading justifies, if any.

    Everything absent from the table is deliberately a no-op: a refund stage
    reading `paid`, a pay stage reading `open` after it already went unknown, a
    malformed body. Those are reported and left alone, because writing them
    would be a regression and a regression is a second POST waiting to happen.
    """
    return _RECONCILE_TRANSITIONS.get((current_status, remote_state), _Transition(status=None))


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
) -> StageReport:
    """Resolve whatever is unresolved, by READING. Never sends a mutation.

    Safe to run as often as an operator likes: every path here is a GET plus a
    monotonic write of what that GET showed.
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

    candidate = snapshot.target_order_uuid
    if candidate is None:
        match = await find_marker_orders(
            reader,
            location_uuid=KARLSRUHE_LOCATION_UUID,
            customer_uuid=identity.customer_uuid,
            staffer_uuid=identity.staffer_uuid,
            marker=snapshot.reconciliation_marker,
            window_start=snapshot.create_window_start,
            window_end=snapshot.create_window_end,
        )
        if match.count > 1:
            result = await ledger_module.record_outcome(
                session_maker,
                status=ledger_module.STATUS_AMBIGUOUS,
                expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED, ledger_module.STATUS_CREATE_UNKNOWN}),
                reason_code=REASON_RECONCILE_AMBIGUOUS,
                evidence={"create_reconcile_matches": match.count},
            )
            return StageReport(
                stage="reconcile",
                outcome=OUTCOME_AMBIGUOUS,
                reasons=[REASON_RECONCILE_AMBIGUOUS],
                manual_cleanup_required=True,
                ledger=result.snapshot.as_safe_dict(),
            )
        if not match.resolved:
            # Zero matches, or a walk that could not prove it saw everything.
            # UNRESOLVED, never "it was not created".
            return StageReport(
                stage="reconcile",
                outcome=OUTCOME_UNKNOWN_MUTATION,
                reasons=[REASON_RECONCILE_UNRESOLVED],
                reconciliation_required=True,
                manual_cleanup_required=True,
                ledger=snapshot.as_safe_dict(),
                observations=[{"create_reconcile_walk_complete": match.complete, "matches": match.count}],
            )
        candidate = match.order_uuid

    # Finding one candidate is not the same as proving it. Exactly the same
    # readback the create path uses decides whether the ledger may say `created`.
    verification = await verify_created_order(
        reader,
        candidate_uuid=candidate,
        identity=identity,
        marker=snapshot.reconciliation_marker,
        expected_config_digest=snapshot.template_config_digest or "",
        stage="create_reconcile_readback",
    )
    evidence = {"create_reconcile_readback": verification.safe_observation}
    expected = frozenset({ledger_module.STATUS_CREATE_CLAIMED, ledger_module.STATUS_CREATE_UNKNOWN})

    if not verification.proven:
        result = await ledger_module.record_outcome(
            session_maker,
            status=ledger_module.STATUS_CREATE_UNKNOWN,
            expected_statuses=expected,
            reason_code=REASON_ORDER_SHAPE_UNPROVEN,
            target_order_uuid=verification.order_uuid,
            evidence=evidence,
            stage_counters={"create_reconcile": verification.counters},
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN_MUTATION,
            reasons=list(verification.reasons),
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[verification.safe_observation] if verification.safe_observation else [],
            template_counters=verification.counters,
            order_state=verification.order_state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        expected_statuses=expected,
        target_order_uuid=verification.order_uuid,
        verified_field="create_verified_at",
        evidence=evidence,
        stage_counters={"create_reconcile": verification.counters},
    )
    if not result.applied:
        return _refusal("reconcile", [REASON_LEDGER_STATE_CONFLICT], result.snapshot.as_safe_dict())
    return StageReport(
        stage="reconcile",
        outcome=OUTCOME_PROVEN,
        ledger=result.snapshot.as_safe_dict(),
        observations=[verification.safe_observation] if verification.safe_observation else [],
        template_counters=verification.counters,
        order_state=verification.order_state,
        manual_cleanup_required=verification.order_state == ORDER_OPEN,
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
    assert snapshot.status is not None
    readback, observation, counters = await _read_order_and_counters(
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
    transition = reconcile_transition(snapshot.status, state)
    evidence = {
        "reconcile_readback": observation.as_safe_dict() if observation else None,
        "payment_proof": proof,
    }

    current = snapshot
    if transition.status is not None:
        result = await ledger_module.record_outcome(
            session_maker,
            status=transition.status,
            expected_statuses=transition.expected_from,
            verified_field=transition.verified_field,
            evidence=evidence,
            stage_counters={"reconcile": counters},
            manual_cleanup_observed=transition.manual_cleanup_observed,
        )
        # A refused write means somebody else moved first. Their state wins.
        current = result.snapshot
    status_now = current.status or snapshot.status

    return StageReport(
        stage="reconcile",
        outcome=_reconcile_outcome(status_now, state),
        reasons=_reconcile_reasons(status_now, state),
        reconciliation_required=status_now in ledger_module.UNRESOLVED_STATUSES,
        manual_cleanup_required=state == ORDER_OPEN and status_now not in ledger_module.UNRESOLVED_STATUSES,
        ledger=current.as_safe_dict(),
        observations=[observation.as_safe_dict()] if observation else [],
        template_counters=counters,
        order_state=state,
        payment_proof=proof,
        remote_rollback_proven=state == ORDER_REFUNDED,
    )


def _reconcile_outcome(status: str, remote_state: str) -> str:
    if status == ledger_module.STATUS_REFUNDED or status == ledger_module.STATUS_MANUALLY_CLEANED:
        return OUTCOME_PROVEN
    if status in ledger_module.UNRESOLVED_STATUSES:
        # A refund whose effect is still unknown is not merely "unknown": the
        # money is still out, so it reports as an unproven rollback.
        if status in {ledger_module.STATUS_REFUND_CLAIMED, ledger_module.STATUS_REFUND_UNKNOWN}:
            return OUTCOME_ROLLBACK_UNPROVEN
        return OUTCOME_UNKNOWN_MUTATION
    if remote_state == ORDER_OPEN:
        return OUTCOME_MANUAL_CLEANUP
    if remote_state == ORDER_MALFORMED:
        return OUTCOME_UNKNOWN_MUTATION
    return OUTCOME_PROVEN


def _reconcile_reasons(status: str, remote_state: str) -> list[str]:
    if status in {ledger_module.STATUS_REFUND_CLAIMED, ledger_module.STATUS_REFUND_UNKNOWN}:
        return [REASON_ROLLBACK_UNPROVEN]
    if status in ledger_module.UNRESOLVED_STATUSES:
        return [REASON_RECONCILE_UNRESOLVED]
    if status == ledger_module.STATUS_REFUNDED or status == ledger_module.STATUS_MANUALLY_CLEANED:
        return []
    if remote_state == ORDER_OPEN:
        return [REASON_MANUAL_CLEANUP_REQUIRED]
    if remote_state == ORDER_MALFORMED:
        return [REASON_ORDER_SHAPE_UNPROVEN]
    return []


async def run_status(session_maker: async_sessionmaker[AsyncSession]) -> StageReport:
    """Database only. No network, no mutation, safe at any moment."""
    snapshot = await ledger_module.load(session_maker)
    manual_cleanup = snapshot.status in {
        ledger_module.STATUS_CREATED,
        ledger_module.STATUS_CREATE_UNKNOWN,
        ledger_module.STATUS_CREATE_CLAIMED,
        ledger_module.STATUS_PAY_REJECTED,
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
