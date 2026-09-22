"""The operator stages of the controlled voucher snapshot batch (§41).

Up to five people, up to five vouchers, up to five messages — and between each
pair of stages, a human who has to look at a plan and decide again. There is no
function here that runs two stages, and there cannot be: the stop between them
IS the control.

The order of every acting stage is the same, and it is not negotiable:

1. rebuild the plan live and check the operator's approval against it;
2. re-prove the whole composition, the template baseline and every order, from
   scratch — and refuse the stage outright if any of it drifted;
3. then, slot by slot in slot order: take the row lock, check the transition,
   write the claim AND the attempt, commit;
4. only then make at most ONE external request for that slot;
5. record what it turned out to be as a compare-and-set;
6. and if that outcome cannot be proven, stop. The remaining slots are not
   attempted at all.

Steps 3 and 4 in that order are the whole design. EasyWeek publishes no write
idempotency key and Meta will happily deliver twice, so a crash between the
commit and the response must read as "it may have happened" — which is the only
reading that cannot charge a card twice or message a person twice.

Step 6 is what makes a batch different from five canaries. One unknown is a
question about one person; five unknowns discovered in a row are an outage
nobody watched. The first one stops everything after it.

What this module refuses to know
--------------------------------
It never learns a phone number, a name or a voucher code for longer than one
call. A code exists in memory between one read of a paid order and one POST to
Meta; what survives is a keyed MAC of it. No report, no ledger column, no log
line and no exception here carries any of the three.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_manual_voucher.baseline import BaselineProof, prove_baseline
from altegio_bot.campaigns.easyweek_manual_voucher.identity import MANUAL_BASELINE_VERSION
from altegio_bot.campaigns.easyweek_voucher_batch import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_batch.authorisation import (
    StagePlan,
    stage_digest,
    verify_plan_authorisation,
)
from altegio_bot.campaigns.easyweek_voucher_batch.composition import (
    BatchComposition,
    prove_batch_composition,
)
from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    APPLY_FLAG_MISSING,
    ARTIFACT_UNPROVEN,
    BASELINE_DRIFT,
    BATCH_ALREADY_EXISTS,
    BATCH_HALTED,
    BATCH_NOT_FROZEN,
    BATCH_SCOPE,
    BINDING_MISMATCH,
    COMPOSITION_DRIFTED,
    DELIVERY_ALREADY_ATTEMPTED,
    FROZEN_DIGEST_MISMATCH,
    HALTED_BY_PREDECESSOR,
    IDENTITY_BINDING_MISMATCH,
    KARLSRUHE_COMPANY_ID,
    LEDGER_STATE_UNEXPECTED,
    MARKER_SEARCH_AMBIGUOUS,
    MARKER_SEARCH_INCOMPLETE,
    MARKER_SEARCH_UNRESOLVED,
    MAX_EXPOSURE_MINOR,
    MAX_RECIPIENTS,
    MUTATION_REJECTED,
    MUTATION_UNKNOWN,
    NEW_CLIENT_CAMPAIGN_CODE,
    ORDER_ALREADY_REFUNDED,
    ORDER_NOT_PAID,
    ORDER_NOT_PAYABLE,
    ORDER_UNPROVEN,
    RECONCILE_UNRESOLVED,
    REFUND_FORBIDDEN_AFTER_SEND,
    SLOT_UNKNOWN,
    SNAPSHOT_NOT_FROZEN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
    TEMPLATE_PARAMETERS_UNPROVEN,
    UNIT_PRICE_MINOR,
    UNKNOWN_STAGE,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_batch.readiness import (
    BatchPrerequisites,
    prove_prerequisites,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
    VoucherBindingKeyError,
    voucher_code_mac,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import DELIVERY_REJECTED, DeliveryOutcome
from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_voucher_canary.artifact import observe_artifact
from altegio_bot.easyweek_voucher_canary.orders import (
    ORDER_CANCELLED,
    ORDER_OPEN,
    ORDER_PAID,
    ORDER_REFUNDED,
    canonical_uuid,
    classify_order,
    find_marker_orders,
    order_object,
    payable_order_reasons,
)
from altegio_bot.easyweek_voucher_canary.voucher_line import prove_voucher_line
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
from altegio_bot.models.models import (
    VOUCHER_BATCH_ITEM_CREATE_CLAIMED,
    VOUCHER_BATCH_ITEM_CREATE_REJECTED,
    VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
    VOUCHER_BATCH_ITEM_CREATED,
    VOUCHER_BATCH_ITEM_MANUALLY_CLEANED,
    VOUCHER_BATCH_ITEM_PAID,
    VOUCHER_BATCH_ITEM_PAY_CLAIMED,
    VOUCHER_BATCH_ITEM_PAY_REJECTED,
    VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
    VOUCHER_BATCH_ITEM_PLANNED,
    VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_BATCH_ITEM_REFUND_CLAIMED,
    VOUCHER_BATCH_ITEM_REFUND_REJECTED,
    VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
    VOUCHER_BATCH_ITEM_REFUNDED,
    VOUCHER_BATCH_ITEM_SEND_CLAIMED,
    VOUCHER_BATCH_ITEM_SEND_REJECTED,
    VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
)
from altegio_bot.utils import utcnow

# How long after a claim a created order may have been opened. Bounded locally,
# because §35 proved the server-side date filters answer 422 for this shape.
CREATE_WINDOW = timedelta(minutes=30)

# What a crash can leave behind, per stage, and what a reconcile may look at.
#
# ``*_claimed`` is a process that died between the commit and the answer;
# ``*_unknown`` is a process that lived long enough to say so. They mean exactly
# the same thing about the world — the request may have gone out — so the
# GET-only reconcile treats them identically. Neither appears in any
# ``*_CLAIMABLE_FROM`` set, so widening reconcile here can never widen what may
# be claimed again.
CREATE_RECOVERABLE_FROM: frozenset[str] = frozenset(
    {VOUCHER_BATCH_ITEM_CREATE_CLAIMED, VOUCHER_BATCH_ITEM_CREATE_UNKNOWN}
)
PAY_RECOVERABLE_FROM: frozenset[str] = frozenset({VOUCHER_BATCH_ITEM_PAY_CLAIMED, VOUCHER_BATCH_ITEM_PAY_UNKNOWN})
REFUND_RECOVERABLE_FROM: frozenset[str] = frozenset(
    {VOUCHER_BATCH_ITEM_REFUND_CLAIMED, VOUCHER_BATCH_ITEM_REFUND_UNKNOWN}
)
# A send is different from the three above and always will be: whether Meta
# delivered a message is not a fact a POS order can answer, and a slot that was
# claimed but never recorded a provider message id has no identifier to ask
# about. It stays fail-closed for a human.
SEND_UNRESOLVED: frozenset[str] = frozenset({VOUCHER_BATCH_ITEM_SEND_CLAIMED, VOUCHER_BATCH_ITEM_SEND_UNKNOWN})

# Where an unresolved claim is parked once a reconcile has looked and still
# cannot prove what happened. Same meaning, one fact added: somebody looked.
_RECONCILED_UNKNOWN: dict[str, str] = {
    VOUCHER_BATCH_ITEM_CREATE_CLAIMED: VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
    VOUCHER_BATCH_ITEM_PAY_CLAIMED: VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
    VOUCHER_BATCH_ITEM_REFUND_CLAIMED: VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
    VOUCHER_BATCH_ITEM_SEND_CLAIMED: VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
}

# Which item states each stage may act on. A slot in any other state is simply
# not part of this stage's work; a stage with no work at all is refused.
STAGE_ITEM_SOURCE_STATUSES: dict[str, frozenset[str]] = {
    STAGE_CREATE: frozenset({VOUCHER_BATCH_ITEM_PLANNED, VOUCHER_BATCH_ITEM_CREATE_REJECTED}),
    STAGE_PAY: frozenset({VOUCHER_BATCH_ITEM_CREATED, VOUCHER_BATCH_ITEM_PAY_REJECTED}),
    STAGE_DELIVER: frozenset({VOUCHER_BATCH_ITEM_PAID}),
    STAGE_REFUND: frozenset(
        {VOUCHER_BATCH_ITEM_PAID, VOUCHER_BATCH_ITEM_PAY_UNKNOWN, VOUCHER_BATCH_ITEM_REFUND_REJECTED}
    ),
}


class VoucherMutator(Protocol):
    """The §35 three-endpoint mutation surface, and nothing wider."""

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


class VoucherSender(Protocol):
    """The one Meta call this phase may make, per slot."""

    async def send_voucher_template(
        self,
        *,
        phone_number_id: str,
        to_e164: str,
        template_name: str,
        language: str,
        params: list[str],
    ) -> DeliveryOutcome: ...


@dataclass
class SlotResult:
    """What one slot's stage turned out to be. PII-free, always."""

    slot: int
    outcome: str
    reasons: list[str] = field(default_factory=list)
    external_effect_attempted: bool = False
    external_send_attempted: bool = False
    order_state: str | None = None
    observations: list[dict[str, Any]] = field(default_factory=list)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "slot": self.slot,
            "outcome": self.outcome,
            "reasons": list(dict.fromkeys(self.reasons)),
            "external_effect_attempted": self.external_effect_attempted,
            "external_send_attempted": self.external_send_attempted,
            "order_state": self.order_state,
            "observations": list(self.observations),
        }


@dataclass
class StageReport:
    """The PII-free result of one operator stage. Safe to print, always."""

    stage: str
    outcome: str
    reasons: list[str] = field(default_factory=list)
    external_effect_attempted: bool = False
    external_send_attempted: bool = False
    reconciliation_required: bool = False
    manual_cleanup_required: bool = False
    halted: bool = False
    batch: dict[str, Any] = field(default_factory=dict)
    slots: list[SlotResult] = field(default_factory=list)
    observations: list[dict[str, Any]] = field(default_factory=list)
    baseline: dict[str, Any] | None = None
    # How many external calls this stage actually made, by kind. An operator
    # comparing this with the batch size is how "at most five" stops being a
    # promise and becomes something they can read off a report.
    external_calls: dict[str, int] = field(default_factory=dict)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_batch_stage",
            "batch_scope": BATCH_SCOPE,
            "stage": self.stage,
            "outcome": self.outcome,
            "reasons": list(dict.fromkeys(self.reasons)),
            # Named to answer the question an operator actually has: did
            # anything leave this process?
            "external_effect_attempted": self.external_effect_attempted,
            "external_send_attempted": self.external_send_attempted,
            "external_calls": dict(self.external_calls),
            "reconciliation_required": self.reconciliation_required,
            "manual_cleanup_required": self.manual_cleanup_required,
            "halted": self.halted,
            "baseline": dict(self.baseline) if self.baseline is not None else None,
            "observations": list(self.observations),
            "slots": [entry.as_safe_dict() for entry in self.slots],
            "batch": dict(self.batch),
            "recipient_basis": "operator_manual_selection",
            "first_visit_proof": "not_applicable",
            "max_recipients": MAX_RECIPIENTS,
            "voucher_unit_price_minor": UNIT_PRICE_MINOR,
            "max_exposure_minor": MAX_EXPOSURE_MINOR,
            # Repeated verbatim on every stage, success included.
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
            "ready_for_send": False,
            "raw_identifiers_omitted": True,
            "voucher_code_omitted": True,
        }


@dataclass(frozen=True)
class BatchRequest:
    """What the operator named on the command line, plus the frozen identity."""

    preview_run_id: int
    sender_code: str
    staffer_uuid: str
    payment_account_uuid: str
    company_id: int = KARLSRUHE_COMPANY_ID
    location_uuid: str = KARLSRUHE_LOCATION_UUID
    voucher_template_uuid: str = EASYWEEK_VOUCHER_TEMPLATE_UUID


def _voucher_code(payload: object) -> str | None:
    """The one issued code of this order, in memory, or ``None``.

    Read through the same §35 proof the payment gate uses, so a body this phase
    would refuse to pay for is also a body it refuses to read a code out of.
    """
    order = order_object(payload)
    if order is None:
        return None
    proof = prove_voucher_line(
        order,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=UNIT_PRICE_MINOR,
    )
    if not proof.proven:
        return None
    vouchers = order.get("vouchers")
    if isinstance(vouchers, list) and len(vouchers) == 1 and isinstance(vouchers[0], dict):
        code = vouchers[0].get("code")
        return code if isinstance(code, str) and code else None
    return None


def _identity_from_composition(
    request: BatchRequest, composition: BatchComposition
) -> ledger_module.BatchIdentity | None:
    """The identity a freeze would write, or ``None`` if the proof is incomplete."""
    if not composition.proven or composition.campaign_period_start is None or composition.campaign_period_end is None:
        return None
    items: list[ledger_module.BatchItemIdentity] = []
    for member in composition.members:
        customer = member.easyweek_customer_uuid
        if customer is None:
            return None
        items.append(
            ledger_module.BatchItemIdentity(
                slot=member.slot,
                campaign_recipient_id=member.campaign_recipient_id,
                easyweek_customer_uuid=customer,
                reconciliation_marker=member.marker(preview_run_id=composition.preview_run_id),
            )
        )
    return ledger_module.BatchIdentity(
        company_id=request.company_id,
        campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
        campaign_run_id=composition.preview_run_id,
        campaign_period_start=composition.campaign_period_start,
        campaign_period_end=composition.campaign_period_end,
        location_uuid=request.location_uuid,
        staffer_uuid=request.staffer_uuid,
        payment_account_uuid=request.payment_account_uuid,
        voucher_template_uuid=request.voucher_template_uuid,
        baseline_version=MANUAL_BASELINE_VERSION,
        frozen_digest=composition.digest(),
        items=tuple(items),
    )


def _identity_from_snapshot(
    snapshot: ledger_module.BatchSnapshot,
) -> ledger_module.BatchIdentity | None:
    """Rebuild the identity the batch was frozen with, or refuse.

    A batch missing any part of it cannot be acted on: the claim compares the
    identity field by field, and an identity assembled out of defaults would
    compare equal to something nobody approved.
    """
    required = (
        snapshot.company_id,
        snapshot.campaign_code,
        snapshot.campaign_run_id,
        snapshot.campaign_period_start,
        snapshot.campaign_period_end,
        snapshot.location_uuid,
        snapshot.staffer_uuid,
        snapshot.payment_account_uuid,
        snapshot.voucher_template_uuid,
        snapshot.baseline_version,
        snapshot.frozen_digest,
    )
    if any(value is None for value in required) or not snapshot.items:
        return None
    assert snapshot.campaign_period_start is not None
    assert snapshot.campaign_period_end is not None
    items = []
    for entry in snapshot.items:
        if entry.easyweek_customer_uuid is None:
            return None
        items.append(
            ledger_module.BatchItemIdentity(
                slot=entry.slot,
                campaign_recipient_id=entry.campaign_recipient_id,
                easyweek_customer_uuid=entry.easyweek_customer_uuid,
                reconciliation_marker=entry.reconciliation_marker,
            )
        )
    return ledger_module.BatchIdentity(
        company_id=int(snapshot.company_id or 0),
        campaign_code=str(snapshot.campaign_code),
        campaign_run_id=int(snapshot.campaign_run_id or 0),
        campaign_period_start=datetime.fromisoformat(snapshot.campaign_period_start),
        campaign_period_end=datetime.fromisoformat(snapshot.campaign_period_end),
        location_uuid=str(snapshot.location_uuid),
        staffer_uuid=str(snapshot.staffer_uuid),
        payment_account_uuid=str(snapshot.payment_account_uuid),
        voucher_template_uuid=str(snapshot.voucher_template_uuid),
        baseline_version=str(snapshot.baseline_version),
        frozen_digest=str(snapshot.frozen_digest),
        items=tuple(items),
    )


def _runtime_identity_matches(request: BatchRequest, snapshot: ledger_module.BatchSnapshot) -> bool:
    """Is the environment this process runs in the one the batch was frozen with?

    Four UUIDs decide where a real €15 goes: which branch, which staffer sells,
    which POS account is charged and which product is sold. All four arrive from
    the environment at start-up, and nothing stops an operator restarting the
    container with a different value between the freeze and the payment.

    The ledger already stores what was approved, so the comparison is cheap and
    the consequence is not: a batch frozen against one payment account must
    never be paid from another. Answered as a boolean — the UUIDs themselves
    stay out of every report, exactly as the redaction policy requires.
    """
    if not snapshot.exists:
        return True
    return (
        snapshot.location_uuid == request.location_uuid
        and snapshot.staffer_uuid == request.staffer_uuid
        and snapshot.payment_account_uuid == request.payment_account_uuid
        and snapshot.voucher_template_uuid == request.voucher_template_uuid
    )


def _refusal(
    stage: str,
    reasons: tuple[str, ...] | list[str],
    snapshot: ledger_module.BatchSnapshot,
    *,
    baseline: BaselineProof | None = None,
) -> StageReport:
    """A stage that did not act. Nothing left this process."""
    return StageReport(
        stage=stage,
        outcome="refused",
        reasons=list(reasons),
        external_effect_attempted=False,
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=any(entry.manual_cleanup_required for entry in snapshot.items),
        halted=snapshot.halted,
        batch=snapshot.as_safe_dict(),
        baseline=baseline.as_safe_dict() if baseline is not None else None,
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
    )


async def _exact_order(order_reader: Any, order_uuid: str | None) -> tuple[object | None, str | None]:
    """One exact GET of a slot's order, or a reason it proves nothing."""
    if not order_uuid:
        return None, ORDER_UNPROVEN
    try:
        payload = await order_reader.get_order(order_uuid)
    except EasyWeekError:
        return None, ORDER_UNPROVEN
    except Exception:  # noqa: BLE001 - an unreadable answer proves nothing
        return None, ORDER_UNPROVEN
    order = order_object(payload)
    if order is None or canonical_uuid(order.get("uuid")) != order_uuid:
        # A 200 is not proof the body describes the order we asked about.
        return None, ORDER_UNPROVEN
    return payload, None


async def _baseline_now(order_reader: Any) -> tuple[BaselineProof, tuple[str, ...]]:
    """Read the template and compare it with the approved 42/42 baseline.

    The same versioned baseline §37.2 proved in production, reused rather than
    restated: this phase buys the same product from the same template, and two
    copies of one number are two numbers that can disagree.

    A drift is reported, never absorbed. The caller decides what a drift means
    for ITS stage — which is not the same answer everywhere: a refund stays
    available on a drifted template, because cleanup matters more than the
    tidiness of the configuration it is cleaning up after.
    """
    try:
        payload = await order_reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except Exception:  # noqa: BLE001 - an unread template is an unproven one
        return BaselineProof(proven=False, baseline_version=MANUAL_BASELINE_VERSION), (BASELINE_DRIFT,)
    proof = prove_baseline(payload)
    return proof, () if proof.proven else (BASELINE_DRIFT,)


async def _item_order_preconditions(
    order_reader: Any,
    *,
    stage: str,
    item: ledger_module.ItemSnapshot,
) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    """What one slot's remote order must look like for this stage."""
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    payload, order_reason = await _exact_order(order_reader, item.target_order_uuid)
    if order_reason is not None:
        return [order_reason], None, observations

    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage=f"{stage}_plan_readback",
        expected_customer_uuid=item.easyweek_customer_uuid or "",
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=UNIT_PRICE_MINOR,
    )
    observations.append({"slot": item.slot, **observation.as_safe_dict()})

    if order.get("comment") != item.reconciliation_marker:
        reasons.append(ORDER_UNPROVEN)
    if not observation.order_customer_binding_proven:
        reasons.append(ORDER_UNPROVEN)
    # The voucher's own shape matters to everything that spends money or sends a
    # message. It deliberately does NOT matter to a refund: an artifact we
    # cannot read is a reason to get the money back, not a reason to leave it
    # out there. The observation is recorded as evidence either way.
    if stage != STAGE_REFUND and not observation.voucher_line_proven:
        reasons.append(ARTIFACT_UNPROVEN)

    if stage == STAGE_PAY:
        if state != ORDER_OPEN:
            reasons.append(ORDER_NOT_PAYABLE)
        reasons.extend(
            ORDER_NOT_PAYABLE
            for _ in payable_order_reasons(
                payload,
                expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
                expected_price_minor=UNIT_PRICE_MINOR,
            )
        )
    elif stage == STAGE_DELIVER:
        if state != ORDER_PAID:
            reasons.append(ORDER_NOT_PAID)
    elif stage == STAGE_REFUND:
        # Strictly paid. An order that already reads refunded has nothing left
        # to refund, and authorising a POST for it would authorise a second real
        # refund attempt against a provider with no idempotency key.
        if state == ORDER_REFUNDED:
            reasons.append(ORDER_ALREADY_REFUNDED)
        elif state != ORDER_PAID:
            reasons.append(ORDER_NOT_PAID)

    return reasons, state, observations


def _stage_slots(snapshot: ledger_module.BatchSnapshot, stage: str) -> list[int]:
    """The slots this stage would act on, in slot order. Deterministic, always."""
    allowed = STAGE_ITEM_SOURCE_STATUSES.get(stage, frozenset())
    return [entry.slot for entry in snapshot.items if entry.status in allowed]


async def build_stage_plan(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    slot: int | None = None,
    now: datetime | None = None,
    enabled: bool | None = None,
) -> tuple[StagePlan, BatchComposition, BatchPrerequisites, BaselineProof]:
    """Re-prove everything THIS stage depends on. Reads only; mutates nothing.

    Creates no batch, sends no request and writes nothing. Every failure is one
    of §41's stable reason codes, and an unready plan still prints, because an
    operator needs to see WHICH fact is missing.
    """
    issued_at = now or utcnow()
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    if stage not in STAGE_ITEM_SOURCE_STATUSES and stage != STAGE_FREEZE:
        reasons.append(UNKNOWN_STAGE)

    prerequisites = await prove_prerequisites(
        session,
        stage=stage,
        company_id=request.company_id,
        sender_code=request.sender_code,
        enabled=enabled,
    )
    reasons.extend(prerequisites.reasons)

    snapshot = await ledger_module.load(session_maker)
    batch_id = await ledger_module.batch_id_of(session_maker)

    # Which preview the composition is read from. Before the freeze it is the
    # one the operator named; afterwards it is the one the batch is bound to, so
    # a later stage cannot be pointed at a different snapshot by a typo.
    run_id = snapshot.campaign_run_id if snapshot.exists else request.preview_run_id
    if snapshot.exists and run_id != request.preview_run_id:
        reasons.append(COMPOSITION_DRIFTED)

    # The four UUIDs that decide where a real €15 goes — branch, staffer,
    # payment account, product — arrive from the environment, and a container
    # restarted with different values between the freeze and the payment would
    # otherwise charge an account nobody approved. Checked for every stage that
    # comes after a freeze, the refund included, and the drift costs zero
    # external calls because the plan simply is not ready.
    runtime_identity_bound = _runtime_identity_matches(request, snapshot)
    if not runtime_identity_bound:
        reasons.append(IDENTITY_BINDING_MISMATCH)

    # The live guard over every member, before every stage that can reach a
    # person — and deliberately NOT before a refund.
    #
    # A refund sends nothing to anybody. Requiring a live customer read, a
    # current phone, a current name or the absence of an opt-out would mean the
    # cleanup path stops working at exactly the moments it is most needed: the
    # customer opted out, changed their number, or EasyWeek is answering 500.
    # Every one of those is a reason to GET THE MONEY BACK, not a reason to
    # leave €15 out there.
    if stage == STAGE_REFUND:
        composition = BatchComposition(
            proven=False,
            preview_run_id=run_id or 0,
            reasons=(),
        )
    else:
        composition = await prove_batch_composition(
            session,
            preview_run_id=run_id or 0,
            client_reader=reader,
            now=issued_at,
            # After the freeze the batch's own slots hold exactly these
            # entitlements. Counting them would make every later stage report
            # the batch as a conflict with itself.
            exclude_batch_id=batch_id,
        )
        reasons.extend(composition.reasons)

    # The template baseline, before every stage that will touch money or a
    # phone. A refund reads it too — for the report — but a drift does not stop
    # it: the money should come back either way.
    baseline, baseline_reasons = await _baseline_now(order_reader)
    if stage != STAGE_REFUND:
        reasons.extend(baseline_reasons)

    ledger_state: dict[str, Any] = {
        "exists": snapshot.exists,
        "status": snapshot.status,
        "halted": snapshot.halted,
        "recipient_count": snapshot.recipient_count,
        "total_exposure_minor": snapshot.total_exposure_minor,
        "frozen_digest": snapshot.frozen_digest,
        "items": [
            {"slot": entry.slot, "status": entry.status, "send_attempt_count": entry.send_attempt_count}
            for entry in snapshot.items
        ],
    }

    if stage == STAGE_FREEZE:
        if snapshot.exists:
            # One batch, ever. The unique constraint says so too; this is the
            # refusal an operator can read.
            reasons.append(BATCH_ALREADY_EXISTS)
        ledger_state["proposed_recipient_count"] = composition.recipient_count
        ledger_state["proposed_frozen_digest"] = composition.digest() if composition.proven else None
        target_slots: list[int] = [member.slot for member in composition.members]
    else:
        if not snapshot.exists:
            reasons.append(BATCH_NOT_FROZEN)
        elif snapshot.halted and stage != STAGE_REFUND:
            # The suffix of a batch that already went wrong is not claimable
            # until a human has resolved the slot that stopped it.
            #
            # The refund is the exception, and it is the whole reason the
            # exception exists. A halt is precisely the situation in which an
            # untouched paid slot most needs its €15 back — one slot's send went
            # unknown, and the slots behind it are sitting paid and unsendable.
            # Blocking the cleanup path because cleanup is needed would have it
            # exactly the wrong way round. What protects the refund is its own
            # guards: one named slot, in a payable state, that nothing was ever
            # sent for, enforced by the plan, the claim AND a CHECK constraint.
            reasons.append(BATCH_HALTED)

        # Drift. The composition is re-derived live and compared with the digest
        # the freeze signed: an operator who edited the preview, or a customer
        # who opted out or changed their number, changes it, and the stage
        # refuses before anything leaves this process.
        if snapshot.exists and stage != STAGE_REFUND:
            if composition.proven and composition.digest() != snapshot.frozen_digest:
                reasons.append(FROZEN_DIGEST_MISMATCH)
            elif not composition.proven:
                reasons.append(COMPOSITION_DRIFTED)

        if stage == STAGE_REFUND:
            # One named slot, and only if it is one this batch has.
            if slot is None or snapshot.item(slot) is None:
                reasons.append(SLOT_UNKNOWN)
                target_slots = []
            else:
                entry = snapshot.item(slot)
                assert entry is not None
                if entry.status in ledger_module.SENT_ITEM_STATUSES:
                    reasons.append(REFUND_FORBIDDEN_AFTER_SEND)
                if entry.status not in STAGE_ITEM_SOURCE_STATUSES[STAGE_REFUND]:
                    reasons.append(LEDGER_STATE_UNEXPECTED)
                target_slots = [slot]
        else:
            target_slots = _stage_slots(snapshot, stage)
            if not target_slots:
                # Nothing this stage could act on. Refused rather than reported
                # as a green no-op: an operator asking for a stage that has no
                # work is an operator whose model of the batch is wrong.
                reasons.append(LEDGER_STATE_UNEXPECTED)

        if stage == STAGE_DELIVER:
            for entry in snapshot.items:
                if entry.slot in target_slots and entry.send_attempt_count:
                    reasons.append(DELIVERY_ALREADY_ATTEMPTED)

        # The remote orders, one exact GET per slot this stage would act on.
        if stage in (STAGE_PAY, STAGE_DELIVER, STAGE_REFUND):
            for entry in snapshot.items:
                if entry.slot not in target_slots:
                    continue
                item_reasons, _state, item_observations = await _item_order_preconditions(
                    order_reader,
                    stage=stage,
                    item=entry,
                )
                reasons.extend(item_reasons)
                observations.extend(item_observations)

    snapshot_facts: dict[str, Any] = {
        "stage": stage,
        "company_id": request.company_id,
        "campaign_code": NEW_CLIENT_CAMPAIGN_CODE,
        "preview_run_id": run_id,
        "location_uuid": request.location_uuid,
        "voucher_template_uuid": request.voucher_template_uuid,
        "voucher_unit_price_minor": UNIT_PRICE_MINOR,
        "max_recipients": MAX_RECIPIENTS,
        "max_exposure_minor": MAX_EXPOSURE_MINOR,
        "target_slots": sorted(target_slots),
        "prerequisites": prerequisites.as_safe_dict(),
        "composition": composition.as_safe_dict(),
        # Stated rather than implied: a refund's report must not read as though
        # a live guard passed when none was run.
        "live_guard_applied": stage != STAGE_REFUND,
        # A boolean, deliberately. The staffer and the payment account are
        # operational identities that no report prints, so what the digest
        # binds is the VERDICT about them: an approval taken while the
        # environment matched cannot be replayed once it no longer does.
        "runtime_identity_matches_frozen": runtime_identity_bound,
        "baseline": baseline.as_safe_dict(),
        "batch": snapshot.as_safe_dict(),
    }

    unique = tuple(dict.fromkeys(reasons))
    plan = StagePlan(
        stage=stage,
        ready=not unique,
        reasons=unique,
        digest=stage_digest(
            stage=stage,
            snapshot=snapshot_facts,
            ledger_state=ledger_state,
            issued_at=issued_at,
        ),
        issued_at=issued_at,
        snapshot=snapshot_facts,
        ledger_state=ledger_state,
        observations=tuple(observations),
    )
    return plan, composition, prerequisites, baseline


async def _authorise(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    slot: int | None = None,
    enabled: bool | None = None,
) -> tuple[
    StagePlan | None,
    BatchComposition | None,
    BatchPrerequisites | None,
    BaselineProof | None,
    StageReport | None,
]:
    """Rebuild the plan live and check the approval. A report means: refused."""
    plan, composition, prerequisites, baseline = await build_stage_plan(
        session,
        session_maker,
        stage=stage,
        request=request,
        reader=reader,
        order_reader=order_reader,
        slot=slot,
        enabled=enabled,
    )
    snapshot = await ledger_module.load(session_maker)

    reasons: list[str] = []
    if not apply:
        # A missing --apply is not an error to explain away: it is the default,
        # and the default is that nothing happens.
        reasons.append(APPLY_FLAG_MISSING)
    reasons.extend(
        verify_plan_authorisation(
            plan,
            supplied_digest=supplied_digest,
            supplied_issued_at=supplied_issued_at,
            supplied_phrase=supplied_phrase,
        )
    )
    if reasons:
        return None, None, None, None, _refusal(stage, tuple(dict.fromkeys(reasons)), snapshot, baseline=baseline)
    return plan, composition, prerequisites, baseline, None


async def run_freeze(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Write the ONE batch and its slots. Local only — nothing leaves this process."""
    plan, composition, _prereq, baseline, refused = await _authorise(
        session,
        session_maker,
        stage=STAGE_FREEZE,
        request=request,
        reader=reader,
        order_reader=order_reader,
        apply=apply,
        supplied_digest=supplied_digest,
        supplied_issued_at=supplied_issued_at,
        supplied_phrase=supplied_phrase,
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and composition is not None and baseline is not None

    identity = _identity_from_composition(request, composition)
    if identity is None:
        snapshot = await ledger_module.load(session_maker)
        return _refusal(STAGE_FREEZE, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)

    outcome = await ledger_module.freeze_batch(
        session_maker,
        identity=identity,
        freeze_plan_digest=plan.digest,
    )
    if not outcome.applied and outcome.reason != ledger_module.FREEZE_APPLIED:
        reason = BATCH_ALREADY_EXISTS if outcome.reason == ledger_module.FREEZE_REFUSED_EXISTS else SNAPSHOT_NOT_FROZEN
        return _refusal(STAGE_FREEZE, [reason], outcome.snapshot, baseline=baseline)

    return StageReport(
        stage=STAGE_FREEZE,
        outcome="frozen",
        # Freezing is a local write. Nothing was sent, nothing was bought.
        external_effect_attempted=False,
        reconciliation_required=outcome.snapshot.reconciliation_required,
        halted=outcome.snapshot.halted,
        batch=outcome.snapshot.as_safe_dict(),
        baseline=baseline.as_safe_dict(),
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
    )


async def run_create(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Create one open order with one voucher line, per slot, in slot order.

    At most one POST per slot and at most five in the batch. The first outcome
    that cannot be proven stops everything after it.
    """
    plan, composition, _prereq, baseline, refused = await _authorise(
        session,
        session_maker,
        stage=STAGE_CREATE,
        request=request,
        reader=reader,
        order_reader=order_reader,
        apply=apply,
        supplied_digest=supplied_digest,
        supplied_issued_at=supplied_issued_at,
        supplied_phrase=supplied_phrase,
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and composition is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    if identity is None:
        return _refusal(STAGE_CREATE, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)

    customers = {member.slot: member.easyweek_customer_uuid for member in composition.members}
    results: list[SlotResult] = []
    calls = 0
    halted = False

    for slot in _stage_slots(snapshot, STAGE_CREATE):
        if halted:
            # The suffix. Not attempted, and said so in the report rather than
            # left to be inferred from a missing entry.
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]))
            continue

        item = snapshot.item(slot)
        customer = customers.get(slot)
        if item is None or customer is None or customer != item.easyweek_customer_uuid:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[COMPOSITION_DRIFTED]))
            halted = True
            continue

        window_start = utcnow()
        # The claim is committed before the request leaves, and from that
        # instant an open draft may exist in the POS. The cleanup flag goes up
        # HERE rather than when an answer comes back: the answers that never
        # come back are exactly the ones that leave a draft behind.
        claim = await ledger_module.claim_create(
            session_maker,
            identity=identity,
            slot=slot,
            plan_digest=plan.digest,
            create_window_start=window_start - CREATE_WINDOW,
            create_window_end=window_start + CREATE_WINDOW,
        )
        if not claim.granted:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[claim.reason]))
            halted = True
            continue

        # Committed. From here on, whatever happens, something MAY have reached
        # EasyWeek, and nothing in this process may assume otherwise.
        calls += 1
        try:
            # Every identity on the wire comes from the FROZEN batch, not from
            # the environment this process happens to have been started with.
            # The plan already refuses a mismatch; using the frozen values here
            # means even a plan that somehow got through cannot sell a
            # different product from a different branch.
            response = await mutator.create_voucher_order(
                location_uuid=identity.location_uuid,
                customer_uuid=customer,
                staffer_uuid=identity.staffer_uuid,
                voucher_template_uuid=identity.voucher_template_uuid,
                price_minor=UNIT_PRICE_MINOR,
                marker=item.reconciliation_marker,
            )
        except EasyWeekVoucherMutationUnknown:
            # A timeout or a connection reset says nothing about whether the
            # order was created. It may exist, unpaid, in the dashboard — so
            # both flags stay up until an exact readback proves otherwise, and
            # the rest of the batch is not attempted at all.
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATE_CLAIMED}),
                reason_code=MUTATION_UNKNOWN,
                reconciliation_required=True,
                manual_cleanup_required=True,
            )
            results.append(
                SlotResult(
                    slot=slot,
                    outcome="unknown",
                    reasons=[MUTATION_UNKNOWN],
                    external_effect_attempted=True,
                )
            )
            halted = True
            continue
        except EasyWeekError:
            # A proven pre-action refusal: the endpoint's own validation
            # declined before acting, so this slot — and only this slot — may be
            # claimed again, from a fresh plan.
            #
            # The claim raised both flags before the request left, because at
            # that moment an open draft might follow. This answer proves one did
            # not: only 400/401/403/404/422 carrying this API's own refusal
            # envelope reach here, and the transport turns every outcome that
            # does NOT prove inaction into ``EasyWeekVoucherMutationUnknown``.
            # So the flags come down here, and the batch continues: a proven
            # refusal about one person says nothing about the next.
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_CREATE_REJECTED,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATE_CLAIMED}),
                reason_code=MUTATION_REJECTED,
                reconciliation_required=False,
                manual_cleanup_required=False,
            )
            results.append(
                SlotResult(
                    slot=slot,
                    outcome="rejected",
                    reasons=[MUTATION_REJECTED],
                    external_effect_attempted=True,
                )
            )
            continue

        result = await _verify_created(
            session_maker,
            slot=slot,
            item=item,
            customer_uuid=customer,
            order_reader=order_reader,
            response=response,
            voucher_template_uuid=identity.voucher_template_uuid,
        )
        results.append(result)
        if result.outcome != "created":
            halted = True

    final = await ledger_module.load(session_maker)
    return _stage_report(
        STAGE_CREATE,
        results,
        final,
        baseline,
        external_calls={"create": calls, "pay": 0, "refund": 0, "meta": 0},
    )


async def _verify_created(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    slot: int,
    item: ledger_module.ItemSnapshot,
    customer_uuid: str,
    order_reader: Any,
    response: VoucherMutationResponse,
    voucher_template_uuid: str,
) -> SlotResult:
    """A 2xx is a claim. Only an exact readback makes it a fact.

    Before a slot may read ``created`` the answer has to survive all of it: a
    canonical order UUID, a successful exact GET, our own marker, the customer
    the batch names, an open order, a provable singleton voucher line and a code
    that binds under this slot's own MAC domain. Anything else is
    ``create_unknown``, which halts the batch.
    """
    # The order identity comes out of the parsed body, at whatever level
    # EasyWeek put it. A 2xx without a canonical uuid is a claim we cannot even
    # address, let alone prove.
    envelope = order_object(response.envelope)
    candidate = canonical_uuid(envelope.get("uuid")) if envelope else None
    if candidate is None:
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATE_CLAIMED}),
            reason_code=ORDER_UNPROVEN,
            reconciliation_required=True,
            manual_cleanup_required=True,
        )
        return SlotResult(slot=slot, outcome="unknown", reasons=[ORDER_UNPROVEN], external_effect_attempted=True)

    payload, order_reason = await _exact_order(order_reader, candidate)
    if order_reason is not None:
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATE_CLAIMED}),
            reason_code=order_reason,
            target_order_uuid=candidate,
            reconciliation_required=True,
            manual_cleanup_required=True,
        )
        return SlotResult(slot=slot, outcome="unknown", reasons=[order_reason], external_effect_attempted=True)

    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage="create_readback",
        expected_customer_uuid=customer_uuid,
        expected_template_uuid=voucher_template_uuid,
        expected_price_minor=UNIT_PRICE_MINOR,
    )
    proven = (
        order.get("comment") == item.reconciliation_marker
        and observation.order_customer_binding_proven
        and observation.voucher_line_proven
        and state == ORDER_OPEN
    )
    code = _voucher_code(payload) if proven else None
    mac: tuple[str, str] | None = None
    if code is not None:
        try:
            mac = voucher_code_mac(
                voucher_code=code,
                ledger_uuid=f"{BATCH_SCOPE}:{slot}",
                target_order_uuid=candidate,
                voucher_template_uuid=voucher_template_uuid,
                domain=ledger_module.VOUCHER_BATCH_DOMAIN,
            )
        except VoucherBindingKeyError:
            mac = None
    del code

    if not proven or mac is None:
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATE_CLAIMED}),
            reason_code=ARTIFACT_UNPROVEN,
            target_order_uuid=candidate,
            reconciliation_required=True,
            manual_cleanup_required=True,
            evidence={"create_readback": observation.as_safe_dict()},
        )
        return SlotResult(
            slot=slot,
            outcome="unknown",
            reasons=[ARTIFACT_UNPROVEN],
            external_effect_attempted=True,
            order_state=state,
            observations=[{"slot": slot, **observation.as_safe_dict()}],
        )

    key_id, digest = mac
    await ledger_module.record_item_outcome(
        session_maker,
        slot=slot,
        status=VOUCHER_BATCH_ITEM_CREATED,
        expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATE_CLAIMED}),
        target_order_uuid=candidate,
        voucher_code_hmac=digest,
        hmac_key_id=key_id,
        verified_field="create_verified_at",
        reconciliation_required=False,
        # A proven open draft still exists. It stops needing a human only when
        # it is paid, refunded, or closed by hand.
        manual_cleanup_required=True,
        evidence={"create_readback": observation.as_safe_dict()},
    )
    return SlotResult(
        slot=slot,
        outcome="created",
        external_effect_attempted=True,
        order_state=ORDER_OPEN,
        observations=[{"slot": slot, **observation.as_safe_dict()}],
    )


def _stage_report(
    stage: str,
    results: list[SlotResult],
    snapshot: ledger_module.BatchSnapshot,
    baseline: BaselineProof,
    *,
    external_calls: dict[str, int],
) -> StageReport:
    """One report over however many slots the stage touched.

    The outcome is the worst thing that happened, never the best: a batch in
    which four slots succeeded and one is unknown is an unknown batch.
    """
    outcomes = {entry.outcome for entry in results}
    if not results:
        outcome = "nothing_to_do"
    elif "unknown" in outcomes:
        outcome = "unknown"
    elif outcomes <= {"refused", "not_attempted"}:
        # Nothing was attempted at all. "Partial" would read as though some of
        # it had worked, which is the one thing an operator must not conclude.
        outcome = "refused"
    elif "rejected" in outcomes or "refused" in outcomes:
        outcome = "partial"
    else:
        outcome = "applied"

    reasons: list[str] = []
    for entry in results:
        reasons.extend(entry.reasons)
    observations: list[dict[str, Any]] = []
    for entry in results:
        observations.extend(entry.observations)

    return StageReport(
        stage=stage,
        outcome=outcome,
        reasons=list(dict.fromkeys(reasons)),
        external_effect_attempted=any(entry.external_effect_attempted for entry in results),
        external_send_attempted=any(entry.external_send_attempted for entry in results),
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=any(entry.manual_cleanup_required for entry in snapshot.items),
        halted=snapshot.halted,
        batch=snapshot.as_safe_dict(),
        slots=results,
        observations=observations,
        baseline=baseline.as_safe_dict(),
        external_calls=dict(external_calls),
    )


async def run_pay(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Pay for each proven order, once. Real money, at most €15 per slot."""
    plan, composition, _prereq, baseline, refused = await _authorise(
        session,
        session_maker,
        stage=STAGE_PAY,
        request=request,
        reader=reader,
        order_reader=order_reader,
        apply=apply,
        supplied_digest=supplied_digest,
        supplied_issued_at=supplied_issued_at,
        supplied_phrase=supplied_phrase,
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and composition is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    if identity is None:
        return _refusal(STAGE_PAY, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)

    results: list[SlotResult] = []
    calls = 0
    halted = False

    for slot in _stage_slots(snapshot, STAGE_PAY):
        if halted:
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]))
            continue
        item = snapshot.item(slot)
        if item is None or item.target_order_uuid is None:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[ORDER_UNPROVEN]))
            halted = True
            continue

        # The code must still be the one this slot was bound to at CREATE.
        # Checked before the payment and not only before the send: paying for an
        # order whose voucher changed underneath us buys something else.
        payload, order_reason = await _exact_order(order_reader, item.target_order_uuid)
        if order_reason is not None:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[order_reason]))
            halted = True
            continue
        code = _voucher_code(payload)
        if code is None or not await ledger_module.binding_matches(
            session_maker,
            slot=slot,
            voucher_code=code,
            target_order_uuid=item.target_order_uuid,
        ):
            del code
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[BINDING_MISMATCH]))
            halted = True
            continue
        del code

        claim = await ledger_module.claim_pay(session_maker, identity=identity, slot=slot, plan_digest=plan.digest)
        if not claim.granted:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[claim.reason]))
            halted = True
            continue

        calls += 1
        try:
            # The account the batch was frozen with, never the one this
            # process was started with.
            await mutator.pay_voucher_order(
                order_uuid=item.target_order_uuid,
                account_uuid=identity.payment_account_uuid,
            )
        except EasyWeekVoucherMutationUnknown:
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_PAY_CLAIMED}),
                reason_code=MUTATION_UNKNOWN,
                reconciliation_required=True,
            )
            results.append(
                SlotResult(slot=slot, outcome="unknown", reasons=[MUTATION_UNKNOWN], external_effect_attempted=True)
            )
            halted = True
            continue
        except EasyWeekError:
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_PAY_REJECTED,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_PAY_CLAIMED}),
                reason_code=MUTATION_REJECTED,
                reconciliation_required=False,
            )
            results.append(
                SlotResult(slot=slot, outcome="rejected", reasons=[MUTATION_REJECTED], external_effect_attempted=True)
            )
            continue

        results.append(
            await _verify_paid(
                session_maker,
                slot=slot,
                item=item,
                order_reader=order_reader,
                voucher_template_uuid=identity.voucher_template_uuid,
            )
        )
        if results[-1].outcome != "paid":
            halted = True

    final = await ledger_module.load(session_maker)
    return _stage_report(
        STAGE_PAY,
        results,
        final,
        baseline,
        external_calls={"create": 0, "pay": calls, "refund": 0, "meta": 0},
    )


async def _verify_paid(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    slot: int,
    item: ledger_module.ItemSnapshot,
    order_reader: Any,
    voucher_template_uuid: str,
) -> SlotResult:
    """A 2xx is a claim. Only a readback showing the exact order paid proves it.

    ``paid`` is a whole identity, not a status word: this exact order, our
    marker, the customer the batch names, one provable voucher line at the
    approved template and price, and a code that still matches the binding.
    """
    assert item.target_order_uuid is not None
    payload, order_reason = await _exact_order(order_reader, item.target_order_uuid)
    state = classify_order(payload)[0] if order_reason is None else None
    if state != ORDER_PAID:
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_PAY_CLAIMED}),
            reason_code=order_reason or ORDER_NOT_PAID,
            reconciliation_required=True,
        )
        return SlotResult(
            slot=slot,
            outcome="unknown",
            reasons=[order_reason or ORDER_NOT_PAID],
            external_effect_attempted=True,
            order_state=state,
        )

    observation = observe_artifact(
        payload,
        stage="pay_readback",
        expected_customer_uuid=item.easyweek_customer_uuid or "",
        expected_template_uuid=voucher_template_uuid,
        expected_price_minor=UNIT_PRICE_MINOR,
    )
    order = order_object(payload) or {}
    identity_proven = (
        order.get("comment") == item.reconciliation_marker
        and observation.order_customer_binding_proven
        and observation.voucher_line_proven
    )
    binding_proven = False
    if identity_proven:
        settled = _voucher_code(payload)
        if settled is not None:
            binding_proven = await ledger_module.binding_matches(
                session_maker,
                slot=slot,
                voucher_code=settled,
                target_order_uuid=item.target_order_uuid,
            )
            del settled

    if not (identity_proven and binding_proven):
        # The money has probably moved and the artifact is not proven. Saying
        # `paid` here would open DELIVER on an unproven voucher; saying nothing
        # would strand the €15. So: an honest uncertainty that keeps the
        # pre-send refund reachable and the send shut.
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_PAY_CLAIMED}),
            reason_code=ARTIFACT_UNPROVEN,
            reconciliation_required=True,
            evidence={"pay_readback": observation.as_safe_dict()},
        )
        return SlotResult(
            slot=slot,
            outcome="unknown",
            reasons=[ARTIFACT_UNPROVEN],
            external_effect_attempted=True,
            order_state=state,
            observations=[{"slot": slot, **observation.as_safe_dict()}],
        )

    await ledger_module.record_item_outcome(
        session_maker,
        slot=slot,
        status=VOUCHER_BATCH_ITEM_PAID,
        expected_statuses=frozenset({VOUCHER_BATCH_ITEM_PAY_CLAIMED}),
        verified_field="pay_verified_at",
        reconciliation_required=False,
        manual_cleanup_required=False,
        evidence={"pay_readback": observation.as_safe_dict()},
    )
    return SlotResult(
        slot=slot,
        outcome="paid",
        external_effect_attempted=True,
        order_state=ORDER_PAID,
        observations=[{"slot": slot, **observation.as_safe_dict()}],
    )


async def run_deliver(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    sender: VoucherSender,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Send one message per paid slot. One attempt per slot, ever, by schema."""
    plan, composition, prerequisites, baseline, refused = await _authorise(
        session,
        session_maker,
        stage=STAGE_DELIVER,
        request=request,
        reader=reader,
        order_reader=order_reader,
        apply=apply,
        supplied_digest=supplied_digest,
        supplied_issued_at=supplied_issued_at,
        supplied_phrase=supplied_phrase,
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and composition is not None and prerequisites is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    if identity is None:
        return _refusal(STAGE_DELIVER, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)

    members = {member.slot: member for member in composition.members}
    results: list[SlotResult] = []
    calls = 0
    halted = False

    for slot in _stage_slots(snapshot, STAGE_DELIVER):
        if halted:
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]))
            continue
        item = snapshot.item(slot)
        member = members.get(slot)
        if (
            item is None
            or member is None
            or item.target_order_uuid is None
            or member.easyweek_customer_uuid != item.easyweek_customer_uuid
        ):
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[COMPOSITION_DRIFTED]))
            halted = True
            continue

        # The paid order is read once, here. The code lives from this line to
        # the POST below and nowhere else — not in the ledger, not in the
        # report, not in an exception, not in a log.
        payload, order_reason = await _exact_order(order_reader, item.target_order_uuid)
        if order_reason is not None:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[order_reason]))
            halted = True
            continue
        if classify_order(payload)[0] != ORDER_PAID:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[ORDER_NOT_PAID]))
            halted = True
            continue
        code = _voucher_code(payload)
        if code is None:
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[ARTIFACT_UNPROVEN]))
            halted = True
            continue
        if not await ledger_module.binding_matches(
            session_maker,
            slot=slot,
            voucher_code=code,
            target_order_uuid=item.target_order_uuid,
        ):
            del code
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[BINDING_MISMATCH]))
            halted = True
            continue

        proof = member.proof
        assert proof.proven_at is not None
        claim = await ledger_module.claim_send(
            session_maker,
            identity=identity,
            slot=slot,
            plan_digest=plan.digest,
            live_guard_reproven_at=proof.proven_at,
            template_code=VOUCHER_TEMPLATE_CODE,
            meta_template_name=prerequisites.meta_template_name or "",
            template_language=prerequisites.template_language or "",
            sender_id=prerequisites.sender_id,
        )
        if not claim.granted:
            del code
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[claim.reason]))
            halted = True
            continue

        # Committed, with this slot's attempt counter already at one. There is
        # no second attempt to fall back on and no code path that could take one.
        assert proof.destination_phone is not None and proof.client_display_name is not None
        # The approved template is POSITIONAL with exactly three BODY
        # parameters: client_name, voucher_code, booking_link. All three are
        # built here and all three must be non-empty — a message with an empty
        # slot is not the message Meta approved, and an empty link is a link to
        # nowhere in a real person's WhatsApp.
        params = [proof.client_display_name, code, prerequisites.booking_link or ""]
        if not all(part for part in params):
            del code
            del params
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_SEND_REJECTED,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_SEND_CLAIMED}),
                reason_code=TEMPLATE_PARAMETERS_UNPROVEN,
                reconciliation_required=True,
                attempt_outcome="rejected",
            )
            results.append(
                SlotResult(
                    slot=slot,
                    outcome="refused",
                    reasons=[TEMPLATE_PARAMETERS_UNPROVEN],
                    # The claim was committed, so this slot's attempt is spent —
                    # but nothing left this process.
                    external_effect_attempted=False,
                )
            )
            halted = True
            continue

        calls += 1
        outcome_meta = await sender.send_voucher_template(
            phone_number_id=prerequisites.phone_number_id or "",
            to_e164=proof.destination_phone,
            template_name=prerequisites.meta_template_name or "",
            language=prerequisites.template_language or "",
            params=params,
        )
        del code
        del params

        if outcome_meta.accepted and outcome_meta.provider_message_id:
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_SEND_CLAIMED}),
                provider_message_id=outcome_meta.provider_message_id,
                # Stamped by the very compare-and-set that records the
                # acceptance, not left for a webhook to invent afterwards.
                verified_field="provider_accepted_at",
                reconciliation_required=False,
                attempt_outcome="provider_accepted",
            )
            results.append(
                SlotResult(
                    slot=slot,
                    # Accepted by Meta. NOT delivered: that word belongs to a
                    # webhook.
                    outcome="provider_accepted",
                    external_effect_attempted=True,
                    external_send_attempted=True,
                    order_state=ORDER_PAID,
                )
            )
            continue

        if outcome_meta.outcome == DELIVERY_REJECTED:
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_SEND_REJECTED,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_SEND_CLAIMED}),
                reason_code=outcome_meta.reason,
                reconciliation_required=True,
                attempt_outcome="rejected",
            )
            results.append(
                SlotResult(
                    slot=slot,
                    outcome="rejected",
                    reasons=[outcome_meta.reason or MUTATION_REJECTED],
                    external_effect_attempted=True,
                    external_send_attempted=True,
                )
            )
            # A proven refusal about one number says nothing about the next, so
            # the batch continues. It still ends this slot's one attempt.
            continue

        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_SEND_CLAIMED}),
            reason_code=outcome_meta.reason,
            reconciliation_required=True,
            attempt_outcome="unknown",
        )
        results.append(
            SlotResult(
                slot=slot,
                outcome="unknown",
                reasons=[outcome_meta.reason or MUTATION_UNKNOWN],
                external_effect_attempted=True,
                external_send_attempted=True,
            )
        )
        halted = True

    final = await ledger_module.load(session_maker)
    return _stage_report(
        STAGE_DELIVER,
        results,
        final,
        baseline,
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": calls},
    )


async def run_refund(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: BatchRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    slot: int,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Refund ONE named slot that nothing has ever been sent for.

    Never the batch: a refund is the escape hatch for a specific €15 that should
    not have been spent, and "refund everything" is not a decision a command
    should be able to take in one keystroke.
    """
    plan, _composition, _prereq, baseline, refused = await _authorise(
        session,
        session_maker,
        stage=STAGE_REFUND,
        request=request,
        reader=reader,
        order_reader=order_reader,
        apply=apply,
        supplied_digest=supplied_digest,
        supplied_issued_at=supplied_issued_at,
        supplied_phrase=supplied_phrase,
        slot=slot,
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    item = snapshot.item(slot)
    if identity is None or item is None:
        return _refusal(STAGE_REFUND, [SLOT_UNKNOWN], snapshot, baseline=baseline)
    if item.status in ledger_module.SENT_ITEM_STATUSES:
        return _refusal(STAGE_REFUND, [REFUND_FORBIDDEN_AFTER_SEND], snapshot, baseline=baseline)
    if item.target_order_uuid is None:
        return _refusal(STAGE_REFUND, [ORDER_UNPROVEN], snapshot, baseline=baseline)

    claim = await ledger_module.claim_refund(session_maker, identity=identity, slot=slot, plan_digest=plan.digest)
    if not claim.granted:
        snapshot = await ledger_module.load(session_maker)
        return _refusal(STAGE_REFUND, [claim.reason], snapshot, baseline=baseline)

    results: list[SlotResult]
    try:
        await mutator.refund_voucher_order(order_uuid=item.target_order_uuid)
    except EasyWeekVoucherMutationUnknown:
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_REFUND_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            reconciliation_required=True,
        )
        results = [SlotResult(slot=slot, outcome="unknown", reasons=[MUTATION_UNKNOWN], external_effect_attempted=True)]
    except EasyWeekError:
        await ledger_module.record_item_outcome(
            session_maker,
            slot=slot,
            status=VOUCHER_BATCH_ITEM_REFUND_REJECTED,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_REFUND_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            reconciliation_required=False,
        )
        results = [
            SlotResult(slot=slot, outcome="rejected", reasons=[MUTATION_REJECTED], external_effect_attempted=True)
        ]
    else:
        payload, order_reason = await _exact_order(order_reader, item.target_order_uuid)
        state = classify_order(payload)[0] if order_reason is None else None
        if state == ORDER_REFUNDED:
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_REFUNDED,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_REFUND_CLAIMED}),
                verified_field="refund_verified_at",
                reconciliation_required=False,
                manual_cleanup_required=False,
            )
            results = [SlotResult(slot=slot, outcome="refunded", external_effect_attempted=True, order_state=state)]
        else:
            await ledger_module.record_item_outcome(
                session_maker,
                slot=slot,
                status=VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
                expected_statuses=frozenset({VOUCHER_BATCH_ITEM_REFUND_CLAIMED}),
                reason_code=order_reason or ORDER_NOT_PAID,
                reconciliation_required=True,
            )
            results = [
                SlotResult(
                    slot=slot,
                    outcome="unknown",
                    reasons=[order_reason or ORDER_NOT_PAID],
                    external_effect_attempted=True,
                    order_state=state,
                )
            ]

    final = await ledger_module.load(session_maker)
    return _stage_report(
        STAGE_REFUND,
        results,
        final,
        baseline,
        external_calls={"create": 0, "pay": 0, "refund": 1, "meta": 0},
    )


async def _recover_unknown_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    item: ledger_module.ItemSnapshot,
    location_uuid: str,
    voucher_template_uuid: str,
    order_reader: Any,
) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    """The GET-only proof path out of one slot's crashed or unknown CREATE.

    Never a second CREATE. Either the order is found and proves out — in which
    case the slot becomes ``created`` — or it stays unknown and a human decides.
    Serves ``create_claimed`` and ``create_unknown`` alike: a process that died
    between the commit and the answer left exactly the same question behind as
    one that lived to report a timeout.

    Zero matches is NOT "it was not created": the walk may simply not have seen
    it, and a create we cannot see is not a create we can deny. Several matches,
    or an incomplete walk, are a full stop.
    """
    observations: list[dict[str, Any]] = []
    candidate = item.target_order_uuid

    if candidate is None:
        if item.easyweek_customer_uuid is None or item.create_window_start is None or item.create_window_end is None:
            return [MARKER_SEARCH_INCOMPLETE], None, observations
        try:
            match = await find_marker_orders(
                order_reader,
                location_uuid=location_uuid,
                customer_uuid=item.easyweek_customer_uuid,
                marker=item.reconciliation_marker,
                window_start=datetime.fromisoformat(item.create_window_start),
                window_end=datetime.fromisoformat(item.create_window_end),
            )
        except Exception:  # noqa: BLE001 - an unread listing proves nothing
            return [MARKER_SEARCH_INCOMPLETE], None, observations
        observations.append(
            {
                "slot": item.slot,
                "stage": "create_marker_search",
                # Counts and booleans. A candidate UUID is never printed.
                "matches": match.count,
                "walk_complete": match.complete,
                "resolved": match.resolved,
            }
        )
        if not match.complete:
            return [MARKER_SEARCH_INCOMPLETE], None, observations
        if match.count > 1:
            return [MARKER_SEARCH_AMBIGUOUS], None, observations
        if not match.resolved or match.order_uuid is None:
            return [MARKER_SEARCH_UNRESOLVED], None, observations
        candidate = match.order_uuid

    payload, order_reason = await _exact_order(order_reader, candidate)
    if order_reason is not None:
        return [order_reason], None, observations

    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage="create_recovery_readback",
        expected_customer_uuid=item.easyweek_customer_uuid or "",
        expected_template_uuid=voucher_template_uuid,
        expected_price_minor=UNIT_PRICE_MINOR,
    )
    observations.append({"slot": item.slot, **observation.as_safe_dict()})

    if (
        order.get("comment") != item.reconciliation_marker
        or not observation.order_customer_binding_proven
        or not observation.voucher_line_proven
        or state != ORDER_OPEN
    ):
        return [ARTIFACT_UNPROVEN], state, observations

    code = _voucher_code(payload)
    mac: tuple[str, str] | None = None
    if code is not None:
        try:
            mac = voucher_code_mac(
                voucher_code=code,
                ledger_uuid=f"{BATCH_SCOPE}:{item.slot}",
                target_order_uuid=candidate,
                voucher_template_uuid=voucher_template_uuid,
                domain=ledger_module.VOUCHER_BATCH_DOMAIN,
            )
        except VoucherBindingKeyError:
            mac = None
    del code
    if mac is None:
        return [ARTIFACT_UNPROVEN], state, observations

    key_id, digest = mac
    # An existing binding is never overwritten. A slot that already carries one
    # was bound by its own CREATE, and a recovery that preferred whichever
    # answer arrived last would quietly re-point the row at a different code.
    # If the two disagree, the mismatch belongs to a human; the recovery simply
    # leaves the stored binding alone and proves against it below.
    keep_existing = item.voucher_binding_recorded
    if keep_existing and not await ledger_module.binding_matches(
        session_maker,
        slot=item.slot,
        voucher_code=_voucher_code(payload) or "",
        target_order_uuid=candidate,
    ):
        return [BINDING_MISMATCH], state, observations

    await ledger_module.record_item_outcome(
        session_maker,
        slot=item.slot,
        status=VOUCHER_BATCH_ITEM_CREATED,
        # Both crash shapes, because both mean the same thing about the world:
        # a process that died holding the claim and one that recorded an
        # unknown answer are recovered by the same exact readback.
        expected_statuses=CREATE_RECOVERABLE_FROM,
        target_order_uuid=candidate,
        voucher_code_hmac=None if keep_existing else digest,
        hmac_key_id=None if keep_existing else key_id,
        verified_field="create_verified_at",
        reconciliation_required=False,
        manual_cleanup_required=True,
        evidence={"create_recovery_readback": observation.as_safe_dict()},
    )
    return [], state, observations


async def _park_unresolved(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    item: ledger_module.ItemSnapshot,
    reason: str,
) -> None:
    """Record that a reconcile looked at a crashed claim and still cannot say.

    Moves ``*_claimed`` to the matching ``*_unknown`` and does nothing else. The
    destination is deliberately another unresolved state: it is not in any
    ``*_CLAIMABLE_FROM`` set, so this can never turn "we could not prove it"
    into permission to send the request again. What it adds is the one fact the
    row was missing — that a human-driven reconcile has already been here, and
    with which reason.
    """
    parked = _RECONCILED_UNKNOWN.get(item.status)
    if parked is None:
        return
    await ledger_module.record_item_outcome(
        session_maker,
        slot=item.slot,
        status=parked,
        expected_statuses=frozenset({item.status}),
        reason_code=reason,
        reconciliation_required=True,
        # A create whose answer was lost may still have left an open draft, and
        # a reconcile that found nothing has not proved otherwise.
        manual_cleanup_required=True if item.status == VOUCHER_BATCH_ITEM_CREATE_CLAIMED else None,
        attempt_outcome="unknown" if item.status == VOUCHER_BATCH_ITEM_SEND_CLAIMED else None,
    )


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: BatchRequest,
    order_reader: Any,
) -> StageReport:
    """Read-only. Ask the world what actually happened, and record only proof.

    Never claims, never sends and never refunds. Its whole job is to turn an
    ``unknown`` into something a human can act on: each slot's exact order is
    read back and its state is moved only where the readback proves the move.
    A batch stops being halted when — and only when — its slots stop being
    unresolved.
    """
    snapshot = await ledger_module.load(session_maker)
    if not snapshot.exists:
        return StageReport(
            stage="reconcile",
            outcome="nothing_to_reconcile",
            batch=snapshot.as_safe_dict(),
            external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
        )

    baseline, _ = await _baseline_now(order_reader)
    observations: list[dict[str, Any]] = []
    reasons: list[str] = []
    results: list[SlotResult] = []

    for item in snapshot.items:
        slot_reasons: list[str] = []
        state: str | None = None

        if item.status in CREATE_RECOVERABLE_FROM:
            # `create_claimed` is the crash case and `create_unknown` the
            # reported one; the world looks identical from here, so one proof
            # path serves both.
            slot_reasons, state, slot_observations = await _recover_unknown_create(
                session_maker,
                item=item,
                location_uuid=snapshot.location_uuid or request.location_uuid,
                voucher_template_uuid=snapshot.voucher_template_uuid or request.voucher_template_uuid,
                order_reader=order_reader,
            )
            observations.extend(slot_observations)
            if slot_reasons:
                # Nothing was proven. Absence is NOT proof the POST never left:
                # the walk may simply not have seen the order, so the slot stays
                # unresolved and nobody gets to send a second CREATE.
                await _park_unresolved(session_maker, item=item, reason=slot_reasons[0])
        elif item.target_order_uuid is not None:
            payload, order_reason = await _exact_order(order_reader, item.target_order_uuid)
            if order_reason is not None:
                slot_reasons.append(order_reason)
            else:
                state = classify_order(payload)[0]
                order = order_object(payload) or {}
                observation = observe_artifact(
                    payload,
                    stage="reconcile_readback",
                    expected_customer_uuid=item.easyweek_customer_uuid or "",
                    expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
                    expected_price_minor=UNIT_PRICE_MINOR,
                )
                observations.append({"slot": item.slot, **observation.as_safe_dict()})

                identity_proven = (
                    order.get("comment") == item.reconciliation_marker
                    and observation.order_customer_binding_proven
                    and observation.voucher_line_proven
                )
                binding_proven = False
                if identity_proven:
                    code = _voucher_code(payload)
                    if code is not None:
                        binding_proven = await ledger_module.binding_matches(
                            session_maker,
                            slot=item.slot,
                            voucher_code=code,
                            target_order_uuid=item.target_order_uuid,
                        )
                        del code
                proven = identity_proven and binding_proven
                # Complained about only where it would decide something. A slot
                # that is already settled — cleaned, refunded, sent — is read
                # here for the report, and a closed order need not still carry
                # the artifact it once issued.
                if not proven and item.status in ledger_module.UNRESOLVED_ITEM_STATUSES:
                    slot_reasons.append(ARTIFACT_UNPROVEN)

                # Only the transitions a readback PROVES, and only forwards.
                if state == ORDER_PAID and item.status in PAY_RECOVERABLE_FROM and proven:
                    # The exact order reads paid and still proves out as ours.
                    # That is the only thing that resolves a payment whose
                    # answer was lost — including one whose process died with
                    # the claim committed and nothing else written.
                    await ledger_module.record_item_outcome(
                        session_maker,
                        slot=item.slot,
                        status=VOUCHER_BATCH_ITEM_PAID,
                        expected_statuses=PAY_RECOVERABLE_FROM,
                        verified_field="pay_verified_at",
                        reconciliation_required=False,
                        manual_cleanup_required=False,
                        evidence={"reconcile_readback": observation.as_safe_dict()},
                    )
                elif state == ORDER_REFUNDED and item.status in (PAY_RECOVERABLE_FROM | REFUND_RECOVERABLE_FROM):
                    # A refunded order needs no artifact proof: the money is
                    # back, which is the outcome.
                    await ledger_module.record_item_outcome(
                        session_maker,
                        slot=item.slot,
                        status=VOUCHER_BATCH_ITEM_REFUNDED,
                        expected_statuses=PAY_RECOVERABLE_FROM | REFUND_RECOVERABLE_FROM,
                        verified_field="refund_verified_at",
                        reconciliation_required=False,
                        manual_cleanup_required=False,
                    )
                elif item.status in (PAY_RECOVERABLE_FROM | REFUND_RECOVERABLE_FROM):
                    # Read, and not proven. The order not reading paid or
                    # refunded does not prove the POST never left, so the slot
                    # is parked unresolved rather than reopened.
                    await _park_unresolved(
                        session_maker, item=item, reason=slot_reasons[0] if slot_reasons else ORDER_UNPROVEN
                    )
                elif state in (ORDER_CANCELLED, ORDER_REFUNDED) and item.status == VOUCHER_BATCH_ITEM_CREATED:
                    # The operator closed the draft by hand in the dashboard.
                    # That is an OBSERVATION, not something this tool did, and
                    # it is recorded as exactly that — with no second mutation.
                    await ledger_module.record_item_outcome(
                        session_maker,
                        slot=item.slot,
                        status=VOUCHER_BATCH_ITEM_MANUALLY_CLEANED,
                        expected_statuses=frozenset({VOUCHER_BATCH_ITEM_CREATED}),
                        reconciliation_required=False,
                        manual_cleanup_required=False,
                        manual_cleanup_observed=True,
                        evidence={"manual_cleanup": observation.as_safe_dict()},
                    )

        # A send whose outcome is unknown is NOT reconciled by reading an order:
        # whether Meta delivered the message is not a fact the POS system holds.
        #
        # `send_claimed` is the same situation arrived at by a crash, and it is
        # worse: the claim was committed, the attempt counter is already at one,
        # and no provider message id was ever written — so there is not even an
        # identifier to ask Meta about. It may not be retried, it may not be
        # declared unsent, and it may not be refunded. It waits for a human.
        if item.status in SEND_UNRESOLVED:
            slot_reasons.append(MUTATION_UNKNOWN)
            await _park_unresolved(session_maker, item=item, reason=MUTATION_UNKNOWN)

        reasons.extend(slot_reasons)
        results.append(
            SlotResult(
                slot=item.slot,
                outcome="unknown" if slot_reasons else "observed",
                reasons=slot_reasons,
                order_state=state,
            )
        )

    # The FINAL state, re-derived after everything this reconcile may have
    # written. Lifting the halt is a consequence of resolving slots, never an
    # action of its own.
    snapshot = await ledger_module.resettle(session_maker)

    unresolved = snapshot.reconciliation_required or snapshot.halted
    if unresolved and not reasons:
        # It ran, it changed nothing, and it cannot say why in more specific
        # terms. That still is not success.
        reasons.append(RECONCILE_UNRESOLVED)

    return StageReport(
        stage="reconcile",
        outcome="unknown" if unresolved else "observed",
        reasons=list(dict.fromkeys(reasons)),
        external_effect_attempted=False,
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=any(entry.manual_cleanup_required for entry in snapshot.items),
        halted=snapshot.halted,
        batch=snapshot.as_safe_dict(),
        slots=results,
        observations=observations,
        baseline=baseline.as_safe_dict(),
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
    )


async def run_status(session_maker: async_sessionmaker[AsyncSession]) -> StageReport:
    """Where this batch stands. Read-only, no network, no approval needed."""
    snapshot = await ledger_module.load(session_maker)
    return StageReport(
        stage="status",
        outcome="observed" if snapshot.exists else "not_started",
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=any(entry.manual_cleanup_required for entry in snapshot.items),
        halted=snapshot.halted,
        batch=snapshot.as_safe_dict(),
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
    )


__all__ = [
    "CREATE_WINDOW",
    "STAGE_ITEM_SOURCE_STATUSES",
    "BatchRequest",
    "SlotResult",
    "StageReport",
    "VoucherMutator",
    "VoucherSender",
    "build_stage_plan",
    "run_create",
    "run_deliver",
    "run_freeze",
    "run_pay",
    "run_reconcile",
    "run_refund",
    "run_status",
]
