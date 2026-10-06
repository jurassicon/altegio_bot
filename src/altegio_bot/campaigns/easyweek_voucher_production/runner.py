"""The operator stages of the production voucher mailing (§42).

However many people the operator curated, one €15 voucher each, one WhatsApp
message each — and between each pair of stages, a human who has to look at a
plan and decide again. There is no function here that runs two stages, and
there cannot be: the stop between them IS the control.

The order of every acting stage is the same, and it is not negotiable:

1. rebuild the plan live and check the operator's approval against it;
2. re-prove the whole composition, the template baseline and every order, from
   scratch — and refuse the stage outright if any of it drifted;
3. then, slot by slot in slot order: take the header lock and this slot's row
   lock, check the transition, write the claim AND the attempt, commit;
4. only then make at most ONE external request for that slot;
5. record what it turned out to be as a compare-and-set;
6. and if that outcome cannot be proven, stop. The remaining slots of THIS
   batch are not attempted at all.

Steps 3 and 4 in that order are the whole design. EasyWeek publishes no write
idempotency key and Meta will happily deliver twice, so a crash between the
commit and the response must read as "it may have happened" — which is the only
reading that cannot charge a card twice or message a person twice.

Step 6 is what keeps a bad day bounded. One unknown is a question about one
person; twenty unknowns discovered in a row are an outage nobody watched. The
first one stops everything after it, in this batch and only in this batch —
other mailings have their own approvals and their own state.

Everything addresses ONE batch, by id
-------------------------------------
§41 had a single batch and could address a slot by number. This phase runs
again next month, so every stage after the freeze takes a ``batch_id``, checks
that it is the batch bound to the preview the operator named, and acts only on
that batch's slots. There is no "latest batch" anywhere in this module.

Work per slot stays work per slot
---------------------------------
The composition is proven ONCE per stage, in the plan, against live data. The
per-slot loop then consults what that plan already established and touches two
rows per claim. A list of forty does forty claims and one proof, not forty
proofs — see :mod:`.ledger` for why that distinction is load bearing rather
than cosmetic.

What this module refuses to know
--------------------------------
It never learns a phone number, a name or a voucher code for longer than one
call. A code exists in memory between one read of a paid order and one POST to
Meta; what survives is a keyed MAC of it. No report, no ledger column, no log
line and no exception here carries any of the three.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
    VoucherBindingKeyError,
    voucher_code_mac,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import DELIVERY_REJECTED, DeliveryOutcome
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production.authorisation import (
    StagePlan,
    stage_digest,
    verify_plan_authorisation,
)
from altegio_bot.campaigns.easyweek_voucher_production.baseline import (
    PRODUCTION_BASELINE_VERSION,
    ProductionBaselineProof,
    prove_production_baseline,
)
from altegio_bot.campaigns.easyweek_voucher_production.composition import (
    BatchApproval,
    ProductionComposition,
    prove_production_composition,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPLY_FLAG_MISSING,
    APPROVAL_ARITHMETIC,
    ARTIFACT_UNPROVEN,
    BASELINE_DRIFT,
    BATCH_HALTED,
    BATCH_NOT_FROZEN,
    BATCH_PREVIEW_MISMATCH,
    BATCH_UNKNOWN,
    BINDING_MISMATCH,
    COMPOSITION_DRIFTED,
    DELIVERY_ALREADY_ATTEMPTED,
    FROZEN_DIGEST_MISMATCH,
    HALTED_BY_PREDECESSOR,
    IDENTITY_BINDING_MISMATCH,
    ISSUER_MEMBERSHIP_INCOMPLETE,
    KARLSRUHE_COMPANY_ID,
    LEDGER_STATE_UNEXPECTED,
    LEDGER_WRITE_LOST,
    MARKER_SEARCH_AMBIGUOUS,
    MARKER_SEARCH_INCOMPLETE,
    MARKER_SEARCH_UNRESOLVED,
    MUTATION_REJECTED,
    MUTATION_UNKNOWN,
    NEW_CLIENT_CAMPAIGN_CODE,
    ORDER_ALREADY_REFUNDED,
    ORDER_NOT_PAID,
    ORDER_NOT_PAYABLE,
    ORDER_UNPROVEN,
    PREVIEW_ALREADY_FROZEN,
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_SCOPE,
    RECONCILE_BUSY,
    RECONCILE_UNRESOLVED,
    REFUND_FORBIDDEN_AFTER_SEND,
    SLOT_UNKNOWN,
    SNAPSHOT_NOT_FROZEN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
    STOPPED_BY_OPERATOR,
    TEMPLATE_PARAMETERS_UNPROVEN,
    UNIT_PRICE_MINOR,
    UNKNOWN_STAGE,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_production.issuer import (
    IssuerMembership,
    pinned_issuer,
    prove_issuer_membership,
)
from altegio_bot.campaigns.easyweek_voucher_production.read_sessions import release_reads_before_http
from altegio_bot.campaigns.easyweek_voucher_production.readiness import (
    ProductionPrerequisites,
    prove_prerequisites,
)
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
    VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED,
    VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_CREATED,
    VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
    VOUCHER_PRODUCTION_ITEM_PAID,
    VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_PAY_REJECTED,
    VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUNDED,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
)
from altegio_bot.settings import settings
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
    {VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED, VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN}
)
PAY_RECOVERABLE_FROM: frozenset[str] = frozenset(
    {VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED, VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN}
)
REFUND_RECOVERABLE_FROM: frozenset[str] = frozenset(
    {VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED, VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN}
)
# A send is different from the three above and always will be: whether Meta
# delivered a message is not a fact a POS order can answer, and a slot that was
# claimed but never recorded a provider message id has no identifier to ask
# about. It stays fail-closed for a human.
SEND_UNRESOLVED: frozenset[str] = frozenset(
    {VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED, VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN}
)

# Where an unresolved claim is parked once a reconcile has looked and still
# cannot prove what happened. Same meaning, one fact added: somebody looked.
_RECONCILED_UNKNOWN: dict[str, str] = {
    VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED: VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED: VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED: VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED: VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
}

# Which item states each stage may act on. A slot in any other state is simply
# not part of this stage's work; a stage with no work at all is refused.
STAGE_ITEM_SOURCE_STATUSES: dict[str, frozenset[str]] = {
    STAGE_CREATE: frozenset({VOUCHER_PRODUCTION_ITEM_PLANNED, VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED}),
    STAGE_PAY: frozenset({VOUCHER_PRODUCTION_ITEM_CREATED, VOUCHER_PRODUCTION_ITEM_PAY_REJECTED}),
    STAGE_DELIVER: frozenset({VOUCHER_PRODUCTION_ITEM_PAID}),
    STAGE_REFUND: frozenset(
        {
            VOUCHER_PRODUCTION_ITEM_PAID,
            VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
            VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
        }
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
    # The operator stopped this stage. Distinct from ``halted``, which means a
    # slot's outcome could not be proven.
    stopped: bool = False
    batch: dict[str, Any] = field(default_factory=dict)
    slots: list[SlotResult] = field(default_factory=list)
    observations: list[dict[str, Any]] = field(default_factory=list)
    baseline: dict[str, Any] | None = None
    # How many external calls this stage actually made, by kind. An operator
    # comparing this with the batch size is how "one per slot" stops being a
    # promise and becomes something they can read off a report.
    external_calls: dict[str, int] = field(default_factory=dict)
    # Every batch this phase knows about, for `status` only.
    batches: list[dict[str, Any]] = field(default_factory=list)

    def as_safe_dict(self) -> dict[str, Any]:
        batch = dict(self.batch)
        return {
            "mode": "voucher_production_stage",
            "batch_scope": PRODUCTION_SCOPE,
            "stage": self.stage,
            # What the EXECUTION of this stage did. Never a claim about what
            # reached a customer: see the four delivery counters below.
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
            # Two different facts, never merged: an operator stopped this stage,
            # versus a slot's outcome could not be proven.
            "stopped_by_operator": self.stopped,
            "baseline": dict(self.baseline) if self.baseline is not None else None,
            "observations": list(self.observations),
            "slots": [entry.as_safe_dict() for entry in self.slots],
            "batch": batch,
            "batches": [dict(entry) for entry in self.batches],
            # The four facts, never merged. "This command finished" is not
            # "Meta accepted", which is not "a webhook said delivered", which is
            # not "a webhook said read". Only the last one means a person has
            # seen their voucher, and a report that blurred them would let an
            # operator believe a mailing landed when it may not have.
            #
            # Named for the COMMAND, deliberately. An earlier spelling called
            # this `stage_execution_complete`, which a tired operator reading a
            # green `status` could take for "the mailing is done" — the exact
            # confusion this block exists to prevent. Whether the mailing's own
            # stages are finished is `batch.execution_completed`, and whether
            # anything arrived is the three counters below.
            "command_completed": self.outcome in ("applied", "frozen", "observed"),
            "provider_accepted_count": batch.get("provider_accepted_count", 0),
            "webhook_delivered_count": batch.get("webhook_delivered_count", 0),
            "webhook_read_count": batch.get("webhook_read_count", 0),
            "recipient_basis": batch.get("recipient_basis"),
            "first_visit_proof": batch.get("first_visit_proof"),
            "voucher_unit_price_minor": UNIT_PRICE_MINOR,
            "approval_arithmetic": APPROVAL_ARITHMETIC,
            # Repeated verbatim on every stage, success included. A mailing of
            # forty proven sends is still not a campaign permission.
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
            "ready_for_send": False,
            "raw_identifiers_omitted": True,
            "voucher_code_omitted": True,
        }


@dataclass(frozen=True)
class ProductionRequest:
    """What the operator named on the command line, plus the frozen identity."""

    preview_run_id: int
    sender_code: str
    staffer_uuid: str
    payment_account_uuid: str
    # ``None`` only for a freeze, which is the stage that creates the id.
    batch_id: int | None = None
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
    request: ProductionRequest, composition: ProductionComposition
) -> ledger_module.BatchIdentity | None:
    """The identity a freeze would write, or ``None`` if the proof is incomplete.

    Signs ``composition_digest()`` — the people — rather than ``digest()``, which
    also covers the operator's approved numbers. The approved numbers go into
    their own columns and are pinned to the composition by CHECK constraints;
    the frozen digest has to stay re-derivable by a later stage that does not
    ask an operator to retype them.
    """
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
                recipient_basis=member.recipient_basis,
                client_id=member.client_id,
                destination_phone=member.proof.destination_phone,
                manual_policy=member.manual_policy,
                source_booking_uuid=member.source_booking_uuid,
                source_proof_digest=member.source_proof_digest,
                manual_policy_checked_at=member.manual_policy_checked_at,
                manual_operator_attested_at=member.manual_operator_attested_at,
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
        baseline_version=PRODUCTION_BASELINE_VERSION,
        frozen_digest=composition.composition_digest(),
        items=tuple(items),
        batch_id=None,
        schema_version=composition.schema_version,
        recipient_basis=composition.recipient_basis,
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
        snapshot.batch_id,
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
                recipient_basis=entry.recipient_basis,
                manual_policy=entry.manual_policy,
                source_booking_uuid=entry.source_booking_uuid,
                source_proof_digest=entry.source_proof_digest,
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
        batch_id=int(snapshot.batch_id or 0),
        schema_version=snapshot.schema_version,
        recipient_basis=snapshot.recipient_basis,
    )


def _runtime_identity_matches(
    request: ProductionRequest,
    snapshot: ledger_module.BatchSnapshot,
    *,
    stage: str,
) -> bool:
    """Is the environment this process runs in the one the batch was frozen with?

    Four UUIDs decide where real money goes: which branch, which staffer sells,
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
    bound = (
        snapshot.location_uuid == request.location_uuid
        and snapshot.payment_account_uuid == request.payment_account_uuid
        and snapshot.voucher_template_uuid == request.voucher_template_uuid
    )
    if stage == STAGE_REFUND:
        # The staffer is who SOLD the voucher, and a refund sells nothing. §43.9
        # is explicit that the issuer rule must not reach into a refund: a
        # pre-send slot whose money should come back must not be stranded
        # because the server's staffer setting was emptied, corrected or pointed
        # at somebody new since the freeze. The frozen order, the payment
        # account, the fence, the binding and the absence of a send claim are
        # what protect it, and none of them moved.
        #
        # Nothing is re-attributed either: the batch keeps the staffer it was
        # frozen with, and this comparison simply does not ask about it.
        return bound
    return bound and snapshot.staffer_uuid == request.staffer_uuid


def _refusal(
    stage: str,
    reasons: tuple[str, ...] | list[str],
    snapshot: ledger_module.BatchSnapshot,
    *,
    baseline: ProductionBaselineProof | None = None,
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


async def _baseline_now(order_reader: Any) -> tuple[ProductionBaselineProof, tuple[str, ...]]:
    """Read the template and compare it with this phase's approved baseline.

    A drift is reported, never absorbed. The caller decides what a drift means
    for ITS stage — which is not the same answer everywhere: a refund stays
    available on a drifted template, because cleanup matters more than the
    tidiness of the configuration it is cleaning up after.
    """
    try:
        payload = await order_reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except Exception:  # noqa: BLE001 - an unread template is an unproven one
        return (
            ProductionBaselineProof(proven=False, baseline_version=PRODUCTION_BASELINE_VERSION),
            (BASELINE_DRIFT,),
        )
    proof = prove_production_baseline(payload)
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


def _stage_slots(
    snapshot: ledger_module.BatchSnapshot,
    stage: str,
    *,
    authorised: tuple[int, ...] | None = None,
) -> list[int]:
    """The slots this stage may act on, in slot order. Deterministic, always.

    ``authorised`` is the slot list the operator's approval actually covers, as
    signed into the plan digest. When it is given, the result is the
    INTERSECTION of "still actionable" and "was approved", so re-reading the
    ledger can only ever narrow the work — never widen it.

    That direction is the whole point. The plan is built against one read of the
    ledger and the claims happen after several live EasyWeek calls, so a CREATE
    for another slot can legitimately land in between. Without the intersection
    a PAY would then charge a slot whose ``target_slots`` the owner never saw,
    which is exactly the drift §42.7 requires to cost zero external calls.

    Omitting ``authorised`` is for the plan itself, which is the thing that
    establishes the list in the first place.
    """
    allowed = STAGE_ITEM_SOURCE_STATUSES.get(stage, frozenset())
    slots = [entry.slot for entry in snapshot.items if entry.status in allowed]
    if authorised is None:
        return slots
    permitted = set(authorised)
    return [slot for slot in slots if slot in permitted]


async def _stop_reached(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    honour_stop: bool,
    batch_id: int,
) -> bool:
    """A cheap pre-claim read of the operator's stop. An optimisation only.

    The GUARANTEE lives in :func:`ledger._claim`, which re-reads the stop inside
    the very transaction that would grant the claim, after the header lock. This
    read exists so a stopped stage does not spend an EasyWeek GET per remaining
    slot on its way to finding out — a deliver, in particular, reads the paid
    order before it claims.

    It is therefore allowed to be stale in exactly one harmless direction: a stop
    pressed after this read is still caught by the claim. A stop pressed before it
    is caught here, one GET earlier.
    """
    if not honour_stop:
        return False
    return await ledger_module.stop_is_active(session_maker, batch_id=batch_id)


def available_item_actions(item: ledger_module.ItemSnapshot) -> tuple[str, ...]:
    """Which per-item stages this slot is in a state to be planned for.

    The ONE place that answers it (review R6). The UI used to decide for itself
    which rows could be refunded, and its list had drifted from the ledger's in both
    directions: it hid two states a refund is genuinely allowed from, and offered one
    — ``send_rejected`` — where a refund is forbidden because an attempt was already
    spent. Both are the same bug, which is a second copy of a rule.

    Derived from ``STAGE_ITEM_SOURCE_STATUSES``, so the buttons and the plan cannot
    disagree: this says what may be PLANNED, never what is authorised. Every real
    refusal still happens in the plan, in the claim and in a CHECK constraint, and a
    slot named here still needs a fresh plan, a live proof and its own confirmation.
    """
    actions: list[str] = []
    for stage in (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND):
        if item.status in STAGE_ITEM_SOURCE_STATUSES.get(stage, frozenset()):
            actions.append(stage)
    if STAGE_REFUND in actions and (
        item.status in ledger_module.SENT_ITEM_STATUSES or int(item.send_attempt_count or 0) > 0
    ):
        # Belt and braces for the rule that costs the most to get wrong: once an
        # attempt is spent, the code may already be in somebody's hands.
        actions.remove(STAGE_REFUND)
    return tuple(actions)


@release_reads_before_http("reader", "order_reader")
async def build_stage_plan(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: ProductionRequest,
    reader: BookingReader,
    order_reader: Any,
    approval: BatchApproval | None = None,
    slot: int | None = None,
    now: datetime | None = None,
    enabled: bool | None = None,
) -> tuple[
    StagePlan,
    ProductionComposition,
    ProductionPrerequisites,
    ProductionBaselineProof,
    ledger_module.BatchSnapshot,
]:
    """Re-prove everything THIS stage of THIS batch depends on. Reads only.

    Creates no batch, sends no request and writes nothing. Every failure is one
    of §42's stable reason codes, and an unready plan still prints, because an
    operator needs to see WHICH fact is missing.

    The composition is proven ONCE here, per stage, against live data. That is
    the whole-snapshot verification for the stage; the per-slot loop that
    follows does not repeat it.

    The snapshot is RETURNED rather than left behind. An earlier version let the
    caller load its own, which meant the plan was checked against one read of
    the ledger and acted on against another — and the second read could hold a
    slot the first did not. Returning it makes the plan, the snapshot it was
    proven against and the targets it authorises one consistent triple, which is
    what every caller below acts on.
    """
    issued_at = now or utcnow()
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    if stage not in STAGE_ITEM_SOURCE_STATUSES and stage != STAGE_FREEZE:
        reasons.append(UNKNOWN_STAGE)

    # §43.9: the approved issuer, and whether they still provably belong to this
    # location — asked ONCE here, per stage, and never per recipient. The walk is
    # skipped for a refund, which neither sells nor sends and must not be held
    # hostage by a staffer catalogue it has no use for.
    issuer = pinned_issuer(settings.easyweek_voucher_production_mailing_staffer_uuid)
    issuer_membership: IssuerMembership | None = None
    if stage != STAGE_REFUND:
        if issuer.pinned and issuer.uuid is not None:
            issuer_membership = await prove_issuer_membership(
                order_reader,
                location_uuid=request.location_uuid,
                issuer_uuid=issuer.uuid,
            )
        else:
            # Nothing to look for. Reported as an unproven membership rather than
            # as a pass, so the refusal set names both facts an administrator has
            # to fix in order.
            issuer_membership = IssuerMembership(reason=ISSUER_MEMBERSHIP_INCOMPLETE)

    prerequisites = await prove_prerequisites(
        session,
        stage=stage,
        company_id=request.company_id,
        sender_code=request.sender_code,
        enabled=enabled,
        issuer_membership=issuer_membership,
    )
    reasons.extend(prerequisites.reasons)

    # Which batch this plan is about, resolved differently before and after the
    # freeze — and never as "the latest one".
    #
    # A freeze has no id yet, so it asks whether this PREVIEW already has a
    # batch; anything else names an id, and that id has to be a batch bound to
    # the preview the operator also named. Requiring both, and comparing them,
    # is what stops a digit slip in one of them from pointing a stage at another
    # month's mailing.
    if stage == STAGE_FREEZE:
        snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=request.preview_run_id)
    elif request.batch_id is None:
        snapshot = ledger_module.BatchSnapshot(exists=False)
        reasons.append(BATCH_UNKNOWN)
    else:
        snapshot = await ledger_module.load(session_maker, batch_id=request.batch_id)
        if not snapshot.exists:
            reasons.append(BATCH_UNKNOWN)
        elif snapshot.campaign_run_id != request.preview_run_id:
            reasons.append(BATCH_PREVIEW_MISMATCH)

    batch_id = snapshot.batch_id if snapshot.exists else None
    # Which preview the composition is read from. Before the freeze it is the
    # one the operator named; afterwards it is the one the batch is bound to, so
    # a later stage cannot be pointed at a different snapshot by a typo.
    run_id = snapshot.campaign_run_id if snapshot.exists else request.preview_run_id

    # The four UUIDs that decide where real money goes — branch, staffer,
    # payment account, product — arrive from the environment, and a container
    # restarted with different values between the freeze and the payment would
    # otherwise charge an account nobody approved. Checked for every stage that
    # comes after a freeze, the refund included, and the drift costs zero
    # external calls because the plan simply is not ready.
    runtime_identity_bound = _runtime_identity_matches(request, snapshot, stage=stage)
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
    # leave real money out there.
    if stage == STAGE_REFUND:
        composition = ProductionComposition(
            proven=False,
            preview_run_id=run_id or 0,
            reasons=(),
        )
    else:
        composition = await prove_production_composition(
            session,
            preview_run_id=run_id or 0,
            client_reader=reader,
            now=issued_at,
            # The operator's stated size and cost, for a freeze only. Later
            # stages read the approved numbers off the frozen row instead of
            # asking anybody to retype them.
            approval=approval if stage == STAGE_FREEZE else None,
            # After the freeze the batch's own slots hold exactly these
            # entitlements. Counting them would make every later stage report
            # the batch as a conflict with itself.
            exclude_batch_id=batch_id,
            schema_version=snapshot.schema_version if snapshot.exists else PRODUCTION_SCHEMA_VERSION,
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
        "batch_id": snapshot.batch_id,
        "status": snapshot.status,
        "halted": snapshot.halted,
        "recipient_count": snapshot.recipient_count,
        "total_exposure_minor": snapshot.total_exposure_minor,
        "approved_recipient_count": snapshot.approved_recipient_count,
        "approved_exposure_minor": snapshot.approved_exposure_minor,
        "frozen_digest": snapshot.frozen_digest,
        "items": [
            {"slot": entry.slot, "status": entry.status, "send_attempt_count": entry.send_attempt_count}
            for entry in snapshot.items
        ],
    }

    if stage == STAGE_FREEZE:
        if snapshot.exists:
            # One batch per preview, ever. The unique constraint says so too;
            # this is the refusal an operator can read.
            reasons.append(PREVIEW_ALREADY_FROZEN)
        ledger_state["proposed_recipient_count"] = composition.recipient_count
        ledger_state["proposed_total_exposure_minor"] = composition.total_exposure_minor
        ledger_state["proposed_frozen_digest"] = composition.composition_digest() if composition.proven else None
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
            # untouched paid slot most needs its money back — one slot's send
            # went unknown, and the slots behind it are sitting paid and
            # unsendable. Blocking the cleanup path because cleanup is needed
            # would have it exactly the wrong way round. What protects the
            # refund is its own guards: one named slot, in a payable state, that
            # nothing was ever sent for, enforced by the plan, the claim AND a
            # CHECK constraint.
            reasons.append(BATCH_HALTED)

        # Drift. The composition is re-derived live and compared with the digest
        # the freeze signed: an operator who edited the preview, or a customer
        # who opted out or changed their number, changes it, and the stage
        # refuses before anything leaves this process.
        if snapshot.exists and stage != STAGE_REFUND:
            live_identity = _identity_from_composition(request, composition) if composition.proven else None
            if composition.proven and (
                composition.composition_digest() != snapshot.frozen_digest
                or live_identity is None
                or not live_identity.matches(snapshot)
            ):
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
        # The batch identity is inside the signed snapshot, which is what makes
        # an approval useless for any other batch — even the same stage of one
        # frozen minutes earlier.
        "batch_id": snapshot.batch_id,
        "location_uuid": request.location_uuid,
        "voucher_template_uuid": request.voucher_template_uuid,
        "voucher_unit_price_minor": UNIT_PRICE_MINOR,
        "approval_arithmetic": APPROVAL_ARITHMETIC,
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
        # §43.9, signed as booleans for the same reason — and sound as booleans
        # BECAUSE the pin admits exactly one UUID. An approval taken while the
        # approved issuer was configured and provably present cannot be replayed
        # after the configuration drifts or the staffer leaves the branch.
        #
        # Deliberately absent from a REFUND's signed material. A refund sells
        # nothing, so these facts are not conditions of it — and signing them
        # would make the staffer setting changing after the plan invalidate the
        # digest, which is precisely the stranding §43.9 forbids: the money would
        # become unreturnable because of a setting that has nothing to do with
        # returning it. ``issuer_check_applied`` says which of the two shapes this
        # snapshot is, so one cannot be mistaken for the other.
        "issuer_check_applied": stage != STAGE_REFUND,
        **(
            {
                "issuer_pinned": issuer.pinned,
                "issuer_membership_proven": (issuer_membership.proven if issuer_membership is not None else None),
            }
            if stage != STAGE_REFUND
            else {}
        ),
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
    return plan, composition, prerequisites, baseline, snapshot


async def _authorise(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: ProductionRequest,
    reader: BookingReader,
    order_reader: Any,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    approval: BatchApproval | None = None,
    slot: int | None = None,
    enabled: bool | None = None,
) -> tuple[
    StagePlan | None,
    ProductionComposition | None,
    ProductionPrerequisites | None,
    ProductionBaselineProof | None,
    ledger_module.BatchSnapshot | None,
    StageReport | None,
]:
    """Rebuild the plan live and check the approval. A report means: refused.

    The snapshot handed back is the plan's OWN — the one the approval was
    verified against — and this function deliberately does not read the ledger
    again.

    It used to. The plan was built and digest-checked against one read, then a
    second read was loaded here and the acting stages derived their targets from
    that. Between the two, a CREATE completing for another slot made that slot
    actionable, so a PAY could reach a slot the approved ``target_slots`` never
    contained: two payments against a plan that authorised one. A later read can
    only ever add work, never remove the approval's ignorance of it, so the
    answer is not to re-read more carefully but not to re-read at all.

    Nothing is lost by using the earlier read. Staleness is handled where it has
    to be anyway: each slot is claimed under its own row lock, which re-checks
    the live status and the item identity before anything leaves the process.
    """
    plan, composition, prerequisites, baseline, snapshot = await build_stage_plan(
        session,
        session_maker,
        stage=stage,
        request=request,
        reader=reader,
        order_reader=order_reader,
        approval=approval,
        slot=slot,
        enabled=enabled,
    )

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
        return (
            None,
            None,
            None,
            None,
            None,
            _refusal(stage, tuple(dict.fromkeys(reasons)), snapshot, baseline=baseline),
        )
    return plan, composition, prerequisites, baseline, snapshot, None


async def run_freeze(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ProductionRequest,
    reader: BookingReader,
    order_reader: Any,
    approval: BatchApproval,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Write one batch and its slots. Local only — nothing leaves this process.

    ``approval`` is the operator's stated count and exposure. It is checked
    against the live snapshot in the plan, signed into the digest, and checked
    once more inside the freeze transaction against the rows that transaction
    can see. There is no path from here to a frozen batch whose approved numbers
    do not describe it.
    """
    plan, composition, _prereq, baseline, _snapshot, refused = await _authorise(
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
        approval=approval,
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and composition is not None and baseline is not None

    identity = _identity_from_composition(request, composition)
    if identity is None or approval.expected_recipient_count is None or approval.approved_exposure_minor is None:
        current = await ledger_module.load_for_preview(session_maker, campaign_run_id=request.preview_run_id)
        return _refusal(STAGE_FREEZE, [COMPOSITION_DRIFTED], current, baseline=baseline)

    outcome = await ledger_module.freeze_batch(
        session_maker,
        identity=identity,
        approved_recipient_count=approval.expected_recipient_count,
        approved_exposure_minor=approval.approved_exposure_minor,
        freeze_plan_digest=plan.digest,
    )
    if not outcome.applied and outcome.reason != ledger_module.FREEZE_APPLIED:
        reason = {
            ledger_module.FREEZE_REFUSED_EXISTS: PREVIEW_ALREADY_FROZEN,
            ledger_module.FREEZE_REFUSED_APPROVAL: COMPOSITION_DRIFTED,
        }.get(outcome.reason, SNAPSHOT_NOT_FROZEN)
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
    request: ProductionRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    honour_stop: bool = False,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Create one open order with one voucher line, per slot, in slot order.

    At most one POST per slot, and never more than one per slot for the lifetime
    of the batch. The first outcome that cannot be proven stops everything after
    it in this batch.
    """
    plan, composition, _prereq, baseline, snapshot, refused = await _authorise(
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
    assert plan is not None and composition is not None and baseline is not None and snapshot is not None

    identity = _identity_from_composition(request, composition)
    if identity is not None:
        identity = replace(identity, batch_id=snapshot.batch_id)
    if identity is None or snapshot.batch_id is None:
        return _refusal(STAGE_CREATE, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)
    batch_id = snapshot.batch_id

    customers = {member.slot: member.easyweek_customer_uuid for member in composition.members}
    results: list[SlotResult] = []
    calls = 0
    halted = False
    stopped = False

    # Bounded by what the operator's approval actually covers. The
    # intersection can only narrow this stage's work, never widen it.
    for slot in _stage_slots(snapshot, STAGE_CREATE, authorised=plan.authorised_slots):
        if halted:
            # The suffix. Not attempted, and said so in the report rather than
            # left to be inferred from a missing entry.
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]))
            continue
        if stopped or await _stop_reached(session_maker, honour_stop=honour_stop, batch_id=batch_id):
            stopped = True
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[STOPPED_BY_OPERATOR]))
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
            batch_id=batch_id,
            slot=slot,
            plan_digest=plan.digest,
            create_window_start=window_start - CREATE_WINDOW,
            create_window_end=window_start + CREATE_WINDOW,
            honour_stop=honour_stop,
        )
        if claim.reason == ledger_module.CLAIM_REFUSED_STOPPED:
            # The authoritative stop: read under the header lock, so nothing was
            # stamped and nothing about this slot is in doubt.
            stopped = True
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[STOPPED_BY_OPERATOR]))
            continue
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED}),
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED}),
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
            batch_id=batch_id,
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

    final = await ledger_module.load(session_maker, batch_id=batch_id)
    return _stage_report(
        STAGE_CREATE,
        results,
        final,
        baseline,
        external_calls={"create": calls, "pay": 0, "refund": 0, "meta": 0},
        stopped=stopped,
    )


async def _verify_created(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
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
    that binds under this batch's and this slot's own MAC material. Anything
    else is ``create_unknown``, which halts the batch.
    """
    # The order identity comes out of the parsed body, at whatever level
    # EasyWeek put it. A 2xx without a canonical uuid is a claim we cannot even
    # address, let alone prove.
    envelope = order_object(response.envelope)
    candidate = canonical_uuid(envelope.get("uuid")) if envelope else None
    if candidate is None:
        await ledger_module.record_item_outcome(
            session_maker,
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED}),
            reason_code=ORDER_UNPROVEN,
            reconciliation_required=True,
            manual_cleanup_required=True,
        )
        return SlotResult(slot=slot, outcome="unknown", reasons=[ORDER_UNPROVEN], external_effect_attempted=True)

    payload, order_reason = await _exact_order(order_reader, candidate)
    if order_reason is not None:
        await ledger_module.record_item_outcome(
            session_maker,
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED}),
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
                ledger_uuid=await ledger_module.load_binding_material(session_maker, batch_id=batch_id, slot=slot),
                target_order_uuid=candidate,
                voucher_template_uuid=voucher_template_uuid,
                domain=ledger_module.VOUCHER_PRODUCTION_DOMAIN,
            )
        except VoucherBindingKeyError:
            mac = None
    del code

    if not proven or mac is None:
        await ledger_module.record_item_outcome(
            session_maker,
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED}),
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
        batch_id=batch_id,
        slot=slot,
        status=VOUCHER_PRODUCTION_ITEM_CREATED,
        expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED}),
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
    baseline: ProductionBaselineProof,
    *,
    external_calls: dict[str, int],
    stopped: bool = False,
) -> StageReport:
    """One report over however many slots the stage touched.

    The outcome is the worst thing that happened, never the best: a batch in
    which thirty-nine slots succeeded and one is unknown is an unknown batch.

    ``stopped`` is its own outcome rather than a flavour of success or of
    failure. A stage in which nine slots were paid and eleven were never claimed
    because the operator pressed stop is neither ``applied`` — eleven people have
    no voucher — nor ``refused``, because nine payments are real. An operator
    reading either of those words would act on the wrong belief, and "unknown"
    would be worse still: a stopped slot was never claimed, so there is nothing
    uncertain about it.
    """
    outcomes = {entry.outcome for entry in results}
    if not results:
        outcome = "nothing_to_do"
    elif "unknown" in outcomes:
        # An unknown outranks a stop: the stop explains the tail, the unknown is
        # still a question about one person.
        outcome = "unknown"
    elif stopped:
        outcome = "stopped"
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
        stopped=stopped,
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
    request: ProductionRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    honour_stop: bool = False,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Pay for each proven order, once. Real money, exactly €15 per slot."""
    plan, composition, _prereq, baseline, snapshot, refused = await _authorise(
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
    assert plan is not None and composition is not None and baseline is not None and snapshot is not None

    identity = _identity_from_composition(request, composition)
    if identity is not None:
        identity = replace(identity, batch_id=snapshot.batch_id)
    if identity is None or snapshot.batch_id is None:
        return _refusal(STAGE_PAY, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)
    batch_id = snapshot.batch_id

    results: list[SlotResult] = []
    calls = 0
    halted = False
    stopped = False

    # Bounded by what the operator's approval actually covers. The
    # intersection can only narrow this stage's work, never widen it.
    for slot in _stage_slots(snapshot, STAGE_PAY, authorised=plan.authorised_slots):
        if halted:
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]))
            continue
        if stopped or await _stop_reached(session_maker, honour_stop=honour_stop, batch_id=batch_id):
            stopped = True
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[STOPPED_BY_OPERATOR]))
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
            batch_id=batch_id,
            slot=slot,
            voucher_code=code,
            target_order_uuid=item.target_order_uuid,
        ):
            del code
            results.append(SlotResult(slot=slot, outcome="refused", reasons=[BINDING_MISMATCH]))
            halted = True
            continue
        del code

        claim = await ledger_module.claim_pay(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=slot,
            plan_digest=plan.digest,
            honour_stop=honour_stop,
        )
        if claim.reason == ledger_module.CLAIM_REFUSED_STOPPED:
            stopped = True
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[STOPPED_BY_OPERATOR]))
            continue
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED}),
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_PAY_REJECTED,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED}),
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
                batch_id=batch_id,
                slot=slot,
                item=item,
                order_reader=order_reader,
                voucher_template_uuid=identity.voucher_template_uuid,
            )
        )
        if results[-1].outcome != "paid":
            halted = True

    final = await ledger_module.load(session_maker, batch_id=batch_id)
    return _stage_report(
        STAGE_PAY,
        results,
        final,
        baseline,
        external_calls={"create": 0, "pay": calls, "refund": 0, "meta": 0},
        stopped=stopped,
    )


async def _verify_paid(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
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
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED}),
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
                batch_id=batch_id,
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
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED}),
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
        batch_id=batch_id,
        slot=slot,
        status=VOUCHER_PRODUCTION_ITEM_PAID,
        expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED}),
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
    request: ProductionRequest,
    reader: BookingReader,
    order_reader: Any,
    sender: VoucherSender,
    apply: bool,
    honour_stop: bool = False,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Send one message per paid slot. One attempt per slot, ever, by schema."""
    plan, composition, prerequisites, baseline, snapshot, refused = await _authorise(
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
    assert (
        plan is not None
        and composition is not None
        and prerequisites is not None
        and baseline is not None
        and snapshot is not None
    )

    identity = _identity_from_composition(request, composition)
    if identity is not None:
        identity = replace(identity, batch_id=snapshot.batch_id)
    if identity is None or snapshot.batch_id is None:
        return _refusal(STAGE_DELIVER, [COMPOSITION_DRIFTED], snapshot, baseline=baseline)
    batch_id = snapshot.batch_id

    members = {member.slot: member for member in composition.members}
    results: list[SlotResult] = []
    calls = 0
    halted = False
    stopped = False

    # Bounded by what the operator's approval actually covers. The
    # intersection can only narrow this stage's work, never widen it.
    for slot in _stage_slots(snapshot, STAGE_DELIVER, authorised=plan.authorised_slots):
        if halted:
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]))
            continue
        # Before the paid order is read, not only before the claim: a stopped
        # deliver should not spend one GET per remaining recipient either.
        if stopped or await _stop_reached(session_maker, honour_stop=honour_stop, batch_id=batch_id):
            stopped = True
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[STOPPED_BY_OPERATOR]))
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
            batch_id=batch_id,
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
            batch_id=batch_id,
            slot=slot,
            plan_digest=plan.digest,
            live_guard_reproven_at=proof.proven_at,
            template_code=VOUCHER_TEMPLATE_CODE,
            meta_template_name=prerequisites.meta_template_name or "",
            template_language=prerequisites.template_language or "",
            sender_id=prerequisites.sender_id,
            honour_stop=honour_stop,
        )
        if claim.reason == ledger_module.CLAIM_REFUSED_STOPPED:
            del code
            stopped = True
            results.append(SlotResult(slot=slot, outcome="not_attempted", reasons=[STOPPED_BY_OPERATOR]))
            continue
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED}),
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
            recorded = await ledger_module.record_item_outcome(
                session_maker,
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
                # Both source states, deliberately (review R2). `send_claimed` is
                # the ordinary one. `send_unknown` is the row a reconcile parked
                # while this very request was in flight: the readback could not
                # know the answer was still coming, and the answer is now here.
                #
                # Accepting it is sound rather than lenient — `provider_accepted`
                # outranks `send_unknown`, so this is a forward move the
                # monotonicity guard already permits, and the alternative is
                # throwing away a PROVEN success and the only identifier by which
                # a later delivered/read callback could find this slot.
                expected_statuses=frozenset(
                    {VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED, VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN}
                ),
                provider_message_id=outcome_meta.provider_message_id,
                # Stamped by the very compare-and-set that records the
                # acceptance, not left for a webhook to invent afterwards.
                verified_field="provider_accepted_at",
                reconciliation_required=False,
                attempt_outcome="provider_accepted",
            )
            if not recorded.applied:
                # Meta accepted and the ledger does not say so. Never reported as
                # success: the message is real, the record is not, and the honest
                # state is one a human has to resolve. Deliberately NOT retried —
                # a second send is the one thing this outcome must not cause.
                results.append(
                    SlotResult(
                        slot=slot,
                        outcome="unknown",
                        reasons=[LEDGER_WRITE_LOST],
                        external_effect_attempted=True,
                        external_send_attempted=True,
                    )
                )
                halted = True
                continue
            results.append(
                SlotResult(
                    slot=slot,
                    # Accepted by Meta. NOT delivered and NOT read: those two
                    # words belong to a webhook, and the report counts them
                    # separately for exactly this reason.
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED}),
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
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED}),
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

    final = await ledger_module.load(session_maker, batch_id=batch_id)
    return _stage_report(
        STAGE_DELIVER,
        results,
        final,
        baseline,
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": calls},
        stopped=stopped,
    )


async def run_refund(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ProductionRequest,
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
    """Refund ONE named slot of ONE named batch that nothing was ever sent for.

    Never the batch: a refund is the escape hatch for a specific €15 that should
    not have been spent, and "refund everything" is not a decision a command
    should be able to take in one keystroke — least of all in a phase whose
    batches can be large.
    """
    plan, _composition, _prereq, baseline, snapshot, refused = await _authorise(
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
    assert plan is not None and baseline is not None and snapshot is not None

    identity = _identity_from_snapshot(snapshot)
    item = snapshot.item(slot)
    if identity is None or item is None or snapshot.batch_id is None:
        return _refusal(STAGE_REFUND, [SLOT_UNKNOWN], snapshot, baseline=baseline)
    # The same bound the other stages apply, stated rather than assumed. A
    # refund's slot IS the operator's argument and the plan signed that exact
    # argument, so this holds by construction today — which is precisely why it
    # is worth asserting: a later change that let the argument and the signed
    # plan diverge would otherwise refund a slot nobody approved, silently.
    if slot not in plan.authorised_slots:
        return _refusal(STAGE_REFUND, [SLOT_UNKNOWN], snapshot, baseline=baseline)
    batch_id = snapshot.batch_id
    if item.status in ledger_module.SENT_ITEM_STATUSES:
        return _refusal(STAGE_REFUND, [REFUND_FORBIDDEN_AFTER_SEND], snapshot, baseline=baseline)
    if item.target_order_uuid is None:
        return _refusal(STAGE_REFUND, [ORDER_UNPROVEN], snapshot, baseline=baseline)

    claim = await ledger_module.claim_refund(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=slot,
        plan_digest=plan.digest,
    )
    if not claim.granted:
        current = await ledger_module.load(session_maker, batch_id=batch_id)
        return _refusal(STAGE_REFUND, [claim.reason], current, baseline=baseline)

    results: list[SlotResult]
    try:
        await mutator.refund_voucher_order(order_uuid=item.target_order_uuid)
    except EasyWeekVoucherMutationUnknown:
        await ledger_module.record_item_outcome(
            session_maker,
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            reconciliation_required=True,
        )
        results = [SlotResult(slot=slot, outcome="unknown", reasons=[MUTATION_UNKNOWN], external_effect_attempted=True)]
    except EasyWeekError:
        await ledger_module.record_item_outcome(
            session_maker,
            batch_id=batch_id,
            slot=slot,
            status=VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
            expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED}),
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
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_REFUNDED,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED}),
                verified_field="refund_verified_at",
                reconciliation_required=False,
                manual_cleanup_required=False,
            )
            results = [SlotResult(slot=slot, outcome="refunded", external_effect_attempted=True, order_state=state)]
        else:
            await ledger_module.record_item_outcome(
                session_maker,
                batch_id=batch_id,
                slot=slot,
                status=VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
                expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED}),
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

    final = await ledger_module.load(session_maker, batch_id=batch_id)
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
    batch_id: int,
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
                ledger_uuid=await ledger_module.load_binding_material(session_maker, batch_id=batch_id, slot=item.slot),
                target_order_uuid=candidate,
                voucher_template_uuid=voucher_template_uuid,
                domain=ledger_module.VOUCHER_PRODUCTION_DOMAIN,
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
        batch_id=batch_id,
        slot=item.slot,
        voucher_code=_voucher_code(payload) or "",
        target_order_uuid=candidate,
    ):
        return [BINDING_MISMATCH], state, observations

    await ledger_module.record_item_outcome(
        session_maker,
        batch_id=batch_id,
        slot=item.slot,
        status=VOUCHER_PRODUCTION_ITEM_CREATED,
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
    batch_id: int,
    item: ledger_module.ItemSnapshot,
    reason: str,
) -> ledger_module.RecordOutcome:
    """Record that a reconcile looked at a crashed claim and still cannot say.

    Returns the outcome rather than discarding it, because one of its refusals is
    load bearing: ``record_refused_busy`` means an executor is using that claim and
    the reconcile must say so instead of implying it reinterpreted anything.

    Moves ``*_claimed`` to the matching ``*_unknown`` and does nothing else. The
    destination is deliberately another unresolved state: it is not in any
    ``*_CLAIMABLE_FROM`` set, so this can never turn "we could not prove it"
    into permission to send the request again. What it adds is the one fact the
    row was missing — that a human-driven reconcile has already been here, and
    with which reason.
    """
    parked = _RECONCILED_UNKNOWN.get(item.status)
    if parked is None:
        return ledger_module.RecordOutcome(False, ledger_module.RECORD_STALE_STATE)
    return await ledger_module.record_item_outcome(
        session_maker,
        batch_id=batch_id,
        slot=item.slot,
        status=parked,
        expected_statuses=frozenset({item.status}),
        reason_code=reason,
        reconciliation_required=True,
        # A create whose answer was lost may still have left an open draft, and
        # a reconcile that found nothing has not proved otherwise.
        manual_cleanup_required=True if item.status == VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED else None,
        attempt_outcome="unknown" if item.status == VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED else None,
        # The guard that makes a readback safe to run at all (review R2): a claim an
        # executor is using is not an abandoned one.
        require_idle=True,
    )


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ProductionRequest,
    order_reader: Any,
) -> StageReport:
    """Read-only. Ask the world what actually happened to ONE batch.

    Never claims, never sends and never refunds. Its whole job is to turn an
    ``unknown`` into something a human can act on: each slot's exact order is
    read back and its state is moved only where the readback proves the move.
    A batch stops being halted when — and only when — its slots stop being
    unresolved.
    """
    if request.batch_id is None:
        return StageReport(
            stage="reconcile",
            outcome="refused",
            reasons=[BATCH_UNKNOWN],
            external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
        )
    snapshot = await ledger_module.load(session_maker, batch_id=request.batch_id)
    if not snapshot.exists:
        return StageReport(
            stage="reconcile",
            outcome="refused",
            reasons=[BATCH_UNKNOWN],
            batch=snapshot.as_safe_dict(),
            external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
        )
    if snapshot.campaign_run_id != request.preview_run_id:
        return StageReport(
            stage="reconcile",
            outcome="refused",
            reasons=[BATCH_PREVIEW_MISMATCH],
            batch=snapshot.as_safe_dict(),
            external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
        )
    batch_id = request.batch_id

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
                batch_id=batch_id,
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
                parked = await _park_unresolved(session_maker, batch_id=batch_id, item=item, reason=slot_reasons[0])
                if parked.reason == ledger_module.RECORD_REFUSED_BUSY:
                    slot_reasons = [RECONCILE_BUSY]
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
                            batch_id=batch_id,
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
                        batch_id=batch_id,
                        slot=item.slot,
                        status=VOUCHER_PRODUCTION_ITEM_PAID,
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
                        batch_id=batch_id,
                        slot=item.slot,
                        status=VOUCHER_PRODUCTION_ITEM_REFUNDED,
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
                        session_maker,
                        batch_id=batch_id,
                        item=item,
                        reason=slot_reasons[0] if slot_reasons else ORDER_UNPROVEN,
                    )
                elif state in (ORDER_CANCELLED, ORDER_REFUNDED) and item.status == VOUCHER_PRODUCTION_ITEM_CREATED:
                    # The operator closed the draft by hand in the dashboard.
                    # That is an OBSERVATION, not something this tool did, and
                    # it is recorded as exactly that — with no second mutation.
                    await ledger_module.record_item_outcome(
                        session_maker,
                        batch_id=batch_id,
                        slot=item.slot,
                        status=VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
                        expected_statuses=frozenset({VOUCHER_PRODUCTION_ITEM_CREATED}),
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
            parked = await _park_unresolved(session_maker, batch_id=batch_id, item=item, reason=MUTATION_UNKNOWN)
            if parked.reason == ledger_module.RECORD_REFUSED_BUSY:
                # An executor is holding this claim. The readback has reinterpreted
                # nothing, and saying "unknown" here would be a claim about a slot
                # whose answer is still on its way.
                slot_reasons.append(RECONCILE_BUSY)
            else:
                slot_reasons.append(MUTATION_UNKNOWN)

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
    snapshot = await ledger_module.resettle(session_maker, batch_id=batch_id)

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


async def run_status(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int | None = None,
    preview_run_id: int | None = None,
) -> StageReport:
    """Where this phase stands. Read-only, no network, no approval needed.

    With no ``batch_id`` it lists every batch as a headline, which is how an
    operator finds the id they then have to type. With one it prints that
    batch in full. A preview id resolves through the unique constraint, so it
    can never pick "one of" several.

    Deliberately reachable with the fence shut: the moment an operator most
    needs to read what a halted batch left behind is just after an emergency
    `false`, and refusing to answer then would not be safety.
    """
    headlines = await ledger_module.list_batches(session_maker)
    snapshot = ledger_module.BatchSnapshot(exists=False)
    if batch_id is not None:
        snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    elif preview_run_id is not None:
        snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=preview_run_id)

    if batch_id is not None or preview_run_id is not None:
        outcome = "observed" if snapshot.exists else "not_started"
    else:
        outcome = "observed" if headlines else "not_started"

    return StageReport(
        stage="status",
        outcome=outcome,
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=any(entry.manual_cleanup_required for entry in snapshot.items),
        halted=snapshot.halted,
        batch=snapshot.as_safe_dict(),
        batches=[entry.as_safe_dict() for entry in headlines],
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
    )


__all__ = [
    "CREATE_WINDOW",
    "STAGE_ITEM_SOURCE_STATUSES",
    "ProductionRequest",
    "SlotResult",
    "StageReport",
    "VoucherMutator",
    "VoucherSender",
    "available_item_actions",
    "build_stage_plan",
    "run_create",
    "run_deliver",
    "run_freeze",
    "run_pay",
    "run_reconcile",
    "run_refund",
    "run_status",
]
