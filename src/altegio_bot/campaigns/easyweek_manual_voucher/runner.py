"""The operator stages of the manual-basis voucher delivery canary (§37.2).

One person, one voucher, one message — and between each pair of them, a human
who has to look at a plan and decide again. There is no function here that runs
two stages, and there cannot be: the stop between them IS the control.

The order of every acting stage is the same, and it is not negotiable:

1. rebuild the plan live and check the operator's approval against it;
2. re-prove the recipient, the template baseline and the order, from scratch;
3. take the row lock, check the transition, write the claim AND the attempt,
   commit;
4. only then make at most one external request;
5. record what it turned out to be as a compare-and-set.

Steps 3 and 4 in that order are the whole design. EasyWeek publishes no write
idempotency key and Meta will happily deliver twice, so a crash between the
commit and the response must read as "it may have happened" — which is the only
reading that cannot charge a card twice or message a person twice.

What this module refuses to know
--------------------------------
It never learns a phone number, a name or a voucher code for longer than one
call. The code exists in memory between one read of the paid order and one POST
to Meta; what survives is a keyed MAC of it. No report, no ledger column, no log
line and no exception here carries any of the three.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_manual_voucher import ledger as ledger_module
from altegio_bot.campaigns.easyweek_manual_voucher.authorisation import (
    StagePlan,
    stage_digest,
    verify_plan_authorisation,
)
from altegio_bot.campaigns.easyweek_manual_voucher.baseline import BaselineProof, prove_baseline
from altegio_bot.campaigns.easyweek_manual_voucher.eligibility import (
    ManualRecipientProof,
    prove_manual_recipient,
)
from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    APPLY_FLAG_MISSING,
    ARTIFACT_UNPROVEN,
    BASELINE_DRIFT,
    BINDING_MISMATCH,
    CANARY_SCOPE_ALREADY_CONSUMED,
    DELIVERY_ALREADY_ATTEMPTED,
    IDENTITY_BINDING_MISMATCH,
    KARLSRUHE_COMPANY_ID,
    LEDGER_IDENTITY_INCOMPLETE,
    LEDGER_STATE_UNEXPECTED,
    MANUAL_BASELINE_VERSION,
    MARKER_SEARCH_AMBIGUOUS,
    MARKER_SEARCH_INCOMPLETE,
    MARKER_SEARCH_UNRESOLVED,
    MUTATION_REJECTED,
    MUTATION_UNKNOWN,
    NEW_CLIENT_CAMPAIGN_CODE,
    ORDER_ALREADY_REFUNDED,
    ORDER_NOT_PAID,
    ORDER_NOT_PAYABLE,
    ORDER_STATE_UNATTRIBUTABLE,
    ORDER_UNPROVEN,
    REFUND_FORBIDDEN_AFTER_SEND,
    SNAPSHOT_NOT_FROZEN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
    STAGE_REFUND,
    TEMPLATE_PARAMETERS_UNPROVEN,
    UNKNOWN_STAGE,
    VOUCHER_TEMPLATE_CODE,
    manual_marker,
)
from altegio_bot.campaigns.easyweek_manual_voucher.readiness import (
    ManualPrerequisites,
    prove_prerequisites,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import MANUAL_VOUCHER_DOMAIN, voucher_code_mac
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
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
from altegio_bot.models.models import (
    MANUAL_VOUCHER_CREATE_CLAIMED,
    MANUAL_VOUCHER_CREATE_REJECTED,
    MANUAL_VOUCHER_CREATE_UNKNOWN,
    MANUAL_VOUCHER_CREATED,
    MANUAL_VOUCHER_MANUALLY_CLEANED,
    MANUAL_VOUCHER_PAID,
    MANUAL_VOUCHER_PAY_CLAIMED,
    MANUAL_VOUCHER_PAY_REJECTED,
    MANUAL_VOUCHER_PAY_UNKNOWN,
    MANUAL_VOUCHER_PLANNED,
    MANUAL_VOUCHER_PROVIDER_ACCEPTED,
    MANUAL_VOUCHER_REFUND_CLAIMED,
    MANUAL_VOUCHER_REFUND_REJECTED,
    MANUAL_VOUCHER_REFUND_UNKNOWN,
    MANUAL_VOUCHER_REFUNDED,
    MANUAL_VOUCHER_SEND_CLAIMED,
    MANUAL_VOUCHER_SEND_REJECTED,
    MANUAL_VOUCHER_SEND_UNKNOWN,
)
from altegio_bot.utils import utcnow

# How long after a claim a created order may have been opened. Bounded locally,
# because §35 proved the server-side date filters answer 422 for this shape.
CREATE_WINDOW = timedelta(minutes=30)

# Which ledger states each stage may be planned from. A stage planned from
# anywhere else is refused before anything is proven live.
STAGE_SOURCE_STATUSES: dict[str, frozenset[str | None]] = {
    STAGE_CREATE: frozenset({None, MANUAL_VOUCHER_PLANNED, MANUAL_VOUCHER_CREATE_REJECTED}),
    STAGE_PAY: frozenset({MANUAL_VOUCHER_CREATED, MANUAL_VOUCHER_PAY_REJECTED}),
    STAGE_DELIVER: frozenset({MANUAL_VOUCHER_PAID}),
    STAGE_REFUND: frozenset({MANUAL_VOUCHER_PAID, MANUAL_VOUCHER_PAY_UNKNOWN, MANUAL_VOUCHER_REFUND_REJECTED}),
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
    """The one Meta call this canary may make."""

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
class StageReport:
    """The PII-free result of one operator stage. Safe to print, always."""

    stage: str
    outcome: str
    reasons: list[str] = field(default_factory=list)
    external_effect_attempted: bool = False
    external_send_attempted: bool = False
    reconciliation_required: bool = False
    manual_cleanup_required: bool = False
    ledger: dict[str, Any] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    order_state: str | None = None
    baseline: dict[str, Any] | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "manual_voucher_stage",
            "stage": self.stage,
            "outcome": self.outcome,
            "reasons": list(dict.fromkeys(self.reasons)),
            # Named to answer the question an operator actually has: did
            # anything leave this process?
            "external_effect_attempted": self.external_effect_attempted,
            "external_send_attempted": self.external_send_attempted,
            "reconciliation_required": self.reconciliation_required,
            "manual_cleanup_required": self.manual_cleanup_required,
            "order_state": self.order_state,
            "baseline": dict(self.baseline) if self.baseline is not None else None,
            "observations": list(self.observations),
            "ledger": dict(self.ledger),
            "recipient_basis": "operator_manual_selection",
            "first_visit_proof": "not_applicable",
            # Repeated verbatim on every stage, success included.
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
            "ready_for_send": False,
            "raw_identifiers_omitted": True,
            "voucher_code_omitted": True,
        }


@dataclass(frozen=True)
class ManualCanaryRequest:
    """What the operator named on the command line, plus the frozen identity."""

    preview_run_id: int
    campaign_recipient_id: int
    sender_code: str
    staffer_uuid: str
    payment_account_uuid: str
    company_id: int = KARLSRUHE_COMPANY_ID
    location_uuid: str = KARLSRUHE_LOCATION_UUID
    voucher_template_uuid: str = EASYWEEK_VOUCHER_TEMPLATE_UUID

    @property
    def marker(self) -> str:
        return manual_marker(
            preview_run_id=self.preview_run_id,
            campaign_recipient_id=self.campaign_recipient_id,
        )


def _voucher_code(payload: object) -> str | None:
    """The one issued code, in memory, or ``None``.

    Read through the same §35 proof the payment gate uses, so a body this canary
    would refuse to pay for is also a body it refuses to read a code out of.
    """
    order = order_object(payload)
    if order is None:
        return None
    proof = prove_voucher_line(
        order,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    if not proof.proven:
        return None
    vouchers = order.get("vouchers")
    if isinstance(vouchers, list) and len(vouchers) == 1 and isinstance(vouchers[0], dict):
        code = vouchers[0].get("code")
        return code if isinstance(code, str) and code else None
    return None


def _identity_from(request: ManualCanaryRequest, proof: ManualRecipientProof) -> ledger_module.ManualCanaryIdentity:
    assert proof.easyweek_customer_uuid is not None
    assert proof.campaign_period_start is not None
    assert proof.campaign_period_end is not None
    return ledger_module.ManualCanaryIdentity(
        company_id=request.company_id,
        campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
        campaign_run_id=request.preview_run_id,
        campaign_recipient_id=request.campaign_recipient_id,
        easyweek_customer_uuid=proof.easyweek_customer_uuid,
        campaign_period_start=proof.campaign_period_start,
        campaign_period_end=proof.campaign_period_end,
        location_uuid=request.location_uuid,
        staffer_uuid=request.staffer_uuid,
        payment_account_uuid=request.payment_account_uuid,
        voucher_template_uuid=request.voucher_template_uuid,
        reconciliation_marker=request.marker,
        baseline_version=MANUAL_BASELINE_VERSION,
    )


def _identity_from_snapshot(
    snapshot: ledger_module.LedgerSnapshot,
) -> ledger_module.ManualCanaryIdentity | None:
    """Rebuild the identity the ledger was opened with, or refuse.

    A row missing any part of it cannot be acted on: the claim compares the
    identity field by field, and an identity assembled out of defaults would
    compare equal to something nobody approved.
    """
    required = (
        snapshot.company_id,
        snapshot.campaign_code,
        snapshot.campaign_run_id,
        snapshot.campaign_recipient_id,
        snapshot.easyweek_customer_uuid,
        snapshot.campaign_period_start,
        snapshot.campaign_period_end,
        snapshot.location_uuid,
        snapshot.staffer_uuid,
        snapshot.payment_account_uuid,
        snapshot.voucher_template_uuid,
        snapshot.reconciliation_marker,
        snapshot.baseline_version,
    )
    if any(value is None for value in required):
        return None
    assert snapshot.campaign_period_start is not None
    assert snapshot.campaign_period_end is not None
    return ledger_module.ManualCanaryIdentity(
        company_id=int(snapshot.company_id or 0),
        campaign_code=str(snapshot.campaign_code),
        campaign_run_id=int(snapshot.campaign_run_id or 0),
        campaign_recipient_id=int(snapshot.campaign_recipient_id or 0),
        easyweek_customer_uuid=str(snapshot.easyweek_customer_uuid),
        campaign_period_start=datetime.fromisoformat(snapshot.campaign_period_start),
        campaign_period_end=datetime.fromisoformat(snapshot.campaign_period_end),
        location_uuid=str(snapshot.location_uuid),
        staffer_uuid=str(snapshot.staffer_uuid),
        payment_account_uuid=str(snapshot.payment_account_uuid),
        voucher_template_uuid=str(snapshot.voucher_template_uuid),
        reconciliation_marker=str(snapshot.reconciliation_marker),
        baseline_version=str(snapshot.baseline_version),
    )


def _refusal(
    stage: str,
    reasons: tuple[str, ...] | list[str],
    snapshot: ledger_module.LedgerSnapshot,
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
        manual_cleanup_required=snapshot.manual_cleanup_required,
        ledger=snapshot.as_safe_dict(),
        baseline=baseline.as_safe_dict() if baseline is not None else None,
    )


async def _exact_order(order_reader: Any, order_uuid: str | None) -> tuple[object | None, str | None]:
    """One exact GET of the ledger's order, or a reason it proves nothing."""
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


async def _order_preconditions(
    order_reader: Any,
    *,
    stage: str,
    snapshot: ledger_module.LedgerSnapshot,
    expected_customer_uuid: str,
) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    """What the remote order must look like for this stage.

    The customer to expect is passed in rather than read off a live proof: a
    refund has no live proof by design, and the customer it must match is the
    one the ledger was opened with.
    """
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    payload, order_reason = await _exact_order(order_reader, snapshot.target_order_uuid)
    if order_reason is not None:
        return [order_reason], None, observations

    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage=f"{stage}_plan_readback",
        expected_customer_uuid=expected_customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    observations.append(observation.as_safe_dict())

    if order.get("comment") != snapshot.reconciliation_marker:
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
                expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
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


async def build_stage_plan(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: ManualCanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    now: datetime | None = None,
    enabled: bool | None = None,
) -> tuple[StagePlan, ManualRecipientProof, ManualPrerequisites, BaselineProof]:
    """Re-prove everything THIS stage depends on. Reads only; mutates nothing.

    Creates no ledger row, sends no request and writes nothing. Every failure is
    one of §37.2's stable reason codes, and an unready plan still prints, because
    an operator needs to see WHICH fact is missing.
    """
    issued_at = now or utcnow()
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    if stage not in (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND):
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
    ledger_state: dict[str, Any] = {
        "status": snapshot.status,
        "target_order_known": snapshot.target_order_uuid is not None,
        "send_attempt_count": snapshot.send_attempt_count,
        "allowed_source_statuses": sorted(value or "none" for value in STAGE_SOURCE_STATUSES.get(stage, frozenset())),
    }
    if snapshot.status not in STAGE_SOURCE_STATUSES.get(stage, frozenset()):
        reasons.append(LEDGER_STATE_UNEXPECTED)

    # A row for a different recipient means this canary is already spent.
    if snapshot.exists and (
        snapshot.campaign_run_id != request.preview_run_id
        or snapshot.campaign_recipient_id != request.campaign_recipient_id
    ):
        reasons.append(CANARY_SCOPE_ALREADY_CONSUMED)

    # The live guard, before every stage that can reach a person — and
    # deliberately NOT before a refund.
    #
    # A refund sends nothing to anybody. Requiring a live customer read, a
    # current phone, a current name or the absence of an opt-out would mean the
    # cleanup path stops working at exactly the moments it is most needed: the
    # customer opted out, changed their number, or EasyWeek is answering 500.
    # Every one of those is a reason to GET THE MONEY BACK, not a reason to
    # leave €15 out there. What a refund proves instead is the order: the exact
    # UUID, the marker, the customer binding recorded in the ledger, the paid
    # state, a single claim, and that nothing was ever sent.
    if stage == STAGE_REFUND:
        proof = ManualRecipientProof(
            proven=False,
            reasons=(),
            proven_at=issued_at,
            company_id=request.company_id,
            campaign_run_id=request.preview_run_id,
            campaign_recipient_id=request.campaign_recipient_id,
            checks={"live_guard_applied": False},
        )
    else:
        proof = await prove_manual_recipient(
            session,
            preview_run_id=request.preview_run_id,
            campaign_recipient_id=request.campaign_recipient_id,
            client_reader=reader,
            now=issued_at,
        )
        reasons.extend(proof.reasons)

    # The template baseline, before every stage that will touch money or a
    # phone. A refund reads it too — for the report — but a drift does not stop
    # it: the money should come back either way.
    baseline, baseline_reasons = await _baseline_now(order_reader)
    if stage != STAGE_REFUND:
        reasons.extend(baseline_reasons)

    identity_bound = True
    if snapshot.exists and proof.proven:
        identity_bound = _identity_from(request, proof).matches(snapshot)
        if not identity_bound:
            reasons.append(IDENTITY_BINDING_MISMATCH)
    elif stage == STAGE_REFUND and snapshot.exists:
        # The refund still has to be about the row the operator named, even
        # though it does not re-prove the person behind it.
        identity_bound = (
            snapshot.campaign_run_id == request.preview_run_id
            and snapshot.campaign_recipient_id == request.campaign_recipient_id
            and snapshot.reconciliation_marker == request.marker
        )
        if not identity_bound:
            reasons.append(IDENTITY_BINDING_MISMATCH)

    order_state: str | None = None
    if stage in (STAGE_PAY, STAGE_DELIVER, STAGE_REFUND):
        stage_reasons, order_state, stage_observations = await _order_preconditions(
            order_reader,
            stage=stage,
            snapshot=snapshot,
            expected_customer_uuid=(proof.easyweek_customer_uuid or snapshot.easyweek_customer_uuid or ""),
        )
        reasons.extend(stage_reasons)
        observations.extend(stage_observations)

    if stage == STAGE_DELIVER and snapshot.send_attempt_count:
        reasons.append(DELIVERY_ALREADY_ATTEMPTED)
    if stage == STAGE_REFUND and snapshot.status in ledger_module.SENT_STATUSES:
        reasons.append(REFUND_FORBIDDEN_AFTER_SEND)

    snapshot_facts: dict[str, Any] = {
        "stage": stage,
        "company_id": request.company_id,
        "campaign_code": NEW_CLIENT_CAMPAIGN_CODE,
        "preview_run_id": request.preview_run_id,
        "campaign_recipient_id": request.campaign_recipient_id,
        "location_uuid": request.location_uuid,
        "voucher_template_uuid": request.voucher_template_uuid,
        "price_minor": SUPPORTED_VOUCHER_PRICE_MINOR,
        "reconciliation_marker": request.marker,
        "prerequisites": prerequisites.as_safe_dict(),
        "recipient": proof.as_safe_dict(),
        # Stated rather than implied: a refund's report must not read as though
        # a live guard passed when none was run.
        "live_guard_applied": stage != STAGE_REFUND,
        "baseline": baseline.as_safe_dict(),
        "identity_binding_proven": identity_bound,
        "order_state": order_state,
    }
    ledger_state["order_state"] = order_state

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
    return plan, proof, prerequisites, baseline


async def _authorise(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: ManualCanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> tuple[
    StagePlan | None,
    ManualRecipientProof | None,
    ManualPrerequisites | None,
    BaselineProof | None,
    StageReport | None,
]:
    """Rebuild the plan live and check the approval. A report means: refused."""
    plan, proof, prerequisites, baseline = await build_stage_plan(
        session,
        session_maker,
        stage=stage,
        request=request,
        reader=reader,
        order_reader=order_reader,
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
    return plan, proof, prerequisites, baseline, None


async def run_create(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ManualCanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Create the ONE open order with the ONE voucher line."""
    plan, proof, _prereq, baseline, refused = await _authorise(
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
    assert plan is not None and proof is not None and baseline is not None

    identity = _identity_from(request, proof)
    # The row exists before the claim: the entitlement constraint has to be
    # protecting this person before anything can be charged for them. Opening it
    # re-proves the run and the recipient under the editor's own row lock, so a
    # Remove that won the race refuses the canary here — before CREATE, and with
    # nothing sent.
    opened = await ledger_module.open_canary(session_maker, identity=identity)
    if not opened.exists:
        return _refusal(STAGE_CREATE, [SNAPSHOT_NOT_FROZEN], opened, baseline=baseline)

    window_start = utcnow()
    claim = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        plan_digest=plan.digest,
        create_window_start=window_start - CREATE_WINDOW,
        create_window_end=window_start + CREATE_WINDOW,
    )
    if not claim.granted:
        snapshot = await ledger_module.load(session_maker)
        return _refusal(STAGE_CREATE, [claim.reason], snapshot, baseline=baseline)

    # Committed. From here on, whatever happens, something MAY have reached
    # EasyWeek, and nothing in this process may assume otherwise.
    assert proof.easyweek_customer_uuid is not None
    try:
        response = await mutator.create_voucher_order(
            location_uuid=request.location_uuid,
            customer_uuid=proof.easyweek_customer_uuid,
            staffer_uuid=request.staffer_uuid,
            voucher_template_uuid=request.voucher_template_uuid,
            price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
            marker=request.marker,
        )
    except EasyWeekVoucherMutationUnknown:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_CREATE_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            reconciliation_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome="unknown",
            reasons=[MUTATION_UNKNOWN],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )
    except EasyWeekError:
        # A proven pre-action refusal: the server said no before doing anything,
        # so this stage — and only this stage — may be claimed again.
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_CREATE_REJECTED,
            expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            reconciliation_required=False,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome="rejected",
            reasons=[MUTATION_REJECTED],
            external_effect_attempted=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )

    return await _verify_created(
        session_maker,
        request=request,
        proof=proof,
        order_reader=order_reader,
        response=response,
        baseline=baseline,
    )


async def _verify_created(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ManualCanaryRequest,
    proof: ManualRecipientProof,
    order_reader: Any,
    response: VoucherMutationResponse,
    baseline: BaselineProof,
) -> StageReport:
    """A 2xx is a claim, not a proof. Read the order back and decide.

    Only a readback that names our marker, our customer and one provable
    voucher line turns the ledger to ``created``. Anything else is
    ``create_unknown`` with the candidate recorded, because "we could not prove
    it" and "it did not happen" are different sentences and only one of them is
    true.
    """
    # The order identity comes out of the parsed body, at whatever level
    # EasyWeek put it. A 2xx without a canonical uuid is a claim we cannot even
    # address, let alone prove.
    envelope = order_object(response.envelope)
    candidate = canonical_uuid(envelope.get("uuid")) if envelope else None
    if candidate is None:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_CREATE_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_CLAIMED}),
            reason_code=ORDER_UNPROVEN,
            reconciliation_required=True,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome="unknown",
            reasons=[ORDER_UNPROVEN],
            external_effect_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )

    payload, order_reason = await _exact_order(order_reader, candidate)
    order = order_object(payload) or {}
    observation = observe_artifact(
        payload,
        stage="create_readback",
        expected_customer_uuid=proof.easyweek_customer_uuid or "",
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    marker_ok = order.get("comment") == request.marker
    proven = (
        order_reason is None
        and marker_ok
        and observation.order_customer_binding_proven
        and observation.voucher_line_proven
        and classify_order(payload)[0] == ORDER_OPEN
    )

    if not proven:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_CREATE_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_CLAIMED}),
            reason_code=order_reason or ORDER_UNPROVEN,
            target_order_uuid=candidate,
            reconciliation_required=True,
            manual_cleanup_required=True,
            evidence={"create_readback": observation.as_safe_dict()},
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome="unknown",
            reasons=[order_reason or ORDER_UNPROVEN],
            external_effect_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            observations=[observation.as_safe_dict()],
            baseline=baseline.as_safe_dict(),
        )

    # The code exists only here, between this readback and this MAC.
    code = _voucher_code(payload)
    if code is None:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_CREATE_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_CLAIMED}),
            reason_code=ARTIFACT_UNPROVEN,
            target_order_uuid=candidate,
            reconciliation_required=True,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome="unknown",
            reasons=[ARTIFACT_UNPROVEN],
            external_effect_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )
    key_id, mac = voucher_code_mac(
        voucher_code=code,
        ledger_uuid=ledger_module.MANUAL_VOUCHER_SCOPE,
        target_order_uuid=candidate,
        voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        domain=MANUAL_VOUCHER_DOMAIN,
    )
    del code

    # The template as it reads AFTER the voucher was issued. Counters move —
    # that is the product working — so a moved counter is not a drift; a changed
    # frozen field is, and it is reported rather than absorbed.
    after_baseline, _ = await _baseline_now(order_reader)

    outcome = await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_CREATED,
        expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_CLAIMED}),
        target_order_uuid=candidate,
        voucher_code_hmac=mac,
        hmac_key_id=key_id,
        verified_field="create_verified_at",
        reconciliation_required=False,
        manual_cleanup_required=True,
        evidence={
            "create_readback": observation.as_safe_dict(),
            "baseline_after_create": after_baseline.as_safe_dict(),
        },
    )
    return StageReport(
        stage=STAGE_CREATE,
        outcome="created",
        reasons=[] if after_baseline.proven else [BASELINE_DRIFT],
        external_effect_attempted=True,
        manual_cleanup_required=True,
        ledger=outcome.snapshot.as_safe_dict(),
        observations=[observation.as_safe_dict()],
        order_state=ORDER_OPEN,
        baseline=after_baseline.as_safe_dict(),
    )


async def run_pay(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ManualCanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Pay for the ONE proven order, once. Real money, once."""
    plan, proof, _prereq, baseline, refused = await _authorise(
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
    assert plan is not None and proof is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    if identity is None:
        return _refusal(STAGE_PAY, [LEDGER_IDENTITY_INCOMPLETE], snapshot, baseline=baseline)
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_PAY, [ORDER_UNPROVEN], snapshot, baseline=baseline)

    # The code must still be the one this row was bound to at CREATE. Checked
    # before the payment and not only before the send: paying for an order whose
    # voucher changed underneath us buys something else.
    payload, order_reason = await _exact_order(order_reader, snapshot.target_order_uuid)
    if order_reason is not None:
        return _refusal(STAGE_PAY, [order_reason], snapshot, baseline=baseline)
    code = _voucher_code(payload)
    if code is None or not await ledger_module.binding_matches(
        session_maker, voucher_code=code, target_order_uuid=snapshot.target_order_uuid
    ):
        del code
        return _refusal(STAGE_PAY, [BINDING_MISMATCH], snapshot, baseline=baseline)
    del code

    claim = await ledger_module.claim_pay(session_maker, identity=identity, plan_digest=plan.digest)
    if not claim.granted:
        snapshot = await ledger_module.load(session_maker)
        return _refusal(STAGE_PAY, [claim.reason], snapshot, baseline=baseline)

    try:
        await mutator.pay_voucher_order(
            order_uuid=snapshot.target_order_uuid,
            account_uuid=request.payment_account_uuid,
        )
    except EasyWeekVoucherMutationUnknown:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_PAY_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_PAY_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            reconciliation_required=True,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome="unknown",
            reasons=[MUTATION_UNKNOWN],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )
    except EasyWeekError:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_PAY_REJECTED,
            expected_statuses=frozenset({MANUAL_VOUCHER_PAY_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            reconciliation_required=False,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome="rejected",
            reasons=[MUTATION_REJECTED],
            external_effect_attempted=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )

    # A 2xx is a claim. Only a readback showing the exact order paid proves it.
    payload, order_reason = await _exact_order(order_reader, snapshot.target_order_uuid)
    state = classify_order(payload)[0] if order_reason is None else None
    # The template as it reads AFTER the money moved. A drift here is reported,
    # never absorbed, and never a reason to block the refund below.
    after_baseline, _ = await _baseline_now(order_reader)
    if state != ORDER_PAID:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_PAY_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_PAY_CLAIMED}),
            reason_code=order_reason or ORDER_NOT_PAID,
            reconciliation_required=True,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome="unknown",
            reasons=[order_reason or ORDER_NOT_PAID],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            order_state=state,
            baseline=after_baseline.as_safe_dict(),
        )

    observation = observe_artifact(
        payload,
        stage="pay_readback",
        expected_customer_uuid=proof.easyweek_customer_uuid or "",
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    # `paid` is a whole identity, not a status word: this exact order, our
    # marker, the customer the ledger names, one provable voucher line at the
    # approved template and price, and a code that still matches the binding.
    order = order_object(payload) or {}
    identity_proven = (
        order.get("comment") == snapshot.reconciliation_marker
        and observation.order_customer_binding_proven
        and observation.voucher_line_proven
    )
    binding_proven = False
    if identity_proven:
        settled_code = _voucher_code(payload)
        if settled_code is not None:
            binding_proven = await ledger_module.binding_matches(
                session_maker,
                voucher_code=settled_code,
                target_order_uuid=snapshot.target_order_uuid,
            )
            del settled_code

    if not (identity_proven and binding_proven):
        # The money has probably moved and the artifact is not proven. Saying
        # `paid` here would open DELIVER on an unproven voucher; saying nothing
        # would strand the €15. So: an honest uncertainty that keeps the
        # pre-send refund reachable and the send shut.
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_PAY_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_PAY_CLAIMED}),
            reason_code=ARTIFACT_UNPROVEN,
            reconciliation_required=True,
            evidence={
                "pay_readback": observation.as_safe_dict(),
                "baseline_after_pay": after_baseline.as_safe_dict(),
            },
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome="unknown",
            reasons=[ARTIFACT_UNPROVEN],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            observations=[observation.as_safe_dict()],
            order_state=state,
            baseline=after_baseline.as_safe_dict(),
        )

    outcome = await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_PAID,
        expected_statuses=frozenset({MANUAL_VOUCHER_PAY_CLAIMED}),
        verified_field="pay_verified_at",
        reconciliation_required=False,
        manual_cleanup_required=False,
        evidence={
            "pay_readback": observation.as_safe_dict(),
            "baseline_after_pay": after_baseline.as_safe_dict(),
        },
    )
    return StageReport(
        stage=STAGE_PAY,
        outcome="paid",
        # A drift observed after the payment is stated, not swallowed — and it
        # does not undo a payment that is otherwise fully proven.
        reasons=[] if after_baseline.proven else [BASELINE_DRIFT],
        external_effect_attempted=True,
        ledger=outcome.snapshot.as_safe_dict(),
        observations=[observation.as_safe_dict()],
        order_state=ORDER_PAID,
        baseline=after_baseline.as_safe_dict(),
    )


async def run_deliver(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ManualCanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    sender: VoucherSender,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Send the ONE message. One attempt, ever, enforced by the schema."""
    plan, proof, prerequisites, baseline, refused = await _authorise(
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
    assert plan is not None and proof is not None and prerequisites is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    if identity is None:
        return _refusal(STAGE_DELIVER, [LEDGER_IDENTITY_INCOMPLETE], snapshot, baseline=baseline)
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_DELIVER, [ORDER_UNPROVEN], snapshot, baseline=baseline)

    # The paid order is read once, here. The code lives from this line to the
    # POST below and nowhere else — not in the ledger, not in the report, not in
    # an exception, not in a log.
    payload, order_reason = await _exact_order(order_reader, snapshot.target_order_uuid)
    if order_reason is not None:
        return _refusal(STAGE_DELIVER, [order_reason], snapshot, baseline=baseline)
    if classify_order(payload)[0] != ORDER_PAID:
        return _refusal(STAGE_DELIVER, [ORDER_NOT_PAID], snapshot, baseline=baseline)
    code = _voucher_code(payload)
    if code is None:
        return _refusal(STAGE_DELIVER, [ARTIFACT_UNPROVEN], snapshot, baseline=baseline)
    if not await ledger_module.binding_matches(
        session_maker, voucher_code=code, target_order_uuid=snapshot.target_order_uuid
    ):
        del code
        return _refusal(STAGE_DELIVER, [BINDING_MISMATCH], snapshot, baseline=baseline)

    assert proof.proven_at is not None
    claim = await ledger_module.claim_send(
        session_maker,
        identity=identity,
        plan_digest=plan.digest,
        live_guard_reproven_at=proof.proven_at,
        template_code=VOUCHER_TEMPLATE_CODE,
        meta_template_name=prerequisites.meta_template_name or "",
        template_language=prerequisites.template_language or "",
        sender_id=prerequisites.sender_id,
    )
    if not claim.granted:
        del code
        snapshot = await ledger_module.load(session_maker)
        return _refusal(STAGE_DELIVER, [claim.reason], snapshot, baseline=baseline)

    # Committed, with the attempt counter already at one. There is no second
    # attempt to fall back on and no code path that could take one.
    assert proof.destination_phone is not None and proof.client_display_name is not None
    # The approved template is POSITIONAL with exactly three BODY parameters:
    # client_name, voucher_code, booking_link. All three are built here and all
    # three must be non-empty — a message with an empty slot is not the message
    # Meta approved, and an empty link is a link to nowhere in a real person's
    # WhatsApp. Checked again at this last moment because the claim is already
    # committed and this is the final gate before the socket.
    params = [proof.client_display_name, code, prerequisites.booking_link or ""]
    if not all(part for part in params):
        del code
        recorded = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_SEND_REJECTED,
            expected_statuses=frozenset({MANUAL_VOUCHER_SEND_CLAIMED}),
            reason_code=TEMPLATE_PARAMETERS_UNPROVEN,
            reconciliation_required=True,
            attempt_outcome="rejected",
        )
        return StageReport(
            stage=STAGE_DELIVER,
            outcome="refused",
            reasons=[TEMPLATE_PARAMETERS_UNPROVEN],
            # The claim was committed, so the attempt is spent — but nothing
            # left this process.
            external_effect_attempted=False,
            reconciliation_required=True,
            ledger=recorded.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )
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
        recorded = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_PROVIDER_ACCEPTED,
            expected_statuses=frozenset({MANUAL_VOUCHER_SEND_CLAIMED}),
            provider_message_id=outcome_meta.provider_message_id,
            reconciliation_required=False,
            attempt_outcome="provider_accepted",
        )
        return StageReport(
            stage=STAGE_DELIVER,
            # Accepted by Meta. NOT delivered: that word belongs to a webhook.
            outcome="provider_accepted",
            external_effect_attempted=True,
            external_send_attempted=True,
            ledger=recorded.snapshot.as_safe_dict(),
            order_state=ORDER_PAID,
            baseline=baseline.as_safe_dict(),
        )

    if outcome_meta.outcome == DELIVERY_REJECTED:
        recorded = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_SEND_REJECTED,
            expected_statuses=frozenset({MANUAL_VOUCHER_SEND_CLAIMED}),
            reason_code=outcome_meta.reason,
            reconciliation_required=True,
            attempt_outcome="rejected",
        )
        return StageReport(
            stage=STAGE_DELIVER,
            outcome="rejected",
            reasons=[outcome_meta.reason or MUTATION_REJECTED],
            external_effect_attempted=True,
            external_send_attempted=True,
            # A proven refusal still ends this canary's one attempt. It is not a
            # reason to try again; it is a reason for a human to look.
            reconciliation_required=True,
            ledger=recorded.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )

    recorded = await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_SEND_UNKNOWN,
        expected_statuses=frozenset({MANUAL_VOUCHER_SEND_CLAIMED}),
        reason_code=outcome_meta.reason,
        reconciliation_required=True,
        attempt_outcome="unknown",
    )
    return StageReport(
        stage=STAGE_DELIVER,
        outcome="unknown",
        reasons=[outcome_meta.reason or MUTATION_UNKNOWN],
        external_effect_attempted=True,
        external_send_attempted=True,
        reconciliation_required=True,
        ledger=recorded.snapshot.as_safe_dict(),
        baseline=baseline.as_safe_dict(),
    )


async def run_refund(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ManualCanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    apply: bool,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    enabled: bool | None = None,
) -> StageReport:
    """Take the money back — and only ever before anything was sent."""
    plan, _proof, _prereq, baseline, refused = await _authorise(
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
        enabled=enabled,
    )
    if refused is not None:
        return refused
    assert plan is not None and baseline is not None

    snapshot = await ledger_module.load(session_maker)
    identity = _identity_from_snapshot(snapshot)
    if identity is None:
        return _refusal(STAGE_REFUND, [LEDGER_IDENTITY_INCOMPLETE], snapshot, baseline=baseline)
    if snapshot.status in ledger_module.SENT_STATUSES:
        return _refusal(STAGE_REFUND, [REFUND_FORBIDDEN_AFTER_SEND], snapshot, baseline=baseline)
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_REFUND, [ORDER_UNPROVEN], snapshot, baseline=baseline)

    claim = await ledger_module.claim_refund(session_maker, identity=identity, plan_digest=plan.digest)
    if not claim.granted:
        snapshot = await ledger_module.load(session_maker)
        return _refusal(STAGE_REFUND, [claim.reason], snapshot, baseline=baseline)

    try:
        await mutator.refund_voucher_order(order_uuid=snapshot.target_order_uuid)
    except EasyWeekVoucherMutationUnknown:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_REFUND_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_REFUND_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            reconciliation_required=True,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome="unknown",
            reasons=[MUTATION_UNKNOWN],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )
    except EasyWeekError:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_REFUND_REJECTED,
            expected_statuses=frozenset({MANUAL_VOUCHER_REFUND_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            reconciliation_required=True,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome="rejected",
            reasons=[MUTATION_REJECTED],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            baseline=baseline.as_safe_dict(),
        )

    payload, order_reason = await _exact_order(order_reader, snapshot.target_order_uuid)
    state = classify_order(payload)[0] if order_reason is None else None
    if state != ORDER_REFUNDED:
        outcome = await ledger_module.record_outcome(
            session_maker,
            status=MANUAL_VOUCHER_REFUND_UNKNOWN,
            expected_statuses=frozenset({MANUAL_VOUCHER_REFUND_CLAIMED}),
            reason_code=order_reason or MUTATION_UNKNOWN,
            reconciliation_required=True,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome="unknown",
            reasons=[order_reason or MUTATION_UNKNOWN],
            external_effect_attempted=True,
            reconciliation_required=True,
            ledger=outcome.snapshot.as_safe_dict(),
            order_state=state,
            baseline=baseline.as_safe_dict(),
        )

    after_baseline, _ = await _baseline_now(order_reader)
    outcome = await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_REFUNDED,
        expected_statuses=frozenset({MANUAL_VOUCHER_REFUND_CLAIMED}),
        verified_field="refund_verified_at",
        reconciliation_required=False,
        manual_cleanup_required=False,
        evidence={"baseline_after_refund": after_baseline.as_safe_dict()},
    )
    return StageReport(
        stage=STAGE_REFUND,
        outcome="refunded",
        external_effect_attempted=True,
        ledger=outcome.snapshot.as_safe_dict(),
        order_state=ORDER_REFUNDED,
        baseline=after_baseline.as_safe_dict(),
    )


async def _resolve_unknown_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    snapshot: ledger_module.LedgerSnapshot,
    order_reader: Any,
    baseline: BaselineProof,
) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    """Find the order an unknown CREATE may have left behind. Reads only.

    Never re-creates. The walk is scoped by branch and customer exactly as §35
    proved it must be — no staffer filter, no server-side date filter — and the
    marker, the bounded window and the customer are proven locally, row by row.

    What it may conclude is narrow on purpose:

    * one match, complete walk → read that exact UUID back and prove it fully;
    * zero matches → unresolved. Not "it was not created": the walk may simply
      not have seen it, and a create we cannot see is not a create we can deny;
    * several matches, or an incomplete walk → a full stop for a human.

    Being open is not enough to promote the row. The singleton artifact has to
    be provable and the code re-bound, because everything downstream — the
    payment gate and the send — verifies against that binding.
    """
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    if (
        snapshot.easyweek_customer_uuid is None
        or snapshot.location_uuid is None
        or snapshot.reconciliation_marker is None
        or snapshot.create_window_start is None
        or snapshot.create_window_end is None
    ):
        return [LEDGER_IDENTITY_INCOMPLETE], None, observations

    try:
        match = await find_marker_orders(
            order_reader,
            location_uuid=snapshot.location_uuid,
            customer_uuid=snapshot.easyweek_customer_uuid,
            marker=snapshot.reconciliation_marker,
            window_start=datetime.fromisoformat(snapshot.create_window_start),
            window_end=datetime.fromisoformat(snapshot.create_window_end),
        )
    except Exception:  # noqa: BLE001 - an unread listing proves nothing
        return [MARKER_SEARCH_INCOMPLETE], None, observations

    observations.append(
        {
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
    state = classify_order(payload)[0]
    observation = observe_artifact(
        payload,
        stage="create_marker_readback",
        expected_customer_uuid=snapshot.easyweek_customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    observations.append(observation.as_safe_dict())

    proven = (
        order.get("comment") == snapshot.reconciliation_marker
        and observation.order_customer_binding_proven
        and observation.voucher_line_proven
        and state == ORDER_OPEN
    )
    if not proven:
        reasons.append(ORDER_UNPROVEN if state == ORDER_OPEN else ORDER_STATE_UNATTRIBUTABLE)
        return reasons, state, observations

    # The artifact is provable, so the code is readable — and the binding has to
    # be restored before the row is promoted. A `created` row with no MAC would
    # be a row the payment gate cannot verify and the send would refuse anyway.
    code = _voucher_code(payload)
    if code is None:
        return [ARTIFACT_UNPROVEN], state, observations
    key_id, mac = voucher_code_mac(
        voucher_code=code,
        ledger_uuid=ledger_module.MANUAL_VOUCHER_SCOPE,
        target_order_uuid=candidate,
        voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        domain=MANUAL_VOUCHER_DOMAIN,
    )
    del code

    await ledger_module.record_outcome(
        session_maker,
        status=MANUAL_VOUCHER_CREATED,
        expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_UNKNOWN}),
        target_order_uuid=candidate,
        voucher_code_hmac=mac,
        hmac_key_id=key_id,
        verified_field="create_verified_at",
        reconciliation_required=False,
        manual_cleanup_required=True,
        evidence={
            "create_marker_readback": observation.as_safe_dict(),
            "baseline": baseline.as_safe_dict(),
        },
    )
    return reasons, state, observations


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: ManualCanaryRequest,
    order_reader: Any,
) -> StageReport:
    """Read-only. Ask the world what actually happened, and record only proof.

    Never claims, never sends and never refunds. Its whole job is to turn an
    ``unknown`` into something a human can act on: the exact order is read back
    and the ledger is moved only where the readback proves the move.
    """
    snapshot = await ledger_module.load(session_maker)
    if not snapshot.exists:
        return StageReport(
            stage="reconcile",
            outcome="nothing_to_reconcile",
            ledger=snapshot.as_safe_dict(),
        )

    baseline, _ = await _baseline_now(order_reader)
    observations: list[dict[str, Any]] = []
    reasons: list[str] = []
    state: str | None = None

    if snapshot.target_order_uuid is None:
        if snapshot.status == MANUAL_VOUCHER_CREATE_UNKNOWN:
            # A create whose answer was lost before it named an order. Look for
            # it by marker rather than guessing, and never send a second CREATE.
            search_reasons, state, search_observations = await _resolve_unknown_create(
                session_maker,
                snapshot=snapshot,
                order_reader=order_reader,
                baseline=baseline,
            )
            reasons.extend(search_reasons)
            observations.extend(search_observations)
    else:
        payload, order_reason = await _exact_order(order_reader, snapshot.target_order_uuid)
        if order_reason is not None:
            reasons.append(order_reason)
        else:
            state = classify_order(payload)[0]
            order = order_object(payload) or {}
            observation = observe_artifact(
                payload,
                stage="reconcile_readback",
                expected_customer_uuid=snapshot.easyweek_customer_uuid or "",
                expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
                expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
            )
            observations.append(observation.as_safe_dict())

            # A state alone never promotes a row. What promotes it is the whole
            # identity: this exact UUID, our marker, the customer the ledger
            # names, one provable voucher line at the approved template and
            # price, and a code that still matches the stored binding.
            identity_proven = (
                order.get("comment") == snapshot.reconciliation_marker
                and observation.order_customer_binding_proven
                and observation.voucher_line_proven
            )
            binding_proven = False
            if identity_proven:
                code = _voucher_code(payload)
                if code is not None:
                    binding_proven = await ledger_module.binding_matches(
                        session_maker,
                        voucher_code=code,
                        target_order_uuid=snapshot.target_order_uuid,
                    )
                    del code
            proven = identity_proven and binding_proven
            if not proven:
                reasons.append(ARTIFACT_UNPROVEN)

            # Only the transitions a readback PROVES, and only forwards.
            if state == ORDER_OPEN and snapshot.status == MANUAL_VOUCHER_CREATE_UNKNOWN and proven:
                await ledger_module.record_outcome(
                    session_maker,
                    status=MANUAL_VOUCHER_CREATED,
                    expected_statuses=frozenset({MANUAL_VOUCHER_CREATE_UNKNOWN}),
                    verified_field="create_verified_at",
                    reconciliation_required=False,
                    manual_cleanup_required=True,
                    evidence={
                        "reconcile_readback": observation.as_safe_dict(),
                        "baseline": baseline.as_safe_dict(),
                    },
                )
            elif state == ORDER_PAID and snapshot.status == MANUAL_VOUCHER_PAY_UNKNOWN and proven:
                await ledger_module.record_outcome(
                    session_maker,
                    status=MANUAL_VOUCHER_PAID,
                    expected_statuses=frozenset({MANUAL_VOUCHER_PAY_UNKNOWN}),
                    verified_field="pay_verified_at",
                    reconciliation_required=False,
                    manual_cleanup_required=False,
                    evidence={
                        "reconcile_readback": observation.as_safe_dict(),
                        "baseline": baseline.as_safe_dict(),
                    },
                )
            elif state == ORDER_REFUNDED and snapshot.status in (
                MANUAL_VOUCHER_REFUND_UNKNOWN,
                MANUAL_VOUCHER_PAY_UNKNOWN,
            ):
                # A refunded order needs no artifact proof: the money is back,
                # which is the outcome, and an unreadable voucher is not a
                # reason to pretend otherwise.
                await ledger_module.record_outcome(
                    session_maker,
                    status=MANUAL_VOUCHER_REFUNDED,
                    expected_statuses=frozenset({MANUAL_VOUCHER_REFUND_UNKNOWN, MANUAL_VOUCHER_PAY_UNKNOWN}),
                    verified_field="refund_verified_at",
                    reconciliation_required=False,
                    manual_cleanup_required=False,
                )
            elif state in (ORDER_CANCELLED, ORDER_REFUNDED) and snapshot.status in (
                MANUAL_VOUCHER_CREATED,
                MANUAL_VOUCHER_CREATE_UNKNOWN,
            ):
                # The operator closed the draft by hand in the dashboard. That
                # is an OBSERVATION, not something this tool did, and it is
                # recorded as exactly that — with no second mutation.
                await ledger_module.record_outcome(
                    session_maker,
                    status=MANUAL_VOUCHER_MANUALLY_CLEANED,
                    expected_statuses=frozenset({MANUAL_VOUCHER_CREATED, MANUAL_VOUCHER_CREATE_UNKNOWN}),
                    reconciliation_required=False,
                    manual_cleanup_required=False,
                    manual_cleanup_observed=True,
                    evidence={"manual_cleanup": observation.as_safe_dict()},
                )

    snapshot = await ledger_module.load(session_maker)
    # A send whose outcome is unknown is NOT reconciled by reading an order:
    # whether Meta delivered the message is not a fact the POS system holds.
    # The report says so rather than quietly clearing the flag.
    if snapshot.status == MANUAL_VOUCHER_SEND_UNKNOWN:
        reasons.append(MUTATION_UNKNOWN)

    return StageReport(
        stage="reconcile",
        outcome="observed",
        reasons=reasons,
        external_effect_attempted=False,
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=snapshot.manual_cleanup_required,
        ledger=snapshot.as_safe_dict(),
        observations=observations,
        order_state=state,
        baseline=baseline.as_safe_dict(),
    )


async def run_status(session_maker: async_sessionmaker[AsyncSession]) -> StageReport:
    """Where this canary stands. Read-only, no network, no approval needed."""
    snapshot = await ledger_module.load(session_maker)
    return StageReport(
        stage="status",
        outcome="observed" if snapshot.exists else "not_started",
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=snapshot.manual_cleanup_required,
        ledger=snapshot.as_safe_dict(),
    )


__all__ = [
    "CREATE_WINDOW",
    "STAGE_SOURCE_STATUSES",
    "ManualCanaryRequest",
    "StageReport",
    "VoucherMutator",
    "VoucherSender",
    "build_stage_plan",
    "run_create",
    "run_deliver",
    "run_pay",
    "run_reconcile",
    "run_refund",
    "run_status",
]
