"""Stage orchestration for the controlled voucher delivery canary (§36).

Each public entry point is ONE operator stage. There is deliberately no function
that runs create, pay and deliver in sequence: the stop between them is the
control, and a human looking at what the last stage actually did before saying
"go on" is the whole safety model.

Every stage follows the same shape:

1. rebuild THIS stage's plan live — fence, key, template, sender, recipient,
   ledger, order — and refuse unless it still authorises this exact stage;
2. claim in PostgreSQL and COMMIT the claim;
3. make at most one external request;
4. record the outcome as a compare-and-set, then verify it by reading.

The voucher code never rests
----------------------------
It is read out of the order in memory, used to compute a keyed MAC or to fill
one request parameter, and dropped. It is not returned by any function here, not
attached to any report, and not written anywhere. Every stage after CREATE
re-reads it and re-checks it against the stored MAC before acting, so a stage
can never send something the paid order did not issue.

An unknown outcome is never retried
-----------------------------------
EasyWeek publishes no write idempotency, and Meta's message endpoint is not
something to guess about either. An unknown create or pay is resolved by
reading; an unknown SEND cannot be resolved by reading at all, so it becomes a
human's problem — with the refund closed, because the customer may be holding
the code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.authorisation import (
    StagePlan,
    stage_digest,
    verify_plan_authorisation,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
    VoucherBindingKeyError,
    voucher_code_mac,
    voucher_code_matches,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    DeliveryOutcome,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import RecipientProof, prove_recipient
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    API_UNAVAILABLE,
    APPLY_FLAG_MISSING,
    CANARY_SCOPE_ALREADY_CONSUMED,
    DELIVERY_ALREADY_ATTEMPTED,
    DELIVERY_OUTCOME_UNKNOWN,
    IDENTITY_BINDING_MISMATCH,
    LEDGER_IDENTITY_INCOMPLETE,
    LEDGER_STATE_UNEXPECTED,
    MANUAL_CLEANUP_REQUIRED,
    MUTATION_REJECTED,
    MUTATION_UNKNOWN,
    NEW_CLIENT_CAMPAIGN_CODE,
    REFUND_FORBIDDEN_AFTER_SEND,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
    STAGE_REFUND,
    UNKNOWN_STAGE,
    VOUCHER_ARTIFACT_UNPROVEN,
    VOUCHER_BINDING_MISMATCH,
    VOUCHER_ORDER_ALREADY_REFUNDED,
    VOUCHER_ORDER_NOT_PAID,
    VOUCHER_ORDER_NOT_PAYABLE,
    VOUCHER_ORDER_UNPROVEN,
    VOUCHER_STATE_UNATTRIBUTABLE,
    delivery_marker,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.readiness import (
    DeliveryPrerequisites,
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
    SUPPORTED_VOUCHER_PRICE_MINOR,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
from altegio_bot.models.models import (
    VOUCHER_DELIVERY_AMBIGUOUS,
    VOUCHER_DELIVERY_BASIS_EARNED,
    VOUCHER_DELIVERY_BASIS_TEST,
    VOUCHER_DELIVERY_CREATE_CLAIMED,
    VOUCHER_DELIVERY_CREATE_REJECTED,
    VOUCHER_DELIVERY_CREATE_UNKNOWN,
    VOUCHER_DELIVERY_CREATED,
    VOUCHER_DELIVERY_DELIVERED,
    VOUCHER_DELIVERY_MANUALLY_CLEANED,
    VOUCHER_DELIVERY_PAID,
    VOUCHER_DELIVERY_PAY_CLAIMED,
    VOUCHER_DELIVERY_PAY_REJECTED,
    VOUCHER_DELIVERY_PAY_UNKNOWN,
    VOUCHER_DELIVERY_PLANNED,
    VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
    VOUCHER_DELIVERY_READ,
    VOUCHER_DELIVERY_REFUND_CLAIMED,
    VOUCHER_DELIVERY_REFUND_REJECTED,
    VOUCHER_DELIVERY_REFUND_UNKNOWN,
    VOUCHER_DELIVERY_REFUNDED,
    VOUCHER_DELIVERY_SEND_CLAIMED,
    VOUCHER_DELIVERY_SEND_REJECTED,
    VOUCHER_DELIVERY_SEND_UNKNOWN,
)
from altegio_bot.utils import utcnow

# Outcomes. A closed vocabulary the CLI maps to exit codes.
OUTCOME_PROVEN: Final = "proven"
OUTCOME_REFUSED: Final = "refused"
OUTCOME_UNKNOWN: Final = "unknown_result"
OUTCOME_CONTRACT_MISMATCH: Final = "contract_mismatch"
# A finished cleanup is not this. This value — and the exit code the CLI maps it
# to — mean the operator still has something to do by hand. Once the cleanup is
# proven and written down, the durable state is terminal and the answer is
# OUTCOME_PROVEN: reporting "manual cleanup required" against a row that says
# `manual_cleanup_required=false` would send an operator, and any wrapper
# reading the exit code, looking for work that does not exist.
OUTCOME_MANUAL_CLEANUP: Final = "manual_cleanup_required"
OUTCOME_AMBIGUOUS: Final = "ambiguous"

# How far around the create the marker walk may look. Bounded so an unresolved
# create cannot widen into a scan of somebody's whole order history.
CREATE_WINDOW_BEFORE: Final = timedelta(minutes=10)
CREATE_WINDOW_AFTER: Final = timedelta(hours=6)

# The ledger states each stage may be planned FROM.
STAGE_SOURCE_STATUSES: Final = {
    STAGE_CREATE: frozenset({None, VOUCHER_DELIVERY_PLANNED, VOUCHER_DELIVERY_CREATE_REJECTED}),
    STAGE_PAY: frozenset({VOUCHER_DELIVERY_CREATED, VOUCHER_DELIVERY_PAY_REJECTED}),
    STAGE_DELIVER: frozenset({VOUCHER_DELIVERY_PAID}),
    STAGE_REFUND: frozenset({VOUCHER_DELIVERY_PAID, VOUCHER_DELIVERY_REFUND_REJECTED}),
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
    external_mutation_attempted: bool = False
    external_send_attempted: bool = False
    reconciliation_required: bool = False
    manual_cleanup_required: bool = False
    ledger: dict[str, Any] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    order_state: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_delivery_stage",
            "stage": self.stage,
            "outcome": self.outcome,
            "reasons": list(dict.fromkeys(self.reasons)),
            "external_mutation_attempted": self.external_mutation_attempted,
            "external_send_attempted": self.external_send_attempted,
            "reconciliation_required": self.reconciliation_required,
            "manual_cleanup_required": self.manual_cleanup_required,
            "order_state": self.order_state,
            "observations": list(self.observations),
            "ledger": dict(self.ledger),
            # Repeated verbatim on every stage, success included.
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
            "raw_identifiers_omitted": True,
            "voucher_code_omitted": True,
        }


@dataclass(frozen=True)
class CanaryRequest:
    """What the operator named on the command line, plus the frozen identity."""

    preview_run_id: int
    campaign_recipient_id: int
    company_id: int
    sender_code: str
    staffer_uuid: str
    payment_account_uuid: str
    location_uuid: str = KARLSRUHE_LOCATION_UUID
    voucher_template_uuid: str = EASYWEEK_VOUCHER_TEMPLATE_UUID

    @property
    def marker(self) -> str:
        return delivery_marker(
            preview_run_id=self.preview_run_id,
            campaign_recipient_id=self.campaign_recipient_id,
        )


def _voucher_code(payload: object) -> str | None:
    """The one issued code, in memory, or ``None``.

    Read through the same §35 proof the payment gate uses, so a body this canary
    would refuse to pay for is also a body it refuses to read a code out of.
    The return value is a plain string on purpose: it has exactly one caller at
    a time and is never stored, logged or returned further.
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


def _identity_from(request: CanaryRequest, proof: RecipientProof) -> ledger_module.CanaryIdentity:
    # The customer is required on both bases. The booking is required on exactly
    # one of them, and forbidden on the other — a test canary that carried a
    # booking would be claiming a visit nobody made, and the ledger's own CHECK
    # would refuse the row anyway.
    assert proof.easyweek_customer_uuid is not None
    if proof.recipient_basis == VOUCHER_DELIVERY_BASIS_EARNED:
        assert proof.source_booking_uuid is not None
    else:
        assert proof.source_booking_uuid is None
    return ledger_module.CanaryIdentity(
        company_id=request.company_id,
        campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
        campaign_run_id=request.preview_run_id,
        campaign_recipient_id=request.campaign_recipient_id,
        recipient_basis=proof.recipient_basis,
        source_booking_uuid=proof.source_booking_uuid,
        easyweek_customer_uuid=proof.easyweek_customer_uuid,
        location_uuid=request.location_uuid,
        staffer_uuid=request.staffer_uuid,
        payment_account_uuid=request.payment_account_uuid,
        voucher_template_uuid=request.voucher_template_uuid,
        reconciliation_marker=request.marker,
    )


def _identity_from_snapshot(snapshot: ledger_module.LedgerSnapshot) -> ledger_module.CanaryIdentity | None:
    """Rebuild the identity the ledger was opened with, or refuse.

    ``None`` means the durable row cannot be turned into a whole identity, and
    the caller must stop rather than act on a partial one.

    The refusal matters more than the rebuild. An earlier version filled the
    gaps — ``or 0`` for the ids, ``or ""`` for the UUIDs — which turns an
    incomplete ledger into an identity that compares equal to nothing and is
    refused far downstream, or worse, compares equal to something. In particular
    an empty-string "source booking" is not a missing booking: it is a value,
    and on the test basis the contract says that column must be NULL and stay
    NULL. So every required field is checked for what it actually is, and the
    basis has to agree with the booking in both directions.
    """
    basis = snapshot.recipient_basis
    if basis not in (VOUCHER_DELIVERY_BASIS_EARNED, VOUCHER_DELIVERY_BASIS_TEST):
        # Missing, empty, or a word this code does not know. Guessing which
        # contract a real €15 is under is not something to do quietly.
        return None

    ids = (snapshot.company_id, snapshot.campaign_run_id, snapshot.campaign_recipient_id)
    if any(value is None for value in ids):
        return None

    uuids = {
        "easyweek_customer_uuid": canonical_uuid(snapshot.easyweek_customer_uuid),
        "location_uuid": canonical_uuid(snapshot.location_uuid),
        "staffer_uuid": canonical_uuid(snapshot.staffer_uuid),
        "payment_account_uuid": canonical_uuid(snapshot.payment_account_uuid),
        "voucher_template_uuid": canonical_uuid(snapshot.voucher_template_uuid),
    }
    if any(value is None for value in uuids.values()):
        return None

    marker = (snapshot.reconciliation_marker or "").strip()
    if not marker:
        return None

    booking = canonical_uuid(snapshot.source_booking_uuid)
    if basis == VOUCHER_DELIVERY_BASIS_EARNED:
        # An earned canary names the visit it was earned by. No booking means
        # the row cannot say whose entitlement this is.
        if booking is None:
            return None
    elif snapshot.source_booking_uuid is not None:
        # A test canary that carries a booking is claiming a visit nobody made.
        return None

    return ledger_module.CanaryIdentity(
        company_id=int(snapshot.company_id or 0),
        campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
        campaign_run_id=int(snapshot.campaign_run_id or 0),
        campaign_recipient_id=int(snapshot.campaign_recipient_id or 0),
        recipient_basis=basis,
        # Genuinely nullable: NULL on the test basis, never an empty string.
        source_booking_uuid=booking,
        easyweek_customer_uuid=uuids["easyweek_customer_uuid"] or "",
        location_uuid=uuids["location_uuid"] or "",
        staffer_uuid=uuids["staffer_uuid"] or "",
        payment_account_uuid=uuids["payment_account_uuid"] or "",
        voucher_template_uuid=uuids["voucher_template_uuid"] or "",
        reconciliation_marker=marker,
    )


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


async def build_stage_plan(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: CanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    now: datetime | None = None,
    enabled: bool | None = None,
) -> tuple[StagePlan, RecipientProof, DeliveryPrerequisites]:
    """Re-prove everything THIS stage depends on. Reads only; mutates nothing.

    Creates no ledger row, sends no request and writes nothing. Every failure is
    one of §36's stable reason codes, and an unready plan still prints, because
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

    # The live guard, every time, for every stage — including the refund, so a
    # refusal to refund is never caused by a stale read of the world.
    proof = await prove_recipient(
        session,
        preview_run_id=request.preview_run_id,
        campaign_recipient_id=request.campaign_recipient_id,
        expected_company_id=request.company_id,
        client_reader=reader,
        now=issued_at,
        enabled=enabled,
    )
    reasons.extend(proof.reasons)

    identity_bound = True
    if snapshot.exists and proof.proven:
        identity_bound = _identity_from(request, proof).matches(snapshot)
        if not identity_bound:
            reasons.append(IDENTITY_BINDING_MISMATCH)

    order_state: str | None = None
    if stage in (STAGE_PAY, STAGE_DELIVER, STAGE_REFUND):
        stage_reasons, order_state, stage_observations = await _order_preconditions(
            order_reader,
            stage=stage,
            snapshot=snapshot,
            proof=proof,
        )
        reasons.extend(stage_reasons)
        observations.extend(stage_observations)

    if stage == STAGE_DELIVER and snapshot.send_attempt_count:
        reasons.append(DELIVERY_ALREADY_ATTEMPTED)
    if stage == STAGE_REFUND and snapshot.status in ledger_module.SEND_TOUCHED_STATUSES:
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
    return plan, proof, prerequisites


async def _order_preconditions(
    order_reader: Any,
    *,
    stage: str,
    snapshot: ledger_module.LedgerSnapshot,
    proof: RecipientProof,
) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    """What the remote order must look like for this stage."""
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []

    if snapshot.target_order_uuid is None:
        return [VOUCHER_ORDER_UNPROVEN], None, observations

    try:
        payload = await order_reader.get_order(snapshot.target_order_uuid)
    except EasyWeekError:
        return [API_UNAVAILABLE], None, observations

    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage=f"{stage}_plan_readback",
        expected_customer_uuid=proof.easyweek_customer_uuid or "",
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    observations.append(observation.as_safe_dict())

    if order.get("comment") != snapshot.reconciliation_marker:
        reasons.append(VOUCHER_ORDER_UNPROVEN)
    if not observation.order_customer_binding_proven:
        reasons.append(VOUCHER_ORDER_UNPROVEN)
    # The voucher's own shape matters to everything that spends money or sends a
    # message. It deliberately does NOT matter to a refund: an artifact we
    # cannot read is a reason to get the money back, not a reason to leave it
    # out there. The observation is still recorded as evidence either way.
    if stage != STAGE_REFUND and not observation.voucher_line_proven:
        reasons.append(VOUCHER_ARTIFACT_UNPROVEN)

    if stage == STAGE_PAY:
        if state != ORDER_OPEN:
            reasons.append(VOUCHER_ORDER_NOT_PAYABLE)
        reasons.extend(
            payable_order_reasons(
                payload,
                expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
                expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
            )
        )
        # The code must still be the one this row was bound to at CREATE.
        # Checked before the payment, not only before the send: paying for an
        # order whose voucher changed underneath us buys something else.
        if not _binding_holds(payload, snapshot):
            reasons.append(VOUCHER_BINDING_MISMATCH)
    elif stage == STAGE_DELIVER:
        if state != ORDER_PAID:
            reasons.append(VOUCHER_ORDER_NOT_PAID)
        # The code must still be the one the ledger was bound to. Checked here,
        # in the plan, so an operator learns it before approving a send.
        if not _binding_holds(payload, snapshot):
            reasons.append(VOUCHER_BINDING_MISMATCH)
    elif stage == STAGE_REFUND:
        # Strictly paid. An order that already reads refunded has nothing left
        # to refund, and authorising a POST for it would be authorising a second
        # real refund attempt against a provider with no idempotency key.
        if state == ORDER_REFUNDED:
            reasons.append(VOUCHER_ORDER_ALREADY_REFUNDED)
        elif state != ORDER_PAID:
            reasons.append(VOUCHER_ORDER_NOT_PAID)

    return reasons, state, observations


def _binding_matches(snapshot: ledger_module.LedgerSnapshot, request: CanaryRequest) -> bool:
    """Is this ledger row the one THIS request is allowed to speak for?

    The immutable half of the identity: the run, the recipient, the branch and
    the voucher template. Judging somebody else's order against this row, or
    this row against somebody else's request, is the mistake this prevents.
    """
    return (
        snapshot.campaign_run_id == request.preview_run_id
        and snapshot.campaign_recipient_id == request.campaign_recipient_id
        and snapshot.location_uuid == request.location_uuid
        and snapshot.voucher_template_uuid == request.voucher_template_uuid
    )


def _binding_holds(payload: object, snapshot: ledger_module.LedgerSnapshot) -> bool:
    """Is the code in this body the one this ledger row was bound to?

    False for a missing binding, a rotated key, an unreadable artifact or a
    mismatch — every one of which must stop a send. A key that cannot be loaded
    at all is a deployment fault and is reported separately by the prerequisites.
    """
    code = _voucher_code(payload)
    if code is None or snapshot.target_order_uuid is None or snapshot.row_id is None:
        return False
    try:
        return voucher_code_matches(
            voucher_code=code,
            expected_mac=snapshot.voucher_code_hmac,
            expected_key_id=snapshot.hmac_key_id,
            ledger_uuid=str(snapshot.row_id),
            target_order_uuid=snapshot.target_order_uuid,
            voucher_template_uuid=snapshot.voucher_template_uuid or "",
        )
    except VoucherBindingKeyError:
        return False


def _refusal(stage: str, reasons: tuple[str, ...] | list[str], snapshot: ledger_module.LedgerSnapshot) -> StageReport:
    return StageReport(
        stage=stage,
        outcome=OUTCOME_REFUSED,
        reasons=list(reasons),
        ledger=snapshot.as_safe_dict(),
        reconciliation_required=snapshot.status in ledger_module.UNRESOLVED_STATUSES,
        manual_cleanup_required=snapshot.manual_cleanup_required,
    )


async def _authorise(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    request: CanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    apply: bool,
    enabled: bool | None = None,
) -> tuple[StagePlan | None, RecipientProof | None, DeliveryPrerequisites | None, tuple[str, ...]]:
    """Rebuild the plan and check it still authorises this exact stage."""
    if not apply:
        return None, None, None, (APPLY_FLAG_MISSING,)
    plan, proof, prerequisites = await build_stage_plan(
        session,
        session_maker,
        stage=stage,
        request=request,
        reader=reader,
        order_reader=order_reader,
        enabled=enabled,
    )
    reasons = verify_plan_authorisation(
        plan,
        supplied_digest=plan_digest,
        supplied_issued_at=plan_issued_at,
        supplied_phrase=confirmation_phrase,
    )
    return plan, proof, prerequisites, reasons


# ---------------------------------------------------------------------------
# Stage: create
# ---------------------------------------------------------------------------


async def run_create(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    apply: bool = False,
    enabled: bool | None = None,
) -> StageReport:
    """Create the ONE voucher order for this recipient, claim committed first."""
    plan, proof, _, refusals = await _authorise(
        session,
        session_maker,
        stage=STAGE_CREATE,
        request=request,
        reader=reader,
        order_reader=order_reader,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
        apply=apply,
        enabled=enabled,
    )
    snapshot = await ledger_module.load(session_maker)
    if refusals or plan is None or proof is None:
        return _refusal(STAGE_CREATE, refusals, snapshot)

    identity = _identity_from(request, proof)
    # Opening the row is what makes the entitlement uniqueness rule start
    # protecting this person — before any money moves.
    opened = await ledger_module.open_canary(session_maker, identity=identity)
    if not identity.matches(opened):
        return _refusal(STAGE_CREATE, [CANARY_SCOPE_ALREADY_CONSUMED], opened)

    now = utcnow()
    claim = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        plan_digest=plan.digest,
        create_window_start=now - CREATE_WINDOW_BEFORE,
        create_window_end=now + CREATE_WINDOW_AFTER,
    )
    if not claim.granted:
        return _refusal(STAGE_CREATE, [claim.reason], await ledger_module.load(session_maker))

    # The claim is committed. From here on an interruption reads as "the request
    # may have gone out", which is exactly what it is.
    try:
        response = await mutator.create_voucher_order(
            location_uuid=request.location_uuid,
            customer_uuid=identity.easyweek_customer_uuid,
            staffer_uuid=request.staffer_uuid,
            voucher_template_uuid=request.voucher_template_uuid,
            price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
            marker=request.marker,
        )
    except EasyWeekVoucherMutationUnknown:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_DELIVERY_CREATE_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN,
            reasons=[MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_CREATE_REJECTED,
            expected_statuses=frozenset({VOUCHER_DELIVERY_CREATE_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            reconciliation_required=False,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[MUTATION_REJECTED],
            external_mutation_attempted=True,
            ledger=result.snapshot.as_safe_dict(),
        )

    return await _verify_created(
        session_maker,
        order_reader=order_reader,
        identity=identity,
        candidate=order_object(response.envelope).get("uuid") if order_object(response.envelope) else None,
        marker=request.marker,
    )


async def _verify_created(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    order_reader: Any,
    identity: ledger_module.CanaryIdentity,
    candidate: object,
    marker: str,
) -> StageReport:
    """A 2xx is a claim, not a proof: read the order back and bind the code."""
    canonical = canonical_uuid(candidate)
    snapshot = await ledger_module.load(session_maker)
    unresolved_from = frozenset({VOUCHER_DELIVERY_CREATE_CLAIMED, VOUCHER_DELIVERY_CREATE_UNKNOWN})

    if canonical is None:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
            expected_statuses=unresolved_from,
            reason_code=VOUCHER_ORDER_UNPROVEN,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN,
            reasons=[VOUCHER_ORDER_UNPROVEN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )

    try:
        payload = await order_reader.get_order(canonical)
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
            expected_statuses=unresolved_from,
            reason_code=API_UNAVAILABLE,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN,
            reasons=[API_UNAVAILABLE],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )

    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage="create_readback",
        expected_customer_uuid=identity.easyweek_customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    reasons: list[str] = []
    if order.get("comment") != marker or not observation.order_customer_binding_proven or state != ORDER_OPEN:
        reasons.append(VOUCHER_ORDER_UNPROVEN)
    if not observation.voucher_line_proven:
        reasons.append(VOUCHER_ARTIFACT_UNPROVEN)

    code = _voucher_code(payload) if not reasons else None
    key_id = mac = None
    if code is not None and snapshot.row_id is not None:
        try:
            key_id, mac = voucher_code_mac(
                voucher_code=code,
                ledger_uuid=str(snapshot.row_id),
                target_order_uuid=canonical,
                voucher_template_uuid=identity.voucher_template_uuid,
            )
        except VoucherBindingKeyError as exc:
            reasons.append(exc.reason)
    elif not reasons:
        reasons.append(VOUCHER_ARTIFACT_UNPROVEN)
    # The plaintext has no further use in this process.
    del code

    if reasons:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
            expected_statuses=unresolved_from,
            reason_code=reasons[0],
            target_order_uuid=canonical,
            evidence={"create_readback": observation.as_safe_dict()},
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_CREATE,
            outcome=OUTCOME_UNKNOWN,
            reasons=reasons,
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[observation.as_safe_dict()],
            order_state=state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_CREATED,
        expected_statuses=unresolved_from,
        target_order_uuid=canonical,
        voucher_code_hmac=mac,
        hmac_key_id=key_id,
        verified_field="create_verified_at",
        evidence={"create_readback": observation.as_safe_dict()},
        manual_cleanup_required=True,
        reconciliation_required=False,
    )
    return StageReport(
        stage=STAGE_CREATE,
        outcome=OUTCOME_PROVEN if result.applied else OUTCOME_REFUSED,
        reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
        external_mutation_attempted=True,
        manual_cleanup_required=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[observation.as_safe_dict()],
        order_state=state,
    )


# ---------------------------------------------------------------------------
# Stage: pay
# ---------------------------------------------------------------------------


async def run_pay(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    apply: bool = False,
    enabled: bool | None = None,
) -> StageReport:
    """Pay the ONE created order, once, on the approved account."""
    plan, proof, _, refusals = await _authorise(
        session,
        session_maker,
        stage=STAGE_PAY,
        request=request,
        reader=reader,
        order_reader=order_reader,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
        apply=apply,
        enabled=enabled,
    )
    snapshot = await ledger_module.load(session_maker)
    if refusals or plan is None or proof is None:
        return _refusal(STAGE_PAY, refusals, snapshot)
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_PAY, [VOUCHER_ORDER_UNPROVEN], snapshot)

    identity = _identity_from(request, proof)
    claim = await ledger_module.claim_pay(session_maker, identity=identity, plan_digest=plan.digest)
    if not claim.granted:
        return _refusal(STAGE_PAY, [claim.reason], await ledger_module.load(session_maker))

    try:
        await mutator.pay_voucher_order(
            order_uuid=snapshot.target_order_uuid,
            account_uuid=request.payment_account_uuid,
        )
    except EasyWeekVoucherMutationUnknown:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PAY_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_DELIVERY_PAY_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN,
            reasons=[MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PAY_REJECTED,
            expected_statuses=frozenset({VOUCHER_DELIVERY_PAY_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            manual_cleanup_required=True,
            reconciliation_required=False,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[MUTATION_REJECTED],
            external_mutation_attempted=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )

    return await _verify_paid(session_maker, order_reader=order_reader, identity=identity)


async def _verify_paid(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    order_reader: Any,
    identity: ledger_module.CanaryIdentity,
) -> StageReport:
    """A 2xx pay is proven only by an order that reads paid and still binds."""
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.target_order_uuid is not None
    expected = frozenset({VOUCHER_DELIVERY_PAY_CLAIMED, VOUCHER_DELIVERY_PAY_UNKNOWN})

    try:
        payload = await order_reader.get_order(snapshot.target_order_uuid)
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PAY_UNKNOWN,
            expected_statuses=expected,
            reason_code=API_UNAVAILABLE,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN,
            reasons=[API_UNAVAILABLE],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )

    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage="paid_readback",
        expected_customer_uuid=identity.easyweek_customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    if state != ORDER_PAID or not _binding_holds(payload, snapshot):
        reason = VOUCHER_ORDER_NOT_PAID if state != ORDER_PAID else VOUCHER_BINDING_MISMATCH
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PAY_UNKNOWN,
            expected_statuses=expected,
            reason_code=reason,
            evidence={"paid_readback": observation.as_safe_dict()},
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_PAY,
            outcome=OUTCOME_UNKNOWN,
            reasons=[reason],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[observation.as_safe_dict()],
            order_state=state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PAID,
        expected_statuses=expected,
        verified_field="pay_verified_at",
        evidence={"paid_readback": observation.as_safe_dict()},
        manual_cleanup_required=True,
        reconciliation_required=False,
    )
    return StageReport(
        stage=STAGE_PAY,
        outcome=OUTCOME_PROVEN if result.applied else OUTCOME_REFUSED,
        reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
        external_mutation_attempted=True,
        manual_cleanup_required=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[observation.as_safe_dict()],
        order_state=state,
    )


# ---------------------------------------------------------------------------
# Stage: deliver
# ---------------------------------------------------------------------------


async def run_deliver(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    sender: VoucherSender,
    booking_link: str,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    apply: bool = False,
    enabled: bool | None = None,
) -> StageReport:
    """Send the code once. The only stage that reaches a real person.

    The order is load-bearing: everything is re-proven, the claim and the
    redacted intent are committed, and only THEN are the parameters built in
    memory and one POST made. A crash between the commit and the response reads
    as "the customer may have it", which is the only safe reading.
    """
    plan, proof, prerequisites, refusals = await _authorise(
        session,
        session_maker,
        stage=STAGE_DELIVER,
        request=request,
        reader=reader,
        order_reader=order_reader,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
        apply=apply,
        enabled=enabled,
    )
    snapshot = await ledger_module.load(session_maker)
    if refusals or plan is None or proof is None or prerequisites is None:
        return _refusal(STAGE_DELIVER, refusals, snapshot)
    if snapshot.target_order_uuid is None or prerequisites.phone_number_id is None:
        return _refusal(STAGE_DELIVER, [VOUCHER_ORDER_UNPROVEN], snapshot)

    # Read the paid order one last time, immediately before claiming, and take
    # the code from it. Nothing that follows may use a value read earlier.
    try:
        payload = await order_reader.get_order(snapshot.target_order_uuid)
    except EasyWeekError:
        return _refusal(STAGE_DELIVER, [API_UNAVAILABLE], snapshot)

    state, _ = classify_order(payload)
    if state != ORDER_PAID:
        return _refusal(STAGE_DELIVER, [VOUCHER_ORDER_NOT_PAID], snapshot)
    if not _binding_holds(payload, snapshot):
        return _refusal(STAGE_DELIVER, [VOUCHER_BINDING_MISMATCH], snapshot)
    code = _voucher_code(payload)
    if code is None or not proof.destination_phone:
        return _refusal(STAGE_DELIVER, [VOUCHER_ARTIFACT_UNPROVEN], snapshot)

    identity = _identity_from(request, proof)
    claim = await ledger_module.claim_send(
        session_maker,
        identity=identity,
        plan_digest=plan.digest,
        live_guard_reproven_at=plan.issued_at,
        template_code=template_contract.VOUCHER_TEMPLATE_CODE,
        meta_template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
        template_language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        sender_id=prerequisites.sender_id,
    )
    if not claim.granted:
        return _refusal(STAGE_DELIVER, [claim.reason], await ledger_module.load(session_maker))

    # The claim and the intent are committed. One request, whatever happens.
    outcome = await sender.send_voucher_template(
        phone_number_id=prerequisites.phone_number_id,
        to_e164=proof.destination_phone,
        template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
        language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        params=[proof.client_display_name or "", code, booking_link],
    )
    del code

    if outcome.outcome == DELIVERY_ACCEPTED:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
            expected_statuses=frozenset({VOUCHER_DELIVERY_SEND_CLAIMED}),
            provider_message_id=outcome.provider_message_id,
            verified_field="provider_accepted_at",
            evidence={"delivery": outcome.as_safe_dict()},
            # The draft became a delivered voucher. There is no open order left
            # for a human to close, so the flag that asks them to stops here.
            manual_cleanup_required=False,
            reconciliation_required=False,
            attempt_outcome="provider_accepted",
        )
        return StageReport(
            stage=STAGE_DELIVER,
            outcome=OUTCOME_PROVEN,
            external_send_attempted=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[outcome.as_safe_dict()],
        )

    if outcome.outcome == DELIVERY_REJECTED:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_SEND_REJECTED,
            expected_statuses=frozenset({VOUCHER_DELIVERY_SEND_CLAIMED}),
            reason_code=outcome.reason,
            evidence={"delivery": outcome.as_safe_dict()},
            manual_cleanup_required=True,
            reconciliation_required=False,
            attempt_outcome="rejected",
        )
        return StageReport(
            stage=STAGE_DELIVER,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[outcome.reason or MUTATION_REJECTED],
            external_send_attempted=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[outcome.as_safe_dict()],
        )

    # UNKNOWN. The customer may be holding the code; the refund is now closed
    # and a human has to decide what happens next.
    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_SEND_UNKNOWN,
        expected_statuses=frozenset({VOUCHER_DELIVERY_SEND_CLAIMED}),
        reason_code=outcome.reason or DELIVERY_OUTCOME_UNKNOWN,
        evidence={"delivery": outcome.as_safe_dict()},
        manual_cleanup_required=True,
        reconciliation_required=True,
        attempt_outcome="unknown",
    )
    return StageReport(
        stage=STAGE_DELIVER,
        outcome=OUTCOME_UNKNOWN,
        reasons=[DELIVERY_OUTCOME_UNKNOWN],
        external_send_attempted=True,
        reconciliation_required=True,
        manual_cleanup_required=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[outcome.as_safe_dict()],
    )


# ---------------------------------------------------------------------------
# Stage: refund
# ---------------------------------------------------------------------------


async def run_refund(
    session: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    reader: BookingReader,
    order_reader: Any,
    mutator: VoucherMutator,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
    apply: bool = False,
    enabled: bool | None = None,
) -> StageReport:
    """Refund the ONE paid order — only while nothing has been sent."""
    plan, proof, _, refusals = await _authorise(
        session,
        session_maker,
        stage=STAGE_REFUND,
        request=request,
        reader=reader,
        order_reader=order_reader,
        plan_digest=plan_digest,
        plan_issued_at=plan_issued_at,
        confirmation_phrase=confirmation_phrase,
        apply=apply,
        enabled=enabled,
    )
    snapshot = await ledger_module.load(session_maker)
    if refusals or plan is None or proof is None:
        # One refusal is not a dead end. When the rebuilt plan says the order is
        # already refunded, the refund has nothing left to do AND the ledger has
        # something left to learn: left alone it stays `paid` forever and every
        # later reconcile answers `contract_mismatch`.
        #
        # That very refusal also invalidates the operator's digest — the world
        # moved under the approval — so demanding a matching phrase here would
        # make the dead end permanent. Instead this settles at exactly the
        # authority `reconcile` already has and no more: the immutable identity
        # binding, an exact read, a proven order, and writes that move no money.
        # The real refund below is untouched; it still needs the full plan.
        if apply and VOUCHER_ORDER_ALREADY_REFUNDED in refusals:
            settled = await _settle_externally_refunded(
                session_maker, request=request, order_reader=order_reader, snapshot=snapshot
            )
            if settled is not None:
                return settled
        return _refusal(STAGE_REFUND, refusals, snapshot)
    if snapshot.status in ledger_module.SEND_TOUCHED_STATUSES:
        return _refusal(STAGE_REFUND, [REFUND_FORBIDDEN_AFTER_SEND], snapshot)
    if snapshot.target_order_uuid is None:
        return _refusal(STAGE_REFUND, [VOUCHER_ORDER_UNPROVEN], snapshot)

    # The plan proved the order was paid a moment ago. Between then and the
    # claim somebody could have refunded it in the dashboard, and a POST for an
    # order that is already back is a second real refund attempt against a
    # provider with no idempotency key. So the last word belongs to one more
    # read, taken immediately before the claim.
    try:
        payload, state, _, identity_ok = await _exact_order(order_reader, snapshot)
    except EasyWeekError:
        return _refusal(STAGE_REFUND, [API_UNAVAILABLE], snapshot)
    if payload is None or not identity_ok:
        return _refusal(STAGE_REFUND, [VOUCHER_ORDER_UNPROVEN], snapshot)
    if state == ORDER_REFUNDED:
        # Nothing left to refund. Record what is already true through the same
        # read-only path reconciliation uses, and send nothing.
        settled = await _reconcile_refund(session_maker, order_reader=order_reader)
        settled.stage = STAGE_REFUND
        settled.reasons = [VOUCHER_ORDER_ALREADY_REFUNDED, *settled.reasons]
        settled.external_mutation_attempted = False
        return settled
    if state != ORDER_PAID:
        return _refusal(STAGE_REFUND, [VOUCHER_ORDER_NOT_PAID], snapshot)

    identity = _identity_from(request, proof)
    claim = await ledger_module.claim_refund(session_maker, identity=identity, plan_digest=plan.digest)
    if not claim.granted:
        return _refusal(STAGE_REFUND, [claim.reason], await ledger_module.load(session_maker))

    try:
        await mutator.refund_voucher_order(order_uuid=snapshot.target_order_uuid)
    except EasyWeekVoucherMutationUnknown:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_REFUND_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_DELIVERY_REFUND_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_UNKNOWN,
            reasons=[MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )
    except EasyWeekError:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_REFUND_REJECTED,
            expected_statuses=frozenset({VOUCHER_DELIVERY_REFUND_CLAIMED}),
            reason_code=MUTATION_REJECTED,
            manual_cleanup_required=True,
            reconciliation_required=False,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[MUTATION_REJECTED],
            external_mutation_attempted=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
        )

    try:
        payload = await order_reader.get_order(snapshot.target_order_uuid)
    except EasyWeekError:
        payload = None
    state = classify_order(payload)[0] if payload is not None else None
    if state != ORDER_REFUNDED:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_REFUND_UNKNOWN,
            expected_statuses=frozenset({VOUCHER_DELIVERY_REFUND_CLAIMED}),
            reason_code=MUTATION_UNKNOWN,
            manual_cleanup_required=True,
        )
        return StageReport(
            stage=STAGE_REFUND,
            outcome=OUTCOME_UNKNOWN,
            reasons=[MUTATION_UNKNOWN],
            external_mutation_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            order_state=state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_REFUNDED,
        expected_statuses=frozenset({VOUCHER_DELIVERY_REFUND_CLAIMED}),
        verified_field="refund_verified_at",
        manual_cleanup_required=False,
        reconciliation_required=False,
    )
    return StageReport(
        stage=STAGE_REFUND,
        outcome=OUTCOME_PROVEN if result.applied else OUTCOME_REFUSED,
        reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
        external_mutation_attempted=True,
        ledger=result.snapshot.as_safe_dict(),
        order_state=state,
    )


# ---------------------------------------------------------------------------
# Reconcile and status
# ---------------------------------------------------------------------------


async def _settle_externally_refunded(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    order_reader: Any,
    snapshot: ledger_module.LedgerSnapshot,
) -> StageReport | None:
    """Write down a refund THIS canary did not perform. Sends nothing.

    ``None`` means the caller must fall back to its ordinary refusal: the order
    could not be proven ours, it is not actually refunded, or the ledger is not
    in a state an outside cleanup may close.

    The read is exact and the identity is proven before anything is written —
    a stranger's refunded order must not be able to close this row.
    """
    if not _binding_matches(snapshot, request):
        return None
    try:
        payload, state, observation, identity_ok = await _exact_order(order_reader, snapshot)
    except EasyWeekError:
        return None
    if payload is None or not identity_ok or state != ORDER_REFUNDED:
        return None
    safe = observation.as_safe_dict() if observation else None
    result = await settle_external_cleanup(session_maker, snapshot=snapshot, observation=safe)
    if result is None:
        return None
    return StageReport(
        stage=STAGE_REFUND,
        # The operation finished: the money is back and the ledger says so.
        # `voucher_order_already_refunded` stays in the reasons because the
        # operator still needs to know WHY no POST went out and who performed
        # the refund — but an informational reason on a completed transition
        # must not turn it into a failing exit code.
        outcome=OUTCOME_PROVEN if result.applied else OUTCOME_UNKNOWN,
        reasons=[VOUCHER_ORDER_ALREADY_REFUNDED]
        if result.applied
        else [VOUCHER_ORDER_ALREADY_REFUNDED, LEDGER_STATE_UNEXPECTED],
        # Zero refund POSTs. That is the whole point of arriving here.
        external_mutation_attempted=False,
        reconciliation_required=result.snapshot.reconciliation_required,
        manual_cleanup_required=result.snapshot.manual_cleanup_required,
        ledger=result.snapshot.as_safe_dict(),
        observations=[safe] if safe else [],
        order_state=state,
    )


async def _exact_order(
    order_reader: Any,
    snapshot: ledger_module.LedgerSnapshot,
    *,
    expected_uuid: str | None = None,
):
    """Read one exact order and say whether it is ours.

    ``identity_ok`` covers the three things every stage needs and a refund needs
    ALONE: the exact order, our marker, our customer. The voucher's own shape is
    a separate question, asked only where it matters.

    ``expected_uuid`` exists for the create reconciliation, where the order to
    prove may be a candidate the marker walk produced rather than one the ledger
    already names. Appearing in a filtered listing is not identity: the listing
    was scoped by branch and customer, and the exact read is where the order,
    the marker and the customer are actually compared.
    """
    target = expected_uuid if expected_uuid is not None else snapshot.target_order_uuid
    if target is None:
        return None, None, None, False
    payload = await order_reader.get_order(target)
    order = order_object(payload) or {}
    state, _ = classify_order(payload)
    observation = observe_artifact(
        payload,
        stage="reconcile_readback",
        expected_customer_uuid=snapshot.easyweek_customer_uuid or "",
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    identity_ok = (
        canonical_uuid(order.get("uuid")) == target
        and order.get("comment") == snapshot.reconciliation_marker
        and observation.order_customer_binding_proven
    )
    return payload, state, observation, identity_ok


def _reconcile_report(
    *,
    outcome: str,
    reasons: list[str],
    snapshot: ledger_module.LedgerSnapshot,
    order_state: str | None = None,
    observation: dict[str, Any] | None = None,
) -> StageReport:
    return StageReport(
        stage="reconcile",
        outcome=outcome,
        reasons=reasons,
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=snapshot.manual_cleanup_required,
        ledger=snapshot.as_safe_dict(),
        order_state=order_state,
        observations=[observation] if observation else [],
    )


async def _record_manual_cleanup(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    expected: frozenset[str],
    observation: dict[str, Any] | None,
) -> ledger_module.RecordOutcome:
    """Somebody closed or reversed the order by hand. Observed, not claimed.

    No refund attempt is invented and no timestamp is back-filled: the ledger
    has to keep saying that this application did not do it.
    """
    return await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_MANUALLY_CLEANED,
        expected_statuses=expected,
        evidence={"reconcile_readback": observation} if observation else None,
        manual_cleanup_required=False,
        reconciliation_required=False,
        manual_cleanup_observed=True,
    )


# The pre-send states an externally closed or reversed order may settle FROM.
# Deliberately excludes every in-flight state: a claimed or unknown stage may
# still act, and excludes every send-touched state, where the money staying put
# is the whole point.
_EXTERNAL_CLEANUP_FROM: Final = frozenset(
    {
        VOUCHER_DELIVERY_CREATED,
        VOUCHER_DELIVERY_PAID,
        VOUCHER_DELIVERY_PAY_REJECTED,
        VOUCHER_DELIVERY_REFUND_REJECTED,
    }
)

# Any of these means a message may exist, and no reading of an order may quietly
# turn that into a tidy pre-send ending.
_SEND_EVIDENCE_FIELDS: Final = (
    "send_claimed_at",
    "send_attempted_at",
    "provider_accepted_at",
    "delivered_at",
    "read_at",
)


def _untouched_by_send(snapshot: ledger_module.LedgerSnapshot) -> bool:
    return (
        snapshot.send_attempt_count == 0
        and snapshot.status not in ledger_module.SEND_TOUCHED_STATUSES
        and not any(snapshot.stage_timestamps.get(field) for field in _SEND_EVIDENCE_FIELDS)
    )


async def settle_external_cleanup(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    snapshot: ledger_module.LedgerSnapshot,
    observation: dict[str, Any] | None,
) -> ledger_module.RecordOutcome | None:
    """Close a canary whose order somebody else already closed or reversed.

    ``None`` means "not applicable here" — the caller then falls through to its
    ordinary handling rather than inventing an ending.

    The result is ``manually_cleaned``, never ``refunded``. The difference is
    provenance: this application did not send that refund, and a ledger that
    said ``refunded`` would be claiming an action it never took. Nothing is
    back-filled either — no claim, no attempt, no verification timestamp and no
    plan digest — because every one of those would be a record of a request
    that was never made.

    Read-only by construction: the only thing it writes is the observation.
    """
    if snapshot.status not in _EXTERNAL_CLEANUP_FROM or not _untouched_by_send(snapshot):
        return None
    return await _record_manual_cleanup(
        session_maker,
        expected=frozenset({snapshot.status or ""}),
        observation=observation,
    )


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    order_reader: Any,
) -> StageReport:
    """Resolve what can be resolved by READING, and write down what was proven.

    Reconciliation that only reports is not reconciliation: a stage left
    ``*_unknown`` blocks every stage after it forever, so a proven reading has
    to reach the durable state or say honestly that it did not.

    Every write here is a compare-and-set from an explicitly allowed source
    state, under the row lock, monotonic by rank. Nothing in this function ever
    creates, pays, refunds or sends: the only external calls are GETs.
    """
    snapshot = await ledger_module.load(session_maker)
    if not snapshot.exists or snapshot.status is None:
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_REFUSED,
            reasons=[LEDGER_STATE_UNEXPECTED],
            ledger=snapshot.as_safe_dict(),
        )

    # The immutable identity is checked before anything is read, let alone
    # written: reconciling under a different recipient would judge somebody
    # else's order against this row.
    if not _binding_matches(snapshot, request):
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_REFUSED,
            reasons=[IDENTITY_BINDING_MISMATCH],
            reconciliation_required=snapshot.reconciliation_required,
            ledger=snapshot.as_safe_dict(),
        )

    if snapshot.status in (VOUCHER_DELIVERY_SEND_CLAIMED, VOUCHER_DELIVERY_SEND_UNKNOWN):
        # Meta's messages endpoint does not answer "did you accept this?", so
        # there is nothing to resolve against and guessing would be worse than
        # saying so. The refund stays closed and a human stays in the loop.
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN,
            reasons=[DELIVERY_OUTCOME_UNKNOWN],
            external_send_attempted=True,
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=snapshot.as_safe_dict(),
        )

    try:
        if snapshot.status in (VOUCHER_DELIVERY_CREATE_CLAIMED, VOUCHER_DELIVERY_CREATE_UNKNOWN):
            return await _reconcile_create(session_maker, request=request, order_reader=order_reader)
        if snapshot.status in (VOUCHER_DELIVERY_PAY_CLAIMED, VOUCHER_DELIVERY_PAY_UNKNOWN):
            return await _reconcile_pay(session_maker, order_reader=order_reader)
        if snapshot.status in (VOUCHER_DELIVERY_REFUND_CLAIMED, VOUCHER_DELIVERY_REFUND_UNKNOWN):
            return await _reconcile_refund(session_maker, order_reader=order_reader)
        return await _reconcile_terminal(session_maker, order_reader=order_reader)
    except EasyWeekError:
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN,
            reasons=[API_UNAVAILABLE],
            reconciliation_required=True,
            manual_cleanup_required=snapshot.manual_cleanup_required,
            ledger=snapshot.as_safe_dict(),
        )


async def _reconcile_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    request: CanaryRequest,
    order_reader: Any,
) -> StageReport:
    """Did the create reach EasyWeek, and what is the order now?

    When the ledger already names an order the exact read answers it; only an
    unknown target needs the marker walk, scoped by branch and customer with the
    window proven locally — the §35 contract, reused verbatim, including the
    rule that an incomplete walk is unresolved rather than "no order exists".
    """
    snapshot = await ledger_module.load(session_maker)
    unresolved = frozenset({VOUCHER_DELIVERY_CREATE_CLAIMED, VOUCHER_DELIVERY_CREATE_UNKNOWN})
    candidate = snapshot.target_order_uuid

    if candidate is None:
        if snapshot.create_window_start is None or snapshot.create_window_end is None:
            return _reconcile_report(outcome=OUTCOME_UNKNOWN, reasons=[LEDGER_STATE_UNEXPECTED], snapshot=snapshot)
        match = await find_marker_orders(
            order_reader,
            location_uuid=request.location_uuid,
            customer_uuid=snapshot.easyweek_customer_uuid or "",
            marker=snapshot.reconciliation_marker or "",
            window_start=snapshot.create_window_start,
            window_end=snapshot.create_window_end,
        )
        if match.count > 1:
            # Two orders carrying our marker. Which one is ours is not a
            # question a machine may answer by picking.
            result = await ledger_module.record_outcome(
                session_maker,
                status=VOUCHER_DELIVERY_AMBIGUOUS,
                expected_statuses=unresolved,
                reason_code=VOUCHER_STATE_UNATTRIBUTABLE,
                manual_cleanup_required=True,
                reconciliation_required=True,
            )
            return StageReport(
                stage="reconcile",
                # A refused compare-and-set means somebody else moved first.
                # This attempt then proved nothing, and saying otherwise would
                # report a transition that did not happen.
                outcome=OUTCOME_AMBIGUOUS if result.applied else OUTCOME_UNKNOWN,
                reasons=[VOUCHER_STATE_UNATTRIBUTABLE],
                reconciliation_required=True,
                manual_cleanup_required=True,
                ledger=result.snapshot.as_safe_dict(),
            )
        if not match.resolved:
            # Zero matches, or a walk that could not prove it saw everything.
            # UNRESOLVED — never "it was not created".
            return StageReport(
                stage="reconcile",
                outcome=OUTCOME_UNKNOWN,
                reasons=[VOUCHER_ORDER_UNPROVEN, MANUAL_CLEANUP_REQUIRED],
                reconciliation_required=True,
                manual_cleanup_required=True,
                ledger=snapshot.as_safe_dict(),
            )
        candidate = match.order_uuid

    # The exact read is where identity is decided, for a stored target and for a
    # candidate alike. A listing row proves only that the branch-and-customer
    # filter matched; the order UUID, our marker and our customer are compared
    # HERE, before any terminal state can be written. Without this a stranger's
    # cancelled order could close this canary and leave a real draft unwatched.
    payload, state, observation, identity_ok = await _exact_order(order_reader, snapshot, expected_uuid=candidate)
    safe = observation.as_safe_dict() if observation else None

    if payload is None or not identity_ok:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
            expected_statuses=unresolved,
            reason_code=VOUCHER_ORDER_UNPROVEN,
            evidence={"create_reconcile_readback": safe},
            manual_cleanup_required=True,
            reconciliation_required=True,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN,
            reasons=[VOUCHER_ORDER_UNPROVEN],
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    assert observation is not None
    if state == ORDER_OPEN:
        # Exactly the proof a fresh create must pass, including the binding —
        # under whichever basis the row was actually opened on, taken from the
        # durable ledger rather than assumed.
        identity = _identity_from_snapshot(snapshot)
        if identity is None:
            result = await ledger_module.record_outcome(
                session_maker,
                status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
                expected_statuses=unresolved,
                reason_code=LEDGER_IDENTITY_INCOMPLETE,
                evidence={"create_reconcile_readback": safe},
                manual_cleanup_required=True,
                reconciliation_required=True,
            )
            return StageReport(
                stage="reconcile",
                outcome=OUTCOME_UNKNOWN,
                reasons=[LEDGER_IDENTITY_INCOMPLETE],
                reconciliation_required=True,
                manual_cleanup_required=True,
                ledger=result.snapshot.as_safe_dict(),
                observations=[safe] if safe else [],
                order_state=state,
            )
        report = await _verify_created(
            session_maker,
            order_reader=order_reader,
            identity=identity,
            candidate=candidate,
            marker=snapshot.reconciliation_marker or "",
        )
        report.stage = "reconcile"
        report.external_mutation_attempted = False
        return report

    if state in (ORDER_CANCELLED, ORDER_REFUNDED):
        # The draft was closed or reversed by hand before anything was paid.
        result = await _record_manual_cleanup(session_maker, expected=unresolved, observation=safe)
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_PROVEN if result.applied else OUTCOME_UNKNOWN,
            reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    if state == ORDER_PAID:
        # Paid, with no payment this application can account for. Declaring it
        # paid would invent provenance and open the delivery stage on it.
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_AMBIGUOUS,
            expected_statuses=unresolved,
            reason_code=VOUCHER_STATE_UNATTRIBUTABLE,
            target_order_uuid=candidate,
            evidence={"create_reconcile_readback": safe},
            manual_cleanup_required=True,
            reconciliation_required=True,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_AMBIGUOUS if result.applied else OUTCOME_UNKNOWN,
            reasons=[VOUCHER_STATE_UNATTRIBUTABLE],
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_CREATE_UNKNOWN,
        expected_statuses=unresolved,
        reason_code=VOUCHER_ORDER_UNPROVEN,
        target_order_uuid=candidate,
        evidence={"create_reconcile_readback": safe},
        manual_cleanup_required=True,
        reconciliation_required=True,
    )
    return StageReport(
        stage="reconcile",
        outcome=OUTCOME_UNKNOWN,
        reasons=[VOUCHER_ORDER_UNPROVEN],
        reconciliation_required=True,
        manual_cleanup_required=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[safe] if safe else [],
        order_state=state,
    )


async def _reconcile_pay(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    order_reader: Any,
) -> StageReport:
    """Did the payment land? Only the exact order the ledger names may say.

    An order that still reads open is NOT proof the payment failed — the answer
    may simply be in flight — so it stays unknown rather than becoming payable
    again.
    """
    snapshot = await ledger_module.load(session_maker)
    unresolved = frozenset({VOUCHER_DELIVERY_PAY_CLAIMED, VOUCHER_DELIVERY_PAY_UNKNOWN})
    payload, state, observation, identity_ok = await _exact_order(order_reader, snapshot)
    safe = observation.as_safe_dict() if observation else None

    if payload is None or not identity_ok:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PAY_UNKNOWN,
            expected_statuses=unresolved,
            reason_code=VOUCHER_ORDER_UNPROVEN,
            manual_cleanup_required=True,
            reconciliation_required=True,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN,
            reasons=[VOUCHER_ORDER_UNPROVEN],
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    if state == ORDER_PAID:
        # The same proofs the pay stage itself demands, including the binding:
        # a paid order whose voucher changed is not our payment proven.
        if not observation.voucher_line_proven or not _binding_holds(payload, snapshot):
            result = await ledger_module.record_outcome(
                session_maker,
                status=VOUCHER_DELIVERY_PAY_UNKNOWN,
                expected_statuses=unresolved,
                reason_code=VOUCHER_BINDING_MISMATCH,
                evidence={"pay_reconcile_readback": safe},
                manual_cleanup_required=True,
                reconciliation_required=True,
            )
            return StageReport(
                stage="reconcile",
                outcome=OUTCOME_UNKNOWN,
                reasons=[VOUCHER_BINDING_MISMATCH],
                reconciliation_required=True,
                manual_cleanup_required=True,
                ledger=result.snapshot.as_safe_dict(),
                observations=[safe] if safe else [],
                order_state=state,
            )
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_PAID,
            expected_statuses=unresolved,
            verified_field="pay_verified_at",
            evidence={"pay_reconcile_readback": safe},
            manual_cleanup_required=True,
            reconciliation_required=False,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_PROVEN if result.applied else OUTCOME_UNKNOWN,
            reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    if state in (ORDER_CANCELLED, ORDER_REFUNDED):
        # Somebody reversed or closed it outside this application. Recorded as
        # an observation, with no refund attempt invented on our behalf.
        result = await _record_manual_cleanup(session_maker, expected=unresolved, observation=safe)
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_PROVEN if result.applied else OUTCOME_UNKNOWN,
            reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    # Still open, or a state we cannot name. The payment may be in flight.
    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PAY_UNKNOWN,
        expected_statuses=unresolved,
        reason_code=VOUCHER_ORDER_NOT_PAID,
        evidence={"pay_reconcile_readback": safe},
        manual_cleanup_required=True,
        reconciliation_required=True,
    )
    return StageReport(
        stage="reconcile",
        outcome=OUTCOME_UNKNOWN,
        reasons=[VOUCHER_ORDER_NOT_PAID],
        reconciliation_required=True,
        manual_cleanup_required=True,
        ledger=result.snapshot.as_safe_dict(),
        observations=[safe] if safe else [],
        order_state=state,
    )


async def _reconcile_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    order_reader: Any,
) -> StageReport:
    """Did the refund land?

    Deliberately does not consult the voucher's shape or its code: proving that
    money came back is a question about the ORDER, and an unreadable artifact
    must never be what keeps a refund unresolved.
    """
    snapshot = await ledger_module.load(session_maker)
    unresolved = frozenset({VOUCHER_DELIVERY_REFUND_CLAIMED, VOUCHER_DELIVERY_REFUND_UNKNOWN})
    payload, state, observation, identity_ok = await _exact_order(order_reader, snapshot)
    safe = observation.as_safe_dict() if observation else None

    if payload is None or not identity_ok or state != ORDER_REFUNDED:
        result = await ledger_module.record_outcome(
            session_maker,
            status=VOUCHER_DELIVERY_REFUND_UNKNOWN,
            expected_statuses=unresolved,
            reason_code=VOUCHER_ORDER_UNPROVEN if not identity_ok else MUTATION_UNKNOWN,
            evidence={"refund_reconcile_readback": safe},
            manual_cleanup_required=True,
            reconciliation_required=True,
        )
        return StageReport(
            stage="reconcile",
            outcome=OUTCOME_UNKNOWN,
            reasons=[VOUCHER_ORDER_UNPROVEN if not identity_ok else MUTATION_UNKNOWN],
            reconciliation_required=True,
            manual_cleanup_required=True,
            ledger=result.snapshot.as_safe_dict(),
            observations=[safe] if safe else [],
            order_state=state,
        )

    # A verification stamp says "the refund WE sent is confirmed", so it is only
    # written where this application actually attempted one.
    attempted = snapshot.stage_timestamps.get("refund_attempted_at") is not None
    result = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_REFUNDED,
        expected_statuses=unresolved,
        verified_field="refund_verified_at" if attempted else None,
        evidence={"refund_reconcile_readback": safe},
        manual_cleanup_required=False,
        reconciliation_required=False,
    )
    return StageReport(
        stage="reconcile",
        outcome=OUTCOME_PROVEN if result.applied else OUTCOME_UNKNOWN,
        reasons=[] if result.applied else [LEDGER_STATE_UNEXPECTED],
        ledger=result.snapshot.as_safe_dict(),
        observations=[safe] if safe else [],
        order_state=state,
    )


async def _reconcile_terminal(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    order_reader: Any,
) -> StageReport:
    """Report a settled canary against what the order says now.

    Writes nothing: a settled state is not re-stamped because somebody ran
    reconcile again. When the order contradicts the ledger the report says so
    instead of claiming an unconditional success.
    """
    snapshot = await ledger_module.load(session_maker)
    payload, state, observation, identity_ok = await _exact_order(order_reader, snapshot)
    safe = observation.as_safe_dict() if observation else None

    consistent = {
        VOUCHER_DELIVERY_CREATED: {ORDER_OPEN},
        VOUCHER_DELIVERY_PAID: {ORDER_PAID},
        VOUCHER_DELIVERY_PROVIDER_ACCEPTED: {ORDER_PAID},
        VOUCHER_DELIVERY_DELIVERED: {ORDER_PAID},
        VOUCHER_DELIVERY_READ: {ORDER_PAID},
        VOUCHER_DELIVERY_REFUNDED: {ORDER_REFUNDED},
        VOUCHER_DELIVERY_MANUALLY_CLEANED: {ORDER_CANCELLED, ORDER_REFUNDED},
        VOUCHER_DELIVERY_SEND_REJECTED: {ORDER_PAID},
        VOUCHER_DELIVERY_CREATE_REJECTED: set(),
        VOUCHER_DELIVERY_PAY_REJECTED: {ORDER_OPEN},
        VOUCHER_DELIVERY_REFUND_REJECTED: {ORDER_PAID},
    }.get(snapshot.status or "", set())

    if payload is None or not identity_ok:
        return _reconcile_report(
            outcome=OUTCOME_UNKNOWN,
            reasons=[VOUCHER_ORDER_UNPROVEN],
            snapshot=snapshot,
            order_state=state,
            observation=safe,
        )
    if snapshot.status == VOUCHER_DELIVERY_AMBIGUOUS:
        return _reconcile_report(
            outcome=OUTCOME_AMBIGUOUS,
            reasons=[VOUCHER_STATE_UNATTRIBUTABLE],
            snapshot=snapshot,
            order_state=state,
            observation=safe,
        )
    if state in {ORDER_REFUNDED, ORDER_CANCELLED}:
        # The order is closed out there and this row never sent anything. That
        # is not a contract mismatch to report forever, it is an ending to
        # record — as `manually_cleaned`, because somebody else did it.
        settled = await settle_external_cleanup(session_maker, snapshot=snapshot, observation=safe)
        if settled is not None:
            return _reconcile_report(
                # Proven and recorded: nothing is left for a human to do here.
                outcome=OUTCOME_PROVEN if settled.applied else OUTCOME_UNKNOWN,
                reasons=[] if settled.applied else [LEDGER_STATE_UNEXPECTED],
                snapshot=settled.snapshot,
                order_state=state,
                observation=safe,
            )
    if state not in consistent:
        return _reconcile_report(
            outcome=OUTCOME_CONTRACT_MISMATCH,
            reasons=[LEDGER_STATE_UNEXPECTED],
            snapshot=snapshot,
            order_state=state,
            observation=safe,
        )
    if snapshot.manual_cleanup_required:
        # The ledger and the order agree, and there is still a draft or a
        # payment out there for a human to deal with — a rejected pay leaves
        # exactly that. Saying `proven` here would be the same lie in the other
        # direction: a zero exit code over work nobody has done yet.
        return _reconcile_report(
            outcome=OUTCOME_MANUAL_CLEANUP,
            reasons=[MANUAL_CLEANUP_REQUIRED],
            snapshot=snapshot,
            order_state=state,
            observation=safe,
        )
    return _reconcile_report(outcome=OUTCOME_PROVEN, reasons=[], snapshot=snapshot, order_state=state, observation=safe)


async def run_status(session_maker: async_sessionmaker[AsyncSession]) -> StageReport:
    """Database only. No network, no identity, no fence, safe at any moment."""
    snapshot = await ledger_module.load(session_maker)
    return StageReport(
        stage="status",
        outcome=OUTCOME_PROVEN,
        reconciliation_required=snapshot.reconciliation_required,
        manual_cleanup_required=snapshot.manual_cleanup_required,
        external_send_attempted=snapshot.send_attempt_count > 0,
        ledger=snapshot.as_safe_dict(),
    )


__all__ = [
    "CanaryRequest",
    "OUTCOME_AMBIGUOUS",
    "OUTCOME_CONTRACT_MISMATCH",
    "OUTCOME_MANUAL_CLEANUP",
    "OUTCOME_PROVEN",
    "OUTCOME_REFUSED",
    "OUTCOME_UNKNOWN",
    "StageReport",
    "build_stage_plan",
    "run_create",
    "run_deliver",
    "run_pay",
    "run_reconcile",
    "run_refund",
    "run_status",
]
