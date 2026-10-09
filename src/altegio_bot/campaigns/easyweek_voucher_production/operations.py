"""Storage for browser-authorised stages: approvals, operations, audit (§43.4–43.5).

Three questions this module answers, and nothing else:

1. **What did the server offer an operator?** An approval row — immutable, with
   its own expiry, holding the principal, the batch, the stage, the exact slots,
   the count and the money. The browser gets its id.
2. **Was that offer spent?** An operation row. ``approval_id`` is UNIQUE, so one
   offer becomes at most one operation no matter how many times the POST arrives.
3. **Who did it?** An audit row, honest about what a shared credential can prove.

Why the confirm is one transaction
----------------------------------
Consuming the approval and creating the operation happen in a single
transaction, under the approval's own row lock. If they were two steps there
would be an instant in which an approval was spent but no operation existed —
the one state from which a browser retry could legitimately make a second one.

The loser of a race is not an error
-----------------------------------
A double-click, a POST retried after a lost response, a refresh and two tabs are
all the same event from the operator's point of view: they pressed the button
once and want to know what happened. So a confirm that finds the approval
already consumed returns THE EXISTING OPERATION rather than a failure. What must
never happen is two operations, and that is the unique constraint's job, not the
button's.

A crash never becomes a retry
-----------------------------
:func:`interrupt_abandoned` moves ``running`` operations to ``interrupted``, which
is terminal. There is deliberately no transition back to ``queued`` anywhere in
this module. The per-item ledger already knows which slots were claimed and
therefore may have reached the outside world; re-running the operation would
re-send exactly those. Continuing is a readback plus a NEW approval for what is
provably still untouched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPROVAL_ALREADY_CONSUMED,
    APPROVAL_COUNT_UNCONFIRMED,
    APPROVAL_EXPOSURE_UNCONFIRMED,
    APPROVAL_PRINCIPAL_MISMATCH,
    APPROVAL_UNKNOWN,
    EXECUTION_INTERRUPTED,
    KARLSRUHE_COMPANY_ID,
    OPERATION_IN_FLIGHT,
    PLAN_EXPIRED,
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_SCOPE,
    STAGE_FREEZE,
    STAGE_REFUND,
    STOP_ACTIVE,
    STOP_TERMINAL,
)
from altegio_bot.easyweek_voucher_production_contract import LEGACY_PRODUCTION_CONTRACT, production_contract
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    VOUCHER_PRODUCTION_APPROVAL_CONSUMED,
    VOUCHER_PRODUCTION_APPROVAL_PENDING,
    VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    VOUCHER_PRODUCTION_OPERATION_EXPIRED,
    VOUCHER_PRODUCTION_OPERATION_INTERRUPTED,
    VOUCHER_PRODUCTION_OPERATION_QUEUED,
    VOUCHER_PRODUCTION_OPERATION_REFUSED,
    VOUCHER_PRODUCTION_OPERATION_RUNNING,
    VOUCHER_PRODUCTION_OPERATION_TERMINAL,
    EasyWeekVoucherProductionApproval,
    EasyWeekVoucherProductionAudit,
    EasyWeekVoucherProductionOperation,
)
from altegio_bot.utils import utcnow

# What one shared Ops credential can and cannot prove, recorded on every row
# rather than implied by its absence. See §43.4: an audit that suggested it
# identified a person would be worse than one that states the limit.
IDENTIFICATION_LIMIT_SHARED: Final = "shared_ops_account"

# How long a worker may hold an operation before a sweep calls it abandoned. Only
# ever reached when the single executor died: a live one renews while it works.
LEASE: Final = timedelta(minutes=10)


@dataclass(frozen=True)
class OpsPrincipal:
    """The authenticated operator, as the SERVER resolved them.

    Never built from a request body. ``account`` is the configured Ops user the
    presented session belongs to, and ``session_fingerprint`` is a digest of that
    session — enough to tell two sessions of the same account apart in an audit,
    and not the token.
    """

    account: str
    session_fingerprint: str
    identification_limit: str = IDENTIFICATION_LIMIT_SHARED

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "principal": self.account,
            # Short, so a page can show which session acted without putting a
            # 64-character digest in front of an operator.
            "session": self.session_fingerprint[:12],
            "identification_limit": self.identification_limit,
        }


@dataclass(frozen=True)
class StageTargets:
    """What THIS stage is about to do, separately from what the batch costs.

    §43.5 insists on the distinction: an operator confirming a payment needs to
    read "€45 for the three remaining slots", not "€300, the batch total". Both
    numbers are stored, and the confirmation compares the stage's.
    """

    slots: tuple[int, ...]
    stage_target_count: int
    stage_amount_minor: int
    batch_recipient_count: int
    batch_exposure_minor: int
    # What this stage and this batch will actually CHARGE, beside the nominal the
    # operator confirms. Zero for the free gift certificate; equal to the nominal
    # for every paid contract, which is why the old fields keep their meaning.
    stage_issue_price_minor: int | None = None
    batch_issue_price_minor: int | None = None

    def __post_init__(self) -> None:
        # Historical callers provided only the nominal: it was also their price.
        # Explicit zero belongs to the gift and must not trigger this fallback.
        if self.stage_issue_price_minor is None:
            object.__setattr__(self, "stage_issue_price_minor", self.stage_amount_minor)
        if self.batch_issue_price_minor is None:
            object.__setattr__(self, "batch_issue_price_minor", self.batch_exposure_minor)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "target_slots": list(self.slots),
            "stage_target_count": self.stage_target_count,
            "stage_amount_minor": self.stage_amount_minor,
            "batch_recipient_count": self.batch_recipient_count,
            "batch_exposure_minor": self.batch_exposure_minor,
            "stage_issue_price_minor": self.stage_issue_price_minor,
            "batch_issue_price_minor": self.batch_issue_price_minor,
            "free_issue": self.stage_issue_price_minor == 0 and self.batch_issue_price_minor == 0,
        }


@dataclass(frozen=True)
class StoredApproval:
    """An approval as a reader needs it. PII-free, and no staffer UUID."""

    id: int
    stage: str
    principal: str
    session_fingerprint: str
    campaign_run_id: int
    batch_id: int | None
    slot: int | None
    target_slots: tuple[int, ...]
    stage_target_count: int
    stage_amount_minor: int
    batch_recipient_count: int
    batch_exposure_minor: int
    stage_issue_price_minor: int
    batch_issue_price_minor: int
    plan_digest: str
    plan_issued_at: datetime
    expires_at: datetime
    status: str
    frozen_digest: str | None
    stop_generation_at_plan: int = 0
    request_schema_version: str = PRODUCTION_SCHEMA_VERSION
    product_contract_version: str = LEGACY_PRODUCTION_CONTRACT.version

    @property
    def pending(self) -> bool:
        return self.status == VOUCHER_PRODUCTION_APPROVAL_PENDING

    def expired_at(self, now: datetime) -> bool:
        return now > self.expires_at

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.id,
            "stage": self.stage,
            "status": self.status,
            "campaign_run_id": self.campaign_run_id,
            "batch_id": self.batch_id,
            "slot": self.slot,
            "target_slots": list(self.target_slots),
            "stage_target_count": self.stage_target_count,
            "stage_amount_minor": self.stage_amount_minor,
            "batch_recipient_count": self.batch_recipient_count,
            "batch_exposure_minor": self.batch_exposure_minor,
            "stage_issue_price_minor": self.stage_issue_price_minor,
            "batch_issue_price_minor": self.batch_issue_price_minor,
            "plan_expires_at": self.expires_at.isoformat(),
            **OpsPrincipal(
                account=self.principal,
                session_fingerprint=self.session_fingerprint,
            ).as_safe_dict(),
        }


@dataclass(frozen=True)
class StoredOperation:
    """A durable operation as a page or a worker needs it."""

    id: int
    approval_id: int
    stage: str
    status: str
    campaign_run_id: int
    batch_id: int | None
    slot: int | None
    principal: str
    attempts: int
    queued_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    outcome_code: str | None
    reason_codes: tuple[str, ...] = ()
    result: dict[str, Any] = field(default_factory=dict)

    @property
    def finished(self) -> bool:
        return self.status in VOUCHER_PRODUCTION_OPERATION_TERMINAL

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.id,
            "approval_id": self.approval_id,
            "stage": self.stage,
            # queued / running / completed / refused / expired / interrupted.
            # Six states and not one of them is "delivered": what reached a
            # customer is the per-item delivery counters, never this field.
            "status": self.status,
            "campaign_run_id": self.campaign_run_id,
            "batch_id": self.batch_id,
            "slot": self.slot,
            "principal": self.principal,
            "attempts": self.attempts,
            "queued_at": self.queued_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "outcome_code": self.outcome_code,
            "reason_codes": list(self.reason_codes),
            "finished": self.finished,
            "report": dict(self.result),
        }


def _approval(row: EasyWeekVoucherProductionApproval) -> StoredApproval:
    slots = tuple(sorted(int(value) for value in (row.target_slots or [])))
    return StoredApproval(
        id=int(row.id),
        request_schema_version=row.request_schema_version,
        product_contract_version=row.product_contract_version,
        stage=str(row.stage),
        principal=str(row.principal),
        session_fingerprint=str(row.session_fingerprint),
        campaign_run_id=int(row.campaign_run_id),
        batch_id=int(row.batch_id) if row.batch_id is not None else None,
        slot=slots[0] if row.stage == STAGE_REFUND and slots else None,
        target_slots=slots,
        stage_target_count=int(row.stage_target_count),
        stage_amount_minor=int(row.stage_amount_minor),
        batch_recipient_count=int(row.batch_recipient_count),
        batch_exposure_minor=int(row.batch_exposure_minor),
        stage_issue_price_minor=int(row.stage_issue_price_minor),
        batch_issue_price_minor=int(row.batch_issue_price_minor),
        plan_digest=str(row.plan_digest),
        plan_issued_at=row.plan_issued_at,
        expires_at=row.expires_at,
        status=str(row.status),
        frozen_digest=row.frozen_digest,
        stop_generation_at_plan=int(row.stop_generation_at_plan or 0),
    )


def _operation(row: EasyWeekVoucherProductionOperation) -> StoredOperation:
    return StoredOperation(
        id=int(row.id),
        approval_id=int(row.approval_id),
        stage=str(row.stage),
        status=str(row.status),
        campaign_run_id=int(row.campaign_run_id),
        batch_id=int(row.batch_id) if row.batch_id is not None else None,
        slot=int(row.slot) if row.slot is not None else None,
        principal=str(row.principal),
        attempts=int(row.attempts or 0),
        queued_at=row.queued_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        outcome_code=row.outcome_code,
        reason_codes=tuple(str(value) for value in (row.reason_codes or [])),
        result=dict(row.result or {}),
    )


async def record_audit(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    principal: OpsPrincipal,
    action: str,
    outcome: str,
    stage: str | None = None,
    campaign_run_id: int | None = None,
    batch_id: int | None = None,
    approval_id: int | None = None,
    operation_id: int | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """One audit row. Safe facts only; never a token, a code or a payload.

    Deliberately best effort for the CALLER: an audit write that fails must not
    turn a completed action into an apparent failure, and the action it describes
    is already durable in its own row. The exception is logged by the caller's
    error handling, not swallowed into a success claim about the action itself.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekVoucherProductionAudit(
                    at=utcnow(),
                    provider=PROVIDER_EASYWEEK,
                    principal=principal.account,
                    session_fingerprint=principal.session_fingerprint,
                    identification_limit=principal.identification_limit,
                    action=action,
                    stage=stage,
                    campaign_run_id=campaign_run_id,
                    batch_id=batch_id,
                    approval_id=approval_id,
                    operation_id=operation_id,
                    outcome=outcome,
                    detail=dict(detail or {}),
                )
            )


async def store_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    principal: OpsPrincipal,
    stage: str,
    campaign_run_id: int,
    batch_id: int | None,
    targets: StageTargets,
    plan_digest: str,
    plan_issued_at: datetime,
    ttl: timedelta,
    campaign_period_start: datetime,
    campaign_period_end: datetime,
    issuer_pinned: bool,
    issuer_membership_proven: bool,
    runtime_identity_bound: bool,
    baseline_version: str,
    frozen_digest: str | None,
    stop_generation_at_plan: int = 0,
    request_schema_version: str = PRODUCTION_SCHEMA_VERSION,
    product_contract_version: str | None = None,
) -> StoredApproval:
    """Write the immutable offer. Only ever called for a plan that was READY.

    ``expires_at`` is derived from when the plan was ISSUED, never from now:
    §43.4 requires that queueing not extend the window, and a TTL measured from
    the write would quietly do exactly that.
    """
    async with session_maker() as session:
        async with session.begin():
            row = EasyWeekVoucherProductionApproval(
                batch_scope=PRODUCTION_SCOPE,
                request_schema_version=request_schema_version,
                product_contract_version=production_contract(
                    request_schema_version, contract_version=product_contract_version
                ).version,
                provider=PROVIDER_EASYWEEK,
                company_id=KARLSRUHE_COMPANY_ID,
                stage=stage,
                principal=principal.account,
                session_fingerprint=principal.session_fingerprint,
                identification_limit=principal.identification_limit,
                campaign_run_id=campaign_run_id,
                batch_id=batch_id,
                target_slots=list(targets.slots),
                target_slot_count=len(targets.slots),
                stage_target_count=targets.stage_target_count,
                stage_amount_minor=targets.stage_amount_minor,
                batch_recipient_count=targets.batch_recipient_count,
                batch_exposure_minor=targets.batch_exposure_minor,
                stage_issue_price_minor=targets.stage_issue_price_minor,
                batch_issue_price_minor=targets.batch_issue_price_minor,
                campaign_period_start=campaign_period_start,
                campaign_period_end=campaign_period_end,
                plan_digest=plan_digest,
                plan_issued_at=plan_issued_at,
                expires_at=plan_issued_at + ttl,
                issuer_pinned=issuer_pinned,
                issuer_membership_proven=issuer_membership_proven,
                runtime_identity_bound=runtime_identity_bound,
                baseline_version=baseline_version,
                frozen_digest=frozen_digest,
                stop_generation_at_plan=stop_generation_at_plan,
                status=VOUCHER_PRODUCTION_APPROVAL_PENDING,
            )
            session.add(row)
            await session.flush()
            return _approval(row)


async def load_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
) -> StoredApproval | None:
    async with session_maker() as session:
        row = await session.get(EasyWeekVoucherProductionApproval, approval_id)
        return _approval(row) if row is not None else None


async def load_operation(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    operation_id: int,
) -> StoredOperation | None:
    async with session_maker() as session:
        row = await session.get(EasyWeekVoucherProductionOperation, operation_id)
        return _operation(row) if row is not None else None


async def operation_for_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
) -> StoredOperation | None:
    async with session_maker() as session:
        row = (
            await session.execute(
                select(EasyWeekVoucherProductionOperation).where(
                    EasyWeekVoucherProductionOperation.approval_id == approval_id
                )
            )
        ).scalar_one_or_none()
        return _operation(row) if row is not None else None


@dataclass(frozen=True)
class ConfirmOutcome:
    """What a confirm did: created an operation, found one, or refused."""

    operation: StoredOperation | None
    reasons: tuple[str, ...] = ()
    # True when this call is the one that created the operation. A second POST of
    # the same click gets the same operation with ``created=False``, which is how
    # the API answers "your click worked, once" rather than failing.
    created: bool = False

    @property
    def accepted(self) -> bool:
        return self.operation is not None


async def confirm_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
    principal: OpsPrincipal,
    confirmed_count: int | None,
    confirmed_amount_minor: int | None,
    now: datetime | None = None,
) -> ConfirmOutcome:
    """Consume ONE approval and create ONE durable operation, atomically.

    Every refusal here happens before any operation exists, so a refused confirm
    has provably started nothing.

    What is checked, and why each one:

    * the approval exists, and belongs to the account presenting it — an id is
      not a capability, and another session's offer is not this one's to spend;
    * it has not expired — a plan read half an hour ago describes a world that
      has had half an hour to change;
    * it is still pending — and if it is not, the operation it already became is
      returned instead of an error;
    * the two numbers the operator confirmed are the stage's own numbers. The
      browser cannot widen anything by sending them, because they are COMPARED,
      never used; what they prove is that the human agreed to this count and this
      amount and not to whatever the page last rendered.

    Two admission rules keep a batch doing one thing at a time (review R1), and
    both are checked here, under the batch header's row lock — the same lock
    ``request_stop`` and every per-item claim take, which is what makes them
    atomic rather than almost atomic:

    *One operation at a time.* A batch with a queued or running operation admits no
    second one. Without this, confirming a plan prepared earlier would clear the
    running operation's stop and let it carry on through the slots behind a request
    that was still in flight.

    *A stop is lifted only by a plan that saw it.* On schemas ``1`` and ``2`` the
    approval records the batch's stop generation at plan time. While a stop is
    active, a confirmation is admitted only when that number still matches, so a
    plan from before the stop — the one sitting in the operator's other tab — is
    refused rather than becoming a resume nobody asked for. A matching generation
    means the plan was built in full knowledge of the stop, which is precisely the
    "fresh plan, confirmed" that §43.6 requires for continuing, and on those two
    schemas it is still the only thing that lifts one.

    *On the fixed €10 contract, nothing lifts it.* §45.4 makes an explicit stop
    terminal for request schema ``3``, so an active stop refuses every CREATE, PAY
    and DELIVER confirmation for that batch whatever its generation says — a fresh
    plan included. A REFUND is the one exemption, because it sends nothing and
    returning money is what has to keep working after a stop. The terminality is
    read from the batch header's own schema, so a payload cannot name an older one
    to get the slots back.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherProductionApproval)
                    .where(EasyWeekVoucherProductionApproval.id == approval_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return ConfirmOutcome(operation=None, reasons=(APPROVAL_UNKNOWN,))

            stored = _approval(row)
            if stored.principal != principal.account:
                return ConfirmOutcome(operation=None, reasons=(APPROVAL_PRINCIPAL_MISMATCH,))

            if stored.status == VOUCHER_PRODUCTION_APPROVAL_CONSUMED:
                existing = (
                    await session.execute(
                        select(EasyWeekVoucherProductionOperation).where(
                            EasyWeekVoucherProductionOperation.approval_id == approval_id
                        )
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    # The second half of a double-click, a retried POST, a
                    # refresh or the other tab. One operation, handed back.
                    return ConfirmOutcome(operation=_operation(existing), reasons=(APPROVAL_ALREADY_CONSUMED,))
                return ConfirmOutcome(operation=None, reasons=(APPROVAL_ALREADY_CONSUMED,))

            if stored.expired_at(moment):
                return ConfirmOutcome(operation=None, reasons=(PLAN_EXPIRED,))

            # The batch's own gate. Taken AFTER the approval's row lock and always
            # in this order — approval, then header — so two confirmations racing
            # on one batch cannot deadlock by taking them the other way round.
            if stored.batch_id is not None:
                admission = await ledger_module.admission_locked(session, batch_id=stored.batch_id)
                if not admission.exists:
                    return ConfirmOutcome(operation=None, reasons=(APPROVAL_UNKNOWN,))
                if admission.busy:
                    # Somebody else's turn. Reported rather than queued: a second
                    # concurrent operation is not a thing this phase has.
                    return ConfirmOutcome(operation=None, reasons=(OPERATION_IN_FLIGHT,))
                if admission.terminally_stopped and stored.stage != STAGE_REFUND:
                    # §45.4, checked before the generation comparison and
                    # deliberately not subject to it: on the fixed €10 contract an
                    # explicit stop is terminal, so the plan HAVING been built in
                    # full knowledge of the stop is no longer a reason to carry on.
                    # A fresh plan, a second operator and a second tab all land
                    # here. A refund is exempt: it sends nothing and getting money
                    # back is exactly what must keep working after a stop.
                    #
                    # Taken off the SAME locked read as the stop itself, so this
                    # cannot pair a live stop with a schema seen at another moment.
                    return ConfirmOutcome(operation=None, reasons=(STOP_TERMINAL,))
                if admission.stop_active and stored.stop_generation_at_plan != admission.stop_generation:
                    # A plan from before this stop. It cannot be the decision to
                    # carry on, because its author had not seen the stop.
                    return ConfirmOutcome(operation=None, reasons=(STOP_ACTIVE,))

            if confirmed_count != stored.stage_target_count:
                return ConfirmOutcome(operation=None, reasons=(APPROVAL_COUNT_UNCONFIRMED,))
            if confirmed_amount_minor != stored.stage_amount_minor:
                return ConfirmOutcome(operation=None, reasons=(APPROVAL_EXPOSURE_UNCONFIRMED,))

            row.status = VOUCHER_PRODUCTION_APPROVAL_CONSUMED
            row.consumed_at = moment
            row.updated_at = moment

            operation = EasyWeekVoucherProductionOperation(
                approval_id=approval_id,
                batch_scope=PRODUCTION_SCOPE,
                provider=PROVIDER_EASYWEEK,
                company_id=int(row.company_id),
                stage=stored.stage,
                campaign_run_id=stored.campaign_run_id,
                batch_id=stored.batch_id,
                slot=stored.slot,
                principal=principal.account,
                session_fingerprint=principal.session_fingerprint,
                identification_limit=principal.identification_limit,
                status=VOUCHER_PRODUCTION_OPERATION_QUEUED,
                attempts=0,
                queued_at=moment,
            )
            # The unique constraint on ``approval_id`` is what makes this the one
            # operation. A second confirm that got this far concurrently fails
            # here, in PostgreSQL, and its transaction is rolled back whole —
            # which is why the approval and the operation have to be written
            # together rather than in two steps.
            session.add(operation)
            await session.flush()

            if stored.batch_id is not None and stored.stage != STAGE_REFUND:
                await ledger_module.clear_stop_locked(
                    session,
                    batch_id=stored.batch_id,
                    cleared_by=principal.account,
                    now=moment,
                )
            return ConfirmOutcome(operation=_operation(operation), reasons=(), created=True)


async def claim_next_operation(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    owner: str,
    now: datetime | None = None,
    lease: timedelta = LEASE,
) -> StoredOperation | None:
    """Take ownership of the oldest queued operation, or answer ``None``.

    ``FOR UPDATE SKIP LOCKED`` plus a compare-and-set on ``status``: two
    executors — which the supported topology does not have, and which a
    mis-deploy can still produce — cannot both take the same row, and the loser
    moves on rather than blocking.

    Only ``queued`` rows are eligible. An ``interrupted`` one is never picked up
    again: see this module's docstring.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherProductionOperation)
                    .where(EasyWeekVoucherProductionOperation.status == VOUCHER_PRODUCTION_OPERATION_QUEUED)
                    .order_by(EasyWeekVoucherProductionOperation.id.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.status = VOUCHER_PRODUCTION_OPERATION_RUNNING
            row.lease_owner = owner
            row.lease_expires_at = moment + lease
            row.started_at = moment
            row.attempts = int(row.attempts or 0) + 1
            row.updated_at = moment
            await session.flush()
            return _operation(row)


async def renew_lease(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    operation_id: int,
    owner: str,
    lease: timedelta = LEASE,
    now: datetime | None = None,
) -> bool:
    """Push this operation's lease out while its owner is still working."""
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherProductionOperation)
                    .where(EasyWeekVoucherProductionOperation.id == operation_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None or row.status != VOUCHER_PRODUCTION_OPERATION_RUNNING or row.lease_owner != owner:
                return False
            row.lease_expires_at = moment + lease
            row.updated_at = moment
            await session.flush()
            return True


async def finish_operation(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    operation_id: int,
    owner: str | None,
    status: str,
    outcome_code: str | None,
    reason_codes: list[str] | tuple[str, ...] = (),
    result: dict[str, Any] | None = None,
    batch_id: int | None = None,
    now: datetime | None = None,
) -> StoredOperation | None:
    """Record a terminal outcome, once.

    ``batch_id`` backfills the link for a FREEZE, which is the one stage whose
    batch does not exist when its approval is written. Without it the operation
    that created a mailing would be missing from that mailing's own history —
    the operator would see the create, the pay and the send, and not the decision
    that started them. Only ever filled in, never changed: a row that already
    names a batch keeps it.

    Refuses to move an operation that is already finished: a late writer must not
    overwrite what an earlier one proved, and in particular must not replace an
    ``interrupted`` row — whose whole meaning is "a human has to look" — with a
    tidier-looking one.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherProductionOperation)
                    .where(EasyWeekVoucherProductionOperation.id == operation_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            if row.status in VOUCHER_PRODUCTION_OPERATION_TERMINAL:
                return _operation(row)
            if owner is not None and row.lease_owner not in (None, owner):
                # Somebody else owns it. Not ours to finish.
                return _operation(row)
            if batch_id is not None and row.batch_id is None:
                row.batch_id = batch_id
            row.status = status
            row.outcome_code = outcome_code
            row.reason_codes = list(dict.fromkeys(reason_codes))
            row.result = dict(result or {})
            row.finished_at = moment
            row.lease_owner = None
            row.lease_expires_at = None
            row.updated_at = moment
            await session.flush()
            return _operation(row)


async def interrupt_abandoned(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    now: datetime | None = None,
    include_all_running: bool = False,
) -> list[StoredOperation]:
    """Mark operations whose executor is gone as ``interrupted``. Never ``queued``.

    Two callers, one transition:

    * the executor at START-UP, with ``include_all_running=True``. The supported
      topology is one API and ONE dedicated executor with no rolling deploy, so
      an executor that is only now starting cannot be the owner of anything that
      claims to be running: every such row belongs to a process that died.
    * the executor's own loop, for leases that expired while it was up — which
      covers a second executor somebody deployed by mistake, and a worker killed
      between the claim and the lease renewal.

    ``interrupted`` is terminal on purpose. The ledger distinguishes a slot that
    was never claimed from one whose request may have reached EasyWeek or Meta,
    and this state is what makes the UI say so rather than offering a retry that
    could charge or message somebody twice.
    """
    moment = now or utcnow()
    interrupted: list[StoredOperation] = []
    async with session_maker() as session:
        async with session.begin():
            stmt = (
                select(EasyWeekVoucherProductionOperation)
                .where(EasyWeekVoucherProductionOperation.status == VOUCHER_PRODUCTION_OPERATION_RUNNING)
                .order_by(EasyWeekVoucherProductionOperation.id.asc())
                .with_for_update(skip_locked=True)
            )
            rows = list((await session.execute(stmt)).scalars().all())
            for row in rows:
                lease_over = row.lease_expires_at is not None and row.lease_expires_at <= moment
                if not include_all_running and not lease_over:
                    continue
                row.status = VOUCHER_PRODUCTION_OPERATION_INTERRUPTED
                row.outcome_code = EXECUTION_INTERRUPTED
                row.reason_codes = [EXECUTION_INTERRUPTED]
                row.finished_at = moment
                row.lease_owner = None
                row.lease_expires_at = None
                row.updated_at = moment
                await session.flush()
                interrupted.append(_operation(row))
    return interrupted


async def list_operations(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int | None = None,
    campaign_run_id: int | None = None,
    limit: int = 50,
) -> list[StoredOperation]:
    """Newest first. What a mailing page shows as its activity."""
    async with session_maker() as session:
        stmt = select(EasyWeekVoucherProductionOperation).order_by(EasyWeekVoucherProductionOperation.id.desc())
        if batch_id is not None:
            stmt = stmt.where(EasyWeekVoucherProductionOperation.batch_id == batch_id)
        if campaign_run_id is not None:
            stmt = stmt.where(EasyWeekVoucherProductionOperation.campaign_run_id == campaign_run_id)
        rows = list((await session.execute(stmt.limit(limit))).scalars().all())
        return [_operation(row) for row in rows]


async def active_operation(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int | None = None,
    campaign_run_id: int | None = None,
) -> StoredOperation | None:
    """The queued or running operation of this mailing, if there is one.

    What stops the UI from offering a second stage while one is in flight — and
    only the UI: the server's own defence is the approval, the claim and the
    per-item state machine, none of which depend on a button being hidden.
    """
    async with session_maker() as session:
        stmt = (
            select(EasyWeekVoucherProductionOperation)
            .where(
                EasyWeekVoucherProductionOperation.status.in_(
                    [VOUCHER_PRODUCTION_OPERATION_QUEUED, VOUCHER_PRODUCTION_OPERATION_RUNNING]
                )
            )
            .order_by(EasyWeekVoucherProductionOperation.id.asc())
            .limit(1)
        )
        if batch_id is not None:
            stmt = stmt.where(EasyWeekVoucherProductionOperation.batch_id == batch_id)
        if campaign_run_id is not None:
            stmt = stmt.where(EasyWeekVoucherProductionOperation.campaign_run_id == campaign_run_id)
        row = (await session.execute(stmt)).scalar_one_or_none()
        return _operation(row) if row is not None else None


def stage_targets_for(
    *,
    stage: str,
    slots: tuple[int, ...] | list[int],
    unit_price_minor: int,
    batch_recipient_count: int,
    batch_exposure_minor: int,
    issue_price_minor: int | None = None,
) -> StageTargets:
    """What an operator is about to approve for THIS stage.

    The money column answers "what does pressing this button move?", which is not
    the batch total except by coincidence on the first stage of a full batch:

    * ``freeze`` — the operator states the batch's own size and cost (§42.5), so
      here the two coincide deliberately;
    * ``create`` and ``pay`` — €15 per slot THIS stage will touch, which after a
      partial run is the remainder and not the total;
    * ``deliver`` — no money at all. A message is not a purchase, and showing a
      sum beside a send button would invite an operator to think it was.
    * ``refund`` — €15 coming back for one named slot.

    ``unit_price_minor`` is the NOMINAL per slot — what the operator confirms — and
    ``issue_price_minor`` is what the same slots will actually charge. They default
    to the same number, which is what every paid contract means by both; the free
    gift certificate is the case where the second is zero while the first is not,
    and a stage that reported only one of them would either ask somebody to approve
    paying for a gift or let a 330 EUR giveaway be confirmed as nothing.
    """
    ordered = tuple(sorted(int(value) for value in slots))
    count = len(ordered)
    price = unit_price_minor if issue_price_minor is None else issue_price_minor
    batch_issue_price = batch_exposure_minor if issue_price_minor is None else price * batch_recipient_count
    if stage == STAGE_FREEZE:
        amount = batch_exposure_minor
        charged = batch_issue_price
    elif stage in ("create", "pay", STAGE_REFUND):
        amount = count * unit_price_minor
        charged = count * price
    else:
        amount = 0
        charged = 0
    return StageTargets(
        slots=ordered,
        stage_target_count=count,
        stage_amount_minor=amount,
        batch_recipient_count=batch_recipient_count,
        batch_exposure_minor=batch_exposure_minor,
        stage_issue_price_minor=charged,
        batch_issue_price_minor=batch_issue_price,
    )


# Mapping from a stage report's own outcome to the operation status that records
# it. "The stage ran and did what it said" is `completed`; everything that did
# not get that far is `refused`, which is a statement about the OPERATION and
# never about whether an external effect happened — that is in the report.
OUTCOME_TO_STATUS: Final[dict[str, str]] = {
    "applied": VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    "frozen": VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    "observed": VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    "partial": VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    "stopped": VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    "unknown": VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    "nothing_to_do": VOUCHER_PRODUCTION_OPERATION_REFUSED,
    "refused": VOUCHER_PRODUCTION_OPERATION_REFUSED,
    "rejected": VOUCHER_PRODUCTION_OPERATION_REFUSED,
}


def status_for_outcome(outcome: str) -> str:
    """Which operation status records this stage outcome.

    ``unknown`` maps to ``completed`` deliberately: the OPERATION finished — it
    ran, it stopped where it should have, and it wrote down what it knows. What is
    unknown is one slot's external effect, which lives in the report and in the
    batch's ``reconciliation_required`` flag. Calling the operation itself
    "failed" would invite a retry, and a retry is the one thing that must not
    happen after an unknown.
    """
    return OUTCOME_TO_STATUS.get(outcome, VOUCHER_PRODUCTION_OPERATION_REFUSED)


__all__ = [
    "IDENTIFICATION_LIMIT_SHARED",
    "LEASE",
    "VOUCHER_PRODUCTION_OPERATION_EXPIRED",
    "ConfirmOutcome",
    "OpsPrincipal",
    "StageTargets",
    "StoredApproval",
    "StoredOperation",
    "active_operation",
    "claim_next_operation",
    "confirm_approval",
    "finish_operation",
    "interrupt_abandoned",
    "list_operations",
    "load_approval",
    "load_operation",
    "operation_for_approval",
    "record_audit",
    "renew_lease",
    "stage_targets_for",
    "status_for_outcome",
    "store_approval",
]
