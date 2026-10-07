"""From a browser click to a durable operation, and from there to a stage (§43).

One module, used by exactly two callers, which is what keeps the authorisation
honest:

* the Ops API, which OFFERS a plan and CONFIRMS it. It never executes anything.
* the dedicated executor, which EXECUTES a confirmed operation. It never
  authorises anything.

No HTTP handler here starts a shell, a subprocess or a stage. A request that
confirmed an operation returns as soon as that operation is committed; the work
happens in the executor, which is why closing the tab, logging out again or
losing the response changes nothing about it.

The plan is rebuilt before execution, deliberately
--------------------------------------------------
The approval carries the digest of the plan the operator read. The executor
builds the plan AGAIN, live, seconds before the first claim, and hands the stored
digest to the same ``verify_plan_authorisation`` §42 already used. So every drift
check that phase earned still applies, unchanged, to a browser-driven stage: an
edited preview, an opt-out, a template paused by Meta, a changed sender, a
baseline that moved, a staffer that is no longer the approved one — any of them
changes the rebuilt digest, and the stage refuses before the first external call.

What the browser cannot do
--------------------------
It cannot name a slot, a staffer, a digest, a timestamp or an amount that is then
used. The two numbers a confirm carries are compared against the stored approval
and discarded. Everything else is read from the approval row the SERVER wrote.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import VoucherDeliveryClient
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_production.authorisation import PLAN_MAX_AGE, phrase_for_digest
from altegio_bot.campaigns.easyweek_voucher_production.composition import (
    BatchApproval,
    prove_production_composition,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    ACCOUNT_UNCONFIGURED,
    API_UNAVAILABLE,
    APPROVAL_COUNT_MISMATCH,
    APPROVAL_COUNT_MISSING,
    APPROVAL_EXPOSURE_MISMATCH,
    APPROVAL_EXPOSURE_MISSING,
    APPROVAL_NOT_READY,
    DATABASE_UNAVAILABLE,
    EXECUTION_INTERRUPTED,
    EXECUTOR_UNAVAILABLE,
    ISSUER_SUPPLIED_BY_CLIENT,
    KARLSRUHE_COMPANY_ID,
    OPERATION_UNKNOWN,
    PLAN_EXPIRED,
    PRODUCTION_DISABLED,
    PRODUCTION_STAGES,
    RECONCILE_BUSY,
    RUNTIME_IDENTITY_UNUSABLE,
    SLOT_UNKNOWN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
    UNKNOWN_STAGE,
)
from altegio_bot.campaigns.easyweek_voucher_production.issuer import (
    APPROVED_ISSUER_DISPLAY_NAME,
    pinned_issuer,
)
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError, EasyWeekError
from altegio_bot.easyweek_voucher_identity import (
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationClient
from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PRODUCTION_CONTRACT,
    production_contract,
)
from altegio_bot.models.models import (
    VOUCHER_PRODUCTION_OPERATION_EXPIRED,
    VOUCHER_PRODUCTION_OPERATION_INTERRUPTED,
    VOUCHER_PRODUCTION_OPERATION_REFUSED,
)
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

# The EasyWeek sender line this phase resolves through, as in §42.
SENDER_CODE = "default"

# The stages an operator drives from the browser, in the only order they may
# happen. A tuple a reader can see, deliberately: there is no function anywhere
# that walks two of them.
UI_STAGES = (STAGE_FREEZE, STAGE_CREATE, STAGE_PAY, STAGE_DELIVER)

# What :func:`_request_for` is asked for when the action issues nothing — a
# reconcile. Spelled as its own name rather than passing ``STAGE_REFUND``, which
# would read as though a readback were a refund: what the two share is only that
# neither sells anything, so neither consults the issuer pin.
NON_ISSUING = STAGE_REFUND


class Transports:
    """How a stage reaches EasyWeek and Meta. Replaceable, for tests only.

    The real implementation opens the same three clients §42's CLI opened, for the
    length of one stage. Tests pass fakes; nothing about the authorisation path
    changes with them, because none of it is in here.
    """

    @contextlib.asynccontextmanager
    async def reader(self) -> AsyncIterator[Any]:
        async with EasyWeekClient() as client:
            yield client

    @contextlib.asynccontextmanager
    async def mutator(self) -> AsyncIterator[Any]:
        async with EasyWeekVoucherMutationClient() as client:
            yield client

    @contextlib.asynccontextmanager
    async def sender(self) -> AsyncIterator[Any]:
        async with VoucherDeliveryClient() as client:
            yield client


@dataclass(frozen=True)
class StageOffer:
    """What the server offers the browser for one stage of one mailing.

    ``approval_id`` exists only when the plan was READY. An unready plan is still
    returned in full — an operator has to see WHICH fact is missing — but there is
    nothing to confirm, so there is no id to confirm it with.
    """

    stage: str
    ready: bool
    reasons: tuple[str, ...]
    approval: operations_module.StoredApproval | None
    plan: dict[str, Any] = field(default_factory=dict)
    targets: operations_module.StageTargets | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "ready": self.ready,
            "reasons": list(self.reasons),
            "approval": self.approval.as_safe_dict() if self.approval is not None else None,
            "targets": self.targets.as_safe_dict() if self.targets is not None else None,
            # The issuer, said in words the operator reads before confirming.
            "issuer_display_name": APPROVED_ISSUER_DISPLAY_NAME,
            "issuer_selectable_by_operator": False,
            "plan": dict(self.plan),
        }


def _request_for(
    *,
    stage: str,
    preview_run_id: int,
    batch_id: int | None,
    frozen_staffer_uuid: str | None,
    schema_version: str = "3",
    product_contract_version: str | None = None,
) -> tuple[runner_module.ProductionRequest | None, tuple[str, ...]]:
    """The frozen identity for this action, or the reasons it is unusable.

    Where the staffer comes from depends on whether the action can ISSUE anything:

    * a freeze, create, pay or deliver takes it from the pinned issuer, so a valid
      UUID that is not the approved one never becomes a ``ProductionRequest`` at
      all;
    * a refund — and a reconcile, which passes :data:`NON_ISSUING` — takes it from
      the batch's own frozen row. §43.9 is explicit that the issuer rule must not
      reach into an allowed pre-send refund: the batch already records who sold the
      voucher, so the money can come back after the setting is emptied, corrected
      or pointed at somebody new. Nothing is re-attributed; the frozen row is read,
      not written. The same is true of a readback, which must stay available
      exactly when the acting path is blocked.
    """
    contract = production_contract(schema_version, contract_version=product_contract_version)
    account = (settings.easyweek_voucher_production_mailing_account_uuid or "").strip()
    reasons: list[str] = []
    if not account:
        reasons.append(ACCOUNT_UNCONFIGURED)

    if stage == STAGE_REFUND:
        staffer = (frozen_staffer_uuid or "").strip()
        if not staffer:
            # No batch, so nothing to refund. Reported as an unknown slot rather
            # than as a staffer problem, because that is the actual situation.
            reasons.append(SLOT_UNKNOWN)
    else:
        issuer = pinned_issuer(settings.easyweek_voucher_production_mailing_staffer_uuid)
        staffer = issuer.uuid or ""
        if issuer.reason is not None:
            reasons.append(issuer.reason)

    if reasons:
        return None, tuple(dict.fromkeys([*reasons, RUNTIME_IDENTITY_UNUSABLE]))
    return (
        runner_module.ProductionRequest(
            preview_run_id=preview_run_id,
            sender_code=SENDER_CODE,
            staffer_uuid=staffer,
            payment_account_uuid=account,
            batch_id=batch_id,
            company_id=KARLSRUHE_COMPANY_ID,
            location_uuid=KARLSRUHE_LOCATION_UUID,
            voucher_template_uuid=contract.template_uuid,
            schema_version=schema_version,
            product_contract_version=contract.version,
        ),
        (),
    )


def _parsed(value: str | None) -> datetime | None:
    """An ISO timestamp from the ledger, or ``None``. Never an exception."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


async def _frozen_staffer(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int | None,
) -> str | None:
    if batch_id is None:
        return None
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    return snapshot.staffer_uuid if snapshot.exists else None


def _refused_offer(stage: str, reasons: tuple[str, ...]) -> StageOffer:
    return StageOffer(stage=stage, ready=False, reasons=reasons, approval=None)


@dataclass(frozen=True)
class RecipientLine:
    """One slot and the person it addresses — for the authorised UI only.

    Why this exists at all (review R7): every other surface of this phase speaks in
    slot numbers, which is right for a ledger and useless to an operator deciding
    whether to return somebody's €15. A slot is an ordinal inside one frozen
    composition and nothing else; it is deliberately NOT the preview row id, and
    confusing the two would point an action at the wrong person.

    Why it is its own type rather than a field on the report: this carries a customer
    name, and the report goes into operation payloads, audit rows and diagnostics.
    Those must stay PII-free, so this is built per request, returned to an
    authenticated operator, and never passed to ``store_approval``, ``record_audit``
    or any ``as_safe_dict``. There is no path from here into a stored row.
    """

    slot: int
    campaign_recipient_id: int
    display_name: str
    preview_run_id: int
    recipient_basis: str | None = None
    manual_policy: str | None = None

    def as_ui_dict(self) -> dict[str, Any]:
        # Named `as_ui_dict`, not `as_safe_dict`, on purpose: the name is the
        # warning. Nothing that serialises reports may call this.
        return {
            "slot": self.slot,
            "campaign_recipient_id": self.campaign_recipient_id,
            "display_name": self.display_name,
            "preview_run_id": self.preview_run_id,
            "recipient_basis": self.recipient_basis,
            "manual_policy": self.manual_policy,
        }


async def recipient_lines(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
) -> tuple[RecipientLine, ...]:
    """Slot → recipient for one frozen batch, in slot order.

    Read from the preview row each slot was frozen from, which is the same source
    the preview editor shows, so the operator sees the name they curated. An empty
    display name is answered as a readable placeholder rather than a blank cell: a
    blank would be indistinguishable from a bug.
    """
    from altegio_bot.models.models import (
        CampaignRecipient,
        EasyWeekVoucherProductionBatchItem,
    )

    async with session_maker() as session:
        rows = (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem.slot,
                    EasyWeekVoucherProductionBatchItem.campaign_recipient_id,
                    EasyWeekVoucherProductionBatchItem.campaign_run_id,
                    CampaignRecipient.display_name,
                    EasyWeekVoucherProductionBatchItem.recipient_basis,
                    EasyWeekVoucherProductionBatchItem.manual_policy,
                )
                .join(
                    CampaignRecipient,
                    CampaignRecipient.id == EasyWeekVoucherProductionBatchItem.campaign_recipient_id,
                )
                .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
            )
        ).all()
    return tuple(
        RecipientLine(
            slot=int(row[0]),
            campaign_recipient_id=int(row[1]),
            display_name=(row[3] or "").strip() or f"без имени (строка preview {int(row[1])})",
            preview_run_id=int(row[2]),
            recipient_basis=row[4],
            manual_policy=row[5],
        )
        for row in rows
    )


@dataclass(frozen=True)
class CompositionView:
    """The audience an operator is about to approve. A READ, never an authorisation.

    Separating this from the freeze plan is review R3, and the defect it fixes made
    the feature unusable: "check the list" asked for a freeze plan, a freeze plan
    needs the count and the exposure, and those are the very numbers the operator was
    about to read off the list. The plan came back unready, the page stopped before
    revealing the fields, and there was no way forward.

    So the two jobs are now two things. This one proves the composition live and
    reports the real period, the real members, the real N and the real total. It
    stores no approval, signs no digest and reaches no customer, so it cannot
    authorise a freeze — and the operator then states the numbers for a plan that
    still demands them exactly.
    """

    proven: bool
    reasons: tuple[str, ...]
    campaign_period: str | None
    recipient_count: int
    total_exposure_minor: int
    unit_price_minor: int
    lines: tuple[RecipientLine, ...] = ()
    # WHICH audience this is, as the freeze would sign it. Reported so the page can
    # tell "the list I am showing" from "the list a plan just re-proved" (review F1):
    # one member swapped for another leaves the count and the money identical, so a
    # screen comparing only numbers would present a stale list as the confirmed one.
    # It is a digest over slots and preview rows — no name, number or secret.
    composition_digest: str | None = None

    def as_ui_dict(self) -> dict[str, Any]:
        return {
            "composition_proven": self.proven,
            "reasons": list(self.reasons),
            "campaign_period": self.campaign_period,
            "recipient_count": self.recipient_count,
            "total_exposure_minor": self.total_exposure_minor,
            "unit_price_minor": self.unit_price_minor,
            "composition_digest": self.composition_digest,
            "recipients": [line.as_ui_dict() for line in self.lines],
            "earned_recipient_count": sum(line.recipient_basis == "earned_first_visit" for line in self.lines),
            "manual_recipient_count": sum(line.recipient_basis == "operator_manual_selection" for line in self.lines),
            "issuer_display_name": APPROVED_ISSUER_DISPLAY_NAME,
        }


# The refusals that mean only "you have not stated the numbers yet". They are the
# whole point of the freeze plan and have no business in a composition READ.
_APPROVAL_NUMBER_REASONS = frozenset(
    {
        APPROVAL_COUNT_MISSING,
        APPROVAL_EXPOSURE_MISSING,
        APPROVAL_COUNT_MISMATCH,
        APPROVAL_EXPOSURE_MISMATCH,
    }
)


async def inspect_composition(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    preview_run_id: int,
    transports: Transports | None = None,
) -> CompositionView:
    """Prove this preview's audience and report it. Writes nothing, sends nothing.

    Uses the same live proof the freeze plan uses, so what the operator reads is what
    a freeze would act on — not a second, friendlier derivation that could disagree.
    The reasons about the missing count and exposure are filtered out, because asking
    for them is this screen's next step rather than a problem with the audience.
    """
    if not settings.easyweek_voucher_production_mailing_enabled:
        return CompositionView(
            proven=False,
            reasons=(PRODUCTION_DISABLED,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=CURRENT_PRODUCTION_CONTRACT.unit_price_minor,
        )
    carrier = transports or Transports()
    try:
        async with carrier.reader() as reader:
            async with session_maker() as session:
                composition = await prove_production_composition(
                    session,
                    preview_run_id=preview_run_id,
                    client_reader=reader,
                    now=utcnow(),
                    approval=None,
                    schema_version=CURRENT_PRODUCTION_CONTRACT.request_schema_version,
                )
    except EasyWeekConfigError:
        return CompositionView(
            proven=False,
            reasons=(RUNTIME_IDENTITY_UNUSABLE,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=CURRENT_PRODUCTION_CONTRACT.unit_price_minor,
        )
    except EasyWeekError:
        return CompositionView(
            proven=False,
            reasons=(API_UNAVAILABLE,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=CURRENT_PRODUCTION_CONTRACT.unit_price_minor,
        )
    except SQLAlchemyError:
        return CompositionView(
            proven=False,
            reasons=(DATABASE_UNAVAILABLE,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=CURRENT_PRODUCTION_CONTRACT.unit_price_minor,
        )

    reasons = tuple(reason for reason in composition.reasons if reason not in _APPROVAL_NUMBER_REASONS)
    lines = tuple(
        RecipientLine(
            slot=member.slot,
            campaign_recipient_id=member.campaign_recipient_id,
            display_name=(member.proof.client_display_name or "").strip()
            or f"без имени (строка preview {member.campaign_recipient_id})",
            preview_run_id=preview_run_id,
            recipient_basis=member.recipient_basis,
            manual_policy=member.manual_policy,
        )
        for member in composition.members
    )
    return CompositionView(
        # Proven for the purpose of this screen: a real audience the operator may
        # now put numbers to. The freeze still proves everything again, including
        # those numbers.
        proven=not reasons and bool(composition.members),
        reasons=reasons,
        campaign_period=composition.period_label,
        recipient_count=composition.recipient_count,
        total_exposure_minor=composition.total_exposure_minor,
        unit_price_minor=CURRENT_PRODUCTION_CONTRACT.unit_price_minor,
        # The same digest a freeze plan carries in its signed snapshot, so the two
        # are comparable at all. Only for a proven audience: there is no identity to
        # report for a composition that could not be established.
        composition_digest=composition.composition_digest() if composition.proven else None,
        lines=lines,
    )


async def offer_stage(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stage: str,
    principal: operations_module.OpsPrincipal,
    preview_run_id: int,
    batch_id: int | None = None,
    slot: int | None = None,
    expected_recipient_count: int | None = None,
    approved_exposure_minor: int | None = None,
    staffer_uuid_from_client: str | None = None,
    transports: Transports | None = None,
) -> StageOffer:
    """Build one stage plan live and, if it is ready, store it as an approval.

    Reads only, apart from the approval row. Creates no batch, sends nothing and
    reaches no customer: a plan is GETs and a row.

    ``staffer_uuid_from_client`` is accepted ONLY so it can be refused. Nothing in
    the browser has any business naming the issuer, and a request that tries is a
    refusal with its own reason code rather than a silently ignored field —
    silence would leave the next reader unsure whether it was honoured.
    """
    if staffer_uuid_from_client:
        return _refused_offer(stage, (ISSUER_SUPPLIED_BY_CLIENT,))
    if stage not in PRODUCTION_STAGES:
        return _refused_offer(stage, (UNKNOWN_STAGE,))
    if not settings.easyweek_voucher_production_mailing_enabled:
        # The fence, before anything opens a socket. A closed fence means the
        # offer does nothing at all, not "nothing that writes".
        return _refused_offer(stage, (PRODUCTION_DISABLED,))

    frozen = await ledger_module.load(session_maker, batch_id=batch_id) if batch_id is not None else None
    frozen_staffer = frozen.staffer_uuid if frozen is not None and frozen.exists else None
    request, reasons = _request_for(
        stage=stage,
        preview_run_id=preview_run_id,
        batch_id=batch_id,
        frozen_staffer_uuid=frozen_staffer,
        schema_version=frozen.schema_version if frozen is not None and frozen.exists else "3",
        product_contract_version=frozen.product_contract_version if frozen is not None and frozen.exists else None,
    )
    if request is None:
        return _refused_offer(stage, reasons)

    approval_numbers = BatchApproval(
        expected_recipient_count=expected_recipient_count,
        approved_exposure_minor=approved_exposure_minor,
    )
    carrier = transports or Transports()
    try:
        async with carrier.reader() as reader:
            async with session_maker() as session:
                plan, composition, _prereq, _baseline, snapshot = await runner_module.build_stage_plan(
                    session,
                    session_maker,
                    stage=stage,
                    request=request,
                    reader=reader,
                    order_reader=reader,
                    approval=approval_numbers,
                    slot=slot,
                )
    except EasyWeekConfigError:
        return _refused_offer(stage, (RUNTIME_IDENTITY_UNUSABLE,))
    except EasyWeekError:
        return _refused_offer(stage, (API_UNAVAILABLE,))
    except SQLAlchemyError:
        return _refused_offer(stage, (DATABASE_UNAVAILABLE,))

    safe_plan = plan.as_safe_dict()
    if not plan.ready:
        return StageOffer(stage=stage, ready=False, reasons=plan.reasons, approval=None, plan=safe_plan)

    # The batch's own size and cost, for the "this stage versus the whole batch"
    # distinction the confirmation screen has to draw.
    if stage == STAGE_FREEZE:
        batch_count = composition.recipient_count
        batch_exposure = composition.total_exposure_minor
        period_start = composition.campaign_period_start
        period_end = composition.campaign_period_end
    else:
        batch_count = int(snapshot.recipient_count or 0)
        batch_exposure = int(snapshot.total_exposure_minor or 0)
        # A ready plan for a later stage implies a frozen batch with a period, so
        # these are set. Parsed defensively anyway: the alternative is a 500 on a
        # page an operator reached, and a refusal naming a reason is a better
        # answer than a stack trace for something nobody can act on.
        period_start = _parsed(snapshot.campaign_period_start)
        period_end = _parsed(snapshot.campaign_period_end)
    if period_start is None or period_end is None:
        return StageOffer(stage=stage, ready=False, reasons=(APPROVAL_NOT_READY,), approval=None, plan=safe_plan)

    targets = operations_module.stage_targets_for(
        stage=stage,
        slots=plan.authorised_slots,
        unit_price_minor=production_contract(request.schema_version).unit_price_minor,
        batch_recipient_count=batch_count,
        batch_exposure_minor=batch_exposure,
    )
    if not targets.slots:
        return StageOffer(stage=stage, ready=False, reasons=(APPROVAL_NOT_READY,), approval=None, plan=safe_plan)

    # Which stop this plan is built in knowledge of (review R1). Read now, so a
    # confirmation can tell a continuation an operator decided on from a plan that
    # predates the stop they are currently looking at.
    generation = (
        await ledger_module.stop_generation(session_maker, batch_id=snapshot.batch_id)
        if snapshot.exists and snapshot.batch_id is not None
        else 0
    )
    stored = await operations_module.store_approval(
        session_maker,
        principal=principal,
        stage=stage,
        campaign_run_id=preview_run_id,
        batch_id=snapshot.batch_id if snapshot.exists else None,
        targets=targets,
        plan_digest=plan.digest,
        plan_issued_at=plan.issued_at,
        # Unchanged from §42, and measured from when the plan was ISSUED.
        ttl=PLAN_MAX_AGE,
        campaign_period_start=period_start,
        campaign_period_end=period_end,
        issuer_pinned=bool(safe_plan.get("snapshot", {}).get("issuer_pinned")),
        issuer_membership_proven=bool(safe_plan.get("snapshot", {}).get("issuer_membership_proven")),
        runtime_identity_bound=bool(safe_plan.get("snapshot", {}).get("runtime_identity_matches_frozen")),
        baseline_version=str((safe_plan.get("snapshot", {}).get("baseline") or {}).get("baseline_version") or ""),
        frozen_digest=snapshot.frozen_digest if snapshot.exists else composition.composition_digest(),
        stop_generation_at_plan=generation,
        request_schema_version=request.schema_version,
        product_contract_version=production_contract(request.schema_version).version,
    )
    await operations_module.record_audit(
        session_maker,
        principal=principal,
        action="plan",
        outcome="offered",
        stage=stage,
        campaign_run_id=preview_run_id,
        batch_id=stored.batch_id,
        approval_id=stored.id,
        detail=targets.as_safe_dict(),
    )
    return StageOffer(
        stage=stage,
        ready=True,
        reasons=(),
        approval=stored,
        plan=safe_plan,
        targets=targets,
    )


async def confirm_stage(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
    principal: operations_module.OpsPrincipal,
    confirmed_count: int | None,
    confirmed_amount_minor: int | None,
    executor_available: Callable[[], bool] | None = None,
) -> operations_module.ConfirmOutcome:
    """Spend one approval and queue one durable operation.

    Returns as soon as the operation is COMMITTED. Nothing has reached EasyWeek or
    Meta yet, and nothing in this process will: the executor picks it up.

    ``executor_available`` is checked before the approval is spent, so a
    deployment whose executor is not running refuses with a reason an operator can
    read instead of parking a confirmation in a queue nobody drains. It is a
    deliberate pre-check and not a guarantee — the executor can stop a second
    later, which is what the operation's own ``queued`` state and the mailing
    page are for.
    """
    if executor_available is not None and not executor_available():
        await operations_module.record_audit(
            session_maker,
            principal=principal,
            action="confirm",
            outcome=EXECUTOR_UNAVAILABLE,
            approval_id=approval_id,
        )
        return operations_module.ConfirmOutcome(operation=None, reasons=(EXECUTOR_UNAVAILABLE,))

    outcome = await operations_module.confirm_approval(
        session_maker,
        approval_id=approval_id,
        principal=principal,
        confirmed_count=confirmed_count,
        confirmed_amount_minor=confirmed_amount_minor,
    )
    await operations_module.record_audit(
        session_maker,
        principal=principal,
        action="confirm",
        outcome=(
            "queued"
            if outcome.created
            else ("already_queued" if outcome.accepted else ",".join(outcome.reasons) or "refused")
        ),
        stage=outcome.operation.stage if outcome.operation is not None else None,
        campaign_run_id=outcome.operation.campaign_run_id if outcome.operation is not None else None,
        batch_id=outcome.operation.batch_id if outcome.operation is not None else None,
        approval_id=approval_id,
        operation_id=outcome.operation.id if outcome.operation is not None else None,
        detail={
            "confirmed_count": confirmed_count,
            "confirmed_amount_minor": confirmed_amount_minor,
            "created": outcome.created,
        },
    )
    return outcome


async def request_stop(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
    principal: operations_module.OpsPrincipal,
) -> ledger_module.StopState:
    """Persist "stop after the current request" for this batch.

    Allowed with the fence closed and while an operation is running — both are
    exactly when an operator wants it. It refunds nothing, halts nothing, and
    blocks neither status, nor webhooks, nor reconciliation, nor an allowed
    pre-send refund.
    """
    state = await ledger_module.request_stop(session_maker, batch_id=batch_id, requested_by=principal.account)
    await operations_module.record_audit(
        session_maker,
        principal=principal,
        action="stop",
        outcome="stop_active" if state.active else "batch_unknown",
        batch_id=batch_id,
        detail=state.as_safe_dict(),
    )
    return state


@dataclass(frozen=True)
class ReconcileOutcome:
    """What a readback did, or the named reason it did not run.

    A report-or-``None`` would collapse "a stage is executing right now" into the
    same answer as "EasyWeek is unreachable", and those need different things from
    an operator: one is "wait and look again", the other is "something is wrong".
    """

    report: runner_module.StageReport | None
    reasons: tuple[str, ...] = ()

    @property
    def ran(self) -> bool:
        return self.report is not None


async def reconcile_batch(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    preview_run_id: int,
    batch_id: int,
    principal: operations_module.OpsPrincipal,
    transports: Transports | None = None,
) -> ReconcileOutcome:
    """Read the outside world back and resolve what can be resolved.

    GET-only against EasyWeek plus local writes that record what was READ. It
    needs no approval and no confirmation because it buys nothing and sends
    nothing — and it must stay available exactly when the rest is blocked: after a
    stop, after an unknown, and with the fence closed.

    What it must NOT run against is a stage that is executing right now (review R2).
    A readback's job is to reinterpret claims left behind by a process that died;
    done to a live claim it moves the row out from under the request in flight, and
    the success coming back has nowhere to land. Refused here with a readable
    reason, and refused again inside each write under the batch header's lock — this
    check is the courtesy, that one is the guarantee.
    """
    busy = await ledger_module.live_execution(session_maker, batch_id=batch_id)
    if busy is not None:
        await operations_module.record_audit(
            session_maker,
            principal=principal,
            action="reconcile",
            outcome=RECONCILE_BUSY,
            batch_id=batch_id,
            campaign_run_id=preview_run_id,
            detail={"blocking_operation_id": busy},
        )
        return ReconcileOutcome(report=None, reasons=(RECONCILE_BUSY,))
    frozen = await ledger_module.load(session_maker, batch_id=batch_id) if batch_id is not None else None
    frozen_staffer = frozen.staffer_uuid if frozen is not None and frozen.exists else None
    request, _reasons = _request_for(
        stage=NON_ISSUING,
        preview_run_id=preview_run_id,
        batch_id=batch_id,
        frozen_staffer_uuid=frozen_staffer,
        schema_version=frozen.schema_version if frozen is not None and frozen.exists else "3",
        product_contract_version=frozen.product_contract_version if frozen is not None and frozen.exists else None,
    )
    if request is None:
        return ReconcileOutcome(report=None, reasons=(RUNTIME_IDENTITY_UNUSABLE,))
    carrier = transports or Transports()
    try:
        async with carrier.reader() as reader:
            report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    except EasyWeekConfigError:
        return ReconcileOutcome(report=None, reasons=(RUNTIME_IDENTITY_UNUSABLE,))
    except EasyWeekError:
        return ReconcileOutcome(report=None, reasons=(API_UNAVAILABLE,))
    except SQLAlchemyError:
        return ReconcileOutcome(report=None, reasons=(DATABASE_UNAVAILABLE,))
    await operations_module.record_audit(
        session_maker,
        principal=principal,
        action="reconcile",
        outcome=report.outcome,
        batch_id=batch_id,
        campaign_run_id=preview_run_id,
        detail={
            "reconciliation_required": report.reconciliation_required,
            "manual_cleanup_required": report.manual_cleanup_required,
        },
    )
    return ReconcileOutcome(report=report)


async def execute_operation(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    operation: operations_module.StoredOperation,
    owner: str,
    transports: Transports | None = None,
    now: datetime | None = None,
) -> operations_module.StoredOperation | None:
    """Run one confirmed stage, then record what it turned out to be.

    The order is not negotiable:

    1. re-read the approval and check it has not expired — queue time is not
       reading time, and no worker re-approves anything;
    2. rebuild the plan LIVE and let §42's own verification compare the operator's
       stored digest against it;
    3. run the stage, which claims per item, commits, then makes at most one
       external call per slot;
    4. write the terminal outcome.

    Anything that goes wrong between 1 and 3 is a refusal with zero external
    effects. Anything that goes wrong inside 3 is already recorded per slot by the
    ledger, and this function's failure path says only that the operation stopped
    — never that nothing happened.
    """
    moment = now or utcnow()
    approval = await operations_module.load_approval(session_maker, approval_id=operation.approval_id)
    if approval is None:
        return await operations_module.finish_operation(
            session_maker,
            operation_id=operation.id,
            owner=owner,
            status=VOUCHER_PRODUCTION_OPERATION_REFUSED,
            outcome_code=OPERATION_UNKNOWN,
            reason_codes=[OPERATION_UNKNOWN],
        )

    if approval.expired_at(moment):
        # Waiting in the queue did not extend anything. A new plan and a new
        # confirmation are what continuing takes.
        return await operations_module.finish_operation(
            session_maker,
            operation_id=operation.id,
            owner=owner,
            status=VOUCHER_PRODUCTION_OPERATION_EXPIRED,
            outcome_code=PLAN_EXPIRED,
            reason_codes=[PLAN_EXPIRED],
        )

    if not settings.easyweek_voucher_production_mailing_enabled:
        return await operations_module.finish_operation(
            session_maker,
            operation_id=operation.id,
            owner=owner,
            status=VOUCHER_PRODUCTION_OPERATION_REFUSED,
            outcome_code=PRODUCTION_DISABLED,
            reason_codes=[PRODUCTION_DISABLED],
        )

    frozen = (
        await ledger_module.load(session_maker, batch_id=approval.batch_id) if approval.batch_id is not None else None
    )
    frozen_staffer = frozen.staffer_uuid if frozen is not None and frozen.exists else None
    request, reasons = _request_for(
        stage=approval.stage,
        preview_run_id=approval.campaign_run_id,
        batch_id=approval.batch_id,
        frozen_staffer_uuid=frozen_staffer,
        schema_version=approval.request_schema_version,
        product_contract_version=approval.product_contract_version,
    )
    if request is None:
        return await operations_module.finish_operation(
            session_maker,
            operation_id=operation.id,
            owner=owner,
            status=VOUCHER_PRODUCTION_OPERATION_REFUSED,
            outcome_code=RUNTIME_IDENTITY_UNUSABLE,
            reason_codes=list(reasons),
        )

    numbers = BatchApproval(
        expected_recipient_count=approval.batch_recipient_count if approval.stage == STAGE_FREEZE else None,
        approved_exposure_minor=approval.batch_exposure_minor if approval.stage == STAGE_FREEZE else None,
    )
    # The phrase §42's verification expects. Derived from the rebuilt plan's own
    # digest inside the runner, so there is nothing for an operator to copy and
    # nothing for a browser to supply: what authorises the stage is the stored
    # digest still describing the live world.
    common: dict[str, Any] = {
        "request": request,
        "apply": True,
        "supplied_digest": approval.plan_digest,
        "supplied_issued_at": approval.plan_issued_at,
        # Derived from the digest this operator's approval stored, not retyped
        # and not rebuilt. If the live world drifted, the plan the runner builds
        # a moment from now hashes differently, the digest comparison fails, and
        # the phrase fails with it — so this adds no path that the digest check
        # does not already govern. See `phrase_for_digest`.
        "supplied_phrase": phrase_for_digest(approval.stage, approval.plan_digest),
    }

    carrier = transports or Transports()
    # Did control actually reach a stage function? It decides how a failure around
    # the stage is recorded, and the two answers are not interchangeable: before
    # the call, nothing can have left this process; after it, the per-item ledger
    # may hold a committed claim whose request is in flight.
    entered = False
    try:
        async with carrier.reader() as reader:
            async with session_maker() as session:
                common["reader"] = reader
                common["order_reader"] = reader
                entered = True
                if approval.stage == STAGE_FREEZE:
                    report = await runner_module.run_freeze(session, session_maker, approval=numbers, **common)
                elif approval.stage == STAGE_DELIVER:
                    async with carrier.sender() as sender:
                        report = await runner_module.run_deliver(
                            session, session_maker, sender=sender, honour_stop=True, **common
                        )
                elif approval.stage == STAGE_CREATE:
                    async with carrier.mutator() as mutator:
                        report = await runner_module.run_create(
                            session, session_maker, mutator=mutator, honour_stop=True, **common
                        )
                elif approval.stage == STAGE_PAY:
                    async with carrier.mutator() as mutator:
                        report = await runner_module.run_pay(
                            session, session_maker, mutator=mutator, honour_stop=True, **common
                        )
                else:
                    async with carrier.mutator() as mutator:
                        report = await runner_module.run_refund(
                            session,
                            session_maker,
                            mutator=mutator,
                            slot=int(approval.slot or 0),
                            **common,
                        )
    except (EasyWeekConfigError, EasyWeekError, SQLAlchemyError, OSError):
        # Something failed AROUND the stage rather than inside one slot's recorded
        # outcome — the per-slot paths each write their own result and never raise
        # out of here.
        #
        # Which terminal state that is depends entirely on whether a stage
        # function was reached. Before it, nothing opened a socket, so the honest
        # answer is a refusal. After it, a claim may be committed with its request
        # in flight, and the only safe answer is `interrupted`: terminal, never
        # retried, and the state the UI turns into "reconcile this". A freeze is
        # the exception on the other side — it is local, reaches nobody, and its
        # own batch row is the record of whether it committed.
        interrupted = entered and approval.stage != STAGE_FREEZE
        return await operations_module.finish_operation(
            session_maker,
            operation_id=operation.id,
            owner=owner,
            status=(VOUCHER_PRODUCTION_OPERATION_INTERRUPTED if interrupted else VOUCHER_PRODUCTION_OPERATION_REFUSED),
            outcome_code=EXECUTION_INTERRUPTED if interrupted else API_UNAVAILABLE,
            reason_codes=[EXECUTION_INTERRUPTED if interrupted else API_UNAVAILABLE],
        )

    safe = report.as_safe_dict()
    created_batch = (safe.get("batch") or {}).get("batch_id")
    return await operations_module.finish_operation(
        session_maker,
        operation_id=operation.id,
        owner=owner,
        status=operations_module.status_for_outcome(report.outcome),
        outcome_code=report.outcome,
        reason_codes=list(report.reasons),
        result=safe,
        # A freeze is the one stage whose batch is born during it, so the link is
        # written here rather than at approval time.
        batch_id=int(created_batch) if isinstance(created_batch, int) else None,
    )


__all__ = [
    "SENDER_CODE",
    "UI_STAGES",
    "CompositionView",
    "ReconcileOutcome",
    "RecipientLine",
    "StageOffer",
    "Transports",
    "confirm_stage",
    "execute_operation",
    "inspect_composition",
    "offer_stage",
    "recipient_lines",
    "reconcile_batch",
    "request_stop",
]
