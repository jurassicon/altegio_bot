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

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import VoucherDeliveryClient
from altegio_bot.campaigns.easyweek_voucher_production import composition as composition_module
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
    COMPOSITION_DUPLICATE_CUSTOMER,
    COMPOSITION_READ_TIMEOUT,
    DATABASE_UNAVAILABLE,
    EXECUTION_INTERRUPTED,
    EXECUTOR_UNAVAILABLE,
    ISSUER_SUPPLIED_BY_CLIENT,
    KARLSRUHE_COMPANY_ID,
    NEW_CLIENT_CAMPAIGN_CODE,
    OPERATION_UNKNOWN,
    PLAN_EXPIRED,
    PRODUCTION_DISABLED,
    PRODUCTION_STAGES,
    RECIPIENT_NOT_EXCLUDABLE,
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
    NEW_MAILING_SCHEMA_VERSION,
    new_mailing_contract,
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
    schema_version: str = NEW_MAILING_SCHEMA_VERSION,
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


# How long the composition READ may take, and why those numbers.
#
# The read path is sequential and per recipient, because it shares one
# ``AsyncSession``: parallelising it would mean using that session from several
# tasks at once, which is exactly what it must not do. So the budget has to scale
# with the audience rather than being one flat number.
#
# What one recipient costs, counted from the code rather than guessed:
#   * manual — ``prove_customer`` is two reads by construction, a full workspace
#     listing walk (every page it claims to have) plus a direct GET of the UUID;
#   * manual under the zero-booking policy — one more read, the booking history,
#     paginated at ``CUSTOMER_BOOKINGS_PER_PAGE``;
#   * earned — the source customer, the source booking, and then the same two
#     reads of ``prove_customer``, so four.
# Four to five provider reads per recipient is therefore the realistic ceiling,
# and each of those reads is itself bounded by the client: a 15 s read timeout,
# at most 3 attempts, backoff capped at 8 s and ``Retry-After`` capped at 10 s.
#
# Measured against that: thirty-four manual recipients, sixty-eight successful
# reads at 350 ms each, answered in 24.73 s. The per-recipient allowance below is
# four times that observed cost, so a slow-but-healthy provider is not cut off,
# and the fixed part covers the once-per-check reads the plan does anyway —
# product baseline, Meta template, workspace, locations, accounts, staffers.
#
# The ceiling is what keeps this bounded rather than merely large. An operator
# gets an answer, or a named timeout, and never an open-ended wait.
COMPOSITION_FIXED_BUDGET_SECONDS: Final = 20
COMPOSITION_PER_RECIPIENT_BUDGET_SECONDS: Final = 3
COMPOSITION_READ_BUDGET_CEILING_SECONDS: Final = 180
# What the page adds on top of the server's bound: the request and the response on
# the wire. Part of the policy rather than a number in the page script, so there is
# one place to read and no pair of constants that can drift apart.
COMPOSITION_TRANSPORT_MARGIN_SECONDS: Final = 15


def composition_read_budget_seconds(active_recipients: int) -> int:
    """The bound this phase gives one composition read, for *active_recipients*.

    Scaled to the audience the read is actually about, so a one-person preview does
    not inherit a three-minute allowance and a large one is not cut off. Bounded by
    the ceiling, so the read is always finite.
    """
    counted = max(0, int(active_recipients))
    budget = COMPOSITION_FIXED_BUDGET_SECONDS + COMPOSITION_PER_RECIPIENT_BUDGET_SECONDS * counted
    return min(budget, COMPOSITION_READ_BUDGET_CEILING_SECONDS)


def composition_browser_wait_seconds() -> int:
    """The longest a page may wait for a composition read, whatever the audience is.

    The CEILING plus the transport margin, deliberately not the budget of any
    particular composition. That is the fix for a race the per-composition number
    could not survive: a page opened at sixteen recipients learned a 68-second
    budget, somebody added eighteen more in the preview editor, the server's bound
    for the real audience became 122 seconds, and the page still gave up at 83 —
    then gave up at 83 again on every retry, because the only way it ever learned a
    new budget was from a final answer it never received.

    This number depends on nothing the audience can change, so it cannot go stale.
    The read itself stays bounded by :func:`composition_read_budget_seconds` for the
    composition in front of it, which is what keeps a small preview answering fast;
    the page simply promises to outlast any bound that function can return.
    """
    return COMPOSITION_READ_BUDGET_CEILING_SECONDS + COMPOSITION_TRANSPORT_MARGIN_SECONDS


# The three states one row of a checked composition can be in. Strings rather than
# booleans: "not proven" and "not checked" are different answers and a pair of
# booleans invites reading one as the other.
LINE_PROVEN: Final = "proven"
LINE_REFUSED: Final = "refused"
LINE_UNCHECKED: Final = "unchecked"


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
    # Which of the three this row is. ``PROVEN`` means this member's own live proof
    # succeeded; ``REFUSED`` means it did not and the reasons below say why;
    # ``UNCHECKED`` means the composition stopped before this row was proven at all.
    # The three are deliberately not two: a row nobody checked must never be shown
    # with the colour of one that passed.
    state: str = LINE_PROVEN
    # This ROW's own reason codes, as the member's proof reported them. A reason
    # that belongs to the batch rather than to one person is reported separately on
    # the view and never attached here, because attributing a shared blocker to
    # whichever row happened to be first is how an operator removes the wrong
    # person.
    reasons: tuple[str, ...] = ()
    # The name came from the local preview row, not from an identity proof. True on
    # an early refusal, where there is no proven customer to name — the preview
    # value is good enough to FIND the row on screen and is not evidence of
    # anything.
    name_from_preview: bool = False

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
            "state": self.state,
            "reasons": list(self.reasons),
            "name_from_preview": self.name_from_preview,
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
    # The FACE VALUE of one voucher, and what issuing one costs. Separate fields
    # because a screen showing a single "amount" for the free certificate would
    # either ask for 330 EUR to be approved or claim the gift is worth nothing.
    unit_price_minor: int
    issue_price_minor: int = 0
    total_issue_price_minor: int = 0
    # The till this contract settles through, by name, for recognition only.
    payment_account_label: str = ""
    lines: tuple[RecipientLine, ...] = ()
    # Whether this answer describes the audience AT ALL. ``False`` when the fence is
    # shut or EasyWeek or the database could not be reached: there is no composition
    # in that answer, only the absence of one. The distinction exists because the
    # alternative — reporting zero — turns a lost connection into a mailing that
    # looks like it has nobody in it, and an operator cannot tell the two apart.
    known: bool = True
    # How many ACTIVE preview rows there are, independently of how many could be
    # proven. An operator reading a refusal needs to know whether the answer is
    # "thirty-four" or "none".
    observed_active: int = 0
    # Reasons that belong to the BATCH rather than to any one row: a duplicate that
    # cannot be pinned to specific rows, an entitlement already taken in another
    # batch, a preview already consumed, a run that is not a usable preview. Kept
    # apart from the row reasons so nothing attributes a shared blocker to whoever
    # happens to be listed first.
    blockers: tuple[str, ...] = ()
    # WHICH audience this is, as the freeze would sign it. Reported so the page can
    # tell "the list I am showing" from "the list a plan just re-proved" (review F1):
    # one member swapped for another leaves the count and the money identical, so a
    # screen comparing only numbers would present a stale list as the confirmed one.
    # It is a digest over slots and preview rows — no name, number or secret.
    composition_digest: str | None = None
    # The bound this read was actually given, in seconds. Reported as a fact about
    # what happened — it belongs in a diagnostic and in a ticket — and deliberately
    # NOT what the page builds its own deadline from: a page that learned its
    # deadline from answers has no deadline for the answer that never arrives.
    read_budget_seconds: int = 0

    @property
    def proven_line_count(self) -> int:
        return sum(line.state == LINE_PROVEN for line in self.lines)

    @property
    def refused_line_count(self) -> int:
        return sum(line.state == LINE_REFUSED for line in self.lines)

    @property
    def unchecked_line_count(self) -> int:
        return sum(line.state == LINE_UNCHECKED for line in self.lines)

    def as_ui_dict(self) -> dict[str, Any]:
        return {
            "composition_proven": self.proven,
            # Said separately from `composition_proven`, which answers "may this be
            # frozen"; this one answers "do we know what the audience is at all".
            "composition_known": self.known,
            "reasons": list(self.reasons),
            "blockers": list(self.blockers),
            "campaign_period": self.campaign_period,
            "recipient_count": self.recipient_count,
            "observed_active_count": self.observed_active,
            "proven_count": self.proven_line_count,
            "refused_count": self.refused_line_count,
            "unchecked_count": self.unchecked_line_count,
            # The NOMINAL for what is SHOWN. Never an approved amount: the freeze
            # takes the operator's own statement and re-proves the whole audience.
            "total_exposure_minor": self.total_exposure_minor,
            "unit_price_minor": self.unit_price_minor,
            # And what issuing it costs, which is zero for the gift certificate.
            "issue_price_minor": self.issue_price_minor,
            "total_issue_price_minor": self.total_issue_price_minor,
            "free_issue": self.issue_price_minor == 0,
            "payment_account_label": self.payment_account_label,
            "composition_digest": self.composition_digest,
            "read_budget_seconds": self.read_budget_seconds,
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
            unit_price_minor=new_mailing_contract().face_value_minor,
            issue_price_minor=new_mailing_contract().issue_price_minor,
            payment_account_label=new_mailing_contract().payment_account_label,
            # Not an audience of nobody: an answer that contains no audience.
            known=False,
            # No read was attempted, so the smallest budget this policy has is the
            # honest number to report rather than one derived from an audience
            # nobody looked at.
            read_budget_seconds=composition_read_budget_seconds(0),
            blockers=(PRODUCTION_DISABLED,),
        )
    # The bound, from the audience this read is actually about. Counted first and
    # cheaply, from the database, so the budget describes the work rather than a
    # guess about it — and so the answer can carry the number the page should wait.
    budget = composition_read_budget_seconds(await active_recipient_count(session_maker, preview_run_id))
    carrier = transports or Transports()
    try:
        async with carrier.reader() as reader:
            async with session_maker() as session:
                async with asyncio.timeout(budget):
                    composition = await prove_production_composition(
                        session,
                        preview_run_id=preview_run_id,
                        client_reader=reader,
                        now=utcnow(),
                        approval=None,
                        schema_version=new_mailing_contract().request_schema_version,
                    )
    except TimeoutError:
        # The read did not finish inside its own bound. A refusal with no audience
        # in it, like the other three below: the composition is UNKNOWN, not empty,
        # and the operator is told to look again rather than shown a mailing with
        # nobody in it. Nothing was written and nothing was sent — this is a read.
        return CompositionView(
            proven=False,
            reasons=(COMPOSITION_READ_TIMEOUT,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=new_mailing_contract().face_value_minor,
            issue_price_minor=new_mailing_contract().issue_price_minor,
            payment_account_label=new_mailing_contract().payment_account_label,
            known=False,
            blockers=(COMPOSITION_READ_TIMEOUT,),
            read_budget_seconds=budget,
        )
    except EasyWeekConfigError:
        return CompositionView(
            proven=False,
            reasons=(RUNTIME_IDENTITY_UNUSABLE,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=new_mailing_contract().face_value_minor,
            issue_price_minor=new_mailing_contract().issue_price_minor,
            payment_account_label=new_mailing_contract().payment_account_label,
            # Not an audience of nobody: an answer that contains no audience.
            known=False,
            read_budget_seconds=budget,
            blockers=(RUNTIME_IDENTITY_UNUSABLE,),
        )
    except EasyWeekError:
        return CompositionView(
            proven=False,
            reasons=(API_UNAVAILABLE,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=new_mailing_contract().face_value_minor,
            issue_price_minor=new_mailing_contract().issue_price_minor,
            payment_account_label=new_mailing_contract().payment_account_label,
            # Not an audience of nobody: an answer that contains no audience.
            known=False,
            read_budget_seconds=budget,
            blockers=(API_UNAVAILABLE,),
        )
    except SQLAlchemyError:
        return CompositionView(
            proven=False,
            reasons=(DATABASE_UNAVAILABLE,),
            campaign_period=None,
            recipient_count=0,
            total_exposure_minor=0,
            unit_price_minor=new_mailing_contract().face_value_minor,
            issue_price_minor=new_mailing_contract().issue_price_minor,
            payment_account_label=new_mailing_contract().payment_account_label,
            # Not an audience of nobody: an answer that contains no audience.
            known=False,
            read_budget_seconds=budget,
            blockers=(DATABASE_UNAVAILABLE,),
        )

    reasons = tuple(reason for reason in composition.reasons if reason not in _APPROVAL_NUMBER_REASONS)
    lines = await _composition_lines(session_maker, preview_run_id=preview_run_id, composition=composition)
    # What the batch could not establish, as opposed to what one row could not. A
    # reason is a ROW's only if that row's own proof reported it; everything left
    # over belongs to the batch and is shown as such.
    attributed = {reason for line in lines for reason in line.reasons}
    blockers = tuple(reason for reason in reasons if reason not in attributed)
    return CompositionView(
        # Proven for the purpose of this screen: a real audience the operator may
        # now put numbers to. The freeze still proves everything again, including
        # those numbers.
        proven=not reasons and bool(composition.members),
        reasons=reasons,
        campaign_period=composition.period_label,
        recipient_count=composition.recipient_count,
        total_exposure_minor=composition.total_exposure_minor,
        unit_price_minor=new_mailing_contract().face_value_minor,
        issue_price_minor=new_mailing_contract().issue_price_minor,
        total_issue_price_minor=composition.total_issue_price_minor,
        payment_account_label=new_mailing_contract().payment_account_label,
        # The same digest a freeze plan carries in its signed snapshot, so the two
        # are comparable at all. Only for a proven audience: there is no identity to
        # report for a composition that could not be established.
        composition_digest=composition.composition_digest() if composition.proven else None,
        lines=lines,
        known=True,
        observed_active=composition.observed_active or composition.recipient_count,
        blockers=blockers,
        read_budget_seconds=budget,
    )


async def active_recipient_count(
    session_maker: async_sessionmaker[AsyncSession],
    preview_run_id: int,
) -> int:
    """How many rows this read will have to prove. One cheap count, no provider.

    A failure to count answers zero, which gives the read its smallest budget
    rather than its largest: a database this read cannot reach is about to refuse
    anyway, and the refusal should not be preceded by a three-minute wait.
    """
    from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient

    try:
        async with session_maker() as session:
            found = await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == preview_run_id)
                .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                .where(CampaignRecipient.status == composition_module.ACTIVE_RECIPIENT_STATUS)
            )
    except SQLAlchemyError:
        return 0
    return int(found or 0)


async def _composition_lines(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    preview_run_id: int,
    composition: composition_module.ProductionComposition,
) -> tuple[RecipientLine, ...]:
    """Every active row of this preview, with what the live proof made of it.

    The whole audience, including the rows that refused: a check that stops on one
    person must still show the other thirty-three, or the operator is told "the list
    is wrong" and given nothing to act on.

    Two shapes, because the composition stops at two different depths. When members
    were built, each one carries its own proof and its own reasons. When the
    composition refused BEFORE the live reads — an unusable run, a mixed basis, an
    approval mismatch — there are no members at all, and the rows are listed from the
    local preview as UNCHECKED so the screen can still say how many there are.

    A row whose proof refused early has no proven customer to name, and the operator
    still has to be able to FIND it. The name the operator curated is in the preview
    row, so it is read from there — once, for the whole list, for display only. It
    never substitutes for an identity proof: ``name_from_preview`` says which source
    was used, the row stays refused, and no proof, binding, eligibility or digest is
    touched by it.
    """
    if not composition.members:
        return await _unchecked_lines(session_maker, preview_run_id=preview_run_id)
    duplicates = _duplicate_slots(composition)
    # The fallback names, in one query, for the rows that will need them. Scoped to
    # THIS preview and this provider, so a row id that belongs to another run cannot
    # lend its name to a row here.
    unnamed = [
        member.campaign_recipient_id
        for member in composition.members
        if not (member.proof.client_display_name or "").strip()
    ]
    preview_names = await _preview_display_names(
        session_maker, preview_run_id=preview_run_id, campaign_recipient_ids=unnamed
    )
    lines: list[RecipientLine] = []
    for member in composition.members:
        reasons = [composition_module.translate_reason(reason) for reason in member.proof.reasons]
        if member.slot in duplicates:
            # The one batch-level blocker that CAN be pinned to rows reliably: the
            # duplicate is these two members and nobody else, which is what the
            # operator has to see to decide which of them to exclude. Only the slots
            # of this preview are named — no customer UUID, and nothing about any
            # other campaign's people.
            reasons.append(COMPOSITION_DUPLICATE_CUSTOMER)
        proven_name = (member.proof.client_display_name or "").strip()
        fallback = preview_names.get(member.campaign_recipient_id, "") if not proven_name else ""
        lines.append(
            RecipientLine(
                slot=member.slot,
                campaign_recipient_id=member.campaign_recipient_id,
                display_name=proven_name
                or fallback
                # Only when there is genuinely no usable name anywhere. A blank cell
                # would be indistinguishable from a bug.
                or f"без имени (строка preview {member.campaign_recipient_id})",
                preview_run_id=preview_run_id,
                recipient_basis=member.recipient_basis,
                manual_policy=member.manual_policy,
                state=LINE_PROVEN if member.proof.proven and not reasons else LINE_REFUSED,
                reasons=tuple(dict.fromkeys(reasons)),
                # Honest about the source: true only when the shown name really did
                # come from the preview, and false for the placeholder, which came
                # from nowhere.
                name_from_preview=bool(fallback),
            )
        )
    return tuple(lines)


async def _preview_display_names(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    preview_run_id: int,
    campaign_recipient_ids: list[int],
) -> dict[int, str]:
    """``{recipient id: curated name}`` for these rows of THIS preview. Display only.

    One query for the whole list rather than one per row: the read path already
    spends a provider round trip per recipient, and this is a local lookup that has
    no business adding to it.

    Both the preview and the provider are in the WHERE clause. Without them a row id
    from another run would match and put somebody else's name on this screen, which
    is worse than the placeholder it would be replacing.
    """
    if not campaign_recipient_ids:
        return {}
    from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient

    try:
        async with session_maker() as session:
            rows = list(
                (
                    await session.execute(
                        select(CampaignRecipient.id, CampaignRecipient.display_name)
                        .where(CampaignRecipient.id.in_(campaign_recipient_ids))
                        .where(CampaignRecipient.campaign_run_id == preview_run_id)
                        .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                    )
                ).all()
            )
    except SQLAlchemyError:
        # A name is a convenience. Failing to read one must not turn a composition
        # that WAS read into an error, so the rows fall back to the placeholder.
        return {}
    return {int(row[0]): (row[1] or "").strip() for row in rows if (row[1] or "").strip()}


def _duplicate_slots(composition: composition_module.ProductionComposition) -> frozenset[int]:
    """Slots whose proven customer appears more than once in this composition.

    Two preview rows, one human. The unique index catches it at freeze; naming the
    rows here is what lets an operator fix it rather than read a constraint. Members
    with no proven customer are skipped: an absent UUID is not a duplicate of
    another absent UUID, and grouping them would accuse rows of a problem they do
    not have.
    """
    seen: dict[str, list[int]] = {}
    for member in composition.members:
        customer = (member.easyweek_customer_uuid or "").strip()
        if customer:
            seen.setdefault(customer, []).append(member.slot)
    return frozenset(slot for slots in seen.values() if len(slots) > 1 for slot in slots)


async def _unchecked_lines(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    preview_run_id: int,
) -> tuple[RecipientLine, ...]:
    """The active preview rows, named from the preview, marked unchecked.

    Display only. The preview's own ``display_name`` is what the operator curated,
    which is enough to FIND a row on screen — and it is deliberately not allowed to
    stand in for an identity proof: these lines carry no proven state, and the freeze
    re-proves every one of them from scratch.
    """
    from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient

    try:
        async with session_maker() as session:
            rows = list(
                (
                    await session.execute(
                        select(
                            CampaignRecipient.id,
                            CampaignRecipient.display_name,
                            CampaignRecipient.recipient_basis,
                            CampaignRecipient.manual_policy,
                        )
                        .where(CampaignRecipient.campaign_run_id == preview_run_id)
                        .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                        .where(CampaignRecipient.status == composition_module.ACTIVE_RECIPIENT_STATUS)
                        .order_by(CampaignRecipient.id.asc())
                    )
                ).all()
            )
    except SQLAlchemyError:
        # The audience is simply unknown then, which the caller already reports.
        return ()
    lines: list[RecipientLine] = []
    for index, row in enumerate(rows, start=1):
        curated = (row[1] or "").strip()
        lines.append(
            RecipientLine(
                slot=index,
                campaign_recipient_id=int(row[0]),
                display_name=curated or f"без имени (строка preview {int(row[0])})",
                preview_run_id=preview_run_id,
                recipient_basis=str(row[2]) if row[2] else None,
                manual_policy=str(row[3]) if row[3] else None,
                state=LINE_UNCHECKED,
                reasons=(),
                # False for the placeholder: it came from nowhere, and claiming the
                # preview supplied it would be a claim about data nobody has.
                name_from_preview=bool(curated),
            )
        )
    return tuple(lines)


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
        # A frozen batch answers for itself; without one this is a NEW mailing,
        # which means the current product rather than the previous one.
        schema_version=frozen.schema_version if frozen is not None and frozen.exists else NEW_MAILING_SCHEMA_VERSION,
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
        unit_price_minor=production_contract(request.schema_version).face_value_minor,
        batch_recipient_count=batch_count,
        batch_exposure_minor=batch_exposure,
        issue_price_minor=production_contract(request.schema_version).issue_price_minor,
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


@dataclass(frozen=True)
class ExclusionOutcome:
    """What an exclusion did, or the named reason it did not happen.

    Three answers, not two. ``applied`` is a durable soft removal; ``refused`` is a
    decision this phase made about the request; and ``reason=None`` with
    ``applied=False`` never occurs, because an exclusion that neither happened nor
    was refused would be an unknown — and an unknown is reported by the caller
    failing, not by this returning a tidy value for it.
    """

    applied: bool
    reason: str | None = None
    campaign_recipient_id: int | None = None
    status: str | None = None
    excluded_reason: str | None = None
    # Already out before this request. The removal is idempotent, and an operator
    # pressing twice must read "already excluded" rather than an error.
    already_excluded: bool = False

    def as_ui_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "reason": self.reason,
            "campaign_recipient_id": self.campaign_recipient_id,
            "status": self.status,
            "excluded_reason": self.excluded_reason,
            "already_excluded": self.already_excluded,
        }


async def exclude_recipient(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    preview_run_id: int,
    campaign_recipient_id: int,
    principal: operations_module.OpsPrincipal,
) -> ExclusionOutcome:
    """Soft-remove ONE recipient from this preview. Reaches no provider at all.

    The business operation is the existing one — ``remove_recipient_from_preview``
    — reused rather than reimplemented, so this shares its editability and canary
    locks, its ``skipped``/``manual_removed`` audit trail, its preserved basis and
    typed bindings, its counter recount in the same transaction, and its
    idempotence. What this adds is the scope: it may only touch a Karlsruhe
    EasyWeek preview of this campaign, and it states that expectation to be
    re-verified under the run's row lock.

    Deliberately NOT routed through the older campaigns endpoint. That one has its
    own protection contour, and promoting it to a privileged path from here would
    mean inheriting an authorisation story nobody reviewed for this use.

    Nothing about a voucher is touched: no Client, no EasyWeek customer, no
    booking, no order, no ledger row and no entitlement. The person stays in the
    database; only this preview's composition changes.
    """
    from altegio_bot.campaigns.runner import remove_recipient_from_preview
    from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient

    # Read beforehand ONLY to word the answer: "excluded" and "already excluded" are
    # the same durable outcome, and an operator who pressed twice should read the
    # second rather than a success that looks like a second removal. Deliberately not
    # a guard — the removal itself is idempotent under the run's row lock, so a race
    # here can mislabel a sentence and can never change what happened.
    async with session_maker() as session:
        before = await session.get(CampaignRecipient, campaign_recipient_id)
        was_excluded = before is not None and before.excluded_reason == "manual_removed"

    try:
        recipient = await remove_recipient_from_preview(
            preview_run_id,
            campaign_recipient_id,
            expect_provider=PROVIDER_EASYWEEK,
            expect_company_id=KARLSRUHE_COMPANY_ID,
            expect_campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
        )
    except ValueError:
        # The operation's own refusal: not editable, frozen, already automatically
        # excluded, another run's row, another branch. Reported as one reason code
        # rather than as its message, because the message is Russian prose written
        # for another screen and this answer goes through a JSON contract.
        await operations_module.record_audit(
            session_maker,
            principal=principal,
            action="exclude_recipient",
            outcome=RECIPIENT_NOT_EXCLUDABLE,
            campaign_run_id=preview_run_id,
            detail={"campaign_recipient_id": campaign_recipient_id},
        )
        return ExclusionOutcome(
            applied=False,
            reason=RECIPIENT_NOT_EXCLUDABLE,
            campaign_recipient_id=campaign_recipient_id,
        )

    await operations_module.record_audit(
        session_maker,
        principal=principal,
        action="exclude_recipient",
        outcome="excluded",
        campaign_run_id=preview_run_id,
        detail={
            "campaign_recipient_id": campaign_recipient_id,
            "status": recipient.status,
            "excluded_reason": recipient.excluded_reason,
        },
    )
    return ExclusionOutcome(
        applied=True,
        campaign_recipient_id=campaign_recipient_id,
        status=recipient.status,
        excluded_reason=recipient.excluded_reason,
        already_excluded=was_excluded,
    )


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
        # A frozen batch answers for itself; without one this is a NEW mailing,
        # which means the current product rather than the previous one.
        schema_version=frozen.schema_version if frozen is not None and frozen.exists else NEW_MAILING_SCHEMA_VERSION,
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
