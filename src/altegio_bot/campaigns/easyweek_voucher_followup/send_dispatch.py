"""From a browser click to a durable run, and from a run to one POST (§46.8).

One module, used by exactly two callers, which is what keeps the authorisation
honest:

* the Ops API, which OFFERS a plan, CONFIRMS it and STOPS it. It never executes.
* the dedicated executor, which EXECUTES a confirmed run. It never authorises.

No HTTP handler here sends anything. A request that confirmed a run returns as
soon as that run is committed; the work happens in the executor, which is why
closing the tab, logging out or losing the response changes nothing about it.

The fence
---------
:func:`send_allowed` reads the follow-up's OWN setting. The primary mailing's
permission does not open it and is not consulted: §46.8 requires a separate
switch, so a deployment with the §42 fence wide open still refuses every offer,
every confirmation and every claim here.

Nothing is handed to anything that retries
------------------------------------------
The one request goes through :class:`...easyweek_voucher_delivery.delivery.
VoucherDeliveryClient`, which makes exactly one POST, follows no redirect and
has no backoff, no attempt count and no requeue. The old follow-up worker, the
generic Outbox, the campaign worker and the generic EasyWeek campaign send are
not reached from here and gain no authority over any of these rows.

The order of a send, and the one line that matters
--------------------------------------------------
For each planned row:

1. re-prove eligibility, locally and then externally, holding nothing;
2. CLAIM — one committed transaction that spends the attempt before anything
   leaves the process. This is the boundary;
3. one POST;
4. record the answer.

A failure between 2 and 4 leaves a ``claimed`` row, which the sweep turns into
an honest ``unknown``. There is deliberately no transition back.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Final

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    VoucherDeliveryClient,
)
from altegio_bot.campaigns.easyweek_voucher_followup import preview as preview_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_eligibility as recheck_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as ids
from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as ledger
from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import (
    CURRENT_FAILURE_POLICY,
    FailurePolicy,
)
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    BATCH_TERMINAL_STOP,
    SENDER_UNPROVEN,
    TEMPLATE_UNPROVEN,
)
from altegio_bot.campaigns.easyweek_voucher_production.operations import OpsPrincipal
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError
from altegio_bot.models.models import (
    FOLLOWUP_RUN_COMPLETED,
    FOLLOWUP_SEND_ACCEPTED,
    FOLLOWUP_SEND_PLANNED,
    FOLLOWUP_SEND_REJECTED,
    FOLLOWUP_SEND_SKIPPED,
    FOLLOWUP_SEND_UNKNOWN,
    EasyWeekVoucherFollowupSend,
)
from altegio_bot.utils import utcnow

logger = logging.getLogger("easyweek_voucher_followup_send_dispatch")

# Who the audit records as having stopped a run the ORIGINAL mailing's terminal
# stop ended. Not an operator account: no operator pressed this run's own stop,
# and attributing it to one would be a claim about a person.
_SOURCE_STOP_PRINCIPAL: Final = "source_batch_terminal_stop"

__all__ = [
    "RunReport",
    "Transports",
    "confirm_send",
    "execute_run",
    "executor_available",
    "offer_send",
    "send_allowed",
]


def send_allowed() -> bool:
    """Is the follow-up's own send fence OPEN?

    Its own setting, read here and nowhere else. The §42 production fence is
    deliberately not consulted: §46.8 requires a separate switch, and an
    implementation that fell back to the primary mailing's permission would be
    the exact bypass the section forbids.
    """
    from altegio_bot.settings import settings

    return bool(getattr(settings, "easyweek_voucher_followup_send_enabled", False))


def executor_available() -> bool:
    """Does this deployment RUN the dedicated follow-up executor?

    A weak claim on purpose, and the UI says so: it means "this deployment runs
    one", never "it is alive this second". An executor that died shows up as a
    run sitting in ``queued`` on the page.

    It authorises nothing. With this true and the fence closed, every offer and
    every confirmation still refuses.
    """
    from altegio_bot.settings import settings

    return bool(getattr(settings, "easyweek_voucher_followup_executor_enabled", False))


class Transports:
    """How a run reaches EasyWeek and Meta. Replaceable, for tests only.

    The real implementation opens the read-only EasyWeek client the re-check
    needs and the one-POST Meta client, each for the length of one run. Tests
    pass fakes; nothing about the authorisation path changes with them, because
    none of it is in here.
    """

    @contextlib.asynccontextmanager
    async def reader(self) -> AsyncIterator[Any | None]:
        try:
            async with EasyWeekClient() as client:
                yield client
        except EasyWeekConfigError:
            # §46.5: a deployment that cannot read EasyWeek cannot prove
            # anybody's bookings, and that must become "unknown" rather than an
            # empty booking list. The re-check does exactly that with ``None``.
            yield None

    @contextlib.asynccontextmanager
    async def sender(self) -> AsyncIterator[Any]:
        async with VoucherDeliveryClient() as client:
            yield client

    def template_reader(self) -> Any | None:
        """The Meta template listing seam for the per-attempt proof.

        ``None`` means "read it the way the deployment does", which is what
        :func:`...preview.prove_meta_followup_template` then does from the
        settings. A test returns its own listing here, so the proof that runs
        before every attempt is the real one with a replaced transport rather
        than a check that was skipped.
        """
        return None


# ---------------------------------------------------------------------------
# The offer
# ---------------------------------------------------------------------------


async def offer_send(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    principal: OpsPrincipal,
    batch_id: int,
    preview_version: int | None = None,
    template_reader: Any | None = None,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime | None = None,
) -> ledger.PlanOffer:
    """Build one plan live and, if it is ready, store it as an approval.

    Reads only, apart from the approval row. It sends nothing, claims nothing,
    spends nothing and reaches no customer: a plan is reads and a row.

    Everything the confirmation will rest on is proven HERE and written by the
    server — the stored preview version, its composition digest, the exact
    message contract, the branch sender, the source stop generation and the
    count. A request may name a batch and a version and nothing else.
    """
    moment = now or utcnow()
    if not send_allowed():
        # The fence, before anything opens a socket. A closed fence means the
        # offer does nothing at all, not "nothing that writes".
        return ledger.PlanOffer(False, (ids.SEND_DISABLED,))
    if not executor_available():
        # Refused up front rather than parking a confirmation in a queue nobody
        # drains. A deliberate pre-check and not a guarantee.
        return ledger.PlanOffer(False, (ids.EXECUTOR_UNAVAILABLE,))

    try:
        async with session_maker() as session:
            gate = await preview_module.batch_gate(session, batch_id=int(batch_id))
            if not gate.allowed:
                return ledger.PlanOffer(False, (gate.reason or ids.PREVIEW_NOT_CURRENT,))
            stored = await preview_module.latest_preview(session, batch_id=int(batch_id), version=preview_version)
            if stored is None:
                return ledger.PlanOffer(False, (ids.PREVIEW_UNKNOWN,))
            local_template = await preview_module.local_template_row_proven(
                session, company_id=int(gate.company_id or 0)
            )
            rows = await ledger.included_preview_rows(session, preview_id=stored.preview_id)
            blocked = await ledger.unsendable_source_items(session, source_batch_id=int(batch_id))
    except SQLAlchemyError:
        return ledger.PlanOffer(False, (ids.DATABASE_UNAVAILABLE,))

    if not local_template:
        return ledger.PlanOffer(False, (TEMPLATE_UNPROVEN,))
    # The live Meta half, with no session open. A previous proof is not evidence:
    # an approved template can be paused between two page loads.
    template = await preview_module.prove_meta_followup_template(template_reader)
    if not template.proven:
        return ledger.PlanOffer(False, (TEMPLATE_UNPROVEN,))

    currency = preview_module.assess_snapshot(
        stored,
        gate=gate,
        template_proven_now=template.proven,
        policy=policy,
        now=moment,
    )
    if not currency.current:
        # An expired snapshot, a contract that moved, a stop since, a sender
        # that is gone. §46.8 requires a fresh preview, not a stale approval.
        return ledger.PlanOffer(False, (ids.PREVIEW_NOT_CURRENT, *currency.drift))
    if not gate.sender_digest:
        return ledger.PlanOffer(False, (SENDER_UNPROVEN,))

    # The count has to be the number of messages the NEXT click can produce, so
    # the local conditions are re-proven here too: a row whose consent, identity,
    # group, interval or entitlement moved since the preview is left out of the
    # offer rather than confirmed and then silently dropped. The confirmation
    # re-checks the same conditions again, and the claim's locked boundary is
    # still the guarantee.
    try:
        refused_now = await _confirmation_recheck(
            session_maker,
            stored=_offer_contract_probe(stored, gate, policy),
            policy=policy,
            now=moment,
        )
    except SQLAlchemyError:
        return ledger.PlanOffer(False, (ids.DATABASE_UNAVAILABLE,))
    # The OFFER is this subset and nothing wider: the rows that could still be
    # sent to when the plan was built. Its identities, its digest and its count
    # are all derived from this one list, so the confirmation can only narrow it
    # — a recipient who regains eligibility afterwards waits for the next offer
    # rather than joining this run.
    offered = [
        (int(item.source_item_id), int(item.source_slot), str(item.source_entitlement_digest))
        for item in rows
        if int(item.source_item_id) not in blocked and int(item.source_item_id) not in refused_now
    ]

    approval = await ledger.store_approval(
        session_maker,
        principal=principal,
        stored_preview=stored,
        gate=gate,
        offered=offered,
        now=moment,
    )
    logger.info(
        "followup_send: offered batch=%s preview_version=%s count=%s approval=%s",
        int(batch_id),
        stored.version,
        len(offered),
        approval.id,
    )
    return ledger.PlanOffer(
        True,
        (),
        approval=approval,
        rows=tuple((slot, digest) for _item_id, slot, digest in offered),
    )


# ---------------------------------------------------------------------------
# The confirmation
# ---------------------------------------------------------------------------


async def confirm_send(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
    presented_signature: str,
    principal: OpsPrincipal,
    confirmed_count: int | None,
    template_reader: Any | None = None,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime | None = None,
) -> ledger.ConfirmOutcome:
    """Spend one approval and queue one durable run.

    Returns as soon as the run is COMMITTED. Nothing has reached Meta yet and
    nothing in this process will: the executor picks it up.

    The fence is re-checked here, not only at the offer: an operator may have
    closed it between reading the plan and pressing the button, and a stored
    approval is not a standing permission.
    """
    moment = now or utcnow()
    if not send_allowed():
        return ledger.ConfirmOutcome(None, (ids.SEND_DISABLED,))
    if not executor_available():
        return ledger.ConfirmOutcome(None, (ids.EXECUTOR_UNAVAILABLE,))

    stored = await ledger.load_approval(session_maker, approval_id=int(approval_id))
    if stored is None:
        return ledger.ConfirmOutcome(None, (ids.APPROVAL_UNKNOWN,))
    # Whether the contract the approval pinned is still the one this deployment
    # would compose. Computed outside the transaction, because the Meta read is
    # external and §46.5 forbids holding anything across it.
    contract_matches = stored.contract().matches_today(policy=policy)
    if contract_matches:
        contract_matches = stored.contract_version == ids.SEND_CONTRACT_VERSION
    if contract_matches:
        # The live Meta half again, not the offer's answer: an approved template
        # can be paused between reading a plan and confirming it, and a previous
        # proof is not evidence.
        template = await preview_module.prove_meta_followup_template(template_reader)
        contract_matches = bool(template.proven)
    if contract_matches:
        try:
            async with session_maker() as session:
                gate = await preview_module.batch_gate(session, batch_id=stored.source_batch_id)
                local_template = await preview_module.local_template_row_proven(session, company_id=stored.company_id)
        except SQLAlchemyError:
            return ledger.ConfirmOutcome(None, (ids.DATABASE_UNAVAILABLE,))
        if gate.reason == BATCH_TERMINAL_STOP:
            # Said by its own name rather than folded into "something moved":
            # §46.5 makes the original mailing's terminal stop a refusal an
            # operator has to be able to read, and it is not resumable.
            return ledger.ConfirmOutcome(None, (ids.SOURCE_TERMINAL_STOP,))
        contract_matches = (
            gate.allowed
            and gate.sender_digest == stored.sender_digest
            and gate.frozen_digest == stored.source_frozen_digest
            and local_template
        )

    # §46.5's re-check AT CONFIRMATION, row by row, local only. The preview is
    # evidence of what was true at its check time; consent, a reply, an identity,
    # a group, an interval or an entitlement can all have moved since, and such a
    # row is excluded here with its reason rather than discovered by the
    # executor. No network call, so no lock is held across one — and this is a
    # pre-read, never the guarantee: that is the claim's locked boundary.
    row_refusals: dict[int, str] = {}
    if contract_matches:
        try:
            row_refusals = await _confirmation_recheck(
                session_maker,
                stored=stored,
                policy=policy,
                now=moment,
            )
        except SQLAlchemyError:
            return ledger.ConfirmOutcome(None, (ids.DATABASE_UNAVAILABLE,))

    try:
        outcome = await ledger.confirm_approval(
            session_maker,
            approval_id=int(approval_id),
            presented_signature=presented_signature,
            principal=principal,
            confirmed_count=confirmed_count,
            live_contract_matches=contract_matches,
            row_refusals=row_refusals,
            now=now,
        )
    except SQLAlchemyError:
        # COMMIT may have succeeded before its acknowledgement was lost. Read
        # back this exact authenticated approval; never repeat its mutation.
        try:
            return await ledger.reconcile_confirmation(
                session_maker,
                approval_id=int(approval_id),
                presented_signature=presented_signature,
                principal=principal,
                confirmed_count=confirmed_count,
            )
        except SQLAlchemyError:
            return ledger.ConfirmOutcome(None, (ids.DATABASE_UNAVAILABLE,), uncertain=True)
    if outcome.created and outcome.run is not None:
        logger.info(
            "followup_send: confirmed run=%s planned=%s skipped=%s",
            outcome.run.id,
            outcome.planned,
            outcome.skipped,
        )
    return outcome


def _offer_contract_probe(
    stored: preview_module.StoredPreview,
    gate: preview_module.BatchGate,
    policy: FailurePolicy,
) -> ledger.StoredApproval:
    """The not-yet-stored approval, so the offer can re-check with it.

    The row-by-row re-check is written against an approval, because that is what
    the confirmation and the executor have. At plan time the approval does not
    exist yet, so this is the same contract assembled from the stored preview and
    the live gate — and assembling it in one place is what keeps the number the
    operator reads and the conditions the executor re-proves from drifting
    apart. It is never written to the database.
    """
    return ledger.StoredApproval(
        id=0,
        action="followup_send",
        preview_id=int(stored.preview_id),
        preview_version=int(stored.version),
        source_batch_id=int(stored.batch_id),
        campaign_run_id=int(gate.campaign_run_id or 0),
        company_id=int(gate.company_id or 0),
        composition_digest=str(stored.composition_digest),
        selected_groups=tuple(stored.selected_groups),
        min_interval_seconds=int(stored.min_interval_seconds),
        approved_recipient_count=0,
        recipient_digest="",
        # The probe is never stored and never confirmed; it exists only to carry
        # the contract into the row-by-row re-check, so it pins nobody.
        offered_source_item_ids=None,
        source_frozen_digest=str(gate.frozen_digest),
        source_request_schema_version=str(gate.schema_version),
        source_stop_generation=int(gate.stop_generation),
        template_code=str(stored.template_code),
        meta_template_name=str(stored.meta_template_name),
        template_language=str(stored.template_language),
        template_category=str(stored.template_category),
        template_parameter_order=tuple(stored.template_parameter_order),
        template_body_digest=str(stored.template_body_digest),
        contract_version=ids.SEND_CONTRACT_VERSION,
        failure_policy_version=str(stored.failure_policy_version or policy.version),
        sender_digest=str(gate.sender_digest or ""),
        principal="",
        session_fingerprint="",
        nonce="",
        plan_signature="",
        issued_at=utcnow(),
        expires_at=utcnow(),
        status="pending",
    )


async def _confirmation_recheck(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    stored: ledger.StoredApproval,
    policy: FailurePolicy,
    now: datetime,
) -> dict[int, str]:
    """``{source_item_id: reason}`` for rows that lost eligibility since the preview.

    Local reads only — the identity, the group, the sender geography, the
    interval, the failure policy, the entitlement, the consent aggregate
    including unresolved inbound, the stored-event backlog and the replies. The
    external proofs (the original artifact and the live booking history) are
    deliberately NOT repeated here: they are the executor's step, taken with no
    transaction open, and running them inside a confirmation would hold a
    request while an operator waits.

    One REPEATABLE READ snapshot, so the several reads per row describe one
    instant rather than a moving world.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup.send_eligibility import local_recheck

    refusals: dict[int, str] = {}
    contract = stored.contract()
    configured_key_id = preview_module.configured_binding_key_id()
    async with session_maker() as session:
        await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        gate = await preview_module.batch_gate(session, batch_id=stored.source_batch_id)
        if not gate.allowed:
            # The caller has already turned this into its own refusal; there is
            # nothing per-row to say about a batch that is out of scope.
            return refusals
        for item in await ledger.included_preview_rows(session, preview_id=stored.preview_id):
            verdict = await local_recheck(
                session,
                gate=gate,
                source_item_id=int(item.source_item_id),
                contract=contract,
                policy=policy,
                now=now,
                configured_key_id=configured_key_id,
            )
            if not verdict.eligible:
                assert verdict.reason is not None
                refusals[int(item.source_item_id)] = verdict.reason
    return refusals


# ---------------------------------------------------------------------------
# The execution
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunReport:
    """What one run turned out to be. Counts and labels; never an identity.

    ``accepted`` is deliberately not called delivered: it means Meta took
    responsibility for the message, which is not a phone displaying it.
    """

    run_id: int
    outcome: str
    attempted: int = 0
    accepted: int = 0
    rejected: int = 0
    unknown: int = 0
    skipped: int = 0
    stopped: bool = False
    reasons: tuple[str, ...] = ()
    booking_reads: int = 0
    meta_requests: int = 0

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "outcome": self.outcome,
            "attempted": self.attempted,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "unknown": self.unknown,
            "skipped": self.skipped,
            "stopped": self.stopped,
            "reasons": list(self.reasons),
            "booking_reads": self.booking_reads,
            "meta_requests": self.meta_requests,
            # Said in the report rather than implied: this phase creates no
            # voucher, pays for nothing and refunds nothing.
            "voucher_mutations": 0,
        }


async def _planned_rows(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
) -> list[tuple[int, int, int]]:
    """``(send_id, source_item_id, slot)`` for every row still to do, in slot order."""
    from sqlalchemy import select

    async with session_maker() as session:
        rows = (
            await session.execute(
                select(
                    EasyWeekVoucherFollowupSend.id,
                    EasyWeekVoucherFollowupSend.source_item_id,
                    EasyWeekVoucherFollowupSend.source_slot,
                )
                .where(EasyWeekVoucherFollowupSend.run_id == int(run_id))
                .where(EasyWeekVoucherFollowupSend.status == FOLLOWUP_SEND_PLANNED)
                .order_by(EasyWeekVoucherFollowupSend.source_slot.asc())
            )
        ).all()
    return [(int(a), int(b), int(c)) for a, b, c in rows]


async def _skip(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    send_id: int,
    reason: str,
    evidence: dict[str, Any],
    now: datetime,
) -> None:
    """Record that this row lost eligibility BEFORE its attempt was spent.

    ``skipped`` spends nothing and blocks nothing, so the row may be recomputed
    in a later preview. Nobody takes its place in this run: §46.5 forbids
    adding a replacement, so the actual result is simply smaller than the
    confirmed count.
    """
    from sqlalchemy import select

    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupSend)
                    .where(EasyWeekVoucherFollowupSend.id == int(send_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None or row.status != FOLLOWUP_SEND_PLANNED:
                return
            row.status = FOLLOWUP_SEND_SKIPPED
            row.reason_code = reason
            row.evidence = {**dict(row.evidence or {}), **evidence}
            row.updated_at = now


async def execute_run(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run: ledger.StoredRun,
    owner: str,
    transports: Transports | None = None,
    template_reader: Any | None = None,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: Callable[[], datetime] = utcnow,
) -> RunReport:
    """Execute one confirmed run: at most one Meta request per planned row.

    The fence is checked again here. A confirmation is not a standing
    permission, and an operator who closed the fence between the confirmation
    and the executor picking the run up must get no messages sent.

    Per row, in this order and with nothing held across a request:

    * re-prove eligibility — locally, then the original artifact and the live
      booking history. A row that lost it is ``skipped`` with a reason;
    * CLAIM. One committed transaction, under the consent address locks and the
      run's row lock, which spends the attempt. See
      :data:`...send_identity.ATTEMPT_BOUNDARY`;
    * one POST, through a client that has no retry path at all;
    * record the answer: accepted with its message id, a proven rejection, or an
      honest unknown.

    The operator's terminal stop is read inside the claim, so a stop that
    becomes durable before the boundary produces zero requests for every row
    that follows. A stop found there ends the run rather than skipping one row.
    """
    moment = now()
    if not send_allowed():
        await ledger.finish_run(
            session_maker,
            run_id=run.id,
            owner=owner,
            status=FOLLOWUP_RUN_COMPLETED,
            outcome_code=ids.SEND_DISABLED,
            reason_codes=(ids.SEND_DISABLED,),
            result={"attempted": 0, "meta_requests": 0},
            now=moment,
        )
        return RunReport(run.id, "refused", reasons=(ids.SEND_DISABLED,))

    carrier = transports or Transports()
    attempted = accepted = rejected = unknown = skipped = 0
    booking_reads = meta_requests = 0
    reasons: list[str] = []
    stopped = False

    async with session_maker() as session:
        gate = await preview_module.batch_gate(session, batch_id=run.source_batch_id)
    if not gate.allowed:
        # A TERMINAL stop of the original mailing is irreversible, so its
        # follow-up rows are tombstoned here rather than left ``planned`` for a
        # later run to claim (§46.7). Every other gate refusal — a sender that
        # is momentarily gone, a template that is not proven — is NOT terminal,
        # so those rows keep their reservation and a later run may use it.
        terminal = gate.reason == BATCH_TERMINAL_STOP
        if terminal:
            await ledger.request_terminal_stop(
                session_maker,
                run_id=run.id,
                requested_by=_SOURCE_STOP_PRINCIPAL,
                now=moment,
            )
        await ledger.finish_run(
            session_maker,
            run_id=run.id,
            owner=owner,
            status=FOLLOWUP_RUN_COMPLETED,
            outcome_code=gate.reason or ids.SOURCE_TERMINAL_STOP,
            reason_codes=(gate.reason or ids.SOURCE_TERMINAL_STOP,),
            result={"attempted": 0, "meta_requests": 0},
            now=moment,
        )
        return RunReport(
            run.id,
            "stopped" if terminal else "refused",
            stopped=terminal,
            reasons=(gate.reason or ids.SOURCE_TERMINAL_STOP,),
        )

    approval = await ledger.load_approval(session_maker, approval_id=run.approval_id)
    assert approval is not None
    contract = approval.contract()

    sender_id = await _active_sender_id(
        session_maker,
        company_id=run.company_id,
        expected_digest=contract.sender_digest,
    )
    if sender_id is None:
        await ledger.finish_run(
            session_maker,
            run_id=run.id,
            owner=owner,
            status=FOLLOWUP_RUN_COMPLETED,
            outcome_code=SENDER_UNPROVEN,
            reason_codes=(SENDER_UNPROVEN,),
            result={"attempted": 0, "meta_requests": 0},
            now=moment,
        )
        return RunReport(run.id, "refused", reasons=(SENDER_UNPROVEN,))

    # One seam object for the whole run; each attempt still performs its own
    # live read through it.
    templates = template_reader if template_reader is not None else carrier.template_reader()

    pending = await _planned_rows(session_maker, run_id=run.id)
    async with carrier.reader() as reader:
        for send_id, source_item_id, slot in pending:
            moment = now()
            verdict = await recheck_module.prepare_row(
                session_maker,
                gate=gate,
                source_item_id=source_item_id,
                contract=contract,
                booking_reader=reader,
                template_reader=templates,
                policy=policy,
                now=moment,
            )
            booking_reads += verdict.booking_reads
            if not verdict.eligible:
                assert verdict.reason is not None
                await _skip(
                    session_maker,
                    send_id=send_id,
                    reason=verdict.reason,
                    evidence=dict(verdict.evidence),
                    now=moment,
                )
                skipped += 1
                reasons.append(verdict.reason)
                continue

            assert verdict.address is not None and verdict.consent_fingerprint is not None
            assert verdict.sender_digest is not None and verdict.recipient_digest is not None
            claim = await ledger.claim_send(
                session_maker,
                run_id=run.id,
                send_id=send_id,
                owner=owner,
                address=verdict.address,
                easyweek_customer_uuid=verdict.customer_uuid,
                expected_consent_fingerprint=verdict.consent_fingerprint,
                sender_digest=verdict.sender_digest,
                recipient_digest=verdict.recipient_digest,
                source_batch_id=run.source_batch_id,
                # The last word on the local conditions, taken INSIDE the
                # claiming transaction once its locks are granted. The pass
                # above describes the world before this claim waited for them.
                final_guard=recheck_module.claim_time_guard(
                    gate=gate,
                    source_item_id=source_item_id,
                    contract=contract,
                    expected=verdict,
                    policy=policy,
                    now=now,
                ),
                now=moment,
            )
            if claim.stopped:
                # The operator's stop, or the original mailing's terminal stop.
                # Zero requests from here on, and the run ends — but not before
                # every row it had not started is TOMBSTONED. §46.7 keeps a
                # terminal cancellation durable per original recipient even at
                # zero attempts, and leaving them ``planned`` would have left
                # them claimable by a later run. The operator's own stop has
                # already written those tombstones; this call is idempotent and
                # is what covers the source batch's terminal stop, which no
                # follow-up row knew about until now.
                stopped = True
                reasons.append(claim.reason or ids.RUN_STOPPED)
                await ledger.request_terminal_stop(
                    session_maker,
                    run_id=run.id,
                    requested_by=(
                        _SOURCE_STOP_PRINCIPAL if claim.reason == ids.SOURCE_TERMINAL_STOP else run.principal
                    ),
                    now=now(),
                )
                break
            if not claim.granted:
                await _skip(
                    session_maker,
                    send_id=send_id,
                    reason=claim.reason or ids.CONSENT_MOVED,
                    evidence={"phase": "claim"},
                    now=moment,
                )
                skipped += 1
                reasons.append(claim.reason or ids.CONSENT_MOVED)
                continue

            # Past the boundary. This recipient's one extra attempt is spent
            # whatever happens from here, and nothing may try again.
            attempted += 1
            # The raw sender id for THIS request, read now rather than cached at
            # the start of the run: the per-row re-check above just proved the
            # sender is active and still digests to the frozen route, and this
            # reads the value that proof was about. ``None`` cannot happen after
            # that proof; it is handled as an unknown rather than asserted,
            # because the attempt is already spent.
            row_sender_id = await _active_sender_id(
                session_maker,
                company_id=run.company_id,
                expected_digest=contract.sender_digest,
            )
            if row_sender_id is None:
                await ledger.record_send_outcome(
                    session_maker,
                    send_id=send_id,
                    status=FOLLOWUP_SEND_UNKNOWN,
                    reason_code=SENDER_UNPROVEN,
                    now=now(),
                )
                unknown += 1
                reasons.append(SENDER_UNPROVEN)
                continue
            params = verdict.params()
            try:
                async with carrier.sender() as sender:
                    meta_requests += 1
                    answer = await sender.send_voucher_template(
                        phone_number_id=row_sender_id,
                        to_e164=verdict.address,
                        template_name=contract.meta_template_name,
                        language=contract.template_language,
                        params=params,
                    )
            except Exception:  # noqa: BLE001 - an unknown is the honest outcome
                # The request may have left this process. Never a retry.
                logger.exception("followup_send: transport failure for run=%s slot=%s — UNKNOWN", run.id, slot)
                await ledger.record_send_outcome(
                    session_maker,
                    send_id=send_id,
                    status=FOLLOWUP_SEND_UNKNOWN,
                    reason_code=ids.META_UNKNOWN,
                    now=now(),
                )
                unknown += 1
                reasons.append(ids.META_UNKNOWN)
                continue
            finally:
                del params

            if answer.outcome == DELIVERY_ACCEPTED and answer.provider_message_id:
                await ledger.record_send_outcome(
                    session_maker,
                    send_id=send_id,
                    status=FOLLOWUP_SEND_ACCEPTED,
                    reason_code=None,
                    provider_message_id=answer.provider_message_id,
                    http_status=answer.http_status,
                    now=now(),
                )
                accepted += 1
            elif answer.outcome == DELIVERY_REJECTED:
                # Meta refused the request itself. The attempt is still spent:
                # §46.7 allows one extra message for the lifetime of the row,
                # and a rejection does not re-open it.
                await ledger.record_send_outcome(
                    session_maker,
                    send_id=send_id,
                    status=FOLLOWUP_SEND_REJECTED,
                    reason_code=ids.META_REJECTED,
                    http_status=answer.http_status,
                    now=now(),
                )
                rejected += 1
                reasons.append(ids.META_REJECTED)
            else:
                await ledger.record_send_outcome(
                    session_maker,
                    send_id=send_id,
                    status=FOLLOWUP_SEND_UNKNOWN,
                    reason_code=ids.META_UNKNOWN,
                    provider_message_id=answer.provider_message_id,
                    http_status=answer.http_status,
                    now=now(),
                )
                unknown += 1
                reasons.append(ids.META_UNKNOWN)

    outcome = "stopped" if stopped else ("sent" if attempted else "nothing_to_do")
    report = RunReport(
        run.id,
        outcome,
        attempted=attempted,
        accepted=accepted,
        rejected=rejected,
        unknown=unknown,
        skipped=skipped,
        stopped=stopped,
        reasons=tuple(dict.fromkeys(reasons)),
        booking_reads=booking_reads,
        meta_requests=meta_requests,
    )
    await ledger.finish_run(
        session_maker,
        run_id=run.id,
        owner=owner,
        status=FOLLOWUP_RUN_COMPLETED,
        outcome_code=outcome,
        reason_codes=report.reasons,
        result=report.as_safe_dict(),
        now=now(),
    )
    logger.info(
        "followup_send: run=%s outcome=%s attempted=%s accepted=%s unknown=%s skipped=%s requests=%s",
        run.id,
        outcome,
        attempted,
        accepted,
        unknown,
        skipped,
        meta_requests,
    )
    return report


async def _active_sender_id(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    company_id: int,
    expected_digest: str,
) -> str | None:
    """This branch's configured, active Meta sender id — if it is the frozen one.

    The raw identifier is needed for the POST and for nothing else: it is a
    configuration value, never written to a row here and never logged. What the
    ledger stores is its digest, and that digest is compared against the one the
    approval pinned, so a deployment repointed at another number cannot inherit
    this audience.

    ``None`` when the branch has no active sender or its identifier no longer
    digests to the approved route, which is a refusal rather than a reason to
    carry on with whatever the settings happen to name.
    """
    from sqlalchemy import select

    from altegio_bot.campaigns.easyweek_voucher_production.dispatch import SENDER_CODE
    from altegio_bot.models.models import PROVIDER_EASYWEEK, WhatsAppSender
    from altegio_bot.whatsapp_consent import sender_route_digest

    async with session_maker() as session:
        row = (
            await session.execute(
                select(WhatsAppSender)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.company_id == int(company_id))
                .where(WhatsAppSender.sender_code == SENDER_CODE)
            )
        ).scalar_one_or_none()
    if row is None or not row.is_active:
        return None
    identifier = (row.phone_number_id or "").strip()
    if not identifier or sender_route_digest(identifier) != expected_digest:
        return None
    return identifier
