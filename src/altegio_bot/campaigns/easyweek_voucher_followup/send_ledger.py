"""The durable follow-up send ledger: approvals, runs, claims, stop (§46.7–46.8).

What this module is responsible for, and nothing else
-----------------------------------------------------
1. **What did the server offer?** An approval row — immutable, single-use, with
   its own expiry and a keyed signature over its own material. It pins the
   operator, the action, the preview version and its composition digest, the
   groups, the interval, the whole message contract, the branch sender, the
   source stop generation and the count.
2. **Was the offer spent?** A run row. ``approval_id`` is UNIQUE, so one offer
   becomes at most one run however many times the POST arrives.
3. **Who may be messaged, once?** A send row per original recipient. Its three
   partial unique indexes are the one-attempt rule, and this module's claim is
   the transaction that occupies them.
4. **When must everything stop?** A terminal stop on the run, which tombstones
   every remaining row in the same key space — so the decision survives a new
   run, a new preview, a new group selection and a new template.

Where the one extra attempt is consumed
---------------------------------------
:func:`claim_send`'s commit, and nowhere else. It writes ``claimed``,
``attempt_count = 1``, a durable intent and the frozen route, and it commits
BEFORE the external POST. After that commit this original recipient's extra
message is spent whatever happens next: a crash, a timeout, a restart, a second
confirmation and a new run all find the row occupied and none of them gives the
attempt back. :data:`...send_identity.ATTEMPT_BOUNDARY` says it in one sentence
for the runbook and the tests to point at.

The lock order, and why it cannot deadlock with what already exists
-------------------------------------------------------------------
Three orders are used here, and ``run`` always precedes ``send``:

* a claim takes the consent ADDRESS advisory locks (through
  :func:`...whatsapp_consent.recheck_consent_boundary`), then the run row, then
  the send row;
* a terminal stop takes the run row, then its send rows;
* a callback takes the message-identifier advisory lock (the one
  ``lock_callback_scope`` already holds for every identifier in a delivery),
  then the send row.

Nothing in this phase holds a production batch header or item lock while asking
for a consent address lock, which is the property
:func:`...whatsapp_consent.lock_consent_address` documents and relies on. The
source batch's own stop is READ here, never locked, for the same reason: taking
the production header under a consent lock would invent the one ordering that
could cross with the existing stages.

No HTTP under a lock
--------------------
Every function here either holds no transaction or holds one that makes no
network call. The POST happens between :func:`claim_send` and
:func:`record_send_outcome`, with nothing open.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final, Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_followup import preview as preview_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as ids
from altegio_bot.campaigns.easyweek_voucher_followup.send_eligibility import SendContract
from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger
from altegio_bot.campaigns.easyweek_voucher_production.operations import (
    IDENTIFICATION_LIMIT_SHARED,
    OpsPrincipal,
)
from altegio_bot.models.models import (
    FOLLOWUP_ACTION_SEND,
    FOLLOWUP_APPROVAL_CONSUMED,
    FOLLOWUP_APPROVAL_PENDING,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_RUN_INTERRUPTED,
    FOLLOWUP_RUN_QUEUED,
    FOLLOWUP_RUN_RUNNING,
    FOLLOWUP_RUN_STOPPED,
    FOLLOWUP_RUN_TERMINAL,
    FOLLOWUP_SEND_CLAIMED,
    FOLLOWUP_SEND_PLANNED,
    FOLLOWUP_SEND_SKIPPED,
    FOLLOWUP_SEND_STOPPED,
    FOLLOWUP_SEND_UNKNOWN,
    EasyWeekVoucherFollowupApproval,
    EasyWeekVoucherFollowupPreviewRow,
    EasyWeekVoucherFollowupRun,
    EasyWeekVoucherFollowupSend,
)
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import recheck_consent_boundary

logger = logging.getLogger("easyweek_voucher_followup_send")

__all__ = [
    "ClaimOutcome",
    "ConfirmOutcome",
    "PlanOffer",
    "SendRowView",
    "StoredApproval",
    "StoredRun",
    "claim_next_run",
    "claim_send",
    "confirm_approval",
    "finish_run",
    "interrupt_abandoned_runs",
    "latest_run_for_batch",
    "load_approval",
    "load_run",
    "plan_signature",
    "recipient_digest_for_rows",
    "record_send_outcome",
    "renew_lease",
    "request_terminal_stop",
    "resolve_claimed_as_unknown",
    "run_for_approval",
    "send_rows",
    "store_approval",
    "unsendable_source_items",
]

_SIGNATURE_DOMAIN: Final = "altegio_bot/easyweek_voucher_followup/send_plan/v1"
_RECIPIENT_DIGEST_DOMAIN: Final = "altegio_bot/easyweek_voucher_followup/recipient_set/v1"


# ---------------------------------------------------------------------------
# The signed plan material
# ---------------------------------------------------------------------------


def _signing_key() -> bytes:
    """The key the plan signature is taken with.

    The same secret the Ops session cookie and its CSRF token are derived from,
    because the signature's job is the same: to prove that THIS server issued
    this offer. It is never sent anywhere and never logged.
    """
    from altegio_bot.settings import settings

    return (settings.ops_secret or settings.ops_pass or "").encode("utf-8")


def recipient_digest_for_rows(rows: Sequence[tuple[int, str]]) -> str:
    """A deterministic handle over WHICH people an offer covers.

    Slots and entitlement digests only — never a phone number, a name, a uuid or
    a MAC — so the value is safe to show, log and compare. It exists because a
    count is not an audience: one member swapped for another leaves the count
    identical, and §46.8 requires the confirmation to be about an exact
    composition.
    """
    material = json.dumps(
        [[int(slot), str(digest)] for slot, digest in sorted(rows)],
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(f"{_RECIPIENT_DIGEST_DOMAIN}\x1f{material}".encode("utf-8")).hexdigest()


def plan_signature(material: dict[str, Any]) -> str:
    """A keyed digest over the plan this server is offering.

    Keyed rather than a plain hash, so an approval row whose values were edited
    outside this code no longer verifies, and so a client cannot compute one for
    a plan the server never made.
    """
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hmac.new(_signing_key(), f"{_SIGNATURE_DOMAIN}\x1f{encoded}".encode("utf-8"), hashlib.sha256).hexdigest()


def _approval_material(row: EasyWeekVoucherFollowupApproval) -> dict[str, Any]:
    """Exactly the facts the signature covers, read back off the stored row."""
    return {
        "action": str(row.action),
        "preview_id": int(row.preview_id),
        "preview_version": int(row.preview_version),
        "source_batch_id": int(row.source_batch_id),
        "campaign_run_id": int(row.campaign_run_id),
        "company_id": int(row.company_id),
        "source_request_schema_version": str(row.source_request_schema_version),
        "source_frozen_digest": str(row.source_frozen_digest),
        "source_stop_generation": int(row.source_stop_generation),
        "composition_digest": str(row.composition_digest),
        "selected_groups": list(row.selected_groups or ()),
        "min_interval_seconds": int(row.min_interval_seconds),
        "approved_recipient_count": int(row.approved_recipient_count),
        "recipient_digest": str(row.recipient_digest),
        "template_code": str(row.template_code),
        "meta_template_name": str(row.meta_template_name),
        "template_language": str(row.template_language),
        "template_category": str(row.template_category),
        "template_parameter_order": list(row.template_parameter_order or ()),
        "template_body_digest": str(row.template_body_digest),
        "contract_version": str(row.contract_version),
        "failure_policy_version": str(row.failure_policy_version),
        "sender_digest": str(row.sender_digest),
        "principal": str(row.principal),
        "session_fingerprint": str(row.session_fingerprint),
        "nonce": str(row.nonce),
        "issued_at": row.issued_at.isoformat(),
        "expires_at": row.expires_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Read-only views
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StoredApproval:
    """One offer as a reader needs it. PII-free; no signing key, no address."""

    id: int
    action: str
    preview_id: int
    preview_version: int
    source_batch_id: int
    campaign_run_id: int
    company_id: int
    composition_digest: str
    selected_groups: tuple[str, ...]
    min_interval_seconds: int
    approved_recipient_count: int
    recipient_digest: str
    source_frozen_digest: str
    source_request_schema_version: str
    source_stop_generation: int
    template_code: str
    meta_template_name: str
    template_language: str
    template_category: str
    template_parameter_order: tuple[str, ...]
    template_body_digest: str
    contract_version: str
    failure_policy_version: str
    sender_digest: str
    principal: str
    session_fingerprint: str
    nonce: str
    plan_signature: str
    issued_at: datetime
    expires_at: datetime
    status: str

    @property
    def pending(self) -> bool:
        return self.status == FOLLOWUP_APPROVAL_PENDING

    def expired_at(self, now: datetime) -> bool:
        return now > self.expires_at

    def contract(self) -> SendContract:
        return SendContract(
            template_code=self.template_code,
            meta_template_name=self.meta_template_name,
            template_language=self.template_language,
            template_category=self.template_category,
            template_parameter_order=tuple(self.template_parameter_order),
            template_body_digest=self.template_body_digest,
            sender_digest=self.sender_digest,
            selected_groups=tuple(self.selected_groups),
            min_interval_seconds=int(self.min_interval_seconds),
            failure_policy_version=self.failure_policy_version,
        )

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.id,
            "action": self.action,
            "status": self.status,
            "preview_id": self.preview_id,
            "preview_version": self.preview_version,
            "source_batch_id": self.source_batch_id,
            "composition_digest": self.composition_digest,
            "selected_groups": list(self.selected_groups),
            "min_interval_seconds": self.min_interval_seconds,
            "approved_recipient_count": self.approved_recipient_count,
            "recipient_digest": self.recipient_digest,
            "template_code": self.template_code,
            "meta_template_name": self.meta_template_name,
            "template_language": self.template_language,
            "template_body_digest": self.template_body_digest,
            "contract_version": self.contract_version,
            "failure_policy_version": self.failure_policy_version,
            "issued_at": self.issued_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "principal": self.principal,
            "session": self.session_fingerprint[:12],
            "identification_limit": IDENTIFICATION_LIMIT_SHARED,
        }


@dataclass(frozen=True)
class StoredRun:
    """One confirmed run as a page or the executor needs it."""

    id: int
    approval_id: int
    preview_id: int
    preview_version: int
    source_batch_id: int
    campaign_run_id: int
    company_id: int
    composition_digest: str
    principal: str
    status: str
    planned_count: int
    attempts: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    stop_requested_at: datetime | None
    stop_requested_by: str | None
    queued_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    outcome_code: str | None
    reason_codes: tuple[str, ...] = ()
    result: dict[str, Any] = field(default_factory=dict)

    @property
    def finished(self) -> bool:
        return self.status in FOLLOWUP_RUN_TERMINAL

    @property
    def stopped(self) -> bool:
        return self.stop_requested_at is not None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.id,
            "approval_id": self.approval_id,
            "preview_id": self.preview_id,
            "preview_version": self.preview_version,
            "source_batch_id": self.source_batch_id,
            # queued / running / completed / stopped / interrupted / refused.
            # Not one of them is "delivered": what reached a person is the
            # per-row status, never this field.
            "status": self.status,
            "planned_count": self.planned_count,
            "attempts": self.attempts,
            "stop_requested": self.stopped,
            "stop_requested_at": self.stop_requested_at.isoformat() if self.stop_requested_at else None,
            "queued_at": self.queued_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "outcome_code": self.outcome_code,
            "reason_codes": list(self.reason_codes),
            "finished": self.finished,
            "report": dict(self.result),
        }


@dataclass(frozen=True)
class SendRowView:
    """One recipient's follow-up row, as a page may print it.

    Slot, state, reason and the follow-up's own statuses. No phone, no name, no
    code, no uuid, no message id and no digest an operator would have to copy.
    """

    slot: int
    group: str
    status: str
    reason: str | None
    attempt_count: int
    claimed_at: datetime | None
    answered_at: datetime | None
    delivered_at: datetime | None
    read_at: datetime | None
    failed_at: datetime | None
    contradiction_count: int

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "slot": self.slot,
            "group": self.group,
            "status": self.status,
            "reason": self.reason,
            "attempt_spent": bool(self.attempt_count),
            "claimed_at": self.claimed_at.isoformat() if self.claimed_at else None,
            "answered_at": self.answered_at.isoformat() if self.answered_at else None,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "read_at": self.read_at.isoformat() if self.read_at else None,
            "failed_at": self.failed_at.isoformat() if self.failed_at else None,
            "contradictions": self.contradiction_count,
        }


def _approval(row: EasyWeekVoucherFollowupApproval) -> StoredApproval:
    return StoredApproval(
        id=int(row.id),
        action=str(row.action),
        preview_id=int(row.preview_id),
        preview_version=int(row.preview_version),
        source_batch_id=int(row.source_batch_id),
        campaign_run_id=int(row.campaign_run_id),
        company_id=int(row.company_id),
        composition_digest=str(row.composition_digest),
        selected_groups=tuple(row.selected_groups or ()),
        min_interval_seconds=int(row.min_interval_seconds),
        approved_recipient_count=int(row.approved_recipient_count),
        recipient_digest=str(row.recipient_digest),
        source_frozen_digest=str(row.source_frozen_digest),
        source_request_schema_version=str(row.source_request_schema_version),
        source_stop_generation=int(row.source_stop_generation or 0),
        template_code=str(row.template_code),
        meta_template_name=str(row.meta_template_name),
        template_language=str(row.template_language),
        template_category=str(row.template_category),
        template_parameter_order=tuple(row.template_parameter_order or ()),
        template_body_digest=str(row.template_body_digest),
        contract_version=str(row.contract_version),
        failure_policy_version=str(row.failure_policy_version),
        sender_digest=str(row.sender_digest),
        principal=str(row.principal),
        session_fingerprint=str(row.session_fingerprint),
        nonce=str(row.nonce),
        plan_signature=str(row.plan_signature),
        issued_at=row.issued_at,
        expires_at=row.expires_at,
        status=str(row.status),
    )


def _run(row: EasyWeekVoucherFollowupRun) -> StoredRun:
    return StoredRun(
        id=int(row.id),
        approval_id=int(row.approval_id),
        preview_id=int(row.preview_id),
        preview_version=int(row.preview_version),
        source_batch_id=int(row.source_batch_id),
        campaign_run_id=int(row.campaign_run_id),
        company_id=int(row.company_id),
        composition_digest=str(row.composition_digest),
        principal=str(row.principal),
        status=str(row.status),
        planned_count=int(row.planned_count or 0),
        attempts=int(row.attempts or 0),
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
        stop_requested_at=row.stop_requested_at,
        stop_requested_by=row.stop_requested_by,
        queued_at=row.queued_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        outcome_code=row.outcome_code,
        reason_codes=tuple(str(value) for value in (row.reason_codes or ())),
        result=dict(row.result or {}),
    )


def _send_view(row: EasyWeekVoucherFollowupSend) -> SendRowView:
    return SendRowView(
        slot=int(row.source_slot),
        group=str(row.group_code),
        status=str(row.status),
        reason=row.reason_code,
        attempt_count=int(row.attempt_count or 0),
        claimed_at=row.claimed_at,
        answered_at=row.answered_at,
        delivered_at=row.delivered_at,
        read_at=row.read_at,
        failed_at=row.failed_at,
        contradiction_count=int(row.contradiction_count or 0),
    )


# ---------------------------------------------------------------------------
# The offer
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlanOffer:
    """What the server offers the browser, or the named reasons it offers nothing."""

    ready: bool
    reasons: tuple[str, ...] = ()
    approval: StoredApproval | None = None
    # The rows the offer covers, for the screen. Slots and reasons only.
    rows: tuple[tuple[int, str], ...] = ()

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "reasons": list(self.reasons),
            "approval": self.approval.as_safe_dict() if self.approval is not None else None,
            "plan_signature": self.approval.plan_signature if self.approval is not None else None,
            "slots": [int(slot) for slot, _digest in self.rows],
        }


async def included_preview_rows(
    session: AsyncSession,
    *,
    preview_id: int,
) -> list[EasyWeekVoucherFollowupPreviewRow]:
    """Every INCLUDED row of one stored preview version, in slot order.

    The composition comes from the stored snapshot and from nothing a request
    can name: §46.8 is explicit that a browser may not supply a recipient list,
    a digest or a count that is then used.
    """
    return list(
        (
            await session.execute(
                select(EasyWeekVoucherFollowupPreviewRow)
                .where(EasyWeekVoucherFollowupPreviewRow.preview_id == int(preview_id))
                .where(EasyWeekVoucherFollowupPreviewRow.decision == FOLLOWUP_ROW_INCLUDED)
                .order_by(EasyWeekVoucherFollowupPreviewRow.source_slot.asc())
            )
        ).scalars()
    )


async def unsendable_source_items(
    session: AsyncSession,
    *,
    source_batch_id: int,
) -> dict[int, str]:
    """``{source_item_id: reason}`` for recipients whose attempt is accounted for.

    Read from the single-attempt key's own rows, so the answer is exactly what
    the database would refuse: a spent attempt, or a terminal tombstone. It is a
    courtesy for the screen and for the confirmation's row-by-row decision — the
    guarantee is the partial unique indexes, not this read.
    """
    rows = (
        await session.execute(
            select(EasyWeekVoucherFollowupSend.source_item_id, EasyWeekVoucherFollowupSend.status).where(
                EasyWeekVoucherFollowupSend.source_batch_id == int(source_batch_id)
            )
        )
    ).all()
    found: dict[int, str] = {}
    for item_id, status in rows:
        if not ids.blocks_further_attempt(str(status)):
            continue
        reason = ids.TERMINALLY_STOPPED if str(status) == FOLLOWUP_SEND_STOPPED else ids.ATTEMPT_ALREADY_USED
        # A tombstone is the stronger statement and must not be overwritten by a
        # spent attempt of another row of the same person.
        if found.get(int(item_id)) != ids.TERMINALLY_STOPPED:
            found[int(item_id)] = reason
    return found


async def store_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    principal: OpsPrincipal,
    stored_preview: preview_module.StoredPreview,
    gate: preview_module.BatchGate,
    rows: Sequence[tuple[int, str]],
    approved_recipient_count: int,
    now: datetime,
    ttl: timedelta = ids.PLAN_MAX_AGE,
) -> StoredApproval:
    """Write the immutable offer. Only ever called for a plan that was READY.

    ``rows`` is every INCLUDED row of the stored preview, and its digest is what
    ties this offer to that exact composition: the snapshot is immutable, so a
    digest that stops matching can only mean the approval and the preview are
    about different people.

    ``approved_recipient_count`` is narrower on purpose — the rows that could
    still be sent to when the plan was built, with the ones whose extra attempt
    is already accounted for left out. That is the number an operator confirms,
    because it is the number of messages pressing the button can produce.

    ``expires_at`` is derived from when the plan was ISSUED, so queueing or a
    slow browser cannot extend the window.
    """
    moment = now
    async with session_maker() as session:
        async with session.begin():
            row = EasyWeekVoucherFollowupApproval(
                action=FOLLOWUP_ACTION_SEND,
                preview_id=int(stored_preview.preview_id),
                preview_version=int(stored_preview.version),
                source_batch_id=int(stored_preview.batch_id),
                campaign_run_id=int(gate.campaign_run_id or 0),
                company_id=int(gate.company_id or 0),
                location_uuid=gate.location_uuid,
                source_request_schema_version=str(gate.schema_version),
                source_frozen_digest=str(gate.frozen_digest),
                source_stop_generation=int(gate.stop_generation),
                composition_digest=str(stored_preview.composition_digest),
                selected_groups=list(stored_preview.selected_groups),
                min_interval_seconds=int(stored_preview.min_interval_seconds),
                approved_recipient_count=int(approved_recipient_count),
                recipient_digest=recipient_digest_for_rows(rows),
                template_code=str(stored_preview.template_code),
                meta_template_name=str(stored_preview.meta_template_name),
                template_language=str(stored_preview.template_language),
                template_category=str(stored_preview.template_category),
                template_parameter_order=list(stored_preview.template_parameter_order),
                template_body_digest=str(stored_preview.template_body_digest),
                contract_version=ids.SEND_CONTRACT_VERSION,
                failure_policy_version=str(stored_preview.failure_policy_version),
                sender_digest=str(gate.sender_digest or ""),
                principal=principal.account,
                session_fingerprint=principal.session_fingerprint,
                identification_limit=principal.identification_limit,
                nonce=secrets.token_hex(16),
                # Filled in below: the signature covers the stored values, so it
                # is computed from the row rather than from the arguments.
                plan_signature="",
                issued_at=moment,
                expires_at=moment + ttl,
                status=FOLLOWUP_APPROVAL_PENDING,
            )
            session.add(row)
            await session.flush()
            row.plan_signature = plan_signature(_approval_material(row))
            await session.flush()
            return _approval(row)


async def load_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
) -> StoredApproval | None:
    async with session_maker() as session:
        row = await session.get(EasyWeekVoucherFollowupApproval, int(approval_id))
        return _approval(row) if row is not None else None


# ---------------------------------------------------------------------------
# The confirmation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConfirmOutcome:
    """What a confirmation did: created a run, found one, or refused."""

    run: StoredRun | None
    reasons: tuple[str, ...] = ()
    created: bool = False
    # How many rows were written as planned and how many were refused on the
    # spot because their attempt is already accounted for. §46.5: the actual
    # result may be smaller than the confirmed count and nobody replaces them.
    planned: int = 0
    skipped: int = 0

    @property
    def accepted(self) -> bool:
        return self.run is not None


async def confirm_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
    presented_signature: str,
    principal: OpsPrincipal,
    confirmed_count: int | None,
    live_contract_matches: bool,
    row_refusals: dict[int, str] | None = None,
    now: datetime | None = None,
) -> ConfirmOutcome:
    """Consume ONE offer and create ONE run with its planned rows, atomically.

    Every refusal happens before any run exists, so a refused confirmation has
    provably started nothing.

    What is checked, and why each one:

    * the approval exists and its stored signature verifies, and the signature
      the client presented is that one. An id is not a capability;
    * it belongs to the account presenting it — another session's offer is not
      this one's to spend;
    * its action is this ledger's one action. An approval can never be spent on
      a different verb than the one the operator read;
    * it has not expired;
    * it is still pending — and if it is not, the run it already became is
      returned instead of an error, because a double click, a retried POST, a
      refresh and a second tab are one event from the operator's point of view;
    * the count the operator confirmed is the approval's own count. It is
      COMPARED, never used, so a browser cannot widen anything by sending it;
    * the message contract and the failure policy are still the ones the
      approval pinned, and the source batch has neither been terminally stopped
      nor stopped again since the plan was built;
    * no other run of this source batch is still in flight.

    Then the rows. Each included preview row becomes a ``planned`` reservation —
    which spends nothing — unless it is refused, in which case it is written as
    ``skipped`` with its reason and **nobody takes its place** (§46.5). Two kinds
    of refusal arrive here:

    * this original recipient's extra attempt is already accounted for — spent in
      an earlier run, or tombstoned by a terminal stop;
    * ``row_refusals``, the caller's local re-check of every condition at
      CONFIRMATION time: §46.5 asks for the eligibility to be proven again here
      and again before the send, and a row that lost consent, identity, its
      group, its interval or its entitlement since the preview is excluded now
      rather than discovered by the executor.

    Both are pre-reads, and neither is the guarantee: the guarantee is the three
    partial unique indexes plus the locked consent boundary inside the claim.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupApproval)
                    .where(EasyWeekVoucherFollowupApproval.id == int(approval_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return ConfirmOutcome(None, (ids.APPROVAL_UNKNOWN,))

            stored = _approval(row)
            expected = plan_signature(_approval_material(row))
            if not stored.plan_signature or not hmac.compare_digest(stored.plan_signature, expected):
                # The row no longer describes the plan this server signed.
                return ConfirmOutcome(None, (ids.APPROVAL_SIGNATURE_INVALID,))
            if not presented_signature or not hmac.compare_digest(str(presented_signature), expected):
                return ConfirmOutcome(None, (ids.APPROVAL_SIGNATURE_INVALID,))
            if stored.action != FOLLOWUP_ACTION_SEND:
                return ConfirmOutcome(None, (ids.APPROVAL_UNKNOWN,))
            if stored.principal != principal.account:
                return ConfirmOutcome(None, (ids.APPROVAL_PRINCIPAL_MISMATCH,))

            if stored.status == FOLLOWUP_APPROVAL_CONSUMED:
                existing = (
                    await session.execute(
                        select(EasyWeekVoucherFollowupRun).where(
                            EasyWeekVoucherFollowupRun.approval_id == int(approval_id)
                        )
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    return ConfirmOutcome(_run(existing), (ids.APPROVAL_ALREADY_CONSUMED,))
                return ConfirmOutcome(None, (ids.APPROVAL_ALREADY_CONSUMED,))

            if stored.expired_at(moment):
                return ConfirmOutcome(None, (ids.APPROVAL_EXPIRED,))
            if not live_contract_matches:
                return ConfirmOutcome(None, (ids.APPROVAL_CONTRACT_MOVED,))
            if confirmed_count != stored.approved_recipient_count:
                return ConfirmOutcome(None, (ids.APPROVAL_COUNT_UNCONFIRMED,))

            # The SOURCE batch's stop, read rather than locked. §45.4 makes it
            # terminal on this contract, and §46.5 extends that to the
            # follow-up: nothing resumes it, so a fresh plan is not a way back.
            if await production_ledger.terminal_stop_active_in_session(session, batch_id=stored.source_batch_id):
                return ConfirmOutcome(None, (ids.SOURCE_TERMINAL_STOP,))
            _active, generation = await production_ledger.stop_facts_in_session(
                session, batch_id=stored.source_batch_id
            )
            if int(generation) != stored.source_stop_generation:
                # The original mailing was stopped (or stopped again) after this
                # plan was built, so its author had not seen that stop.
                return ConfirmOutcome(None, (ids.APPROVAL_CONTRACT_MOVED,))

            busy = (
                await session.execute(
                    select(EasyWeekVoucherFollowupRun.id)
                    .where(EasyWeekVoucherFollowupRun.source_batch_id == stored.source_batch_id)
                    .where(EasyWeekVoucherFollowupRun.status.in_([FOLLOWUP_RUN_QUEUED, FOLLOWUP_RUN_RUNNING]))
                    .limit(1)
                )
            ).first()
            if busy is not None:
                return ConfirmOutcome(None, (ids.RUN_ALREADY_ACTIVE,))

            preview_rows = await included_preview_rows(session, preview_id=stored.preview_id)
            if (
                recipient_digest_for_rows(
                    [(int(item.source_slot), str(item.source_entitlement_digest)) for item in preview_rows]
                )
                != stored.recipient_digest
            ):
                # The stored snapshot is immutable, so this can only mean the
                # approval and the preview no longer describe the same people.
                return ConfirmOutcome(None, (ids.APPROVAL_CONTRACT_MOVED,))

            row.status = FOLLOWUP_APPROVAL_CONSUMED
            row.consumed_at = moment
            row.updated_at = moment

            run = EasyWeekVoucherFollowupRun(
                approval_id=int(approval_id),
                preview_id=stored.preview_id,
                preview_version=stored.preview_version,
                source_batch_id=stored.source_batch_id,
                campaign_run_id=stored.campaign_run_id,
                company_id=stored.company_id,
                composition_digest=stored.composition_digest,
                principal=principal.account,
                session_fingerprint=principal.session_fingerprint,
                identification_limit=principal.identification_limit,
                status=FOLLOWUP_RUN_QUEUED,
                planned_count=0,
                attempts=0,
                queued_at=moment,
                updated_at=moment,
            )
            # The UNIQUE on ``approval_id`` is what makes this the one run. A
            # second confirmation that got this far concurrently fails here, in
            # PostgreSQL, and its whole transaction rolls back — which is why the
            # approval and the run are written together rather than in two steps.
            session.add(run)
            await session.flush()

            blocked = await unsendable_source_items(session, source_batch_id=stored.source_batch_id)
            refused_now = dict(row_refusals or {})
            planned = 0
            skipped = 0
            for item in preview_rows:
                # A spent attempt or a tombstone is the stronger statement and
                # is reported as such; otherwise this confirmation's own
                # re-check has the say.
                refusal = blocked.get(int(item.source_item_id)) or refused_now.get(int(item.source_item_id))
                session.add(
                    EasyWeekVoucherFollowupSend(
                        run_id=int(run.id),
                        source_batch_id=int(item.source_batch_id),
                        source_item_id=int(item.source_item_id),
                        source_slot=int(item.source_slot),
                        campaign_recipient_id=int(item.campaign_recipient_id),
                        easyweek_customer_uuid=item.easyweek_customer_uuid,
                        source_entitlement_digest=str(item.source_entitlement_digest),
                        source_attempted_at=item.source_attempted_at,
                        group_code=str(item.group_code),
                        status=FOLLOWUP_SEND_PLANNED if refusal is None else FOLLOWUP_SEND_SKIPPED,
                        attempt_count=0,
                        reason_code=refusal,
                        template_code=stored.template_code,
                        meta_template_name=stored.meta_template_name,
                        template_language=stored.template_language,
                        template_body_digest=stored.template_body_digest,
                        evidence={},
                        updated_at=moment,
                    )
                )
                if refusal is None:
                    planned += 1
                else:
                    skipped += 1
            run.planned_count = planned
            await session.flush()
            return ConfirmOutcome(_run(run), (), created=True, planned=planned, skipped=skipped)


async def run_for_approval(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    approval_id: int,
) -> StoredRun | None:
    async with session_maker() as session:
        row = (
            await session.execute(
                select(EasyWeekVoucherFollowupRun).where(EasyWeekVoucherFollowupRun.approval_id == int(approval_id))
            )
        ).scalar_one_or_none()
        return _run(row) if row is not None else None


async def load_run(session_maker: async_sessionmaker[AsyncSession], *, run_id: int) -> StoredRun | None:
    async with session_maker() as session:
        row = await session.get(EasyWeekVoucherFollowupRun, int(run_id))
        return _run(row) if row is not None else None


async def latest_run_for_batch(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    source_batch_id: int,
) -> StoredRun | None:
    """The newest run of this original mailing, for the status screen."""
    async with session_maker() as session:
        row = (
            await session.execute(
                select(EasyWeekVoucherFollowupRun)
                .where(EasyWeekVoucherFollowupRun.source_batch_id == int(source_batch_id))
                .order_by(EasyWeekVoucherFollowupRun.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        return _run(row) if row is not None else None


async def send_rows(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
) -> tuple[SendRowView, ...]:
    async with session_maker() as session:
        rows = (
            await session.execute(
                select(EasyWeekVoucherFollowupSend)
                .where(EasyWeekVoucherFollowupSend.run_id == int(run_id))
                .order_by(EasyWeekVoucherFollowupSend.source_slot.asc())
            )
        ).scalars()
        return tuple(_send_view(row) for row in rows)


# ---------------------------------------------------------------------------
# The executor's hold on a run
# ---------------------------------------------------------------------------


async def claim_next_run(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    owner: str,
    now: datetime | None = None,
    lease: timedelta = ids.LEASE,
) -> StoredRun | None:
    """Take ownership of the oldest queued run, or answer ``None``.

    ``FOR UPDATE SKIP LOCKED`` plus a compare-and-set on ``status``: two
    executors — which the supported topology does not have, and which a
    mis-deploy can still produce — cannot both take the same run, and the loser
    moves on rather than blocking.

    Only ``queued`` runs are eligible. An ``interrupted`` one is never picked up
    again.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupRun)
                    .where(EasyWeekVoucherFollowupRun.status == FOLLOWUP_RUN_QUEUED)
                    .order_by(EasyWeekVoucherFollowupRun.id.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.status = FOLLOWUP_RUN_RUNNING
            row.lease_owner = owner
            row.lease_expires_at = moment + lease
            row.started_at = row.started_at or moment
            row.attempts = int(row.attempts or 0) + 1
            row.updated_at = moment
            await session.flush()
            return _run(row)


async def renew_lease(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    owner: str,
    lease: timedelta = ids.LEASE,
    now: datetime | None = None,
) -> bool:
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupRun)
                    .where(EasyWeekVoucherFollowupRun.id == int(run_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None or row.status != FOLLOWUP_RUN_RUNNING or row.lease_owner != owner:
                return False
            row.lease_expires_at = moment + lease
            row.updated_at = moment
            await session.flush()
            return True


async def finish_run(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    owner: str | None,
    status: str,
    outcome_code: str | None,
    reason_codes: Sequence[str] = (),
    result: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> StoredRun | None:
    """Record a terminal outcome, once.

    Refuses to move a run that is already finished: a late writer must not
    overwrite what an earlier one proved, and in particular must not replace an
    ``interrupted`` row — whose whole meaning is "a human has to look" — with a
    tidier-looking one. A run an operator stopped stays ``stopped``.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupRun)
                    .where(EasyWeekVoucherFollowupRun.id == int(run_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            if row.status in FOLLOWUP_RUN_TERMINAL:
                return _run(row)
            if owner is not None and row.lease_owner not in (None, owner):
                return _run(row)
            # An operator's stop outranks whatever the executor would have
            # called this run: the decision is theirs and it is terminal.
            row.status = FOLLOWUP_RUN_STOPPED if row.stop_requested_at is not None else status
            row.outcome_code = outcome_code
            row.reason_codes = list(dict.fromkeys(reason_codes))
            row.result = dict(result or {})
            row.finished_at = moment
            row.lease_owner = None
            row.lease_expires_at = None
            row.updated_at = moment
            await session.flush()
            return _run(row)


async def interrupt_abandoned_runs(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    now: datetime | None = None,
    include_all_running: bool = False,
) -> list[StoredRun]:
    """Mark runs whose executor is gone as ``interrupted``. Never ``queued``.

    Two callers, one transition: the executor at START-UP, where the supported
    topology (one API, one dedicated executor, no rolling deploy) means a
    ``running`` row belongs to a process that died; and the executor's own loop,
    for a lease that expired while it was up.

    Every ``claimed`` row of such a run becomes ``unknown`` in the same pass.
    That is the honest reading: the attempt was spent before the POST, so the
    message may be on its way to a real person and nothing may retry it.
    """
    moment = now or utcnow()
    interrupted: list[StoredRun] = []
    async with session_maker() as session:
        async with session.begin():
            rows = list(
                (
                    await session.execute(
                        select(EasyWeekVoucherFollowupRun)
                        .where(EasyWeekVoucherFollowupRun.status == FOLLOWUP_RUN_RUNNING)
                        .order_by(EasyWeekVoucherFollowupRun.id.asc())
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                lease_over = row.lease_expires_at is not None and row.lease_expires_at <= moment
                if not include_all_running and not lease_over:
                    continue
                await _resolve_claimed_locked(session, run_id=int(row.id), now=moment)
                row.status = FOLLOWUP_RUN_INTERRUPTED
                row.outcome_code = ids.RUN_INTERRUPTED
                row.reason_codes = [ids.RUN_INTERRUPTED]
                row.finished_at = moment
                row.lease_owner = None
                row.lease_expires_at = None
                row.updated_at = moment
                await session.flush()
                interrupted.append(_run(row))
    return interrupted


async def _resolve_claimed_locked(session: AsyncSession, *, run_id: int, now: datetime) -> int:
    """Turn every still-``claimed`` row of this run into an honest ``unknown``.

    Inside a transaction that already holds the run row, which is the order
    every writer here uses. The attempt stays spent — that is the whole point —
    and the row keeps its intent, its route and its moment so a later
    reconciliation has something exact to work from.
    """
    rows = list(
        (
            await session.execute(
                select(EasyWeekVoucherFollowupSend)
                .where(EasyWeekVoucherFollowupSend.run_id == int(run_id))
                .where(EasyWeekVoucherFollowupSend.status == FOLLOWUP_SEND_CLAIMED)
                .order_by(EasyWeekVoucherFollowupSend.id.asc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    for row in rows:
        row.status = FOLLOWUP_SEND_UNKNOWN
        row.reason_code = ids.META_UNKNOWN
        row.answered_at = now
        row.updated_at = now
        row.evidence = {**dict(row.evidence or {}), "unknown_cause": "executor_interrupted"}
    if rows:
        await session.flush()
    return len(rows)


async def resolve_claimed_as_unknown(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    now: datetime | None = None,
) -> int:
    """The same transition, for a caller that holds no transaction."""
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                select(EasyWeekVoucherFollowupRun).where(EasyWeekVoucherFollowupRun.id == int(run_id)).with_for_update()
            )
            return await _resolve_claimed_locked(session, run_id=int(run_id), now=moment)


# ---------------------------------------------------------------------------
# The terminal stop
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StopOutcome:
    """What a terminal stop did. Always durable, even when it stopped nobody."""

    applied: bool
    reason: str | None = None
    run_id: int | None = None
    tombstoned: int = 0
    already_stopped: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "reason": self.reason,
            "run_id": self.run_id,
            "tombstoned": self.tombstoned,
            "already_stopped": self.already_stopped,
        }


async def request_terminal_stop(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    requested_by: str,
    now: datetime | None = None,
) -> StopOutcome:
    """Stop this run for good and tombstone every row it has not started.

    Terminal by contract. Nothing clears it: a new confirmation, a refresh, a
    restart and a recomputation all find the tombstones in the single-attempt
    key and are refused for exactly those recipients (§46.7). Rows that were
    already claimed keep their own state — an attempt that may have reached Meta
    is not something a stop can pretend to cancel.

    Allowed while the run is queued, running or already finished, and with the
    fence closed: those are exactly the moments an operator wants it. Pressing
    it twice is reported as such rather than as an error.

    Takes the run row first and its send rows second, which is this phase's one
    order; it takes no consent lock and no production lock, so it cannot take
    part in a cycle with either.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            run = (
                await session.execute(
                    select(EasyWeekVoucherFollowupRun)
                    .where(EasyWeekVoucherFollowupRun.id == int(run_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if run is None:
                return StopOutcome(False, ids.APPROVAL_UNKNOWN)
            already = run.stop_requested_at is not None
            if not already:
                run.stop_requested_at = moment
                run.stop_requested_by = str(requested_by)[:128]
                run.updated_at = moment

            rows = list(
                (
                    await session.execute(
                        select(EasyWeekVoucherFollowupSend)
                        .where(EasyWeekVoucherFollowupSend.run_id == int(run_id))
                        .where(EasyWeekVoucherFollowupSend.status == FOLLOWUP_SEND_PLANNED)
                        .order_by(EasyWeekVoucherFollowupSend.id.asc())
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                row.status = FOLLOWUP_SEND_STOPPED
                row.reason_code = ids.TERMINALLY_STOPPED
                row.stopped_at = moment
                row.updated_at = moment
            # A run that has nothing left to do is finished here, so a stopped
            # run never sits in ``queued`` waiting for an executor that would
            # only find tombstones. A RUNNING one is left to its executor, which
            # checks the stop before each claim and finishes the run itself.
            if run.status == FOLLOWUP_RUN_QUEUED:
                run.status = FOLLOWUP_RUN_STOPPED
                run.outcome_code = ids.RUN_STOPPED
                run.reason_codes = [ids.RUN_STOPPED]
                run.finished_at = moment
                run.lease_owner = None
                run.lease_expires_at = None
            await session.flush()
            return StopOutcome(
                True,
                None,
                run_id=int(run_id),
                tombstoned=len(rows),
                already_stopped=already,
            )


# ---------------------------------------------------------------------------
# The claim — the attempt boundary
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClaimOutcome:
    """Whether this row's one attempt is now spent, or the reason it is not."""

    granted: bool
    reason: str | None = None
    send_id: int | None = None
    intent_uuid: str | None = None
    # True when the refusal is the run's terminal stop, which the executor reads
    # as "stop the whole run" rather than "skip this row".
    stopped: bool = False


async def claim_send(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    send_id: int,
    owner: str,
    address: str,
    easyweek_customer_uuid: uuid_module.UUID | None,
    expected_consent_fingerprint: str,
    sender_digest: str,
    recipient_digest: str,
    source_batch_id: int,
    now: datetime | None = None,
) -> ClaimOutcome:
    """Spend this original recipient's ONE extra attempt, before the POST.

    This is the attempt boundary. In one committed transaction it:

    1. re-proves the consent boundary under the consent ADDRESS locks, through
       :func:`...whatsapp_consent.recheck_consent_boundary` — so a durable
       refusal, a durable unresolved inbound message, an address set that moved
       or a consent decision that landed and was answered since the re-check all
       refuse here, and a STOP that commits before this point produces zero Meta
       requests;
    2. re-reads the run under its row lock and refuses if the operator stopped
       it or if this executor no longer holds the lease;
    3. re-reads the ORIGINAL batch's terminal stop, which forbids its follow-up;
    4. moves this row to ``claimed`` with ``attempt_count = 1``, a durable
       intent and the frozen route.

    The three partial unique indexes make step 4 atomic against everything this
    function cannot see: a second executor, a second run, another preview
    version, another template, a duplicate card. One of the two transactions
    wins and the other's INSERT or UPDATE violates the index.

    No network call happens inside it, and the caller is expected to commit this
    and only then make its one request.
    """
    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            # 1. The final consent boundary, with the address locks held.
            guard = await recheck_consent_boundary(
                session,
                phone=address,
                expected_fingerprint=expected_consent_fingerprint,
                easyweek_customer_uuid=easyweek_customer_uuid,
            )
            if not guard.allowed:
                return ClaimOutcome(False, ids.CONSENT_MOVED)

            # 2. The run, under its own row lock.
            run = (
                await session.execute(
                    select(EasyWeekVoucherFollowupRun)
                    .where(EasyWeekVoucherFollowupRun.id == int(run_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if run is None:
                return ClaimOutcome(False, ids.APPROVAL_UNKNOWN)
            if run.stop_requested_at is not None:
                return ClaimOutcome(False, ids.RUN_STOPPED, stopped=True)
            if run.status != FOLLOWUP_RUN_RUNNING or run.lease_owner != owner:
                return ClaimOutcome(False, ids.CLAIM_LEASE_LOST)

            # 3. The original mailing's terminal stop, read and not locked.
            if await production_ledger.terminal_stop_active_in_session(session, batch_id=int(source_batch_id)):
                return ClaimOutcome(False, ids.SOURCE_TERMINAL_STOP, stopped=True)

            # 4. The row itself.
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupSend)
                    .where(EasyWeekVoucherFollowupSend.id == int(send_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None or int(row.run_id) != int(run_id):
                return ClaimOutcome(False, ids.APPROVAL_UNKNOWN)
            if row.status != FOLLOWUP_SEND_PLANNED:
                # Somebody else already accounted for this attempt: another
                # executor, a stop, or this very row in an earlier pass.
                return ClaimOutcome(False, ids.ATTEMPT_ALREADY_USED)

            intent = uuid_module.uuid4()
            row.status = FOLLOWUP_SEND_CLAIMED
            row.attempt_count = 1
            row.intent_uuid = intent
            row.claimed_at = moment
            row.reason_code = None
            row.sender_digest = str(sender_digest)
            row.recipient_digest = str(recipient_digest)
            row.consent_fingerprint = guard.fingerprint
            row.updated_at = moment
            await session.flush()
            return ClaimOutcome(True, None, send_id=int(row.id), intent_uuid=str(intent))


async def record_send_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    send_id: int,
    status: str,
    reason_code: str | None,
    provider_message_id: str | None = None,
    http_status: int | None = None,
    now: datetime | None = None,
) -> str:
    """Write down what the one external request turned out to be.

    Separate from the claim, and deliberately so: the claim is the commit before
    the POST and this is the commit after it. A process that dies between them
    leaves a ``claimed`` row, which the next sweep turns into ``unknown`` —
    never back into something another pass could send.

    Takes the message identifier's advisory lock first when there is one, which
    is the same lock the callback path takes before it looks for the row. That
    is what lets a status callback that overtook this commit be bound to this
    row afterwards instead of being lost.

    Never downgrades. A row that a callback has already moved to ``delivered``
    or ``read`` keeps it: the answer to the POST is older news than a delivery
    receipt, and §46.3's precedence is that a success is not rolled back.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import callbacks as callbacks_module

    moment = now or utcnow()
    async with session_maker() as session:
        async with session.begin():
            if provider_message_id:
                await callbacks_module.lock_followup_message(session, str(provider_message_id))
            row = (
                await session.execute(
                    select(EasyWeekVoucherFollowupSend)
                    .where(EasyWeekVoucherFollowupSend.id == int(send_id))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return "missing"
            if row.status != FOLLOWUP_SEND_CLAIMED:
                # A callback already moved it, or a sweep already called it
                # unknown. Either is a later or stronger statement than this.
                return "superseded"
            row.status = status
            row.reason_code = reason_code
            row.provider_message_id = provider_message_id
            row.http_status = http_status
            row.answered_at = moment
            row.updated_at = moment
            await session.flush()
            if provider_message_id:
                # §46.3's early-callback binding, under the identifier lock this
                # transaction already holds: a status that arrived before this
                # commit is bound now rather than dropped.
                await callbacks_module.bind_unmatched_locked(
                    session,
                    send_row=row,
                    provider_message_id=str(provider_message_id),
                    now=moment,
                )
            return "recorded"


async def run_counters(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
) -> dict[str, int]:
    """How many rows of this run are in each state. Counts only, no identities."""
    async with session_maker() as session:
        rows = (
            await session.execute(
                select(EasyWeekVoucherFollowupSend.status, func.count())
                .where(EasyWeekVoucherFollowupSend.run_id == int(run_id))
                .group_by(EasyWeekVoucherFollowupSend.status)
            )
        ).all()
        return {str(status): int(count) for status, count in rows}
