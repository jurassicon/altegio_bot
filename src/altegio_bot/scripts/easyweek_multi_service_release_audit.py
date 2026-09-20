"""§38.9: the FULL release-set audit that must precede either send-fence opening.

The structural preflight answers a different question. It starts from ACTIVE,
FUTURE records whose ``services_count`` is 2 and looks at the jobs those
records own. ``EASYWEEK_MULTI_SERVICE_SEND_ENABLED`` is not scoped that way:
it is a global switch, and opening it releases every due EasyWeek
lifecycle/reminder job whose payload carries the canonical pair digest —
including jobs whose record has since been deleted, whose appointment has
already started, or which the structural selector never looked at.

So this command starts from the JOBS. It selects every open pair job of the
supported types regardless of ``Record.is_deleted``, ``Record.starts_at`` or
any record-side selector, and it answers, for each one, what actually happens
when it is released.

The answer is not a second implementation of the send path. Every verdict here
comes from the functions the outbox worker itself calls — the fence and canary
resolver, the past-record rule, the reminder staleness rule, the retry
deadline, the pair-snapshot resolver, the category policy and the deleted-record
allowlist. A projection that judged any of this on its own would bless a queue
the worker then refuses, or bless one it does not.

Strictly read-only and DB-only. It selects; it writes no job, record, outbox
row or event, never commits, and makes no EasyWeek, Meta or Chatwoot call at
all. Because it makes no live call, a ``*_provider_candidate`` is an UPPER
BOUND: a version 2 job additionally faces a live re-proof at send time, which
can still refuse it. The audit therefore never under-states what may go out.

Output is counts, stable reason codes and internal ``message_jobs.id`` values.
No booking uuid, no customer name, phone, e-mail, service text, price, URL or
API body ever reaches stdout: this output is read in a terminal and pasted into
tickets.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_JOB_DIGEST_KEY,
    MULTI_SERVICE_SNAPSHOT_MISSING,
    ServiceEligibilityPurpose,
    evaluate_service_eligibility,
    resolve_effective_multi_service_snapshot,
)
from altegio_bot.easyweek_multi_service_rollout import (
    MULTI_SERVICE_CANARY_JOB_MISMATCH,
    MULTI_SERVICE_CANARY_JOB_NOT_FOUND,
    RolloutPhase,
    multi_service_canary,
    multi_service_configuration_error,
)
from altegio_bot.easyweek_policy import EASYWEEK_LIFECYCLE_JOB_TYPES, EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageJob, OutboxMessage, Record
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

DEFAULT_LIMIT: Final = 500
# Internal job ids are technical, not customer data, and the whole point of the
# inventory is that an operator can approve it item by item. It is still
# bounded: a release set larger than this is not reviewable in a terminal and
# is reported as incomplete rather than silently clipped.
MAX_REPORTED_IDS: Final = 100

SUPPORTED_JOB_TYPES: Final = EASYWEEK_LIFECYCLE_JOB_TYPES | EASYWEEK_REMINDER_JOB_TYPES
OPEN_JOB_STATUSES: Final = ("queued", "processing")
NON_TERMINAL_OUTBOX_STATUSES: Final = ("queued", "sending")

# --- classifications --------------------------------------------------------
#
# Exactly one per job, assigned in the order the runtime itself decides things.

# The row is not in a stable auditable state: another worker holds it, or a
# previous pass left it claimed. Nothing else about it can be projected.
PROCESSING_OR_LOCKED: Final = "processing_or_locked"
# This job's record already has a non-terminal outbox row, so a message for it
# may be in flight. Reported before any projection rather than after it.
NONTERMINAL_OUTBOX_PRESENT: Final = "nonterminal_outbox_present"
# The audit cannot state what this job does: no record, an unowned branch, an
# unresolvable pair snapshot, or a category configuration the worker would
# defer on. Never green.
UNSAFE_OR_UNPROVEN: Final = "unsafe_or_unproven"
# The worker terminalises it locally, with no attempt and no external call:
# a past appointment, a stale reminder, an expired deadline, a deleted record
# this job type is not about, or a category the allowlist does not permit.
LOCAL_CANCEL_OR_NOOP: Final = "local_cancel_or_noop"
# It would reach the provider, but its `run_at` has not arrived. It is part of
# the bulk inventory: it fires on schedule once the fence is open.
FUTURE_NOT_DUE: Final = "future_not_due"
# Due, provable, and held only by the global send fence.
HELD_BY_SEND_FENCE: Final = "held_by_send_fence"
# Due, provable, the fence is open — and the canary names a different job.
HELD_BY_CANARY: Final = "held_by_canary"
# The one job the canary names, which will reach the provider.
SELECTED_CANARY_PROVIDER_CANDIDATE: Final = "selected_canary_provider_candidate"
# Due, provable, no restriction in force: it reaches the provider now.
BULK_PROVIDER_CANDIDATE: Final = "bulk_provider_candidate"

# Every classification that means "this job reaches the provider once it is
# released and due". The four are one set on purpose: which of them a job gets
# depends only on today's configuration, not on the job.
_RELEASABLE: Final = (
    HELD_BY_SEND_FENCE,
    HELD_BY_CANARY,
    SELECTED_CANARY_PROVIDER_CANDIDATE,
    BULK_PROVIDER_CANDIDATE,
)

CLASSIFICATIONS: Final = (
    PROCESSING_OR_LOCKED,
    NONTERMINAL_OUTBOX_PRESENT,
    UNSAFE_OR_UNPROVEN,
    LOCAL_CANCEL_OR_NOOP,
    FUTURE_NOT_DUE,
    *_RELEASABLE,
)

# --- stable reason codes the classification above is justified by ------------
REASON_RECORD_MISSING: Final = "record_missing"
REASON_BRANCH_NOT_IN_REGISTRY: Final = "branch_not_in_registry"
REASON_RECORD_IN_PAST: Final = "record_starts_at_in_past"
REASON_REMINDER_STALE: Final = "reminder_stale_after_reschedule"
REASON_DEADLINE_EXPIRED: Final = "deadline_expired"
REASON_RECORD_DELETED: Final = "record_deleted"
REASON_CATEGORY_CONFIG: Final = "category_configuration_unavailable"
REASON_PAYLOAD_MALFORMED: Final = "job_payload_malformed"
# The inventory did not fit in the report, so no operator approved it.
REASON_INVENTORY_INCOMPLETE: Final = "release_inventory_incomplete"


@dataclass
class ReleaseAuditReport:
    """Counts, stable reason codes and internal job ids — nothing else."""

    # The effective configuration this report is a statement about. Set when
    # the configuration is not the one the requested phase describes.
    config_error: str | None = None
    phase: str = RolloutPhase.PRE_OPEN.value
    open_pair_jobs: int = 0
    classified: int = 0
    truncated: bool = False
    nonterminal_outbox_rows: int = 0
    canary_job_id: int | None = None
    intended_canary_job_id: int | None = None
    canary_error: str | None = None
    classifications: Counter[str] = field(default_factory=Counter)
    reasons: Counter[str] = field(default_factory=Counter)
    provider_candidate_job_ids: list[int] = field(default_factory=list)
    future_provider_candidate_job_ids: list[int] = field(default_factory=list)

    @property
    def release_set_size(self) -> int:
        """Every job that reaches the provider once the bulk fence is open."""
        return len(self.provider_candidate_job_ids) + len(self.future_provider_candidate_job_ids)

    @property
    def inventory_complete(self) -> bool:
        """The whole release set fits in the report, so it can be approved."""
        return (
            len(self.provider_candidate_job_ids) <= MAX_REPORTED_IDS
            and len(self.future_provider_candidate_job_ids) <= MAX_REPORTED_IDS
        )

    @property
    def audit_sound(self) -> bool:
        """The audit itself is trustworthy, whatever it then concludes."""
        if self.config_error is not None:
            return False
        if self.truncated or not self.inventory_complete:
            return False
        if self.classifications.get(UNSAFE_OR_UNPROVEN, 0):
            return False
        if self.classifications.get(PROCESSING_OR_LOCKED, 0):
            return False
        if self.classifications.get(NONTERMINAL_OUTBOX_PRESENT, 0):
            return False
        return True

    @property
    def canary_ready(self) -> bool:
        """Exactly one named job may be released, and it is in the release set.

        Deliberately narrow. A canary is a statement about ONE job, so the
        report is green only when the operator named that job and this audit
        found it among the jobs the fence would actually release. "No problems
        found" over a set nobody named is how a bulk send gets called a canary.
        """
        if not self.audit_sound or self.canary_error is not None:
            return False
        return self.intended_canary_job_id is not None

    @property
    def bulk_ready(self) -> bool:
        """The complete release set is known, and no restriction is in force."""
        if not self.audit_sound:
            return False
        if self.canary_job_id is not None:
            return False
        return self.release_set_size > 0

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "read_only": True,
            # Stated rather than implied by the absence of writes: an operator
            # reading this in a ticket must see that running it authorised no
            # message and changed no flag.
            "send_authorized": False,
            "config_changed": False,
            "phase": self.phase,
            "config_error": self.config_error,
            "open_pair_jobs": self.open_pair_jobs,
            "classified": self.classified,
            "truncated": self.truncated,
            "inventory_complete": self.inventory_complete,
            "nonterminal_outbox_rows": self.nonterminal_outbox_rows,
            "canary_job_id": self.canary_job_id,
            "intended_canary_job_id": self.intended_canary_job_id,
            "canary_error": self.canary_error,
            "classifications": {name: self.classifications.get(name, 0) for name in CLASSIFICATIONS},
            "reasons": dict(sorted(self.reasons.items())),
            "release_set_size": self.release_set_size,
            "provider_candidate_job_ids": sorted(self.provider_candidate_job_ids)[:MAX_REPORTED_IDS],
            "future_provider_candidate_job_ids": sorted(self.future_provider_candidate_job_ids)[:MAX_REPORTED_IDS],
            "audit_sound": self.audit_sound,
            "canary_ready": self.canary_ready,
            "bulk_ready": self.bulk_ready,
        }


async def select_open_pair_jobs(session: AsyncSession, *, limit: int) -> tuple[list[MessageJob], bool]:
    """Every open EasyWeek pair job, with no record-side filter whatsoever.

    There is deliberately no join to ``Record``: the global send fence does not
    consult ``is_deleted`` or ``starts_at`` when it releases a queue, so an
    audit that did would describe a smaller set than the one being released.

    Provider, job type and the presence of the canonical pair digest are the
    whole predicate — exactly the three facts the claim-time fence uses. One
    extra row is fetched purely to detect truncation: a bounded look at a
    longer queue must be reported as bounded, not as a clean bill of health.
    """
    stmt = (
        select(MessageJob)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.job_type.in_(sorted(SUPPORTED_JOB_TYPES)))
        .where(MessageJob.status.in_(OPEN_JOB_STATUSES))
        .where(MessageJob.payload.op("?")(MULTI_SERVICE_JOB_DIGEST_KEY))
        .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
        .limit(limit + 1)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return rows[:limit], len(rows) > limit


async def _records_by_id(session: AsyncSession, jobs: list[MessageJob]) -> dict[int, Record]:
    """Load every referenced record, deleted and past ones included."""
    record_ids = sorted({job.record_id for job in jobs if job.record_id is not None})
    if not record_ids:
        return {}
    stmt = select(Record).where(Record.id.in_(record_ids))
    return {row.id: row for row in (await session.execute(stmt)).scalars()}


async def _nonterminal_outbox_record_ids(session: AsyncSession, jobs: list[MessageJob]) -> tuple[set[int], int]:
    record_ids = sorted({job.record_id for job in jobs if job.record_id is not None})
    if not record_ids:
        return set(), 0
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.record_id.in_(record_ids))
        .where(OutboxMessage.status.in_(NON_TERMINAL_OUTBOX_STATUSES))
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return {row.record_id for row in rows if row.record_id is not None}, len(rows)


def _classify(
    job: MessageJob,
    *,
    record: Record | None,
    outbox_blocked: bool,
    now: datetime,
) -> tuple[str, str | None]:
    """One job, one classification, in the order the runtime decides things.

    Imported runtime helpers do the deciding. The local imports keep this
    module free of a cycle with the worker, exactly as the reminder preflight
    already does.
    """
    from altegio_bot.workers.outbox_worker import (
        DELETED_RECORD_ALLOWED_JOB_TYPES,
        _check_reminder_stale,
        _deadline_passed_for_send,
        _easyweek_multi_service_fence_reason,
        _easyweek_owned_branch,
        _record_is_in_past,
    )

    if job.status != "queued" or job.locked_at is not None:
        return PROCESSING_OR_LOCKED, None
    if outbox_blocked:
        return NONTERMINAL_OUTBOX_PRESENT, None

    payload = job.payload if isinstance(job.payload, Mapping) else None
    if payload is None:
        return UNSAFE_OR_UNPROVEN, REASON_PAYLOAD_MALFORMED
    if record is None:
        return UNSAFE_OR_UNPROVEN, REASON_RECORD_MISSING

    # Local terminal outcomes the worker reaches BEFORE it resolves the pair.
    if _record_is_in_past(record, job_type=job.job_type):
        return LOCAL_CANCEL_OR_NOOP, REASON_RECORD_IN_PAST
    if job.job_type in EASYWEEK_REMINDER_JOB_TYPES:
        stale, _stale_reason = _check_reminder_stale(job, record)
        if stale:
            return LOCAL_CANCEL_OR_NOOP, REASON_REMINDER_STALE
    if _deadline_passed_for_send(job, record):
        return LOCAL_CANCEL_OR_NOOP, REASON_DEADLINE_EXPIRED

    owned_location, _profile, owned_error = _easyweek_owned_branch(job.company_id)
    if owned_error is not None or owned_location is None:
        # The text of the runtime error names a company id; only the stable
        # code is kept, and the id is already reported as a number elsewhere.
        return UNSAFE_OR_UNPROVEN, REASON_BRANCH_NOT_IN_REGISTRY

    effective, pair_error = resolve_effective_multi_service_snapshot(
        record_raw=record.raw,
        job_payload=job.payload,
        record_total_cost=record.total_cost,
        expected_booking_uuid=record.easyweek_booking_uuid,
        expected_location_uuid=owned_location.location_uuid,
    )
    if pair_error is not None or effective is None:
        return UNSAFE_OR_UNPROVEN, pair_error or MULTI_SERVICE_SNAPSHOT_MISSING

    eligibility = evaluate_service_eligibility(
        record_raw=record.raw,
        allowed_categories_raw=settings.easyweek_allowed_service_categories,
        purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
        effective_multi_service_snapshot=effective,
    )
    if not eligibility.allowed:
        if eligibility.recoverable_configuration:
            # The worker defers rather than cancels, so the job neither goes
            # out nor goes away — an outcome no rollout should be opened on.
            return UNSAFE_OR_UNPROVEN, eligibility.reason or REASON_CATEGORY_CONFIG
        return LOCAL_CANCEL_OR_NOOP, eligibility.reason

    if bool(getattr(record, "is_deleted", False)) and job.job_type not in DELETED_RECORD_ALLOWED_JOB_TYPES:
        return LOCAL_CANCEL_OR_NOOP, REASON_RECORD_DELETED

    # From here the job WOULD reach the provider. Only timing and today's
    # configuration decide which of the releasable classes it gets.
    if job.run_at is not None and job.run_at > now:
        return FUTURE_NOT_DUE, None

    if _easyweek_multi_service_fence_reason() is not None:
        return HELD_BY_SEND_FENCE, None
    canary = multi_service_canary()
    if canary.restricted:
        if job.id == canary.job_id:
            return SELECTED_CANARY_PROVIDER_CANDIDATE, None
        return HELD_BY_CANARY, None
    return BULK_PROVIDER_CANDIDATE, None


async def run_release_audit(
    session: AsyncSession,
    *,
    limit: int = DEFAULT_LIMIT,
    phase: RolloutPhase = RolloutPhase.PRE_OPEN,
    intended_canary_job_id: int | None = None,
) -> ReleaseAuditReport:
    """Audit the whole pair release set without mutating or sending anything."""
    report = ReleaseAuditReport(phase=phase.value)
    report.config_error = multi_service_configuration_error(phase)

    canary = multi_service_canary()
    report.canary_job_id = canary.job_id if canary.restricted else None

    jobs, truncated = await select_open_pair_jobs(session, limit=limit)
    report.open_pair_jobs = len(jobs)
    report.truncated = truncated

    records = await _records_by_id(session, jobs)
    blocked_record_ids, outbox_rows = await _nonterminal_outbox_record_ids(session, jobs)
    report.nonterminal_outbox_rows = outbox_rows

    now = utcnow()
    for job in jobs:
        record = records.get(job.record_id) if job.record_id is not None else None
        outbox_blocked = job.record_id is not None and job.record_id in blocked_record_ids
        classification, reason = _classify(job, record=record, outbox_blocked=outbox_blocked, now=now)
        report.classified += 1
        report.classifications[classification] += 1
        if reason:
            report.reasons[reason] += 1
        if classification in _RELEASABLE:
            report.provider_candidate_job_ids.append(job.id)
        elif classification == FUTURE_NOT_DUE:
            report.future_provider_candidate_job_ids.append(job.id)

    if not report.inventory_complete:
        report.reasons[REASON_INVENTORY_INCOMPLETE] += 1

    report.canary_error = _canary_error(report, intended_canary_job_id=intended_canary_job_id)
    if report.canary_error is None and intended_canary_job_id is not None:
        report.intended_canary_job_id = intended_canary_job_id
    return report


def _canary_error(report: ReleaseAuditReport, *, intended_canary_job_id: int | None) -> str | None:
    """Is the named job one this audit actually found in the release set?

    Answered here rather than left to the operator's eyes: "release exactly
    job N" is worth nothing if N is not a job the fence would release, and the
    place to learn that is before the fence opens, not from a silent send.
    """
    if intended_canary_job_id is None:
        return None
    known = set(report.provider_candidate_job_ids) | set(report.future_provider_candidate_job_ids)
    if intended_canary_job_id not in known:
        return MULTI_SERVICE_CANARY_JOB_NOT_FOUND
    if report.canary_job_id is not None and report.canary_job_id != intended_canary_job_id:
        return MULTI_SERVICE_CANARY_JOB_MISMATCH
    return None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only audit of the FULL EasyWeek pair release set. Writes nothing, sends nothing.",
        allow_abbrev=False,
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--phase",
        choices=[item.value for item in RolloutPhase],
        default=RolloutPhase.PRE_OPEN.value,
        help="The configuration this report is meant to be a statement about.",
    )
    parser.add_argument(
        "--canary-job-id",
        type=int,
        default=None,
        help="The one job the operator intends to release. Checked against the audited release set.",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.canary_job_id is not None and args.canary_job_id < 1:
        parser.error("--canary-job-id must be a positive message_jobs.id")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    phase = RolloutPhase(args.phase)
    async with SessionLocal() as session:
        report = await run_release_audit(
            session,
            limit=args.limit,
            phase=phase,
            intended_canary_job_id=args.canary_job_id,
        )
        # Defence in depth: even an accidental dirty ORM object cannot become a
        # write when the context exits.
        await session.rollback()
    print(report.as_safe_dict())
    if args.canary_job_id is not None:
        return 0 if report.canary_ready else 1
    # Every other run exits on whether the AUDIT is trustworthy, not on
    # whether it recommends an opening. `bulk_ready` is reported and read by
    # the operator: a post-open audit legitimately finds an empty release set,
    # and exiting non-zero on "there is nothing left to send" would turn the
    # successful end of a rollout into an alarm.
    return 0 if report.audit_sound else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
