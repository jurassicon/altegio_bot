"""§38.9 step 58: prove what the bulk opening ACTUALLY sent, job by job.

The pre-open audit (``easyweek_multi_service_release_audit``) answers "what
will go out". It selects open jobs, so a job that finished — successfully or
not — simply leaves its result set. After the fence opens that makes it the
wrong instrument twice over:

* an empty open release set is not evidence of success. A provider send, a
  permanent Meta refusal, exhausted retries, a local cancel and a job that
  reached ``done`` without a single proven Outbox row all look identical from
  there: gone;
* immediately after the opening it reads as a red light. ``_lock_next_jobs``
  marks a whole batch ``processing`` in one committed transaction and only
  then works through it one job at a time, so ordinary healthy delivery is a
  handful of ``processing`` rows and a couple of ``sending`` Outbox rows.

So this command starts from the APPROVED IDS instead — the exact list the
operator signed off at step 56 — and checks each one's terminal outcome. It
never tries to reconstruct the past from the present.

Success is not ``job.status == 'done'``. It is a ``done`` job WITH an Outbox
row in the project's own ``SUCCESS_OUTBOX_STATUSES`` (``sent``/``delivered``/
``read``), imported rather than restated. An Outbox row left ``unknown`` is
indeterminate by the model's own contract — never a success, and never
something to retry automatically.

Strictly read-only and DB-only: it selects, never commits, changes no flag,
calls no Meta, Chatwoot or EasyWeek endpoint, and performs no rollback. When
it is not green it says exactly which internal job ids are not, and the
operator decides.

Output is counts, stable reason codes and internal ``message_jobs.id`` values.
No booking uuid, customer name, phone, e-mail, service text, price, body, URL
or API response ever reaches stdout.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_multi_service import MULTI_SERVICE_JOB_DIGEST_KEY
from altegio_bot.easyweek_multi_service_rollout import (
    JobIdListError,
    RolloutPhase,
    multi_service_configuration_error,
    parse_job_id_list,
)
from altegio_bot.easyweek_policy import EASYWEEK_LIFECYCLE_JOB_TYPES, EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageJob, OutboxMessage
from altegio_bot.utils import utcnow
from altegio_bot.workers.outbox_worker import SUCCESS_OUTBOX_STATUSES

PROG: Final = "easyweek_multi_service_release_verify"

SUPPORTED_JOB_TYPES: Final = EASYWEEK_LIFECYCLE_JOB_TYPES | EASYWEEK_REMINDER_JOB_TYPES

# The Outbox lifecycle, as the model itself documents it:
# queued -> sending -> sent/delivered/read | failed | canceled | unknown.
IN_FLIGHT_OUTBOX_STATUSES: Final = ("queued", "sending")
# "an attempt whose Meta outcome cannot be proven (crash/indeterminate) — it is
# never auto-retried and requires manual review". So: never a success here.
INDETERMINATE_OUTBOX_STATUS: Final = "unknown"

# How long a verification may wait for an in-flight batch to settle, and how
# often it looks. Bounded on both ends: a rollout step that can hang is a
# rollout step nobody will run under pressure.
DEFAULT_SETTLE_SEC: Final = 180
MAX_SETTLE_SEC: Final = 1800
DEFAULT_POLL_SEC: Final = 10.0
MIN_POLL_SEC: Final = 1.0
DEFAULT_LIMIT: Final = 500
MAX_REPORTED_IDS: Final = 100

# --- outcomes for an approved DUE job --------------------------------------
SUCCEEDED: Final = "succeeded"
IN_PROGRESS: Final = "in_progress"
RETRY_SCHEDULED: Final = "retry_scheduled"
FAILED: Final = "failed"
CANCELED_WITHOUT_PROVIDER_SEND: Final = "canceled_without_provider_send"
UNKNOWN_OR_INDETERMINATE: Final = "unknown_or_indeterminate"
MISSING: Final = "missing"
IDENTITY_MISMATCH: Final = "identity_mismatch"

DUE_OUTCOMES: Final = (
    SUCCEEDED,
    IN_PROGRESS,
    RETRY_SCHEDULED,
    FAILED,
    CANCELED_WITHOUT_PROVIDER_SEND,
    UNKNOWN_OR_INDETERMINATE,
    MISSING,
    IDENTITY_MISMATCH,
)
# Outcomes that may still change on their own. The settle window exists for
# exactly these two and for nothing else.
PENDING_OUTCOMES: Final = (IN_PROGRESS, RETRY_SCHEDULED)

# --- outcomes for an approved FUTURE job -----------------------------------
#
# "It has not gone out" is the expected result for a future job, but it is not
# the ONLY thing that can be true of one. A job can also be cancelled, fail,
# reach `done` with nothing behind it, or be claimed long before its `run_at`,
# and none of those is the state the inventory was approved in.

# Still queued, still scheduled, nothing attempted: exactly as approved.
FUTURE_PENDING: Final = "future_pending"
# Its `run_at` arrived and it went out afterwards, provably.
FUTURE_RELEASED_ON_SCHEDULE: Final = "future_released_on_schedule"
# Its `run_at` arrived DURING this verification and a worker has it now. Not
# accepted and not a failure: the settle window exists for exactly this.
FUTURE_MATURED_PENDING: Final = "future_matured_pending"
# A proven send that precedes the job's own `run_at`.
FUTURE_SENT_EARLY: Final = "future_sent_early"
# An indeterminate Meta outcome, or a send whose timing cannot be proven.
FUTURE_INDETERMINATE: Final = "future_indeterminate"
# Terminal or in-flight where the approval said "scheduled": failed, canceled,
# `done` with no proven Outbox row, claimed before `run_at`, or a status this
# code does not recognise.
FUTURE_UNEXPECTED_STATE: Final = "future_unexpected_state"

FUTURE_OUTCOMES: Final = (
    FUTURE_PENDING,
    FUTURE_RELEASED_ON_SCHEDULE,
    FUTURE_MATURED_PENDING,
    FUTURE_SENT_EARLY,
    FUTURE_INDETERMINATE,
    FUTURE_UNEXPECTED_STATE,
    MISSING,
    IDENTITY_MISMATCH,
)
FUTURE_ACCEPTED: Final = (FUTURE_PENDING, FUTURE_RELEASED_ON_SCHEDULE)
FUTURE_PENDING_OUTCOMES: Final = (FUTURE_MATURED_PENDING,)

# --- stable reason codes ----------------------------------------------------
REASON_JOB_ROW_MISSING: Final = "job_row_missing"
REASON_WRONG_PROVIDER: Final = "job_provider_not_easyweek"
REASON_WRONG_JOB_TYPE: Final = "job_type_not_pair_supported"
REASON_DIGEST_ABSENT: Final = "job_payload_without_pair_digest"
REASON_DONE_WITHOUT_PROVEN_SEND: Final = "done_without_proven_outbox_send"
REASON_JOB_FAILED: Final = "job_status_failed"
REASON_JOB_CANCELED: Final = "job_status_canceled"
REASON_OUTBOX_UNKNOWN: Final = "outbox_outcome_unknown"
REASON_SEND_CONTRADICTS_JOB: Final = "proven_send_contradicts_job_status"
REASON_UNRECOGNISED_JOB_STATUS: Final = "job_status_unrecognised"
REASON_SETTLE_TIMEOUT: Final = "settle_window_expired"
# A pair job that is NOT in the approved list reached the provider inside the
# rollout window. The producer was supposed to be paused; this is the check
# that says otherwise.
REASON_UNAPPROVED_SEND: Final = "unapproved_pair_provider_send"
REASON_EMPTY_DUE_SET: Final = "approved_due_set_empty"
REASON_EMPTY_INVENTORY: Final = "approved_inventory_empty"
REASON_SCAN_TRUNCATED: Final = "unapproved_scan_truncated"
# Future-specific codes. Kept apart from the due vocabulary on purpose: an
# operator reading "failed" must be able to tell a message that should have
# gone out from one that should not have gone out yet.
REASON_FUTURE_SEND_BEFORE_RUN_AT: Final = "future_send_before_run_at"
REASON_FUTURE_SEND_TIME_UNPROVABLE: Final = "future_send_time_unprovable"
REASON_FUTURE_CLAIMED_BEFORE_RUN_AT: Final = "future_claimed_before_run_at"
REASON_FUTURE_RUN_AT_MISSING: Final = "future_run_at_missing"


@dataclass
class ReleaseVerifyReport:
    """Counts, stable reason codes and internal job ids — nothing else."""

    opened_at: str = ""
    settle_sec: int = DEFAULT_SETTLE_SEC
    waited_sec: float = 0.0
    polls: int = 0
    settled: bool = False
    config_error: str | None = None

    approved_due: int = 0
    approved_future: int = 0
    # Set when the operator passed --allow-empty-due, so a zero due count in
    # this report is never something they have to guess the reason for.
    empty_due_allowed: bool = False
    due_outcomes: Counter[str] = field(default_factory=Counter)
    future_outcomes: Counter[str] = field(default_factory=Counter)
    reasons: Counter[str] = field(default_factory=Counter)

    succeeded_job_ids: list[int] = field(default_factory=list)
    pending_job_ids: list[int] = field(default_factory=list)
    unsuccessful_job_ids: list[int] = field(default_factory=list)
    future_problem_job_ids: list[int] = field(default_factory=list)
    # Approved future jobs whose `run_at` arrived during this verification.
    # Tracked separately from the due queue so a future job's own timing can
    # never be read as a due job's outcome.
    future_maturing_job_ids: list[int] = field(default_factory=list)
    unapproved_sent_job_ids: list[int] = field(default_factory=list)
    scan_truncated: bool = False

    @property
    def due_all_succeeded(self) -> bool:
        """Every approved due job provably reached the provider.

        Vacuously true for an empty due set — an empty set is admitted by
        ``inventory_proven`` below, under its own explicit conditions, not by
        quietly satisfying this one.
        """
        return self.due_outcomes.get(SUCCEEDED, 0) == self.approved_due

    @property
    def future_all_accepted(self) -> bool:
        return not self.future_problem_job_ids

    @property
    def inventory_proven(self) -> bool:
        """The approved inventory, whatever shape it has, is accounted for.

        After a successful canary the remaining inventory can legitimately be
        all-future: the one due job the canary released is terminal and gone,
        and what is left are reminders that fire on their own schedule. That
        rollout still has to be closable, so an empty due set is allowed —
        but only when the operator said so, and only when there is a non-empty
        future set to actually verify.

        An inventory with nothing in it at all is never proven. There would be
        no evidence in it, and "no evidence" is the one thing this command
        exists to stop being read as success.
        """
        if self.approved_due == 0 and self.approved_future == 0:
            return False
        if self.approved_due == 0 and not self.empty_due_allowed:
            return False
        return self.due_all_succeeded and self.future_all_accepted

    @property
    def pending(self) -> bool:
        """Something may still change by itself, so no verdict is final yet."""
        return bool(self.pending_job_ids) or bool(self.future_maturing_job_ids)

    @property
    def verified(self) -> bool:
        """Green, and only for the narrow case that actually proves delivery.

        The effective bulk configuration is part of it, and it carries most of
        the weight in the all-future case: with no due job to send, the
        absence of an immediate message proves nothing on its own — the fence
        could simply still be shut. ``config_error`` is what rules that out.
        """
        if self.config_error is not None:
            return False
        if self.pending or not self.settled:
            return False
        if self.scan_truncated or self.unapproved_sent_job_ids:
            return False
        return self.inventory_proven

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "read_only": True,
            # Stated rather than implied by the absence of writes: this command
            # authorises nothing, changes no flag and performs no rollback.
            "send_authorized": False,
            "config_changed": False,
            "rollback_performed": False,
            "opened_at": self.opened_at,
            "settle_sec": self.settle_sec,
            "waited_sec": round(self.waited_sec, 1),
            "polls": self.polls,
            "settled": self.settled,
            "config_error": self.config_error,
            "approved_due": self.approved_due,
            "approved_future": self.approved_future,
            "empty_due_allowed": self.empty_due_allowed,
            "inventory_proven": self.inventory_proven,
            "due_outcomes": {name: self.due_outcomes.get(name, 0) for name in DUE_OUTCOMES},
            "future_outcomes": {name: self.future_outcomes.get(name, 0) for name in FUTURE_OUTCOMES},
            "reasons": dict(sorted(self.reasons.items())),
            "succeeded_job_ids": sorted(self.succeeded_job_ids)[:MAX_REPORTED_IDS],
            "pending_job_ids": sorted(self.pending_job_ids)[:MAX_REPORTED_IDS],
            "unsuccessful_job_ids": sorted(self.unsuccessful_job_ids)[:MAX_REPORTED_IDS],
            "future_problem_job_ids": sorted(self.future_problem_job_ids)[:MAX_REPORTED_IDS],
            "future_maturing_job_ids": sorted(self.future_maturing_job_ids)[:MAX_REPORTED_IDS],
            "unapproved_sent_job_ids": sorted(self.unapproved_sent_job_ids)[:MAX_REPORTED_IDS],
            "scan_truncated": self.scan_truncated,
            "verified": self.verified,
        }


def _identity_reason(job: MessageJob) -> str | None:
    """Is this row the pair job the approval was about? One answer, three facts."""
    if getattr(job, "provider", None) != PROVIDER_EASYWEEK:
        return REASON_WRONG_PROVIDER
    if getattr(job, "job_type", None) not in SUPPORTED_JOB_TYPES:
        return REASON_WRONG_JOB_TYPE
    payload = job.payload if isinstance(job.payload, Mapping) else {}
    if MULTI_SERVICE_JOB_DIGEST_KEY not in payload:
        return REASON_DIGEST_ABSENT
    return None


def classify_due_job(job: MessageJob | None, outbox: list[OutboxMessage]) -> tuple[str, str | None]:
    """The terminal outcome of one approved due job, and why.

    Order matters, and it is ordered by what is hardest to argue with rather
    than by what is most convenient:

    1. an indeterminate Outbox row outranks everything — the model says its
       Meta outcome cannot be proven, so no other field may overrule it;
    2. a proven provider success outranks the job row, because the customer
       already has the message. It is only a green ``succeeded`` when the job
       agrees; a success under a failed, canceled or still-running job is a
       contradiction a human has to look at, not something to average out;
    3. only then do the in-flight and terminal job statuses decide.
    """
    if job is None:
        return MISSING, REASON_JOB_ROW_MISSING
    identity = _identity_reason(job)
    if identity is not None:
        return IDENTITY_MISMATCH, identity

    statuses = {getattr(row, "status", None) for row in outbox}
    if INDETERMINATE_OUTBOX_STATUS in statuses:
        return UNKNOWN_OR_INDETERMINATE, REASON_OUTBOX_UNKNOWN

    proven_send = bool(statuses & set(SUCCESS_OUTBOX_STATUSES))
    job_status = getattr(job, "status", None)
    if proven_send:
        if job_status == "done":
            return SUCCEEDED, None
        return UNKNOWN_OR_INDETERMINATE, REASON_SEND_CONTRADICTS_JOB

    if job_status == "processing" or getattr(job, "locked_at", None) is not None:
        return IN_PROGRESS, None
    if statuses & set(IN_FLIGHT_OUTBOX_STATUSES):
        return IN_PROGRESS, None
    if job_status == "queued":
        if (getattr(job, "attempts", 0) or 0) > 0:
            return RETRY_SCHEDULED, None
        return IN_PROGRESS, None
    if job_status == "done":
        # The job says it sent; the Outbox has no row that proves it did.
        return FAILED, REASON_DONE_WITHOUT_PROVEN_SEND
    if job_status == "failed":
        return FAILED, REASON_JOB_FAILED
    if job_status == "canceled":
        return CANCELED_WITHOUT_PROVIDER_SEND, REASON_JOB_CANCELED
    return UNKNOWN_OR_INDETERMINATE, REASON_UNRECOGNISED_JOB_STATUS


def classify_future_job(
    job: MessageJob | None,
    outbox: list[OutboxMessage],
    *,
    now: datetime,
) -> tuple[str, str | None]:
    """An approved future job must be exactly where the approval left it.

    "It has not gone out" is the expected result, but absence of a send is not
    the same as being in the approved state. A future job can equally be
    cancelled, fail, reach ``done`` with nothing behind it, carry an
    indeterminate Meta outcome, or be sitting claimed by a worker hours before
    its ``run_at`` — and a check that only looked for a success row would have
    called every one of those "pending" and gone green.

    So the expected state is stated positively: identity proven, still
    ``queued``, unclaimed, nothing attempted, and its scheduled instant still
    ahead. Everything else is named.

    The one genuinely open case is a job whose ``run_at`` arrives DURING the
    verification. It is neither wrong nor finished, so it gets its own pending
    outcome and the settle window decides.
    """
    if job is None:
        return MISSING, REASON_JOB_ROW_MISSING
    identity = _identity_reason(job)
    if identity is not None:
        return IDENTITY_MISMATCH, identity

    run_at = getattr(job, "run_at", None)
    if run_at is None:
        # Without its scheduled instant nothing below can be judged: "early"
        # and "on schedule" both stop meaning anything.
        return FUTURE_INDETERMINATE, REASON_FUTURE_RUN_AT_MISSING
    matured = run_at <= now

    statuses = {getattr(row, "status", None) for row in outbox}
    if INDETERMINATE_OUTBOX_STATUS in statuses:
        return FUTURE_INDETERMINATE, REASON_OUTBOX_UNKNOWN

    successes = [row for row in outbox if getattr(row, "status", None) in SUCCESS_OUTBOX_STATUSES]
    job_status = getattr(job, "status", None)
    if successes:
        earliest = min((row.sent_at for row in successes if row.sent_at is not None), default=None)
        if earliest is None:
            # A success with no `sent_at` cannot be placed relative to
            # `run_at`, so it can be neither cleared nor convicted here.
            return FUTURE_INDETERMINATE, REASON_FUTURE_SEND_TIME_UNPROVABLE
        if earliest < run_at:
            return FUTURE_SENT_EARLY, REASON_FUTURE_SEND_BEFORE_RUN_AT
        if job_status != "done":
            return FUTURE_INDETERMINATE, REASON_SEND_CONTRADICTS_JOB
        return FUTURE_RELEASED_ON_SCHEDULE, None

    # No proven send. From here only the job row and the clock decide.
    in_flight = bool(statuses & set(IN_FLIGHT_OUTBOX_STATUSES))
    claimed = job_status == "processing" or getattr(job, "locked_at", None) is not None

    if job_status == "queued":
        if matured:
            return FUTURE_MATURED_PENDING, None
        if claimed or in_flight:
            # Held for later, yet something is already working on it.
            return FUTURE_UNEXPECTED_STATE, REASON_FUTURE_CLAIMED_BEFORE_RUN_AT
        return FUTURE_PENDING, None
    if job_status == "processing":
        if matured:
            return FUTURE_MATURED_PENDING, None
        return FUTURE_UNEXPECTED_STATE, REASON_FUTURE_CLAIMED_BEFORE_RUN_AT
    if job_status == "done":
        return FUTURE_UNEXPECTED_STATE, REASON_DONE_WITHOUT_PROVEN_SEND
    if job_status == "failed":
        return FUTURE_UNEXPECTED_STATE, REASON_JOB_FAILED
    if job_status == "canceled":
        return FUTURE_UNEXPECTED_STATE, REASON_JOB_CANCELED
    return FUTURE_UNEXPECTED_STATE, REASON_UNRECOGNISED_JOB_STATUS


async def _jobs_by_id(session: AsyncSession, job_ids: list[int]) -> dict[int, MessageJob]:
    if not job_ids:
        return {}
    stmt = select(MessageJob).where(MessageJob.id.in_(job_ids))
    return {row.id: row for row in (await session.execute(stmt)).scalars()}


async def _outbox_by_job_id(session: AsyncSession, job_ids: list[int]) -> dict[int, list[OutboxMessage]]:
    if not job_ids:
        return {}
    stmt = select(OutboxMessage).where(OutboxMessage.job_id.in_(job_ids)).order_by(OutboxMessage.id.asc())
    result: dict[int, list[OutboxMessage]] = {}
    for row in (await session.execute(stmt)).scalars():
        if row.job_id is not None:
            result.setdefault(row.job_id, []).append(row)
    return result


async def find_unapproved_pair_sends(
    session: AsyncSession,
    *,
    opened_at: datetime,
    approved: set[int],
    limit: int = DEFAULT_LIMIT,
) -> tuple[list[int], bool]:
    """EasyWeek pair jobs that reached the provider in the window, unapproved.

    The window is bounded by ``opened_at``, a UTC instant the operator records
    BEFORE the bulk opening — a technical marker, never a customer value. With
    the inbox producer paused, the approved inventory is the complete set of
    pair jobs that may send, so a proven success outside it is exactly the
    event the pause was meant to make impossible.
    """
    stmt = (
        select(OutboxMessage.job_id)
        .join(MessageJob, MessageJob.id == OutboxMessage.job_id)
        .where(OutboxMessage.status.in_(SUCCESS_OUTBOX_STATUSES))
        .where(OutboxMessage.sent_at.is_not(None))
        .where(OutboxMessage.sent_at >= opened_at)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.job_type.in_(sorted(SUPPORTED_JOB_TYPES)))
        .where(MessageJob.payload.op("?")(MULTI_SERVICE_JOB_DIGEST_KEY))
        .order_by(OutboxMessage.job_id.asc())
        .limit(limit + 1)
    )
    rows = [int(value) for value in (await session.execute(stmt)).scalars() if value is not None]
    truncated = len(rows) > limit
    found = sorted({job_id for job_id in rows[:limit] if job_id not in approved})
    return found, truncated


async def _collect_once(
    session: AsyncSession,
    *,
    due_ids: list[int],
    future_ids: list[int],
    opened_at: datetime,
    limit: int,
) -> ReleaseVerifyReport:
    report = ReleaseVerifyReport(
        opened_at=opened_at.isoformat(),
        approved_due=len(due_ids),
        approved_future=len(future_ids),
    )
    all_ids = due_ids + future_ids
    jobs = await _jobs_by_id(session, all_ids)
    outbox = await _outbox_by_job_id(session, all_ids)
    now = utcnow()

    for job_id in due_ids:
        outcome, reason = classify_due_job(jobs.get(job_id), outbox.get(job_id, []))
        report.due_outcomes[outcome] += 1
        if reason:
            report.reasons[reason] += 1
        if outcome == SUCCEEDED:
            report.succeeded_job_ids.append(job_id)
        elif outcome in PENDING_OUTCOMES:
            report.pending_job_ids.append(job_id)
        else:
            report.unsuccessful_job_ids.append(job_id)

    for job_id in future_ids:
        outcome, reason = classify_future_job(jobs.get(job_id), outbox.get(job_id, []), now=now)
        report.future_outcomes[outcome] += 1
        if reason:
            report.reasons[reason] += 1
        if outcome in FUTURE_PENDING_OUTCOMES:
            report.future_maturing_job_ids.append(job_id)
        elif outcome not in FUTURE_ACCEPTED:
            report.future_problem_job_ids.append(job_id)

    approved = set(all_ids)
    unapproved, truncated = await find_unapproved_pair_sends(
        session,
        opened_at=opened_at,
        approved=approved,
        limit=limit,
    )
    report.unapproved_sent_job_ids = unapproved
    report.scan_truncated = truncated
    if unapproved:
        report.reasons[REASON_UNAPPROVED_SEND] += len(unapproved)
    if truncated:
        report.reasons[REASON_SCAN_TRUNCATED] += 1
    return report


async def verify_release(
    session_factory: Any,
    *,
    due_job_ids: list[int],
    future_job_ids: list[int] | None = None,
    opened_at: datetime,
    settle_sec: int = DEFAULT_SETTLE_SEC,
    poll_sec: float = DEFAULT_POLL_SEC,
    limit: int = DEFAULT_LIMIT,
    allow_empty_due: bool = False,
    sleep: Any = None,
    now: Any = None,
) -> ReleaseVerifyReport:
    """Poll the approved ids until they settle, or until the window expires.

    Bounded by construction: the loop runs at most ``settle_sec / poll_sec``
    times plus one, and each pass opens and closes its own read-only session.
    A rollout step that could spin forever is a rollout step that gets killed
    mid-flight by the operator, which is worse than a red report.

    ``sleep`` and ``now`` are injected so the tests drive the window without
    waiting for it.
    """
    future_ids = list(future_job_ids or [])
    pause = sleep if sleep is not None else asyncio.sleep
    clock = now if now is not None else utcnow

    # The effective bulk configuration, from the SAME shared resolver both
    # preflights and the audit use — send fence open, canary unset, every
    # mandatory prerequisite on. It is checked BEFORE the wait, and a wrong
    # configuration ends the wait immediately: there is nothing to settle if
    # the fence the report is about is not the fence that is open.
    #
    # It matters most in the all-future case. With no due job to release, an
    # absent message is not evidence of anything — a shut fence looks exactly
    # the same — so this check is what the green verdict actually rests on.
    bulk_config_error = multi_service_configuration_error(RolloutPhase.BULK)
    effective_settle = 0 if bulk_config_error is not None else settle_sec

    started = clock()
    max_polls = max(1, int(effective_settle // max(poll_sec, MIN_POLL_SEC)) + 1)
    report: ReleaseVerifyReport | None = None

    for attempt in range(max_polls):
        async with session_factory() as session:
            report = await _collect_once(
                session,
                due_ids=due_job_ids,
                future_ids=future_ids,
                opened_at=opened_at,
                limit=limit,
            )
            # Defence in depth: even an accidental dirty ORM object cannot
            # become a write when the context exits.
            await session.rollback()
        report.polls = attempt + 1
        report.waited_sec = (clock() - started).total_seconds()
        if not report.pending:
            report.settled = True
            break
        if report.waited_sec >= effective_settle:
            break
        await pause(poll_sec)
    else:  # pragma: no cover - the loop always assigns `report` on its first pass
        pass

    assert report is not None
    report.settle_sec = settle_sec
    report.empty_due_allowed = allow_empty_due
    if report.pending and not report.settled:
        # The wait ended with work still moving. That is not a failure yet, and
        # the report must not call it one: it is an operator decision between
        # watching longer and closing the fence.
        report.reasons[REASON_SETTLE_TIMEOUT] += 1

    # Configuration first: a shut fence or a leftover canary explains every
    # other number in the report, so it is the reason worth naming.
    if bulk_config_error is not None:
        report.config_error = bulk_config_error
    elif not due_job_ids and not future_ids:
        # Nothing was approved, so nothing can be proven. `--allow-empty-due`
        # widens the shape of an inventory, never its emptiness.
        report.config_error = REASON_EMPTY_INVENTORY
    elif not due_job_ids and not allow_empty_due:
        report.config_error = REASON_EMPTY_DUE_SET
    return report


def _parse_opened_at(raw: str) -> datetime:
    text = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise JobIdListError("--opened-at must be an ISO-8601 instant with a timezone") from None
    if parsed.tzinfo is None:
        # A naive timestamp would silently be read in whatever zone the
        # container happens to use, and the window is the whole point.
        raise JobIdListError("--opened-at must carry an explicit timezone")
    parsed = parsed.astimezone(timezone.utc)
    if parsed > utcnow():
        # A marker in the future makes the scan window empty, which would hide
        # every unapproved send instead of reporting it.
        raise JobIdListError("--opened-at is in the future")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Read-only post-open verification of the approved EasyWeek pair release. Sends nothing.",
        allow_abbrev=False,
    )
    parser.add_argument("--due-job-id", dest="due", action="append", default=[], metavar="ID")
    parser.add_argument("--due-job-ids", dest="due", action="append", metavar="ID,ID,...")
    parser.add_argument("--future-job-id", dest="future", action="append", default=[], metavar="ID")
    parser.add_argument("--future-job-ids", dest="future", action="append", metavar="ID,ID,...")
    parser.add_argument("--opened-at", required=True, metavar="ISO8601")
    parser.add_argument("--settle-sec", type=int, default=DEFAULT_SETTLE_SEC)
    parser.add_argument("--poll-sec", type=float, default=DEFAULT_POLL_SEC)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--allow-empty-due",
        action="store_true",
        help="Explicitly accept an empty approved due set. Off by default.",
    )
    return parser


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if not 0 <= args.settle_sec <= MAX_SETTLE_SEC:
        parser.error(f"--settle-sec must be between 0 and {MAX_SETTLE_SEC}")
    if args.poll_sec < MIN_POLL_SEC:
        parser.error(f"--poll-sec must be at least {MIN_POLL_SEC}")
    try:
        args.due_ids = parse_job_id_list(args.due)
        args.future_ids = parse_job_id_list(args.future)
        args.opened_at_dt = _parse_opened_at(args.opened_at)
    except JobIdListError as exc:
        parser.error(str(exc))
    overlap = sorted(set(args.due_ids) & set(args.future_ids))
    if overlap:
        parser.error(f"job id in both the due and the future set: {overlap[0]}")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = await verify_release(
        SessionLocal,
        due_job_ids=args.due_ids,
        future_job_ids=args.future_ids,
        opened_at=args.opened_at_dt,
        settle_sec=args.settle_sec,
        poll_sec=args.poll_sec,
        limit=args.limit,
        allow_empty_due=bool(args.allow_empty_due),
    )
    print(report.as_safe_dict())
    return 0 if report.verified else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
