"""PR-8: prove the reminder queue is sendable, before opening the send fence.

``EASYWEEK_REMINDER_API_GUARD_ENABLED`` starts closed, so real reminder jobs
accumulate as ``queued`` without a single message going out. This command is
what earns the right to open it: it runs the SAME guard the outbox worker runs,
against the REAL open jobs production is holding, and answers one question —
would every one of them be provably sendable right now?

Deliberately not "check the last N records". A record is not a job. The thing
that will actually fire is a row in ``message_jobs``, with its own payload, its
own planned start instant and its own company; checking anything else would
prove something adjacent to the question and call it an answer.

Strictly read-only. It opens a session, selects, and calls ``GET /bookings``.
It writes no ``MessageJob``, no ``Record``, no ``RecordService``, no
``OutboxMessage``, no ``easyweek_events`` row; it never calls Meta, never calls
Chatwoot and never plans a reminder. The session is never committed.

Green is narrow on purpose:

* at least one candidate — an empty queue proves nothing, and a preflight that
  passes on nothing is how a fence gets opened before the queue exists;
* not truncated — a bounded look at a longer queue says nothing about the rest;
* every candidate ``PROVEN_CURRENT`` — one unverifiable reminder is one customer
  who may be told to show up for an appointment that moved.

Anything else exits non-zero.

Rate limit: EasyWeek allows 60 requests/minute. Calls are issued sequentially
with a small pause, which is both simpler than a concurrency pool and impossible
to get wrong.

Output is counts, reason codes and technical ids. No booking uuid, no URL, no
name, phone, e-mail, service text or price, no API body, no Bearer key and no
Workspace header ever reaches stdout: this output is read in a terminal and
pasted into tickets.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.easyweek_reminder_guard import (
    BookingReader,
    GuardOutcome,
    classify_client_error,
    verify_reminder_is_current,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageJob, Record

DEFAULT_LIMIT: Final = 200
# 60 requests/minute is the documented EasyWeek ceiling. One second between
# sequential calls stays under it without needing a token bucket.
DEFAULT_PAUSE_SEC: Final = 1.0
# Ids are for an operator to spot-check a row, not a data export.
MAX_REPORTED_IDS: Final = 25

# The statuses an open reminder can be in. `processing` is included because a
# job claimed by the worker in the same second is still an open reminder;
# excluding it would let a preflight miss exactly the job that is about to fire.
OPEN_STATUSES: Final = ("queued", "processing")


@dataclass
class PreflightReport:
    """Counts, reason codes and technical ids — never a value from a booking."""

    candidate_count: int = 0
    checked_count: int = 0
    truncated: bool = False
    outcomes: Counter = field(default_factory=Counter)
    unproven_job_ids: list[int] = field(default_factory=list)
    unproven_record_ids: list[int] = field(default_factory=list)
    company_ids: set[int] = field(default_factory=set)

    @property
    def ready(self) -> bool:
        """Green, and only for the narrow case that actually proves something."""
        if self.truncated:
            return False
        if self.candidate_count == 0:
            return False
        if self.checked_count != self.candidate_count:
            return False
        return self.outcomes.get(GuardOutcome.PROVEN_CURRENT.value, 0) == self.candidate_count

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "candidate_count": self.candidate_count,
            "checked_count": self.checked_count,
            "truncated": self.truncated,
            "outcomes": dict(sorted(self.outcomes.items())),
            "unproven_job_ids": sorted(self.unproven_job_ids)[:MAX_REPORTED_IDS],
            "unproven_record_ids": sorted(self.unproven_record_ids)[:MAX_REPORTED_IDS],
            "company_ids": sorted(self.company_ids)[:MAX_REPORTED_IDS],
            "ready": self.ready,
        }


async def select_open_reminder_jobs(session: AsyncSession, *, limit: int) -> tuple[list[MessageJob], bool]:
    """Open EasyWeek reminder jobs, and whether the queue was longer than *limit*.

    One extra row is fetched purely to detect truncation: a bounded look at a
    longer queue must be reported as bounded, not as a clean bill of health.
    """
    stmt = (
        select(MessageJob)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
        .where(MessageJob.status.in_(OPEN_STATUSES))
        .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
        .limit(limit + 1)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    truncated = len(rows) > limit
    return rows[:limit], truncated


async def run_preflight(
    session: AsyncSession,
    *,
    client: BookingReader,
    limit: int = DEFAULT_LIMIT,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> PreflightReport:
    """Check every open reminder job with the runtime guard. Writes nothing.

    ``client`` and ``sleep`` are injected so the tests drive a fake and never
    touch the live EasyWeek API or actually wait.
    """
    from altegio_bot.easyweek_locations import configured_easyweek_locations

    pause = sleep if sleep is not None else asyncio.sleep

    jobs, truncated = await select_open_reminder_jobs(session, limit=limit)
    report = PreflightReport(candidate_count=len(jobs), truncated=truncated)

    registry = configured_easyweek_locations()
    locations = registry.locations if registry.valid else {}

    for index, job in enumerate(jobs):
        if index:
            await pause(pause_sec)

        record = None
        if job.record_id is not None:
            record = (await session.execute(select(Record).where(Record.id == job.record_id))).scalars().one_or_none()

        try:
            result = await verify_reminder_is_current(
                job=job,
                record=record,
                location=locations.get(job.company_id),
                client=client,
            )
        except Exception as exc:  # noqa: BLE001 — mapped by class, text never kept
            result = classify_client_error(exc)

        report.checked_count += 1
        report.outcomes[result.outcome.value] += 1
        if job.company_id is not None:
            report.company_ids.add(job.company_id)
        if result.outcome is not GuardOutcome.PROVEN_CURRENT:
            report.unproven_job_ids.append(job.id)
            if job.record_id is not None:
                report.unproven_record_ids.append(job.record_id)

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only preflight for EasyWeek reminder jobs. Writes nothing, sends nothing."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum reminder jobs to check (default {DEFAULT_LIMIT}). A truncated queue is never green.",
    )
    parser.add_argument(
        "--pause-sec",
        type=float,
        default=DEFAULT_PAUSE_SEC,
        help=f"Pause between sequential API calls (default {DEFAULT_PAUSE_SEC}); EasyWeek allows 60/min.",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.pause_sec < 0:
        parser.error("--pause-sec must not be negative")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    client = EasyWeekClient()
    try:
        async with SessionLocal() as session:
            report = await run_preflight(
                session,
                client=client,
                limit=args.limit,
                pause_sec=args.pause_sec,
            )
    finally:
        await client.aclose()

    print(report.as_safe_dict())
    # Non-zero on anything short of "every open reminder is provably sendable".
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
