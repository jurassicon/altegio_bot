"""Read-only post-recovery audit for EasyWeek lifecycle deliveries.

Answers one narrow question per DELIVERY, not per booking: did *this* captured
delivery produce *its own* lifecycle job?

Why a helper instead of SQL in the runbook
------------------------------------------
The identity of a lifecycle job is a SHA-256 over
``event_hint | booking_uuid | payload_hash``, computed by
:func:`easyweek_job_dedupe_key`. Restating that in SQL would fork the algorithm:
the day the key format changes, the audit would keep reporting green against a
formula nobody updated. So the audit imports the production function, and the
hint -> job_type mapping comes from the production maps as well.

What this deliberately does NOT do
----------------------------------
It never classifies *why* a delivery has no job. A missing job can be terminal
business suppression, ``booking-succeeded``, a post-cancel no-op, a replay, or a
genuine lost notification — and today's ``Record.raw`` cannot tell them apart,
because a later delivery may have overwritten the snapshot the decision was made
against. Such deliveries are reported as
``no_event_specific_job_unclassified``: a list to work through, never a verdict.

Strictly read-only: SELECT only, no UPDATE, no DELETE, no replay. It prints
event ids, technical statuses and counts — never payload, category values,
names, phones, emails, secrets or the dedupe key itself.
"""

from __future__ import annotations

import argparse
import asyncio
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_normalizer import _EVENT_HINT_MAP, easyweek_job_dedupe_key
from altegio_bot.easyweek_policy import EASYWEEK_LIFECYCLE_JOB_TYPES
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.workers.easyweek_inbox_worker import _ACTION_TO_JOB_TYPE


def expected_job_type(event_hint: str | None) -> str | None:
    """The lifecycle job type this hint must produce, or ``None``.

    ``None`` covers both "no job is expected" (``booking-succeeded`` is terminal
    with no Client/Record/Job side effect) and "not a recognised delivery".
    Composed from the production maps so the audit cannot drift from the worker:
    booking-created -> record_created, booking-updated and booking-rescheduled ->
    record_updated, booking-canceled -> record_canceled.
    """
    if not isinstance(event_hint, str):
        return None
    action = _EVENT_HINT_MAP.get(event_hint)
    if action is None:
        return None
    job_type = _ACTION_TO_JOB_TYPE.get(action)
    if job_type not in EASYWEEK_LIFECYCLE_JOB_TYPES:
        return None
    return job_type


@dataclass(frozen=True)
class DeliveryGroup:
    """Deliveries that are the same business fact, and share one expected job.

    A byte-identical Resend repeats ``event_hint``, ``booking_uuid`` and
    ``payload_hash``, so it lands here as extra ``event_ids`` on one group. One
    job for the whole group is successful deduplication, not a lost
    notification — which is exactly why the audit counts groups, not rows.
    """

    job_type: str
    expected_dedupe_key: str
    event_ids: tuple[int, ...]

    @property
    def is_resend(self) -> bool:
        return len(self.event_ids) > 1


@dataclass
class AuditReport:
    window_start: str
    window_end: str | None = None
    event_status_counts: dict[str, int] = field(default_factory=dict)
    lifecycle_groups: int = 0
    resend_groups: int = 0
    groups_with_exact_job: int = 0
    job_status_counts: dict[str, int] = field(default_factory=dict)
    no_event_specific_job_unclassified: tuple[int, ...] = ()
    non_lifecycle_event_ids: tuple[int, ...] = ()
    unmappable_event_ids: tuple[int, ...] = ()

    def as_safe_dict(self) -> dict[str, object]:
        """Only ids, technical statuses and counts leave this function."""
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "event_status_counts": dict(sorted(self.event_status_counts.items())),
            "lifecycle_delivery_groups": self.lifecycle_groups,
            "resend_groups": self.resend_groups,
            "groups_with_exact_job": self.groups_with_exact_job,
            "job_status_counts": dict(sorted(self.job_status_counts.items())),
            "no_event_specific_job_unclassified": list(self.no_event_specific_job_unclassified),
            "non_lifecycle_event_ids": list(self.non_lifecycle_event_ids),
            "unmappable_event_ids": list(self.unmappable_event_ids),
        }


def group_deliveries(rows: list[EasyWeekEvent]) -> tuple[list[DeliveryGroup], list[int], list[int]]:
    """Split captured rows into delivery groups, non-lifecycle and unmappable.

    Returns ``(groups, non_lifecycle_event_ids, unmappable_event_ids)``.
    """
    grouped: dict[tuple[str, str], list[int]] = {}
    non_lifecycle: list[int] = []
    unmappable: list[int] = []

    for row in rows:
        job_type = expected_job_type(row.event_hint)
        if job_type is None:
            # `booking-succeeded` and anything unrecognised: no job is owed.
            (non_lifecycle if row.event_hint == "booking-succeeded" else unmappable).append(int(row.id))
            continue
        if row.booking_uuid is None:
            # Without the canonical UUID no key can be computed for this row.
            unmappable.append(int(row.id))
            continue
        key = easyweek_job_dedupe_key(
            event_hint=row.event_hint or "",
            booking_uuid=row.booking_uuid,
            payload_hash=row.payload_hash,
            job_type=job_type,
        )
        grouped.setdefault((job_type, key), []).append(int(row.id))

    groups = [
        DeliveryGroup(job_type=job_type, expected_dedupe_key=key, event_ids=tuple(sorted(ids)))
        for (job_type, key), ids in grouped.items()
    ]
    groups.sort(key=lambda group: group.event_ids)
    return groups, sorted(non_lifecycle), sorted(unmappable)


async def audit_recovery(
    session: AsyncSession,
    *,
    since: datetime,
    until: datetime | None = None,
) -> AuditReport:
    """Read-only: what did the deliveries in this window actually produce?"""
    stmt = select(EasyWeekEvent).where(EasyWeekEvent.received_at >= since)
    if until is not None:
        stmt = stmt.where(EasyWeekEvent.received_at <= until)
    rows = list((await session.execute(stmt.order_by(EasyWeekEvent.id))).scalars().all())

    groups, non_lifecycle, unmappable = group_deliveries(rows)

    job_status_by_key: dict[str, str] = {}
    keys = [group.expected_dedupe_key for group in groups]
    if keys:
        job_rows = (
            await session.execute(
                select(MessageJob.dedupe_key, MessageJob.status)
                .where(MessageJob.provider == PROVIDER_EASYWEEK)
                .where(MessageJob.dedupe_key.in_(keys))
            )
        ).all()
        job_status_by_key = {str(key): str(status) for key, status in job_rows}

    unclassified: list[int] = []
    matched = 0
    # Every status counts, not just queued/processing: after recovery a job may
    # legitimately be done, retrying, canceled or failed by the normal delivery
    # deadline. Existence plus an explainable outcome is the invariant.
    statuses: Counter[str] = Counter()
    for group in groups:
        status = job_status_by_key.get(group.expected_dedupe_key)
        if status is None:
            unclassified.extend(group.event_ids)
            continue
        matched += 1
        statuses[status] += 1

    return AuditReport(
        window_start=since.isoformat(),
        window_end=until.isoformat() if until is not None else None,
        event_status_counts=dict(Counter(str(row.status) for row in rows)),
        lifecycle_groups=len(groups),
        resend_groups=sum(1 for group in groups if group.is_resend),
        groups_with_exact_job=matched,
        job_status_counts=dict(statuses),
        no_event_specific_job_unclassified=tuple(sorted(unclassified)),
        non_lifecycle_event_ids=tuple(non_lifecycle),
        unmappable_event_ids=tuple(unmappable),
    )


class SmokeVerificationError(RuntimeError):
    """Fail-closed: the smoke cannot be judged, so it is not green."""


# A provider attempt that actually landed. `sent` is the first status that
# proves Meta accepted the message; `delivered`/`read` are later stages of the
# same success and must not be treated as regressions.
OUTBOX_DELIVERED_STATUSES: Final[frozenset[str]] = frozenset({"sent", "delivered", "read"})
# Not finished yet: re-run the audit rather than judging the smoke.
OUTBOX_PENDING_STATUSES: Final[frozenset[str]] = frozenset({"queued", "sending"})


@dataclass(frozen=True)
class SmokeEventReport:
    """One controlled smoke delivery, in booleans / counts / technical ids."""

    event_id: int
    event_status: str
    newer_than_baseline: bool
    booking_first_seen_here: bool
    expected_job_type: str
    record_id: int | None
    record_company_id: int | None
    exact_job_ids: tuple[int, ...]
    job_statuses: tuple[str, ...]
    job_created_after_smoke_start: bool
    job_type_matches_event: bool
    job_company_matches_record: bool
    job_record_matches_booking: bool
    outbox_rows: int
    outbox_status_counts: dict[str, int]

    @property
    def outbox_delivery_proven(self) -> bool:
        """One Outbox row that actually reached the provider.

        A row COUNT proves the planner ran, nothing more: `queued`, `sending`,
        `failed` and `unknown` all count as one. Delivery is proven only by a
        status in :data:`OUTBOX_DELIVERED_STATUSES`, and only when the smoke
        produced the single row it was supposed to — several rows mean the
        chain did something unplanned and needs reading, not a green tick.
        """
        if self.outbox_rows != 1:
            return False
        return set(self.outbox_status_counts) <= OUTBOX_DELIVERED_STATUSES

    @property
    def outbox_outcome(self) -> str:
        """`proven`, `pending`, `not_green`, or `none` — never a bare count."""
        if self.outbox_rows == 0:
            return "none"
        if self.outbox_delivery_proven:
            return "proven"
        statuses = set(self.outbox_status_counts)
        if statuses <= (OUTBOX_PENDING_STATUSES | OUTBOX_DELIVERED_STATUSES) and self.outbox_rows == 1:
            return "pending"
        return "not_green"

    def as_safe_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "event_status": self.event_status,
            "newer_than_baseline": self.newer_than_baseline,
            "booking_first_seen_here": self.booking_first_seen_here,
            "expected_job_type": self.expected_job_type,
            "record_id": self.record_id,
            "record_company_id": self.record_company_id,
            "exact_jobs": len(self.exact_job_ids),
            "exact_job_ids": list(self.exact_job_ids),
            "job_statuses": list(self.job_statuses),
            "job_created_after_smoke_start": self.job_created_after_smoke_start,
            "job_type_matches_event": self.job_type_matches_event,
            "job_company_matches_record": self.job_company_matches_record,
            "job_record_matches_booking": self.job_record_matches_booking,
            "outbox_rows": self.outbox_rows,
            "outbox_status_counts": dict(sorted(self.outbox_status_counts.items())),
            "outbox_delivery_proven": self.outbox_delivery_proven,
            "outbox_outcome": self.outbox_outcome,
        }


async def verify_controlled_smoke(
    session: AsyncSession,
    *,
    event_ids: list[int],
    baseline_event_id: int,
    smoke_start: datetime,
) -> dict[str, object]:
    """Read-only proof that the pipeline created a job *now*, for a *new* booking.

    A byte-identical Resend cannot serve as this proof: it reproduces the same
    hint, booking uuid and payload hash, hence the same expected key, so a job
    created long before the outage answers for it. That is correct dedup
    behaviour historically (see :func:`audit_recovery`) and useless as a
    positive smoke. So freshness is asserted on two independent axes here — the
    event is newer than the pre-smoke baseline and is the FIRST event ever seen
    for its booking, and the job itself was created after ``smoke_start``.

    Fail-closed: a missing event, an unrecognised hint or a NULL booking uuid
    raises instead of returning a green-looking report.
    """
    if not event_ids:
        raise SmokeVerificationError("no smoke event ids given")

    reports: list[SmokeEventReport] = []
    bookings: set[uuid.UUID] = set()

    for event_id in event_ids:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalar_one_or_none()
        if event is None:
            raise SmokeVerificationError(f"event {event_id} not found")
        if event.booking_uuid is None:
            raise SmokeVerificationError(f"event {event_id} has no booking uuid")
        job_type = expected_job_type(event.event_hint)
        if job_type is None:
            raise SmokeVerificationError(f"event {event_id} carries no lifecycle hint")

        bookings.add(event.booking_uuid)

        # Freshness axis 1: nothing for this booking predates the smoke.
        earliest_id = (
            await session.execute(
                select(EasyWeekEvent.id)
                .where(EasyWeekEvent.booking_uuid == event.booking_uuid)
                .order_by(EasyWeekEvent.id)
                .limit(1)
            )
        ).scalar_one()

        record = (
            await session.execute(
                select(Record)
                .where(Record.provider == PROVIDER_EASYWEEK)
                .where(Record.easyweek_booking_uuid == event.booking_uuid)
            )
        ).scalar_one_or_none()

        key = easyweek_job_dedupe_key(
            event_hint=event.event_hint or "",
            booking_uuid=event.booking_uuid,
            payload_hash=event.payload_hash,
            job_type=job_type,
        )
        jobs = list(
            (
                await session.execute(
                    select(MessageJob)
                    .where(MessageJob.provider == PROVIDER_EASYWEEK)
                    .where(MessageJob.dedupe_key == key)
                )
            )
            .scalars()
            .all()
        )

        # Status, not just a count: a row exists in `queued`, `failed` and
        # `sent` alike, and only the last one says anything was delivered.
        outbox_status_counts: dict[str, int] = {}
        if jobs:
            status_rows = (
                await session.execute(
                    select(OutboxMessage.status, func.count(OutboxMessage.id))
                    .where(OutboxMessage.job_id.in_([job.id for job in jobs]))
                    .group_by(OutboxMessage.status)
                )
            ).all()
            outbox_status_counts = {str(status): int(count) for status, count in status_rows}
        outbox_rows = sum(outbox_status_counts.values())

        reports.append(
            SmokeEventReport(
                event_id=int(event.id),
                event_status=str(event.status),
                newer_than_baseline=int(event.id) > baseline_event_id,
                booking_first_seen_here=int(earliest_id) == int(event.id),
                expected_job_type=job_type,
                record_id=int(record.id) if record is not None else None,
                record_company_id=int(record.company_id) if record is not None else None,
                exact_job_ids=tuple(sorted(int(job.id) for job in jobs)),
                job_statuses=tuple(sorted(str(job.status) for job in jobs)),
                # Freshness axis 2: the job itself is new, not a survivor.
                job_created_after_smoke_start=bool(jobs)
                and all(job.created_at is not None and job.created_at >= smoke_start for job in jobs),
                job_type_matches_event=all(str(job.job_type) == job_type for job in jobs),
                job_company_matches_record=bool(jobs)
                and record is not None
                and all(int(job.company_id) == int(record.company_id) for job in jobs),
                job_record_matches_booking=bool(jobs)
                and record is not None
                and all(job.record_id is not None and int(job.record_id) == int(record.id) for job in jobs),
                outbox_rows=outbox_rows,
                outbox_status_counts=outbox_status_counts,
            )
        )

    return {
        "baseline_event_id": baseline_event_id,
        "smoke_start": smoke_start.isoformat(),
        "distinct_bookings": len(bookings),
        "events": [report.as_safe_dict() for report in reports],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only EasyWeek post-recovery delivery audit.")
    parser.add_argument("--since", default=None, help="ISO-8601 start of the window, e.g. 2026-08-12T09:00:00+00:00")
    parser.add_argument("--until", default=None, help="Optional ISO-8601 end of the window.")
    parser.add_argument(
        "--smoke-event-id",
        type=int,
        action="append",
        default=[],
        dest="smoke_event_ids",
        help="Controlled-smoke mode: repeat once per smoke event id.",
    )
    parser.add_argument("--baseline-event-id", type=int, default=None, help="MAX(easyweek_events.id) before the smoke.")
    parser.add_argument("--smoke-start", default=None, help="ISO-8601 UTC instant recorded before the smoke bookings.")

    args = parser.parse_args(argv)
    if args.smoke_event_ids:
        if args.baseline_event_id is None or args.smoke_start is None:
            parser.error("--smoke-event-id requires --baseline-event-id and --smoke-start")
    elif args.since is None:
        parser.error("either --since or --smoke-event-id is required")
    return args


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    async with SessionLocal() as session:
        if args.smoke_event_ids:
            payload: dict[str, object] = await verify_controlled_smoke(
                session,
                event_ids=args.smoke_event_ids,
                baseline_event_id=args.baseline_event_id,
                smoke_start=datetime.fromisoformat(args.smoke_start),
            )
        else:
            report = await audit_recovery(
                session,
                since=datetime.fromisoformat(args.since),
                until=datetime.fromisoformat(args.until) if args.until else None,
            )
            payload = report.as_safe_dict()

    print(payload)


if __name__ == "__main__":
    asyncio.run(main())
