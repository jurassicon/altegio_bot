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
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_normalizer import _EVENT_HINT_MAP, easyweek_job_dedupe_key
from altegio_bot.easyweek_policy import EASYWEEK_LIFECYCLE_JOB_TYPES
from altegio_bot.models.models import PROVIDER_EASYWEEK, EasyWeekEvent, MessageJob
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


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only EasyWeek post-recovery delivery audit.")
    parser.add_argument("--since", required=True, help="ISO-8601 start of the window, e.g. 2026-08-12T09:00:00+00:00")
    parser.add_argument("--until", default=None, help="Optional ISO-8601 end of the window.")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    since = datetime.fromisoformat(args.since)
    until = datetime.fromisoformat(args.until) if args.until else None

    async with SessionLocal() as session:
        report = await audit_recovery(session, since=since, until=until)

    print(report.as_safe_dict())


if __name__ == "__main__":
    asyncio.run(main())
