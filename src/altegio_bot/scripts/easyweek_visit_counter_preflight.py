"""Read-only preflight for the PR-11 EasyWeek visit counter. Writes nothing.

One question, asked before ``EASYWEEK_VISIT_COUNTER_ENABLED`` is turned on:
**would the `booking-succeeded` deliveries production is actually receiving move
the counter?**

That is not a rhetorical question. PR-9 shipped a `review_url` validator against
a field EasyWeek turned out never to send, and nothing noticed until a live
booking produced no job at all (plan §18). ``visits_total`` is documented and
present in the captured production fixture, but "documented" is exactly what
``review_url`` was. So this reads real captured rows and proves the field with
the same normalizer the worker will use.

What it is NOT:

* not a backfill — a green report changes nothing, and past visits stay
  uncounted because no stored delivery proves them;
* not a re-queue — ``processed`` rows are read where they lie and their status
  is never touched;
* not a send fence — the counter sends nothing, so there is no queue to audit.

Green means: real candidates exist, none were cut off by the limit, and every
one of them carries a usable ``visits_total`` AND resolves to a Record and
Client whose provider, company, booking id and customer id all match. Anything
less is red, including "no candidates": a flag opened on the strength of an
empty sample is opened blind.

Output carries technical ids, providers, company ids, booleans, aggregated
reason codes and counts. Never a payload, phone, name, e-mail, ``body_raw``,
token or secret.

Usage::

    python -m altegio_bot.scripts.easyweek_visit_counter_preflight [--limit N]

Exit code 0 only when the report is ready.
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
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_normalizer import NormalizationError, normalize_succeeded_visit_event
from altegio_bot.models.models import PROVIDER_EASYWEEK, Client, EasyWeekEvent, Record

DEFAULT_LIMIT: Final = 200
MAX_REPORTED_IDS: Final = 50
SUCCEEDED_EVENT_HINT: Final = "booking-succeeded"

PROVEN: Final = "proven"

# Identity refusals, re-derived from the database exactly as the worker does.
REASON_NO_RECORD: Final = "no_record"
REASON_COMPANY_MISMATCH: Final = "company_mismatch"
REASON_BOOKING_ID_MISMATCH: Final = "booking_id_mismatch"
REASON_NO_CLIENT: Final = "no_client"
REASON_CLIENT_MISMATCH: Final = "client_mismatch"
REASON_CHECK_FAILED: Final = "check_failed"

# Configuration states checked BEFORE the queue. A preflight is a statement
# about one configuration, and it is only meaningful in the state the rollout is
# supposed to be in.
REASON_PROCESSING_DISABLED: Final = "processing_disabled"
REASON_LOCATION_REGISTRY_UNREADY: Final = "location_registry_unready"
REASON_COUNTER_ALREADY_ENABLED: Final = "visit_counter_already_enabled"


@dataclass
class VisitCounterPreflightReport:
    """Counts, stable reason codes and technical ids — nothing else."""

    candidate_count: int = 0
    checked_count: int = 0
    truncated: bool = False
    config_error: str | None = None
    reasons: Counter = field(default_factory=Counter)
    blocked_event_ids: list[int] = field(default_factory=list)
    proven_record_ids: list[int] = field(default_factory=list)
    proven_client_ids: list[int] = field(default_factory=list)
    company_ids: set[int] = field(default_factory=set)

    @property
    def green_count(self) -> int:
        return self.reasons.get(PROVEN, 0)

    @property
    def blocked_count(self) -> int:
        return self.checked_count - self.green_count

    @property
    def ready(self) -> bool:
        if self.config_error is not None:
            return False
        if self.truncated:
            # The limit cut the sample off, so "every candidate is fine" is a
            # statement about an arbitrary prefix, not about production.
            return False
        if self.candidate_count == 0:
            return False
        if self.checked_count != self.candidate_count:
            return False
        return self.green_count == self.candidate_count

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "config_error": self.config_error,
            "candidate_count": self.candidate_count,
            "checked_count": self.checked_count,
            "green_count": self.green_count,
            "blocked_count": self.blocked_count,
            "truncated": self.truncated,
            "reasons": dict(sorted(self.reasons.items())),
            "blocked_event_ids": sorted(self.blocked_event_ids)[:MAX_REPORTED_IDS],
            "proven_record_ids": sorted(self.proven_record_ids)[:MAX_REPORTED_IDS],
            "proven_client_ids": sorted(self.proven_client_ids)[:MAX_REPORTED_IDS],
            "company_ids": sorted(self.company_ids)[:MAX_REPORTED_IDS],
            "provider": PROVIDER_EASYWEEK,
            "ready": self.ready,
        }


def rollout_state_error() -> str | None:
    """Is this deployment in the state the preflight is meant to describe?"""
    from altegio_bot.settings import settings

    if bool(settings.easyweek_visit_counter_enabled):
        # Auditing "would it work" after it is already running describes a world
        # that no longer exists — and a green report would be mistaken for
        # permission to enable something already enabled.
        return REASON_COUNTER_ALREADY_ENABLED
    if not bool(settings.easyweek_processing_enabled):
        return REASON_PROCESSING_DISABLED
    if not configured_easyweek_locations().ready:
        # Without a valid registry every delivery is foreign traffic, so every
        # candidate would fail for a reason that says nothing about the payload.
        return REASON_LOCATION_REGISTRY_UNREADY
    return None


async def select_succeeded_events(
    session: AsyncSession,
    *,
    limit: int,
) -> tuple[list[EasyWeekEvent], bool]:
    """The most recent stored ``booking-succeeded`` deliveries, newest first.

    Deliberately not filtered by status. A `processed` row is the best evidence
    available — it is what production actually received — and reading it changes
    nothing. Selecting only `captured` rows would describe the backlog rather
    than the payload shape, and on a healthy deployment would return nothing.
    """
    rows = list(
        (
            await session.execute(
                select(EasyWeekEvent)
                .where(EasyWeekEvent.event_hint == SUCCEEDED_EVENT_HINT)
                .order_by(EasyWeekEvent.received_at.desc(), EasyWeekEvent.id.desc())
                .limit(limit + 1)
            )
        )
        .scalars()
        .all()
    )
    truncated = len(rows) > limit
    return rows[:limit], truncated


async def check_succeeded_event(
    session: AsyncSession,
    event: EasyWeekEvent,
) -> tuple[str, Record | None, Client | None]:
    """Would this delivery move the counter? Same proofs, no writes.

    Returns ``(reason, record, client)``. ``reason`` is :data:`PROVEN` or a
    stable refusal code — never a payload value.
    """
    registry = configured_easyweek_locations()
    try:
        visit = normalize_succeeded_visit_event(
            event_hint=event.event_hint,
            payload=event.payload,
            body_truncated=bool(event.body_truncated),
            location_registry=registry.locations if registry.ready else {},
        )
    except NormalizationError as exc:
        return exc.code, None, None

    record = (
        (
            await session.execute(
                select(Record)
                .where(Record.provider == PROVIDER_EASYWEEK)
                .where(Record.easyweek_booking_uuid == visit.booking_uuid)
            )
        )
        .scalars()
        .first()
    )
    if record is None:
        return REASON_NO_RECORD, None, None
    if record.company_id != visit.company_id:
        return REASON_COMPANY_MISMATCH, record, None
    if record.altegio_record_id is not None and record.altegio_record_id != visit.booking_id:
        return REASON_BOOKING_ID_MISMATCH, record, None
    if record.client_id is None:
        return REASON_NO_CLIENT, record, None

    client = (
        (
            await session.execute(
                select(Client)
                .where(Client.id == record.client_id)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.company_id == record.company_id)
            )
        )
        .scalars()
        .first()
    )
    if client is None:
        return REASON_NO_CLIENT, record, None
    if int(client.altegio_client_id) != visit.customer_id:
        return REASON_CLIENT_MISMATCH, record, client
    return PROVEN, record, client


async def run_visit_counter_preflight(
    session: AsyncSession,
    *,
    limit: int = DEFAULT_LIMIT,
) -> VisitCounterPreflightReport:
    """Check stored succeeded deliveries with the runtime rules. Writes nothing."""
    config_error = rollout_state_error()
    if config_error is not None:
        return VisitCounterPreflightReport(config_error=config_error)

    events, truncated = await select_succeeded_events(session, limit=limit)
    report = VisitCounterPreflightReport(candidate_count=len(events), truncated=truncated)

    for event in events:
        try:
            reason, record, client = await check_succeeded_event(session, event)
        except Exception:  # noqa: BLE001 — a failure to check is never a pass
            reason, record, client = REASON_CHECK_FAILED, None, None

        report.checked_count += 1
        report.reasons[reason] += 1
        if record is not None and record.company_id is not None:
            report.company_ids.add(record.company_id)
        if reason == PROVEN:
            if record is not None:
                report.proven_record_ids.append(record.id)
            if client is not None:
                report.proven_client_ids.append(client.id)
        else:
            report.blocked_event_ids.append(event.id)

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only preflight for the EasyWeek visit counter. Writes nothing, sends nothing."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum succeeded deliveries to check (default {DEFAULT_LIMIT}). A truncated sample is never green.",
    )
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        async with SessionLocal() as session:
            report = await run_visit_counter_preflight(session, limit=args.limit)
    except Exception as exc:  # noqa: BLE001 — class name only, never the text
        # Not evidence that the payloads are fine; evidence that we could not look.
        print({"mode": "read-only", "ready": False, "error": type(exc).__name__})
        return 1

    print(report.as_safe_dict())
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
