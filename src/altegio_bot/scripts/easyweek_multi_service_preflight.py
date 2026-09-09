"""Read-only readiness audit for PR-7.4 exactly-two-service notifications.

The command reads future EasyWeek records, the latest relevant captured
webhook, the live booking and the complete location catalogue.  It applies the
same proof and all-categories eligibility helpers as runtime, then inspects the
open lifecycle/reminder queue.  It never plans or claims a job, never commits,
and has no Meta, Chatwoot or mutating EasyWeek capability.

Output is deliberately aggregate-only: counts and stable reason codes.  No
booking/customer identifiers, names, prices, URLs, payloads or API bodies are
printed.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekError
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_API_UNAVAILABLE,
    MULTI_SERVICE_CATALOG_UNAVAILABLE,
    MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED,
    MULTI_SERVICE_JOB_DIGEST_KEY,
    MULTI_SERVICE_SNAPSHOT_KEY,
    MultiServiceProofError,
    MultiServiceReader,
    MultiServiceSnapshot,
    ServiceEligibilityPurpose,
    WebhookServicePair,
    evaluate_service_eligibility,
    multi_service_snapshot_from_record_raw,
    prove_exactly_two_service_custom_duration_exclusion,
    prove_exactly_two_service_snapshot,
    read_catalog_rows_cached,
    record_raw_with_multi_service_snapshot,
    resolve_effective_multi_service_snapshot,
)
from altegio_bot.easyweek_normalizer import NormalizationError, normalize_event
from altegio_bot.easyweek_policy import EASYWEEK_LIFECYCLE_JOB_TYPES, EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.easyweek_service_category import (
    EASYWEEK_RAW_NAMESPACE,
    SERVICES_COUNT_SNAPSHOT_KEY,
    record_raw_with_services_count,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, EasyWeekEvent, MessageJob, OutboxMessage, Record
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

DEFAULT_LIMIT: Final = 500
# Leaves headroom for the first catalogue page per configured location under
# EasyWeek's workspace-wide 60 requests/minute ceiling.
DEFAULT_PAUSE_SEC: Final = 1.1
OPEN_JOB_STATUSES: Final = ("queued", "processing")
NON_TERMINAL_OUTBOX_STATUSES: Final = ("queued", "sending")
_LIFECYCLE_HINTS: Final = (
    "booking-created",
    "booking-updated",
    "booking-rescheduled",
    "booking-canceled",
)
_PROOF_TRIGGER_KEYS: Final = frozenset(
    {
        "service_name",
        "service_related",
        "services_description",
        "services_count",
        "quantity",
        "booking_price",
        "booking_price_currency",
    }
)


@dataclass
class MultiServicePreflightReport:
    active_multi_service: int = 0
    checked: int = 0
    structurally_proven: int = 0
    allowed: int = 0
    disallowed_by_category: int = 0
    contract_excluded: int = 0
    ambiguous: int = 0
    open_jobs: int = 0
    jobs_held_by_send_fence: int = 0
    stale_snapshot_digest: int = 0
    unexplained: int = 0
    truncated: bool = False
    reasons: Counter[str] = field(default_factory=Counter)

    @property
    def ready(self) -> bool:
        return (
            self.active_multi_service > 0
            and not self.truncated
            and self.checked == self.active_multi_service
            and self.structurally_proven + self.contract_excluded == self.active_multi_service
            and self.allowed + self.disallowed_by_category == self.structurally_proven
            and self.ambiguous == 0
            and self.stale_snapshot_digest == 0
            and self.unexplained == 0
            and self.open_jobs == self.jobs_held_by_send_fence
        )

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "active_multi_service": self.active_multi_service,
            "checked": self.checked,
            "structurally_proven": self.structurally_proven,
            "allowed": self.allowed,
            "disallowed_by_category": self.disallowed_by_category,
            "contract_excluded": self.contract_excluded,
            "ambiguous": self.ambiguous,
            "open_jobs": self.open_jobs,
            "jobs_held_by_send_fence": self.jobs_held_by_send_fence,
            "stale_snapshot_digest": self.stale_snapshot_digest,
            "unexplained": self.unexplained,
            "truncated": self.truncated,
            "reasons": dict(sorted(self.reasons.items())),
            "ready": self.ready,
        }


async def select_active_multi_service_records(
    session: AsyncSession,
    *,
    limit: int,
) -> tuple[list[Record], bool]:
    """Select only future, active, provider-scoped records whose count is 2."""
    stmt = (
        select(Record)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at.is_not(None), Record.starts_at > utcnow())
        .where(Record.easyweek_booking_uuid.is_not(None))
        .where(Record.raw.contains({EASYWEEK_RAW_NAMESPACE: {SERVICES_COUNT_SNAPSHOT_KEY: 2}}))
        .order_by(Record.starts_at.asc(), Record.id.asc())
        .limit(limit + 1)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return rows[:limit], len(rows) > limit


async def _latest_lifecycle_events(
    session: AsyncSession,
    records: list[Record],
) -> dict[object, EasyWeekEvent]:
    booking_uuids = [record.easyweek_booking_uuid for record in records if record.easyweek_booking_uuid is not None]
    if not booking_uuids:
        return {}
    stmt = (
        select(EasyWeekEvent)
        .where(EasyWeekEvent.booking_uuid.in_(booking_uuids))
        .where(EasyWeekEvent.event_hint.in_(_LIFECYCLE_HINTS))
        .order_by(EasyWeekEvent.received_at.desc(), EasyWeekEvent.id.desc())
    )
    result: dict[object, EasyWeekEvent] = {}
    for event in (await session.execute(stmt)).scalars():
        payload = event.payload if isinstance(event.payload, Mapping) else {}
        # Preserve runtime patch semantics: a later status/comment-only update
        # does not erase the last service proof.  A total-only or partially
        # carried service update does trigger re-proof and therefore remains
        # visible here as an ambiguity rather than being skipped.
        if event.booking_uuid not in result and _PROOF_TRIGGER_KEYS & payload.keys():
            result[event.booking_uuid] = event
    return result


async def _open_jobs_by_record(
    session: AsyncSession,
    records: list[Record],
) -> dict[int, list[MessageJob]]:
    record_ids = [record.id for record in records]
    if not record_ids:
        return {}
    supported = EASYWEEK_LIFECYCLE_JOB_TYPES | EASYWEEK_REMINDER_JOB_TYPES
    stmt = (
        select(MessageJob)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.record_id.in_(record_ids))
        .where(MessageJob.job_type.in_(supported))
        .where(MessageJob.status.in_(OPEN_JOB_STATUSES))
        .order_by(MessageJob.id.asc())
    )
    result: dict[int, list[MessageJob]] = {}
    for job in (await session.execute(stmt)).scalars():
        if job.record_id is not None:
            result.setdefault(job.record_id, []).append(job)
    return result


async def _non_terminal_outboxes_by_record(
    session: AsyncSession,
    records: list[Record],
) -> dict[int, list[OutboxMessage]]:
    record_ids = [record.id for record in records]
    if not record_ids:
        return {}
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.record_id.in_(record_ids))
        .where(OutboxMessage.status.in_(NON_TERMINAL_OUTBOX_STATUSES))
        .order_by(OutboxMessage.id.asc())
    )
    result: dict[int, list[OutboxMessage]] = {}
    for row in (await session.execute(stmt)).scalars():
        if row.record_id is not None:
            result.setdefault(row.record_id, []).append(row)
    return result


def _snapshot_key_present(raw: object) -> bool:
    if not isinstance(raw, Mapping):
        return False
    namespace = raw.get(EASYWEEK_RAW_NAMESPACE)
    return isinstance(namespace, Mapping) and MULTI_SERVICE_SNAPSHOT_KEY in namespace


def _record_job_consistency(
    report: MultiServicePreflightReport,
    *,
    record: Record,
    live_snapshot: MultiServiceSnapshot,
    jobs: list[MessageJob],
) -> None:
    stored, stored_error = multi_service_snapshot_from_record_raw(record.raw)
    has_stored_value = _snapshot_key_present(record.raw)
    if has_stored_value and (stored is None or stored.digest != live_snapshot.digest):
        report.stale_snapshot_digest += 1
        report.unexplained += 1
        report.reasons[stored_error or "multi_service_snapshot_digest_mismatch"] += 1

    for job in jobs:
        report.open_jobs += 1
        payload = job.payload if isinstance(job.payload, Mapping) else {}
        claims_pair = MULTI_SERVICE_JOB_DIGEST_KEY in payload
        safely_held = (
            claims_pair
            and not bool(settings.easyweek_multi_service_send_enabled)
            and job.status == "queued"
            and job.attempts == 0
            and job.locked_at is None
        )
        if safely_held:
            report.jobs_held_by_send_fence += 1
        else:
            report.unexplained += 1
            report.reasons["multi_service_job_not_held"] += 1

        effective, guard_error = resolve_effective_multi_service_snapshot(
            record_raw=record.raw,
            job_payload=payload,
            record_total_cost=record.total_cost,
            expected_booking_uuid=record.easyweek_booking_uuid,
            expected_location_uuid=live_snapshot.location_uuid,
        )
        if guard_error is not None or effective is None or effective.digest != live_snapshot.digest:
            report.stale_snapshot_digest += 1
            report.unexplained += 1
            report.reasons[guard_error or "multi_service_snapshot_digest_mismatch"] += 1


def _contract_exclusion_consistency(
    report: MultiServicePreflightReport,
    *,
    record: Record,
    jobs: list[MessageJob],
    outboxes: list[OutboxMessage],
) -> None:
    if _snapshot_key_present(record.raw):
        _snapshot, error = multi_service_snapshot_from_record_raw(record.raw)
        report.stale_snapshot_digest += 1
        report.unexplained += 1
        report.reasons[error or "contract_excluded_snapshot_present"] += 1
    for _job in jobs:
        report.open_jobs += 1
        report.unexplained += 1
        report.reasons["contract_excluded_open_job"] += 1
    for _outbox in outboxes:
        report.unexplained += 1
        report.reasons["contract_excluded_non_terminal_outbox"] += 1


async def run_preflight(
    session: AsyncSession,
    *,
    client: MultiServiceReader,
    limit: int = DEFAULT_LIMIT,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> MultiServicePreflightReport:
    """Audit every selected record without mutating the session or sending."""
    pause = sleep if sleep is not None else asyncio.sleep
    records, truncated = await select_active_multi_service_records(session, limit=limit)
    report = MultiServicePreflightReport(active_multi_service=len(records), truncated=truncated)
    events = await _latest_lifecycle_events(session, records)
    jobs = await _open_jobs_by_record(session, records)
    outboxes = await _non_terminal_outboxes_by_record(session, records)
    registry = configured_easyweek_locations()

    api_reads = 0
    for record in records:
        report.checked += 1
        event = events.get(record.easyweek_booking_uuid)
        if event is None or not registry.ready:
            report.ambiguous += 1
            report.unexplained += 1
            report.reasons["multi_service_webhook_shape_unproven"] += 1
            continue
        try:
            booking = normalize_event(
                event_hint=event.event_hint,
                payload=event.payload,
                body_truncated=bool(event.body_truncated),
                location_registry=registry.locations,
            )
        except NormalizationError:
            booking = None
        location = registry.locations.get(record.company_id)
        if booking is None or location is None or booking.booking_uuid != record.easyweek_booking_uuid:
            report.ambiguous += 1
            report.unexplained += 1
            report.reasons["multi_service_webhook_shape_unproven"] += 1
            continue

        if api_reads:
            await pause(pause_sec)
        api_reads += 1
        webhook_pair = WebhookServicePair(
            booking_uuid=booking.booking_uuid,
            location_uuid=location.location_uuid,
            service_name=booking.service_name,
            service_related=booking.service_related,
            services_description=booking.services_description,
            services_count=booking.services_count,
            quantity=booking.service_quantity,
            booking_currency=booking.booking_currency,
            total_cost=record.total_cost,
        )
        try:
            try:
                booking_payload = await client.get_booking(str(booking.booking_uuid))
            except EasyWeekError:
                raise MultiServiceProofError(MULTI_SERVICE_API_UNAVAILABLE, recoverable=True) from None
            catalog_rows = await read_catalog_rows_cached(client, location_uuid=location.location_uuid)
            try:
                live = prove_exactly_two_service_snapshot(
                    webhook=webhook_pair,
                    booking_payload=booking_payload,
                    catalog_rows=catalog_rows,
                )
            except MultiServiceProofError as initial_error:
                if initial_error.reason != MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED:
                    raise
                prove_exactly_two_service_custom_duration_exclusion(
                    webhook=webhook_pair,
                    booking_payload=booking_payload,
                    catalog_rows=catalog_rows,
                )
                live = None
        except MultiServiceProofError as exc:
            report.ambiguous += 1
            report.unexplained += 1
            report.reasons[exc.reason] += 1
            continue
        except Exception:  # noqa: BLE001 — no exception text reaches the report
            report.ambiguous += 1
            report.unexplained += 1
            report.reasons[MULTI_SERVICE_CATALOG_UNAVAILABLE] += 1
            continue

        if live is None:
            report.contract_excluded += 1
            report.reasons[MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED] += 1
            _contract_exclusion_consistency(
                report,
                record=record,
                jobs=jobs.get(record.id, []),
                outboxes=outboxes.get(record.id, []),
            )
            continue

        report.structurally_proven += 1
        proof_raw = record_raw_with_multi_service_snapshot(
            record_raw_with_services_count(record.raw, 2),
            live,
        )
        eligibility = evaluate_service_eligibility(
            record_raw=proof_raw,
            allowed_categories_raw=settings.easyweek_allowed_service_categories,
            purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
        )
        if eligibility.allowed:
            report.allowed += 1
        elif eligibility.reason == "multi_service_category_not_allowed":
            report.disallowed_by_category += 1
            report.reasons[eligibility.reason] += 1
        else:
            report.ambiguous += 1
            report.unexplained += 1
            report.reasons[eligibility.reason] += 1

        _record_job_consistency(
            report,
            record=record,
            live_snapshot=live,
            jobs=jobs.get(record.id, []),
        )

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only aggregate preflight for EasyWeek exactly-two-service notifications."
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--pause-sec", type=float, default=DEFAULT_PAUSE_SEC)
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
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
