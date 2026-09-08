"""Local, read-only EasyWeek campaign segmentation for the PR-14 subset."""

from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Final

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.contracts import ClientCandidate, ClientSnapshot
from altegio_bot.campaigns.easyweek_eligibility import (
    BOOKING_IDENTITY_MISMATCH,
    COMPANY_MISMATCH,
    CONFLICTING_FIRST_VISIT_EVIDENCE,
    CURRENT_VISITS_MISSING,
    CURRENT_VISITS_NOT_ONE,
    CURRENT_VISITS_UNSTAMPED,
    DELETED_RECORD,
    DUPLICATE_EVIDENCE,
    EVENT_NOT_PROCESSED,
    FUTURE_BOOKING,
    INVALID_PHONE,
    MISSING_CLIENT,
    MISSING_RECORD,
    OPTED_OUT,
    PROVIDER_MISMATCH,
    REGISTRY_UNAVAILABLE,
    SEGMENT_CONTRACT_UNAVAILABLE,
    SOURCE_VISITS_NOT_ONE,
    evaluate_local_evidence,
)
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.easyweek_normalizer import NormalizationError, SucceededVisit, normalize_succeeded_visit_event
from altegio_bot.easyweek_service_category import parse_allowed_service_categories
from altegio_bot.models.models import PROVIDER_EASYWEEK, Client, EasyWeekEvent, Record
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

SEGMENT_SOURCE: Final = "easyweek_booking_succeeded_local_proven_subset"

_COVERAGE_KEYS: Final = (
    "succeeded_events_seen",
    "normalized_succeeded_events",
    "visits_total_out_of_range",
    "missing_current_visits_total",
    "source_visits_total_not_one",
    "current_visits_total_not_one",
    "missing_record",
    "missing_client",
    "identity_mismatch",
    "deleted_source_record",
    "service_count_unproven",
    "category_missing",
    "category_not_allowed",
    "invalid_or_missing_phone",
    "opted_out",
    "locally_known_future_booking",
    "eligible_proven_rows",
    "eligible_unique_clients",
    "duplicate_evidence_for_client",
    "conflicting_first_visit_evidence",
)


class EasyWeekSegmentUnavailable(RuntimeError):
    """Stable, PII-free configuration/contract refusal."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class EasyWeekSourceProof:
    event_id: int
    record_id: int
    booking_uuid: uuid.UUID
    visits_total: int
    visits_total_updated_at: datetime


@dataclass
class EasyWeekCandidate:
    candidate: ClientCandidate
    proof: EasyWeekSourceProof | None = None


@dataclass
class EasyWeekSegmentResult:
    candidates: list[EasyWeekCandidate]
    coverage: Counter[str] = field(default_factory=Counter)
    reason_counts: Counter[str] = field(default_factory=Counter)

    def safe_meta(self) -> dict[str, object]:
        coverage = {key: int(self.coverage.get(key, 0)) for key in _COVERAGE_KEYS}
        return {
            "discovery_source": SEGMENT_SOURCE,
            "segment_completeness": "proven_subset",
            "coverage": coverage,
            "reason_counts": dict(sorted(self.reason_counts.items())),
        }


@dataclass(frozen=True)
class _Evidence:
    event_id: int
    visit: SucceededVisit
    record: Record
    client: Client
    candidate: ClientCandidate
    locally_eligible: bool


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _candidate(client: Client, *, phone: str | None, reason: str | None) -> ClientCandidate:
    return ClientCandidate(
        client=ClientSnapshot(
            id=client.id,
            provider=client.provider,
            company_id=client.company_id,
            altegio_client_id=client.altegio_client_id,
            display_name=client.display_name,
            phone_e164=phone,
            wa_opted_out=client.wa_opted_out,
        ),
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=[],
        records_before_period=0,
        records_after_period=0,
        local_client_found=True,
        excluded_reason=reason,
    )


def _coverage_key(reason: str) -> str | None:
    return {
        NormalizationError.VISITS_TOTAL_OUT_OF_RANGE: "visits_total_out_of_range",
        CURRENT_VISITS_MISSING: "missing_current_visits_total",
        CURRENT_VISITS_UNSTAMPED: "missing_current_visits_total",
        SOURCE_VISITS_NOT_ONE: "source_visits_total_not_one",
        CURRENT_VISITS_NOT_ONE: "current_visits_total_not_one",
        MISSING_RECORD: "missing_record",
        MISSING_CLIENT: "missing_client",
        PROVIDER_MISMATCH: "identity_mismatch",
        COMPANY_MISMATCH: "identity_mismatch",
        BOOKING_IDENTITY_MISMATCH: "identity_mismatch",
        "easyweek_campaign_customer_identity_mismatch": "identity_mismatch",
        DELETED_RECORD: "deleted_source_record",
        "service_count_unproven": "service_count_unproven",
        "category_ambiguous_multi_service": "service_count_unproven",
        "category_missing": "category_missing",
        "category_not_allowed": "category_not_allowed",
        INVALID_PHONE: "invalid_or_missing_phone",
        OPTED_OUT: "opted_out",
        FUTURE_BOOKING: "locally_known_future_booking",
        DUPLICATE_EVIDENCE: "duplicate_evidence_for_client",
        CONFLICTING_FIRST_VISIT_EVIDENCE: "conflicting_first_visit_evidence",
    }.get(reason)


def _record_failure(result: EasyWeekSegmentResult, reason: str) -> None:
    result.reason_counts[reason] += 1
    if key := _coverage_key(reason):
        result.coverage[key] += 1


async def _future_booking_exists(
    session: AsyncSession,
    *,
    company_id: int,
    client_id: int,
    customer_id: int,
    now: datetime,
) -> bool:
    stmt = (
        select(Record.id)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.company_id == company_id)
        .where(or_(Record.client_id == client_id, Record.altegio_client_id == customer_id))
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at.is_not(None))
        .where(Record.starts_at > _as_utc(now))
        .order_by(Record.starts_at.asc(), Record.id.asc())
        .limit(1)
    )
    return (await session.scalar(stmt)) is not None


async def _resolve_record(
    session: AsyncSession,
    *,
    visit: SucceededVisit,
) -> tuple[Record | None, str | None]:
    exact = await session.scalar(
        select(Record)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.company_id == visit.company_id)
        .where(Record.easyweek_booking_uuid == visit.booking_uuid)
        .order_by(Record.id.asc())
        .limit(1)
    )
    if exact is not None:
        if exact.altegio_record_id != visit.booking_id:
            return None, BOOKING_IDENTITY_MISMATCH
        return exact, None

    uuid_collision = await session.scalar(
        select(Record).where(Record.easyweek_booking_uuid == visit.booking_uuid).order_by(Record.id.asc()).limit(1)
    )
    if uuid_collision is not None:
        reason = PROVIDER_MISMATCH if uuid_collision.provider != PROVIDER_EASYWEEK else COMPANY_MISMATCH
        return None, reason
    numeric_collision = await session.scalar(
        select(Record.id)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.company_id == visit.company_id)
        .where(Record.altegio_record_id == visit.booking_id)
        .limit(1)
    )
    return (None, BOOKING_IDENTITY_MISMATCH if numeric_collision is not None else MISSING_RECORD)


async def build_easyweek_segment(
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
    registry: EasyWeekLocationRegistry | None = None,
    allowed_categories_raw: object | None = None,
    now: datetime | None = None,
) -> EasyWeekSegmentResult:
    """Build one deterministic provider/company-scoped local proven subset."""
    locations = registry or configured_easyweek_locations()
    if not locations.ready or company_id not in locations.locations:
        raise EasyWeekSegmentUnavailable(REGISTRY_UNAVAILABLE)
    allowed_raw = (
        settings.easyweek_allowed_service_categories if allowed_categories_raw is None else allowed_categories_raw
    )
    if not parse_allowed_service_categories(allowed_raw).ready:
        raise EasyWeekSegmentUnavailable(SEGMENT_CONTRACT_UNAVAILABLE)
    if period_start.tzinfo is None or period_end.tzinfo is None or period_start >= period_end:
        raise EasyWeekSegmentUnavailable(SEGMENT_CONTRACT_UNAVAILABLE)

    result = EasyWeekSegmentResult(candidates=[])
    evidence_by_client: dict[tuple[str, int, int], list[_Evidence]] = defaultdict(list)
    clock = now or utcnow()

    async with SessionLocal() as session:
        events = list(
            (
                await session.execute(
                    select(EasyWeekEvent)
                    .where(EasyWeekEvent.event_hint == "booking-succeeded")
                    .order_by(EasyWeekEvent.id.asc())
                )
            )
            .scalars()
            .all()
        )
        for event in events:
            payload = event.payload
            if not isinstance(payload, dict) or type(payload.get("location_id")) is not int:
                continue
            if payload.get("location_id") != company_id:
                continue
            result.coverage["succeeded_events_seen"] += 1
            if event.status != "processed":
                _record_failure(result, EVENT_NOT_PROCESSED)
                continue
            try:
                visit = normalize_succeeded_visit_event(
                    event_hint=event.event_hint,
                    payload=payload,
                    body_truncated=bool(event.body_truncated),
                    location_registry=locations.locations,
                )
            except NormalizationError as exc:
                _record_failure(result, exc.code)
                continue
            result.coverage["normalized_succeeded_events"] += 1

            record, resolution_reason = await _resolve_record(session, visit=visit)
            if record is None:
                _record_failure(result, resolution_reason or MISSING_RECORD)
                continue
            if record.client_id is None:
                _record_failure(result, MISSING_CLIENT)
                continue
            client = await session.scalar(
                select(Client).where(Client.id == record.client_id).order_by(Client.id.asc()).limit(1)
            )
            if client is None:
                _record_failure(result, MISSING_CLIENT)
                continue
            future = await _future_booking_exists(
                session,
                company_id=company_id,
                client_id=client.id,
                customer_id=visit.customer_id,
                now=clock,
            )
            local = evaluate_local_evidence(
                visit=visit,
                record=record,
                client=client,
                company_id=company_id,
                period_start=period_start,
                period_end=period_end,
                allowed_categories_raw=allowed_raw,
                has_future_booking=future,
            )
            candidate = _candidate(client, phone=local.normalized_phone, reason=local.reason)
            if local.eligible:
                result.coverage["eligible_proven_rows"] += 1
            elif local.reason is not None:
                _record_failure(result, local.reason)
            evidence_by_client[(PROVIDER_EASYWEEK, company_id, client.id)].append(
                _Evidence(
                    event_id=event.id,
                    visit=visit,
                    record=record,
                    client=client,
                    candidate=candidate,
                    locally_eligible=local.eligible,
                )
            )

    for key in sorted(evidence_by_client):
        rows = sorted(evidence_by_client[key], key=lambda row: (row.event_id, str(row.visit.booking_uuid)))
        by_booking: dict[object, list[_Evidence]] = defaultdict(list)
        for row in rows:
            by_booking[row.visit.booking_uuid].append(row)
        duplicate_count = sum(max(0, len(group) - 1) for group in by_booking.values())
        if duplicate_count:
            result.coverage["duplicate_evidence_for_client"] += duplicate_count
            result.reason_counts[DUPLICATE_EVIDENCE] += duplicate_count

        representatives = [sorted(group, key=lambda row: row.event_id)[0] for group in by_booking.values()]
        representatives.sort(key=lambda row: (row.event_id, str(row.visit.booking_uuid)))
        visits_values = {row.visit.visits_total for row in rows}
        first_visit_bookings = {row.visit.booking_uuid for row in rows if row.visit.visits_total == 1}
        conflict = len(visits_values) > 1 or len(first_visit_bookings) > 1
        chosen = representatives[0]
        if conflict:
            chosen.candidate.excluded_reason = CONFLICTING_FIRST_VISIT_EVIDENCE
            _record_failure(result, CONFLICTING_FIRST_VISIT_EVIDENCE)
            result.candidates.append(EasyWeekCandidate(candidate=chosen.candidate))
            continue

        if chosen.locally_eligible:
            stamp = chosen.client.easyweek_visits_total_updated_at
            assert stamp is not None
            proof = EasyWeekSourceProof(
                event_id=chosen.event_id,
                record_id=chosen.record.id,
                booking_uuid=chosen.visit.booking_uuid,
                visits_total=chosen.visit.visits_total,
                visits_total_updated_at=stamp,
            )
            result.coverage["eligible_unique_clients"] += 1
            result.candidates.append(EasyWeekCandidate(candidate=chosen.candidate, proof=proof))
        else:
            result.candidates.append(EasyWeekCandidate(candidate=chosen.candidate))

    result.candidates.sort(
        key=lambda item: (
            item.candidate.client.company_id,
            item.candidate.client.id or 0,
            item.proof.event_id if item.proof is not None else 0,
        )
    )
    return result


__all__ = [
    "EasyWeekCandidate",
    "EasyWeekSegmentResult",
    "EasyWeekSegmentUnavailable",
    "SEGMENT_SOURCE",
    "build_easyweek_segment",
]
