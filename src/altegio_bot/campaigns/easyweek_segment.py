"""Local, read-only EasyWeek campaign segmentation for the PR-14 subset."""

from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Final, Mapping

from sqlalchemy import func, or_, select
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
    BookingIdentity,
    evaluate_local_evidence,
    prove_event_record_client_link,
)
from altegio_bot.campaigns.easyweek_zero_visit import (
    CONTACT_EVENT_HINTS,
    ZERO_VISITS_REPORTED,
    ZeroVisitReport,
    ZeroVisitSourceBinding,
    captured_contact_phone,
    normalize_zero_visit_event,
    payload_reports_zero_visits_total,
    zero_visit_source_meta,
)
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_locations import (
    EasyWeekLocation,
    EasyWeekLocationRegistry,
    configured_easyweek_locations,
)
from altegio_bot.easyweek_normalizer import NormalizationError, SucceededVisit, normalize_succeeded_visit_event
from altegio_bot.easyweek_service_category import parse_allowed_service_categories
from altegio_bot.models.models import PROVIDER_EASYWEEK, Client, EasyWeekEvent, Record
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow
from altegio_bot.webhooks.common import normalize_phone_candidate

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
    # Zero-counter diagnostic. Deliveries whose `visits_total` was the integer zero AND whose
    # Event -> Record -> Client chain and period are provable, and the number of
    # distinct people those deliveries are about. Two counters, because a repeated
    # webhook is one more delivery and not one more person, and an operator
    # deciding how many cards to review needs the second number.
    "zero_visits_total_reported_events",
    "zero_visits_total_unique_clients",
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
    # The zero-counter diagnostic's binding, serialised, for the one kind of row
    # that has one. Separate from ``proof`` because it is the opposite claim:
    # ``proof`` says a first visit was proven, this says which delivery an
    # operator has to go and look at.
    zero_source: dict[str, object] | None = None


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


@dataclass(frozen=True)
class _Delivery:
    """One captured delivery, as plain values rather than an ORM row.

    The contact resolution runs in a session of its own, after the archive pass
    has closed its own. Carrying values instead of a detached ``EasyWeekEvent``
    keeps that boundary honest: nothing here can trigger a lazy load, and
    nothing depends on which session an object happened to be loaded in.
    """

    event_id: int
    event_hint: str | None
    payload: object
    body_truncated: bool


@dataclass(frozen=True)
class _ZeroVisitRow:
    """One delivery that reported the integer zero, with its proven chain."""

    event_id: int
    delivery: _Delivery
    report: ZeroVisitReport
    record: Record
    client: Client


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _candidate(
    client: Client,
    *,
    phone: str | None,
    reason: str | None,
    evaluated: bool = True,
    records_in_period: int = 1,
) -> ClientCandidate:
    """One row's snapshot counters.

    ``evaluated`` says whether this row went through the proven-subset
    evaluation — the service category, the counter and the whole local evidence
    rule. Only then may the row claim a confirmed in-category visit, because
    only then was one checked.

    The zero-counter diagnostic never reaches that evaluation: it is attributed
    and then stopped, so every counter that would otherwise assert a confirmed
    first visit in an allowed service category stays 0. A row the operator is
    being asked to investigate must not also be a row the screen describes as
    already proven.

    ``records_in_period`` is the field's own contract — how many of this
    client's records start inside the campaign's half-open period — and is
    counted, not assumed. The evaluated path proves exactly one source booking
    and passes 1; the diagnostic path counts, because a person may have two
    different bookings in the period (which is 2, never 1) and one booking may
    have been delivered twice (which is 1, never 2).
    """
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
        total_records_in_period=records_in_period,
        confirmed_records_in_period=1 if evaluated else 0,
        lash_records_in_period=1 if evaluated else 0,
        confirmed_lash_records_in_period=1 if evaluated else 0,
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
        ZERO_VISITS_REPORTED: "zero_visits_total_unique_clients",
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
    visit: BookingIdentity,
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


async def _resolve_zero_visit_row(
    session: AsyncSession,
    *,
    event: EasyWeekEvent,
    payload: dict,
    company_id: int,
    locations: Mapping[int, EasyWeekLocation],
    period_start: datetime,
    period_end: datetime,
) -> _ZeroVisitRow | None:
    """Can this zero-counter delivery be attributed to a person, or not at all?

    ``None`` means it cannot, and then nothing is shown: a truncated payload, a
    foreign branch, a booking we do not hold, a booking whose numeric id belongs
    to a different UUID, a booking reassigned to another client, a mismatched
    customer id, a start outside the campaign's half-open period and a deleted
    source record all end here. A diagnostic row attached to the wrong person is
    worse than no diagnostic row: an operator would review somebody else's card
    and a later manual decision would be made about the wrong human.

    Every check is the existing one. The record lookup is
    :func:`_resolve_record`, the chain and the period are
    :func:`prove_event_record_client_link`, and the payload validation is the
    full succeeded contract. Nothing about identity is restated here.
    """
    try:
        report = normalize_zero_visit_event(
            event_hint=event.event_hint,
            payload=payload,
            body_truncated=bool(event.body_truncated),
            location_registry=locations,
        )
    except NormalizationError:
        return None

    record, _unused_reason = await _resolve_record(session, visit=report)
    if record is None or record.client_id is None:
        return None
    client = await session.scalar(
        select(Client).where(Client.id == record.client_id).order_by(Client.id.asc()).limit(1)
    )
    if client is None:
        return None
    if (
        prove_event_record_client_link(
            booking=report,
            record=record,
            client=client,
            company_id=company_id,
            period_start=period_start,
            period_end=period_end,
        )
        is not None
    ):
        return None
    # A deleted source record already has its own exclusion reason and its own
    # count. This diagnostic is for the row where everything EXCEPT the counter
    # is proven, so it does not take over a case the segmenter can already name.
    if record.is_deleted is not False:
        return None
    return _ZeroVisitRow(
        event_id=event.id,
        delivery=_Delivery(
            event_id=event.id,
            event_hint=event.event_hint,
            payload=payload,
            body_truncated=bool(event.body_truncated),
        ),
        report=report,
        record=record,
        client=client,
    )


async def _records_in_period(
    session: AsyncSession,
    *,
    company_id: int,
    client_id: int,
    period_start: datetime,
    period_end: datetime,
) -> int:
    """How many of this client's records start inside the half-open period.

    The literal contract of ``total_records_in_period``, counted in SQL over
    distinct records rather than inferred from how many deliveries arrived. Two
    deliveries of one booking are one record; two different bookings are two.
    Deleted records are left out, exactly as the rest of this path leaves them
    out: a cancelled booking is not a record in the period for campaign
    purposes, and the zero-counter row refuses a deleted source outright.
    """
    total = await session.scalar(
        select(func.count(func.distinct(Record.id)))
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.company_id == company_id)
        .where(Record.client_id == client_id)
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at.is_not(None))
        .where(Record.starts_at >= _as_utc(period_start))
        .where(Record.starts_at < _as_utc(period_end))
    )
    return int(total or 0)


async def _contact_evidence(
    session: AsyncSession,
    *,
    source: _Delivery,
    report: ZeroVisitReport,
    company_id: int,
    locations: Mapping[int, EasyWeekLocation],
) -> tuple[int | None, int]:
    """``(contact_event_id, distinct_contacts)`` for one zero-counter booking.

    §44.2's second identity route needs a contact this workspace captured for
    this booking. The zero-counter delivery itself often carries none, and
    §44.2 is equally clear that the booking's CURRENT owner may not stand in for
    it. So the preview looks — once, while it still has the archive open — at
    the deliveries of this same booking that could carry one: a repeat of the
    same ``booking-succeeded``, and its ``booking-created``/``booking-updated``
    deliveries.

    Every candidate must prove the branch, the booking UUID, the numeric booking
    id and the numeric customer id against the source delivery; that is
    :func:`captured_contact_phone` and is not restated here. A delivery about
    another booking, another branch or another customer is not a candidate.

    The result is deliberately a COUNT plus a pointer, not a number:

    * ``distinct_contacts == 1`` — one contact, however many deliveries carried
      it. The pointer is the source delivery itself when that is one of them,
      otherwise the earliest sibling that carried it, so the stored binding does
      not depend on which replay arrived first.
    * ``distinct_contacts == 0`` — nothing captured a contact for this booking.
    * ``distinct_contacts > 1`` — the deliveries contradict each other about who
      this is. Recorded, never resolved: picking one would pick a person.

    Bounded by the deliveries of ONE booking, keyed on the indexed booking uuid,
    and the phone itself is never stored.
    """
    siblings = list(
        (
            await session.execute(
                select(EasyWeekEvent)
                .where(EasyWeekEvent.booking_uuid == report.booking_uuid)
                .where(EasyWeekEvent.status == "processed")
                .where(EasyWeekEvent.event_hint.in_(CONTACT_EVENT_HINTS))
                .where(EasyWeekEvent.id != source.event_id)
                .order_by(EasyWeekEvent.id.asc())
            )
        )
        .scalars()
        .all()
    )
    # The source delivery is always a candidate, whatever the index says about
    # it: it is the delivery this row was written from and it is already in
    # hand, so its own captured contact never depends on a lookup succeeding.
    deliveries = [source] + [
        _Delivery(
            event_id=row.id,
            event_hint=row.event_hint,
            payload=row.payload,
            body_truncated=bool(row.body_truncated),
        )
        for row in siblings
    ]
    carriers: dict[str, list[int]] = defaultdict(list)
    for delivery in deliveries:
        phone = captured_contact_phone(
            event_hint=delivery.event_hint,
            payload=delivery.payload,
            body_truncated=delivery.body_truncated,
            location_registry=locations,
            company_id=company_id,
            booking_uuid=report.booking_uuid,
            booking_id=report.booking_id,
            customer_id=report.customer_id,
        )
        if phone is not None:
            carriers[phone].append(delivery.event_id)
    if len(carriers) != 1:
        return None, len(carriers)
    event_ids = sorted(next(iter(carriers.values())))
    if source.event_id in event_ids:
        # The source delivery carries it itself; no pointer is needed and the
        # check reads it from the delivery the binding already names.
        return None, 1
    return event_ids[0], 1


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
    zero_by_client: dict[tuple[str, int, int], list[_ZeroVisitRow]] = defaultdict(list)
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
                # `visits_total_out_of_range` also covers negatives and
                # overflow, so the code alone does not select this branch: the
                # payload must carry the integer zero itself.
                if exc.code == NormalizationError.VISITS_TOTAL_OUT_OF_RANGE and payload_reports_zero_visits_total(
                    payload
                ):
                    zero_row = await _resolve_zero_visit_row(
                        session,
                        event=event,
                        payload=payload,
                        company_id=company_id,
                        locations=locations.locations,
                        period_start=period_start,
                        period_end=period_end,
                    )
                    if zero_row is not None:
                        result.coverage["zero_visits_total_reported_events"] += 1
                        zero_by_client[(PROVIDER_EASYWEEK, company_id, zero_row.client.id)].append(zero_row)
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

    # The diagnostic rows, last and only for people the ordinary selection
    # produced no row for at all. A client the segmenter proved keeps the proof,
    # and a client it excluded for a reason of its own keeps that reason: the
    # zero-counter row exists to stop somebody DISAPPEARING, never to override a
    # verdict or to open a way around an exclusion. Keyed on the client, so the
    # outcome is the same whichever order the deliveries arrived in.
    async with SessionLocal() as session:
        for key in sorted(zero_by_client):
            if key in evidence_by_client:
                continue
            rows = sorted(zero_by_client[key], key=lambda row: (row.event_id, str(row.report.booking_uuid)))
            representative = rows[0]
            # Deliveries are not people and bookings are not deliveries. A
            # replayed webhook repeats one booking and must change neither the
            # row nor the count; two DIFFERENT zero-counter bookings for one
            # card mean the question "which visit is this about?" has no single
            # answer, and that fact is recorded now rather than rediscovered —
            # or guessed at — when the row is opened.
            distinct = {row.report.booking_uuid for row in rows}
            contact_event_id, contact_sources = await _contact_evidence(
                session,
                source=representative.delivery,
                report=representative.report,
                company_id=company_id,
                locations=locations.locations,
            )
            binding = ZeroVisitSourceBinding(
                event_id=representative.event_id,
                record_id=representative.record.id,
                booking_uuid=representative.report.booking_uuid,
                booking_id=representative.report.booking_id,
                customer_id=representative.report.customer_id,
                distinct_sources=len(distinct),
                contact_event_id=contact_event_id,
                contact_sources=contact_sources,
            )
            # Once per person, not once per delivery: a replayed webhook must
            # not produce a second row or a second reason in the breakdown.
            _record_failure(result, ZERO_VISITS_REPORTED)
            result.candidates.append(
                EasyWeekCandidate(
                    candidate=_candidate(
                        representative.client,
                        phone=normalize_phone_candidate(representative.client.phone_e164),
                        reason=ZERO_VISITS_REPORTED,
                        # Not evaluated: attributed and stopped. See `_candidate`.
                        evaluated=False,
                        records_in_period=await _records_in_period(
                            session,
                            company_id=company_id,
                            client_id=representative.client.id,
                            period_start=period_start,
                            period_end=period_end,
                        ),
                    ),
                    # No source proof, ever. The delivery proved no first visit,
                    # and a proof here would be a number nobody reported.
                    proof=None,
                    zero_source=zero_visit_source_meta(binding),
                )
            )

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
