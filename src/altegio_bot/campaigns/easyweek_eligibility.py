"""Read-only EasyWeek campaign evidence for the PR-14 proven subset.

This module deliberately does not implement a live send guard.  It can prove
the local source event and re-read one booking, but the confirmed EasyWeek API
does not expose the customer-level history needed to release a campaign send.
Consequently every :class:`CampaignEligibilityResult` remains ``send_ready``
false and carries ``campaign_live_guard_unproven``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final, Protocol

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.provider import CAMPAIGN_LIVE_GUARD_UNPROVEN
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_locations import EasyWeekLocation, EasyWeekLocationRegistry
from altegio_bot.easyweek_normalizer import NormalizationError, SucceededVisit, normalize_succeeded_visit_event
from altegio_bot.easyweek_service_category import evaluate_service_category
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    Record,
)
from altegio_bot.webhooks.common import normalize_phone_candidate

EVENT_NOT_PROCESSED: Final = "easyweek_campaign_event_not_processed"
MISSING_RECORD: Final = "easyweek_campaign_record_missing"
MISSING_CLIENT: Final = "easyweek_campaign_client_missing"
PROVIDER_MISMATCH: Final = "easyweek_campaign_provider_mismatch"
COMPANY_MISMATCH: Final = "easyweek_campaign_company_mismatch"
BOOKING_IDENTITY_MISMATCH: Final = "easyweek_campaign_booking_identity_mismatch"
CUSTOMER_IDENTITY_MISMATCH: Final = "easyweek_campaign_customer_identity_mismatch"
RECORD_START_UNPROVEN: Final = "easyweek_campaign_record_start_unproven"
OUTSIDE_PERIOD: Final = "easyweek_campaign_outside_period"
SOURCE_VISITS_NOT_ONE: Final = "easyweek_campaign_source_visits_total_not_one"
CURRENT_VISITS_MISSING: Final = "easyweek_campaign_current_visits_total_missing"
CURRENT_VISITS_NOT_ONE: Final = "easyweek_campaign_current_visits_total_not_one"
CURRENT_VISITS_UNSTAMPED: Final = "easyweek_campaign_current_visits_total_unstamped"
DELETED_RECORD: Final = "easyweek_campaign_record_deleted"
INVALID_PHONE: Final = "easyweek_campaign_phone_unproven"
OPTED_OUT: Final = "easyweek_campaign_opted_out"
FUTURE_BOOKING: Final = "easyweek_campaign_future_booking_known_locally"
DUPLICATE_EVIDENCE: Final = "easyweek_campaign_duplicate_evidence"
CONFLICTING_FIRST_VISIT_EVIDENCE: Final = "easyweek_campaign_conflicting_first_visit_evidence"
DURABLE_SOURCE_PROOF_UNPROVEN: Final = "easyweek_campaign_durable_source_proof_unproven"
REGISTRY_UNAVAILABLE: Final = "easyweek_campaign_location_registry_unavailable"
SEGMENT_CONTRACT_UNAVAILABLE: Final = "easyweek_campaign_segment_contract_unavailable"

BOOKING_RESPONSE_MALFORMED: Final = "easyweek_campaign_booking_response_malformed"
BOOKING_UUID_MISMATCH: Final = "easyweek_campaign_booking_uuid_mismatch"
BOOKING_LOCATION_MISMATCH: Final = "easyweek_campaign_booking_location_mismatch"
BOOKING_CANCELED: Final = "easyweek_campaign_booking_canceled"
BOOKING_SERVICE_COUNT_UNPROVEN: Final = "easyweek_campaign_booking_service_count_unproven"
BOOKING_NOT_FOUND: Final = "easyweek_campaign_booking_not_found"
BOOKING_RETRYABLE_UNAVAILABLE: Final = "easyweek_campaign_booking_retryable_unavailable"
BOOKING_CONFIGURATION_UNAVAILABLE: Final = "easyweek_campaign_booking_configuration_unavailable"
BOOKING_PERMANENT_ERROR: Final = "easyweek_campaign_booking_permanent_error"


class BookingReader(Protocol):
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class LocalEligibility:
    eligible: bool
    reason: str | None
    normalized_phone: str | None


@dataclass(frozen=True)
class SourceBookingProof:
    current: bool
    reason: str | None
    retryable: bool = False


@dataclass(frozen=True)
class CampaignEligibilityResult:
    local_eligible: bool
    source_booking_current: bool
    send_ready: bool
    reasons: tuple[str, ...]
    retryable_uncertainty: bool


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _canonical_uuid(value: object, *, require_canonical_text: bool = False) -> uuid.UUID | None:
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None
    if require_canonical_text and value != str(parsed):
        return None
    return parsed


def evaluate_local_evidence(
    *,
    visit: SucceededVisit,
    record: Record,
    client: Client,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
    allowed_categories_raw: object,
    has_future_booking: bool,
) -> LocalEligibility:
    """Apply the exact local proven-subset contract in one stable order."""
    if record.provider != PROVIDER_EASYWEEK or client.provider != PROVIDER_EASYWEEK:
        return LocalEligibility(False, PROVIDER_MISMATCH, None)
    if record.company_id != company_id or client.company_id != company_id or visit.company_id != company_id:
        return LocalEligibility(False, COMPANY_MISMATCH, None)
    if record.easyweek_booking_uuid != visit.booking_uuid or record.altegio_record_id != visit.booking_id:
        return LocalEligibility(False, BOOKING_IDENTITY_MISMATCH, None)
    if record.client_id != client.id:
        return LocalEligibility(False, CUSTOMER_IDENTITY_MISMATCH, None)
    if record.altegio_client_id != visit.customer_id or client.altegio_client_id != visit.customer_id:
        return LocalEligibility(False, CUSTOMER_IDENTITY_MISMATCH, None)
    if record.starts_at is None:
        return LocalEligibility(False, RECORD_START_UNPROVEN, None)
    starts_at = _as_utc(record.starts_at)
    if not (_as_utc(period_start) <= starts_at < _as_utc(period_end)):
        return LocalEligibility(False, OUTSIDE_PERIOD, None)
    if type(visit.visits_total) is not int or visit.visits_total != 1:
        return LocalEligibility(False, SOURCE_VISITS_NOT_ONE, None)
    current = client.easyweek_visits_total
    if current is None:
        return LocalEligibility(False, CURRENT_VISITS_MISSING, None)
    if type(current) is not int or current != 1:
        return LocalEligibility(False, CURRENT_VISITS_NOT_ONE, None)
    if client.easyweek_visits_total_updated_at is None:
        return LocalEligibility(False, CURRENT_VISITS_UNSTAMPED, None)
    if record.is_deleted is not False:
        return LocalEligibility(False, DELETED_RECORD, None)
    category = evaluate_service_category(
        record_raw=record.raw,
        allowed_categories_raw=allowed_categories_raw,
    )
    if not category.allowed:
        return LocalEligibility(False, category.reason, None)
    phone = normalize_phone_candidate(client.phone_e164)
    if phone is None:
        return LocalEligibility(False, INVALID_PHONE, None)
    if client.wa_opted_out is not False:
        return LocalEligibility(False, OPTED_OUT, phone)
    if has_future_booking:
        return LocalEligibility(False, FUTURE_BOOKING, phone)
    return LocalEligibility(True, None, phone)


def evaluate_booking_response(
    payload: object,
    *,
    expected_booking_uuid: uuid.UUID,
    location: EasyWeekLocation,
) -> SourceBookingProof:
    """Project only the fields confirmed by the real GET booking response."""
    if not isinstance(payload, dict):
        return SourceBookingProof(False, BOOKING_RESPONSE_MALFORMED)
    observed_uuid = _canonical_uuid(payload.get("uuid"), require_canonical_text=True)
    if observed_uuid is None:
        return SourceBookingProof(False, BOOKING_RESPONSE_MALFORMED)
    if observed_uuid != expected_booking_uuid:
        return SourceBookingProof(False, BOOKING_UUID_MISMATCH)
    if payload.get("location_uuid") != location.location_uuid:
        return SourceBookingProof(False, BOOKING_LOCATION_MISMATCH)
    canceled = payload.get("is_canceled")
    if type(canceled) is not bool:
        return SourceBookingProof(False, BOOKING_RESPONSE_MALFORMED)
    if canceled:
        return SourceBookingProof(False, BOOKING_CANCELED)
    services = payload.get("ordered_services")
    if not isinstance(services, list) or len(services) != 1:
        return SourceBookingProof(False, BOOKING_SERVICE_COUNT_UNPROVEN)
    return SourceBookingProof(True, None)


def classify_booking_error(exc: Exception) -> SourceBookingProof:
    """Classify typed client errors without retaining their possibly sensitive text."""
    if isinstance(exc, EasyWeekRetryableError):
        return SourceBookingProof(False, BOOKING_RETRYABLE_UNAVAILABLE, retryable=True)
    if isinstance(exc, (EasyWeekConfigError, EasyWeekAuthError)):
        return SourceBookingProof(False, BOOKING_CONFIGURATION_UNAVAILABLE)
    if isinstance(exc, EasyWeekNotFoundError):
        return SourceBookingProof(False, BOOKING_NOT_FOUND)
    if isinstance(exc, EasyWeekProtocolError):
        return SourceBookingProof(False, BOOKING_RESPONSE_MALFORMED)
    if isinstance(exc, EasyWeekPermanentError):
        return SourceBookingProof(False, BOOKING_PERMANENT_ERROR)
    return SourceBookingProof(False, BOOKING_PERMANENT_ERROR)


async def _has_future_booking(
    session: AsyncSession,
    *,
    client_id: int,
    customer_id: int,
    company_id: int,
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


async def evaluate_recipient(
    session: AsyncSession,
    *,
    run: CampaignRun,
    recipient: CampaignRecipient,
    registry: EasyWeekLocationRegistry,
    allowed_categories_raw: object,
    client_reader: BookingReader | None,
    now: datetime,
) -> CampaignEligibilityResult:
    """Re-prove one durable recipient locally and, optionally, with one GET."""
    local_reason: str | None = None
    location = registry.locations.get(recipient.company_id) if registry.ready else None
    if location is None:
        local_reason = REGISTRY_UNAVAILABLE
    elif run.provider != PROVIDER_EASYWEEK or recipient.provider != PROVIDER_EASYWEEK:
        local_reason = PROVIDER_MISMATCH
    elif recipient.campaign_run_id != run.id or recipient.company_id not in (run.company_ids or []):
        local_reason = COMPANY_MISMATCH

    proof_values = (
        recipient.source_easyweek_event_id,
        recipient.source_record_id,
        recipient.source_booking_uuid,
        recipient.source_visits_total,
        recipient.source_visits_total_updated_at,
    )
    if local_reason is None and any(value is None for value in proof_values):
        local_reason = DURABLE_SOURCE_PROOF_UNPROVEN

    event = record = client = None
    visit: SucceededVisit | None = None
    if local_reason is None:
        event = await session.get(EasyWeekEvent, recipient.source_easyweek_event_id)
        record = await session.get(Record, recipient.source_record_id)
        client = await session.get(Client, recipient.client_id) if recipient.client_id is not None else None
        if event is None or record is None:
            local_reason = MISSING_RECORD if record is None else DURABLE_SOURCE_PROOF_UNPROVEN
        elif client is None:
            local_reason = MISSING_CLIENT

    if local_reason is None and event is not None:
        if event.status != "processed":
            local_reason = EVENT_NOT_PROCESSED
        else:
            try:
                visit = normalize_succeeded_visit_event(
                    event_hint=event.event_hint,
                    payload=event.payload,
                    body_truncated=bool(event.body_truncated),
                    location_registry=registry.locations,
                )
            except NormalizationError as exc:
                local_reason = exc.code

    if local_reason is None and visit is not None and record is not None and client is not None:
        if (
            recipient.source_easyweek_event_id != event.id
            or recipient.source_record_id != record.id
            or recipient.source_booking_uuid != visit.booking_uuid
            or recipient.source_visits_total != visit.visits_total
            or recipient.source_visits_total_updated_at != client.easyweek_visits_total_updated_at
            or recipient.client_id != client.id
        ):
            local_reason = DURABLE_SOURCE_PROOF_UNPROVEN
        else:
            future = await _has_future_booking(
                session,
                client_id=client.id,
                customer_id=visit.customer_id,
                company_id=recipient.company_id,
                now=now,
            )
            local = evaluate_local_evidence(
                visit=visit,
                record=record,
                client=client,
                company_id=recipient.company_id,
                period_start=run.period_start,
                period_end=run.period_end,
                allowed_categories_raw=allowed_categories_raw,
                has_future_booking=future,
            )
            local_reason = local.reason

    if local_reason is not None:
        return CampaignEligibilityResult(
            local_eligible=False,
            source_booking_current=False,
            send_ready=False,
            reasons=tuple(dict.fromkeys((local_reason, CAMPAIGN_LIVE_GUARD_UNPROVEN))),
            retryable_uncertainty=False,
        )

    assert visit is not None and location is not None
    if client_reader is None:
        return CampaignEligibilityResult(
            local_eligible=True,
            source_booking_current=False,
            send_ready=False,
            reasons=(CAMPAIGN_LIVE_GUARD_UNPROVEN,),
            retryable_uncertainty=False,
        )
    try:
        payload = await client_reader.get_booking(str(visit.booking_uuid))
    except Exception as exc:  # noqa: BLE001 - class only; exception text is never retained
        booking = classify_booking_error(exc)
    else:
        booking = evaluate_booking_response(
            payload,
            expected_booking_uuid=visit.booking_uuid,
            location=location,
        )
    reasons = tuple(
        dict.fromkeys(reason for reason in (booking.reason, CAMPAIGN_LIVE_GUARD_UNPROVEN) if reason is not None)
    )
    return CampaignEligibilityResult(
        local_eligible=True,
        source_booking_current=booking.current,
        send_ready=False,
        reasons=reasons,
        retryable_uncertainty=booking.retryable,
    )


__all__ = [
    "BOOKING_CANCELED",
    "BOOKING_NOT_FOUND",
    "BOOKING_RESPONSE_MALFORMED",
    "BOOKING_RETRYABLE_UNAVAILABLE",
    "BOOKING_SERVICE_COUNT_UNPROVEN",
    "BOOKING_UUID_MISMATCH",
    "BOOKING_LOCATION_MISMATCH",
    "CampaignEligibilityResult",
    "LocalEligibility",
    "SourceBookingProof",
    "classify_booking_error",
    "evaluate_booking_response",
    "evaluate_local_evidence",
    "evaluate_recipient",
]
