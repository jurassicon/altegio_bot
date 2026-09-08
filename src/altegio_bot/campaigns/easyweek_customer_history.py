"""Strict read-only EasyWeek customer booking-history proof for PR-15."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Final, Protocol

from altegio_bot.easyweek_client import (
    CUSTOMER_BOOKINGS_PER_PAGE,
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_locations import EasyWeekLocation

MAX_CUSTOMER_HISTORY_PAGES: Final = 50

SOURCE_BOOKING_MALFORMED: Final = "easyweek_campaign_source_booking_malformed"
SOURCE_BOOKING_NOT_COMPLETED: Final = "easyweek_campaign_source_booking_not_completed"
SOURCE_CUSTOMER_UUID_UNPROVEN: Final = "easyweek_campaign_source_customer_uuid_unproven"
BOOKING_UUID_MISMATCH: Final = "easyweek_campaign_booking_uuid_mismatch"
BOOKING_LOCATION_MISMATCH: Final = "easyweek_campaign_booking_location_mismatch"
BOOKING_CANCELED: Final = "easyweek_campaign_booking_canceled"
BOOKING_SERVICE_COUNT_UNPROVEN: Final = "easyweek_campaign_booking_service_count_unproven"
BOOKING_NOT_FOUND: Final = "easyweek_campaign_booking_not_found"
CUSTOMER_NOT_FOUND: Final = "easyweek_campaign_customer_not_found"
CUSTOMER_UUID_MISMATCH: Final = "easyweek_campaign_customer_uuid_mismatch"
CUSTOMER_RESPONSE_MALFORMED: Final = "easyweek_campaign_customer_response_malformed"
HISTORY_UNREADABLE: Final = "easyweek_campaign_customer_history_unreadable"
HISTORY_PAGINATION_INCOMPLETE: Final = "easyweek_campaign_history_pagination_incomplete"
HISTORY_PAGINATION_INCONSISTENT: Final = "easyweek_campaign_history_pagination_inconsistent"
HISTORY_PAGINATION_UNBOUNDED: Final = "easyweek_campaign_history_pagination_unbounded"
HISTORY_ROW_MALFORMED: Final = "easyweek_campaign_history_row_malformed"
HISTORY_IDENTITY_MISMATCH: Final = "easyweek_campaign_history_identity_mismatch"
HISTORY_DUPLICATE_BOOKING_UUID: Final = "easyweek_campaign_history_duplicate_booking_uuid"
HISTORY_SOURCE_BOOKING_ABSENT: Final = "easyweek_campaign_source_booking_absent_from_history"
HISTORY_SOURCE_BOOKING_STATE_MISMATCH: Final = "easyweek_campaign_source_booking_state_mismatch"
HISTORY_COMPLETED_VISITS_NOT_ONE: Final = "easyweek_campaign_completed_visits_not_one"
HISTORY_ACTIVE_FUTURE_BOOKING: Final = "easyweek_campaign_active_future_booking"
API_CONFIGURATION_UNAVAILABLE: Final = "easyweek_campaign_api_configuration_unavailable"
API_RETRYABLE_UNCERTAINTY: Final = "easyweek_campaign_api_retryable_uncertainty"


class LiveProofDisposition(str, Enum):
    READY = "ready"
    BUSINESS_EXCLUSION = "business_exclusion"
    PERMANENT_REFUSAL = "permanent_refusal"
    RETRYABLE_UNCERTAINTY = "retryable_uncertainty"


class CustomerHistoryReader(Protocol):
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]: ...

    async def list_customer_bookings(
        self, customer_uuid: str, page: int, per_page: int = CUSTOMER_BOOKINGS_PER_PAGE
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class CustomerLiveProof:
    source_booking_current: bool = False
    customer_identity_current: bool = False
    history_complete: bool = False
    completed_visit_count: int | None = None
    first_visit_current: bool = False
    no_active_future_booking: bool = False
    live_guard_ready: bool = False
    retryable_uncertainty: bool = False
    reason: str | None = None
    disposition: LiveProofDisposition = LiveProofDisposition.PERMANENT_REFUSAL
    pages_read: int = 0


@dataclass(frozen=True)
class _Booking:
    booking_uuid: uuid.UUID
    customer_uuid: uuid.UUID
    is_canceled: bool
    is_completed: bool
    start_time: datetime


def _canonical_uuid(value: object) -> uuid.UUID | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None
    return parsed if value == str(parsed) else None


def _aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _refusal(
    reason: str,
    *,
    disposition: LiveProofDisposition = LiveProofDisposition.PERMANENT_REFUSAL,
    source_booking_current: bool = False,
    customer_identity_current: bool = False,
    history_complete: bool = False,
    completed_visit_count: int | None = None,
    first_visit_current: bool = False,
    no_active_future_booking: bool = False,
    pages_read: int = 0,
) -> CustomerLiveProof:
    return CustomerLiveProof(
        source_booking_current=source_booking_current,
        customer_identity_current=customer_identity_current,
        history_complete=history_complete,
        completed_visit_count=completed_visit_count,
        first_visit_current=first_visit_current,
        no_active_future_booking=no_active_future_booking,
        retryable_uncertainty=disposition is LiveProofDisposition.RETRYABLE_UNCERTAINTY,
        reason=reason,
        disposition=disposition,
        pages_read=pages_read,
    )


def _api_refusal(exc: Exception, *, stage: str, pages_read: int = 0) -> CustomerLiveProof:
    if isinstance(exc, EasyWeekRetryableError):
        return _refusal(
            API_RETRYABLE_UNCERTAINTY,
            disposition=LiveProofDisposition.RETRYABLE_UNCERTAINTY,
            pages_read=pages_read,
        )
    if isinstance(exc, (EasyWeekConfigError, EasyWeekAuthError)):
        return _refusal(API_CONFIGURATION_UNAVAILABLE, pages_read=pages_read)
    if stage == "customer" and isinstance(exc, EasyWeekNotFoundError):
        return _refusal(CUSTOMER_NOT_FOUND, source_booking_current=True, pages_read=pages_read)
    if stage == "customer":
        return _refusal(CUSTOMER_RESPONSE_MALFORMED, source_booking_current=True, pages_read=pages_read)
    if stage == "history":
        return _refusal(
            HISTORY_UNREADABLE,
            source_booking_current=True,
            customer_identity_current=True,
            pages_read=pages_read,
        )
    if isinstance(exc, EasyWeekNotFoundError):
        return _refusal(BOOKING_NOT_FOUND, pages_read=pages_read)
    return _refusal(SOURCE_BOOKING_MALFORMED, pages_read=pages_read)


def _source_booking(
    payload: object,
    *,
    expected_booking_uuid: uuid.UUID,
    location: EasyWeekLocation,
) -> tuple[_Booking | None, str | None, LiveProofDisposition]:
    if not isinstance(payload, dict):
        return None, SOURCE_BOOKING_MALFORMED, LiveProofDisposition.PERMANENT_REFUSAL
    booking_uuid = _canonical_uuid(payload.get("uuid"))
    if booking_uuid is None:
        return None, SOURCE_BOOKING_MALFORMED, LiveProofDisposition.PERMANENT_REFUSAL
    if booking_uuid != expected_booking_uuid:
        return None, BOOKING_UUID_MISMATCH, LiveProofDisposition.PERMANENT_REFUSAL
    if payload.get("location_uuid") != location.location_uuid:
        return None, BOOKING_LOCATION_MISMATCH, LiveProofDisposition.PERMANENT_REFUSAL
    canceled = payload.get("is_canceled")
    completed = payload.get("is_completed")
    if type(canceled) is not bool or type(completed) is not bool:
        return None, SOURCE_BOOKING_MALFORMED, LiveProofDisposition.PERMANENT_REFUSAL
    if canceled:
        return None, BOOKING_CANCELED, LiveProofDisposition.BUSINESS_EXCLUSION
    if not completed:
        return None, SOURCE_BOOKING_NOT_COMPLETED, LiveProofDisposition.BUSINESS_EXCLUSION
    services = payload.get("ordered_services")
    if not isinstance(services, list) or len(services) != 1:
        return None, BOOKING_SERVICE_COUNT_UNPROVEN, LiveProofDisposition.PERMANENT_REFUSAL
    customer = payload.get("customer")
    customer_uuid = _canonical_uuid(customer.get("uuid")) if isinstance(customer, dict) else None
    if customer_uuid is None:
        return None, SOURCE_CUSTOMER_UUID_UNPROVEN, LiveProofDisposition.PERMANENT_REFUSAL
    start_time = _aware_datetime(payload.get("start_time"))
    if start_time is None:
        return None, SOURCE_BOOKING_MALFORMED, LiveProofDisposition.PERMANENT_REFUSAL
    return _Booking(booking_uuid, customer_uuid, canceled, completed, start_time), None, LiveProofDisposition.READY


def _customer_uuid(payload: object) -> uuid.UUID | None:
    if isinstance(payload, dict) and "data" in payload:
        payload = payload.get("data")
    return _canonical_uuid(payload.get("uuid")) if isinstance(payload, dict) else None


def _history_row(payload: object, *, customer_uuid: uuid.UUID) -> tuple[_Booking | None, str | None]:
    if not isinstance(payload, dict):
        return None, HISTORY_ROW_MALFORMED
    booking_uuid = _canonical_uuid(payload.get("uuid"))
    customer = payload.get("customer")
    observed_customer_uuid = _canonical_uuid(customer.get("uuid")) if isinstance(customer, dict) else None
    canceled = payload.get("is_canceled")
    completed = payload.get("is_completed")
    start_time = _aware_datetime(payload.get("start_time"))
    if booking_uuid is None or observed_customer_uuid is None:
        return None, HISTORY_ROW_MALFORMED
    if observed_customer_uuid != customer_uuid:
        return None, HISTORY_IDENTITY_MISMATCH
    if type(canceled) is not bool or type(completed) is not bool or start_time is None:
        return None, HISTORY_ROW_MALFORMED
    return _Booking(booking_uuid, observed_customer_uuid, canceled, completed, start_time), None


def _meta_value(meta: dict[str, Any], key: str) -> int | None:
    value = meta.get(key)
    return value if type(value) is int else None


async def prove_customer_booking_history(
    reader: CustomerHistoryReader,
    *,
    expected_booking_uuid: uuid.UUID,
    location: EasyWeekLocation,
    now: datetime,
    pause: Callable[[float], Awaitable[None]] | None = None,
    pause_sec: float = 0.0,
) -> CustomerLiveProof:
    """Read and strictly reconcile one EasyWeek customer card and all bookings."""

    async def paced() -> None:
        if pause is not None:
            await pause(pause_sec)

    try:
        direct_payload = await reader.get_booking(str(expected_booking_uuid))
    except Exception as exc:  # noqa: BLE001 - only typed classes are inspected
        return _api_refusal(exc, stage="source")
    source, reason, disposition = _source_booking(
        direct_payload,
        expected_booking_uuid=expected_booking_uuid,
        location=location,
    )
    if source is None:
        return _refusal(reason or SOURCE_BOOKING_MALFORMED, disposition=disposition)

    await paced()
    try:
        customer_payload = await reader.get_customer(str(source.customer_uuid))
    except Exception as exc:  # noqa: BLE001
        result = _api_refusal(exc, stage="customer")
        return CustomerLiveProof(**{**result.__dict__, "source_booking_current": True})
    observed_customer_uuid = _customer_uuid(customer_payload)
    if observed_customer_uuid is None:
        return _refusal(CUSTOMER_RESPONSE_MALFORMED, source_booking_current=True)
    if observed_customer_uuid != source.customer_uuid:
        return _refusal(CUSTOMER_UUID_MISMATCH, source_booking_current=True)

    rows: dict[uuid.UUID, _Booking] = {}
    expected_total: int | None = None
    expected_last: int | None = None
    pages_read = 0
    page = 1
    while True:
        await paced()
        try:
            payload = await reader.list_customer_bookings(
                str(source.customer_uuid), page=page, per_page=CUSTOMER_BOOKINGS_PER_PAGE
            )
        except Exception as exc:  # noqa: BLE001
            result = _api_refusal(exc, stage="history", pages_read=pages_read)
            return CustomerLiveProof(
                **{
                    **result.__dict__,
                    "source_booking_current": True,
                    "customer_identity_current": True,
                }
            )
        pages_read += 1
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("data"), list)
            or not isinstance(payload.get("meta"), dict)
        ):
            return _refusal(
                HISTORY_PAGINATION_INCOMPLETE,
                source_booking_current=True,
                customer_identity_current=True,
                pages_read=pages_read,
            )
        data = payload["data"]
        meta = payload["meta"]
        current = _meta_value(meta, "current_page")
        last = _meta_value(meta, "last_page")
        per_page = _meta_value(meta, "per_page")
        total = _meta_value(meta, "total")
        if (
            current != page
            or last is None
            or last < 1
            or per_page != CUSTOMER_BOOKINGS_PER_PAGE
            or total is None
            or total < 0
            or last != max(1, (total + CUSTOMER_BOOKINGS_PER_PAGE - 1) // CUSTOMER_BOOKINGS_PER_PAGE)
        ):
            return _refusal(
                HISTORY_PAGINATION_INCONSISTENT,
                source_booking_current=True,
                customer_identity_current=True,
                pages_read=pages_read,
            )
        if last > MAX_CUSTOMER_HISTORY_PAGES:
            return _refusal(
                HISTORY_PAGINATION_UNBOUNDED,
                source_booking_current=True,
                customer_identity_current=True,
                pages_read=pages_read,
            )
        if expected_total is None:
            expected_total, expected_last = total, last
        elif total != expected_total or last != expected_last:
            return _refusal(
                HISTORY_PAGINATION_INCONSISTENT,
                source_booking_current=True,
                customer_identity_current=True,
                pages_read=pages_read,
            )
        expected_count = 0 if total == 0 else min(CUSTOMER_BOOKINGS_PER_PAGE, total - (page - 1) * per_page)
        if expected_count < 0 or len(data) != expected_count:
            return _refusal(
                HISTORY_PAGINATION_INCOMPLETE,
                source_booking_current=True,
                customer_identity_current=True,
                pages_read=pages_read,
            )
        for raw_row in data:
            row, row_reason = _history_row(raw_row, customer_uuid=source.customer_uuid)
            if row is None:
                return _refusal(
                    row_reason or HISTORY_ROW_MALFORMED,
                    source_booking_current=True,
                    customer_identity_current=True,
                    pages_read=pages_read,
                )
            if row.booking_uuid in rows:
                return _refusal(
                    HISTORY_DUPLICATE_BOOKING_UUID,
                    source_booking_current=True,
                    customer_identity_current=True,
                    pages_read=pages_read,
                )
            rows[row.booking_uuid] = row
        if page == last:
            break
        page += 1

    if expected_total is None or len(rows) != expected_total:
        return _refusal(
            HISTORY_PAGINATION_INCOMPLETE,
            source_booking_current=True,
            customer_identity_current=True,
            pages_read=pages_read,
        )
    history_source = rows.get(source.booking_uuid)
    if history_source is None:
        return _refusal(
            HISTORY_SOURCE_BOOKING_ABSENT,
            source_booking_current=True,
            customer_identity_current=True,
            history_complete=True,
            pages_read=pages_read,
        )
    if (history_source.is_canceled, history_source.is_completed) != (source.is_canceled, source.is_completed):
        return _refusal(
            HISTORY_SOURCE_BOOKING_STATE_MISMATCH,
            source_booking_current=True,
            customer_identity_current=True,
            history_complete=True,
            pages_read=pages_read,
        )
    completed_count = sum(row.is_completed and not row.is_canceled for row in rows.values())
    first_visit_current = completed_count == 1
    if not first_visit_current:
        return _refusal(
            HISTORY_COMPLETED_VISITS_NOT_ONE,
            disposition=LiveProofDisposition.BUSINESS_EXCLUSION,
            source_booking_current=True,
            customer_identity_current=True,
            history_complete=True,
            completed_visit_count=completed_count,
            pages_read=pages_read,
        )
    now_utc = now.astimezone(timezone.utc) if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    has_active_future = any(
        not row.is_canceled and row.start_time.astimezone(timezone.utc) > now_utc for row in rows.values()
    )
    if has_active_future:
        return _refusal(
            HISTORY_ACTIVE_FUTURE_BOOKING,
            disposition=LiveProofDisposition.BUSINESS_EXCLUSION,
            source_booking_current=True,
            customer_identity_current=True,
            history_complete=True,
            completed_visit_count=completed_count,
            first_visit_current=True,
            pages_read=pages_read,
        )
    return CustomerLiveProof(
        source_booking_current=True,
        customer_identity_current=True,
        history_complete=True,
        completed_visit_count=completed_count,
        first_visit_current=True,
        no_active_future_booking=True,
        live_guard_ready=True,
        disposition=LiveProofDisposition.READY,
        pages_read=pages_read,
    )
