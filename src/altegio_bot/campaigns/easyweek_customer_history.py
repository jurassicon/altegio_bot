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


async def _collect_customer_history(
    reader: CustomerHistoryReader,
    *,
    customer_uuid: uuid.UUID,
    paced: Callable[[], Awaitable[None]],
) -> tuple[dict[uuid.UUID, _Booking] | None, str | None, LiveProofDisposition, int]:
    """Every booking of one customer card, or why the set could not be trusted.

    Lifted out of :func:`prove_customer_booking_history` unchanged, so a second
    caller with a different POLICY can reuse the same READ. The strictness is
    the valuable part and must not be reimplemented anywhere else: the page
    index, the page size, ``last_page`` against ``total``, every page's row
    count, a page cap, a total that stays the same across pages, per-row
    identity, duplicate booking uuids, and finally the collected count against
    the declared total. Any one of them failing makes the whole set unusable,
    which is why this returns ``None`` instead of a short list.

    ``None`` is NEVER "this customer has no bookings". An empty history is an
    empty dict, behind a complete and structurally valid zero-row response.
    """
    rows: dict[uuid.UUID, _Booking] = {}
    expected_total: int | None = None
    expected_last: int | None = None
    pages_read = 0
    page = 1
    while True:
        await paced()
        try:
            payload = await reader.list_customer_bookings(
                str(customer_uuid), page=page, per_page=CUSTOMER_BOOKINGS_PER_PAGE
            )
        except Exception as exc:  # noqa: BLE001 - only typed classes are inspected
            result = _api_refusal(exc, stage="history", pages_read=pages_read)
            return None, result.reason, result.disposition, pages_read
        pages_read += 1
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("data"), list)
            or not isinstance(payload.get("meta"), dict)
        ):
            return None, HISTORY_PAGINATION_INCOMPLETE, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
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
            return None, HISTORY_PAGINATION_INCONSISTENT, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
        if last > MAX_CUSTOMER_HISTORY_PAGES:
            return None, HISTORY_PAGINATION_UNBOUNDED, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
        if expected_total is None:
            expected_total, expected_last = total, last
        elif total != expected_total or last != expected_last:
            return None, HISTORY_PAGINATION_INCONSISTENT, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
        expected_count = 0 if total == 0 else min(CUSTOMER_BOOKINGS_PER_PAGE, total - (page - 1) * per_page)
        if expected_count < 0 or len(data) != expected_count:
            return None, HISTORY_PAGINATION_INCOMPLETE, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
        for raw_row in data:
            row, row_reason = _history_row(raw_row, customer_uuid=customer_uuid)
            if row is None:
                return (
                    None,
                    row_reason or HISTORY_ROW_MALFORMED,
                    LiveProofDisposition.PERMANENT_REFUSAL,
                    pages_read,
                )
            if row.booking_uuid in rows:
                return None, HISTORY_DUPLICATE_BOOKING_UUID, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
            rows[row.booking_uuid] = row
        if page == last:
            break
        page += 1

    if expected_total is None or len(rows) != expected_total:
        return None, HISTORY_PAGINATION_INCOMPLETE, LiveProofDisposition.PERMANENT_REFUSAL, pages_read
    return rows, None, LiveProofDisposition.READY, pages_read


@dataclass(frozen=True)
class CustomerBookingSet:
    """One customer's complete booking set, for a caller with its own policy.

    ``complete`` is the only licence to draw a negative conclusion from
    ``bookings``. While it is ``False`` the caller knows nothing about this
    customer's bookings — not "none", not "some" — and ``reason`` says which
    part of the read could not be trusted.
    """

    complete: bool
    identity_current: bool
    bookings: tuple[_Booking, ...] = ()
    reason: str | None = None
    disposition: LiveProofDisposition = LiveProofDisposition.PERMANENT_REFUSAL
    pages_read: int = 0

    @property
    def retryable_uncertainty(self) -> bool:
        return self.disposition is LiveProofDisposition.RETRYABLE_UNCERTAINTY


async def read_customer_bookings(
    reader: CustomerHistoryReader,
    *,
    customer_uuid: uuid.UUID,
    pause: Callable[[float], Awaitable[None]] | None = None,
    pause_sec: float = 0.0,
) -> CustomerBookingSet:
    """Prove one customer card's identity, then read every booking it has.

    Why this exists beside :func:`prove_customer_booking_history`
    ------------------------------------------------------------
    That function answers PR-15's question — "is this person still a first-time
    visitor, with exactly one completed visit and nothing booked ahead?" — and
    its ``completed_visits_not_one`` refusal is part of that answer rather than
    a fault in the data. A §46.2 follow-up audience asks something different
    about people who already earned and were sent a voucher: they may well have
    come back since, and a second completed visit must not read as a failed
    proof. Reusing the PR-15 verdict would have excluded exactly the recipients
    a follow-up is about, for a reason belonging to another campaign's
    eligibility rule.

    So the READ is shared and the POLICY is not. This returns the facts; the
    caller decides what they mean.

    Keyed on the customer uuid alone, with no ``location`` argument, and
    deliberately: EasyWeek's customer booking list is workspace-wide, so a
    booking at any configured branch of the same workspace is visible here.
    That is what §46.5 asks of the booking question, which spans branches even
    though the send audience does not.

    Strictly read-only — ``get_customer`` and ``list_customer_bookings``, no
    mutation, no database access and no lock. The caller must not hold a row
    lock or an advisory lock across this call.
    """

    async def paced() -> None:
        if pause is not None:
            await pause(pause_sec)

    try:
        customer_payload = await reader.get_customer(str(customer_uuid))
    except Exception as exc:  # noqa: BLE001 - only typed classes are inspected
        result = _api_refusal(exc, stage="customer")
        return CustomerBookingSet(
            complete=False,
            identity_current=False,
            reason=result.reason,
            disposition=result.disposition,
        )
    observed = _customer_uuid(customer_payload)
    if observed is None:
        return CustomerBookingSet(complete=False, identity_current=False, reason=CUSTOMER_RESPONSE_MALFORMED)
    if observed != customer_uuid:
        # The card this uuid resolves to belongs to somebody else. Never
        # silently followed: §46.1 forbids substituting the addressee.
        return CustomerBookingSet(complete=False, identity_current=False, reason=CUSTOMER_UUID_MISMATCH)

    rows, reason, disposition, pages_read = await _collect_customer_history(
        reader, customer_uuid=customer_uuid, paced=paced
    )
    if rows is None:
        return CustomerBookingSet(
            complete=False,
            identity_current=True,
            reason=reason,
            disposition=disposition,
            pages_read=pages_read,
        )
    return CustomerBookingSet(
        complete=True,
        identity_current=True,
        bookings=tuple(rows.values()),
        disposition=LiveProofDisposition.READY,
        pages_read=pages_read,
    )


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

    rows_or_none, collect_reason, collect_disposition, pages_read = await _collect_customer_history(
        reader, customer_uuid=source.customer_uuid, paced=paced
    )
    if rows_or_none is None:
        return _refusal(
            collect_reason or HISTORY_PAGINATION_INCOMPLETE,
            disposition=collect_disposition,
            source_booking_current=True,
            customer_identity_current=True,
            pages_read=pages_read,
        )
    rows = rows_or_none
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
