"""The operator's one bounded, read-only check of a zero-counter row.

What this answers
-----------------
"EasyWeek told us this person's visit counter was 0 — what does EasyWeek say
now?" Side by side: the number the delivery carried, and the number of completed
non-canceled visits in that customer's complete booking history. Plus whether
the exact source booking is among them, whether anything is booked ahead, and
the moment the answer was read.

What it does not answer
-----------------------
Whether anybody may be sent a voucher. The report says nothing is authorised,
every time, and it cannot: the automatic audience still needs the evidence the
ordinary rules ask for — a source and current counter of exactly 1 with a paired
timestamp — and looking at a row produces none of that. The one thing an
operator may do with a green answer is make a decision, and a decision is
recorded as ``operator_manual_selection`` through the existing single Add, where
it stays visibly a decision.

What it never does
------------------
Write. No status, no reason, no counter, no consent, no entitlement, no plan and
no durable diagnostic row. The database session is opened, read and closed
**before** the first network call, so a slow or failing EasyWeek never holds a
row lock or an advisory lock. The whole live phase is time-bounded and the
history read is page-bounded by the shared reader.

What the browser may say
------------------------
A run id and a recipient id. Not a customer UUID, not a phone number, not a
booking UUID — the server re-derives the whole chain from its own rows, because
"this is the customer" is exactly the claim an old delivery cannot make for
itself: §44.2 is explicit that a booking may since have been reassigned, so the
CURRENT card of the booking proves nothing on its own. The identity link used
here is the stored numeric-to-UUID correspondence on the local card, and the
live booking must still name that same customer.

Nothing in the report, and nothing logged by it, contains a name, a phone, a
UUID, a URL or a raw API body.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Final

from sqlalchemy import select

from altegio_bot.campaigns.easyweek_customer_history import (
    API_RETRYABLE_UNCERTAINTY,
    CUSTOMER_RESPONSE_MALFORMED,
    SOURCE_BOOKING_MALFORMED,
    CustomerHistoryReader,
    read_booking_binding,
    read_customer_bookings,
)
from altegio_bot.campaigns.easyweek_segment import ZeroVisitRow, resolve_zero_visit_row
from altegio_bot.campaigns.easyweek_zero_visit import ZERO_VISITS_REPORTED
from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
)
from altegio_bot.utils import utcnow

# -- stable, PII-free refusals ---------------------------------------------
# Two families on purpose. An operator reading "could not confirm" has learned
# that we will not say whose history this is; one reading "could not check" has
# learned that the history itself could not be trusted. Collapsing them would
# hide which of the two happened, and they call for different next steps.
RUN_NOT_FOUND: Final = "zero_visit_check_run_not_found"
RUN_NOT_PREVIEW: Final = "zero_visit_check_run_not_completed_easyweek_preview"
BRANCH_UNKNOWN: Final = "zero_visit_check_branch_unknown"
RECIPIENT_NOT_FOUND: Final = "zero_visit_check_recipient_not_found"
ROW_NOT_ZERO_DIAGNOSTIC: Final = "zero_visit_check_row_not_zero_diagnostic"
SOURCE_EVENT_UNPROVEN: Final = "zero_visit_check_source_event_unproven"
SOURCE_EVENT_AMBIGUOUS: Final = "zero_visit_check_source_event_ambiguous"
CUSTOMER_IDENTITY_UNPROVEN: Final = "zero_visit_check_customer_identity_unproven"
BOOKING_REASSIGNED: Final = "zero_visit_check_booking_reassigned"
HISTORY_UNVERIFIED: Final = "zero_visit_check_history_unverified"

# The whole live phase. The shared history reader bounds the page COUNT; this
# bounds the wall clock, so one unresponsive branch cannot pin an operator's
# request open indefinitely.
CHECK_TIMEOUT_SEC: Final = 20.0

# Between reads, so one operator's check cannot burst the API.
CHECK_PAUSE_SEC: Final = 0.2


@dataclass(frozen=True)
class ZeroVisitHistoryReport:
    """What the operator screen renders, and nothing more.

    ``ok`` means the check produced trustworthy facts: the person's identity was
    re-proven from our own rows and confirmed live, and the history came back
    complete. It is NOT an entitlement and never becomes one — see
    :meth:`as_safe_dict`, which repeats that on every answer, refusals included.

    Every count is ``None`` until it is known. An unread history is never
    reported as zero visits: that is the one mistake that would turn a failed
    read into "this person is new".
    """

    ok: bool
    run_id: int
    recipient_id: int
    reason: str | None = None
    # The underlying stable code from the shared reader, when there is one. It
    # says WHICH part of the read could not be trusted (incomplete pagination, a
    # 429/5xx, a malformed row) without the caller having to guess.
    detail_reason: str | None = None
    identity_confirmed: bool = False
    history_verified: bool = False
    # The number THIS delivery carried. Always 0 for a row this check accepts;
    # present so the screen can state it beside the live answer instead of
    # describing it in prose.
    event_visits_total: int | None = None
    completed_visit_count: int | None = None
    source_booking_completed_in_history: bool | None = None
    active_future_booking: bool | None = None
    pages_read: int = 0
    checked_at: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "mode": "read-only",
            "run_id": self.run_id,
            "recipient_id": self.recipient_id,
            "reason": self.reason,
            "detail_reason": self.detail_reason,
            "identity_confirmed": self.identity_confirmed,
            "history_verified": self.history_verified,
            "event_visits_total": self.event_visits_total,
            "completed_visit_count": self.completed_visit_count,
            "source_booking_completed_in_history": self.source_booking_completed_in_history,
            "active_future_booking": self.active_future_booking,
            "pages_read": self.pages_read,
            "checked_at": self.checked_at,
            # Repeated on every answer. Reading a row is not evidence, and a
            # green read does not move the row, the basis or the audience.
            "recipient_changed": False,
            "voucher_entitlement_granted": False,
            "campaign_send_authorized": False,
        }


@dataclass(frozen=True)
class _LocalFacts:
    source: ZeroVisitRow
    customer_uuid: uuid.UUID
    location_uuid: str


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


async def _local_facts(
    session_maker: Any,
    *,
    run_id: int,
    recipient_id: int,
    registry: EasyWeekLocationRegistry,
) -> tuple[_LocalFacts | None, str | None]:
    """Re-derive the row, the person and the source delivery from our own rows.

    Nothing the browser sent is trusted beyond the two ids, and even those are
    only used to LOOK UP: the provider, the branch, the period, the client and
    the originating booking are all re-proven here, through exactly the rule the
    segmenter used to attribute the delivery in the first place.
    """
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            return None, RUN_NOT_FOUND
        if run.provider != PROVIDER_EASYWEEK or run.mode != "preview" or run.status != "completed":
            return None, RUN_NOT_PREVIEW
        company_ids = list(run.company_ids or [])
        if len(company_ids) != 1:
            return None, RUN_NOT_PREVIEW
        company_id = int(company_ids[0])
        location = registry.locations.get(company_id) if registry.ready else None
        if location is None:
            return None, BRANCH_UNKNOWN

        recipient = await session.get(CampaignRecipient, recipient_id)
        if (
            recipient is None
            or recipient.campaign_run_id != run_id
            or recipient.provider != PROVIDER_EASYWEEK
            or recipient.company_id != company_id
        ):
            return None, RECIPIENT_NOT_FOUND
        # Only the excluded diagnostic row, and only while it has no earned
        # proof. A proven candidate has the ordinary preflight; a row an operator
        # already included is a decision, not a question.
        if (
            recipient.excluded_reason != ZERO_VISITS_REPORTED
            or recipient.recipient_basis != RECIPIENT_BASIS_EARNED
            or recipient.client_id is None
            or any(
                value is not None
                for value in (
                    recipient.source_easyweek_event_id,
                    recipient.source_record_id,
                    recipient.source_booking_uuid,
                    recipient.source_visits_total,
                    recipient.source_visits_total_updated_at,
                )
            )
        ):
            return None, ROW_NOT_ZERO_DIAGNOSTIC

        client = await session.get(Client, recipient.client_id)
        if client is None or client.provider != PROVIDER_EASYWEEK or client.company_id != company_id:
            return None, RECIPIENT_NOT_FOUND

        events = list(
            (
                await session.execute(
                    select(EasyWeekEvent)
                    .where(EasyWeekEvent.event_hint == "booking-succeeded")
                    .where(EasyWeekEvent.status == "processed")
                    .order_by(EasyWeekEvent.id.asc())
                )
            )
            .scalars()
            .all()
        )
        found: dict[uuid.UUID, ZeroVisitRow] = {}
        for event in events:
            payload = event.payload
            if not isinstance(payload, dict) or type(payload.get("location_id")) is not int:
                continue
            if payload.get("location_id") != company_id:
                continue
            row = await resolve_zero_visit_row(
                session,
                event=event,
                payload=payload,
                company_id=company_id,
                locations=registry.locations,
                period_start=run.period_start,
                period_end=run.period_end,
            )
            if row is None or row.client.id != client.id:
                continue
            found.setdefault(row.report.booking_uuid, row)

        if not found:
            # The row exists but the delivery behind it no longer attributes to
            # this person — reassigned, deleted, or moved out of the period.
            return None, SOURCE_EVENT_UNPROVEN
        if len(found) > 1:
            # Two different zero-counter bookings for one card. Which visit an
            # operator is being asked about is not a question to answer by
            # picking the lowest id.
            return None, SOURCE_EVENT_AMBIGUOUS
        source = next(iter(found.values()))

        # §44.2: the numeric id an old delivery names is linked to a workspace
        # customer only by an already stored correspondence. Without one there is
        # nothing to read a history for, and guessing from a phone number here
        # would be the cross-provider guess this integration forbids.
        if client.easyweek_customer_uuid is None:
            return None, CUSTOMER_IDENTITY_UNPROVEN
        return (
            _LocalFacts(
                source=source,
                customer_uuid=client.easyweek_customer_uuid,
                location_uuid=location.location_uuid,
            ),
            None,
        )


async def check_zero_visit_history(
    session_maker: Any,
    *,
    run_id: int,
    recipient_id: int,
    reader: CustomerHistoryReader,
    now: datetime | None = None,
    registry: EasyWeekLocationRegistry | None = None,
    pause: Callable[[float], Awaitable[None]] | None = None,
    pause_sec: float = CHECK_PAUSE_SEC,
) -> ZeroVisitHistoryReport:
    """Read-only: what the API says now about one zero-counter row's customer."""
    locations = registry or configured_easyweek_locations()
    facts, reason = await _local_facts(session_maker, run_id=run_id, recipient_id=recipient_id, registry=locations)
    clock = _as_utc(now or utcnow())
    if facts is None:
        return ZeroVisitHistoryReport(False, run_id, recipient_id, reason=reason, checked_at=clock.isoformat())

    reported = facts.source.report.reported_visits_total
    base = {
        "run_id": run_id,
        "recipient_id": recipient_id,
        "event_visits_total": reported,
        # Stated on every answer, success or not: an operator has to know WHEN
        # the API was asked, including when it could not be read.
        "checked_at": clock.isoformat(),
    }

    # Everything below is network, and the database session above is closed.
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SEC):
            try:
                booking_payload = await reader.get_booking(str(facts.source.report.booking_uuid))
            except Exception:  # noqa: BLE001 - a failed read is never a fact
                return ZeroVisitHistoryReport(
                    False,
                    reason=HISTORY_UNVERIFIED,
                    detail_reason=SOURCE_BOOKING_MALFORMED,
                    **base,
                )
            binding = read_booking_binding(booking_payload)
            if (
                binding.booking_uuid != facts.source.report.booking_uuid
                or binding.location_uuid != facts.location_uuid
                or binding.customer_uuid is None
            ):
                # Not the booking we asked about, not this branch, or a body that
                # does not name a customer at all. None of them is a reassignment
                # and none of them is a confirmation.
                return ZeroVisitHistoryReport(
                    False,
                    reason=HISTORY_UNVERIFIED,
                    detail_reason=SOURCE_BOOKING_MALFORMED,
                    **base,
                )
            if binding.customer_uuid != facts.customer_uuid:
                # The booking belongs to somebody else now. Following it would
                # read one person's history and show it against another's row.
                return ZeroVisitHistoryReport(False, reason=BOOKING_REASSIGNED, **base)

            bookings = await read_customer_bookings(
                reader,
                customer_uuid=facts.customer_uuid,
                pause=pause if pause is not None else asyncio.sleep,
                pause_sec=pause_sec,
            )
    except TimeoutError:
        return ZeroVisitHistoryReport(False, reason=HISTORY_UNVERIFIED, detail_reason=API_RETRYABLE_UNCERTAINTY, **base)

    if not bookings.identity_current:
        return ZeroVisitHistoryReport(
            False,
            reason=CUSTOMER_IDENTITY_UNPROVEN,
            detail_reason=bookings.reason or CUSTOMER_RESPONSE_MALFORMED,
            pages_read=bookings.pages_read,
            **base,
        )
    if not bookings.complete:
        # Incomplete pagination, a timeout, a 429, a 5xx, a malformed row. The
        # counts stay None: an unread history is unknown, never zero.
        return ZeroVisitHistoryReport(
            False,
            reason=HISTORY_UNVERIFIED,
            detail_reason=bookings.reason,
            identity_confirmed=True,
            pages_read=bookings.pages_read,
            **base,
        )

    completed = [row for row in bookings.bookings if row.is_completed and not row.is_canceled]
    source_in_history = any(row.booking_uuid == facts.source.report.booking_uuid for row in completed)
    future = any(not row.is_canceled and _as_utc(row.start_time) > clock for row in bookings.bookings)
    return ZeroVisitHistoryReport(
        True,
        reason=None,
        identity_confirmed=True,
        history_verified=True,
        completed_visit_count=len(completed),
        source_booking_completed_in_history=source_in_history,
        active_future_booking=future,
        pages_read=bookings.pages_read,
        **base,
    )


__all__ = [
    "BOOKING_REASSIGNED",
    "BRANCH_UNKNOWN",
    "CHECK_PAUSE_SEC",
    "CHECK_TIMEOUT_SEC",
    "CUSTOMER_IDENTITY_UNPROVEN",
    "HISTORY_UNVERIFIED",
    "RECIPIENT_NOT_FOUND",
    "ROW_NOT_ZERO_DIAGNOSTIC",
    "RUN_NOT_FOUND",
    "RUN_NOT_PREVIEW",
    "SOURCE_EVENT_AMBIGUOUS",
    "SOURCE_EVENT_UNPROVEN",
    "ZeroVisitHistoryReport",
    "check_zero_visit_history",
]
