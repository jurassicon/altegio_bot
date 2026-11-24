"""The operator's one bounded, read-only check of a zero-counter row.

What this answers
-----------------
"EasyWeek told us this person's visit counter was 0 — what does EasyWeek say
now?" Side by side: the number the delivery carried, and the number of completed
non-canceled visits in that customer's complete booking history. Plus what
became of the exact source booking, whether anything is booked ahead, and the
moment the answer was read.

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
Write. No status, no reason, no counter, no consent, no entitlement, no UUID, no
phone, no plan and no durable diagnostic row. Every database session is opened,
read and closed **before** the first network call, so a slow or failing EasyWeek
never holds a row lock or an advisory lock. One deadline covers the local phase
and the live phase together, and the history read is page-bounded by the shared
reader.

Which delivery is "the source"
------------------------------
The one the preview recorded when it created the row, read back from the row's
own binding — never re-derived by scanning the archive. Re-deriving answered a
different question than the row was created from: a LATER zero-counter booking
could become the source of an EARLIER row, and a replay or a reassignment could
silently move it. If the stored binding is missing, or the chain behind it no
longer proves out, the answer is a refusal rather than a substitute source.

What the browser may say
------------------------
A run id and a recipient id. Not a customer UUID, not a phone number, not a
booking UUID — the server re-derives the whole chain from its own rows, because
"this is the customer" is exactly the claim an old delivery cannot make for
itself: §44.2 is explicit that a booking may since have been reassigned, so the
CURRENT card of the booking proves nothing on its own.

The two identity branches §44.2 allows, and only those
------------------------------------------------------
1. The stored numeric-to-UUID correspondence on the local card, when there is
   one. The live booking must still name that same customer.
2. Otherwise the contact **this delivery itself captured**, proven the long way:
   a complete workspace-wide lookup of that number, exactly one customer on it,
   a direct read of that customer's card, and agreement with the source record.
   The captured number must be the one ingestion stored on the source client, so
   it is historical evidence rather than the current owner's details, and a
   number shared with a second local card refuses outright.

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
    LiveProofDisposition,
    classify_read_failure,
    read_booking_binding,
    read_customer_bookings,
)
from altegio_bot.campaigns.easyweek_eligibility import prove_event_record_client_link
from altegio_bot.campaigns.easyweek_manual_recipient import CUSTOMER_AMBIGUOUS, prove_customer_identity
from altegio_bot.campaigns.easyweek_zero_visit import (
    ZERO_VISITS_REPORTED,
    ZeroVisitSourceBinding,
    normalize_zero_visit_event,
    payload_reports_zero_visits_total,
    read_zero_visit_source_meta,
)
from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.easyweek_normalizer import NormalizationError, captured_customer_phone
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    Record,
)
from altegio_bot.utils import utcnow

# -- stable, PII-free refusals ---------------------------------------------
# Three families on purpose, because they call for three different next steps.
#
#   "Не удалось подтвердить"  — we will not say whose history this is.
#   "Не удалось проверить"    — the history itself could not be trusted.
#   "Временно недоступно"     — nothing is wrong with the data; try again.
#
# Collapsing any two of them would hide which happened. In particular a 429, a
# 5xx or a timeout must never read as "this person's identity is in doubt", and
# an identity conflict must never read as "try again later", because retrying
# cannot resolve it.
RUN_NOT_FOUND: Final = "zero_visit_check_run_not_found"
RUN_NOT_PREVIEW: Final = "zero_visit_check_run_not_completed_easyweek_preview"
BRANCH_UNKNOWN: Final = "zero_visit_check_branch_unknown"
RECIPIENT_NOT_FOUND: Final = "zero_visit_check_recipient_not_found"
ROW_NOT_ZERO_DIAGNOSTIC: Final = "zero_visit_check_row_not_zero_diagnostic"
# The row was this diagnostic and an operator has since decided about it,
# possibly in another tab. Its own code: the screen must be able to say "this is
# already a decision, reload" instead of claiming the person is still excluded.
ROW_ALREADY_DECIDED: Final = "zero_visit_check_row_already_decided"
# No provable binding to a delivery — a row from before this path existed, or
# one whose stored binding cannot be read. "Источник не установлен".
SOURCE_NOT_ESTABLISHED: Final = "zero_visit_check_source_not_established"
SOURCE_EVENT_UNPROVEN: Final = "zero_visit_check_source_event_unproven"
SOURCE_EVENT_AMBIGUOUS: Final = "zero_visit_check_source_event_ambiguous"
CUSTOMER_IDENTITY_UNPROVEN: Final = "zero_visit_check_customer_identity_unproven"
CUSTOMER_IDENTITY_AMBIGUOUS: Final = "zero_visit_check_customer_identity_ambiguous"
BOOKING_REASSIGNED: Final = "zero_visit_check_booking_reassigned"
HISTORY_UNVERIFIED: Final = "zero_visit_check_history_unverified"
TEMPORARILY_UNAVAILABLE: Final = "zero_visit_check_temporarily_unavailable"

# Which of the two §44.2 branches produced the customer, reported so an operator
# knows whether they are looking at a stored correspondence or at one proven
# from the contact the delivery captured.
IDENTITY_VIA_STORED_UUID: Final = "stored_numeric_uuid_binding"
IDENTITY_VIA_CAPTURED_CONTACT: Final = "captured_contact_workspace_lookup"

# What became of the exact booking the delivery named. Four answers, not two:
# "present in the history" and "counted as a completed visit" are different
# facts, and a canceled or unfinished source booking is present without being
# counted. Reporting only the second made those two cases indistinguishable
# from a booking that is not in the history at all.
SOURCE_STATE_COMPLETED: Final = "completed_visit"
SOURCE_STATE_CANCELED: Final = "canceled"
SOURCE_STATE_NOT_COMPLETED: Final = "not_completed"
SOURCE_STATE_ABSENT: Final = "absent"

# The whole check — the local re-proof and every network read together. The
# shared history reader bounds the page COUNT; this bounds the wall clock, so
# neither a slow database nor an unresponsive branch can pin an operator's
# request open indefinitely.
CHECK_TIMEOUT_SEC: Final = 20.0

# Between reads, so one operator's check cannot burst the API.
CHECK_PAUSE_SEC: Final = 0.2

# What the row was while the check ran, as the server read it — not as the page
# that drew the button assumed.
ROW_STATE_EXCLUDED: Final = "excluded_zero_visit_diagnostic"


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
    # True only for a 429, a 5xx, a timeout or an exhausted retry chain. The
    # screen may suggest trying again exactly when this is true.
    retryable: bool = False
    # Which §44.2 branch proved the customer, when one did.
    identity_source: str | None = None
    identity_confirmed: bool = False
    history_verified: bool = False
    # The number THIS delivery carried. Always 0 for a row this check accepts;
    # present so the screen can state it beside the live answer instead of
    # describing it in prose.
    event_visits_total: int | None = None
    completed_visit_count: int | None = None
    source_booking_in_history: bool | None = None
    source_booking_state: str | None = None
    active_future_booking: bool | None = None
    pages_read: int = 0
    checked_at: str | None = None
    # What the row actually was while this ran. Absent on a refusal that never
    # got as far as reading the row.
    row_state: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "mode": "read-only",
            "run_id": self.run_id,
            "recipient_id": self.recipient_id,
            "reason": self.reason,
            "detail_reason": self.detail_reason,
            "retryable": self.retryable,
            "identity_source": self.identity_source,
            "identity_confirmed": self.identity_confirmed,
            "history_verified": self.history_verified,
            "event_visits_total": self.event_visits_total,
            "completed_visit_count": self.completed_visit_count,
            "source_booking_in_history": self.source_booking_in_history,
            "source_booking_state": self.source_booking_state,
            "active_future_booking": self.active_future_booking,
            "pages_read": self.pages_read,
            "checked_at": self.checked_at,
            "row_state": self.row_state,
            # Repeated on every answer. Reading a row is not evidence, and a
            # green read does not move the row, the basis or the audience.
            "recipient_changed": False,
            "voucher_entitlement_granted": False,
            "campaign_send_authorized": False,
        }


@dataclass(frozen=True)
class _LocalFacts:
    """Everything the live phase needs, read once and then let go of."""

    binding: ZeroVisitSourceBinding
    client_id: int
    location_uuid: str
    # Exactly one of the two is set: the stored correspondence, or the contact
    # this delivery captured and that ingestion stored on the source client.
    stored_customer_uuid: uuid.UUID | None
    captured_phone: str | None
    company_id: int


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


async def _local_facts(
    session_maker: Any,
    *,
    run_id: int,
    recipient_id: int,
    registry: EasyWeekLocationRegistry,
) -> tuple[_LocalFacts | None, str | None]:
    """Re-prove the row, the person and the recorded delivery from our own rows.

    Nothing the browser sent is trusted beyond the two ids, and even those are
    only used to LOOK UP: the provider, the branch, the period, the client and
    the originating booking are all re-proven here, through exactly the rule the
    segmenter used to attribute the delivery in the first place.

    Bounded by construction: the run, the recipient, the client, the ONE stored
    event and the ONE stored record, each by primary key. Nothing scans the
    event archive and nothing resolves another customer's rows, so a branch with
    a hundred thousand deliveries costs an operator's click exactly what an
    empty one does.
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
        # A row an operator already decided about is a decision, not a question.
        # Its own refusal, so a tab that still shows the button can say "this is
        # already included, reload" rather than "still excluded". Only an
        # operator's own selection counts as that: any other basis is simply not
        # this diagnostic and is refused as such below.
        if recipient.recipient_basis == RECIPIENT_BASIS_MANUAL:
            return None, ROW_ALREADY_DECIDED
        if recipient.recipient_basis != RECIPIENT_BASIS_EARNED:
            return None, ROW_NOT_ZERO_DIAGNOSTIC
        # Only the excluded diagnostic row, and only while it has no earned
        # proof. A proven candidate has the ordinary preflight.
        if (
            recipient.excluded_reason != ZERO_VISITS_REPORTED
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

        # The source the PREVIEW recorded. Not a fresh derivation: a later
        # delivery must not be able to become this row's source.
        binding = read_zero_visit_source_meta(recipient.meta)
        if binding is None:
            return None, SOURCE_NOT_ESTABLISHED
        if binding.distinct_sources != 1:
            # The preview already saw more than one zero-counter booking for
            # this card. Which visit the operator is being asked about has no
            # single answer, and that was true when the row was written.
            return None, SOURCE_EVENT_AMBIGUOUS

        event = await session.get(EasyWeekEvent, binding.event_id)
        if (
            event is None
            or event.event_hint != "booking-succeeded"
            or event.status != "processed"
            or not isinstance(event.payload, dict)
            or type(event.payload.get("location_id")) is not int
            or event.payload.get("location_id") != company_id
            or not payload_reports_zero_visits_total(event.payload)
        ):
            return None, SOURCE_EVENT_UNPROVEN
        try:
            report = normalize_zero_visit_event(
                event_hint=event.event_hint,
                payload=event.payload,
                body_truncated=bool(event.body_truncated),
                location_registry=registry.locations,
            )
        except NormalizationError:
            return None, SOURCE_EVENT_UNPROVEN
        if (report.booking_uuid, report.booking_id, report.customer_id) != (
            binding.booking_uuid,
            binding.booking_id,
            binding.customer_id,
        ):
            # The stored event no longer says what the row was written from.
            return None, SOURCE_EVENT_UNPROVEN

        record = await session.get(Record, binding.record_id)
        if record is None or record.is_deleted is not False:
            return None, SOURCE_EVENT_UNPROVEN
        # The whole chain again, through the segmenter's own rule: provider,
        # company, booking uuid and numeric id, customer id on both the record
        # and the card, and the record's start inside the campaign's half-open
        # period. A record reassigned to another client, or one that has moved
        # out of the period, stops here.
        if (
            prove_event_record_client_link(
                booking=report,
                record=record,
                client=client,
                company_id=company_id,
                period_start=run.period_start,
                period_end=run.period_end,
            )
            is not None
        ):
            return None, SOURCE_EVENT_UNPROVEN

        stored_uuid = client.easyweek_customer_uuid
        captured_phone: str | None = None
        if stored_uuid is None:
            # §44.2 branch two. The contact must be the one THIS delivery
            # captured and the one ingestion stored on this card: a number that
            # does not match the card is not evidence about this client, and the
            # current owner's details are explicitly not a substitute.
            captured_phone = captured_customer_phone(event.payload)
            if captured_phone is None or client.phone_e164 != captured_phone:
                return None, CUSTOMER_IDENTITY_UNPROVEN
            shared = await session.scalar(
                select(Client.id)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.company_id == company_id)
                .where(Client.phone_e164 == captured_phone)
                .where(Client.id != client.id)
                .limit(1)
            )
            if shared is not None:
                # Two local cards on one number in one branch. Reading a history
                # for it would be reading a history for "one of two people".
                return None, CUSTOMER_IDENTITY_AMBIGUOUS
        return (
            _LocalFacts(
                binding=binding,
                client_id=client.id,
                location_uuid=location.location_uuid,
                stored_customer_uuid=stored_uuid,
                captured_phone=captured_phone,
                company_id=company_id,
            ),
            None,
        )


async def _uuid_is_shared_locally(
    session_maker: Any,
    *,
    company_id: int,
    client_id: int,
    customer_uuid: uuid.UUID,
) -> bool:
    """Does another local card in this branch already hold this UUID?

    Asked after the live proof, in a session of its own, so no lock is held
    across a network call. A number proving out to a customer another card
    already owns is an identity conflict: the two cards are either the same
    person recorded twice or two people sharing a number, and neither licenses
    showing one of them the other's history.
    """
    async with session_maker() as session:
        found = await session.scalar(
            select(Client.id)
            .where(Client.provider == PROVIDER_EASYWEEK)
            .where(Client.company_id == company_id)
            .where(Client.easyweek_customer_uuid == customer_uuid)
            .where(Client.id != client_id)
            .limit(1)
        )
    return found is not None


async def _prove_live_customer(
    facts: _LocalFacts,
    *,
    reader: Any,
    session_maker: Any,
) -> tuple[uuid.UUID | None, str, str | None, bool]:
    """``(customer_uuid, identity_source, refusal, retryable)``.

    Branch one is already proven locally and needs no read. Branch two is the
    full §44.2 proof, reusing the existing workspace lookup and direct card read
    rather than restating either: complete pagination, exactly one customer on
    the number, and the card read back at that number.
    """
    if facts.stored_customer_uuid is not None:
        return facts.stored_customer_uuid, IDENTITY_VIA_STORED_UUID, None, False
    assert facts.captured_phone is not None  # noqa: S101 - the local phase set exactly one
    outcome = await prove_customer_identity(reader, phone=facts.captured_phone, require_name=False)
    if outcome.customer is None:
        if outcome.retryable:
            return None, IDENTITY_VIA_CAPTURED_CONTACT, TEMPORARILY_UNAVAILABLE, True
        if outcome.reason == CUSTOMER_AMBIGUOUS:
            return None, IDENTITY_VIA_CAPTURED_CONTACT, CUSTOMER_IDENTITY_AMBIGUOUS, False
        return None, IDENTITY_VIA_CAPTURED_CONTACT, CUSTOMER_IDENTITY_UNPROVEN, False
    try:
        proven = uuid.UUID(outcome.customer.uuid)
    except (ValueError, AttributeError, TypeError):
        return None, IDENTITY_VIA_CAPTURED_CONTACT, CUSTOMER_IDENTITY_UNPROVEN, False
    if await _uuid_is_shared_locally(
        session_maker,
        company_id=facts.company_id,
        client_id=facts.client_id,
        customer_uuid=proven,
    ):
        return None, IDENTITY_VIA_CAPTURED_CONTACT, CUSTOMER_IDENTITY_AMBIGUOUS, False
    return proven, IDENTITY_VIA_CAPTURED_CONTACT, None, False


def _source_state(bookings: tuple[Any, ...], *, booking_uuid: uuid.UUID) -> tuple[bool, str]:
    """Is the source booking in the history, and what is it there?"""
    for row in bookings:
        if row.booking_uuid != booking_uuid:
            continue
        if row.is_canceled:
            return True, SOURCE_STATE_CANCELED
        if not row.is_completed:
            return True, SOURCE_STATE_NOT_COMPLETED
        return True, SOURCE_STATE_COMPLETED
    return False, SOURCE_STATE_ABSENT


async def _check(
    session_maker: Any,
    *,
    run_id: int,
    recipient_id: int,
    reader: CustomerHistoryReader,
    clock: datetime,
    registry: EasyWeekLocationRegistry,
    pause: Callable[[float], Awaitable[None]] | None,
    pause_sec: float,
) -> ZeroVisitHistoryReport:
    facts, reason = await _local_facts(session_maker, run_id=run_id, recipient_id=recipient_id, registry=registry)
    if facts is None:
        return ZeroVisitHistoryReport(False, run_id, recipient_id, reason=reason, checked_at=clock.isoformat())

    base: dict[str, Any] = {
        "run_id": run_id,
        "recipient_id": recipient_id,
        "event_visits_total": facts.binding.reported_visits_total,
        # Stated on every answer, success or not: an operator has to know WHEN
        # the API was asked, including when it could not be read.
        "checked_at": clock.isoformat(),
        # What the row was while this ran, read by the server. The screen states
        # this instead of assuming the row is still what the button implied.
        "row_state": ROW_STATE_EXCLUDED,
    }

    # Everything below is network, and every database session above is closed.
    customer_uuid, identity_source, refusal, retryable = await _prove_live_customer(
        facts, reader=reader, session_maker=session_maker
    )
    if customer_uuid is None:
        return ZeroVisitHistoryReport(
            False,
            reason=refusal,
            retryable=retryable,
            identity_source=identity_source,
            **base,
        )

    try:
        booking_payload = await reader.get_booking(str(facts.binding.booking_uuid))
    except Exception as exc:  # noqa: BLE001 - a failed read is never a fact
        detail, disposition = classify_read_failure(exc, stage="source")
        temporary = disposition is LiveProofDisposition.RETRYABLE_UNCERTAINTY
        return ZeroVisitHistoryReport(
            False,
            reason=TEMPORARILY_UNAVAILABLE if temporary else HISTORY_UNVERIFIED,
            detail_reason=detail,
            retryable=temporary,
            identity_source=identity_source,
            **base,
        )
    binding = read_booking_binding(booking_payload)
    if (
        binding.booking_uuid != facts.binding.booking_uuid
        or binding.location_uuid != facts.location_uuid
        or binding.customer_uuid is None
    ):
        # Not the booking we asked about, not this branch, or a body that does
        # not name a customer at all. None of them is a reassignment and none of
        # them is a confirmation.
        return ZeroVisitHistoryReport(
            False,
            reason=HISTORY_UNVERIFIED,
            detail_reason=SOURCE_BOOKING_MALFORMED,
            identity_source=identity_source,
            **base,
        )
    if binding.customer_uuid != customer_uuid:
        # The booking belongs to somebody else now. Following it would read one
        # person's history and show it against another's row. Not temporary and
        # never retried: this is an answer, not an outage.
        return ZeroVisitHistoryReport(False, reason=BOOKING_REASSIGNED, identity_source=identity_source, **base)

    bookings = await read_customer_bookings(
        reader,
        customer_uuid=customer_uuid,
        pause=pause if pause is not None else asyncio.sleep,
        pause_sec=pause_sec,
    )
    if not bookings.identity_current:
        temporary = bookings.retryable_uncertainty
        return ZeroVisitHistoryReport(
            False,
            reason=TEMPORARILY_UNAVAILABLE if temporary else CUSTOMER_IDENTITY_UNPROVEN,
            detail_reason=bookings.reason or CUSTOMER_RESPONSE_MALFORMED,
            retryable=temporary,
            identity_source=identity_source,
            pages_read=bookings.pages_read,
            **base,
        )
    if not bookings.complete:
        # Incomplete pagination, a timeout, a 429, a 5xx, a malformed row. The
        # counts stay None: an unread history is unknown, never zero.
        temporary = bookings.retryable_uncertainty
        return ZeroVisitHistoryReport(
            False,
            reason=TEMPORARILY_UNAVAILABLE if temporary else HISTORY_UNVERIFIED,
            detail_reason=bookings.reason,
            retryable=temporary,
            identity_source=identity_source,
            identity_confirmed=True,
            pages_read=bookings.pages_read,
            **base,
        )

    completed = [row for row in bookings.bookings if row.is_completed and not row.is_canceled]
    in_history, state = _source_state(bookings.bookings, booking_uuid=facts.binding.booking_uuid)
    future = any(not row.is_canceled and _as_utc(row.start_time) > clock for row in bookings.bookings)
    return ZeroVisitHistoryReport(
        True,
        reason=None,
        identity_source=identity_source,
        identity_confirmed=True,
        history_verified=True,
        completed_visit_count=len(completed),
        source_booking_in_history=in_history,
        source_booking_state=state,
        active_future_booking=future,
        pages_read=bookings.pages_read,
        **base,
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
    """Read-only: what the API says now about one zero-counter row's customer.

    One deadline over the whole thing. It used to cover only the network, which
    left the local re-proof able to run long before the clock even started; an
    operator's request must be bounded end to end, whichever half is slow.
    """
    locations = registry or configured_easyweek_locations()
    clock = _as_utc(now or utcnow())
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SEC):
            return await _check(
                session_maker,
                run_id=run_id,
                recipient_id=recipient_id,
                reader=reader,
                clock=clock,
                registry=locations,
                pause=pause,
                pause_sec=pause_sec,
            )
    except TimeoutError:
        return ZeroVisitHistoryReport(
            False,
            run_id,
            recipient_id,
            reason=TEMPORARILY_UNAVAILABLE,
            detail_reason=API_RETRYABLE_UNCERTAINTY,
            retryable=True,
            checked_at=clock.isoformat(),
        )


__all__ = [
    "BOOKING_REASSIGNED",
    "BRANCH_UNKNOWN",
    "CHECK_PAUSE_SEC",
    "CHECK_TIMEOUT_SEC",
    "CUSTOMER_IDENTITY_AMBIGUOUS",
    "CUSTOMER_IDENTITY_UNPROVEN",
    "HISTORY_UNVERIFIED",
    "IDENTITY_VIA_CAPTURED_CONTACT",
    "IDENTITY_VIA_STORED_UUID",
    "RECIPIENT_NOT_FOUND",
    "ROW_ALREADY_DECIDED",
    "ROW_NOT_ZERO_DIAGNOSTIC",
    "ROW_STATE_EXCLUDED",
    "RUN_NOT_FOUND",
    "RUN_NOT_PREVIEW",
    "SOURCE_EVENT_AMBIGUOUS",
    "SOURCE_EVENT_UNPROVEN",
    "SOURCE_NOT_ESTABLISHED",
    "SOURCE_STATE_ABSENT",
    "SOURCE_STATE_CANCELED",
    "SOURCE_STATE_COMPLETED",
    "SOURCE_STATE_NOT_COMPLETED",
    "TEMPORARILY_UNAVAILABLE",
    "ZeroVisitHistoryReport",
    "check_zero_visit_history",
]
