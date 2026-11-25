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
1. The stored numeric-to-UUID correspondence, read **workspace-wide** through
   the same rule the webhook bridge uses. One customer UUID may legitimately
   hold cards in several proven branches, so a correspondence stored on another
   branch's card identifies this person here as well; reading only the branch
   card treated a known person as unknown. The correspondence is checked in both
   directions: two different UUIDs against one numeric id is a conflict, and so
   is one UUID carrying two different numeric ids. The live booking must still
   name the same customer.
2. Otherwise the contact **one of this booking's deliveries captured**, proven
   the long way: a complete workspace-wide lookup of that number under this
   module's stricter coverage contract, exactly one customer on it, a direct
   read of that customer's card, and agreement with the source record. Which
   delivery supplies the contact was decided by the preview and recorded on the
   row; the captured number must still be the one ingestion stored on the source
   client, so it is historical evidence rather than the current owner's details,
   and a number shared with a second local card — or claimed anywhere in the
   workspace for a different owner — refuses outright. A row that does not carry
   the preview's proof that exactly ONE contact was captured for the booking is
   refused with its own reason: the number on the card cannot distinguish a
   booking with a single captured contact from one whose deliveries disagreed.

Every network call is a window, and the webhook bridge writes while they are
open, so the local identity facts — this branch's card included — are read again
after the lookup, after the source booking read, and after the history read. A
correspondence that appeared mid-call and names somebody else, a UUID that has
since been bound to another numeric customer, a second card that has taken the
proven UUID, a local card that now claims the proof's number, and a card whose
contact has since changed all stop the check. The first two rechecks stop it
before a single page of anybody's history is read; the last one stops a
confirmation that was already known to be out of date from reaching an operator.

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
    CUSTOMER_UUID_MISMATCH,
    SOURCE_BOOKING_MALFORMED,
    CustomerHistoryReader,
    LiveProofDisposition,
    classify_read_failure,
    read_booking_binding,
    read_customer_bookings,
)
from altegio_bot.campaigns.easyweek_eligibility import prove_event_record_client_link
from altegio_bot.campaigns.easyweek_manual_identity import (
    known_numeric_uuid_binding,
    phone_identity_conflict,
    uuid_numeric_conflict,
)
from altegio_bot.campaigns.easyweek_manual_recipient import CUSTOMER_AMBIGUOUS, prove_customer_identity
from altegio_bot.campaigns.easyweek_zero_visit import (
    ZERO_VISITS_REPORTED,
    ZeroVisitSourceBinding,
    captured_contact_phone,
    normalize_zero_visit_event,
    payload_reports_zero_visits_total,
    read_zero_visit_source_meta,
)
from altegio_bot.campaigns.easyweek_zero_visit_lookup import (
    LOOKUP_CARD_MALFORMED,
    LOOKUP_CARD_UUID_MISMATCH,
    DiagnosticReader,
)
from altegio_bot.easyweek_client import EasyWeekConfigError, EasyWeekError, EasyWeekProtocolError
from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.easyweek_normalizer import NormalizationError
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
# The workspace itself disagrees about who this numeric customer is, or a second
# card already owns the UUID the lookup proved. Its own code because it is the
# one identity refusal that is neither "we could not read enough" nor "two
# people share a number": the stored correspondence and the live answer point at
# different customers, and no retry and no further read can settle it.
IDENTITY_UUID_CONFLICT: Final = "zero_visit_check_identity_uuid_conflict"
# Which direction of the correspondence disagreed, as a quotable detail. The
# reason an operator reads is the same — the workspace cannot say who this
# customer is — but the two are different facts, and a report that cannot name
# which one it hit is a report nobody can act on.
NUMERIC_OWNER_CONFLICT: Final = "zero_visit_check_numeric_owner_conflict"
CONTACT_OWNER_CONFLICT: Final = "zero_visit_check_contact_owner_conflict"
# The row holds no proof that exactly one contact was captured for this booking.
# Rows written before that proof existed are exactly this case: the number on
# the card may be the one a single delivery captured, or it may be one of
# several the deliveries disagreed about, and the row cannot tell us which. A
# stored numeric↔UUID correspondence is still a valid basis; a bare phone is not.
CONTACT_EVIDENCE_MISSING: Final = "zero_visit_check_contact_evidence_missing"
BOOKING_REASSIGNED: Final = "zero_visit_check_booking_reassigned"
HISTORY_UNVERIFIED: Final = "zero_visit_check_history_unverified"
TEMPORARILY_UNAVAILABLE: Final = "zero_visit_check_temporarily_unavailable"
# A permanent 4xx from EasyWeek — 401, 403, 404, 422 — or an unusable API
# configuration. The data may be fine; the call is not allowed or the resource
# is not there. Kept apart from the temporary family precisely so the screen
# never offers a retry that cannot work.
API_REFUSED: Final = "zero_visit_check_api_refused"
# A 2xx whose body is not the shape the endpoint promises. Also not temporary:
# repeating a well-formed wrong answer returns the same wrong answer.
API_RESPONSE_MALFORMED: Final = "zero_visit_check_api_response_malformed"
# The integration itself is not usable: no API key, no workspace slug, a base
# URL that is not the EasyWeek API. Its own code because "EasyWeek did not
# answer" sends an operator to look at a branch, and a branch cannot fix our
# configuration — nobody even asked it anything.
API_NOT_CONFIGURED: Final = "zero_visit_check_api_not_configured"
# The deadline expired while the check was still reading our OWN rows, before a
# single external call was attempted. Its own code because reporting it as
# "EasyWeek did not answer" blames a branch that was never asked, and an
# operator would go looking at the wrong system.
LOCAL_DEADLINE: Final = "zero_visit_check_local_deadline"
LOCAL_DEADLINE_DETAIL: Final = "zero_visit_check_local_phase_timeout"
# The deadline expired on our own rows AFTER EasyWeek had already answered — a
# slow database during the identity recheck, say. EasyWeek answered, so blaming
# it is wrong; the check still did not finish, so a confirmation would be a
# guess. Hence a neutral code of its own: our side ran out of time.
DEADLINE_AFTER_READ: Final = "zero_visit_check_deadline_after_api_read"
DEADLINE_AFTER_READ_DETAIL: Final = "zero_visit_check_local_phase_timeout_after_read"

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
    # one of this booking's deliveries captured and that ingestion stored on the
    # source client.
    stored_customer_uuid: uuid.UUID | None
    captured_phone: str | None
    company_id: int


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


async def _captured_contact(
    session: Any,
    *,
    event: EasyWeekEvent,
    binding: ZeroVisitSourceBinding,
    company_id: int,
    registry: EasyWeekLocationRegistry,
) -> tuple[str | None, str]:
    """The contact a delivery of THIS booking captured, or why there is none.

    Which delivery is not decided here. The preview decided it, while it still
    had the archive in front of it, and recorded two things: how many different
    contacts the booking's deliveries carried, and — when the one contact came
    from a sibling delivery rather than from the zero-counter one — which
    delivery that was. Re-deciding now would let a delivery that arrived AFTER
    the preview supply the contact, which is exactly the kind of substitution
    §44.2 forbids.

    ``contact_sources`` of anything but one is refused rather than resolved:
    zero means nothing captured a contact for this booking, and more than one
    means the deliveries contradict each other about who it was.

    A row that does not carry the count at all — one written before the count
    existed — is refused too, with a reason of its own. It used to fall back to
    the zero-counter delivery's own captured number, and that fallback cannot
    tell the two worlds apart: a booking whose single delivery captured one
    number, and a booking whose deliveries captured several and happened to
    include this one. The second is precisely the case §44.2 forbids resolving,
    so a bare number is not a licence to go looking the person up. What such a
    row still has is the other branch — a stored numeric↔UUID correspondence —
    and that branch never reaches this function. Re-deriving the count now is
    not an option either: deliveries that arrived after the preview would
    decide it, which is the substitution this whole path refuses.
    """
    if binding.contact_sources is None:
        return None, CONTACT_EVIDENCE_MISSING
    if binding.contact_sources != 1:
        return None, CUSTOMER_IDENTITY_UNPROVEN
    contact_event = event
    if binding.contact_event_id is not None and binding.contact_event_id != binding.event_id:
        found = await session.get(EasyWeekEvent, binding.contact_event_id)
        if found is None or found.status != "processed":
            return None, CUSTOMER_IDENTITY_UNPROVEN
        contact_event = found
    phone = captured_contact_phone(
        event_hint=contact_event.event_hint,
        payload=contact_event.payload,
        body_truncated=bool(contact_event.body_truncated),
        location_registry=registry.locations,
        company_id=company_id,
        booking_uuid=binding.booking_uuid,
        booking_id=binding.booking_id,
        customer_id=binding.customer_id,
    )
    return (phone, CUSTOMER_IDENTITY_UNPROVEN)


async def _local_facts(
    session_maker: Any,
    *,
    run_id: int,
    recipient_id: int,
    registry: EasyWeekLocationRegistry,
) -> tuple[_LocalFacts | None, str | None, str | None]:
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
            return None, RUN_NOT_FOUND, None
        if run.provider != PROVIDER_EASYWEEK or run.mode != "preview" or run.status != "completed":
            return None, RUN_NOT_PREVIEW, None
        company_ids = list(run.company_ids or [])
        if len(company_ids) != 1:
            return None, RUN_NOT_PREVIEW, None
        company_id = int(company_ids[0])
        location = registry.locations.get(company_id) if registry.ready else None
        if location is None:
            return None, BRANCH_UNKNOWN, None

        recipient = await session.get(CampaignRecipient, recipient_id)
        if (
            recipient is None
            or recipient.campaign_run_id != run_id
            or recipient.provider != PROVIDER_EASYWEEK
            or recipient.company_id != company_id
        ):
            return None, RECIPIENT_NOT_FOUND, None
        # A row an operator already decided about is a decision, not a question.
        # Its own refusal, so a tab that still shows the button can say "this is
        # already included, reload" rather than "still excluded". Only an
        # operator's own selection counts as that: any other basis is simply not
        # this diagnostic and is refused as such below.
        if recipient.recipient_basis == RECIPIENT_BASIS_MANUAL:
            return None, ROW_ALREADY_DECIDED, None
        if recipient.recipient_basis != RECIPIENT_BASIS_EARNED:
            return None, ROW_NOT_ZERO_DIAGNOSTIC, None
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
            return None, ROW_NOT_ZERO_DIAGNOSTIC, None

        client = await session.get(Client, recipient.client_id)
        if client is None or client.provider != PROVIDER_EASYWEEK or client.company_id != company_id:
            return None, RECIPIENT_NOT_FOUND, None

        # The source the PREVIEW recorded. Not a fresh derivation: a later
        # delivery must not be able to become this row's source.
        binding = read_zero_visit_source_meta(recipient.meta)
        if binding is None:
            return None, SOURCE_NOT_ESTABLISHED, None
        if binding.distinct_sources != 1:
            # The preview already saw more than one zero-counter booking for
            # this card. Which visit the operator is being asked about has no
            # single answer, and that was true when the row was written.
            return None, SOURCE_EVENT_AMBIGUOUS, None

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
            return None, SOURCE_EVENT_UNPROVEN, None
        try:
            report = normalize_zero_visit_event(
                event_hint=event.event_hint,
                payload=event.payload,
                body_truncated=bool(event.body_truncated),
                location_registry=registry.locations,
            )
        except NormalizationError:
            return None, SOURCE_EVENT_UNPROVEN, None
        if (report.booking_uuid, report.booking_id, report.customer_id) != (
            binding.booking_uuid,
            binding.booking_id,
            binding.customer_id,
        ):
            # The stored event no longer says what the row was written from.
            return None, SOURCE_EVENT_UNPROVEN, None

        record = await session.get(Record, binding.record_id)
        if record is None or record.is_deleted is not False:
            return None, SOURCE_EVENT_UNPROVEN, None
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
            return None, SOURCE_EVENT_UNPROVEN, None

        # §44.2 branch one, read the way the webhook bridge reads it: the
        # numeric↔UUID correspondence is a property of the WORKSPACE, not of one
        # branch's card. One customer UUID may hold cards in several proven
        # branches, so a correspondence already stored on another branch's card
        # identifies this person here too — treating them as unknown would send
        # a known identity down the path that has to prove one from a contact.
        known_uuid, workspace_conflict = await known_numeric_uuid_binding(session, binding.customer_id)
        if workspace_conflict:
            # Two different UUIDs stored against one numeric customer id. The
            # webhook refuses this outright and so does this: the workspace
            # holds contradictory bindings and neither can be preferred.
            return None, IDENTITY_UUID_CONFLICT, NUMERIC_OWNER_CONFLICT
        branch_uuid = client.easyweek_customer_uuid
        if branch_uuid is not None and known_uuid is not None and branch_uuid != known_uuid:
            return None, IDENTITY_UUID_CONFLICT, NUMERIC_OWNER_CONFLICT
        stored_uuid = branch_uuid or known_uuid
        if stored_uuid is not None and await uuid_numeric_conflict(
            session, customer_uuid=stored_uuid, customer_id=binding.customer_id
        ):
            # The other direction of the same correspondence: this UUID is
            # already bound to a different numeric customer somewhere in the
            # workspace. One UUID may hold cards in several proven branches, but
            # they are one person with one numeric id — two of them means the
            # workspace is describing two people as one, and the booking history
            # of a UUID cannot be attributed to either.
            return None, IDENTITY_UUID_CONFLICT, NUMERIC_OWNER_CONFLICT
        captured_phone: str | None = None
        if stored_uuid is None:
            # §44.2 branch two. The contact must be one THIS BOOKING's
            # deliveries captured and the one ingestion stored on this card: a
            # number that does not match the card is not evidence about this
            # client, and the current owner's details are explicitly not a
            # substitute.
            captured_phone, contact_reason = await _captured_contact(
                session,
                event=event,
                binding=binding,
                company_id=company_id,
                registry=registry,
            )
            if captured_phone is None:
                return None, contact_reason, None
            if client.phone_e164 != captured_phone:
                return None, CUSTOMER_IDENTITY_UNPROVEN, None
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
                return None, CUSTOMER_IDENTITY_AMBIGUOUS, None
            if await phone_identity_conflict(
                session,
                phone=captured_phone,
                customer_id=binding.customer_id,
                exclude_client_id=client.id,
            ):
                # A card anywhere in the workspace carries this number for a
                # different numeric customer. Our own records already disagree
                # about whose number it is, so whatever the workspace lookup
                # answers cannot be attached to this person — and the number is
                # never sent anywhere to find that out.
                return None, IDENTITY_UUID_CONFLICT, CONTACT_OWNER_CONFLICT
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
            None,
        )


async def _recheck_local_identity(
    session_maker: Any,
    *,
    facts: _LocalFacts,
    customer_uuid: uuid.UUID,
) -> tuple[str | None, str | None]:
    """Read the local identity facts again, after each network call.

    Why again
    ---------
    The lookup is a network call, and the webhook bridge writes while it is in
    flight. A real sequence: the check starts with no correspondence stored
    anywhere, so it goes looking for the person by the contact this booking
    captured; mid-lookup an ingesting delivery proves and stores ``N → A`` on
    this very card; the lookup comes back with ``B``. Every fact the check read
    at the start was true when it read it, and the answer it is about to act on
    is now wrong. Checking the card a second time is the only thing that
    catches it, and it has to include the branch card itself — a recheck that
    only looked at other branches would miss the write that landed here.

    The lookup is not the only window. The source booking read is another
    network call, and so is every page of the history: a webhook that proves a
    different UUID for this client while either is in flight leaves the check
    holding an answer about somebody else. So this runs after each of them — and
    after the history read as well, because serving a confirmation that was
    already known to be out of date is the one outcome worse than refusing.

    Still no lock held across the network: a short session of its own each
    time, after the reads, exactly like the first one.

    Six ways it can fail, none of them retryable:
    * the workspace now holds contradictory correspondences for this numeric id;
    * a correspondence now exists and names somebody other than the customer the
      lookup proved;
    * that customer UUID is now bound to a different numeric customer somewhere;
    * another card in this branch already owns that customer, so the two cards
      are either one person twice or two people sharing a number;
    * a local card now claims the proof's number for a different owner; or
    * the card no longer carries the contact the proof was built on, which means
      the historical evidence this check used is no longer this card's.
    """
    async with session_maker() as session:
        client = await session.get(Client, facts.client_id)
        if client is None or client.provider != PROVIDER_EASYWEEK or client.company_id != facts.company_id:
            return RECIPIENT_NOT_FOUND, None
        known_uuid, workspace_conflict = await known_numeric_uuid_binding(session, facts.binding.customer_id)
        if workspace_conflict:
            return IDENTITY_UUID_CONFLICT, NUMERIC_OWNER_CONFLICT
        for candidate in (client.easyweek_customer_uuid, known_uuid):
            if candidate is not None and candidate != customer_uuid:
                return IDENTITY_UUID_CONFLICT, NUMERIC_OWNER_CONFLICT
        if facts.captured_phone is not None:
            if client.phone_e164 != facts.captured_phone:
                return CUSTOMER_IDENTITY_UNPROVEN, None
            twin = await session.scalar(
                select(Client.id)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.company_id == facts.company_id)
                .where(Client.phone_e164 == facts.captured_phone)
                .where(Client.id != facts.client_id)
                .limit(1)
            )
            if twin is not None:
                # A second card on this number in this branch, created while the
                # call was in flight. The same rule the first read applied, so
                # the window cannot be used to get past it.
                return CUSTOMER_IDENTITY_AMBIGUOUS, None
            if await phone_identity_conflict(
                session,
                phone=facts.captured_phone,
                customer_id=facts.binding.customer_id,
                customer_uuid=customer_uuid,
                exclude_client_id=facts.client_id,
            ):
                return IDENTITY_UUID_CONFLICT, CONTACT_OWNER_CONFLICT
        shared = await session.scalar(
            select(Client.id)
            .where(Client.provider == PROVIDER_EASYWEEK)
            .where(Client.company_id == facts.company_id)
            .where(Client.easyweek_customer_uuid == customer_uuid)
            .where(Client.id != facts.client_id)
            .limit(1)
        )
        if shared is not None:
            # A second card in THIS branch on that customer: asked first,
            # because "two cards here, one customer" is the more specific thing
            # to tell an operator than "the workspace disagrees".
            return CUSTOMER_IDENTITY_AMBIGUOUS, None
        if await uuid_numeric_conflict(session, customer_uuid=customer_uuid, customer_id=facts.binding.customer_id):
            return IDENTITY_UUID_CONFLICT, NUMERIC_OWNER_CONFLICT
    return None, None


def _api_family(exc: Exception, *, stage: str) -> tuple[str, str | None, bool]:
    """``(reason, detail, retryable)`` for one failed EasyWeek read.

    Five families, never collapsed, because each one tells an operator to do
    something different:

    * a 429, a 5xx, a timeout or an exhausted retry chain — temporary, retry;
    * an unusable API configuration — nobody was asked anything, and no branch
      can fix it;
    * a permanent 4xx (401/403/404/422) — the call is refused or the resource is
      not there, and retrying cannot help;
    * a 2xx whose body is not the promised shape — a contract problem; and
    * anything our own strictness refused, which is handled by the caller.

    The stable code comes from the shared classifier, so the detail an operator
    can quote is the same one the PR-15 proof reports for the same failure.
    """
    detail, disposition = classify_read_failure(exc, stage=stage)
    if disposition is LiveProofDisposition.RETRYABLE_UNCERTAINTY:
        return TEMPORARILY_UNAVAILABLE, detail, True
    if isinstance(exc, EasyWeekConfigError):
        # Missing key, missing workspace slug, a base URL that is not the
        # EasyWeek API. The request was never made, so this is not a refusal by
        # EasyWeek and not an outage at EasyWeek — it is ours.
        return API_NOT_CONFIGURED, detail, False
    if isinstance(exc, EasyWeekProtocolError):
        return API_RESPONSE_MALFORMED, detail, False
    if isinstance(exc, EasyWeekError):
        return API_REFUSED, detail, False
    return API_RESPONSE_MALFORMED, detail, False


def _lookup_refusal(reader: DiagnosticReader, outcome_reason: str | None) -> tuple[str, str | None, bool]:
    """Why the phone lookup produced no identity, in the operator's terms.

    The shared proof deliberately answers with one refusal for every unreadable
    lookup, which is right for a path that only needs "no". A diagnostic has to
    say WHICH, so the typed exception the wrapper kept and the coverage refusal
    it recorded are consulted here. Our own strictness is never reported as an
    EasyWeek failure: a listing we would not trust is an unproven identity, not
    an outage.
    """
    if outcome_reason == CUSTOMER_AMBIGUOUS:
        return CUSTOMER_IDENTITY_AMBIGUOUS, None, False
    exc = reader.failure
    if isinstance(exc, EasyWeekError):
        # A typed failure is reported before our own strictness: a 429 that
        # stopped the walk did not prove the listing incoherent, it prevented us
        # from reading it.
        return _api_family(exc, stage="customer")
    if reader.refusal == LOOKUP_CARD_MALFORMED:
        # The direct card read answered 200 with a body that is not a card.
        # A format problem, not an identity one — and reading it again would
        # return the same well-formed wrong answer.
        return API_RESPONSE_MALFORMED, LOOKUP_CARD_MALFORMED, False
    if reader.refusal == LOOKUP_CARD_UUID_MISMATCH:
        # A readable card — for a different customer than the one asked about.
        # That is an answer about identity, kept apart from the shape problem
        # above because the two send an operator to different places.
        return IDENTITY_UUID_CONFLICT, LOOKUP_CARD_UUID_MISMATCH, False
    if reader.refusal is not None:
        return CUSTOMER_IDENTITY_UNPROVEN, reader.refusal, False
    return CUSTOMER_IDENTITY_UNPROVEN, None, False


async def _prove_live_customer(
    facts: _LocalFacts,
    *,
    reader: DiagnosticReader,
    session_maker: Any,
) -> tuple[uuid.UUID | None, str, str | None, str | None, bool]:
    """``(customer_uuid, identity_source, refusal, detail, retryable)``.

    Branch one is already proven locally and needs no read. Branch two is the
    full §44.2 proof, reusing the existing workspace lookup and direct card read
    rather than restating either — complete pagination, exactly one customer on
    the number, and the card read back at that number — under the diagnostic's
    stricter coverage contract, and then rechecking the local identity facts the
    lookup may have raced.
    """
    if facts.stored_customer_uuid is not None:
        return facts.stored_customer_uuid, IDENTITY_VIA_STORED_UUID, None, None, False
    phone = facts.captured_phone
    if phone is None:  # pragma: no cover - the local phase sets exactly one
        return None, IDENTITY_VIA_CAPTURED_CONTACT, CUSTOMER_IDENTITY_UNPROVEN, None, False
    reader.expect_phone(phone)
    outcome = await prove_customer_identity(reader, phone=phone, require_name=False)
    if outcome.customer is None:
        reason, detail, retryable = _lookup_refusal(reader, outcome.reason)
        return None, IDENTITY_VIA_CAPTURED_CONTACT, reason, detail, retryable
    try:
        proven = uuid.UUID(outcome.customer.uuid)
    except (ValueError, AttributeError, TypeError):
        return None, IDENTITY_VIA_CAPTURED_CONTACT, API_RESPONSE_MALFORMED, None, False
    conflict, conflict_detail = await _recheck_local_identity(session_maker, facts=facts, customer_uuid=proven)
    if conflict is not None:
        return None, IDENTITY_VIA_CAPTURED_CONTACT, conflict, conflict_detail, False
    return proven, IDENTITY_VIA_CAPTURED_CONTACT, None, None, False


def _history_refusal(
    reader: DiagnosticReader,
    *,
    bookings: Any,
    fallback: str,
) -> tuple[str, str | None, bool]:
    """Why the history read produced no trustworthy set.

    The shared reader reports WHAT could not be trusted and whether retrying
    could help; it folds a permanent 4xx and a malformed body into one
    "unreadable" code, because for its own purpose they are the same answer.
    Here they are not, so the typed exception the wrapper kept decides the
    family and ``fallback`` covers the cases that never involved an exception at
    all — incomplete pagination, an inconsistent total, a row that did not
    belong to this customer.
    """
    if bookings.retryable_uncertainty:
        return TEMPORARILY_UNAVAILABLE, bookings.reason, True
    exc = reader.failure
    if isinstance(exc, EasyWeekError):
        reason, detail, temporary = _api_family(exc, stage="history")
        return reason, bookings.reason or detail, temporary
    return fallback, bookings.reason, False


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
    reader: DiagnosticReader,
    clock: datetime,
    registry: EasyWeekLocationRegistry,
    pause: Callable[[float], Awaitable[None]] | None,
    pause_sec: float,
) -> ZeroVisitHistoryReport:
    facts, reason, detail = await _local_facts(
        session_maker, run_id=run_id, recipient_id=recipient_id, registry=registry
    )
    if facts is None:
        return ZeroVisitHistoryReport(
            False,
            run_id,
            recipient_id,
            reason=reason,
            detail_reason=detail,
            checked_at=clock.isoformat(),
        )

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
    customer_uuid, identity_source, refusal, refusal_detail, retryable = await _prove_live_customer(
        facts, reader=reader, session_maker=session_maker
    )
    if customer_uuid is None:
        return ZeroVisitHistoryReport(
            False,
            reason=refusal,
            detail_reason=refusal_detail,
            retryable=retryable,
            identity_source=identity_source,
            **base,
        )

    try:
        booking_payload = await reader.get_booking(str(facts.binding.booking_uuid))
    except Exception as exc:  # noqa: BLE001 - a failed read is never a fact
        reason, detail, temporary = _api_family(exc, stage="source")
        return ZeroVisitHistoryReport(
            False,
            reason=reason,
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
        # them is a confirmation: it is a body that does not keep its contract.
        return ZeroVisitHistoryReport(
            False,
            reason=API_RESPONSE_MALFORMED,
            detail_reason=SOURCE_BOOKING_MALFORMED,
            identity_source=identity_source,
            **base,
        )
    if binding.customer_uuid != customer_uuid:
        # The booking belongs to somebody else now. Following it would read one
        # person's history and show it against another's row. Not temporary and
        # never retried: this is an answer, not an outage.
        return ZeroVisitHistoryReport(False, reason=BOOKING_REASSIGNED, identity_source=identity_source, **base)

    # The booking read was a network call of its own, and the webhook bridge
    # writes during it. By the time the history would be asked for, the identity
    # this check is about may already be contradicted — so ask our own rows
    # again, before spending a single page on anybody's history.
    conflict, conflict_detail = await _recheck_local_identity(session_maker, facts=facts, customer_uuid=customer_uuid)
    if conflict is not None:
        return ZeroVisitHistoryReport(
            False,
            reason=conflict,
            detail_reason=conflict_detail,
            identity_source=identity_source,
            **base,
        )

    bookings = await read_customer_bookings(
        reader,
        customer_uuid=customer_uuid,
        pause=pause if pause is not None else asyncio.sleep,
        pause_sec=pause_sec,
    )
    if not bookings.identity_current:
        reason, detail, temporary = _history_refusal(
            reader,
            bookings=bookings,
            fallback=(
                IDENTITY_UUID_CONFLICT if bookings.reason == CUSTOMER_UUID_MISMATCH else CUSTOMER_IDENTITY_UNPROVEN
            ),
        )
        return ZeroVisitHistoryReport(
            False,
            reason=reason,
            detail_reason=detail or CUSTOMER_RESPONSE_MALFORMED,
            retryable=temporary,
            identity_source=identity_source,
            pages_read=bookings.pages_read,
            **base,
        )
    if not bookings.complete:
        # Incomplete pagination, a timeout, a 429, a 5xx, a malformed row. The
        # counts stay None: an unread history is unknown, never zero.
        reason, detail, temporary = _history_refusal(reader, bookings=bookings, fallback=HISTORY_UNVERIFIED)
        return ZeroVisitHistoryReport(
            False,
            reason=reason,
            detail_reason=detail,
            retryable=temporary,
            identity_source=identity_source,
            identity_confirmed=True,
            pages_read=bookings.pages_read,
            **base,
        )

    # The last window, closed last. The pages were read for a customer this card
    # was proven to be a moment ago; a webhook that contradicted that while they
    # were being walked makes the count an answer about somebody else. Serving
    # it as a confirmation — a number an operator would then act on — is worse
    # than refusing, so the refusal is what happens.
    conflict, conflict_detail = await _recheck_local_identity(session_maker, facts=facts, customer_uuid=customer_uuid)
    if conflict is not None:
        return ZeroVisitHistoryReport(
            False,
            reason=conflict,
            detail_reason=conflict_detail,
            identity_source=identity_source,
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
    tracked = DiagnosticReader(reader)
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SEC):
            return await _check(
                session_maker,
                run_id=run_id,
                recipient_id=recipient_id,
                reader=tracked,
                clock=clock,
                registry=locations,
                pause=pause,
                pause_sec=pause_sec,
            )
    except TimeoutError:
        # WHERE the deadline expired decides what the operator is told, and
        # there are three places, not two.
        #
        # Nothing asked yet — the deadline expired on our own rows and EasyWeek
        # was never asked. Reporting "EasyWeek did not answer" would send
        # somebody to look at a branch that had nothing to do with it.
        #
        # Inside a call — EasyWeek genuinely did not answer in time. Temporary,
        # retry.
        #
        # Between calls — EasyWeek answered, and then OUR phase ran out of time:
        # a slow database during an identity recheck, say. Blaming EasyWeek
        # would be wrong, and a confirmation would be a guess, so this gets a
        # neutral reason of its own.
        if tracked.external_calls == 0:
            reason, detail = LOCAL_DEADLINE, LOCAL_DEADLINE_DETAIL
        elif tracked.awaiting:
            reason, detail = TEMPORARILY_UNAVAILABLE, API_RETRYABLE_UNCERTAINTY
        else:
            reason, detail = DEADLINE_AFTER_READ, DEADLINE_AFTER_READ_DETAIL
        return ZeroVisitHistoryReport(
            False,
            run_id,
            recipient_id,
            reason=reason,
            detail_reason=detail,
            retryable=True,
            checked_at=clock.isoformat(),
        )


__all__ = [
    "API_NOT_CONFIGURED",
    "API_REFUSED",
    "API_RESPONSE_MALFORMED",
    "BOOKING_REASSIGNED",
    "BRANCH_UNKNOWN",
    "CHECK_PAUSE_SEC",
    "CHECK_TIMEOUT_SEC",
    "CONTACT_EVIDENCE_MISSING",
    "CONTACT_OWNER_CONFLICT",
    "CUSTOMER_IDENTITY_AMBIGUOUS",
    "CUSTOMER_IDENTITY_UNPROVEN",
    "DEADLINE_AFTER_READ",
    "DEADLINE_AFTER_READ_DETAIL",
    "HISTORY_UNVERIFIED",
    "IDENTITY_UUID_CONFLICT",
    "IDENTITY_VIA_CAPTURED_CONTACT",
    "IDENTITY_VIA_STORED_UUID",
    "LOCAL_DEADLINE",
    "LOCAL_DEADLINE_DETAIL",
    "NUMERIC_OWNER_CONFLICT",
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
