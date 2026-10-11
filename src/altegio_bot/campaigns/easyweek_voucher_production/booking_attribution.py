"""Did a recipient of one voucher mailing book again afterwards?

What this answers, and what it refuses to answer
------------------------------------------------
One observable fact per recipient: a NEW booking was created somewhere in this
EasyWeek workspace after a send this application can PROVE left it. That is the
whole claim. It is not evidence that the message caused the booking, that the
person came to the salon, that anything was paid, or that the voucher was used,
and no field here is ever phrased as though it were.

Read-only, and separate from execution
--------------------------------------
Nothing in this module writes. No row of the mailing, the preview, the
recipients, the approvals, the ledger, the entitlement or the bindings is
touched, and no external service is contacted — not EasyWeek, not Meta. The
booking state computed here lives beside the execution statuses and never
replaces them: ``sent``, ``paid`` and ``delivered`` keep meaning what they
meant, and ``booked`` is a separate answer to a separate question.

Where the window starts, and why it is not the callback
-------------------------------------------------------
The anchor is the slot's own durable ``send_attempted_at`` — the instant this
process attempted the send — and it is used ONLY when the same send carries
positive acceptance evidence: Meta's acceptance, or a delivered/read webhook,
which the schema already requires to imply acceptance. An attempt with no
acceptance proves nothing about whether a message left, so it yields no
positive attribution at all.

``provider_accepted_at`` is deliberately NOT the anchor. A callback can land
long after the attempt — and it is what first records acceptance when the
callback beats the commit — so anchoring on it would silently move the start of
the window forward and retroactively disqualify bookings already inside it.

Reproducibility
---------------
The window and the policy are code constants, printed next to the report. They
are not read from settings and cannot be changed by an environment variable,
because a mailing's report has to keep meaning the same thing tomorrow. A
change to them is a code change, reviewed like any other, and the report names
the version that produced the numbers it shows.

Identity, from proofs that already exist
----------------------------------------
A frozen slot carries the customer UUID that was proven when the batch was
frozen. EasyWeek's booking webhooks, however, name a customer by NUMERIC id, so
the two have to be bridged. This module reuses bridges that were already
proven and stores none of its own:

* the local card the freeze itself re-proved for that slot
  (``CampaignRecipient.client_id``, re-checked under lock by ``local_identity``
  during the freeze) and the EasyWeek numeric id on it;
* any EasyWeek ``Client`` row whose ``easyweek_customer_uuid`` equals the
  slot's proven UUID — the independent binding the inbox worker establishes.

A name or a phone that merely looks the same is never a bridge. Two different
numeric ids for one proven UUID, or a numeric id that another card binds to a
different UUID, is a CONFLICT: the row is reported as unknown and nobody is
credited. Opening the report fills no UUID, merges no cards and writes nothing.

One person may hold a card in several branches of the workspace. That is one
recipient, not several conversions, and the cards are left exactly as they are.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.easyweek_normalizer import (
    PG_BIGINT_MAX,
    PG_INT_MAX,
    NormalizationError,
    canonical_booking_uuid,
    parse_iso_utc,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED,
    VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_PAY_REJECTED,
    VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUNDED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
    CampaignRecipient,
    Client,
    EasyWeekEvent,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.utils import utcnow

# ---------------------------------------------------------------------------
# The policy, as constants a report can name
# ---------------------------------------------------------------------------
# Printed beside every answer. Not a setting: see "Reproducibility" above.
ATTRIBUTION_POLICY_VERSION: Final = "easyweek-booking-report-v1"
ATTRIBUTION_WINDOW_DAYS: Final = 30
# A duration from an instant, not a calendar step: thirty times twenty-four
# hours. A DST change inside the window therefore moves no boundary, while the
# LOCAL rendering of both boundaries shifts exactly as the clock did.
ATTRIBUTION_WINDOW: Final = timedelta(days=ATTRIBUTION_WINDOW_DAYS)

# How far BEFORE the earliest anchor the capture scan starts. A webhook is
# received after the booking it describes was created, but not instantly, and
# this margin keeps a delivery that straddles the anchor inside the scan. It
# never widens the attribution window itself, which is checked on
# ``booking_created_at`` alone.
LATE_CAPTURE_SLACK: Final = timedelta(days=1)

# A hard ceiling on one scan. The scan is already bounded below by the earliest
# proven send, so this only matters for a mailing read very long afterwards —
# and when it bites, the report SAYS the coverage was truncated rather than
# presenting a short answer as a complete one.
MAX_SCANNED_EVENTS: Final = 50_000

# ---------------------------------------------------------------------------
# The five real lifecycle triggers
# ---------------------------------------------------------------------------
CREATED_HINT: Final = "booking-created"
CANCELLED_HINT: Final = "booking-canceled"
SUCCEEDED_HINT: Final = "booking-succeeded"
UPDATED_HINTS: Final = ("booking-updated", "booking-rescheduled")
LIFECYCLE_HINTS: Final = (CREATED_HINT, *UPDATED_HINTS, CANCELLED_HINT, SUCCEEDED_HINT)

# ---------------------------------------------------------------------------
# Vocabularies. Every one of these is a stable code the interface translates.
# ---------------------------------------------------------------------------
SEND_NOT_ATTEMPTED: Final = "send_not_attempted"
SEND_ATTEMPT_UNCONFIRMED: Final = "send_attempt_unconfirmed"
SEND_CONFIRMED: Final = "send_confirmed"

IDENTITY_PROVEN: Final = "identity_proven"
IDENTITY_NUMERIC_MISSING: Final = "identity_numeric_missing"
IDENTITY_CONFLICT: Final = "identity_conflict"

# Where the numeric id came from. Provenance, kept because "we matched a
# number" is not a finding an operator can check without it.
SOURCE_FROZEN_LOCAL_CARD: Final = "frozen_local_card"
SOURCE_CLIENT_UUID_BINDING: Final = "client_uuid_binding"

BOOKING_FOUND: Final = "booking_found"
BOOKING_NOT_OBSERVED: Final = "booking_not_observed"
INSUFFICIENT_DATA: Final = "insufficient_data"

MESSAGE_NOT_SENT: Final = "not_sent"
MESSAGE_ATTEMPT_UNCONFIRMED: Final = "attempt_unconfirmed"
MESSAGE_ACCEPTED: Final = "provider_accepted"
MESSAGE_DELIVERED: Final = "delivered_confirmed"
MESSAGE_READ: Final = "read_confirmed"
MESSAGE_REFUNDED: Final = "refunded"

BOOKING_ACTIVE: Final = "active"
BOOKING_CANCELLED: Final = "cancelled"
BOOKING_COMPLETED: Final = "completed"
BOOKING_STATE_UNKNOWN: Final = "unknown"

# Why a captured delivery could not be used as a positive fact. Aggregated into
# counts; never attached to a person.
REJECT_TRUNCATED: Final = "truncated_payload"
REJECT_INVALID_PAYLOAD: Final = "invalid_payload"
REJECT_FOREIGN_LOCATION: Final = "foreign_location"
REJECT_LOCATION_MISMATCH: Final = "location_identity_mismatch"
REJECT_BOOKING_UUID: Final = "booking_identity_unusable"
REJECT_CUSTOMER_ID: Final = "customer_identity_unusable"
REJECT_CREATED_AT: Final = "booking_created_at_unusable"
REJECT_CONFLICTING_CREATION: Final = "conflicting_creation_events"

REGISTRY_UNAVAILABLE: Final = "easyweek_location_registry_unavailable"
BATCH_UNKNOWN: Final = "voucher_production_batch_unknown"

# The item statuses that are an execution FAILURE rather than a stage not yet
# reached. A row in one of these is shown as an error whatever else is true of
# it, because an operator deciding what to do next needs to see it.
_ERROR_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED,
        VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_PAY_REJECTED,
        VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
        VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
        VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    }
)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _strict_positive_int(value: Any, *, maximum: int) -> int | None:
    """A JSON number that is really an id, or ``None``.

    ``bool`` is excluded on purpose: ``True`` is an ``int`` in Python and would
    otherwise be read as customer 1.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 < value <= maximum else None


# ---------------------------------------------------------------------------
# One captured booking, as this report is allowed to see it
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObservedBooking:
    """One booking of this workspace, deduplicated by its own identity.

    ``booking_uuid`` is the dedupe key and is deliberately never serialised: a
    report does not need an external identifier to say "a booking was created
    in Durlach on the 3rd", and printing one would put a provider id on screen
    for no operator benefit.
    """

    booking_uuid: uuid_module.UUID
    company_id: int
    branch_name: str
    customer_id: int
    booking_created_at: datetime
    state: str = BOOKING_ACTIVE
    # A later delivery named a DIFFERENT customer for this booking. The booking
    # was still created by the customer its creation event named — that is the
    # historical fact — but the current owner is somebody else, and a report
    # that hid that would be inviting a wrong conclusion.
    customer_changed_later: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "branch_name": self.branch_name,
            "branch_company_id": self.company_id,
            "booking_created_at": _iso(self.booking_created_at),
            "state": self.state,
            "customer_changed_later": self.customer_changed_later,
        }


@dataclass
class _BookingDraft:
    """Accumulator for one booking UUID while the capture is being read."""

    booking_uuid: uuid_module.UUID
    company_id: int | None = None
    branch_name: str = ""
    customer_id: int | None = None
    booking_created_at: datetime | None = None
    state: str = BOOKING_STATE_UNKNOWN
    customer_changed_later: bool = False
    conflicting: bool = False
    seen_creation: bool = False


@dataclass(frozen=True)
class CaptureScan:
    """Every booking the bounded scan could prove, plus what it could not."""

    bookings: tuple[ObservedBooking, ...] = ()
    scanned_events: int = 0
    truncated: bool = False
    scan_from: datetime | None = None
    scan_to: datetime | None = None
    rejected: dict[str, int] = field(default_factory=dict)
    # Per configured branch: how many usable creations were observed inside the
    # scanned range. Zero is a real answer — "nothing was observed here" — and
    # is NOT the same as "this branch has no bookings", which this report has
    # no way to establish.
    branch_observed: dict[int, int] = field(default_factory=dict)

    def by_customer(self) -> dict[int, list[ObservedBooking]]:
        grouped: dict[int, list[ObservedBooking]] = {}
        for booking in self.bookings:
            grouped.setdefault(booking.customer_id, []).append(booking)
        return grouped


# ---------------------------------------------------------------------------
# The send anchor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SendAnchor:
    """When this slot's message left, when that is provable at all."""

    state: str
    anchor: datetime | None = None
    evidence: tuple[str, ...] = ()

    @property
    def confirmed(self) -> bool:
        return self.state == SEND_CONFIRMED and self.anchor is not None

    def window(self) -> tuple[datetime, datetime] | None:
        if not self.confirmed or self.anchor is None:
            return None
        return self.anchor, self.anchor + ATTRIBUTION_WINDOW


def resolve_send_anchor(item: EasyWeekVoucherProductionBatchItem) -> SendAnchor:
    """The durable attempt, used only with positive acceptance evidence.

    The three kinds of evidence are listed rather than collapsed, so the report
    can say WHY it believes a message left. ``delivered``/``read`` are listed
    even though the schema makes them imply acceptance: a callback landing
    beside the commit that records acceptance is a real case, and an operator
    reading the row should see which fact carried it.
    """
    if item.send_attempted_at is None:
        return SendAnchor(SEND_NOT_ATTEMPTED)
    evidence: list[str] = []
    if item.provider_accepted_at is not None:
        evidence.append("provider_accepted")
    if item.delivered_at is not None:
        evidence.append("webhook_delivered")
    if item.read_at is not None:
        evidence.append("webhook_read")
    if not evidence:
        # One attempt, no acceptance. The message may or may not have left, and
        # a report must not resolve that ambiguity by guessing.
        return SendAnchor(SEND_ATTEMPT_UNCONFIRMED)
    return SendAnchor(SEND_CONFIRMED, item.send_attempted_at, tuple(evidence))


def message_state(item: EasyWeekVoucherProductionBatchItem) -> str:
    """The furthest delivery fact that is actually CONFIRMED for this slot.

    Five separate things, never merged: no send yet, an attempt with no
    acceptance, acceptance without a delivery confirmation, a delivery
    confirmation, a read confirmation. The absence of a read callback is not
    evidence that nobody read the message, and the interface says so.
    """
    if item.status == VOUCHER_PRODUCTION_ITEM_REFUNDED:
        return MESSAGE_REFUNDED
    if item.read_at is not None:
        return MESSAGE_READ
    if item.delivered_at is not None:
        return MESSAGE_DELIVERED
    if item.provider_accepted_at is not None:
        return MESSAGE_ACCEPTED
    if item.send_attempted_at is not None:
        return MESSAGE_ATTEMPT_UNCONFIRMED
    return MESSAGE_NOT_SENT


def has_execution_error(item: EasyWeekVoucherProductionBatchItem) -> bool:
    return item.status in _ERROR_ITEM_STATUSES


# ---------------------------------------------------------------------------
# Identity: the proven UUID, bridged to the numeric id the webhooks use
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecipientIdentity:
    state: str
    customer_id: int | None = None
    sources: tuple[str, ...] = ()
    # Which branches hold a card for this one person. Informational: one
    # recipient with three cards is still one recipient.
    card_company_ids: tuple[int, ...] = ()

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "identity_state": self.state,
            "identity_sources": list(self.sources),
            "identity_card_branches": list(self.card_company_ids),
        }


async def resolve_identities(
    session: AsyncSession,
    *,
    items: list[EasyWeekVoucherProductionBatchItem],
) -> dict[int, RecipientIdentity]:
    """One numeric id per slot, or a named reason there is none.

    Three bounded queries for the whole batch rather than three per slot: the
    frozen recipients, the cards that either the freeze bound or a UUID binding
    claims, and the reverse check that no candidate number belongs to anybody
    else.
    """
    if not items:
        return {}

    recipient_ids = {int(item.campaign_recipient_id) for item in items}
    uuids = {item.easyweek_customer_uuid for item in items if item.easyweek_customer_uuid is not None}

    frozen_client_id: dict[int, int | None] = {}
    for recipient_id, client_id in (
        await session.execute(
            select(CampaignRecipient.id, CampaignRecipient.client_id).where(CampaignRecipient.id.in_(recipient_ids))
        )
    ).all():
        frozen_client_id[int(recipient_id)] = int(client_id) if client_id is not None else None

    wanted_client_ids = {value for value in frozen_client_id.values() if value is not None}
    cards: dict[int, tuple[int, int | None, uuid_module.UUID | None]] = {}
    uuid_cards: dict[uuid_module.UUID, list[tuple[int, int | None]]] = {}
    if wanted_client_ids or uuids:
        conditions = []
        if wanted_client_ids:
            conditions.append(Client.id.in_(wanted_client_ids))
        if uuids:
            conditions.append(Client.easyweek_customer_uuid.in_(uuids))
        rows = (
            await session.execute(
                select(
                    Client.id,
                    Client.company_id,
                    Client.altegio_client_id,
                    Client.easyweek_customer_uuid,
                ).where(Client.provider == PROVIDER_EASYWEEK, or_(*conditions))
            )
        ).all()
        for client_id, company_id, numeric, bound_uuid in rows:
            cards[int(client_id)] = (
                int(company_id),
                int(numeric) if numeric is not None else None,
                bound_uuid,
            )
            if bound_uuid is not None:
                uuid_cards.setdefault(bound_uuid, []).append(
                    (int(company_id), int(numeric) if numeric is not None else None)
                )

    candidate_numbers = {numeric for _company, numeric, _bound in cards.values() if numeric is not None}

    # The reverse direction. A number that another card binds to a different
    # UUID is not this person's number, whatever the first direction suggested.
    numeric_bindings: dict[int, set[uuid_module.UUID]] = {}
    if candidate_numbers:
        for numeric, bound_uuid in (
            await session.execute(
                select(Client.altegio_client_id, Client.easyweek_customer_uuid).where(
                    Client.provider == PROVIDER_EASYWEEK,
                    Client.altegio_client_id.in_(candidate_numbers),
                    Client.easyweek_customer_uuid.is_not(None),
                )
            )
        ).all():
            numeric_bindings.setdefault(int(numeric), set()).add(bound_uuid)

    resolved: dict[int, RecipientIdentity] = {}
    for item in items:
        slot = int(item.slot)
        proven_uuid = item.easyweek_customer_uuid
        numbers: dict[int, set[str]] = {}
        branches: set[int] = set()
        conflict = False

        card_id = frozen_client_id.get(int(item.campaign_recipient_id))
        card = cards.get(card_id) if card_id is not None else None
        if card is not None:
            company_id, numeric, bound_uuid = card
            if bound_uuid is not None and proven_uuid is not None and bound_uuid != proven_uuid:
                # The card the freeze bound now claims a different person.
                conflict = True
            branches.add(company_id)
            if numeric is not None:
                numbers.setdefault(numeric, set()).add(SOURCE_FROZEN_LOCAL_CARD)

        for company_id, numeric in uuid_cards.get(proven_uuid, []) if proven_uuid is not None else ():
            branches.add(company_id)
            if numeric is not None:
                numbers.setdefault(numeric, set()).add(SOURCE_CLIENT_UUID_BINDING)

        if conflict or len(numbers) > 1:
            resolved[slot] = RecipientIdentity(IDENTITY_CONFLICT, card_company_ids=tuple(sorted(branches)))
            continue
        if not numbers:
            resolved[slot] = RecipientIdentity(IDENTITY_NUMERIC_MISSING, card_company_ids=tuple(sorted(branches)))
            continue

        chosen = next(iter(numbers))
        bound = numeric_bindings.get(chosen, set())
        if proven_uuid is not None and any(value != proven_uuid for value in bound):
            resolved[slot] = RecipientIdentity(IDENTITY_CONFLICT, card_company_ids=tuple(sorted(branches)))
            continue
        resolved[slot] = RecipientIdentity(
            IDENTITY_PROVEN,
            customer_id=chosen,
            sources=tuple(sorted(numbers[chosen])),
            card_company_ids=tuple(sorted(branches)),
        )
    return resolved


# ---------------------------------------------------------------------------
# The bounded capture scan
# ---------------------------------------------------------------------------


def _validate_delivery(
    *,
    event_hint: str | None,
    payload: Any,
    body_truncated: bool,
    registry: EasyWeekLocationRegistry,
) -> tuple[dict[str, Any] | None, str | None]:
    """One captured delivery, validated narrowly for THIS report's purpose.

    Deliberately not the inbox worker's normalizer and deliberately not its
    ``status``. ``processed`` says a delivery was consumed by a pipeline with
    its own requirements; ``failed`` may mean a notification or a service the
    mailing does not handle, and neither answers "is this a booking of this
    workspace, by this customer, created at this instant". So the facts this
    report needs are proven here, from the captured payload, and a delivery
    that cannot prove them becomes a counted gap rather than a positive fact.
    """
    if body_truncated:
        return None, REJECT_TRUNCATED
    if not isinstance(payload, dict) or not payload:
        return None, REJECT_INVALID_PAYLOAD

    location_id = _strict_positive_int(payload.get("location_id"), maximum=PG_INT_MAX)
    if location_id is None:
        return None, REJECT_INVALID_PAYLOAD
    location = registry.locations.get(location_id)
    if location is None:
        # Another workspace, an unconfigured branch, or an Altegio-era row.
        return None, REJECT_FOREIGN_LOCATION
    raw_location_uuid = payload.get("location_uuid")
    try:
        payload_location_uuid = str(uuid_module.UUID(raw_location_uuid)) if isinstance(raw_location_uuid, str) else None
    except (ValueError, AttributeError, TypeError):
        payload_location_uuid = None
    if payload_location_uuid != location.location_uuid:
        return None, REJECT_LOCATION_MISMATCH

    booking_uuid = canonical_booking_uuid(payload)
    if booking_uuid is None:
        return None, REJECT_BOOKING_UUID

    facts: dict[str, Any] = {
        "booking_uuid": booking_uuid,
        "company_id": location_id,
        "branch_name": location.name,
        "customer_id": _strict_positive_int(payload.get("customer_id"), maximum=PG_BIGINT_MAX),
    }

    if event_hint == CREATED_HINT:
        if facts["customer_id"] is None:
            return None, REJECT_CUSTOMER_ID
        try:
            created_at = parse_iso_utc(payload.get("booking_created_at"), required=True)
        except NormalizationError:
            return None, REJECT_CREATED_AT
        if created_at is None or created_at.tzinfo is None:
            return None, REJECT_CREATED_AT
        facts["booking_created_at"] = created_at
    return facts, None


async def scan_capture(
    session: AsyncSession,
    *,
    anchors: list[datetime],
    now: datetime,
    registry: EasyWeekLocationRegistry,
) -> CaptureScan:
    """Every booking of this workspace that could matter, once.

    Bounded below by the earliest proven send and above by a hard row cap, and
    ordered by the capture's own primary key so a later delivery is applied
    after an earlier one. With no proven send there is no window, so there is
    nothing to scan and nothing is read.

    Deduplication is by booking identity, which is what makes a retried
    webhook, a Resend and a rescheduled appointment one booking rather than
    three conversions.
    """
    if not anchors or not registry.ready:
        return CaptureScan(branch_observed={location_id: 0 for location_id in registry.locations})

    floor = min(anchors) - LATE_CAPTURE_SLACK
    rows = (
        await session.execute(
            select(
                EasyWeekEvent.event_hint,
                EasyWeekEvent.payload,
                EasyWeekEvent.body_truncated,
            )
            .where(EasyWeekEvent.event_hint.in_(LIFECYCLE_HINTS))
            .where(EasyWeekEvent.received_at >= floor)
            .where(EasyWeekEvent.received_at <= now)
            .order_by(EasyWeekEvent.id.asc())
            .limit(MAX_SCANNED_EVENTS + 1)
        )
    ).all()

    truncated = len(rows) > MAX_SCANNED_EVENTS
    rows = rows[:MAX_SCANNED_EVENTS]

    rejected: dict[str, int] = {}
    drafts: dict[uuid_module.UUID, _BookingDraft] = {}
    branch_observed: dict[int, int] = {location_id: 0 for location_id in registry.locations}

    for event_hint, payload, body_truncated in rows:
        facts, reason = _validate_delivery(
            event_hint=event_hint,
            payload=payload,
            body_truncated=bool(body_truncated),
            registry=registry,
        )
        if facts is None:
            code = reason or REJECT_INVALID_PAYLOAD
            rejected[code] = rejected.get(code, 0) + 1
            continue

        booking_uuid = facts["booking_uuid"]
        draft = drafts.setdefault(booking_uuid, _BookingDraft(booking_uuid=booking_uuid))

        if event_hint == CREATED_HINT:
            if not draft.seen_creation:
                draft.seen_creation = True
                draft.company_id = facts["company_id"]
                draft.branch_name = facts["branch_name"]
                draft.customer_id = facts["customer_id"]
                draft.booking_created_at = facts["booking_created_at"]
                draft.state = BOOKING_ACTIVE
                branch_observed[facts["company_id"]] = branch_observed.get(facts["company_id"], 0) + 1
            elif (
                draft.customer_id != facts["customer_id"]
                or draft.booking_created_at != facts["booking_created_at"]
                or draft.company_id != facts["company_id"]
            ):
                # Two creations of one booking that do not agree. A repeated
                # delivery of the SAME creation is the ordinary case and lands
                # in neither branch of this check.
                draft.conflicting = True
                rejected[REJECT_CONFLICTING_CREATION] = rejected.get(REJECT_CONFLICTING_CREATION, 0) + 1
            continue

        # A lifecycle change. It never creates a booking — a rescheduled or
        # updated appointment is the same appointment — and it decides only the
        # CURRENT state.
        if facts["customer_id"] is not None and draft.customer_id is not None:
            if facts["customer_id"] != draft.customer_id:
                draft.customer_changed_later = True
        if event_hint == CANCELLED_HINT:
            draft.state = BOOKING_CANCELLED
        elif event_hint == SUCCEEDED_HINT:
            draft.state = BOOKING_COMPLETED
        elif draft.state in (BOOKING_ACTIVE, BOOKING_STATE_UNKNOWN) and draft.seen_creation:
            # An ordinary edit or a reschedule of a live appointment. It does
            # NOT revive a cancellation and does not undo a completed visit:
            # neither of those transitions has been proven for this provider,
            # and inventing one would turn an edit into a claim about the
            # salon's day.
            draft.state = BOOKING_ACTIVE

    bookings = tuple(
        ObservedBooking(
            booking_uuid=draft.booking_uuid,
            company_id=int(draft.company_id or 0),
            branch_name=draft.branch_name,
            customer_id=int(draft.customer_id or 0),
            booking_created_at=created_at,
            state=draft.state,
            customer_changed_later=draft.customer_changed_later,
        )
        for draft in drafts.values()
        if draft.seen_creation
        and not draft.conflicting
        and draft.customer_id is not None
        and (created_at := draft.booking_created_at) is not None
    )
    return CaptureScan(
        bookings=bookings,
        scanned_events=len(rows),
        truncated=truncated,
        scan_from=floor,
        scan_to=now,
        rejected=rejected,
        branch_observed=branch_observed,
    )


# ---------------------------------------------------------------------------
# One row of the report
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecipientRow:
    slot: int
    campaign_recipient_id: int
    recipient_basis: str
    item_status: str
    reason_code: str | None
    message_state: str
    execution_error: bool
    send_anchor: SendAnchor
    identity: RecipientIdentity
    attribution_state: str
    reasons: tuple[str, ...] = ()
    bookings: tuple[ObservedBooking, ...] = ()
    reconciliation_required: bool = False
    manual_cleanup_required: bool = False
    send_attempt_count: int = 0

    @property
    def booked(self) -> bool:
        return self.attribution_state == BOOKING_FOUND

    @property
    def nearest(self) -> ObservedBooking | None:
        return self.bookings[0] if self.bookings else None

    def as_safe_dict(self) -> dict[str, Any]:
        """Slot, codes, states and dates. Never a name, a phone or a UUID."""
        window = self.send_anchor.window()
        return {
            "slot": self.slot,
            "campaign_recipient_id": self.campaign_recipient_id,
            "recipient_basis": self.recipient_basis,
            "item_status": self.item_status,
            "reason_code": self.reason_code,
            "message_state": self.message_state,
            "execution_error": self.execution_error,
            "send_state": self.send_anchor.state,
            "send_attempted_at": _iso(self.send_anchor.anchor),
            "send_evidence": list(self.send_anchor.evidence),
            "send_attempt_count": self.send_attempt_count,
            "window_start": _iso(window[0]) if window else None,
            "window_end": _iso(window[1]) if window else None,
            "attribution_state": self.attribution_state,
            "reasons": list(self.reasons),
            "booking_count": len(self.bookings),
            "bookings": [booking.as_safe_dict() for booking in self.bookings],
            "reconciliation_required": self.reconciliation_required,
            "manual_cleanup_required": self.manual_cleanup_required,
            **self.identity.as_safe_dict(),
        }


@dataclass(frozen=True)
class BookingReport:
    """The whole answer for one mailing, or the reason there is none."""

    report_known: bool
    batch_id: int | None = None
    preview_run_id: int | None = None
    batch_exists: bool = False
    batch_status: str | None = None
    schema_version: str | None = None
    campaign_period: str | None = None
    recipient_count: int = 0
    preview_active_count: int = 0
    rows: tuple[RecipientRow, ...] = ()
    scan: CaptureScan = field(default_factory=CaptureScan)
    registry_ready: bool = False
    branches: tuple[dict[str, Any], ...] = ()
    generated_at: datetime | None = None
    reasons: tuple[str, ...] = ()

    @property
    def any_send_confirmed(self) -> bool:
        return any(row.send_anchor.confirmed for row in self.rows)

    def summary(self) -> dict[str, Any]:
        """The six numbers at the top, each one its own independent fact.

        Acceptance, delivery and reading OVERLAP by nature — a read message was
        also delivered and also accepted — so they are never presented as parts
        of a whole. The interface repeats that in words next to them.
        """
        return {
            "recipients": self.recipient_count,
            "provider_accepted": sum(
                1 for row in self.rows if row.message_state in (MESSAGE_ACCEPTED, MESSAGE_DELIVERED, MESSAGE_READ)
            ),
            "delivered_confirmed": sum(
                1 for row in self.rows if row.message_state in (MESSAGE_DELIVERED, MESSAGE_READ)
            ),
            "read_confirmed": sum(1 for row in self.rows if row.message_state == MESSAGE_READ),
            "booked_after_send": sum(1 for row in self.rows if row.booked),
            "attention": sum(
                1 for row in self.rows if row.execution_error or row.attribution_state == INSUFFICIENT_DATA
            ),
            "counters_overlap": True,
        }

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "report_known": self.report_known,
            "reasons": list(self.reasons),
            "policy_version": ATTRIBUTION_POLICY_VERSION,
            "attribution_window_days": ATTRIBUTION_WINDOW_DAYS,
            "batch_id": self.batch_id,
            "preview_run_id": self.preview_run_id,
            "batch_exists": self.batch_exists,
            "batch_status": self.batch_status,
            "schema_version": self.schema_version,
            "campaign_period": self.campaign_period,
            "recipient_count": self.recipient_count,
            "preview_active_count": self.preview_active_count,
            "any_send_confirmed": self.any_send_confirmed,
            "generated_at": _iso(self.generated_at),
            "summary": self.summary(),
            "rows": [row.as_safe_dict() for row in self.rows],
            "coverage": {
                "registry_ready": self.registry_ready,
                "branches": list(self.branches),
                "scanned_events": self.scan.scanned_events,
                "scan_truncated": self.scan.truncated,
                "scan_from": _iso(self.scan.scan_from),
                "scan_to": _iso(self.scan.scan_to),
                "rejected": dict(self.scan.rejected),
            },
        }


def _attribute(
    *,
    anchor: SendAnchor,
    identity: RecipientIdentity,
    by_customer: dict[int, list[ObservedBooking]],
) -> tuple[str, tuple[str, ...], tuple[ObservedBooking, ...]]:
    """The four answers section D asks to be told apart, and never a fifth."""
    if anchor.state == SEND_NOT_ATTEMPTED:
        return SEND_NOT_ATTEMPTED, (), ()
    if anchor.state == SEND_ATTEMPT_UNCONFIRMED:
        # One attempt, nothing proving it was accepted. Not a window, and not a
        # negative answer either.
        return INSUFFICIENT_DATA, (SEND_ATTEMPT_UNCONFIRMED,), ()
    if identity.state != IDENTITY_PROVEN or identity.customer_id is None:
        return INSUFFICIENT_DATA, (identity.state,), ()

    window = anchor.window()
    assert window is not None
    start, end = window
    matched = tuple(
        sorted(
            (
                booking
                for booking in by_customer.get(identity.customer_id, [])
                # Strictly after the send, up to and including the end of the
                # window. A booking created before the send is not a booking
                # after it, however late its webhook arrived.
                if start < booking.booking_created_at <= end
            ),
            key=lambda booking: booking.booking_created_at,
        )
    )
    if matched:
        return BOOKING_FOUND, (), matched
    # Nothing was OBSERVED. That is weaker than "there was no booking", and the
    # code says so: the absence of a webhook is not proof of absence.
    return BOOKING_NOT_OBSERVED, (), ()


def _branch_rows(registry: EasyWeekLocationRegistry, scan: CaptureScan) -> tuple[dict[str, Any], ...]:
    """Which branches are in scope, and what was actually observed in each.

    The scope is the configured EasyWeek registry — every branch of the
    workspace — and deliberately NOT the mailing's own company. A zero here
    means "nothing observed in the scanned range", which is a gap in the data
    and not a proven absence of bookings.
    """
    return tuple(
        {
            "name": location.name,
            "company_id": location.location_id,
            "bookings_observed": int(scan.branch_observed.get(location_id, 0)),
        }
        for location_id, location in sorted(registry.locations.items())
    )


async def build_report(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int | None = None,
    preview_run_id: int | None = None,
    now: datetime | None = None,
) -> BookingReport:
    """The whole report, read in one session, or an exception.

    Every read is inside this function so a database that dies halfway reaches
    the caller as a failure rather than as a believable but partial answer —
    the same rule this phase's status snapshot already follows.
    """
    moment = now or utcnow()
    registry = configured_easyweek_locations()
    registry_reasons: tuple[str, ...] = () if registry.ready else (REGISTRY_UNAVAILABLE,)

    async with session_maker() as session:
        header = None
        if batch_id is not None:
            header = await session.get(EasyWeekVoucherProductionBatch, batch_id)
        elif preview_run_id is not None:
            header = (
                await session.execute(
                    select(EasyWeekVoucherProductionBatch).where(
                        EasyWeekVoucherProductionBatch.campaign_run_id == preview_run_id
                    )
                )
            ).scalar_one_or_none()

        if header is None:
            # No batch. Either a preview nobody has frozen yet, or an id that
            # names nothing — reported as exactly that, and never as a mailing
            # that happens to be empty.
            preview_active = 0
            if preview_run_id is not None:
                preview_active = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(CampaignRecipient)
                        .where(
                            CampaignRecipient.campaign_run_id == preview_run_id,
                            CampaignRecipient.provider == PROVIDER_EASYWEEK,
                            CampaignRecipient.status == "candidate",
                        )
                    )
                    or 0
                )
            return BookingReport(
                report_known=True,
                preview_run_id=preview_run_id,
                batch_exists=False,
                preview_active_count=preview_active,
                registry_ready=registry.ready,
                branches=_branch_rows(registry, CaptureScan()),
                generated_at=moment,
                reasons=registry_reasons + ((BATCH_UNKNOWN,) if batch_id is not None else ()),
            )

        items = list(
            (
                await session.scalars(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.batch_id == int(header.id))
                    .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
                )
            ).all()
        )
        anchors = [resolved.anchor for item in items if (resolved := resolve_send_anchor(item)).confirmed]
        identities = await resolve_identities(session, items=items)
        scan = await scan_capture(
            session,
            anchors=[value for value in anchors if value is not None],
            now=moment,
            registry=registry,
        )

    by_customer = scan.by_customer()
    rows: list[RecipientRow] = []
    for item in items:
        anchor = resolve_send_anchor(item)
        identity = identities.get(int(item.slot), RecipientIdentity(IDENTITY_NUMERIC_MISSING))
        state, reasons, matched = _attribute(anchor=anchor, identity=identity, by_customer=by_customer)
        if not registry.ready and state in (BOOKING_NOT_OBSERVED, BOOKING_FOUND):
            # Without a proven branch registry there is no workspace to look
            # in, so neither a positive nor a negative answer is available.
            state, reasons, matched = INSUFFICIENT_DATA, (REGISTRY_UNAVAILABLE,), ()
        rows.append(
            RecipientRow(
                slot=int(item.slot),
                campaign_recipient_id=int(item.campaign_recipient_id),
                recipient_basis=str(item.recipient_basis),
                item_status=str(item.status),
                reason_code=item.reason_code,
                message_state=message_state(item),
                execution_error=has_execution_error(item),
                send_anchor=anchor,
                identity=identity,
                attribution_state=state,
                reasons=reasons,
                bookings=matched,
                reconciliation_required=bool(item.reconciliation_required),
                manual_cleanup_required=bool(item.manual_cleanup_required),
                send_attempt_count=int(item.send_attempt_count or 0),
            )
        )

    period = None
    if header.campaign_period_start is not None and header.campaign_period_end is not None:
        period = f"{header.campaign_period_start.date().isoformat()}..{header.campaign_period_end.date().isoformat()}"

    return BookingReport(
        report_known=True,
        batch_id=int(header.id),
        preview_run_id=int(header.campaign_run_id),
        batch_exists=True,
        batch_status=str(header.status),
        schema_version=str(header.request_schema_version),
        campaign_period=period,
        recipient_count=len(items),
        preview_active_count=len(items),
        rows=tuple(rows),
        scan=scan,
        registry_ready=registry.ready,
        branches=_branch_rows(registry, scan),
        generated_at=moment,
        reasons=registry_reasons,
    )


__all__ = [
    "ATTRIBUTION_POLICY_VERSION",
    "ATTRIBUTION_WINDOW",
    "ATTRIBUTION_WINDOW_DAYS",
    "BATCH_UNKNOWN",
    "BOOKING_ACTIVE",
    "BOOKING_CANCELLED",
    "BOOKING_COMPLETED",
    "BOOKING_FOUND",
    "BOOKING_NOT_OBSERVED",
    "BOOKING_STATE_UNKNOWN",
    "IDENTITY_CONFLICT",
    "IDENTITY_NUMERIC_MISSING",
    "IDENTITY_PROVEN",
    "INSUFFICIENT_DATA",
    "LIFECYCLE_HINTS",
    "MAX_SCANNED_EVENTS",
    "MESSAGE_ACCEPTED",
    "MESSAGE_ATTEMPT_UNCONFIRMED",
    "MESSAGE_DELIVERED",
    "MESSAGE_NOT_SENT",
    "MESSAGE_READ",
    "MESSAGE_REFUNDED",
    "REGISTRY_UNAVAILABLE",
    "REJECT_CONFLICTING_CREATION",
    "REJECT_CREATED_AT",
    "REJECT_CUSTOMER_ID",
    "REJECT_FOREIGN_LOCATION",
    "REJECT_INVALID_PAYLOAD",
    "REJECT_LOCATION_MISMATCH",
    "REJECT_TRUNCATED",
    "SEND_ATTEMPT_UNCONFIRMED",
    "SEND_CONFIRMED",
    "SEND_NOT_ATTEMPTED",
    "SOURCE_CLIENT_UUID_BINDING",
    "SOURCE_FROZEN_LOCAL_CARD",
    "BookingReport",
    "CaptureScan",
    "ObservedBooking",
    "RecipientIdentity",
    "RecipientRow",
    "SendAnchor",
    "build_report",
    "has_execution_error",
    "message_state",
    "resolve_identities",
    "resolve_send_anchor",
    "scan_capture",
]
