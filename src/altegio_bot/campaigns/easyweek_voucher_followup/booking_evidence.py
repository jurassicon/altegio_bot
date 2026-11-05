"""When a booking was CREATED, as opposed to when the appointment is (§46.5).

The two dates §46.5 keeps apart
-------------------------------
The section names two different exclusions, and they rest on two different
facts:

* «новая запись после исходной попытки (включая впоследствии отменённую)» — a
  booking that was **created** after this system last wrote to the person. That
  is engagement since the mailing, and a cancellation afterwards does not undo
  it;
* «существующая будущая активная запись» — an appointment that still lies
  ahead, whenever it was made. That is somebody who already has a slot booked.

Reading the appointment time as the creation time gets both of them wrong, in
both directions. A booking made in June for a July appointment, cancelled in
July, looks "new" although nothing happened since the mailing; and a booking
made the day after the mailing to record a visit that already happened looks
"old" although it is exactly the engagement the rule is about.

Where the creation time comes from
----------------------------------
From the capture this project already has, and from nowhere else. EasyWeek's
booking webhooks carry ``booking_created_at`` — the provider's own statement of
when the booking was made — and the §PR-17 booking report already validates it
narrowly, deduplicates by booking identity, keeps creation evidence apart from
lifecycle evidence, and bounds its scan. This module reuses that whole proof
(:func:`...booking_attribution.scan_capture`) rather than writing a second one.

How far back the scan reaches
-----------------------------
Further than the report's own window, and deliberately. The report asks "was a
booking created after the send?", so a day of slack below the earliest send is
all it can ever need. The follow-up asks a second question about the SAME
bookings — "when was this one, which the live read can see, actually made?" —
and the answer may be months before the mailing. With the report's floor that
evidence sits in the database unread and every such recipient comes out
``unknown``, which is safe and wrong.

So this module passes ``lookback=None`` and takes the bounded newest-first pass
that :func:`...booking_attribution.scan_capture` documents: the most recent
:data:`...booking_attribution.MAX_SCANNED_EVENTS` lifecycle deliveries, read
through the primary key and applied in their own order, with ``truncated``
reported rather than a short answer. Bounded in rows and in work, and no change
to what the report itself scans.

Two ways to find one person's creations
---------------------------------------
By booking identity, and by numeric customer id:

* **by booking uuid.** A live read keyed on a proven customer uuid returns the
  ids of that person's bookings, so their creations can be looked up without
  knowing any numeric id at all. This is what makes a genuine manual recipient
  provable: ``ensure_local_identity`` creates their card with the proven uuid
  and ``altegio_client_id=None``, which is the ordinary shape of that data and
  not damage, and demanding a numeric bridge from it would refuse somebody for
  the way their card was created;
* **by numeric id**, when the frozen card carries one. This finds the creations
  the live set cannot show — a booking of theirs that has since been moved to
  somebody else, and so left their list.

Without the numeric bridge the live set is the only account of this person's
bookings, and the one thing it cannot show is a booking that left it. So a
reassignment observed after this slot's own send anchor, for a booking that is
not in their list, leaves this slot ``unknown``: that is exactly the case where
the missing bridge could hide a real creation. A quiet window with no
reassignments hides nothing, and the recipient is provable.

What this module refuses to do
------------------------------
* It never reads the webhook's ARRIVAL time as the creation time. A delivery can
  arrive days late, and the report's own validator rejects a creation payload
  whose ``booking_created_at`` cannot be parsed rather than substituting
  ``received_at``.
* It never reads "no creation was captured" as "no booking was created". The
  capture is a bounded scan of deliveries this deployment happened to store;
  §46.5 forbids turning that into proof. An absent creation for a booking the
  live read can see is ``unknown``, with its own reason.
* It never credits a creation to somebody the creation delivery did not name.
  When the recipient's numeric id is proven the two are simply compared, so a
  booking sitting in their current history whose creation named a different
  customer is not their engagement — no separate move delivery is needed for
  the disagreement to exist.
* It never reads a booking the capture could not attribute as a booking that
  did not happen. Two creations of one booking naming different owners, and a
  booking with lifecycle deliveries from two customers and no proven creation,
  are reported by the capture as DISPUTED; a dispute that could belong to this
  person leaves the slot ``unknown`` instead of letting an empty live history
  read as "nothing since the mailing".
* It never moves a creation to the customer a booking was LATER moved to. The
  creation stays with the customer its own creation delivery named — that is
  the historical fact, and a later reassignment does not undo somebody's visit
  to the booking page. For the new owner that booking's creation is not theirs
  and is reported unknown rather than credited to them.
* It invents no numeric id, bridges no identity by phone, and asks the provider
  for nothing: the whole module is a local read.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Collection, Mapping

from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_production.booking_attribution import (
    BOOKING_CANCELLED,
    IDENTITY_NUMERIC_MISSING,
    IDENTITY_PROVEN,
    DisputedBooking,
    ObservedBooking,
    resolve_identities,
    scan_capture,
)
from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.models.models import EasyWeekVoucherProductionBatchItem

__all__ = ["CapturedCreations", "SlotCreations", "load_creation_evidence"]


@dataclass(frozen=True)
class SlotCreations:
    """What the capture proves about ONE slot's person.

    ``proven`` is the only licence to conclude anything negative. While it is
    ``False`` this person's creations are simply not established — the scan was
    truncated, two numeric ids claim them, or a booking may have left their list
    unseen — and every booking of theirs whose creation matters becomes
    ``unknown``.
    """

    proven: bool
    reason: str | None = None
    # booking uuid → the provider's own creation instant, for the creations this
    # person is proven to have made. Only bookings whose creation delivery was
    # captured, validated and attributed to THEM appear here.
    created_at: dict[uuid_module.UUID, datetime] = field(default_factory=dict)
    # How the numeric customer id was established, when it was. Shape fact for
    # the stored snapshot; ``None`` means the slot was proven from booking
    # identities alone.
    bridge: str | None = None
    # Bookings whose deliveries disagree about which customer they belong to.
    # Reported rather than used: a creation is never credited to the customer a
    # booking was later moved to.
    reassigned: frozenset[uuid_module.UUID] = frozenset()

    def creation_of(self, booking_uuid: uuid_module.UUID) -> datetime | None:
        return self.created_at.get(booking_uuid)

    def created_after(self, boundary: datetime) -> bool:
        """Did this person create any booking after *boundary*?

        Cancelled ones included, by §46.5: somebody who booked and changed their
        mind has engaged with us since the mailing.
        """
        return any(created > boundary for created in self.created_at.values())


@dataclass(frozen=True)
class CapturedCreations:
    """One bounded capture pass, resolved per slot."""

    complete: bool
    scanned_events: int = 0
    truncated: bool = False
    registry_ready: bool = True
    # How many bookings the capture saw and could not attribute. A shape fact
    # for the stored snapshot and the operator's screen; which of them matter to
    # which slot is decided per slot.
    disputed: int = 0
    by_slot: dict[int, SlotCreations] = field(default_factory=dict)

    def for_slot(self, slot: int, *, reason: str) -> SlotCreations:
        found = self.by_slot.get(int(slot))
        if found is not None:
            return found
        return SlotCreations(False, reason)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "scanned_events": int(self.scanned_events),
            "truncated": bool(self.truncated),
            "registry_ready": bool(self.registry_ready),
            "disputed_bookings": int(self.disputed),
        }


def _attributable(booking: ObservedBooking, *, numeric_id: int | None) -> bool:
    """May this booking's creation be credited to the person being judged?

    The creation delivery NAMES a customer. When this person's numeric id is
    proven, that comparison is available and it decides — and it needs no
    separate move delivery to disagree. The earlier rule only distrusted a
    booking whose deliveries contradicted each other, so a booking sitting in
    somebody's current history whose single creation named a DIFFERENT customer
    was credited to them: their own engagement, invented from somebody else's
    visit to the booking page.

    With no numeric bridge the comparison cannot be made at all. The live read,
    keyed on the proven uuid, has established that the booking is theirs NOW,
    and the older rule stands: a booking nothing moved belongs to whoever
    created it, and one whose deliveries disagree is not attributable.
    """
    if numeric_id is not None:
        return int(booking.customer_id) == int(numeric_id)
    return not booking.customer_reassigned


def _dispute_touches(
    dispute: DisputedBooking,
    *,
    numeric_id: int | None,
    live: frozenset[uuid_module.UUID],
    anchor: datetime | None,
) -> bool:
    """Could this unattributable booking be the person being judged's?

    Two ways, and either one is enough to refuse an answer about them:

    * it is in their CURRENT history. Whoever made it, its creation is in
      dispute, so "nothing new since the attempt" cannot be asserted about a
      list that contains it;
    * a delivery of it named their proven numeric id. Then it may be their
      booking that left the list, or one whose ownership two deliveries
      disagree about — and with no usable creation time, or one after the
      attempt, neither verdict is available.

    A dispute whose every stated creation time is at or before the attempt, and
    which is not in their list, cannot be a booking made since the mailing and
    is left alone. With no numeric bridge the second test cannot be made, so an
    undatable dispute outside their list is treated like a booking that left
    somebody's list: unknown.
    """
    if dispute.booking_uuid in live:
        return True
    datable_before_anchor = (
        bool(dispute.created_at) and anchor is not None and all(moment <= anchor for moment in dispute.created_at)
    )
    if datable_before_anchor:
        return False
    if numeric_id is not None:
        return int(numeric_id) in dispute.customer_ids
    return True


async def load_creation_evidence(
    session: AsyncSession,
    *,
    items: list[EasyWeekVoucherProductionBatchItem],
    now: datetime,
    unproven_reason: str,
    conflict_reason: str | None = None,
    slot_bookings: Mapping[int, Collection[uuid_module.UUID]] | None = None,
    registry: EasyWeekLocationRegistry | None = None,
) -> CapturedCreations:
    """One bounded local pass: which bookings each slot's person created, when.

    Reads nothing external and writes nothing, and takes no lock. Called AFTER
    the live booking reads have finished, in a session of its own, because the
    booking identities the live read returns are half of how a person's
    creations are found — and because no database handle may be open while a
    request is in flight.

    ``slot_bookings`` is each slot's live booking identities, for the slots
    whose live read came back complete. A slot missing from it gets no evidence:
    its live set is already unknown and the booking rules refuse it on that.

    ``conflict_reason`` is what a slot whose IDENTITY does not line up is
    reported as — two numeric ids claiming one person, or a frozen card that now
    names a different uuid. That is an identity problem rather than a missing
    bridge, and saying so is what lets the operator read the right sentence.

    The branch scope comes from the configured location registry, so a creation
    at any configured branch of the same workspace counts, which is what §46.5
    asks of the booking question.
    """
    locations = registry if registry is not None else configured_easyweek_locations()
    anchors = [item.send_attempted_at for item in items if item.send_attempted_at is not None]
    if not anchors or not locations.ready:
        # No anchor means nothing to ask about; an unconfigured registry means
        # the branch scope itself is unknown. Neither is an empty answer.
        return CapturedCreations(
            complete=False,
            registry_ready=bool(locations.ready),
            by_slot={int(item.slot): SlotCreations(False, unproven_reason) for item in items},
        )

    identities = await resolve_identities(session, items=items)
    # No lower bound: the question is when a booking the live read can see was
    # MADE, which may be long before the mailing. Bounded newest-first by the
    # report's own row cap, which reports truncation.
    scan = await scan_capture(session, anchors=list(anchors), now=now, registry=locations, lookback=None)
    grouped = scan.by_customer()
    by_uuid = scan.by_booking_uuid()
    live_strays = [booking for booking in scan.bookings if booking.customer_reassigned]

    by_slot: dict[int, SlotCreations] = {}
    for item in items:
        slot = int(item.slot)
        if scan.truncated:
            # A partial pass is not an answer about anybody.
            by_slot[slot] = SlotCreations(False, unproven_reason)
            continue
        live = frozenset(slot_bookings.get(slot, ()) if slot_bookings is not None else ())
        identity = identities.get(slot)
        numeric_id: int | None = None
        bridge: str | None = None
        if identity is not None and identity.state == IDENTITY_PROVEN and identity.customer_id is not None:
            numeric_id = int(identity.customer_id)
            bridge = "frozen_card_numeric_id"
        elif identity is None or identity.state != IDENTITY_PROVEN:
            # Two numeric ids claiming one person, or a frozen card that now
            # names a different uuid, is an identity problem and not a missing
            # bridge. A card that simply carries no numeric id is neither, and
            # falls through to the booking-identity path below.
            if identity is None or identity.state != IDENTITY_NUMERIC_MISSING:
                by_slot[slot] = SlotCreations(False, conflict_reason or unproven_reason)
                continue

        created: dict[uuid_module.UUID, datetime] = {}
        reassigned: set[uuid_module.UUID] = set()
        if numeric_id is not None:
            for booking in grouped.get(numeric_id, []):
                if booking.customer_reassigned:
                    reassigned.add(booking.booking_uuid)
                # Grouped by the customer its own CREATION named, so a booking
                # that has since moved away is still this person's creation.
                created[booking.booking_uuid] = booking.booking_created_at
        for booking_uuid in live:
            booking = by_uuid.get(booking_uuid)
            if booking is None:
                continue
            if booking.customer_reassigned:
                reassigned.add(booking_uuid)
            if _attributable(booking, numeric_id=numeric_id):
                created[booking_uuid] = booking.booking_created_at

        proven = True
        reason: str | None = None
        anchor = item.send_attempted_at
        # A booking the capture could not attribute at all, which could be
        # theirs. Reported rather than dropped, because an empty live history
        # plus a forgotten dispute reads as "nothing happened since the
        # mailing" — and that is exactly what it is not.
        if any(_dispute_touches(dispute, numeric_id=numeric_id, live=live, anchor=anchor) for dispute in scan.disputed):
            proven = False
            reason = unproven_reason
        elif numeric_id is None:
            # No bridge: the live set is the whole account of this person's
            # bookings, and a booking that LEFT it is what it cannot show. A
            # reassignment after this slot's own anchor, of a booking not in
            # their list, is exactly that case.
            if any(
                booking.booking_uuid not in live and anchor is not None and booking.booking_created_at > anchor
                for booking in live_strays
            ):
                proven = False
                reason = unproven_reason
        by_slot[slot] = SlotCreations(
            proven,
            reason,
            created_at=created,
            bridge=bridge,
            reassigned=frozenset(reassigned),
        )

    return CapturedCreations(
        complete=not scan.truncated,
        scanned_events=int(scan.scanned_events),
        truncated=bool(scan.truncated),
        registry_ready=bool(locations.ready),
        disputed=len(scan.disputed),
        by_slot=by_slot,
    )


# Re-exported so the eligibility rules have one import for the cancelled label
# and cannot drift from the report's own vocabulary.
CAPTURED_CANCELLED = BOOKING_CANCELLED
