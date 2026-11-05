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
* It never credits a creation to somebody the creation delivery did not name,
  in either direction. With a numeric id on the card the two are compared; with
  none — the ordinary shape of a manual recipient's card — the creation's own
  numeric id is resolved BACK to the proven uuid it belongs to in this
  workspace, and a positive link to somebody else refuses the attribution. No
  id is invented and no customer is linked by phone.
* It never reads a booking it had ALREADY HEARD OF before the attempt as a
  booking made since. A delivery about a booking proves the booking existed when
  that delivery was made, which is a lower bound on its existence and never a
  creation date — so a booking whose first sighting predates the mailing stops
  blocking a recipient, including one of their own sitting undated in their
  current history. A booking whose creation deliveries CONTRADICT each other is
  not settled this way: an early sighting answers an incomplete record, never a
  self-contradictory one.
* It never reads an ambiguous reverse link as an absent one. A numeric id two
  cards of this workspace claim against two different proven uuids is a positive
  claim that the creation may be somebody else's, and the rule that follows a
  genuinely absent link is the permissive one — so collapsing the two credited
  an unrelated person's booking to a manual recipient.
* It never reads a booking somebody else provably made as one that could have
  left THIS person's list unseen. A reassignment of a booking whose creator
  resolves to another proven uuid is an event in that person's history, and the
  recipient being judged stays provable.
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
from typing import Any, Collection, Final, Mapping

from sqlalchemy import select
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
from altegio_bot.models.models import PROVIDER_EASYWEEK, Client, EasyWeekVoucherProductionBatchItem

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
    # Bookings in this person's CURRENT history whose creation instant the
    # capture could not establish and which it had provably already heard of
    # before the attempt. They carry no creation date — none is invented — and
    # they answer the only question §46.5's creation rule asks of them: no,
    # this cannot be a booking made since the mailing.
    existed_before_attempt: frozenset[uuid_module.UUID] = frozenset()

    def creation_of(self, booking_uuid: uuid_module.UUID) -> datetime | None:
        return self.created_at.get(booking_uuid)

    def existed_at_attempt(self, booking_uuid: uuid_module.UUID) -> bool:
        """Was this booking proven to exist already when the attempt was made?

        A weaker fact than a creation date and a sufficient one for the rule
        that asks whether anything is NEW. Kept apart from
        :attr:`created_at` precisely so the two are never confused: a lower
        bound on when a booking existed is not a statement about when it was
        made, and only the latter can exclude anybody.
        """
        return booking_uuid in self.existed_before_attempt

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


@dataclass(frozen=True)
class CreationOwners:
    """numeric customer id → whose it is, in the three states it can be in.

    The reverse of the bridge ``resolve_identities`` walks, and the three
    states are not interchangeable:

    * **one proven uuid.** The link is positive and decides, in either
      direction — the creation is this person's or demonstrably somebody
      else's;
    * **conflicting.** Two cards of this workspace carry the same numeric id
      against two different proven uuids. That is not an absent link: the
      capture holds a POSITIVE claim that this creation may belong to somebody
      else, and reading it as "nothing known" is how an unrelated person's
      visit to the booking page used to become this recipient's engagement;
    * **absent.** No card of this workspace bridges that id at all, and
      nothing has been established either way.

    Collapsing the middle state into the last one is exactly the bug: both
    looked like "no link", and the rule that follows an absent link is the
    permissive one.
    """

    single: Mapping[int, uuid_module.UUID] = field(default_factory=dict)
    conflicting: frozenset[int] = frozenset()

    def owner_of(self, numeric_id: int) -> uuid_module.UUID | None:
        return self.single.get(int(numeric_id))

    def is_conflicting(self, numeric_id: int) -> bool:
        return int(numeric_id) in self.conflicting


async def _creation_owner_uuids(
    session: AsyncSession,
    *,
    customer_ids: Collection[int],
    registry: EasyWeekLocationRegistry,
) -> CreationOwners:
    """Read the numeric ids a creation delivery named back to their owners.

    UUID-first in the same sense as the forward bridge: the link is a card of
    this workspace that carries BOTH a numeric id and a proven customer uuid,
    so reading it backwards names the person a creation delivery was about. No
    phone is involved and no id is invented.

    Scoped to the configured branches, so a card of another workspace cannot
    speak about this mailing's bookings — and cannot manufacture a conflict
    either, which is what keeps an unrelated tenant's numbering out of this.
    """
    wanted = {int(value) for value in customer_ids}
    if not wanted or not registry.ready:
        return CreationOwners()
    rows = (
        await session.execute(
            select(Client.altegio_client_id, Client.easyweek_customer_uuid)
            .where(Client.provider == PROVIDER_EASYWEEK)
            .where(Client.company_id.in_(sorted(registry.locations)))
            .where(Client.altegio_client_id.in_(sorted(wanted)))
            .where(Client.easyweek_customer_uuid.is_not(None))
        )
    ).all()
    claims: dict[int, set[uuid_module.UUID]] = {}
    for numeric, customer_uuid in rows:
        claims.setdefault(int(numeric), set()).add(customer_uuid)
    return CreationOwners(
        single={numeric: next(iter(owners)) for numeric, owners in claims.items() if len(owners) == 1},
        conflicting=frozenset(numeric for numeric, owners in claims.items() if len(owners) > 1),
    )


# Whose creation a booking's own creation delivery named, as far as the
# capture can establish it. Three answers rather than two, because "proven
# somebody else's" and "nobody knows" license entirely different conclusions:
# the first takes the booking out of this person's account of themselves, and
# the second refuses to answer about them at all.
ATTRIBUTION_THEIRS: Final = "theirs"
ATTRIBUTION_ANOTHERS: Final = "anothers"
ATTRIBUTION_UNRESOLVED: Final = "unresolved"


def _attribution(
    booking: ObservedBooking,
    *,
    numeric_id: int | None,
    slot_uuid: uuid_module.UUID | None = None,
    owners: CreationOwners | None = None,
) -> str:
    """Whose creation is this, as far as the capture can prove?

    The creation delivery NAMES a customer. When this person's numeric id is
    proven, that comparison is available and it decides — and it needs no
    separate move delivery to disagree. The earlier rule only distrusted a
    booking whose deliveries contradicted each other, so a booking sitting in
    somebody's current history whose single creation named a DIFFERENT customer
    was credited to them: their own engagement, invented from somebody else's
    visit to the booking page.

    With no numeric bridge on the card the comparison is made the other way
    round, through :class:`CreationOwners`. A single proven uuid settles it in
    either direction. An id two cards of this workspace claim against two
    different uuids settles nothing and must not fall through to the permissive
    rule below: a positive claim that this creation may be somebody else's is
    not the same as never having heard of the id.

    Only a genuinely absent link reaches the older rule: a booking nothing
    moved belongs to whoever created it, and one whose deliveries disagree
    about its owner is nobody's as far as this module is concerned.
    """
    if numeric_id is not None:
        return ATTRIBUTION_THEIRS if int(booking.customer_id) == int(numeric_id) else ATTRIBUTION_ANOTHERS
    links = owners if owners is not None else CreationOwners()
    if links.is_conflicting(booking.customer_id):
        return ATTRIBUTION_UNRESOLVED
    owner = links.owner_of(booking.customer_id)
    if owner is not None and slot_uuid is not None:
        return ATTRIBUTION_THEIRS if owner == slot_uuid else ATTRIBUTION_ANOTHERS
    return ATTRIBUTION_THEIRS if not booking.customer_reassigned else ATTRIBUTION_UNRESOLVED


def _dispute_predates(dispute: DisputedBooking, *, anchor: datetime | None) -> bool:
    """Was this booking proven to exist ALREADY when the attempt was made?

    A delivery about a booking proves the booking existed when that delivery
    was made. That is a lower bound on its existence and never a creation date,
    and it is exactly the fact §46.5's creation rule needs about a booking it
    is asked to treat as possibly new: a booking this deployment had already
    heard of before the mailing is not one made since.

    Self-contradictory disputes are excluded, and that exclusion is the whole
    care this rule needs. An early sighting settles an INCOMPLETE record — no
    creation captured, or one that stated no usable instant. It settles nothing
    about a booking whose own creation deliveries name two owners or two
    instants: there the record does not merely lack a fact, it asserts two that
    cannot both hold, and letting the earliest delivery of such a booking clear
    it would hide the louder problem behind the quieter one.
    """
    if anchor is None or dispute.contradictory:
        return False
    if dispute.first_observed_at is not None and dispute.first_observed_at <= anchor:
        return True
    # Or its creation deliveries all state an instant at or before the attempt.
    return bool(dispute.created_at) and all(moment <= anchor for moment in dispute.created_at)


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

    What takes a dispute out of the way is :func:`_dispute_predates`, and it
    invents no creation date. It is asked before either test above, because a
    booking proven to have existed when the mailing went out cannot be a
    booking made since it — and that is true of the person's own visible
    bookings exactly as it is of a stranger's.

    With no numeric bridge the ownership test cannot be made, so a dispute
    first seen AFTER the attempt and outside their list stays unknown: that is
    the window in which it really could be theirs.
    """
    if _dispute_predates(dispute, anchor=anchor):
        # Asked FIRST, and so applying to a booking the live read can see as
        # well. The question this rule exists to answer is whether anything
        # could have appeared since the mailing, and a booking already observed
        # by then provably could not have — whether or not it is still in
        # somebody's list, and with no creation date guessed for it. Asking it
        # second meant the person's own cancelled booking, visible in their
        # history and heard of weeks before the send, held them as a possible
        # new one for ever.
        return False
    if dispute.booking_uuid in live:
        return True
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

    # Which slots have a numeric bridge on their own card, decided first and
    # without touching the database, so the reverse lookup below is asked about
    # exactly the creations that need it.
    bridges: dict[int, int | None] = {}
    refused: dict[int, str] = {}
    for item in items:
        slot = int(item.slot)
        identity = identities.get(slot)
        if identity is not None and identity.state == IDENTITY_PROVEN and identity.customer_id is not None:
            bridges[slot] = int(identity.customer_id)
            continue
        # Two numeric ids claiming one person, or a frozen card that now names a
        # different uuid, is an identity problem and not a missing bridge. A card
        # that simply carries no numeric id is neither.
        if identity is None or identity.state != IDENTITY_NUMERIC_MISSING:
            refused[slot] = conflict_reason or unproven_reason
            continue
        bridges[slot] = None

    needed_owners: set[int] = set()
    for item in items:
        slot = int(item.slot)
        if bridges.get(slot) is not None or slot in refused:
            continue
        for booking_uuid in slot_bookings.get(slot, ()) if slot_bookings is not None else ():
            booking = by_uuid.get(booking_uuid)
            if booking is not None:
                needed_owners.add(int(booking.customer_id))
        # And the bookings OUTSIDE this person's list that a reassignment
        # touched. They are the reason a bridge-less slot is refused at all, so
        # they are precisely where a proven creator is worth asking about: a
        # booking somebody else provably made and then handed on is not a
        # booking that could have left THIS person's list unseen.
        for booking in live_strays:
            needed_owners.add(int(booking.customer_id))
    owners = await _creation_owner_uuids(session, customer_ids=needed_owners, registry=locations)

    by_slot: dict[int, SlotCreations] = {}
    for item in items:
        slot = int(item.slot)
        if scan.truncated:
            # A partial pass is not an answer about anybody.
            by_slot[slot] = SlotCreations(False, unproven_reason)
            continue
        if slot in refused:
            by_slot[slot] = SlotCreations(False, refused[slot])
            continue
        live = frozenset(slot_bookings.get(slot, ()) if slot_bookings is not None else ())
        numeric_id = bridges.get(slot)
        bridge = "frozen_card_numeric_id" if numeric_id is not None else None
        anchor = item.send_attempted_at

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
            if (
                _attribution(
                    booking,
                    numeric_id=numeric_id,
                    slot_uuid=item.easyweek_customer_uuid,
                    owners=owners,
                )
                == ATTRIBUTION_THEIRS
            ):
                created[booking_uuid] = booking.booking_created_at

        # Bookings of theirs the capture cannot date and had provably already
        # heard of when the attempt was made. No creation date is taken from
        # the sighting; all it answers is that nothing NEW is hiding here.
        existed_before = frozenset(
            dispute.booking_uuid
            for dispute in scan.disputed
            if dispute.booking_uuid in live and _dispute_predates(dispute, anchor=anchor)
        )

        proven = True
        reason: str | None = None
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
                booking.booking_uuid not in live
                and anchor is not None
                and booking.booking_created_at > anchor
                # Unless its creator is PROVEN to be somebody else. The whole
                # worry is a booking of THIS person's that left their list
                # unseen, and a creation resolved through a card of this
                # workspace to another proven uuid is demonstrably not one. An
                # absent or conflicting link proves nothing, and those stay
                # unknown.
                and _attribution(
                    booking,
                    numeric_id=None,
                    slot_uuid=item.easyweek_customer_uuid,
                    owners=owners,
                )
                != ATTRIBUTION_ANOTHERS
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
            existed_before_attempt=existed_before,
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
