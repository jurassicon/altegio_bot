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
* It never reads a numeric bridge as a complete view of a person. The bridge is
  proven in the branches that person's own card covers and says nothing in the
  others, so a booking created in another configured branch and handed on is
  asked about for a bridged slot exactly as it is for a bridge-less one — with
  the same single way out, a creator proven to be somebody else in the branch
  that delivered the creation.
* It never reads matching digits as the same customer. Every statement about a
  customer is a (branch, number) PAIR, because that pair is what names a
  customer record: a booking created in one branch under a number and then
  named by another branch's delivery under the same digits has moved between
  two records, and comparing bare numbers reported it as a booking nothing had
  moved. The pairs a dispute's creations really named are also what its owner
  is resolved from, never a product of its branches and its numbers.
* It never reads a missing field as a fact. A delivery that proved its branch
  and its booking and nothing else still proves those two: a lifecycle delivery
  from another branch that named no customer leaves the booking's custody
  unproven, which is not a change of person and not a proof that nothing
  changed; and a creation delivery that named no customer still states where —
  and, when the payload carried a usable one, when — so a second creation of
  the same booking has to agree with it. Neither names an owner, and neither is
  evidence of its own: a booking nothing else was delivered about holds nobody
  open.
* It never lets what one delivery proved answer for what another left open.
  A creation delivery that named nobody, from a branch no owned creation of
  that booking named, leaves whose creation it was unestablished, and a second
  creation naming a proven owner speaks for its own delivery only — so a
  contradiction about where a booking was made is not set aside because one of
  the two deliveries is demonstrably a stranger's. From the branch an owned
  creation DID name it is that creation again as far as anything here can tell,
  and refusing on it withdrew an owner the capture had already proven.
* It never reads a delivery that named nobody as proof that nothing moved. The
  permissive rule for a card with no numeric bridge needs both halves: no
  delivery naming a different customer record, and none that named no customer
  at all. That rule reaches every decision about such a card — the bookings in
  their list, the bookings outside it, and the disputed ones — and it used to
  reach only the last two.
* It never answers a question about the CREATOR with a statement about a later
  holder. An update or a cancellation naming somebody else says who has the
  booking now, and a booking this person made and handed on is exactly the case
  the gap is about, so a dispute with no creation observed stays open whatever
  its later deliveries named.
* It never reads a creation instant that its own capture contradicts. A booking
  whose creation claims a moment later than the first delivery stored about it,
  or later than the moment the preview is being computed at, is not a proven
  creation — and the attempt's own date decides neither comparison.
* It invents no numeric id, bridges no identity by phone, and asks the provider
  for nothing: the whole module is a local read, in statements whose bound
  parameters are bounded by the protocol's own limit rather than by the size of
  the capture.
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
    CaptureScan,
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
    """(branch, numeric customer id) → whose it is, in three states.

    The reverse of the bridge ``resolve_identities`` walks, and keyed on the
    PAIR rather than the number alone. A numeric customer id is unique per
    branch and not per workspace, so the same digits name one person in
    Karlsruhe and somebody entirely different in Ettlingen. Reading a creation
    delivered by branch X through a card of branch Y is reading a different
    customer's record — the number matched and the person did not.

    The three states are not interchangeable:

    * **one proven uuid**, on a card of the creation's OWN branch. The link is
      positive and decides in either direction — the creation is this person's
      or demonstrably somebody else's;
    * **conflicting.** Two cards of that one branch carry the same numeric id
      against two different proven uuids. Defence in depth: the schema makes a
      number unique within a branch, so this shape should not exist, and if a
      historical row produces it the link is not positive;
    * **absent.** No card of that branch bridges that id, and nothing is
      established either way. A card in another configured branch is this
      state and NOT the first one: a number matching only elsewhere is not
      enough to establish an owner.

    Collapsing any of these into the last one is the family of bugs this class
    exists for, because the rule that follows an absent link is the permissive
    one.
    """

    in_branch: Mapping[tuple[int, int], uuid_module.UUID] = field(default_factory=dict)
    conflicting: frozenset[tuple[int, int]] = frozenset()

    def owner_in(self, company_id: int, numeric_id: int) -> uuid_module.UUID | None:
        return self.in_branch.get((int(company_id), int(numeric_id)))

    def is_conflicting(self, company_id: int, numeric_id: int) -> bool:
        return (int(company_id), int(numeric_id)) in self.conflicting


# How many bound parameters one statement may carry. The PostgreSQL wire
# protocol counts them in a 16-bit field, so the driver refuses a statement
# with more — and this lookup asks about one number per (branch, customer id)
# the capture named, which a single branch with a long history exceeds. The
# limit is a property of the protocol and not a safety bound of this phase:
# nothing here reads less than it did, the work is simply split across
# statements.
MAX_STATEMENT_PARAMETERS: Final = 32_767


async def _creation_owner_uuids(
    session: AsyncSession,
    *,
    wanted: Collection[tuple[int, int]],
    registry: EasyWeekLocationRegistry,
) -> CreationOwners:
    """Read the (branch, numeric id) a creation delivery named back to its owner.

    UUID-first in the same sense as the forward bridge: the link is a card of
    this workspace that carries BOTH a numeric id and a proven customer uuid,
    so reading it backwards names the person a creation delivery was about. No
    phone is involved and no id is invented.

    Scoped to the configured branches, so a card of another workspace cannot
    speak about this mailing's bookings — and cannot manufacture a conflict
    either, which is what keeps an unrelated tenant's numbering out of this.
    Within that scope the branch of the CARD must equal the branch of the
    creation, which is what makes the answer about a person rather than about a
    number.
    """
    pairs = {(int(company_id), int(numeric)) for company_id, numeric in wanted}
    if not pairs or not registry.ready:
        return CreationOwners()
    branches = sorted({company_id for company_id, _numeric in pairs} & set(registry.locations))
    numbers = sorted({numeric for _company_id, numeric in pairs})
    if not branches:
        return CreationOwners()
    # One parameter for the provider, one per branch and one per number. The
    # numbers are asked for in order and in slices that fit, so the answer is
    # the same answer the single statement gave while it fitted — every number
    # is asked about, including the last slice, and a failure is never turned
    # into an empty selection.
    budget = max(1, MAX_STATEMENT_PARAMETERS - 1 - len(branches))
    claims: dict[tuple[int, int], set[uuid_module.UUID]] = {}
    for start in range(0, len(numbers), budget):
        rows = (
            await session.execute(
                select(Client.company_id, Client.altegio_client_id, Client.easyweek_customer_uuid)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.company_id.in_(branches))
                .where(Client.altegio_client_id.in_(numbers[start : start + budget]))
                .where(Client.easyweek_customer_uuid.is_not(None))
            )
        ).all()
        for company_id, numeric, customer_uuid in rows:
            key = (int(company_id), int(numeric))
            if key in pairs:
                claims.setdefault(key, set()).add(customer_uuid)
    return CreationOwners(
        in_branch={key: next(iter(owners)) for key, owners in claims.items() if len(owners) == 1},
        conflicting=frozenset(key for key, owners in claims.items() if len(owners) > 1),
    )


# Whose creation a booking's own creation delivery named, as far as the
# capture can establish it. Three answers rather than two, because "proven
# somebody else's" and "nobody knows" license entirely different conclusions:
# the first takes the booking out of this person's account of themselves, and
# the second refuses to answer about them at all.
ATTRIBUTION_THEIRS: Final = "theirs"
ATTRIBUTION_ANOTHERS: Final = "anothers"
ATTRIBUTION_UNRESOLVED: Final = "unresolved"


def _owner_verdict(
    *,
    company_id: int,
    customer_id: int,
    numeric_id: int | None,
    numeric_branches: Collection[int],
    slot_uuid: uuid_module.UUID | None,
    owners: CreationOwners | None,
) -> str | None:
    """Whose is a creation delivered by *company_id* naming *customer_id*?

    ``None`` means nothing has been established either way — deliberately
    distinct from "somebody else's", because the two license opposite
    conclusions and treating the first as the second is how a stranger's
    booking became a recipient's engagement.

    Both directions of the UUID ↔ numeric-id link are asked here, and both are
    asked WITHIN the creation's own branch:

    * the reverse one first: a card of that branch carrying both the number and
      a proven uuid names the person the delivery was about;
    * then the forward one: this recipient's own number, when their card proves
      it in that same branch, is compared with the number the delivery named.

    A number proven only in ANOTHER branch establishes nothing here. The digits
    may match, but a numeric customer id is unique per branch, so matching
    digits elsewhere are a different customer record — and a positive link
    built on them credits one person with another's booking.
    """
    links = owners if owners is not None else CreationOwners()
    if links.is_conflicting(company_id, customer_id):
        return None
    owner = links.owner_in(company_id, customer_id)
    if owner is not None and slot_uuid is not None:
        return ATTRIBUTION_THEIRS if owner == slot_uuid else ATTRIBUTION_ANOTHERS
    if numeric_id is not None and int(company_id) in {int(value) for value in numeric_branches}:
        return ATTRIBUTION_THEIRS if int(customer_id) == int(numeric_id) else ATTRIBUTION_ANOTHERS
    return None


def _attribution(
    booking: ObservedBooking,
    *,
    numeric_id: int | None,
    numeric_branches: Collection[int] = (),
    slot_uuid: uuid_module.UUID | None = None,
    owners: CreationOwners | None = None,
) -> str:
    """May this booking's creation be credited to the person being judged?

    The creation delivery NAMES a customer, in a branch. :func:`_owner_verdict`
    answers from that pair whenever anything is established, and it needs no
    separate move delivery to disagree: a booking sitting in somebody's current
    history whose single creation named a DIFFERENT customer of that branch is
    not their engagement.

    When nothing is established the older rule stands — but only for a card
    with no numeric bridge at all, which is the ordinary shape of a manual
    recipient's. For a recipient whose own number IS proven, a creation in a
    branch their card does not cover is simply unresolved: being permissive
    there is what credited them with a booking made by whoever holds those
    digits somewhere else.
    """
    verdict = _owner_verdict(
        company_id=int(booking.company_id),
        customer_id=int(booking.customer_id),
        numeric_id=numeric_id,
        numeric_branches=numeric_branches,
        slot_uuid=slot_uuid,
        owners=owners,
    )
    if verdict is not None:
        return verdict
    if numeric_id is not None:
        return ATTRIBUTION_UNRESOLVED
    # The older rule, for a card with no numeric bridge: a booking nothing
    # moved belongs to whoever created it, and the only account of this
    # person's bookings is the list it is in. "Nothing moved it" needs both
    # halves — no delivery naming a different customer record, and no delivery
    # that named NO customer. The second was missing, so a nameless delivery,
    # which is precisely the one that cannot say the booking stayed, read as
    # proof that it did: a booking made after the mailing excluded this person
    # and the same booking with an older instant left them included.
    if booking.customer_reassigned or booking.custody_unproven:
        return ATTRIBUTION_UNRESOLVED
    return ATTRIBUTION_THEIRS


def _unowned_creation_branches(dispute: DisputedBooking) -> frozenset[int]:
    """Branches whose creation of this booking named nobody and nothing else did.

    A creation delivery that named no customer leaves one claim about who made
    the booking unstated. Whether that claim is a SECOND one depends on the
    branch it came from, and one booking is made in exactly one branch:

    * from a branch an OWNED creation of the same booking also named, it may be
      that same creation delivered again. It adds no candidate owner that the
      capture cannot already name — the one thing that would make it a separate
      claim, a different customer, is precisely what it did not state — and a
      disagreement of any other kind it does carry (a second instant, a second
      owner beside it) is counted where disagreements are counted and refuses
      there;
    * from any OTHER branch it cannot be that creation again, because the two
      deliveries cannot both be right about where the booking was made. Then
      whose creation that one was is a second, unanswered question, and a
      proven owner for the first branch does not answer it.

    Refusing on the bare flag made no such distinction, so an ownerless repeat
    of a creation withdrew the owner its own branch had already proven: their
    own cancelled booking, known since before the mailing, became unknown, and
    one stranger's booking held a whole batch open.
    """
    owned = {int(company_id) for company_id, _customer_id in dispute.creation_pairs}
    return frozenset(int(branch) for branch in dispute.ownerless_creation_branches if int(branch) not in owned)


def _dispute_owner(
    dispute: DisputedBooking,
    *,
    numeric_id: int | None,
    numeric_branches: Collection[int] = (),
    slot_uuid: uuid_module.UUID | None = None,
    owners: CreationOwners | None = None,
) -> str | None:
    """Whose creation does a DISPUTED booking name, when it names one at all?

    Separating the two things a dispute can be about, which §46.5 needs kept
    apart: whether its OWNER is in doubt, or only its TIME. A booking whose
    creations agree on the creator and disagree on the instant has a proven
    owner and an unproven date — so it is that person's problem and nobody
    else's, and it has no business holding a stranger's empty history open.

    Asked of the pairs the creation deliveries REALLY named — (that branch,
    that number) — and never of a product of the branches and the numbers,
    which invents customer records nothing observed. Two creations naming two
    branches are a contradiction about the booking; they are not a
    contradiction about its owner when both pairs are read back, through cards
    of this workspace, to one and the same person.

    ``None`` whenever that reading leaves anything open: no creation was
    observed, a pair bridges to nobody, or the pairs disagree about whose it
    is — including the case where one of them may be this very recipient.
    Then nothing about whose it is has been established, which is not the same
    as its being anybody's.
    """
    if not dispute.creation_pairs or _unowned_creation_branches(dispute):
        # A creation delivery that named nobody, from a branch no owned
        # creation of this booking named, leaves one of the claims about who
        # made it simply absent — and a second creation naming a proven owner
        # speaks for its own delivery and not for that one. The known owner was
        # allowed to answer for both, so a contradiction about where the
        # booking was made was set aside as "demonstrably somebody else's"
        # while whose the contradicting delivery was had never been
        # established.
        return None
    verdicts = {
        _owner_verdict(
            company_id=int(company_id),
            customer_id=int(customer_id),
            numeric_id=numeric_id,
            numeric_branches=numeric_branches,
            slot_uuid=slot_uuid,
            owners=owners,
        )
        for company_id, customer_id in dispute.creation_pairs
    }
    if len(verdicts) > 1:
        # The pairs do not agree about whose booking this is, so nothing is
        # established. Fail-closed, deliberately: one of them resolving to the
        # recipient is exactly the case where the dispute may be theirs.
        return None
    (verdict,) = verdicts
    if verdict is not None:
        return verdict
    if numeric_id is not None:
        return None
    # The same fallback a DATED booking of this shape gets, and deliberately
    # the same one: for a card with no numeric bridge, a booking nothing moved
    # belongs to whoever created it. Losing the creation instant must not make
    # a row admissible that would read unknown with it, and it must not make
    # one unknown that would read fine with it either — the instant is not
    # evidence about whose booking it is, in either direction.
    #
    # "Nothing moved it" is read from the pairs, and from the deliveries that
    # named no pair at all: a lifecycle delivery naming the same digits in
    # ANOTHER branch names a different customer record, and one naming no
    # customer says nothing about who holds the booking now — which is not the
    # same as saying nobody else does.
    if dispute.customer_pairs - dispute.creation_pairs or dispute.custody_unproven:
        return None
    return ATTRIBUTION_THEIRS


def _chronology_contradicted(booking: ObservedBooking, *, now: datetime) -> bool:
    """Does its own creation instant contradict when it was first heard of?

    A delivery about a booking proves the booking existed when that delivery
    was made. So a booking whose creation claims an instant LATER than the
    first delivery this deployment stored about it is making two claims that
    cannot both be true — and the one this phase acts on is the harsher: it
    excluded somebody for engagement the capture's own earlier row says had
    already happened. The attempt's own date is not what decides this: a
    cancellation stored the day after the mailing bounds the creation just as
    firmly as one stored weeks before it.

    The same rule at the other end catches a creation dated after the moment
    the preview is being computed at, which nothing can have been created
    after.

    Both comparisons are between instants the capture already holds, and
    neither invents a date: an ordinary late delivery is still allowed to
    arrive after the cancellation it precedes and to carry a truthful EARLIER
    creation instant, which contradicts nothing, and ``received_at`` is still
    never substituted for a creation date.
    """
    if booking.booking_created_at > now:
        return True
    if booking.first_observed_at is None:
        return False
    return booking.booking_created_at > booking.first_observed_at


def _dispute_chronology_contradicted(dispute: DisputedBooking, *, now: datetime) -> bool:
    """Do its own claimed instants fight what was already known about it?

    The same question :func:`_chronology_contradicted` asks of an attributed
    booking, asked of a disputed one — which was the asymmetry: a booking with
    an owner on every creation delivery had its instants checked against the
    first delivery stored about it, and a booking whose creations left an owner
    out did not, although such a record is weaker rather than stronger.

    A claim that this booking was made after it was first heard of, or after
    the moment the preview is being computed at, cannot be true. Nothing is
    invented from that: the claim is simply not evidence, and neither is the
    sighting it fights.
    """
    if any(moment > now for moment in dispute.created_at):
        return True
    if dispute.first_observed_at is None:
        return False
    return any(moment > dispute.first_observed_at for moment in dispute.created_at)


def _dispute_predates(dispute: DisputedBooking, *, anchor: datetime | None, now: datetime) -> bool:
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
    it would hide the louder problem behind the quieter one. A record whose own
    claimed instant fights that earliest delivery
    (:func:`_dispute_chronology_contradicted`) is in the same position, and is
    checked BEFORE the sighting is applied rather than after.
    """
    if anchor is None or dispute.contradictory or _dispute_chronology_contradicted(dispute, now=now):
        return False
    if dispute.first_observed_at is not None and dispute.first_observed_at <= anchor:
        return True
    # Or its creation deliveries all state an instant at or before the attempt.
    return bool(dispute.created_at) and all(moment <= anchor for moment in dispute.created_at)


def _dispute_touches(
    dispute: DisputedBooking,
    *,
    numeric_id: int | None,
    numeric_branches: Collection[int] = (),
    slot_uuid: uuid_module.UUID | None = None,
    owners: CreationOwners | None = None,
    live: frozenset[uuid_module.UUID],
    anchor: datetime | None,
    now: datetime,
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

    Two things take a dispute out of the way, and neither invents a creation
    date: a creator proven to be somebody else (:func:`_dispute_owner`), and a
    booking proven to have existed when the mailing went out
    (:func:`_dispute_predates`). Which of the two applies depends on whether
    the booking is in this person's list, because that is what decides whose
    question the dispute is.

    With no numeric bridge and no proven creator the ownership test cannot be
    made, so a dispute first seen AFTER the attempt and outside their list
    stays unknown: that is the window in which it really could be theirs.
    """
    owner = _dispute_owner(
        dispute,
        numeric_id=numeric_id,
        numeric_branches=numeric_branches,
        slot_uuid=slot_uuid,
        owners=owners,
    )
    if dispute.booking_uuid in live:
        # It is in their list, so this is a booking of THEIRS whose provenance
        # the capture cannot settle, and "has anything appeared since the
        # mailing?" is a question about it. Two facts together answer it: the
        # creation names them, in the branch that made it, and the booking was
        # provably already there. That is the same bar the dated case has to
        # clear, and it has to be the same bar — a row that reads unknown with a
        # usable creation instant must not become admissible by losing it.
        return not (owner == ATTRIBUTION_THEIRS and _dispute_predates(dispute, anchor=anchor, now=now))
    if owner == ATTRIBUTION_ANOTHERS:
        # Provably somebody else's creation, proven in the branch that made it,
        # and not in this person's list. Whatever is disputed about it — when it
        # was made, or where it went — is disputed about that person's booking.
        # Holding an unrelated recipient's empty history open on it confuses
        # "the capture has a gap" with "this recipient may have booked".
        return False
    if _dispute_predates(dispute, anchor=anchor, now=now):
        # A booking proven to have existed when the mailing went out cannot be
        # one made since, whoever made it and with no creation date guessed.
        return False
    if numeric_id is not None:
        # Only a CREATION delivery can answer the question this rule asks. Who
        # holds a booking now is a different fact: an update or a cancellation
        # naming somebody else is an event in the life of a booking this person
        # may well have made and handed on, and reading it as "not theirs" shut
        # the question that was open. So a dispute with no creation observed at
        # all leaves it open, whatever its later deliveries named.
        #
        # Where creations were observed, their number is proven in the branches
        # their own card covers and answers nothing anywhere else: a creation
        # from another configured branch may be about a customer record of
        # theirs this deployment cannot see, so matching digits are not the
        # question there — the absence of an answer is.
        branches = {int(value) for value in numeric_branches}
        if not dispute.creation_pairs or _unowned_creation_branches(dispute):
            # And an unanswered claim from a branch of its own is the same
            # position seen from the other side: one of the claims about who
            # made this booking is absent, so comparing their number against
            # the claims that ARE present answers nothing.
            return True
        return any(
            int(customer_id) == int(numeric_id) if int(company_id) in branches else True
            for company_id, customer_id in dispute.creation_pairs
        )
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
    previous_capture: CaptureScan | None = None,
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

    ``previous_capture`` is the send executor's snapshot taken before its live
    read. Changed bookings outside that read's list must be proven unrelated;
    otherwise the live negative proof is stale. Preview leaves this unset.
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

    # No lower bound: the question is when a booking the live read can see was
    # MADE, which may be long before the mailing. Bounded newest-first by the
    # report's own row cap, which reports truncation.
    scan = await scan_capture(session, anchors=list(anchors), now=now, registry=locations, lookback=None)
    if scan.truncated or (
        previous_capture is not None
        and (
            previous_capture.truncated
            or previous_capture.delivery_fingerprints is None
            or scan.delivery_fingerprints is None
            or previous_capture.unattributed_fingerprints is None
            or scan.unattributed_fingerprints is None
            or previous_capture.delivery_customers is None
            or scan.delivery_customers is None
            or previous_capture.lifecycle_deliveries is None
            or scan.lifecycle_deliveries is None
        )
    ):
        # A partial pass is not an answer about anybody, so nothing below has
        # anything left to decide — and with a capture large enough to
        # truncate, the per-pair owner lookup was also the query that failed
        # first, turning this planned incomplete result into "the database is
        # unavailable" on the operator's screen. The answer is the incomplete
        # one this phase has always promised for a truncated scan.
        return CapturedCreations(
            complete=False,
            scanned_events=int(scan.scanned_events),
            truncated=True,
            registry_ready=bool(locations.ready),
            disputed=len(scan.disputed),
            by_slot={int(item.slot): SlotCreations(False, unproven_reason) for item in items},
        )

    # Scoped to the configured branches: a card of a branch nobody configured
    # is another workspace's row or an Altegio-era one, and it may neither
    # supply this recipient's numeric bridge nor contradict it. The §PR-17
    # report keeps its own workspace-wide scope, which this does not touch.
    identities = await resolve_identities(session, items=items, registry=locations)
    grouped = scan.by_customer()
    by_uuid = scan.by_booking_uuid()
    previous_bookings = previous_capture.by_booking_uuid() if previous_capture is not None else {}
    previous_deliveries = previous_capture.delivery_fingerprints or {} if previous_capture is not None else {}
    current_deliveries = scan.delivery_fingerprints or {}
    changed_ids = (
        {
            booking_uuid
            for booking_uuid in previous_deliveries.keys() | current_deliveries.keys()
            if previous_deliveries.get(booking_uuid) != current_deliveries.get(booking_uuid)
        }
        if previous_capture is not None
        else set()
    )
    changed_bookings = (
        [
            booking
            for booking in scan.bookings
            if booking.booking_uuid in changed_ids or previous_bookings.get(booking.booking_uuid) != booking
        ]
        if previous_capture is not None
        else []
    )
    # Creation attribution cannot settle who a new lifecycle delivery names.
    # Check the delta even for disputed creations, and do not reapply old
    # holders/ownerless facts the complete live read already covered.
    changed_customers: set[tuple[int, int | None] | None] = set()
    removed_delivery = False
    for booking_uuid in changed_ids:
        before = previous_deliveries.get(booking_uuid, frozenset())
        after = current_deliveries.get(booking_uuid, frozenset())
        removed_delivery = removed_delivery or bool(before - after)
        customers = (scan.delivery_customers or {}).get(booking_uuid, {})
        changed_customers.update(customers.get(fingerprint) for fingerprint in after - before)
    # Identical payloads can be separate lifecycle occurrences (for example,
    # returning to a previous holder). Compare every visible receipt ID, so a
    # lower-ID transaction committed after the live read is also detected.
    # Keep these UUIDs apart from changed_bookings: a new receipt names its
    # current holder and must not reapply the original creator's ownership.
    lifecycle_changed_ids: set[uuid_module.UUID] = set()
    if previous_capture is not None:
        previous_lifecycle = previous_capture.lifecycle_deliveries or {}
        current_lifecycle = scan.lifecycle_deliveries or {}
        removed_delivery = removed_delivery or bool(previous_lifecycle.keys() - current_lifecycle.keys())
        for event_id in current_lifecycle.keys() - previous_lifecycle.keys():
            delivery = current_lifecycle[event_id]
            if delivery is None:
                changed_customers.add(None)
            else:
                booking_uuid, company_id, customer_id = delivery
                lifecycle_changed_ids.add(booking_uuid)
                changed_customers.add((company_id, customer_id))
    # Bookings that may have left somebody's list. Two shapes, and the second
    # one used to be invisible: a delivery naming a DIFFERENT customer record,
    # and a delivery from another branch naming no customer at all. The second
    # proves nothing about who holds the booking now, which is exactly why it
    # cannot be read as proof that nobody else does.
    live_strays = [booking for booking in scan.bookings if booking.customer_reassigned or booking.custody_unproven]

    # Which slots have a numeric bridge on their own card, decided first and
    # without touching the database, so the reverse lookup below is asked about
    # exactly the creations that need it.
    bridges: dict[int, int | None] = {}
    # And WHERE each bridged number was proven. A numeric customer id is unique
    # per branch, so the comparison "this creation named their number" is only
    # available in the branches their own card covers.
    bridge_branches: dict[int, frozenset[int]] = {}
    refused: dict[int, str] = {}
    for item in items:
        slot = int(item.slot)
        identity = identities.get(slot)
        if identity is not None and identity.state == IDENTITY_PROVEN and identity.customer_id is not None:
            bridges[slot] = int(identity.customer_id)
            bridge_branches[slot] = frozenset(int(value) for value in identity.numeric_company_ids)
            continue
        # Two numeric ids claiming one person, or a frozen card that now names a
        # different uuid, is an identity problem and not a missing bridge. A card
        # that simply carries no numeric id is neither.
        if identity is None or identity.state != IDENTITY_NUMERIC_MISSING:
            refused[slot] = conflict_reason or unproven_reason
            continue
        bridges[slot] = None

    # Every (branch, numeric id) a creation this pass has to judge named, asked
    # for in one bounded query. Collected for EVERY slot and not only the
    # bridge-less ones: a recipient whose own number is proven in one branch
    # still needs the reverse link to read a creation delivered by another, and
    # the pair is what makes that reading about a person rather than a number.
    needed_owners: set[tuple[int, int]] = set()
    judged = [item for item in items if int(item.slot) not in refused]
    for item in judged:
        for booking_uuid in slot_bookings.get(int(item.slot), ()) if slot_bookings is not None else ():
            booking = by_uuid.get(booking_uuid)
            if booking is not None:
                needed_owners.add((int(booking.company_id), int(booking.customer_id)))
    if judged:
        for pair in changed_customers:
            if pair is not None and pair[1] is not None:
                needed_owners.add((pair[0], pair[1]))
        for booking in changed_bookings:
            needed_owners.add((int(booking.company_id), int(booking.customer_id)))
        # And the bookings OUTSIDE anybody's list that a reassignment touched.
        # They are the reason a slot whose own list cannot show them is refused
        # at all, so they are precisely where a proven creator is worth asking
        # about: a booking somebody else provably made and then handed on is
        # not a booking that could have left THIS person's list unseen.
        for booking in live_strays:
            needed_owners.add((int(booking.company_id), int(booking.customer_id)))
        # And the disputes, for the same reason in the other direction: a
        # booking whose creations agree on the creator and disagree on the date
        # is that creator's problem, so knowing whose it is keeps the gap away
        # from everybody else. The pairs the deliveries really named, never a
        # product of the branches and the numbers — that product would ask
        # about customer records nothing observed, and a card answering one of
        # them would speak about a booking it has nothing to do with.
        for dispute in scan.disputed:
            needed_owners.update(
                (int(company_id), int(customer_id)) for company_id, customer_id in dispute.creation_pairs
            )
    owners = await _creation_owner_uuids(session, wanted=needed_owners, registry=locations)

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
        numeric_branches = bridge_branches.get(slot, frozenset())
        bridge = "frozen_card_numeric_id" if numeric_id is not None else None
        anchor = item.send_attempted_at

        created: dict[uuid_module.UUID, datetime] = {}
        reassigned: set[uuid_module.UUID] = set()
        # Bookings whose own captured facts disagree with each other about when
        # they appeared. Kept out of the creations and reported, because the
        # half this phase would otherwise act on is the one that excludes.
        contradicted: set[uuid_module.UUID] = set()
        # A changed booking in the cached live list invalidates its live state
        # even when the creation/owner summary stayed the same (rescheduling).
        live_proof_moved = bool(live & (changed_ids | lifecycle_changed_ids)) or (
            previous_capture is not None
            and previous_capture.unattributed_fingerprints != scan.unattributed_fingerprints
        )
        if removed_delivery or any(
            pair is None
            or pair[1] is None
            or _owner_verdict(
                company_id=pair[0],
                customer_id=pair[1],
                numeric_id=numeric_id,
                numeric_branches=numeric_branches,
                slot_uuid=item.easyweek_customer_uuid,
                owners=owners,
            )
            != ATTRIBUTION_ANOTHERS
            for pair in changed_customers
        ):
            live_proof_moved = True
        represented = set(by_uuid) | {dispute.booking_uuid for dispute in scan.disputed}
        if changed_ids - represented:
            # An ownerless/removed candidate cannot be proven unrelated to the
            # live proof. Do not turn missing evidence into a permission.
            live_proof_moved = True

        def _credit(booking: ObservedBooking) -> None:
            if _chronology_contradicted(booking, now=now):
                contradicted.add(booking.booking_uuid)
                return
            created[booking.booking_uuid] = booking.booking_created_at

        if numeric_id is not None:
            for booking in grouped.get(numeric_id, []):
                if int(booking.company_id) not in numeric_branches:
                    # The same digits in a branch their card does not cover are
                    # a different customer record. A booking of that person's
                    # is not this recipient's engagement, and the live read
                    # below is what speaks for anything really in their list.
                    continue
                if booking.customer_reassigned:
                    reassigned.add(booking.booking_uuid)
                # Grouped by the customer its own CREATION named, so a booking
                # that has since moved away is still this person's creation.
                _credit(booking)
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
                    numeric_branches=numeric_branches,
                    slot_uuid=item.easyweek_customer_uuid,
                    owners=owners,
                )
                == ATTRIBUTION_THEIRS
            ):
                _credit(booking)

        # A UUID-only card may have no numeric bridge. Its live list cannot
        # establish ownership of a booking captured after that read started.
        # Resolve changed facts explicitly instead of treating the old list as
        # a current proof that none of them belong to this recipient.
        for booking in changed_bookings:
            if booking.booking_uuid in live:
                continue
            owner = _owner_verdict(
                company_id=int(booking.company_id),
                customer_id=int(booking.customer_id),
                numeric_id=numeric_id,
                numeric_branches=numeric_branches,
                slot_uuid=item.easyweek_customer_uuid,
                owners=owners,
            )
            if owner == ATTRIBUTION_THEIRS:
                _credit(booking)
            if owner != ATTRIBUTION_ANOTHERS:
                live_proof_moved = True

        # Bookings of theirs the capture cannot date and had provably already
        # heard of when the attempt was made. No creation date is taken from
        # the sighting; all it answers is that nothing NEW is hiding here.
        existed_before = frozenset(
            dispute.booking_uuid
            for dispute in scan.disputed
            if dispute.booking_uuid in live
            and _dispute_predates(dispute, anchor=anchor, now=now)
            and _dispute_owner(
                dispute,
                numeric_id=numeric_id,
                numeric_branches=numeric_branches,
                slot_uuid=item.easyweek_customer_uuid,
                owners=owners,
            )
            == ATTRIBUTION_THEIRS
        )

        proven = True
        reason: str | None = None
        # A booking the capture could not attribute at all, which could be
        # theirs. Reported rather than dropped, because an empty live history
        # plus a forgotten dispute reads as "nothing happened since the
        # mailing" — and that is exactly what it is not.
        if contradicted or live_proof_moved:
            # Two captured facts about one booking that cannot both hold. The
            # answer is not the harsher of them.
            proven = False
            reason = unproven_reason
        elif any(
            _dispute_touches(
                dispute,
                numeric_id=numeric_id,
                numeric_branches=numeric_branches,
                slot_uuid=item.easyweek_customer_uuid,
                owners=owners,
                live=live,
                anchor=anchor,
                now=now,
            )
            for dispute in scan.disputed
        ):
            proven = False
            reason = unproven_reason
        elif any(
            # A booking that LEFT somebody's list is the one thing their list
            # cannot show, and a reassignment after this slot's own anchor, of
            # a booking not in their list, is exactly that case.
            #
            # Asked of every slot and not only of the bridge-less ones. A
            # numeric bridge is proven in the branches that person's own card
            # covers, so it reads their bookings in those branches and nothing
            # at all in the others; treating a bridge as a complete view used
            # to skip this question entirely, and a booking created in another
            # configured branch and handed on then passed unnoticed — with the
            # same digits, which are a different customer record there, and
            # with different ones, which say nothing about who they are there.
            booking.booking_uuid not in live
            and anchor is not None
            and booking.booking_created_at > anchor
            # Unless its creator is PROVEN to be somebody else, in the branch
            # that delivered the creation. The whole worry is a booking of THIS
            # person's that left their list unseen, and a creation resolved to
            # another proven uuid — or to a number their own card proves in
            # that branch and does not match — is demonstrably not one. An
            # absent or conflicting link proves nothing, and those stay
            # unknown, as they did before any card existed.
            and _attribution(
                booking,
                numeric_id=numeric_id,
                numeric_branches=numeric_branches,
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
