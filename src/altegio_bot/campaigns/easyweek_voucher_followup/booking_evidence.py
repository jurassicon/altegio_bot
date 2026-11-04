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
(:func:`...booking_attribution.scan_capture`) rather than writing a second one,
and it reuses that report's identity bridge
(:func:`...booking_attribution.resolve_identities`) to get from a slot's proven
customer uuid to the numeric id the webhooks name.

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
* It invents no semantics for undocumented fields, and asks the provider for
  nothing: the whole module is a local read.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_production.booking_attribution import (
    BOOKING_CANCELLED,
    IDENTITY_PROVEN,
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
    ``False`` this person's creations are simply not established — their numeric
    id could not be bridged, or the scan was truncated — and every booking of
    theirs whose creation matters becomes ``unknown``.
    """

    proven: bool
    reason: str | None = None
    # booking uuid → the provider's own creation instant. Only bookings whose
    # creation delivery was captured and validated appear here.
    created_at: dict[uuid_module.UUID, datetime] = field(default_factory=dict)
    # Bookings whose deliveries disagree about which customer they belong to.
    # Their creation is not attributable, so it is reported rather than used.
    reassigned: frozenset[uuid_module.UUID] = frozenset()

    def creation_of(self, booking_uuid: uuid_module.UUID) -> datetime | None:
        if booking_uuid in self.reassigned:
            return None
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
        }


async def load_creation_evidence(
    session: AsyncSession,
    *,
    items: list[EasyWeekVoucherProductionBatchItem],
    now: datetime,
    unproven_reason: str,
    registry: EasyWeekLocationRegistry | None = None,
) -> CapturedCreations:
    """One bounded local pass: which bookings each slot's person created, when.

    Reads nothing external and writes nothing. The scan is bounded below by the
    earliest attempt among *items* — creations before that cannot matter to a
    "since the attempt" question — and above by the report's own row cap, which
    reports truncation rather than silently returning a short answer.

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
    scan = await scan_capture(session, anchors=list(anchors), now=now, registry=locations)
    grouped = scan.by_customer()

    by_slot: dict[int, SlotCreations] = {}
    for item in items:
        slot = int(item.slot)
        identity = identities.get(slot)
        if identity is None or identity.state != IDENTITY_PROVEN or identity.customer_id is None:
            # The webhooks name a numeric id and this person's could not be
            # bridged to their proven uuid, or two ids claim them. Either way
            # their creations are not established.
            by_slot[slot] = SlotCreations(False, unproven_reason)
            continue
        if scan.truncated:
            by_slot[slot] = SlotCreations(False, unproven_reason)
            continue
        observed = grouped.get(int(identity.customer_id), [])
        created: dict[uuid_module.UUID, datetime] = {}
        reassigned: set[uuid_module.UUID] = set()
        for booking in observed:
            if booking.customer_reassigned:
                reassigned.add(booking.booking_uuid)
                continue
            created[booking.booking_uuid] = booking.booking_created_at
        by_slot[slot] = SlotCreations(True, None, created_at=created, reassigned=frozenset(reassigned))

    return CapturedCreations(
        complete=not scan.truncated,
        scanned_events=int(scan.scanned_events),
        truncated=bool(scan.truncated),
        registry_ready=bool(locations.ready),
        by_slot=by_slot,
    )


# Re-exported so the eligibility rules have one import for the cancelled label
# and cannot drift from the report's own vocabulary.
CAPTURED_CANCELLED = BOOKING_CANCELLED
