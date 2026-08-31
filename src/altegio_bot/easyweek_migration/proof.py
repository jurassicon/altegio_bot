"""One way to prove a live EasyWeek booking is the one we meant (PR-11.1, rev 17).

Four places need the same answer — *is the booking sitting in EasyWeek right now
the booking this migration created, unchanged?* — and before this module they
answered it four different ways, three of them too weakly:

* the **canary** did it properly: read back, compare every write-critical field;
* **reconcile** with a known UUID accepted a 2xx as proof, checking no fields at
  all and storing no fingerprint;
* **resolve-created** proved the marker and the branch, but not the staff,
  service, customer, start time or duration — so an operator could point it at a
  booking for the right customer at the wrong time and have it accepted;
* **final reconciliation** never looked at EasyWeek at all: a ledger row saying
  ``created`` was treated as proof, which stays true after somebody deletes,
  cancels, moves or reassigns the booking.

They now share :func:`prove_live_target`. Reuse here is not tidiness — it is the
only way the four answers cannot drift apart again, and the weakest of them was
the one an operator leaned on hardest.

Nothing in this module weakens the canary or the rollback. Rollback keeps its own
contract (compare against the fingerprint stored when the booking was written)
because it is the one caller that must refuse on *any* difference, including
differences from a source that has legitimately moved on since.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Final

from altegio_bot.easyweek_client import EasyWeekError, EasyWeekNotFoundError
from altegio_bot.easyweek_migration.classify import Decision
from altegio_bot.easyweek_migration.service_catalog import (
    ServiceEvidenceError,
    ServiceExpectation,
    prove_ordered_service,
    read_ordered_service,
    read_pagination,
)
from altegio_bot.easyweek_migration.target_snapshot import (
    SNAPSHOT_NOT_ACTIVE,
    TargetSnapshot,
    TargetSnapshotError,
    compare,
    expected_snapshot,
    project_target,
)
from altegio_bot.easyweek_migration.write_client import (
    EASYWEEK_BOOKING_TIMEZONE,
    EasyWeekMigrationWriteClient,
)

logger = logging.getLogger("easyweek_migration.proof")

# Stable, PII-free reasons a live target could not be proven.
TARGET_UUID_MISSING: Final = "target_booking_uuid_missing"
TARGET_SNAPSHOT_MISSING: Final = "target_snapshot_fingerprint_missing"
TARGET_ABSENT: Final = "target_not_found_in_easyweek"
TARGET_UNREADABLE: Final = "target_unreadable"
TARGET_MALFORMED: Final = "target_malformed"
TARGET_FINGERPRINT_MISMATCH: Final = "target_fingerprint_mismatch"
TARGET_PROVEN: Final = "target_proven"
# The master. The booking response names no staffer at all, so assignment is
# proven by the documented filtered list and by nothing else.
STAFF_LIST_UNREADABLE: Final = "staff_assignment_list_unreadable"
STAFF_LIST_INCOMPLETE: Final = "staff_assignment_list_incomplete"
STAFF_NOT_ASSIGNED: Final = "staff_assignment_absent"
STAFF_ASSIGNMENT_PROVEN: Final = "staff_assignment_proven"
# The service, under the limited attribute method of plan §28.
SERVICE_EVIDENCE_MISSING: Final = "service_evidence_missing"

# `GET /bookings` caps a page at 100 (documented). Asking for the maximum keeps
# the number of round trips down without relying on a default we did not set.
_BOOKINGS_PER_PAGE: Final = 100
# A filtered list bounded to one instant at one location for one master should
# be a handful of rows. Anything past this is a filter that is not filtering,
# and reading on would be pretending we understand the response.
_MAX_BOOKING_PAGES: Final = 20


@dataclass(frozen=True)
class TargetProof:
    """Whether one live booking matched, and — when it did not — why."""

    proven: bool
    reason: str
    live: TargetSnapshot | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {"target_proof": self.reason, "target_proven": self.proven}


@dataclass(frozen=True)
class StaffAssignmentProof:
    """Whether a named master really holds this booking."""

    proven: bool
    reason: str
    pages_read: int = 0

    def as_safe_dict(self) -> dict[str, Any]:
        return {"staff_assignment": self.reason, "staff_assignment_proven": self.proven}


async def prove_staff_assignment(
    write_client: EasyWeekMigrationWriteClient,
    *,
    target_booking_uuid: str,
    location_uuid: str,
    staff_uuid: str,
    start_time_utc: str,
) -> StaffAssignmentProof:
    """Prove one booking belongs to one master, by documented list membership.

    ``GET /bookings/{uuid}`` returns no staffer field of any kind, so there is
    nothing on the booking to compare. What there is, is a documented filter:
    ``GET /bookings?staffer_uuid=...``. An operator probe confirmed it
    discriminates on live data — a known test booking appeared under its own
    master and was absent from an otherwise identical query naming a different
    one.

    So the question asked here is membership: with the list bounded to this
    location, this master and this exact instant, does our booking's UUID appear?

    Everything that is not a complete, readable list containing the target is a
    refusal, and each has its own code:

    * the request failed, or a page could not be read → unreadable;
    * the pagination metadata is missing, inconsistent or unbounded → incomplete;
    * the list read cleanly to its last page and our booking is not in it → not
      assigned to this master.

    The last one is the finding that matters: it is what "EasyWeek gave the
    appointment to somebody else" looks like. Reading only the first page would
    make it indistinguishable from "it is on page two", which is why an
    incomplete list is never allowed to answer.

    Read-only. Never logs a row: this list carries customer names and phones.
    """
    params = {
        "location_uuid": location_uuid,
        "staffer_uuid": staff_uuid,
        # Inclusive bounds, both the booking's own instant: the tightest window
        # the documented filter allows.
        "reserved_on_from": start_time_utc,
        "reserved_on_to": start_time_utc,
        "per_page": _BOOKINGS_PER_PAGE,
    }

    page = 1
    while page <= _MAX_BOOKING_PAGES:
        try:
            payload = await write_client.list_bookings(params={**params, "page": page})
        except EasyWeekError:
            return StaffAssignmentProof(proven=False, reason=STAFF_LIST_UNREADABLE, pages_read=page - 1)

        rows = payload.get("data")
        if not isinstance(rows, list):
            return StaffAssignmentProof(proven=False, reason=STAFF_LIST_INCOMPLETE, pages_read=page - 1)
        try:
            last_page, _total = read_pagination(payload.get("meta"), page=page)
        except ServiceEvidenceError:
            return StaffAssignmentProof(proven=False, reason=STAFF_LIST_INCOMPLETE, pages_read=page - 1)

        for row in rows:
            if isinstance(row, dict) and row.get("uuid") == target_booking_uuid:
                return StaffAssignmentProof(proven=True, reason=STAFF_ASSIGNMENT_PROVEN, pages_read=page)

        if page >= last_page:
            # The list is complete and our booking is not in it.
            return StaffAssignmentProof(proven=False, reason=STAFF_NOT_ASSIGNED, pages_read=page)
        page += 1

    return StaffAssignmentProof(proven=False, reason=STAFF_LIST_INCOMPLETE, pages_read=_MAX_BOOKING_PAGES)


def expected_target_for(
    decision: Decision,
    *,
    booking_uuid: str,
    marker: str,
    expectation: ServiceExpectation,
    timezone_name: str = EASYWEEK_BOOKING_TIMEZONE,
) -> TargetSnapshot:
    """The snapshot a correctly-migrated booking of *decision* would have.

    Built from the decision the classifier just produced plus the service
    expectation pinned from the live catalogue, so it describes the booking as
    the migration would create it **today** — a real comparison rather than a
    restatement of whatever EasyWeek returned.

    The money and the length come from :class:`ServiceExpectation` rather than
    from the decision, because the API takes neither on the request: EasyWeek
    prices and times the booking from its own catalogue, so the catalogue entry
    is what the result has to be measured against.
    """
    assert decision.starts_at_utc is not None
    assert decision.duration_minutes is not None
    assert decision.easyweek_location_uuid is not None
    assert decision.easyweek_staff_uuid is not None
    assert decision.easyweek_service_uuid is not None
    assert decision.easyweek_customer_uuid is not None
    # The plan and the catalogue must already agree; `pin_service_expectation`
    # refuses otherwise, and this restates it where it would be read.
    assert decision.easyweek_service_uuid == expectation.easyweek_service_uuid
    assert decision.duration_minutes == expectation.duration_minutes
    return expected_snapshot(
        booking_uuid=booking_uuid,
        location_uuid=decision.easyweek_location_uuid,
        staff_uuid=decision.easyweek_staff_uuid,
        customer_uuid=decision.easyweek_customer_uuid,
        start_time_utc=decision.starts_at_utc.isoformat().replace("+00:00", "Z"),
        duration_minutes=expectation.duration_minutes,
        timezone_name=timezone_name,
        currency=expectation.currency,
        price_minor=expectation.price_minor,
        service_name=expectation.normalized_name,
        marker=marker,
    )


async def prove_live_target(
    write_client: EasyWeekMigrationWriteClient,
    *,
    target_booking_uuid: str | None,
    marker: str,
    expected: TargetSnapshot | None = None,
    expected_fingerprint: str | None = None,
    expected_staff_uuid: str | None = None,
    expected_location_uuid: str | None = None,
    service_expectation: ServiceExpectation | None = None,
) -> TargetProof:
    """Fetch one booking and prove it is unchanged. Read-only; never POSTs.

    Two ways to say what "unchanged" means, and a caller must supply at least one:

    ``expected``
        the snapshot a correct booking would have, rebuilt from the current
        source. Used by resolve-created and by the reconcile paths, where the
        question is "does this booking match the appointment it stands for?".
    ``expected_fingerprint``
        the digest stored when the booking was written. Used by rollback and by
        the final reconciliation, where the question is "is this still exactly
        what we created?".

    When both are given both must hold.

    Every refusal is fail-closed and named. A missing UUID, a missing stored
    fingerprint, a 404, an unreadable response, a rewritten marker, a cancelled
    booking, an absent field and a changed field all end here as ``proven=False``
    — because each of them means the same thing: *we cannot show this is right.*
    """
    if expected is None and expected_fingerprint is None:
        raise ValueError("prove_live_target needs an expected snapshot or an expected fingerprint")

    if not target_booking_uuid:
        # Nothing to fetch. A row that claims a created booking without naming it
        # is exactly the state reconciliation exists to surface.
        return TargetProof(proven=False, reason=TARGET_UUID_MISSING)

    try:
        payload = await write_client.get_booking(target_booking_uuid)
    except EasyWeekNotFoundError:
        return TargetProof(proven=False, reason=TARGET_ABSENT)
    except EasyWeekError:
        # Could not read it. "We could not check" is not "it is fine".
        return TargetProof(proven=False, reason=TARGET_UNREADABLE)

    try:
        # `project_target` is the strict one: it proves the marker belongs to this
        # source identity, that the booking is neither cancelled nor completed,
        # and that every field we need is actually present.
        live = project_target(payload, expected_marker=marker)
    except TargetSnapshotError as exc:
        return TargetProof(proven=False, reason=f"{TARGET_MALFORMED}:{exc.reason}")

    # The service, under the limited method plan §28 authorises. Run separately
    # from `compare` because it is the only place a DIRECT catalogue uuid — if
    # EasyWeek ever returns one — can be seen to contradict us. A conflicting
    # direct link must never be rescued by matching attributes.
    if service_expectation is not None:
        try:
            prove_ordered_service(read_ordered_service(payload), service_expectation)
        except ServiceEvidenceError as exc:
            return TargetProof(proven=False, reason=str(exc), live=live)

    # The master, from the documented filtered list. Attempted only when a
    # caller states which master it expects; without one there is nothing to
    # query for, and `compare` reports the snapshot as staff-unproven.
    staff_uuid = expected_staff_uuid or (expected.staff_uuid if expected is not None else None)
    location_uuid = expected_location_uuid or (expected.location_uuid if expected is not None else live.location_uuid)
    if staff_uuid is not None:
        assignment = await prove_staff_assignment(
            write_client,
            target_booking_uuid=live.booking_uuid,
            location_uuid=location_uuid,
            staff_uuid=staff_uuid,
            start_time_utc=live.start_time_utc,
        )
        if not assignment.proven:
            return TargetProof(proven=False, reason=assignment.reason, live=live)
        live = live.with_proven_staff(staff_uuid)

    if expected is not None:
        mismatch = compare(live, expected)
        if not mismatch.matched:
            return TargetProof(proven=False, reason=mismatch.reasons[0], live=live)

    if expected_fingerprint is not None and live.fingerprint != expected_fingerprint:
        return TargetProof(proven=False, reason=TARGET_FINGERPRINT_MISMATCH, live=live)

    return TargetProof(proven=True, reason=TARGET_PROVEN, live=live)


# ---------------------------------------------------------------------------
# The other direction: a target that outlived its source
# ---------------------------------------------------------------------------
TARGET_INACTIVE_OR_ABSENT: Final = "target_inactive_or_absent"
GHOST_TARGET_STILL_ACTIVE: Final = "source_inactive_target_still_active"
GHOST_TARGET_UNREADABLE: Final = "source_inactive_target_unreadable"
GHOST_TARGET_MALFORMED: Final = "source_inactive_target_malformed"
GHOST_TARGET_UUID_MISSING: Final = "source_inactive_target_uuid_missing"


async def prove_target_inactive_or_absent(
    write_client: EasyWeekMigrationWriteClient,
    *,
    target_booking_uuid: str | None,
    marker: str,
) -> TargetProof:
    """Prove a migrated booking is gone or finished — the mirror of the usual check.

    The usual question is "is the target still the booking we created?". This is
    the one that goes the other way: the source booking has since been cancelled,
    deleted or has vanished from Altegio, so the appointment we created in
    EasyWeek should not be standing any more.

    Left unasked, that gap was silent. The completeness check only looked at
    *active* source bookings, so a cancelled source dropped out of the loop and
    took its EasyWeek target with it — an extra appointment a customer never
    made, sitting in the new schedule, while the reconciliation reported success.

    What counts as consistent:

    * **404** — the booking is not there. Proven absent.
    * **cancelled or completed** — its life has ended. Proven inactive.

    What does not:

    * an active booking — that is the ghost;
    * an unreadable or malformed response — "we could not tell" is not "it is
      gone", and this is exactly the case where guessing leaves a real customer
      with an appointment nobody expects.

    Read-only. Reconciliation never cancels anything: it reports the ghost and
    refuses to pass, and a human decides what to do about it.
    """
    if not target_booking_uuid:
        return TargetProof(proven=False, reason=GHOST_TARGET_UUID_MISSING)

    try:
        payload = await write_client.get_booking(target_booking_uuid)
    except EasyWeekNotFoundError:
        # Proven absent: nothing is standing.
        return TargetProof(proven=True, reason=TARGET_INACTIVE_OR_ABSENT)
    except EasyWeekError:
        return TargetProof(proven=False, reason=GHOST_TARGET_UNREADABLE)

    try:
        live = project_target(payload, expected_marker=marker)
    except TargetSnapshotError as exc:
        if exc.reason == SNAPSHOT_NOT_ACTIVE:
            # Cancelled or completed, and the marker still proves it is ours.
            return TargetProof(proven=True, reason=TARGET_INACTIVE_OR_ABSENT)
        # A rewritten marker or a missing field. We cannot say this booking is
        # finished, so we do not say it.
        return TargetProof(proven=False, reason=f"{GHOST_TARGET_MALFORMED}:{exc.reason}")

    # It read cleanly, which means it is active. That is the ghost.
    return TargetProof(proven=False, reason=GHOST_TARGET_STILL_ACTIVE, live=live)
