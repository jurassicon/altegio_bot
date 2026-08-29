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
from altegio_bot.easyweek_migration.target_snapshot import (
    TargetSnapshot,
    TargetSnapshotError,
    compare,
    expected_snapshot,
    project_target,
)
from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient

logger = logging.getLogger("easyweek_migration.proof")

# Stable, PII-free reasons a live target could not be proven.
TARGET_UUID_MISSING: Final = "target_booking_uuid_missing"
TARGET_SNAPSHOT_MISSING: Final = "target_snapshot_fingerprint_missing"
TARGET_ABSENT: Final = "target_not_found_in_easyweek"
TARGET_UNREADABLE: Final = "target_unreadable"
TARGET_MALFORMED: Final = "target_malformed"
TARGET_FINGERPRINT_MISMATCH: Final = "target_fingerprint_mismatch"
TARGET_PROVEN: Final = "target_proven"


@dataclass(frozen=True)
class TargetProof:
    """Whether one live booking matched, and — when it did not — why."""

    proven: bool
    reason: str
    live: TargetSnapshot | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {"target_proof": self.reason, "target_proven": self.proven}


def expected_target_for(decision: Decision, *, booking_uuid: str, marker: str) -> TargetSnapshot:
    """The snapshot a correctly-migrated booking of *decision* would have.

    Built from the decision the classifier just produced, so it always describes
    the booking as the migration would create it **today** — which is what makes
    it a real comparison rather than a restatement of whatever EasyWeek returned.
    """
    assert decision.starts_at_utc is not None
    assert decision.duration_minutes is not None
    assert decision.easyweek_location_uuid is not None
    assert decision.easyweek_staff_uuid is not None
    assert decision.easyweek_service_uuid is not None
    assert decision.easyweek_customer_uuid is not None
    return expected_snapshot(
        booking_uuid=booking_uuid,
        location_uuid=decision.easyweek_location_uuid,
        staff_uuid=decision.easyweek_staff_uuid,
        service_uuid=decision.easyweek_service_uuid,
        customer_uuid=decision.easyweek_customer_uuid,
        start_time_utc=decision.starts_at_utc.isoformat().replace("+00:00", "Z"),
        duration_minutes=decision.duration_minutes,
        marker=marker,
    )


async def prove_live_target(
    write_client: EasyWeekMigrationWriteClient,
    *,
    target_booking_uuid: str | None,
    marker: str,
    expected: TargetSnapshot | None = None,
    expected_fingerprint: str | None = None,
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

    if expected is not None:
        mismatch = compare(live, expected)
        if not mismatch.matched:
            return TargetProof(proven=False, reason=mismatch.reasons[0], live=live)

    if expected_fingerprint is not None and live.fingerprint != expected_fingerprint:
        return TargetProof(proven=False, reason=TARGET_FINGERPRINT_MISMATCH, live=live)

    return TargetProof(proven=True, reason=TARGET_PROVEN, live=live)
