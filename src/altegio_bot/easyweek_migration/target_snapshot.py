"""Reading a live EasyWeek booking back, and proving it is still ours (PR-11.1, rev 16).

One projection, used by three callers that must agree:

* the **canary** — after its single POST, it re-reads the booking and proves
  every write-critical field came back as sent;
* **reconciliation** — an operator supplying a UUID for an uncertain row must be
  proven right, not believed;
* **rollback** — before cancelling anything, the live booking must still be the
  booking this run created.

The first version of rollback asked two questions: is the marker still in the
comment, and is the booking neither cancelled nor completed. Both can be true
while the appointment has been moved to a different day, given to a different
master, or reassigned to a different customer — and cancelling *that* destroys
work somebody did deliberately. So the projection covers every field the
migration itself wrote.

**Absence is a mismatch.** If EasyWeek does not return a field we need, the
answer is "we cannot prove this is unchanged", which is treated exactly like
"this changed". The alternative — silently skipping fields the response happens
to omit — turns a thin response into a green light.

The stored form is a digest, so nothing derived from a customer is kept: the
customer appears only as their EasyWeek UUID inside the hash input, and the hash
is what lands in the ledger and the reports.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final

from altegio_bot.easyweek_migration.manifest import canonical_uuid

# The version of the projection itself. Bumping it invalidates every stored
# snapshot and every canary proof — which is the point: if the set of fields we
# compare changes, an old proof no longer means what it used to.
TARGET_SNAPSHOT_VERSION: Final = "v1"

# Bumped whenever `build_booking_request` changes shape. A canary proves one
# request schema; a changed schema is unproven again.
REQUEST_SCHEMA_VERSION: Final = "v1"

SNAPSHOT_FIELD_MISSING: Final = "target_field_missing"
SNAPSHOT_FIELD_MISMATCH: Final = "target_field_mismatch"
SNAPSHOT_NOT_ACTIVE: Final = "target_not_active"
SNAPSHOT_MARKER_MISSING: Final = "target_marker_missing"


class TargetSnapshotError(ValueError):
    """A live booking could not be projected into a comparable snapshot."""

    def __init__(self, reason: str, field_name: str | None = None) -> None:
        self.reason = reason
        self.field_name = field_name
        super().__init__(reason if field_name is None else f"{reason}:{field_name}")


@dataclass(frozen=True)
class TargetSnapshot:
    """The write-critical projection of one EasyWeek booking."""

    booking_uuid: str
    location_uuid: str
    staff_uuid: str
    service_uuid: str
    customer_uuid: str
    start_time_utc: str
    duration_minutes: int
    marker: str
    active: bool

    @property
    def fingerprint(self) -> str:
        """Stable digest of every field above, plus the projection version."""
        blob = "|".join(
            [
                TARGET_SNAPSHOT_VERSION,
                self.booking_uuid,
                self.location_uuid,
                self.staff_uuid,
                self.service_uuid,
                self.customer_uuid,
                self.start_time_utc,
                str(self.duration_minutes),
                self.marker,
                "active" if self.active else "inactive",
            ]
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def as_safe_dict(self) -> dict[str, Any]:
        """Technical identifiers and an instant. No name, phone or free text."""
        return {
            "booking_uuid": self.booking_uuid,
            "location_uuid": self.location_uuid,
            "staff_uuid": self.staff_uuid,
            "service_uuid": self.service_uuid,
            "customer_uuid": self.customer_uuid,
            "start_time_utc": self.start_time_utc,
            "duration_minutes": self.duration_minutes,
            "marker": self.marker,
            "active": self.active,
            "snapshot_version": TARGET_SNAPSHOT_VERSION,
        }


@dataclass
class SnapshotMismatch:
    """Which fields did not match, as stable codes. Never the values themselves."""

    reasons: list[str] = field(default_factory=list)

    @property
    def matched(self) -> bool:
        return not self.reasons


def _require_uuid(payload: dict[str, Any], *keys: str, field_name: str) -> str:
    """First present key that parses as a canonical UUID, or refuse.

    Several keys are accepted because EasyWeek's read shape for a nested entity
    is not pinned by the plan (it documents the endpoint, not the schema). A
    *missing* value is never tolerated, though — only an alternative spelling of
    a present one.
    """
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("uuid") or value.get("uid")
        parsed = canonical_uuid(value)
        if parsed is not None:
            return parsed
    raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, field_name)


def _require_service_uuid(payload: dict[str, Any]) -> str:
    """The single service's UUID, from either a flat key or the services list.

    A booking that comes back with more than one service is not the booking we
    created — the migration only ever writes single-service bookings — so that
    is a mismatch, not something to pick the first element out of.
    """
    for key in ("service_uuid", "service"):
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("uuid") or value.get("uid")
        parsed = canonical_uuid(value)
        if parsed is not None:
            return parsed

    for key in ("services", "ordered_services"):
        items = payload.get(key)
        if isinstance(items, list) and len(items) == 1 and isinstance(items[0], dict):
            item = items[0]
            for inner in ("service_uuid", "uuid", "uid"):
                candidate = item.get(inner)
                if isinstance(candidate, dict):
                    candidate = candidate.get("uuid") or candidate.get("uid")
                parsed = canonical_uuid(candidate)
                if parsed is not None:
                    return parsed
    raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "service_uuid")


def _require_start_time(payload: dict[str, Any]) -> str:
    """The booking's start instant, normalised to UTC ISO-8601 with ``Z``."""
    for key in ("start_time", "start_at", "starts_at"):
        raw = payload.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        text = raw.strip()
        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            # A naive start time cannot be compared to the aware instant we sent.
            continue
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "start_time")


def _require_duration_minutes(payload: dict[str, Any]) -> int:
    """Duration in whole minutes, from an explicit field or start/end."""
    raw = payload.get("duration")
    if type(raw) is int and raw > 0:
        return raw

    start = payload.get("start_time")
    end = payload.get("end_time")
    if isinstance(start, str) and isinstance(end, str):
        try:
            begin = datetime.fromisoformat(start.replace("Z", "+00:00"))
            finish = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except ValueError:
            raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "duration") from None
        if begin.tzinfo and finish.tzinfo:
            seconds = (finish - begin).total_seconds()
            if seconds > 0 and seconds % 60 == 0:
                return int(seconds // 60)
    raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "duration")


def _require_marker(payload: dict[str, Any], *, expected_marker: str) -> str:
    comment = payload.get("comment")
    if not isinstance(comment, str) or expected_marker not in comment:
        raise TargetSnapshotError(SNAPSHOT_MARKER_MISSING, "comment")
    return expected_marker


def _require_active(payload: dict[str, Any]) -> bool:
    """Active means neither cancelled nor completed, both explicitly stated.

    A response that says nothing about either is not proof of an active booking.
    """
    for key in ("is_canceled", "is_completed"):
        value = payload.get(key)
        if type(value) is not bool:
            raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, key)
        if value:
            raise TargetSnapshotError(SNAPSHOT_NOT_ACTIVE, key)
    return True


def project_target(payload: dict[str, Any], *, expected_marker: str) -> TargetSnapshot:
    """Project one live EasyWeek booking into its comparable snapshot.

    Raises :class:`TargetSnapshotError` on the first field that cannot be read.
    The caller turns that into a blocked or unproven outcome — never into a
    partial match.
    """
    if not isinstance(payload, dict):
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "booking")

    return TargetSnapshot(
        booking_uuid=_require_uuid(payload, "uuid", "uid", field_name="booking_uuid"),
        location_uuid=_require_uuid(payload, "location_uuid", "location", field_name="location_uuid"),
        staff_uuid=_require_uuid(payload, "staff_uuid", "staff", "user", field_name="staff_uuid"),
        service_uuid=_require_service_uuid(payload),
        customer_uuid=_require_uuid(payload, "customer_uuid", "customer", field_name="customer_uuid"),
        start_time_utc=_require_start_time(payload),
        duration_minutes=_require_duration_minutes(payload),
        marker=_require_marker(payload, expected_marker=expected_marker),
        active=_require_active(payload),
    )


def expected_snapshot(
    *,
    booking_uuid: str,
    location_uuid: str,
    staff_uuid: str,
    service_uuid: str,
    customer_uuid: str,
    start_time_utc: str,
    duration_minutes: int,
    marker: str,
) -> TargetSnapshot:
    """The snapshot a successful write SHOULD produce, built from what we sent."""
    return TargetSnapshot(
        booking_uuid=booking_uuid,
        location_uuid=location_uuid,
        staff_uuid=staff_uuid,
        service_uuid=service_uuid,
        customer_uuid=customer_uuid,
        start_time_utc=start_time_utc,
        duration_minutes=duration_minutes,
        marker=marker,
        active=True,
    )


def compare(live: TargetSnapshot, expected: TargetSnapshot) -> SnapshotMismatch:
    """Field-by-field comparison, reporting every difference as a stable code."""
    mismatch = SnapshotMismatch()
    for field_name in (
        "booking_uuid",
        "location_uuid",
        "staff_uuid",
        "service_uuid",
        "customer_uuid",
        "start_time_utc",
        "duration_minutes",
        "marker",
        "active",
    ):
        if getattr(live, field_name) != getattr(expected, field_name):
            mismatch.reasons.append(f"{SNAPSHOT_FIELD_MISMATCH}:{field_name}")
    return mismatch
