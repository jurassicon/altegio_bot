"""Reading a live EasyWeek booking back, and proving it is still ours.

One projection, used by four callers that must agree:

* the **canary** — after its single POST, it re-reads the booking and proves
  every write-critical field came back as sent;
* **reconciliation** — an operator supplying a UUID for an uncertain row must be
  proven right, not believed;
* **final reconciliation** — a ledger row saying ``created`` proves nothing about
  a booking somebody has since moved;
* **rollback** — before cancelling anything, the live booking must still be the
  booking this run created.

**Absence is a mismatch.** If EasyWeek does not return a field we need, the
answer is "we cannot prove this is unchanged", which is treated exactly like
"this changed". The alternative — silently skipping fields the response happens
to omit — turns a thin response into a green light.

What the v1 projection got wrong
--------------------------------
It was written against the shape of our own ``POST`` body, because the plan
documented the endpoint and not the schema, and the test transport echoed the
request back. Every one of those guesses was wrong against the live API:

===================  ==================================================
we read              the booking actually carries
===================  ==================================================
``comment``          ``public_notes``
``duration`` as int  ``duration`` as ``{value, label, iso_8601}``
``staff_uuid``       **nothing** — no staffer field of any kind
``service_uuid``     ``ordered_services[]``, whose ``uuid`` is the order
                     line, not the catalogue service
===================  ==================================================

So two of the fields are not in the response at all, and each needed its own
answer rather than a looser read of the booking:

* **the master** is proven by an independent ``GET /bookings`` filtered by
  ``staffer_uuid`` — see :mod:`proof`. :attr:`TargetSnapshot.staff_uuid` is
  ``None`` until that query has actually placed this booking under that master.
  It is never populated from the booking payload, and never from what we
  expected, because a field filled in from the expectation would make the
  comparison compare the expectation with itself;
* **the service** is proven by its exact attributes against the location
  catalogue — see :mod:`service_catalog` and plan §28.

The stored form is a digest, so nothing derived from a customer is kept: the
customer appears only as their EasyWeek UUID inside the hash input, the service
only as a digest of its normalised name, and the hash is what lands in the ledger
and the reports.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from altegio_bot.easyweek_migration.manifest import canonical_uuid
from altegio_bot.easyweek_migration.service_catalog import (
    SERVICE_PROOF_METHOD,
    SERVICE_PROOF_TAG,
    ServiceEvidenceError,
    normalize_service_name,
    read_ordered_service,
)

# The version of the projection itself. Bumping it invalidates every stored
# snapshot and every canary proof — which is the point: if the set of fields we
# compare changes, an old proof no longer means what it used to.
#
# v2: projected from the real GET shape (public_notes, duration object, nested
# customer, ordered_services) instead of from an echo of our own request; staff
# proven separately; service proven by catalogue attributes.
TARGET_SNAPSHOT_VERSION: Final = "v2"

# Bumped whenever `build_booking_request` changes shape. A canary proves one
# request schema; a changed schema is unproven again. The service-proof method is
# folded in deliberately: a proof recorded under a different method must not
# license this one, which is plan §28.2 point 6.
REQUEST_SCHEMA_VERSION: Final = f"v2+{SERVICE_PROOF_TAG}"
# The canary proof stores this in a varchar(16). Widening that column would mean
# a migration for a string, so the version stays short instead — and says so here
# rather than failing on an INSERT during a production canary.
assert len(REQUEST_SCHEMA_VERSION) <= 16

SNAPSHOT_FIELD_MISSING: Final = "target_field_missing"
SNAPSHOT_FIELD_MISMATCH: Final = "target_field_mismatch"
SNAPSHOT_NOT_ACTIVE: Final = "target_not_active"
SNAPSHOT_MARKER_MISSING: Final = "target_marker_missing"
SNAPSHOT_STAFF_UNPROVEN: Final = "target_staff_unproven"

# Every field `compare` looks at. Kept as one list so a field added to the
# snapshot cannot be forgotten by the comparison.
COMPARED_FIELDS: Final[tuple[str, ...]] = (
    "booking_uuid",
    "location_uuid",
    "staff_uuid",
    "customer_uuid",
    "start_time_utc",
    "end_time_utc",
    "duration_minutes",
    "timezone_name",
    "currency",
    "price_minor",
    "service_name_digest",
    "marker",
    "active",
)


class TargetSnapshotError(ValueError):
    """A live booking could not be projected into a comparable snapshot."""

    def __init__(self, reason: str, field_name: str | None = None) -> None:
        self.reason = reason
        self.field_name = field_name
        super().__init__(reason if field_name is None else f"{reason}:{field_name}")


def service_name_digest(normalized_name: str) -> str:
    """Short digest of a normalised service name.

    A service name is not personal data, but it is operator-authored free text
    and this projection is written into reports and the ledger. A digest compares
    exactly and prints nothing.
    """
    return hashlib.sha256(normalized_name.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class TargetSnapshot:
    """The write-critical projection of one EasyWeek booking."""

    booking_uuid: str
    location_uuid: str
    customer_uuid: str
    start_time_utc: str
    end_time_utc: str
    duration_minutes: int
    timezone_name: str
    currency: str
    price_minor: int
    service_name_digest: str
    marker: str
    active: bool
    # ``None`` until an independent `GET /bookings?staffer_uuid=...` has placed
    # this booking under that master. The booking payload never sets it.
    staff_uuid: str | None = None

    def with_proven_staff(self, staff_uuid: str) -> TargetSnapshot:
        """Return a copy carrying a master that was independently proven."""
        return TargetSnapshot(**{**self.__dict__, "staff_uuid": staff_uuid})

    @property
    def fingerprint(self) -> str:
        """Stable digest of every compared field, plus the projection version."""
        blob = "|".join(
            [TARGET_SNAPSHOT_VERSION, *(str(getattr(self, name)) for name in COMPARED_FIELDS)],
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def as_safe_dict(self) -> dict[str, Any]:
        """Technical identifiers, instants and numbers. No name, phone or text."""
        return {
            "booking_uuid": self.booking_uuid,
            "location_uuid": self.location_uuid,
            "staff_uuid": self.staff_uuid,
            "customer_uuid": self.customer_uuid,
            "start_time_utc": self.start_time_utc,
            "end_time_utc": self.end_time_utc,
            "duration_minutes": self.duration_minutes,
            "timezone": self.timezone_name,
            "currency": self.currency,
            "price_minor_units": self.price_minor,
            "service_name_digest": self.service_name_digest,
            "service_proof_method": SERVICE_PROOF_METHOD,
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

    A nested object is unwrapped by its own ``uuid``/``uid`` — which is how the
    live response carries the customer (``customer.uuid``). A *missing* value is
    never tolerated, only an alternative spelling of a present one.
    """
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("uuid") or value.get("uid")
        parsed = canonical_uuid(value)
        if parsed is not None:
            return parsed
    raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, field_name)


def _require_instant(payload: dict[str, Any], key: str) -> str:
    """One offset-bearing timestamp, normalised to UTC ISO-8601 with ``Z``.

    A naive timestamp is refused rather than assumed local: an hour's silent
    error puts a customer in front of a locked door.
    """
    raw = payload.get(key)
    if not isinstance(raw, str) or not raw.strip():
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, key)
    text = raw.strip()
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, key) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, key)
    return to_utc_iso(parsed)


def to_utc_iso(moment: datetime) -> str:
    """One spelling of an instant, used on both sides of every comparison."""
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_duration_minutes(payload: dict[str, Any]) -> int:
    """Whole minutes from the booking's ``duration`` object.

    The unit comes from the payload's own ``label``. v1 read this field as a bare
    integer and fell back to ``end - start`` when it was not one; against the real
    API the field is always an object, so the fallback was doing all the work and
    a wrong duration would only have been caught by accident.
    """
    duration = payload.get("duration")
    if not isinstance(duration, dict):
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "duration")
    value = duration.get("value")
    label = duration.get("label")
    if type(value) is not int or value <= 0:
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "duration_value")
    if not isinstance(label, str) or label.strip().lower() != "minutes":
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "duration_label")
    return value


def _require_marker(payload: dict[str, Any], *, expected_marker: str) -> str:
    """The migration marker, in the field the API actually returns it in.

    ``booking_comment`` goes out; ``public_notes`` comes back. v1 looked for
    ``comment`` — a field that does not exist on the response — so the marker
    check could never have passed against the live API.

    A ``null`` ``public_notes`` is not evidence of anything except that the
    marker is not there, which is a refusal: without it we cannot say the booking
    in front of us is one this migration wrote.
    """
    notes = payload.get("public_notes")
    if not isinstance(notes, str) or expected_marker not in notes:
        raise TargetSnapshotError(SNAPSHOT_MARKER_MISSING, "public_notes")
    return expected_marker


def _require_timezone(payload: dict[str, Any]) -> str:
    value = payload.get("timezone")
    if not isinstance(value, str) or not value.strip():
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "timezone")
    return value.strip()


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


def prove_canceled_target(payload: dict[str, Any], *, expected_marker: str) -> bool:
    """Is this OUR booking, and does it literally read as cancelled?

    Needed because `project_target` refuses a cancelled booking outright — which
    is right for "is this untouched?" and useless for "did our cancel land?".
    The rollback recovery has to ask the second question about a booking the
    first one has already rejected.

    Deliberately narrow. It proves the marker first, so a booking that is not
    one this migration wrote can never answer the question at all, and it reads
    ``is_canceled`` as a literal boolean: a missing field, a string, a number or
    a ``null`` is a shape nobody proved, and treating it as cancelled would let
    a live appointment be recorded as rolled back.

    Raises :class:`TargetSnapshotError` when the payload is not ours or not
    readable; the caller treats that as "cannot say", never as "cancelled".
    """
    if not isinstance(payload, dict):
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "booking")
    _require_uuid(payload, "uuid", "uid", field_name="booking_uuid")
    _require_marker(payload, expected_marker=expected_marker)
    flag = payload.get("is_canceled")
    if type(flag) is not bool:
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "is_canceled")
    return flag


def project_target(payload: dict[str, Any], *, expected_marker: str) -> TargetSnapshot:
    """Project one live EasyWeek booking into its comparable snapshot.

    Raises :class:`TargetSnapshotError` on the first field that cannot be read.
    The caller turns that into a blocked or unproven outcome — never into a
    partial match. ``staff_uuid`` is left unset: the payload does not carry one,
    and :mod:`proof` fills it in only after proving it independently.
    """
    if not isinstance(payload, dict):
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISSING, "booking")

    # Identity, then ownership, then liveness — before anything expensive.
    #
    # The order matters to one caller in particular. `prove_target_inactive_or_absent`
    # reads a cancelled booking on purpose and needs `SNAPSHOT_NOT_ACTIVE` back,
    # which is its proof that a ghost is not standing. If the service or time
    # projection ran first, a cancelled booking with a thin `ordered_services`
    # would come back "malformed" instead of "terminal", and reconciliation would
    # report an unresolvable row where it should have reported a clean ending.
    booking_uuid = _require_uuid(payload, "uuid", "uid", field_name="booking_uuid")
    marker = _require_marker(payload, expected_marker=expected_marker)
    active = _require_active(payload)

    try:
        ordered = read_ordered_service(payload)
    except ServiceEvidenceError as exc:
        raise TargetSnapshotError(exc.reason, exc.detail) from None

    start = _require_instant(payload, "start_time")
    end = _require_instant(payload, "end_time")
    duration = _require_duration_minutes(payload)

    # The three time fields must agree with each other. A booking whose stated
    # length disagrees with its own start and end is not a booking we can say
    # anything confident about.
    begin = datetime.fromisoformat(start.replace("Z", "+00:00"))
    finish = datetime.fromisoformat(end.replace("Z", "+00:00"))
    if finish - begin != timedelta(minutes=duration):
        raise TargetSnapshotError(SNAPSHOT_FIELD_MISMATCH, "duration_vs_end_time")

    return TargetSnapshot(
        booking_uuid=booking_uuid,
        location_uuid=_require_uuid(payload, "location_uuid", "location", field_name="location_uuid"),
        customer_uuid=_require_uuid(payload, "customer", "customer_uuid", field_name="customer_uuid"),
        start_time_utc=start,
        end_time_utc=end,
        duration_minutes=duration,
        timezone_name=_require_timezone(payload),
        currency=ordered.currency,
        price_minor=ordered.price_minor,
        service_name_digest=service_name_digest(ordered.normalized_name),
        marker=marker,
        active=active,
    )


def expected_snapshot(
    *,
    booking_uuid: str,
    location_uuid: str,
    staff_uuid: str,
    customer_uuid: str,
    start_time_utc: str,
    duration_minutes: int,
    timezone_name: str,
    currency: str,
    price_minor: int,
    service_name: str,
    marker: str,
) -> TargetSnapshot:
    """The snapshot a successful write SHOULD produce, built from what we sent.

    ``end_time_utc`` is derived rather than accepted: the end EasyWeek reports has
    to follow from the start and the catalogue length, and deriving it here is
    what makes the comparison able to notice when it does not.
    """
    begin = datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
    return TargetSnapshot(
        booking_uuid=booking_uuid,
        location_uuid=location_uuid,
        staff_uuid=staff_uuid,
        customer_uuid=customer_uuid,
        start_time_utc=start_time_utc,
        end_time_utc=to_utc_iso(begin + timedelta(minutes=duration_minutes)),
        duration_minutes=duration_minutes,
        timezone_name=timezone_name,
        currency=currency,
        price_minor=price_minor,
        service_name_digest=service_name_digest(normalize_service_name(service_name) or ""),
        marker=marker,
        active=True,
    )


def compare(live: TargetSnapshot, expected: TargetSnapshot) -> SnapshotMismatch:
    """Field-by-field comparison, reporting every difference as a stable code.

    An unproven master (``staff_uuid is None`` on the live side) is reported
    under its own code rather than as a generic mismatch, because "we did not
    manage to check" and "it is the wrong master" call for different actions.
    """
    mismatch = SnapshotMismatch()
    if live.staff_uuid is None:
        mismatch.reasons.append(SNAPSHOT_STAFF_UNPROVEN)
    for field_name in COMPARED_FIELDS:
        if field_name == "staff_uuid" and live.staff_uuid is None:
            continue
        if getattr(live, field_name) != getattr(expected, field_name):
            mismatch.reasons.append(f"{SNAPSHOT_FIELD_MISMATCH}:{field_name}")
    return mismatch
