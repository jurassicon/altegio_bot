"""PR-8: prove a reminder is still true, immediately before it is sent.

A lifecycle notification describes a delivery that just arrived. A reminder is
different in the one way that matters: it is planned hours or days ahead and
then fires on a clock. Everything that justified it can have changed in between
— the customer moved the appointment, the salon cancelled it, the booking was
completed early, the branch was reconfigured — and none of those necessarily
produce a webhook we saw. Sending a reminder for an appointment that no longer
exists is not a cosmetic bug: it tells a real person to show up.

So before every single Meta attempt, this module asks EasyWeek one read-only
question::

    GET /bookings/{uuid}

and refuses unless the answer proves, together and without interpretation:

* the job is an EasyWeek reminder carrying a canonical booking uuid and the
  immutable start instant it was planned for;
* the Record is the EasyWeek record for that booking, in that company, not
  deleted, and still starting at exactly that instant;
* the company is a branch of the current ``EASYWEEK_LOCATION_MAP`` with a
  canonical location uuid;
* the API's booking is the same uuid, in the same location, starting at the
  same instant, and is explicitly neither cancelled nor completed.

**Nothing is read by truthiness.** ``is_canceled`` must be a real ``bool``
``False``; the string ``"false"``, ``0``, ``None`` and a missing key are all
refusals, because each of them is what a changed API or a partial response looks
like, and each of them is truthy-or-falsy in a way that would quietly pass. The
localized ``booking_status`` prose is never consulted — it is salon-editable
text — and a ``status.type`` that contradicts the boolean flags is a refusal
rather than a tie-break.

The result is a typed outcome, never a response. No caller can reach the raw
body, the ``customer`` subtree, the price or the URL, and no reason code
contains any of them: these strings end up in ``MessageJob.last_error``, which
is read in a terminal and pasted into tickets.

The same function serves the runtime outbox and the read-only preflight. That is
the point of it being here rather than inside the worker: a preflight that
checked something subtly different from what production checks would be worse
than no preflight at all.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_locations import EasyWeekLocation
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageJob, Record


class BookingReader(Protocol):
    """The one call this guard is allowed to make.

    Typed as a protocol so the preflight and the tests can pass a fake without
    anything gaining the ability to POST: an object satisfying this interface
    cannot create, modify or cancel a booking.
    """

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...


class GuardOutcome(str, Enum):
    """What the guard proved, or why it refused.

    Only ``PROVEN_CURRENT`` permits a send. The rest split into recoverable
    (try again later, the world may change) and terminal (this reminder is
    wrong and will not become right).
    """

    PROVEN_CURRENT = "proven_current"
    # Recoverable: nothing is known to be wrong, we just could not ask.
    RETRYABLE_UNAVAILABLE = "retryable_unavailable"
    CONFIGURATION_UNAVAILABLE = "configuration_unavailable"
    # Terminal: the reminder is provably wrong.
    NOT_FOUND = "not_found"
    IDENTITY_MISMATCH = "identity_mismatch"
    LOCATION_MISMATCH = "location_mismatch"
    START_TIME_MISMATCH = "start_time_mismatch"
    CANCELED = "canceled"
    COMPLETED = "completed"
    MALFORMED_RESPONSE = "malformed_response"
    PERMANENT_ERROR = "permanent_error"


# Outcomes the caller may retry later. Everything else is terminal, and a
# terminal outcome cancels the job locally without ever calling Meta.
RECOVERABLE_OUTCOMES: frozenset[GuardOutcome] = frozenset(
    {
        GuardOutcome.RETRYABLE_UNAVAILABLE,
        GuardOutcome.CONFIGURATION_UNAVAILABLE,
    }
)


@dataclass(frozen=True)
class GuardResult:
    """A verdict and a short, stable, PII-free reason."""

    outcome: GuardOutcome
    reason: str

    @property
    def proven(self) -> bool:
        return self.outcome is GuardOutcome.PROVEN_CURRENT

    @property
    def recoverable(self) -> bool:
        return self.outcome in RECOVERABLE_OUTCOMES


def _refuse(outcome: GuardOutcome, detail: str) -> GuardResult:
    """Build a verdict whose text names a field, never a value."""
    return GuardResult(outcome=outcome, reason=f"easyweek_reminder_guard:{outcome.value}:{detail}")


def _canonical_uuid(value: object) -> uuid.UUID | None:
    """A canonical UUID, or ``None``. Never raises on hostile input."""
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str):
        return None
    try:
        return uuid.UUID(value.strip())
    except (ValueError, AttributeError, TypeError):
        return None


def _aware_utc(value: object) -> datetime | None:
    """A timezone-AWARE instant in UTC, or ``None``.

    A naive timestamp is refused rather than assumed to be UTC. Guessing an
    offset here would compare two different moments and call them equal, which
    is precisely the failure this guard exists to prevent.
    """
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith(("Z", "z")):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _strict_false(value: object) -> bool | None:
    """``True`` when the flag is a real ``False``; ``None`` when unusable.

    Only ``bool`` is accepted. ``"false"``, ``0``, ``""`` and ``None`` each mean
    "the API did not tell us", and each of them would read as "not cancelled"
    under ordinary truthiness.
    """
    if value is True:
        return False
    if value is False:
        return True
    return None


def _status_type_contradicts(payload: dict[str, Any]) -> str | None:
    """A ``status.type`` that disagrees with the boolean flags, if present.

    Optional field, strictly checked: when EasyWeek does send it and it says the
    booking is cancelled or completed while the booleans say otherwise, the two
    halves of the response disagree and neither is trusted.
    """
    status = payload.get("status")
    if not isinstance(status, dict):
        return None
    status_type = status.get("type")
    if not isinstance(status_type, str):
        return None
    normalized = status_type.strip().casefold()
    if normalized in {"canceled", "cancelled"}:
        return "status_type_canceled"
    if normalized in {"completed", "succeeded", "finished"}:
        return "status_type_completed"
    return None


def _observed_status_contradiction(
    payload: dict[str, Any],
    *,
    is_canceled: bool,
    is_completed: bool,
) -> str | None:
    """Judge optional status prose against status facts for handover reads.

    The runtime send guard deliberately keeps its established ordering and
    reason codes in :func:`_status_type_contradicts`.  The handover has a
    different job: it must be able to *read* a consistently terminal booking so
    it can retire the obsolete Altegio reminder without planning a replacement.
    """
    if is_canceled and is_completed:
        return "status_flags_both_terminal"
    status = payload.get("status")
    if not isinstance(status, dict):
        return None
    status_type = status.get("type")
    if not isinstance(status_type, str):
        return None
    normalized = status_type.strip().casefold()
    canceled_types = {"canceled", "cancelled"}
    completed_types = {"completed", "succeeded", "finished"}
    if is_canceled:
        return None if normalized in canceled_types else "status_type_vs_canceled"
    if is_completed:
        return None if normalized in completed_types else "status_type_vs_completed"
    if normalized in canceled_types:
        return "status_type_canceled"
    if normalized in completed_types:
        return "status_type_completed"
    return None


def _job_booking_uuid(job: MessageJob) -> uuid.UUID | None:
    payload = getattr(job, "payload", None)
    if not isinstance(payload, dict):
        return None
    return _canonical_uuid(payload.get("booking_uuid"))


def _job_record_starts_at(job: MessageJob) -> datetime | None:
    payload = getattr(job, "payload", None)
    if not isinstance(payload, dict):
        return None
    return _aware_utc(payload.get("record_starts_at"))


def check_local_preconditions(
    job: MessageJob,
    record: Record | None,
    location: EasyWeekLocation | None,
) -> GuardResult | None:
    """Everything provable without the network, or ``None`` when all holds.

    Runs first so a job that is already wrong locally never costs an API call —
    and, more importantly, so a mismatch is reported as the mismatch it is
    rather than as whatever the API happens to answer.
    """
    provider = getattr(job, "provider", None)
    if provider != PROVIDER_EASYWEEK:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "job_provider")
    if getattr(job, "job_type", None) not in EASYWEEK_REMINDER_JOB_TYPES:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "job_type")
    if getattr(job, "record_id", None) is None:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "job_record_id")

    booking_uuid = _job_booking_uuid(job)
    if booking_uuid is None:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "job_booking_uuid")
    planned_start = _job_record_starts_at(job)
    if planned_start is None:
        return _refuse(GuardOutcome.START_TIME_MISMATCH, "job_record_starts_at")

    if record is None:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "record_missing")
    if getattr(record, "provider", None) != PROVIDER_EASYWEEK:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "record_provider")
    if getattr(record, "company_id", None) != getattr(job, "company_id", None):
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "record_company_id")
    if _canonical_uuid(getattr(record, "easyweek_booking_uuid", None)) != booking_uuid:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "record_booking_uuid")
    if bool(getattr(record, "is_deleted", False)):
        return _refuse(GuardOutcome.CANCELED, "record_is_deleted")

    record_start = _aware_utc(getattr(record, "starts_at", None))
    if record_start is None:
        return _refuse(GuardOutcome.START_TIME_MISMATCH, "record_starts_at")
    if record_start != planned_start:
        # The appointment moved after this reminder was planned. Sending it now
        # would name the old hour.
        return _refuse(GuardOutcome.START_TIME_MISMATCH, "record_vs_job")

    if location is None:
        return _refuse(GuardOutcome.LOCATION_MISMATCH, "registry_entry")
    if _canonical_uuid(location.location_uuid) is None:
        return _refuse(GuardOutcome.LOCATION_MISMATCH, "registry_location_uuid")

    return None


@dataclass(frozen=True)
class ObservedBooking:
    """What ``GET /bookings/{uuid}`` says a booking IS, right now.

    The send guard asks a different question — "does this booking still match the
    reminder we planned?" — and answers it against an expectation it was given.
    The post-migration handover has no expectation to compare against: it is
    working out what reminders a booking OWES, so it needs the booking's current
    start and status read out rather than judged.

    Deliberately a second reader over the SAME field-parsing helpers rather than
    a refactor of :func:`check_api_response`. That function's refusal ORDER is
    part of its contract — a payload with both a wrong start and a malformed
    ``is_canceled`` must keep reporting the start mismatch — and reordering it to
    share a code path would have changed the runtime guard's behaviour to serve a
    caller that is not the runtime. Nothing about the send path moves.
    """

    booking_uuid: uuid.UUID
    location_uuid: uuid.UUID
    starts_at: datetime
    is_canceled: bool
    is_completed: bool

    @property
    def is_active(self) -> bool:
        return not (self.is_canceled or self.is_completed)


def read_booking_state(
    payload: object,
    *,
    booking_uuid: uuid.UUID,
    location: EasyWeekLocation,
) -> ObservedBooking | GuardResult:
    """Read one booking body into facts, or refuse with the guard's own codes.

    Identity is still PROVEN, not assumed: the body must name the uuid that was
    asked for and the branch the caller claims, or nothing is read out of it.
    Only after that are the start and the two status flags taken at face value —
    and a flag that is neither ``true`` nor ``false`` is malformed, never
    optimistically read as "not cancelled".
    """
    if not isinstance(payload, dict):
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "not_an_object")

    api_uuid = _canonical_uuid(payload.get("uuid"))
    if api_uuid is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "uuid")
    if api_uuid != booking_uuid:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "api_uuid")

    api_location = _canonical_uuid(payload.get("location_uuid"))
    if api_location is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "location_uuid")
    if api_location != _canonical_uuid(location.location_uuid):
        return _refuse(GuardOutcome.LOCATION_MISMATCH, "api_location_uuid")

    api_start = _aware_utc(payload.get("start_time"))
    if api_start is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "start_time")

    canceled_ok = _strict_false(payload.get("is_canceled"))
    if canceled_ok is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "is_canceled")
    completed_ok = _strict_false(payload.get("is_completed"))
    if completed_ok is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "is_completed")

    is_canceled = not canceled_ok
    is_completed = not completed_ok
    contradiction = _observed_status_contradiction(
        payload,
        is_canceled=is_canceled,
        is_completed=is_completed,
    )
    if contradiction is not None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, contradiction)

    return ObservedBooking(
        booking_uuid=api_uuid,
        location_uuid=api_location,
        starts_at=api_start,
        # `_strict_false` returns True when the flag is a literal `false`, so
        # these read inverted: "the flag was cleanly false" means "not that".
        is_canceled=is_canceled,
        is_completed=is_completed,
    )


def check_api_response(
    payload: object,
    *,
    booking_uuid: uuid.UUID,
    location: EasyWeekLocation,
    expected_start: datetime,
) -> GuardResult:
    """Judge one ``GET /bookings/{uuid}`` body. Pure, and never echoes it."""
    if not isinstance(payload, dict):
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "not_an_object")

    api_uuid = _canonical_uuid(payload.get("uuid"))
    if api_uuid is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "uuid")
    if api_uuid != booking_uuid:
        return _refuse(GuardOutcome.IDENTITY_MISMATCH, "api_uuid")

    api_location = _canonical_uuid(payload.get("location_uuid"))
    if api_location is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "location_uuid")
    if api_location != _canonical_uuid(location.location_uuid):
        # The booking belongs to a different branch than the job claims. Its
        # template, footer and sender would all be the wrong branch's.
        return _refuse(GuardOutcome.LOCATION_MISMATCH, "api_location_uuid")

    api_start = _aware_utc(payload.get("start_time"))
    if api_start is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "start_time")
    if api_start != expected_start:
        return _refuse(GuardOutcome.START_TIME_MISMATCH, "api_start_time")

    canceled_ok = _strict_false(payload.get("is_canceled"))
    if canceled_ok is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "is_canceled")
    if not canceled_ok:
        return _refuse(GuardOutcome.CANCELED, "api_is_canceled")

    completed_ok = _strict_false(payload.get("is_completed"))
    if completed_ok is None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "is_completed")
    if not completed_ok:
        return _refuse(GuardOutcome.COMPLETED, "api_is_completed")

    contradiction = _status_type_contradicts(payload)
    if contradiction is not None:
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, contradiction)

    return GuardResult(outcome=GuardOutcome.PROVEN_CURRENT, reason="easyweek_reminder_guard:proven_current")


def classify_client_error(exc: Exception) -> GuardResult:
    """Map a typed client failure onto an outcome, without keeping its text.

    Order matters: the config and auth cases are checked before the generic
    permanent one they inherit from.
    """
    if isinstance(exc, EasyWeekRetryableError):
        # 429, 5xx, timeout, transport. Nothing is known to be wrong.
        return _refuse(GuardOutcome.RETRYABLE_UNAVAILABLE, "api_unavailable")
    if isinstance(exc, (EasyWeekConfigError, EasyWeekAuthError)):
        # A missing key, a missing slug, a 401/403. The booking may be perfectly
        # fine; we are the ones who cannot ask. Recoverable, and red in preflight.
        return _refuse(GuardOutcome.CONFIGURATION_UNAVAILABLE, "api_credentials")
    if isinstance(exc, EasyWeekNotFoundError):
        return _refuse(GuardOutcome.NOT_FOUND, "api_404")
    if isinstance(exc, EasyWeekProtocolError):
        return _refuse(GuardOutcome.MALFORMED_RESPONSE, "api_protocol")
    if isinstance(exc, EasyWeekPermanentError):
        return _refuse(GuardOutcome.PERMANENT_ERROR, "api_permanent")
    # An unexpected exception class is not evidence of anything. Fail closed
    # without recording the message, which can carry a URL or a body.
    return _refuse(GuardOutcome.PERMANENT_ERROR, "api_unexpected")


async def verify_reminder_is_current(
    *,
    job: MessageJob,
    record: Record | None,
    location: EasyWeekLocation | None,
    client: BookingReader,
) -> GuardResult:
    """The whole guard: local proof, one GET, and a typed verdict.

    The local checks run first and can refuse without an API call. Once the API
    is asked, its answer is judged field by field against the values already
    proven locally — never the other way round.
    """
    local = check_local_preconditions(job, record, location)
    if local is not None:
        return local

    # Non-None by construction: check_local_preconditions proved all three.
    booking_uuid = _job_booking_uuid(job)
    expected_start = _job_record_starts_at(job)
    assert booking_uuid is not None and expected_start is not None and location is not None

    try:
        payload = await client.get_booking(str(booking_uuid))
    except Exception as exc:  # noqa: BLE001 — mapped by class, text never kept
        return classify_client_error(exc)

    return check_api_response(
        payload,
        booking_uuid=booking_uuid,
        location=location,
        expected_start=expected_start,
    )
