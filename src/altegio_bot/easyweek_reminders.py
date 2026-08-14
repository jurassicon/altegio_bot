"""PR-8: which EasyWeek reminders a booking owes, and when.

Deliberately NOT a call into ``plan_jobs_for_record_event``. That planner is the
Altegio one: it counts visits, plans review/repeat/comeback marketing, applies
the Altegio service filter and reaches for the Altegio API. EasyWeek has an
equivalent for none of those, and its Altegio semantics — job types, dedupe key
format, ordering — must stay byte-for-byte unchanged. So this module states the
EasyWeek reminder rules on their own, and imports nothing from either worker.

The rules, in one place because they have to agree with each other:

* more than 24h away  -> a 24h reminder and a 2h reminder;
* between 2h and 24h  -> only the 2h reminder, because the 24h one is already
  in the past and sending it late is worse than not sending it;
* 2h away or less     -> nothing. A "reminder" that arrives after the customer
  has left for the appointment is noise;
* cancelled booking   -> nothing, and any queued reminder is withdrawn.

Identity, and why the dedupe key looks the way it does. A reminder is not
triggered by a delivery, it is triggered by a TIME — so the lifecycle key
(which is keyed on the delivery's ``payload_hash``) is exactly wrong here: two
different deliveries describing the same appointment owe the SAME reminder, and
a Resend must not produce a second one. The key is therefore keyed on the
business fact a reminder is about:

    provider, booking uuid, job type, and the appointment's start instant.

A reschedule changes the start instant, so it produces a genuinely different
key — a new reminder for a new fact — while the stale ones are cancelled
explicitly rather than left to fire for a time the appointment no longer has.

Nothing here decides eligibility, and nothing here talks to a database: the
caller holds the Record lock and owns the transaction, so planning commits with
the domain write that justified it.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final

from altegio_bot.easyweek_policy import REMINDER_2H, REMINDER_24H

# How far before the appointment each reminder goes out.
REMINDER_OFFSETS: Final[dict[str, timedelta]] = {
    REMINDER_24H: timedelta(hours=24),
    REMINDER_2H: timedelta(hours=2),
}

# Longest reminder dedupe key we will emit. `message_jobs.dedupe_key` is bounded,
# and a key is built from values we do not control the length of, so it is
# hashed rather than concatenated.
_KEY_PREFIX: Final = "easyweek_reminder"


@dataclass(frozen=True)
class PlannedReminder:
    """One reminder a booking owes: which kind, when it fires, and its identity."""

    job_type: str
    run_at: datetime
    dedupe_key: str


def _as_utc(value: datetime) -> datetime:
    """Naive input is a bug upstream, but it must not become a silent offset."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def easyweek_reminder_dedupe_key(
    *,
    booking_uuid: uuid.UUID,
    job_type: str,
    starts_at: datetime,
) -> str:
    """Stable identity for "this booking, this reminder, this start instant".

    Includes the provider and hashes the parts, so it can neither collide with
    an Altegio key nor grow past the column. The start instant is normalised to
    UTC first: the same appointment expressed in two offsets is one fact.
    """
    material = "|".join(
        (
            _KEY_PREFIX,
            str(booking_uuid),
            job_type,
            _as_utc(starts_at).isoformat(),
        )
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:40]
    return f"{_KEY_PREFIX}:{job_type}:{digest}"


def plan_reminders(
    *,
    booking_uuid: uuid.UUID,
    starts_at: datetime | None,
    now: datetime,
    is_deleted: bool = False,
) -> list[PlannedReminder]:
    """The reminders this booking owes from *now*, soonest first.

    Total and side-effect free, so the same function answers for the planner,
    for a test, and for anyone reasoning about what production should hold.

    An appointment with no known start owes nothing: a reminder needs a time,
    and inventing one would send a customer to the wrong hour.
    """
    if is_deleted or starts_at is None:
        return []

    start = _as_utc(starts_at)
    current = _as_utc(now)

    planned: list[PlannedReminder] = []
    for job_type, offset in REMINDER_OFFSETS.items():
        run_at = start - offset
        # Strictly in the future: a reminder whose moment has passed is not
        # "slightly late", it is a message about something the customer is
        # already on their way to.
        if run_at <= current:
            continue
        planned.append(
            PlannedReminder(
                job_type=job_type,
                run_at=run_at,
                dedupe_key=easyweek_reminder_dedupe_key(
                    booking_uuid=booking_uuid,
                    job_type=job_type,
                    starts_at=start,
                ),
            )
        )

    planned.sort(key=lambda item: item.run_at)
    return planned


def reminder_job_payload(
    *,
    booking_uuid: uuid.UUID,
    company_id: int,
    starts_at: datetime,
    job_type: str,
) -> dict[str, object]:
    """The minimal technical payload a reminder job carries.

    Deliberately tiny. No webhook body, no name, no phone, no e-mail, no service
    text, no price, no API response: everything a reminder needs at send time is
    re-read from the Record and re-proven against the API, and a payload is a
    place data goes to rot.

    ``record_starts_at`` is the exception, and it is here for a reason: it is the
    start instant this reminder was planned FOR. At send time it is compared
    against the current ``Record.starts_at`` and against the live API, so a
    reschedule that slipped past the cancellation path cannot deliver a reminder
    for an hour that no longer exists.
    """
    return {
        "provider": "easyweek",
        "booking_uuid": str(booking_uuid),
        "company_id": company_id,
        "job_type": job_type,
        "record_starts_at": _as_utc(starts_at).isoformat(),
    }
