"""PR-8: which reminders a booking owes, and how they are identified.

Two things are decided here and nowhere else: the scheduling windows, and the
dedupe key. Both are the kind of rule that looks obvious until it is wrong in
production — a reminder that fires for an hour the appointment no longer has, or
a Resend that produces a second copy of a message a customer already received.

The key is the subtler half. A lifecycle job is identified by the DELIVERY that
produced it (its payload hash), which is exactly wrong for a reminder: two
different deliveries describing the same appointment owe the SAME reminder. So
the key is built from the business fact instead — booking, kind, and the start
instant — and these tests pin that distinction from both sides.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES, REMINDER_2H, REMINDER_24H
from altegio_bot.easyweek_reminders import (
    REMINDER_OFFSETS,
    easyweek_reminder_dedupe_key,
    plan_reminders,
    reminder_job_payload,
)

BOOKING = uuid.UUID("11111111-2222-4333-8444-555555555555")
OTHER_BOOKING = uuid.UUID("99999999-8888-4777-8666-555555555555")
NOW = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
COMPANY_ID = 999001


def _plan(starts_at: datetime | None, *, now: datetime = NOW, is_deleted: bool = False):
    return plan_reminders(booking_uuid=BOOKING, starts_at=starts_at, now=now, is_deleted=is_deleted)


def _kinds(planned) -> list[str]:
    return [item.job_type for item in planned]


# ---------------------------------------------------------------------------
# The scheduling windows
# ---------------------------------------------------------------------------


def test_a_booking_more_than_a_day_away_owes_both_reminders() -> None:
    planned = _plan(NOW + timedelta(days=3))

    assert _kinds(planned) == [REMINDER_24H, REMINDER_2H], "soonest first"
    assert planned[0].run_at == NOW + timedelta(days=3) - timedelta(hours=24)
    assert planned[1].run_at == NOW + timedelta(days=3) - timedelta(hours=2)


@pytest.mark.parametrize("hours", [23, 12, 3])
def test_a_booking_inside_the_day_owes_only_the_two_hour_reminder(hours: int) -> None:
    """The 24h moment has already passed; sending it late is worse than not."""
    planned = _plan(NOW + timedelta(hours=hours))

    assert _kinds(planned) == [REMINDER_2H]
    assert planned[0].run_at == NOW + timedelta(hours=hours - 2)


@pytest.mark.parametrize("hours", [2, 1, 0.5, 0])
def test_a_booking_two_hours_away_or_less_owes_nothing(hours: float) -> None:
    """A reminder arriving after the customer has left is noise."""
    assert _plan(NOW + timedelta(hours=hours)) == []


def test_a_booking_already_in_the_past_owes_nothing() -> None:
    assert _plan(NOW - timedelta(days=1)) == []


@pytest.mark.parametrize("offset_hours", [24, 2])
def test_the_boundary_is_strictly_in_the_future(offset_hours: int) -> None:
    """`run_at == now` is not "just in time", it is already late."""
    exactly_now = NOW + timedelta(hours=offset_hours)
    planned = _plan(exactly_now)
    assert all(item.run_at > NOW for item in planned)
    assert REMINDER_24H not in _kinds(planned) or offset_hours != 24


def test_a_cancelled_booking_owes_nothing() -> None:
    assert _plan(NOW + timedelta(days=3), is_deleted=True) == []


def test_a_booking_without_a_known_start_owes_nothing() -> None:
    """A reminder needs a time; inventing one sends a customer to the wrong hour."""
    assert _plan(None) == []


def test_the_offsets_are_exactly_the_two_documented_ones() -> None:
    assert REMINDER_OFFSETS == {REMINDER_24H: timedelta(hours=24), REMINDER_2H: timedelta(hours=2)}
    assert set(REMINDER_OFFSETS) == EASYWEEK_REMINDER_JOB_TYPES


def test_a_naive_start_is_read_as_utc_rather_than_shifting_the_appointment() -> None:
    naive = (NOW + timedelta(days=3)).replace(tzinfo=None)
    assert [item.run_at for item in _plan(naive)] == [item.run_at for item in _plan(NOW + timedelta(days=3))]


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def _key(*, booking: uuid.UUID = BOOKING, job_type: str = REMINDER_24H, starts_at: datetime | None = None) -> str:
    return easyweek_reminder_dedupe_key(
        booking_uuid=booking,
        job_type=job_type,
        starts_at=starts_at or (NOW + timedelta(days=3)),
    )


def test_the_same_business_fact_always_produces_the_same_key() -> None:
    """A Resend, an unrelated edit, a second delivery: one reminder."""
    assert _key() == _key()


def test_a_different_booking_produces_a_different_key() -> None:
    assert _key() != _key(booking=OTHER_BOOKING)


def test_the_two_reminder_kinds_never_share_a_key() -> None:
    assert _key(job_type=REMINDER_24H) != _key(job_type=REMINDER_2H)


def test_moving_the_appointment_produces_a_genuinely_different_key() -> None:
    """A reschedule is a new fact; the stale job is cancelled separately."""
    moved = NOW + timedelta(days=3, hours=1)
    assert _key() != _key(starts_at=moved)


def test_the_same_instant_in_another_offset_is_one_fact() -> None:
    start = NOW + timedelta(days=3)
    assert _key(starts_at=start) == _key(starts_at=start.astimezone(timezone(timedelta(hours=2))))


def test_the_key_is_namespaced_and_fits_the_real_column() -> None:
    """`message_jobs.dedupe_key` is a bounded column shared with Altegio.

    The bound is read from the model rather than restated, so a column change
    cannot leave this passing against a number nobody updated.
    """
    from altegio_bot.models.models import MessageJob as _MessageJob

    limit = _MessageJob.__table__.c.dedupe_key.type.length
    key = _key()
    assert key.startswith("easyweek_reminder:")
    assert REMINDER_24H in key
    assert len(key) <= limit, f"key of {len(key)} would be truncated into {limit}"


def test_a_planned_reminder_carries_the_key_it_will_be_inserted_with() -> None:
    planned = _plan(NOW + timedelta(days=3))
    assert planned[0].dedupe_key == _key(job_type=REMINDER_24H)
    assert planned[1].dedupe_key == _key(job_type=REMINDER_2H)


# ---------------------------------------------------------------------------
# The payload is deliberately tiny
# ---------------------------------------------------------------------------


def test_the_payload_carries_only_what_the_send_path_cannot_re_read() -> None:
    payload = reminder_job_payload(
        booking_uuid=BOOKING,
        company_id=COMPANY_ID,
        starts_at=NOW + timedelta(days=3),
        job_type=REMINDER_24H,
    )

    assert set(payload) == {"provider", "booking_uuid", "company_id", "job_type", "record_starts_at"}
    assert payload["provider"] == "easyweek"
    assert payload["booking_uuid"] == str(BOOKING)
    assert payload["job_type"] == REMINDER_24H


def test_the_payload_holds_no_customer_data() -> None:
    """A payload is where data goes to rot; everything else is re-read at send."""
    payload = reminder_job_payload(
        booking_uuid=BOOKING,
        company_id=COMPANY_ID,
        starts_at=NOW + timedelta(days=3),
        job_type=REMINDER_2H,
    )
    text = str(payload)
    for forbidden in ("name", "phone", "email", "service", "price", "title", "comment"):
        assert forbidden not in text.lower()


def test_the_stored_start_is_normalised_and_timezone_aware() -> None:
    """The guard compares it against the API; a naive value would compare wrong."""
    payload = reminder_job_payload(
        booking_uuid=BOOKING,
        company_id=COMPANY_ID,
        starts_at=datetime(2026, 9, 14, 10, 30, tzinfo=timezone(timedelta(hours=2))),
        job_type=REMINDER_24H,
    )
    parsed = datetime.fromisoformat(str(payload["record_starts_at"]))
    assert parsed.tzinfo is not None
    assert parsed == datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)
