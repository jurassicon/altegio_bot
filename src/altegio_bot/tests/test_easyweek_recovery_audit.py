"""PR-7.1: the post-recovery audit must answer per DELIVERY, not per booking.

The audit this file covers replaced a query that joined an event to *any*
lifecycle job of the same Record. That join is unsound in both directions, and
both directions are pinned here:

* false negative — a booking-created job from last week satisfies the join for a
  booking-updated delivery that was silently dropped during the outage, so the
  audit reports zero losses while a notification is gone;
* false positive — a delivery that legitimately produced no job (suppressed
  category, `booking-succeeded`, post-cancel no-op, replay) is reported as lost.

The fix is to identify a delivery by the same key production uses,
`easyweek_job_dedupe_key()`, and to leave anything without an exact job
*unclassified* rather than declaring it either fine or lost.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_normalizer import easyweek_job_dedupe_key
from altegio_bot.models.models import PROVIDER_EASYWEEK, EasyWeekEvent, MessageJob, Record
from altegio_bot.scripts.easyweek_recovery_audit import (
    SmokeVerificationError,
    audit_recovery,
    expected_job_type,
    group_deliveries,
    verify_controlled_smoke,
)

OUTAGE_START = datetime(2026, 8, 12, 9, 0, tzinfo=timezone.utc)
BOOKING = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000001")


def _event(
    event_id: int,
    hint: str | None,
    *,
    booking: uuid.UUID | None = BOOKING,
    payload_hash: str | None = "hash-a",
    status: str = "processed",
    minutes: int = 5,
) -> EasyWeekEvent:
    return EasyWeekEvent(
        id=event_id,
        event_hint=hint,
        booking_uuid=booking,
        payload_hash=payload_hash,
        status=status,
        received_at=OUTAGE_START + timedelta(minutes=minutes),
    )


# ---------------------------------------------------------------------------
# Mapping — taken from production, not restated
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("hint", "job_type"),
    [
        ("booking-created", "record_created"),
        ("booking-updated", "record_updated"),
        ("booking-rescheduled", "record_updated"),
        ("booking-canceled", "record_canceled"),
    ],
)
def test_lifecycle_hints_map_to_their_job_type(hint: str, job_type: str) -> None:
    assert expected_job_type(hint) == job_type


def test_booking_succeeded_owes_no_lifecycle_job() -> None:
    """It is terminal with no Client/Record/Job side effect — absence is correct."""
    assert expected_job_type("booking-succeeded") is None

    groups, non_lifecycle, unmappable = group_deliveries([_event(1, "booking-succeeded")])

    assert groups == []
    assert non_lifecycle == [1], "booking-succeeded belongs in its own bucket, not in the unclassified list"
    assert unmappable == []


def test_unknown_hint_and_missing_uuid_are_unmappable_not_lost() -> None:
    groups, non_lifecycle, unmappable = group_deliveries(
        [_event(1, "booking-invented"), _event(2, "booking-updated", booking=None)]
    )

    assert groups == []
    assert non_lifecycle == []
    assert unmappable == [1, 2]


# ---------------------------------------------------------------------------
# Delivery identity — why the old record_id join was unsound
# ---------------------------------------------------------------------------


def test_an_older_created_job_cannot_satisfy_a_later_update_delivery() -> None:
    """The exact false negative the previous audit query produced.

    Same booking, same Record — and under the old join the create job answered
    for the update. Under the canonical key it cannot: both the hint and the
    payload hash are inside the digest.
    """
    created = easyweek_job_dedupe_key(
        event_hint="booking-created",
        booking_uuid=BOOKING,
        payload_hash="hash-a",
        job_type="record_created",
    )
    updated = easyweek_job_dedupe_key(
        event_hint="booking-updated",
        booking_uuid=BOOKING,
        payload_hash="hash-b",
        job_type="record_updated",
    )

    assert created != updated


def test_updated_and_rescheduled_share_a_job_type_but_not_an_identity() -> None:
    """Same job_type is not the same delivery — the helper keeps them apart."""
    assert expected_job_type("booking-updated") == expected_job_type("booking-rescheduled")

    groups, _, _ = group_deliveries(
        [
            _event(1, "booking-updated", payload_hash="hash-a"),
            _event(2, "booking-rescheduled", payload_hash="hash-b"),
        ]
    )

    assert len(groups) == 2, "distinct deliveries must not collapse just because job_type matches"
    assert {group.expected_dedupe_key for group in groups} == {
        easyweek_job_dedupe_key(
            event_hint=hint, booking_uuid=BOOKING, payload_hash=payload_hash, job_type="record_updated"
        )
        for hint, payload_hash in (("booking-updated", "hash-a"), ("booking-rescheduled", "hash-b"))
    }


def test_byte_identical_resend_is_one_delivery_group_expecting_one_job() -> None:
    """Three deliveries, one key, one job — deduplication working, not loss."""
    groups, _, _ = group_deliveries([_event(index, "booking-created") for index in (1, 2, 3)])

    assert len(groups) == 1
    group = groups[0]
    assert group.event_ids == (1, 2, 3)
    assert group.is_resend is True


# ---------------------------------------------------------------------------
# Against the database
# ---------------------------------------------------------------------------


async def _seed(session: AsyncSession, *rows: EasyWeekEvent | MessageJob | Record) -> None:
    session.add_all(list(rows))
    await session.commit()


def _job(dedupe_key: str, *, job_type: str, status: str = "queued", record_id: int | None = None) -> MessageJob:
    return MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=999501,
        record_id=record_id,
        job_type=job_type,
        status=status,
        dedupe_key=dedupe_key,
        run_at=OUTAGE_START,
    )


async def test_a_stale_created_job_does_not_green_light_a_dropped_update(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """End-to-end version of the false negative, with a Record-shaped setup.

    A create job exists for this booking. An update delivery was processed
    during the outage and produced nothing. The old audit answered `0`; this one
    must surface the update's event id.
    """
    created_key = easyweek_job_dedupe_key(
        event_hint="booking-created", booking_uuid=BOOKING, payload_hash="hash-a", job_type="record_created"
    )
    async with session_maker() as session:
        # The shared Record is the whole point: the old join walked through it.
        record = Record(
            id=4242,
            provider=PROVIDER_EASYWEEK,
            company_id=999501,
            altegio_record_id=4242,
            easyweek_booking_uuid=BOOKING,
        )
        await _seed(session, record)
        await _seed(
            session,
            _event(1, "booking-created", payload_hash="hash-a", minutes=1),
            _event(2, "booking-updated", payload_hash="hash-b", minutes=9),
            _job(created_key, job_type="record_created", record_id=4242),
        )

        report = await audit_recovery(session, since=OUTAGE_START)

    assert report.lifecycle_groups == 2
    assert report.groups_with_exact_job == 1, "only the create delivery has its own job"
    assert report.no_event_specific_job_unclassified == (2,), (
        "the dropped update must surface even though its Record already has an older job"
    )


async def test_a_delivery_with_its_own_job_is_proven(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    key = easyweek_job_dedupe_key(
        event_hint="booking-updated", booking_uuid=BOOKING, payload_hash="hash-b", job_type="record_updated"
    )
    async with session_maker() as session:
        await _seed(session, _event(2, "booking-updated", payload_hash="hash-b"), _job(key, job_type="record_updated"))

        report = await audit_recovery(session, since=OUTAGE_START)

    assert report.groups_with_exact_job == 1
    assert report.no_event_specific_job_unclassified == ()


async def test_resend_group_is_satisfied_by_a_single_job(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    key = easyweek_job_dedupe_key(
        event_hint="booking-created", booking_uuid=BOOKING, payload_hash="hash-a", job_type="record_created"
    )
    async with session_maker() as session:
        await _seed(
            session,
            _event(1, "booking-created", minutes=1),
            _event(2, "booking-created", minutes=2),
            _job(key, job_type="record_created"),
        )

        report = await audit_recovery(session, since=OUTAGE_START)

    assert report.lifecycle_groups == 1
    assert report.resend_groups == 1
    assert report.groups_with_exact_job == 1
    assert report.no_event_specific_job_unclassified == ()


@pytest.mark.parametrize("status", ["queued", "processing", "done", "failed", "canceled"])
async def test_every_job_status_counts_as_created(
    session_maker: async_sessionmaker[AsyncSession],
    status: str,
) -> None:
    """After recovery a job may legitimately be done, retrying or terminal.

    Requiring queued/processing would report a successfully delivered
    notification as a loss.
    """
    key = easyweek_job_dedupe_key(
        event_hint="booking-canceled", booking_uuid=BOOKING, payload_hash="hash-c", job_type="record_canceled"
    )
    async with session_maker() as session:
        await _seed(
            session,
            _event(3, "booking-canceled", payload_hash="hash-c"),
            _job(key, job_type="record_canceled", status=status),
        )

        report = await audit_recovery(session, since=OUTAGE_START)

    assert report.groups_with_exact_job == 1
    assert report.job_status_counts == {status: 1}
    assert report.no_event_specific_job_unclassified == ()


async def test_the_audit_window_is_honoured(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _seed(session, _event(1, "booking-created", minutes=-120), _event(2, "booking-updated", minutes=5))

        report = await audit_recovery(session, since=OUTAGE_START)

    assert report.no_event_specific_job_unclassified == (2,), "events before the outage window are out of scope"


# ---------------------------------------------------------------------------
# Controlled smoke — Resend can never be the positive proof
# ---------------------------------------------------------------------------

SMOKE_START = OUTAGE_START + timedelta(hours=1)
FRESH_BOOKING = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000042")


def _record(record_id: int, booking: uuid.UUID) -> Record:
    return Record(
        id=record_id,
        provider=PROVIDER_EASYWEEK,
        company_id=999501,
        altegio_record_id=record_id,
        easyweek_booking_uuid=booking,
    )


async def test_a_fresh_booking_with_a_new_job_is_a_valid_positive_smoke(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    key = easyweek_job_dedupe_key(
        event_hint="booking-created", booking_uuid=FRESH_BOOKING, payload_hash="fresh", job_type="record_created"
    )
    async with session_maker() as session:
        await _seed(session, _record(77, FRESH_BOOKING))
        job = _job(key, job_type="record_created", record_id=77)
        job.created_at = SMOKE_START + timedelta(minutes=1)
        await _seed(session, _event(90, "booking-created", booking=FRESH_BOOKING, payload_hash="fresh"), job)

        result = await verify_controlled_smoke(session, event_ids=[90], baseline_event_id=89, smoke_start=SMOKE_START)

    [event] = result["events"]  # type: ignore[index]
    assert event["newer_than_baseline"] is True
    assert event["booking_first_seen_here"] is True
    assert event["exact_jobs"] == 1
    assert event["job_created_after_smoke_start"] is True
    assert event["job_type_matches_event"] is True
    assert event["job_company_matches_record"] is True
    assert event["job_record_matches_booking"] is True


async def test_a_resend_of_a_pre_smoke_delivery_cannot_pass_as_a_positive_smoke(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The false green this contract exists for.

    The Resend reproduces hint, booking uuid and payload hash, so it resolves to
    the very same expected key and finds the job created long before the smoke.
    `exact_jobs=1` looks green — both freshness axes must refuse it.
    """
    key = easyweek_job_dedupe_key(
        event_hint="booking-created", booking_uuid=BOOKING, payload_hash="hash-a", job_type="record_created"
    )
    async with session_maker() as session:
        await _seed(session, _record(55, BOOKING))
        old_job = _job(key, job_type="record_created", record_id=55)
        old_job.created_at = OUTAGE_START - timedelta(days=3)
        await _seed(
            session,
            _event(10, "booking-created", payload_hash="hash-a", minutes=-4000),
            _event(90, "booking-created", payload_hash="hash-a", minutes=70),  # the Resend
            old_job,
        )

        result = await verify_controlled_smoke(session, event_ids=[90], baseline_event_id=89, smoke_start=SMOKE_START)

    [event] = result["events"]  # type: ignore[index]
    assert event["newer_than_baseline"] is True, "the Resend row itself is new — which is exactly the trap"
    assert event["exact_jobs"] == 1, "and it does find a job, because the key is byte-identical"
    assert event["booking_first_seen_here"] is False, "but this booking existed before the smoke"
    assert event["job_created_after_smoke_start"] is False, "and the job predates the smoke — no new job was created"


async def test_a_disallowed_smoke_must_have_no_exact_job_and_no_outbox(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await _seed(session, _record(78, FRESH_BOOKING))
        await _seed(session, _event(91, "booking-created", booking=FRESH_BOOKING, payload_hash="disallowed"))

        result = await verify_controlled_smoke(session, event_ids=[91], baseline_event_id=89, smoke_start=SMOKE_START)

    [event] = result["events"]  # type: ignore[index]
    assert event["event_status"] == "processed", "a suppressed delivery is still processed terminally"
    assert event["record_id"] == 78, "the domain snapshot is kept"
    assert event["exact_jobs"] == 0
    assert event["outbox_rows"] == 0


async def test_distinct_bookings_are_counted_so_both_smokes_cannot_share_one(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    other = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000043")
    async with session_maker() as session:
        await _seed(
            session,
            _event(90, "booking-created", booking=FRESH_BOOKING),
            _event(91, "booking-created", booking=other),
        )

        result = await verify_controlled_smoke(
            session, event_ids=[90, 91], baseline_event_id=89, smoke_start=SMOKE_START
        )

    assert result["distinct_bookings"] == 2


@pytest.mark.parametrize(
    ("event_id", "row"),
    [
        (404, None),
        (92, ("booking-created", None)),
        (93, ("booking-succeeded", FRESH_BOOKING)),
    ],
    ids=["missing-event", "null-booking-uuid", "non-lifecycle-hint"],
)
async def test_the_smoke_fails_closed_instead_of_reporting_green(
    session_maker: async_sessionmaker[AsyncSession],
    event_id: int,
    row: tuple[str, uuid.UUID | None] | None,
) -> None:
    async with session_maker() as session:
        if row is not None:
            hint, booking = row
            await _seed(session, _event(event_id, hint, booking=booking))

        with pytest.raises(SmokeVerificationError):
            await verify_controlled_smoke(session, event_ids=[event_id], baseline_event_id=89, smoke_start=SMOKE_START)


async def test_the_smoke_report_leaks_no_booking_uuid_or_dedupe_key(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    key = easyweek_job_dedupe_key(
        event_hint="booking-created", booking_uuid=FRESH_BOOKING, payload_hash="fresh", job_type="record_created"
    )
    async with session_maker() as session:
        await _seed(session, _record(79, FRESH_BOOKING))
        await _seed(
            session,
            _event(90, "booking-created", booking=FRESH_BOOKING, payload_hash="fresh"),
            _job(key, job_type="record_created", record_id=79),
        )

        result = await verify_controlled_smoke(session, event_ids=[90], baseline_event_id=89, smoke_start=SMOKE_START)

    rendered = str(result)
    assert str(FRESH_BOOKING) not in rendered
    assert key not in rendered
    assert "fresh" not in rendered


async def test_the_printed_report_carries_no_dedupe_key_or_payload(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """Operators paste this output into tickets — it must stay safe."""
    async with session_maker() as session:
        await _seed(session, _event(1, "booking-created"))

        report = await audit_recovery(session, since=OUTAGE_START)

    rendered = str(report.as_safe_dict())
    assert str(BOOKING) not in rendered
    assert "hash-a" not in rendered
    assert "easyweek:record_created" not in rendered
