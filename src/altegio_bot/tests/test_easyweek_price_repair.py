"""PR-7.3: the historical price repair must fix the bug and nothing else.

Every EasyWeek record written before PR-7.3 holds a price a hundred times too
small, because the old parser divided ``booking_price_int`` by 100. Correcting
stored production money is the kind of operation that is far more dangerous than
the defect it repairs, so the command is built as a proof rather than a scan:
it repairs a row only when the row still carries the exact signature of the bug,
and it counts everything else as a skip.

These tests run against PostgreSQL and assert on both directions — that the
corrupted row really is fixed, and that every row we cannot prove is left
untouched, including Altegio records, ambiguous service sets, truncated
captures, hand-edited snapshots and rows that are already right.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
    RecordService,
)
from altegio_bot.scripts.easyweek_price_repair import (
    SKIP_ALREADY_CORRECT,
    SKIP_NO_BOOKING_UUID,
    SKIP_NO_USABLE_EVENT,
    SKIP_NOT_ONE_SERVICE,
    SKIP_SIGNATURE_MISMATCH,
    SKIP_SNAPSHOT_INCONSISTENT,
    _parse_args,
    legacy_price,
    repair_prices,
)
from altegio_bot.tests.easyweek_fixtures import TEST_LOCATION_ID, booking_created

# `asyncio_mode = auto` handles the async tests; this file also holds sync ones,
# so there is deliberately no module-level asyncio mark.
BOOKING = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000101")
STARTS_AT = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)

# The confirmed production quartet for 120.00 €. The old parser turned the first
# field into 1.20; the fixed parser reads the second and gets 120.00.
PRODUCTION_MINOR_UNITS = "12000"
PRODUCTION_MAJOR_UNITS = 120
CORRUPTED = Decimal("1.20")
CORRECT = Decimal("120.00")


def _price_payload(
    *,
    minor_units: str | None = PRODUCTION_MINOR_UNITS,
    major_units: int | None = PRODUCTION_MAJOR_UNITS,
    projection: str | None = "120.00",
    booking: uuid.UUID = BOOKING,
) -> dict[str, Any]:
    payload = booking_created()
    payload["uid"] = str(booking)
    payload["location_id"] = TEST_LOCATION_ID
    payload["booking_price"] = minor_units
    payload["booking_price_int"] = major_units
    payload["booking_price_float"] = projection
    payload["booking_price_formatted"] = f"€{projection}"
    return payload


async def _seed_event(
    session: AsyncSession,
    *,
    payload: dict[str, Any] | None = None,
    booking: uuid.UUID | None = BOOKING,
    body_truncated: bool = False,
    minutes: int = 0,
) -> EasyWeekEvent:
    event = EasyWeekEvent(
        event_hint="booking-created",
        booking_uuid=booking,
        payload=payload if payload is not None else _price_payload(),
        payload_hash="hash-a",
        status="processed",
        body_truncated=body_truncated,
        received_at=STARTS_AT - timedelta(days=1) + timedelta(minutes=minutes),
    )
    session.add(event)
    await session.flush()
    return event


async def _seed_record(
    session: AsyncSession,
    *,
    total_cost: Decimal | None = CORRUPTED,
    service_costs: tuple[Decimal | None, ...] = (CORRUPTED,),
    booking: uuid.UUID | None = BOOKING,
    provider: str = PROVIDER_EASYWEEK,
    altegio_record_id: int = 4200001,
) -> Record:
    record = Record(
        provider=provider,
        company_id=TEST_LOCATION_ID,
        altegio_record_id=altegio_record_id,
        easyweek_booking_uuid=booking if provider == PROVIDER_EASYWEEK else None,
        client_id=1,
        staff_name="Tanja",
        starts_at=STARTS_AT,
        total_cost=total_cost,
        raw={},
    )
    session.add(record)
    await session.flush()
    for offset, cost in enumerate(service_costs):
        session.add(
            RecordService(
                record_id=record.id,
                service_id=11 + offset,
                title="Wimpernverlängerung",
                cost_to_pay=cost,
                raw={},
            )
        )
    await session.flush()
    return record


async def _snapshot(session_maker: async_sessionmaker[AsyncSession], record_id: int) -> tuple:
    async with session_maker() as session:
        record = (await session.execute(select(Record).where(Record.id == record_id))).scalars().one()
        costs = sorted(
            cost or Decimal("-1")
            for cost in (
                await session.execute(select(RecordService.cost_to_pay).where(RecordService.record_id == record_id))
            )
            .scalars()
            .all()
        )
        return record.total_cost, costs


# ---------------------------------------------------------------------------
# The old formula, replayed
# ---------------------------------------------------------------------------


def test_the_legacy_formula_is_reproduced_exactly() -> None:
    """The proof that a row came from the bug depends on this being faithful."""
    assert legacy_price({"booking_price_int": 120}) == Decimal("1.20")
    assert legacy_price({"booking_price_int": 3500}) == Decimal("35.00")
    assert legacy_price({"booking_price_int": 0}) == Decimal("0.00")


@pytest.mark.parametrize("bad", [None, "120", True, 1.5, -1, 10**30])
def test_the_legacy_formula_refuses_what_the_old_parser_refused(bad: object) -> None:
    """A value the old parser would have rejected cannot have written a row."""
    assert legacy_price({"booking_price_int": bad}) is None


# ---------------------------------------------------------------------------
# Dry-run and apply
# ---------------------------------------------------------------------------


async def test_the_default_run_reports_the_damage_and_changes_nothing(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session)
    record_id = record.id

    report = await repair_prices(session_maker)

    assert report.applied is False
    assert report.scanned == 1
    assert report.repairable == 1
    assert report.repaired == 0
    assert report.repairable_record_ids == [record_id]
    assert await _snapshot(session_maker, record_id) == (CORRUPTED, [CORRUPTED])


async def test_apply_corrects_both_halves_of_the_snapshot(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 1
    assert report.repaired_record_ids == [record_id]
    assert await _snapshot(session_maker, record_id) == (CORRECT, [CORRECT])


async def test_a_second_apply_changes_nothing(session_maker) -> None:
    """Idempotent by construction: the repaired value no longer matches the bug."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session)
    record_id = record.id

    await repair_prices(session_maker, apply=True)
    second = await repair_prices(session_maker, apply=True)

    assert second.repaired == 0
    assert second.repairable == 0
    assert second.skipped[SKIP_ALREADY_CORRECT] == 1
    assert await _snapshot(session_maker, record_id) == (CORRECT, [CORRECT])


async def test_a_row_that_is_already_correct_is_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session, total_cost=CORRECT, service_costs=(CORRECT,))
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0
    assert report.skipped[SKIP_ALREADY_CORRECT] == 1
    assert await _snapshot(session_maker, record_id) == (CORRECT, [CORRECT])


# ---------------------------------------------------------------------------
# Everything the repair refuses to touch
# ---------------------------------------------------------------------------


async def test_an_altegio_record_is_never_read_or_written(session_maker) -> None:
    """Altegio prices are a different subsystem with different semantics."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            altegio = await _seed_record(
                session,
                provider=PROVIDER_ALTEGIO,
                booking=None,
                altegio_record_id=999001,
            )
    altegio_id = altegio.id

    report = await repair_prices(session_maker, apply=True)

    assert report.scanned == 0, "the query must not even see an Altegio record"
    assert await _snapshot(session_maker, altegio_id) == (CORRUPTED, [CORRUPTED])


async def test_a_hand_edited_snapshot_is_skipped(session_maker) -> None:
    """The stored value is not what the old formula produced, so its meaning is
    unknown and overwriting it would destroy someone's correction."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session, total_cost=Decimal("99.00"), service_costs=(Decimal("99.00"),))
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0
    assert report.skipped[SKIP_SIGNATURE_MISMATCH] == 1
    assert await _snapshot(session_maker, record_id) == (Decimal("99.00"), [Decimal("99.00")])


async def test_a_diverged_snapshot_is_skipped_rather_than_papered_over(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session, total_cost=CORRUPTED, service_costs=(Decimal("7.00"),))
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0
    assert report.skipped[SKIP_SNAPSHOT_INCONSISTENT] == 1
    assert await _snapshot(session_maker, record_id) == (CORRUPTED, [Decimal("7.00")])


@pytest.mark.parametrize(
    ("label", "service_costs"),
    [("two-services", (CORRUPTED, CORRUPTED)), ("no-service", ())],
)
async def test_an_ambiguous_service_set_is_skipped(session_maker, label: str, service_costs: tuple) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session, service_costs=service_costs)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0, label
    assert report.skipped[SKIP_NOT_ONE_SERVICE] == 1, label
    async with session_maker() as session:
        total = (await session.execute(select(Record.total_cost).where(Record.id == record_id))).scalar_one()
    assert total == CORRUPTED, label


async def test_a_truncated_capture_proves_nothing(session_maker) -> None:
    """The missing bytes are exactly where a contradicting price would be."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session, body_truncated=True)
            record = await _seed_record(session)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0
    assert report.skipped[SKIP_NO_USABLE_EVENT] == 1
    assert await _snapshot(session_maker, record_id) == (CORRUPTED, [CORRUPTED])


@pytest.mark.parametrize(
    ("label", "payload"),
    [
        ("conflicting-fields", _price_payload(minor_units="12000", projection="1.20")),
        ("localized-authority", _price_payload(minor_units="120,00")),
        ("no-authoritative-field", _price_payload(minor_units=None, projection="120.00")),
        ("no-legacy-field", _price_payload(major_units=None)),
        ("empty-payload", {}),
    ],
)
async def test_an_unusable_event_leaves_the_row_alone(session_maker, label: str, payload: dict) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session, payload=payload)
            record = await _seed_record(session)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0, label
    assert report.skipped[SKIP_NO_USABLE_EVENT] == 1, label
    assert await _snapshot(session_maker, record_id) == (CORRUPTED, [CORRUPTED]), label


async def test_a_record_without_a_booking_uuid_is_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            record = await _seed_record(session, booking=None)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0
    assert report.skipped[SKIP_NO_BOOKING_UUID] == 1
    assert await _snapshot(session_maker, record_id) == (CORRUPTED, [CORRUPTED])


async def test_identity_is_the_booking_uuid_and_not_the_company(session_maker) -> None:
    """Another booking's event must never price this one."""
    other = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000202")
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session, payload=_price_payload(booking=other), booking=other)
            record = await _seed_record(session)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 0
    assert report.skipped[SKIP_NO_USABLE_EVENT] == 1
    assert await _snapshot(session_maker, record_id) == (CORRUPTED, [CORRUPTED])


async def test_the_newest_proving_event_wins_deterministically(session_maker) -> None:
    """Capture is append-only, so "newest id" is a total order, not a guess."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session, payload=_price_payload(minor_units="3000", major_units=30, projection="30.00"))
            await _seed_event(session, minutes=10)
            # Newer, but proves nothing: it must not shadow the event above.
            await _seed_event(session, payload={}, minutes=20)
            record = await _seed_record(session)
    record_id = record.id

    report = await repair_prices(session_maker, apply=True)

    assert report.repaired == 1
    assert await _snapshot(session_maker, record_id) == (CORRECT, [CORRECT])


# ---------------------------------------------------------------------------
# Blast radius
# ---------------------------------------------------------------------------


async def test_the_repair_touches_no_job_no_outbox_and_no_event(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            event = await _seed_event(session)
            record = await _seed_record(session)
            job = MessageJob(
                provider=PROVIDER_EASYWEEK,
                company_id=TEST_LOCATION_ID,
                record_id=record.id,
                client_id=1,
                job_type="record_created",
                status="done",
                dedupe_key="eyw-price-repair-1",
                run_at=STARTS_AT,
            )
            session.add(job)
            await session.flush()
            session.add(
                OutboxMessage(
                    job_id=job.id,
                    company_id=TEST_LOCATION_ID,
                    phone_e164="+490000000000",
                    template_code="record_created",
                    body="fixture body",
                    status="sent",
                    scheduled_at=STARTS_AT,
                )
            )
    event_id, job_id = event.id, job.id

    async with session_maker() as session:
        before_event = (
            await session.execute(
                select(EasyWeekEvent.status, EasyWeekEvent.payload, EasyWeekEvent.error_code).where(
                    EasyWeekEvent.id == event_id
                )
            )
        ).one()
        before_jobs = (await session.execute(select(MessageJob.id, MessageJob.status))).all()
        before_outbox = (await session.execute(select(OutboxMessage.id, OutboxMessage.status))).all()

    report = await repair_prices(session_maker, apply=True)
    assert report.repaired == 1

    async with session_maker() as session:
        after_event = (
            await session.execute(
                select(EasyWeekEvent.status, EasyWeekEvent.payload, EasyWeekEvent.error_code).where(
                    EasyWeekEvent.id == event_id
                )
            )
        ).one()
        after_jobs = (await session.execute(select(MessageJob.id, MessageJob.status))).all()
        after_outbox = (await session.execute(select(OutboxMessage.id, OutboxMessage.status))).all()

    assert after_event == before_event, "the captured delivery is evidence, not state to rewrite"
    assert after_jobs == before_jobs == [(job_id, "done")], "no job may be created, reopened or retried"
    assert after_outbox == before_outbox, "nothing may be queued for delivery"


async def test_batching_covers_every_record_without_rescanning(session_maker) -> None:
    bookings = [uuid.UUID(f"aaaaaaaa-bbbb-4ccc-8ddd-0000000003{index:02d}") for index in range(5)]
    async with session_maker() as session:
        async with session.begin():
            for index, booking in enumerate(bookings):
                await _seed_event(session, payload=_price_payload(booking=booking), booking=booking)
                await _seed_record(session, booking=booking, altegio_record_id=5000000 + index)

    report = await repair_prices(session_maker, apply=True, batch_size=2)

    assert report.scanned == 5
    assert report.repaired == 5
    assert len(set(report.repaired_record_ids)) == 5


async def test_max_records_bounds_a_first_look(session_maker) -> None:
    bookings = [uuid.UUID(f"aaaaaaaa-bbbb-4ccc-8ddd-0000000004{index:02d}") for index in range(4)]
    async with session_maker() as session:
        async with session.begin():
            for index, booking in enumerate(bookings):
                await _seed_event(session, payload=_price_payload(booking=booking), booking=booking)
                await _seed_record(session, booking=booking, altegio_record_id=6000000 + index)

    report = await repair_prices(session_maker, batch_size=3, max_records=2)

    assert report.scanned == 2
    assert report.repairable == 2
    assert report.repaired == 0


# ---------------------------------------------------------------------------
# Output discipline
# ---------------------------------------------------------------------------


async def test_the_report_carries_ids_and_counts_but_never_an_amount(session_maker) -> None:
    """A price is customer data, and this output is pasted into tickets."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_event(session)
            await _seed_record(session)

    text = str((await repair_prices(session_maker, apply=True)).as_safe_dict())

    for leak in ("120.00", "1.20", "12000", "Wimpernverlängerung", "Tanja", "booking_price", str(BOOKING)):
        assert leak not in text, f"the report leaked {leak!r}"
    assert "repaired" in text and "skipped" in text


async def test_the_report_caps_the_id_lists_it_prints(session_maker) -> None:
    """A report is a spot-check aid, never a data export."""
    from altegio_bot.scripts.easyweek_price_repair import MAX_REPORTED_IDS, RepairReport

    report = RepairReport(applied=True)
    report.repaired_record_ids = list(range(MAX_REPORTED_IDS * 3))
    report.repairable_record_ids = list(range(MAX_REPORTED_IDS * 3))
    report.evidence_event_ids = list(range(MAX_REPORTED_IDS * 3))

    safe = report.as_safe_dict()
    assert len(safe["repaired_record_ids"]) == MAX_REPORTED_IDS
    assert len(safe["repairable_record_ids"]) == MAX_REPORTED_IDS
    assert len(safe["evidence_event_ids"]) == MAX_REPORTED_IDS


# ---------------------------------------------------------------------------
# The command line itself
# ---------------------------------------------------------------------------


def test_writing_requires_an_explicit_flag() -> None:
    assert _parse_args([]).apply is False
    assert _parse_args(["--apply"]).apply is True


@pytest.mark.parametrize("argv", [["--batch-size", "0"], ["--max-records", "0"]])
def test_nonsensical_bounds_are_refused(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        _parse_args(argv)
