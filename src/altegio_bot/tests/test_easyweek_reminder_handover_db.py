"""The reminder handover against a real PostgreSQL (plan §30.4–30.7).

The rules live in ``test_easyweek_reminder_handover.py``. What is here is
everything that only a database can prove: that a dry run writes nothing, that
the apply is one all-or-nothing transaction, that creation strictly precedes
cancellation, that a concurrent insert of the same reminder resolves to
"already there" rather than to a duplicate or an error, and that nothing outside
the frozen ledger scope is ever touched.

Every test runs on the project's real Postgres fixture. SQLite would not answer
any of these questions: no ``ON CONFLICT DO NOTHING``, no ``FOR UPDATE``, no
partial unique index on the EasyWeek booking uuid.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.reminder_handover import (
    CANCEL_REASON,
    HandoverPlan,
    freeze_plan,
    write_snapshot,
)
from altegio_bot.easyweek_migration.reminder_handover_db import (
    apply_plan,
    build_plan,
    verify_handover,
)
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES, REMINDER_2H, REMINDER_24H
from altegio_bot.easyweek_reminders import (
    REMINDER_OFFSETS,
    easyweek_reminder_dedupe_key,
    reminder_job_payload,
)
from altegio_bot.message_planner import make_dedupe_key
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekMigrationLedger,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.reminder_ownership import REASON_HANDED_OVER, REASON_UNKNOWN, ReminderOwner
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_migration_harness import (
    KA_LOCATION_ID,
    apply_production_flags,
    manifest_json,
)
from altegio_bot.tests.test_easyweek_migration_planning import KA_LOCATION_UUID

COMPANY = KARLSRUHE_COMPANY_ID
BOOKING = uuid_module.UUID("aaaaaaaa-0000-4000-8000-000000000001")
BOOKING_TWO = uuid_module.UUID("aaaaaaaa-0000-4000-8000-000000000002")
SOURCE_RECORD_ID = 900001

# An EasyWeek Record — and every EasyWeek MessageJob — carries the EasyWeek
# LOCATION id in `company_id`, not the Altegio company id. The manifest is what
# pairs the two, and the runtime registry is what proves the location is that
# branch, so the fixtures below use the harness's real registry ids.
LOCATION_ID = KA_LOCATION_ID


def wave_manifest() -> Any:
    """The manifest for this wave, re-pointed at the harness registry."""
    parsed = parse_manifest(manifest_json())
    assert parsed.valid, parsed.reason
    return parsed


@pytest.fixture(autouse=True)
def registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """The runtime branch registry the handover proves the manifest against."""
    apply_production_flags(monkeypatch)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Wimpernverlängerung"]')


def booking_body(
    starts_at: datetime,
    *,
    booking_uuid: uuid_module.UUID = BOOKING,
    canceled: bool = False,
    completed: bool = False,
) -> dict[str, Any]:
    return {
        "uuid": str(booking_uuid),
        "location_uuid": KA_LOCATION_UUID,
        "start_time": starts_at.isoformat().replace("+00:00", "Z"),
        "is_canceled": canceled,
        "is_completed": completed,
    }


class FakeBookings:
    """A live EasyWeek read, scripted. Counts calls so pacing can be asserted."""

    def __init__(self, answer: Any) -> None:
        self.answer = answer
        self.calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(booking_uuid)
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class FakeBookingMap:
    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(booking_uuid)
        answer = self.answers[booking_uuid]
        if isinstance(answer, Exception):
            raise answer
        return answer


def webhook_booking() -> SimpleNamespace:
    """The two fields ``sync_reminder_jobs`` actually reads off a booking.

    A full ``NormalizedBooking`` takes twenty-two constructor arguments, and
    building one here would make the test about the normaliser rather than about
    what these tests are for: that after the handover, the ordinary webhook path
    is the thing that owns rescheduling. The planner reads the appointment's
    time and deletion state off the RECORD, which is a real row.
    """
    return SimpleNamespace(booking_uuid=BOOKING, company_id=LOCATION_ID)


async def _no_sleep(_seconds: float) -> None:
    """Tests must not spend wall-clock time on the rate limiter."""


@pytest_asyncio.fixture
async def seeded(session_maker: async_sessionmaker[AsyncSession]):
    """One migrated booking: an Altegio source, an EasyWeek target, a ledger row.

    Returns a helper that can be re-run to read state back.
    """
    starts = datetime.now(timezone.utc) + timedelta(hours=48)
    async with session_maker() as session:
        async with session.begin():
            client = await session.get(Client, 1)
            client.provider = PROVIDER_EASYWEEK
            client.company_id = LOCATION_ID
            source = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=COMPANY,
                altegio_record_id=SOURCE_RECORD_ID,
                staff_id=5001,
                starts_at=starts,
                is_deleted=False,
            )
            target = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=LOCATION_ID,
                altegio_record_id=1811630,
                easyweek_booking_uuid=BOOKING,
                starts_at=starts,
                is_deleted=False,
                client_id=1,
                raw={"easyweek": {"service_category": "Wimpernverlängerung", "services_count": 1}},
            )
            session.add_all([source, target])
            await session.flush()
            session.add(
                EasyWeekMigrationLedger(
                    source_provider=PROVIDER_ALTEGIO,
                    source_company_id=COMPANY,
                    source_record_id=SOURCE_RECORD_ID,
                    source_fingerprint="f" * 64,
                    target_provider=PROVIDER_EASYWEEK,
                    target_booking_uuid=str(BOOKING),
                    run_id="run-1",
                    status=ledger_module.STATUS_CREATED,
                )
            )
            source_pk, target_pk = source.id, target.id
    return {"starts": starts, "source_pk": source_pk, "target_pk": target_pk}


async def add_job(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    provider: str,
    record_pk: int,
    job_type: str,
    status: str,
    dedupe_key: str,
    run_at: datetime | None = None,
    company_id: int | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    # An EasyWeek job belongs to the EasyWeek location; an Altegio job to the
    # Altegio company. Getting this wrong is what the guard's company check
    # catches, so the fixture defaults it correctly rather than hiding it.
    if company_id is None:
        company_id = LOCATION_ID if provider == PROVIDER_EASYWEEK else COMPANY
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, record_pk)
            if record is None:
                raise AssertionError("fixture record is missing")
            if provider == PROVIDER_EASYWEEK and job_type in REMINDER_OFFSETS:
                starts_at = record.starts_at
                booking_uuid = record.easyweek_booking_uuid
                if starts_at is None or booking_uuid is None:
                    raise AssertionError("EasyWeek fixture record has no reminder identity")
                if run_at is None:
                    run_at = starts_at - REMINDER_OFFSETS[job_type]
                if payload is None:
                    payload = reminder_job_payload(
                        booking_uuid=booking_uuid,
                        company_id=company_id,
                        starts_at=starts_at,
                        job_type=job_type,
                    )
            job = MessageJob(
                provider=provider,
                company_id=company_id,
                record_id=record_pk,
                client_id=record.client_id,
                job_type=job_type,
                run_at=run_at or (datetime.now(timezone.utc) + timedelta(hours=24)),
                status=status,
                dedupe_key=dedupe_key,
                payload=payload or {},
            )
            session.add(job)
            await session.flush()
            return job.id


async def add_migrated_pair(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    source_record_id: int,
    booking_uuid: uuid_module.UUID,
    starts_at: datetime,
) -> tuple[int, int]:
    async with session_maker() as session:
        async with session.begin():
            source = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=COMPANY,
                altegio_record_id=source_record_id,
                staff_id=5001,
                starts_at=starts_at,
                is_deleted=False,
            )
            target = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=LOCATION_ID,
                altegio_record_id=source_record_id + 100000,
                easyweek_booking_uuid=booking_uuid,
                starts_at=starts_at,
                is_deleted=False,
                client_id=1,
                raw={"easyweek": {"service_category": "Wimpernverlängerung", "services_count": 1}},
            )
            session.add_all([source, target])
            await session.flush()
            session.add(
                EasyWeekMigrationLedger(
                    source_provider=PROVIDER_ALTEGIO,
                    source_company_id=COMPANY,
                    source_record_id=source_record_id,
                    source_fingerprint="e" * 64,
                    target_provider=PROVIDER_EASYWEEK,
                    target_booking_uuid=str(booking_uuid),
                    run_id="run-2",
                    status=ledger_module.STATUS_CREATED,
                )
            )
            return source.id, target.id


async def jobs(session_maker: async_sessionmaker[AsyncSession]) -> list[MessageJob]:
    async with session_maker() as session:
        return list((await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars().all())


async def add_outbox(session_maker: async_sessionmaker[AsyncSession], *, record_pk: int, company_id: int) -> int:
    async with session_maker() as session:
        async with session.begin():
            row = OutboxMessage(
                company_id=company_id,
                record_id=record_pk,
                phone_e164="+490000000000",
                template_code="test_only",
                language="de",
                body="test",
                status="queued",
                scheduled_at=datetime.now(timezone.utc),
                meta={},
            )
            session.add(row)
            await session.flush()
            return row.id


async def plan_for(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    answer: Any = None,
    starts: datetime | None = None,
) -> HandoverPlan:
    client = FakeBookings(answer if answer is not None else booking_body(starts))
    async with session_maker() as session:
        return await build_plan(
            session,
            manifest=wave_manifest(),
            company_ids=(COMPANY,),
            run_ids=("run-1", "run-2"),
            client=client,
            sleep=_no_sleep,
        )


async def run_apply(
    session_maker: async_sessionmaker[AsyncSession],
    plan: HandoverPlan,
    *,
    now: datetime | None = None,
):
    frozen = freeze_plan(plan)
    async with session_maker() as session:
        async with session.begin():
            return await apply_plan(session, frozen, now=now)


# ---------------------------------------------------------------------------
# plan writes nothing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_dry_run_mutates_nothing(session_maker, seeded) -> None:
    before = await jobs(session_maker)
    plan = await plan_for(session_maker, starts=seeded["starts"])

    assert plan.to_create == 2
    assert await jobs(session_maker) == before == []
    async with session_maker() as session:
        assert (await session.execute(select(OutboxMessage))).scalars().all() == []
        ledger = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
        assert ledger.status == ledger_module.STATUS_CREATED


@pytest.mark.asyncio
async def test_a_dry_run_proves_the_target_with_one_live_read(session_maker, seeded) -> None:
    client = FakeBookings(booking_body(seeded["starts"]))
    async with session_maker() as session:
        await build_plan(
            session,
            manifest=wave_manifest(),
            company_ids=(COMPANY,),
            run_ids=("run-1",),
            client=client,
            sleep=_no_sleep,
        )

    assert client.calls == [str(BOOKING)]


# ---------------------------------------------------------------------------
# scope refusals
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        pytest.param(
            lambda led, src, tgt: setattr(led, "status", ledger_module.STATUS_UNCERTAIN),
            "ledger_not_created",
            id="ledger_not_created",
        ),
        pytest.param(
            lambda led, src, tgt: setattr(led, "target_booking_uuid", "NOT-A-UUID"),
            "target_uuid_invalid",
            id="target_uuid_invalid",
        ),
        pytest.param(
            lambda led, src, tgt: setattr(led, "source_record_id", 424242),
            "source_record_missing",
            id="source_record_missing",
        ),
        pytest.param(
            lambda led, src, tgt: setattr(tgt, "easyweek_booking_uuid", None),
            "target_record_missing",
            id="target_record_missing",
        ),
        pytest.param(
            lambda led, src, tgt: setattr(tgt, "company_id", 315607),
            "company_mismatch",
            id="target_belongs_to_another_branch",
        ),
    ],
)
async def test_a_row_that_cannot_be_proven_never_enters_the_wave(
    mutate: Any, reason: str, session_maker, seeded
) -> None:
    async with session_maker() as session:
        async with session.begin():
            led = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
            src = (await session.execute(select(Record).where(Record.id == seeded["source_pk"]))).scalars().one()
            tgt = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            mutate(led, src, tgt)

    plan = await plan_for(session_maker, starts=seeded["starts"])

    assert plan.scoped == ()
    if reason == "ledger_not_created":
        assert plan.historical_rows == {ledger_module.STATUS_UNCERTAIN: 1}
        assert plan.refused == {}
    else:
        assert reason in plan.refused


@pytest.mark.asyncio
async def test_an_unproven_branch_refuses_the_whole_run(session_maker, seeded, monkeypatch) -> None:
    """Not a per-row refusal: a wave against an unproven branch never starts."""
    from altegio_bot.easyweek_migration.reminder_handover_db import HandoverError

    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)

    with pytest.raises(HandoverError):
        await plan_for(session_maker, starts=seeded["starts"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    [
        pytest.param({"uuid": str(BOOKING), "location_uuid": KA_LOCATION_UUID}, id="malformed"),
        pytest.param({"uuid": "bbbbbbbb-0000-4000-8000-000000000002"}, id="other_booking"),
    ],
)
async def test_a_target_the_api_cannot_prove_keeps_the_row_out(answer: Any, session_maker, seeded) -> None:
    plan = await plan_for(session_maker, answer=answer)

    assert plan.scoped == ()
    assert "target_unproven" in plan.refused


@pytest.mark.asyncio
async def test_an_api_failure_keeps_the_row_out(session_maker, seeded) -> None:
    from altegio_bot.easyweek_client import EasyWeekRetryableError

    plan = await plan_for(session_maker, answer=EasyWeekRetryableError("down", operation="get_booking"))

    assert plan.scoped == ()
    assert "target_unproven" in plan.refused


@pytest.mark.asyncio
async def test_one_failed_live_proof_blocks_the_entire_two_row_scope(session_maker, seeded) -> None:
    from altegio_bot.easyweek_client import EasyWeekRetryableError

    await add_migrated_pair(
        session_maker,
        source_record_id=900002,
        booking_uuid=BOOKING_TWO,
        starts_at=seeded["starts"],
    )
    client = FakeBookingMap(
        {
            str(BOOKING): booking_body(seeded["starts"]),
            str(BOOKING_TWO): EasyWeekRetryableError("down", operation="get_booking"),
        }
    )
    async with session_maker() as session:
        plan = await build_plan(
            session,
            manifest=wave_manifest(),
            company_ids=(COMPANY,),
            run_ids=("run-1", "run-2"),
            client=client,
            sleep=_no_sleep,
        )

    result = await run_apply(session_maker, plan)

    assert len(plan.scoped) == 1
    assert len(plan.eligible_refusals) == 1
    assert plan.eligible_created_rows == 2
    assert plan.cutover_ready is False
    assert result.halted == "snapshot_incomplete_scope"
    assert await jobs(session_maker) == []


@pytest.mark.asyncio
async def test_a_local_record_disagreeing_with_the_api_keeps_the_row_out(session_maker, seeded) -> None:
    """Planning from either side would be planning from a guess."""
    moved = seeded["starts"] + timedelta(hours=2)
    plan = await plan_for(session_maker, answer=booking_body(moved))

    assert plan.scoped == ()
    assert "local_target_mismatch" in plan.refused
    assert plan.cutover_ready is False
    assert (await run_apply(session_maker, plan)).halted == "snapshot_incomplete_scope"


@pytest.mark.asyncio
async def test_zero_created_rows_is_information_not_cutover_permission(session_maker, seeded) -> None:
    async with session_maker() as session:
        async with session.begin():
            ledger = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
            ledger.status = ledger_module.STATUS_FAILED

    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert plan.eligible_created_rows == 0
    assert plan.historical_rows == {ledger_module.STATUS_FAILED: 1}
    assert plan.cutover_ready is False
    assert result.halted == "snapshot_incomplete_scope"


@pytest.mark.asyncio
async def test_a_cancelled_target_owes_nothing_but_stays_in_scope(session_maker, seeded) -> None:
    """Its stale Altegio reminders still have to be withdrawn."""
    async with session_maker() as session:
        async with session.begin():
            tgt = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            tgt.is_deleted = True

    plan = await plan_for(session_maker, answer=booking_body(seeded["starts"], canceled=True))

    assert len(plan.scoped) == 1
    assert plan.to_create == 0


# ---------------------------------------------------------------------------
# the reminder windows, end to end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("hours", "expected"),
    [(48, [REMINDER_24H, REMINDER_2H]), (6, [REMINDER_2H]), (1, [])],
    ids=["more_than_a_day", "inside_the_day", "about_to_start"],
)
async def test_the_window_decides_what_is_created(hours: int, expected: list[str], session_maker, seeded) -> None:
    starts = datetime.now(timezone.utc) + timedelta(hours=hours)
    async with session_maker() as session:
        async with session.begin():
            for record in (await session.execute(select(Record))).scalars().all():
                record.starts_at = starts

    plan = await plan_for(session_maker, answer=booking_body(starts))
    await run_apply(session_maker, plan)

    created = [job.job_type for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK]
    assert sorted(created) == sorted(expected)


# ---------------------------------------------------------------------------
# apply: create first, then cancel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_creates_the_missing_reminders_and_withdraws_the_old_ones(session_maker, seeded) -> None:
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert len(result.created_job_ids) == 2
    assert result.canceled_job_ids == (stale,)

    rows = {job.id: job for job in await jobs(session_maker)}
    assert rows[stale].status == "canceled"
    assert rows[stale].last_error == CANCEL_REASON
    assert rows[stale].locked_at is None
    created = [job for job in rows.values() if job.provider == PROVIDER_EASYWEEK]
    assert {job.record_id for job in created} == {seeded["target_pk"]}
    assert all(job.status == "queued" for job in created)


@pytest.mark.asyncio
async def test_an_existing_queued_reminder_is_not_duplicated(session_maker, seeded) -> None:
    existing = await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=seeded["target_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING, job_type=REMINDER_24H, starts_at=seeded["starts"]
        ),
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert len(result.created_job_ids) == 1, "only the 2h one was missing"
    easyweek = [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK]
    assert len(easyweek) == 2
    assert existing in {job.id for job in easyweek}


@pytest.mark.asyncio
async def test_a_done_reminder_is_never_re_opened(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=seeded["target_pk"],
        job_type=REMINDER_24H,
        status="done",
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING, job_type=REMINDER_24H, starts_at=seeded["starts"]
        ),
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    statuses = {job.job_type: job.status for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK}
    assert statuses[REMINDER_24H] == "done", "it already went out"
    assert statuses[REMINDER_2H] == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["canceled", "failed", "future-status"])
async def test_a_cancelled_or_failed_key_blocks_the_wave(status: str, session_maker, seeded) -> None:
    """Re-opening it is an operator decision, never the tool's."""
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=seeded["target_pk"],
        job_type=REMINDER_24H,
        status=status,
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING, job_type=REMINDER_24H, starts_at=seeded["starts"]
        ),
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert plan.guard_ready is False
    assert plan.cutover_ready is False
    assert result.halted == "snapshot_obligation_blocked"
    current = {job.id: job for job in await jobs(session_maker)}
    assert len(current) == 2
    assert current[stale].status == "queued"


@pytest.mark.asyncio
async def test_one_blocker_preserves_every_row_in_a_two_booking_batch(session_maker, seeded) -> None:
    source_two, target_two = await add_migrated_pair(
        session_maker,
        source_record_id=900002,
        booking_uuid=BOOKING_TWO,
        starts_at=seeded["starts"],
    )
    stale_one = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:first:stale",
    )
    stale_two = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=source_two,
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:second:stale",
    )
    blocker = await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=target_two,
        job_type=REMINDER_24H,
        status="canceled",
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING_TWO,
            job_type=REMINDER_24H,
            starts_at=seeded["starts"],
        ),
    )
    client = FakeBookingMap(
        {
            str(BOOKING): booking_body(seeded["starts"]),
            str(BOOKING_TWO): booking_body(seeded["starts"], booking_uuid=BOOKING_TWO),
        }
    )
    async with session_maker() as session:
        plan = await build_plan(
            session,
            manifest=wave_manifest(),
            company_ids=(COMPANY,),
            run_ids=("run-1", "run-2"),
            client=client,
            sleep=_no_sleep,
        )

    result = await run_apply(session_maker, plan)

    assert result.halted == "snapshot_obligation_blocked"
    current = {job.id: job for job in await jobs(session_maker)}
    assert current[stale_one].status == current[stale_two].status == "queued"
    assert set(current) == {stale_one, stale_two, blocker}


@pytest.mark.asyncio
async def test_a_processing_source_job_stops_the_whole_apply(session_maker, seeded) -> None:
    """The worker holds it and may already have reached Meta."""
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_2H,
        status="processing",
        dedupe_key="altegio:reminder_2h:claimed",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert result.halted == "source_reminder_processing"
    assert [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK] == []


@pytest.mark.asyncio
async def test_creation_precedes_cancellation(session_maker, seeded) -> None:
    """If creation fails, the customer keeps the reminder they already had."""
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])

    frozen = freeze_plan(plan)
    async with session_maker() as session:
        try:
            async with session.begin():
                await apply_plan(session, frozen)
                raise RuntimeError("something failed after the inserts")
        except RuntimeError:
            pass

    rows = {job.id: job for job in await jobs(session_maker)}
    assert rows[stale].status == "queued", "the old reminder is intact"
    assert [job for job in rows.values() if job.provider == PROVIDER_EASYWEEK] == []


@pytest.mark.asyncio
async def test_an_exception_between_insert_and_cancel_rolls_everything_back(session_maker, seeded) -> None:
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_2H,
        status="queued",
        dedupe_key="altegio:reminder_2h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)

    async with session_maker() as session:
        with pytest.raises(RuntimeError):
            async with session.begin():
                await apply_plan(session, frozen)
                raise RuntimeError("boom")

    rows = {job.id: job for job in await jobs(session_maker)}
    assert len(rows) == 1
    assert rows[stale].status == "queued"


@pytest.mark.asyncio
async def test_a_second_apply_changes_nothing(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    first = await run_apply(session_maker, plan)
    before = {job.id: (job.status, job.dedupe_key) for job in await jobs(session_maker)}

    second = await run_apply(session_maker, plan)

    assert second.created_job_ids == ()
    assert second.canceled_job_ids == (), "the old job is already canceled"
    assert second.already_present == len(first.created_job_ids)
    assert {job.id: (job.status, job.dedupe_key) for job in await jobs(session_maker)} == before


@pytest.mark.asyncio
async def test_a_concurrent_insert_of_the_same_reminder_is_idempotent(session_maker, seeded) -> None:
    """The inbox worker planning the same fact must not become a duplicate."""
    plan = await plan_for(session_maker, starts=seeded["starts"])

    # The webhook path gets there first, with the identical canonical key.
    await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=seeded["target_pk"],
        job_type=REMINDER_2H,
        status="queued",
        dedupe_key=easyweek_reminder_dedupe_key(booking_uuid=BOOKING, job_type=REMINDER_2H, starts_at=seeded["starts"]),
    )
    result = await run_apply(session_maker, plan)

    assert result.halted is None
    assert result.already_present == 1
    keys = [job.dedupe_key for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK]
    assert len(keys) == len(set(keys)) == 2


# ---------------------------------------------------------------------------
# apply refuses a world that moved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_target_that_moved_after_the_plan_blocks_the_apply(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    async with session_maker() as session:
        async with session.begin():
            tgt = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            tgt.starts_at = seeded["starts"] + timedelta(hours=1)

    result = await run_apply(session_maker, plan)

    assert result.halted == "local_target_mismatch"
    assert [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK] == []


@pytest.mark.asyncio
async def test_a_target_cancelled_after_the_plan_blocks_the_apply(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    async with session_maker() as session:
        async with session.begin():
            tgt = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            tgt.is_deleted = True

    assert (await run_apply(session_maker, plan)).halted == "local_target_mismatch"


@pytest.mark.asyncio
async def test_a_ledger_row_that_moved_after_the_plan_blocks_the_apply(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    async with session_maker() as session:
        async with session.begin():
            led = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
            led.status = ledger_module.STATUS_ROLLED_BACK

    assert (await run_apply(session_maker, plan)).halted == "eligible_scope_changed"


@pytest.mark.asyncio
async def test_a_reminder_that_crossed_its_boundary_blocks_the_apply(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan, now=seeded["starts"] + timedelta(hours=1))

    assert result.halted == "reminder_boundary_passed"
    assert [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK] == []


# ---------------------------------------------------------------------------
# nothing outside the frozen scope is touched
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_jobs_outside_the_scope_are_left_alone(session_maker, seeded) -> None:
    async with session_maker() as session:
        async with session.begin():
            other = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=1271200,
                altegio_record_id=700001,
                starts_at=seeded["starts"],
            )
            session.add(other)
            await session.flush()
            other_pk = other.id

    untouched = [
        await add_job(
            session_maker,
            provider=PROVIDER_ALTEGIO,
            record_pk=other_pk,
            job_type=REMINDER_24H,
            status="queued",
            dedupe_key="altegio:other-company",
            company_id=1271200,
        ),
        await add_job(
            session_maker,
            provider=PROVIDER_ALTEGIO,
            record_pk=seeded["source_pk"],
            job_type="review_3d",
            status="queued",
            dedupe_key="altegio:review",
        ),
        await add_job(
            session_maker,
            provider=PROVIDER_ALTEGIO,
            record_pk=seeded["source_pk"],
            job_type=REMINDER_24H,
            status="done",
            dedupe_key="altegio:already-sent",
        ),
        await add_job(
            session_maker,
            provider=PROVIDER_ALTEGIO,
            record_pk=seeded["source_pk"],
            job_type=REMINDER_2H,
            status="failed",
            dedupe_key="altegio:already-failed",
        ),
    ]
    plan = await plan_for(session_maker, starts=seeded["starts"])
    before = {job.id: job.status for job in await jobs(session_maker)}

    await run_apply(session_maker, plan)
    after = {job.id: job.status for job in await jobs(session_maker)}

    for job_id in untouched:
        assert after[job_id] == before[job_id], job_id


@pytest.mark.asyncio
async def test_the_tool_never_writes_an_outbox_message(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    async with session_maker() as session:
        assert (await session.execute(select(OutboxMessage))).scalars().all() == []


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verify_passes_after_a_clean_apply(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is True
    assert report["open_altegio_reminders"] == []
    assert report["unmet_obligations"] == 0


@pytest.mark.asyncio
async def test_verify_rejects_an_apply_report_for_another_snapshot(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = replace(
        result.apply_report(frozen, applied_at=datetime.now(timezone.utc)),
        snapshot_digest="0" * 64,
    )

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is False
    assert report["snapshot_digest_matches_apply_report"] is False


@pytest.mark.asyncio
async def test_verify_rejects_apply_counts_that_do_not_account_for_the_snapshot(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = replace(
        result.apply_report(frozen, applied_at=datetime.now(timezone.utc)),
        already_present_count=1,
    )

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is False
    assert report["apply_counts_match"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["created_identity", "created_canceled", "canceled_source_outcome"])
async def test_verify_rejects_changed_jobs_named_by_the_apply_report(change: str, session_maker, seeded) -> None:
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    async with session_maker() as session:
        async with session.begin():
            if change == "canceled_source_outcome":
                job = await session.get(MessageJob, stale)
                job.status = "done"
            else:
                job = await session.get(MessageJob, result.created_job_ids[0])
                if change == "created_identity":
                    job.company_id = 315607
                else:
                    job.status = "canceled"

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is False
    if change == "canceled_source_outcome":
        assert report["canceled_job_ids_invalid"] == [stale]
    else:
        assert result.created_job_ids[0] in report["created_job_ids_invalid"]


@pytest.mark.asyncio
async def test_verify_rejects_a_new_scoped_outbox_row_after_apply(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    scoped = await add_outbox(session_maker, record_pk=seeded["target_pk"], company_id=LOCATION_ID)

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is False
    assert report["scoped_outbox_ids_current"] == [scoped]
    assert report["messages_sent_by_handover"] is None


@pytest.mark.asyncio
async def test_verify_ignores_an_outbox_row_for_an_unrelated_record(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    async with session_maker() as session:
        async with session.begin():
            other = Record(provider=PROVIDER_ALTEGIO, company_id=1271200, altegio_record_id=700002)
            session.add(other)
            await session.flush()
            other_pk = other.id
    await add_outbox(session_maker, record_pk=other_pk, company_id=1271200)

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is True
    assert report["scoped_outbox_ids_current"] == []


@pytest.mark.asyncio
async def test_verify_fails_while_an_old_altegio_reminder_is_still_open(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    leftover = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_2H,
        status="queued",
        dedupe_key="altegio:missed-one",
    )

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is False
    assert report["open_altegio_reminders"] == [leftover]


@pytest.mark.asyncio
async def test_verify_flags_an_easyweek_reminder_for_the_wrong_instant(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    applied = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    stray = await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=seeded["target_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING, job_type=REMINDER_24H, starts_at=seeded["starts"] + timedelta(days=3)
        ),
    )

    async with session_maker() as session:
        report = await verify_handover(session, frozen, applied)

    assert report["passed"] is False
    assert report["stray_easyweek_reminders"] == [stray]


@pytest.mark.asyncio
async def test_a_second_plan_after_apply_reports_nothing_missing(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    again = await plan_for(session_maker, starts=seeded["starts"])

    assert again.to_create == 0
    assert again.coverage_ready is True


@pytest.mark.asyncio
async def test_the_snapshot_of_a_real_plan_is_pii_free(session_maker, seeded, tmp_path) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    path = write_snapshot(plan, tmp_path / "plan.json")
    blob = path.read_text()

    for leaked in ("phone", "email", "first_name", "last_name", "Bearer", "Workspace"):
        assert leaked not in blob, leaked


@pytest.mark.asyncio
async def test_a_key_held_by_another_record_stops_the_cancellation(session_maker, seeded) -> None:
    """An insert that silently did nothing must never license a withdrawal.

    The dedupe key is globally unique. If some other record already holds the
    key this booking's reminder needs, ``ON CONFLICT DO NOTHING`` skips the
    insert without complaining — and cancelling the old Altegio reminders on the
    strength of that would leave the customer covered by neither side.
    """
    async with session_maker() as session:
        async with session.begin():
            thief = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=LOCATION_ID,
                altegio_record_id=1999999,
                easyweek_booking_uuid=uuid_module.UUID("cccccccc-0000-4000-8000-000000000009"),
                starts_at=seeded["starts"],
            )
            session.add(thief)
            await session.flush()
            thief_pk = thief.id

    # Another record holds exactly the key this booking's 24h reminder needs.
    await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=thief_pk,
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING, job_type=REMINDER_24H, starts_at=seeded["starts"]
        ),
    )
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )

    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert plan.refused == {"obligation_identity_mismatch": 1}
    assert result.halted == "snapshot_incomplete_scope"
    rows = {job.id: job for job in await jobs(session_maker)}
    assert rows[stale].status == "queued", "the customer keeps the reminder they had"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conflict",
    ["company", "job_type", "run_at", "payload"],
)
async def test_a_concurrent_key_with_wrong_identity_rolls_back_the_whole_apply(
    conflict: str, session_maker, seeded
) -> None:
    """The dedupe key alone can never license withdrawal of the source job."""
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    expected_payload = reminder_job_payload(
        booking_uuid=BOOKING,
        company_id=LOCATION_ID,
        starts_at=seeded["starts"],
        job_type=REMINDER_24H,
    )
    kwargs: dict[str, Any] = {
        "provider": PROVIDER_EASYWEEK,
        "record_pk": seeded["target_pk"],
        "job_type": REMINDER_24H,
        "status": "queued",
        "dedupe_key": easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING,
            job_type=REMINDER_24H,
            starts_at=seeded["starts"],
        ),
        "payload": expected_payload,
    }
    if conflict == "company":
        kwargs["company_id"] = 315607
    elif conflict == "job_type":
        kwargs["job_type"] = REMINDER_2H
    elif conflict == "run_at":
        kwargs["run_at"] = seeded["starts"] - timedelta(hours=25)
    elif conflict == "payload":
        kwargs["payload"] = {**expected_payload, "booking_uuid": "bbbbbbbb-0000-4000-8000-000000000002"}
    conflict_id = await add_job(session_maker, **kwargs)

    result = await run_apply(session_maker, plan)

    assert result.halted == ("company_mismatch" if conflict == "company" else "obligation_identity_mismatch")
    current = {job.id: job for job in await jobs(session_maker)}
    assert current[stale].status == "queued"
    assert set(current) == {stale, conflict_id}, "the other missing obligation was rolled back too"


# ---------------------------------------------------------------------------
# After the cutover, the ordinary webhooks own rescheduling (plan §30.2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_reschedule_after_the_cutover_replaces_the_reminders(session_maker, seeded) -> None:
    """The handover leaves no synchroniser behind: `sync_reminder_jobs` takes over."""
    from altegio_bot.workers.easyweek_inbox_worker import sync_reminder_jobs

    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    before = {job.dedupe_key for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK}
    assert len(before) == 2

    moved = seeded["starts"] + timedelta(days=1)
    async with session_maker() as session:
        async with session.begin():
            target = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            target.starts_at = moved
            await sync_reminder_jobs(
                session,
                record=target,
                booking=webhook_booking(),
                client=None,
            )

    rows = [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK]
    old = [job for job in rows if job.dedupe_key in before]
    assert all(job.status == "canceled" for job in old), "the reminders for the old hour are withdrawn"


@pytest.mark.asyncio
async def test_a_cancel_after_the_cutover_withdraws_the_reminders(session_maker, seeded) -> None:
    from altegio_bot.workers.easyweek_inbox_worker import sync_reminder_jobs

    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    async with session_maker() as session:
        async with session.begin():
            target = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            target.is_deleted = True
            await sync_reminder_jobs(
                session,
                record=target,
                booking=webhook_booking(),
                client=None,
            )

    rows = [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK]
    assert rows and all(job.status == "canceled" for job in rows)


@pytest.mark.asyncio
async def test_the_existing_preflight_still_sees_what_the_handover_created(session_maker, seeded) -> None:
    """The created jobs pass the runtime guard the preflight runs. Unchanged."""
    from altegio_bot.scripts.easyweek_reminder_preflight import run_preflight

    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    client = FakeBookings(booking_body(seeded["starts"]))
    async with session_maker() as session:
        report = await run_preflight(session, client=client, sleep=_no_sleep)

    assert report.candidate_count == 2
    assert report.ready is True, report.as_safe_dict()


@pytest.mark.asyncio
async def test_neither_the_report_nor_the_logs_carry_personal_data(session_maker, seeded, caplog) -> None:
    import json as json_module

    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    with caplog.at_level("INFO"):
        plan = await plan_for(session_maker, starts=seeded["starts"])
        result = await run_apply(session_maker, plan)

    for blob in (json_module.dumps(plan.as_safe_dict()), json_module.dumps(result.as_safe_dict()), caplog.text):
        for leaked in ("phone", "email", "first_name", "Bearer", "Workspace", "+49"):
            assert leaked not in blob, leaked


# ---------------------------------------------------------------------------
# The durable ownership marker (plan §30.11)
# ---------------------------------------------------------------------------


async def ledger_marker(session_maker: async_sessionmaker[AsyncSession]) -> tuple[Any, Any]:
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
        return row.reminders_handed_over_at, row.reminder_handover_plan_digest


@pytest.mark.asyncio
async def test_a_clean_apply_stamps_the_marker_in_the_same_transaction(session_maker, seeded) -> None:
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert result.halted is None
    assert len(result.created_job_ids) == 2
    assert result.canceled_job_ids == (stale,)
    assert len(result.marked_ledger_ids) == 1

    handed_at, digest = await ledger_marker(session_maker)
    assert handed_at is not None
    assert digest == plan.digest()


@pytest.mark.asyncio
async def test_an_exception_after_the_marker_rolls_the_whole_wave_back(session_maker, seeded) -> None:
    """The marker, the created jobs and the cancellation are one fact."""
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])

    async with session_maker() as session:
        with pytest.raises(RuntimeError):
            async with session.begin():
                await apply_plan(session, freeze_plan(plan))
                raise RuntimeError("boom after the marker")

    assert await ledger_marker(session_maker) == (None, None)
    rows = {job.id: job for job in await jobs(session_maker)}
    assert rows[stale].status == "queued"
    assert [job for job in rows.values() if job.provider == PROVIDER_EASYWEEK] == []


@pytest.mark.asyncio
async def test_a_blocked_row_leaves_the_whole_batch_unmarked(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    # The target moves after the plan: the wave stops before anything is done.
    async with session_maker() as session:
        async with session.begin():
            target = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            target.starts_at = seeded["starts"] + timedelta(hours=1)

    result = await run_apply(session_maker, plan)

    assert result.halted is not None
    assert result.marked_ledger_ids == ()
    assert await ledger_marker(session_maker) == (None, None)


@pytest.mark.asyncio
async def test_a_processing_source_reminder_leaves_the_marker_unset(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_2H,
        status="processing",
        dedupe_key="altegio:reminder_2h:claimed",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])

    assert (await run_apply(session_maker, plan)).halted == "source_reminder_processing"
    assert await ledger_marker(session_maker) == (None, None)


@pytest.mark.asyncio
async def test_a_reminder_queued_after_the_plan_stops_the_apply(session_maker, seeded) -> None:
    """Altegio inbox keeps running during a handover, so this is a live race.

    Checking only the frozen ids would have missed it, and the new reminder
    would have survived the wave — open, on the Altegio side, for a booking
    whose reminders had just become EasyWeek's.
    """
    plan = await plan_for(session_maker, starts=seeded["starts"])
    latecomer = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:planned-after-the-snapshot",
    )

    result = await run_apply(session_maker, plan)

    assert result.halted == "source_reminder_scope_changed"
    assert result.marked_ledger_ids == ()
    assert await ledger_marker(session_maker) == (None, None)
    rows = {job.id: job for job in await jobs(session_maker)}
    assert rows[latecomer].status == "queued", "nothing was touched"
    assert [job for job in rows.values() if job.provider == PROVIDER_EASYWEEK] == []


@pytest.mark.asyncio
async def test_an_occupied_target_key_leaves_the_marker_unset(session_maker, seeded) -> None:
    async with session_maker() as session:
        async with session.begin():
            thief = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=LOCATION_ID,
                altegio_record_id=1999999,
                easyweek_booking_uuid=uuid_module.UUID("cccccccc-0000-4000-8000-000000000009"),
                starts_at=seeded["starts"],
            )
            session.add(thief)
            await session.flush()
            thief_pk = thief.id

    await add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=thief_pk,
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key=easyweek_reminder_dedupe_key(
            booking_uuid=BOOKING, job_type=REMINDER_24H, starts_at=seeded["starts"]
        ),
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    result = await run_apply(session_maker, plan)

    assert result.halted is not None
    assert await ledger_marker(session_maker) == (None, None)


@pytest.mark.asyncio
async def test_a_repeat_of_the_exact_snapshot_is_a_no_op(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    first_at, first_digest = await ledger_marker(session_maker)
    before = {job.id: (job.status, job.dedupe_key) for job in await jobs(session_maker)}

    second = await run_apply(session_maker, plan)

    assert second.halted is None
    assert second.created_job_ids == ()
    assert second.canceled_job_ids == ()
    assert second.marked_ledger_ids == (), "nothing new was marked"
    assert second.already_marked_ledger_ids != (), "and the existing marker was recognised"
    assert await ledger_marker(session_maker) == (first_at, first_digest), "the instant is never rewritten"
    assert {job.id: (job.status, job.dedupe_key) for job in await jobs(session_maker)} == before


@pytest.mark.asyncio
async def test_a_marker_from_another_plan_blocks_the_apply(session_maker, seeded) -> None:
    """Somebody else's reviewed decision is not ours to overwrite."""
    plan = await plan_for(session_maker, starts=seeded["starts"])
    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
            row.reminders_handed_over_at = datetime.now(timezone.utc)
            row.reminder_handover_plan_digest = "b" * 64

    result = await run_apply(session_maker, plan)

    assert result.halted == "reminder_marker_conflict"
    assert [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK] == []
    _at, digest = await ledger_marker(session_maker)
    assert digest == "b" * 64, "the other plan's marker is intact"


@pytest.mark.asyncio
async def test_the_database_refuses_half_an_ownership_marker(session_maker, seeded) -> None:
    """The two columns are one fact, enforced by PostgreSQL rather than by us.

    The runtime fence reads the instant while an apply compares the digest, so
    half a marker would let one of them answer while the other could not. The
    CHECK makes that state unreachable, which is why the `marker_incomplete`
    branch in `build_plan` is defence against a hand-written row and not a case
    the supported paths can produce.
    """
    for statement in (
        "UPDATE easyweek_migration_ledger SET reminders_handed_over_at = now()",
        "UPDATE easyweek_migration_ledger SET reminder_handover_plan_digest = repeat('a', 64)",
    ):
        async with session_maker() as session:
            with pytest.raises(IntegrityError):
                async with session.begin():
                    await session.execute(text(statement))

    assert await ledger_marker(session_maker) == (None, None)


@pytest.mark.asyncio
async def test_a_fresh_plan_after_the_handover_shows_ownership_has_moved(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    again = await plan_for(session_maker, starts=seeded["starts"])
    [row] = [item for item in again.scoped]

    assert row.marker_action == "already_handed_over"
    assert row.marker_existing_digest == plan.digest()
    assert again.to_create == 0
    assert again.coverage_ready is True


@pytest.mark.asyncio
async def test_verify_fails_when_the_marker_is_missing(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
            row.reminders_handed_over_at = None
            row.reminder_handover_plan_digest = None

    async with session_maker() as session:
        verdict = await verify_handover(session, frozen, report)

    assert verdict["passed"] is False
    assert verdict["ledger_rows_missing_marker"]


@pytest.mark.asyncio
async def test_verify_fails_when_the_marker_belongs_to_another_plan(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
            row.reminder_handover_plan_digest = "c" * 64

    async with session_maker() as session:
        verdict = await verify_handover(session, frozen, report)

    assert verdict["passed"] is False
    assert verdict["ledger_rows_with_foreign_marker"]


@pytest.mark.asyncio
async def test_verify_fails_when_the_apply_report_marker_ids_were_edited(session_maker, seeded) -> None:
    from dataclasses import replace as dc_replace

    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    edited = dc_replace(report, marked_ledger_ids=(*report.marked_ledger_ids, 99999))

    async with session_maker() as session:
        verdict = await verify_handover(session, frozen, edited)

    assert verdict["passed"] is False
    assert verdict["marker_matches_apply_report"] is False


@pytest.mark.asyncio
async def test_verify_passes_after_a_clean_marked_apply(session_maker, seeded) -> None:
    await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:stale",
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = freeze_plan(plan)
    result = await run_apply(session_maker, plan)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        verdict = await verify_handover(session, frozen, report)

    assert verdict["passed"] is True
    assert verdict["ledger_rows_marked"] == 1
    assert verdict["marker_matches_apply_report"] is True


# ---------------------------------------------------------------------------
# Late Altegio deliveries after the handover (plan §30.11)
# ---------------------------------------------------------------------------


async def plan_altegio_event(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    record_pk: int,
    status: str = "create",
    starts_at: datetime | None = None,
) -> None:
    """One ordinary Altegio delivery, through the real planner."""
    from altegio_bot.message_planner import plan_jobs_for_record_event

    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record).where(Record.id == record_pk))).scalars().one()
            if starts_at is not None:
                record.starts_at = starts_at
            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record=record,
                event_status=status,
            )


async def altegio_reminders(session_maker: async_sessionmaker[AsyncSession], record_pk: int) -> list[MessageJob]:
    return [
        job
        for job in await jobs(session_maker)
        if job.provider == PROVIDER_ALTEGIO
        and job.record_id == record_pk
        and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
    ]


@pytest.mark.asyncio
async def test_a_late_create_does_not_reopen_a_handed_over_reminder(session_maker, seeded) -> None:
    """The exact defect: `add_job` re-queues a cancelled job on conflict."""
    stale = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key=make_dedupe_key(
            job_type=REMINDER_24H,
            company_id=COMPANY,
            record_id=seeded["source_pk"],
            run_at=seeded["starts"] - timedelta(hours=24),
        ),
        run_at=seeded["starts"] - timedelta(hours=24),
    )
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    assert {job.id: job.status for job in await altegio_reminders(session_maker, seeded["source_pk"])} == {
        stale: "canceled"
    }

    # The delayed webhook lands. Same booking, same start, same dedupe key.
    await plan_altegio_event(session_maker, record_pk=seeded["source_pk"])

    rows = {job.id: job.status for job in await altegio_reminders(session_maker, seeded["source_pk"])}
    assert rows == {stale: "canceled"}, "the withdrawn reminder stayed withdrawn"


@pytest.mark.asyncio
async def test_a_late_update_with_an_unchanged_start_creates_no_reminder(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    await plan_altegio_event(session_maker, record_pk=seeded["source_pk"], status="update")

    assert await altegio_reminders(session_maker, seeded["source_pk"]) == []


@pytest.mark.asyncio
async def test_a_late_reschedule_creates_no_reminder_under_a_new_key(session_maker, seeded) -> None:
    """A moved appointment yields a NEW dedupe key, which no cancellation covers."""
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    await plan_altegio_event(
        session_maker,
        record_pk=seeded["source_pk"],
        status="update",
        starts_at=seeded["starts"] + timedelta(days=2),
    )

    assert await altegio_reminders(session_maker, seeded["source_pk"]) == []


@pytest.mark.asyncio
async def test_a_delivery_blocked_behind_the_apply_sees_the_marker_afterwards(session_maker, seeded) -> None:
    """The post-commit half of the race, in the order production produces it."""
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    # Only now is the waiting delivery unblocked, and it reads the committed row.
    await plan_altegio_event(session_maker, record_pk=seeded["source_pk"], status="create")

    assert await altegio_reminders(session_maker, seeded["source_pk"]) == []


@pytest.mark.asyncio
async def test_a_migrated_record_still_gets_its_other_jobs(session_maker, seeded) -> None:
    """The fence covers reminders. Nothing else on the path is suppressed."""
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    await plan_altegio_event(session_maker, record_pk=seeded["source_pk"], status="create")

    other = [
        job.job_type
        for job in await jobs(session_maker)
        if job.provider == PROVIDER_ALTEGIO
        and job.record_id == seeded["source_pk"]
        and job.job_type not in EASYWEEK_REMINDER_JOB_TYPES
    ]
    assert other, "the record_* job is still planned"


@pytest.mark.asyncio
async def test_an_unmigrated_record_keeps_planning_reminders(session_maker, seeded) -> None:
    """No ledger row means nothing moved, and the ordinary path is untouched."""
    async with session_maker() as session:
        async with session.begin():
            other = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=COMPANY,
                altegio_record_id=900555,
                starts_at=seeded["starts"],
                is_deleted=False,
            )
            session.add(other)
            await session.flush()
            other_pk = other.id

    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    await plan_altegio_event(session_maker, record_pk=other_pk, status="create")

    planned = {job.job_type for job in await altegio_reminders(session_maker, other_pk)}
    assert planned == {REMINDER_24H, REMINDER_2H}


@pytest.mark.asyncio
async def test_a_record_in_another_company_is_not_suppressed(session_maker, seeded) -> None:
    async with session_maker() as session:
        async with session.begin():
            other = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=1271200,
                # The SAME source record id, in a different branch. Suppressing
                # this one would mean the fence keys on the id alone.
                altegio_record_id=SOURCE_RECORD_ID,
                starts_at=seeded["starts"],
                is_deleted=False,
            )
            session.add(other)
            await session.flush()
            other_pk = other.id

    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    await plan_altegio_event(session_maker, record_pk=other_pk, status="create")

    assert {job.job_type for job in await altegio_reminders(session_maker, other_pk)} == {
        REMINDER_24H,
        REMINDER_2H,
    }


@pytest.mark.asyncio
async def test_an_easyweek_reschedule_still_replaces_its_own_reminders(session_maker, seeded) -> None:
    """The fence is Altegio-only: the EasyWeek planner keeps working."""
    from altegio_bot.workers.easyweek_inbox_worker import sync_reminder_jobs

    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    before = {job.dedupe_key for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK}

    moved = seeded["starts"] + timedelta(days=1)
    async with session_maker() as session:
        async with session.begin():
            target = (await session.execute(select(Record).where(Record.id == seeded["target_pk"]))).scalars().one()
            target.starts_at = moved
            await sync_reminder_jobs(session, record=target, booking=webhook_booking(), client=None)

    rows = [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK]
    assert all(job.status == "canceled" for job in rows if job.dedupe_key in before)


# ---------------------------------------------------------------------------
# The send-time guard, the second line of defence (plan §30.11)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def no_meta(monkeypatch: pytest.MonkeyPatch):
    """Meta and Chatwoot, wired to fail loudly if the guard ever lets one through."""
    from unittest.mock import AsyncMock

    from altegio_bot.workers import outbox_worker as worker

    meta = AsyncMock(side_effect=AssertionError("Meta must not be called"))
    monkeypatch.setattr(worker, "send_template_message", meta, raising=False)
    return meta


async def process(session_maker: async_sessionmaker[AsyncSession], job_id: int) -> MessageJob:
    """Run one job through the real outbox path and read the row back."""
    from altegio_bot.providers.dummy import DummyProvider
    from altegio_bot.workers.outbox_worker import process_job_in_session

    async with session_maker() as session:
        async with session.begin():
            await process_job_in_session(session=session, job_id=job_id, provider=DummyProvider())
    async with session_maker() as session:
        return (await session.execute(select(MessageJob).where(MessageJob.id == job_id))).scalars().one()


@pytest.mark.asyncio
async def test_a_queued_altegio_reminder_for_a_marked_record_is_terminalised(session_maker, seeded, no_meta) -> None:
    """Queued by hand, by a race, or before the handover. It must not send."""
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)

    job_id = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_2H,
        status="queued",
        dedupe_key="altegio:reminder_2h:slipped-through",
        run_at=seeded["starts"] - timedelta(hours=2),
    )
    before = await process(session_maker, job_id)

    assert before.status == "canceled"
    assert before.last_error == REASON_HANDED_OVER
    assert before.locked_at is None
    assert before.attempts == 0, "nothing was attempted, so nothing was spent"
    no_meta.assert_not_awaited()

    async with session_maker() as session:
        assert (await session.execute(select(OutboxMessage))).scalars().all() == []


@pytest.mark.asyncio
async def test_the_send_guard_reason_carries_no_personal_data(session_maker, seeded, no_meta) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    job_id = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:slipped-through",
        run_at=seeded["starts"] - timedelta(hours=24),
    )
    job = await process(session_maker, job_id)

    for leaked in ("phone", "@", "+49", "Testkundin"):
        assert leaked not in (job.last_error or "")


@pytest.mark.asyncio
async def test_an_unmarked_altegio_reminder_is_not_touched_by_the_guard(session_maker, seeded) -> None:
    """No handover for this record: the ordinary send path is unchanged.

    The job still stops for its own unrelated reasons further down; what matters
    is that it is NOT the ownership guard that stopped it.
    """
    job_id = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:never-handed-over",
        run_at=seeded["starts"] - timedelta(hours=24),
    )
    job = await process(session_maker, job_id)

    assert job.last_error != REASON_HANDED_OVER
    assert job.last_error != REASON_UNKNOWN


@pytest.mark.asyncio
async def test_an_easyweek_reminder_is_never_suppressed_by_the_guard(session_maker, seeded) -> None:
    """It is the very thing the handover created."""
    plan = await plan_for(session_maker, starts=seeded["starts"])
    await run_apply(session_maker, plan)
    created = [job for job in await jobs(session_maker) if job.provider == PROVIDER_EASYWEEK and job.status == "queued"]
    assert created

    job = await process(session_maker, created[0].id)

    assert job.last_error != REASON_HANDED_OVER
    assert job.last_error != REASON_UNKNOWN


@pytest.mark.asyncio
async def test_an_unanswerable_ownership_lookup_never_sends(session_maker, seeded, no_meta, monkeypatch) -> None:
    """The database could not answer. That is not permission."""
    from altegio_bot.workers import outbox_worker as worker

    async def _cannot_tell(*args, **kwargs):
        return True, ReminderOwner.UNKNOWN

    monkeypatch.setattr(worker, "altegio_reminders_are_suppressed", _cannot_tell)
    job_id = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_24H,
        status="queued",
        dedupe_key="altegio:reminder_24h:unanswerable",
        run_at=seeded["starts"] - timedelta(hours=24),
    )
    job = await process(session_maker, job_id)

    assert job.status == "queued", "held for a later pass, not sent and not failed"
    assert job.last_error == REASON_UNKNOWN
    assert job.attempts == 0, "an unanswerable question is not a failed send"
    assert job.payload.get("_reminder_ownership_attempts") == 1
    no_meta.assert_not_awaited()
    async with session_maker() as session:
        assert (await session.execute(select(OutboxMessage))).scalars().all() == []


@pytest.mark.asyncio
async def test_a_corrupt_marker_is_refused_rather_than_crashing(session_maker, seeded, monkeypatch) -> None:
    """Half a marker keeps the row out of the wave, and says why.

    The database CHECK makes this state unreachable through every supported
    path, so it can only come from a row written directly. That is precisely
    when a defensive branch has to work — and this one called its own helper
    with the wrong arity, so instead of refusing the row it raised TypeError and
    took the whole plan down with it.

    The corruption is injected at the boundary rather than in the table: writing
    it for real is impossible, and weakening the CHECK to make the test easy
    would remove the protection the test exists to complement.
    """
    from altegio_bot.easyweek_migration import reminder_handover_db as db

    async with session_maker() as session:
        entry = (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()
        corrupt = SimpleNamespace(
            id=entry.id,
            status=entry.status,
            source_provider=entry.source_provider,
            source_company_id=entry.source_company_id,
            source_record_id=entry.source_record_id,
            target_provider=entry.target_provider,
            target_booking_uuid=entry.target_booking_uuid,
            # The instant without the digest: the forbidden half.
            reminders_handed_over_at=datetime.now(timezone.utc),
            reminder_handover_plan_digest=None,
        )

    async def _corrupt_rows(*_args, **_kwargs):
        return [corrupt]

    monkeypatch.setattr(db, "_ledger_rows", _corrupt_rows)

    client = FakeBookings(booking_body(seeded["starts"]))
    jobs_before = {job.id: job.status for job in await jobs(session_maker)}

    async with session_maker() as session:
        plan = await db.build_plan(
            session,
            manifest=wave_manifest(),
            company_ids=(COMPANY,),
            run_ids=("run-1", "run-2"),
            client=client,
            sleep=_no_sleep,
        )

    assert plan.scoped == ()
    assert [item.reason for item in plan.eligible_refusals] == ["marker_incomplete"]
    assert [item.ledger_id for item in plan.eligible_refusals] == [entry.id]
    assert client.calls == [], "a corrupt row is refused before any API call"
    assert {job.id: job.status for job in await jobs(session_maker)} == jobs_before
    assert await ledger_marker(session_maker) == (None, None), "the real row is untouched"
