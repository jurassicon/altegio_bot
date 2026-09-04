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
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.reminder_handover import (
    CANCEL_REASON,
    HandoverPlan,
    write_snapshot,
)
from altegio_bot.easyweek_migration.reminder_handover_db import (
    apply_plan,
    build_plan,
    verify_handover,
)
from altegio_bot.easyweek_policy import REMINDER_2H, REMINDER_24H
from altegio_bot.easyweek_reminders import easyweek_reminder_dedupe_key
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    EasyWeekMigrationLedger,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_migration_harness import (
    KA_LOCATION_ID,
    apply_production_flags,
    manifest_json,
)
from altegio_bot.tests.test_easyweek_migration_planning import KA_LOCATION_UUID

COMPANY = KARLSRUHE_COMPANY_ID
BOOKING = uuid_module.UUID("aaaaaaaa-0000-4000-8000-000000000001")
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


def booking_body(starts_at: datetime, *, canceled: bool = False, completed: bool = False) -> dict[str, Any]:
    return {
        "uuid": str(BOOKING),
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
            source = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=COMPANY,
                altegio_record_id=SOURCE_RECORD_ID,
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
) -> int:
    # An EasyWeek job belongs to the EasyWeek location; an Altegio job to the
    # Altegio company. Getting this wrong is what the guard's company check
    # catches, so the fixture defaults it correctly rather than hiding it.
    if company_id is None:
        company_id = LOCATION_ID if provider == PROVIDER_EASYWEEK else COMPANY
    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                provider=provider,
                company_id=company_id,
                record_id=record_pk,
                job_type=job_type,
                run_at=run_at or (datetime.now(timezone.utc) + timedelta(hours=24)),
                status=status,
                dedupe_key=dedupe_key,
            )
            session.add(job)
            await session.flush()
            return job.id


async def jobs(session_maker: async_sessionmaker[AsyncSession]) -> list[MessageJob]:
    async with session_maker() as session:
        return list((await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars().all())


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
            client=client,
            sleep=_no_sleep,
        )


async def run_apply(
    session_maker: async_sessionmaker[AsyncSession],
    plan: HandoverPlan,
    *,
    now: datetime | None = None,
):
    frozen = tuple(row.as_safe_dict() for row in plan.scoped)
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
        await build_plan(session, manifest=wave_manifest(), company_ids=(COMPANY,), client=client, sleep=_no_sleep)

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
async def test_a_local_record_disagreeing_with_the_api_keeps_the_row_out(session_maker, seeded) -> None:
    """Planning from either side would be planning from a guess."""
    moved = seeded["starts"] + timedelta(hours=2)
    plan = await plan_for(session_maker, answer=booking_body(moved))

    assert plan.scoped == ()
    assert "local_target_mismatch" in plan.refused


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
@pytest.mark.parametrize("status", ["canceled", "failed"])
async def test_a_cancelled_or_failed_key_blocks_the_wave(status: str, session_maker, seeded) -> None:
    """Re-opening it is an operator decision, never the tool's."""
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

    assert plan.guard_ready is False
    assert plan.cutover_ready is False


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

    frozen = tuple(row.as_safe_dict() for row in plan.scoped)
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
    frozen = tuple(row.as_safe_dict() for row in plan.scoped)

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

    assert (await run_apply(session_maker, plan)).halted == "ledger_not_created"


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
    frozen = tuple(row.as_safe_dict() for row in plan.scoped)
    await run_apply(session_maker, plan)

    async with session_maker() as session:
        report = await verify_handover(session, frozen)

    assert report["passed"] is True
    assert report["open_altegio_reminders"] == []
    assert report["unmet_obligations"] == 0


@pytest.mark.asyncio
async def test_verify_fails_while_an_old_altegio_reminder_is_still_open(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = tuple(row.as_safe_dict() for row in plan.scoped)
    await run_apply(session_maker, plan)
    leftover = await add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=seeded["source_pk"],
        job_type=REMINDER_2H,
        status="queued",
        dedupe_key="altegio:missed-one",
    )

    async with session_maker() as session:
        report = await verify_handover(session, frozen)

    assert report["passed"] is False
    assert report["open_altegio_reminders"] == [leftover]


@pytest.mark.asyncio
async def test_verify_flags_an_easyweek_reminder_for_the_wrong_instant(session_maker, seeded) -> None:
    plan = await plan_for(session_maker, starts=seeded["starts"])
    frozen = tuple(row.as_safe_dict() for row in plan.scoped)
    await run_apply(session_maker, plan)
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
        report = await verify_handover(session, frozen)

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

    assert result.halted == "obligation_not_created"
    rows = {job.id: job for job in await jobs(session_maker)}
    assert rows[stale].status == "queued", "the customer keeps the reminder they had"


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
