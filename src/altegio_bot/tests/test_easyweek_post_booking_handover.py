"""The post-booking marketing handover, against real PostgreSQL (plan §31).

Three record-bound jobs — `review_3d`, `repeat_10d`, `comeback_3d` — are planned
from an Altegio booking and would be sent with Altegio's template, sender and
booking link. After the booking has moved to EasyWeek every one of those is
wrong, and withdrawing them once is not enough: `add_job` resurrects a cancelled
job on conflict, so a late Altegio delivery re-opens exactly what was withdrawn,
and a booking that had no such job can acquire its first one afterwards.

So this phase withdraws the open ones and writes a marker for EVERY eligible row
— and creates nothing at all, because a migrated future booking proves no
completed visit and no cancellation.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration import post_booking_handover_db as db
from altegio_bot.easyweek_migration.post_booking_handover import (
    STOP_LEDGER_SCOPE_CHANGED,
    STOP_MARKER_CONFLICT,
    STOP_NON_TERMINAL_OUTBOX,
    STOP_OUTBOX_SET_CHANGED,
    STOP_REMINDER_HANDOVER_INCOMPLETE,
    STOP_SOURCE_JOB_SET_CHANGED,
    STOP_SOURCE_PROCESSING,
    STOP_TARGET_BRANCH_MISMATCH,
    STOP_TARGET_JOB_SET_CHANGED,
    STOP_WAVE_NOT_CLOSED,
    STOP_WAVE_UNRESOLVED,
    PostBookingSnapshotError,
    check_snapshot_usable,
    confirmation_phrase,
    freeze_plan,
    invalidate_snapshot,
    read_snapshot,
    write_snapshot,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekMigrationLedger,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.post_booking_ownership import (
    PostBookingOwner,
    post_booking_owner,
)
from altegio_bot.scripts import easyweek_post_booking_handover as cli
from altegio_bot.settings import settings
from altegio_bot.tests import test_easyweek_reminder_handover_db as h

registry = h.registry
seeded = h.seeded

REVIEW = "review_3d"
REPEAT = "repeat_10d"
COMEBACK = "comeback_3d"


@pytest_asyncio.fixture
async def prepared(session_maker: async_sessionmaker[AsyncSession], seeded, registry):
    """A wave whose §30 handover is finished and whose closure is durable.

    Both are prerequisites, not decorations: without the reminder marker the
    booking's reminders may still be Altegio's, and without the closure a new
    booking can still be added to the wave — and it would arrive with no marker
    of either kind.
    """
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(EasyWeekMigrationLedger).values(
                    reminders_handed_over_at=datetime.now(timezone.utc),
                    reminder_handover_plan_digest="a" * 64,
                )
            )
            await ledger_module.close_migration_wave(
                session,
                source_company_id=h.COMPANY,
                run_id="run-1",
                plan_digest="a" * 64,
            )
    return seeded


async def plan(session_maker, *, runs=("run-1",), companies=(h.COMPANY,)):
    async with session_maker() as session:
        return await db.build_plan(
            session,
            manifest=h.wave_manifest(),
            company_ids=tuple(companies),
            run_ids=tuple(runs),
        )


async def apply(session_maker, planned, *, now=None):
    frozen = freeze_plan(planned)
    async with session_maker() as session:
        async with session.begin():
            result = await db.apply_plan(session, frozen, now=now)
            if result.halted is not None:
                await session.rollback()
    return result


async def add_source_job(session_maker, prepared, *, job_type: str, status: str = "queued", key: str = "s") -> int:
    return await h.add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=prepared["source_pk"],
        job_type=job_type,
        status=status,
        dedupe_key=f"altegio-{job_type}-{key}",
    )


async def add_target_job(session_maker, prepared, *, job_type: str, status: str = "queued", key: str = "t") -> int:
    return await h.add_job(
        session_maker,
        provider=PROVIDER_EASYWEEK,
        record_pk=prepared["target_pk"],
        job_type=job_type,
        status=status,
        dedupe_key=f"easyweek-{job_type}-{key}",
    )


async def jobs(session_maker) -> list[MessageJob]:
    async with session_maker() as session:
        return list((await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars().all())


async def ledger_row(session_maker) -> EasyWeekMigrationLedger:
    async with session_maker() as session:
        return (await session.execute(select(EasyWeekMigrationLedger))).scalars().one()


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


async def test_plan_is_read_only_and_counts_every_state(session_maker, prepared):
    queued = await add_source_job(session_maker, prepared, job_type=REVIEW)
    await add_source_job(session_maker, prepared, job_type=REPEAT, status="done", key="done")
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    before = [(job.id, job.status) for job in await jobs(session_maker)]

    planned = await plan(session_maker)
    report = planned.as_safe_dict()

    assert report["rows_in_scope"] == 1
    assert report["source_jobs_queued"] == 1
    assert report["source_jobs_processing"] == 0
    assert report["source_jobs_terminal"] == 1
    assert report["target_easyweek_jobs_present"] == 1
    # The exact overlap the production audit found: one booking holding an open
    # job of the same type on BOTH sides. Reported, not refused — the correct
    # end state is the EasyWeek one standing and the Altegio one withdrawn.
    assert report["rows_with_source_target_overlap"] == 1
    assert report["apply_ready"] is True
    assert planned.rows[0].queued_source_job_ids == (queued,)
    assert [job.job_id for job in planned.rows[0].target_jobs] == [target]

    # Nothing moved: plan writes nothing anywhere.
    assert [(job.id, job.status) for job in await jobs(session_maker)] == before
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_plan_refuses_a_second_transaction(session_maker, prepared):
    async with session_maker() as session:
        async with session.begin():
            await session.execute(select(EasyWeekMigrationLedger))
            with pytest.raises(db.PostBookingHandoverError):
                await db.build_plan(
                    session,
                    manifest=h.wave_manifest(),
                    company_ids=(h.COMPANY,),
                    run_ids=("run-1",),
                )


async def test_a_row_without_the_reminder_marker_is_refused(session_maker, seeded, registry):
    """§30 first. A booking whose reminders may still be Altegio's is not ours."""
    async with session_maker() as session:
        async with session.begin():
            await ledger_module.close_migration_wave(
                session, source_company_id=h.COMPANY, run_id="run-1", plan_digest="a" * 64
            )

    planned = await plan(session_maker)

    assert planned.rows == ()
    assert planned.refusals == {STOP_REMINDER_HANDOVER_INCOMPLETE: 1}
    assert planned.apply_ready is False
    assert STOP_LEDGER_SCOPE_CHANGED in planned.blockers


async def test_a_wave_that_never_ran_the_reminder_handover_is_not_apply_ready(session_maker, seeded, registry):
    """Neither prerequisite present: no closure row and no row-level marker.

    §30 closure is accepted from either — a closure row, or the marker a wave
    handed over by an earlier revision carries. With neither, this phase has no
    ground to stand on at all.
    """
    planned = await plan(session_maker)

    assert planned.wave_closed is False
    assert STOP_WAVE_NOT_CLOSED in planned.blockers
    assert planned.refusals == {STOP_REMINDER_HANDOVER_INCOMPLETE: 1}
    assert planned.apply_ready is False


async def test_an_unresolved_row_blocks_the_plan(session_maker, prepared):
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekMigrationLedger(
                    source_provider=PROVIDER_ALTEGIO,
                    source_company_id=h.COMPANY,
                    source_record_id=900777,
                    source_fingerprint="b" * 64,
                    target_provider=PROVIDER_EASYWEEK,
                    target_booking_uuid=None,
                    run_id="run-1",
                    status="uncertain",
                )
            )

    planned = await plan(session_maker)

    assert planned.unresolved_rows == {"uncertain": 1}
    assert STOP_WAVE_UNRESOLVED in planned.blockers
    assert planned.apply_ready is False


async def test_the_snapshot_carries_no_pii(session_maker, prepared, tmp_path):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    path = write_snapshot(planned, tmp_path / "plan.json")

    body = path.read_text(encoding="utf-8")
    for leaked in ("phone", "@", "+49", "wa_", "template", "http", "body"):
        assert leaked not in body, leaked
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert read_snapshot(path).digest == planned.digest()


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


async def test_apply_withdraws_queued_jobs_and_marks_every_row(session_maker, prepared):
    review = await add_source_job(session_maker, prepared, job_type=REVIEW)
    repeat = await add_source_job(session_maker, prepared, job_type=REPEAT, key="r")
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    done = await add_source_job(session_maker, prepared, job_type=COMEBACK, status="done", key="hist")

    planned = await plan(session_maker)
    result = await apply(session_maker, planned)

    assert result.halted is None
    assert result.canceled_job_ids == tuple(sorted((review, repeat)))
    by_id = {job.id: job for job in await jobs(session_maker)}
    assert by_id[review].status == "canceled"
    assert by_id[review].last_error == db.CANCEL_REASON
    assert by_id[repeat].status == "canceled"
    # History is history: a delivered job is not rewritten and not re-opened.
    assert by_id[done].status == "done"
    # The EasyWeek side is not this phase's to touch.
    assert by_id[target].status == "queued"
    assert by_id[target].last_error is None

    row = await ledger_row(session_maker)
    assert row.post_booking_jobs_handed_over_at is not None
    assert row.post_booking_handover_plan_digest == planned.digest()


async def test_a_row_with_no_marketing_job_is_still_marked(session_maker, prepared):
    """The case that makes the marker necessary at all.

    Nothing to cancel here — and without the marker a late Altegio delivery
    would create the FIRST obligation after the handover, with nothing for the
    runtime fences to find.
    """
    planned = await plan(session_maker)
    assert planned.as_safe_dict()["rows_without_source_job"] == 1
    assert planned.apply_ready is True

    result = await apply(session_maker, planned)

    assert result.halted is None
    assert result.canceled_job_ids == ()
    assert result.marked_ledger_ids == ((await ledger_row(session_maker)).id,)


async def test_a_processing_source_job_stops_the_whole_wave(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    claimed = await add_source_job(session_maker, prepared, job_type=REPEAT, status="processing", key="claimed")

    result = await apply(session_maker, planned)

    # The new job is also unknown to the plan, so the scope check fires first —
    # either way nothing is cancelled and nothing is marked.
    assert result.halted in {STOP_SOURCE_PROCESSING, STOP_SOURCE_JOB_SET_CHANGED}
    assert all(job.status in {"queued", "processing"} for job in await jobs(session_maker))
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None
    assert claimed


async def test_a_processing_job_the_plan_saw_still_stops_the_wave(session_maker, prepared):
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW, status="processing")
    planned = await plan(session_maker)
    assert planned.as_safe_dict()["source_jobs_processing"] == 1
    assert planned.apply_ready is False

    result = await apply(session_maker, planned)

    assert result.halted == STOP_SOURCE_PROCESSING
    assert (await jobs(session_maker))[0].id == job_id
    assert (await jobs(session_maker))[0].status == "processing"
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


@pytest.mark.parametrize("status", ["queued", "sending", "unknown"])
async def test_a_non_terminal_outbox_row_stops_the_whole_wave(session_maker, prepared, status):
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=h.COMPANY,
                    record_id=prepared["source_pk"],
                    job_id=job_id,
                    phone_e164="+490000000000",
                    template_code="t",
                    body="b",
                    status=status,
                    scheduled_at=datetime.now(timezone.utc),
                )
            )

    planned = await plan(session_maker)
    assert planned.as_safe_dict()["source_jobs_with_non_terminal_outbox"] == 1
    assert planned.apply_ready is False

    result = await apply(session_maker, planned)

    assert result.halted == STOP_NON_TERMINAL_OUTBOX
    assert (await jobs(session_maker))[0].status == "queued"
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


@pytest.mark.parametrize("status", ["sent", "delivered", "read", "failed", "canceled"])
async def test_a_terminal_outbox_row_is_history_and_does_not_block(session_maker, prepared, status):
    """The audit found deliveries that already reached `read`.

    They are evidence about the past, not a reason to leave the future
    unprotected: the message cannot be recalled, and the marker still has to be
    written or a late delivery would plan a new one.
    """
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW, status="done")
    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=h.COMPANY,
                    record_id=prepared["source_pk"],
                    job_id=job_id,
                    phone_e164="+490000000000",
                    template_code="t",
                    body="b",
                    status=status,
                    scheduled_at=datetime.now(timezone.utc),
                )
            )

    planned = await plan(session_maker)
    assert planned.apply_ready is True

    result = await apply(session_maker, planned)

    assert result.halted is None
    assert (await jobs(session_maker))[0].status == "done", "a delivered job is never rewritten"
    assert (await ledger_row(session_maker)).post_booking_handover_plan_digest == planned.digest()


async def test_a_job_created_after_the_plan_stops_the_wave(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    late = await add_source_job(session_maker, prepared, job_type=REPEAT, key="late")

    result = await apply(session_maker, planned)

    assert result.halted == STOP_SOURCE_JOB_SET_CHANGED
    by_id = {job.id: job for job in await jobs(session_maker)}
    assert by_id[late].status == "queued"
    assert all(job.status == "queued" for job in by_id.values())
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_a_changed_target_job_stops_the_wave(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(MessageJob).where(MessageJob.id == target).values(status="canceled"))

    result = await apply(session_maker, planned)

    assert result.halted == STOP_TARGET_JOB_SET_CHANGED
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_a_changed_ledger_scope_stops_the_wave(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekMigrationLedger(
                    source_provider=PROVIDER_ALTEGIO,
                    source_company_id=h.COMPANY,
                    source_record_id=900778,
                    source_fingerprint="c" * 64,
                    target_provider=PROVIDER_EASYWEEK,
                    target_booking_uuid="bbbbbbbb-0000-4000-8000-000000000009",
                    run_id="run-1",
                    status="created",
                )
            )

    result = await apply(session_maker, planned)

    assert result.halted == STOP_LEDGER_SCOPE_CHANGED
    assert (await jobs(session_maker))[0].status == "queued"


async def test_repeating_the_same_snapshot_is_idempotent(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)

    first = await apply(session_maker, planned)
    row = await ledger_row(session_maker)
    stamped = row.post_booking_jobs_handed_over_at
    second = await apply(session_maker, planned)

    assert first.halted is None and second.halted is None
    assert first.canceled_job_ids and second.canceled_job_ids == ()
    assert second.marked_ledger_ids == ()
    assert second.already_marked_ledger_ids == (row.id,)
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at == stamped


async def test_a_foreign_digest_is_refused(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(EasyWeekMigrationLedger).values(
                    post_booking_jobs_handed_over_at=datetime.now(timezone.utc),
                    post_booking_handover_plan_digest="f" * 64,
                )
            )

    result = await apply(session_maker, planned)

    assert result.halted == STOP_MARKER_CONFLICT
    assert (await jobs(session_maker))[0].status == "queued"


async def test_an_exception_rolls_back_both_halves(session_maker, prepared, monkeypatch):
    """The cancellation and the marker are one fact or neither."""
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    frozen = freeze_plan(planned)

    real_flush = AsyncSession.flush

    async def explode(self, *args, **kwargs):
        await real_flush(self, *args, **kwargs)
        raise RuntimeError("interrupted between cancellation and marker")

    monkeypatch.setattr(AsyncSession, "flush", explode)
    with pytest.raises(RuntimeError):
        async with session_maker() as session:
            async with session.begin():
                await db.apply_plan(session, frozen)
    monkeypatch.setattr(AsyncSession, "flush", real_flush)

    by_id = {job.id: job for job in await jobs(session_maker)}
    assert by_id[job_id].status == "queued", "the cancellation was rolled back"
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_another_run_is_not_in_scope_and_is_not_touched(session_maker, prepared):
    other_source, _other_target = await h.add_migrated_pair(
        session_maker,
        source_record_id=900902,
        booking_uuid=h.BOOKING_TWO,
        starts_at=prepared["starts"],
    )
    other_job = await h.add_job(
        session_maker,
        provider=PROVIDER_ALTEGIO,
        record_pk=other_source,
        job_type=REVIEW,
        status="queued",
        dedupe_key="altegio-review-other-run",
    )
    await add_source_job(session_maker, prepared, job_type=REVIEW)

    planned = await plan(session_maker)
    result = await apply(session_maker, planned)

    assert result.halted is None
    by_id = {job.id: job for job in await jobs(session_maker)}
    assert by_id[other_job].status == "queued", "run-2 is a different wave and is not touched"
    async with session_maker() as session:
        rows = (
            await session.execute(select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.run_id == "run-2"))
        ).scalars()
        assert all(row.post_booking_jobs_handed_over_at is None for row in rows)


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


async def test_verify_proves_the_end_state(session_maker, prepared):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    await add_target_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    frozen = freeze_plan(planned)
    result = await apply(session_maker, planned)
    assert result.halted is None
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        findings = await db.verify_handover(session, frozen, report)

    assert findings["passed"] is True
    assert findings["open_source_job_ids"] == []
    assert findings["rows_missing_marker"] == []
    assert findings["wave_closed"] is True

    # And a fresh plan now has nothing left to cancel.
    again = await plan(session_maker)
    assert again.as_safe_dict()["source_jobs_queued"] == 0
    assert again.as_safe_dict()["rows_already_marked"] == 1


async def test_verify_fails_when_a_source_job_is_open_again(session_maker, prepared):
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    frozen = freeze_plan(planned)
    result = await apply(session_maker, planned)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    # Exactly what `add_job` does on conflict, simulated directly.
    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(MessageJob).where(MessageJob.id == job_id).values(status="queued"))

    async with session_maker() as session:
        findings = await db.verify_handover(session, frozen, report)
    assert findings["passed"] is False
    assert findings["open_source_job_ids"] == [job_id]


# ---------------------------------------------------------------------------
# the runtime lookup
# ---------------------------------------------------------------------------


async def test_ownership_is_altegio_until_the_marker_exists(session_maker, prepared):
    async with session_maker() as session:
        owner = await post_booking_owner(session, company_id=h.COMPANY, altegio_record_id=h.SOURCE_RECORD_ID)
    assert owner is PostBookingOwner.ALTEGIO


async def test_ownership_becomes_easyweek_after_the_apply(session_maker, prepared):
    planned = await plan(session_maker)
    assert (await apply(session_maker, planned)).halted is None

    async with session_maker() as session:
        owner = await post_booking_owner(session, company_id=h.COMPANY, altegio_record_id=h.SOURCE_RECORD_ID)
    assert owner is PostBookingOwner.EASYWEEK


@pytest.mark.parametrize(
    "half",
    [
        pytest.param("instant", id="instant-only"),
        pytest.param("digest", id="digest-only"),
    ],
)
async def test_half_a_marker_cannot_be_written_at_all(session_maker, prepared, half):
    """The database refuses it, so the runtime never has to interpret it.

    An instant with no digest would claim ownership moved under no authority; a
    digest with no instant would claim an authority with no handover. The fence
    reads the instant and the apply compares the digest, so either half alone
    would let one of them answer while the other could not — which is why the
    lookup still treats a half marker as UNKNOWN if one ever appears.
    """
    values: dict[str, Any] = (
        {"post_booking_jobs_handed_over_at": datetime.now(timezone.utc)}
        if half == "instant"
        else {"post_booking_handover_plan_digest": "b" * 64}
    )
    with pytest.raises(IntegrityError) as refused:
        async with session_maker() as session:
            async with session.begin():
                await session.execute(update(EasyWeekMigrationLedger).values(**values))
    assert "post_booking_handover_complete" in str(refused.value)

    row = await ledger_row(session_maker)
    assert row.post_booking_jobs_handed_over_at is None
    assert row.post_booking_handover_plan_digest is None


@pytest.mark.parametrize(
    "company_id, record_id",
    [
        pytest.param(h.COMPANY + 1, h.SOURCE_RECORD_ID, id="another-company"),
        pytest.param(h.COMPANY, h.SOURCE_RECORD_ID + 1, id="another-record"),
        pytest.param(None, h.SOURCE_RECORD_ID, id="unusable-company"),
        pytest.param(h.COMPANY, "900001", id="unusable-record"),
    ],
)
async def test_ownership_is_exact(session_maker, prepared, company_id, record_id):
    planned = await plan(session_maker)
    assert (await apply(session_maker, planned)).halted is None

    async with session_maker() as session:
        owner = await post_booking_owner(session, company_id=company_id, altegio_record_id=record_id)

    # A different identity is never covered by this booking's marker, and an
    # unusable one is UNKNOWN rather than permission.
    assert owner in {PostBookingOwner.ALTEGIO, PostBookingOwner.UNKNOWN}
    assert owner is not PostBookingOwner.EASYWEEK


# ---------------------------------------------------------------------------
# the operator's permission
# ---------------------------------------------------------------------------


def _applicable(tmp_path: Path, planned) -> Path:
    path = write_snapshot(planned, tmp_path / "plan.json")
    assert read_snapshot(path).digest
    return path


async def test_the_permission_needs_the_exact_digest_and_phrase(session_maker, prepared, tmp_path):
    planned = await plan(session_maker)
    frozen = freeze_plan(planned)
    now = datetime.now(timezone.utc)

    with pytest.raises(PostBookingSnapshotError):
        check_snapshot_usable(frozen, supplied_digest=None, supplied_confirmation=None, now=now)
    with pytest.raises(PostBookingSnapshotError):
        check_snapshot_usable(
            frozen,
            supplied_digest="f" * 64,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=now,
        )
    with pytest.raises(PostBookingSnapshotError):
        check_snapshot_usable(frozen, supplied_digest=frozen.digest, supplied_confirmation="yes", now=now)
    with pytest.raises(PostBookingSnapshotError):
        check_snapshot_usable(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=now + timedelta(hours=2),
        )
    # And the correct combination passes.
    check_snapshot_usable(
        frozen,
        supplied_digest=frozen.digest,
        supplied_confirmation=confirmation_phrase(frozen.digest),
        now=now,
    )


def test_the_confirmation_phrase_is_not_the_reminder_handover_one():
    from altegio_bot.easyweek_migration import reminder_handover as reminders

    digest = "a" * 64
    assert confirmation_phrase(digest) != reminders.confirmation_phrase(digest)


async def test_a_reminder_snapshot_cannot_authorise_this_phase(session_maker, prepared, tmp_path):
    """The §30 artefact is a different authorisation for a different transaction."""
    from altegio_bot.easyweek_migration import reminder_handover as reminders
    from altegio_bot.tests.test_easyweek_reminder_handover import handover_row, owed, plan_with

    other = reminders.write_snapshot(plan_with(handover_row(obligations=owed(48))), tmp_path / "reminder.json")
    with pytest.raises(PostBookingSnapshotError):
        read_snapshot(other)


async def test_an_edited_snapshot_is_refused(session_maker, prepared, tmp_path):
    await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    path = _applicable(tmp_path, planned)

    payload = json.loads(path.read_text())
    payload["rows"][0]["source_jobs"] = []
    path.write_text(json.dumps(payload))

    with pytest.raises(PostBookingSnapshotError):
        read_snapshot(path)


async def test_a_new_plan_attempt_destroys_the_old_permission(session_maker, prepared, tmp_path):
    planned = await plan(session_maker)
    path = _applicable(tmp_path, planned)

    invalidate_snapshot(path, reason="superseded_by_new_plan")

    with pytest.raises(PostBookingSnapshotError) as refused:
        read_snapshot(path)
    assert str(refused.value) == "snapshot_invalidated"
    body = json.loads(path.read_text())
    assert set(body) == {"version", "kind", "mode", "invalidated_at", "reason"}
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("mode", ["apply", "verify"])
def test_an_archive_path_is_refused_before_any_session(tmp_path, monkeypatch, mode):
    archived = tmp_path / "plan.json.invalidated"
    archived.write_text("{}")

    def _no_session(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("an archive path reached a database session")

    monkeypatch.setattr(cli, "SessionLocal", _no_session)
    monkeypatch.setenv(cli.APPLY_ENV_FLAG, "true")

    exit_code = cli.main(
        [
            mode,
            "--company-id",
            str(h.COMPANY),
            "--run-id",
            "run-1",
            "--manifest",
            str(tmp_path / "m.json"),
            "--snapshot",
            str(archived),
            *(["--apply", "--plan-digest", "d" * 64, "--confirm", "x"] if mode == "apply" else []),
        ]
    )
    assert exit_code == 1


def test_apply_without_the_environment_flag_never_opens_a_session(tmp_path, monkeypatch, capsys):
    """Both halves of the permission, or nothing at all."""
    snapshot = tmp_path / "plan.json"
    snapshot.write_text("{}")

    def _no_session(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("a refused apply opened a database session")

    monkeypatch.setattr(cli, "SessionLocal", _no_session)
    monkeypatch.delenv(cli.APPLY_ENV_FLAG, raising=False)

    exit_code = cli.main(
        [
            "apply",
            "--company-id",
            str(h.COMPANY),
            "--run-id",
            "run-1",
            "--manifest",
            str(tmp_path / "m.json"),
            "--snapshot",
            str(snapshot),
            "--apply",
            "--plan-digest",
            "d" * 64,
            "--confirm",
            "x",
        ]
    )

    assert exit_code == 1
    assert "post_booking_handover_unexpected_error" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["plan", "--company-id", "not-a-number", "--run-id", "run-1"], id="malformed-company"),
        pytest.param(["--company-id", "758285"], id="missing-run-id"),
        pytest.param(["plan", "--company-id", "758285", "--unknown"], id="unknown-argument"),
    ],
)
def test_a_plan_that_fails_to_parse_still_destroys_the_old_permission(tmp_path, argv):
    snapshot = tmp_path / "plan.json"
    snapshot.write_text(json.dumps({"kind": "post_booking_handover", "version": 1, "mode": "read-only"}))

    with pytest.raises(SystemExit) as exited:
        cli.main([*argv, "--manifest", "m.json", "--snapshot", str(snapshot)])

    assert exited.value.code == 2
    assert json.loads(snapshot.read_text())["mode"] == "invalidated"


def test_an_apply_option_value_of_plan_invalidates_nothing(tmp_path, monkeypatch):
    snapshot = tmp_path / "plan.json"
    snapshot.write_text(json.dumps({"kind": "post_booking_handover", "version": 1, "mode": "read-only"}))
    monkeypatch.setattr(cli, "SessionLocal", lambda *a, **k: None)

    cli.main(
        [
            "verify",
            "--company-id",
            str(h.COMPANY),
            "--run-id",
            "plan",
            "--manifest",
            "plan",
            "--snapshot",
            str(snapshot),
        ]
    )

    assert json.loads(snapshot.read_text())["mode"] == "read-only"


def test_the_cli_gates_cannot_be_reordered_away():
    """`--apply` alone is not permission, and neither is the environment alone."""
    parser = cli.build_parser()
    args = parser.parse_args(["apply", "--company-id", "1", "--run-id", "run-1", "--manifest", "m.json", "--apply"])
    assert cli._apply_permitted(args) is False
    args_without_flag = parser.parse_args(["apply", "--company-id", "1", "--run-id", "run-1", "--manifest", "m.json"])
    assert cli._apply_permitted(args_without_flag) is False


def test_the_pre_parser_mirrors_the_real_option_arity():
    real = {
        action.option_strings[0]
        for action in cli.build_parser()._actions
        if action.option_strings and action.nargs != 0
    }
    pre = {
        action.option_strings[0]
        for action in cli.build_pre_parser()._actions
        if action.option_strings and action.nargs != 0
    }
    assert sorted(real - pre) == [], "an option whose value the pre-parser does not know can be read as the mode"


def test_every_stop_code_is_pii_free():
    from altegio_bot.easyweek_migration import post_booking_handover as artefact

    codes = [value for name, value in vars(artefact).items() if name.startswith("STOP_")]
    assert codes
    for code in codes:
        assert code == code.lower()
        for leaked in ("phone", "@", "+49", "http"):
            assert leaked not in code


def test_the_marker_columns_are_not_the_reminder_ones():
    """Two proofs, two column pairs. One must never stand in for the other."""
    columns = {column.name for column in EasyWeekMigrationLedger.__table__.columns}
    assert {"post_booking_jobs_handed_over_at", "post_booking_handover_plan_digest"} <= columns
    assert {"reminders_handed_over_at", "reminder_handover_plan_digest"} <= columns


def test_the_apply_path_creates_no_easyweek_job_and_no_outbox_row() -> None:
    """Structural: this phase withdraws, it never creates.

    A migrated future booking proves no completed visit and no cancellation, so
    an EasyWeek `review_3d`, `repeat_10d` or `comeback_3d` created here would be
    a message invented by a migration tool.
    """
    import inspect

    source = inspect.getsource(db)
    assert "OutboxMessage(" not in source
    assert "MessageJob(" not in source
    assert "pg_insert(MessageJob)" not in source
    assert "insert_values" not in source
    for forbidden in ("EasyWeekClient", "httpx", "chatwoot", "meta_", "send_"):
        assert forbidden not in source, forbidden


def test_the_apply_report_shape_is_stable(tmp_path):
    from altegio_bot.easyweek_migration.post_booking_handover import (
        PostBookingApplyReport,
        write_apply_report,
    )

    report = PostBookingApplyReport(
        snapshot_digest="a" * 64,
        applied_at=datetime.now(timezone.utc),
        company_ids=(h.COMPANY,),
        run_ids=("run-1",),
        rows_in_scope=1,
        canceled_job_ids=(1,),
        already_canceled_job_ids=(),
        marked_ledger_ids=(1,),
        already_marked_ledger_ids=(),
        target_job_ids=(2,),
        scoped_outbox_ids=(),
    )
    path = write_apply_report(report, tmp_path / "apply.json")
    payload: dict[str, Any] = json.loads(path.read_text())
    assert payload["kind"] == "post_booking_handover_apply"
    assert path.stat().st_mode & 0o777 == 0o600


# ---------------------------------------------------------------------------
# Review fixes: the target side is proven, not assumed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("landed", ["done", "canceled"])
async def test_a_normal_target_transition_after_apply_is_diagnostic_not_a_failure(session_maker, prepared, landed):
    """The documented rollout restarts the outbox worker BEFORE verify.

    So a target `review_3d` may legitimately move `queued -> done`, or be
    cancelled by its own send guard, between the apply and the verification.
    That is the EasyWeek side doing its work; treating it as proof that the
    handover mutated a target would make the prescribed rollout report a
    violation every time. It is reported, and it does not fail.
    """
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    frozen = freeze_plan(planned)
    result = await apply(session_maker, planned)
    assert result.halted is None
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    # The worker is back, and the target job finishes its ordinary life: either
    # it is delivered, or its own send guard withdraws it (the §31.11 visit
    # limit does exactly that). Neither is the handover's doing.
    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(MessageJob).where(MessageJob.id == target).values(status=landed))

    async with session_maker() as session:
        findings = await db.verify_handover(session, frozen, report)

    assert findings["passed"] is True, landed
    assert findings["rows_with_changed_target_jobs"] == []
    assert findings["target_job_transitions"] == [
        {
            "ledger_id": (await ledger_row(session_maker)).id,
            "job_id": target,
            "frozen_status": "queued",
            "current_status": landed,
        }
    ], "the change is shown, never hidden"

    # And the handover itself still issued no UPDATE against an EasyWeek job.
    async with session_maker() as session:
        job = await session.get(MessageJob, target)
    assert job.last_error is None


async def test_verify_fails_when_a_target_job_disappears(session_maker, prepared):
    """A changed SET is the thing the handover could be responsible for.

    It creates no EasyWeek job and updates none, so an id that vanished or
    appeared is the only target-side change that could implicate it.
    """
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    frozen = freeze_plan(planned)
    result = await apply(session_maker, planned)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        async with session.begin():
            await session.execute(delete(MessageJob).where(MessageJob.id == target))

    async with session_maker() as session:
        findings = await db.verify_handover(session, frozen, report)

    assert findings["passed"] is False
    assert findings["rows_with_changed_target_jobs"] == [(await ledger_row(session_maker)).id]


async def test_the_apply_proves_target_statuses_inside_its_own_transaction(session_maker, prepared):
    """Where a status DOES have to be exact: under the locks, before the write."""
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(MessageJob).where(MessageJob.id == target).values(status="done"))

    result = await apply(session_maker, planned)

    assert result.halted == STOP_TARGET_JOB_SET_CHANGED
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_a_target_in_another_branch_is_refused_by_the_plan(session_maker, prepared):
    """The booking uuid alone is not identity.

    The manifest says which EasyWeek location this Altegio company migrated
    into. A target filed under a different one means source and target are
    crossed, and cancelling that booking's Altegio follow-ups would hand
    ownership to a branch nobody proved — durably, because the marker stays.
    """
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(Record).where(Record.id == prepared["target_pk"]).values(company_id=h.LOCATION_ID + 1)
            )

    planned = await plan(session_maker)

    assert planned.rows == ()
    assert planned.refusals == {STOP_TARGET_BRANCH_MISMATCH: 1}
    assert planned.apply_ready is False
    # And nothing was written by asking.
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


# ---------------------------------------------------------------------------
# Drift between plan and apply: the whole set, by id and status
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param({"status": "done"}, id="queued-to-done"),
        pytest.param({"status": "failed"}, id="queued-to-failed"),
        pytest.param({"status": "canceled", "last_error": "cancelled by somebody else"}, id="foreign-cancel"),
        pytest.param({"status": "processing"}, id="queued-to-processing"),
    ],
)
async def test_a_source_job_that_moved_after_the_plan_stops_the_wave(session_maker, prepared, mutate):
    """Only asking for ADDED ids was not enough.

    `queued -> done` means the message was sent; `queued -> failed` means it was
    attempted; a cancel carrying somebody else's reason is not this handover's
    work. Each of those left the operator a report describing a withdrawal that
    never happened.
    """
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(MessageJob).where(MessageJob.id == job_id).values(**mutate))

    result = await apply(session_maker, planned)

    assert result.halted in {STOP_SOURCE_JOB_SET_CHANGED, STOP_SOURCE_PROCESSING}
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None
    async with session_maker() as session:
        job = await session.get(MessageJob, job_id)
    assert job.status == mutate["status"], "nothing was cancelled on a picture that had moved"


async def test_a_source_job_deleted_after_the_plan_stops_the_wave(session_maker, prepared):
    job_id = await add_source_job(session_maker, prepared, job_type=REPEAT)
    planned = await plan(session_maker)

    async with session_maker() as session:
        async with session.begin():
            await session.execute(delete(MessageJob).where(MessageJob.id == job_id))

    result = await apply(session_maker, planned)

    assert result.halted == STOP_SOURCE_JOB_SET_CHANGED
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_a_terminal_outbox_row_created_after_the_plan_stops_the_wave(session_maker, prepared):
    """A send that happened between the plan and the stop must not be invisible.

    The row is terminal by the time the apply looks, so a check that only asked
    whether anything was still in flight saw nothing — and the wave would have
    been withdrawn as if that customer had never been messaged.
    """
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    assert planned.apply_ready is True

    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=h.COMPANY,
                    record_id=prepared["source_pk"],
                    job_id=job_id,
                    phone_e164="+490000000000",
                    template_code="t",
                    body="b",
                    status="sent",
                    scheduled_at=datetime.now(timezone.utc),
                )
            )

    result = await apply(session_maker, planned)

    assert result.halted == STOP_OUTBOX_SET_CHANGED
    async with session_maker() as session:
        job = await session.get(MessageJob, job_id)
    assert job.status == "queued"
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_an_outbox_row_that_changed_status_after_the_plan_stops_the_wave(session_maker, prepared):
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW, status="done")
    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=h.COMPANY,
                    record_id=prepared["source_pk"],
                    job_id=job_id,
                    phone_e164="+490000000000",
                    template_code="t",
                    body="b",
                    status="sent",
                    scheduled_at=datetime.now(timezone.utc),
                )
            )
    planned = await plan(session_maker)
    assert planned.apply_ready is True

    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(OutboxMessage).values(status="read"))

    result = await apply(session_maker, planned)

    assert result.halted == STOP_OUTBOX_SET_CHANGED
    assert (await ledger_row(session_maker)).post_booking_jobs_handed_over_at is None


async def test_the_exact_repeat_after_our_own_cancellation_still_applies(session_maker, prepared):
    """The one allowed difference: our own withdrawal, under our own digest."""
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)

    first = await apply(session_maker, planned)
    assert first.canceled_job_ids == (job_id,)
    second = await apply(session_maker, planned)

    assert second.halted is None
    assert second.canceled_job_ids == ()
    assert second.already_canceled_job_ids == (job_id,)
    assert second.marked_ledger_ids == ()


async def test_a_repeat_under_a_foreign_digest_is_refused(session_maker, prepared):
    """A cancelled job is only forgiven when THIS digest already owns the row."""
    job_id = await add_source_job(session_maker, prepared, job_type=REVIEW)
    planned = await plan(session_maker)
    assert (await apply(session_maker, planned)).halted is None

    async with session_maker() as session:
        async with session.begin():
            await session.execute(update(EasyWeekMigrationLedger).values(post_booking_handover_plan_digest="f" * 64))

    result = await apply(session_maker, planned)

    assert result.halted in {STOP_MARKER_CONFLICT, STOP_SOURCE_JOB_SET_CHANGED}
    async with session_maker() as session:
        job = await session.get(MessageJob, job_id)
    assert job.status == "canceled", "the first withdrawal stands"


def test_the_snapshot_version_was_raised_and_the_old_one_is_refused(tmp_path):
    """v1 froze only the non-terminal Outbox ids; v2 freezes every row.

    Reading a v1 file under v2 rules would silently mean "no Outbox row existed"
    for rows the old plan never recorded, so it is refused rather than
    reinterpreted.
    """
    from altegio_bot.easyweek_migration.post_booking_handover import SNAPSHOT_VERSION

    assert SNAPSHOT_VERSION == 2
    legacy = tmp_path / "v1.json"
    legacy.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "post_booking_handover",
                "mode": "read-only",
                "created_at": "2026-09-05T00:00:00Z",
                "company_ids": [h.COMPANY],
                "wave": {},
                "ledger_rows_seen": 1,
                "eligible_created_rows": 1,
                "refusals": {},
                "rows": [],
                "plan_digest": "a" * 64,
            }
        )
    )
    with pytest.raises(PostBookingSnapshotError):
        read_snapshot(legacy)


# ---------------------------------------------------------------------------
# §31.11: the plan reports what the send guard will decide — and decides nothing
# ---------------------------------------------------------------------------


async def _set_visits(session_maker, total: int | None) -> None:
    async with session_maker() as session:
        async with session.begin():
            client = await session.get(Client, 1)
            client.easyweek_visits_total = total
            client.easyweek_visits_total_updated_at = datetime.now(timezone.utc) if total is not None else None


@pytest.mark.parametrize(
    ("label", "total", "expected"),
    [
        ("at-the-limit", 3, "review_visit_count_eligible"),
        ("over-the-limit", 4, "review_visit_limit_exceeded"),
        ("no-snapshot", None, "review_visit_count_unproven"),
    ],
)
async def test_the_plan_buckets_open_target_reviews_by_visit_verdict(
    session_maker, prepared, monkeypatch, label: str, total: int | None, expected: str
):
    """An operator sees how much of the EasyWeek backlog is provably eligible.

    The same question the sender asks, answered from the same stored snapshot —
    never a payload, never an Altegio call. Counts only: no names, phones or ids
    of customers.
    """
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)
    await add_target_job(session_maker, prepared, job_type=REVIEW)
    await _set_visits(session_maker, total)

    planned = await plan(session_maker)

    assert planned.target_review_visit_buckets == {expected: 1}, label
    payload = planned.as_safe_dict()
    assert payload["target_review_visit_buckets"] == {expected: 1}
    for leaked in ("phone", "@", "+49", "http", "name"):
        assert leaked not in json.dumps(payload["target_review_visit_buckets"]), leaked


async def test_a_disabled_counter_is_reported_as_its_own_bucket(session_maker, prepared, monkeypatch):
    """ "Nobody is asking the question right now" is not "the customer is eligible"."""
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", False, raising=False)
    await add_target_job(session_maker, prepared, job_type=REVIEW)
    await _set_visits(session_maker, 1)

    planned = await plan(session_maker)

    assert planned.target_review_visit_buckets == {"review_visit_counter_disabled": 1}


async def test_a_terminal_target_review_is_not_bucketed(session_maker, prepared, monkeypatch):
    """The buckets describe what is still ABOUT to be sent."""
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)
    await add_target_job(session_maker, prepared, job_type=REVIEW, status="done")
    await _set_visits(session_maker, 9)

    planned = await plan(session_maker)

    assert planned.target_review_visit_buckets == {}


async def test_the_buckets_change_nothing_about_the_target_jobs(session_maker, prepared, monkeypatch):
    """Reporting is not acting. An over-limit review is stopped by the runtime."""
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)
    target = await add_target_job(session_maker, prepared, job_type=REVIEW)
    await _set_visits(session_maker, 4)
    await add_source_job(session_maker, prepared, job_type=REVIEW)

    planned = await plan(session_maker)
    assert planned.target_review_visit_buckets == {"review_visit_limit_exceeded": 1}
    result = await apply(session_maker, planned)
    assert result.halted is None

    async with session_maker() as session:
        job = await session.get(MessageJob, target)
    assert job.status == "queued"
    assert job.last_error is None
    assert job.attempts == 0
