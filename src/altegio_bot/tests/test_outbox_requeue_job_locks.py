"""Returning a claimed batch must share the booking planner's job lock order."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text

from altegio_bot.models.models import AltegioEvent, MessageJob, Record
from altegio_bot.tests.test_whatsapp_consent_job_locks import (
    _add_review_job,
    _reassignment,
    _wait_for_sql_lock,
    isolated_http,  # noqa: F401
)
from altegio_bot.workers import inbox_worker, outbox_worker


@pytest_asyncio.fixture
async def requeue_scan_by_run_at(session_maker):
    """Expose a legal UPDATE order without relying on index creation order.

    The competing status-only/locked-at indexes make PostgreSQL's choice a tie
    in this small fixture. Remove only those non-unique access paths, before
    the concurrent transactions, and restore their exact definitions afterward.
    No primary/unique/FK constraint or row-lock behaviour is changed.
    """
    names = ("ix_message_jobs_status", "ix_message_jobs_status_locked_at")
    async with session_maker() as session, session.begin():
        indexes = (
            await session.execute(text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename='message_jobs'"))
        ).all()
        definitions = {name: definition for name, definition in indexes if name in names}
        assert set(definitions) == set(names)
        for name in names:
            await session.execute(text(f"DROP INDEX {name}"))
    try:
        yield
    finally:
        async with session_maker() as session, session.begin():
            for name in names:
                await session.execute(text(definitions[name]))


async def _use_index_scan(session) -> None:
    await session.execute(text("SET LOCAL enable_seqscan = off"))
    await session.execute(text("SET LOCAL enable_bitmapscan = off"))


async def test_claimed_batch_requeue_and_record_update_share_job_lock_order(
    session_maker, monkeypatch, requeue_scan_by_run_at
) -> None:
    """The old bulk UPDATE takes J2 then J1; the planner takes J1 then J2.

    A BEFORE UPDATE barrier pauses requeue after it has locked J2. It is
    released before either task finishes, so it cannot form the final cycle.
    The real claim commits both jobs as processing. Nothing changes their
    run_at or physical placement afterward.
    """
    scenario = await _reassignment(session_maker)
    second = await _add_review_job(session_maker, scenario)
    ids = [scenario.job, second]
    async with session_maker() as session, session.begin():
        await session.execute(
            text(
                "UPDATE message_jobs SET run_at=now()-CASE WHEN id=:first "
                "THEN interval '1 hour' ELSE interval '2 hours' END"
            ),
            {"first": ids[0]},
        )
        claimed = await outbox_worker._lock_next_jobs(session, 50)
        assert [job.id for job in claimed] == [second, scenario.job]
        assert [event.id for event in await inbox_worker.lock_next_batch(session, 50)] == [scenario.event]
    async with session_maker() as session, session.begin():
        await _use_index_scan(session)
        plan = list(
            (
                await session.scalars(
                    text(
                        "EXPLAIN UPDATE message_jobs SET status='queued',locked_at=NULL "
                        f"WHERE id IN ({ids[0]}, {ids[1]}) AND status='processing'"
                    )
                )
            ).all()
        )
        assert any("Index Scan using ix_message_jobs_status_run_at" in line for line in plan), plan
        assert not any("Sort" in line or "Bitmap" in line for line in plan), plan
        assert list(
            (
                await session.scalars(
                    text(
                        "SELECT id FROM message_jobs WHERE status='processing' "
                        f"AND id IN ({ids[0]}, {ids[1]}) ORDER BY run_at"
                    )
                )
            ).all()
        ) == [second, scenario.job]

    barrier_key = 938188
    async with session_maker() as session, session.begin():
        await session.execute(
            text(f"""
            CREATE FUNCTION requeue_test_pause() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.id={second} AND OLD.status='processing' AND NEW.status='queued' THEN
                PERFORM pg_advisory_xact_lock({barrier_key});
              END IF;
              RETURN NEW;
            END $$
        """)
        )
        await session.execute(
            text(
                "CREATE TRIGGER requeue_test_pause BEFORE UPDATE ON message_jobs "
                "FOR EACH ROW EXECUTE FUNCTION requeue_test_pause()"
            )
        )

    monkeypatch.setattr(inbox_worker, "SessionLocal", session_maker)
    writer_started = asyncio.Event()
    requeue_started = asyncio.Event()
    pids = {}
    original_handle = inbox_worker.handle_event

    async def capture_writer(session, event):
        await session.execute(text("SET LOCAL deadlock_timeout = '100ms'"))
        pids["writer"] = await session.scalar(text("SELECT pg_backend_pid()"))
        writer_started.set()
        return await original_handle(session, event)

    monkeypatch.setattr(inbox_worker, "handle_event", capture_writer)

    async def requeue():
        async with session_maker() as session, session.begin():
            await _use_index_scan(session)
            await session.execute(text("SET LOCAL deadlock_timeout = '10s'"))
            pids["requeue"] = await session.scalar(text("SELECT pg_backend_pid()"))
            requeue_started.set()
            await outbox_worker._requeue_processing_jobs(session, [second, scenario.job])

    tasks = []
    try:
        async with session_maker() as barrier, barrier.begin():
            await barrier.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": barrier_key})
            tasks.append(asyncio.create_task(requeue()))
            await asyncio.wait_for(requeue_started.wait(), 10)
            await _wait_for_sql_lock(session_maker, pids["requeue"])
            async with session_maker() as session:
                assert await session.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted AND locktype='advisory')"
                    ),
                    {"pid": pids["requeue"]},
                )
            tasks.append(asyncio.create_task(inbox_worker.process_one_event(scenario.event)))
            await asyncio.wait_for(writer_started.wait(), 10)
            await _wait_for_sql_lock(session_maker, pids["writer"])
        await asyncio.wait_for(asyncio.gather(*tasks), 20)
        async with session_maker() as session, session.begin():
            event = await session.get(AltegioEvent, scenario.event)
            assert event.status == "processed" and event.error is None
            assert (await session.get(Record, scenario.record)).client_id == scenario.moved_card
            for job_id in ids:
                job = await session.get(MessageJob, job_id)
                assert job.status == "canceled" and job.locked_at is None
            assert list(await inbox_worker.lock_next_batch(session, 50)) == []
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        async with session_maker() as session, session.begin():
            await session.execute(text("DROP TRIGGER requeue_test_pause ON message_jobs"))
            await session.execute(text("DROP FUNCTION requeue_test_pause()"))


def _job(label, *, status="processing", provider="altegio"):
    now = datetime.now(timezone.utc)
    return MessageJob(
        company_id=1,
        client_id=1,
        provider=provider,
        job_type="campaign_message",
        status=status,
        locked_at=now,
        run_at=now - timedelta(minutes=1),
        dedupe_key=f"requeue-locks:{label}",
        payload={"contract": label},
        attempts=2,
        last_error="saved reason",
    )


async def test_batch_requeue_is_limited_idempotent_and_empty_safe(session_maker) -> None:
    async with session_maker() as session, session.begin():
        included = [_job("altegio"), _job("easyweek", provider="easyweek")]
        excluded = [_job("outside")]
        untouched = [_job(status, status=status) for status in ("queued", "done", "failed", "canceled", "running")]
        jobs = included + excluded + untouched
        session.add_all(jobs)
        await session.flush()
        ids = [job.id for job in included + untouched]
        before = {
            job.id: (job.status, job.locked_at, job.run_at, job.payload, job.attempts, job.last_error) for job in jobs
        }
    async with session_maker() as session, session.begin():
        await outbox_worker._requeue_processing_jobs(session, [])
        for job_id, expected in before.items():
            job = await session.get(MessageJob, job_id)
            assert (job.status, job.locked_at, job.run_at, job.payload, job.attempts, job.last_error) == expected
        await outbox_worker._requeue_processing_jobs(session, list(reversed(ids)) + ids)
    async with session_maker() as session, session.begin():
        await outbox_worker._requeue_processing_jobs(session, ids)
        for job_id, expected in before.items():
            job = await session.get(MessageJob, job_id)
            if job_id in {row.id for row in included}:
                expected = ("queued", None, *expected[2:])
            assert (job.status, job.locked_at, job.run_at, job.payload, job.attempts, job.last_error) == expected


async def test_batch_requeue_stays_in_the_callers_transaction(session_maker) -> None:
    async with session_maker() as session, session.begin():
        jobs = [_job("first"), _job("second")]
        session.add_all(jobs)
        await session.flush()
        ids = [job.id for job in jobs]
    async with session_maker() as session:
        transaction = await session.begin()
        await outbox_worker._requeue_processing_jobs(session, ids)
        assert transaction.is_active
        await transaction.rollback()
    async with session_maker() as session:
        for before in jobs:
            after = await session.get(MessageJob, before.id)
            assert after.status == "processing" and after.locked_at == before.locked_at


async def test_batch_requeue_does_not_expand_to_jobs_becoming_processing_while_it_waits(session_maker) -> None:
    async with session_maker() as session, session.begin():
        first, second = _job("first"), _job("second", status="queued")
        session.add_all([first, second])
        await session.flush()
        ids = [first.id, second.id]
    started = asyncio.Event()
    requeue_pid = None

    async def requeue():
        nonlocal requeue_pid
        async with session_maker() as session, session.begin():
            requeue_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            started.set()
            await outbox_worker._requeue_processing_jobs(session, ids)

    task = None
    try:
        async with session_maker() as holder, holder.begin():
            await holder.get(MessageJob, first.id, with_for_update=True)
            task = asyncio.create_task(requeue())
            await asyncio.wait_for(started.wait(), 10)
            await _wait_for_sql_lock(session_maker, requeue_pid)
            async with session_maker() as claimant, claimant.begin():
                job = await claimant.get(MessageJob, second.id, with_for_update=True)
                job.status = "processing"
        await asyncio.wait_for(task, 10)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with session_maker() as session:
        first_after = await session.get(MessageJob, first.id)
        second_after = await session.get(MessageJob, second.id)
        assert first_after.status == "queued" and first_after.locked_at is None
        assert second_after.status == "processing" and second_after.locked_at == second.locked_at


@pytest.mark.parametrize("new_status", ["done", "failed", "canceled", "queued"])
async def test_batch_requeue_rechecks_status_after_waiting(session_maker, new_status) -> None:
    async with session_maker() as session, session.begin():
        first, second = _job("first"), _job("second")
        session.add_all([first, second])
        await session.flush()
        ids = [first.id, second.id]
    started = asyncio.Event()
    requeue_pid = None

    async def requeue():
        nonlocal requeue_pid
        async with session_maker() as session, session.begin():
            requeue_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            started.set()
            await outbox_worker._requeue_processing_jobs(session, ids)

    task = None
    try:
        async with session_maker() as holder, holder.begin():
            job = await holder.get(MessageJob, first.id, with_for_update=True)
            job.status = new_status
            job.last_error = "concurrent decision"
            await holder.flush()
            task = asyncio.create_task(requeue())
            await asyncio.wait_for(started.wait(), 10)
            await _wait_for_sql_lock(session_maker, requeue_pid)
        await asyncio.wait_for(task, 10)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with session_maker() as session:
        first_after = await session.get(MessageJob, first.id)
        second_after = await session.get(MessageJob, second.id)
        assert first_after.status == new_status
        assert first_after.last_error == "concurrent decision"
        assert first_after.locked_at == first.locked_at
        assert second_after.status == "queued" and second_after.locked_at is None
