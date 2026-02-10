from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, MessageJob, Record

UPDATE_DEBOUNCE_SEC = 60


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dedupe_key(
    job_type: str,
    record_id: int,
    run_at: datetime,
    *,
    now: datetime | None = None,
) -> str:
    """
    Dedupe rules:
    - record_updated: dedupe внутри окна debounce (bucket), но допускаем
      новые update-job'ы в следующих окнах.
    - остальные: dedupe по (job_type, record_id, run_at).
    """
    if job_type == "record_updated":
        base_dt = now or run_at
        bucket = int(base_dt.timestamp()) // UPDATE_DEBOUNCE_SEC
        return f"{job_type}:{record_id}:{bucket}"

    return f"{job_type}:{record_id}:{run_at.isoformat()}"


async def cancel_queued_jobs(
    session: AsyncSession,
    record_id: int,
    job_types: Iterable[str],
) -> None:
    stmt = (
        update(MessageJob)
        .where(MessageJob.record_id == record_id)
        .where(MessageJob.job_type.in_(list(job_types)))
        .where(MessageJob.status == "queued")
        .values(status="canceled")
    )
    await session.execute(stmt)


async def enqueue_job(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
    client_id: int | None,
    job_type: str,
    run_at: datetime,
    now: datetime | None = None,
) -> None:
    insert_stmt = pg_insert(MessageJob).values(
        dedupe_key=_dedupe_key(
            job_type,
            record_id,
            run_at,
            now=now,
        ),
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=job_type,
        run_at=run_at,
        status="queued",
    )

    excluded = insert_stmt.excluded

    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["dedupe_key"],
        set_={
            "company_id": excluded.company_id,
            "record_id": excluded.record_id,
            "client_id": excluded.client_id,
            "job_type": excluded.job_type,
            "run_at": excluded.run_at,
            "status": "queued",
        },
        where=MessageJob.status == "canceled",
    )
    await session.execute(stmt)


async def plan_jobs_for_record_event(
    session: AsyncSession,
    *,
    record: Record,
    client: Client | None,
    event_status: str,
) -> None:
    now = utcnow()
    client_id = client.id if client is not None else None

    if event_status == "create":
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="record_created",
            run_at=now,
            now=now,
        )
        await _plan_reminders(session, record, client_id, now)
        return

    if event_status == "update":
        await cancel_queued_jobs(
            session,
            record_id=record.id,
            job_types=("record_updated", "reminder_24h", "reminder_2h"),
        )
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="record_updated",
            run_at=now + timedelta(seconds=UPDATE_DEBOUNCE_SEC),
            now=now,
        )
        await _plan_reminders(session, record, client_id, now)
        return

    if event_status == "delete":
        await cancel_queued_jobs(
            session,
            record_id=record.id,
            job_types=("reminder_24h", "reminder_2h"),
        )
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="record_canceled",
            run_at=now,
            now=now,
        )
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="comeback_3d",
            run_at=now + timedelta(days=3),
            now=now,
        )
        return


async def _plan_reminders(
    session: AsyncSession,
    record: Record,
    client_id: int | None,
    now: datetime,
) -> None:
    if record.starts_at is None:
        return

    delta = record.starts_at - now

    if delta <= timedelta(hours=2):
        return

    if delta > timedelta(hours=24):
        run_24h = record.starts_at - timedelta(hours=24)
        if run_24h > now:
            await enqueue_job(
                session,
                company_id=record.company_id,
                record_id=record.id,
                client_id=client_id,
                job_type="reminder_24h",
                run_at=run_24h,
                now=now,
            )
        return

    run_2h = record.starts_at - timedelta(hours=2)
    if run_2h > now:
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="reminder_2h",
            run_at=run_2h,
            now=now,
        )
