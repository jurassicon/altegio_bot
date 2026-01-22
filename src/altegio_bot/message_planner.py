from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, MessageJob, Record


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dedupe_key(job_type: str, record_id: int, run_at: datetime) -> str:
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
) -> None:
    stmt = pg_insert(MessageJob).values(
        dedupe_key=_dedupe_key(job_type, record_id, run_at),
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=job_type,
        run_at=run_at,
        status="queued",
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["dedupe_key"])
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
        )
        await _plan_reminders(session, record, client_id, now)
        return

    if event_status == "update":
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
            job_type="record_updated",
            run_at=now,
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
        )
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="comeback_3d",
            run_at=now + timedelta(days=3),
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

    run_24h = record.starts_at - timedelta(hours=24)
    run_2h = record.starts_at - timedelta(hours=2)

    if run_24h > now:
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="reminder_24h",
            run_at=run_24h,
        )

    if run_2h > now:
        await enqueue_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type="reminder_2h",
            run_at=run_2h,
        )
