from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import update, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import MessageJob, Record

UPDATE_DEBOUNCE_SEC = 60

FOLLOWUP_REPEAT_DAYS = 10
REVIEW_REQUEST_DAYS = 3

FOLLOWUP_JOB_TYPES = (
    'repeat_10d',
    'review_3d',
)

REMINDER_JOB_TYPES = (
    'reminder_24h',
    'reminder_2h',
)

SYSTEM_JOB_TYPES = (
    'record_created',
    'record_updated',
    'record_canceled',
) + REMINDER_JOB_TYPES + ('comeback_3d',) + FOLLOWUP_JOB_TYPES


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
        *,
        record_id: int,
        job_types: list[str],
) -> int:
    if not job_types:
        return 0

    stmt = (
        update(MessageJob)
        .where(MessageJob.record_id == record_id)
        .where(MessageJob.job_type.in_(job_types))
        .where(MessageJob.status == 'queued')
        .values(
            status='canceled',
            updated_at=utcnow(),
        )
    )
    res = await session.execute(stmt)
    return int(getattr(res, 'rowcount', 0) or 0)


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
        company_id: int,
        record_id: int,
        status: str,
) -> None:
    stmt = (
        select(Record)
        .where(Record.company_id == company_id)
        .where(Record.id == record_id)
        .limit(1)
    )
    res = await session.execute(stmt)
    record = res.scalar_one_or_none()
    if record is None:
        return

    client_id = record.client_id

    if status == 'create':
        await add_job(
            session,
            company_id=company_id,
            record_id=record.id,
            client_id=client_id,
            job_type='record_created',
            run_at=utcnow(),
            payload={'kind': 'record_created'},
        )
        await _plan_reminders(session, record=record, client_id=client_id)
        await _plan_followups(session, record=record, client_id=client_id)
        return

    if status == 'update':
        await cancel_queued_jobs(
            session,
            record_id=record.id,
            job_types=list(SYSTEM_JOB_TYPES),
        )

        await add_job(
            session,
            company_id=company_id,
            record_id=record.id,
            client_id=client_id,
            job_type='record_updated',
            run_at=utcnow(),
            payload={'kind': 'record_updated'},
        )
        await _plan_reminders(session, record=record, client_id=client_id)
        await _plan_followups(session, record=record, client_id=client_id)
        return

    if status == 'delete':
        await cancel_queued_jobs(
            session,
            record_id=record.id,
            job_types=list(SYSTEM_JOB_TYPES),
        )

        await add_job(
            session,
            company_id=company_id,
            record_id=record.id,
            client_id=client_id,
            job_type='record_canceled',
            run_at=utcnow(),
            payload={'kind': 'record_canceled'},
        )

        await add_job(
            session,
            company_id=company_id,
            record_id=record.id,
            client_id=client_id,
            job_type='comeback_3d',
            run_at=utcnow() + timedelta(days=3),
            payload={'kind': 'comeback_3d'},
        )
        return


async def _plan_reminders(
        session: AsyncSession,
        *,
        record: Record,
        client_id: int | None,
) -> None:
    now = utcnow()

    if record.starts_at is None:
        return

    delta = record.starts_at - now

    reminder_at_24h = record.starts_at - timedelta(hours=24)
    reminder_at_2h = record.starts_at - timedelta(hours=2)

    if delta > timedelta(hours=24):
        await add_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type='reminder_24h',
            run_at=reminder_at_24h,
            payload={'kind': 'reminder_24h'},
        )
        return

    if delta > timedelta(hours=2):
        await add_job(
            session,
            company_id=record.company_id,
            record_id=record.id,
            client_id=client_id,
            job_type='reminder_2h',
            run_at=reminder_at_2h,
            payload={'kind': 'reminder_2h'},
        )
        return


async def add_job(
        session: AsyncSession,
        *,
        company_id: int,
        record_id: int,
        client_id: int | None,
        job_type: str,
        run_at: Any,
        payload: dict[str, Any],
) -> MessageJob:
    dedupe_key = f'{job_type}:{company_id}:{record_id}:{run_at}'

    job = MessageJob(
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=job_type,
        run_at=run_at,
        status='queued',
        dedupe_key=dedupe_key,
        payload=payload,
    )
    session.add(job)
    return job


async def _plan_followups(
        session: AsyncSession,
        *,
        record: Record,
        client_id: int | None,
) -> None:
    if record.starts_at is None:
        return

    await add_job(
        session,
        company_id=record.company_id,
        record_id=record.id,
        client_id=client_id,
        job_type='review_3d',
        run_at=record.starts_at + timedelta(days=REVIEW_REQUEST_DAYS),
        payload={'kind': 'review_3d'},
    )

    await add_job(
        session,
        company_id=record.company_id,
        record_id=record.id,
        client_id=client_id,
        job_type='repeat_10d',
        run_at=record.starts_at + timedelta(days=FOLLOWUP_REPEAT_DAYS),
        payload={'kind': 'repeat_10d'},
    )
