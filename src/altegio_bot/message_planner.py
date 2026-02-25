from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, MessageJob, MessageTemplate, Record
from altegio_bot.utils import utcnow

REMINDER_24H = 'reminder_24h'
REVIEW_3D = 'review_3d'
REPEAT_10D = 'repeat_10d'
COMEBACK_3D = 'comeback_3d'
COMEBACK_14D = 'comeback_14d'
COMEBACK_30D = 'comeback_30d'
REMINDER_2H = 'reminder_2h'
FIRST_VISIT_REMINDER = 'first_visit_reminder'

SYSTEM_JOB_TYPES = (
    REMINDER_24H,
    REVIEW_3D,
    REPEAT_10D,
    COMEBACK_3D,
    COMEBACK_14D,
    COMEBACK_30D,
    REMINDER_2H,
    FIRST_VISIT_REMINDER,
)


def make_dedupe_key(
    *,
    job_type: str,
    company_id: int,
    record_id: int,
    run_at: datetime,
) -> str:
    return f'{job_type}:{company_id}:{record_id}:{run_at.isoformat()}'


async def cancel_queued_jobs(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
) -> int:
    stmt = (
        update(MessageJob)
        .where(MessageJob.company_id == company_id)
        .where(MessageJob.record_id == record_id)
        .where(MessageJob.job_type.in_(SYSTEM_JOB_TYPES))
        .where(MessageJob.status == 'queued')
        .values(status='canceled', updated_at=utcnow())
    )
    res = await session.execute(stmt)
    return int(res.rowcount or 0)


async def delete_jobs(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
) -> int:
    stmt = (
        delete(MessageJob)
        .where(MessageJob.company_id == company_id)
        .where(MessageJob.record_id == record_id)
        .where(MessageJob.job_type.in_(SYSTEM_JOB_TYPES))
    )
    res = await session.execute(stmt)
    return int(res.rowcount or 0)


async def enqueue_job(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
    client_id: int,
    job_type: str,
    run_at: datetime,
    payload: dict[str, Any],
) -> None:
    dedupe_key = make_dedupe_key(
        job_type=job_type,
        company_id=company_id,
        record_id=record_id,
        run_at=run_at,
    )

    stmt = pg_insert(MessageJob).values(
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=job_type,
        run_at=run_at,
        status='queued',
        last_error=None,
        dedupe_key=dedupe_key,
        payload=payload,
        locked_at=None,
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[MessageJob.dedupe_key],
        set_={
            'status': 'queued',
            'last_error': None,
            'locked_at': None,
            'payload': stmt.excluded.payload,
            'updated_at': utcnow(),
        },
        where=MessageJob.status.in_(('canceled', 'failed')),
    )

    await session.execute(stmt)


async def enqueue_followup(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
    client_id: int,
    code: str,
    run_at: datetime,
) -> None:
    await enqueue_job(
        session,
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=code,
        run_at=run_at,
        payload={'kind': code},
    )


async def get_template(
    session: AsyncSession,
    *,
    company_id: int,
    code: str,
    language: str,
) -> MessageTemplate | None:
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == code)
        .where(MessageTemplate.language == language)
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _has_template(
    session: AsyncSession,
    *,
    company_id: int,
    code: str,
    language: str,
) -> bool:
    return await get_template(
        session,
        company_id=company_id,
        code=code,
        language=language,
    ) is not None


async def _should_send_followups(
    session: AsyncSession,
    *,
    company_id: int,
    client_id: int,
) -> bool:
    client = await session.get(Client, client_id)
    if client is None:
        return False
    if client.whatsapp_opt_out:
        return False
    return True


async def _plan_reminders(
    session: AsyncSession,
    *,
    company_id: int,
    record: Record,
    client_id: int,
) -> None:
    if record.starts_at is None:
        return

    starts_at = record.starts_at

    run_at_24h = starts_at - timedelta(hours=24)
    if run_at_24h > utcnow():
        await add_job(
            session,
            company_id=company_id,
            record_id=int(record.id),
            client_id=client_id,
            job_type=REMINDER_24H,
            run_at=run_at_24h,
            payload={'kind': REMINDER_24H},
        )

    run_at_2h = starts_at - timedelta(hours=2)
    if run_at_2h > utcnow():
        await add_job(
            session,
            company_id=company_id,
            record_id=int(record.id),
            client_id=client_id,
            job_type=REMINDER_2H,
            run_at=run_at_2h,
            payload={'kind': REMINDER_2H},
        )


async def _plan_followups(
    session: AsyncSession,
    *,
    company_id: int,
    record: Record,
    client_id: int,
    language: str,
) -> None:
    if record.starts_at is None:
        return

    if not await _should_send_followups(
        session,
        company_id=company_id,
        client_id=client_id,
    ):
        return

    starts_at = record.starts_at

    review_at = starts_at + timedelta(days=3)
    if review_at > utcnow():
        if await _has_template(
            session,
            company_id=company_id,
            code=REVIEW_3D,
            language=language,
        ):
            await enqueue_followup(
                session,
                company_id=company_id,
                record_id=int(record.id),
                client_id=client_id,
                code=REVIEW_3D,
                run_at=review_at,
            )

    repeat_at = starts_at + timedelta(days=10)
    if repeat_at > utcnow():
        if await _has_template(
            session,
            company_id=company_id,
            code=REPEAT_10D,
            language=language,
        ):
            await enqueue_followup(
                session,
                company_id=company_id,
                record_id=int(record.id),
                client_id=client_id,
                code=REPEAT_10D,
                run_at=repeat_at,
            )


async def plan_jobs_for_record_event(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
    client_id: int,
    event_kind: str,
) -> None:
    record = await session.get(Record, record_id)
    if record is None:
        return

    client = await session.get(Client, client_id)
    if client is None:
        return

    language = client.language or 'de'

    if event_kind == 'record_updated':
        await cancel_queued_jobs(
            session,
            company_id=company_id,
            record_id=record_id,
        )

    if event_kind in ('record_created', 'record_updated'):
        if record.is_first_visit:
            if record.starts_at is not None:
                first_visit_at = record.starts_at - timedelta(hours=24)
                if first_visit_at > utcnow():
                    await add_job(
                        session,
                        company_id=company_id,
                        record_id=record_id,
                        client_id=client_id,
                        job_type=FIRST_VISIT_REMINDER,
                        run_at=first_visit_at,
                        payload={'kind': FIRST_VISIT_REMINDER},
                    )

        await _plan_reminders(
            session,
            company_id=company_id,
            record=record,
            client_id=client_id,
        )

        await _plan_followups(
            session,
            company_id=company_id,
            record=record,
            client_id=client_id,
            language=language,
        )


async def add_job(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
    client_id: int,
    job_type: str,
    run_at: datetime,
    payload: dict[str, Any],
) -> None:
    '''Create a job idempotently.

    Inbox/outbox workers can receive duplicates (retries, webhook repeats, etc).
    We keep `dedupe_key` as a unique constraint and use UPSERT to avoid worker
    crashes on IntegrityError.

    If the job already exists and was previously canceled/failed, we re-queue
    it (same dedupe_key means the schedule is the same).
    '''
    dedupe_key = make_dedupe_key(
        job_type=job_type,
        company_id=company_id,
        record_id=record_id,
        run_at=run_at,
    )

    stmt = pg_insert(MessageJob).values(
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=job_type,
        run_at=run_at,
        status='queued',
        last_error=None,
        dedupe_key=dedupe_key,
        payload=payload,
        locked_at=None,
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[MessageJob.dedupe_key],
        set_={
            'status': 'queued',
            'last_error': None,
            'locked_at': None,
            'payload': stmt.excluded.payload,
            'updated_at': utcnow(),
        },
        where=MessageJob.status.in_(('canceled', 'failed')),
    )

    await session.execute(stmt)