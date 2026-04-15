from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.runner import (
    CAMPAIGN_EXECUTION_JOB_TYPE,
    execute_queued_send_real,
)
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageJob
from altegio_bot.utils import utcnow

logger = logging.getLogger("campaign_worker")

STALE_PROCESSING_MINUTES = 30


async def _lock_next_jobs(
    session: AsyncSession,
    batch_size: int,
) -> list[MessageJob]:
    now = utcnow()

    stmt = (
        select(MessageJob)
        .where(MessageJob.status == "queued")
        .where(MessageJob.job_type == CAMPAIGN_EXECUTION_JOB_TYPE)
        .where(MessageJob.run_at <= now)
        .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    jobs = list(result.scalars().all())

    for job in jobs:
        job.status = "processing"
        job.locked_at = now

    return jobs


async def _load_job(
    session: AsyncSession,
    job_id: int,
) -> MessageJob | None:
    stmt = (
        select(MessageJob)
        .where(MessageJob.id == job_id)
        .where(MessageJob.job_type == CAMPAIGN_EXECUTION_JOB_TYPE)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    if job is not None:
        return job

    exists_stmt = select(MessageJob.id).where(MessageJob.id == job_id)
    exists_result = await session.execute(exists_stmt)
    exists_id = exists_result.scalar_one_or_none()

    if exists_id is None:
        raise RuntimeError(f"MessageJob not found: id={job_id}")

    logger.info("Skip campaign job_id=%s (locked)", job_id)
    return None


async def _requeue_stale_processing_jobs(session: AsyncSession) -> int:
    cutoff = utcnow() - timedelta(minutes=STALE_PROCESSING_MINUTES)

    stmt = (
        update(MessageJob)
        .where(MessageJob.status == "processing")
        .where(MessageJob.job_type == CAMPAIGN_EXECUTION_JOB_TYPE)
        .where(MessageJob.locked_at.is_not(None))
        .where(MessageJob.locked_at < cutoff)
        .values(
            status="queued",
            locked_at=None,
            run_at=utcnow(),
            last_error="Recovered: stale campaign execution job",
        )
    )
    result = await session.execute(stmt)
    return int(getattr(result, "rowcount", 0) or 0)


async def process_job_in_session(
    session: AsyncSession,
    job_id: int,
) -> None:
    job = await _load_job(session, job_id)
    if job is None:
        return

    payload = getattr(job, "payload", None) or {}
    run_id = payload.get("campaign_run_id")

    if run_id is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "Missing campaign_run_id in payload"
        logger.error("campaign job_id=%s: missing campaign_run_id in payload", job.id)
        return

    logger.info(
        "picked campaign execution job_id=%s run_id=%s",
        job.id,
        run_id,
    )

    try:
        await execute_queued_send_real(int(run_id))
    except Exception as exc:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"Campaign execution failed: {exc}"
        logger.exception(
            "campaign execution failed job_id=%s run_id=%s: %s",
            job.id,
            run_id,
            exc,
        )
        return

    job.status = "done"
    job.locked_at = None
    job.last_error = None

    logger.info(
        "campaign execution done job_id=%s run_id=%s",
        job.id,
        run_id,
    )


async def process_job(job_id: int) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            await process_job_in_session(session, job_id)


async def run_loop(
    batch_size: int = 1,
    poll_sec: float = 1.0,
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("Campaign worker started")

    while True:
        async with SessionLocal() as session:
            async with session.begin():
                recovered = await _requeue_stale_processing_jobs(session)
                if recovered:
                    logger.warning(
                        "Recovered stale campaign execution jobs: %s",
                        recovered,
                    )

        async with SessionLocal() as session:
            async with session.begin():
                jobs = await _lock_next_jobs(session, batch_size)
                job_ids = [job.id for job in jobs]

        if not job_ids:
            await asyncio.sleep(poll_sec)
            continue

        for job_id in job_ids:
            await process_job(job_id)


async def run_once(
    session_maker,
    *,
    limit: int = 1,
) -> int:
    async with session_maker() as session:
        async with session.begin():
            await _requeue_stale_processing_jobs(session)

        stmt = (
            select(MessageJob.id)
            .where(MessageJob.status == "queued")
            .where(MessageJob.job_type == CAMPAIGN_EXECUTION_JOB_TYPE)
            .where(MessageJob.run_at <= utcnow())
            .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        job_ids = list(result.scalars().all())

        for job_id in job_ids:
            await process_job_in_session(session, int(job_id))

        await session.commit()
        return len(job_ids)


if __name__ == "__main__":
    asyncio.run(run_loop())
