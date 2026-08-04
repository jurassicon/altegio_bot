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
from altegio_bot.delivery_retry_identity import claims_delivery_retry, resolve_retry_reference
from altegio_bot.easyweek_policy import easyweek_job_type_error, normalize_provider
from altegio_bot.models.models import PROVIDER_ALTEGIO, MessageJob
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

    # Independent boundary: campaign execution has its own worker and never
    # passes through outbox_worker. A retry-like row is terminal before its
    # payload can be interpreted as a campaign run.
    if claims_delivery_retry(job):
        reference = resolve_retry_reference(job)
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"Rejected delivery retry claim: {reference.error or 'campaign_route_forbidden'}"
        logger.error("Campaign job rejected a delivery retry claim job_id=%s", job.id)
        return

    # Fail-closed provider guard, before the payload is trusted and before the
    # campaign runner is reached.
    #
    # The guard in outbox_worker._run_job_logic does NOT cover this path:
    # campaign execution jobs are excluded from the outbox claim and picked up
    # here instead. Campaigns are Altegio-only in PR-5 — the runner resolves
    # recipients through Altegio client ids and calls the Altegio API — so an
    # EasyWeek execution job must die here rather than start a campaign run
    # against the wrong CRM.
    job_provider = normalize_provider(getattr(job, "provider", None), default=PROVIDER_ALTEGIO)
    job_type_err = easyweek_job_type_error(job_provider, job.job_type)
    if job_type_err is not None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = job_type_err
        logger.error(
            "EasyWeek job type not enabled: campaign job_id=%s company=%s job_type=%s",
            job.id,
            job.company_id,
            job.job_type,
        )
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
