"""
Отменяет задачи review_3d (status='queued') для клиентов,
у которых более MAX_VISITS_FOR_REVIEW подтверждённых визитов (attendance=1 или
visit_attendance=1) в данном филиале.

Используется для бэкфилла: задачи, созданные до введения фильтра на стороне
планировщика, могут относиться к «не-новым» клиентам и должны быть отменены.

Запуск (dry-run по умолчанию):
    python -m altegio_bot.scripts.cancel_review_3d_over_visit_limit \\
        --company-id 758285 --dry-run

Реальная отмена:
    python -m altegio_bot.scripts.cancel_review_3d_over_visit_limit \\
        --company-id 758285
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW, REVIEW_3D
from altegio_bot.models.models import MessageJob, Record

logger = logging.getLogger("cancel_review_3d_over_visit_limit")

CANCEL_REASON = f"Skipped: client has >{MAX_VISITS_FOR_REVIEW} attended visits (backfill)"


async def _count_attended_visits(
    session: AsyncSession,
    *,
    client_id: int,
    company_id: int,
) -> int:
    """Количество подтверждённых визитов клиента в филиале (не удалённых)."""
    stmt = (
        select(Record.id)
        .where(Record.client_id == client_id)
        .where(Record.company_id == company_id)
        .where(Record.is_deleted.is_(False))
        .where(
            or_(
                Record.attendance == 1,
                Record.visit_attendance == 1,
            )
        )
    )
    result = await session.execute(stmt)
    return len(result.all())


async def run_cancel(
    *,
    company_id: int,
    dry_run: bool,
    limit: int | None,
) -> None:
    # ── Phase 1: read-only — collect job IDs and per-client attended counts ──
    job_rows: list[tuple[int, int | None, int | None]] = []  # (job_id, client_id, record_id)

    async with SessionLocal() as session:
        stmt = (
            select(MessageJob.id, MessageJob.client_id, MessageJob.record_id)
            .where(MessageJob.company_id == company_id)
            .where(MessageJob.job_type == REVIEW_3D)
            .where(MessageJob.status == "queued")
            .order_by(MessageJob.id.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        result = await session.execute(stmt)
        job_rows = list(result.all())

    logger.info(
        "Found %d queued review_3d jobs for company_id=%d (limit=%s)",
        len(job_rows),
        company_id,
        limit,
    )

    checked = 0
    skipped_no_client = 0
    kept = 0
    ids_to_cancel: list[int] = []

    async with SessionLocal() as session:
        for job_id, client_id, record_id in job_rows:
            checked += 1

            if client_id is None:
                skipped_no_client += 1
                logger.warning(
                    "Skip job id=%d: client_id is NULL (record_id=%s)",
                    job_id,
                    record_id,
                )
                continue

            attended = await _count_attended_visits(
                session,
                client_id=client_id,
                company_id=company_id,
            )

            if attended > MAX_VISITS_FOR_REVIEW:
                logger.info(
                    "CANCEL job id=%d client_id=%d record_id=%s attended=%d dry_run=%s",
                    job_id,
                    client_id,
                    record_id,
                    attended,
                    dry_run,
                )
                ids_to_cancel.append(job_id)
            else:
                logger.debug(
                    "Keep job id=%d client_id=%d record_id=%s attended=%d",
                    job_id,
                    client_id,
                    record_id,
                    attended,
                )
                kept += 1

    # ── Phase 2: write — cancel collected jobs in a single short transaction ──
    if ids_to_cancel and not dry_run:
        async with SessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(MessageJob)
                    .where(MessageJob.id.in_(ids_to_cancel))
                    .where(MessageJob.status == "queued")
                    .values(status="canceled", last_error=CANCEL_REASON)
                )

    logger.info(
        "Done: checked=%d canceled=%d kept=%d skipped_no_client=%d dry_run=%s",
        checked,
        len(ids_to_cancel),
        kept,
        skipped_no_client,
        dry_run,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel queued review_3d jobs for clients with >3 attended visits.",
    )
    parser.add_argument(
        "--company-id",
        type=int,
        required=True,
        help="ID of the company (branch) to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only — do not cancel any jobs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N queued jobs.",
    )
    return parser.parse_args()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    await run_cancel(
        company_id=args.company_id,
        dry_run=args.dry_run,
        limit=args.limit,
    )


if __name__ == "__main__":
    asyncio.run(main())
