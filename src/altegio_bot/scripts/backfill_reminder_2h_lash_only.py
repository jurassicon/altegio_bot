from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select

from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import REMINDER_2H, add_job
from altegio_bot.models.models import MessageJob, Record
from altegio_bot.service_filter import (
    LASH_CATEGORY_IDS_BY_COMPANY,
    record_has_allowed_service,
)
from altegio_bot.utils import utcnow

logger = logging.getLogger("backfill_reminder_2h_lash_only")

EXACT_ALLOWED_CATEGORY_IDS = {
    10707687,
    12414859,
    13351956,
}


def apply_exact_category_filter() -> None:
    for company_id in list(LASH_CATEGORY_IDS_BY_COMPANY.keys()):
        LASH_CATEGORY_IDS_BY_COMPANY[company_id] = set(EXACT_ALLOWED_CATEGORY_IDS)


async def reminder_2h_exists(record_id: int, run_at) -> bool:
    async with SessionLocal() as session:
        stmt = (
            select(MessageJob.id)
            .where(MessageJob.record_id == record_id)
            .where(MessageJob.job_type == REMINDER_2H)
            .where(MessageJob.run_at == run_at)
            .limit(1)
        )
        res = await session.execute(stmt)
        return res.scalar_one_or_none() is not None


async def get_candidate_records(
    *,
    horizon_days: int,
    record_id: int | None,
    limit: int | None,
) -> list[Record]:
    now = utcnow()

    async with SessionLocal() as session:
        stmt = (
            select(Record)
            .where(Record.starts_at.is_not(None))
            .where(Record.starts_at > now + timedelta(hours=2))
            .where(Record.starts_at < now + timedelta(days=horizon_days))
            .where(Record.is_deleted.is_(False))
            .where(Record.client_id.is_not(None))
            .order_by(Record.starts_at.asc())
        )

        if record_id is not None:
            stmt = stmt.where(Record.id == record_id)

        if limit is not None:
            stmt = stmt.limit(limit)

        res = await session.execute(stmt)
        return list(res.scalars().all())


async def run_backfill(
    *,
    dry_run: bool,
    horizon_days: int,
    record_id: int | None,
    limit: int | None,
) -> None:
    apply_exact_category_filter()

    records = await get_candidate_records(
        horizon_days=horizon_days,
        record_id=record_id,
        limit=limit,
    )

    checked = 0
    skipped_existing = 0
    skipped_not_lash = 0
    inserted = 0

    async with SessionLocal() as session:
        async with session.begin():
            for record in records:
                checked += 1

                if record.starts_at is None:
                    continue

                run_at = record.starts_at - timedelta(hours=2)

                exists = await reminder_2h_exists(
                    record_id=int(record.id),
                    run_at=run_at,
                )
                if exists:
                    skipped_existing += 1
                    logger.info(
                        "Skip existing reminder_2h: record_id=%s staff=%s starts_at=%s run_at=%s",
                        record.id,
                        record.staff_name,
                        record.starts_at,
                        run_at,
                    )
                    continue

                allowed = await record_has_allowed_service(
                    session=session,
                    company_id=int(record.company_id),
                    record_id=int(record.id),
                )
                if not allowed:
                    skipped_not_lash += 1
                    logger.info(
                        "Skip not-lash record: record_id=%s staff=%s company_id=%s starts_at=%s",
                        record.id,
                        record.staff_name,
                        record.company_id,
                        record.starts_at,
                    )
                    continue

                logger.info(
                    "Candidate reminder_2h: record_id=%s staff=%s company_id=%s starts_at=%s run_at=%s",
                    record.id,
                    record.staff_name,
                    record.company_id,
                    record.starts_at,
                    run_at,
                )

                if dry_run:
                    continue

                await add_job(
                    session,
                    company_id=int(record.company_id),
                    record_id=int(record.id),
                    client_id=record.client_id,
                    job_type=REMINDER_2H,
                    run_at=run_at,
                    payload={"kind": REMINDER_2H},
                )
                inserted += 1

    logger.info(
        "Backfill finished: checked=%s inserted=%s skipped_existing=%s skipped_not_lash=%s dry_run=%s",
        checked,
        inserted,
        skipped_existing,
        skipped_not_lash,
        dry_run,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill reminder_2h only for lash categories.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only, do not insert jobs.",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=8,
        help="How many days ahead to scan.",
    )
    parser.add_argument(
        "--record-id",
        type=int,
        default=None,
        help="Process only one specific record_id.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of candidate records.",
    )
    return parser.parse_args()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    await run_backfill(
        dry_run=args.dry_run,
        horizon_days=args.horizon_days,
        record_id=args.record_id,
        limit=args.limit,
    )


if __name__ == "__main__":
    asyncio.run(main())
