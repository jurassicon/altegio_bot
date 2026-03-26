"""
Скрипт для исправления смещения часового пояса в поле starts_at
для будущих записей (is_deleted=False, starts_at > now).

Altegio присылает в вебхуках поле "datetime" с некорректным UTC offset.
Из-за этого записи, обработанные до мерджа фикса, хранят starts_at со
смещением. Этот скрипт берёт корректное локальное время из поля raw["date"]
и пересчитывает starts_at/ends_at используя таймзону Europe/Belgrade.

Запуск:
  python -m altegio_bot.scripts.sync_future --dry-run   # только отчёт
  python -m altegio_bot.scripts.sync_future             # реальное обновление
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import Record

logger = logging.getLogger(__name__)
TZ = ZoneInfo("Europe/Belgrade")


def parse_local_date(raw: str | None) -> datetime | None:
    """Parse the ``date`` field from a webhook payload as naive local time.

    Stamps the result with the correct local timezone so that Python resolves
    the right DST offset, then converts to UTC.
    """
    if not raw:
        return None
    try:
        naive = datetime.fromisoformat(raw.strip().replace(" ", "T"))
        return naive.replace(tzinfo=TZ).astimezone(timezone.utc)
    except ValueError:
        return None


async def run(*, dry_run: bool) -> None:
    now_utc = datetime.now(timezone.utc)
    fixed = 0
    skipped = 0

    async with SessionLocal() as session:
        async with session.begin():
            stmt = select(Record).where(
                Record.is_deleted == False,  # noqa: E712
                Record.starts_at > now_utc,
            )
            result = await session.execute(stmt)
            records = result.scalars().all()

        logger.info("Записей для проверки: %d", len(records))

        # Build the list of changes before opening a write transaction so that
        # dry-run mode never touches the database.
        changes: list[tuple[Record, datetime, datetime | None]] = []
        for rec in records:
            raw_date = (rec.raw or {}).get("date")
            new_starts_at = parse_local_date(raw_date)

            if new_starts_at is None:
                logger.warning(
                    "record_id=%s altegio_id=%s: поле 'date' отсутствует в raw, пропускаем",
                    rec.id,
                    rec.altegio_record_id,
                )
                skipped += 1
                continue

            if rec.starts_at is None:
                skipped += 1
                continue

            delta_sec = (new_starts_at - rec.starts_at).total_seconds()
            if abs(delta_sec) < 1:
                skipped += 1
                continue

            new_ends_at = None
            if rec.duration_sec:
                new_ends_at = new_starts_at + timedelta(seconds=rec.duration_sec)

            logger.info(
                "record_id=%s altegio_id=%s company=%s: starts_at %s → %s (Δ=%.0f сек)",
                rec.id,
                rec.altegio_record_id,
                rec.company_id,
                rec.starts_at.isoformat(),
                new_starts_at.isoformat(),
                delta_sec,
            )

            changes.append((rec, new_starts_at, new_ends_at))
            fixed += 1

        if dry_run:
            logger.info("DRY-RUN: %d записей будет исправлено, транзакция НЕ открыта.", fixed)
        else:
            async with session.begin():
                for rec, new_starts_at, new_ends_at in changes:
                    rec.starts_at = new_starts_at
                    if new_ends_at is not None:
                        rec.ends_at = new_ends_at
            logger.info("Транзакция закоммичена.")

    logger.info(
        "Итого: исправлено=%d, пропущено=%d",
        fixed,
        skipped,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только отчёт — не писать в БД",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
