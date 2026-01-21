from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import AltegioEvent


logger = logging.getLogger("inbox_worker")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _lock_next_batch(session: AsyncSession, batch_size: int) -> Sequence[AltegioEvent]:
    """
    Берём пачку событий со статусом 'received' и лочим их.
    FOR UPDATE SKIP LOCKED позволяет нескольким воркерам работать параллельно без дублей.
    """
    stmt = (
        select(AltegioEvent)
        .where(AltegioEvent.status == "received")
        .order_by(AltegioEvent.received_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    events = list(res.scalars().all())

    # помечаем как processing внутри той же транзакции
    for e in events:
        e.status = "processing"

    return events


async def handle_event(session: AsyncSession, event: AltegioEvent) -> None:
    """
    Здесь позже будет реальная бизнес-логика:
    - upsert client/record/services
    - planner jobs
    - analytics
    Сейчас: заглушка, чтобы проверить конвейер.
    """
    # Пример: можно логировать минимум
    logger.info(
        "Handling event id=%s company=%s resource=%s resource_id=%s status=%s",
        event.id,
        event.company_id,
        event.resource,
        event.resource_id,
        event.event_status,
    )
    # ничего не делаем


async def process_one_event(event_id: int) -> None:
    """
    Обрабатываем одно событие в отдельной транзакции.
    Так безопаснее: одно упало — другие не блокируются.
    """
    async with SessionLocal() as session:
        async with session.begin():
            # Лочим конкретное событие, чтобы два процесса не обработали одно и то же
            stmt = select(AltegioEvent).where(AltegioEvent.id == event_id).with_for_update()
            res = await session.execute(stmt)
            event = res.scalar_one_or_none()

            if event is None:
                return

            try:
                await handle_event(session, event)
                event.status = "processed"
                event.processed_at = utcnow()
                event.error = None
            except Exception as e:
                event.status = "failed"
                event.error = str(e)
                event.processed_at = utcnow()
                logger.exception("Event failed id=%s: %s", event_id, e)


async def run_loop(batch_size: int = 50, poll_interval_sec: float = 1.0) -> None:
    logger.info("Inbox worker started. batch_size=%s poll=%ss", batch_size, poll_interval_sec)

    while True:
        # 1) Забираем пачку событий и помечаем processing
        event_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                events = await _lock_next_batch(session, batch_size)
                event_ids = [e.id for e in events]

        if not event_ids:
            await asyncio.sleep(poll_interval_sec)
            continue

        # 2) Обрабатываем каждое событие отдельно (можно будет распараллелить позже)
        for eid in event_ids:
            await process_one_event(eid)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run_loop())


if __name__ == "__main__":
    main()
