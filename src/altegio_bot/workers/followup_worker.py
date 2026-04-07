"""Автоматический планировщик follow-up для send-real campaign run'ов.

Периодически опрашивает БД, находит completed send-real run'ы,
у которых истёк followup_delay_days с момента завершения, и автоматически
выполняет plan_followup + execute_followup.

Состояние хранится в run.meta (без миграций):
  followup_auto_status:        "processing" | "completed" | "failed"
  followup_auto_started_at:    ISO timestamp
  followup_auto_completed_at:  ISO timestamp
  followup_auto_last_error:    строка ошибки (только при "failed")
  followup_auto_planned_count: int
  followup_auto_queued_count:  int

Политика ошибок:
  - Ошибка одного run не обрывает worker.
  - После ошибки статус = "failed", авто-retry не делается.
  - Для повторного запуска оператор использует ручные endpoint'ы:
    POST /ops/campaigns/runs/{run_id}/followup/plan
    POST /ops/campaigns/runs/{run_id}/followup/run-now
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.followup import execute_followup, plan_followup
from altegio_bot.campaigns.runner import CAMPAIGN_CODE
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import CampaignRun
from altegio_bot.utils import utcnow

logger = logging.getLogger("followup_worker")


async def _claim_due_runs(session: AsyncSession, *, limit: int = 5) -> list[int]:
    """Найти и атомарно забрать due runs для авто-follow-up.

    Run считается due, если:
      completed_at + followup_delay_days * interval '1 day' <= now

    Атомарность обеспечивается за счёт SELECT FOR UPDATE SKIP LOCKED:
    два параллельных worker'а не заберут один и тот же run.

    Возвращает список run_id.
    """
    now = utcnow()

    stmt = (
        select(CampaignRun)
        .where(
            and_(
                CampaignRun.campaign_code == CAMPAIGN_CODE,
                CampaignRun.mode == "send-real",
                CampaignRun.status == "completed",
                CampaignRun.followup_enabled.is_(True),
                CampaignRun.followup_delay_days.is_not(None),
                CampaignRun.followup_template_name.is_not(None),
                CampaignRun.completed_at.is_not(None),
                # due: completed_at + N days <= now
                text(
                    "campaign_runs.completed_at + (campaign_runs.followup_delay_days * interval '1 day') <= :now"
                ).bindparams(now=now),
                # ещё не обработан авто-worker'ом
                or_(
                    CampaignRun.meta.is_(None),
                    CampaignRun.meta["followup_auto_status"].astext.is_(None),
                ),
            )
        )
        .order_by(CampaignRun.completed_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    runs = (await session.execute(stmt)).scalars().all()
    if not runs:
        return []

    started_at = now.isoformat()
    run_ids: list[int] = []
    for run in runs:
        meta = dict(run.meta or {})
        meta["followup_auto_status"] = "processing"
        meta["followup_auto_started_at"] = started_at
        run.meta = meta
        run_ids.append(run.id)

    return run_ids


async def process_run(run_id: int) -> None:
    """Обработать один run: plan + execute + записать результат в meta.

    При ошибке — записывает в meta["followup_auto_last_error"] и возвращает.
    Исключение наружу не бросается: worker не должен умирать из-за одного run.
    """
    logger.info("followup_worker: processing run_id=%d", run_id)

    try:
        # 1. Планируем: помечаем eligible получателей как followup_planned
        async with SessionLocal() as session:
            async with session.begin():
                planned_count = await plan_followup(session, run_id)

        logger.info(
            "followup_worker: planned run_id=%d planned=%d",
            run_id,
            planned_count,
        )

        # 2. Выполняем: создаём MessageJob для каждого followup_planned получателя
        stats = await execute_followup(run_id)
        queued_count = stats.get("queued", 0)

        logger.info(
            "followup_worker: executed run_id=%d stats=%s",
            run_id,
            stats,
        )

        # 3. Записываем успех
        async with SessionLocal() as session:
            async with session.begin():
                run = await session.get(CampaignRun, run_id)
                if run is not None:
                    meta = dict(run.meta or {})
                    meta["followup_auto_status"] = "completed"
                    meta["followup_auto_completed_at"] = utcnow().isoformat()
                    meta["followup_auto_planned_count"] = planned_count
                    meta["followup_auto_queued_count"] = queued_count
                    meta.pop("followup_auto_last_error", None)
                    run.meta = meta

        logger.info(
            "followup_worker: done run_id=%d queued=%d",
            run_id,
            queued_count,
        )

    except Exception as exc:
        logger.exception(
            "followup_worker: failed run_id=%d: %s",
            run_id,
            exc,
        )

        # Статус "failed" финален: авто-retry не делается.
        # Повторный запуск — только вручную через /followup/plan + /followup/run-now.
        try:
            async with SessionLocal() as session:
                async with session.begin():
                    run = await session.get(CampaignRun, run_id)
                    if run is not None:
                        meta = dict(run.meta or {})
                        meta["followup_auto_status"] = "failed"
                        meta["followup_auto_last_error"] = str(exc)
                        run.meta = meta
        except Exception as inner_exc:
            logger.error(
                "followup_worker: cannot record error for run_id=%d: %s",
                run_id,
                inner_exc,
            )


async def run_loop(poll_sec: float = 60.0) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("Follow-up worker started (poll_sec=%.0f)", poll_sec)

    while True:
        try:
            async with SessionLocal() as session:
                async with session.begin():
                    run_ids = await _claim_due_runs(session)

            if run_ids:
                logger.info(
                    "followup_worker: claimed %d run(s): %s",
                    len(run_ids),
                    run_ids,
                )
                for run_id in run_ids:
                    await process_run(run_id)
            else:
                logger.debug("followup_worker: no due runs")

        except Exception as exc:
            logger.exception("followup_worker: loop error: %s", exc)

        await asyncio.sleep(poll_sec)


if __name__ == "__main__":
    asyncio.run(run_loop())
