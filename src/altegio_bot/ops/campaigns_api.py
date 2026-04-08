"""Ops JSON API для управления кампаниями рассылок.

Все endpoint'ы защищены require_ops_auth (та же авторизация,
что и в ops/router.py).

Endpoint'ы:
  GET  /ops/campaigns/new-clients/card-types
  POST /ops/campaigns/new-clients/preview
  POST /ops/campaigns/new-clients/run
  GET  /ops/campaigns/runs
  GET  /ops/campaigns/runs/{run_id}
  GET  /ops/campaigns/runs/{run_id}/progress
  POST /ops/campaigns/runs/{run_id}/resume
  GET  /ops/campaigns/runs/{run_id}/recipients
  GET  /ops/campaigns/runs/{run_id}/report
  GET  /ops/campaigns/dashboard/monthly
  POST /ops/campaigns/runs/{run_id}/followup/plan
  POST /ops/campaigns/runs/{run_id}/followup/run-now
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.campaigns.followup import execute_followup, followup_run_at, plan_followup
from altegio_bot.campaigns.reports import monthly_dashboard, run_report
from altegio_bot.campaigns.runner import (
    CAMPAIGN_CODE,
    CAMPAIGN_EXECUTION_JOB_TYPE,
    RunParams,
    discard_preview_run,
    enqueue_send_real,
    resume_send_real,
    run_preview,
)
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import CampaignRecipient, CampaignRun, MessageJob, MessageTemplate
from altegio_bot.ops.auth import require_ops_auth

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ops/campaigns",
    dependencies=[Depends(require_ops_auth)],
    tags=["campaigns"],
)

# Допустимые значения followup_policy
FollowupPolicy = Literal["unread_only", "unread_or_not_booked"]


# ==========================================================================
# Pydantic схемы запросов
# ==========================================================================


class CampaignBaseRequest(BaseModel):
    company_id: int
    location_id: int
    period_start: datetime
    period_end: datetime
    card_type_id: str | None = None
    attribution_window_days: int = Field(default=30, ge=1, le=365)
    followup_enabled: bool = False
    followup_delay_days: int | None = Field(default=None, ge=1, le=90)
    followup_policy: FollowupPolicy | None = None
    followup_template_name: str | None = None


class PreviewRequest(CampaignBaseRequest):
    pass


class RunRequest(CampaignBaseRequest):
    # Ссылка на preview-run (необязательна)
    source_preview_run_id: int | None = None


class FollowupPlanRequest(BaseModel):
    # Можно переопределить политику относительно run
    followup_policy: FollowupPolicy | None = None
    followup_delay_days: int | None = Field(default=None, ge=1, le=90)
    followup_template_name: str | None = None


# ==========================================================================
# Preview
# ==========================================================================


@router.post("/new-clients/preview")
async def create_preview(body: PreviewRequest) -> dict[str, Any]:
    """Запустить preview: сегментация без отправки.

    Сохраняет исторический снимок в БД (CampaignRun mode='preview').
    Возвращает базовую статистику и run_id для последующих запросов.
    """
    _validate_period(body.period_start, body.period_end)

    params = RunParams(
        company_id=body.company_id,
        location_id=body.location_id,
        period_start=_ensure_utc(body.period_start),
        period_end=_ensure_utc(body.period_end),
        mode="preview",
        card_type_id=body.card_type_id,
        attribution_window_days=body.attribution_window_days,
        followup_enabled=body.followup_enabled,
        followup_delay_days=body.followup_delay_days,
        followup_policy=body.followup_policy,
        followup_template_name=body.followup_template_name,
    )

    try:
        run = await run_preview(params)
    except Exception as exc:
        logger.exception("preview failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    return _run_summary(run)


# ==========================================================================
# Send-real
# ==========================================================================


@router.post(
    "/new-clients/run",
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_run(body: RunRequest) -> dict[str, Any]:
    """Поставить send-real кампанию в очередь.

    Endpoint быстро создаёт CampaignRun со статусом queued,
    ставит execution-job в message_jobs и сразу возвращает accepted.
    """
    _validate_period(body.period_start, body.period_end)

    params = RunParams(
        company_id=body.company_id,
        location_id=body.location_id,
        period_start=_ensure_utc(body.period_start),
        period_end=_ensure_utc(body.period_end),
        mode="send-real",
        card_type_id=body.card_type_id,
        source_preview_run_id=body.source_preview_run_id,
        attribution_window_days=body.attribution_window_days,
        followup_enabled=body.followup_enabled,
        followup_delay_days=body.followup_delay_days,
        followup_policy=body.followup_policy,
        followup_template_name=body.followup_template_name,
    )

    try:
        run = await enqueue_send_real(params)
    except Exception as exc:
        logger.exception("send-real enqueue failed: %s", exc)
        raise HTTPException(
            status_code=500,
            detail="Failed to queue campaign run",
        )

    result = _run_summary(run)
    result["accepted"] = True
    result["message"] = "Campaign run accepted and queued"
    return result


# ==========================================================================
# Справочник типов карт лояльности
# ==========================================================================


@router.get("/new-clients/card-types")
async def get_card_types_for_location(
    location_id: int = Query(..., description="Altegio salon/location ID"),
) -> list[dict[str, Any]]:
    """Возвращает список типов карт лояльности для заданного location_id.

    Используется HTML-страницей запуска кампании для динамической
    подгрузки типов карт через AJAX.
    """
    client = AltegioLoyaltyClient()
    try:
        card_types = await client.get_card_types(location_id)
        return card_types
    except Exception as exc:
        logger.exception("get_card_types failed for location_id=%d: %s", location_id, exc)
        raise HTTPException(status_code=502, detail=f"Altegio API error: {exc}")
    finally:
        await client.aclose()


# ==========================================================================
# Нормализация имени Meta-шаблона
# ==========================================================================

# Все шаблоны этого проекта имеют префикс вида "kitilash_{brand}_".
# Regex снимает любой такой префикс, поэтому добавление нового бренда
# (kitilash_xx_) не требует изменения этого кода.
_BRAND_PREFIX_RE = re.compile(r"^kitilash_[a-z]+_")


def normalize_meta_template_name(template_name: str) -> str:
    """Нормализовать имя Meta-шаблона в code для точного поиска в БД.

    Пример:
        "kitilash_ka_newsletter_new_clients_monthly_v2"
        → "newsletter_new_clients_monthly"

    Алгоритм:
        1. Убрать брендовый префикс вида kitilash_{brand}_ (любой бренд).
        2. Убрать версионный суффикс (_v1, _v2, _v3, ...).
    """
    code = _BRAND_PREFIX_RE.sub("", template_name)
    # Убираем версионный суффикс: _v1, _v2, _v3, …
    code = re.sub(r"_v\d+$", "", code)
    return code


# ==========================================================================
# Текст Meta-шаблона из БД
# ==========================================================================


@router.get("/new-clients/template-text")
async def get_template_text(
    template_name: str = Query(..., description="Meta template name"),
    company_id: int | None = Query(default=None, description="Campaign company ID"),
) -> dict[str, Any]:
    """Загрузить текст шаблона из локальной БД (message_templates).

    Шаблоны синхронизируются из Meta через sync_meta_templates.py.

    Алгоритм поиска:
        1. Нормализуем template_name → code через normalize_meta_template_name().
        2. Ищем точное совпадение: WHERE code = :code AND is_active = true.
        3. Если передан company_id — предпочитаем запись для этой компании.
        4. Если для company_id не найдено — берём любую активную запись с этим кодом.
        5. Результат детерминирован: ORDER BY id ASC LIMIT 1.

    Если шаблон не найден — возвращает 404.
    """
    code = normalize_meta_template_name(template_name)

    async with SessionLocal() as session:
        base_stmt = (
            select(MessageTemplate)
            .where(MessageTemplate.code == code)
            .where(MessageTemplate.is_active.is_(True))
            .order_by(MessageTemplate.id.asc())
            .limit(1)
        )

        match = None

        # Сначала ищем шаблон для конкретной компании
        if company_id is not None:
            company_stmt = base_stmt.where(MessageTemplate.company_id == company_id)
            match = (await session.execute(company_stmt)).scalar_one_or_none()

        # Fallback: берём первый активный шаблон с таким кодом (детерминированно по id ASC)
        if match is None:
            match = (await session.execute(base_stmt)).scalar_one_or_none()

    if match is None:
        raise HTTPException(
            status_code=404,
            detail=f"Шаблон с кодом '{code}' (из '{template_name}') не найден в локальной БД",
        )

    return {
        "template_name": template_name,
        "code": match.code,
        "language": match.language,
        "body": match.body,
        "company_id": match.company_id,
        "is_active": match.is_active,
    }


# ==========================================================================
# Список runs
# ==========================================================================


@router.get("/runs")
async def list_runs(
    company_id: int | None = Query(default=None),
    mode: str | None = Query(default=None),
    campaign_code: str = Query(default=CAMPAIGN_CODE),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Список CampaignRun с пагинацией.

    Фильтр по company_id применяется в SQL через оператор JSONB @>
    (containment), чтобы не отсекать результаты до применения
    limit/offset и считать честный total.
    """
    async with SessionLocal() as session:
        # Строим базовое условие
        conditions = [CampaignRun.campaign_code == campaign_code]
        if mode:
            conditions.append(CampaignRun.mode == mode)
        if company_id is not None:
            # JSONB containment: company_ids @> '[company_id]'
            conditions.append(CampaignRun.company_ids.contains([company_id]))

        # Честный total (без limit/offset)
        count_stmt = select(func.count()).select_from(CampaignRun).where(*conditions)
        total: int = (await session.scalar(count_stmt)) or 0

        # Страница результатов
        data_stmt = (
            select(CampaignRun).where(*conditions).order_by(CampaignRun.created_at.desc()).limit(limit).offset(offset)
        )
        runs = (await session.execute(data_stmt)).scalars().all()

    return {
        "items": [_run_summary(r) for r in runs],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


# ==========================================================================
# Детали run
# ==========================================================================


@router.get("/runs/{run_id}")
async def get_run(run_id: int) -> dict[str, Any]:
    """Детальная информация о конкретном run.

    Включает execution_job (фоновый MessageJob, ведущий кампанию),
    progress (live-счётчики из CampaignRecipient) и last_error.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        execution_job = await _fetch_execution_job(session, run_id)
        progress = await _fetch_progress(session, run_id, run.total_clients_seen or 0)

    result = _run_detail(run)
    result["execution_job"] = execution_job
    result["progress"] = progress
    result["followup_auto"] = _followup_auto(run)
    return result


# ==========================================================================
# Progress (polling endpoint)
# ==========================================================================


@router.get("/runs/{run_id}/progress")
async def get_run_progress(run_id: int) -> dict[str, Any]:
    """Live-прогресс выполнения run.

    Лёгкий polling endpoint для оператора: возвращает execution_job,
    live-счётчики получателей и last_error без полной детализации run.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        execution_job = await _fetch_execution_job(session, run_id)
        progress = await _fetch_progress(session, run_id, run.total_clients_seen or 0)

    return {
        "run_id": run_id,
        "status": run.status,
        "execution_job": execution_job,
        "progress": progress,
        "last_error": _last_error(run),
        "followup_auto": _followup_auto(run),
    }


# ==========================================================================
# Получатели run
# ==========================================================================


@router.get("/runs/{run_id}/recipients")
async def get_recipients(
    run_id: int,
    status: str | None = Query(default=None),
    excluded_reason: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Список получателей run с фильтрацией."""
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")

        conditions = [CampaignRecipient.campaign_run_id == run_id]
        if status:
            conditions.append(CampaignRecipient.status == status)
        if excluded_reason:
            conditions.append(CampaignRecipient.excluded_reason == excluded_reason)

        count_stmt = select(func.count()).select_from(CampaignRecipient).where(*conditions)
        total: int = (await session.scalar(count_stmt)) or 0

        stmt = (
            select(CampaignRecipient)
            .where(*conditions)
            .order_by(CampaignRecipient.id.asc())
            .limit(limit)
            .offset(offset)
        )
        recipients = (await session.execute(stmt)).scalars().all()

    return {
        "run_id": run_id,
        "items": [_recipient_dict(r) for r in recipients],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


# ==========================================================================
# Отчёт по run
# ==========================================================================


@router.get("/runs/{run_id}/report")
async def get_run_report(run_id: int) -> dict[str, Any]:
    """Полный отчёт по run: сегментация, delivery, атрибуция."""
    async with SessionLocal() as session:
        try:
            return await run_report(session, run_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))


# ==========================================================================
# Monthly dashboard
# ==========================================================================


@router.get("/dashboard/monthly")
async def get_monthly_dashboard(
    year: int = Query(..., ge=2020, le=2100),
    month: int = Query(..., ge=1, le=12),
    company_ids: str | None = Query(
        default=None,
        description="Comma-separated company IDs",
    ),
) -> dict[str, Any]:
    """Monthly dashboard по филиалам (только send-real runs)."""
    cids: list[int] | None = None
    if company_ids:
        try:
            cids = [int(x.strip()) for x in company_ids.split(",")]
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="company_ids must be comma-separated integers",
            )

    async with SessionLocal() as session:
        return await monthly_dashboard(session, year=year, month=month, company_ids=cids)


# ==========================================================================
# Follow-up
# ==========================================================================


@router.post("/runs/{run_id}/followup/plan")
async def plan_followup_endpoint(
    run_id: int,
    body: FollowupPlanRequest,
) -> dict[str, Any]:
    """Запланировать follow-up: оценить получателей по политике.

    Помечает eligible получателей статусом 'followup_planned'.
    Не отправляет никаких сообщений.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="Run not found")

            if run.mode != "send-real":
                raise HTTPException(
                    status_code=400,
                    detail="Follow-up доступен только для send-real run",
                )

            if run.status != "completed":
                raise HTTPException(
                    status_code=400,
                    detail="Follow-up можно планировать только для completed run",
                )

            if body.followup_policy:
                run.followup_policy = body.followup_policy
            if body.followup_delay_days is not None:
                run.followup_delay_days = body.followup_delay_days
            if body.followup_template_name:
                run.followup_template_name = body.followup_template_name

            run.followup_enabled = True

            try:
                planned = await plan_followup(session, run_id)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

    return {
        "run_id": run_id,
        "followup_planned": planned,
        "followup_run_at": followup_run_at(run),
    }


@router.post("/runs/{run_id}/followup/run-now")
async def run_followup_now(run_id: int) -> dict[str, Any]:
    """Немедленно запустить follow-up для запланированных получателей.

    Создаёт MessageJob для каждого получателя со статусом
    'followup_planned'.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")

        if run.mode != "send-real":
            raise HTTPException(
                status_code=400,
                detail="Follow-up доступен только для send-real run",
            )

        if not run.followup_enabled:
            raise HTTPException(
                status_code=400,
                detail="Follow-up не включён для этого run",
            )

    try:
        stats = await execute_followup(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("followup failed run_id=%d: %s", run_id, exc)
        raise HTTPException(
            status_code=500,
            detail="Internal follow-up error",
        )

    return {"run_id": run_id, "stats": stats}


# ==========================================================================
# Discard preview
# ==========================================================================


@router.post("/runs/{run_id}/discard")
async def discard_run(run_id: int) -> dict[str, Any]:
    """Пометить preview-run как discarded (soft-delete).

    Разрешено только для preview-run, который ещё не использован
    как source_preview_run_id для какого-либо send-real.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")

    try:
        await discard_preview_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("discard failed run_id=%d: %s", run_id, exc)
        raise HTTPException(status_code=500, detail="Internal discard error")

    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)

    if run is None:
        raise HTTPException(status_code=500, detail="Run disappeared after discard")

    return {
        "run_id": run_id,
        "status": run.status,
        "message": "Preview run discarded",
    }


# ==========================================================================
# Resume
# ==========================================================================


@router.post("/runs/{run_id}/resume")
async def resume_run(run_id: int) -> dict[str, Any]:
    """Продолжить failed send-real run по сохранённому snapshot получателей."""
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")

    try:
        result = await resume_send_real(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("resume failed run_id=%d: %s", run_id, exc)
        raise HTTPException(status_code=500, detail="Internal resume error")

    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)

    if run is None:
        raise HTTPException(
            status_code=500,
            detail="Run disappeared after resume",
        )

    return {
        "run_id": run_id,
        "accepted": True,
        "message": "Resume completed",
        "status": run.status,
        "summary": result,
    }


# ==========================================================================
# Вспомогательные async-функции (требуют session)
# ==========================================================================


async def _fetch_execution_job(
    session: AsyncSession,
    run_id: int,
) -> dict[str, Any] | None:
    """Найти последний execution MessageJob для данного run_id."""
    stmt = (
        select(MessageJob)
        .where(MessageJob.job_type == CAMPAIGN_EXECUTION_JOB_TYPE)
        .where(MessageJob.payload.contains({"campaign_run_id": run_id}))
        .order_by(MessageJob.id.desc())
        .limit(1)
    )
    job = (await session.execute(stmt)).scalar_one_or_none()
    if job is None:
        return None

    return {
        "id": job.id,
        "status": job.status,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "run_at": _iso(job.run_at),
        "locked_at": _iso(job.locked_at),
        "last_error": job.last_error,
        "created_at": _iso(job.created_at),
        "updated_at": _iso(job.updated_at),
    }


async def _fetch_progress(
    session: AsyncSession,
    run_id: int,
    total_clients_seen: int,
) -> dict[str, Any]:
    """Live-счётчики получателей по статусам из CampaignRecipient.

    skipped + queue_failed не считается done — такой получатель resumable,
    поэтому попадает в recipients_in_progress.
    """
    stmt = (
        select(
            CampaignRecipient.status,
            CampaignRecipient.excluded_reason,
            func.count(CampaignRecipient.id).label("cnt"),
        )
        .where(CampaignRecipient.campaign_run_id == run_id)
        .group_by(CampaignRecipient.status, CampaignRecipient.excluded_reason)
    )
    rows = (await session.execute(stmt)).all()

    skipped_done = 0
    queue_failed_pending = 0
    cleanup_failed = 0
    queued = 0
    candidate = 0
    card_issued = 0
    other = 0

    for status_value, reason, count in rows:
        cnt = int(count)
        if status_value == "skipped":
            if reason == "queue_failed":
                queue_failed_pending += cnt
            else:
                skipped_done += cnt
        elif status_value == "cleanup_failed":
            cleanup_failed += cnt
        elif status_value == "queued":
            queued += cnt
        elif status_value == "candidate":
            candidate += cnt
        elif status_value == "card_issued":
            card_issued += cnt
        else:
            other += cnt

    recipients_total = skipped_done + queue_failed_pending + cleanup_failed + queued + candidate + card_issued + other
    recipients_done = skipped_done + cleanup_failed + queued + other
    recipients_in_progress = candidate + card_issued + queue_failed_pending

    raw_progress = round(recipients_done / total_clients_seen, 4) if total_clients_seen > 0 else 0.0
    progress_pct = min(raw_progress, 1.0)

    return {
        "recipients_total": recipients_total,
        "recipients_skipped": skipped_done,
        "recipients_queue_failed_pending": queue_failed_pending,
        "recipients_cleanup_failed": cleanup_failed,
        "recipients_queued": queued,
        "recipients_candidate": candidate,
        "recipients_card_issued": card_issued,
        "recipients_done": recipients_done,
        "recipients_in_progress": recipients_in_progress,
        "progress_pct": progress_pct,
    }


# ==========================================================================
# Вспомогательные функции
# ==========================================================================


def _last_error(run: CampaignRun) -> str | None:
    """Последняя ошибка из run.meta['last_error']."""
    return (run.meta or {}).get("last_error")


def _followup_auto(run: CampaignRun) -> dict[str, Any] | None:
    """Блок auto follow-up из run.meta, если worker уже запускался."""
    meta = run.meta or {}

    keys = (
        "followup_auto_status",
        "followup_auto_started_at",
        "followup_auto_completed_at",
        "followup_auto_last_error",
        "followup_auto_planned_count",
        "followup_auto_queued_count",
        "followup_auto_skipped_count",
        "followup_auto_failed_count",
        "followup_auto_recovered",
        "followup_auto_recovered_at",
    )

    if not any(key in meta for key in keys):
        return None

    return {
        "followup_auto_status": meta.get("followup_auto_status"),
        "followup_auto_started_at": meta.get("followup_auto_started_at"),
        "followup_auto_completed_at": meta.get("followup_auto_completed_at"),
        "followup_auto_last_error": meta.get("followup_auto_last_error"),
        "followup_auto_planned_count": meta.get("followup_auto_planned_count"),
        "followup_auto_queued_count": meta.get("followup_auto_queued_count"),
        "followup_auto_skipped_count": meta.get("followup_auto_skipped_count"),
        "followup_auto_failed_count": meta.get("followup_auto_failed_count"),
        "followup_auto_recovered": meta.get("followup_auto_recovered"),
        "followup_auto_recovered_at": meta.get("followup_auto_recovered_at"),
    }


def _run_summary(run: CampaignRun) -> dict[str, Any]:
    """Краткая сводка по run для списков."""
    return {
        "id": run.id,
        "campaign_code": run.campaign_code,
        "mode": run.mode,
        "status": run.status,
        "company_ids": run.company_ids,
        "period_start": _iso(run.period_start),
        "period_end": _iso(run.period_end),
        "source_preview_run_id": run.source_preview_run_id,
        "total_clients_seen": run.total_clients_seen,
        "candidates_count": run.candidates_count,
        "queued_count": run.queued_count,
        "cards_issued_count": run.cards_issued_count,
        "followup_enabled": run.followup_enabled,
        "last_error": _last_error(run),
        "created_at": _iso(run.created_at),
        "completed_at": _iso(run.completed_at),
        # Preview-специфичные поля
        "is_preview": run.mode == "preview",
        "is_discardable": (run.mode == "preview" and run.status not in ("discarded",)),
        "excluded": {
            "opted_out": run.excluded_opted_out or 0,
            "no_phone": run.excluded_no_phone or 0,
            "invalid_phone": run.excluded_invalid_phone or 0,
            "no_whatsapp": run.excluded_no_whatsapp or 0,
            "multiple_records_in_period": run.excluded_multiple_records or 0,
            "no_confirmed_record_in_period": run.excluded_no_confirmed_record or 0,
            "has_records_before_period": run.excluded_has_records_before or 0,
        },
    }


def _run_detail(run: CampaignRun) -> dict[str, Any]:
    """Полная информация по run."""
    base = _run_summary(run)
    base.update(
        {
            "location_id": run.location_id,
            "card_type_id": run.card_type_id,
            "attribution_window_days": run.attribution_window_days,
            "followup_delay_days": run.followup_delay_days,
            "followup_policy": run.followup_policy,
            "followup_template_name": run.followup_template_name,
            "followup_auto": _followup_auto(run),
            "excluded": {
                "opted_out": run.excluded_opted_out,
                "no_phone": run.excluded_no_phone,
                "invalid_phone": run.excluded_invalid_phone,
                "no_whatsapp": run.excluded_no_whatsapp,
                "multiple_records_in_period": run.excluded_multiple_records,
                "no_confirmed_record_in_period": run.excluded_no_confirmed_record,
                "has_records_before_period": run.excluded_has_records_before,
            },
            "delivery": {
                "sent": run.sent_count,
                "queued": run.queued_count,
                "provider_accepted": run.provider_accepted_count,
                "delivered": run.delivered_count,
                "read": run.read_count,
                "replied": run.replied_count,
                "booked_after": run.booked_after_count,
                "opted_out_after": run.opted_out_after_count,
            },
            "loyalty": {
                "cards_deleted": run.cards_deleted_count,
                "cards_issued": run.cards_issued_count,
                "cleanup_failed": run.cleanup_failed_count,
            },
        }
    )
    return base


def _recipient_dict(r: CampaignRecipient) -> dict[str, Any]:
    """Словарь для одного CampaignRecipient."""
    return {
        "id": r.id,
        "client_id": r.client_id,
        "altegio_client_id": r.altegio_client_id,
        "phone_e164": r.phone_e164,
        "display_name": r.display_name,
        "status": r.status,
        "excluded_reason": r.excluded_reason,
        "segment": {
            "total_records_in_period": r.total_records_in_period,
            "confirmed_records_in_period": r.confirmed_records_in_period,
            "records_before_period": r.records_before_period,
            "lash_records_in_period": r.lash_records_in_period,
            "confirmed_lash_records_in_period": r.confirmed_lash_records_in_period,
            "service_titles_in_period": list(r.service_titles_in_period or []),
            "total_records_before_period_any": r.total_records_before_period_any,
            "local_client_found": r.local_client_found,
            "is_opted_out": r.is_opted_out,
        },
        "loyalty": {
            "loyalty_card_id": r.loyalty_card_id,
            "loyalty_card_number": r.loyalty_card_number,
            "loyalty_card_type_id": r.loyalty_card_type_id,
            "cleanup_card_ids": r.cleanup_card_ids,
            "cleanup_failed_reason": r.cleanup_failed_reason,
        },
        "tracking": {
            "message_job_id": r.message_job_id,
            "outbox_message_id": r.outbox_message_id,
            "provider_message_id": r.provider_message_id,
        },
        "attribution": {
            "sent_at": _iso(r.sent_at),
            "read_at": _iso(r.read_at),
            "replied_at": _iso(r.replied_at),
            "booked_after_at": _iso(r.booked_after_at),
            "opted_out_after_at": _iso(r.opted_out_after_at),
        },
        "followup": {
            "followup_status": r.followup_status,
            "followup_sent_at": _iso(r.followup_sent_at),
        },
        "created_at": _iso(r.created_at),
    }


def _validate_period(period_start: datetime, period_end: datetime) -> None:
    if period_end <= period_start:
        raise HTTPException(
            status_code=400,
            detail="period_end должен быть позже period_start",
        )


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None
