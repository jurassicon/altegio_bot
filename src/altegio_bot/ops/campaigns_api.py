"""Ops JSON API для управления кампаниями рассылок.

Все endpoint'ы защищены require_ops_auth (та же авторизация,
что и в ops/router.py).

Endpoint'ы:
  GET  /ops/campaigns/new-clients/card-types
  POST /ops/campaigns/new-clients/preview
  POST /ops/campaigns/new-clients/run
  GET  /ops/campaigns/debug-client
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

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.campaigns.altegio_crm import (
    CrmRecord,
    CrmUnavailableError,
    classify_crm_records,
    get_client_crm_records,
)
from altegio_bot.campaigns.followup import execute_followup, followup_run_at, plan_followup
from altegio_bot.campaigns.reports import monthly_dashboard, run_report
from altegio_bot.campaigns.runner import (
    CAMPAIGN_CODE,
    CAMPAIGN_EXECUTION_JOB_TYPE,
    RunParams,
    delete_preview_run,
    discard_preview_run,
    enqueue_send_real,
    recompute_run_counters,
    remove_recipient_from_preview,
    resume_send_real,
    run_preview,
)
from altegio_bot.campaigns.segment import check_lash_services, compute_excluded_reason
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import CampaignRecipient, CampaignRun, Client, MessageJob, MessageTemplate, Record
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.service_filter import ServiceLookupError
from altegio_bot.settings import settings

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
        raise HTTPException(
            status_code=500,
            detail="Preview failed due to internal error. See server logs for details.",
        )

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

    Если передан source_preview_run_id — валидирует совместимость:
    company_id, period_start, period_end и campaign_code должны совпадать
    с параметрами preview-run. Несовпадение → 400.
    """
    _validate_period(body.period_start, body.period_end)

    period_start_utc = _ensure_utc(body.period_start)
    period_end_utc = _ensure_utc(body.period_end)

    # Backend-валидация: параметры запуска должны совпадать с preview-snapshot
    if body.source_preview_run_id is not None:
        await _validate_run_matches_preview(
            preview_run_id=body.source_preview_run_id,
            company_id=body.company_id,
            period_start=period_start_utc,
            period_end=period_end_utc,
            campaign_code=CAMPAIGN_CODE,
            card_type_id=body.card_type_id,
            followup_enabled=body.followup_enabled,
            followup_delay_days=body.followup_delay_days,
            followup_policy=body.followup_policy,
            followup_template_name=body.followup_template_name,
        )

    params = RunParams(
        company_id=body.company_id,
        location_id=body.location_id,
        period_start=period_start_utc,
        period_end=period_end_utc,
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


def _normalise_nullable_str(v: str | None) -> str | None:
    """Нормализовать nullable строку: trim + пустая/пробельная строка → None.

    Используется при сравнении параметров preview и send-real запросов.
    Предотвращает ложные mismatch:
    - '' vs None → оба None (эквивалентны)
    - '   ' vs None → оба None (пробелы — не значимое значение)
    - ' abc ' vs 'abc' → оба 'abc' (trim убирает случайные пробелы)
    - '0' → '0' (truthy строка остаётся как есть)
    """
    if v is None:
        return None
    stripped = v.strip()
    return stripped if stripped else None


async def _validate_run_matches_preview(
    *,
    preview_run_id: int,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
    campaign_code: str,
    card_type_id: str | None,
    followup_enabled: bool,
    followup_delay_days: int | None,
    followup_policy: str | None,
    followup_template_name: str | None,
) -> None:
    """Проверить совместимость параметров send-real с preview-snapshot.

    Гарантирует, что send-real запускается именно для того снимка,
    который оператор видел в preview. Несовпадение любого ключевого
    параметра возвращает HTTP 400.

    Проверяемые поля:
      - mode == 'preview' и status == 'completed' (только завершённые previews)
      - campaign_code
      - company_id (первый из company_ids)
      - period_start, period_end (с точностью до секунды)
      - card_type_id
      - followup_enabled, followup_delay_days, followup_policy, followup_template_name
    """
    async with SessionLocal() as session:
        preview_run = await session.get(CampaignRun, preview_run_id)

    if preview_run is None:
        raise HTTPException(
            status_code=400,
            detail=f"Preview run {preview_run_id} not found",
        )

    if preview_run.mode != "preview":
        raise HTTPException(
            status_code=400,
            detail=f"Run {preview_run_id} is not a preview (mode={preview_run.mode!r})",
        )

    # Только completed previews могут быть источником send-real.
    # failed/running/queued/discarded — нельзя: данные неполные или сброшены.
    if preview_run.status != "completed":
        raise HTTPException(
            status_code=400,
            detail=(
                f"Preview run {preview_run_id} has status={preview_run.status!r}; "
                f"only 'completed' previews can be used as source for send-real"
            ),
        )

    # Проверка campaign_code
    if preview_run.campaign_code != campaign_code:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Campaign code mismatch: preview has {preview_run.campaign_code!r}, request has {campaign_code!r}"
            ),
        )

    # Проверка company_id
    preview_company_ids = preview_run.company_ids or []
    if not preview_company_ids or int(preview_company_ids[0]) != company_id:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Company ID mismatch: preview was for company_id={preview_company_ids}, "
                f"request has company_id={company_id}"
            ),
        )

    # Проверка периода (с точностью до секунды)
    def _truncate_sec(dt: datetime) -> datetime:
        return dt.replace(microsecond=0)

    preview_start = _ensure_utc(preview_run.period_start)
    preview_end = _ensure_utc(preview_run.period_end)

    if _truncate_sec(preview_start) != _truncate_sec(period_start):
        raise HTTPException(
            status_code=400,
            detail=(f"Period start mismatch: preview={preview_start.isoformat()}, request={period_start.isoformat()}"),
        )

    if _truncate_sec(preview_end) != _truncate_sec(period_end):
        raise HTTPException(
            status_code=400,
            detail=(f"Period end mismatch: preview={preview_end.isoformat()}, request={period_end.isoformat()}"),
        )

    # Проверка card_type_id и followup-параметров — зафиксированы в снимке.
    # Используем _normalise_nullable_str для строк: пустая строка эквивалентна None.
    # Для int полей (followup_delay_days) — прямое сравнение без or-None,
    # чтобы 0 не трактовался как None.
    nullable_str_checks: list[tuple[str, str | None, str | None]] = [
        ("card_type_id", preview_run.card_type_id, card_type_id),
        ("followup_policy", preview_run.followup_policy, followup_policy),
        ("followup_template_name", preview_run.followup_template_name, followup_template_name),
    ]
    for field_name, preview_val, request_val in nullable_str_checks:
        if _normalise_nullable_str(preview_val) != _normalise_nullable_str(request_val):
            raise HTTPException(
                status_code=400,
                detail=(f"{field_name} mismatch: preview has {preview_val!r}, request has {request_val!r}"),
            )

    if bool(preview_run.followup_enabled) != bool(followup_enabled):
        raise HTTPException(
            status_code=400,
            detail=(f"followup_enabled mismatch: preview={preview_run.followup_enabled}, request={followup_enabled}"),
        )

    if preview_run.followup_delay_days != followup_delay_days:
        raise HTTPException(
            status_code=400,
            detail=(
                f"followup_delay_days mismatch: preview={preview_run.followup_delay_days}, "
                f"request={followup_delay_days}"
            ),
        )


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
# Debug: диагностика сегментации для конкретного клиента
# ==========================================================================


def _debug_normalize_phone(raw: str) -> str | None:
    """Нормализовать телефон в формат E.164 (+цифры). None если пустой."""
    digits = re.sub(r"\D+", "", raw)
    return f"+{digits}" if digits else None


def _debug_parse_dt(value: str) -> datetime:
    """Распарсить ISO datetime строку → UTC datetime.

    Принимает любой ISO 8601 формат, включая «2026-03-01T00:00:00+00:00»
    и «2026-03-01T00:00:00Z». Используется вместо FastAPI datetime Query
    чтобы избежать 422 при передаче «+00:00» в query string.
    """
    # Заменить Z → +00:00 для совместимости с fromisoformat
    v = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(v)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Невалидный формат datetime: {value!r} — {exc}")
    return _ensure_utc(dt)


@router.get("/debug-client")
async def debug_client_segmentation(
    company_id: int = Query(..., description="Altegio company ID"),
    phone: str = Query(..., description="Номер телефона клиента (любой формат)"),
    period_start: str = Query(..., description="Начало периода (ISO 8601, UTC)"),
    period_end: str = Query(..., description="Конец периода (ISO 8601, UTC)"),
) -> dict[str, Any]:
    """Диагностика сегментации для конкретного клиента.

    Возвращает пошаговую диагностику: локальный клиент, CRM-записи,
    разбивку по периоду/до периода, ресничные/посещённые записи и итоговый вывод.

    Используется для отладки false positive / false negative случаев.
    Делает живой запрос к Altegio CRM API.
    """
    period_start_utc = _debug_parse_dt(period_start)
    period_end_utc = _debug_parse_dt(period_end)

    phone_e164 = _debug_normalize_phone(phone)
    if not phone_e164:
        raise HTTPException(status_code=400, detail=f"Не удалось нормализовать телефон: {phone!r}")

    # ------------------------------------------------------------------
    # 1. Локальный клиент и его записи в периоде
    # ------------------------------------------------------------------
    async with SessionLocal() as session:
        client_stmt = select(Client).where(Client.company_id == company_id, Client.phone_e164 == phone_e164).limit(1)
        local_client: Client | None = (await session.execute(client_stmt)).scalar_one_or_none()

        local_records_in_period: list[Record] = []
        if local_client is not None:
            rec_stmt = (
                select(Record)
                .where(
                    Record.company_id == company_id,
                    Record.client_id == local_client.id,
                    Record.starts_at >= period_start_utc,
                    Record.starts_at < period_end_utc,
                    Record.is_deleted.is_(False),
                )
                .order_by(Record.starts_at.asc())
            )
            local_records_in_period = list((await session.execute(rec_stmt)).scalars().all())

    local_client_dict: dict[str, Any] | None = None
    if local_client is not None:
        local_client_dict = {
            "id": local_client.id,
            "company_id": local_client.company_id,
            "altegio_client_id": local_client.altegio_client_id,
            "phone_e164": local_client.phone_e164,
            "display_name": local_client.display_name,
            "wa_opted_out": bool(local_client.wa_opted_out),
        }

    if local_client is None or local_client.altegio_client_id is None:
        return {
            "phone_e164": phone_e164,
            "local_client": local_client_dict,
            "error": (
                "Клиент не найден в локальной БД"
                if local_client is None
                else "altegio_client_id отсутствует у клиента в локальной БД"
            ),
            "local_records_in_period": [],
            "crm_records": None,
            "classification": None,
            "eligibility": {"is_eligible": False, "excluded_reason": "crm_history_unavailable"},
        }

    altegio_client_id: int = int(local_client.altegio_client_id)

    # ------------------------------------------------------------------
    # 2. CRM-записи (все записи клиента)
    # ------------------------------------------------------------------
    crm_error: str | None = None
    crm_records: list[CrmRecord] = []
    in_period_records: list[CrmRecord] = []
    count_before: int = 0
    count_after: int = 0
    lash_count: int = 0
    attended_lash_count: int = 0
    confirmed_count: int = 0
    lash_svc_ids: frozenset[int] = frozenset()
    service_lookup_error: str | None = None

    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            crm_records = await get_client_crm_records(
                http,
                company_id=company_id,
                altegio_client_id=altegio_client_id,
            )

            # 3. Разбить по периоду
            in_period_records, count_before, count_after = classify_crm_records(
                crm_records, period_start_utc, period_end_utc
            )

            # 4. Подтверждённые записи (диагностика, не является критерием eligible)
            confirmed_count = sum(1 for r in in_period_records if r.is_confirmed)

            # 5. Ресничные и посещённые записи.
            # Используем ту же функцию, что и основной segment pipeline — никакого расхождения.
            try:
                lash_count, attended_lash_count, _, lash_svc_ids = await check_lash_services(
                    http, company_id, in_period_records
                )
            except ServiceLookupError as exc:
                service_lookup_error = str(exc)

    except CrmUnavailableError as exc:
        crm_error = str(exc)

    # ------------------------------------------------------------------
    # 6. Классификация.
    # Используем ту же функцию, что и основной segment pipeline — никакого расхождения.
    # ------------------------------------------------------------------
    excluded_reason: str | None = compute_excluded_reason(
        wa_opted_out=bool(local_client.wa_opted_out),
        phone_e164=local_client.phone_e164,
        crm_unavailable=crm_error is not None,
        service_unavailable=service_lookup_error is not None,
        count_before=count_before,
        count_after=count_after,
        lash_count=lash_count,
        attended_lash_count=attended_lash_count,
    )

    def _crm_rec_dict(r: CrmRecord) -> dict[str, Any]:
        return {
            "crm_id": r.crm_id,
            "starts_at": r.starts_at.isoformat() if r.starts_at else None,
            # Нормализованные поля (после парсинга)
            "confirmed": r.confirmed,
            "attendance": r.attendance,
            "attendance_source": r.attendance_source,
            "deleted": r.deleted,
            "is_confirmed": r.is_confirmed,
            "is_attended": r.is_attended,
            "service_ids": r.service_ids,
            "service_titles": r.service_titles,
            # Сырые поля из CRM ответа (до парсинга) — для расследования mismatch
            "raw_crm_fields": r.raw_debug,
        }

    return {
        "phone_e164": phone_e164,
        "local_client": local_client_dict,
        "local_records_in_period": [
            {
                "id": r.id,
                "altegio_record_id": r.altegio_record_id,
                "starts_at": r.starts_at.isoformat() if r.starts_at else None,
                "confirmed": r.confirmed,
                "attendance": r.attendance,
                "visit_attendance": r.visit_attendance,
                "is_deleted": r.is_deleted,
            }
            for r in local_records_in_period
        ],
        "crm_error": crm_error,
        "crm_records_total": len(crm_records),
        "crm_records": [_crm_rec_dict(r) for r in crm_records],
        "classification": None
        if crm_error
        else {
            "count_before_period": count_before,
            "count_after_period": count_after,
            "in_period_records": [_crm_rec_dict(r) for r in in_period_records],
            "total_in_period": len(in_period_records),
            "confirmed_in_period": confirmed_count,
            "lash_records_in_period": lash_count,
            "attended_lash_records_in_period": attended_lash_count,
            "service_lookup_error": service_lookup_error,
            "lash_service_ids_found": sorted(lash_svc_ids) if service_lookup_error is None else None,
            # Явная диагностика по каждой in-period записи: откуда взят attended flag
            "attendance_notes": [
                {
                    "crm_id": r.crm_id,
                    "is_attended": r.is_attended,
                    "attendance_value": r.attendance,
                    "attendance_source": r.attendance_source,
                    "explanation": (
                        f"attended=True (источник: {r.attendance_source!r})"
                        if r.is_attended
                        else f"attended=False (источник: {r.attendance_source!r}, "
                        f"raw_attendance={r.raw_debug.get('attendance')!r}, "
                        f"raw_visit_attendance={r.raw_debug.get('visit_attendance')!r})"
                    ),
                }
                for r in in_period_records
            ],
        },
        "eligibility": {
            "is_eligible": excluded_reason is None,
            "excluded_reason": excluded_reason,
            # Резюме: что конкретно привело к решению
            "decision_notes": (
                "eligible: все условия выполнены" if excluded_reason is None else f"excluded: {excluded_reason}"
            ),
        },
    }


# ==========================================================================
# Debug batch: диагностика сегментации для списка клиентов
# ==========================================================================


class BatchDebugRequest(BaseModel):
    """Тело запроса для batch debug endpoint.

    phones — список номеров в любом формате (с пробелами, скобками, +49…).
    Нормализация выполняется внутри endpoint'а.
    Максимум 50 номеров: больше не нужно для ручного расследования,
    а лимит защищает CRM API от перегрузки.
    """

    company_id: int
    period_start: str = Field(..., description="ISO 8601 UTC, напр. 2026-03-01T00:00:00Z")
    period_end: str = Field(..., description="ISO 8601 UTC, напр. 2026-04-01T00:00:00Z")
    phones: list[str] = Field(..., min_length=1, max_length=50)


@router.post("/debug-clients-batch")
async def debug_clients_batch(body: BatchDebugRequest) -> dict[str, Any]:
    """Batch диагностика сегментации для списка клиентов.

    Предназначен для расследования mismatch «в CRM 15 клиентов, в preview 3»:
    передаёшь все телефоны из CRM-скрина — получаешь таблицу с объяснением
    по каждому, почему он eligible или excluded.

    Каждый клиент в ``results`` содержит:
      - ``discovery_status``       — виден ли пайплайну через локальную БД:
            "in_local_db_with_records" — найден и записи за период есть → обнаружен
            "in_local_db_no_records"   — найден, но записей за период нет → пропущен
            "not_in_local_db"          — не найден вообще → пропущен
      - ``local_records_in_period`` — кол-во записей в локальной БД за период
      - ``crm_records_total``       — всего записей в CRM (null если нет altegio_client_id)
      - ``crm_in_period``           — записей в CRM за период
      - ``crm_before_period``       — записей в CRM до начала периода
      - ``lash_in_period``          — ресничных записей в CRM за период
      - ``attended_lash_in_period`` — ресничных с attendance=1
      - ``is_eligible`` / ``excluded_reason`` / ``decision_notes`` — итог

    ``summary`` агрегирует counts по всем клиентам — для быстрого диагноза
    без чтения каждой строки.

    Конкурентность CRM-вызовов ограничена ``settings.campaign_crm_max_concurrency``
    (тот же семафор, что и в основном segment pipeline).
    """
    period_start_utc = _debug_parse_dt(body.period_start)
    period_end_utc = _debug_parse_dt(body.period_end)

    # ------------------------------------------------------------------
    # 1. Нормализация телефонов
    # ------------------------------------------------------------------
    normalized: list[tuple[str, str | None]] = []  # (phone_input, phone_e164 | None)
    for raw in body.phones:
        normalized.append((raw, _debug_normalize_phone(raw)))

    valid_phones_e164 = [e164 for _, e164 in normalized if e164]

    # ------------------------------------------------------------------
    # 2. Bulk lookup: clients + records в периоде (одна сессия, два запроса)
    # ------------------------------------------------------------------
    clients_by_phone: dict[str, Client] = {}
    records_by_client_id: dict[int, int] = {}  # client_id → кол-во записей в периоде

    async with SessionLocal() as session:
        if valid_phones_e164:
            cl_stmt = select(Client).where(
                Client.company_id == body.company_id,
                Client.phone_e164.in_(valid_phones_e164),
            )
            for c in (await session.execute(cl_stmt)).scalars():
                if c.phone_e164:
                    clients_by_phone[c.phone_e164] = c

            found_client_ids = [c.id for c in clients_by_phone.values() if c.id is not None]
            if found_client_ids:
                # Подсчёт записей в периоде без загрузки всех объектов
                from sqlalchemy import func as sql_func

                count_stmt = (
                    select(Record.client_id, sql_func.count().label("cnt"))
                    .where(
                        Record.company_id == body.company_id,
                        Record.client_id.in_(found_client_ids),
                        Record.starts_at >= period_start_utc,
                        Record.starts_at < period_end_utc,
                        Record.is_deleted.is_(False),
                    )
                    .group_by(Record.client_id)
                )
                for row in (await session.execute(count_stmt)).all():
                    records_by_client_id[row.client_id] = row.cnt

    # ------------------------------------------------------------------
    # 3. CRM calls: concurrent с семафором
    # ------------------------------------------------------------------
    async def _process_one(
        http: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        phone_e164: str,
        local_client: Client | None,
        local_rec_count: int,
    ) -> dict[str, Any]:
        """Диагностика одного клиента: локальная БД + CRM + классификация."""

        # Базовые поля, которые всегда заполняются
        entry: dict[str, Any] = {
            "phone_e164": phone_e164,
            "display_name": local_client.display_name if local_client else None,
            "local_client_found": local_client is not None,
            "local_records_in_period": local_rec_count,
        }

        # Клиент не найден в локальной БД → discovery его пропустит
        if local_client is None:
            entry["discovery_status"] = "not_in_local_db"
            entry["crm_records_total"] = None
            entry["crm_in_period"] = None
            entry["crm_before_period"] = None
            entry["lash_in_period"] = None
            entry["attended_lash_in_period"] = None
            entry["is_eligible"] = False
            entry["excluded_reason"] = "not_in_local_db"
            entry["decision_notes"] = (
                "не обнаружен: клиента нет в локальной таблице clients — discovery его пропускает без вызова CRM"
            )
            return entry

        entry["discovery_status"] = "in_local_db_with_records" if local_rec_count > 0 else "in_local_db_no_records"

        # Нет altegio_client_id → CRM недоступен
        if not local_client.altegio_client_id:
            entry["crm_records_total"] = None
            entry["crm_in_period"] = None
            entry["crm_before_period"] = None
            entry["lash_in_period"] = None
            entry["attended_lash_in_period"] = None
            entry["is_eligible"] = False
            entry["excluded_reason"] = "crm_history_unavailable"
            entry["decision_notes"] = "excluded: нет altegio_client_id — невозможно запросить CRM"
            return entry

        # CRM call под семафором
        async with sem:
            try:
                crm_records = await get_client_crm_records(
                    http,
                    company_id=body.company_id,
                    altegio_client_id=int(local_client.altegio_client_id),
                )
                in_period_recs, count_before, count_after_batch = classify_crm_records(
                    crm_records, period_start_utc, period_end_utc
                )

                lash_count = 0
                attended_lash_count = 0
                service_lookup_error: str | None = None
                try:
                    lash_count, attended_lash_count, _, _ = await check_lash_services(
                        http, body.company_id, in_period_recs
                    )
                except ServiceLookupError as exc:
                    service_lookup_error = str(exc)

                entry["crm_records_total"] = len(crm_records)
                entry["crm_in_period"] = len(in_period_recs)
                entry["crm_before_period"] = count_before
                entry["crm_after_period"] = count_after_batch
                entry["lash_in_period"] = lash_count
                entry["attended_lash_in_period"] = attended_lash_count

                excluded = compute_excluded_reason(
                    wa_opted_out=bool(local_client.wa_opted_out),
                    phone_e164=local_client.phone_e164,
                    crm_unavailable=False,
                    service_unavailable=service_lookup_error is not None,
                    count_before=count_before,
                    count_after=count_after_batch,
                    lash_count=lash_count,
                    attended_lash_count=attended_lash_count,
                )
                entry["is_eligible"] = excluded is None
                entry["excluded_reason"] = excluded
                entry["decision_notes"] = (
                    "eligible: все условия выполнены" if excluded is None else f"excluded: {excluded}"
                )

            except CrmUnavailableError as exc:
                entry["crm_records_total"] = None
                entry["crm_in_period"] = None
                entry["crm_before_period"] = None
                entry["lash_in_period"] = None
                entry["attended_lash_in_period"] = None
                entry["is_eligible"] = False
                entry["excluded_reason"] = "crm_history_unavailable"
                entry["decision_notes"] = f"excluded: CRM недоступен — {exc}"

        return entry

    # ------------------------------------------------------------------
    # Вспомогательная функция для невалидного телефона
    # ------------------------------------------------------------------
    def _invalid_phone_entry(phone_input: str) -> dict[str, Any]:
        return {
            "phone_input": phone_input,
            "phone_e164": None,
            "display_name": None,
            "local_client_found": False,
            "local_records_in_period": 0,
            "discovery_status": "invalid_phone",
            "crm_records_total": None,
            "crm_in_period": None,
            "crm_before_period": None,
            "lash_in_period": None,
            "attended_lash_in_period": None,
            "is_eligible": False,
            "excluded_reason": "invalid_phone",
            "decision_notes": "excluded: не удалось нормализовать телефон",
        }

    # ------------------------------------------------------------------
    # Собираем слоты: каждый слот — это либо готовый dict (невалидный телефон),
    # либо индекс в список задач (валидный телефон, нужен CRM-запрос).
    # Такая схема сохраняет порядок входных телефонов при любом их чередовании
    # и не использует хрупкую индексацию через промежуточные списки.
    # ------------------------------------------------------------------
    # slot: dict → готовый результат (invalid)
    #        int → индекс в tasks (будет заменён после gather)
    slots: list[tuple[str, str | None, dict[str, Any] | int]] = []
    tasks: list[Any] = []

    async with httpx.AsyncClient(timeout=30.0) as http:
        sem = asyncio.Semaphore(settings.campaign_crm_max_concurrency)

        for phone_input, phone_e164 in normalized:
            if phone_e164 is None:
                slots.append((phone_input, None, _invalid_phone_entry(phone_input)))
            else:
                local_c = clients_by_phone.get(phone_e164)
                local_cnt = records_by_client_id.get(local_c.id, 0) if local_c else 0
                task_index = len(tasks)
                tasks.append(_process_one(http, sem, phone_e164, local_c, local_cnt))
                slots.append((phone_input, phone_e164, task_index))

        raw = await asyncio.gather(*tasks, return_exceptions=True)

    # Один проход — собираем final_results строго в порядке входных телефонов.
    # Если gather вернул исключение вместо dict — логируем и возвращаем error-запись.
    final_results: list[dict[str, Any]] = []
    for phone_input, phone_e164, slot in slots:
        if isinstance(slot, dict):
            # Невалидный телефон — готовый результат
            final_results.append(slot)
        else:
            # Валидный телефон — берём результат gather по индексу
            outcome = raw[slot]
            if isinstance(outcome, dict):
                outcome["phone_input"] = phone_input
                final_results.append(outcome)
            else:
                logger.error(
                    "debug_clients_batch: unexpected exception for phone=%r: %r",
                    phone_e164,
                    outcome,
                )
                final_results.append(
                    {
                        "phone_input": phone_input,
                        "phone_e164": phone_e164,
                        "display_name": None,
                        "local_client_found": False,
                        "local_records_in_period": 0,
                        "discovery_status": "error",
                        "crm_records_total": None,
                        "crm_in_period": None,
                        "crm_before_period": None,
                        "lash_in_period": None,
                        "attended_lash_in_period": None,
                        "is_eligible": False,
                        "excluded_reason": "internal_error",
                        "decision_notes": f"excluded: внутренняя ошибка — {outcome!r}",
                    }
                )

    # ------------------------------------------------------------------
    # 4. Summary: агрегированные счётчики для быстрого диагноза
    # ------------------------------------------------------------------
    discovery_counts: dict[str, int] = {}
    exclusion_counts: dict[str, int] = {}
    eligible_count = 0

    for r in final_results:
        ds = r.get("discovery_status", "unknown")
        discovery_counts[ds] = discovery_counts.get(ds, 0) + 1
        if r.get("is_eligible"):
            eligible_count += 1
        else:
            reason = r.get("excluded_reason") or "unknown"
            exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1

    summary: dict[str, Any] = {
        "total_phones": len(final_results),
        "eligible": eligible_count,
        "excluded": len(final_results) - eligible_count,
        "discovery_status_counts": discovery_counts,
        "excluded_by_reason": exclusion_counts,
    }

    return {
        "company_id": body.company_id,
        "period_start": period_start_utc.isoformat(),
        "period_end": period_end_utc.isoformat(),
        "summary": summary,
        "results": final_results,
    }


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
    status: str | None = Query(default=None),
    include_deleted: bool = Query(default=False),
    campaign_code: str = Query(default=CAMPAIGN_CODE),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Список CampaignRun с пагинацией.

    Фильтр по company_id применяется в SQL через оператор JSONB @>
    (containment), чтобы не отсекать результаты до применения
    limit/offset и считать честный total.

    По умолчанию soft-deleted runs (status='deleted') скрыты.
    Передайте include_deleted=true чтобы их увидеть.
    """
    async with SessionLocal() as session:
        # Строим базовое условие
        conditions = [CampaignRun.campaign_code == campaign_code]
        if not include_deleted and status != "deleted":
            conditions.append(CampaignRun.status != "deleted")
        if status:
            conditions.append(CampaignRun.status == status)
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
# Delete preview run (stronger than discard — hides from list)
# ==========================================================================


@router.post("/runs/{run_id}/delete")
async def delete_run(run_id: int) -> dict[str, Any]:
    """Soft-delete preview-run: status='deleted', скрывается из списка.

    Строже, чем /discard: run перестаёт отображаться в /runs по умолчанию.
    CampaignRecipient физически не удаляются — аудит сохраняется.

    Запрещено, если preview уже используется как source_preview_run_id
    в каком-либо send-real.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")

    try:
        await delete_preview_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("delete_preview failed run_id=%d: %s", run_id, exc)
        raise HTTPException(status_code=500, detail="Internal delete error")

    return {
        "run_id": run_id,
        "status": "deleted",
        "message": "Preview run удалён (soft-delete)",
    }


# ==========================================================================
# Manual remove recipient from preview snapshot
# ==========================================================================


@router.post("/runs/{run_id}/recipients/{recipient_id}/remove")
async def remove_recipient(run_id: int, recipient_id: int) -> dict[str, Any]:
    """Мягко исключить получателя из preview snapshot.

    Устанавливает status='skipped', excluded_reason='manual_removed'.
    Пересчитывает счётчики run.

    Разрешено только для completed/running preview, который ещё не
    использован как source_preview_run_id в send-real.
    """
    try:
        r = await remove_recipient_from_preview(run_id, recipient_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception(
            "remove_recipient failed run_id=%d recipient_id=%d: %s",
            run_id,
            recipient_id,
            exc,
        )
        raise HTTPException(status_code=500, detail="Internal remove error")

    return {
        "run_id": run_id,
        "recipient_id": recipient_id,
        "recipient": _recipient_dict(r),
        "message": "Получатель исключён из snapshot (manual_removed)",
    }


# ==========================================================================
# Manual add recipient to preview snapshot
# ==========================================================================


class AddRecipientRequest(BaseModel):
    """Запрос на добавление клиента в preview snapshot.

    Нужно указать хотя бы одно из: phone или altegio_client_id.
    Если передан только phone — клиент ищется в локальной БД для
    получения altegio_client_id (необходим для CRM-проверки).
    """

    phone: str | None = None
    altegio_client_id: int | None = None


@router.post("/runs/{run_id}/recipients/add", status_code=201)
async def add_recipient(run_id: int, body: AddRecipientRequest) -> dict[str, Any]:
    """Добавить клиента в preview snapshot вручную.

    Алгоритм:
      1. Проверить, что run — editable preview (completed, не используется send-real).
      2. Нормализовать phone, найти клиента в локальной БД.
      3. Проверить дубликат в snapshot.
      4. Вызвать Altegio CRM API: получить записи, классифицировать, проверить eligible.
      5. Если клиент не eligible — вернуть 422 с excluded_reason.
      6. Создать CampaignRecipient, пересчитать счётчики run.

    Ограничения:
      - Для полной проверки eligible нужен altegio_client_id (получается автоматически
        из локальной БД по phone, или передаётся напрямую).
      - Если клиент не найден в локальной БД и altegio_client_id не передан — 400.
    """
    if not body.phone and body.altegio_client_id is None:
        raise HTTPException(
            status_code=400,
            detail="Нужно указать phone или altegio_client_id",
        )

    # --- Загрузить и проверить run ---
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")

    if run.mode != "preview":
        raise HTTPException(
            status_code=400,
            detail="Add recipient доступен только для preview run",
        )
    if run.status not in ("completed", "running"):
        raise HTTPException(
            status_code=400,
            detail=f"Run status={run.status!r} не позволяет редактирование snapshot",
        )

    async with SessionLocal() as session:
        used = await session.scalar(
            select(func.count())
            .select_from(CampaignRun)
            .where(CampaignRun.source_preview_run_id == run_id)
            .where(CampaignRun.mode == "send-real")
        )
    if int(used or 0) > 0:
        raise HTTPException(
            status_code=400,
            detail="Preview уже использован для send-real — редактирование запрещено",
        )

    company_ids = run.company_ids or []
    if not company_ids:
        raise HTTPException(status_code=500, detail="Run has no company_ids")
    company_id = int(company_ids[0])

    # --- Нормализация phone ---
    phone_e164: str | None = None
    if body.phone:
        phone_e164 = _debug_normalize_phone(body.phone)
        if not phone_e164:
            raise HTTPException(
                status_code=400,
                detail=f"Невалидный телефон: {body.phone!r}",
            )

    # --- Поиск клиента в локальной БД ---
    local_client: Client | None = None
    altegio_cid: int | None = body.altegio_client_id

    async with SessionLocal() as session:
        if phone_e164 and altegio_cid is None:
            stmt = select(Client).where(Client.company_id == company_id, Client.phone_e164 == phone_e164).limit(1)
            local_client = (await session.execute(stmt)).scalar_one_or_none()
            if local_client is not None:
                altegio_cid = local_client.altegio_client_id
        elif altegio_cid is not None:
            stmt = (
                select(Client)
                .where(
                    Client.company_id == company_id,
                    Client.altegio_client_id == altegio_cid,
                )
                .limit(1)
            )
            local_client = (await session.execute(stmt)).scalar_one_or_none()
            if local_client is not None and phone_e164 is None:
                phone_e164 = local_client.phone_e164

    if altegio_cid is None:
        raise HTTPException(
            status_code=400,
            detail=("Клиент не найден в локальной БД; укажите altegio_client_id для прямой проверки в Altegio CRM"),
        )

    if not phone_e164:
        raise HTTPException(
            status_code=400,
            detail="Не удалось определить phone_e164 для клиента",
        )

    # --- Проверка дубликата в snapshot ---
    async with SessionLocal() as session:
        dup_count = (
            await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(
                    CampaignRecipient.campaign_run_id == run_id,
                    CampaignRecipient.phone_e164 == phone_e164,
                )
            )
        ) or 0

    if dup_count > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Клиент с phone_e164={phone_e164!r} уже есть в snapshot run {run_id}",
        )

    # --- CRM: проверка eligible ---
    wa_opted_out = bool(local_client.wa_opted_out) if local_client else False
    display_name: str | None = local_client.display_name if local_client else None
    local_client_found = local_client is not None

    crm_error: str | None = None
    in_period_records: list = []
    count_before = 0
    count_after = 0
    lash_count = 0
    attended_lash_count = 0
    service_titles: list[str] = []
    service_lookup_error: str | None = None

    period_start_utc = _ensure_utc(run.period_start)
    period_end_utc = _ensure_utc(run.period_end)

    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            crm_records = await get_client_crm_records(
                http,
                company_id=company_id,
                altegio_client_id=int(altegio_cid),
            )
            in_period_records, count_before, count_after = classify_crm_records(
                crm_records,
                period_start_utc,
                period_end_utc,
            )
            service_titles = [t for r in in_period_records for t in (r.service_titles or [])]
            try:
                lash_count, attended_lash_count, _, _ = await check_lash_services(http, company_id, in_period_records)
            except ServiceLookupError as exc:
                service_lookup_error = str(exc)
    except CrmUnavailableError as exc:
        crm_error = str(exc)
    except Exception as exc:
        logger.exception(
            "add_recipient CRM error run_id=%d altegio_cid=%s: %s",
            run_id,
            altegio_cid,
            exc,
        )
        raise HTTPException(status_code=502, detail=f"Altegio CRM API error: {exc}")

    excluded_reason = compute_excluded_reason(
        wa_opted_out=wa_opted_out,
        phone_e164=phone_e164,
        crm_unavailable=crm_error is not None,
        service_unavailable=service_lookup_error is not None,
        count_before=count_before,
        count_after=count_after,
        lash_count=lash_count,
        attended_lash_count=attended_lash_count,
    )

    if excluded_reason is not None:
        raise HTTPException(
            status_code=422,
            detail={
                "message": f"Клиент не eligible: {excluded_reason}",
                "excluded_reason": excluded_reason,
                "crm_error": crm_error,
                "service_lookup_error": service_lookup_error,
                "count_before": count_before,
                "count_after": count_after,
                "lash_count": lash_count,
                "attended_lash_count": attended_lash_count,
            },
        )

    # --- Создать CampaignRecipient ---
    now_iso = datetime.now(timezone.utc).isoformat()
    async with SessionLocal() as session:
        async with session.begin():
            new_r = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=company_id,
                client_id=local_client.id if local_client else None,
                altegio_client_id=int(altegio_cid),
                phone_e164=phone_e164,
                display_name=display_name,
                total_records_in_period=len(in_period_records),
                confirmed_records_in_period=sum(1 for r in in_period_records if r.is_confirmed),
                records_before_period=count_before,
                records_after_period=count_after,
                lash_records_in_period=lash_count,
                confirmed_lash_records_in_period=attended_lash_count,
                service_titles_in_period=service_titles,
                total_records_before_period_any=count_before,
                local_client_found=local_client_found,
                is_opted_out=wa_opted_out,
                status="candidate",
                excluded_reason=None,
                meta={"manually_added_at": now_iso},
            )
            session.add(new_r)
            await session.flush()
            new_recipient_id = new_r.id

    await recompute_run_counters(run_id)

    logger.info(
        "manual_added recipient_id=%d run_id=%d phone=%s",
        new_recipient_id,
        run_id,
        phone_e164,
    )

    async with SessionLocal() as session:
        r = await session.get(CampaignRecipient, new_recipient_id)

    return {
        "run_id": run_id,
        "recipient": _recipient_dict(r),
        "message": "Получатель добавлен в snapshot",
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
        "is_discardable": (run.mode == "preview" and run.status not in ("discarded", "deleted")),
        "is_deletable": (run.mode == "preview" and run.status != "deleted"),
        "is_snapshot_editable": (run.mode == "preview" and run.status in ("completed", "running")),
        "excluded": {
            "opted_out": run.excluded_opted_out or 0,
            "no_phone": run.excluded_no_phone or 0,
            "invalid_phone": run.excluded_invalid_phone or 0,
            "no_whatsapp": run.excluded_no_whatsapp or 0,
            "multiple_records_in_period": run.excluded_multiple_records or 0,
            "no_confirmed_record_in_period": run.excluded_no_confirmed_record or 0,
            "has_records_before_period": run.excluded_has_records_before or 0,
            "crm_history_unavailable": run.excluded_crm_unavailable or 0,
            "service_category_unavailable": run.excluded_service_category_unavailable or 0,
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
                "crm_history_unavailable": run.excluded_crm_unavailable,
                "service_category_unavailable": run.excluded_service_category_unavailable,
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
