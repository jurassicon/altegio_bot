from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.campaigns.loyalty_cleanup import (
    cleanup_campaign_cards,
    make_card_number,
    make_card_text,
    resolve_or_issue_loyalty_card,
)
from altegio_bot.campaigns.segment import ClientCandidate, ClientSnapshot, find_candidates
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job, make_dedupe_key
from altegio_bot.models.models import (
    AltegioEvent,
    CampaignRecipient,
    CampaignRun,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.utils import utcnow

logger = logging.getLogger(__name__)

NEWSLETTER_JOB_TYPE = "newsletter_new_clients_monthly"
FOLLOWUP_JOB_TYPE = "newsletter_new_clients_followup"
CAMPAIGN_CODE = "new_clients_monthly"

# Новый job type: не отправка сообщения, а фоновое выполнение всей кампании
CAMPAIGN_EXECUTION_JOB_TYPE = "campaign_execute_new_clients_monthly"


@dataclass
class RunParams:
    """Параметры запуска кампании."""

    company_id: int
    location_id: int
    period_start: datetime
    period_end: datetime
    mode: Literal["preview", "send-real"]
    campaign_code: str = CAMPAIGN_CODE
    card_type_id: str | None = None
    source_preview_run_id: int | None = None
    attribution_window_days: int = 30
    followup_enabled: bool = False
    followup_delay_days: int | None = None
    followup_policy: str | None = None
    followup_template_name: str | None = None


async def _create_run(
    session: AsyncSession,
    params: RunParams,
    *,
    status: str = "running",
) -> CampaignRun:
    """Создать и сохранить CampaignRun с указанным статусом."""
    run = CampaignRun(
        campaign_code=params.campaign_code,
        mode=params.mode,
        company_ids=[params.company_id],
        source_preview_run_id=params.source_preview_run_id,
        location_id=params.location_id,
        card_type_id=params.card_type_id,
        period_start=params.period_start,
        period_end=params.period_end,
        status=status,
        attribution_window_days=params.attribution_window_days,
        followup_enabled=params.followup_enabled,
        followup_delay_days=params.followup_delay_days,
        followup_policy=params.followup_policy,
        followup_template_name=params.followup_template_name,
    )
    session.add(run)
    await session.flush()
    return run


async def _enqueue_campaign_execution_job(
    session: AsyncSession,
    *,
    run_id: int,
    company_id: int,
) -> MessageJob:
    """Поставить фоновый execution-job для кампании.

    Здесь не используем add_job(), потому что dedupe_key для campaign
    execution должен включать run_id, а стандартный make_dedupe_key()
    его не учитывает.
    """
    run_at = utcnow()
    job = MessageJob(
        company_id=company_id,
        record_id=None,
        client_id=None,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        run_at=run_at,
        status="queued",
        attempts=0,
        max_attempts=1,
        last_error=None,
        dedupe_key=(f"{CAMPAIGN_EXECUTION_JOB_TYPE}:{company_id}:run:{run_id}"),
        payload={
            "kind": CAMPAIGN_EXECUTION_JOB_TYPE,
            "campaign_run_id": run_id,
        },
        locked_at=None,
    )
    session.add(job)
    await session.flush()
    return job


def _params_from_run(run: CampaignRun) -> RunParams:
    """Собрать RunParams из уже созданного CampaignRun."""
    if run.mode != "send-real":
        raise RuntimeError(f"_params_from_run expects send-real run, got mode={run.mode!r}")

    company_ids = run.company_ids or []
    if not company_ids:
        raise RuntimeError(f"CampaignRun {run.id} has empty company_ids")

    return RunParams(
        company_id=int(company_ids[0]),
        location_id=int(run.location_id),
        period_start=run.period_start,
        period_end=run.period_end,
        mode="send-real",
        campaign_code=run.campaign_code,
        card_type_id=run.card_type_id,
        source_preview_run_id=run.source_preview_run_id,
        attribution_window_days=run.attribution_window_days,
        followup_enabled=run.followup_enabled,
        followup_delay_days=run.followup_delay_days,
        followup_policy=run.followup_policy,
        followup_template_name=run.followup_template_name,
    )


async def _mark_run_failed(run_id: int, error: str) -> None:
    """Пометить CampaignRun как failed и сохранить last_error в meta."""
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                return

            meta = dict(run.meta or {})
            meta["last_error"] = error
            run.meta = meta
            run.status = "failed"
            run.completed_at = utcnow()


def _build_recipient(
    run_id: int,
    candidate: ClientCandidate,
) -> CampaignRecipient:
    """Создать CampaignRecipient из кандидата."""
    client = candidate.client
    recipient = CampaignRecipient(
        campaign_run_id=run_id,
        company_id=client.company_id,
        client_id=client.id,
        altegio_client_id=client.altegio_client_id,
        phone_e164=client.phone_e164,
        display_name=client.display_name,
        total_records_in_period=candidate.total_records_in_period,
        confirmed_records_in_period=candidate.confirmed_records_in_period,
        records_before_period=candidate.records_before_period,
        records_after_period=candidate.records_after_period,
        lash_records_in_period=candidate.lash_records_in_period,
        confirmed_lash_records_in_period=candidate.confirmed_lash_records_in_period,
        service_titles_in_period=candidate.service_titles_in_period or [],
        total_records_before_period_any=candidate.records_before_period,
        local_client_found=candidate.local_client_found,
        is_opted_out=client.wa_opted_out,
        status="skipped" if candidate.excluded_reason else "candidate",
        excluded_reason=candidate.excluded_reason,
    )
    return recipient


def _update_run_exclusion_counters(
    run: CampaignRun,
    candidates: list[ClientCandidate],
) -> None:
    """Пересчитать счётчики исключений в run по результатам сегментации.

    Идемпотентна: перед накоплением все агрегируемые поля обнуляются.
    Повторный вызов с теми же candidates всегда даёт тот же результат.
    """
    # Обнуляем все счётчики перед пересчётом — идемпотентность
    run.total_clients_seen = 0
    run.candidates_count = 0
    run.excluded_opted_out = 0
    run.excluded_no_phone = 0
    run.excluded_has_records_before = 0
    run.excluded_multiple_records = 0
    run.excluded_more_than_one_record = 0
    run.excluded_no_confirmed_record = 0
    run.excluded_crm_unavailable = 0
    run.excluded_service_category_unavailable = 0
    run.excluded_returned_after_visit = 0

    run.total_clients_seen = len(candidates)

    for candidate in candidates:
        reason = candidate.excluded_reason
        if not reason:
            run.candidates_count += 1
            continue

        if reason == "opted_out":
            run.excluded_opted_out += 1
        elif reason == "no_phone":
            run.excluded_no_phone += 1
        elif reason == "has_records_before_period":
            run.excluded_has_records_before += 1
        elif reason in ("multiple_records_in_period", "multiple_lash_records_in_period"):
            run.excluded_multiple_records += 1
            run.excluded_more_than_one_record += 1
        elif reason in (
            "no_confirmed_record_in_period",
            "no_lash_record_in_period",
            "no_confirmed_lash_record_in_period",
        ):
            run.excluded_no_confirmed_record += 1
        elif reason == "crm_history_unavailable":
            run.excluded_crm_unavailable += 1
        elif reason == "service_category_unavailable":
            run.excluded_service_category_unavailable += 1
        elif reason == "returned_after_first_visit":
            run.excluded_returned_after_visit += 1


async def run_preview(params: RunParams) -> CampaignRun:
    """Запустить preview: сегментация без отправки.

    Транзакционная безопасность:
      1. Короткая транзакция: создать run со статусом 'running'.
      2. CRM HTTP-запросы без открытой транзакции БД.
      3. Короткая транзакция: сохранить recipients + перевести в 'completed'.
    """
    # Фаза 1: создать run (короткая транзакция)
    async with SessionLocal() as session:
        async with session.begin():
            run = await _create_run(session, params, status="running")
            run_id = run.id

    # Фаза 2: CRM HTTP-запросы (без открытой транзакции БД)
    try:
        candidates = await find_candidates(
            company_id=params.company_id,
            period_start=params.period_start,
            period_end=params.period_end,
        )
    except Exception as exc:
        await _mark_run_failed(run_id, str(exc))
        raise

    # Фаза 3: сохранить результаты (короткая транзакция)
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f"CampaignRun {run_id} disappeared after creation")

            # Финальная проверка статуса — до любых изменений.
            # Если между фазой 1 и фазой 3 run был переведён во внешнее состояние
            # (например, failed через timeout или отмену), откатываем транзакцию
            # и сигнализируем об ошибке. Не должно быть ложного success-flow.
            if run.status != "running":
                logger.error(
                    "preview run_id=%d: finalization aborted — status=%r instead of 'running' "
                    "(run was externally modified between segmentation and finalization)",
                    run_id,
                    run.status,
                )
                raise RuntimeError(
                    f"preview run_id={run_id}: finalization aborted — status was {run.status!r} instead of 'running'"
                )

            _update_run_exclusion_counters(run, candidates)
            for candidate in candidates:
                session.add(_build_recipient(run_id, candidate))

            # Записать источник обнаружения кандидатов в meta
            meta = dict(run.meta or {})
            meta["discovery_source"] = "crm_api"
            run.meta = meta

            run.status = "completed"
            run.completed_at = utcnow()

            # Захватить значения счётчиков до закрытия сессии.
            # SessionLocal настроен с expire_on_commit=False, поэтому атрибуты
            # доступны после коммита. Но явные локальные переменные надёжнее.
            total_seen = run.total_clients_seen
            cands_count = run.candidates_count

    logger.info(
        "preview completed run_id=%d company=%d period=[%s, %s) total=%d eligible=%d",
        run_id,
        params.company_id,
        params.period_start.date(),
        params.period_end.date(),
        total_seen,
        cands_count,
    )
    # expire_on_commit=False — атрибуты run доступны после закрытия сессии.
    return run


async def enqueue_send_real(params: RunParams) -> CampaignRun:
    """Создать queued-run и поставить execution-job в message_jobs.

    HTTP endpoint использует именно эту функцию, чтобы быстро вернуть
    accepted и не ждать весь send-real синхронно.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await _create_run(session, params, status="queued")
            await _enqueue_campaign_execution_job(
                session,
                run_id=run.id,
                company_id=params.company_id,
            )

    logger.info(
        "send-real queued run_id=%d company=%d period=[%s, %s)",
        run.id,
        params.company_id,
        params.period_start.date(),
        params.period_end.date(),
    )
    return run


async def _load_candidates_from_preview_snapshot(
    session: AsyncSession,
    preview_run_id: int,
) -> list[ClientCandidate]:
    """Загрузить кандидатов из snapshot preview-run.

    Не пересчитывает сегментацию — использует сохранённые CampaignRecipient.

    Три случая для каждого recipient:
      1. r.client_id is None (CRM-only client): локальной записи не было и не ожидается.
         Восстанавливаем snapshot из recipient.phone_e164 (нормализованный через CRM fallback).
         excluded_reason=None (eligible) сохраняется как-есть — send-real обработает его.
      2. r.client_id is not None, local client найден: используем свежие данные из БД.
      3. r.client_id is not None, local client не найден: данные испорчены / клиент удалён.
         Помечаем excluded_reason='no_local_client'.
    """
    stmt = (
        select(CampaignRecipient)
        .where(CampaignRecipient.campaign_run_id == preview_run_id)
        .order_by(CampaignRecipient.id.asc())
    )
    preview_recipients = (await session.execute(stmt)).scalars().all()

    if not preview_recipients:
        raise RuntimeError(f"Preview run {preview_run_id} has no recipients (empty snapshot)")

    # Предзагрузить клиентов одним запросом (только те, у кого есть client_id)
    local_client_ids = [r.client_id for r in preview_recipients if r.client_id is not None]
    clients_map: dict[int, Client] = {}
    if local_client_ids:
        clients_stmt = select(Client).where(Client.id.in_(local_client_ids))
        clients_map = {c.id: c for c in (await session.execute(clients_stmt)).scalars()}

    candidates: list[ClientCandidate] = []
    for r in preview_recipients:
        local_client = clients_map.get(r.client_id) if r.client_id else None

        if local_client is not None:
            # Случай 2: локальный клиент найден — используем свежие данные
            excluded = r.excluded_reason  # None → eligible
            snapshot = ClientSnapshot(
                id=local_client.id,
                company_id=local_client.company_id,
                altegio_client_id=local_client.altegio_client_id,
                display_name=local_client.display_name,
                phone_e164=local_client.phone_e164,
                wa_opted_out=bool(local_client.wa_opted_out),
            )
        elif r.client_id is None:
            # Случай 1: CRM-only client (client_id=None в preview snapshot).
            # phone_e164 сохранён в recipient из CRM fallback (_normalize_phone).
            # Восстанавливаем snapshot точно как в preview — не назначаем no_local_client.
            excluded = r.excluded_reason  # None если eligible в preview
            snapshot = ClientSnapshot(
                id=None,
                company_id=r.company_id,
                altegio_client_id=r.altegio_client_id,
                display_name=r.display_name,
                phone_e164=r.phone_e164,
                wa_opted_out=bool(r.is_opted_out),
            )
        else:
            # Случай 3: client_id был задан, но клиент исчез из БД (удалён/испорчен).
            # Это нештатная ситуация — помечаем excluded.
            excluded = r.excluded_reason or "no_local_client"
            snapshot = ClientSnapshot(
                id=None,
                company_id=r.company_id,
                altegio_client_id=r.altegio_client_id,
                display_name=r.display_name,
                phone_e164=r.phone_e164,
                wa_opted_out=bool(r.is_opted_out),
            )

        c = ClientCandidate(
            client=snapshot,
            total_records_in_period=r.total_records_in_period or 0,
            confirmed_records_in_period=r.confirmed_records_in_period or 0,
            lash_records_in_period=r.lash_records_in_period or 0,
            confirmed_lash_records_in_period=r.confirmed_lash_records_in_period or 0,
            service_titles_in_period=list(r.service_titles_in_period or []),
            records_before_period=r.records_before_period or 0,
            records_after_period=r.records_after_period or 0,
            local_client_found=local_client is not None,
            excluded_reason=excluded,
        )
        candidates.append(c)

    logger.info(
        "snapshot loaded preview_run_id=%d total=%d eligible=%d",
        preview_run_id,
        len(candidates),
        sum(1 for c in candidates if c.is_eligible),
    )
    return candidates


async def _auto_hide_preview_run(
    session: AsyncSession,
    preview_run_id: int,
    *,
    send_real_run_id: int,
) -> None:
    """Set meta['hidden']=True on a preview run after a clean send-real.

    The preview is NOT deleted — it stays in the DB and remains accessible
    via direct link.  This call is idempotent: if the preview is already
    hidden it does nothing.

    Must be called inside an active transaction (the caller's session.begin()
    context).
    """
    preview = await session.get(CampaignRun, preview_run_id)
    if preview is None:
        logger.warning(
            "auto-hide: preview run_id=%d not found (referenced by send-real run_id=%d)",
            preview_run_id,
            send_real_run_id,
        )
        return
    if preview.mode != "preview":
        logger.warning(
            "auto-hide: run_id=%d is mode=%r, not preview — skipping",
            preview_run_id,
            preview.mode,
        )
        return
    preview_meta = dict(preview.meta or {})
    if preview_meta.get("hidden"):
        return  # Already hidden — idempotent
    preview_meta["hidden"] = True
    preview_meta["hidden_reason"] = "auto_hidden_after_successful_send_real"
    preview_meta["hidden_by_run_id"] = send_real_run_id
    preview.meta = preview_meta
    logger.info(
        "auto-hid preview run_id=%d after successful send-real run_id=%d",
        preview_run_id,
        send_real_run_id,
    )


async def _execute_send_real_for_existing_run(
    run_id: int,
    params: RunParams,
) -> None:
    """Выполнить send-real для уже созданного CampaignRun.

    Если source_preview_run_id задан — использует snapshot preview-run
    и НЕ пересчитывает сегментацию. Это гарантирует, что оператор
    запускает именно то, что видел в preview.

    Транзакционная безопасность:
      1. Короткая транзакция: перевести run в 'running'.
      2. Загрузить кандидатов — без открытой транзакции БД:
         - snapshot: короткая сессия (только SELECT).
         - fresh: CRM HTTP-запросы без открытой транзакции.
      3. Короткая транзакция: обновить счётчики run.
      4. Обработать каждого кандидата (loyalty + messages).
      5. Короткая транзакция: финализировать run.
    """
    # Фаза 1: перевести в 'running' (короткая транзакция)
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f"CampaignRun {run_id} not found")

            existing_recipients = await session.scalar(
                select(func.count()).select_from(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
            )
            if int(existing_recipients or 0) > 0:
                raise RuntimeError(f"CampaignRun {run_id} already has recipients; re-running is unsafe")

            run.status = "running"
            run.completed_at = None

            meta = dict(run.meta or {})
            meta.pop("last_error", None)
            if params.source_preview_run_id:
                meta["used_preview_snapshot"] = params.source_preview_run_id
                # Источник данных — snapshot preview-run (сегментация через CRM API в preview).
                meta["discovery_source"] = "preview_snapshot"
            else:
                # Свежая сегментация через CRM API (get_company_period_client_refs).
                meta["discovery_source"] = "crm_api"
            run.meta = meta

    # Фаза 2: загрузить кандидатов (вне открытой транзакции)
    if params.source_preview_run_id:
        # Snapshot: короткая сессия только для SELECT.
        # CRM-only клиенты восстанавливаются из recipient.phone_e164 (CRM fallback).
        async with SessionLocal() as session:
            candidates = await _load_candidates_from_preview_snapshot(
                session,
                params.source_preview_run_id,
            )
    else:
        # Свежая сегментация — CRM API discovery без открытой транзакции БД.
        candidates = await find_candidates(
            company_id=params.company_id,
            period_start=params.period_start,
            period_end=params.period_end,
        )

    # Фаза 3: обновить счётчики (короткая транзакция)
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f"CampaignRun {run_id} not found")
            _update_run_exclusion_counters(run, candidates)

    loyalty = AltegioLoyaltyClient()

    try:
        card_type_id = await _resolve_card_type(
            loyalty,
            params.location_id,
            params.card_type_id,
        )

        stats = {
            "cleanup_failed": 0,
            "cards_deleted": 0,
            "cards_issued": 0,
            "cards_reused": 0,
            "queued": 0,
            "failed": 0,
        }

        # candidates уже получены выше при подсчёте счётчиков — повторный
        # find_candidates не нужен и мог бы дать расхождение с уже сохранёнными
        # run.candidates_count / run.excluded_* если данные изменились.
        for candidate in candidates:
            if candidate.excluded_reason:
                async with SessionLocal() as session:
                    async with session.begin():
                        recipient = _build_recipient(run_id, candidate)
                        session.add(recipient)
                continue

            await _process_eligible(
                loyalty=loyalty,
                candidate=candidate,
                run_id=run_id,
                company_id=params.company_id,
                location_id=params.location_id,
                card_type_id=card_type_id,
                campaign_code=params.campaign_code,
                stats=stats,
            )

        async with SessionLocal() as session:
            async with session.begin():
                run = await session.get(CampaignRun, run_id)
                if run is None:
                    raise RuntimeError(f"CampaignRun {run_id} not found")

                # Финальная проверка статуса — до любых изменений.
                # Если между фазой 1 и финализацией run был переведён во внешнее состояние,
                # откатываем транзакцию и сигнализируем. Не должно быть ложного success-flow.
                if run.status != "running":
                    logger.error(
                        "send-real run_id=%d: finalization aborted — status=%r instead of 'running' "
                        "(run was externally modified between processing and finalization)",
                        run_id,
                        run.status,
                    )
                    raise RuntimeError(
                        f"send-real run_id={run_id}: finalization aborted — "
                        f"status was {run.status!r} instead of 'running'"
                    )

                run.cleanup_failed_count = stats["cleanup_failed"]
                run.cards_deleted_count = stats["cards_deleted"]
                run.cards_issued_count = stats["cards_issued"]
                run.queued_count = stats["queued"]
                run.failed_count = stats["failed"]
                run.sent_count = stats["queued"]

                # Determine final status based on outcomes.
                # eligible_count = candidates that entered _process_eligible (no pre-run exclusion)
                eligible_count = sum(1 for c in candidates if not c.excluded_reason)

                # prequeue_failures covers every path that stops a recipient short of
                # being queued: card issuance failure AND loyalty-card cleanup failure.
                prequeue_failures = stats["failed"] + stats["cleanup_failed"]

                meta = dict(run.meta or {})
                meta.pop("last_error", None)
                meta.pop("partial_failure", None)
                meta.pop("partial_failure_count", None)
                meta.pop("cards_reused_count", None)
                if stats["cards_reused"]:
                    meta["cards_reused_count"] = stats["cards_reused"]

                if eligible_count > 0 and stats["queued"] == 0 and prequeue_failures > 0:
                    # All eligible recipients failed before any job was queued.
                    # Setting status=completed would silently hide the failure — use failed instead.
                    error_msg = (
                        f"Campaign finished with 0 queued recipients; "
                        f"pre-queue failures for {prequeue_failures} recipient(s) "
                        f"(card_issue_failed={stats['failed']}, cleanup_failed={stats['cleanup_failed']})"
                    )
                    meta["last_error"] = error_msg
                    run.status = "failed"
                    logger.error(
                        "send-real all-failed run_id=%d company=%d eligible=%d "
                        "prequeue_failures=%d (failed=%d cleanup_failed=%d)",
                        run_id,
                        params.company_id,
                        eligible_count,
                        prequeue_failures,
                        stats["failed"],
                        stats["cleanup_failed"],
                    )
                elif stats["queued"] > 0 and prequeue_failures > 0:
                    # Partial success: some recipients queued, some failed before queue.
                    meta["partial_failure"] = True
                    meta["partial_failure_count"] = prequeue_failures
                    run.status = "completed"
                else:
                    run.status = "completed"
                    # Preview is NOT hidden here: message jobs are only
                    # queued at this point — outbox has not processed them
                    # yet.  Auto-hide runs later inside
                    # recompute_campaign_run_stats once all recipients
                    # have left the in-flight states.

                run.meta = meta
                run.completed_at = utcnow()
                final_status = run.status  # capture before session closes

        # Лог только после успешной финализации (exception выше не допустит сюда при guard)
        logger.info(
            "send-real finished run_id=%d company=%d status=%s stats=%s",
            run_id,
            params.company_id,
            final_status,
            stats,
        )

    except Exception as exc:
        logger.exception(
            "send-real failed run_id=%d company=%d: %s",
            run_id,
            params.company_id,
            exc,
        )
        await _mark_run_failed(run_id, str(exc))
        raise

    finally:
        await loyalty.aclose()


async def execute_queued_send_real(run_id: int) -> None:
    """Выполнить queued send-real CampaignRun."""
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise RuntimeError(f"CampaignRun {run_id} not found")

        if run.mode != "send-real":
            raise RuntimeError(f"CampaignRun {run_id} mode={run.mode} is not send-real")

        if run.status == "completed":
            logger.info(
                "skip queued execution run_id=%d (already completed)",
                run_id,
            )
            return

        if run.status == "failed":
            # failed run нельзя повторно исполнять через старый execution job —
            # для retry используется resume_send_real.
            logger.warning(
                "skip queued execution run_id=%d (already failed)",
                run_id,
            )
            return

        params = _params_from_run(run)

    await _execute_send_real_for_existing_run(run_id, params)


async def run_send_real(params: RunParams) -> CampaignRun:
    """Синхронный запуск send-real.

    Оставлен для ручных/тестовых сценариев. API лучше использовать
    через enqueue_send_real(), чтобы не держать HTTP-запрос.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await _create_run(session, params, status="running")
            run_id = run.id

    await _execute_send_real_for_existing_run(run_id, params)

    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise RuntimeError(f"CampaignRun {run_id} not found")
        return run


async def _process_eligible(
    *,
    loyalty: AltegioLoyaltyClient,
    candidate: ClientCandidate,
    run_id: int,
    company_id: int,
    location_id: int,
    card_type_id: str,
    campaign_code: str,
    stats: dict,
) -> None:
    client = candidate.client
    client_id = client.id  # None для CRM-only clients — допустимо

    async with SessionLocal() as session:
        async with session.begin():
            recipient = _build_recipient(run_id, candidate)
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    async with SessionLocal() as session:
        cleanup = await cleanup_campaign_cards(
            session,
            loyalty,
            location_id=location_id,
            client_id=client_id,
            campaign_code=campaign_code,
        )

    if not cleanup.ok:
        stats["cleanup_failed"] += 1
        stats["cards_deleted"] += len(cleanup.deleted_ids)
        async with SessionLocal() as session:
            async with session.begin():
                recipient = await session.get(
                    CampaignRecipient,
                    recipient_id,
                )
                if recipient:
                    recipient.status = "cleanup_failed"
                    recipient.excluded_reason = "cleanup_failed"
                    recipient.cleanup_card_ids = cleanup.deleted_ids
                    recipient.cleanup_failed_reason = cleanup.reason
        return

    stats["cards_deleted"] += len(cleanup.deleted_ids)

    phone_e164 = client.phone_e164

    try:
        async with SessionLocal() as session:
            resolution = await resolve_or_issue_loyalty_card(
                session,
                loyalty,
                phone_e164=phone_e164,
                location_id=location_id,
                card_type_id=card_type_id,
                campaign_code=campaign_code,
                company_id=company_id,
            )
    except Exception as exc:
        stats["failed"] += 1
        async with SessionLocal() as session:
            async with session.begin():
                recipient = await session.get(
                    CampaignRecipient,
                    recipient_id,
                )
                if recipient:
                    recipient.status = "skipped"
                    recipient.excluded_reason = "card_issue_failed"
                    recipient.cleanup_card_ids = cleanup.deleted_ids
        logger.error("card_issue_failed client_id=%s: %s", client_id, exc)
        return

    if resolution.outcome == "failed_conflict":
        stats["failed"] += 1
        async with SessionLocal() as session:
            async with session.begin():
                recipient = await session.get(
                    CampaignRecipient,
                    recipient_id,
                )
                if recipient:
                    recipient.status = "skipped"
                    recipient.excluded_reason = "card_conflict"
                    recipient.cleanup_failed_reason = resolution.reason
                    recipient.cleanup_card_ids = cleanup.deleted_ids
        logger.error(
            "card_conflict client_id=%s phone=%s: %s",
            client_id,
            phone_e164,
            resolution.reason,
        )
        return

    if resolution.outcome == "reused_existing":
        stats["cards_reused"] += 1
    else:
        stats["cards_issued"] += 1

    issued_id = resolution.loyalty_card_id
    issued_number = resolution.loyalty_card_number
    loyalty_card_text = make_card_text(issued_number)
    run_at = utcnow()

    try:
        async with SessionLocal() as session:
            async with session.begin():
                recipient = await session.get(
                    CampaignRecipient,
                    recipient_id,
                )
                if recipient is None:
                    raise RuntimeError(f"recipient_id={recipient_id} not found")

                recipient.loyalty_card_id = issued_id
                recipient.loyalty_card_number = issued_number
                recipient.loyalty_card_type_id = card_type_id
                recipient.cleanup_card_ids = cleanup.deleted_ids
                recipient.status = "card_issued"

                # Для CRM-only клиентов (client_id=None) сохраняем phone_e164 в payload,
                # чтобы outbox_worker мог отправить сообщение без локальной записи Client
                # (и создать Chatwoot-зеркало по номеру телефона).
                job_payload: dict = {
                    "kind": NEWSLETTER_JOB_TYPE,
                    "loyalty_card_text": loyalty_card_text,
                    "campaign_run_id": run_id,
                    "campaign_recipient_id": recipient_id,
                }
                if client_id is None:
                    job_payload["phone_e164"] = phone_e164
                    if client.display_name:
                        job_payload["contact_name"] = client.display_name

                await add_job(
                    session,
                    company_id=company_id,
                    record_id=None,
                    client_id=client_id,
                    job_type=NEWSLETTER_JOB_TYPE,
                    run_at=run_at,
                    payload=job_payload,
                )

                dedupe_key = make_dedupe_key(
                    job_type=NEWSLETTER_JOB_TYPE,
                    company_id=company_id,
                    record_id=None,
                    run_at=run_at,
                )
                job = await session.scalar(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
                if job:
                    recipient.message_job_id = job.id

                recipient.status = "queued"

        stats["queued"] += 1
        logger.info(
            "queued client_id=%s card=%s",
            client_id,
            issued_number,
        )

    except Exception as exc:
        stats["failed"] += 1
        logger.error(
            "queue_failed client_id=%s: %s",
            client_id,
            exc,
        )

        async with SessionLocal() as session:
            async with session.begin():
                recipient = await session.get(
                    CampaignRecipient,
                    recipient_id,
                )
                if recipient:
                    recipient.status = "skipped"
                    recipient.excluded_reason = "queue_failed"
                    recipient.loyalty_card_id = issued_id
                    recipient.loyalty_card_number = issued_number
                    recipient.loyalty_card_type_id = card_type_id
                    recipient.cleanup_card_ids = cleanup.deleted_ids


async def _resolve_card_type(
    loyalty: AltegioLoyaltyClient,
    location_id: int,
    card_type_id: str | None,
) -> str:
    """Вернуть card_type_id, запросив из API если не задан."""
    if card_type_id:
        return card_type_id

    types = await loyalty.get_card_types(location_id)
    if not types:
        raise RuntimeError(f"No loyalty card types for location_id={location_id}")

    type_id = types[0].get("id") or types[0].get("loyalty_card_type_id")
    return str(type_id)


# ==========================================================================
# Resume: продолжение failed send-real run
# ==========================================================================


async def _enqueue_newsletter_job_for_recipient(
    *,
    recipient_id: int,
    company_id: int,
    client_id: int | None,
    run_id: int,
    loyalty_card_number: str,
    loyalty_card_id: str,
    loyalty_card_type_id: str,
    phone_e164: str | None = None,
    contact_name: str | None = None,
) -> None:
    """Создать MessageJob рассылки и перевести recipient → queued.

    Не выполняет cleanup и не выпускает карту. Используется при resume
    для получателей, у которых карта уже выпущена (card_issued, queue_failed).

    Для CRM-only клиентов (client_id=None) передавать phone_e164, чтобы
    outbox_worker мог отправить сообщение без локальной записи Client.
    """
    loyalty_card_text = make_card_text(loyalty_card_number)
    run_at = utcnow()

    async with SessionLocal() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_id)
            if recipient is None:
                raise RuntimeError(f"recipient_id={recipient_id} not found")

            recipient.loyalty_card_id = loyalty_card_id
            recipient.loyalty_card_number = loyalty_card_number
            recipient.loyalty_card_type_id = loyalty_card_type_id
            # Сбрасываем excluded_reason — получатель теперь снова в пайплайне
            recipient.excluded_reason = None

            resume_payload: dict = {
                "kind": NEWSLETTER_JOB_TYPE,
                "loyalty_card_text": loyalty_card_text,
                "campaign_run_id": run_id,
                "campaign_recipient_id": recipient_id,
            }
            if client_id is None and phone_e164:
                resume_payload["phone_e164"] = phone_e164
                if contact_name:
                    resume_payload["contact_name"] = contact_name

            await add_job(
                session,
                company_id=company_id,
                record_id=None,
                client_id=client_id,
                job_type=NEWSLETTER_JOB_TYPE,
                run_at=run_at,
                payload=resume_payload,
            )

            dedupe_key = make_dedupe_key(
                job_type=NEWSLETTER_JOB_TYPE,
                company_id=company_id,
                record_id=None,
                run_at=run_at,
            )
            job = await session.scalar(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
            if job:
                recipient.message_job_id = job.id

            recipient.status = "queued"

    logger.info(
        "resume queued recipient_id=%d client_id=%d run_id=%d",
        recipient_id,
        client_id,
        run_id,
    )


def _is_manual_action_recipient(recipient: CampaignRecipient) -> bool:
    if recipient.status == "cleanup_failed":
        return True
    if recipient.status == "skipped" and recipient.excluded_reason == "card_issue_failed":
        return True
    return False


def _is_resume_pending_recipient(recipient: CampaignRecipient) -> bool:
    if recipient.status in ("candidate", "card_issued"):
        return True
    if recipient.status == "skipped" and recipient.excluded_reason == "queue_failed":
        return True
    return False


async def _recalculate_run_after_resume(run_id: int) -> tuple[int, int]:
    """Пересчитать агрегаты CampaignRun по snapshot-данным recipient'ов.

    Возвращает:
      remaining_manual_count,
      remaining_pending_count
    """
    async with SessionLocal() as session:
        stmt = select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
        recipients = (await session.execute(stmt)).scalars().all()

        queued_count = 0
        cleanup_failed_count = 0
        failed_count = 0
        cards_issued_count = 0
        cards_deleted_count = 0
        remaining_manual_count = 0
        remaining_pending_count = 0

        for recipient in recipients:
            if recipient.message_job_id is not None:
                queued_count += 1

            if recipient.loyalty_card_id:
                cards_issued_count += 1

            cards_deleted_count += len(recipient.cleanup_card_ids or [])

            if recipient.status == "cleanup_failed":
                cleanup_failed_count += 1

            if recipient.status == "skipped" and recipient.excluded_reason in (
                "card_issue_failed",
                "queue_failed",
            ):
                failed_count += 1

            if _is_manual_action_recipient(recipient):
                remaining_manual_count += 1

            if _is_resume_pending_recipient(recipient):
                remaining_pending_count += 1

    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f"CampaignRun {run_id} not found after resume")

            run.queued_count = queued_count
            run.sent_count = queued_count
            run.cards_issued_count = cards_issued_count
            run.cards_deleted_count = cards_deleted_count
            run.cleanup_failed_count = cleanup_failed_count
            run.failed_count = failed_count

    return remaining_manual_count, remaining_pending_count


async def _resume_candidate_recipient(
    *,
    loyalty: AltegioLoyaltyClient,
    recipient: CampaignRecipient,
    run_id: int,
    company_id: int,
    location_id: int,
    card_type_id: str,
    campaign_code: str,
) -> bool:
    """Resume flow для candidate recipient: cleanup → выпуск карты → очередь.

    Использует уже существующего CampaignRecipient (создавать нового не нужно).
    Возвращает True, если recipient успешно поставлен в очередь.
    """
    recipient_id = recipient.id
    client_id = recipient.client_id
    phone_e164 = recipient.phone_e164

    # Cleanup — читаем в отдельной сессии, удаление карт через API
    async with SessionLocal() as session:
        cleanup = await cleanup_campaign_cards(
            session,
            loyalty,
            location_id=location_id,
            client_id=client_id,
            campaign_code=campaign_code,
        )

    if not cleanup.ok:
        async with SessionLocal() as session:
            async with session.begin():
                r = await session.get(CampaignRecipient, recipient_id)
                if r:
                    r.status = "cleanup_failed"
                    r.excluded_reason = "cleanup_failed"
                    r.cleanup_card_ids = cleanup.deleted_ids
                    r.cleanup_failed_reason = cleanup.reason
        logger.warning(
            "resume cleanup_failed recipient_id=%d run_id=%d: %s",
            recipient_id,
            run_id,
            cleanup.reason,
        )
        return False

    card_number = make_card_number(phone_e164)
    phone_num = int(phone_e164.lstrip("+"))

    try:
        card = await loyalty.issue_card(
            location_id,
            loyalty_card_number=card_number,
            loyalty_card_type_id=card_type_id,
            phone=phone_num,
        )
        issued_number = str(card.get("loyalty_card_number") or card_number)
        issued_id = str(card.get("id") or card.get("loyalty_card_id") or "")
    except Exception as exc:
        async with SessionLocal() as session:
            async with session.begin():
                r = await session.get(CampaignRecipient, recipient_id)
                if r:
                    r.status = "skipped"
                    r.excluded_reason = "card_issue_failed"
                    r.cleanup_card_ids = cleanup.deleted_ids
        logger.error(
            "resume card_issue_failed recipient_id=%d run_id=%d: %s",
            recipient_id,
            run_id,
            exc,
        )
        return False

    try:
        await _enqueue_newsletter_job_for_recipient(
            recipient_id=recipient_id,
            company_id=company_id,
            client_id=client_id,
            run_id=run_id,
            loyalty_card_number=issued_number,
            loyalty_card_id=issued_id,
            loyalty_card_type_id=card_type_id,
            phone_e164=phone_e164,
            contact_name=recipient.display_name,
        )
        # Сохранить cleanup_card_ids, если cleanup что-то удалил и поле ещё пустое
        if cleanup.deleted_ids:
            async with SessionLocal() as session:
                async with session.begin():
                    r = await session.get(CampaignRecipient, recipient_id)
                    if r and not r.cleanup_card_ids:
                        r.cleanup_card_ids = cleanup.deleted_ids
        return True

    except Exception as exc:
        logger.error(
            "resume queue_failed recipient_id=%d run_id=%d: %s",
            recipient_id,
            run_id,
            exc,
        )
        async with SessionLocal() as session:
            async with session.begin():
                r = await session.get(CampaignRecipient, recipient_id)
                if r:
                    r.status = "skipped"
                    r.excluded_reason = "queue_failed"
                    r.loyalty_card_id = issued_id
                    r.loyalty_card_number = issued_number
                    r.loyalty_card_type_id = card_type_id
                    r.cleanup_card_ids = cleanup.deleted_ids
        return False


async def resume_send_real(run_id: int) -> dict:
    """Продолжить выполнение failed send-real run по сохранённому snapshot.

    Не пересчитывает сегмент. Работает только с уже существующими
    CampaignRecipient. Безопасен: не трогает cleanup_failed и
    card_issue_failed.

    Разрешённые для resume статусы получателей:
      - candidate: полный flow (cleanup → выпуск карты → очередь)
      - card_issued: только допоставить MessageJob
      - skipped + queue_failed с картой: только допоставить MessageJob

    Не трогаются (manual action required):
      - cleanup_failed
      - skipped + card_issue_failed

    Логика финального статуса run:
      - все pending обработаны, ничего manual → completed
      - все pending обработаны, но есть manual → failed +
        "partially completed"
      - остались pending-получатели → failed + "still pending"
        (можно повторить)

    Возвращает:
      resumed_count — успешно поставлено в очередь за этот запуск
      skipped_count — не тронуто (уже queued или исключено по другой
        причине)
      remaining_manual_count — требует ручных действий
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise ValueError(f"CampaignRun {run_id} not found")
        if run.mode != "send-real":
            raise ValueError(f"Resume доступен только для send-real. mode={run.mode!r}")
        if run.status != "failed":
            raise ValueError(f"Resume доступен только для failed run. status={run.status!r}")
        params = _params_from_run(run)

    async with SessionLocal() as session:
        stmt = select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
        recipients = (await session.execute(stmt)).scalars().all()

    loyalty = AltegioLoyaltyClient()
    stats = {"resumed": 0, "skipped": 0}

    try:
        card_type_id = await _resolve_card_type(
            loyalty,
            params.location_id,
            params.card_type_id,
        )

        for recipient in recipients:
            st = recipient.status
            reason = recipient.excluded_reason

            if st == "queued":
                stats["skipped"] += 1
                continue

            if st == "cleanup_failed":
                continue

            if st == "skipped" and reason == "card_issue_failed":
                continue

            if st == "skipped" and reason != "queue_failed":
                stats["skipped"] += 1
                continue

            try:
                if st == "candidate":
                    ok = await _resume_candidate_recipient(
                        loyalty=loyalty,
                        recipient=recipient,
                        run_id=run_id,
                        company_id=params.company_id,
                        location_id=params.location_id,
                        card_type_id=card_type_id,
                        campaign_code=params.campaign_code,
                    )
                    if ok:
                        stats["resumed"] += 1

                elif st == "card_issued" or (st == "skipped" and reason == "queue_failed"):
                    await _enqueue_newsletter_job_for_recipient(
                        recipient_id=recipient.id,
                        company_id=params.company_id,
                        client_id=recipient.client_id,
                        run_id=run_id,
                        loyalty_card_number=(recipient.loyalty_card_number or ""),
                        loyalty_card_id=(recipient.loyalty_card_id or ""),
                        loyalty_card_type_id=(recipient.loyalty_card_type_id or card_type_id),
                        phone_e164=recipient.phone_e164,
                        contact_name=recipient.display_name,
                    )
                    stats["resumed"] += 1

            except Exception as exc:
                logger.error(
                    "resume step failed recipient_id=%d run_id=%d: %s",
                    recipient.id,
                    run_id,
                    exc,
                )
                # Не прерываем весь resume из-за одного получателя

    finally:
        await loyalty.aclose()

    remaining_manual_count, remaining_pending_count = await _recalculate_run_after_resume(run_id)

    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f"CampaignRun {run_id} not found after resume")

            meta = dict(run.meta or {})

            if remaining_pending_count == 0 and remaining_manual_count == 0:
                run.status = "completed"
                meta.pop("last_error", None)
                run.completed_at = utcnow()
            elif remaining_pending_count == 0:
                run.status = "failed"
                meta["last_error"] = "Resume completed partially: some recipients still require manual action"
                run.completed_at = utcnow()
            else:
                run.status = "failed"
                meta["last_error"] = f"Resume partially failed: {remaining_pending_count} recipients still pending"

            run.meta = meta

    logger.info(
        "resume_send_real run_id=%d stats=%s manual=%d pending=%d",
        run_id,
        stats,
        remaining_manual_count,
        remaining_pending_count,
    )

    return {
        "resumed_count": stats["resumed"],
        "skipped_count": stats["skipped"],
        "remaining_manual_count": remaining_manual_count,
    }


# ==========================================================================
# Discard preview run
# ==========================================================================


async def discard_preview_run(run_id: int) -> None:
    """Пометить preview-run как discarded (soft-delete).

    Правила:
    - Только для mode='preview'.
    - Только если run ещё не использован как source для send-real.
    - Только если status не 'discarded' уже.
    - Дискардинг запрещён, если run использован как source_preview_run_id
      хотя бы в одном send-real run.

    Raises:
        ValueError: при нарушении правил.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise ValueError(f"CampaignRun {run_id} not found")

            if run.mode != "preview":
                raise ValueError(f"Discard доступен только для preview run. mode={run.mode!r}")

            # Discard разрешён только для completed preview.
            # running → race condition: run_preview() ещё записывает статус.
            # discarded/deleted → уже в терминальном состоянии.
            # failed/queued → нестабильные состояния, discard не имеет смысла.
            if run.status != "completed":
                raise ValueError(f"Discard доступен только для completed preview. status={run.status!r}")

            # Проверить: не использован ли этот preview как источник send-real
            used_as_source = await session.scalar(
                select(func.count())
                .select_from(CampaignRun)
                .where(CampaignRun.source_preview_run_id == run_id)
                .where(CampaignRun.mode == "send-real")
            )
            if int(used_as_source or 0) > 0:
                raise ValueError(f"Preview run {run_id} уже использован как источник для send-real — discard запрещён.")

            run.status = "discarded"
            run.completed_at = utcnow()
            meta = dict(run.meta or {})
            meta["discarded_at"] = utcnow().isoformat()
            run.meta = meta

    logger.info("preview run_id=%d discarded", run_id)


# ==========================================================================
# Counter recompute from existing DB recipients
# ==========================================================================


def _update_run_counters_from_recipients(
    run: CampaignRun,
    recipients: list[CampaignRecipient],
) -> None:
    """Пересчитать счётчики CampaignRun по существующим CampaignRecipient.

    Идемпотентна: обнуляет все агрегаты перед накоплением.
    Логика маппинга excluded_reason → счётчики идентична
    _update_run_exclusion_counters.  Используется после ручного
    добавления / удаления получателей из snapshot.

    Причина «manual_removed» не маппируется ни в один специализированный
    счётчик — такой получатель виден в total_clients_seen, но не в
    candidates_count и не в excluded_*.
    """
    run.total_clients_seen = 0
    run.candidates_count = 0
    run.excluded_opted_out = 0
    run.excluded_no_phone = 0
    run.excluded_has_records_before = 0
    run.excluded_multiple_records = 0
    run.excluded_more_than_one_record = 0
    run.excluded_no_confirmed_record = 0
    run.excluded_crm_unavailable = 0
    run.excluded_service_category_unavailable = 0
    run.excluded_returned_after_visit = 0

    run.total_clients_seen = len(recipients)

    for r in recipients:
        reason = r.excluded_reason
        if not reason:
            run.candidates_count += 1
            continue
        if reason == "opted_out":
            run.excluded_opted_out += 1
        elif reason == "no_phone":
            run.excluded_no_phone += 1
        elif reason == "has_records_before_period":
            run.excluded_has_records_before += 1
        elif reason in ("multiple_records_in_period", "multiple_lash_records_in_period"):
            run.excluded_multiple_records += 1
            run.excluded_more_than_one_record += 1
        elif reason in (
            "no_confirmed_record_in_period",
            "no_lash_record_in_period",
            "no_confirmed_lash_record_in_period",
        ):
            run.excluded_no_confirmed_record += 1
        elif reason == "crm_history_unavailable":
            run.excluded_crm_unavailable += 1
        elif reason == "service_category_unavailable":
            run.excluded_service_category_unavailable += 1
        elif reason == "returned_after_first_visit":
            run.excluded_returned_after_visit += 1
        # "manual_removed" и прочие нестандартные причины:
        # учитываются только в total_clients_seen.


async def recompute_run_counters(run_id: int) -> None:
    """Загрузить получателей из БД и пересчитать счётчики CampaignRun.

    Используется после ручного изменения snapshot (add/remove recipient).
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise ValueError(f"CampaignRun {run_id} not found")

            stmt = select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
            recipients = list((await session.execute(stmt)).scalars().all())

            _update_run_counters_from_recipients(run, recipients)

    logger.info(
        "recompute_run_counters run_id=%d total_seen=%d",
        run_id,
        len(recipients),
    )


# ==========================================================================
# Full stats recompute (segmentation + delivery + loyalty)
# ==========================================================================

# Outbox statuses that count as provider-accepted (cumulative)
_PROVIDER_ACCEPTED = frozenset({"sent", "delivered", "read"})
# Outbox statuses that count as delivered (cumulative)
_DELIVERED = frozenset({"delivered", "read"})

# Recipient statuses that block auto-hide of the source preview run.
# These indicate in-flight processing or a problematic state that may
# still require operator attention / manual recovery.
# Checked inside recompute_campaign_run_stats AFTER
# _sync_recipient_statuses, so values reflect real outbox state.
_BLOCKING_HIDE_STATUSES: frozenset[str] = frozenset(
    {
        "candidate",  # not yet processed
        "cleanup_failed",  # pre-queue failure — recovery may be needed
        "card_issued",  # card issued but message not queued (stuck)
        "queued",  # queued but outbox has not confirmed delivery yet
    }
)


def _is_reconciled_for_hide(
    run: CampaignRun,
    recipients: list[CampaignRecipient],
) -> bool:
    """Return True when a send-real run is fully reconciled.

    Checks that:
    - run is a completed send-real with a source preview
    - no last_error or partial_failure in meta
    - no recipient is in an in-flight / problematic status

    Must be called after _sync_recipient_statuses so statuses reflect
    actual outbox delivery progress.
    """
    if run.mode != "send-real":
        return False
    if run.status != "completed":
        return False
    if run.source_preview_run_id is None:
        return False
    meta = run.meta or {}
    if meta.get("last_error"):
        return False
    if meta.get("partial_failure"):
        return False
    return not any(r.status in _BLOCKING_HIDE_STATUSES for r in recipients)


# Rank for picking the "best" outbox row when multiple exist for one job.
# Higher = more advanced delivery state.
_OUTBOX_STATUS_RANK: dict[str, int] = {
    "failed": 0,
    "queued": 1,
    "sending": 2,
    "sent": 3,
    "delivered": 4,
    "read": 5,
}

# Rank for CampaignRecipient status — used to ensure we never regress.
_RECIPIENT_STATUS_RANK: dict[str, int] = {
    "candidate": 0,
    "cleanup_ok": 1,
    "card_issued": 2,
    "queued": 3,
    "provider_accepted": 4,
    "delivered": 5,
    "read": 6,
    "replied": 7,
    "booked_after_campaign": 8,
}

# Maps OutboxMessage.status → the CampaignRecipient.status it implies.
# Only delivery-advancing statuses are mapped; 'failed'/'queued' etc. are
# intentionally omitted so they never regress a recipient.
_OUTBOX_TO_RECIPIENT_STATUS: dict[str, str] = {
    "sent": "provider_accepted",
    "delivered": "delivered",
    "read": "read",
}


async def _resolve_outbox_for_recipients(
    session: AsyncSession,
    recipients: list[CampaignRecipient],
) -> dict[int, "OutboxMessage"]:
    """Return {recipient.id -> best OutboxMessage} for all recipients.

    Two-pass lookup:
    1. Direct: recipients that already have outbox_message_id set.
    2. Fallback: recipients with outbox_message_id=None but message_job_id
       set — resolved via OutboxMessage.job_id.  The "best" row per job is
       the one with the highest _OUTBOX_STATUS_RANK (most advanced delivery
       state).  Ties are broken by highest id (most recent row).
    """
    result: dict[int, OutboxMessage] = {}

    # --- Pass 1: direct via outbox_message_id ---
    direct_ids = [r.outbox_message_id for r in recipients if r.outbox_message_id is not None]
    if direct_ids:
        stmt = select(OutboxMessage).where(OutboxMessage.id.in_(direct_ids))
        rows = (await session.execute(stmt)).scalars().all()
        outbox_by_id: dict[int, OutboxMessage] = {ob.id: ob for ob in rows}
        for r in recipients:
            if r.outbox_message_id is not None:
                ob = outbox_by_id.get(r.outbox_message_id)
                if ob is not None:
                    result[r.id] = ob

    # --- Pass 2: fallback via message_job_id ---
    needs_job_lookup = [
        r for r in recipients if r.id not in result and r.outbox_message_id is None and r.message_job_id is not None
    ]
    if needs_job_lookup:
        job_ids = list({r.message_job_id for r in needs_job_lookup})
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.job_id.in_(job_ids))
            .order_by(OutboxMessage.job_id, OutboxMessage.id.desc())
        )
        all_outboxes = (await session.execute(stmt)).scalars().all()

        # For each job_id pick the row with the highest delivery rank.
        best_by_job: dict[int, OutboxMessage] = {}
        for ob in all_outboxes:
            jid = ob.job_id
            if jid is None:
                continue
            if jid not in best_by_job:
                best_by_job[jid] = ob  # first = highest id (desc order)
            else:
                existing_rank = _OUTBOX_STATUS_RANK.get(best_by_job[jid].status, 0)
                new_rank = _OUTBOX_STATUS_RANK.get(ob.status, 0)
                if new_rank > existing_rank:
                    best_by_job[jid] = ob

        for r in needs_job_lookup:
            ob = best_by_job.get(r.message_job_id)  # type: ignore[arg-type]
            if ob is not None:
                result[r.id] = ob

    return result


def _backfill_recipient_links(
    recipients: list[CampaignRecipient],
    outbox_by_recipient: dict[int, "OutboxMessage"],
) -> None:
    """Write resolved outbox links back onto recipients (in-place, no I/O).

    Only sets fields that are currently None — never overwrites existing
    data.  Applied before computing delivery counters so subsequent stats
    use the fully-resolved state.
    """
    for r in recipients:
        ob = outbox_by_recipient.get(r.id)
        if ob is None:
            continue
        if r.outbox_message_id is None:
            r.outbox_message_id = ob.id
        if r.provider_message_id is None and ob.provider_message_id:
            r.provider_message_id = ob.provider_message_id
        if r.sent_at is None and ob.sent_at:
            r.sent_at = ob.sent_at


# Recipient statuses that imply provider-accepted / sent for booked-after attribution.
_BOOKED_AFTER_POSITIVE_STATUSES: frozenset[str] = frozenset(
    {
        "provider_accepted",
        "delivered",
        "read",
        "replied",
        "booked_after_campaign",
    }
)

# Recipient statuses that definitively block booked-after attribution.
_BOOKED_AFTER_FAILURE_STATUSES: frozenset[str] = frozenset(
    {"skipped", "cleanup_failed", "queue_failed", "card_issue_failed"}
)

# OutboxMessage statuses that confirm the provider accepted the message.
_OUTBOX_POSITIVE_FOR_ATTRIBUTION: frozenset[str] = frozenset({"sent", "delivered", "read"})


def _is_booked_after_eligible(
    r: CampaignRecipient,
    ob: "OutboxMessage | None",
) -> bool:
    """Return True when recipient has a positive send signal for booked-after attribution.

    If a linked OutboxMessage exists its status is authoritative — a failed/queued
    outbox row HARD BLOCKS attribution even if _backfill_recipient_links already
    copied sent_at/provider_message_id to the recipient.

    When no outbox row is linked, fall back to recipient-level signals (status,
    provider_message_id, sent_at).

    message_job_id alone and outbox_message_id alone are intentionally NOT
    positive signals — a job can be queued/canceled/failed without a message
    ever reaching the provider.
    """
    if r.excluded_reason is not None:
        return False
    if r.status in _BOOKED_AFTER_FAILURE_STATUSES:
        return False
    if ob is not None:
        # Outbox row is authoritative — do not fall through to copied recipient fields.
        return ob.status in _OUTBOX_POSITIVE_FOR_ATTRIBUTION
    # No linked outbox — use recipient-level signals.
    if r.status in _BOOKED_AFTER_POSITIVE_STATUSES:
        return True
    if r.provider_message_id is not None:
        return True
    if r.sent_at is not None:
        return True
    return False


def _backfill_recipient_read_at(
    recipients: list[CampaignRecipient],
    outbox_by_recipient: dict[int, OutboxMessage],
) -> None:
    """Set CampaignRecipient.read_at from linked OutboxMessage when status='read'.

    Only fills if read_at is currently None — never overwrites.
    Timestamp is taken from outbox.meta['wa_status_read']['timestamp'] (Unix epoch,
    int or numeric string).  If the timestamp is absent or unparseable, read_at is
    left as None — utcnow() is NOT used as a fallback to avoid fabricating audit
    timestamps.

    read_count is still incremented by _recompute_stats_from_db from
    outbox_status_counts, so a missing timestamp does NOT break delivery metrics.
    _sync_recipient_statuses sets recipient.status='read' so follow-up eligibility
    also remains correct without read_at.
    """
    for r in recipients:
        if r.read_at is not None:
            continue
        ob = outbox_by_recipient.get(r.id)
        if ob is None or ob.status != "read":
            continue
        wa_read = (ob.meta or {}).get("wa_status_read")
        ts_raw = wa_read.get("timestamp") if isinstance(wa_read, dict) else None
        if ts_raw is not None:
            try:
                r.read_at = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
            except (ValueError, TypeError, OSError):
                pass  # leave read_at as None — do not fabricate a timestamp


async def _sync_booked_after_from_altegio_events(
    session: AsyncSession,
    run: CampaignRun,
    recipients: list[CampaignRecipient],
    outbox_by_recipient: dict[int, "OutboxMessage"],
) -> None:
    """Backfill CampaignRecipient.booked_after_at from AltegioEvent records.

    For each eligible recipient without booked_after_at, finds the first
    'record/create' AltegioEvent within the per-recipient attribution window
    that links (via Record) to the same client.

    Eligibility (positive send signal required — see _is_booked_after_eligible).
    Attribution window per recipient:
      start = recipient.sent_at (after _backfill_recipient_links) or run.completed_at
      end   = start + run.attribution_window_days
    Matching priority: Record.client_id → Record.altegio_client_id (fallback).
    Deleted records (Record.is_deleted=True) are excluded.
    Does nothing if run.completed_at is None.
    """
    if run.completed_at is None:
        return

    window_days = timedelta(days=run.attribution_window_days)

    # Only positive-send recipients without booked_after_at
    eligible = [
        r
        for r in recipients
        if r.booked_after_at is None and _is_booked_after_eligible(r, outbox_by_recipient.get(r.id))
    ]

    if not eligible:
        return

    company_ids = list({r.company_id for r in eligible})

    def _attr_start(r: CampaignRecipient) -> datetime:
        if r.sent_at is not None:
            return max(run.completed_at, r.sent_at)  # type: ignore[arg-type]
        return run.completed_at  # type: ignore[return-value]

    # One DB query covering the union of all per-recipient windows
    overall_start = min(_attr_start(r) for r in eligible)
    overall_end = max(_attr_start(r) for r in eligible) + window_days

    # Build lookup maps: (company_id, client_id) and (company_id, altegio_client_id)
    by_client: dict[tuple[int, int], CampaignRecipient] = {}
    by_altegio_client: dict[tuple[int, int], CampaignRecipient] = {}
    for r in eligible:
        if r.client_id is not None:
            key = (r.company_id, r.client_id)
            if key not in by_client:
                by_client[key] = r
        if r.altegio_client_id is not None:
            key = (r.company_id, int(r.altegio_client_id))
            if key not in by_altegio_client:
                by_altegio_client[key] = r

    stmt = (
        select(
            AltegioEvent.received_at,
            Record.company_id,
            Record.client_id,
            Record.altegio_client_id,
        )
        .join(
            Record,
            and_(
                Record.company_id == AltegioEvent.company_id,
                Record.altegio_record_id == AltegioEvent.resource_id,
            ),
        )
        .where(AltegioEvent.company_id.in_(company_ids))
        .where(AltegioEvent.resource == "record")
        .where(AltegioEvent.event_status == "create")
        .where(AltegioEvent.received_at > overall_start)
        .where(AltegioEvent.received_at <= overall_end)
        .where(func.coalesce(Record.is_deleted, False).is_(False))
        .order_by(AltegioEvent.received_at.asc())
    )

    rows = (await session.execute(stmt)).all()

    for row in rows:
        r: CampaignRecipient | None = None
        if row.client_id is not None:
            r = by_client.get((row.company_id, row.client_id))
        if r is None and row.altegio_client_id is not None:
            r = by_altegio_client.get((row.company_id, int(row.altegio_client_id)))

        if r is not None and r.booked_after_at is None:
            # Per-recipient window check (start may differ from overall_start)
            attr_start = _attr_start(r)
            attr_end = attr_start + window_days
            if row.received_at > attr_start and row.received_at <= attr_end:
                r.booked_after_at = row.received_at


def _sync_recipient_statuses(
    recipients: list[CampaignRecipient],
    outbox_by_recipient: dict[int, "OutboxMessage"],
) -> None:
    """Advance CampaignRecipient.status based on linked OutboxMessage state.

    Variant A fix: ensures the detail-page recipients summary never shows
    'queued' for a message that was already sent/delivered/read.

    Rules:
    - Only advances status (never regresses).
    - Maps: outbox 'sent' → recipient 'provider_accepted'
             outbox 'delivered' → recipient 'delivered'
             outbox 'read'      → recipient 'read'
    - Other outbox statuses ('failed', 'queued', etc.) are not mapped and
      do not change recipient status.
    """
    for r in recipients:
        ob = outbox_by_recipient.get(r.id)
        if ob is None:
            continue
        target = _OUTBOX_TO_RECIPIENT_STATUS.get(ob.status)
        if target is None:
            continue
        current_rank = _RECIPIENT_STATUS_RANK.get(r.status, 0)
        target_rank = _RECIPIENT_STATUS_RANK.get(target, 0)
        if target_rank > current_rank:
            r.status = target


def _recompute_stats_from_db(
    run: CampaignRun,
    recipients: list[CampaignRecipient],
    outbox_status_counts: dict[str, int],
) -> None:
    """Apply full stats recompute to *run* (in-place, no DB I/O).

    Three groups of counters are refreshed:
    1. Segmentation — from CampaignRecipient.excluded_reason
       (delegates to _update_run_counters_from_recipients).
    2. Delivery funnel — from *outbox_status_counts* (pre-computed by
       caller from _resolve_outbox_for_recipients).
    3. Loyalty / attribution — from CampaignRecipient fields.

    Idempotent: all counters are zeroed before accumulation.

    Note: queued_count uses message_job_id (job was created) as source of
    truth, not outbox_message_id.  This is correct because a message is
    "queued" the moment a MessageJob is enqueued, before any OutboxMessage
    row exists.
    """
    # --- 1. Segmentation counters (delegates to existing helper) ---
    _update_run_counters_from_recipients(run, recipients)

    # --- 2. Delivery funnel ---
    # queued / sent: a MessageJob was created for this recipient
    queued = sum(1 for r in recipients if r.message_job_id is not None)
    run.queued_count = queued
    run.sent_count = queued  # redundant alias, kept in sync

    # failed: recipients who tried to queue but failed
    failed = sum(1 for r in recipients if r.excluded_reason == "queue_failed")
    run.failed_count = failed

    # provider_accepted / delivered / read from OutboxMessage statuses
    run.provider_accepted_count = sum(outbox_status_counts.get(s, 0) for s in _PROVIDER_ACCEPTED)
    run.delivered_count = sum(outbox_status_counts.get(s, 0) for s in _DELIVERED)
    run.read_count = outbox_status_counts.get("read", 0)

    # --- 3. Attribution (CampaignRecipient timestamps) ---
    run.replied_count = sum(1 for r in recipients if r.replied_at is not None)
    run.booked_after_count = sum(1 for r in recipients if r.booked_after_at is not None)
    run.opted_out_after_count = sum(1 for r in recipients if r.opted_out_after_at is not None)

    # --- 4. Loyalty counters ---
    run.cards_issued_count = sum(1 for r in recipients if r.loyalty_card_id is not None)
    run.cards_deleted_count = sum(len(r.cleanup_card_ids or []) for r in recipients)
    run.cleanup_failed_count = sum(
        1 for r in recipients if r.cleanup_failed_reason is not None or r.status == "cleanup_failed"
    )


async def recompute_campaign_run_stats(
    session: AsyncSession,
    run_id: int,
) -> dict:
    """Full idempotent recompute of all CampaignRun aggregate counters.

    Accepts an open SQLAlchemy *session* (must already be inside a
    transaction when the caller wants atomic writes).

    Steps:
    1. Load all CampaignRecipient rows for the run.
    2. Resolve the best OutboxMessage for each recipient — first via the
       direct outbox_message_id FK, then via message_job_id lookup for
       rows where the back-reference was never filled (the production
       gap this function was designed to close).
    3. Backfill missing outbox_message_id / provider_message_id / sent_at
       on recipients in memory (persisted at transaction commit).
    4. Sync CampaignRecipient.status forward from outbox state (Variant A).
    4a. Backfill CampaignRecipient.read_at from outbox meta when status='read'.
    4b. Backfill CampaignRecipient.booked_after_at from AltegioEvent records.
    5. Compute outbox_status_counts in Python from the resolved map.
    6. Recompute all CampaignRun aggregate counters.

    Safe to call multiple times — always converges to the same result.
    Supports both preview and send-real runs.
    """
    run = await session.get(CampaignRun, run_id)
    if run is None:
        raise ValueError(f"CampaignRun {run_id} not found")

    # Step 1: load recipients
    r_stmt = select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
    recipients = list((await session.execute(r_stmt)).scalars().all())

    # Step 2: resolve best outbox per recipient (direct + job_id fallback)
    outbox_by_recipient = await _resolve_outbox_for_recipients(session, recipients)

    # Step 3: backfill missing outbox links (in-memory + persisted on commit)
    _backfill_recipient_links(recipients, outbox_by_recipient)

    # Step 4: sync CampaignRecipient.status from outbox state (Variant A)
    _sync_recipient_statuses(recipients, outbox_by_recipient)

    # Step 4a: backfill read_at from outbox meta (Meta delivery timestamp)
    _backfill_recipient_read_at(recipients, outbox_by_recipient)

    # Step 4b: backfill booked_after_at from AltegioEvent record/create events
    await _sync_booked_after_from_altegio_events(session, run, recipients, outbox_by_recipient)

    # Step 5: compute outbox_status_counts from resolved map (Python, no SQL)
    outbox_status_counts: dict[str, int] = {}
    for ob in outbox_by_recipient.values():
        if ob is not None:
            s = ob.status
            outbox_status_counts[s] = outbox_status_counts.get(s, 0) + 1

    # Step 6: recompute all run counters
    _recompute_stats_from_db(run, recipients, outbox_status_counts)

    logger.info(
        "recompute_campaign_run_stats run_id=%d total_seen=%d queued=%d provider_accepted=%d delivered=%d read=%d",
        run_id,
        run.total_clients_seen,
        run.queued_count,
        run.provider_accepted_count,
        run.delivered_count,
        run.read_count,
    )

    # Step 7: auto-hide source preview if the run is fully reconciled.
    # Triggered only when all recipients have left the in-flight states
    # (candidate / cleanup_failed / card_issued / queued), meaning the
    # outbox has confirmed every message queued in this send-real.
    # Called after _sync_recipient_statuses (step 4) so statuses already
    # reflect actual outbox delivery progress.
    if _is_reconciled_for_hide(run, recipients):
        await _auto_hide_preview_run(
            session,
            run.source_preview_run_id,
            send_real_run_id=run_id,
        )

    return {
        "run_id": run_id,
        "total_clients_seen": run.total_clients_seen,
        "candidates_count": run.candidates_count,
        "queued_count": run.queued_count,
        "sent_count": run.sent_count,
        "failed_count": run.failed_count,
        "provider_accepted_count": run.provider_accepted_count,
        "delivered_count": run.delivered_count,
        "read_count": run.read_count,
        "replied_count": run.replied_count,
        "booked_after_count": run.booked_after_count,
        "opted_out_after_count": run.opted_out_after_count,
        "cards_issued_count": run.cards_issued_count,
        "cards_deleted_count": run.cards_deleted_count,
        "cleanup_failed_count": run.cleanup_failed_count,
    }


# ==========================================================================
# Delete preview run (soft-delete)
# ==========================================================================


async def delete_preview_run(run_id: int) -> None:
    """Soft-delete preview run: status='deleted', recipients сохраняются.

    Правила:
    - Только для mode='preview'.
    - Запрещено, если preview используется как source_preview_run_id в
      каком-либо send-real.
    - Запрещено, если run уже 'deleted'.
    - CampaignRecipient физически НЕ удаляются — аудит сохраняется.

    Raises:
        ValueError: при нарушении правил.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise ValueError(f"CampaignRun {run_id} not found")

            if run.mode != "preview":
                raise ValueError(f"Delete доступен только для preview run. mode={run.mode!r}")

            if run.status == "deleted":
                raise ValueError(f"CampaignRun {run_id} уже удалён")

            if run.status not in ("completed", "discarded"):
                raise ValueError(f"Delete доступен только для completed или discarded preview. status={run.status!r}")

            used_as_source = await session.scalar(
                select(func.count())
                .select_from(CampaignRun)
                .where(CampaignRun.source_preview_run_id == run_id)
                .where(CampaignRun.mode == "send-real")
            )
            if int(used_as_source or 0) > 0:
                raise ValueError(f"Preview run {run_id} используется как источник для send-real — удаление запрещено.")

            run.status = "deleted"
            run.completed_at = utcnow()
            meta = dict(run.meta or {})
            meta["deleted_at"] = utcnow().isoformat()
            run.meta = meta

    logger.info("preview run_id=%d soft-deleted", run_id)


# ==========================================================================
# Remove recipient from preview (soft-exclude)
# ==========================================================================


async def remove_recipient_from_preview(
    run_id: int,
    recipient_id: int,
) -> CampaignRecipient:
    """Мягко исключить получателя из preview snapshot.

    Устанавливает status='skipped', excluded_reason='manual_removed'.
    Пересчитывает счётчики run через recompute_run_counters.

    Правила:
    - run должен быть mode='preview' и status='completed'.
    - run не должен использоваться как source_preview_run_id в send-real.
    - recipient должен принадлежать указанному run.
    - Повторный вызов на уже excluded получателе — idempotent.

    Raises:
        ValueError: при нарушении правил.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise ValueError(f"CampaignRun {run_id} not found")

            if run.mode != "preview":
                raise ValueError(f"Редактирование snapshot доступно только для preview run. mode={run.mode!r}")

            if run.status != "completed":
                raise ValueError(
                    f"Редактирование snapshot доступно только для completed preview. status={run.status!r}"
                )

            used_as_source = await session.scalar(
                select(func.count())
                .select_from(CampaignRun)
                .where(CampaignRun.source_preview_run_id == run_id)
                .where(CampaignRun.mode == "send-real")
            )
            if int(used_as_source or 0) > 0:
                raise ValueError(
                    f"Preview run {run_id} уже использован как источник для send-real — редактирование запрещено."
                )

            recipient = await session.get(CampaignRecipient, recipient_id)
            if recipient is None:
                raise ValueError(f"CampaignRecipient {recipient_id} not found")

            if recipient.campaign_run_id != run_id:
                raise ValueError(f"CampaignRecipient {recipient_id} не принадлежит run {run_id}")

            if recipient.excluded_reason != "manual_removed":
                meta = dict(recipient.meta or {})
                meta["manually_removed_at"] = utcnow().isoformat()
                recipient.meta = meta
                recipient.status = "skipped"
                recipient.excluded_reason = "manual_removed"

    await recompute_run_counters(run_id)

    logger.info(
        "manual_removed recipient_id=%d run_id=%d",
        recipient_id,
        run_id,
    )

    async with SessionLocal() as session:
        r = await session.get(CampaignRecipient, recipient_id)
        if r is None:
            raise RuntimeError(f"CampaignRecipient {recipient_id} disappeared after removal")
        return r


# ==========================================================================
# Retry: сброс упавшего/застрявшего message_job для одного получателя
# ==========================================================================

# Статусы OutboxMessage, означающие что сообщение уже доставлено.
# Дублирует SUCCESS_OUTBOX_STATUSES из outbox_worker, чтобы не создавать
# циклического импорта.
_SENT_OUTBOX_STATUSES = frozenset({"sent", "delivered", "read"})

# Статусы MessageJob, которые разрешают принудительный retry.
#
# 'failed'  — job исчерпала max_attempts, нужна повторная попытка.
# 'queued'  — job застряла (locked_at не сброшен, attempts > 0).
#
# 'canceled' намеренно исключён: cancel означает policy-решение
# (unsubscribe / opt-out / запись в прошлом и т.д.).
# Разрешать retry для canceled = обходить compliance guardrails.
# Если оператор хочет форсировать send после opt-out, он должен
# сначала восстановить согласие клиента, а не делать raw retry.
#
# 'done' / 'running' — финальный или активный статус, retry запрещён.
_JOB_RETRYABLE_STATUSES = frozenset({"failed", "queued"})


async def retry_recipient_job(recipient_id: int) -> dict:
    """Безопасный сброс message_job для повторной доставки сообщения.

    Проверяет все guardrails перед изменением состояния:
      - у получателя есть message_job_id
      - job существует и имеет тип NEWSLETTER_JOB_TYPE
      - нет успешного OutboxMessage (дубликат запрещён)
      - job в допустимом для retry статусе: failed или queued

    'canceled' специально исключён из допустимых статусов — cancel
    означает policy-решение (unsubscribe, opt-out, запись в прошлом).
    Retry для canceled = обход compliance guardrails.

    Сброс: status='queued', locked_at=None, last_error=None,
           run_at=now(), attempts=0.

    После retry outbox_worker подберёт job как обычную очередную задачу.
    После успешной отправки auto-sync (recompute_campaign_run_stats)
    обновит счётчики run автоматически.

    Возвращает dict с полем ``outcome``:
      retried              — job сброшен, ждёт обработки
      already_sent         — есть успешный OutboxMessage, дубликат запрещён
      no_message_job       — у получателя нет message_job_id или job не найден
      wrong_job_type       — job не является NEWSLETTER_JOB_TYPE
      not_retryable        — job в финальном/активном/policy статусе
      recipient_not_found  — получатель не найден
    """
    async with SessionLocal() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_id)
            if recipient is None:
                return {"outcome": "recipient_not_found"}

            if not recipient.message_job_id:
                return {"outcome": "no_message_job"}

            job = await session.get(MessageJob, recipient.message_job_id)
            if job is None:
                return {"outcome": "no_message_job"}

            if job.job_type != NEWSLETTER_JOB_TYPE:
                return {
                    "outcome": "wrong_job_type",
                    "job_type": job.job_type,
                }

            # Duplicate guard: проверить наличие успешного OutboxMessage
            success_stmt = (
                select(OutboxMessage)
                .where(OutboxMessage.job_id == job.id)
                .where(OutboxMessage.status.in_(_SENT_OUTBOX_STATUSES))
                .order_by(OutboxMessage.id.desc())
                .limit(1)
            )
            success = (await session.execute(success_stmt)).scalar_one_or_none()
            if success is not None:
                return {
                    "outcome": "already_sent",
                    "outbox_id": success.id,
                }

            if job.status not in _JOB_RETRYABLE_STATUSES:
                return {
                    "outcome": "not_retryable",
                    "job_status": job.status,
                }

            prev_status = job.status
            prev_attempts = job.attempts

            job.status = "queued"
            job.locked_at = None
            job.last_error = None
            job.run_at = utcnow()
            job.attempts = 0

            meta = dict(recipient.meta or {})
            meta["last_manual_retry_at"] = utcnow().isoformat()
            recipient.meta = meta

    logger.info(
        "manual_retry recipient_id=%d job_id=%d prev_status=%s prev_attempts=%d",
        recipient_id,
        job.id,
        prev_status,
        prev_attempts,
    )

    return {
        "outcome": "retried",
        "job_id": job.id,
        "prev_status": prev_status,
        "prev_attempts": prev_attempts,
    }
