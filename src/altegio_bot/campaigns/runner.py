from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.campaigns.loyalty_cleanup import (
    cleanup_campaign_cards,
    make_card_number,
    make_card_text,
)
from altegio_bot.campaigns.segment import ClientCandidate, find_candidates
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job, make_dedupe_key
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    MessageJob,
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
        is_opted_out=client.wa_opted_out,
        status="skipped" if candidate.excluded_reason else "candidate",
        excluded_reason=candidate.excluded_reason,
    )
    return recipient


def _update_run_exclusion_counters(
    run: CampaignRun,
    candidates: list[ClientCandidate],
) -> None:
    """Обновить счётчики исключений в run по результатам сегментации."""
    run.total_clients_seen = len(candidates)
    for candidate in candidates:
        if not candidate.excluded_reason:
            run.candidates_count += 1
            continue

        reason = candidate.excluded_reason
        if reason == "opted_out":
            run.excluded_opted_out += 1
        elif reason == "no_phone":
            run.excluded_no_phone += 1
        elif reason == "has_records_before_period":
            run.excluded_has_records_before += 1
        elif reason == "multiple_records_in_period":
            run.excluded_multiple_records += 1
            run.excluded_more_than_one_record += 1
        elif reason == "no_confirmed_record_in_period":
            run.excluded_no_confirmed_record += 1


async def run_preview(params: RunParams) -> CampaignRun:
    """Запустить preview: сегментация без отправки."""
    async with SessionLocal() as session:
        async with session.begin():
            run = await _create_run(session, params, status="running")

            candidates = await find_candidates(
                session,
                company_id=params.company_id,
                period_start=params.period_start,
                period_end=params.period_end,
            )

            _update_run_exclusion_counters(run, candidates)

            for candidate in candidates:
                recipient = _build_recipient(run.id, candidate)
                session.add(recipient)

            run.status = "completed"
            run.completed_at = utcnow()

    logger.info(
        "preview run_id=%d company=%d period=[%s, %s) total=%d eligible=%d",
        run.id,
        params.company_id,
        params.period_start.date(),
        params.period_end.date(),
        run.total_clients_seen,
        run.candidates_count,
    )
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


async def _execute_send_real_for_existing_run(
    run_id: int,
    params: RunParams,
) -> None:
    """Выполнить send-real для уже созданного CampaignRun."""
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
            run.meta = meta

            candidates = await find_candidates(
                session,
                company_id=params.company_id,
                period_start=params.period_start,
                period_end=params.period_end,
            )
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

                run.cleanup_failed_count = stats["cleanup_failed"]
                run.cards_deleted_count = stats["cards_deleted"]
                run.cards_issued_count = stats["cards_issued"]
                run.queued_count = stats["queued"]
                run.failed_count = stats["failed"]
                run.sent_count = stats["queued"]
                run.status = "completed"
                run.completed_at = utcnow()

                meta = dict(run.meta or {})
                meta.pop("last_error", None)
                run.meta = meta

        logger.info(
            "send-real completed run_id=%d company=%d stats=%s",
            run_id,
            params.company_id,
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
    client_id = client.id

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
        logger.error(
            "card_issue_failed client_id=%d: %s",
            client_id,
            exc,
        )
        return

    stats["cards_issued"] += 1
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

                await add_job(
                    session,
                    company_id=company_id,
                    record_id=None,
                    client_id=client_id,
                    job_type=NEWSLETTER_JOB_TYPE,
                    run_at=run_at,
                    payload={
                        "kind": NEWSLETTER_JOB_TYPE,
                        "loyalty_card_text": loyalty_card_text,
                        "campaign_run_id": run_id,
                        "campaign_recipient_id": recipient_id,
                    },
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
            "queued client_id=%d card=%s",
            client_id,
            issued_number,
        )

    except Exception as exc:
        stats["failed"] += 1
        logger.error(
            "queue_failed client_id=%d: %s",
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
    client_id: int,
    run_id: int,
    loyalty_card_number: str,
    loyalty_card_id: str,
    loyalty_card_type_id: str,
) -> None:
    """Создать MessageJob рассылки и перевести recipient → queued.

    Не выполняет cleanup и не выпускает карту. Используется при resume
    для получателей, у которых карта уже выпущена (card_issued, queue_failed).
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

            await add_job(
                session,
                company_id=company_id,
                record_id=None,
                client_id=client_id,
                job_type=NEWSLETTER_JOB_TYPE,
                run_at=run_at,
                payload={
                    "kind": NEWSLETTER_JOB_TYPE,
                    "loyalty_card_text": loyalty_card_text,
                    "campaign_run_id": run_id,
                    "campaign_recipient_id": recipient_id,
                },
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
    CampaignRecipient. Безопасен: не трогает cleanup_failed и card_issue_failed.

    Разрешённые для resume статусы получателей:
      - candidate: полный flow (cleanup → выпуск карты → очередь)
      - card_issued: только допоставить MessageJob (карта уже выпущена)
      - skipped + queue_failed с картой: только допоставить MessageJob

    Не трогаются (manual action required):
      - cleanup_failed
      - skipped + card_issue_failed

    Логика финального статуса run:
      - все pending обработаны, ничего manual → completed
      - все pending обработаны, но есть manual → failed + "partially completed"
      - остались pending-получатели → failed + "still pending" (можно повторить)

    Возвращает:
      resumed_count — успешно поставлено в очередь за этот запуск
      skipped_count — не тронуто (уже queued или исключено по другой причине)
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
                # Уже в очереди — пропускаем
                stats["skipped"] += 1
                continue

            if st == "cleanup_failed":
                # Требует ручных действий — не трогаем
                continue

            if st == "skipped" and reason == "card_issue_failed":
                # Выпуск карты провалился — требует ручных действий
                continue

            if st == "skipped" and reason != "queue_failed":
                # Исключён по другой причине (opted_out, no_phone и т.д.)
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

                elif st == "card_issued":
                    # Карта уже выпущена — только допоставить job
                    await _enqueue_newsletter_job_for_recipient(
                        recipient_id=recipient.id,
                        company_id=params.company_id,
                        client_id=recipient.client_id,
                        run_id=run_id,
                        loyalty_card_number=recipient.loyalty_card_number or "",
                        loyalty_card_id=recipient.loyalty_card_id or "",
                        loyalty_card_type_id=recipient.loyalty_card_type_id or card_type_id,
                    )
                    stats["resumed"] += 1

                else:
                    # skipped + queue_failed: карта выпущена, очередь не прошла
                    await _enqueue_newsletter_job_for_recipient(
                        recipient_id=recipient.id,
                        company_id=params.company_id,
                        client_id=recipient.client_id,
                        run_id=run_id,
                        loyalty_card_number=recipient.loyalty_card_number or "",
                        loyalty_card_id=recipient.loyalty_card_id or "",
                        loyalty_card_type_id=recipient.loyalty_card_type_id or card_type_id,
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

    # Подсчитать финальное состояние получателей после resume
    async with SessionLocal() as session:
        cleanup_failed_count: int = (
            await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.status == "cleanup_failed")
            )
            or 0
        )
        card_issue_failed_count: int = (
            await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.status == "skipped")
                .where(CampaignRecipient.excluded_reason == "card_issue_failed")
            )
            or 0
        )
        remaining_manual_count = cleanup_failed_count + card_issue_failed_count

        # Получатели, которые всё ещё можно возобновить (не обработались в этом запуске)
        still_candidate_count: int = (
            await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.status.in_(["candidate", "card_issued"]))
            )
            or 0
        )
        still_queue_failed_count: int = (
            await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.status == "skipped")
                .where(CampaignRecipient.excluded_reason == "queue_failed")
            )
            or 0
        )
        remaining_pending_count = still_candidate_count + still_queue_failed_count

    # Обновить статус run на основе результатов
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f"CampaignRun {run_id} not found after resume")

            meta = dict(run.meta or {})

            if remaining_pending_count == 0 and remaining_manual_count == 0:
                # Все получатели обработаны успешно
                run.status = "completed"
                meta.pop("last_error", None)
                run.completed_at = utcnow()
            elif remaining_pending_count == 0:
                # Все resumable обработаны, но часть требует ручных действий
                run.status = "failed"
                meta["last_error"] = "Resume completed partially: some recipients still require manual action"
                run.completed_at = utcnow()
            else:
                # Часть получателей не удалось обработать — можно повторить resume
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
