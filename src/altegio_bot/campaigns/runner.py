"""Оркестратор кампании рассылки по новым клиентам.

Поддерживает два режима:
  * preview   — только сегментация и сохранение снимка в БД,
                без отправки сообщений и выпуска карт.
  * send-real — сегментация, cleanup старых карт, выпуск новой
                карты, постановка в очередь MessageJob.

Каждый preview и каждый send-real сохраняется как отдельный
исторический CampaignRun. send-real может ссылаться на preview
через source_preview_run_id.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from sqlalchemy import select
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

NEWSLETTER_JOB_TYPE = 'newsletter_new_clients_monthly'
FOLLOWUP_JOB_TYPE = 'newsletter_new_clients_followup'
CAMPAIGN_CODE = 'new_clients_monthly'


@dataclass
class RunParams:
    """Параметры запуска кампании."""

    company_id: int
    location_id: int
    period_start: datetime
    period_end: datetime
    mode: Literal['preview', 'send-real']
    campaign_code: str = CAMPAIGN_CODE
    card_type_id: str | None = None
    source_preview_run_id: int | None = None
    attribution_window_days: int = 30
    followup_enabled: bool = False
    followup_delay_days: int | None = None
    # 'unread_only' | 'unread_or_not_booked'
    followup_policy: str | None = None
    followup_template_name: str | None = None


async def _create_run(
    session: AsyncSession,
    params: RunParams,
) -> CampaignRun:
    """Создать и сохранить CampaignRun со статусом 'running'."""
    run = CampaignRun(
        campaign_code=params.campaign_code,
        mode=params.mode,
        company_ids=[params.company_id],
        source_preview_run_id=params.source_preview_run_id,
        location_id=params.location_id,
        card_type_id=params.card_type_id,
        period_start=params.period_start,
        period_end=params.period_end,
        status='running',
        attribution_window_days=params.attribution_window_days,
        followup_enabled=params.followup_enabled,
        followup_delay_days=params.followup_delay_days,
        followup_policy=params.followup_policy,
        followup_template_name=params.followup_template_name,
    )
    session.add(run)
    await session.flush()
    return run


def _build_recipient(
    run_id: int,
    candidate: ClientCandidate,
) -> CampaignRecipient:
    """Создать CampaignRecipient из кандидата."""
    client = candidate.client
    r = CampaignRecipient(
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
        status='skipped' if candidate.excluded_reason else 'candidate',
        excluded_reason=candidate.excluded_reason,
    )
    return r


def _update_run_exclusion_counters(
    run: CampaignRun,
    candidates: list[ClientCandidate],
) -> None:
    """Обновить счётчики исключений в run по результатам сегментации."""
    run.total_clients_seen = len(candidates)
    for c in candidates:
        if not c.excluded_reason:
            run.candidates_count += 1
            continue
        reason = c.excluded_reason
        if reason == 'opted_out':
            run.excluded_opted_out += 1
        elif reason == 'no_phone':
            run.excluded_no_phone += 1
            run.excluded_multiple_records += 0  # для clarity
        elif reason == 'has_records_before_period':
            run.excluded_has_records_before += 1
        elif reason == 'multiple_records_in_period':
            run.excluded_multiple_records += 1
            run.excluded_more_than_one_record += 1  # legacy
        elif reason == 'no_confirmed_record_in_period':
            run.excluded_no_confirmed_record += 1


async def run_preview(params: RunParams) -> CampaignRun:
    """Запустить preview: сегментация без отправки.

    Сохраняет CampaignRun(mode='preview') и CampaignRecipient
    для каждого клиента. Карты не выпускаются, сообщения не
    отправляются.
    """
    async with SessionLocal() as session:
        async with session.begin():
            run = await _create_run(session, params)

            candidates = await find_candidates(
                session,
                company_id=params.company_id,
                period_start=params.period_start,
                period_end=params.period_end,
            )

            _update_run_exclusion_counters(run, candidates)

            for c in candidates:
                recipient = _build_recipient(run.id, c)
                # В preview нет loyalty-действий — status остаётся
                # 'candidate' (eligible) или 'skipped' (excluded)
                session.add(recipient)

            run.status = 'completed'
            run.completed_at = utcnow()

    logger.info(
        'preview run_id=%d company=%d period=[%s, %s) '
        'total=%d eligible=%d',
        run.id,
        params.company_id,
        params.period_start.date(),
        params.period_end.date(),
        run.total_clients_seen,
        run.candidates_count,
    )
    return run


async def run_send_real(params: RunParams) -> CampaignRun:
    """Запустить send-real: сегментация + выпуск карт + очередь.

    Для каждого eligible клиента:
      1. Cleanup старых campaign-карт.
         Если cleanup failed — пометить, пропустить клиента.
      2. Выпустить новую loyalty-карту.
         Если выпуск не удался — пометить, пропустить.
      3. Создать MessageJob.
      4. Обновить CampaignRecipient.status = 'queued'.

    Каждый клиент обрабатывается в отдельной транзакции,
    чтобы сбой одного не влиял на остальных.
    """
    # ------------------------------------------------------------------
    # Шаг 1: создать run и найти кандидатов
    # ------------------------------------------------------------------
    async with SessionLocal() as session:
        async with session.begin():
            run = await _create_run(session, params)
            run_id = run.id

            candidates = await find_candidates(
                session,
                company_id=params.company_id,
                period_start=params.period_start,
                period_end=params.period_end,
            )
            _update_run_exclusion_counters(run, candidates)

    # ------------------------------------------------------------------
    # Шаг 2: resolve card_type_id, если не задан
    # ------------------------------------------------------------------
    loyalty = AltegioLoyaltyClient()
    try:
        card_type_id = await _resolve_card_type(
            loyalty, params.location_id, params.card_type_id
        )

        # ------------------------------------------------------------------
        # Шаг 3: обработать каждого кандидата
        # ------------------------------------------------------------------
        stats = {
            'cleanup_failed': 0,
            'cards_deleted': 0,
            'cards_issued': 0,
            'queued': 0,
            'failed': 0,
        }

        for candidate in candidates:
            if candidate.excluded_reason:
                # Сохранить excluded получателя
                async with SessionLocal() as session:
                    async with session.begin():
                        r = _build_recipient(run_id, candidate)
                        session.add(r)
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

    finally:
        await loyalty.aclose()

    # ------------------------------------------------------------------
    # Шаг 4: завершить run, обновить счётчики
    # ------------------------------------------------------------------
    async with SessionLocal() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise RuntimeError(f'CampaignRun {run_id} not found')
            run.cleanup_failed_count = stats['cleanup_failed']
            run.cards_deleted_count = stats['cards_deleted']
            run.cards_issued_count = stats['cards_issued']
            run.queued_count = stats['queued']
            run.failed_count = stats['failed']
            run.sent_count = stats['queued']  # legacy alias
            run.status = 'completed'
            run.completed_at = utcnow()

    logger.info(
        'send-real run_id=%d company=%d stats=%s',
        run_id,
        params.company_id,
        stats,
    )
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
    """Обработать одного eligible клиента: cleanup → card → job."""
    client = candidate.client
    client_id = client.id

    # Создать получателя со статусом 'candidate'
    async with SessionLocal() as session:
        async with session.begin():
            recipient = _build_recipient(run_id, candidate)
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    # ------------------------------------------------------------------
    # Cleanup старых campaign-карт (HTTP-вызов вне транзакции)
    # ------------------------------------------------------------------
    async with SessionLocal() as session:
        cleanup = await cleanup_campaign_cards(
            session,
            loyalty,
            location_id=location_id,
            client_id=client_id,
            campaign_code=campaign_code,
        )

    if not cleanup.ok:
        stats['cleanup_failed'] += 1
        stats['cards_deleted'] += len(cleanup.deleted_ids)
        async with SessionLocal() as session:
            async with session.begin():
                r = await session.get(CampaignRecipient, recipient_id)
                if r:
                    r.status = 'cleanup_failed'
                    r.excluded_reason = 'cleanup_failed'
                    r.cleanup_card_ids = cleanup.deleted_ids
                    r.cleanup_failed_reason = cleanup.reason
        logger.warning(
            'cleanup_failed client_id=%d reason=%s',
            client_id,
            cleanup.reason,
        )
        return

    stats['cards_deleted'] += len(cleanup.deleted_ids)

    # ------------------------------------------------------------------
    # Выпуск новой loyalty-карты (HTTP-вызов вне транзакции)
    # ------------------------------------------------------------------
    phone_e164 = client.phone_e164
    card_number = make_card_number(phone_e164)
    phone_num = int(phone_e164.lstrip('+'))

    try:
        card = await loyalty.issue_card(
            location_id,
            loyalty_card_number=card_number,
            loyalty_card_type_id=card_type_id,
            phone=phone_num,
        )
        issued_number = str(
            card.get('loyalty_card_number') or card_number
        )
        issued_id = str(card.get('id') or card.get('loyalty_card_id') or '')
    except Exception as exc:
        stats['failed'] += 1
        async with SessionLocal() as session:
            async with session.begin():
                r = await session.get(CampaignRecipient, recipient_id)
                if r:
                    r.status = 'skipped'
                    r.excluded_reason = 'card_issue_failed'
                    r.cleanup_card_ids = cleanup.deleted_ids
        logger.error(
            'card_issue_failed client_id=%d: %s', client_id, exc
        )
        return

    stats['cards_issued'] += 1

    # ------------------------------------------------------------------
    # Создать MessageJob и обновить получателя (одна транзакция)
    # ------------------------------------------------------------------
    loyalty_card_text = make_card_text(issued_number)
    run_at = utcnow()

    async with SessionLocal() as session:
        async with session.begin():
            r = await session.get(CampaignRecipient, recipient_id)
            if r is None:
                logger.error(
                    'recipient_id=%d not found', recipient_id
                )
                return

            r.loyalty_card_id = issued_id
            r.loyalty_card_number = issued_number
            r.loyalty_card_type_id = card_type_id
            r.cleanup_card_ids = cleanup.deleted_ids
            r.status = 'card_issued'

            await add_job(
                session,
                company_id=company_id,
                record_id=None,
                client_id=client_id,
                job_type=NEWSLETTER_JOB_TYPE,
                run_at=run_at,
                payload={
                    'kind': NEWSLETTER_JOB_TYPE,
                    'loyalty_card_text': loyalty_card_text,
                    'campaign_run_id': run_id,
                    'campaign_recipient_id': recipient_id,
                },
            )

            # Получить ID созданного job
            dedupe_key = make_dedupe_key(
                job_type=NEWSLETTER_JOB_TYPE,
                company_id=company_id,
                record_id=None,
                run_at=run_at,
            )
            job = await session.scalar(
                select(MessageJob).where(
                    MessageJob.dedupe_key == dedupe_key
                )
            )
            if job:
                r.message_job_id = job.id

            r.status = 'queued'

    stats['queued'] += 1
    logger.info(
        'queued client_id=%d card=%s', client_id, issued_number
    )


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
        raise RuntimeError(
            f'No loyalty card types for location_id={location_id}'
        )
    tid = types[0].get('id') or types[0].get('loyalty_card_type_id')
    return str(tid)
