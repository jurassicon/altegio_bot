"""Follow-up рассылка: дорассылка по клиентам основной кампании.

Логика:
  1. В момент создания run сохраняется follow-up policy.
  2. После истечения followup_delay_days проверяем каждого получателя.
  3. По политике unread_only берём только тех, у кого нет read_at.
  4. По политике unread_or_not_booked берём тех, у кого нет read_at
     или нет booked_after_at.
  5. Исключения: клиенты с hard failure, cleanup_failed, no_phone,
     invalid_phone, no_whatsapp, provider_error, delivery_failed.

Follow-up использует отдельный WhatsApp template (followup_template_name)
т.к. отправка происходит за пределами 24-часового окна.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job, make_dedupe_key
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    MessageJob,
)
from altegio_bot.utils import utcnow

logger = logging.getLogger(__name__)

# Статусы, при которых follow-up не отправляется
_HARD_FAILURE_STATUSES = {
    'cleanup_failed',
}

# Причины исключения, при которых follow-up не отправляется
_HARD_FAILURE_REASONS = {
    'no_phone',
    'invalid_phone',
    'no_whatsapp',
    'cleanup_failed',
    'provider_error',
    'delivery_failed',
}


def _is_eligible_for_followup(
    recipient: CampaignRecipient,
    policy: str,
) -> bool:
    """Проверить, нужно ли отправить follow-up этому получателю."""
    # Hard failure — исключаем
    if recipient.status in _HARD_FAILURE_STATUSES:
        return False
    if recipient.excluded_reason in _HARD_FAILURE_REASONS:
        return False

    # Клиент не прошёл сегментацию — исключаем
    if recipient.status == 'skipped':
        return False

    # Не было отправки — нечего follow-up'ить
    if recipient.status not in (
        'queued',
        'provider_accepted',
        'delivered',
        'read',
        'replied',
        'booked_after_campaign',
    ):
        return False

    if policy == 'unread_only':
        return recipient.read_at is None

    if policy == 'unread_or_not_booked':
        return (
            recipient.read_at is None
            or recipient.booked_after_at is None
        )

    logger.warning('Unknown followup_policy=%s', policy)
    return False


async def plan_followup(session: AsyncSession, run_id: int) -> int:
    """Пометить получателей follow-up статусом 'followup_planned'.

    Возвращает количество клиентов, запланированных для follow-up.
    """
    run = await session.get(CampaignRun, run_id)
    if run is None:
        raise ValueError(f'CampaignRun {run_id} not found')

    if not run.followup_enabled:
        raise ValueError(
            f'Follow-up не включён для run_id={run_id}'
        )

    if not run.followup_policy:
        raise ValueError(
            f'followup_policy не задана для run_id={run_id}'
        )

    stmt = select(CampaignRecipient).where(
        CampaignRecipient.campaign_run_id == run_id
    )
    recipients = (await session.execute(stmt)).scalars().all()

    count = 0
    for r in recipients:
        if _is_eligible_for_followup(r, run.followup_policy):
            r.followup_status = 'followup_planned'
            count += 1
        else:
            if r.followup_status is None and r.is_eligible:
                r.followup_status = 'followup_skipped'

    await session.flush()
    logger.info(
        'plan_followup run_id=%d planned=%d', run_id, count
    )
    return count


async def execute_followup(run_id: int) -> dict:
    """Запустить follow-up: поставить MessageJob для planned клиентов.

    Возвращает dict со статистикой: queued, skipped, failed.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise ValueError(f'CampaignRun {run_id} not found')

        if not run.followup_enabled:
            raise ValueError(
                f'Follow-up не включён для run_id={run_id}'
            )

        template_name = run.followup_template_name
        if not template_name:
            raise ValueError(
                f'followup_template_name не задан для run_id={run_id}'
            )

        policy = run.followup_policy or ''
        company_id_list = run.company_ids or []

    stats = {'queued': 0, 'skipped': 0, 'failed': 0}

    async with SessionLocal() as session:
        stmt = (
            select(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(
                CampaignRecipient.followup_status == 'followup_planned'
            )
        )
        recipients = (await session.execute(stmt)).scalars().all()

    for recipient in recipients:
        company_id = recipient.company_id
        client_id = recipient.client_id

        if not client_id:
            stats['skipped'] += 1
            continue

        # Re-evaluate по актуальным данным
        async with SessionLocal() as session:
            fresh = await session.get(CampaignRecipient, recipient.id)
            if fresh is None:
                stats['skipped'] += 1
                continue
            if not _is_eligible_for_followup(fresh, policy):
                fresh.followup_status = 'followup_skipped'
                await session.commit()
                stats['skipped'] += 1
                continue

        # Создать follow-up job
        run_at = utcnow()
        try:
            async with SessionLocal() as session:
                async with session.begin():
                    await add_job(
                        session,
                        company_id=company_id,
                        record_id=None,
                        client_id=client_id,
                        job_type=FOLLOWUP_JOB_TYPE,
                        run_at=run_at,
                        payload={
                            'kind': FOLLOWUP_JOB_TYPE,
                            'template_name': template_name,
                            'campaign_run_id': run_id,
                            'campaign_recipient_id': recipient.id,
                        },
                    )

                    dedupe_key = make_dedupe_key(
                        job_type=FOLLOWUP_JOB_TYPE,
                        company_id=company_id,
                        record_id=None,
                        run_at=run_at,
                    )
                    job = await session.scalar(
                        select(MessageJob).where(
                            MessageJob.dedupe_key == dedupe_key
                        )
                    )

                    r = await session.get(
                        CampaignRecipient, recipient.id
                    )
                    if r:
                        r.followup_status = 'followup_queued'
                        if job:
                            r.followup_message_job_id = job.id

            stats['queued'] += 1
            logger.info(
                'followup queued client_id=%d run_id=%d',
                client_id,
                run_id,
            )
        except Exception as exc:
            stats['failed'] += 1
            logger.error(
                'followup failed client_id=%d run_id=%d: %s',
                client_id,
                run_id,
                exc,
            )

    logger.info(
        'execute_followup run_id=%d stats=%s', run_id, stats
    )
    return stats


def followup_run_at(run: CampaignRun) -> str | None:
    """Вернуть дату запуска follow-up в ISO-формате или None."""
    if not run.followup_enabled or not run.followup_delay_days:
        return None
    if run.completed_at is None:
        return None
    run_at = run.completed_at + timedelta(days=run.followup_delay_days)
    return run_at.isoformat()
