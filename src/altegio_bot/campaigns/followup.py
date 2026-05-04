from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job, make_dedupe_key
from altegio_bot.models.models import CampaignRecipient, CampaignRun, MessageJob
from altegio_bot.utils import utcnow

logger = logging.getLogger(__name__)

# Статусы CampaignRecipient, при которых follow-up не отправляется
_HARD_FAILURE_STATUSES = {
    "cleanup_failed",
}

# Причины excluded_reason, при которых follow-up не отправляется
_HARD_FAILURE_REASONS = {
    "no_phone",
    "invalid_phone",
    "no_whatsapp",
    "cleanup_failed",
    "provider_error",
    "delivery_failed",
}

# Статусы, при которых получатель «участвовал» в send-real (не исключён)
_SENT_PIPELINE_STATUSES = {
    "queued",
    "provider_accepted",
    "delivered",
    "read",
    "replied",
    "booked_after_campaign",
}

# Временный служебный статус, чтобы два параллельных follow-up запуска
# не забрали одного и того же получателя одновременно.
_FOLLOWUP_PROCESSING = "followup_processing"


def _is_eligible_for_followup(
    recipient: CampaignRecipient,
    policy: str,
) -> bool:
    """Проверить, нужно ли отправить follow-up этому получателю."""
    if recipient.status in _HARD_FAILURE_STATUSES:
        return False
    if recipient.excluded_reason in _HARD_FAILURE_REASONS:
        return False

    if recipient.status == "skipped":
        return False

    if recipient.status not in _SENT_PIPELINE_STATUSES:
        return False

    if policy == "unread_only":
        is_read = recipient.read_at is not None or recipient.status == "read"
        return not is_read

    if policy == "unread_or_not_booked":
        is_read = recipient.read_at is not None or recipient.status == "read"
        return not is_read or recipient.booked_after_at is None

    logger.warning("Unknown followup_policy=%s", policy)
    return False


async def plan_followup(session: AsyncSession, run_id: int) -> int:
    """Пометить получателей follow-up статусом 'followup_planned'.

    Возвращает количество клиентов, запланированных для follow-up.

    Recovery от застрявших followup_processing:
    В начале функции выполняется UPDATE, который сбрасывает всех получателей
    из followup_processing обратно в followup_planned. Это покрывает случай,
    когда execute_followup упал (SIGKILL, OOM) после атомарного claim, но до
    создания MessageJob, и получатель навсегда завис. plan_followup
    вызывается либо оператором явно, либо при stale-recovery worker'а — в
    обоих случаях предыдущая сессия execute_followup мертва, сброс безопасен.
    Защита от дублей сохраняется: execute_followup по-прежнему клеймит
    каждого получателя атомарно перед отправкой.

    Конкурентная безопасность:
    Запрос к получателям выполняется с FOR UPDATE. Второй параллельный вызов
    plan_followup будет ждать завершения первого, а затем увидит уже
    выставленные статусы и вернёт 0 (идемпотентность сохраняется).
    UPDATE-операции execute_followup по отдельным строкам также сериализуются
    относительно этого запроса.
    """
    run = await session.get(CampaignRun, run_id)
    if run is None:
        raise ValueError(f"CampaignRun {run_id} not found")

    if not run.followup_enabled:
        raise ValueError(f"Follow-up не включён для run_id={run_id}")

    if not run.followup_policy:
        raise ValueError(f"followup_policy не задана для run_id={run_id}")

    # Recovery: сбросить застрявших получателей followup_processing → followup_planned.
    # Основной цикл ниже пропустит их (followup_status is not None), но
    # execute_followup заберёт их при следующем вызове.
    await session.execute(
        update(CampaignRecipient)
        .where(CampaignRecipient.campaign_run_id == run_id)
        .where(CampaignRecipient.followup_status == _FOLLOWUP_PROCESSING)
        .values(followup_status="followup_planned")
    )
    await session.flush()

    stmt = select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id).with_for_update()
    recipients = (await session.execute(stmt)).scalars().all()

    count = 0
    for recipient in recipients:
        # Идемпотентность: если статус уже выставлен — не трогаем.
        # Повторный вызов plan_followup не должен сбрасывать
        # followup_queued / followup_skipped обратно в planned.
        if recipient.followup_status is not None:
            continue

        if _is_eligible_for_followup(recipient, run.followup_policy):
            recipient.followup_status = "followup_planned"
            count += 1
        elif recipient.excluded_reason is None:
            recipient.followup_status = "followup_skipped"

    await session.flush()
    logger.info("plan_followup run_id=%d planned=%d", run_id, count)
    return count


async def _set_followup_status(
    recipient_id: int,
    status: str,
    *,
    message_job_id: int | None = None,
) -> None:
    """Best-effort обновление статуса follow-up получателя."""
    async with SessionLocal() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_id)
            if recipient is None:
                return
            recipient.followup_status = status
            if message_job_id is not None:
                recipient.followup_message_job_id = message_job_id


async def execute_followup(run_id: int) -> dict:
    """Запустить follow-up: поставить MessageJob для planned клиентов.

    Возвращает dict со статистикой: queued, skipped, failed.
    """
    async with SessionLocal() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise ValueError(f"CampaignRun {run_id} not found")

        if not run.followup_enabled:
            raise ValueError(f"Follow-up не включён для run_id={run_id}")

        template_name = run.followup_template_name
        if not template_name:
            raise ValueError(f"followup_template_name не задан для run_id={run_id}")

        policy = run.followup_policy or ""

    stats: dict[str, int] = {
        "queued": 0,
        "skipped": 0,
        "failed": 0,
    }

    async with SessionLocal() as session:
        stmt = (
            select(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.followup_status == "followup_planned")
        )
        recipients = (await session.execute(stmt)).scalars().all()

    for recipient in recipients:
        company_id = recipient.company_id
        client_id = recipient.client_id
        recipient_id = recipient.id

        # Пропускаем только если нет ни локального клиента, ни телефона.
        # CRM-only получатели (client_id=None, но phone_e164 есть) — обрабатываем:
        # phone хранится в payload MessageJob, outbox_worker умеет его использовать.
        if not client_id and not recipient.phone_e164:
            await _set_followup_status(
                recipient_id,
                "followup_skipped",
            )
            stats["skipped"] += 1
            continue

        # Шаг 1. Атомарно "забираем" получателя в обработку.
        # Это защищает от дублей при параллельных /followup/run-now.
        try:
            async with SessionLocal() as session:
                async with session.begin():
                    claim_result = await session.execute(
                        update(CampaignRecipient)
                        .where(CampaignRecipient.id == recipient_id)
                        .where(CampaignRecipient.followup_status == "followup_planned")
                        .values(followup_status=_FOLLOWUP_PROCESSING)
                    )

            if (claim_result.rowcount or 0) == 0:
                stats["skipped"] += 1
                continue

        except Exception as exc:
            stats["failed"] += 1
            logger.error(
                "followup claim failed recipient_id=%d run_id=%d: %s",
                recipient_id,
                run_id,
                exc,
            )
            continue

        # Шаг 2. Re-evaluate по актуальным данным.
        # Ошибка одного recipient не должна обрывать весь цикл.
        try:
            async with SessionLocal() as session:
                async with session.begin():
                    fresh = await session.get(
                        CampaignRecipient,
                        recipient_id,
                    )
                    if fresh is None:
                        # Недостижимый кейс после claim-шага,
                        # но ошибка re-evaluate подхватит и вернёт в planned.
                        raise RuntimeError(f"recipient_id={recipient_id} not found after claim")

                    if not _is_eligible_for_followup(fresh, policy):
                        fresh.followup_status = "followup_skipped"
                        stats["skipped"] += 1
                        continue

        except Exception as exc:
            stats["failed"] += 1
            logger.error(
                "followup re-evaluate failed recipient_id=%d client_id=%d run_id=%d: %s",
                recipient_id,
                client_id,
                run_id,
                exc,
            )
            # Возвращаем в planned, чтобы можно было безопасно
            # повторно запустить /followup/run-now.
            await _set_followup_status(
                recipient_id,
                "followup_planned",
            )
            continue

        # Шаг 3. Создаём follow-up job.
        run_at = utcnow()
        try:
            async with SessionLocal() as session:
                async with session.begin():
                    current = await session.get(
                        CampaignRecipient,
                        recipient_id,
                    )
                    if current is None:
                        raise RuntimeError(f"recipient_id={recipient_id} not found")

                    if current.followup_status != _FOLLOWUP_PROCESSING:
                        raise RuntimeError("recipient not claimed for follow-up")

                    # Для CRM-only получателей (client_id=None) сохраняем phone_e164
                    # в payload — outbox_worker использует его при отправке.
                    followup_payload: dict = {
                        "kind": FOLLOWUP_JOB_TYPE,
                        "template_name": template_name,
                        "campaign_run_id": run_id,
                        "campaign_recipient_id": recipient_id,
                    }
                    if not current.client_id and current.phone_e164:
                        followup_payload["phone_e164"] = current.phone_e164
                        if current.display_name:
                            followup_payload["contact_name"] = current.display_name

                    await add_job(
                        session,
                        company_id=company_id,
                        record_id=None,
                        client_id=client_id,
                        job_type=FOLLOWUP_JOB_TYPE,
                        run_at=run_at,
                        payload=followup_payload,
                    )

                    dedupe_key = make_dedupe_key(
                        job_type=FOLLOWUP_JOB_TYPE,
                        company_id=company_id,
                        record_id=None,
                        run_at=run_at,
                    )
                    job = await session.scalar(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))

                    current.followup_status = "followup_queued"
                    if job:
                        current.followup_message_job_id = job.id

            stats["queued"] += 1
            logger.info(
                "followup queued client_id=%d run_id=%d",
                client_id,
                run_id,
            )

        except Exception as exc:
            stats["failed"] += 1
            logger.error(
                "followup failed recipient_id=%d client_id=%d run_id=%d: %s",
                recipient_id,
                client_id,
                run_id,
                exc,
            )
            # Возвращаем в planned для безопасного ручного ретрая.
            await _set_followup_status(
                recipient_id,
                "followup_planned",
            )

    logger.info(
        "execute_followup run_id=%d stats=%s",
        run_id,
        stats,
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
