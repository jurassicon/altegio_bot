from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.runner import FOLLOWUP_JOB_TYPE
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job, make_dedupe_key
from altegio_bot.models.models import AltegioEvent, CampaignRecipient, CampaignRun, Client, MessageJob, Record
from altegio_bot.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class FollowupFinalEligibilityResult:
    """Result of the final eligibility check before a follow-up is sent."""

    eligible: bool
    skip_reason: str | None
    followup_status: str | None
    booked_after_at: datetime | None


# Статусы, которые означают «сообщение прочитано или позже» для follow-up политик.
_READ_OR_LATER_STATUSES: frozenset[str] = frozenset({"read", "replied", "booked_after_campaign"})

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
        is_read = recipient.read_at is not None or recipient.status in _READ_OR_LATER_STATUSES
        return not is_read

    if policy == "unread_or_not_booked":
        is_read = recipient.read_at is not None or recipient.status in _READ_OR_LATER_STATUSES
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


async def _find_record_create_event(
    session: AsyncSession,
    *,
    recipient: CampaignRecipient,
    company_id: int,
    attribution_start: datetime,
) -> datetime | None:
    """Return received_at of the earliest 'record create' AltegioEvent after attribution_start.

    Joins AltegioEvent with Record to match by client identity.
    Primary match: Record.client_id == recipient.client_id.
    Fallback: Record.altegio_client_id == recipient.altegio_client_id (when client_id is None).

    is_deleted is intentionally NOT filtered: a record created after the campaign but later
    cancelled/deleted still means the marketing goal was achieved — follow-up must not be sent.
    """
    if recipient.client_id is not None:
        client_cond = Record.client_id == recipient.client_id
    else:
        client_cond = Record.altegio_client_id == recipient.altegio_client_id

    stmt = (
        select(AltegioEvent.received_at)
        .join(
            Record,
            and_(
                Record.altegio_record_id == AltegioEvent.resource_id,
                Record.company_id == company_id,
            ),
        )
        .where(AltegioEvent.company_id == company_id)
        .where(AltegioEvent.resource == "record")
        .where(AltegioEvent.event_status == "create")
        .where(AltegioEvent.received_at > attribution_start)
        .where(client_cond)
        .order_by(AltegioEvent.received_at.asc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def _has_future_record(
    session: AsyncSession,
    *,
    recipient: CampaignRecipient,
    company_id: int,
    now: datetime,
) -> bool:
    """Return True if the client has at least one non-deleted future record."""
    if recipient.client_id is not None:
        client_cond = Record.client_id == recipient.client_id
    else:
        client_cond = Record.altegio_client_id == recipient.altegio_client_id

    stmt = (
        select(Record.id)
        .where(Record.company_id == company_id)
        .where(Record.starts_at > now)
        .where(func.coalesce(Record.is_deleted, False).is_(False))
        .where(client_cond)
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def check_followup_final_eligibility(
    session: AsyncSession,
    recipient: CampaignRecipient,
    run: CampaignRun | None,
    now: datetime,
) -> FollowupFinalEligibilityResult:
    """Final eligibility guard before a follow-up message is sent.

    Called at actual send time (outbox_worker) to catch state changes that occurred
    between job creation (execute_followup) and actual delivery — typically 14 days.

    Checks in order:
      3.1  recipient status / read_at / booked_after_at
      3.2  current opt-out state
      3.3  new record create event after the original campaign (AltegioEvent)
      3.4  any non-deleted future record already booked

    Returns FollowupFinalEligibilityResult(eligible=True) when safe to send.
    Eligible=False includes a followup_status string to persist on the recipient
    and a human-readable skip_reason for job.last_error.
    """
    # 3.1 — status / attribution timestamps
    if recipient.status in _READ_OR_LATER_STATUSES:
        if recipient.status == "booked_after_campaign":
            fs = "skipped_booked_after"
            reason = "Follow-up skipped: recipient already booked after campaign"
        else:
            fs = "skipped_read"
            reason = "Follow-up skipped: recipient already read or replied to original campaign"
        return FollowupFinalEligibilityResult(
            eligible=False,
            skip_reason=reason,
            followup_status=fs,
            booked_after_at=recipient.booked_after_at,
        )

    if recipient.read_at is not None:
        return FollowupFinalEligibilityResult(
            eligible=False,
            skip_reason="Follow-up skipped: recipient already read original campaign",
            followup_status="skipped_read",
            booked_after_at=None,
        )

    if recipient.booked_after_at is not None:
        return FollowupFinalEligibilityResult(
            eligible=False,
            skip_reason="Follow-up skipped: recipient already booked after campaign",
            followup_status="skipped_booked_after",
            booked_after_at=recipient.booked_after_at,
        )

    # 3.2 — current opt-out
    # Primary: look up by local client_id.
    # Fallback: CRM-only recipients (client_id=None) — look up by phone+company.
    if recipient.client_id is not None:
        opt_client = await session.get(Client, recipient.client_id)
    elif recipient.phone_e164:
        opt_result = await session.execute(
            select(Client)
            .where(Client.company_id == recipient.company_id)
            .where(Client.phone_e164 == recipient.phone_e164)
            .limit(1)
        )
        opt_client = opt_result.scalar_one_or_none()
    else:
        opt_client = None

    if opt_client is not None and opt_client.wa_opted_out:
        return FollowupFinalEligibilityResult(
            eligible=False,
            skip_reason="Follow-up skipped: client has opted out",
            followup_status="skipped_opted_out",
            booked_after_at=None,
        )

    # Without any client identifier we cannot query records or events.
    has_client_id = recipient.client_id is not None
    has_altegio_id = recipient.altegio_client_id is not None
    if not has_client_id and not has_altegio_id:
        return FollowupFinalEligibilityResult(
            eligible=True,
            skip_reason=None,
            followup_status=None,
            booked_after_at=None,
        )

    # Attribution start: max(run.completed_at, recipient.sent_at).
    attribution_start: datetime | None = None
    if run is not None and run.completed_at is not None:
        attribution_start = run.completed_at
    if recipient.sent_at is not None:
        if attribution_start is None or recipient.sent_at > attribution_start:
            attribution_start = recipient.sent_at

    # 3.3 — AltegioEvent record create after campaign
    if attribution_start is not None:
        evt_at = await _find_record_create_event(
            session,
            recipient=recipient,
            company_id=recipient.company_id,
            attribution_start=attribution_start,
        )
        if evt_at is not None:
            return FollowupFinalEligibilityResult(
                eligible=False,
                skip_reason="Follow-up skipped: client booked after campaign",
                followup_status="skipped_booked_after",
                booked_after_at=evt_at,
            )

    # 3.4 — future record already booked
    has_future = await _has_future_record(
        session,
        recipient=recipient,
        company_id=recipient.company_id,
        now=now,
    )
    if has_future:
        return FollowupFinalEligibilityResult(
            eligible=False,
            skip_reason="Follow-up skipped: client has future record",
            followup_status="skipped_future_record",
            booked_after_at=None,
        )

    return FollowupFinalEligibilityResult(
        eligible=True,
        skip_reason=None,
        followup_status=None,
        booked_after_at=None,
    )


def followup_run_at(run: CampaignRun) -> str | None:
    """Вернуть дату запуска follow-up в ISO-формате или None."""
    if not run.followup_enabled or not run.followup_delay_days:
        return None
    if run.completed_at is None:
        return None
    run_at = run.completed_at + timedelta(days=run.followup_delay_days)
    return run_at.isoformat()
