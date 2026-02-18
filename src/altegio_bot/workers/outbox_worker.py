from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import (
    Client,
    ContactRateLimit,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    Record,
    RecordService,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import DummyProvider, safe_send
from altegio_bot.whatsapp_routing import (
    pick_sender_code_for_record,
    pick_sender_id
)

logger = logging.getLogger('outbox_worker')

MIN_SECONDS_BETWEEN_MESSAGES = 30
DEFAULT_LANGUAGE = 'de'
PAST_RECORD_GRACE_MINUTES = 5

TOKEN_EXPIRED_RETRY_SECONDS = 60
STOP_WORKER_ON_TOKEN_EXPIRED_ENV = 'STOP_WORKER_ON_TOKEN_EXPIRED'
_TOKEN_EXPIRED = False

STALE_PROCESSING_MINUTES = 10

DEFAULT_LANGUAGE_BY_COMPANY = {
    758285: 'de',   # Karlsruhe
    1271200: 'de',  # Rastatt
}

UNSUBSCRIBE_LINKS = {
    758285: 'https://example.com/unsubscribe/karlsruhe',
    1271200: 'https://example.com/unsubscribe/rastatt',
}

PRE_APPOINTMENT_NOTES_DE = (
    '\n\nWichtige Hinweise vor dem Termin:\n'
    '• Bitte pünktlich kommen — ab 15 Min. Verspätung können wir '
    'nicht garantieren, dass der Termin stattfindet.\n'
    '• Wimpern bitte sauber: ohne Mascara, ohne geklebte Wimpern.\n'
    '• Falls Sie schon eine Kundenkarte haben, bitte mitbringen.\n'
    '• Auffüllen: ab 3. Woche 60 €, ab 4. Woche 70 €, ab 5. Woche '
    'keine Auffüllung (Neuauflage).\n'
    '• Zahlung: bar oder mit Karte.\n'
)

SUCCESS_OUTBOX_STATUSES = ('sent', 'delivered', 'read')


def _stop_worker_on_token_expired() -> bool:
    return os.getenv(STOP_WORKER_ON_TOKEN_EXPIRED_ENV, '0').strip() == '1'


def _mark_token_expired() -> None:
    global _TOKEN_EXPIRED
    _TOKEN_EXPIRED = True


def _token_expired() -> bool:
    return _TOKEN_EXPIRED


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _record_is_in_past(record: Record | None) -> bool:
    if record is None or record.starts_at is None:
        return False
    cutoff = utcnow() - timedelta(minutes=PAST_RECORD_GRACE_MINUTES)
    return record.starts_at < cutoff


async def _find_success_outbox(
    session: AsyncSession,
    job_id: int,
) -> OutboxMessage | None:
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.job_id == job_id)
        .where(OutboxMessage.status.in_(SUCCESS_OUTBOX_STATUSES))
        .order_by(OutboxMessage.id.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _find_existing_outbox(
    session: AsyncSession,
    job_id: int,
) -> OutboxMessage | None:
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.job_id == job_id)
        .order_by(OutboxMessage.id.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


def _retry_delay_seconds(attempt: int) -> int:
    base = 30
    delay = base * (2 ** (attempt - 1))
    return min(delay, 15 * 60)


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return '0.00'
    return f'{value:.2f}'


def _fmt_date(dt: datetime | None) -> str:
    if dt is None:
        return ''
    return dt.astimezone().strftime('%d.%m.%Y')


def _fmt_time(dt: datetime | None) -> str:
    if dt is None:
        return ''
    return dt.astimezone().strftime('%H:%M')


async def _lock_next_jobs(
    session: AsyncSession,
    batch_size: int,
) -> list[MessageJob]:
    stmt = (
        select(MessageJob)
        .where(MessageJob.status == 'queued')
        .where(MessageJob.run_at <= utcnow())
        .order_by(MessageJob.run_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    jobs = list(res.scalars().all())

    now = utcnow()
    for job in jobs:
        job.status = 'processing'
        job.locked_at = now

    return jobs


async def _requeue_processing_jobs(
    session: AsyncSession,
    job_ids: list[int],
) -> None:
    if not job_ids:
        return

    stmt = (
        update(MessageJob)
        .where(MessageJob.id.in_(job_ids))
        .where(MessageJob.status == 'processing')
        .values(status='queued', locked_at=None)
    )
    await session.execute(stmt)


async def _requeue_stale_processing_jobs(session: AsyncSession) -> int:
    cutoff = utcnow() - timedelta(minutes=STALE_PROCESSING_MINUTES)

    stmt = (
        update(MessageJob)
        .where(MessageJob.status == 'processing')
        .where(MessageJob.locked_at.is_not(None))
        .where(MessageJob.locked_at < cutoff)
        .values(
            status='queued',
            locked_at=None,
            run_at=utcnow(),
            last_error='Recovered: stale processing job',
        )
    )
    res = await session.execute(stmt)
    return int(getattr(res, 'rowcount', 0) or 0)


async def _apply_rate_limit(
    session: AsyncSession,
    phone_e164: str,
) -> datetime | None:
    now = utcnow()

    await session.execute(
        text(
            '''
            INSERT INTO contact_rate_limits (phone_e164, next_allowed_at)
            VALUES (:phone_e164, :next_allowed_at)
            ON CONFLICT (phone_e164) DO NOTHING;
            '''
        ),
        {'phone_e164': phone_e164, 'next_allowed_at': now},
    )

    stmt = (
        select(ContactRateLimit)
        .where(ContactRateLimit.phone_e164 == phone_e164)
        .with_for_update()
    )
    res = await session.execute(stmt)
    rl = res.scalar_one()

    if rl.next_allowed_at > now:
        return rl.next_allowed_at

    rl.next_allowed_at = now + timedelta(seconds=MIN_SECONDS_BETWEEN_MESSAGES)
    return None


async def _load_record(
    session: AsyncSession,
    job: MessageJob,
) -> Record | None:
    if job.record_id is None:
        return None
    return await session.get(Record, job.record_id)


def _pick_language(company_id: int, client: Client | None) -> str:
    return DEFAULT_LANGUAGE_BY_COMPANY.get(company_id, DEFAULT_LANGUAGE)


async def _load_template(
    session: AsyncSession,
    *,
    company_id: int,
    template_code: str,
    language: str,
) -> tuple[MessageTemplate | None, str]:
    base = (
        select(MessageTemplate)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == template_code)
        .where(MessageTemplate.is_active.is_(True))
    )

    stmt = base.where(MessageTemplate.language == language).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        return tmpl, language

    if language != DEFAULT_LANGUAGE:
        stmt = base.where(MessageTemplate.language == DEFAULT_LANGUAGE).limit(1)
        res = await session.execute(stmt)
        tmpl = res.scalar_one_or_none()
        if tmpl is not None:
            return tmpl, DEFAULT_LANGUAGE

    stmt = base.order_by(MessageTemplate.id.asc()).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        return tmpl, tmpl.language

    return None, language


async def _load_client(
    session: AsyncSession,
    job: MessageJob,
    record: Record | None,
) -> Client | None:
    if job.client_id is not None:
        return await session.get(Client, job.client_id)

    if record is not None and record.client_id is not None:
        return await session.get(Client, record.client_id)

    return None


async def _is_new_client_for_record(
    session: AsyncSession,
    *,
    company_id: int,
    client_id: int | None,
    record_id: int | None,
    record_starts_at: datetime | None,
) -> bool:
    if client_id is None or record_id is None or record_starts_at is None:
        return False

    stmt = (
        select(Record.id)
        .where(Record.company_id == company_id)
        .where(Record.client_id == client_id)
        .where(Record.id != record_id)
        .where(Record.starts_at.is_not(None))
        .where(Record.starts_at < record_starts_at)
        .limit(1)
    )
    res = await session.execute(stmt)
    prev_id = res.scalar_one_or_none()
    return prev_id is None


async def _render_message(
    session: AsyncSession,
    *,
    company_id: int,
    template_code: str,
    record: Record | None,
    client: Client | None,
) -> tuple[str, int, str]:
    language = _pick_language(company_id, client)

    tmpl, used_lang = await _load_template(
        session,
        company_id=company_id,
        template_code=template_code,
        language=language,
    )
    if tmpl is None:
        raise ValueError(
            f'Template not found: company={company_id} code={template_code}'
        )

    services_text = ''
    total_cost = Decimal('0.00')

    if record is not None:
        svc_stmt = (
            select(RecordService)
            .where(RecordService.record_id == record.id)
            .order_by(RecordService.service_id.asc())
        )
        svc_res = await session.execute(svc_stmt)
        services = list(svc_res.scalars().all())

        lines: list[str] = []
        for svc in services:
            lines.append(f'{svc.title} — {_fmt_money(svc.cost_to_pay)}€')
            if svc.cost_to_pay is not None:
                total_cost += svc.cost_to_pay

        services_text = '\n'.join(lines)

    unsubscribe_link = UNSUBSCRIBE_LINKS.get(company_id, '')

    sender_code = 'default'
    if record is not None:
        sender_code = await pick_sender_code_for_record(
            session=session,
            company_id=company_id,
            record_id=record.id,
        )

    sender_id = await pick_sender_id(
        session=session,
        company_id=company_id,
        sender_code=sender_code,
    )
    if sender_id is None:
        raise ValueError(
            f'No active sender for company={company_id} code={sender_code}'
        )

    pre_appointment_notes = ''
    if (
        template_code == 'record_created'
        and record is not None
        and used_lang == 'de'
    ):
        is_new = await _is_new_client_for_record(
            session=session,
            company_id=company_id,
            client_id=record.client_id,
            record_id=record.id,
            record_starts_at=record.starts_at,
        )
        if is_new:
            pre_appointment_notes = PRE_APPOINTMENT_NOTES_DE

    ctx = {
        'client_name': (client.display_name if client else ''),
        'staff_name': (record.staff_name if record else ''),
        'date': _fmt_date(record.starts_at if record else None),
        'time': _fmt_time(record.starts_at if record else None),
        'services': services_text,
        'total_cost': _fmt_money(total_cost),
        'short_link': (record.short_link if record else ''),
        'unsubscribe_link': unsubscribe_link,
        'sender_id': sender_id,
        'sender_code': sender_code,
        'pre_appointment_notes': pre_appointment_notes,
    }

    body = tmpl.body.format(**ctx)
    return body, sender_id, used_lang


async def _load_job(
    session: AsyncSession,
    job_id: int,
) -> MessageJob | None:
    stmt = (
        select(MessageJob)
        .where(MessageJob.id == job_id)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    job = res.scalar_one_or_none()
    if job is not None:
        return job

    exists_stmt = select(MessageJob.id).where(MessageJob.id == job_id)
    exists_res = await session.execute(exists_stmt)
    exists_id = exists_res.scalar_one_or_none()

    if exists_id is None:
        raise RuntimeError(f'MessageJob not found: id={job_id}')

    logger.info('Skip job_id=%s (locked)', job_id)
    return None


def _is_token_expired_error(err: str) -> bool:
    low = err.lower()
    return 'access token' in low and 'expired' in low


async def process_job_in_session(
    session: AsyncSession,
    job_id: int,
    provider: WhatsAppProvider,
) -> None:
    job = await _load_job(session, job_id)
    if job is None:
        return

    success = await _find_success_outbox(session, job.id)
    if success is not None:
        logger.info(
            'Skip job_id=%s (already sent outbox_id=%s)',
            job.id,
            getattr(success, 'id', None),
        )
        job.status = 'done'
        job.locked_at = None
        job.last_error = None
        return

    attempts = getattr(job, 'attempts', 0)
    max_attempts = getattr(job, 'max_attempts', 5)

    if attempts >= max_attempts:
        job.status = 'failed'
        job.locked_at = None
        job.last_error = 'Max attempts reached'
        return

    record = await _load_record(session, job)
    if _record_is_in_past(record):
        job.status = 'canceled'
        job.locked_at = None
        job.last_error = 'Skipped: record starts_at is in the past'
        return

    client = await _load_client(session, job, record)

    phone = client.phone_e164 if client else None
    if not phone:
        job.status = 'failed'
        job.locked_at = None
        job.last_error = 'No phone_e164'
        return

    delay_until = await _apply_rate_limit(session, phone)
    if delay_until is not None:
        job.status = 'queued'
        job.locked_at = None
        job.run_at = delay_until
        return

    try:
        body, sender_id, lang = await _render_message(
            session=session,
            company_id=job.company_id,
            template_code=job.job_type,
            record=record,
            client=client,
        )
    except Exception as exc:
        job.status = 'failed'
        job.locked_at = None
        job.last_error = f'Template render error: {exc}'
        return

    attempts = getattr(job, 'attempts', 0) + 1
    setattr(job, 'attempts', attempts)

    msg_id, err = await safe_send(
        provider=provider,
        sender_id=sender_id,
        phone=phone,
        text=body,
    )

    if err is not None:
        out = OutboxMessage(
            company_id=job.company_id,
            client_id=(client.id if client else None),
            record_id=(record.id if record else None),
            job_id=job.id,
            sender_id=sender_id,
            phone_e164=phone,
            template_code=job.job_type,
            language=lang,
            body=body,
            status='failed',
            error=err,
            provider_message_id=msg_id,
            scheduled_at=job.run_at,
            sent_at=utcnow(),
            meta={},
        )
        session.add(out)

        if _is_token_expired_error(err):
            _mark_token_expired()
            job.status = 'queued'
            job.locked_at = None
            job.run_at = utcnow() + timedelta(
                seconds=TOKEN_EXPIRED_RETRY_SECONDS
            )
            job.last_error = f'Send blocked: {err}'
            return

        job.last_error = f'Send failed: {err}'

        max_attempts = getattr(job, 'max_attempts', 5)
        if attempts >= max_attempts:
            job.status = 'failed'
            job.locked_at = None
            return

        delay = _retry_delay_seconds(attempts)
        job.status = 'queued'
        job.locked_at = None
        job.run_at = utcnow() + timedelta(seconds=delay)
        return

    out = OutboxMessage(
        company_id=job.company_id,
        client_id=(client.id if client else None),
        record_id=(record.id if record else None),
        job_id=job.id,
        sender_id=sender_id,
        phone_e164=phone,
        template_code=job.job_type,
        language=lang,
        body=body,
        status='sent',
        error=None,
        provider_message_id=msg_id,
        scheduled_at=job.run_at,
        sent_at=utcnow(),
        meta={},
    )
    session.add(out)

    job.status = 'done'
    job.locked_at = None
    job.last_error = None

    logger.info(
        'Outbox sent job_id=%s outbox_id=%s sender_id=%s phone=%s',
        job.id,
        getattr(out, 'id', None),
        sender_id,
        phone,
    )


async def process_job(
    job_id: int,
    provider: WhatsAppProvider,
) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            await process_job_in_session(
                session=session,
                job_id=job_id,
                provider=provider,
            )


async def run_loop(
    provider: WhatsAppProvider,
    batch_size: int = 50,
    poll_sec: float = 1.0,
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    logger.info('Outbox worker started')

    while True:
        async with SessionLocal() as session:
            async with session.begin():
                recovered = await _requeue_stale_processing_jobs(session)
                if recovered:
                    logger.warning(
                        'Recovered stale processing jobs: %s',
                        recovered,
                    )

        job_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                jobs = await _lock_next_jobs(session, batch_size)
                job_ids = [j.id for j in jobs]

        if not job_ids:
            await asyncio.sleep(poll_sec)
            continue

        for idx, jid in enumerate(job_ids):
            await process_job(job_id=jid, provider=provider)

            if _token_expired() and _stop_worker_on_token_expired():
                remaining = job_ids[idx + 1:]
                if remaining:
                    async with SessionLocal() as session:
                        async with session.begin():
                            await _requeue_processing_jobs(session, remaining)

                logger.error(
                    'Stopping outbox worker: access token expired '
                    '(requeued %s jobs)',
                    len(remaining),
                )
                return


async def run_once(
    session_maker: Any,
    *,
    provider: Any,
    limit: int = 10,
    company_id: int | None = None,
) -> int:
    from sqlalchemy import func, select

    async with session_maker() as session:
        async with session.begin():
            await _requeue_stale_processing_jobs(session)

        stmt = (
            select(MessageJob.id)
            .where(MessageJob.status == 'queued')
            .where(MessageJob.run_at <= func.now())
            .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
            .limit(limit)
        )
        if company_id is not None:
            stmt = stmt.where(MessageJob.company_id == company_id)

        res = await session.execute(stmt)
        ids = list(res.scalars().all())

        for job_id in ids:
            await process_job_in_session(session, int(job_id), provider=provider)

        await session.commit()
        return len(ids)


def _build_provider() -> WhatsAppProvider:
    name = os.getenv('WHATSAPP_PROVIDER', 'dummy').strip()
    if name == 'meta_cloud':
        from altegio_bot.providers.meta_cloud import MetaCloudProvider

        return MetaCloudProvider()
    return DummyProvider()


def main() -> None:
    provider = _build_provider()
    asyncio.run(run_loop(provider=provider))


if __name__ == '__main__':
    main()
