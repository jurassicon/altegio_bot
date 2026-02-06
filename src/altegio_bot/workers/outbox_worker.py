from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select
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
    pick_sender_id,
)

logger = logging.getLogger("outbox_worker")

MIN_SECONDS_BETWEEN_MESSAGES = 30

UNSUBSCRIBE_LINKS = {
    758285: "https://example.com/unsubscribe/karlsruhe",
    1271200: "https://example.com/unsubscribe/rastatt",
}

PRE_APPOINTMENT_NOTES_DE = (
    "\n\nWichtige Hinweise vor dem Termin:\n"
    "• Bitte pünktlich kommen — ab 15 Min. Verspätung können wir "
    "nicht garantieren, dass der Termin stattfindet.\n"
    "• Wimpern bitte sauber: ohne Mascara, ohne geklebte Wimpern.\n"
    "• Falls Sie schon eine Kundenkarte haben, bitte mitbringen.\n"
    "• Auffüllen: ab 3. Woche 60 €, ab 4. Woche 70 €, ab 5. Woche "
    "keine Auffüllung (Neuauflage).\n"
    "• Zahlung: bar oder mit Karte.\n"
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "0.00"
    return f"{value:.2f}"


def _fmt_date(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone().strftime("%d.%m.%Y")


def _fmt_time(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone().strftime("%H:%M")


async def _lock_next_jobs(
    session: AsyncSession,
    batch_size: int,
) -> list[MessageJob]:
    stmt = (
        select(MessageJob)
        .where(MessageJob.status == "queued")
        .where(MessageJob.run_at <= utcnow())
        .order_by(MessageJob.run_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    jobs = list(res.scalars().all())

    for job in jobs:
        job.status = "processing"

    return jobs


async def _apply_rate_limit(
    session: AsyncSession,
    phone_e164: str,
) -> datetime | None:
    now = utcnow()
    stmt = (
        select(ContactRateLimit)
        .where(ContactRateLimit.phone_e164 == phone_e164)
        .with_for_update()
    )
    res = await session.execute(stmt)
    rl = res.scalar_one_or_none()

    if rl is None:
        rl = ContactRateLimit(phone_e164=phone_e164, next_allowed_at=now)
        session.add(rl)
        await session.flush()

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
    """
    Новый клиент = нет более ранних записей (records) у этого клиента в этой
    компании (по starts_at), кроме текущей записи.
    """
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
) -> tuple[str, int]:
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == template_code)
        .where(MessageTemplate.is_active.is_(True))
    )
    res = await session.execute(stmt)
    tmpl = res.scalar_one()

    services_text = ""
    total_cost = Decimal("0.00")

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
            lines.append(f"{svc.title} — {_fmt_money(svc.cost_to_pay)}€")
            if svc.cost_to_pay is not None:
                total_cost += svc.cost_to_pay

        services_text = "\n".join(lines)

    unsubscribe_link = UNSUBSCRIBE_LINKS.get(company_id, "")

    sender_code = "default"
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
            f"No active sender for company={company_id} code={sender_code}"
        )

    pre_appointment_notes = ""
    if template_code == "record_created" and record is not None:
        is_new = await _is_new_client_for_record(
            session=session,
            company_id=company_id,
            client_id=(record.client_id if record else None),
            record_id=record.id,
            record_starts_at=record.starts_at,
        )
        if is_new:
            pre_appointment_notes = PRE_APPOINTMENT_NOTES_DE

    ctx = {
        "client_name": (client.display_name if client else ""),
        "staff_name": (record.staff_name if record else ""),
        "date": _fmt_date(record.starts_at if record else None),
        "time": _fmt_time(record.starts_at if record else None),
        "services": services_text,
        "total_cost": _fmt_money(total_cost),
        "short_link": (record.short_link if record else ""),
        "unsubscribe_link": unsubscribe_link,
        "sender_id": sender_id,
        "sender_code": sender_code,
        "pre_appointment_notes": pre_appointment_notes,
    }

    body = tmpl.body.format(**ctx)
    return body, sender_id


async def _load_job(
    session: AsyncSession,
    job_id: int,
) -> MessageJob | None:
    stmt = (
        select(MessageJob)
        .where(MessageJob.id == job_id)
        .with_for_update()
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


async def process_job(
    job_id: int,
    provider: WhatsAppProvider,
) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            job = await _load_job(session, job_id)
            if job is None:
                return

            existing = await _find_existing_outbox(session, job.id)
            if existing is not None:
                logger.info(
                    "Skip job_id=%s (already sent outbox_id=%s)",
                    job.id,
                    existing.id,
                )
                job.status = "done"
                job.last_error = None
                return

            record = await _load_record(session, job)
            client = await _load_client(session, job, record)

            phone = client.phone_e164 if client else None
            if not phone:
                job.status = "failed"
                job.last_error = "No phone_e164"
                return

            delay_until = await _apply_rate_limit(session, phone)
            if delay_until is not None:
                job.status = "queued"
                job.run_at = delay_until
                return

            try:
                body, sender_id = await _render_message(
                    session=session,
                    company_id=job.company_id,
                    template_code=job.job_type,
                    record=record,
                    client=client,
                )
            except Exception as exc:
                job.status = "failed"
                job.last_error = f"Template render error: {exc}"
                return

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
                    language="de",
                    body=body,
                    status="failed",
                    error=err,
                    provider_message_id=msg_id,
                    scheduled_at=utcnow(),
                    sent_at=utcnow(),
                    meta={},
                )
                session.add(out)

                job.status = "failed"
                job.last_error = f"Send failed: {err}"
                return

            out = OutboxMessage(
                company_id=job.company_id,
                client_id=(client.id if client else None),
                record_id=(record.id if record else None),
                job_id=job.id,
                sender_id=sender_id,
                phone_e164=phone,
                template_code=job.job_type,
                language="de",
                body=body,
                status="sent",
                error=None,
                provider_message_id=msg_id,
                scheduled_at=utcnow(),
                sent_at=utcnow(),
                meta={},
            )
            session.add(out)

            job.status = "done"
            job.last_error = None

            logger.info(
                "Outbox sent job_id=%s outbox_id=%s sender_id=%s phone=%s",
                job.id,
                out.id,
                sender_id,
                phone,
            )


async def run_loop(
    provider: WhatsAppProvider,
    batch_size: int = 50,
    poll_sec: float = 1.0,
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("Outbox worker started")

    while True:
        job_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                jobs = await _lock_next_jobs(session, batch_size)
                job_ids = [j.id for j in jobs]

        if not job_ids:
            await asyncio.sleep(poll_sec)
            continue

        for jid in job_ids:
            await process_job(job_id=jid, provider=provider)


def main() -> None:
    provider = DummyProvider()
    asyncio.run(run_loop(provider=provider))


if __name__ == "__main__":
    main()
