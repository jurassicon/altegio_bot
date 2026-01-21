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

logger = logging.getLogger("outbox_worker")

MIN_SECONDS_BETWEEN_MESSAGES = 30


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
        rl = ContactRateLimit(
            phone_e164=phone_e164,
            next_allowed_at=now,
        )
        session.add(rl)
        await session.flush()

    if rl.next_allowed_at is not None and rl.next_allowed_at > now:
        return rl.next_allowed_at

    rl.next_allowed_at = now + timedelta(seconds=MIN_SECONDS_BETWEEN_MESSAGES)
    return None


async def _render_message(
    session: AsyncSession,
    *,
    company_id: int,
    template_code: str,
    record: Record | None,
    client: Client | None,
) -> str:
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
        for s in services:
            lines.append(f"{s.title} — {_fmt_money(s.cost_to_pay)}€")
            if s.cost_to_pay is not None:
                total_cost += s.cost_to_pay

        services_text = "\n".join(lines)

    ctx = {
        "client_name": (client.display_name if client else ""),
        "staff_name": (record.staff_name if record else ""),
        "date": _fmt_date(record.starts_at if record else None),
        "time": _fmt_time(record.starts_at if record else None),
        "services": services_text,
        "total_cost": _fmt_money(total_cost),
        "short_link": (record.short_link if record else ""),
    }

    return tmpl.body.format(**ctx)


async def process_job(job_id: int) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            stmt = (
                select(MessageJob)
                .where(MessageJob.id == job_id)
                .with_for_update()
            )
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()
            if job is None:
                return

            phone = job.phone_e164
            if not phone and job.client_id:
                c = await session.get(Client, job.client_id)
                phone = c.phone_e164 if c else None

            if not phone:
                job.status = "failed"
                job.error = "No phone_e164"
                job.processed_at = utcnow()
                return

            delay_until = await _apply_rate_limit(session, phone)
            if delay_until is not None:
                job.status = "queued"
                job.run_at = delay_until
                return

            record = None
            if job.record_id:
                record = await session.get(Record, job.record_id)

            client = None
            if job.client_id:
                client = await session.get(Client, job.client_id)

            try:
                body = await _render_message(
                    session,
                    company_id=job.company_id,
                    template_code=job.job_type,
                    record=record,
                    client=client,
                )
            except Exception as exc:
                job.status = "failed"
                job.error = f"Template render error: {exc}"
                job.processed_at = utcnow()
                return

            out = OutboxMessage(
                company_id=job.company_id,
                job_id=job.id,
                phone_e164=phone,
                template_code=job.job_type,
                body=body,
                status="sent",
                created_at=utcnow(),
                sent_at=utcnow(),
            )
            session.add(out)

            job.status = "done"
            job.error = None
            job.processed_at = utcnow()

            logger.info(
                "Outbox sent (test) job_id=%s phone=%s type=%s",
                job.id,
                phone,
                job.job_type,
            )


async def run_loop(batch_size: int = 50, poll_sec: float = 1.0) -> None:
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
            await process_job(jid)


def main() -> None:
    asyncio.run(run_loop())


if __name__ == "__main__":
    main()
