from __future__ import annotations

import argparse
import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from altegio_bot.models.models import (
    Client,
    MessageJob,
    MessageTemplate,
    Record,
    RecordService,
    ServiceSenderRule,
    WhatsAppSender,
)
from altegio_bot.workers import outbox_worker as ow

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass
class SmokeConfig:
    company_id: int
    service_id: int
    sender_code: str
    template_code: str
    language: str
    phone_e164: str
    display_name: str
    staff_name: str


class SmokeProvider:
    async def send_text(self, *args: Any, **kwargs: Any) -> str:
        return f"smoke-{os.urandom(4).hex()}"

    async def send_message(self, *args: Any, **kwargs: Any) -> str:
        return await self.send_text(*args, **kwargs)

    async def __call__(self, *args: Any, **kwargs: Any) -> str:
        return await self.send_text(*args, **kwargs)


def _database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


def _make_sessionmaker() -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(_database_url(), echo=False)
    return async_sessionmaker(engine, expire_on_commit=False)


async def _fix_sequences(session: AsyncSession) -> None:
    tables = [
        "whatsapp_senders",
        "message_templates",
        "clients",
        "records",
        "message_jobs",
        "outbox_messages",
    ]
    for table in tables:
        sql = f"""
        SELECT setval(
          pg_get_serial_sequence('{table}', 'id'),
          COALESCE((SELECT MAX(id) FROM {table}), 1),
          (SELECT MAX(id) FROM {table}) IS NOT NULL
        );
        """
        await session.execute(text(sql))


async def _get_or_create_sender(
    session: AsyncSession,
    cfg: SmokeConfig,
) -> WhatsAppSender:
    stmt = select(WhatsAppSender).where(
        WhatsAppSender.company_id == cfg.company_id,
        WhatsAppSender.sender_code == cfg.sender_code,
    )
    res = await session.execute(stmt)
    sender = res.scalar_one_or_none()
    if sender:
        return sender

    sender = WhatsAppSender(
        company_id=cfg.company_id,
        sender_code=cfg.sender_code,
        phone_number_id=f"dummy-{os.urandom(8).hex()}",
        display_phone="+491111111111",
        is_active=True,
    )
    session.add(sender)
    await session.flush()
    return sender


async def _ensure_service_sender_rule(
    session: AsyncSession,
    cfg: SmokeConfig,
) -> None:
    stmt = select(ServiceSenderRule).where(
        ServiceSenderRule.company_id == cfg.company_id,
        ServiceSenderRule.service_id == cfg.service_id,
    )
    res = await session.execute(stmt)
    rule = res.scalar_one_or_none()
    if rule:
        return

    rule = ServiceSenderRule(
        company_id=cfg.company_id,
        service_id=cfg.service_id,
        sender_code=cfg.sender_code,
    )
    session.add(rule)
    await session.flush()


async def _ensure_template(
    session: AsyncSession,
    cfg: SmokeConfig,
) -> None:
    stmt = select(MessageTemplate).where(
        MessageTemplate.company_id == cfg.company_id,
        MessageTemplate.code == cfg.template_code,
        MessageTemplate.language == cfg.language,
    )
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()

    body = (
        "*{client_name}, hallo!*\\n\\n"
        "Это smoke-шаблон.\\n"
        "Mitarbeiterin: {staff_name}\\n"
        "Link: {short_link}\\n"
    )

    if tmpl:
        tmpl.body = body
        tmpl.is_active = True
        return

    tmpl = MessageTemplate(
        company_id=cfg.company_id,
        code=cfg.template_code,
        language=cfg.language,
        body=body,
        is_active=True,
    )
    session.add(tmpl)
    await session.flush()


async def _create_fixtures(
    session: AsyncSession,
    cfg: SmokeConfig,
    *,
    fix_sequences: bool,
) -> int:
    if fix_sequences:
        await _fix_sequences(session)

    await _get_or_create_sender(session, cfg)
    await _ensure_service_sender_rule(session, cfg)
    await _ensure_template(session, cfg)

    client = Client(
        company_id=cfg.company_id,
        altegio_client_id=int.from_bytes(os.urandom(6), "big"),
        phone_e164=cfg.phone_e164,
        display_name=cfg.display_name,
        email=None,
        raw={},
    )
    session.add(client)
    await session.flush()

    starts_at = utcnow() + timedelta(hours=2)
    record = Record(
        company_id=cfg.company_id,
        altegio_record_id=int.from_bytes(os.urandom(6), "big"),
        client_id=client.id,
        altegio_client_id=client.altegio_client_id,
        staff_id=None,
        staff_name=cfg.staff_name,
        starts_at=starts_at,
        ends_at=None,
        duration_sec=None,
        comment=None,
        short_link="https://example.com/smoke",
        confirmed=None,
        attendance=None,
        visit_attendance=None,
        is_deleted=False,
        total_cost=Decimal("10.00"),
        last_change_at=None,
        raw={},
    )
    session.add(record)
    await session.flush()

    service = RecordService(
        record_id=record.id,
        service_id=cfg.service_id,
        title="Smoke service",
        amount=1,
        cost_to_pay=Decimal("10.00"),
        raw={},
    )
    session.add(service)
    await session.flush()

    job = MessageJob(
        company_id=cfg.company_id,
        record_id=record.id,
        client_id=client.id,
        job_type=cfg.template_code,
        run_at=utcnow() - timedelta(minutes=1),
        status="queued",
        attempts=0,
        max_attempts=5,
        last_error=None,
        dedupe_key=os.urandom(12).hex(),
        payload={},
    )
    session.add(job)
    await session.flush()

    await session.commit()
    return int(job.id)


async def _run_worker_once(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    provider: Any,
    limit: int,
    job_id: int | None,
    company_id: int | None = None,
) -> int:
    from sqlalchemy import func, select

    async with session_maker() as session:
        if job_id is not None:
            await ow.process_job_in_session(session, job_id, provider=provider)
            await session.commit()
            return 1

        stmt = (
            select(MessageJob.id)
            .where(MessageJob.status == "queued")
            .where(MessageJob.run_at <= func.now())
            .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
            .limit(limit)
        )
        if company_id is not None:
            stmt = stmt.where(MessageJob.company_id == company_id)

        res = await session.execute(stmt)
        ids = list(res.scalars().all())

        for _id in ids:
            await ow.process_job_in_session(session, int(_id), provider=provider)

        await session.commit()
        return len(ids)



async def _print_job_state(
    session: AsyncSession,
    job_id: int,
) -> None:
    job = await ow._load_job(session, job_id)  # noqa: SLF001
    print(f"job_id={job.id} status={job.status} attempts={job.attempts}")
    print(f"job_error={job.last_error!r}")

    stmt = (
        select(ow.OutboxMessage)
        .where(ow.OutboxMessage.job_id == job.id)
        .order_by(ow.OutboxMessage.id.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    out = res.scalar_one_or_none()
    if out is None:
        print("outbox: <none>")
        return

    print(
        "outbox: "
        f"id={out.id} status={out.status} msg_id={out.provider_message_id}"
    )
    print(f"outbox_error={out.error!r}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="smoke_outbox",
        description="Seed fixtures and/or run outbox worker once.",
    )
    parser.add_argument("--seed-only", action="store_true")
    parser.add_argument("--run-worker-once", action="store_true")
    parser.add_argument("--job-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--no-fix-sequences", action="store_true")

    parser.add_argument("--company-id", type=int, default=758285)
    parser.add_argument("--service-id", type=int, default=1)
    parser.add_argument("--sender-code", type=str, default="default")
    parser.add_argument("--template-code", type=str, default="record_updated")
    parser.add_argument("--language", type=str, default="de")

    parser.add_argument("--phone", type=str, default="+491234567890")
    parser.add_argument("--name", type=str, default="Anna")
    parser.add_argument("--staff", type=str, default="Tanja")

    return parser.parse_args()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()

    cfg = SmokeConfig(
        company_id=args.company_id,
        service_id=args.service_id,
        sender_code=args.sender_code,
        template_code=args.template_code,
        language=args.language,
        phone_e164=args.phone,
        display_name=args.name,
        staff_name=args.staff,
    )
    session_maker = _make_sessionmaker()
    provider = SmokeProvider()

    if args.run_worker_once:
        count = await _run_worker_once(
            session_maker,
            provider=provider,
            limit=args.limit,
            job_id=args.job_id,
            company_id=cfg.company_id,
        )
        print(f"processed_jobs={count}")
        return

    async with session_maker() as session:
        job_id = await _create_fixtures(
            session,
            cfg,
            fix_sequences=not args.no_fix_sequences,
        )
        print(f"seed_job_id={job_id}")

    if args.seed_only:
        return

    await _run_worker_once(
        session_maker,
        provider=provider,
        limit=1,
        job_id=job_id,
        company_id=cfg.company_id,
    )

    async with session_maker() as session:
        await _print_job_state(session, job_id)


if __name__ == "__main__":
    asyncio.run(main())
