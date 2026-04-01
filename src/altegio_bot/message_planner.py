from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, MessageJob, Record
from altegio_bot.utils import utcnow

RECORD_CREATED = "record_created"
RECORD_UPDATED = "record_updated"
RECORD_CANCELED = "record_canceled"

REMINDER_24H = "reminder_24h"
REMINDER_2H = "reminder_2h"

REVIEW_3D = "review_3d"
REPEAT_10D = "repeat_10d"
COMEBACK_3D = "comeback_3d"

SYSTEM_JOB_TYPES = (
    RECORD_CREATED,
    RECORD_UPDATED,
    RECORD_CANCELED,
    REMINDER_24H,
    REMINDER_2H,
    REVIEW_3D,
    REPEAT_10D,
    COMEBACK_3D,
)

MARKETING_JOB_TYPES = (
    REVIEW_3D,
    REPEAT_10D,
    COMEBACK_3D,
)

# Максимальное количество посещений клиента, при котором ещё отправляется запрос
# на отзыв. Клиентам с большим опытом (более MAX_VISITS_FOR_REVIEW визитов)
# сообщение с просьбой об отзыве не отправляется.
MAX_VISITS_FOR_REVIEW = 3


def _normalize_event_status(value: str | None) -> str | None:
    if value is None:
        return None

    v = str(value).strip().lower()
    if not v:
        return None

    if v in ("create", "created", "record_created"):
        return "create"

    if v in ("update", "updated", "record_updated"):
        return "update"

    if v in ("delete", "deleted", "cancel", "canceled", "record_canceled"):
        return "delete"

    return None


def _record_event_job_type(event_status: str) -> str:
    if event_status == "create":
        return RECORD_CREATED

    if event_status == "update":
        return RECORD_UPDATED

    return RECORD_CANCELED


def make_dedupe_key(
    *,
    job_type: str,
    company_id: int,
    record_id: int | None,
    run_at: datetime,
) -> str:
    rid = int(record_id) if record_id is not None else 0
    return f"{job_type}:{company_id}:{rid}:{run_at.isoformat()}"


async def cancel_queued_jobs(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
    reason: str,
) -> int:
    stmt = (
        update(MessageJob)
        .where(MessageJob.company_id == company_id)
        .where(MessageJob.record_id == record_id)
        .where(MessageJob.job_type.in_(SYSTEM_JOB_TYPES))
        .where(MessageJob.status == "queued")
        .values(
            status="canceled",
            updated_at=utcnow(),
            last_error=reason,
            locked_at=None,
        )
    )
    res = await session.execute(stmt)
    return int(getattr(res, "rowcount", 0) or 0)


async def add_job(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int | None,
    client_id: int | None,
    job_type: str,
    run_at: datetime,
    payload: dict[str, Any],
) -> None:
    dedupe_key = make_dedupe_key(
        job_type=job_type,
        company_id=company_id,
        record_id=record_id,
        run_at=run_at,
    )

    stmt = pg_insert(MessageJob).values(
        company_id=company_id,
        record_id=record_id,
        client_id=client_id,
        job_type=job_type,
        run_at=run_at,
        status="queued",
        last_error=None,
        dedupe_key=dedupe_key,
        payload=payload,
        locked_at=None,
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=[MessageJob.dedupe_key],
        set_={
            "status": "queued",
            "last_error": None,
            "locked_at": None,
            "payload": stmt.excluded.payload,
            "updated_at": utcnow(),
        },
        where=MessageJob.status.in_(("canceled", "failed")),
    )

    await session.execute(stmt)


async def _count_client_visits(
    session: AsyncSession,
    *,
    client_id: int,
    company_id: int,
) -> int:
    """Возвращает количество не удалённых записей (визитов) клиента в филиале."""
    stmt = (
        select(func.count())
        .select_from(Record)
        .where(Record.client_id == client_id)
        .where(Record.company_id == company_id)
        .where(Record.is_deleted.is_(False))
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def _load_record_and_client(
    session: AsyncSession,
    *,
    record: Record | None,
    record_id: int | None,
    client: Client | None,
    client_id: int | None,
) -> tuple[Record | None, Client | None]:
    rec = record
    if rec is None and record_id is not None:
        rec = await session.get(Record, record_id)

    cli = client
    if cli is None:
        cid = client_id
        if cid is None and rec is not None:
            cid = rec.client_id
        if cid is not None:
            cli = await session.get(Client, cid)

    return rec, cli


async def plan_jobs_for_record_event(
    session: AsyncSession,
    *,
    company_id: int | None = None,
    record_id: int | None = None,
    client_id: int | None = None,
    record: Record | None = None,
    client: Client | None = None,
    event_status: str | None = None,
    status: str | None = None,
    event_kind: str | None = None,
) -> None:
    norm_status = _normalize_event_status(event_status)
    if norm_status is None:
        norm_status = _normalize_event_status(status)
    if norm_status is None:
        norm_status = _normalize_event_status(event_kind)

    if norm_status is None:
        return

    record_obj, client_obj = await _load_record_and_client(
        session,
        record=record,
        record_id=record_id,
        client=client,
        client_id=client_id,
    )

    if record_obj is None:
        return

    cid = int(company_id) if company_id is not None else int(record_obj.company_id)

    now = utcnow().replace(microsecond=0)

    if norm_status in ("update", "delete"):
        reason = "Canceled: rescheduled"
        if norm_status == "delete":
            reason = "Canceled: record deleted"

        await cancel_queued_jobs(
            session,
            company_id=cid,
            record_id=int(record_obj.id),
            reason=reason,
        )

    job_type = _record_event_job_type(norm_status)
    await add_job(
        session,
        company_id=cid,
        record_id=int(record_obj.id),
        client_id=record_obj.client_id,
        job_type=job_type,
        run_at=now,
        payload={"kind": job_type},
    )

    starts_at = record_obj.starts_at

    if norm_status in ("create", "update") and starts_at is not None:
        run_at_24h = starts_at - timedelta(hours=24)
        if run_at_24h > now:
            await add_job(
                session,
                company_id=cid,
                record_id=int(record_obj.id),
                client_id=record_obj.client_id,
                job_type=REMINDER_24H,
                run_at=run_at_24h,
                payload={"kind": REMINDER_24H},
            )

        delta = starts_at - now
        if delta > timedelta(hours=2):
            run_at_2h = starts_at - timedelta(hours=2)
            if run_at_2h > now:
                await add_job(
                    session,
                    company_id=cid,
                    record_id=int(record_obj.id),
                    client_id=record_obj.client_id,
                    job_type=REMINDER_2H,
                    run_at=run_at_2h,
                    payload={"kind": REMINDER_2H},
                )

    opted_out = bool(getattr(client_obj, "wa_opted_out", False))

    if norm_status in ("create", "update") and not opted_out:
        if starts_at is not None:
            review_at = starts_at + timedelta(days=3)
            if review_at > now:
                # Отправляем запрос на отзыв только новым клиентам (не более MAX_VISITS_FOR_REVIEW визитов)
                is_new_visitor = True
                if record_obj.client_id is not None:
                    visit_count = await _count_client_visits(
                        session,
                        client_id=record_obj.client_id,
                        company_id=cid,
                    )
                    is_new_visitor = visit_count <= MAX_VISITS_FOR_REVIEW

                if is_new_visitor:
                    await add_job(
                        session,
                        company_id=cid,
                        record_id=int(record_obj.id),
                        client_id=record_obj.client_id,
                        job_type=REVIEW_3D,
                        run_at=review_at,
                        payload={"kind": REVIEW_3D},
                    )

            repeat_at = starts_at + timedelta(days=10)
            if repeat_at > now:
                await add_job(
                    session,
                    company_id=cid,
                    record_id=int(record_obj.id),
                    client_id=record_obj.client_id,
                    job_type=REPEAT_10D,
                    run_at=repeat_at,
                    payload={"kind": REPEAT_10D},
                )

    if norm_status == "delete" and not opted_out:
        already_queued = False

        # Дедупликация на входе: проверяем, нет ли уже такой задачи в очереди
        if record_obj.client_id is not None:
            stmt = (
                select(MessageJob.id)
                .where(MessageJob.company_id == cid)
                .where(MessageJob.client_id == record_obj.client_id)
                .where(MessageJob.job_type == COMEBACK_3D)
                .where(MessageJob.status == "queued")
                .limit(1)
            )
            res = await session.execute(stmt)
            if res.scalar_one_or_none() is not None:
                already_queued = True

        # Создаем задачу только если дубликатов нет
        if not already_queued:
            comeback_at = utcnow() + timedelta(days=3)
            await add_job(
                session,
                company_id=cid,
                record_id=int(record_obj.id),
                client_id=record_obj.client_id,
                job_type=COMEBACK_3D,
                run_at=comeback_at,
                payload={"kind": COMEBACK_3D},
            )
