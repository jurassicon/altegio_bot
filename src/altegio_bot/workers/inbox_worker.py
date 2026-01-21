from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import plan_jobs_for_record_event
from altegio_bot.models.models import AltegioEvent, Client, Record, RecordService

logger = logging.getLogger("inbox_worker")
TZ = ZoneInfo("Europe/Belgrade")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    v = value.strip()

    # fix +0100 -> +01:00
    if len(v) >= 5 and v[-5] in "+-" and v[-3] != ":":
        v = v[:-2] + ":" + v[-2:]

    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        try:
            dt = datetime.fromisoformat(v.replace(" ", "T"))
        except ValueError:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)

    return dt


def sum_total_cost(services: list[dict[str, Any]]) -> Decimal | None:
    total = Decimal("0")
    any_found = False
    for s in services:
        cost = s.get("cost_to_pay")
        if cost is None:
            continue
        amount = s.get("amount") or 1
        any_found = True
        total += Decimal(str(cost)) * Decimal(str(amount))
    return total if any_found else None


async def upsert_client(session: AsyncSession, company_id: int, client_data: dict[str, Any]) -> int:
    altegio_client_id = client_data.get("id")
    if altegio_client_id is None:
        raise ValueError("client.id missing in payload")

    display_name = client_data.get("display_name") or client_data.get("name")

    stmt = (
        insert(Client)
        .values(
            company_id=int(company_id),
            altegio_client_id=int(altegio_client_id),
            phone_e164=client_data.get("phone"),
            display_name=display_name,
            email=client_data.get("email"),
            raw=client_data,
        )
        .on_conflict_do_update(
            constraint="uq_clients_company_altegio_id",
            set_={
                "phone_e164": client_data.get("phone"),
                "display_name": display_name,
                "email": client_data.get("email"),
                "raw": client_data,
            },
        )
        .returning(Client.id)
    )
    res = await session.execute(stmt)
    return int(res.scalar_one())


async def upsert_record(
    session: AsyncSession,
    company_id: int,
    payload_event_status: str | None,
    record_data: dict[str, Any],
    client_pk: int | None,
) -> int:
    altegio_record_id = record_data.get("id")
    if altegio_record_id is None:
        raise ValueError("record.id missing in payload")

    client_data = record_data.get("client") or {}
    staff_data = record_data.get("staff") or {}

    starts_at = parse_dt(record_data.get("datetime")) or parse_dt(record_data.get("date"))
    duration_sec = record_data.get("seance_length") or record_data.get("length")
    duration_sec = int(duration_sec) if duration_sec is not None else None
    ends_at = (starts_at + timedelta(seconds=duration_sec)) if (starts_at and duration_sec) else None

    services = record_data.get("services") or []
    total_cost = sum_total_cost(services)

    is_deleted = bool(record_data.get("deleted")) or (payload_event_status == "delete")
    last_change_at = parse_dt(record_data.get("last_change_date"))

    staff_id = record_data.get("staff_id") or staff_data.get("id")
    staff_name = staff_data.get("name")

    stmt = (
        insert(Record)
        .values(
            company_id=int(company_id),
            altegio_record_id=int(altegio_record_id),
            client_id=client_pk,
            altegio_client_id=client_data.get("id"),
            staff_id=int(staff_id) if staff_id is not None else None,
            staff_name=staff_name,
            starts_at=starts_at,
            ends_at=ends_at,
            duration_sec=duration_sec,
            comment=record_data.get("comment"),
            short_link=record_data.get("short_link"),
            confirmed=record_data.get("confirmed"),
            attendance=record_data.get("attendance"),
            visit_attendance=record_data.get("visit_attendance"),
            is_deleted=is_deleted,
            total_cost=total_cost,
            last_change_at=last_change_at,
            raw=record_data,
        )
        .on_conflict_do_update(
            constraint="uq_records_company_altegio_id",
            set_={
                "client_id": client_pk,
                "altegio_client_id": client_data.get("id"),
                "staff_id": int(staff_id) if staff_id is not None else None,
                "staff_name": staff_name,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "duration_sec": duration_sec,
                "comment": record_data.get("comment"),
                "short_link": record_data.get("short_link"),
                "confirmed": record_data.get("confirmed"),
                "attendance": record_data.get("attendance"),
                "visit_attendance": record_data.get("visit_attendance"),
                "is_deleted": is_deleted,
                "total_cost": total_cost,
                "last_change_at": last_change_at,
                "raw": record_data,
            },
        )
        .returning(Record.id)
    )

    res = await session.execute(stmt)
    return int(res.scalar_one())


async def replace_record_services(session: AsyncSession, record_pk: int, services: list[dict[str, Any]]) -> None:
    await session.execute(delete(RecordService).where(RecordService.record_id == record_pk))

    if not services:
        return

    rows: list[dict[str, Any]] = []
    for s in services:
        sid = s.get("id")
        if sid is None:
            continue
        rows.append(
            {
                "record_id": record_pk,
                "service_id": int(sid),
                "title": s.get("title"),
                "amount": s.get("amount"),
                "cost_to_pay": Decimal(str(s.get("cost_to_pay"))) if s.get("cost_to_pay") is not None else None,
                "raw": s,
            }
        )

    await session.execute(insert(RecordService), rows)


async def lock_next_batch(session: AsyncSession, batch_size: int) -> Sequence[AltegioEvent]:
    stmt = (
        select(AltegioEvent)
        .where(AltegioEvent.status == "received")
        .order_by(AltegioEvent.received_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    events = list(res.scalars().all())

    for e in events:
        e.status = "processing"

    return events


async def handle_event(session: AsyncSession, event: AltegioEvent) -> None:
    payload = event.payload or {}
    company_id = event.company_id or payload.get("company_id")
    resource = event.resource or payload.get("resource")
    data = payload.get("data") or {}

    logger.info(
        "event=%s company=%s resource=%s resource_id=%s",
        event.id,
        company_id,
        resource,
        event.resource_id,
    )

    if not company_id:
        raise ValueError("company_id missing")

    if resource == "client":
        await upsert_client(session, int(company_id), data)
        return

    if resource == "record":
        client_data = data.get("client") or {}
        client_pk: int | None = None
        if client_data.get("id") is not None:
            client_pk = await upsert_client(session, int(company_id), client_data)

        record_pk = await upsert_record(
            session=session,
            company_id=int(company_id),
            payload_event_status=event.event_status,
            record_data=data,
            client_pk=client_pk,
        )

        await plan_jobs_for_record_event(
            session,
            record=record_obj,
            client=client_obj,
            event_status=event.event_status,
        )

        await replace_record_services(session, record_pk, data.get("services") or [])
        return

    logger.info("skip resource=%s event=%s", resource, event.id)


async def process_one_event(event_id: int) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            stmt = select(AltegioEvent).where(AltegioEvent.id == event_id).with_for_update()
            res = await session.execute(stmt)
            event = res.scalar_one_or_none()
            if event is None:
                return

            try:
                await handle_event(session, event)
                event.status = "processed"
                event.processed_at = utcnow()
                event.error = None
            except Exception as e:
                event.status = "failed"
                event.processed_at = utcnow()
                event.error = str(e)
                logger.exception("Event failed id=%s: %s", event_id, e)


async def run_loop(batch_size: int = 50, poll_interval_sec: float = 1.0) -> None:
    logger.info("Inbox worker started. batch_size=%s poll=%ss", batch_size, poll_interval_sec)

    while True:
        event_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                events = await lock_next_batch(session, batch_size)
                event_ids = [e.id for e in events]

        if not event_ids:
            await asyncio.sleep(poll_interval_sec)
            continue

        for eid in event_ids:
            await process_one_event(eid)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run_loop())


if __name__ == "__main__":
    main()
