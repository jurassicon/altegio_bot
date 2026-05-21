from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_records import (
    AltegioRecordResearchError,
    extract_booking_created_at_from_record_details,
    fetch_record_details_for_booking_created_at,
)
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import plan_jobs_for_record_event
from altegio_bot.models.models import AltegioEvent, Client, Record, RecordService
from altegio_bot.perf import perf_log
from altegio_bot.promo_discount_apply import should_suppress_promo_origin_record_update, try_apply_promo_discount
from altegio_bot.service_filter import record_has_allowed_service
from altegio_bot.settings import settings

logger = logging.getLogger("inbox_worker")
TZ = ZoneInfo("Europe/Belgrade")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_phone(raw: str | None) -> str | None:
    """Normalize any phone string to E.164 (+digits). Returns None if empty."""
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    return f"+{digits}"


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    v = value.strip()

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
        # Altegio sends local time without timezone info (Europe/Belgrade).
        # Using .replace(tzinfo=TZ) alone can produce the wrong UTC offset during
        # DST transitions because fold=0 is assumed by default.
        # Normalising via UTC forces Python to resolve the correct DST-aware offset.
        dt = dt.replace(tzinfo=TZ).astimezone(timezone.utc).astimezone(TZ)

    return dt


def _normalize_event_status(value: str | None) -> str | None:
    if value is None:
        return None

    v = str(value).strip().lower()
    if v in ("delete", "deleted", "cancel", "canceled", "record_canceled"):
        return "delete"

    if v in ("create", "created", "record_created"):
        return "create"

    if v in ("update", "updated", "record_updated"):
        return "update"

    return None


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _resolve_source_cancelled_at(
    event: AltegioEvent,
    payload: dict[str, Any],
    event_status: str | None,
) -> datetime | None:
    if _normalize_event_status(event_status) != "delete":
        return None

    data = payload.get("data") or {}
    last_change_at = parse_dt(data.get("last_change_date"))
    if last_change_at is not None:
        return _as_utc(last_change_at)

    received_at = getattr(event, "received_at", None)
    if isinstance(received_at, datetime):
        return _as_utc(received_at)

    return utcnow()


def _parse_starts_at(record_data: dict[str, Any]) -> datetime | None:
    """Return starts_at from record_data, preferring the ``date`` field.

    Altegio's ``datetime`` field contains a **wrong** UTC offset (e.g. +01:00
    in summer when Europe/Belgrade is actually UTC+2 / CEST).  We therefore:
      1. Prefer the ``date`` field — correct naïve local wall-clock time.
      2. Fall back to ``datetime`` but **strip the offset** (first 19 chars only).
    In both cases we stamp with the correct local timezone so Python resolves
    the right DST offset automatically, then convert to UTC.
    """
    raw_date_str = record_data.get("date")
    if raw_date_str:
        try:
            naive_dt = datetime.fromisoformat(raw_date_str.strip().replace(" ", "T"))
            return naive_dt.replace(tzinfo=TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    dt_str = record_data.get("datetime")
    if dt_str and len(dt_str) >= 19:
        try:
            naive_dt = datetime.fromisoformat(dt_str[:19])
            return naive_dt.replace(tzinfo=TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    return None


def extract_booking_created_at(record_data: dict[str, Any]) -> datetime | None:
    """Return the confirmed booking creation timestamp from an Altegio record payload.

    Only dedicated creation timestamp fields are accepted:
    - ``create_date``
    - ``created_at``
    - ``datetime_created``

    ``date`` / ``datetime`` are appointment start fields, and
    ``last_change_date`` is a mutation timestamp; they are deliberately ignored.
    The webhook ``received_at`` audit timestamp is also never used here.
    """
    return extract_booking_created_at_from_record_details(record_data)


def _resolve_location_id_for_booking_created_at(
    *,
    company_id: int,
    record_data: dict[str, Any],
) -> int | None:
    for field in ("location_id", "salon_id"):
        value = record_data.get(field)
        if value is None:
            continue
        try:
            location_id = int(value)
        except (TypeError, ValueError):
            logger.warning("booking_created_at: invalid %s=%r in record payload", field, value)
            return None
        if location_id > 0:
            return location_id

    try:
        location_map = json.loads(settings.promo_location_id_by_company or "{}")
    except json.JSONDecodeError as exc:
        logger.warning("booking_created_at: invalid promo_location_id_by_company JSON: %s", exc)
        return None

    if not isinstance(location_map, dict):
        logger.warning("booking_created_at: invalid promo_location_id_by_company JSON: expected object")
        return None

    raw_location_id = location_map.get(str(company_id))
    if raw_location_id is None:
        logger.info("booking_created_at: no location_id mapping for company_id=%s", company_id)
        return None

    try:
        location_id = int(raw_location_id)
    except (TypeError, ValueError):
        logger.warning(
            "booking_created_at: invalid location_id mapping for company_id=%s: %r",
            company_id,
            raw_location_id,
        )
        return None

    return location_id if location_id > 0 else None


async def resolve_booking_created_at_for_record_create(
    *,
    company_id: int,
    record_data: dict[str, Any],
    record: Record,
) -> datetime | None:
    """Resolve booking creation time for a record-create webhook.

    Source order:
    1. Dedicated creation field in the webhook payload.
    2. Read-only Altegio GET /record/{location_id}/{record_id}, parsed from the
       same trusted creation fields.

    Returns None on any uncertainty so promo apply remains fail-closed. This
    helper never falls back to webhook received_at.
    """
    from_payload = extract_booking_created_at(record_data)
    if from_payload is not None:
        return from_payload

    location_id = _resolve_location_id_for_booking_created_at(
        company_id=company_id,
        record_data=record_data,
    )
    if location_id is None:
        return None

    raw_record_id = record.altegio_record_id or record_data.get("id")
    try:
        altegio_record_id = int(raw_record_id)
    except (TypeError, ValueError):
        logger.warning("booking_created_at: invalid altegio_record_id=%r", raw_record_id)
        return None

    try:
        details = await fetch_record_details_for_booking_created_at(
            location_id=location_id,
            record_id=altegio_record_id,
        )
    except AltegioRecordResearchError as exc:
        logger.warning(
            "booking_created_at: GET /record failed location_id=%s record_id=%s: %s",
            location_id,
            altegio_record_id,
            exc,
        )
        return None

    return extract_booking_created_at(details)


def sum_total_cost(services: list[dict[str, Any]]) -> Decimal | None:
    total = Decimal("0")
    any_found = False

    for svc in services:
        cost = svc.get("cost_to_pay")
        if cost is None:
            continue

        amount = svc.get("amount") or 1
        any_found = True
        total += Decimal(str(cost)) * Decimal(str(amount))

    return total if any_found else None


async def _load_existing_record_and_services(
    session: AsyncSession,
    company_id: int,
    altegio_record_id: int,
) -> tuple[Record | None, list[RecordService]]:
    """Load existing Record + services before upsert (for no-op detection)."""
    res = await session.execute(
        select(Record).where(Record.company_id == company_id).where(Record.altegio_record_id == altegio_record_id)
    )
    rec = res.scalar_one_or_none()
    if rec is None:
        return None, []
    svcs_res = await session.execute(select(RecordService).where(RecordService.record_id == rec.id))
    return rec, list(svcs_res.scalars().all())


def _is_noop_update(
    existing_record: Record,
    existing_services: list[RecordService],
    record_data: dict[str, Any],
) -> bool:
    """Return True when the incoming update payload is client-visibly identical.

    Conservative: returns False (process normally) if services are absent
    from the payload or when any visible field differs.
    """
    services_raw = record_data.get("services")
    if services_raw is None:
        return False

    # starts_at
    incoming_starts_at = _parse_starts_at(record_data)
    if (incoming_starts_at is None) != (existing_record.starts_at is None):
        return False
    if incoming_starts_at is not None and existing_record.starts_at is not None:
        inc = incoming_starts_at.replace(microsecond=0)
        exi = _as_utc(existing_record.starts_at).replace(microsecond=0)
        if inc != exi:
            return False

    # staff_id
    staff_data = record_data.get("staff") or {}
    raw_staff_id = record_data.get("staff_id") or staff_data.get("id")
    staff_id_val = int(raw_staff_id) if raw_staff_id is not None else None
    if staff_id_val != existing_record.staff_id:
        return False

    # staff_name
    if staff_data.get("name") != existing_record.staff_name:
        return False

    # short_link
    if record_data.get("short_link") != existing_record.short_link:
        return False

    # services (sorted by service_id for stable comparison)
    inc_svcs = sorted(
        [
            (
                int(s["id"]),
                s.get("title"),
                s.get("amount"),
                (Decimal(str(s["cost_to_pay"])) if s.get("cost_to_pay") is not None else None),
            )
            for s in services_raw
            if s.get("id") is not None
        ],
        key=lambda x: x[0],
    )
    exi_svcs = sorted(
        [(s.service_id, s.title, s.amount, s.cost_to_pay) for s in existing_services],
        key=lambda x: x[0],
    )
    if inc_svcs != exi_svcs:
        return False

    # total_cost
    incoming_total = sum_total_cost(services_raw)
    if incoming_total != existing_record.total_cost:
        return False

    return True


async def upsert_client(
    session: AsyncSession,
    company_id: int,
    client_data: dict[str, Any],
) -> int:
    altegio_client_id = client_data.get("id")
    if altegio_client_id is None:
        raise ValueError("client.id missing in payload")

    display_name = client_data.get("display_name") or client_data.get("name")
    phone = _normalize_phone(client_data.get("phone"))

    stmt = (
        insert(Client)
        .values(
            company_id=int(company_id),
            altegio_client_id=int(altegio_client_id),
            phone_e164=phone,
            display_name=display_name,
            email=client_data.get("email"),
            raw=client_data,
        )
        .on_conflict_do_update(
            constraint="uq_clients_company_altegio_id",
            set_={
                "phone_e164": phone,
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

    starts_at = _parse_starts_at(record_data)

    duration_sec = record_data.get("seance_length") or record_data.get("length")
    duration_sec = int(duration_sec) if duration_sec is not None else None

    ends_at = None
    if starts_at and duration_sec:
        ends_at = starts_at + timedelta(seconds=duration_sec)

    services = record_data.get("services") or []
    total_cost = sum_total_cost(services)

    is_deleted = bool(record_data.get("deleted"))
    if payload_event_status == "delete":
        is_deleted = True

    last_change_at = parse_dt(record_data.get("last_change_date"))

    staff_id = record_data.get("staff_id") or staff_data.get("id")
    staff_id_val = int(staff_id) if staff_id is not None else None
    staff_name = staff_data.get("name")

    stmt = (
        insert(Record)
        .values(
            company_id=int(company_id),
            altegio_record_id=int(altegio_record_id),
            client_id=client_pk,
            altegio_client_id=client_data.get("id"),
            staff_id=staff_id_val,
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
                "staff_id": staff_id_val,
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


async def replace_record_services(
    session: AsyncSession,
    record_pk: int,
    services: list[dict[str, Any]] | None,
) -> None:
    # Если services вообще не пришли в вебхуке — ничего не трогаем.
    if services is None:
        return

    # Если пришёл пустой список — значит сервисов реально нет -> очищаем.
    await session.execute(delete(RecordService).where(RecordService.record_id == record_pk))

    if not services:
        return

    rows: list[dict[str, Any]] = []
    for svc in services:
        sid = svc.get("id")
        if sid is None:
            continue

        cost_to_pay = svc.get("cost_to_pay")
        cost_val = Decimal(str(cost_to_pay)) if cost_to_pay is not None else None

        rows.append(
            {
                "record_id": record_pk,
                "service_id": int(sid),
                "title": svc.get("title"),
                "amount": svc.get("amount"),
                "cost_to_pay": cost_val,
                "raw": svc,
            }
        )

    await session.execute(insert(RecordService), rows)


async def lock_next_batch(
    session: AsyncSession,
    batch_size: int,
) -> Sequence[AltegioEvent]:
    stmt = (
        select(AltegioEvent)
        .where(AltegioEvent.status == "received")
        .order_by(AltegioEvent.received_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    events = list(res.scalars().all())

    for event in events:
        event.status = "processing"

    return events


async def handle_event(session: AsyncSession, event: AltegioEvent) -> None:
    payload = event.payload or {}

    company_id = event.company_id or payload.get("company_id")
    resource = event.resource or payload.get("resource")
    data = payload.get("data") or {}
    event_status = event.event_status or payload.get("status")

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

        # No-op update detection: load existing record before upsert mutates DB.
        _before_rec: Record | None = None
        _before_svcs: list[RecordService] = []
        _raw_altegio_id = data.get("id")
        if _normalize_event_status(event_status) == "update" and _raw_altegio_id is not None:
            _before_rec, _before_svcs = await _load_existing_record_and_services(
                session,
                int(company_id),
                int(_raw_altegio_id),
            )

        record_pk = await upsert_record(
            session=session,
            company_id=int(company_id),
            payload_event_status=event_status,
            record_data=data,
            client_pk=client_pk,
        )

        services_payload = data.get("services")
        await replace_record_services(session, record_pk, services_payload)

        # Пропускаем события смены статуса визита (visit_attendance).
        # Альтеджио присылает update с visit_attendance != 0 когда клиент отмечен как
        # пришедший (1) или не пришедший (-1). Такие события не требуют создания job'ов.
        if event_status == "update":
            visit_attendance = data.get("visit_attendance")
            if visit_attendance is not None and int(visit_attendance) != 0:
                logger.info(
                    "Skip visit_attendance change: record_id=%s visit_attendance=%s",
                    record_pk,
                    visit_attendance,
                )
                return

        record_obj = await session.get(Record, record_pk)

        if record_obj is not None and event_status is not None:
            # Promo discount apply runs on create events only. Update webhooks are
            # intentionally skipped to avoid applying promo to bookings that existed
            # before the promo was issued.
            normalized_status = _normalize_event_status(event_status)
            if normalized_status == "create" and not record_obj.is_deleted:
                booking_created_at_resolver = None
                if settings.promo_apply_discount_enabled:
                    company_id_int = int(company_id)

                    async def booking_created_at_resolver(
                        *,
                        _company_id: int = company_id_int,
                        _record_data: dict[str, Any] = data,
                        _record: Record = record_obj,
                    ) -> datetime | None:
                        return await resolve_booking_created_at_for_record_create(
                            company_id=_company_id,
                            record_data=_record_data,
                            record=_record,
                        )

                await try_apply_promo_discount(
                    session,
                    record_obj,
                    int(company_id),
                    booking_created_at_resolver=booking_created_at_resolver,
                )

            # Suppress plan_jobs for record_updated webhooks triggered by our own
            # promo price-override PUT. The suppression applies only within a
            # 5-minute window after the PUT (promo_record_put_at in PromoLead.meta)
            # so that legitimate future edits to the same record are not silenced.
            if normalized_status == "update" and await should_suppress_promo_origin_record_update(
                session, record_obj, event
            ):
                logger.info(
                    "promo_discount: suppress plan_jobs for promo-origin record_updated record_id=%s",
                    record_obj.id,
                )
                return

            allowed = await record_has_allowed_service(
                session=session,
                company_id=int(record_obj.company_id),
                record_id=int(record_obj.id),
            )
            if not allowed:
                logger.info(
                    "Ignore record_id=%s company_id=%s (not lashes services)",
                    record_obj.id,
                    record_obj.company_id,
                )
                return

            if (
                normalized_status == "update"
                and _before_rec is not None
                and _is_noop_update(_before_rec, _before_svcs, data)
            ):
                logger.info(
                    "Skip no-op record update: record_id=%s",
                    record_pk,
                )
                return

            await plan_jobs_for_record_event(
                session=session,
                company_id=int(record_obj.company_id),
                record_id=int(record_obj.id),
                status=str(event_status),
                source_cancelled_at=_resolve_source_cancelled_at(event, payload, event_status),
            )

        return

    logger.info("skip resource=%s event=%s", resource, event.id)


async def process_one_event(event_id: int) -> None:
    with perf_log("inbox_worker", "process_event", event_id=event_id) as ctx:
        async with SessionLocal() as session:
            async with session.begin():
                stmt = select(AltegioEvent).where(AltegioEvent.id == event_id).with_for_update()
                res = await session.execute(stmt)
                event = res.scalar_one_or_none()
                if event is None:
                    return

                ctx.update(
                    company_id=event.company_id,
                    resource=event.resource,
                    resource_id=event.resource_id,
                )

                try:
                    await handle_event(session, event)
                    event.status = "processed"
                    event.processed_at = utcnow()
                    event.error = None
                except Exception as exc:
                    event.status = "failed"
                    event.processed_at = utcnow()
                    event.error = str(exc)
                    logger.exception("Event failed id=%s", event_id)

                ctx.update(outcome=event.status)


def _resolve_poll_sec(
    explicit: float | None,
    settings_value: float,
) -> float:
    return settings_value if explicit is None else explicit


async def run_loop(
    batch_size: int = 50,
    poll_sec: float | None = None,
) -> None:
    effective_poll_sec = _resolve_poll_sec(poll_sec, settings.inbox_worker_poll_sec)
    logger.info(
        "Inbox worker started. batch_size=%s poll=%ss",
        batch_size,
        effective_poll_sec,
    )

    while True:
        event_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                events = await lock_next_batch(session, batch_size)
                event_ids = [e.id for e in events]

        if not event_ids:
            await asyncio.sleep(effective_poll_sec)
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
