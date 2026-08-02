"""EasyWeek inbox worker — captured deliveries → provider-scoped domain rows.

Deliberately a SEPARATE worker from the Altegio inbox worker: the live Altegio
path is not touched by PR-4 at all.

Three independent gates, all fail-closed:

``EASYWEEK_ENABLED``
    Only the public capture endpoint. Untouched here — turning processing off
    must never stop capture.
``EASYWEEK_PROCESSING_ENABLED``
    Whether this worker claims anything at all. Production deploys PR-4 with it
    OFF so the existing captured backlog is not swept up automatically.
``EASYWEEK_NOTIFICATIONS_ENABLED``
    Whether normalisation may create queue-consumable EasyWeek ``MessageJob``
    rows. With it off, Client/Record are still kept current.

Transaction contract, per event:

* one ``captured`` row is claimed with ``FOR UPDATE SKIP LOCKED``, ONE at a
  time, so a rollback can never strand a sibling event;
* ``captured -> processing -> domain writes -> processed`` all happen inside a
  single transaction, so a crash mid-flight leaves nothing committed as
  ``processing`` — the row is still ``captured`` and will be retried;
* a deterministic validation failure is committed as ``failed`` plus a safe
  ``error_code``, in its own transaction, with no domain writes;
* a transient/unexpected failure rolls the whole transaction back, so the row
  stays ``captured`` and is retried;
* SIGTERM stops the worker from claiming the NEXT event but never interrupts a
  transaction already in flight.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import SessionLocal
from ..easyweek_normalizer import (
    CREATE,
    DELETE,
    UPDATE,
    NormalizationError,
    NormalizedBooking,
    easyweek_job_dedupe_key,
    normalize_event,
)
from ..models.models import Client, EasyWeekEvent, MessageJob, Record
from ..settings import settings

logger = logging.getLogger("easyweek_inbox_worker")

PROVIDER = "easyweek"

RECORD_CREATED = "record_created"
RECORD_UPDATED = "record_updated"
RECORD_CANCELED = "record_canceled"

# The ONLY job types PR-4 may plan. Reminders, review, repeat, comeback, promo,
# campaign and follow-up jobs are explicitly out of scope (phase 2).
EASYWEEK_LIFECYCLE_JOB_TYPES = (RECORD_CREATED, RECORD_UPDATED, RECORD_CANCELED)

_ACTION_TO_JOB_TYPE = {
    CREATE: RECORD_CREATED,
    UPDATE: RECORD_UPDATED,
    DELETE: RECORD_CANCELED,
}

STATUS_CAPTURED = "captured"
STATUS_PROCESSING = "processing"
STATUS_PROCESSED = "processed"
STATUS_FAILED = "failed"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def processing_is_configured() -> bool:
    """True when this worker is allowed to claim events at all.

    Processing enabled but no location configured is treated as NOT configured:
    without the numeric location id the worker cannot tell its own location from
    a foreign one, and guessing would be exactly the cross-location leak the
    plan forbids.
    """
    return bool(settings.easyweek_processing_enabled) and int(settings.easyweek_location_id or 0) > 0


async def claim_next_event(session: AsyncSession) -> EasyWeekEvent | None:
    """Claim exactly ONE captured row, oldest first.

    One row per transaction on purpose: a batch would make the rollback story
    ambiguous, because one poisoned row would drag its siblings back to
    ``captured`` along with it.
    """
    stmt = (
        select(EasyWeekEvent)
        .where(EasyWeekEvent.status == STATUS_CAPTURED)
        .order_by(EasyWeekEvent.received_at.asc(), EasyWeekEvent.id.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    event = (await session.execute(stmt)).scalars().first()
    if event is None:
        return None
    event.status = STATUS_PROCESSING
    return event


async def upsert_client(session: AsyncSession, booking: NormalizedBooking) -> Client | None:
    """Provider-scoped Client upsert. Returns None when there is no customer id.

    The conflict target is the provider-scoped constraint, so an EasyWeek
    customer whose numeric id collides with an Altegio client id can never
    overwrite the Altegio row.
    """
    if booking.customer_id is None:
        return None

    values: dict[str, Any] = {
        "provider": PROVIDER,
        "company_id": booking.company_id,
        "altegio_client_id": booking.customer_id,
        "phone_e164": booking.phone_e164,
        "display_name": booking.display_name,
        "email": booking.email,
    }
    stmt = pg_insert(Client).values(**values)
    # Only overwrite contact fields with a value we actually received: a
    # delivery that omits the phone must not blank an already known one.
    update_set = {
        "phone_e164": stmt.excluded.phone_e164,
        "display_name": stmt.excluded.display_name,
        "email": stmt.excluded.email,
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_clients_provider_company_altegio_id",
        set_=update_set,
    ).returning(Client.id)
    client_id = (await session.execute(stmt)).scalar_one()

    return (await session.execute(select(Client).where(Client.id == client_id))).scalars().one()


async def upsert_record(
    session: AsyncSession,
    booking: NormalizedBooking,
    client: Client | None,
) -> Record:
    """UUID-first, provider-scoped Record upsert.

    The booking UUID — not the numeric id — is the authoritative identity, so
    the lookup is by UUID first. That is what keeps create/update/rescheduled/
    cancel collapsing onto ONE row, and what makes a numeric-id collision with
    an Altegio record harmless.
    """
    existing = (
        (
            await session.execute(
                select(Record)
                .where(Record.provider == PROVIDER)
                .where(Record.easyweek_booking_uuid == booking.booking_uuid)
                .with_for_update()
            )
        )
        .scalars()
        .first()
    )

    if existing is None:
        # No UUID row yet: fall back to the provider-scoped numeric identity so
        # a row created before the UUID was known is adopted rather than
        # duplicated.
        existing = (
            (
                await session.execute(
                    select(Record)
                    .where(Record.provider == PROVIDER)
                    .where(Record.company_id == booking.company_id)
                    .where(Record.altegio_record_id == booking.booking_id)
                    .with_for_update()
                )
            )
            .scalars()
            .first()
        )

    record = existing
    if record is None:
        record = Record(
            provider=PROVIDER,
            company_id=booking.company_id,
            altegio_record_id=booking.booking_id,
            easyweek_booking_uuid=booking.booking_uuid,
        )
        session.add(record)

    record.easyweek_booking_uuid = booking.booking_uuid
    record.altegio_record_id = booking.booking_id
    record.altegio_client_id = booking.customer_id
    record.client_id = client.id if client is not None else None
    record.starts_at = booking.starts_at
    record.ends_at = booking.ends_at
    record.duration_sec = booking.duration_sec
    record.staff_name = booking.staff_name
    record.comment = booking.comment
    record.last_change_at = utcnow()
    record.is_deleted = booking.action == DELETE

    _apply_manage_link(record, booking)
    return record


def _apply_manage_link(record: Record, booking: NormalizedBooking) -> None:
    """Fail-closed manage link, per INTEGRATION_PLAN §1.6.3.

    * proven pair            -> store URL + hash;
    * fields present but bad -> CLEAR, so a stale link can never be presented
                                alongside an unproven new hash;
    * fields absent entirely -> keep whatever was already proven.
    """
    if booking.manage_link is not None:
        record.short_link = booking.manage_link.url
        record.easyweek_booking_hash_id = booking.manage_link.hash_id
        return
    if booking.manage_link_present:
        record.short_link = None
        record.easyweek_booking_hash_id = None


async def plan_lifecycle_job(
    session: AsyncSession,
    *,
    booking: NormalizedBooking,
    record: Record,
    client: Client | None,
    event_hint: str,
    payload_hash: str | None,
) -> None:
    """Create the single lifecycle job for this event, if notifications are on.

    A minimal EasyWeek-only planner rather than a call into
    ``plan_jobs_for_record_event``: that one carries Altegio-only side effects
    (visit counting, review/repeat/comeback marketing, service filtering) and
    its Altegio semantics — job types and dedupe key format — must stay
    byte-for-byte unchanged.
    """
    if not settings.easyweek_notifications_enabled:
        return

    job_type = _ACTION_TO_JOB_TYPE[booking.action]
    if job_type not in EASYWEEK_LIFECYCLE_JOB_TYPES:  # pragma: no cover - defensive
        return

    dedupe_key = easyweek_job_dedupe_key(
        event_hint=event_hint,
        booking_uuid=booking.booking_uuid,
        payload_hash=payload_hash,
        job_type=job_type,
    )

    stmt = pg_insert(MessageJob).values(
        provider=PROVIDER,
        company_id=booking.company_id,
        record_id=record.id,
        client_id=client.id if client is not None else None,
        job_type=job_type,
        run_at=utcnow(),
        status="queued",
        dedupe_key=dedupe_key,
        payload={
            "provider": PROVIDER,
            "booking_uuid": str(booking.booking_uuid),
            "event_hint": event_hint,
        },
    )
    # A Resend of the same delivery produces the same key; do nothing rather
    # than requeue, so the duplicate can never become a second notification.
    stmt = stmt.on_conflict_do_nothing(index_elements=[MessageJob.dedupe_key])
    await session.execute(stmt)


async def apply_booking(
    session: AsyncSession,
    booking: NormalizedBooking,
    *,
    event_hint: str,
    payload_hash: str | None,
) -> Record:
    client = await upsert_client(session, booking)
    record = await upsert_record(session, booking, client)
    # Flush so the record has its primary key before the job references it.
    await session.flush()
    await plan_lifecycle_job(
        session,
        booking=booking,
        record=record,
        client=client,
        event_hint=event_hint,
        payload_hash=payload_hash,
    )
    return record


async def process_claimed_event(session: AsyncSession, event: EasyWeekEvent) -> None:
    """Normalise and apply one already-claimed event.

    Runs inside the caller's transaction: the claim, the domain writes and the
    terminal status all commit together, or none of them do. Raises
    :class:`NormalizationError` for deterministic rejections.
    """
    event_id = event.id
    event_hint = event.event_hint
    payload_hash = event.payload_hash

    booking = normalize_event(
        event_hint=event_hint,
        payload=event.payload,
        body_truncated=bool(event.body_truncated),
        expected_location_id=int(settings.easyweek_location_id or 0),
    )

    if booking is None:
        # booking-succeeded: terminal, no Client/Record/Job side effects.
        event.status = STATUS_PROCESSED
        event.processed_at = utcnow()
        event.error_code = None
        logger.info("easyweek event=%s hint=%s ignored (no side effects)", event_id, event_hint)
        return

    await apply_booking(
        session,
        booking,
        event_hint=str(event_hint),
        payload_hash=payload_hash,
    )

    event.status = STATUS_PROCESSED
    event.processed_at = utcnow()
    event.error_code = None
    # Safe metadata only: ids and the action. No phone, e-mail, name or payload.
    logger.info(
        "easyweek event=%s hint=%s action=%s booking_uuid=%s processed",
        event_id,
        event_hint,
        booking.action,
        booking.booking_uuid,
    )


async def _fail_event(event_id: int, code: str) -> None:
    """Commit a deterministic rejection in its own transaction."""
    async with SessionLocal() as session:
        async with session.begin():
            event = (
                (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id).with_for_update()))
                .scalars()
                .first()
            )
            if event is None:  # pragma: no cover - defensive
                return
            event.status = STATUS_FAILED
            event.processed_at = utcnow()
            event.error_code = code
    logger.warning("easyweek event=%s failed code=%s", event_id, code)


class _Rollback(Exception):
    """Internal signal: abandon the transaction without swallowing real errors."""


async def process_one() -> bool:
    """One full claim/process cycle. Returns False when there was nothing to do.

    A deterministic rejection rolls the whole transaction back — so no partial
    domain write can survive — and is then committed as ``failed`` on its own.
    A transient/unexpected failure propagates with the transaction rolled back,
    leaving the row ``captured`` for another attempt.
    """
    failed: tuple[int, str] | None = None
    try:
        async with SessionLocal() as session:
            async with session.begin():
                event = await claim_next_event(session)
                if event is None:
                    return False
                event_id = int(event.id)
                try:
                    await process_claimed_event(session, event)
                except NormalizationError as exc:
                    failed = (event_id, exc.code)
                    raise _Rollback from exc
                return True
    except _Rollback:
        pass

    if failed is not None:
        await _fail_event(*failed)
        return True
    return False


async def _sleep_unless_stopping(delay: float, stop_event: asyncio.Event | None) -> None:
    if stop_event is None:
        await asyncio.sleep(delay)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        pass


async def run_loop(
    poll_sec: float | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Poll for captured events until asked to stop.

    When processing is disabled the loop stays alive and simply sleeps: the
    container must remain healthy and quiet, not restart-loop, not busy-loop and
    not spam the log. The disabled state is announced once.
    """
    effective_poll_sec = poll_sec if poll_sec is not None else settings.easyweek_inbox_worker_poll_sec
    announced_disabled = False

    logger.info(
        "EasyWeek inbox worker started. processing=%s notifications=%s poll=%ss",
        bool(settings.easyweek_processing_enabled),
        bool(settings.easyweek_notifications_enabled),
        effective_poll_sec,
    )

    while True:
        # Checked here and only here: past this point the claimed event is ours.
        if stop_event is not None and stop_event.is_set():
            logger.info("EasyWeek inbox worker shutdown requested; not claiming a new event.")
            break

        if not processing_is_configured():
            if not announced_disabled:
                logger.info(
                    "EasyWeek processing is disabled or unconfigured; not claiming events. "
                    "processing_enabled=%s location_configured=%s",
                    bool(settings.easyweek_processing_enabled),
                    int(settings.easyweek_location_id or 0) > 0,
                )
                announced_disabled = True
            await _sleep_unless_stopping(effective_poll_sec, stop_event)
            continue

        announced_disabled = False
        did_work = await process_one()
        if not did_work:
            await _sleep_unless_stopping(effective_poll_sec, stop_event)

    logger.info("EasyWeek inbox worker stopped cleanly.")


def _install_stop_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signal_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover - platform dependent
            logger.warning("Cannot install %s handler; graceful drain unavailable.", signal_name)


async def _run_with_graceful_shutdown() -> None:
    stop_event = asyncio.Event()
    _install_stop_handlers(stop_event)
    await run_loop(stop_event=stop_event)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(_run_with_graceful_shutdown())


__all__ = [
    "EASYWEEK_LIFECYCLE_JOB_TYPES",
    "PROVIDER",
    "apply_booking",
    "claim_next_event",
    "main",
    "process_claimed_event",
    "process_one",
    "processing_is_configured",
    "run_loop",
    "upsert_client",
    "upsert_record",
]

if __name__ == "__main__":
    main()
