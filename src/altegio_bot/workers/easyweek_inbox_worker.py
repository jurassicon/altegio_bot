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
* a deterministic validation failure rolls back to a SAVEPOINT — undoing the
  domain writes while KEEPING the claim and its row lock — and commits
  ``failed`` plus a safe ``error_code`` in the same transaction, so the row is
  never republished as ``captured`` in between;
* a transient/unexpected failure rolls the whole transaction back, so the row
  stays ``captured`` and is retried after a bounded backoff; its exception is
  logged as a class name and a fixed code only, never as text or a traceback;
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
from ..models.models import Client, EasyWeekEvent, MessageJob, Record, RecordService
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

# Upper bound on the transient-error backoff, so a wedged dependency costs
# one poll every 30s rather than a hot loop.
MAX_ERROR_BACKOFF_SEC = 30.0

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


async def already_applied(session: AsyncSession, event: EasyWeekEvent) -> bool:
    """True when an equivalent delivery already reached ``processed``.

    THE stale-replay guard. EasyWeek's Resend button re-delivers a byte-identical
    body, so a Resend of an old ``booking-created`` arrives AFTER the cancel and
    is therefore *newer* by arrival order. Arrival order alone can never tell the
    two apart — and the payload carries no per-delivery sequence or version
    field, only ``booking_created_at``, which is identical across the whole
    lifecycle of one booking.

    What DOES distinguish them is that a Resend is content-identical to a
    delivery we already applied. So the identity of a delivery is
    ``(booking uuid, exact event_hint, payload_hash)``; seeing it twice means
    replay, and replay must not touch the domain again.

    Limitation, stated plainly: this detects *exact* replays, which is what
    Resend produces. A hypothetical stale delivery whose body differs from the
    original is indistinguishable from a genuine later edit, and would be
    applied. The cancel-terminality rule in :func:`upsert_record` is the second
    line of defence for the case that actually matters.
    """
    if not event.payload_hash:
        # Non-JSON bodies have no hash; nothing to compare. Lifecycle events
        # always carry one, so this only affects deliveries we reject anyway.
        return False
    booking_uid = (event.payload or {}).get("uid")
    if not isinstance(booking_uid, str) or not booking_uid:
        return False

    stmt = (
        select(EasyWeekEvent.id)
        .where(EasyWeekEvent.id != event.id)
        .where(EasyWeekEvent.status == STATUS_PROCESSED)
        .where(EasyWeekEvent.event_hint == event.event_hint)
        .where(EasyWeekEvent.payload_hash == event.payload_hash)
        .where(EasyWeekEvent.payload["uid"].astext == booking_uid)
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().first() is not None


def _patch(target: Any, attribute: str, booking: NormalizedBooking, value: Any) -> None:
    """Assign only when the delivery actually carried the field.

    Patch semantics, chosen deliberately over a blanket COALESCE: ``present but
    empty`` stays authoritative (the salon really did clear the comment), while
    ``absent`` preserves what we already proved. ``booking-updated`` legitimately
    omits everything the salon did not touch.
    """
    if booking.carries(attribute):
        setattr(target, attribute, value)


async def upsert_client(session: AsyncSession, booking: NormalizedBooking) -> Client | None:
    """Provider-scoped Client upsert. Returns None when there is no customer id.

    The conflict target is the provider-scoped constraint, so an EasyWeek
    customer whose numeric id collides with an Altegio client id can never
    overwrite the Altegio row.
    """
    if booking.customer_id is None:
        return None

    existing = (
        (
            await session.execute(
                select(Client)
                .where(Client.provider == PROVIDER)
                .where(Client.company_id == booking.company_id)
                .where(Client.altegio_client_id == booking.customer_id)
                .with_for_update()
            )
        )
        .scalars()
        .first()
    )

    if existing is None:
        client = Client(
            provider=PROVIDER,
            company_id=booking.company_id,
            altegio_client_id=booking.customer_id,
            phone_e164=booking.phone_e164,
            display_name=booking.display_name,
            email=booking.email,
        )
        session.add(client)
        await session.flush()
        return client

    # Patch, never blanket overwrite: a cancel delivery that omits the e-mail
    # must not erase the address the create delivery proved.
    _patch(existing, "phone_e164", booking, booking.phone_e164)
    _patch(existing, "display_name", booking, booking.display_name)
    _patch(existing, "email", booking, booking.email)
    return existing


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
    record = (
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

    if record is None:
        # STRICTLY UUID-first. There is deliberately NO fallback lookup by
        # numeric booking id: the numeric id is an attribute, not an identity.
        # Adopting a row found by numeric id would let one booking seize the
        # row of another whenever EasyWeek reuses or collides that id, and
        # would then overwrite its UUID — silently destroying the authoritative
        # identity of a different booking.
        clash = (
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
        if clash is not None and clash.easyweek_booking_uuid is not None:
            # The numeric id already belongs to a DIFFERENT booking. Creating a
            # second row would violate the provider-scoped unique constraint
            # anyway; fail closed and leave the existing row untouched.
            raise NormalizationError(NormalizationError.IDENTITY_CONFLICT)

        record = clash
        if record is None:
            record = Record(
                provider=PROVIDER,
                company_id=booking.company_id,
                altegio_record_id=booking.booking_id,
                easyweek_booking_uuid=booking.booking_uuid,
            )
            session.add(record)
        else:
            # A row with this numeric id but NO UUID yet: it cannot belong to
            # another booking, so claiming it is safe.
            record.easyweek_booking_uuid = booking.booking_uuid

    record.altegio_record_id = booking.booking_id
    record.altegio_client_id = booking.customer_id
    record.client_id = client.id if client is not None else None
    record.last_change_at = utcnow()

    # Patch semantics: only fields this delivery carried are written.
    _patch(record, "starts_at", booking, booking.starts_at)
    _patch(record, "ends_at", booking, booking.ends_at)
    _patch(record, "duration_sec", booking, booking.duration_sec)
    _patch(record, "staff_name", booking, booking.staff_name)
    _patch(record, "comment", booking, booking.comment)
    _patch(record, "total_cost", booking, booking.total_cost)

    if booking.action == DELETE:
        record.is_deleted = True
    elif record.is_deleted:
        # Cancel is terminal. A `booking-created` for a booking we already know
        # to be cancelled can only be a replay of the original creation — the
        # real "un-cancel" path in EasyWeek is `booking-updated`. Resurrecting
        # here would revive a cancelled appointment and re-notify the customer.
        if booking.action != CREATE:
            record.is_deleted = False

    _apply_manage_link(record, booking)
    return record


async def sync_record_service(session: AsyncSession, record: Record, booking: NormalizedBooking) -> None:
    """Keep the booking's service row in step with the delivery.

    PR-5 renders templates from DOMAIN data, so the service and its price have
    to be persisted here or the lifecycle messages would carry an empty service
    and a 0.00 total.

    ``record_services`` is keyed ``(record_id, service_id)``. That is already
    provider-safe without a schema change: ``record_id`` points at a row whose
    ``provider`` is ``easyweek``, so an EasyWeek service id can never collide
    with an Altegio one — they hang off different records.

    A delivery that does not mention the service leaves the known service
    alone: the payload carries a single flat ``service_id``/``service_name``
    pair, not a list, so its absence is "unchanged", never "the booking now has
    no services".
    """
    if not booking.carries("service_id") or booking.service_id is None:
        return

    existing = list(
        (await session.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars()
    )

    for stale in existing:
        # The payload describes exactly one service; anything else attached to
        # this booking is from a previous, different service selection.
        if stale.service_id != booking.service_id:
            await session.delete(stale)

    current = next((row for row in existing if row.service_id == booking.service_id), None)
    if current is None:
        current = RecordService(record_id=record.id, service_id=booking.service_id)
        session.add(current)

    if booking.carries("service_name"):
        current.title = booking.service_name
    if booking.carries("service_quantity"):
        current.amount = booking.service_quantity
    if booking.carries("total_cost"):
        current.cost_to_pay = booking.total_cost


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
    # Flush so the record has its primary key before services and the job
    # reference it.
    await session.flush()
    await sync_record_service(session, record, booking)
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
        # booking-succeeded: terminal, no Client/Record/Job side effects. It
        # still passed truncation, payload and location isolation above.
        event.status = STATUS_PROCESSED
        event.processed_at = utcnow()
        event.error_code = None
        logger.info("easyweek event=%s hint=%s ignored (no side effects)", event_id, event_hint)
        return

    # Stale-replay guard, BEFORE any domain write. A Resend re-delivers a
    # byte-identical body, so without this an old `booking-created` replayed
    # after a cancel would un-delete the booking and restore its old times.
    if await already_applied(session, event):
        event.status = STATUS_PROCESSED
        event.processed_at = utcnow()
        event.error_code = None
        logger.info(
            "easyweek event=%s hint=%s booking_uuid=%s replay; no domain writes",
            event_id,
            event_hint,
            booking.booking_uuid,
        )
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


async def process_one() -> bool:
    """One full claim/process cycle. Returns False when there was nothing to do.

    Deterministic rejection is handled with a SAVEPOINT inside the SAME
    transaction that holds the claim. Rolling the outer transaction back
    instead would release the row lock and publish it as ``captured`` again;
    another worker could then claim it, process it successfully and mark it
    ``processed``, only for this worker's follow-up transaction to overwrite
    that with ``failed``. Keeping the claim, undoing only the domain writes and
    committing the terminal status together closes that window entirely.

    A transient/unexpected failure rolls everything back — including the claim —
    so the row stays ``captured`` and is retried by a later cycle.
    """
    async with SessionLocal() as session:
        async with session.begin():
            event = await claim_next_event(session)
            if event is None:
                return False
            event_id = int(event.id)

            savepoint = await session.begin_nested()
            try:
                await process_claimed_event(session, event)
            except NormalizationError as exc:
                # Undo ONLY the domain writes; the claim and the row lock stay.
                await savepoint.rollback()
                event = (
                    (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
                )
                event.status = STATUS_FAILED
                event.processed_at = utcnow()
                event.error_code = exc.code
                logger.warning("easyweek event=%s failed code=%s", event_id, exc.code)
                return True

            if savepoint.is_active:
                await savepoint.commit()
            return True


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
    consecutive_errors = 0

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
        try:
            did_work = await process_one()
        except Exception as exc:
            # Deliberately NOT `logger.exception`, NOT `str(exc)` and NO
            # traceback. A SQLAlchemy error renders the failing statement WITH
            # its bound parameters, which for this worker means the customer's
            # phone, e-mail, name and comment. Only the exception class name and
            # a fixed code are safe to record.
            #
            # `except Exception` on purpose: BaseException — CancelledError,
            # KeyboardInterrupt, SystemExit — must keep propagating so shutdown
            # is never swallowed.
            consecutive_errors += 1
            logger.error(
                "easyweek processing_error type=%s consecutive=%s; event stays captured for retry",
                type(exc).__name__,
                consecutive_errors,
            )
            # Bounded exponential backoff. One permanently failing row would
            # otherwise spin the loop at full speed and block the whole backlog
            # behind it; backing off keeps the process alive and cheap while an
            # operator investigates. The row is NOT marked failed — a transient
            # fault must not become a permanent verdict.
            backoff = min(effective_poll_sec * (2**consecutive_errors), MAX_ERROR_BACKOFF_SEC)
            await _sleep_unless_stopping(backoff, stop_event)
            continue

        consecutive_errors = 0
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
    "MAX_ERROR_BACKOFF_SEC",
    "already_applied",
    "apply_booking",
    "claim_next_event",
    "main",
    "process_claimed_event",
    "process_one",
    "processing_is_configured",
    "run_loop",
    "sync_record_service",
    "upsert_client",
    "upsert_record",
]

if __name__ == "__main__":
    main()
