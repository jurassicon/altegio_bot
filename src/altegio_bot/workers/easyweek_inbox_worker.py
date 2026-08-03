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
* a transient/unexpected failure rolls the whole transaction back and then
  schedules a PER-EVENT retry (``processing_attempts`` + ``next_retry_at``),
  so the row stays ``captured`` yet stops blocking the queue head — other
  eligible events keep flowing while it waits. Its exception is logged as a
  class name and a fixed code only, never as text or a traceback. After
  ``MAX_PROCESSING_ATTEMPTS`` the row is quarantined as ``failed`` /
  ``retry_exhausted``;
* SIGTERM stops the worker from claiming the NEXT event but never interrupts a
  transaction already in flight.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select
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

# Per-event retry schedule. A transient fault re-queues the row with a
# next_retry_at in the future so the REST of the backlog keeps moving; the
# global loop backoff alone cannot do that, because the claim always picks
# the oldest eligible row and would keep picking the same poisoned one.
RETRY_BASE_SEC = 5.0
MAX_RETRY_DELAY_SEC = 300.0
# After this many transient failures the row is quarantined so it stops
# consuming attempts forever and becomes visible to the operator.
MAX_PROCESSING_ATTEMPTS = 5
# Terminal code for a row that exhausted its retries. Fixed and PII-free —
# it deliberately does NOT say what went wrong, because the only detail we
# have is an exception whose text can contain customer data.
RETRY_EXHAUSTED_CODE = "retry_exhausted"

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
    """Claim exactly ONE eligible captured row, oldest first.

    Eligible means ``status='captured'`` AND its retry delay has elapsed. One
    row per transaction on purpose: a batch would make the rollback story
    ambiguous, because one poisoned row would drag its siblings back to
    ``captured`` along with it.
    """
    stmt = (
        select(EasyWeekEvent)
        .where(EasyWeekEvent.status == STATUS_CAPTURED)
        # Rows waiting out a retry delay are not eligible yet. This is what
        # keeps one permanently failing delivery from blocking the queue head.
        .where(
            or_(
                EasyWeekEvent.next_retry_at.is_(None),
                EasyWeekEvent.next_retry_at <= func.now(),
            )
        )
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


async def resolve_record(session: AsyncSession, booking: NormalizedBooking) -> Record | None:
    """Resolve the Record this booking owns, or raise ``identity_conflict``.

    The booking UUID is the ONLY identity. The numeric booking id is an
    attribute that happens to be unique per (provider, company) — it is never
    used to *find* a row to adopt, because EasyWeek can reuse or collide it and
    adopting would hand one booking the row of another.

    Both sides are resolved up front, so the conflict is detected BEFORE any
    write. Writing first and letting the unique constraint object would surface
    as an IntegrityError, which the worker cannot distinguish from a transient
    database fault — it would be retried forever instead of failing once.

    Allowed:
      * no UUID row, no numeric row            -> create;
      * UUID row, no numeric row               -> adopt the new numeric id;
      * UUID row that IS the numeric row       -> ordinary update.
    Everything else is ``identity_conflict``, including a numeric row whose
    UUID is NULL: its ownership was never proven, so claiming it would be a
    guess.
    """
    by_uuid = (
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
    by_numeric = (
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

    if by_numeric is not None and (by_uuid is None or by_numeric.id != by_uuid.id):
        # The numeric id belongs to some other row — either a different booking
        # or a row whose ownership was never established.
        raise NormalizationError(NormalizationError.IDENTITY_CONFLICT)

    return by_uuid


async def upsert_record(
    session: AsyncSession,
    booking: NormalizedBooking,
    client: Client | None,
    *,
    client_present: bool,
) -> Record:
    """Create or patch the Record for this booking.

    Assumes :func:`resolve_record` already proved the identity is unambiguous.
    """
    record = await resolve_record(session, booking)

    if record is None:
        record = Record(
            provider=PROVIDER,
            company_id=booking.company_id,
            altegio_record_id=booking.booking_id,
            easyweek_booking_uuid=booking.booking_uuid,
        )
        session.add(record)

    record.altegio_record_id = booking.booking_id
    record.last_change_at = utcnow()

    # Client link follows the SAME presence rule as every other field: a
    # delivery that does not mention `customer_id` must not unlink a client we
    # already resolved. An explicit null is not treated as "unlink" — the
    # confirmed payloads never send one, so that semantics is unproven and
    # guessing it would silently orphan a booking.
    if client_present and client is not None:
        record.altegio_client_id = booking.customer_id
        record.client_id = client.id

    # Patch semantics: only fields this delivery carried are written.
    _patch(record, "starts_at", booking, booking.starts_at)
    _patch(record, "ends_at", booking, booking.ends_at)
    _patch(record, "duration_sec", booking, booking.duration_sec)
    _patch(record, "staff_name", booking, booking.staff_name)
    _patch(record, "comment", booking, booking.comment)
    _patch(record, "total_cost", booking, booking.total_cost)

    if booking.action == DELETE:
        record.is_deleted = True

    _apply_manage_link(record, booking)
    return record


def _service_title(booking: NormalizedBooking) -> str | None:
    """Customer-facing service text.

    ``services_description`` describes the WHOLE set and is the only confirmed
    field that stays correct for a multi-service booking; the singular
    ``service_name`` names just one of them and would be misleading. So the
    description wins whenever it is present, and ``service_name`` is the
    fallback for the single-service shape the live capture actually contained.
    """
    if booking.carries("services_description") and booking.services_description:
        return booking.services_description
    if booking.carries("service_name"):
        return booking.service_name
    return None


async def sync_record_service(session: AsyncSession, record: Record, booking: NormalizedBooking) -> None:
    """Keep the booking's service snapshot in step with the delivery.

    PR-5 renders templates from DOMAIN data, so the service text and its price
    have to be persisted here or the lifecycle messages would carry an empty
    service and a 0.00 total.

    ``record_services`` is keyed ``(record_id, service_id)``. That is already
    provider-safe without a schema change: ``record_id`` points at a row whose
    ``provider`` is ``easyweek``, so an EasyWeek service id can never collide
    with an Altegio one — they hang off different records.

    Canonical mapping (single-row snapshot, matching the current model):
      * ``title``       <- services_description, else service_name;
      * ``amount``      <- services_count, else quantity;
      * ``cost_to_pay`` <- the same value as ``Record.total_cost``.

    The price is synchronised even when the delivery omits ``service_id``, so a
    price-only edit can never leave a stale ``cost_to_pay`` next to a fresh
    ``Record.total_cost``. A delivery mentioning no service or price field at
    all leaves the whole snapshot untouched: the payload carries one flat
    service, not a list, so silence means "unchanged", never "no services".
    """
    touches_service = any(
        booking.carries(field)
        for field in ("service_id", "service_name", "services_description", "services_count", "service_quantity")
    )
    touches_price = booking.carries("total_cost")
    if not touches_service and not touches_price:
        return

    existing = list(
        (await session.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars()
    )

    target: RecordService | None
    if booking.carries("service_id") and booking.service_id is not None:
        # The payload names exactly one service; anything else attached to this
        # booking came from a previous, different selection.
        for stale in existing:
            if stale.service_id != booking.service_id:
                await session.delete(stale)
        target = next((row for row in existing if row.service_id == booking.service_id), None)
        if target is None:
            target = RecordService(record_id=record.id, service_id=booking.service_id)
            session.add(target)
    else:
        # No service identity in this delivery. Update the snapshot we already
        # have; never invent a synthetic service id just to create one.
        target = existing[0] if len(existing) == 1 else None
        if target is None:
            return

    title = _service_title(booking)
    if title is not None:
        target.title = title
    if booking.carries("services_count") and booking.services_count is not None:
        target.amount = booking.services_count
    elif booking.carries("service_quantity"):
        target.amount = booking.service_quantity
    if touches_price:
        # Kept identical to Record.total_cost by construction.
        target.cost_to_pay = booking.total_cost


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


async def is_cancel_terminal(session: AsyncSession, booking: NormalizedBooking) -> bool:
    """True when this booking is already cancelled and must stay that way.

    Cancel is TERMINAL in PR-4. The previous rule let any post-cancel
    update/reschedule clear ``is_deleted``, which silently assumed that such a
    delivery means "un-cancelled". That contract is not proven: EasyWeek has no
    confirmed un-cancel trigger, no machine-readable status in the webhook body
    (``booking_status`` is localized salon-editable prose), and a stale
    delivery that merely arrived late is indistinguishable from a real edit.
    Resurrecting a cancelled appointment would re-notify a customer whose
    booking no longer exists, so the fail-closed reading is the safe one.

    If EasyWeek is later confirmed to emit a real un-cancel signal, that signal
    — not prose, and not "an update arrived" — is what should reopen this.
    """
    if booking.action == DELETE:
        return False
    existing = (
        (
            await session.execute(
                select(Record.is_deleted)
                .where(Record.provider == PROVIDER)
                .where(Record.easyweek_booking_uuid == booking.booking_uuid)
            )
        )
        .scalars()
        .first()
    )
    return bool(existing)


async def apply_booking(
    session: AsyncSession,
    booking: NormalizedBooking,
    *,
    event_hint: str,
    payload_hash: str | None,
) -> Record | None:
    """Apply one validated delivery. Returns None when it was a no-op.

    Order matters: identity and the cancel guard are settled BEFORE any write,
    so a conflicting or stale delivery leaves the database exactly as it was.
    """
    # A post-cancel delivery changes nothing at all — not the times, not the
    # service, not the client link — and plans no job.
    if await is_cancel_terminal(session, booking):
        return None

    client_present = booking.carries("customer_id")
    client = await upsert_client(session, booking) if client_present else None
    record = await upsert_record(session, booking, client, client_present=client_present)
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

    applied = await apply_booking(
        session,
        booking,
        event_hint=str(event_hint),
        payload_hash=payload_hash,
    )
    if applied is None:
        logger.info(
            "easyweek event=%s hint=%s booking_uuid=%s ignored; booking is cancelled",
            event_id,
            event_hint,
            booking.booking_uuid,
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


def retry_delay_for(attempts: int) -> float:
    """Bounded exponential backoff before the next attempt at this event."""
    return min(RETRY_BASE_SEC * (2 ** max(attempts - 1, 0)), MAX_RETRY_DELAY_SEC)


async def schedule_retry(event_id: int) -> bool:
    """Re-queue a transiently failed event, or quarantine it. True if quarantined.

    Runs in its OWN transaction: the caller's transaction was rolled back by the
    fault, so an attempt counter written there would have been rolled back with
    it. The lookup is conditional on the row still being ``captured``, so this
    can never overwrite a terminal status another worker committed meanwhile.

    Nothing about the exception is persisted — only a counter and a timestamp.
    """
    async with SessionLocal() as session:
        async with session.begin():
            event = (
                (
                    await session.execute(
                        select(EasyWeekEvent)
                        .where(EasyWeekEvent.id == event_id)
                        .where(EasyWeekEvent.status == STATUS_CAPTURED)
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if event is None:
                # Already terminal — do not resurrect it.
                return False

            attempts = int(event.processing_attempts or 0) + 1
            event.processing_attempts = attempts

            if attempts >= MAX_PROCESSING_ATTEMPTS:
                # Quarantine: stop burning attempts forever and surface the row
                # to the operator under a fixed, PII-free code.
                event.status = STATUS_FAILED
                event.processed_at = utcnow()
                event.error_code = RETRY_EXHAUSTED_CODE
                event.next_retry_at = None
                logger.error(
                    "easyweek event=%s quarantined after %s attempts code=%s",
                    event_id,
                    attempts,
                    RETRY_EXHAUSTED_CODE,
                )
                return True

            delay = retry_delay_for(attempts)
            event.next_retry_at = utcnow() + timedelta(seconds=delay)
            logger.warning(
                "easyweek event=%s rescheduled attempt=%s retry_in=%ss",
                event_id,
                attempts,
                int(delay),
            )
            return False


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
    and then schedules a PER-EVENT retry, so the row stays ``captured`` but
    stops blocking the rest of the backlog.
    """
    transient_event_id: int | None = None
    try:
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
                        (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id)))
                        .scalars()
                        .one()
                    )
                    event.status = STATUS_FAILED
                    event.processed_at = utcnow()
                    event.error_code = exc.code
                    logger.warning("easyweek event=%s failed code=%s", event_id, exc.code)
                    return True
                except Exception:
                    # Remember which row it was, then let the WHOLE transaction
                    # roll back before anything else touches the database.
                    transient_event_id = event_id
                    raise

                if savepoint.is_active:
                    await savepoint.commit()
                return True
    except Exception as exc:
        if transient_event_id is None:
            # The fault was not tied to a claimed row (e.g. the claim itself
            # could not run). Let the loop's handler deal with it.
            raise
        # Safe metadata only: never str(exc), never a traceback. A SQLAlchemy
        # error renders the statement WITH its bound parameters, which here
        # means the customer's phone, e-mail, name and comment.
        logger.error(
            "easyweek event=%s processing_error type=%s",
            transient_event_id,
            type(exc).__name__,
        )
        await schedule_retry(transient_event_id)
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
    "MAX_PROCESSING_ATTEMPTS",
    "RETRY_EXHAUSTED_CODE",
    "already_applied",
    "apply_booking",
    "is_cancel_terminal",
    "claim_next_event",
    "main",
    "process_claimed_event",
    "process_one",
    "processing_is_configured",
    "run_loop",
    "resolve_record",
    "retry_delay_for",
    "schedule_retry",
    "sync_record_service",
    "upsert_client",
    "upsert_record",
]

if __name__ == "__main__":
    main()
