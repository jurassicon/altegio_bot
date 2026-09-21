"""Operator-only recovery of ONE kind of terminally failed EasyWeek delivery.

The production defect this exists for
-------------------------------------
EasyWeek delivered a real ``booking-canceled`` webhook whose ``service_id`` was
a literal JSON ``null``. The normalizer of the day rejected every non-positive
``service_id``, so the delivery reached ``status=failed`` /
``error_code=invalid_payload`` and changed nothing: the booking is cancelled in
EasyWeek and still active in our database.

Widening the normalizer (see
:func:`altegio_bot.easyweek_normalizer.normalize_event`) fixes every FUTURE
delivery of that shape. It does not fix the row that already failed. A
terminally failed ``easyweek_events`` row is never claimed again — by design,
because re-claiming terminal rows is how a research-grade capture table turns
into an accidental replay engine. Recovering it therefore needs a separate,
deliberate, operator-driven act, and this module is that act.

What it deliberately is NOT
---------------------------
* **Not a replay.** It takes explicitly named ``--event-id`` values and nothing
  else. There is no ``--all``, no scan, no deploy hook and no background pass.
* **Not** :func:`~altegio_bot.workers.easyweek_inbox_worker.apply_booking`.
  Feeding a weeks-old cancellation through the ordinary lifecycle path would
  plan a ``record_canceled`` for a customer who cancelled long ago, and could
  earn them a ``comeback_3d`` as well. This module performs an explicit SILENT
  historical cancellation instead: the record state, the reminder withdrawal
  and the event's terminal status, and nothing else.
* **Not a change to the runtime.** The real-time webhook path keeps its
  canonical behaviour, including the lifecycle job a timely cancellation earns.

What one apply may do, in one transaction
-----------------------------------------
``Record.is_deleted = True``; the record's own ``queued`` EasyWeek
``reminder_24h``/``reminder_2h`` move to ``canceled``; the named event moves to
``processed`` through the worker's single terminal helper. Nothing else: no
``MessageJob`` is created, no ``OutboxMessage`` is written, Meta and Chatwoot
are never called, no EasyWeek mutation endpoint is called, and no ``done`` /
``failed`` / ``canceled`` job is re-opened.

Afterwards the existing post-migration reminder handover (§30) can classify the
row as ``handover_terminal_canceled`` instead of ``local_target_mismatch``, and
finish the ownership transfer it was blocking. This module never touches an
Altegio record or an Altegio job: that half stays the handover's.

Plan / apply / verify follow the frozen-plan machinery already proved by the
§34.5 reminder recovery and the §38.7 snapshot recovery, imported rather than
copied so the three operator tools cannot drift apart on digests, file
permissions or plan freshness.
"""

from __future__ import annotations

import asyncio
import json
import uuid as uuid_module
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_multi_service_recovery import (
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    DEFAULT_PAUSE_SEC,
    MAX_SNAPSHOT_AGE_SEC,
    MODE_PLAN,
    MODE_VERIFY,
    RecoveryError,
    _client_state_digest,
    _digest,
    _parse_timestamp,
    _record_service_state_digests,
    _timestamp,
    _utc,
    _valid_digest,
    write_private_json,
)
from altegio_bot.easyweek_normalizer import (
    CANCEL_EVENT_HINT,
    DELETE,
    NormalizationError,
    canonical_booking_uuid,
    normalize_event,
)
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.easyweek_reminder_guard import (
    GuardOutcome,
    ObservedBooking,
    classify_client_error,
    read_booking_state,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
)

# The one terminal transition for a captured delivery. Imported rather than
# re-implemented: §30-style recoveries must not invent a second way to
# terminalize an event, or the two would drift on `error_code` and
# `next_retry_at` exactly as the worker's own docstring warns.
from altegio_bot.workers.easyweek_inbox_worker import (
    STATUS_FAILED,
    STATUS_PROCESSED,
    mark_processed,
)

__all__ = [
    "APPLY_REPORT_VERSION",
    "DISPOSITION_BLOCKED",
    "DISPOSITION_RECOVER",
    "MAX_EVENT_IDS",
    "OUTCOME_ALREADY_APPLIED",
    "OUTCOME_APPLIED",
    "PLAN_VERSION",
    "RECOVERY_CANCEL_REASON",
    "REQUIRED_ERROR_CODE",
    "ApplyResult",
    "FailedCancellationPlan",
    "FrozenPlan",
    "RecoveryError",
    "apply_recovery_plan",
    "build_recovery_plan",
    "check_apply_authorization",
    "configuration_digest",
    "confirmation_phrase",
    "read_apply_report",
    "read_plan",
    "validate_event_ids",
    "verify_recovery",
    "write_plan",
]

PLAN_VERSION: Final = 1
APPLY_REPORT_VERSION: Final = 1

# An operator names the rows. A command that could name a hundred is a replay
# with extra steps, so the list is bounded as well as explicit.
MAX_EVENT_IDS: Final = 10

REQUIRED_ERROR_CODE: Final = NormalizationError.INVALID_PAYLOAD

DISPOSITION_RECOVER: Final = "recover_failed_cancellation"
DISPOSITION_BLOCKED: Final = "blocked"

OUTCOME_APPLIED: Final = "applied"
OUTCOME_ALREADY_APPLIED: Final = "already_applied"

# Why a queued reminder was withdrawn. Stable, PII-free, and distinct from the
# runtime planner's own wording so `message_jobs` says which hand did it.
RECOVERY_CANCEL_REASON: Final = "EasyWeek booking canceled; operator historical failed-cancellation recovery"

# ---------------------------------------------------------------------------
# Blockers — every one of them stops the WHOLE apply, never just one row
# ---------------------------------------------------------------------------
BLOCKER_EVENT_NOT_FOUND: Final = "event_not_found"
BLOCKER_STATUS_NOT_FAILED: Final = "event_status_not_failed"
BLOCKER_ERROR_CODE_UNEXPECTED: Final = "event_error_code_not_invalid_payload"
BLOCKER_HINT_NOT_CANCELLATION: Final = "event_hint_not_booking_canceled"
BLOCKER_BODY_TRUNCATED: Final = "event_body_truncated"
BLOCKER_SERVICE_ID_NOT_NULL: Final = "payload_service_id_not_literal_null"
BLOCKER_CAPTURED_BODY_DISAGREES: Final = "captured_body_disagrees_with_payload"
BLOCKER_BOOKING_UUID_INVALID: Final = "booking_uuid_not_canonical"
BLOCKER_BOOKING_UUID_UNSTABLE: Final = "booking_uuid_column_disagrees_with_payload"
BLOCKER_LOCATION_UNCONFIGURED: Final = "location_registry_unavailable"
BLOCKER_NORMALIZER_REFUSES: Final = "normalizer_still_refuses"
BLOCKER_NORMALIZER_NOT_DELETE: Final = "normalizer_action_not_delete"
BLOCKER_NORMALIZER_SERVICE_ID_CARRIED: Final = "normalizer_service_id_still_carried"
BLOCKER_RECORD_MISSING: Final = "local_record_missing"
BLOCKER_RECORD_AMBIGUOUS: Final = "local_record_ambiguous"
BLOCKER_RECORD_COMPANY_MISMATCH: Final = "local_record_company_mismatch"
BLOCKER_RECORD_NUMERIC_MISMATCH: Final = "local_record_numeric_id_mismatch"
BLOCKER_RECORD_ALREADY_DELETED: Final = "local_record_already_deleted"
BLOCKER_LATER_EVENT: Final = "later_event_for_same_booking"
BLOCKER_API_NOT_FOUND: Final = "api_not_found"
BLOCKER_API_UNAUTHORIZED: Final = "api_unauthorized"
BLOCKER_API_UNAVAILABLE: Final = "api_unavailable"
BLOCKER_API_RATE_LIMITED: Final = "api_rate_limited"
BLOCKER_API_MALFORMED: Final = "api_malformed_response"
BLOCKER_LIVE_IDENTITY_MISMATCH: Final = "live_identity_mismatch"
BLOCKER_LIVE_NOT_CANCELED: Final = "live_booking_not_canceled"
BLOCKER_LIVE_CONTRADICTION: Final = "live_state_contradiction"
BLOCKER_LIVE_START_DRIFT: Final = "live_start_instant_drift"
BLOCKER_REMINDER_PROCESSING: Final = "easyweek_reminder_processing"

_API_BLOCKERS: Final[dict[GuardOutcome, str]] = {
    GuardOutcome.NOT_FOUND: BLOCKER_API_NOT_FOUND,
    GuardOutcome.CONFIGURATION_UNAVAILABLE: BLOCKER_API_UNAUTHORIZED,
    GuardOutcome.RETRYABLE_UNAVAILABLE: BLOCKER_API_UNAVAILABLE,
    GuardOutcome.IDENTITY_MISMATCH: BLOCKER_LIVE_IDENTITY_MISMATCH,
    GuardOutcome.LOCATION_MISMATCH: BLOCKER_LIVE_IDENTITY_MISMATCH,
    GuardOutcome.MALFORMED_RESPONSE: BLOCKER_API_MALFORMED,
}

_REMINDER_TYPES: Final = tuple(sorted(EASYWEEK_REMINDER_JOB_TYPES))
_OPEN_REMINDER_STATUSES: Final = ("queued", "processing")


class RecoveryReader(Protocol):
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...


def confirmation_phrase(plan_digest: str) -> str:
    return f"recover easyweek failed cancellation {plan_digest}"


def configuration_digest() -> str:
    """Digest the ONE configuration this recovery depends on: branch identity.

    Deliberately narrower than the multi-service tools' digest. This recovery
    reads no service allowlist and plans no notification, so binding the plan
    to the allowlist would invalidate an operator's snapshot for a change that
    cannot affect its outcome. The location registry, on the other hand, is
    what the normalizer isolates on and what the live read proves against.
    """
    registry = configured_easyweek_locations()
    return _digest(
        {
            "registry_configured": registry.configured,
            "registry_valid": registry.valid,
            "locations": [
                {
                    "company_id": item.company_id,
                    "location_uuid": item.location_uuid,
                    "name": item.name,
                }
                for item in sorted(registry.locations.values(), key=lambda value: value.company_id)
            ],
        }
    )


def validate_event_ids(raw: Sequence[int]) -> tuple[int, ...]:
    """Exactly the ids an operator typed: positive, unique, bounded, ordered."""
    ids = sorted({int(value) for value in raw})
    if not ids:
        raise RecoveryError("event_ids_required")
    if any(value <= 0 for value in ids):
        raise RecoveryError("event_id_invalid")
    if len(ids) > MAX_EVENT_IDS:
        raise RecoveryError("event_ids_too_many")
    return tuple(ids)


def _optional_timestamp(value: datetime | None) -> str | None:
    return _timestamp(value) if value is not None else None


def _historical_state_digest(record: Record) -> str:
    """Hash the Record state this recovery promises NOT to change.

    ``is_deleted`` is the one field an apply writes, so it is deliberately
    outside the digest and carried as its own explicit expectation. Keeping it
    in would make the fingerprint unusable for the very case it has to survive:
    proving, on a second apply of the same snapshot, that nothing OTHER than
    the cancellation state moved.

    The digest is one-way, so hashing ``comment`` and ``raw`` here does not put
    customer text into the snapshot file.
    """
    return _digest(
        {
            "provider": record.provider,
            "company_id": record.company_id,
            "altegio_record_id": record.altegio_record_id,
            "easyweek_booking_uuid": (
                str(record.easyweek_booking_uuid) if record.easyweek_booking_uuid is not None else None
            ),
            "easyweek_booking_hash_id": record.easyweek_booking_hash_id,
            "client_id": record.client_id,
            "altegio_client_id": record.altegio_client_id,
            "staff_id": record.staff_id,
            "staff_name": record.staff_name,
            "starts_at": _optional_timestamp(record.starts_at),
            "ends_at": _optional_timestamp(record.ends_at),
            "duration_sec": record.duration_sec,
            "comment": record.comment,
            "short_link": record.short_link,
            "confirmed": record.confirmed,
            "attendance": record.attendance,
            "visit_attendance": record.visit_attendance,
            "total_cost": str(record.total_cost) if record.total_cost is not None else None,
            "last_change_at": _optional_timestamp(record.last_change_at),
            "raw": record.raw,
        }
    )


def _job_row(job: MessageJob) -> dict[str, Any]:
    """PII-free projection of one job, precise enough to prove it did not move."""
    return {
        "id": job.id,
        "provider": job.provider,
        "company_id": job.company_id,
        "record_id": job.record_id,
        "client_id": job.client_id,
        "job_type": job.job_type,
        "status": job.status,
        "run_at": _optional_timestamp(job.run_at),
        "dedupe_key": job.dedupe_key,
        "attempts": job.attempts,
        "locked": job.locked_at is not None,
    }


def _event_row(event: EasyWeekEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_hint": event.event_hint,
        "status": event.status,
        "error_code": event.error_code,
        "received_at": _optional_timestamp(event.received_at),
        "processed_at": _optional_timestamp(event.processed_at),
        "next_retry_at": _optional_timestamp(event.next_retry_at),
        "processing_attempts": event.processing_attempts,
    }


async def _record_jobs(session: AsyncSession, record_id: int, *, lock: bool = False) -> list[MessageJob]:
    stmt = select(MessageJob).where(MessageJob.record_id == record_id).order_by(MessageJob.id.asc())
    if lock:
        stmt = stmt.with_for_update()
    return list((await session.execute(stmt)).scalars().all())


async def _record_outbox_ids(session: AsyncSession, record_id: int) -> list[int]:
    """Outbox rows bound to this Record. The set must be identical afterwards.

    An ``OutboxMessage`` is the thing a customer actually receives, so "this
    recovery wrote none" has to be proved against the record it could have
    written one for, not inferred from a table-wide count that other workers
    move on their own.
    """
    return [
        int(value)
        for value in (
            await session.execute(
                select(OutboxMessage.id).where(OutboxMessage.record_id == record_id).order_by(OutboxMessage.id.asc())
            )
        )
        .scalars()
        .all()
    ]


async def _sibling_events(
    session: AsyncSession,
    *,
    booking_uuid: uuid_module.UUID,
    exclude_event_id: int,
) -> list[EasyWeekEvent]:
    """Every OTHER captured delivery of the same booking, oldest first."""
    stmt = (
        select(EasyWeekEvent)
        .where(EasyWeekEvent.booking_uuid == booking_uuid)
        .where(EasyWeekEvent.id != exclude_event_id)
        .order_by(EasyWeekEvent.received_at.asc(), EasyWeekEvent.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


def _later_event_ids(siblings: Sequence[EasyWeekEvent], *, target: EasyWeekEvent) -> list[int]:
    """Siblings strictly after the target in the worker's own claim order.

    ``(received_at, id)`` is exactly the key ``claim_next_event`` serialises a
    booking on, so "later" here means the same thing it means to the worker. A
    later delivery is the one case where reviving this cancellation could
    contradict a newer truth, so it is a blocker rather than a note.
    """
    key = (target.received_at, target.id)
    return [item.id for item in siblings if item.received_at is not None and (item.received_at, item.id) > key]


def _service_id_is_literal_null(event: EasyWeekEvent) -> str | None:
    """Prove the payload really carries ``service_id: null``, or say why not.

    The JSONB column is authoritative for what we parsed, but this recovery is
    resurrecting a row an operator only ever saw as ``invalid_payload``, so the
    original bytes are cross-checked when they are still on the row. JSONB
    preserves both key presence and JSON null, so the two must agree; if they
    do not, something rewrote one of them and nothing here may proceed.
    """
    payload = event.payload
    if not isinstance(payload, Mapping) or "service_id" not in payload or payload["service_id"] is not None:
        return BLOCKER_SERVICE_ID_NOT_NULL

    source: bytes | str | None = event.body_raw if event.body_raw is not None else event.body_text
    if source is None:
        # A retention pass may blank the bytes. The parsed payload remains the
        # row's own record of the delivery, and `body_truncated=false` is
        # separately required, so this is not by itself a refusal.
        return None
    try:
        decoded = json.loads(source)
    except (ValueError, UnicodeDecodeError):
        return BLOCKER_CAPTURED_BODY_DISAGREES
    if not isinstance(decoded, dict) or "service_id" not in decoded or decoded["service_id"] is not None:
        return BLOCKER_CAPTURED_BODY_DISAGREES
    return None


async def _scope_fingerprint(session: AsyncSession) -> dict[str, Any]:
    """An aggregate over the four tables a stray write would land in.

    EVIDENCE, not a gate, and the distinction is deliberate. Capture, the inbox
    worker and the outbox worker all keep running while an operator reads a
    report and types a confirmation, so these numbers legitimately move for
    reasons that have nothing to do with this recovery. Failing an apply on
    that would make a fail-closed tool unusable on a live system without making
    it any safer.

    What actually gates the write is exact and row-level: the locked target
    event, the locked Record, the locked set of that Record's jobs, its outbox
    rows and the other events of its booking. Anything this module could create
    would be bound to the target Record — it knows no other — so a
    record-scoped proof is the precise version of "nothing wider happened", and
    :func:`_assert_scope_only` is where it lives.
    """
    events_by_status = {
        str(status): int(count)
        for status, count in (
            await session.execute(select(EasyWeekEvent.status, func.count()).group_by(EasyWeekEvent.status))
        ).all()
    }
    jobs_by_provider_status = {
        f"{provider}|{status}": int(count)
        for provider, status, count in (
            await session.execute(
                select(MessageJob.provider, MessageJob.status, func.count()).group_by(
                    MessageJob.provider, MessageJob.status
                )
            )
        ).all()
    }
    outbox_by_status = {
        str(status): int(count)
        for status, count in (
            await session.execute(select(OutboxMessage.status, func.count()).group_by(OutboxMessage.status))
        ).all()
    }
    easyweek_records = select(func.count()).select_from(Record).where(Record.provider == PROVIDER_EASYWEEK)
    return {
        "easyweek_events": {
            "total": int((await session.execute(select(func.count()).select_from(EasyWeekEvent))).scalar_one()),
            "max_id": (await session.execute(select(func.max(EasyWeekEvent.id)))).scalar(),
            "by_status": events_by_status,
        },
        "easyweek_records": {
            "total": int((await session.execute(easyweek_records)).scalar_one()),
            "deleted": int((await session.execute(easyweek_records.where(Record.is_deleted.is_(True)))).scalar_one()),
            "max_id": (await session.execute(select(func.max(Record.id)))).scalar(),
        },
        "message_jobs": {
            "total": int((await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()),
            "max_id": (await session.execute(select(func.max(MessageJob.id)))).scalar(),
            "by_provider_status": jobs_by_provider_status,
        },
        "outbox_messages": {
            "total": int((await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()),
            "max_id": (await session.execute(select(func.max(OutboxMessage.id)))).scalar(),
            "by_status": outbox_by_status,
        },
    }


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FailedCancellationPlan:
    planned_at: datetime
    configuration_digest: str
    requested_event_ids: tuple[int, ...]
    events: tuple[dict[str, Any], ...]
    scope_fingerprint: dict[str, Any]

    @property
    def recoverable(self) -> tuple[dict[str, Any], ...]:
        return tuple(row for row in self.events if row["disposition"] == DISPOSITION_RECOVER)

    @property
    def blockers(self) -> tuple[str, ...]:
        seen: list[str] = []
        for row in self.events:
            for reason in row["blockers"]:
                if reason not in seen:
                    seen.append(reason)
        return tuple(seen)

    @property
    def apply_ready(self) -> bool:
        """Every named event is recoverable, and at least one was named."""
        return bool(self.events) and len(self.recoverable) == len(self.events) == len(self.requested_event_ids)

    def summary(self) -> dict[str, Any]:
        return {
            "requested": len(self.requested_event_ids),
            "recoverable": len(self.recoverable),
            "blocked": len(self.events) - len(self.recoverable),
            "blockers": list(self.blockers),
            "apply_ready": self.apply_ready,
        }

    def _material(self) -> dict[str, Any]:
        return {
            "version": PLAN_VERSION,
            "mode": MODE_PLAN,
            "planned_at": _timestamp(self.planned_at),
            "configuration_digest": self.configuration_digest,
            "requested_event_ids": list(self.requested_event_ids),
            "events": [dict(row) for row in self.events],
            "scope_fingerprint": self.scope_fingerprint,
            "summary": self.summary(),
        }

    def snapshot(self) -> dict[str, Any]:
        material = self._material()
        return {**material, "plan_digest": _digest(material)}

    def as_safe_dict(self) -> dict[str, Any]:
        """What the CLI prints. Same content as the file; there is no PII in either."""
        return self.snapshot()


async def build_recovery_plan(
    session: AsyncSession,
    *,
    event_ids: Sequence[int],
    client: RecoveryReader,
    now: datetime | None = None,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> FailedCancellationPlan:
    """Freeze what is true about the named events. Writes nothing, ever.

    Guaranteed read-only by construction AND by a final rollback: the ORM can
    be left dirty by an attribute touched during comparison, and a plan that
    flushed such a change would be a write nobody authorised.
    """
    requested = validate_event_ids(event_ids)
    moment = _utc(now or datetime.now(timezone.utc))
    pause = sleep if sleep is not None else asyncio.sleep
    registry = configured_easyweek_locations()
    rows: list[dict[str, Any]] = []

    try:
        fingerprint = await _scope_fingerprint(session)
        found = {
            event.id: event
            for event in (
                await session.execute(
                    select(EasyWeekEvent).where(EasyWeekEvent.id.in_(requested)).order_by(EasyWeekEvent.id.asc())
                )
            )
            .scalars()
            .all()
        }
        api_calls = 0
        for event_id in requested:
            event = found.get(event_id)
            if event is None:
                rows.append(_blocked_row(event_id, [BLOCKER_EVENT_NOT_FOUND]))
                continue
            row, booking = _inspect_event(event, registry=registry)
            if booking is None:
                rows.append(row)
                continue
            record, local_row, local_blockers = await _inspect_local_state(session, event=event, booking=booking)
            row.update(local_row)
            if local_blockers or record is None:
                row["blockers"] = [*row["blockers"], *local_blockers]
                row["disposition"] = DISPOSITION_BLOCKED
                rows.append(row)
                continue

            if api_calls:
                await pause(pause_sec)
            api_calls += 1
            live_blocker, observed = await _prove_live_cancellation(
                client,
                booking_uuid=booking.booking_uuid,
                company_id=booking.company_id,
                registry=registry,
                expected_start=record.starts_at,
            )
            row["live"] = (
                {
                    "starts_at": _optional_timestamp(observed.starts_at),
                    "is_canceled": observed.is_canceled,
                    "is_completed": observed.is_completed,
                    "status_type": observed.normalized_status_type,
                }
                if observed is not None
                else None
            )
            if live_blocker is not None:
                row["blockers"] = [*row["blockers"], live_blocker]
                row["disposition"] = DISPOSITION_BLOCKED
            else:
                row["disposition"] = DISPOSITION_RECOVER
            rows.append(row)
    finally:
        # Belt and braces over "this function does not write": an accidental
        # dirty attribute must not survive the plan, whatever raised.
        await session.rollback()

    return FailedCancellationPlan(
        planned_at=moment,
        configuration_digest=configuration_digest(),
        requested_event_ids=requested,
        events=tuple(rows),
        scope_fingerprint=fingerprint,
    )


def _blocked_row(event_id: int, blockers: list[str]) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event": None,
        "booking_uuid": None,
        "company_id": None,
        "record_id": None,
        "record_altegio_record_id": None,
        "record_is_deleted": None,
        "record_state_digest": None,
        "record_services_digest": None,
        "client_state_digest": None,
        "record_jobs": [],
        "record_outbox_ids": [],
        "queued_reminder_job_ids": [],
        "processing_reminder_job_ids": [],
        "other_events_digest": None,
        "later_event_ids": [],
        "live": None,
        "disposition": DISPOSITION_BLOCKED,
        "blockers": blockers,
    }


def _inspect_event(event: EasyWeekEvent, *, registry: Any) -> tuple[dict[str, Any], Any]:
    """The payload-only half of the admission test. No database, no API.

    Returns ``(row, None)`` when the event is already refused, or
    ``(row, booking)`` when it still has to be matched against the database.
    The booking comes from the CURRENT normalizer: an event is only recoverable
    if the fixed contract now reads it as a DELETE that carries no service
    identity, which is precisely the shape this hotfix proved.
    """
    row = _blocked_row(event.id, [])
    row["event"] = _event_row(event)
    blockers: list[str] = []

    if event.status != STATUS_FAILED:
        blockers.append(BLOCKER_STATUS_NOT_FAILED)
    if event.error_code != REQUIRED_ERROR_CODE:
        blockers.append(BLOCKER_ERROR_CODE_UNEXPECTED)
    if (event.event_hint or "").strip() != CANCEL_EVENT_HINT:
        blockers.append(BLOCKER_HINT_NOT_CANCELLATION)
    if bool(event.body_truncated):
        blockers.append(BLOCKER_BODY_TRUNCATED)
    if (null_blocker := _service_id_is_literal_null(event)) is not None:
        blockers.append(null_blocker)

    booking_uuid = canonical_booking_uuid(event.payload)
    if booking_uuid is None:
        blockers.append(BLOCKER_BOOKING_UUID_INVALID)
    elif event.booking_uuid is not None and event.booking_uuid != booking_uuid:
        # The capture column and the body must name the same booking; the claim
        # orders on the column and every lookup below uses it.
        blockers.append(BLOCKER_BOOKING_UUID_UNSTABLE)
    row["booking_uuid"] = str(booking_uuid) if booking_uuid is not None else None

    if not registry.ready:
        blockers.append(BLOCKER_LOCATION_UNCONFIGURED)

    booking = None
    if not blockers:
        try:
            booking = normalize_event(
                event_hint=event.event_hint,
                payload=event.payload,
                body_truncated=bool(event.body_truncated),
                location_registry=registry.locations,
            )
        except NormalizationError as error:
            blockers.append(BLOCKER_NORMALIZER_REFUSES)
            row["normalizer_error_code"] = error.code
        else:
            if booking is None or booking.action != DELETE:
                blockers.append(BLOCKER_NORMALIZER_NOT_DELETE)
            elif booking.carries("service_id") or booking.service_id is not None:
                # Not the proven shape: a cancellation that really names a
                # service was never the failing case, and letting it through
                # here would silently move a service snapshot.
                blockers.append(BLOCKER_NORMALIZER_SERVICE_ID_CARRIED)

    if blockers or booking is None:
        row["blockers"] = blockers
        row["disposition"] = DISPOSITION_BLOCKED
        return row, None

    row["company_id"] = booking.company_id
    return row, booking


async def _resolve_record(
    session: AsyncSession, *, booking: Any, lock: bool = False
) -> tuple[Record | None, str | None]:
    """The ONE local EasyWeek Record this delivery owns, or why there is none.

    UUID-first, exactly like the runtime resolver: the booking UUID selects the
    row and the numeric id is then verified as an attribute of it. The numeric
    id is never used to FIND a row — that is how one booking would adopt
    another's — so a numeric collision is reported, not resolved.
    """
    stmt = (
        select(Record)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.easyweek_booking_uuid == booking.booking_uuid)
        .order_by(Record.id.asc())
    )
    if lock:
        stmt = stmt.with_for_update()
    matches = list((await session.execute(stmt)).scalars().all())
    if not matches:
        return None, BLOCKER_RECORD_MISSING
    if len(matches) > 1:
        return None, BLOCKER_RECORD_AMBIGUOUS
    record = matches[0]
    if record.company_id != booking.company_id:
        return record, BLOCKER_RECORD_COMPANY_MISMATCH
    if record.altegio_record_id != booking.booking_id:
        return record, BLOCKER_RECORD_NUMERIC_MISMATCH
    return record, None


async def _inspect_local_state(
    session: AsyncSession,
    *,
    event: EasyWeekEvent,
    booking: Any,
) -> tuple[Record | None, dict[str, Any], list[str]]:
    """Match the delivery to exactly one local Record and fingerprint its world."""
    record, mismatch = await _resolve_record(session, booking=booking)
    partial: dict[str, Any] = {"record_id": record.id if record is not None else None}
    if record is None or mismatch is not None:
        return None, partial, [mismatch or BLOCKER_RECORD_MISSING]

    jobs = await _record_jobs(session, record.id)
    client = await session.get(Client, record.client_id) if record.client_id is not None else None
    services = await _record_service_state_digests(session, [record])
    siblings = await _sibling_events(session, booking_uuid=booking.booking_uuid, exclude_event_id=event.id)
    later = _later_event_ids(siblings, target=event)

    partial.update(
        {
            "record_altegio_record_id": record.altegio_record_id,
            "record_is_deleted": bool(record.is_deleted),
            "record_state_digest": _historical_state_digest(record),
            "record_services_digest": services[record.id],
            "client_state_digest": _client_state_digest(client),
            "record_jobs": [_job_row(job) for job in jobs],
            "record_outbox_ids": await _record_outbox_ids(session, record.id),
            "queued_reminder_job_ids": sorted(
                job.id
                for job in jobs
                if job.provider == PROVIDER_EASYWEEK
                and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
                and job.status == "queued"
            ),
            "processing_reminder_job_ids": sorted(
                job.id
                for job in jobs
                if job.provider == PROVIDER_EASYWEEK
                and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
                and job.status == "processing"
            ),
            "other_events_digest": _digest([_event_row(item) for item in siblings]),
            "later_event_ids": later,
        }
    )

    blockers: list[str] = []
    if bool(record.is_deleted):
        # Nothing for this tool to recover: the historical cancellation state
        # is already there, so the event's terminal status is a separate
        # question an operator must look at rather than one an apply may fix.
        blockers.append(BLOCKER_RECORD_ALREADY_DELETED)
    if later:
        blockers.append(BLOCKER_LATER_EVENT)
    if partial["processing_reminder_job_ids"]:
        blockers.append(BLOCKER_REMINDER_PROCESSING)
    return record, partial, blockers


async def _prove_live_cancellation(
    client: RecoveryReader,
    *,
    booking_uuid: uuid_module.UUID,
    company_id: int,
    registry: Any,
    expected_start: datetime | None,
) -> tuple[str | None, ObservedBooking | None]:
    """One real ``GET /bookings/{uuid}``, judged fail-closed.

    Reuses the guard's own reader, so identity, branch, malformed fields and
    the ``is_canceled``/``status.type`` contradictions are decided by the same
    code the runtime send guard and the §30 handover use.
    """
    location = registry.locations.get(company_id) if registry.ready else None
    if location is None:
        return BLOCKER_LOCATION_UNCONFIGURED, None
    try:
        payload = await client.get_booking(str(booking_uuid))
    except Exception as exc:  # noqa: BLE001 — mapped by class; the text is never kept
        if getattr(exc, "status_code", None) == 429:
            return BLOCKER_API_RATE_LIMITED, None
        outcome = classify_client_error(exc).outcome
        return _API_BLOCKERS.get(outcome, BLOCKER_API_UNAVAILABLE), None

    observed = read_booking_state(payload, booking_uuid=booking_uuid, location=location)
    if not isinstance(observed, ObservedBooking):
        return _API_BLOCKERS.get(observed.outcome, BLOCKER_API_MALFORMED), None
    if not observed.is_canceled:
        return BLOCKER_LIVE_NOT_CANCELED, observed
    if observed.is_completed:
        # `read_booking_state` already refuses both flags at once; this is the
        # belt to that braces, and it keeps the reason specific if it ever
        # becomes reachable.
        return BLOCKER_LIVE_CONTRADICTION, observed
    if expected_start is None or _utc(expected_start) != observed.starts_at:
        return BLOCKER_LIVE_START_DRIFT, observed
    return None, observed


# ---------------------------------------------------------------------------
# the frozen plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FrozenPlan:
    payload: dict[str, Any]

    @property
    def digest(self) -> str:
        return str(self.payload["plan_digest"])

    @property
    def planned_at(self) -> datetime:
        return _parse_timestamp(self.payload["planned_at"])

    @property
    def events(self) -> list[dict[str, Any]]:
        return self.payload["events"]

    @property
    def event_ids(self) -> tuple[int, ...]:
        return tuple(int(value) for value in self.payload["requested_event_ids"])

    @property
    def apply_ready(self) -> bool:
        return bool(self.payload["summary"].get("apply_ready"))

    @property
    def scope_fingerprint(self) -> dict[str, Any]:
        return self.payload["scope_fingerprint"]


def write_plan(plan: FailedCancellationPlan, path: str | Path) -> Path:
    return write_private_json(plan.snapshot(), path)


_REQUIRED_PLAN_KEYS: Final = frozenset(
    {
        "version",
        "mode",
        "planned_at",
        "configuration_digest",
        "requested_event_ids",
        "events",
        "scope_fingerprint",
        "summary",
        "plan_digest",
    }
)


def read_plan(path: str | Path) -> FrozenPlan:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RecoveryError("plan_unreadable") from None
    if not isinstance(payload, dict) or payload.get("version") != PLAN_VERSION or payload.get("mode") != MODE_PLAN:
        raise RecoveryError("plan_version_unsupported")
    if (
        set(payload) != _REQUIRED_PLAN_KEYS
        or not isinstance(payload.get("events"), list)
        or not isinstance(payload.get("summary"), dict)
        or not isinstance(payload.get("requested_event_ids"), list)
        or not isinstance(payload.get("scope_fingerprint"), dict)
    ):
        raise RecoveryError("plan_malformed")
    unsigned = {key: value for key, value in payload.items() if key != "plan_digest"}
    if not _valid_digest(payload.get("plan_digest")) or payload["plan_digest"] != _digest(unsigned):
        # Covers both a tampered file and one edited by hand between plan and
        # apply: the digest signs every field the apply re-proves.
        raise RecoveryError("plan_digest_mismatch")
    return FrozenPlan(payload)


def check_apply_authorization(
    frozen: FrozenPlan,
    *,
    supplied_digest: str | None,
    supplied_confirmation: str | None,
    now: datetime,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> None:
    """Everything that must hold before a single byte may be written.

    The typed ``--apply`` flag and the host's environment authorisation are
    checked by the CLI; these are the plan-bound halves. The confirmation
    phrase embeds the digest on purpose, so a phrase copied out of yesterday's
    terminal cannot authorise today's plan.
    """
    if supplied_digest != frozen.digest:
        raise RecoveryError("plan_digest_mismatch")
    if supplied_confirmation != confirmation_phrase(frozen.digest):
        raise RecoveryError("confirmation_mismatch")
    age = (_utc(now) - frozen.planned_at).total_seconds()
    if age < 0 or age > min(max_age_sec, MAX_SNAPSHOT_AGE_SEC):
        raise RecoveryError("plan_expired")
    if not frozen.apply_ready:
        raise RecoveryError("plan_not_apply_ready")
    if frozen.payload.get("configuration_digest") != configuration_digest():
        raise RecoveryError("configuration_digest_changed")


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ApplyResult:
    plan_digest: str
    outcome: str
    events: tuple[dict[str, Any], ...]
    fingerprint_before: dict[str, Any]
    fingerprint_after: dict[str, Any]
    applied_at: datetime

    @property
    def canceled_reminder_job_ids(self) -> tuple[int, ...]:
        return tuple(sorted(job_id for row in self.events for job_id in row["canceled_reminder_job_ids"]))

    def report(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": APPLY_REPORT_VERSION,
            "mode": "apply-report",
            "plan_version": PLAN_VERSION,
            "plan_digest": self.plan_digest,
            "outcome": self.outcome,
            "events": [dict(row) for row in self.events],
            "mutation_counts": {
                "records_marked_deleted": sum(1 for row in self.events if row["record_marked_deleted"]),
                "reminders_canceled": len(self.canceled_reminder_job_ids),
                "events_terminalized": sum(1 for row in self.events if row["event_terminalized"]),
                # Stated rather than implied: this tool has no code path that
                # can create either, and the report is what verify checks.
                "message_jobs_created": 0,
                "outbox_messages_created": 0,
            },
            "fingerprint_before": self.fingerprint_before,
            "fingerprint_after": self.fingerprint_after,
            "applied_at": _timestamp(self.applied_at),
        }
        return {**payload, "report_digest": _digest(payload)}


def read_apply_report(path: str | Path, *, frozen: FrozenPlan) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RecoveryError("apply_report_unreadable") from None
    if not isinstance(payload, dict) or payload.get("version") != APPLY_REPORT_VERSION:
        raise RecoveryError("apply_report_version_unsupported")
    digest = payload.get("report_digest")
    unsigned = {key: value for key, value in payload.items() if key != "report_digest"}
    if not isinstance(digest, str) or digest != _digest(unsigned):
        raise RecoveryError("apply_report_digest_mismatch")
    if payload.get("plan_digest") != frozen.digest or payload.get("plan_version") != PLAN_VERSION:
        raise RecoveryError("plan_apply_digest_mismatch")
    return payload


def _frozen_row(frozen: FrozenPlan, event_id: int) -> dict[str, Any]:
    for row in frozen.events:
        if int(row["event_id"]) == event_id:
            return row
    raise RecoveryError("plan_malformed")


async def _reprove_live(
    frozen: FrozenPlan,
    *,
    client: RecoveryReader,
    registry: Any,
    pause: Any,
    pause_sec: float,
) -> None:
    """Ask EasyWeek again, right before the transaction. Any doubt aborts.

    The plan's live proof may be minutes old. A booking that was un-cancelled,
    moved or became unreadable in the meantime must not be written as a
    historical cancellation, and the API is the only place that knows.
    """
    for index, row in enumerate(frozen.events):
        if index:
            await pause(pause_sec)
        booking_uuid = uuid_module.UUID(str(row["booking_uuid"]))
        expected_start = _parse_timestamp(row["live"]["starts_at"])
        blocker, _observed = await _prove_live_cancellation(
            client,
            booking_uuid=booking_uuid,
            company_id=int(row["company_id"]),
            registry=registry,
            expected_start=expected_start,
        )
        if blocker is not None:
            raise RecoveryError(blocker)


def _already_applied(
    *,
    frozen_row: Mapping[str, Any],
    event: EasyWeekEvent,
    record: Record,
    jobs: Sequence[MessageJob],
) -> bool:
    """Is this exact plan's end state already in the database?

    A committed transaction whose one-off container then died leaves an
    operator with an undetermined result and one safe instinct: run the exact
    same command again. That repeat must be a no-op, not a second attempt and
    not a refusal, so the end state is recognised explicitly.
    """
    if event.status != STATUS_PROCESSED or event.error_code is not None or event.next_retry_at is not None:
        return False
    if not bool(record.is_deleted):
        return False
    expected = set(int(value) for value in frozen_row["queued_reminder_job_ids"])
    by_id = {job.id: job for job in jobs}
    for job_id in expected:
        job = by_id.get(job_id)
        if job is None or job.status != "canceled" or job.locked_at is not None:
            return False
    open_now = {
        job.id
        for job in jobs
        if job.provider == PROVIDER_EASYWEEK
        and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
        and job.status in _OPEN_REMINDER_STATUSES
    }
    return not open_now


async def apply_recovery_plan(
    session: AsyncSession,
    *,
    frozen: FrozenPlan,
    client: RecoveryReader,
    now: datetime | None = None,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> ApplyResult:
    """Re-prove, then perform the silent historical cancellation atomically.

    Either the record state, the reminder withdrawal and the event's terminal
    status all commit together, or nothing changes. The caller owns no
    transaction on entry: this function opens and closes its own, so a failure
    anywhere inside rolls the whole wave back.
    """
    if not frozen.apply_ready:
        raise RecoveryError("plan_not_apply_ready")
    registry = configured_easyweek_locations()
    if not registry.ready:
        raise RecoveryError(BLOCKER_LOCATION_UNCONFIGURED)
    pause = sleep if sleep is not None else asyncio.sleep

    await _reprove_live(frozen, client=client, registry=registry, pause=pause, pause_sec=pause_sec)
    # The live walk read nothing it may keep: start the write from a clean slate.
    await session.rollback()

    moment = _utc(now or datetime.now(timezone.utc))
    applied: list[dict[str, Any]] = []
    outcomes: set[str] = set()

    async with session.begin():
        # Read INSIDE the transaction, so the "nothing wider moved" proof below
        # compares two observations of the same snapshot. The plan's own
        # fingerprint is deliberately not required to match it: capture keeps
        # running while an operator reads the report, and an unrelated webhook
        # arriving must not veto a cancellation whose own rows are unchanged.
        # The per-row locks and digests are what authorise the write.
        before = await _scope_fingerprint(session)
        events = {
            event.id: event
            for event in (
                await session.execute(
                    select(EasyWeekEvent)
                    .where(EasyWeekEvent.id.in_(frozen.event_ids))
                    .order_by(EasyWeekEvent.id.asc())
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        }
        if set(events) != set(frozen.event_ids):
            raise RecoveryError("event_state_changed")

        for event_id in frozen.event_ids:
            row = _frozen_row(frozen, event_id)
            event = events[event_id]
            booking_uuid = uuid_module.UUID(str(row["booking_uuid"]))

            record = (
                (
                    await session.execute(
                        select(Record)
                        .where(Record.provider == PROVIDER_EASYWEEK)
                        .where(Record.easyweek_booking_uuid == booking_uuid)
                        .order_by(Record.id.asc())
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if len(record) != 1 or record[0].id != int(row["record_id"]):
                raise RecoveryError("record_state_changed")
            target = record[0]
            if (
                target.company_id != int(row["company_id"])
                or target.altegio_record_id != int(row["record_altegio_record_id"])
                or _historical_state_digest(target) != row["record_state_digest"]
            ):
                raise RecoveryError("record_state_changed")

            jobs = await _record_jobs(session, target.id, lock=True)
            client_row = await session.get(Client, target.client_id) if target.client_id is not None else None
            services = await _record_service_state_digests(session, [target], lock=True)
            if (
                services[target.id] != row["record_services_digest"]
                or _client_state_digest(client_row) != row["client_state_digest"]
            ):
                raise RecoveryError("record_state_changed")

            siblings = await _sibling_events(session, booking_uuid=booking_uuid, exclude_event_id=event.id)
            if _digest([_event_row(item) for item in siblings]) != row["other_events_digest"]:
                raise RecoveryError("event_state_changed")
            if _later_event_ids(siblings, target=event):
                raise RecoveryError(BLOCKER_LATER_EVENT)

            # A claimed reminder may already have reached Meta. Nothing here is
            # allowed to assume it did not, so one processing job stops the
            # whole wave before any write.
            if any(
                job.provider == PROVIDER_EASYWEEK
                and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
                and job.status == "processing"
                for job in jobs
            ):
                raise RecoveryError(BLOCKER_REMINDER_PROCESSING)

            if _already_applied(frozen_row=row, event=event, record=target, jobs=jobs):
                outcomes.add(OUTCOME_ALREADY_APPLIED)
                applied.append(
                    {
                        "event_id": event_id,
                        "record_id": target.id,
                        "record_marked_deleted": False,
                        "canceled_reminder_job_ids": [],
                        "event_terminalized": False,
                        "already_recovered": True,
                    }
                )
                continue

            if event.status != STATUS_FAILED or event.error_code != REQUIRED_ERROR_CODE:
                raise RecoveryError("event_state_changed")
            if bool(target.is_deleted) != bool(row["record_is_deleted"]) or bool(target.is_deleted):
                raise RecoveryError("record_state_changed")
            queued_now = sorted(
                job.id
                for job in jobs
                if job.provider == PROVIDER_EASYWEEK
                and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
                and job.status == "queued"
            )
            if queued_now != [int(value) for value in row["queued_reminder_job_ids"]]:
                raise RecoveryError("job_state_changed")

            # ---- the only three writes this module performs -----------------
            target.is_deleted = True
            for job in jobs:
                if job.id in set(queued_now):
                    job.status = "canceled"
                    job.locked_at = None
                    job.last_error = RECOVERY_CANCEL_REASON
            mark_processed(event)
            outcomes.add(OUTCOME_APPLIED)
            applied.append(
                {
                    "event_id": event_id,
                    "record_id": target.id,
                    "record_marked_deleted": True,
                    "canceled_reminder_job_ids": queued_now,
                    "event_terminalized": True,
                    "already_recovered": False,
                }
            )

        await session.flush()
        await _assert_scope_only(session, frozen=frozen, applied=applied)
        after = await _scope_fingerprint(session)

    return ApplyResult(
        plan_digest=frozen.digest,
        outcome=OUTCOME_APPLIED if OUTCOME_APPLIED in outcomes else OUTCOME_ALREADY_APPLIED,
        events=tuple(applied),
        fingerprint_before=before,
        fingerprint_after=after,
        applied_at=moment,
    )


async def _assert_scope_only(
    session: AsyncSession,
    *,
    frozen: FrozenPlan,
    applied: Sequence[Mapping[str, Any]],
) -> None:
    """Prove, inside the transaction, that only the intended rows moved.

    Record-scoped on purpose. This module knows exactly one Record per event,
    so anything it could have created — a lifecycle job, a comeback, a review,
    an ``OutboxMessage`` — would be bound to that Record. Checking there is
    both precise and stable: a table-wide count would also move when capture
    stores an unrelated webhook or the outbox worker sends somebody else's
    message, and aborting a correct apply over that would be noise, not safety.

    Every row read here is already locked by the caller.
    """
    for row in applied:
        expected = _frozen_row(frozen, int(row["event_id"]))
        record_id = int(row["record_id"])
        # The plan's queued set, not this run's action list: a second apply of
        # the same plan withdraws nothing and must still recognise the jobs the
        # FIRST one withdrew as being in their intended end state.
        withdrawn = {int(value) for value in expected["queued_reminder_job_ids"]}

        promised = {int(item["id"]): item for item in expected["record_jobs"]}
        actual = {job.id: job for job in await _record_jobs(session, record_id)}
        if set(actual) != set(promised):
            # A job appeared or vanished under a lock we hold. Either way the
            # world is not the one the plan was frozen against.
            raise RecoveryError("message_job_created" if len(actual) > len(promised) else "job_state_changed")

        for job_id, job in actual.items():
            observed = _job_row(job)
            if job_id in withdrawn:
                if observed["status"] != "canceled" or observed["locked"] or job.last_error != RECOVERY_CANCEL_REASON:
                    raise RecoveryError("job_state_changed")
                # Only the withdrawal fields may differ from the frozen row.
                if {key: value for key, value in observed.items() if key not in ("status", "locked")} != {
                    key: value for key, value in promised[job_id].items() if key not in ("status", "locked")
                }:
                    raise RecoveryError("job_state_changed")
            elif observed != promised[job_id]:
                raise RecoveryError("job_state_changed")

        if await _record_outbox_ids(session, record_id) != [int(value) for value in expected["record_outbox_ids"]]:
            raise RecoveryError("outbox_message_created")


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


async def verify_recovery(
    session: AsyncSession,
    *,
    frozen: FrozenPlan,
    apply_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Read the end state back and prove it is exactly what the report claims.

    Read-only, PII-free, and safe to run as often as an operator likes.
    """
    failures: list[str] = []
    checked: list[dict[str, Any]] = []
    reported = {int(row["event_id"]): row for row in apply_report["events"]}
    if set(reported) != set(frozen.event_ids):
        failures.append("apply_report_scope_mismatch")

    for event_id in frozen.event_ids:
        row = _frozen_row(frozen, event_id)
        event = await session.get(EasyWeekEvent, event_id)
        record = await session.get(Record, int(row["record_id"]))
        item: dict[str, Any] = {"event_id": event_id, "record_id": row["record_id"], "problems": []}

        if event is None or record is None:
            item["problems"].append("target_row_missing")
            checked.append(item)
            failures.extend(item["problems"])
            continue

        if event.status != STATUS_PROCESSED:
            item["problems"].append("event_not_processed")
        if event.error_code is not None:
            item["problems"].append("event_error_code_not_cleared")
        if event.next_retry_at is not None:
            item["problems"].append("event_next_retry_not_cleared")
        if not bool(record.is_deleted):
            item["problems"].append("record_not_deleted")
        if _historical_state_digest(record) != row["record_state_digest"]:
            item["problems"].append("record_historical_state_changed")

        jobs = await _record_jobs(session, record.id)
        open_reminders = sorted(
            job.id
            for job in jobs
            if job.provider == PROVIDER_EASYWEEK
            and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
            and job.status in _OPEN_REMINDER_STATUSES
        )
        if open_reminders:
            item["problems"].append("open_easyweek_reminder_remains")
        expected_job_ids = sorted(int(entry["id"]) for entry in row["record_jobs"])
        if sorted(job.id for job in jobs) != expected_job_ids:
            item["problems"].append("record_job_set_changed")

        # Same reasoning as in the apply: the plan's queued set is the stable
        # answer to "which jobs were this recovery's to move", while an
        # `already_applied` report legitimately lists none.
        withdrawn = {int(value) for value in row["queued_reminder_job_ids"]}
        for job in jobs:
            observed = _job_row(job)
            if job.id in withdrawn:
                if observed["status"] != "canceled" or observed["locked"]:
                    item["problems"].append("withdrawn_reminder_reopened")
            elif observed not in row["record_jobs"]:
                item["problems"].append("unrelated_job_changed")

        if await _record_outbox_ids(session, record.id) != [int(value) for value in row["record_outbox_ids"]]:
            item["problems"].append("record_outbox_changed")

        siblings = await _sibling_events(
            session, booking_uuid=uuid_module.UUID(str(row["booking_uuid"])), exclude_event_id=event_id
        )
        if _digest([_event_row(entry) for entry in siblings]) != row["other_events_digest"]:
            item["problems"].append("sibling_event_changed")

        item["open_easyweek_reminder_ids"] = open_reminders
        checked.append(item)
        failures.extend(item["problems"])

    # Evidence, printed but not judged. See `_scope_fingerprint`: capture and
    # the workers keep running between apply and verify, so table-wide numbers
    # legitimately differ. What proves "nothing wider happened" is the
    # record-scoped comparison above, which apply made under locks and verify
    # repeats here.
    fingerprint = await _scope_fingerprint(session)
    counts = apply_report["mutation_counts"]
    if counts.get("message_jobs_created") or counts.get("outbox_messages_created"):
        failures.append("apply_report_claims_created_rows")

    return {
        "version": APPLY_REPORT_VERSION,
        "mode": MODE_VERIFY,
        "plan_digest": frozen.digest,
        "outcome": apply_report.get("outcome"),
        "events": checked,
        "mutation_counts": dict(counts),
        "scope_fingerprint": fingerprint,
        "failures": sorted(set(failures)),
        "passed": not failures,
    }
