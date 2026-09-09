"""Operator-controlled recovery of PR-7.4 reminder obligations.

This module is intentionally narrower than either webhook replay or a second
planner.  It can freeze a read-only proof of existing exactly-two-service
EasyWeek bookings, atomically create only still-future ``reminder_24h`` and
``reminder_2h`` jobs, and verify those exact rows.  It has no Meta, Chatwoot or
EasyWeek mutation capability.

The recovery projection is embedded in the new MessageJob because the recovery
contract expressly forbids changing the historical Record.  It is the same
bounded, PII-free and digest-protected projection used by normal PR-7.4 jobs.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_API_UNAVAILABLE,
    MULTI_SERVICE_CATEGORY_NOT_ALLOWED,
    MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED,
    MULTI_SERVICE_JOB_DIGEST_KEY,
    MultiServiceProofError,
    MultiServiceSnapshot,
    ServiceEligibilityPurpose,
    WebhookServicePair,
    evaluate_service_eligibility,
    multi_service_job_payload,
    multi_service_snapshot_from_job_payload,
    prove_exactly_two_service_snapshot,
    read_catalog_rows_cached,
    record_raw_with_multi_service_snapshot,
)
from altegio_bot.easyweek_normalizer import NormalizationError, normalize_event
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES, REMINDER_2H, REMINDER_24H
from altegio_bot.easyweek_reminder_guard import GuardResult, read_booking_state
from altegio_bot.easyweek_reminders import (
    REMINDER_OFFSETS,
    easyweek_multi_service_reminder_dedupe_key,
    plan_reminders,
    reminder_job_payload,
)
from altegio_bot.easyweek_service_category import (
    EASYWEEK_RAW_NAMESPACE,
    SERVICES_COUNT_SNAPSHOT_KEY,
    parse_allowed_service_categories,
    record_raw_with_services_count,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
    RecordService,
)
from altegio_bot.settings import settings

SNAPSHOT_VERSION: Final = 2
APPLY_REPORT_VERSION: Final = 2
DEFAULT_MAX_SNAPSHOT_AGE_SEC: Final = 600
MAX_SNAPSHOT_AGE_SEC: Final = 900
DEFAULT_LIMIT: Final = 500
DEFAULT_PAUSE_SEC: Final = 1.1
FILE_MODE: Final = 0o600
DIR_MODE: Final = 0o700

MODE_PLAN: Final = "plan"
MODE_APPLY: Final = "apply"
MODE_VERIFY: Final = "verify"

CREATE: Final = "create"
ALREADY_QUEUED: Final = "already_queued"
ALREADY_PROCESSING: Final = "already_processing"
ALREADY_DONE: Final = "already_done"
TERMINAL_HISTORY_PRESENT: Final = "terminal_history_present"
WINDOW_PASSED: Final = "window_passed"
CATEGORY_NOT_ALLOWED: Final = "category_not_allowed"
LIVE_BOOKING_NOT_ACTIVE: Final = "live_booking_not_active"
PROOF_FAILED: Final = "proof_failed"
CONTRACT_NOT_SUPPORTED: Final = "contract_not_supported"
IDENTITY_MISMATCH: Final = "identity_mismatch"
NON_TERMINAL_OUTBOX_PRESENT: Final = "non_terminal_outbox_present"

EXCLUSION_OPEN_REMINDER_JOB_IDS: Final = "open_reminder_job_ids"
EXCLUSION_IDENTITY_MISMATCH_JOB_IDS: Final = "identity_mismatch_job_ids"
EXCLUSION_NON_TERMINAL_OUTBOX_IDS: Final = "non_terminal_outbox_ids"

RECOVERY_PLAN_DIGEST_KEY: Final = "multi_service_recovery_plan_digest"
RECOVERY_SNAPSHOT_VERSION_KEY: Final = "multi_service_recovery_snapshot_version"

OPEN_JOB_STATUSES: Final = frozenset({"queued", "processing"})
DONE_JOB_STATUS: Final = "done"
TERMINAL_JOB_STATUSES: Final = frozenset({"canceled", "failed"})
NON_TERMINAL_OUTBOX_STATUSES: Final = frozenset({"queued", "sending"})
BLOCKING_DISPOSITIONS: Final = frozenset(
    {
        ALREADY_PROCESSING,
        TERMINAL_HISTORY_PRESENT,
        PROOF_FAILED,
        IDENTITY_MISMATCH,
        NON_TERMINAL_OUTBOX_PRESENT,
    }
)
_LIFECYCLE_HINTS: Final = (
    "booking-created",
    "booking-updated",
    "booking-rescheduled",
    "booking-canceled",
)
_PROOF_TRIGGER_KEYS: Final = frozenset(
    {
        "service_name",
        "service_related",
        "services_description",
        "services_count",
        "quantity",
        "booking_price",
        "booking_price_currency",
    }
)


class RecoveryReader(Protocol):
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...


class RecoveryError(RuntimeError):
    """A stable, PII-free refusal.  Text is safe for an operator terminal."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise RecoveryError("snapshot_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise RecoveryError("snapshot_timestamp_invalid") from None
    if parsed.tzinfo is None:
        raise RecoveryError("snapshot_timestamp_invalid")
    return _utc(parsed)


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def confirmation_phrase(plan_digest: str) -> str:
    return f"create easyweek multi-service reminders {plan_digest}"


def configuration_digest() -> str:
    """Digest the parsed routing and category security boundaries."""
    registry = configured_easyweek_locations()
    allowed = parse_allowed_service_categories(settings.easyweek_allowed_service_categories)
    locations = [
        {
            "company_id": item.company_id,
            "location_uuid": item.location_uuid,
            "name": item.name,
            "meta_template_prefix": item.meta_template_prefix,
            "booking_page_url": item.booking_page_url,
        }
        for item in sorted(registry.locations.values(), key=lambda value: value.company_id)
    ]
    return _digest(
        {
            "registry_configured": registry.configured,
            "registry_valid": registry.valid,
            "locations": locations,
            "allowed_configured": allowed.configured,
            "allowed_valid": allowed.valid,
            "allowed_category_keys": sorted(allowed.keys),
        }
    )


def _job_state(job: MessageJob) -> dict[str, object]:
    payload = job.payload if isinstance(job.payload, Mapping) else {}
    return {
        "id": job.id,
        "provider": job.provider,
        "company_id": job.company_id,
        "record_id": job.record_id,
        "client_id": job.client_id,
        "job_type": job.job_type,
        "run_at": _timestamp(job.run_at),
        "status": job.status,
        "dedupe_key": job.dedupe_key,
        "attempts": job.attempts,
        "locked": job.locked_at is not None,
        "payload_digest": _digest(payload),
        "recovery_plan_digest": payload.get(RECOVERY_PLAN_DIGEST_KEY),
    }


def _outbox_state(row: OutboxMessage) -> dict[str, object]:
    return {
        "id": row.id,
        "company_id": row.company_id,
        "record_id": row.record_id,
        "client_id": row.client_id,
        "job_id": row.job_id,
        "template_code": row.template_code,
        "status": row.status,
    }


def _business_pair_digest(snapshot: MultiServiceSnapshot) -> str:
    return _digest([line.business_signature_digest for line in snapshot.lines])


def _category_proof_digest(snapshot: MultiServiceSnapshot) -> str:
    return _digest([line.category for line in snapshot.lines])


def _contract_exclusion_context_digest(
    booking_payload: Mapping[str, Any],
    catalog_rows: list[object] | tuple[object, ...],
) -> str:
    """Freeze every non-PII input that can keep an unsupported proof stable."""
    return _digest(
        {
            "booking_contract": {
                key: booking_payload.get(key)
                for key in (
                    "uuid",
                    "location_uuid",
                    "start_time",
                    "is_canceled",
                    "is_completed",
                    "status",
                    "currency",
                    "order",
                    "ordered_services",
                )
            },
            "catalog": catalog_rows,
        }
    )


def _snapshot_projection(snapshot: MultiServiceSnapshot) -> dict[str, object]:
    # This is PII-free and is needed to render and re-prove a recovery-created
    # job without writing the historical Record.
    return snapshot.as_dict()


def _record_identity(record: Record, client: Client | None) -> dict[str, object]:
    return {
        "record_id": record.id,
        "provider": record.provider,
        "company_id": record.company_id,
        "client_id": record.client_id,
        "client_provider": client.provider if client is not None else None,
        "client_company_id": client.company_id if client is not None else None,
        "booking_uuid": str(record.easyweek_booking_uuid) if record.easyweek_booking_uuid is not None else None,
        "starts_at": _timestamp(record.starts_at) if record.starts_at is not None else None,
        "is_deleted": bool(record.is_deleted),
        "total_cost": str(record.total_cost) if record.total_cost is not None else None,
    }


def _record_state_digest(record: Record) -> str:
    """Hash mutable Record state without exposing customer or service data."""
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
            "starts_at": _timestamp(record.starts_at) if record.starts_at is not None else None,
            "ends_at": _timestamp(record.ends_at) if record.ends_at is not None else None,
            "duration_sec": record.duration_sec,
            "comment": record.comment,
            "short_link": record.short_link,
            "confirmed": record.confirmed,
            "attendance": record.attendance,
            "visit_attendance": record.visit_attendance,
            "is_deleted": bool(record.is_deleted),
            "total_cost": str(record.total_cost) if record.total_cost is not None else None,
            "last_change_at": _timestamp(record.last_change_at) if record.last_change_at is not None else None,
            "raw": record.raw,
        }
    )


def _client_state_digest(client: Client | None) -> str | None:
    """Hash mutable Client state; the private plan stores only the digest."""
    if client is None:
        return None
    return _digest(
        {
            "provider": client.provider,
            "company_id": client.company_id,
            "altegio_client_id": client.altegio_client_id,
            "phone_e164": client.phone_e164,
            "display_name": client.display_name,
            "email": client.email,
            "raw": client.raw,
            "wa_opted_out": bool(client.wa_opted_out),
            "wa_opted_out_at": _timestamp(client.wa_opted_out_at) if client.wa_opted_out_at is not None else None,
            "wa_opt_out_reason": client.wa_opt_out_reason,
            "easyweek_visits_total": client.easyweek_visits_total,
            "easyweek_visits_total_updated_at": (
                _timestamp(client.easyweek_visits_total_updated_at)
                if client.easyweek_visits_total_updated_at is not None
                else None
            ),
        }
    )


async def _record_service_state_digests(
    session: AsyncSession,
    records: list[Record],
    *,
    lock: bool = False,
) -> dict[int, str]:
    record_ids = [record.id for record in records]
    grouped: dict[int, list[dict[str, object]]] = {record_id: [] for record_id in record_ids}
    if record_ids:
        stmt = (
            select(RecordService)
            .where(RecordService.record_id.in_(record_ids))
            .order_by(RecordService.record_id.asc(), RecordService.service_id.asc())
        )
        if lock:
            stmt = stmt.with_for_update()
        for service in (await session.execute(stmt)).scalars():
            grouped[service.record_id].append(
                {
                    "service_id": service.service_id,
                    "title": service.title,
                    "amount": service.amount,
                    "cost_to_pay": str(service.cost_to_pay) if service.cost_to_pay is not None else None,
                    "raw": service.raw,
                }
            )
    return {record_id: _digest(rows) for record_id, rows in grouped.items()}


def _identity_matches(record: Record, client: Client | None) -> bool:
    return bool(
        record.provider == PROVIDER_EASYWEEK
        and record.client_id is not None
        and client is not None
        and client.id == record.client_id
        and client.provider == PROVIDER_EASYWEEK
        and client.company_id == record.company_id
        and record.easyweek_booking_uuid is not None
        and record.starts_at is not None
        and not record.is_deleted
    )


def _excluded_reminder_identity_matches(record: Record, job: MessageJob) -> bool:
    payload = job.payload if isinstance(job.payload, Mapping) else {}
    try:
        payload_start = _parse_timestamp(payload.get("record_starts_at"))
    except RecoveryError:
        return False
    return bool(
        job.provider == PROVIDER_EASYWEEK
        and job.company_id == record.company_id
        and job.record_id == record.id
        and job.client_id == record.client_id
        and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
        and record.starts_at is not None
        and record.easyweek_booking_uuid is not None
        and _utc(job.run_at) == _utc(record.starts_at) - REMINDER_OFFSETS[job.job_type]
        and payload.get("provider") == PROVIDER_EASYWEEK
        and payload.get("booking_uuid") == str(record.easyweek_booking_uuid)
        and payload.get("company_id") == record.company_id
        and payload.get("job_type") == job.job_type
        and payload_start == _utc(record.starts_at)
    )


def _contract_exclusion_blockers(
    *,
    record: Record,
    jobs: list[MessageJob],
    outboxes: list[OutboxMessage],
) -> dict[str, list[int]]:
    open_reminders = [
        job for job in jobs if job.job_type in EASYWEEK_REMINDER_JOB_TYPES and job.status in OPEN_JOB_STATUSES
    ]
    return {
        EXCLUSION_OPEN_REMINDER_JOB_IDS: sorted(job.id for job in open_reminders),
        EXCLUSION_IDENTITY_MISMATCH_JOB_IDS: sorted(
            job.id for job in open_reminders if not _excluded_reminder_identity_matches(record, job)
        ),
        EXCLUSION_NON_TERMINAL_OUTBOX_IDS: sorted(
            row.id for row in outboxes if row.status in NON_TERMINAL_OUTBOX_STATUSES
        ),
    }


async def _select_records(
    session: AsyncSession,
    *,
    now: datetime,
    limit: int,
) -> tuple[list[Record], bool]:
    stmt = (
        select(Record)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at.is_not(None), Record.starts_at > now)
        .where(Record.easyweek_booking_uuid.is_not(None))
        .where(Record.raw.contains({EASYWEEK_RAW_NAMESPACE: {SERVICES_COUNT_SNAPSHOT_KEY: 2}}))
        .order_by(Record.starts_at.asc(), Record.id.asc())
        .limit(limit + 1)
    )
    records = list((await session.execute(stmt)).scalars().all())
    return records[:limit], len(records) > limit


async def _latest_proof_events(
    session: AsyncSession,
    records: list[Record],
) -> dict[uuid.UUID, EasyWeekEvent]:
    booking_uuids = [record.easyweek_booking_uuid for record in records if record.easyweek_booking_uuid is not None]
    if not booking_uuids:
        return {}
    stmt = (
        select(EasyWeekEvent)
        .where(EasyWeekEvent.booking_uuid.in_(booking_uuids))
        .where(EasyWeekEvent.event_hint.in_(_LIFECYCLE_HINTS))
        .order_by(EasyWeekEvent.received_at.desc(), EasyWeekEvent.id.desc())
    )
    result: dict[uuid.UUID, EasyWeekEvent] = {}
    for event in (await session.execute(stmt)).scalars():
        payload = event.payload if isinstance(event.payload, Mapping) else {}
        if event.booking_uuid not in result and _PROOF_TRIGGER_KEYS & payload.keys():
            result[event.booking_uuid] = event
    return result


async def _scope_state(
    session: AsyncSession,
    records: list[Record],
    *,
    lock: bool = False,
) -> tuple[dict[int, Client], dict[int, list[MessageJob]], dict[int, list[OutboxMessage]]]:
    client_ids = [record.client_id for record in records if record.client_id is not None]
    client_stmt = select(Client).where(Client.id.in_(client_ids))
    if lock:
        client_stmt = client_stmt.with_for_update()
    clients = {item.id: item for item in ((await session.execute(client_stmt)).scalars().all() if client_ids else [])}
    record_ids = [record.id for record in records]
    jobs_by_record: dict[int, list[MessageJob]] = {}
    outbox_by_record: dict[int, list[OutboxMessage]] = {}
    if record_ids:
        jobs_stmt = select(MessageJob).where(MessageJob.record_id.in_(record_ids)).order_by(MessageJob.id.asc())
        outbox_stmt = (
            select(OutboxMessage).where(OutboxMessage.record_id.in_(record_ids)).order_by(OutboxMessage.id.asc())
        )
        if lock:
            jobs_stmt = jobs_stmt.with_for_update()
            outbox_stmt = outbox_stmt.with_for_update()
        jobs = list((await session.execute(jobs_stmt)).scalars().all())
        for job in jobs:
            if job.record_id is not None:
                jobs_by_record.setdefault(job.record_id, []).append(job)
        outboxes = list((await session.execute(outbox_stmt)).scalars().all())
        for row in outboxes:
            if row.record_id is not None:
                outbox_by_record.setdefault(row.record_id, []).append(row)
    return clients, jobs_by_record, outbox_by_record


def _reminder_disposition(
    *,
    dedupe_key: str,
    jobs: list[MessageJob],
    outboxes: list[OutboxMessage],
) -> str:
    exact = [job for job in jobs if job.dedupe_key == dedupe_key]
    if not exact:
        # A non-terminal outbox for the same reminder type but an unreadable or
        # missing job link is uncertainty, never permission to create another.
        if any(row.status in NON_TERMINAL_OUTBOX_STATUSES for row in outboxes):
            return NON_TERMINAL_OUTBOX_PRESENT
        return CREATE
    job = exact[0]
    linked = [row for row in outboxes if row.job_id == job.id]
    if any(row.status in NON_TERMINAL_OUTBOX_STATUSES for row in linked):
        return NON_TERMINAL_OUTBOX_PRESENT
    if job.status == "queued":
        return ALREADY_QUEUED
    if job.status == "processing":
        return ALREADY_PROCESSING
    if job.status == DONE_JOB_STATUS:
        return ALREADY_DONE
    return TERMINAL_HISTORY_PRESENT


def _reminder_rows(
    *,
    record: Record,
    snapshot: MultiServiceSnapshot | None,
    now: datetime,
    jobs: list[MessageJob],
    outboxes: list[OutboxMessage],
    forced_disposition: str | None,
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    assert record.starts_at is not None and record.easyweek_booking_uuid is not None
    future = {
        item.job_type: item
        for item in plan_reminders(
            booking_uuid=record.easyweek_booking_uuid,
            starts_at=record.starts_at,
            now=now,
            is_deleted=bool(record.is_deleted),
        )
    }
    for job_type in (REMINDER_24H, REMINDER_2H):
        planned = future.get(job_type)
        run_at = planned.run_at if planned is not None else _utc(record.starts_at) - REMINDER_OFFSETS[job_type]
        dedupe_key = (
            easyweek_multi_service_reminder_dedupe_key(
                booking_uuid=record.easyweek_booking_uuid,
                job_type=job_type,
                starts_at=record.starts_at,
                multi_service_snapshot_digest=snapshot.digest,
            )
            if snapshot is not None
            else None
        )
        disposition = forced_disposition
        if disposition is None and snapshot is not None and dedupe_key is not None:
            existing = _reminder_disposition(dedupe_key=dedupe_key, jobs=jobs, outboxes=outboxes)
            disposition = WINDOW_PASSED if planned is None and existing == CREATE else existing
        result.append(
            {
                "job_type": job_type,
                "run_at": _timestamp(run_at),
                "dedupe_key": dedupe_key,
                "disposition": disposition or PROOF_FAILED,
                "existing_job_ids": [job.id for job in jobs if dedupe_key is not None and job.dedupe_key == dedupe_key],
                "existing_outbox_ids": [
                    row.id
                    for row in outboxes
                    if any(
                        job.id == row.job_id for job in jobs if dedupe_key is not None and job.dedupe_key == dedupe_key
                    )
                ],
            }
        )
    return result


@dataclass(frozen=True)
class RecoveryPlan:
    planned_at: datetime
    configuration_digest: str
    records: tuple[dict[str, object], ...]
    summary: dict[str, object]
    truncated: bool = False

    def unsigned(self) -> dict[str, object]:
        return {
            "version": SNAPSHOT_VERSION,
            "mode": MODE_PLAN,
            "planned_at": _timestamp(self.planned_at),
            "configuration_digest": self.configuration_digest,
            "truncated": self.truncated,
            "records": list(self.records),
            "summary": self.summary,
        }

    @property
    def plan_digest(self) -> str:
        return _digest(self.unsigned())

    def snapshot(self) -> dict[str, object]:
        return {**self.unsigned(), "plan_digest": self.plan_digest}

    def safe_report(self) -> dict[str, object]:
        return {
            "mode": "read-only plan",
            **self.summary,
            "plan_digest": self.plan_digest,
            "snapshot_contains_booking_uuids": True,
        }


def _summary(records: list[dict[str, object]], *, truncated: bool) -> dict[str, object]:
    reminders = [item for record in records for item in record["reminders"]]  # type: ignore[index]
    dispositions = [item["disposition"] for item in reminders]  # type: ignore[index]
    structural = [record for record in records if record.get("multi_service_snapshot_digest")]
    allowed = [record for record in records if record.get("eligibility") == "allowed"]
    disallowed = [record for record in records if record.get("eligibility") == CATEGORY_NOT_ALLOWED]
    contract_excluded = [record for record in records if record.get("eligibility") == CONTRACT_NOT_SUPPORTED]
    exclusion_blocker_count = sum(
        bool(ids)
        for record in contract_excluded
        for ids in record.get("contract_exclusion_blockers", {}).values()  # type: ignore[union-attr]
    )
    blocker_count = sum(value in BLOCKING_DISPOSITIONS for value in dispositions) + exclusion_blocker_count
    create_count = dispositions.count(CREATE)
    return {
        "records_seen": len(records),
        "structurally_proven": len(structural),
        "allowed_records": len(allowed),
        "disallowed_records": len(disallowed),
        "contract_excluded_records": len(contract_excluded),
        "contract_excluded_by_reason": {
            MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED: len(contract_excluded),
        },
        "contract_exclusion_blockers": exclusion_blocker_count,
        "reminders_to_create": create_count,
        "reminder_24h_to_create": sum(
            item["job_type"] == REMINDER_24H and item["disposition"] == CREATE
            for item in reminders  # type: ignore[index]
        ),
        "reminder_2h_to_create": sum(
            item["job_type"] == REMINDER_2H and item["disposition"] == CREATE
            for item in reminders  # type: ignore[index]
        ),
        "windows_passed": dispositions.count(WINDOW_PASSED),
        "already_satisfied": sum(value in {ALREADY_QUEUED, ALREADY_DONE} for value in dispositions),
        "blockers": blocker_count,
        "truncated": truncated,
        "apply_ready": bool(records and not blocker_count and not truncated),
    }


async def build_recovery_plan(
    session: AsyncSession,
    *,
    client: RecoveryReader,
    now: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> RecoveryPlan:
    """Read and freeze the complete current scope.  No ORM object is changed."""
    moment = _utc(now or datetime.now(timezone.utc))
    records, truncated = await _select_records(session, now=moment, limit=limit)
    events = await _latest_proof_events(session, records)
    clients, jobs_by_record, outbox_by_record = await _scope_state(session, records)
    service_state_digests = await _record_service_state_digests(session, records)
    registry = configured_easyweek_locations()
    pause = sleep if sleep is not None else asyncio.sleep
    rows: list[dict[str, object]] = []
    api_calls = 0

    for record in records:
        client_row = clients.get(record.client_id) if record.client_id is not None else None
        jobs = jobs_by_record.get(record.id, [])
        outboxes = outbox_by_record.get(record.id, [])
        identity = _record_identity(record, client_row)
        event = events.get(record.easyweek_booking_uuid) if record.easyweek_booking_uuid is not None else None
        location = registry.locations.get(record.company_id) if registry.ready else None
        snapshot: MultiServiceSnapshot | None = None
        eligibility = PROOF_FAILED
        refusal_reason: str | None = None
        forced: str | None = None
        contract_exclusion_context_digest: str | None = None
        contract_blockers: dict[str, list[int]] = {
            EXCLUSION_OPEN_REMINDER_JOB_IDS: [],
            EXCLUSION_IDENTITY_MISMATCH_JOB_IDS: [],
            EXCLUSION_NON_TERMINAL_OUTBOX_IDS: [],
        }

        if not _identity_matches(record, client_row) or event is None or location is None:
            forced = IDENTITY_MISMATCH
            refusal_reason = IDENTITY_MISMATCH
        else:
            try:
                booking = normalize_event(
                    event_hint=event.event_hint,
                    payload=event.payload,
                    body_truncated=bool(event.body_truncated),
                    location_registry=registry.locations,
                )
            except NormalizationError:
                booking = None
            if (
                booking is None
                or booking.booking_uuid != record.easyweek_booking_uuid
                or booking.company_id != record.company_id
            ):
                forced = IDENTITY_MISMATCH
                refusal_reason = IDENTITY_MISMATCH
            else:
                if api_calls:
                    await pause(pause_sec)
                api_calls += 1
                try:
                    live_payload = await client.get_booking(str(record.easyweek_booking_uuid))
                    observed = read_booking_state(
                        live_payload,
                        booking_uuid=record.easyweek_booking_uuid,
                        location=location,
                    )
                    if isinstance(observed, GuardResult):
                        forced = IDENTITY_MISMATCH
                        refusal_reason = IDENTITY_MISMATCH
                    elif not observed.is_active:
                        forced = LIVE_BOOKING_NOT_ACTIVE
                        refusal_reason = LIVE_BOOKING_NOT_ACTIVE
                    elif _utc(observed.starts_at) != _utc(record.starts_at):
                        forced = IDENTITY_MISMATCH
                        refusal_reason = IDENTITY_MISMATCH
                    else:
                        catalog = await read_catalog_rows_cached(client, location_uuid=location.location_uuid)
                        snapshot = prove_exactly_two_service_snapshot(
                            webhook=WebhookServicePair(
                                booking_uuid=booking.booking_uuid,
                                location_uuid=location.location_uuid,
                                service_name=booking.service_name,
                                service_related=booking.service_related,
                                services_description=booking.services_description,
                                services_count=booking.services_count,
                                quantity=booking.service_quantity,
                                booking_currency=booking.booking_currency,
                                total_cost=record.total_cost,
                            ),
                            booking_payload=live_payload,
                            catalog_rows=catalog,
                        )
                        proof_raw = record_raw_with_multi_service_snapshot(
                            record_raw_with_services_count(record.raw, 2), snapshot
                        )
                        decision = evaluate_service_eligibility(
                            record_raw=proof_raw,
                            allowed_categories_raw=settings.easyweek_allowed_service_categories,
                            purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
                        )
                        if decision.allowed:
                            eligibility = "allowed"
                        elif decision.reason == MULTI_SERVICE_CATEGORY_NOT_ALLOWED:
                            eligibility = CATEGORY_NOT_ALLOWED
                            forced = CATEGORY_NOT_ALLOWED
                            refusal_reason = CATEGORY_NOT_ALLOWED
                        else:
                            forced = PROOF_FAILED
                            refusal_reason = decision.reason
                except MultiServiceProofError as exc:
                    if exc.reason == MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED:
                        eligibility = CONTRACT_NOT_SUPPORTED
                        forced = CONTRACT_NOT_SUPPORTED
                        contract_exclusion_context_digest = _contract_exclusion_context_digest(
                            live_payload,
                            catalog,
                        )
                    else:
                        forced = PROOF_FAILED
                    refusal_reason = exc.reason
                except Exception:  # noqa: BLE001 - exception text can carry API material
                    forced = PROOF_FAILED
                    refusal_reason = MULTI_SERVICE_API_UNAVAILABLE

        if eligibility == CONTRACT_NOT_SUPPORTED:
            contract_blockers = _contract_exclusion_blockers(
                record=record,
                jobs=jobs,
                outboxes=outboxes,
            )

        reminders = _reminder_rows(
            record=record,
            snapshot=snapshot,
            now=moment,
            jobs=jobs,
            outboxes=outboxes,
            forced_disposition=forced,
        )
        rows.append(
            {
                **identity,
                "record_state_digest": _record_state_digest(record),
                "client_state_digest": _client_state_digest(client_row),
                "record_services_state_digest": service_state_digests[record.id],
                "proof_event_id": event.id if event is not None else None,
                "location_uuid": location.location_uuid if location is not None else None,
                "multi_service_snapshot_digest": snapshot.digest if snapshot is not None else None,
                "live_business_pair_digest": _business_pair_digest(snapshot) if snapshot is not None else None,
                "category_proof_digest": _category_proof_digest(snapshot) if snapshot is not None else None,
                "multi_service_snapshot": _snapshot_projection(snapshot) if snapshot is not None else None,
                "eligibility": eligibility,
                "refusal_reason": refusal_reason,
                "contract_exclusion_context_digest": contract_exclusion_context_digest,
                "contract_exclusion_blockers": contract_blockers,
                "reminders": reminders,
                "existing_jobs": [_job_state(job) for job in jobs],
                "existing_outboxes": [_outbox_state(row) for row in outboxes],
            }
        )

    summary = _summary(rows, truncated=truncated)
    return RecoveryPlan(
        planned_at=moment,
        configuration_digest=configuration_digest(),
        records=tuple(rows),
        summary=summary,
        truncated=truncated,
    )


@dataclass(frozen=True)
class FrozenRecoveryPlan:
    payload: dict[str, Any]

    @property
    def digest(self) -> str:
        return str(self.payload["plan_digest"])

    @property
    def planned_at(self) -> datetime:
        return _parse_timestamp(self.payload["planned_at"])

    @property
    def records(self) -> list[dict[str, Any]]:
        return self.payload["records"]

    @property
    def apply_ready(self) -> bool:
        return bool(self.payload["summary"].get("apply_ready"))


def write_private_json(payload: Mapping[str, object], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(target.parent, DIR_MODE)
    temporary = target.with_suffix(target.suffix + ".tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, FILE_MODE)
    try:
        os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, target)
    os.chmod(target, FILE_MODE)
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return target


def write_snapshot(plan: RecoveryPlan, path: str | Path) -> Path:
    return write_private_json(plan.snapshot(), path)


def read_snapshot(path: str | Path) -> FrozenRecoveryPlan:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RecoveryError("snapshot_unreadable") from None
    if not isinstance(payload, dict) or payload.get("version") != SNAPSHOT_VERSION or payload.get("mode") != MODE_PLAN:
        raise RecoveryError("snapshot_version_unsupported")
    required = {
        "version",
        "mode",
        "planned_at",
        "configuration_digest",
        "truncated",
        "records",
        "summary",
        "plan_digest",
    }
    if (
        set(payload) != required
        or not isinstance(payload.get("records"), list)
        or not isinstance(payload.get("summary"), dict)
    ):
        raise RecoveryError("snapshot_schema_invalid")
    for row in payload["records"]:
        exclusion_blockers = row.get("contract_exclusion_blockers") if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or type(row.get("record_id")) is not int
            or type(row.get("company_id")) is not int
            or type(row.get("client_id")) is not int
            or row.get("provider") != PROVIDER_EASYWEEK
            or not isinstance(row.get("reminders"), list)
            or not isinstance(row.get("existing_jobs"), list)
            or not isinstance(row.get("existing_outboxes"), list)
            or not _valid_digest(row.get("record_state_digest"))
            or not _valid_digest(row.get("record_services_state_digest"))
            or (row.get("client_state_digest") is not None and not _valid_digest(row.get("client_state_digest")))
            or not isinstance(exclusion_blockers, dict)
            or set(exclusion_blockers)
            != {
                EXCLUSION_OPEN_REMINDER_JOB_IDS,
                EXCLUSION_IDENTITY_MISMATCH_JOB_IDS,
                EXCLUSION_NON_TERMINAL_OUTBOX_IDS,
            }
            or any(
                not isinstance(ids, list) or any(type(item) is not int for item in ids)
                for ids in exclusion_blockers.values()
            )
        ):
            raise RecoveryError("snapshot_schema_invalid")
        if len(row["reminders"]) != 2 or {
            item.get("job_type") for item in row["reminders"] if isinstance(item, dict)
        } != {
            REMINDER_24H,
            REMINDER_2H,
        }:
            raise RecoveryError("snapshot_schema_invalid")
        try:
            booking_uuid = uuid.UUID(str(row.get("booking_uuid")))
            if str(booking_uuid) != row.get("booking_uuid"):
                raise ValueError
            record_start = _parse_timestamp(row.get("starts_at"))
        except (RecoveryError, TypeError, ValueError):
            raise RecoveryError("snapshot_schema_invalid") from None
        if any(type(item) is not dict or type(item.get("id")) is not int for item in row["existing_jobs"]):
            raise RecoveryError("snapshot_schema_invalid")
        if any(type(item) is not dict or type(item.get("id")) is not int for item in row["existing_outboxes"]):
            raise RecoveryError("snapshot_schema_invalid")
        for reminder in row["reminders"]:
            if (
                not isinstance(reminder, dict)
                or reminder.get("job_type") not in EASYWEEK_REMINDER_JOB_TYPES
                or reminder.get("disposition")
                not in {
                    CREATE,
                    ALREADY_QUEUED,
                    ALREADY_PROCESSING,
                    ALREADY_DONE,
                    TERMINAL_HISTORY_PRESENT,
                    WINDOW_PASSED,
                    CATEGORY_NOT_ALLOWED,
                    LIVE_BOOKING_NOT_ACTIVE,
                    PROOF_FAILED,
                    IDENTITY_MISMATCH,
                    NON_TERMINAL_OUTBOX_PRESENT,
                    CONTRACT_NOT_SUPPORTED,
                }
            ):
                raise RecoveryError("snapshot_schema_invalid")
            run_at = _parse_timestamp(reminder.get("run_at"))
            if run_at != record_start - REMINDER_OFFSETS[str(reminder["job_type"])]:
                raise RecoveryError("snapshot_schema_invalid")
            if reminder.get("disposition") == CREATE:
                raw_snapshot = row.get("multi_service_snapshot")
                snapshot, error = multi_service_snapshot_from_job_payload({"multi_service_snapshot": raw_snapshot})
                expected_key = (
                    easyweek_multi_service_reminder_dedupe_key(
                        booking_uuid=booking_uuid,
                        job_type=str(reminder["job_type"]),
                        starts_at=record_start,
                        multi_service_snapshot_digest=snapshot.digest,
                    )
                    if snapshot is not None
                    else None
                )
                if (
                    snapshot is None
                    or error is not None
                    or row.get("eligibility") != "allowed"
                    or row.get("multi_service_snapshot_digest") != snapshot.digest
                    or reminder.get("dedupe_key") != expected_key
                ):
                    raise RecoveryError("snapshot_schema_invalid")
        if row.get("eligibility") == CONTRACT_NOT_SUPPORTED:
            expected_open_ids = sorted(
                int(item["id"])
                for item in row["existing_jobs"]
                if item.get("job_type") in EASYWEEK_REMINDER_JOB_TYPES and item.get("status") in OPEN_JOB_STATUSES
            )
            expected_outbox_ids = sorted(
                int(item["id"])
                for item in row["existing_outboxes"]
                if item.get("status") in NON_TERMINAL_OUTBOX_STATUSES
            )
            if (
                row.get("refusal_reason") != MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED
                or not _valid_digest(row.get("contract_exclusion_context_digest"))
                or any(
                    row.get(key) is not None
                    for key in (
                        "multi_service_snapshot_digest",
                        "live_business_pair_digest",
                        "category_proof_digest",
                        "multi_service_snapshot",
                    )
                )
                or any(
                    reminder.get("disposition") != CONTRACT_NOT_SUPPORTED or reminder.get("dedupe_key") is not None
                    for reminder in row["reminders"]
                )
                or exclusion_blockers[EXCLUSION_OPEN_REMINDER_JOB_IDS] != expected_open_ids
                or not set(exclusion_blockers[EXCLUSION_IDENTITY_MISMATCH_JOB_IDS]).issubset(expected_open_ids)
                or exclusion_blockers[EXCLUSION_NON_TERMINAL_OUTBOX_IDS] != expected_outbox_ids
            ):
                raise RecoveryError("snapshot_schema_invalid")
        elif (
            row.get("contract_exclusion_context_digest") is not None
            or any(exclusion_blockers.values())
            or any(reminder.get("disposition") == CONTRACT_NOT_SUPPORTED for reminder in row["reminders"])
        ):
            raise RecoveryError("snapshot_schema_invalid")
    _parse_timestamp(payload["planned_at"])
    embedded = payload.get("plan_digest")
    unsigned = dict(payload)
    del unsigned["plan_digest"]
    if not isinstance(embedded, str) or embedded != _digest(unsigned):
        raise RecoveryError("snapshot_digest_mismatch")
    if payload["summary"] != _summary(payload["records"], truncated=bool(payload["truncated"])):
        raise RecoveryError("snapshot_summary_invalid")
    return FrozenRecoveryPlan(payload=payload)


def check_apply_authorization(
    frozen: FrozenRecoveryPlan,
    *,
    supplied_digest: str | None,
    supplied_confirmation: str | None,
    now: datetime,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> None:
    if supplied_digest != frozen.digest:
        raise RecoveryError("plan_digest_mismatch")
    if supplied_confirmation != confirmation_phrase(frozen.digest):
        raise RecoveryError("confirmation_mismatch")
    age = (_utc(now) - frozen.planned_at).total_seconds()
    if age < 0 or age > min(max_age_sec, MAX_SNAPSHOT_AGE_SEC):
        raise RecoveryError("snapshot_expired")
    if not frozen.apply_ready:
        raise RecoveryError("snapshot_not_apply_ready")
    if frozen.payload.get("configuration_digest") != configuration_digest():
        raise RecoveryError("configuration_digest_changed")
    if not bool(settings.easyweek_multi_service_notifications_enabled):
        raise RecoveryError("multi_service_planning_fence_closed")
    if bool(settings.easyweek_multi_service_send_enabled):
        raise RecoveryError("multi_service_send_fence_open")
    if not bool(settings.easyweek_reminders_enabled):
        raise RecoveryError("reminder_planning_disabled")
    if not bool(settings.easyweek_reminder_api_guard_enabled):
        raise RecoveryError("reminder_api_guard_disabled")


def _plan_stable_view(record: Mapping[str, Any]) -> dict[str, object]:
    keys = (
        "record_id",
        "provider",
        "company_id",
        "client_id",
        "client_provider",
        "client_company_id",
        "booking_uuid",
        "starts_at",
        "is_deleted",
        "total_cost",
        "proof_event_id",
        "location_uuid",
        "multi_service_snapshot_digest",
        "live_business_pair_digest",
        "category_proof_digest",
        "multi_service_snapshot",
        "eligibility",
        "refusal_reason",
        "record_state_digest",
        "client_state_digest",
        "record_services_state_digest",
        "contract_exclusion_context_digest",
        "contract_exclusion_blockers",
    )
    return {key: record.get(key) for key in keys}


def _created_by_this_plan(job: Mapping[str, Any], *, plan_digest: str) -> bool:
    return job.get("recovery_plan_digest") == plan_digest


def _expected_payload(
    *,
    record: Mapping[str, Any],
    reminder: Mapping[str, Any],
    plan_digest: str,
) -> dict[str, object]:
    raw_snapshot = record.get("multi_service_snapshot")
    snapshot, error = multi_service_snapshot_from_job_payload({"multi_service_snapshot": raw_snapshot})
    if snapshot is None:
        raise RecoveryError(error or "snapshot_projection_invalid")
    try:
        booking_uuid = uuid.UUID(str(record["booking_uuid"]))
        starts_at = _parse_timestamp(record["starts_at"])
        company_id = int(record["company_id"])
        job_type = str(reminder["job_type"])
    except (KeyError, TypeError, ValueError):
        raise RecoveryError("snapshot_schema_invalid") from None
    return {
        **reminder_job_payload(
            booking_uuid=booking_uuid,
            company_id=company_id,
            starts_at=starts_at,
            job_type=job_type,
        ),
        **multi_service_job_payload(snapshot, include_snapshot=True),
        RECOVERY_PLAN_DIGEST_KEY: plan_digest,
        RECOVERY_SNAPSHOT_VERSION_KEY: SNAPSHOT_VERSION,
    }


def compare_revalidated_plan(
    frozen: FrozenRecoveryPlan,
    current: RecoveryPlan,
    *,
    now: datetime,
) -> None:
    """Permit only exact state, plus rows made by an earlier identical apply."""
    if current.configuration_digest != frozen.payload["configuration_digest"]:
        raise RecoveryError("configuration_digest_changed")
    if current.truncated or len(current.records) != len(frozen.records):
        raise RecoveryError("candidate_scope_changed")
    old_by_id = {row.get("record_id"): row for row in frozen.records}
    new_by_id = {row.get("record_id"): row for row in current.records}
    if set(old_by_id) != set(new_by_id):
        raise RecoveryError("candidate_scope_changed")
    for record_id, old in old_by_id.items():
        new = new_by_id[record_id]
        if _plan_stable_view(old) != _plan_stable_view(new):
            raise RecoveryError("live_scope_changed")
        old_jobs = {item["id"]: item for item in old.get("existing_jobs", [])}
        new_jobs = {item["id"]: item for item in new.get("existing_jobs", [])}
        extra_jobs = [item for job_id, item in new_jobs.items() if job_id not in old_jobs]
        expected_keys = {
            item.get("dedupe_key") for item in old.get("reminders", []) if item.get("disposition") == CREATE
        }
        reminders_by_key = {
            item.get("dedupe_key"): item for item in old.get("reminders", []) if item.get("disposition") == CREATE
        }
        for item in extra_jobs:
            key = item.get("dedupe_key")
            reminder = reminders_by_key.get(key)
            if (
                key not in expected_keys
                or reminder is None
                or not _created_by_this_plan(item, plan_digest=frozen.digest)
                or item.get("provider") != PROVIDER_EASYWEEK
                or item.get("company_id") != old.get("company_id")
                or item.get("record_id") != old.get("record_id")
                or item.get("client_id") != old.get("client_id")
                or item.get("job_type") != reminder.get("job_type")
                or item.get("run_at") != reminder.get("run_at")
                or item.get("payload_digest")
                != _digest(_expected_payload(record=old, reminder=reminder, plan_digest=frozen.digest))
                or item.get("status") == "processing"
            ):
                raise RecoveryError("job_state_changed")
        for job_id, old_job in old_jobs.items():
            if new_jobs.get(job_id) != old_job:
                raise RecoveryError("job_state_changed")
        old_outboxes = {item["id"]: item for item in old.get("existing_outboxes", [])}
        new_outboxes = {item["id"]: item for item in new.get("existing_outboxes", [])}
        if new_outboxes != old_outboxes:
            raise RecoveryError("outbox_state_changed")
        for reminder in old.get("reminders", []):
            if reminder.get("disposition") == CREATE and _parse_timestamp(reminder.get("run_at")) <= _utc(now):
                raise RecoveryError("reminder_window_passed")


async def _lock_and_compare_local_state(
    session: AsyncSession,
    *,
    current: RecoveryPlan,
) -> list[Record]:
    ids = [int(row["record_id"]) for row in current.records]
    locked = list(
        (await session.execute(select(Record).where(Record.id.in_(ids)).order_by(Record.id.asc()).with_for_update()))
        .scalars()
        .all()
    )
    if [row.id for row in locked] != sorted(ids):
        raise RecoveryError("record_state_changed")
    clients, jobs, outboxes = await _scope_state(session, locked, lock=True)
    service_state_digests = await _record_service_state_digests(session, locked, lock=True)
    expected = {int(row["record_id"]): row for row in current.records}
    for record in locked:
        client = clients.get(record.client_id) if record.client_id is not None else None
        row = expected[record.id]
        if (
            _record_identity(record, client) != {key: row.get(key) for key in _record_identity(record, client)}
            or _record_state_digest(record) != row.get("record_state_digest")
            or _client_state_digest(client) != row.get("client_state_digest")
            or service_state_digests[record.id] != row.get("record_services_state_digest")
        ):
            raise RecoveryError("record_state_changed")
        if [_job_state(item) for item in jobs.get(record.id, [])] != row.get("existing_jobs", []):
            raise RecoveryError("job_state_changed")
        if [_outbox_state(item) for item in outboxes.get(record.id, [])] != row.get("existing_outboxes", []):
            raise RecoveryError("outbox_state_changed")
    return locked


@dataclass(frozen=True)
class ApplyResult:
    plan_digest: str
    created_job_ids: tuple[int, ...]
    already_present_job_ids: tuple[int, ...]
    skipped_window_passed: int
    outbox_ids_before: tuple[int, ...]
    outbox_ids_after: tuple[int, ...]
    contract_excluded_record_ids: tuple[int, ...]
    applied_at: datetime

    def report(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "version": APPLY_REPORT_VERSION,
            "mode": "apply-report",
            "snapshot_version": SNAPSHOT_VERSION,
            "plan_digest": self.plan_digest,
            "created_job_ids": list(self.created_job_ids),
            "already_present_job_ids": list(self.already_present_job_ids),
            "skipped_window_passed": self.skipped_window_passed,
            "blocked_or_refused_record_ids": [],
            "mutation_counts": {"message_jobs_created": len(self.created_job_ids), "outbox_messages_created": 0},
            "unexpected_state": [],
            "halted": False,
            "outbox_ids_before": list(self.outbox_ids_before),
            "outbox_ids_after": list(self.outbox_ids_after),
            "contract_excluded_record_ids": list(self.contract_excluded_record_ids),
            "applied_at": _timestamp(self.applied_at),
        }
        return {**payload, "report_digest": _digest(payload)}


def read_apply_report(path: str | Path, *, frozen: FrozenRecoveryPlan) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RecoveryError("apply_report_unreadable") from None
    if not isinstance(payload, dict) or payload.get("version") != APPLY_REPORT_VERSION:
        raise RecoveryError("apply_report_version_unsupported")
    digest = payload.get("report_digest")
    unsigned = dict(payload)
    unsigned.pop("report_digest", None)
    if not isinstance(digest, str) or digest != _digest(unsigned):
        raise RecoveryError("apply_report_digest_mismatch")
    if payload.get("plan_digest") != frozen.digest or payload.get("snapshot_version") != SNAPSHOT_VERSION:
        raise RecoveryError("plan_apply_digest_mismatch")
    expected_excluded_ids = sorted(
        int(row["record_id"]) for row in frozen.records if row.get("eligibility") == CONTRACT_NOT_SUPPORTED
    )
    if payload.get("contract_excluded_record_ids") != expected_excluded_ids:
        raise RecoveryError("plan_apply_digest_mismatch")
    return payload


async def apply_recovery_plan(
    session: AsyncSession,
    *,
    frozen: FrozenRecoveryPlan,
    client: RecoveryReader,
    now: datetime | None = None,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> ApplyResult:
    """Re-prove live state, then create the exact jobs in one transaction."""
    fixed_now = _utc(now) if now is not None else None
    current = await build_recovery_plan(
        session,
        client=client,
        now=frozen.planned_at,
        limit=max(len(frozen.records), 1) + 1,
        pause_sec=pause_sec,
        sleep=sleep,
    )
    mutation_boundary = fixed_now or _utc(datetime.now(timezone.utc))
    compare_revalidated_plan(frozen, current, now=mutation_boundary)
    age = (mutation_boundary - frozen.planned_at).total_seconds()
    if age < 0 or age > min(max_age_sec, MAX_SNAPSHOT_AGE_SEC):
        raise RecoveryError("snapshot_expired")
    if (
        not bool(settings.easyweek_multi_service_notifications_enabled)
        or bool(settings.easyweek_multi_service_send_enabled)
        or not bool(settings.easyweek_reminders_enabled)
        or not bool(settings.easyweek_reminder_api_guard_enabled)
    ):
        raise RecoveryError("runtime_fence_changed")
    await session.rollback()

    created: list[int] = []
    already: list[int] = []
    before_outboxes = sorted(
        int(item["id"]) for record in current.records for item in record.get("existing_outboxes", [])
    )
    async with session.begin():
        await _lock_and_compare_local_state(session, current=current)
        current_by_id = {int(row["record_id"]): row for row in current.records}
        for frozen_row in frozen.records:
            record_id = int(frozen_row["record_id"])
            current_row = current_by_id[record_id]
            existing_by_key = {item["dedupe_key"]: item for item in current_row.get("existing_jobs", [])}
            if not isinstance(frozen_row.get("multi_service_snapshot"), Mapping):
                continue
            for reminder in frozen_row.get("reminders", []):
                if reminder.get("disposition") != CREATE:
                    continue
                key = str(reminder["dedupe_key"])
                existing = existing_by_key.get(key)
                if existing is not None:
                    if not _created_by_this_plan(existing, plan_digest=frozen.digest):
                        raise RecoveryError("job_state_changed")
                    already.append(int(existing["id"]))
                    continue
                run_at = _parse_timestamp(reminder["run_at"])
                if run_at <= mutation_boundary:
                    raise RecoveryError("reminder_window_passed")
                job_type = str(reminder["job_type"])
                payload = _expected_payload(
                    record=frozen_row,
                    reminder=reminder,
                    plan_digest=frozen.digest,
                )
                stmt = (
                    pg_insert(MessageJob)
                    .values(
                        provider=PROVIDER_EASYWEEK,
                        company_id=int(frozen_row["company_id"]),
                        record_id=record_id,
                        client_id=int(frozen_row["client_id"]),
                        job_type=job_type,
                        run_at=run_at,
                        status="queued",
                        dedupe_key=key,
                        payload=payload,
                    )
                    .on_conflict_do_nothing(index_elements=[MessageJob.dedupe_key])
                    .returning(MessageJob.id)
                )
                inserted = (await session.execute(stmt)).scalar_one_or_none()
                if inserted is None:
                    raise RecoveryError("concurrent_job_created")
                created.append(int(inserted))

        record_ids = [int(row["record_id"]) for row in current.records]
        after_outboxes = sorted(
            int(value)
            for value in (
                (await session.execute(select(OutboxMessage.id).where(OutboxMessage.record_id.in_(record_ids))))
                .scalars()
                .all()
                if record_ids
                else []
            )
        )
        if after_outboxes != before_outboxes:
            raise RecoveryError("outbox_state_changed")

    return ApplyResult(
        plan_digest=frozen.digest,
        created_job_ids=tuple(sorted(created)),
        already_present_job_ids=tuple(sorted(already)),
        skipped_window_passed=sum(
            reminder.get("disposition") == WINDOW_PASSED
            for row in frozen.records
            for reminder in row.get("reminders", [])
        ),
        outbox_ids_before=tuple(before_outboxes),
        outbox_ids_after=tuple(after_outboxes),
        contract_excluded_record_ids=tuple(
            sorted(int(row["record_id"]) for row in frozen.records if row.get("eligibility") == CONTRACT_NOT_SUPPORTED)
        ),
        applied_at=mutation_boundary,
    )


async def verify_recovery(
    session: AsyncSession,
    *,
    frozen: FrozenRecoveryPlan,
    apply_report: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, object]:
    """Verify exact recovery rows and prove that recovery had no wider effect."""
    verified_at = _utc(now or datetime.now(timezone.utc))
    expected = [
        (row, reminder)
        for row in frozen.records
        for reminder in row.get("reminders", [])
        if reminder.get("disposition") == CREATE
    ]
    record_ids = [int(row["record_id"]) for row in frozen.records]
    records = list(
        (await session.execute(select(Record).where(Record.id.in_(record_ids)).order_by(Record.id.asc())))
        .scalars()
        .all()
        if record_ids
        else []
    )
    clients, _unused_jobs, _unused_outboxes = await _scope_state(session, records)
    service_state_digests = await _record_service_state_digests(session, records)
    frozen_by_id = {int(row["record_id"]): row for row in frozen.records}
    identity_mismatches: list[int] = []
    state_mismatches: list[int] = []
    for record in records:
        client = clients.get(record.client_id) if record.client_id is not None else None
        expected_identity = frozen_by_id.get(record.id)
        if expected_identity is None or _record_identity(record, client) != {
            key: expected_identity.get(key) for key in _record_identity(record, client)
        }:
            identity_mismatches.append(record.id)
        if expected_identity is None or (
            _record_state_digest(record) != expected_identity.get("record_state_digest")
            or _client_state_digest(client) != expected_identity.get("client_state_digest")
            or service_state_digests[record.id] != expected_identity.get("record_services_state_digest")
        ):
            state_mismatches.append(record.id)
    missing_record_ids = sorted(set(record_ids) - {record.id for record in records})
    identity_mismatches.extend(missing_record_ids)
    state_mismatches.extend(missing_record_ids)
    jobs = list(
        (
            await session.execute(
                select(MessageJob).where(MessageJob.record_id.in_(record_ids)).order_by(MessageJob.id.asc())
            )
        )
        .scalars()
        .all()
        if record_ids
        else []
    )
    jobs_by_key = {job.dedupe_key: job for job in jobs}
    created_ids = {int(value) for value in apply_report.get("created_job_ids", [])}
    already_ids = {int(value) for value in apply_report.get("already_present_job_ids", [])}
    expected_ids: set[int] = set()
    digest_mismatches: list[int] = []
    missing_keys = 0
    for row, reminder in expected:
        job = jobs_by_key.get(reminder.get("dedupe_key"))
        if job is None:
            missing_keys += 1
            continue
        expected_ids.add(job.id)
        payload = job.payload if isinstance(job.payload, Mapping) else {}
        embedded, embedded_error = multi_service_snapshot_from_job_payload(payload)
        if (
            job.provider != PROVIDER_EASYWEEK
            or job.company_id != row.get("company_id")
            or job.record_id != row.get("record_id")
            or job.client_id != row.get("client_id")
            or job.job_type != reminder.get("job_type")
            or _timestamp(job.run_at) != reminder.get("run_at")
            or payload.get(MULTI_SERVICE_JOB_DIGEST_KEY) != row.get("multi_service_snapshot_digest")
            or payload.get(RECOVERY_PLAN_DIGEST_KEY) != frozen.digest
            or payload.get(RECOVERY_SNAPSHOT_VERSION_KEY) != SNAPSHOT_VERSION
            or embedded is None
            or embedded_error is not None
            or embedded.digest != row.get("multi_service_snapshot_digest")
        ):
            digest_mismatches.append(job.id)

    tagged = [
        job
        for job in jobs
        if isinstance(job.payload, Mapping) and job.payload.get(RECOVERY_PLAN_DIGEST_KEY) == frozen.digest
    ]
    expected_keys = {str(reminder["dedupe_key"]) for _row, reminder in expected}
    unexpected_jobs = [job.id for job in tagged if job.dedupe_key not in expected_keys]
    disallowed_ids = {int(row["record_id"]) for row in frozen.records if row.get("eligibility") == CATEGORY_NOT_ALLOWED}
    disallowed_jobs = [
        job.id
        for job in jobs
        if job.record_id in disallowed_ids
        and job.provider == PROVIDER_EASYWEEK
        and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
        and _utc(job.created_at) >= frozen.planned_at
    ]
    contract_excluded_ids = {
        int(row["record_id"]) for row in frozen.records if row.get("eligibility") == CONTRACT_NOT_SUPPORTED
    }
    contract_excluded_jobs = [
        job.id
        for job in jobs
        if job.record_id in contract_excluded_ids
        and job.provider == PROVIDER_EASYWEEK
        and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
        and _utc(job.created_at) >= frozen.planned_at
    ]
    try:
        applied_at = _parse_timestamp(apply_report.get("applied_at"))
    except RecoveryError:
        raise RecoveryError("apply_report_timestamp_invalid") from None
    # "Overdue" means the recovery inserted an obligation whose moment had
    # already passed at apply. A normally sent job is of course in the past
    # when an operator verifies it a day later; that is a subsequent lifecycle,
    # not evidence of a late insertion.
    overdue_jobs = [job.id for job in tagged if _utc(job.run_at) <= applied_at]
    job_status_counts: dict[str, int] = {}
    subsequent_job_ids: list[int] = []
    for job in tagged:
        job_status_counts[job.status] = job_status_counts.get(job.status, 0) + 1
        if job.status != "queued":
            subsequent_job_ids.append(job.id)

    outboxes = list(
        (
            await session.execute(
                select(OutboxMessage).where(OutboxMessage.record_id.in_(record_ids)).order_by(OutboxMessage.id.asc())
            )
        )
        .scalars()
        .all()
        if record_ids
        else []
    )
    baseline_outbox_ids = {int(value) for value in apply_report.get("outbox_ids_after", [])}
    expected_job_ids = {job.id for job in jobs if job.dedupe_key in expected_keys}
    subsequent_outbox_ids: list[int] = []
    unexpected_outbox_ids: list[int] = []
    for row in outboxes:
        if row.id in baseline_outbox_ids:
            continue
        linked = jobs_by_key.get(next((key for key, job in jobs_by_key.items() if job.id == row.job_id), ""))
        if row.job_id in expected_job_ids and linked is not None and linked.status != "queued":
            subsequent_outbox_ids.append(row.id)
        else:
            unexpected_outbox_ids.append(row.id)

    counts_match = len(expected_ids) == len(expected) and expected_ids == created_ids | already_ids
    created_jobs_match = not missing_keys and not digest_mismatches and not unexpected_jobs
    passed = bool(
        counts_match
        and created_jobs_match
        and not unexpected_outbox_ids
        and not disallowed_jobs
        and not contract_excluded_jobs
        and not overdue_jobs
        and not identity_mismatches
        and not state_mismatches
    )
    return {
        "mode": MODE_VERIFY,
        "plan_digest": frozen.digest,
        "verified_at": _timestamp(verified_at),
        "passed": passed,
        "counts_match": counts_match,
        "created_jobs_match": created_jobs_match,
        "unexpected_job_ids": sorted(unexpected_jobs),
        "unexpected_outbox_ids": sorted(unexpected_outbox_ids),
        "subsequent_outbox_ids": sorted(subsequent_outbox_ids),
        "subsequent_job_ids": sorted(subsequent_job_ids),
        "job_status_counts": dict(sorted(job_status_counts.items())),
        "disallowed_jobs": sorted(disallowed_jobs),
        "contract_excluded_record_ids": sorted(contract_excluded_ids),
        "contract_excluded_jobs": sorted(contract_excluded_jobs),
        "overdue_jobs": sorted(overdue_jobs),
        "digest_mismatches": sorted(digest_mismatches),
        "identity_mismatches": sorted(identity_mismatches),
        "state_mismatch_record_ids": sorted(state_mismatches),
        "missing_expected_jobs": missing_keys,
    }


__all__ = [
    "ALREADY_DONE",
    "ALREADY_PROCESSING",
    "ALREADY_QUEUED",
    "APPLY_REPORT_VERSION",
    "CATEGORY_NOT_ALLOWED",
    "CONTRACT_NOT_SUPPORTED",
    "CREATE",
    "DEFAULT_LIMIT",
    "DEFAULT_MAX_SNAPSHOT_AGE_SEC",
    "DEFAULT_PAUSE_SEC",
    "FrozenRecoveryPlan",
    "IDENTITY_MISMATCH",
    "LIVE_BOOKING_NOT_ACTIVE",
    "MODE_APPLY",
    "MODE_PLAN",
    "MODE_VERIFY",
    "NON_TERMINAL_OUTBOX_PRESENT",
    "PROOF_FAILED",
    "RECOVERY_PLAN_DIGEST_KEY",
    "RecoveryError",
    "RecoveryPlan",
    "SNAPSHOT_VERSION",
    "TERMINAL_HISTORY_PRESENT",
    "WINDOW_PASSED",
    "apply_recovery_plan",
    "build_recovery_plan",
    "check_apply_authorization",
    "compare_revalidated_plan",
    "configuration_digest",
    "confirmation_phrase",
    "read_apply_report",
    "read_snapshot",
    "verify_recovery",
    "write_private_json",
    "write_snapshot",
]
