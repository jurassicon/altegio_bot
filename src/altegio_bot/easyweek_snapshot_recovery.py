"""Operator-controlled migration of proven v1 pair snapshots to version 2.

Why this exists
---------------
PR-7.5 taught the shared proof to recognise the Karlsruhe resource shadow.  A
booking that was proved before that change carries a durable
``multi_service_snapshot`` at version 1; the same booking re-proved today
yields the richer version 2 projection.  Both are correct for the moment they
were written, and §38.3 is deliberate about the consequence: *"старый
snapshot/job нельзя молча адаптировать: version/digest mismatch удерживает job
до нового доказанного события или отдельного операторского recovery"*.

So the multi-service preflight is right to report ``stale_snapshot_digest`` for
those records, and it must keep doing so.  What was missing is the other half
of that sentence — the separate operator recovery.  This module is it.

What it is not
--------------
It is not a service-recognition fix: the resolver already works.  It is not a
backfill, a replay, a planner or a sender.  It has no Meta, Chatwoot or
EasyWeek mutation capability, it never creates or touches a MessageJob or an
OutboxMessage, and it runs only from an explicit operator command.

The single mutation
-------------------
For a record that re-proves cleanly today, exactly one JSONB key is replaced:

    Record.raw["easyweek"]["multi_service_snapshot"]   v1 -> proven v2

Every other key of ``Record.raw``, and every other row in the database, is left
byte-identical.  The replacement value is the live-proven snapshot object built
by the shared production resolver, never a value adapted from the stored one.

The frozen-plan mechanics — canonical plan digest, confirmation phrase, private
0600 JSON, bounded snapshot age, transactional apply under ``FOR UPDATE`` — are
the ones already proved by the §34.5 reminder recovery and are imported from it
rather than re-implemented.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_API_UNAVAILABLE,
    MULTI_SERVICE_CATEGORY_NOT_ALLOWED,
    MULTI_SERVICE_SNAPSHOT_KEY,
    MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION,
    MULTI_SERVICE_SNAPSHOT_VERSION,
    MultiServiceProofError,
    MultiServiceSnapshot,
    ServiceEligibilityPurpose,
    WebhookServicePair,
    evaluate_service_eligibility,
    multi_service_snapshot_from_record_raw,
    prove_exactly_two_service_snapshot,
    read_catalog_rows_cached,
    record_raw_with_multi_service_snapshot,
)

# The frozen-plan machinery below is the one already proved by the §34.5
# reminder recovery.  It is imported rather than copied so the two operator
# tools cannot drift apart on digests, file permissions or plan freshness.
from altegio_bot.easyweek_multi_service_recovery import (
    DEFAULT_LIMIT,
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    DEFAULT_PAUSE_SEC,
    MAX_SNAPSHOT_AGE_SEC,
    MODE_APPLY,
    MODE_PLAN,
    MODE_VERIFY,
    RecoveryError,
    _client_state_digest,
    _digest,
    _job_state,
    _outbox_state,
    _parse_timestamp,
    _record_service_state_digests,
    _timestamp,
    _utc,
    _valid_digest,
    configuration_digest,
    write_private_json,
)
from altegio_bot.easyweek_normalizer import NormalizationError, normalize_event
from altegio_bot.easyweek_reminder_guard import GuardResult, read_booking_state
from altegio_bot.easyweek_resource_shadow_contract import (
    KARLSRUHE_COMPANY_ID,
    RESOURCE_SHADOW_PROOF_KIND,
    resolve_resource_shadow_contract,
)
from altegio_bot.easyweek_service_category import (
    EASYWEEK_RAW_NAMESPACE,
    SERVICES_COUNT_SNAPSHOT_KEY,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.settings import settings

PLAN_VERSION: Final = 1
APPLY_REPORT_VERSION: Final = 1

# Dispositions.  Only MIGRATE is a mutation.
MIGRATE: Final = "migrate"
SNAPSHOT_CURRENT: Final = "snapshot_current"
BLOCKED: Final = "blocked"

# Stable, PII-free refusal reasons.  The shared proof's own reason codes are
# reused verbatim wherever it produced them.
IDENTITY_MISMATCH: Final = "identity_mismatch"
LIVE_BOOKING_NOT_ACTIVE: Final = "live_booking_not_active"
SNAPSHOT_UNREADABLE: Final = "stored_snapshot_unreadable"
RESOURCE_SHADOW_NOT_PROVEN: Final = "resource_shadow_not_proven"
CONTRACT_DRIFT: Final = "contract_drift"
CATEGORY_NOW_ALLOWED: Final = "category_now_allowed"
JOBS_PRESENT: Final = "message_jobs_present"
OUTBOX_PRESENT: Final = "outbox_messages_present"

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


class SnapshotRecoveryReader(Protocol):
    """The two read-only endpoints this tool may use, and nothing else."""

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...


def confirmation_phrase(plan_digest: str) -> str:
    return f"migrate easyweek multi-service snapshots {plan_digest}"


def _raw_without_snapshot(raw: object) -> object:
    """``Record.raw`` with only the one key this tool may replace removed.

    Everything the recovery is forbidden to touch stays inside the digest, so
    any drift elsewhere in ``raw`` still blocks the wave, while the intended
    replacement does not make the record look changed to itself.
    """
    if not isinstance(raw, Mapping):
        return raw
    updated = dict(raw)
    namespace = updated.get(EASYWEEK_RAW_NAMESPACE)
    if isinstance(namespace, Mapping):
        updated[EASYWEEK_RAW_NAMESPACE] = {
            key: value for key, value in namespace.items() if key != MULTI_SERVICE_SNAPSHOT_KEY
        }
    return updated


def _record_state_digest_without_snapshot(record: Record) -> str:
    """Hash every mutable Record field except the snapshot being replaced."""
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
            "raw_without_multi_service_snapshot": _raw_without_snapshot(record.raw),
        }
    )


def _record_identity(record: Record, client: Client | None) -> dict[str, object]:
    return {
        "record_id": record.id,
        "provider": record.provider,
        "company_id": record.company_id,
        "client_id": record.client_id,
        "client_provider": client.provider if client is not None else None,
        "client_company_id": client.company_id if client is not None else None,
        "booking_uuid": str(record.easyweek_booking_uuid) if record.easyweek_booking_uuid is not None else None,
        "location_uuid": None,
        "starts_at": _timestamp(record.starts_at) if record.starts_at is not None else None,
        "is_deleted": bool(record.is_deleted),
        "total_cost": str(record.total_cost) if record.total_cost is not None else None,
    }


async def _select_candidate_records(
    session: AsyncSession,
    *,
    now: datetime,
    limit: int,
) -> tuple[list[Record], bool]:
    """Every active future Karlsruhe record still carrying a version 1 pair.

    The universe is narrowed by invariants, never by a list of record ids: the
    contract's own company, an exactly-two service count and a stored snapshot
    that is still version 1.  A record outside the contract is not a candidate
    and is not a blocker either — its version 1 snapshot is simply correct.
    """
    stmt = (
        select(Record)
        .where(Record.provider == PROVIDER_EASYWEEK)
        .where(Record.company_id == KARLSRUHE_COMPANY_ID)
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at.is_not(None), Record.starts_at > now)
        .where(Record.easyweek_booking_uuid.is_not(None))
        .where(
            Record.raw.contains(
                {
                    EASYWEEK_RAW_NAMESPACE: {
                        SERVICES_COUNT_SNAPSHOT_KEY: 2,
                        MULTI_SERVICE_SNAPSHOT_KEY: {"version": MULTI_SERVICE_SNAPSHOT_VERSION},
                    }
                }
            )
        )
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
    """Clients plus EVERY job and outbox row of the scope, whatever the status."""
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
        for job in (await session.execute(jobs_stmt)).scalars():
            if job.record_id is not None:
                jobs_by_record.setdefault(job.record_id, []).append(job)
        for row in (await session.execute(outbox_stmt)).scalars():
            if row.record_id is not None:
                outbox_by_record.setdefault(row.record_id, []).append(row)
    return clients, jobs_by_record, outbox_by_record


@dataclass(frozen=True)
class SnapshotRecoveryPlan:
    planned_at: datetime
    configuration_digest: str
    records: tuple[dict[str, Any], ...]
    summary: dict[str, Any]
    truncated: bool
    # Live-proven objects, kept in memory only.  Apply writes THESE, never a
    # value rebuilt from the JSON file.
    proven: dict[int, MultiServiceSnapshot] = field(default_factory=dict, repr=False)

    def unsigned(self) -> dict[str, Any]:
        return {
            "version": PLAN_VERSION,
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

    def snapshot(self) -> dict[str, Any]:
        return {**self.unsigned(), "plan_digest": self.plan_digest}

    def safe_report(self) -> dict[str, Any]:
        """Aggregates, stable reason codes and technical record ids only."""
        reasons: dict[str, int] = {}
        for row in self.records:
            reason = row.get("refusal_reason")
            if isinstance(reason, str):
                reasons[reason] = reasons.get(reason, 0) + 1
        return {
            "mode": MODE_PLAN,
            "plan_digest": self.plan_digest,
            **self.summary,
            "candidate_record_ids": [int(row["record_id"]) for row in self.records],
            "migrate_record_ids": [int(row["record_id"]) for row in self.records if row["disposition"] == MIGRATE],
            "blocked_record_ids": [int(row["record_id"]) for row in self.records if row["disposition"] == BLOCKED],
            "reasons": dict(sorted(reasons.items())),
        }


def _summary(rows: list[dict[str, Any]], *, truncated: bool) -> dict[str, Any]:
    migrate = [row for row in rows if row["disposition"] == MIGRATE]
    blocked = [row for row in rows if row["disposition"] == BLOCKED]
    return {
        "candidates": len(rows),
        "source_version_1": sum(row["source_snapshot_version"] == MULTI_SERVICE_SNAPSHOT_VERSION for row in rows),
        "target_version_2": len(migrate),
        "snapshot_current": sum(row["disposition"] == SNAPSHOT_CURRENT for row in rows),
        "blocked": len(blocked),
        "truncated": truncated,
        # An empty scope is the expected steady state after a successful
        # recovery, and it must stay safely applicable: apply then mutates
        # nothing at all.
        "apply_ready": not truncated and not blocked,
    }


def _webhook_pair(record: Record, booking: Any, location_uuid: str) -> WebhookServicePair:
    """Exactly the pair the inbox worker and the preflight build.

    Building it any other way would prove a different digest than the one the
    preflight compares against, which is the whole point of this recovery.
    """
    return WebhookServicePair(
        booking_uuid=booking.booking_uuid,
        location_uuid=location_uuid,
        service_name=booking.service_name,
        service_related=booking.service_related,
        services_description=booking.services_description,
        services_count=booking.services_count,
        quantity=booking.service_quantity,
        booking_currency=booking.booking_currency,
        total_cost=record.total_cost,
        company_id=record.company_id,
        service_id=booking.service_id,
    )


def _contract_state(snapshot: MultiServiceSnapshot) -> dict[str, object]:
    proof = snapshot.resource_shadow_proof
    if proof is None:
        return {"proof_kind": snapshot.proof_kind, "contract_revision": None, "contract_digest": None}
    return {
        "proof_kind": proof.proof_kind,
        "contract_revision": proof.contract_revision,
        "contract_digest": proof.contract_digest,
    }


def _resource_shadow_is_current(record: Record, snapshot: MultiServiceSnapshot) -> bool:
    """The live projection really is a version 2 Karlsruhe resource shadow."""
    proof = snapshot.resource_shadow_proof
    if snapshot.version != MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION or proof is None:
        return False
    if proof.proof_kind != RESOURCE_SHADOW_PROOF_KIND:
        return False
    contract = resolve_resource_shadow_contract(
        provider=record.provider,
        company_id=record.company_id,
        location_uuid=snapshot.location_uuid,
    )
    return (
        contract is not None
        and proof.company_id == contract.company_id
        and proof.contract_revision == contract.revision
        and proof.contract_digest == contract.digest
    )


async def build_snapshot_recovery_plan(
    session: AsyncSession,
    *,
    client: SnapshotRecoveryReader,
    now: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> SnapshotRecoveryPlan:
    """Freeze the complete current scope.  Read-only: no ORM object changes."""
    moment = _utc(now or datetime.now(timezone.utc))
    records, truncated = await _select_candidate_records(session, now=moment, limit=limit)
    events = await _latest_proof_events(session, records)
    clients, jobs_by_record, outbox_by_record = await _scope_state(session, records)
    service_state_digests = await _record_service_state_digests(session, records)
    registry = configured_easyweek_locations()
    pause = sleep if sleep is not None else asyncio.sleep

    rows: list[dict[str, Any]] = []
    proven: dict[int, MultiServiceSnapshot] = {}
    api_calls = 0

    for record in records:
        client_row = clients.get(record.client_id) if record.client_id is not None else None
        jobs = jobs_by_record.get(record.id, [])
        outboxes = outbox_by_record.get(record.id, [])
        event = events.get(record.easyweek_booking_uuid) if record.easyweek_booking_uuid is not None else None
        location = registry.locations.get(record.company_id) if registry.ready else None
        contract = (
            resolve_resource_shadow_contract(
                provider=record.provider,
                company_id=record.company_id,
                location_uuid=location.location_uuid,
            )
            if location is not None
            else None
        )

        stored, stored_error = multi_service_snapshot_from_record_raw(record.raw)
        disposition = BLOCKED
        refusal_reason: str | None = None
        live: MultiServiceSnapshot | None = None

        if stored is None or stored.version != MULTI_SERVICE_SNAPSHOT_VERSION:
            refusal_reason = stored_error or SNAPSHOT_UNREADABLE
        elif jobs:
            refusal_reason = JOBS_PRESENT
        elif outboxes:
            refusal_reason = OUTBOX_PRESENT
        elif event is None or location is None or contract is None:
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
                        refusal_reason = IDENTITY_MISMATCH
                    elif not observed.is_active:
                        refusal_reason = LIVE_BOOKING_NOT_ACTIVE
                    elif _utc(observed.starts_at) != _utc(record.starts_at):
                        refusal_reason = IDENTITY_MISMATCH
                    else:
                        catalog = await read_catalog_rows_cached(client, location_uuid=location.location_uuid)
                        candidate = prove_exactly_two_service_snapshot(
                            webhook=_webhook_pair(record, booking, location.location_uuid),
                            booking_payload=live_payload,
                            catalog_rows=catalog,
                        )
                        if candidate.digest == stored.digest:
                            # Already the projection the preflight computes.
                            live = candidate
                            disposition = SNAPSHOT_CURRENT
                        elif not _resource_shadow_is_current(record, candidate):
                            refusal_reason = (
                                CONTRACT_DRIFT
                                if candidate.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
                                else RESOURCE_SHADOW_NOT_PROVEN
                            )
                        else:
                            decision = evaluate_service_eligibility(
                                record_raw=record_raw_with_multi_service_snapshot(record.raw, candidate),
                                allowed_categories_raw=settings.easyweek_allowed_service_categories,
                                purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
                            )
                            if decision.allowed:
                                # This recovery is suppression-only: a sendable
                                # pair must go through the ordinary planner.
                                refusal_reason = CATEGORY_NOW_ALLOWED
                            elif decision.reason != MULTI_SERVICE_CATEGORY_NOT_ALLOWED:
                                refusal_reason = decision.reason
                            else:
                                live = candidate
                                disposition = MIGRATE
                except MultiServiceProofError as exc:
                    refusal_reason = exc.reason
                except Exception:  # noqa: BLE001 — exception text can carry API material
                    refusal_reason = MULTI_SERVICE_API_UNAVAILABLE

        if disposition == MIGRATE and live is not None:
            proven[record.id] = live

        rows.append(
            {
                **_record_identity(record, client_row),
                "location_uuid": location.location_uuid if location is not None else None,
                "record_state_digest": _record_state_digest_without_snapshot(record),
                "client_state_digest": _client_state_digest(client_row),
                "record_services_state_digest": service_state_digests[record.id],
                "proof_event_id": event.id if event is not None else None,
                "source_snapshot_version": stored.version if stored is not None else None,
                "source_snapshot_digest": stored.digest if stored is not None else None,
                "target_snapshot_version": live.version if live is not None else None,
                "target_snapshot_digest": live.digest if live is not None else None,
                "target_snapshot": live.as_dict() if live is not None else None,
                "target_contract": _contract_state(live) if live is not None else None,
                "disposition": disposition,
                "refusal_reason": refusal_reason,
                "existing_jobs": [_job_state(job) for job in jobs],
                "existing_outboxes": [_outbox_state(row) for row in outboxes],
            }
        )

    return SnapshotRecoveryPlan(
        planned_at=moment,
        configuration_digest=configuration_digest(),
        records=tuple(rows),
        summary=_summary(rows, truncated=truncated),
        truncated=truncated,
        proven=proven,
    )


@dataclass(frozen=True)
class FrozenSnapshotPlan:
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

    @property
    def migrate_rows(self) -> list[dict[str, Any]]:
        return [row for row in self.records if row.get("disposition") == MIGRATE]


def write_plan(plan: SnapshotRecoveryPlan, path: str | Path) -> Path:
    return write_private_json(plan.snapshot(), path)


def read_plan(path: str | Path) -> FrozenSnapshotPlan:
    import json

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise RecoveryError("plan_unreadable") from None
    if not isinstance(payload, dict) or payload.get("version") != PLAN_VERSION or payload.get("mode") != MODE_PLAN:
        raise RecoveryError("plan_version_unsupported")
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
        raise RecoveryError("plan_malformed")
    unsigned = {key: value for key, value in payload.items() if key != "plan_digest"}
    if not _valid_digest(payload.get("plan_digest")) or payload["plan_digest"] != _digest(unsigned):
        raise RecoveryError("plan_digest_mismatch")
    return FrozenSnapshotPlan(payload)


def check_apply_authorization(
    frozen: FrozenSnapshotPlan,
    *,
    supplied_digest: str | None,
    supplied_confirmation: str | None,
    now: datetime,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> None:
    """Everything that must be true before a single byte may be written."""
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
    _check_runtime_fences()


def _check_runtime_fences() -> None:
    if not bool(settings.easyweek_multi_service_notifications_enabled):
        raise RecoveryError("multi_service_planning_fence_closed")
    if not bool(settings.easyweek_resource_shadow_proof_enabled):
        raise RecoveryError("resource_shadow_fence_closed")
    # The recovery writes a suppression-only projection; an open send fence
    # would mean the wave lands while jobs can already leave.
    if bool(settings.easyweek_multi_service_send_enabled):
        raise RecoveryError("multi_service_send_fence_open")


_STABLE_ROW_KEYS: Final = (
    "record_id",
    "provider",
    "company_id",
    "client_id",
    "client_provider",
    "client_company_id",
    "booking_uuid",
    "location_uuid",
    "starts_at",
    "is_deleted",
    "total_cost",
    "record_state_digest",
    "client_state_digest",
    "record_services_state_digest",
    "source_snapshot_version",
    "source_snapshot_digest",
    "target_snapshot_version",
    "target_snapshot_digest",
    "target_contract",
    "disposition",
    "refusal_reason",
    "existing_jobs",
    "existing_outboxes",
)


def _stable_view(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in _STABLE_ROW_KEYS}


def compare_revalidated_plan(frozen: FrozenSnapshotPlan, current: SnapshotRecoveryPlan) -> None:
    """The whole scope, not only the migrating rows, must still be identical."""
    if current.truncated:
        raise RecoveryError("scope_truncated")
    if current.configuration_digest != frozen.payload.get("configuration_digest"):
        raise RecoveryError("configuration_digest_changed")
    expected = [_stable_view(row) for row in frozen.records]
    actual = [_stable_view(row) for row in current.records]
    if expected != actual:
        raise RecoveryError("scope_drift")
    for row in frozen.migrate_rows:
        record_id = int(row["record_id"])
        live = current.proven.get(record_id)
        if live is None or live.digest != row.get("target_snapshot_digest"):
            raise RecoveryError("scope_drift")
        if live.as_dict() != row.get("target_snapshot"):
            raise RecoveryError("scope_drift")


async def _lock_and_compare(
    session: AsyncSession,
    *,
    current: SnapshotRecoveryPlan,
) -> list[Record]:
    ids = sorted(int(row["record_id"]) for row in current.records)
    locked = list(
        (await session.execute(select(Record).where(Record.id.in_(ids)).order_by(Record.id.asc()).with_for_update()))
        .scalars()
        .all()
    )
    if [row.id for row in locked] != ids:
        raise RecoveryError("record_state_changed")

    clients, jobs, outboxes = await _scope_state(session, locked, lock=True)
    service_state_digests = await _record_service_state_digests(session, locked, lock=True)
    expected = {int(row["record_id"]): row for row in current.records}
    for record in locked:
        row = expected[record.id]
        client_row = clients.get(record.client_id) if record.client_id is not None else None
        identity = _record_identity(record, client_row)
        identity["location_uuid"] = row.get("location_uuid")
        if (
            identity != {key: row.get(key) for key in identity}
            or _record_state_digest_without_snapshot(record) != row.get("record_state_digest")
            or _client_state_digest(client_row) != row.get("client_state_digest")
            or service_state_digests[record.id] != row.get("record_services_state_digest")
        ):
            raise RecoveryError("record_state_changed")
        stored, _error = multi_service_snapshot_from_record_raw(record.raw)
        if stored is None or stored.digest != row.get("source_snapshot_digest"):
            raise RecoveryError("record_state_changed")
        # Under the lock, a migrating record still has to own nothing.
        if row.get("disposition") == MIGRATE and (jobs.get(record.id) or outboxes.get(record.id)):
            raise RecoveryError("job_state_changed" if jobs.get(record.id) else "outbox_state_changed")
        if [_job_state(item) for item in jobs.get(record.id, [])] != row.get("existing_jobs", []):
            raise RecoveryError("job_state_changed")
        if [_outbox_state(item) for item in outboxes.get(record.id, [])] != row.get("existing_outboxes", []):
            raise RecoveryError("outbox_state_changed")
    return locked


@dataclass(frozen=True)
class SnapshotApplyResult:
    plan_digest: str
    migrated: tuple[dict[str, Any], ...]
    applied_at: datetime

    def report(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": APPLY_REPORT_VERSION,
            "mode": "apply-report",
            "plan_version": PLAN_VERSION,
            "plan_digest": self.plan_digest,
            "migrated": list(self.migrated),
            "migrated_record_ids": [int(row["record_id"]) for row in self.migrated],
            "mutation_counts": {
                "records_snapshot_migrated": len(self.migrated),
                "message_jobs_created": 0,
                "message_jobs_changed": 0,
                "outbox_messages_created": 0,
                "outbox_messages_changed": 0,
                "clients_changed": 0,
                "record_services_changed": 0,
            },
            "halted": False,
            "applied_at": _timestamp(self.applied_at),
        }
        return {**payload, "report_digest": _digest(payload)}


def read_apply_report(path: str | Path, *, frozen: FrozenSnapshotPlan) -> dict[str, Any]:
    import json

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
    if payload.get("plan_digest") != frozen.digest:
        raise RecoveryError("plan_apply_digest_mismatch")
    expected_ids = sorted(int(row["record_id"]) for row in frozen.migrate_rows)
    if payload.get("migrated_record_ids") != expected_ids:
        raise RecoveryError("plan_apply_digest_mismatch")
    return payload


async def apply_snapshot_recovery_plan(
    session: AsyncSession,
    *,
    frozen: FrozenSnapshotPlan,
    client: SnapshotRecoveryReader,
    now: datetime | None = None,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> SnapshotApplyResult:
    """Re-prove everything live, then replace exactly one key per record."""
    fixed_now = _utc(now) if now is not None else None
    current = await build_snapshot_recovery_plan(
        session,
        client=client,
        now=frozen.planned_at,
        limit=max(len(frozen.records), 1) + 1,
        pause_sec=pause_sec,
        sleep=sleep,
    )
    boundary = fixed_now or _utc(datetime.now(timezone.utc))
    compare_revalidated_plan(frozen, current)
    age = (boundary - frozen.planned_at).total_seconds()
    if age < 0 or age > min(max_age_sec, MAX_SNAPSHOT_AGE_SEC):
        raise RecoveryError("plan_expired")
    _check_runtime_fences()
    # The plan phase is read-only; drop anything it loaded before the write.
    await session.rollback()

    migrated: list[dict[str, Any]] = []
    async with session.begin():
        locked = await _lock_and_compare(session, current=current)
        by_id = {record.id: record for record in locked}
        for row in frozen.migrate_rows:
            record_id = int(row["record_id"])
            record = by_id[record_id]
            target = current.proven[record_id]
            # Belt and braces: the object about to be written is the live one,
            # and it must still equal the frozen promise byte for byte.
            if target.digest != row.get("target_snapshot_digest") or target.as_dict() != row.get("target_snapshot"):
                raise RecoveryError("scope_drift")
            before = _raw_without_snapshot(record.raw)
            # The canonical helper replaces only this key and rebuilds the dict
            # so SQLAlchemy always sees the JSONB change.
            record.raw = record_raw_with_multi_service_snapshot(record.raw, target)
            if _raw_without_snapshot(record.raw) != before:
                raise RecoveryError("record_raw_mutation_out_of_scope")
            migrated.append(
                {
                    "record_id": record_id,
                    "company_id": int(row["company_id"]),
                    "old_snapshot_version": row.get("source_snapshot_version"),
                    "old_snapshot_digest": row.get("source_snapshot_digest"),
                    "new_snapshot_version": target.version,
                    "new_snapshot_digest": target.digest,
                    "proof_kind": target.proof_kind,
                    "contract_revision": row.get("target_contract", {}).get("contract_revision"),
                    "contract_digest": row.get("target_contract", {}).get("contract_digest"),
                }
            )

        record_ids = [int(row["record_id"]) for row in current.records]
        if record_ids:
            jobs_after = (
                (await session.execute(select(MessageJob.id).where(MessageJob.record_id.in_(record_ids))))
                .scalars()
                .all()
            )
            outbox_after = (
                (await session.execute(select(OutboxMessage.id).where(OutboxMessage.record_id.in_(record_ids))))
                .scalars()
                .all()
            )
            expected_jobs = sorted(int(item["id"]) for row in current.records for item in row.get("existing_jobs", []))
            expected_outbox = sorted(
                int(item["id"]) for row in current.records for item in row.get("existing_outboxes", [])
            )
            if sorted(int(value) for value in jobs_after) != expected_jobs:
                raise RecoveryError("job_state_changed")
            if sorted(int(value) for value in outbox_after) != expected_outbox:
                raise RecoveryError("outbox_state_changed")

    return SnapshotApplyResult(
        plan_digest=frozen.digest,
        migrated=tuple(migrated),
        applied_at=boundary,
    )


async def verify_snapshot_recovery(
    session: AsyncSession,
    *,
    frozen: FrozenSnapshotPlan,
    apply_report: Mapping[str, Any],
    client: SnapshotRecoveryReader,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> dict[str, Any]:
    """Read-only proof that the wave landed exactly and only as promised."""
    expected_ids = sorted(int(row["record_id"]) for row in frozen.migrate_rows)
    frozen_by_id = {int(row["record_id"]): row for row in frozen.records}
    reported_by_id = {int(row["record_id"]): row for row in apply_report.get("migrated", [])}

    mismatched: list[int] = []
    missing: list[int] = []
    unexpected_jobs: list[int] = []
    unexpected_outboxes: list[int] = []
    live_mismatches: list[int] = []
    non_target_raw_changed: list[int] = []

    records = list(
        (await session.execute(select(Record).where(Record.id.in_(expected_ids)).order_by(Record.id.asc())))
        .scalars()
        .all()
    )
    found = {record.id: record for record in records}
    missing = [record_id for record_id in expected_ids if record_id not in found]

    _clients, jobs, outboxes = await _scope_state(session, records)
    registry = configured_easyweek_locations()
    pause = sleep if sleep is not None else asyncio.sleep
    api_calls = 0

    for record_id in expected_ids:
        record = found.get(record_id)
        if record is None:
            continue
        row = frozen_by_id[record_id]
        reported = reported_by_id.get(record_id, {})

        if [item.id for item in jobs.get(record_id, [])]:
            unexpected_jobs.extend(int(item.id) for item in jobs[record_id])
        if [item.id for item in outboxes.get(record_id, [])]:
            unexpected_outboxes.extend(int(item.id) for item in outboxes[record_id])
        if _record_state_digest_without_snapshot(record) != row.get("record_state_digest"):
            non_target_raw_changed.append(record_id)

        stored, _error = multi_service_snapshot_from_record_raw(record.raw)
        if (
            stored is None
            or stored.version != MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
            or stored.digest != row.get("target_snapshot_digest")
            or stored.digest != reported.get("new_snapshot_digest")
            or not _resource_shadow_is_current(record, stored)
        ):
            mismatched.append(record_id)
            continue

        location = registry.locations.get(record.company_id) if registry.ready else None
        event_map = await _latest_proof_events(session, [record])
        event = event_map.get(record.easyweek_booking_uuid) if record.easyweek_booking_uuid is not None else None
        if location is None or event is None:
            live_mismatches.append(record_id)
            continue
        if api_calls:
            await pause(pause_sec)
        api_calls += 1
        try:
            booking = normalize_event(
                event_hint=event.event_hint,
                payload=event.payload,
                body_truncated=bool(event.body_truncated),
                location_registry=registry.locations,
            )
            live_payload = await client.get_booking(str(record.easyweek_booking_uuid))
            catalog = await read_catalog_rows_cached(client, location_uuid=location.location_uuid)
            live = prove_exactly_two_service_snapshot(
                webhook=_webhook_pair(record, booking, location.location_uuid),
                booking_payload=live_payload,
                catalog_rows=catalog,
            )
        except Exception:  # noqa: BLE001 — proof, normalization or API text must never be kept
            live_mismatches.append(record_id)
            continue
        if live.digest != stored.digest:
            live_mismatches.append(record_id)

    # A fresh selection must no longer see these records as version 1 work.
    remaining, _truncated = await _select_candidate_records(
        session,
        now=frozen.planned_at,
        limit=max(len(expected_ids), 1) + len(expected_ids),
    )
    still_version_1 = sorted(record.id for record in remaining if record.id in set(expected_ids))

    mutation_counts = apply_report.get("mutation_counts", {})
    counts_match = (
        isinstance(mutation_counts, Mapping)
        and mutation_counts.get("records_snapshot_migrated") == len(expected_ids)
        and mutation_counts.get("message_jobs_created") == 0
        and mutation_counts.get("outbox_messages_created") == 0
    )

    passed = (
        not missing
        and not mismatched
        and not unexpected_jobs
        and not unexpected_outboxes
        and not live_mismatches
        and not non_target_raw_changed
        and not still_version_1
        and counts_match
        and sorted(reported_by_id) == expected_ids
    )
    return {
        "mode": MODE_VERIFY,
        "plan_digest": frozen.digest,
        "expected_record_ids": expected_ids,
        "verified_records": len(expected_ids) - len(missing),
        "missing_record_ids": missing,
        "snapshot_mismatch_record_ids": sorted(mismatched),
        "live_proof_mismatch_record_ids": sorted(live_mismatches),
        "non_target_raw_changed_record_ids": sorted(non_target_raw_changed),
        "unexpected_job_ids": sorted(unexpected_jobs),
        "unexpected_outbox_ids": sorted(unexpected_outboxes),
        "still_version_1_record_ids": still_version_1,
        "counts_match": counts_match,
        "passed": passed,
    }


__all__ = [
    "APPLY_REPORT_VERSION",
    "BLOCKED",
    "CATEGORY_NOW_ALLOWED",
    "CONTRACT_DRIFT",
    "IDENTITY_MISMATCH",
    "JOBS_PRESENT",
    "LIVE_BOOKING_NOT_ACTIVE",
    "MIGRATE",
    "MODE_APPLY",
    "MODE_PLAN",
    "MODE_VERIFY",
    "OUTBOX_PRESENT",
    "PLAN_VERSION",
    "RESOURCE_SHADOW_NOT_PROVEN",
    "SNAPSHOT_CURRENT",
    "SNAPSHOT_UNREADABLE",
    "FrozenSnapshotPlan",
    "RecoveryError",
    "SnapshotApplyResult",
    "SnapshotRecoveryPlan",
    "SnapshotRecoveryReader",
    "apply_snapshot_recovery_plan",
    "build_snapshot_recovery_plan",
    "check_apply_authorization",
    "compare_revalidated_plan",
    "confirmation_phrase",
    "read_apply_report",
    "read_plan",
    "verify_snapshot_recovery",
    "write_plan",
]
