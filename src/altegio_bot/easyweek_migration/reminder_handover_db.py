"""The database half of the reminder handover (plan §30.4–30.7).

Split from :mod:`reminder_handover` so the rules — which reminders are owed,
what a key being occupied means, what a snapshot authorises — stay pure and
testable without a database, and everything that touches PostgreSQL or the
EasyWeek API lives here where the transaction boundaries are visible.

Three entry points, matching the three commands:

* :func:`build_plan` — reads, proves against the live API, writes nothing;
* :func:`apply_plan` — ONE transaction, no API call at all;
* :func:`verify_handover` — reads back and proves the end state.

The API is only ever called from :func:`build_plan`. That is what keeps the
outbox stop short: by the time an operator stops the worker, every booking has
already been proven, and the remaining work is a single transaction.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.branch_identity import verify_branch_identity
from altegio_bot.easyweek_migration.manifest import MigrationManifest
from altegio_bot.easyweek_migration.reminder_handover import (
    CANCEL_REASON,
    COVERING_STATUSES,
    OBLIGATION_DONE,
    OBLIGATION_MISSING,
    OBLIGATION_PRESENT_OPEN,
    OBLIGATION_PROCESSING,
    OPEN_STATUSES,
    ROW_BRANCH_UNPROVEN,
    ROW_COMPANY_MISMATCH,
    ROW_LEDGER_NOT_CREATED,
    ROW_LOCAL_TARGET_MISMATCH,
    ROW_PROVIDER_MISMATCH,
    ROW_SOURCE_RECORD_MISSING,
    ROW_TARGET_RECORD_MISSING,
    ROW_TARGET_UNPROVEN,
    ROW_TARGET_UUID_INVALID,
    ApplyReport,
    EligibleRefusal,
    FrozenPlan,
    HandoverPlan,
    HandoverRow,
    canonical_uuid,
    insert_values,
    obligations_for,
)
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.easyweek_reminder_guard import (
    ObservedBooking,
    classify_client_error,
    read_booking_state,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    EasyWeekMigrationLedger,
    MessageJob,
    OutboxMessage,
    Record,
)

logger = logging.getLogger("easyweek_migration.reminder_handover")

# 60 requests/minute is the documented EasyWeek ceiling. One second between
# sequential calls stays under it without needing a token bucket.
DEFAULT_PAUSE_SEC: Final = 1.0
HALT_ELIGIBLE_SCOPE_CHANGED: Final = "eligible_scope_changed"
HALT_OBLIGATION_IDENTITY: Final = "obligation_identity_mismatch"
HALT_SOURCE_REMINDER_CHANGED: Final = "source_reminder_changed"
HALT_OUTBOX_SIDE_EFFECT: Final = "scoped_outbox_side_effect"
HALT_SNAPSHOT_INCOMPLETE: Final = "snapshot_incomplete_scope"
HALT_SNAPSHOT_BLOCKED: Final = "snapshot_obligation_blocked"
HALT_SOURCE_PROCESSING: Final = "source_reminder_processing"


class HandoverError(Exception):
    """The handover refuses to continue. Message is a code, never a payload."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


async def _ledger_rows(session: AsyncSession, company_ids: tuple[int, ...]) -> list[EasyWeekMigrationLedger]:
    stmt = (
        select(EasyWeekMigrationLedger)
        .where(EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO)
        .where(EasyWeekMigrationLedger.target_provider == PROVIDER_EASYWEEK)
        .where(EasyWeekMigrationLedger.source_company_id.in_(company_ids))
        .order_by(EasyWeekMigrationLedger.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _existing_reminder_jobs(session: AsyncSession, record_pk: int) -> dict[str, tuple[int, str]]:
    """Every EasyWeek reminder key already held for one target record.

    Keyed by dedupe key, because that is the identity a new reminder would
    collide with. Statuses are kept verbatim so the caller can tell "already
    queued" from "somebody cancelled this" — which are opposite answers.
    """
    stmt = (
        select(MessageJob.dedupe_key, MessageJob.id, MessageJob.status)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.record_id == record_pk)
        .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
    )
    return {key: (job_id, status) for key, job_id, status in (await session.execute(stmt)).all()}


async def _source_reminder_jobs(session: AsyncSession, record_pk: int) -> tuple[list[int], list[int]]:
    """Open Altegio reminder jobs for one migrated source booking.

    Returns ``(queued, processing)``. They are kept apart because they lead to
    opposite actions: a queued job is what the handover withdraws, and a single
    processing one stops the entire apply — the worker has claimed it and may
    already be talking to Meta.
    """
    stmt = (
        select(MessageJob.id, MessageJob.status)
        .where(MessageJob.provider == PROVIDER_ALTEGIO)
        .where(MessageJob.record_id == record_pk)
        .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
        .where(MessageJob.status.in_(OPEN_STATUSES))
        .order_by(MessageJob.id.asc())
    )
    queued: list[int] = []
    processing: list[int] = []
    for job_id, status in (await session.execute(stmt)).all():
        (queued if status == "queued" else processing).append(job_id)
    return queued, processing


async def build_plan(
    session: AsyncSession,
    *,
    manifest: MigrationManifest,
    company_ids: tuple[int, ...],
    client: Any,
    now: datetime | None = None,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> HandoverPlan:
    """Read, prove and plan. Writes nothing, anywhere.

    Every SQL statement here is a ``SELECT``; the session is never committed and
    nothing calls Meta, Chatwoot or the outbox. The only network traffic is one
    ``GET /bookings/{uuid}`` per in-scope target, paced under the documented
    60/min ceiling.
    """
    moment = _aware(now) or _utcnow()
    pause = sleep if sleep is not None else asyncio.sleep

    # The manifest says which EasyWeek location each Altegio company migrated
    # into; only the runtime registry can prove that location IS that branch.
    # Both, up front and for the whole run: a wave planned against an unproven
    # branch would create reminders carrying another branch's template, footer
    # and sender.
    identity = verify_branch_identity(manifest)
    if not identity.proven:
        raise HandoverError(f"branch identity unproven ({', '.join(identity.failures)})")

    registry = configured_easyweek_locations()
    locations = registry.locations if registry.valid else {}

    rows: list[HandoverRow] = []
    eligible_refusals: list[EligibleRefusal] = []
    historical_rows: dict[str, int] = {}
    api_calls = 0

    def _refuse(entry: EasyWeekMigrationLedger, reason: str) -> None:
        eligible_refusals.append(
            EligibleRefusal(
                ledger_id=entry.id,
                source_company_id=entry.source_company_id,
                source_record_id=entry.source_record_id,
                reason=reason,
            )
        )

    ledger_rows = await _ledger_rows(session, company_ids)
    for entry in ledger_rows:
        if entry.status != ledger_module.STATUS_CREATED:
            historical_rows[entry.status] = historical_rows.get(entry.status, 0) + 1
            continue
        booking_uuid = canonical_uuid(entry.target_booking_uuid)
        if booking_uuid is None:
            _refuse(entry, ROW_TARGET_UUID_INVALID)
            continue

        source = (
            (
                await session.execute(
                    select(Record)
                    .where(Record.provider == PROVIDER_ALTEGIO)
                    .where(Record.company_id == entry.source_company_id)
                    .where(Record.altegio_record_id == entry.source_record_id)
                )
            )
            .scalars()
            .one_or_none()
        )
        if source is None:
            _refuse(entry, ROW_SOURCE_RECORD_MISSING)
            continue

        target = (
            (
                await session.execute(
                    select(Record)
                    .where(Record.provider == PROVIDER_EASYWEEK)
                    .where(Record.easyweek_booking_uuid == booking_uuid)
                )
            )
            .scalars()
            .one_or_none()
        )
        if target is None:
            _refuse(entry, ROW_TARGET_RECORD_MISSING)
            continue
        if source.provider != PROVIDER_ALTEGIO or target.provider != PROVIDER_EASYWEEK:
            # Belt and braces: the queries above already scope by provider, so
            # reaching here would mean the two sides had been swapped.
            _refuse(entry, ROW_PROVIDER_MISMATCH)
            continue
        branch = manifest.branch(entry.source_company_id)
        if branch is None:
            # The wave names a company this manifest does not describe, so there
            # is nothing that pairs it with an EasyWeek location.
            _refuse(entry, ROW_BRANCH_UNPROVEN)
            continue
        if target.company_id != branch.easyweek_location_id:
            # The EasyWeek record belongs to a different branch than the one this
            # Altegio company migrated into. Source and target are crossed.
            _refuse(entry, ROW_COMPANY_MISMATCH)
            continue

        location = locations.get(target.company_id)
        if location is None or canonical_uuid(location.location_uuid) is None:
            _refuse(entry, ROW_BRANCH_UNPROVEN)
            continue

        if api_calls:
            await pause(pause_sec)
        api_calls += 1
        try:
            payload = await client.get_booking(str(booking_uuid))
        except Exception as exc:  # noqa: BLE001 — mapped by class, text never kept
            logger.info(
                "reminder_handover: target unproven ledger_id=%s outcome=%s",
                entry.id,
                classify_client_error(exc).outcome.value,
            )
            _refuse(entry, ROW_TARGET_UNPROVEN)
            continue

        observed = read_booking_state(payload, booking_uuid=booking_uuid, location=location)
        if not isinstance(observed, ObservedBooking):
            logger.info(
                "reminder_handover: target unproven ledger_id=%s outcome=%s",
                entry.id,
                observed.outcome.value,
            )
            _refuse(entry, ROW_TARGET_UNPROVEN)
            continue

        local_start = _aware(target.starts_at)
        if local_start != observed.starts_at or bool(target.is_deleted) != observed.is_canceled:
            # The database and the live CRM disagree about this appointment.
            # Planning from either would be planning from a guess.
            _refuse(entry, ROW_LOCAL_TARGET_MISMATCH)
            continue

        existing = await _existing_reminder_jobs(session, target.id)
        queued, processing = await _source_reminder_jobs(session, source.id)

        rows.append(
            HandoverRow(
                ledger_id=entry.id,
                source_company_id=entry.source_company_id,
                source_record_id=entry.source_record_id,
                source_record_pk=source.id,
                target_record_pk=target.id,
                target_company_id=target.company_id,
                target_booking_uuid=str(booking_uuid),
                target_starts_at=observed.starts_at,
                target_is_canceled=observed.is_canceled,
                target_is_completed=observed.is_completed,
                obligations=obligations_for(
                    booking_uuid=booking_uuid,
                    starts_at=observed.starts_at,
                    now=moment,
                    is_active=observed.is_active,
                    existing=existing,
                ),
                stale_source_job_ids=tuple(queued),
                processing_source_job_ids=tuple(processing),
            )
        )

    return HandoverPlan(
        company_ids=tuple(sorted(company_ids)),
        created_at=moment,
        rows=tuple(rows),
        historical_rows=historical_rows,
        eligible_refusals=tuple(eligible_refusals),
        ledger_rows_seen=len(ledger_rows),
        eligible_created_rows=sum(entry.status == ledger_module.STATUS_CREATED for entry in ledger_rows),
    )


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ApplyResult:
    """What one apply transaction did. PII-free; safe to print and to keep."""

    created_job_ids: tuple[int, ...] = ()
    canceled_job_ids: tuple[int, ...] = ()
    already_present: int = 0
    scoped_outbox_ids_before: tuple[int, ...] = ()
    scoped_outbox_ids_after: tuple[int, ...] = ()
    halted: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "apply",
            "easyweek_reminders_created": len(self.created_job_ids),
            "altegio_reminders_canceled": len(self.canceled_job_ids),
            "already_present": self.already_present,
            "mutations": len(self.created_job_ids) + len(self.canceled_job_ids),
            "created_job_ids": list(self.created_job_ids),
            "canceled_job_ids": list(self.canceled_job_ids),
            "scoped_outbox_ids_before": list(self.scoped_outbox_ids_before),
            "scoped_outbox_ids_after": list(self.scoped_outbox_ids_after),
            "halted": self.halted,
        }

    def apply_report(self, frozen: FrozenPlan, *, applied_at: datetime) -> ApplyReport:
        if self.halted is not None:
            raise HandoverError("a halted apply cannot produce an apply report")
        return ApplyReport(
            snapshot_version=frozen.version,
            snapshot_digest=frozen.digest,
            company_ids=frozen.company_ids,
            applied_at=applied_at,
            eligible_created_rows=frozen.eligible_created_rows,
            rows_in_scope=len(frozen.rows),
            created_job_ids=self.created_job_ids,
            canceled_job_ids=self.canceled_job_ids,
            already_present_count=self.already_present,
            scoped_outbox_ids_before=self.scoped_outbox_ids_before,
            scoped_outbox_ids_after=self.scoped_outbox_ids_after,
        )


async def _lock_scope(session: AsyncSession, identities: list[dict[str, Any]]) -> None:
    """Take row locks on everything the transaction will read or write.

    Ledger, both Records and every reminder job for them, in one pass and in a
    stable order. Without this, a concurrent inbox delivery could plan the same
    reminder between our check and our insert — the unique key would still save
    us from a duplicate, but the cancel half would then run against a picture
    that had already moved.
    """
    if not identities:
        return
    ledger_ids = sorted(int(item["ledger_id"]) for item in identities)
    record_pks = sorted(
        {int(item["source_record_pk"]) for item in identities} | {int(item["target_record_pk"]) for item in identities}
    )

    await session.execute(
        select(EasyWeekMigrationLedger.id)
        .where(EasyWeekMigrationLedger.id.in_(ledger_ids))
        .order_by(EasyWeekMigrationLedger.id.asc())
        .with_for_update()
    )
    await session.execute(
        select(Record.id).where(Record.id.in_(record_pks)).order_by(Record.id.asc()).with_for_update()
    )
    await session.execute(
        select(MessageJob.id)
        .where(MessageJob.record_id.in_(record_pks))
        .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
        .order_by(MessageJob.id.asc())
        .with_for_update()
    )


async def _scope_still_matches(session: AsyncSession, identities: list[dict[str, Any]]) -> str | None:
    """Re-prove the frozen scope against the locked rows, or name what moved."""
    for item in identities:
        entry = (
            (
                await session.execute(
                    select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.id == int(item["ledger_id"]))
                )
            )
            .scalars()
            .one_or_none()
        )
        if entry is None or entry.status != ledger_module.STATUS_CREATED:
            return ROW_LEDGER_NOT_CREATED
        if entry.source_provider != PROVIDER_ALTEGIO or entry.target_provider != PROVIDER_EASYWEEK:
            return ROW_PROVIDER_MISMATCH
        if entry.source_company_id != int(item["source_company_id"]):
            return ROW_COMPANY_MISMATCH
        if canonical_uuid(entry.target_booking_uuid) != canonical_uuid(item["target_booking_uuid"]):
            return ROW_TARGET_UUID_INVALID

        target = (
            (await session.execute(select(Record).where(Record.id == int(item["target_record_pk"]))))
            .scalars()
            .one_or_none()
        )
        if target is None or target.provider != PROVIDER_EASYWEEK:
            return ROW_TARGET_RECORD_MISSING
        if target.company_id != int(item["target_company_id"]):
            return ROW_COMPANY_MISMATCH
        if canonical_uuid(target.easyweek_booking_uuid) != canonical_uuid(item["target_booking_uuid"]):
            return ROW_TARGET_RECORD_MISSING
        if bool(target.is_deleted) != bool(item["target_is_canceled"]):
            return ROW_LOCAL_TARGET_MISMATCH
        local_start = _aware(target.starts_at)
        frozen_start = _aware(datetime.fromisoformat(str(item["target_starts_at"]).replace("Z", "+00:00")))
        if local_start != frozen_start:
            # The appointment moved. Its reminder keys moved with it, so every
            # key in the snapshot now names a time that is not this booking's.
            return ROW_LOCAL_TARGET_MISMATCH

        source = (
            (await session.execute(select(Record).where(Record.id == int(item["source_record_pk"]))))
            .scalars()
            .one_or_none()
        )
        if source is None or source.provider != PROVIDER_ALTEGIO:
            return ROW_SOURCE_RECORD_MISSING
        if source.company_id != int(item["source_company_id"]):
            return ROW_COMPANY_MISMATCH
    return None


async def _eligible_scope_still_complete(
    session: AsyncSession,
    frozen: FrozenPlan,
    *,
    lock: bool,
) -> bool:
    """Re-read the whole existing company/status scope while its rows are locked."""
    if lock:
        # Row locks do not protect this predicate from a newly inserted ledger
        # row. SHARE blocks ledger INSERT/UPDATE for this short transaction, so
        # the frozen company/status scope cannot acquire a phantom eligible row.
        await session.execute(text("LOCK TABLE easyweek_migration_ledger IN SHARE MODE"))
    stmt = (
        select(EasyWeekMigrationLedger)
        .where(EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO)
        .where(EasyWeekMigrationLedger.target_provider == PROVIDER_EASYWEEK)
        .where(EasyWeekMigrationLedger.source_company_id.in_(frozen.company_ids))
        .order_by(EasyWeekMigrationLedger.id.asc())
    )
    if lock:
        stmt = stmt.with_for_update()
    current = list((await session.execute(stmt)).scalars().all())
    created = [entry for entry in current if entry.status == ledger_module.STATUS_CREATED]
    expected = {
        (
            int(row["identity"]["ledger_id"]),
            int(row["identity"]["source_company_id"]),
            int(row["identity"]["source_record_id"]),
            str(row["identity"]["target_booking_uuid"]),
        )
        for row in frozen.rows
    }
    actual = {
        (entry.id, entry.source_company_id, entry.source_record_id, str(entry.target_booking_uuid)) for entry in created
    }
    return (
        frozen.cutover_ready
        and not frozen.eligible_refusals
        and frozen.eligible_created_rows == len(frozen.rows)
        and len(created) == frozen.eligible_created_rows
        and actual == expected
    )


async def _scoped_outbox_ids(session: AsyncSession, identities: list[dict[str, Any]]) -> tuple[int, ...]:
    record_ids = sorted(
        {int(item["source_record_pk"]) for item in identities} | {int(item["target_record_pk"]) for item in identities}
    )
    if not record_ids:
        return ()
    rows = await session.execute(
        select(OutboxMessage.id).where(OutboxMessage.record_id.in_(record_ids)).order_by(OutboxMessage.id.asc())
    )
    return tuple(int(value) for (value,) in rows.all())


async def _processing_in_scope(session: AsyncSession, identities: list[dict[str, Any]]) -> list[int]:
    """Source reminder jobs the worker holds right now.

    Checked after the outbox has been stopped and the rows are locked. Even one
    means STOP: a claimed job may already have reached Meta, and cancelling it
    would leave a customer with a message we then pretend we withdrew.
    """
    if not identities:
        return []
    source_pks = sorted({int(item["source_record_pk"]) for item in identities})
    stmt = (
        select(MessageJob.id)
        .where(MessageJob.provider == PROVIDER_ALTEGIO)
        .where(MessageJob.record_id.in_(source_pks))
        .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
        .where(MessageJob.status == "processing")
        .order_by(MessageJob.id.asc())
    )
    return [row for (row,) in (await session.execute(stmt)).all()]


async def _apply_plan_inner(
    session: AsyncSession,
    frozen: FrozenPlan,
    *,
    now: datetime | None = None,
) -> ApplyResult:
    """Create the missing EasyWeek reminders, then withdraw the old Altegio ones.

    One transaction, opened by the caller and committed by the caller, so a
    failure anywhere leaves the wave exactly as it was. No EasyWeek request is
    made here at all: the live proof belongs to ``plan``, and repeating it under
    a stopped outbox would turn a database transaction into a long API walk.

    The ORDER is the safety property. Creation first means the failure case
    leaves the customer with the reminder they already had; cancelling first
    would leave a real appointment briefly — or, if the second half failed,
    permanently — with none.
    """
    moment = _aware(now) or _utcnow()
    if frozen.eligible_refusals or not frozen.rows or frozen.eligible_created_rows != len(frozen.rows):
        return ApplyResult(halted=HALT_SNAPSHOT_INCOMPLETE)
    if any(
        obligation["outcome"]
        not in {OBLIGATION_MISSING, OBLIGATION_PRESENT_OPEN, OBLIGATION_PROCESSING, OBLIGATION_DONE}
        for row in frozen.rows
        for obligation in row["obligations"]
    ):
        return ApplyResult(halted=HALT_SNAPSHOT_BLOCKED)
    if any(row["processing_source_job_ids"] for row in frozen.rows):
        return ApplyResult(halted=HALT_SOURCE_PROCESSING)
    if not frozen.cutover_ready:
        return ApplyResult(halted="snapshot_not_cutover_ready")

    frozen_rows = frozen.rows
    identities = [row["identity"] for row in frozen_rows]

    await _lock_scope(session, identities)

    if not await _eligible_scope_still_complete(session, frozen, lock=True):
        return ApplyResult(halted=HALT_ELIGIBLE_SCOPE_CHANGED)

    drift = await _scope_still_matches(session, identities)
    if drift is not None:
        return ApplyResult(halted=drift)

    processing = await _processing_in_scope(session, identities)
    if processing:
        return ApplyResult(halted=HALT_SOURCE_PROCESSING)

    outbox_before = await _scoped_outbox_ids(session, identities)

    # -- 1. create ---------------------------------------------------------
    created: list[int] = []
    already_present = sum(item["outcome"] != OBLIGATION_MISSING for row in frozen_rows for item in row["obligations"])
    for row, identity in zip(frozen_rows, identities, strict=True):
        target_pk = int(identity["target_record_pk"])
        target = (await session.execute(select(Record).where(Record.id == target_pk))).scalars().one()
        handover_row = HandoverRow(
            ledger_id=int(identity["ledger_id"]),
            source_company_id=int(identity["source_company_id"]),
            source_record_id=int(identity["source_record_id"]),
            source_record_pk=int(identity["source_record_pk"]),
            target_record_pk=target_pk,
            target_company_id=int(identity["target_company_id"]),
            target_booking_uuid=str(identity["target_booking_uuid"]),
            target_starts_at=_aware(datetime.fromisoformat(str(identity["target_starts_at"]).replace("Z", "+00:00"))),
            target_is_canceled=bool(identity["target_is_canceled"]),
            target_is_completed=bool(identity["target_is_completed"]),
        )
        for item in row.get("obligations") or ():
            if not isinstance(item, dict) or item.get("outcome") != OBLIGATION_MISSING:
                continue
            run_at = _aware(datetime.fromisoformat(str(item["run_at"]).replace("Z", "+00:00")))
            if run_at is None or run_at <= moment:
                # Crossed its moment between the plan and now. Refuse the whole
                # wave rather than queue a reminder for an hour already gone.
                return ApplyResult(halted="reminder_boundary_passed")

            values = insert_values(
                handover_row,
                _obligation_from_snapshot(item, run_at),
                client_id=target.client_id,
            )
            stmt = (
                pg_insert(MessageJob)
                .values(**values)
                # A concurrent inbox delivery planning the same business fact is
                # not an error and must not become a second row: the unique key
                # makes the race resolve to "already there".
                .on_conflict_do_nothing(index_elements=[MessageJob.dedupe_key])
                .returning(MessageJob.id)
            )
            inserted = (await session.execute(stmt)).scalar_one_or_none()
            if inserted is None:
                already_present += 1
            else:
                created.append(int(inserted))

    # -- 2. prove every obligation is now held before withdrawing anything --
    unmet = await _unmet_obligations(session, frozen_rows, identities)
    if unmet:
        return ApplyResult(halted=HALT_OBLIGATION_IDENTITY)

    # -- 3. prove and cancel -----------------------------------------------
    canceled: list[int] = []
    for row, identity in zip(frozen_rows, identities, strict=True):
        stale = [int(job_id) for job_id in (row.get("stale_source_job_ids") or ())]
        if not stale:
            continue
        held = list(
            (
                await session.execute(
                    select(MessageJob).where(MessageJob.id.in_(stale)).order_by(MessageJob.id.asc()).with_for_update()
                )
            )
            .scalars()
            .all()
        )
        by_id = {job.id: job for job in held}
        queued: list[int] = []
        for job_id in stale:
            job = by_id.get(job_id)
            if job is None or not _source_job_has_identity(job, identity):
                return ApplyResult(halted=HALT_SOURCE_REMINDER_CHANGED)
            if job.status == "queued":
                queued.append(job_id)
            elif job.status != "canceled" or job.last_error != CANCEL_REASON:
                # Only a cancellation made by this exact handover is an
                # idempotent success. A manual cancellation or any other state
                # needs a fresh operator-reviewed plan.
                return ApplyResult(halted=HALT_SOURCE_REMINDER_CHANGED)
        if not queued:
            continue
        stmt = (
            update(MessageJob)
            .where(MessageJob.id.in_(queued))
            # Every one of these is re-asserted rather than trusted from the
            # snapshot: provider, the exact source record, the two reminder job
            # types, and `queued`. A job that moved to `done` or `processing`
            # since the plan is simply not matched.
            .where(MessageJob.provider == PROVIDER_ALTEGIO)
            .where(MessageJob.company_id == int(identity["source_company_id"]))
            .where(MessageJob.record_id == int(identity["source_record_pk"]))
            .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
            .where(MessageJob.status == "queued")
            .values(status="canceled", locked_at=None, last_error=CANCEL_REASON)
            .returning(MessageJob.id)
        )
        changed = [int(job_id) for (job_id,) in (await session.execute(stmt)).all()]
        if len(changed) != len(queued):
            return ApplyResult(halted=HALT_SOURCE_REMINDER_CHANGED)
        canceled.extend(changed)

    outbox_after = await _scoped_outbox_ids(session, identities)
    if outbox_after != outbox_before:
        return ApplyResult(halted=HALT_OUTBOX_SIDE_EFFECT)

    return ApplyResult(
        created_job_ids=tuple(sorted(created)),
        canceled_job_ids=tuple(sorted(canceled)),
        already_present=already_present,
        scoped_outbox_ids_before=outbox_before,
        scoped_outbox_ids_after=outbox_after,
    )


async def apply_plan(
    session: AsyncSession,
    frozen: FrozenPlan,
    *,
    now: datetime | None = None,
) -> ApplyResult:
    """Apply atomically, rolling back even when a caller mishandles a refusal.

    The caller still owns the outer transaction and its final commit.  This
    savepoint makes the fail-closed return value safe on its own: a blocker
    discovered after an insert cannot be accidentally committed by a caller
    that forgets to roll the outer transaction back.
    """
    savepoint = await session.begin_nested()
    try:
        result = await _apply_plan_inner(session, frozen, now=now)
    except Exception:
        await savepoint.rollback()
        raise
    if result.halted is not None:
        await savepoint.rollback()
    else:
        await savepoint.commit()
    return result


def _source_job_has_identity(job: MessageJob, identity: dict[str, Any]) -> bool:
    return (
        job.provider == PROVIDER_ALTEGIO
        and job.company_id == int(identity["source_company_id"])
        and job.record_id == int(identity["source_record_pk"])
        and job.job_type in EASYWEEK_REMINDER_JOB_TYPES
    )


def _obligation_from_snapshot(item: dict[str, Any], run_at: datetime) -> Any:
    from altegio_bot.easyweek_migration.reminder_handover import Obligation

    return Obligation(
        job_type=str(item["job_type"]),
        run_at=run_at,
        dedupe_key=str(item["dedupe_key"]),
        outcome=OBLIGATION_MISSING,
    )


async def _unmet_obligations(
    session: AsyncSession,
    frozen_rows: tuple[dict[str, Any], ...],
    identities: list[dict[str, Any]],
) -> list[str]:
    """Keys the snapshot said were owed and the database still does not hold.

    Checked between creation and cancellation, so a wave can never withdraw the
    old reminders on the strength of an insert that silently did nothing.
    """
    wanted: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for row, identity in zip(frozen_rows, identities, strict=True):
        for item in row.get("obligations") or ():
            if isinstance(item, dict) and item.get("dedupe_key"):
                wanted[str(item["dedupe_key"])] = (identity, item)
    if not wanted:
        return []

    stmt = select(MessageJob).where(MessageJob.dedupe_key.in_(sorted(wanted)))
    held = {job.dedupe_key: job for job in (await session.execute(stmt)).scalars().all()}

    unmet: list[str] = []
    for key, (identity, obligation) in sorted(wanted.items()):
        job = held.get(key)
        if job is None or not _job_covers_obligation(job, identity, obligation):
            unmet.append(key)
    return unmet


def _job_covers_obligation(
    job: MessageJob,
    identity: dict[str, Any],
    obligation: dict[str, Any],
) -> bool:
    """Full durable identity of the reminder fact, not merely its unique key."""
    if (
        job.provider != PROVIDER_EASYWEEK
        or job.company_id != int(identity["target_company_id"])
        or job.record_id != int(identity["target_record_pk"])
        or job.job_type != obligation["job_type"]
        or job.status not in COVERING_STATUSES
        or job.dedupe_key != obligation["dedupe_key"]
    ):
        return False
    expected_existing_id = obligation.get("existing_job_id")
    if expected_existing_id is not None and job.id != int(expected_existing_id):
        return False
    expected_run_at = _aware(datetime.fromisoformat(obligation["run_at"].replace("Z", "+00:00")))
    if _aware(job.run_at) != expected_run_at:
        return False
    payload = job.payload
    if not isinstance(payload, dict):
        return False
    return (
        payload.get("provider") == PROVIDER_EASYWEEK
        and canonical_uuid(payload.get("booking_uuid")) == canonical_uuid(identity["target_booking_uuid"])
        and type(payload.get("company_id")) is int
        and payload.get("company_id") == int(identity["target_company_id"])
        and payload.get("job_type") == obligation["job_type"]
        and _aware(_parse_payload_timestamp(payload.get("record_starts_at")))
        == _aware(datetime.fromisoformat(identity["target_starts_at"].replace("Z", "+00:00")))
    )


def _parse_payload_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


async def verify_handover(
    session: AsyncSession,
    frozen: FrozenPlan,
    apply_report: ApplyReport,
) -> dict[str, Any]:
    """Prove the end state after an apply. Reads only.

    Deliberately re-derived from the frozen scope rather than from the apply's
    own report: a verification that trusted the report would prove the report
    self-consistent and nothing else.
    """
    frozen_rows = frozen.rows
    identities = [row["identity"] for row in frozen_rows]
    source_pks = sorted({int(item["source_record_pk"]) for item in identities})
    target_pks = sorted({int(item["target_record_pk"]) for item in identities})

    open_source = []
    if source_pks:
        open_source = [
            job_id
            for (job_id,) in (
                await session.execute(
                    select(MessageJob.id)
                    .where(MessageJob.provider == PROVIDER_ALTEGIO)
                    .where(MessageJob.record_id.in_(source_pks))
                    .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
                    .where(MessageJob.status.in_(OPEN_STATUSES))
                    .order_by(MessageJob.id.asc())
                )
            ).all()
        ]

    unmet = await _unmet_obligations(session, frozen_rows, identities)

    # Any EasyWeek reminder queued for an in-scope target whose key does not
    # belong to the appointment's current start instant. A leftover from an
    # earlier time would fire naming an hour the booking no longer has.
    expected_keys = {
        str(item["dedupe_key"])
        for row in frozen_rows
        for item in (row.get("obligations") or ())
        if isinstance(item, dict) and item.get("dedupe_key")
    }
    stray: list[int] = []
    if target_pks:
        stray = [
            job_id
            for (job_id, key) in (
                await session.execute(
                    select(MessageJob.id, MessageJob.dedupe_key)
                    .where(MessageJob.provider == PROVIDER_EASYWEEK)
                    .where(MessageJob.record_id.in_(target_pks))
                    .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
                    .where(MessageJob.status.in_(OPEN_STATUSES))
                    .order_by(MessageJob.id.asc())
                )
            ).all()
            if key not in expected_keys
        ]

    # A reminder must never point across a provider or a company boundary.
    crossed = (
        [
            job_id
            for (job_id,) in (
                await session.execute(
                    select(MessageJob.id)
                    .join(Record, Record.id == MessageJob.record_id)
                    .where(MessageJob.record_id.in_(target_pks + source_pks))
                    .where(MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
                    .where(MessageJob.status.in_(OPEN_STATUSES))
                    .where((MessageJob.provider != Record.provider) | (MessageJob.company_id != Record.company_id))
                    .order_by(MessageJob.id.asc())
                )
            ).all()
        ]
        if (target_pks or source_pks)
        else []
    )

    expected_by_key = {
        item["dedupe_key"]: (identity, item)
        for row, identity in zip(frozen_rows, identities, strict=True)
        for item in row["obligations"]
    }
    created_invalid: list[int] = []
    if apply_report.created_job_ids:
        created_jobs = list(
            (await session.execute(select(MessageJob).where(MessageJob.id.in_(apply_report.created_job_ids))))
            .scalars()
            .all()
        )
        by_id = {job.id: job for job in created_jobs}
        for job_id in apply_report.created_job_ids:
            job = by_id.get(job_id)
            expected = expected_by_key.get(job.dedupe_key) if job is not None else None
            if job is None or expected is None or not _job_covers_obligation(job, *expected):
                created_invalid.append(job_id)

    canceled_identity = {
        int(job_id): identity
        for row, identity in zip(frozen_rows, identities, strict=True)
        for job_id in row["stale_source_job_ids"]
    }
    canceled_invalid: list[int] = []
    if apply_report.canceled_job_ids:
        canceled_jobs = list(
            (await session.execute(select(MessageJob).where(MessageJob.id.in_(apply_report.canceled_job_ids))))
            .scalars()
            .all()
        )
        by_id = {job.id: job for job in canceled_jobs}
        for job_id in apply_report.canceled_job_ids:
            job = by_id.get(job_id)
            expected_identity = canceled_identity.get(job_id)
            if (
                job is None
                or expected_identity is None
                or not _source_job_has_identity(job, expected_identity)
                or job.status != "canceled"
                or job.last_error != CANCEL_REASON
            ):
                canceled_invalid.append(job_id)

    current_outbox = await _scoped_outbox_ids(session, identities)
    outbox_unchanged = apply_report.scoped_outbox_ids_before == apply_report.scoped_outbox_ids_after == current_outbox
    complete_scope = await _eligible_scope_still_complete(session, frozen, lock=False)

    counts_match = (
        len(apply_report.created_job_ids) == len(set(apply_report.created_job_ids))
        and len(apply_report.canceled_job_ids) == len(set(apply_report.canceled_job_ids))
        and apply_report.rows_in_scope == len(frozen_rows)
        and apply_report.eligible_created_rows == frozen.eligible_created_rows
        and len(apply_report.created_job_ids) + apply_report.already_present_count
        == sum(len(row["obligations"]) for row in frozen_rows)
    )

    return {
        "mode": "verify",
        "snapshot_version": frozen.version,
        "snapshot_digest_matches_apply_report": apply_report.snapshot_digest == frozen.digest,
        "rows_in_scope": len(identities),
        "eligible_scope_complete": complete_scope,
        "apply_counts_match": counts_match,
        "created_job_ids_invalid": created_invalid,
        "canceled_job_ids_invalid": canceled_invalid,
        "open_altegio_reminders": open_source,
        "unmet_obligations": len(unmet),
        "stray_easyweek_reminders": stray,
        "cross_provider_or_company_jobs": crossed,
        "scoped_outbox_ids_before": list(apply_report.scoped_outbox_ids_before),
        "scoped_outbox_ids_after": list(apply_report.scoped_outbox_ids_after),
        "scoped_outbox_ids_current": list(current_outbox),
        "scoped_outbox_unchanged": outbox_unchanged,
        "messages_sent_by_handover": 0 if outbox_unchanged else None,
        "passed": (
            complete_scope
            and counts_match
            and apply_report.snapshot_digest == frozen.digest
            and not created_invalid
            and not canceled_invalid
            and not open_source
            and not unmet
            and not stray
            and not crossed
            and outbox_unchanged
        ),
    }
