"""Plan, apply and verify the post-booking marketing handover (plan §31).

One transaction, three record-bound job types, and nothing created anywhere.

The apply cancels only. `review_3d` and `repeat_10d` may be created for an
EasyWeek booking solely by a proven `booking-succeeded`, and `comeback_3d`
solely by a proven cancellation — a migrated future appointment proves neither,
so inventing a target obligation here would invent a message to a customer.

Everything the transaction touches is re-proven under locks: the ledger scope,
the §30 reminder marker on every row, the durable wave closure, the complete set
of source jobs (not merely the frozen ids), the target EasyWeek jobs, and the
Outbox rows belonging to those source jobs. Any source job still `processing`,
or any related Outbox row in `queued` / `sending` / `unknown`, stops the whole
wave: a claimed job may already have reached Meta, and cancelling it would let
the tool claim it withdrew something a customer has already been sent.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.handover_evidence import (
    configuration_digest,
    configuration_ready,
)
from altegio_bot.easyweek_migration.manifest import MigrationManifest
from altegio_bot.easyweek_migration.post_booking_handover import (
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    NON_TERMINAL_OUTBOX_STATUSES,
    POST_BOOKING_JOB_TYPES,
    STOP_CONFIGURATION_CHANGED,
    STOP_LEDGER_SCOPE_CHANGED,
    STOP_MANIFEST_SCOPE,
    STOP_MARKER_CONFLICT,
    STOP_NON_TERMINAL_OUTBOX,
    STOP_PLAN_TRANSACTION,
    STOP_REMINDER_HANDOVER_INCOMPLETE,
    STOP_SNAPSHOT_EMPTY,
    STOP_SOURCE_JOB_SET_CHANGED,
    STOP_SOURCE_PROCESSING,
    STOP_SOURCE_RECORD,
    STOP_TARGET_JOB_SET_CHANGED,
    STOP_TARGET_RECORD,
    STOP_WAVE_NOT_CLOSED,
    STOP_WAVE_UNRESOLVED,
    FrozenPostBookingPlan,
    JobRef,
    PostBookingApplyReport,
    PostBookingPlan,
    ScopeRow,
)
from altegio_bot.easyweek_migration.reminder_handover import canonical_uuid, validate_run_ids
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    EasyWeekMigrationLedger,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.post_booking_ownership import REASON_HANDED_OVER

logger = logging.getLogger("easyweek_migration.post_booking_handover")

# What a withdrawn source job says in `message_jobs.last_error`. Stable and
# PII-free: an operator reads it and a report may quote it.
CANCEL_REASON: Final = REASON_HANDED_OVER


class PostBookingHandoverError(Exception):
    """The plan cannot be built. Always a full stop."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


async def _ledger_rows(session: AsyncSession, company_ids: tuple[int, ...], run_ids: tuple[str, ...]):
    stmt = (
        select(EasyWeekMigrationLedger)
        .where(EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO)
        .where(EasyWeekMigrationLedger.target_provider == PROVIDER_EASYWEEK)
        .where(EasyWeekMigrationLedger.source_company_id.in_(company_ids))
        .where(EasyWeekMigrationLedger.run_id.in_(run_ids))
        .order_by(EasyWeekMigrationLedger.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _jobs_for(session: AsyncSession, *, provider: str, record_pk: int) -> list[MessageJob]:
    """Every job of the three owned types for one record, in every status.

    Every status on purpose: the plan has to count the historical ones and the
    apply has to re-prove the COMPLETE set. Comparing only the frozen ids would
    miss a job created between plan and apply, which is exactly the one that
    would survive the handover.
    """
    stmt = (
        select(MessageJob)
        .where(MessageJob.provider == provider)
        .where(MessageJob.record_id == record_pk)
        .where(MessageJob.job_type.in_(POST_BOOKING_JOB_TYPES))
        .order_by(MessageJob.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _non_terminal_outbox_ids(session: AsyncSession, job_ids: list[int]) -> tuple[int, ...]:
    """Outbox rows of those jobs that may still be in flight.

    Keyed on `job_id`, not on the record: this asks about the messages these
    exact jobs produced. A terminal row — sent, delivered, read, failed,
    canceled — is history and is deliberately not returned; the handover never
    rewrites it and never pretends to recall a message.
    """
    if not job_ids:
        return ()
    stmt = (
        select(OutboxMessage.id)
        .where(OutboxMessage.job_id.in_(job_ids))
        .where(OutboxMessage.status.in_(NON_TERMINAL_OUTBOX_STATUSES))
        .order_by(OutboxMessage.id.asc())
    )
    return tuple(int(value) for (value,) in (await session.execute(stmt)).all())


def _job_refs(jobs: list[MessageJob]) -> tuple[JobRef, ...]:
    return tuple(
        JobRef(job_id=int(job.id), job_type=str(job.job_type), status=str(job.status))
        for job in sorted(jobs, key=lambda item: int(item.id))
    )


async def build_plan(
    session: AsyncSession,
    *,
    manifest: MigrationManifest,
    company_ids: tuple[int, ...],
    run_ids: tuple[str, ...],
    now: datetime | None = None,
) -> PostBookingPlan:
    """Read, prove and plan. Writes nothing, anywhere, and calls no API.

    Every statement is a `SELECT` under `SET TRANSACTION READ ONLY`, and unlike
    the reminder plan this one makes no EasyWeek request at all: what it decides
    is whether to withdraw Altegio obligations, and no EasyWeek fact bears on
    that. The live EasyWeek side was already proven by the migration and by §30.
    """
    moment = _aware(now) or _utcnow()
    validate_run_ids(run_ids)
    if not manifest.valid or not set(company_ids).issubset(manifest.company_ids):
        raise PostBookingHandoverError(STOP_MANIFEST_SCOPE)
    if not configuration_ready():
        raise PostBookingHandoverError(STOP_CONFIGURATION_CHANGED)
    if session.in_transaction():
        # Never silently inherit an already-used read/write transaction.
        raise PostBookingHandoverError(STOP_PLAN_TRANSACTION)
    await session.execute(text("SET TRANSACTION READ ONLY"))

    refusals: dict[str, int] = {}

    def _refuse(reason: str) -> None:
        refusals[reason] = refusals.get(reason, 0) + 1

    entries = await _ledger_rows(session, company_ids, run_ids)
    created = [entry for entry in entries if entry.status == ledger_module.STATUS_CREATED]
    unresolved: dict[str, int] = {}
    for entry in entries:
        if entry.status in ledger_module.UNRESOLVED_STATUSES:
            unresolved[entry.status] = unresolved.get(entry.status, 0) + 1

    # Every claimed pair must be durably closed by §30 — otherwise a booking can
    # still be added to this wave, and it would arrive with no marker of either
    # kind.
    wave_closed = True
    for company_id in sorted(company_ids):
        for run_id in sorted(run_ids):
            if not await ledger_module.wave_handed_over(session, source_company_id=company_id, run_id=run_id):
                wave_closed = False

    rows: list[ScopeRow] = []
    for entry in created:
        booking_uuid = canonical_uuid(entry.target_booking_uuid)
        if booking_uuid is None:
            _refuse(STOP_TARGET_RECORD)
            continue
        if entry.reminders_handed_over_at is None or not entry.reminder_handover_plan_digest:
            # §30 is a prerequisite. A row whose reminders may still be
            # Altegio's is not a row whose marketing jobs may be withdrawn.
            _refuse(STOP_REMINDER_HANDOVER_INCOMPLETE)
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
            _refuse(STOP_SOURCE_RECORD)
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
            _refuse(STOP_TARGET_RECORD)
            continue

        source_jobs = await _jobs_for(session, provider=PROVIDER_ALTEGIO, record_pk=int(source.id))
        target_jobs = await _jobs_for(session, provider=PROVIDER_EASYWEEK, record_pk=int(target.id))
        outbox = await _non_terminal_outbox_ids(session, [int(job.id) for job in source_jobs])

        rows.append(
            ScopeRow(
                ledger_id=int(entry.id),
                source_company_id=int(entry.source_company_id),
                source_record_id=int(entry.source_record_id),
                source_record_pk=int(source.id),
                target_record_pk=int(target.id),
                target_company_id=int(target.company_id),
                target_booking_uuid=str(booking_uuid),
                run_id=str(entry.run_id),
                reminder_handover_digest=str(entry.reminder_handover_plan_digest),
                post_booking_digest=entry.post_booking_handover_plan_digest,
                source_jobs=_job_refs(source_jobs),
                target_jobs=_job_refs(target_jobs),
                non_terminal_outbox_ids=outbox,
            )
        )

    return PostBookingPlan(
        company_ids=tuple(sorted(company_ids)),
        run_ids=tuple(run_ids),
        created_at=moment,
        manifest_digest=manifest.digest,
        configuration_digest=configuration_digest(),
        rows=tuple(rows),
        refusals=refusals,
        ledger_rows_seen=len(entries),
        eligible_created_rows=len(created),
        unresolved_rows=unresolved,
        wave_closed=wave_closed,
    )


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@dataclass
class PostBookingApplyResult:
    """What one apply did, or the single stable code that stopped it."""

    halted: str | None = None
    canceled_job_ids: tuple[int, ...] = ()
    already_canceled_job_ids: tuple[int, ...] = ()
    marked_ledger_ids: tuple[int, ...] = ()
    already_marked_ledger_ids: tuple[int, ...] = ()
    target_job_ids: tuple[int, ...] = ()
    scoped_outbox_ids: tuple[int, ...] = ()
    rows_in_scope: int = 0
    reasons: dict[str, int] = field(default_factory=dict)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "halted": self.halted,
            "rows_in_scope": self.rows_in_scope,
            "canceled_job_ids": list(self.canceled_job_ids),
            "already_canceled_job_ids": list(self.already_canceled_job_ids),
            "marked_ledger_ids": list(self.marked_ledger_ids),
            "already_marked_ledger_ids": list(self.already_marked_ledger_ids),
            "target_job_ids": list(self.target_job_ids),
            "scoped_outbox_ids": list(self.scoped_outbox_ids),
            "reasons": dict(sorted(self.reasons.items())),
        }

    def apply_report(self, frozen: FrozenPostBookingPlan, *, applied_at: datetime) -> PostBookingApplyReport:
        return PostBookingApplyReport(
            snapshot_digest=frozen.digest,
            applied_at=applied_at,
            company_ids=frozen.company_ids,
            run_ids=frozen.run_ids,
            rows_in_scope=self.rows_in_scope,
            canceled_job_ids=self.canceled_job_ids,
            already_canceled_job_ids=self.already_canceled_job_ids,
            marked_ledger_ids=self.marked_ledger_ids,
            already_marked_ledger_ids=self.already_marked_ledger_ids,
            target_job_ids=self.target_job_ids,
            scoped_outbox_ids=self.scoped_outbox_ids,
        )


async def _lock_scope(session: AsyncSession, frozen: FrozenPostBookingPlan) -> None:
    """Wave locks first, then every row this transaction reads or writes.

    The wave locks are the §30 primitive, unchanged: they serialise this
    transaction against anything that could add a booking to the wave. The row
    locks are taken in a fixed order — ledger, records, jobs — so two operators
    running this at once queue rather than deadlock.
    """
    for company_id, run_id in sorted(
        (int(company), str(run)) for company in frozen.company_ids for run in frozen.run_ids
    ):
        await ledger_module.lock_migration_wave(session, source_company_id=company_id, run_id=run_id)

    ledger_ids = sorted(int(row["ledger_id"]) for row in frozen.rows)
    record_pks = sorted(
        {int(row["source_record_pk"]) for row in frozen.rows} | {int(row["target_record_pk"]) for row in frozen.rows}
    )
    if not ledger_ids:
        return
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
        .where(MessageJob.job_type.in_(POST_BOOKING_JOB_TYPES))
        .order_by(MessageJob.id.asc())
        .with_for_update()
    )


async def _apply_inner(
    session: AsyncSession,
    frozen: FrozenPostBookingPlan,
    *,
    now: datetime | None,
    max_age_sec: int,
) -> PostBookingApplyResult:
    moment = _aware(now) or _utcnow()
    began = time.monotonic()

    if not frozen.rows:
        return PostBookingApplyResult(halted=STOP_SNAPSHOT_EMPTY)
    if frozen.refusals:
        return PostBookingApplyResult(halted=STOP_LEDGER_SCOPE_CHANGED)
    if frozen.unresolved_rows:
        return PostBookingApplyResult(halted=STOP_WAVE_UNRESOLVED)

    await _lock_scope(session, frozen)
    # The lock wait is real time: a snapshot that expired while this transaction
    # queued is a snapshot about a different world.
    elapsed = time.monotonic() - began
    age = frozen.age_seconds(moment) + elapsed
    if age < 0:
        return PostBookingApplyResult(halted="snapshot_time_invalid")
    if age > min(max_age_sec, DEFAULT_MAX_SNAPSHOT_AGE_SEC):
        return PostBookingApplyResult(halted="snapshot_expired")
    if frozen.configuration_digest != configuration_digest():
        return PostBookingApplyResult(halted=STOP_CONFIGURATION_CHANGED)

    # -- 1. re-prove the wave ---------------------------------------------
    for company_id, run_id in sorted(
        (int(company), str(run)) for company in frozen.company_ids for run in frozen.run_ids
    ):
        if not await ledger_module.wave_handed_over(session, source_company_id=company_id, run_id=run_id):
            return PostBookingApplyResult(halted=STOP_WAVE_NOT_CLOSED)

    entries = await _ledger_rows(session, frozen.company_ids, frozen.run_ids)
    created = {int(entry.id): entry for entry in entries if entry.status == ledger_module.STATUS_CREATED}
    if any(entry.status in ledger_module.UNRESOLVED_STATUSES for entry in entries):
        return PostBookingApplyResult(halted=STOP_WAVE_UNRESOLVED)
    if sorted(created) != list(frozen.ledger_ids):
        # The eligible set moved between plan and apply. Nothing is cancelled on
        # a picture that has changed.
        return PostBookingApplyResult(halted=STOP_LEDGER_SCOPE_CHANGED)

    canceled: list[int] = []
    already_canceled: list[int] = []
    target_ids: list[int] = []
    outbox_ids: list[int] = []
    marked: list[int] = []
    already_marked: list[int] = []

    for row in frozen.rows:
        entry = created[int(row["ledger_id"])]
        if entry.source_company_id != int(row["source_company_id"]) or entry.source_record_id != int(
            row["source_record_id"]
        ):
            return PostBookingApplyResult(halted=STOP_LEDGER_SCOPE_CHANGED)
        if canonical_uuid(entry.target_booking_uuid) != canonical_uuid(row["target_booking_uuid"]):
            return PostBookingApplyResult(halted=STOP_TARGET_RECORD)
        if (
            entry.reminders_handed_over_at is None
            or entry.reminder_handover_plan_digest != row["reminder_handover_digest"]
        ):
            # §30 ownership changed under us — or was never there.
            return PostBookingApplyResult(halted=STOP_REMINDER_HANDOVER_INCOMPLETE)
        if entry.post_booking_handover_plan_digest and entry.post_booking_handover_plan_digest != frozen.digest:
            # Somebody else's authorisation already owns this row.
            return PostBookingApplyResult(halted=STOP_MARKER_CONFLICT)

        source = await session.get(Record, int(row["source_record_pk"]), populate_existing=True)
        target = await session.get(Record, int(row["target_record_pk"]), populate_existing=True)
        if (
            source is None
            or source.provider != PROVIDER_ALTEGIO
            or int(source.company_id) != int(row["source_company_id"])
            or source.altegio_record_id != int(row["source_record_id"])
        ):
            return PostBookingApplyResult(halted=STOP_SOURCE_RECORD)
        if (
            target is None
            or target.provider != PROVIDER_EASYWEEK
            or int(target.company_id) != int(row["target_company_id"])
            or canonical_uuid(target.easyweek_booking_uuid) != canonical_uuid(row["target_booking_uuid"])
        ):
            return PostBookingApplyResult(halted=STOP_TARGET_RECORD)

        # -- 2. the COMPLETE current job sets, not the frozen ids ----------
        source_jobs = await _jobs_for(session, provider=PROVIDER_ALTEGIO, record_pk=int(row["source_record_pk"]))
        frozen_source = {int(item["job_id"]): str(item["status"]) for item in row["source_jobs"]}
        current_source = {int(job.id): str(job.status) for job in source_jobs}
        if set(current_source) - set(frozen_source):
            # A job the plan never saw. It is exactly the one that would survive
            # this handover, so the wave stops instead.
            return PostBookingApplyResult(halted=STOP_SOURCE_JOB_SET_CHANGED)

        if any(job.status == "processing" for job in source_jobs):
            # A claimed job may already have reached Meta. Cancelling it would
            # let this tool claim it withdrew a message somebody has been sent.
            return PostBookingApplyResult(halted=STOP_SOURCE_PROCESSING)

        live_outbox = await _non_terminal_outbox_ids(session, list(current_source))
        if live_outbox:
            return PostBookingApplyResult(halted=STOP_NON_TERMINAL_OUTBOX)

        target_jobs = await _jobs_for(session, provider=PROVIDER_EASYWEEK, record_pk=int(row["target_record_pk"]))
        frozen_target = {int(item["job_id"]): str(item["status"]) for item in row["target_jobs"]}
        if {int(job.id): str(job.status) for job in target_jobs} != frozen_target:
            # The EasyWeek side is not this phase's to change, so it must be
            # exactly what was reviewed.
            return PostBookingApplyResult(halted=STOP_TARGET_JOB_SET_CHANGED)
        target_ids.extend(sorted(frozen_target))

        # -- 3. withdraw only queued source jobs of the three types --------
        queued = sorted(job_id for job_id, status in current_source.items() if status == "queued")
        already_canceled.extend(
            sorted(
                job_id
                for job_id, status in current_source.items()
                if status == "canceled"
                and frozen_source.get(job_id) in {"queued", "canceled"}
                and any(int(job.id) == job_id and job.last_error == CANCEL_REASON for job in source_jobs)
            )
        )
        if queued:
            stmt = (
                update(MessageJob)
                .where(MessageJob.id.in_(queued))
                # Re-asserted rather than trusted from the snapshot: provider,
                # the exact source record, the three types and `queued`.
                .where(MessageJob.provider == PROVIDER_ALTEGIO)
                .where(MessageJob.company_id == int(row["source_company_id"]))
                .where(MessageJob.record_id == int(row["source_record_pk"]))
                .where(MessageJob.job_type.in_(POST_BOOKING_JOB_TYPES))
                .where(MessageJob.status == "queued")
                .values(status="canceled", locked_at=None, last_error=CANCEL_REASON, updated_at=moment)
                .returning(MessageJob.id)
            )
            changed = [int(job_id) for (job_id,) in (await session.execute(stmt)).all()]
            if len(changed) != len(queued):
                return PostBookingApplyResult(halted=STOP_SOURCE_JOB_SET_CHANGED)
            canceled.extend(changed)

        outbox_ids.extend(
            int(value)
            for (value,) in (
                await session.execute(
                    select(OutboxMessage.id)
                    .where(OutboxMessage.job_id.in_(list(current_source) or [0]))
                    .order_by(OutboxMessage.id.asc())
                )
            ).all()
        )

        # -- 4. the durable marker, for every row of the scope -------------
        # Including rows with no job at all: without it a late Altegio delivery
        # would create the FIRST obligation after the handover.
        if entry.post_booking_handover_plan_digest == frozen.digest:
            already_marked.append(int(entry.id))
            continue
        entry.post_booking_jobs_handed_over_at = moment
        entry.post_booking_handover_plan_digest = frozen.digest
        entry.updated_at = moment
        marked.append(int(entry.id))

    await session.flush()
    return PostBookingApplyResult(
        canceled_job_ids=tuple(sorted(canceled)),
        already_canceled_job_ids=tuple(sorted(set(already_canceled))),
        marked_ledger_ids=tuple(sorted(marked)),
        already_marked_ledger_ids=tuple(sorted(already_marked)),
        target_job_ids=tuple(sorted(set(target_ids))),
        scoped_outbox_ids=tuple(sorted(set(outbox_ids))),
        rows_in_scope=len(frozen.rows),
    )


async def apply_plan(
    session: AsyncSession,
    frozen: FrozenPostBookingPlan,
    *,
    now: datetime | None = None,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    lock_timeout_ms: int = 5000,
    statement_timeout_ms: int = 15000,
) -> PostBookingApplyResult:
    """Apply atomically, rolling back even when a caller mishandles a refusal.

    The savepoint makes the fail-closed return value safe on its own: a blocker
    found after a cancellation cannot be committed by a caller that forgets to
    roll the outer transaction back, and an exception between the cancellations
    and the marker takes both halves with it.
    """
    savepoint = await session.begin_nested()
    try:
        await session.execute(
            text("SELECT set_config('lock_timeout', :value, true)"), {"value": f"{lock_timeout_ms}ms"}
        )
        await session.execute(
            text("SELECT set_config('statement_timeout', :value, true)"), {"value": f"{statement_timeout_ms}ms"}
        )
        result = await _apply_inner(session, frozen, now=now, max_age_sec=max_age_sec)
    except DBAPIError as error:
        await savepoint.rollback()
        code = getattr(error.orig, "sqlstate", None)
        return PostBookingApplyResult(
            halted={"55P03": "database_lock_timeout", "57014": "database_statement_timeout"}.get(code, "database_error")
        )
    except Exception:
        await savepoint.rollback()
        raise
    if result.halted is not None:
        await savepoint.rollback()
    else:
        await savepoint.commit()
    return result


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


async def verify_handover(
    session: AsyncSession,
    frozen: FrozenPostBookingPlan,
    report: PostBookingApplyReport,
) -> dict[str, Any]:
    """Prove the end state independently. Read-only, and it trusts nothing.

    The apply report is compared against the database rather than believed: it
    is a file, and a file is not evidence about a row.
    """
    findings: dict[str, Any] = {}
    entries = {int(entry.id): entry for entry in await _ledger_rows(session, frozen.company_ids, frozen.run_ids)}

    missing_marker: list[int] = []
    lost_reminder_marker: list[int] = []
    for row in frozen.rows:
        entry = entries.get(int(row["ledger_id"]))
        if entry is None or entry.post_booking_handover_plan_digest != frozen.digest:
            missing_marker.append(int(row["ledger_id"]))
            continue
        if entry.post_booking_jobs_handed_over_at is None:
            missing_marker.append(int(row["ledger_id"]))
        if entry.reminders_handed_over_at is None or not entry.reminder_handover_plan_digest:
            lost_reminder_marker.append(int(row["ledger_id"]))

    open_source: list[int] = []
    target_now: list[int] = []
    outbox_now: list[int] = []
    for row in frozen.rows:
        source_jobs = await _jobs_for(session, provider=PROVIDER_ALTEGIO, record_pk=int(row["source_record_pk"]))
        open_source.extend(int(job.id) for job in source_jobs if job.status in {"queued", "processing"})
        target_jobs = await _jobs_for(session, provider=PROVIDER_EASYWEEK, record_pk=int(row["target_record_pk"]))
        target_now.extend(int(job.id) for job in target_jobs)
        outbox_now.extend(
            int(value)
            for (value,) in (
                await session.execute(
                    select(OutboxMessage.id)
                    .where(OutboxMessage.job_id.in_([int(job.id) for job in source_jobs] or [0]))
                    .order_by(OutboxMessage.id.asc())
                )
            ).all()
        )

    wave_closed = True
    for company_id, run_id in sorted(
        (int(company), str(run)) for company in frozen.company_ids for run in frozen.run_ids
    ):
        if not await ledger_module.wave_handed_over(session, source_company_id=company_id, run_id=run_id):
            wave_closed = False

    findings.update(
        {
            "rows_in_scope": len(frozen.rows),
            "rows_missing_marker": sorted(set(missing_marker)),
            "rows_missing_reminder_marker": sorted(set(lost_reminder_marker)),
            "open_source_job_ids": sorted(set(open_source)),
            "target_job_ids": sorted(set(target_now)),
            "scoped_outbox_ids": sorted(set(outbox_now)),
            "wave_closed": wave_closed,
            "report_marked": sorted(set(report.marked_ledger_ids) | set(report.already_marked_ledger_ids)),
            "report_target_job_ids": list(report.target_job_ids),
        }
    )
    findings["counts_match"] = (
        findings["report_marked"] == sorted(int(row["ledger_id"]) for row in frozen.rows)
        and findings["target_job_ids"] == sorted(set(report.target_job_ids))
        and findings["scoped_outbox_ids"] == sorted(set(report.scoped_outbox_ids))
    )
    findings["passed"] = bool(
        wave_closed
        and not findings["rows_missing_marker"]
        and not findings["rows_missing_reminder_marker"]
        and not findings["open_source_job_ids"]
        and findings["counts_match"]
    )
    return findings
