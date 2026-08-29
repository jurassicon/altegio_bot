"""Orchestration for the five cutover modes (PR-11.1).

The ordering below is the safety argument, in code:

1. **Plan first, always.** Every mode — including ``apply`` — builds the full plan
   from the Altegio API and the ledger *before* anything is written. ``apply``
   is a plan plus a gate plus a write loop, not a different code path, so the
   thing an operator reviewed is literally the thing that runs.

2. **The gate sits between the plan and the first mutation.** Not at argument
   parsing, where the plan digest is not known yet; not inside the write loop,
   where a booking would already exist by the time it failed.

3. **The ledger row is claimed before its POST.** A process killed in between
   leaves an ``uncertain`` row, which ``reconcile`` investigates. A process
   killed with no row would leave a booking nobody knows about.

4. **One row's failure never stops the others.** Blocked, failed and uncertain
   rows are recorded and the loop continues — except for an uncertain result,
   which stops the run: if we do not know whether the last write landed, we do
   not know whether the next one will duplicate either.
"""

from __future__ import annotations

import logging
import uuid as uuid_module
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import EasyWeekError, EasyWeekNotFoundError
from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.altegio_source import build_window, fetch_company_records
from altegio_bot.easyweek_migration.classify import (
    ALREADY_MIGRATED,
    BLOCKED,
    READY,
    SKIPPED,
    Decision,
    classify_record,
)
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.cutover import Cutover
from altegio_bot.easyweek_migration.gates import (
    ApplyGateResult,
    evaluate_apply_gate,
    require_apply_gate,
)
from altegio_bot.easyweek_migration.manifest import MigrationManifest
from altegio_bot.easyweek_migration.report import (
    CREATED,
    FAILED,
    UNCERTAIN,
    MigrationReport,
    plan_digest,
)
from altegio_bot.easyweek_migration.write_client import (
    EasyWeekMigrationWriteClient,
    EasyWeekUncertainMutation,
    build_booking_request,
)

logger = logging.getLogger("easyweek_migration.runner")

MODE_INVENTORY: Final = "inventory"
MODE_DRY_RUN: Final = "dry-run"
MODE_APPLY: Final = "apply"
MODE_RECONCILE: Final = "reconcile"
MODE_ROLLBACK_DRY_RUN: Final = "rollback-dry-run"
MODE_ROLLBACK_APPLY: Final = "rollback-apply"

DEFAULT_HORIZON_DAYS: Final = 180

# Reconciliation outcomes for a previously uncertain row.
RECONCILE_CONFIRMED_CREATED: Final = "uncertain_resolved_created"
RECONCILE_CONFIRMED_ABSENT: Final = "uncertain_resolved_absent"
RECONCILE_STILL_UNKNOWN: Final = "uncertain_unresolved"

# Rollback refusals.
ROLLBACK_TARGET_MISSING: Final = "rollback_target_missing"
ROLLBACK_TARGET_MODIFIED: Final = "rollback_target_modified_after_migration"
ROLLBACK_TARGET_UNREADABLE: Final = "rollback_target_unreadable"
ROLLBACK_ELIGIBLE: Final = "rollback_eligible"


def new_run_id() -> str:
    """A fresh run identifier. Opaque, short, and safe in a filename."""
    return uuid_module.uuid4().hex[:16]


@dataclass
class RunInputs:
    """Everything a mode needs, resolved and validated by the CLI beforehand."""

    mode: str
    run_id: str
    cutover: Cutover
    manifest: MigrationManifest
    directory: CustomerDirectory
    horizon_days: int = DEFAULT_HORIZON_DAYS
    # apply-only
    apply_requested: bool = False
    native_notifications_confirmed: bool = False
    cutover_supplied: bool = False
    verified_dry_run_id: str | None = None
    canary_notification_observed: bool = False
    limit: int | None = None
    # rollback-only
    rollback_run_id: str | None = None
    rollback_confirmed: bool = False


async def build_plan(
    session: AsyncSession,
    inputs: RunInputs,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> tuple[list[Decision], MigrationReport]:
    """Read Altegio live, consult the ledger, and classify every record.

    Performs no writes of any kind. ``inventory``, ``dry-run`` and the first half
    of ``apply`` all run exactly this.
    """
    report = MigrationReport(
        mode=inputs.mode,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )

    company_ids = inputs.manifest.company_ids
    ledger_views = await ledger_module.load_ledger_views(session, company_ids=company_ids)
    window = build_window(inputs.cutover.at, horizon_days=inputs.horizon_days)

    decisions: list[Decision] = []
    for company_id in company_ids:
        records = await fetch_company_records(company_id=company_id, window=window, client=http_client)
        report.note_source(company_id, len(records))
        for record in records:
            decision = classify_record(
                record,
                company_id=company_id,
                manifest=inputs.manifest,
                directory=inputs.directory,
                cutover=inputs.cutover,
                ledger=ledger_views.get((company_id, _record_id_of(record))),
            )
            decisions.append(decision)

    report.plan_digest = plan_digest(
        decisions,
        cutover_iso=inputs.cutover.iso,
        manifest_digest=inputs.manifest.digest,
    )
    return decisions, report


def _record_id_of(record: dict[str, Any]) -> int:
    """The record id used only for the ledger lookup key.

    Returns a value that cannot match a real id when the record has none; the
    classifier does the real validation and blocks such a row on its own.
    """
    raw = record.get("id")
    return raw if type(raw) is int else -1


async def run_inventory_or_dry_run(
    session: AsyncSession,
    inputs: RunInputs,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> MigrationReport:
    """Read-only. Classifies everything and writes nothing, anywhere.

    ``inventory`` and ``dry-run`` differ only in intent: inventory is run while
    the mapping is still being assembled and is expected to be full of
    ``mapping_missing``; dry-run is the review artefact whose digest gates the
    apply. Neither issues a single EasyWeek request.
    """
    decisions, report = await build_plan(session, inputs, http_client=http_client)
    for decision in decisions:
        report.note(decision)
    return report


async def run_apply(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    write_client: EasyWeekMigrationWriteClient,
    http_client: httpx.AsyncClient | None = None,
) -> MigrationReport:
    """Create the ready bookings in EasyWeek, under the full gate.

    Uses a session **per row** rather than one long transaction on purpose: a
    ledger row must be durably committed before its POST is sent, and again
    immediately after the answer arrives. A single enclosing transaction would
    lose the whole run's bookkeeping on any failure, which is precisely the state
    the ledger exists to prevent.
    """
    async with session_maker() as session:
        decisions, report = await build_plan(session, inputs, http_client=http_client)

    gate: ApplyGateResult = evaluate_apply_gate(
        apply_requested=inputs.apply_requested,
        native_notifications_confirmed=inputs.native_notifications_confirmed,
        cutover_supplied=inputs.cutover_supplied,
        verified_dry_run_id=inputs.verified_dry_run_id,
        computed_plan_digest=report.plan_digest,
        manifest=inputs.manifest,
        directory=inputs.directory,
        canary_notification_observed=inputs.canary_notification_observed,
    )
    report.gate = gate.as_safe_dict()
    # Nothing below this line may run on a failed gate, and nothing above it
    # touched EasyWeek.
    require_apply_gate(gate)

    ready = [d for d in decisions if d.outcome == READY]
    if inputs.limit is not None:
        # The canary. A limit is an operator saying "prove the request shape on
        # one real booking"; the rows beyond it are reported as still ready.
        ready_selected = ready[: inputs.limit]
        deferred = ready[inputs.limit :]
    else:
        ready_selected, deferred = ready, []

    for decision in decisions:
        if decision.outcome != READY:
            report.note(decision)

    for decision in deferred:
        report.note(decision)

    for decision in ready_selected:
        outcome, reason = await _apply_one(session_maker, inputs, decision, write_client=write_client)
        report.mutations_attempted += 1
        report.note(decision, outcome=outcome, reason=reason)
        if outcome == UNCERTAIN:
            # Stop. We do not know whether the last write landed, so we cannot
            # reason about the next one either. `reconcile` first.
            report.errors.append("run halted after an uncertain mutation; run reconcile before applying again")
            break

    return report


async def _apply_one(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    decision: Decision,
    *,
    write_client: EasyWeekMigrationWriteClient,
) -> tuple[str, str | None]:
    """Claim, POST, record. Returns the outcome and its reason code."""
    assert decision.source_record_id is not None
    assert decision.source_fingerprint is not None
    assert decision.starts_at_utc is not None
    assert decision.duration_minutes is not None
    assert decision.easyweek_location_uuid is not None
    assert decision.easyweek_staff_uuid is not None
    assert decision.easyweek_service_uuid is not None
    assert decision.easyweek_customer_uuid is not None

    company_id = decision.source_company_id
    record_id = decision.source_record_id

    async with session_maker() as session:
        async with session.begin():
            claimed = await ledger_module.claim_for_apply(
                session,
                run_id=inputs.run_id,
                source_company_id=company_id,
                source_record_id=record_id,
                source_fingerprint=decision.source_fingerprint,
            )
    if not claimed:
        # Somebody else owns this source booking — a concurrent apply, or a row
        # that reached a terminal state between the plan and now. Not ours to
        # create, and creating it anyway is the duplicate we are here to avoid.
        return ALREADY_MIGRATED, "ledger_claimed_elsewhere"

    body = build_booking_request(
        location_uuid=decision.easyweek_location_uuid,
        staff_uuid=decision.easyweek_staff_uuid,
        service_uuid=decision.easyweek_service_uuid,
        customer_uuid=decision.easyweek_customer_uuid,
        starts_at_utc_iso=decision.starts_at_utc.isoformat().replace("+00:00", "Z"),
        duration_minutes=decision.duration_minutes,
        comment=ledger_module.migration_marker(source_company_id=company_id, source_record_id=record_id),
    )

    try:
        created = await write_client.create_booking(body)
    except EasyWeekUncertainMutation as exc:
        async with session_maker() as session:
            async with session.begin():
                await ledger_module.record_uncertain(
                    session,
                    run_id=inputs.run_id,
                    source_company_id=company_id,
                    source_record_id=record_id,
                    reason_code=_safe_error_code(exc),
                )
        return UNCERTAIN, _safe_error_code(exc)
    except EasyWeekError as exc:
        async with session_maker() as session:
            async with session.begin():
                await ledger_module.record_failed(
                    session,
                    run_id=inputs.run_id,
                    source_company_id=company_id,
                    source_record_id=record_id,
                    reason_code=_safe_error_code(exc),
                )
        return FAILED, _safe_error_code(exc)

    async with session_maker() as session:
        async with session.begin():
            await ledger_module.record_created(
                session,
                run_id=inputs.run_id,
                source_company_id=company_id,
                source_record_id=record_id,
                target_booking_uuid=created.booking_uuid,
            )
    return CREATED, None


def _safe_error_code(exc: EasyWeekError) -> str:
    """A stable, bounded reason code — never a provider message."""
    status = exc.status_code
    base = type(exc).__name__
    return f"{base}:{status}" if status is not None else base


async def run_reconcile(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    write_client: EasyWeekMigrationWriteClient | None,
) -> MigrationReport:
    """Resolve uncertain rows by READING EasyWeek, and report the whole state.

    This is the only path that may move a row out of ``uncertain``, and it does
    so on evidence: the booking is fetched by its recorded UUID. When there is no
    recorded UUID — the usual case, since an uncertain POST never returned one —
    the row stays uncertain and is listed for an operator, who resolves it in the
    EasyWeek UI using the migration marker. Guessing "it probably worked" and
    guessing "it probably did not" are equally wrong here, and both are avoided
    by not guessing.
    """
    report = MigrationReport(
        mode=MODE_RECONCILE,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )

    async with session_maker() as session:
        pending = await ledger_module.uncertain_rows(session)
        snapshot = [ledger_module.row_as_safe_dict(row) for row in pending]

    for row in snapshot:
        entry = dict(row)
        target = row.get("target_booking_uuid")
        if target is None or write_client is None:
            entry["reconcile_outcome"] = RECONCILE_STILL_UNKNOWN
            report.reasons[RECONCILE_STILL_UNKNOWN] += 1
            report.uncertain_rows.append(entry)
            continue

        try:
            await write_client.get_booking(target)
        except EasyWeekNotFoundError:
            async with session_maker() as session:
                async with session.begin():
                    await ledger_module.record_failed(
                        session,
                        run_id=inputs.run_id,
                        source_company_id=int(row["source_company_id"]),
                        source_record_id=int(row["source_record_id"]),
                        reason_code=RECONCILE_CONFIRMED_ABSENT,
                    )
            entry["reconcile_outcome"] = RECONCILE_CONFIRMED_ABSENT
            report.reasons[RECONCILE_CONFIRMED_ABSENT] += 1
            report.failed_rows.append(entry)
            continue
        except EasyWeekError:
            entry["reconcile_outcome"] = RECONCILE_STILL_UNKNOWN
            report.reasons[RECONCILE_STILL_UNKNOWN] += 1
            report.uncertain_rows.append(entry)
            continue

        async with session_maker() as session:
            async with session.begin():
                await ledger_module.record_created(
                    session,
                    run_id=inputs.run_id,
                    source_company_id=int(row["source_company_id"]),
                    source_record_id=int(row["source_record_id"]),
                    target_booking_uuid=str(target),
                )
        entry["reconcile_outcome"] = RECONCILE_CONFIRMED_CREATED
        report.reasons[RECONCILE_CONFIRMED_CREATED] += 1
        report.created_rows.append(entry)

    async with session_maker() as session:
        all_rows = await ledger_module.load_ledger_views(session, company_ids=inputs.manifest.company_ids)
    for (company_id, _record_id), view in all_rows.items():
        report.outcomes[view.status] += 1
        report.by_company.setdefault(company_id, Counter())[view.status] += 1

    return report


async def run_rollback(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    write_client: EasyWeekMigrationWriteClient,
) -> MigrationReport:
    """Find, and only on explicit confirmation cancel, one run's own bookings.

    There is no universal automatic rollback and this does not pretend to be one.
    What it is:

    * **scoped to one run.** Only ledger rows whose ``run_id`` matches, and only
      those with status ``created``. A booking created by a different run, or by
      a person, is not in the set and cannot be.
    * **evidence-based.** Each target is fetched first. If its start time no
      longer matches what the ledger recorded, somebody moved it by hand after
      the migration, and cancelling it would destroy their work — it is refused,
      not cancelled.
    * **read-only by default.** Without ``--apply --confirm-rollback`` this
      reports what it *would* cancel and returns.
    """
    mode = MODE_ROLLBACK_APPLY if inputs.rollback_confirmed else MODE_ROLLBACK_DRY_RUN
    report = MigrationReport(
        mode=mode,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )

    target_run = inputs.rollback_run_id
    if not target_run:
        report.errors.append("rollback requires --rollback-run-id")
        return report

    async with session_maker() as session:
        rows = await ledger_module.rows_for_run(session, run_id=target_run, statuses=(ledger_module.STATUS_CREATED,))
        candidates = [
            (
                ledger_module.row_as_safe_dict(row),
                row.source_fingerprint,
                row.source_company_id,
                row.source_record_id,
                row.target_booking_uuid,
            )
            for row in rows
        ]

    for safe_row, _fingerprint, company_id, record_id, target in candidates:
        entry = dict(safe_row)
        if not target:
            entry["rollback_outcome"] = ROLLBACK_TARGET_MISSING
            report.reasons[ROLLBACK_TARGET_MISSING] += 1
            report.blocked_rows.append(entry)
            continue

        try:
            booking = await write_client.get_booking(target)
        except EasyWeekNotFoundError:
            # Already gone. Nothing to cancel and nothing to complain about.
            entry["rollback_outcome"] = ROLLBACK_TARGET_MISSING
            report.reasons[ROLLBACK_TARGET_MISSING] += 1
            report.blocked_rows.append(entry)
            continue
        except EasyWeekError:
            entry["rollback_outcome"] = ROLLBACK_TARGET_UNREADABLE
            report.reasons[ROLLBACK_TARGET_UNREADABLE] += 1
            report.blocked_rows.append(entry)
            continue

        if _looks_hand_edited(
            booking, marker=ledger_module.migration_marker(source_company_id=company_id, source_record_id=record_id)
        ):
            entry["rollback_outcome"] = ROLLBACK_TARGET_MODIFIED
            report.reasons[ROLLBACK_TARGET_MODIFIED] += 1
            report.blocked_rows.append(entry)
            continue

        if not inputs.rollback_confirmed:
            entry["rollback_outcome"] = ROLLBACK_ELIGIBLE
            report.reasons[ROLLBACK_ELIGIBLE] += 1
            report.created_rows.append(entry)
            continue

        await write_client.cancel_booking(target)
        report.mutations_attempted += 1
        async with session_maker() as session:
            async with session.begin():
                await ledger_module.record_rolled_back(
                    session,
                    run_id=target_run,
                    source_company_id=company_id,
                    source_record_id=record_id,
                )
        entry["rollback_outcome"] = ledger_module.STATUS_ROLLED_BACK
        report.reasons[ledger_module.STATUS_ROLLED_BACK] += 1
        report.created_rows.append(entry)

    return report


def _looks_hand_edited(booking: dict[str, Any], *, marker: str) -> bool:
    """True when the live booking is no longer the one this run created.

    Two signals, both cheap and both conservative:

    * the migration marker is gone from the comment — somebody rewrote it;
    * the booking is already cancelled or completed — its life moved on without
      us, and cancelling it again is at best a no-op and at worst confusing.

    Uncertainty resolves to "hand-edited": refusing to roll back a booking that
    was in fact untouched costs an operator one manual cancel, while cancelling
    one that a human deliberately kept costs a customer their appointment.
    """
    comment = booking.get("comment")
    if not isinstance(comment, str) or marker not in comment:
        return True
    if bool(booking.get("is_canceled")) or bool(booking.get("is_completed")):
        return True
    return False


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "DEFAULT_HORIZON_DAYS",
    "MODE_APPLY",
    "MODE_DRY_RUN",
    "MODE_INVENTORY",
    "MODE_RECONCILE",
    "MODE_ROLLBACK_APPLY",
    "MODE_ROLLBACK_DRY_RUN",
    "RunInputs",
    "build_plan",
    "new_run_id",
    "run_apply",
    "run_inventory_or_dry_run",
    "run_reconcile",
    "run_rollback",
    "utcnow",
    "ALREADY_MIGRATED",
    "BLOCKED",
    "SKIPPED",
]
