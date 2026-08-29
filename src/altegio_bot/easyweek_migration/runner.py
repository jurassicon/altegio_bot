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
from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult, verify_branch_identity
from altegio_bot.easyweek_migration.canary import (
    CANARY_IDENTITY_REQUIRED,
    CANARY_NOT_IN_PLAN,
    CANARY_NOT_READY,
    CANARY_POST_FAILED,
    CANARY_POST_UNCERTAIN,
    CANARY_READBACK_FAILED,
    CANARY_REPROOF_FAILED,
    CanaryVerdict,
    build_binding,
    find_licensing_proof,
    record_proof,
)
from altegio_bot.easyweek_migration.classify import (
    ALREADY_MIGRATED,
    BLOCK_SOURCE_CHANGED,
    BLOCKED,
    READY,
    SKIP_STAFF_DEFERRED,
    SKIPPED,
    Decision,
    classify_record,
)
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.cutover import Cutover
from altegio_bot.easyweek_migration.gates import (
    GATE_BRANCH_IDENTITY_UNPROVEN,
    GATE_MANIFEST_INVALID,
    ApplyGateResult,
    evaluate_apply_gate,
    require_apply_gate,
)
from altegio_bot.easyweek_migration.manifest import (
    STAFF_DEFERRED,
    STAFF_SELECTED,
    STAFF_UNKNOWN,
    MigrationManifest,
)
from altegio_bot.easyweek_migration.proof import (
    TARGET_SNAPSHOT_MISSING,
    TARGET_UUID_MISSING,
    expected_target_for,
    prove_live_target,
)
from altegio_bot.easyweek_migration.report import (
    CREATED,
    FAILED,
    UNCERTAIN,
    MigrationReport,
    plan_digest,
)
from altegio_bot.easyweek_migration.reproof import (
    reclassify_source_for_resolution,
    reprove_source_booking,
)
from altegio_bot.easyweek_migration.target_snapshot import (
    TargetSnapshotError,
    compare,
    expected_snapshot,
    project_target,
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
MODE_CANARY: Final = "canary"
MODE_RESOLVE_CREATED: Final = "resolve-created"
MODE_RESOLVE_ABSENT: Final = "resolve-absent"

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
ROLLBACK_NO_SNAPSHOT: Final = "rollback_target_snapshot_missing"
ROLLBACK_GATE_REFUSED: Final = "rollback_notification_gate_refused"

# Operator resolution of an unknown outcome.
RESOLVE_NOT_UNCERTAIN: Final = "row_is_not_uncertain"
RESOLVE_ROW_MISSING: Final = "ledger_row_not_found"
RESOLVE_TARGET_UNREADABLE: Final = "target_unreadable"
RESOLVE_TARGET_ABSENT: Final = "target_not_found"
RESOLVE_PROOF_FAILED: Final = "target_does_not_match_source_identity"
RESOLVE_CONFIRMED: Final = "resolved_created"
RESOLVE_ABSENT_CONFIRMED: Final = "resolved_absent_by_operator"
RESOLVE_ABSENT_UNCONFIRMED: Final = "absent_resolution_not_confirmed"
# A resolution path needs the source to rebuild what SHOULD be in EasyWeek,
# and the customer directory to rebuild which customer it was for.
RESOLVE_SOURCE_UNPROVEN: Final = "source_could_not_be_reproved"
RESOLVE_INPUTS_MISSING: Final = "resolution_inputs_missing"

# Final reconciliation, per row.
COMPLETENESS_TARGET_UNPROVEN: Final = "target_not_proven_in_easyweek"
COMPLETENESS_NO_LEDGER_ROW: Final = "no_ledger_row_for_active_booking"
COMPLETENESS_LEDGER_NOT_CREATED: Final = "ledger_row_not_created"


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
    # canary-only: the ONE booking an operator names, by exact source identity.
    canary_company_id: int | None = None
    canary_record_id: int | None = None
    # rollback-only
    rollback_run_id: str | None = None
    rollback_confirmed: bool = False
    # resolution-only
    resolve_company_id: int | None = None
    resolve_record_id: int | None = None
    resolve_target_booking_uuid: str | None = None
    resolve_absent_acknowledged: bool = False
    resolve_absent_confirmed: bool = False
    # reconcile-only
    final: bool = False


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
    # Which source identifiers the live bookings actually use. Collected on every
    # plan because it costs nothing, surfaced by `inventory`, which is the mode
    # whose job is to tell an operator what the manifest still needs.
    staff_seen: dict[int, Counter] = {}
    service_seen: dict[int, Counter] = {}
    # company_id -> altegio staff id -> {"scope": ..., "active_bookings": n}
    wave_seen: dict[int, dict[int, dict[str, Any]]] = {}

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
            # A deferred master's booking is skipped, but it is NOT invisible:
            # the whole point of naming the wave is that an operator can count
            # what it left behind. So the wave tally sees every booking that is
            # in scope for the branch and the window, deferred ones included,
            # and only genuinely out-of-scope rows (past, cancelled, finished)
            # are left out.
            if decision.outcome != SKIPPED or decision.reason == SKIP_STAFF_DEFERRED:
                _collect_wave_counts(
                    record,
                    decision,
                    inputs.manifest.branch(company_id),
                    wave_seen.setdefault(company_id, {}),
                )
            if decision.outcome != SKIPPED:
                _collect_identifiers(
                    record,
                    staff_seen.setdefault(company_id, Counter()),
                    service_seen.setdefault(company_id, Counter()),
                )

    report.source_identifiers = _identifier_summary(inputs, staff_seen, service_seen)
    report.wave = _wave_summary(inputs, wave_seen)

    report.plan_digest = plan_digest(
        decisions,
        cutover_iso=inputs.cutover.iso,
        manifest_digest=inputs.manifest.digest,
    )
    return decisions, report


def _collect_identifiers(record: dict[str, Any], staff: Counter, services: Counter) -> None:
    """Tally the Altegio staff and service ids one active booking uses.

    Numeric ids only. They are technical source identifiers, not customer data,
    and an operator needs to read them to build the mapping — which is exactly
    why the names, phones and payloads that sit next to them never come along.
    """
    staff_id = _staff_id_of(record)
    if type(staff_id) is int:
        staff[staff_id] += 1
    raw_services = record.get("services")
    if isinstance(raw_services, list):
        for item in raw_services:
            if isinstance(item, dict) and type(item.get("id")) is int:
                services[item["id"]] += 1


def _staff_id_of(record: dict[str, Any]) -> Any:
    flat = record.get("staff_id")
    if flat is not None:
        return flat
    staff = record.get("staff")
    if isinstance(staff, dict):
        return staff.get("id")
    return None


def _identifier_summary(
    inputs: RunInputs,
    staff_seen: dict[int, Counter],
    service_seen: dict[int, Counter],
) -> dict[str, Any]:
    """Per-company: ids in use, how often, and which the manifest already covers.

    ``missing`` is the actionable half — those are the ids an operator has to
    look up in EasyWeek and write into the manifest before a dry-run can pass.
    """
    summary: dict[str, Any] = {}
    for company_id in sorted(set(staff_seen) | set(service_seen)):
        branch = inputs.manifest.branch(company_id)
        staff_counts = staff_seen.get(company_id, Counter())
        service_counts = service_seen.get(company_id, Counter())
        mapped_staff = set(branch.staff) if branch else set()
        mapped_services = set(branch.services) if branch else set()

        summary[str(company_id)] = {
            "staff": {
                "bookings_by_altegio_staff_id": {str(k): v for k, v in sorted(staff_counts.items())},
                "mapped": sorted(set(staff_counts) & mapped_staff),
                "missing": sorted(set(staff_counts) - mapped_staff),
            },
            "services": {
                "bookings_by_altegio_service_id": {str(k): v for k, v in sorted(service_counts.items())},
                "mapped": sorted(set(service_counts) & mapped_services),
                "missing": sorted(set(service_counts) - mapped_services),
            },
        }
    return summary


def _collect_wave_counts(
    record: dict[str, Any],
    decision: Decision,
    branch: Any,
    per_staff: dict[int, dict[str, Any]],
) -> None:
    """Tally one in-scope booking against its master's wave classification.

    Counting by Altegio staff id, not by name: the operator compares these
    numbers against the Altegio and EasyWeek interfaces, and an id is the only
    thing all three agree on. A name here would also be PII-adjacent for a small
    salon, and it would be a second, softer identity next to the id.
    """
    staff_id = _staff_id_of(record)
    if type(staff_id) is not int:
        # An unreadable master id cannot be tallied per master; it is already
        # blocked as unknown, and the branch-level `unknown` total covers it.
        per_staff.setdefault(-1, {"scope": STAFF_UNKNOWN, "active_bookings": 0})
        per_staff[-1]["active_bookings"] += 1
        return
    scope = branch.staff_scope(staff_id) if branch is not None else STAFF_UNKNOWN
    entry = per_staff.setdefault(staff_id, {"scope": scope, "active_bookings": 0})
    entry["active_bookings"] += 1
    if decision.outcome == BLOCKED:
        entry["blocked"] = entry.get("blocked", 0) + 1


def _wave_summary(inputs: RunInputs, wave_seen: dict[int, dict[int, dict[str, Any]]]) -> dict[str, Any]:
    """Per branch and per Altegio staff id: how many active bookings, and whose wave.

    This is the number an operator reads next to the Altegio and EasyWeek screens
    to answer "did everything I expected actually move?". Ids and counts only —
    no names, no customers, no payloads.
    """
    summary: dict[str, Any] = {}
    for company_id in sorted(set(wave_seen) | set(inputs.manifest.company_ids)):
        per_staff = wave_seen.get(company_id, {})
        branch = inputs.manifest.branch(company_id)
        selected_ids = sorted(branch.selected_staff_ids) if branch else []
        deferred_ids = sorted(branch.deferred_staff_ids) if branch else []

        by_scope: Counter = Counter()
        staff_rows: dict[str, Any] = {}
        for staff_id, entry in sorted(per_staff.items()):
            by_scope[entry["scope"]] += entry["active_bookings"]
            staff_rows[str(staff_id)] = {
                "scope": entry["scope"],
                "active_bookings": entry["active_bookings"],
                "blocked": entry.get("blocked", 0),
            }

        summary[str(company_id)] = {
            "selected_staff_ids": selected_ids,
            "deferred_staff_ids": deferred_ids,
            "active_bookings_total": sum(entry["active_bookings"] for entry in per_staff.values()),
            "active_bookings_selected": by_scope.get(STAFF_SELECTED, 0),
            "active_bookings_deferred": by_scope.get(STAFF_DEFERRED, 0),
            "active_bookings_unknown_staff": by_scope.get(STAFF_UNKNOWN, 0),
            "by_altegio_staff_id": staff_rows,
        }
    return summary


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

    ``inventory`` runs while the mapping is still being assembled — it accepts an
    unfinished manifest and its whole job is to say which Altegio staff and
    service ids the future bookings actually use, so an operator knows what to
    fill in. ``dry-run`` is the review artefact whose digest gates the apply and
    demands a complete manifest.

    Neither issues a single EasyWeek request.
    """
    decisions, report = await build_plan(session, inputs, http_client=http_client)
    for decision in decisions:
        report.note(decision)

    return report


async def _prepare_write_gate(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    report: MigrationReport,
    require_canary_proof: bool,
    http_client: httpx.AsyncClient | None = None,
) -> tuple[list[Decision], BranchIdentityResult]:
    """Plan, prove the branches, check the canary, and pass the gate — or raise.

    Shared by ``apply``, ``canary`` and the rollback write path so that the three
    things that can mutate EasyWeek cannot drift apart on what they check first.
    Nothing here touches EasyWeek.
    """
    async with session_maker() as session:
        decisions, planned = await build_plan(session, inputs, http_client=http_client)
    report.plan_digest = planned.plan_digest
    report.source_records_fetched = planned.source_records_fetched
    # The wave counters travel with the plan: an apply report has to answer the
    # same "what did this wave leave behind?" question a dry-run does.
    report.wave = planned.wave
    report.source_identifiers = planned.source_identifiers

    # The manifest says which EasyWeek location a branch maps to; only the
    # runtime registry can say whether that location IS that branch.
    branch_identity = verify_branch_identity(inputs.manifest)

    canary_verdict: CanaryVerdict | None = None
    if require_canary_proof:
        binding = build_binding(
            manifest_digest=inputs.manifest.digest,
            cutover_at=inputs.cutover.at,
            branch_result=branch_identity,
        )
        async with session_maker() as session:
            canary_verdict = await find_licensing_proof(session, binding=binding)

    gate: ApplyGateResult = evaluate_apply_gate(
        apply_requested=inputs.apply_requested,
        native_notifications_confirmed=inputs.native_notifications_confirmed,
        cutover_supplied=inputs.cutover_supplied,
        verified_dry_run_id=inputs.verified_dry_run_id,
        computed_plan_digest=report.plan_digest,
        manifest=inputs.manifest,
        directory=inputs.directory,
        canary_notification_observed=inputs.canary_notification_observed,
        branch_identity=branch_identity,
        canary_verdict=canary_verdict,
        require_canary_proof=require_canary_proof,
    )
    report.gate = gate.as_safe_dict()
    # Nothing below this line may run on a failed gate, and nothing above it
    # touched EasyWeek.
    require_apply_gate(gate)
    return decisions, branch_identity


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

    A bulk apply requires a verified canary proof that still applies to this
    manifest, request schema, cutover and branch mapping. ``--limit`` no longer
    licenses anything; it is gone, and :func:`run_canary` is the only way to
    create the one booking that earns the proof.
    """
    report = MigrationReport(
        mode=inputs.mode,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )
    decisions, _branches = await _prepare_write_gate(
        session_maker,
        inputs,
        report=report,
        require_canary_proof=True,
        http_client=http_client,
    )

    for decision in decisions:
        if decision.outcome != READY:
            report.note(decision)

    for decision in (d for d in decisions if d.outcome == READY):
        outcome, reason = await _apply_one(
            session_maker, inputs, decision, write_client=write_client, http_client=http_client
        )
        if outcome in (CREATED, UNCERTAIN, FAILED):
            report.mutations_attempted += 1
        report.note(decision, outcome=outcome, reason=reason)
        if outcome == UNCERTAIN:
            # Stop. We do not know whether the last write landed, so we cannot
            # reason about the next one either. `reconcile` first.
            report.errors.append("run halted after an uncertain mutation; run reconcile before applying again")
            break

    return report


async def run_canary(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    write_client: EasyWeekMigrationWriteClient,
    http_client: httpx.AsyncClient | None = None,
) -> MigrationReport:
    """Create ONE named booking, read it back, and record durable proof.

    The booking is chosen by exact source identity, never by position in an API
    response: the same plan in a different order must canary the same customer,
    and an operator has to be able to go and look at the booking afterwards.
    """
    report = MigrationReport(
        mode=MODE_CANARY,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )

    if inputs.canary_company_id is None or inputs.canary_record_id is None:
        report.errors.append(CANARY_IDENTITY_REQUIRED)
        return report

    # The canary is the one apply that cannot have a canary proof yet.
    decisions, branch_identity = await _prepare_write_gate(
        session_maker,
        inputs,
        report=report,
        require_canary_proof=False,
        http_client=http_client,
    )
    binding = build_binding(
        manifest_digest=inputs.manifest.digest,
        cutover_at=inputs.cutover.at,
        branch_result=branch_identity,
    )
    report.canary_binding = binding.as_safe_dict()

    chosen: Decision | None = None
    for decision in decisions:
        if (
            decision.source_company_id == inputs.canary_company_id
            and decision.source_record_id == inputs.canary_record_id
        ):
            chosen = decision
            break

    for decision in decisions:
        if decision is not chosen:
            report.note(decision)

    if chosen is None:
        # Naming a booking the reviewed plan never contained would let the canary
        # prove the schema on a row nobody approved.
        report.errors.append(CANARY_NOT_IN_PLAN)
        return report
    if chosen.outcome != READY:
        report.note(chosen)
        report.errors.append(CANARY_NOT_READY)
        return report

    outcome, reason = await _apply_one(
        session_maker,
        inputs,
        chosen,
        write_client=write_client,
        http_client=http_client,
        verify_readback=True,
        binding_for_proof=binding,
    )
    if outcome in (CREATED, UNCERTAIN, FAILED):
        report.mutations_attempted += 1
    report.note(chosen, outcome=outcome, reason=reason)

    # A canary is only green when the booking was created AND read back clean.
    # `CREATED` with a reason means the write landed but the verification did
    # not — the booking exists and is rollback-able, and it licenses nothing.
    if outcome != CREATED or reason is not None:
        report.errors.append(reason or CANARY_POST_FAILED)
    return report


async def _apply_one(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    decision: Decision,
    *,
    write_client: EasyWeekMigrationWriteClient,
    http_client: httpx.AsyncClient | None = None,
    verify_readback: bool = False,
    binding_for_proof: Any = None,
) -> tuple[str, str | None]:
    """Re-prove, claim, POST, read back, record. Returns outcome and reason code."""
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

    # -- LAST look at the source, before the claim and before the POST -----
    # The plan was built once; a bulk run walks it for many minutes, paced by the
    # EasyWeek rate limit. In those minutes a booking can be cancelled, moved, or
    # reassigned, and creating it from the old snapshot books a customer who has
    # already called to cancel. Re-proof happens BEFORE the ledger claim so a
    # refusal leaves nothing that looks like an unresolved mutation.
    reproof = await reprove_source_booking(
        decision,
        manifest=inputs.manifest,
        directory=inputs.directory,
        cutover=inputs.cutover,
        http_client=http_client,
    )
    if not reproof.confirmed:
        detail = f"{reproof.reason}:{reproof.detail}" if reproof.detail else reproof.reason
        if binding_for_proof is not None:
            await _store_canary_proof(
                session_maker,
                inputs,
                decision,
                binding=binding_for_proof,
                verified=False,
                target_booking_uuid=None,
                target_snapshot=None,
                failure_reason=CANARY_REPROOF_FAILED,
            )
        return BLOCKED, detail

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

    marker = ledger_module.migration_marker(source_company_id=company_id, source_record_id=record_id)
    starts_at_iso = decision.starts_at_utc.isoformat().replace("+00:00", "Z")
    body = build_booking_request(
        location_uuid=decision.easyweek_location_uuid,
        staff_uuid=decision.easyweek_staff_uuid,
        service_uuid=decision.easyweek_service_uuid,
        customer_uuid=decision.easyweek_customer_uuid,
        starts_at_utc_iso=starts_at_iso,
        duration_minutes=decision.duration_minutes,
        comment=marker,
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
        if binding_for_proof is not None:
            await _store_canary_proof(
                session_maker,
                inputs,
                decision,
                binding=binding_for_proof,
                verified=False,
                target_booking_uuid=None,
                target_snapshot=None,
                failure_reason=CANARY_POST_UNCERTAIN,
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
        if binding_for_proof is not None:
            await _store_canary_proof(
                session_maker,
                inputs,
                decision,
                binding=binding_for_proof,
                verified=False,
                target_booking_uuid=None,
                target_snapshot=None,
                failure_reason=CANARY_POST_FAILED,
            )
        return FAILED, _safe_error_code(exc)

    wanted = expected_snapshot(
        booking_uuid=created.booking_uuid,
        location_uuid=decision.easyweek_location_uuid,
        staff_uuid=decision.easyweek_staff_uuid,
        service_uuid=decision.easyweek_service_uuid,
        customer_uuid=decision.easyweek_customer_uuid,
        start_time_utc=starts_at_iso,
        duration_minutes=decision.duration_minutes,
        marker=marker,
    )

    readback_failure: str | None = None
    stored_snapshot = wanted
    if verify_readback:
        # The canary reads the booking back and compares every write-critical
        # field. A 2xx alone says the request was accepted, not that it landed
        # where we meant it to.
        try:
            live_payload = await write_client.get_booking(created.booking_uuid)
            live = project_target(live_payload, expected_marker=marker)
        except TargetSnapshotError as exc:
            readback_failure = f"{CANARY_READBACK_FAILED}:{exc.reason}"
        except EasyWeekError:
            readback_failure = CANARY_READBACK_FAILED
        else:
            mismatch = compare(live, wanted)
            if not mismatch.matched:
                readback_failure = f"{CANARY_READBACK_FAILED}:{mismatch.reasons[0]}"
            else:
                stored_snapshot = live

    async with session_maker() as session:
        async with session.begin():
            await ledger_module.record_created(
                session,
                run_id=inputs.run_id,
                source_company_id=company_id,
                source_record_id=record_id,
                target_booking_uuid=created.booking_uuid,
                target_snapshot_fingerprint=stored_snapshot.fingerprint,
            )

    if binding_for_proof is not None:
        await _store_canary_proof(
            session_maker,
            inputs,
            decision,
            binding=binding_for_proof,
            verified=readback_failure is None,
            target_booking_uuid=created.booking_uuid,
            target_snapshot=stored_snapshot if readback_failure is None else None,
            failure_reason=readback_failure,
        )

    if readback_failure is not None:
        # The booking exists — it is recorded and rollback-able — but it is NOT
        # proof of a correct request shape, so no bulk follows.
        return CREATED, readback_failure
    return CREATED, None


async def _store_canary_proof(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    decision: Decision,
    *,
    binding: Any,
    verified: bool,
    target_booking_uuid: str | None,
    target_snapshot: Any,
    failure_reason: str | None,
) -> None:
    assert decision.source_record_id is not None
    async with session_maker() as session:
        async with session.begin():
            await record_proof(
                session,
                run_id=inputs.run_id,
                binding=binding,
                source_company_id=decision.source_company_id,
                source_record_id=decision.source_record_id,
                source_fingerprint=decision.source_fingerprint or "",
                verified=verified,
                target_booking_uuid=target_booking_uuid,
                target_snapshot=target_snapshot,
                failure_reason=failure_reason,
            )


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
    http_client: httpx.AsyncClient | None = None,
) -> MigrationReport:
    """Report the state of the cutover, and — with ``--final`` — prove it is complete.

    Two jobs, deliberately in one command because they answer the same question
    at different strengths.

    The everyday job resolves what CAN be resolved on evidence. A 404 proves a
    booking is not there. A 2xx proves only that *something* is there — so a row
    is promoted to ``created`` only after the same full proof ``resolve-created``
    uses: the source still describes the attempted booking, and the live booking
    matches it field for field. A row with no UUID — the usual shape after a
    timeout — stays unresolved, because "it probably worked" and "it probably did
    not" are equally wrong and both are avoided by not guessing. The operator
    resolves those explicitly through ``resolve-created`` / ``resolve-absent``.

    The ``--final`` job is the one that says the migration is over, and it cannot
    be answered from the ledger alone: a ledger listing only proves things about
    rows we happen to have. So it re-reads the live Altegio source and checks
    every active booking against a proven target. An empty source that was never
    actually read is not success — it is an unasked question.
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
        # The source fingerprint is carried alongside the report-safe row: it is
        # what the proof compares the live source against, and it never reaches
        # the report itself.
        snapshot = [(ledger_module.row_as_safe_dict(row), row.source_fingerprint) for row in pending]

    for row, stored_fingerprint in snapshot:
        entry = dict(row)
        target = row.get("target_booking_uuid")
        company_id = int(row["source_company_id"])
        record_id = int(row["source_record_id"])

        def _leave_uncertain(reason: str) -> None:
            entry["reconcile_outcome"] = reason
            report.reasons[reason] += 1
            report.uncertain_rows.append(entry)

        if target is None or write_client is None:
            _leave_uncertain(RECONCILE_STILL_UNKNOWN)
            continue

        # A 404 is the one thing a bare GET *can* prove: the booking named by
        # this row is not in EasyWeek. Everything else needs the full proof.
        try:
            await write_client.get_booking(str(target))
        except EasyWeekNotFoundError:
            async with session_maker() as session:
                async with session.begin():
                    await ledger_module.record_failed(
                        session,
                        run_id=inputs.run_id,
                        source_company_id=company_id,
                        source_record_id=record_id,
                        reason_code=RECONCILE_CONFIRMED_ABSENT,
                    )
            entry["reconcile_outcome"] = RECONCILE_CONFIRMED_ABSENT
            report.reasons[RECONCILE_CONFIRMED_ABSENT] += 1
            report.failed_rows.append(entry)
            continue
        except EasyWeekError:
            _leave_uncertain(RECONCILE_STILL_UNKNOWN)
            continue

        # The booking exists. That is NOT proof it is the right booking — the
        # earlier version stopped here and promoted the row on a bare 2xx, which
        # would accept a booking at the wrong time, for the wrong master or the
        # wrong customer. Same proof as `resolve-created`, and it also produces
        # the target fingerprint this row was missing.
        if stored_fingerprint is None:
            _leave_uncertain(f"{RECONCILE_STILL_UNKNOWN}:{RESOLVE_ROW_MISSING}")
            continue

        proven, reason, live = await _prove_uncertain_row_against_target(
            inputs,
            company_id=company_id,
            record_id=record_id,
            stored_source_fingerprint=stored_fingerprint,
            target_booking_uuid=str(target),
            write_client=write_client,
            http_client=http_client,
        )
        if not proven or live is None:
            _leave_uncertain(reason)
            continue

        async with session_maker() as session:
            async with session.begin():
                await ledger_module.record_created(
                    session,
                    run_id=inputs.run_id,
                    source_company_id=company_id,
                    source_record_id=record_id,
                    target_booking_uuid=live.booking_uuid,
                    target_snapshot_fingerprint=live.fingerprint,
                )
        entry["reconcile_outcome"] = RECONCILE_CONFIRMED_CREATED
        report.reasons[RECONCILE_CONFIRMED_CREATED] += 1
        report.created_rows.append(entry)

    async with session_maker() as session:
        ledger_rows = await ledger_module.all_rows(session, company_ids=inputs.manifest.company_ids)
    by_identity = {(row.source_company_id, row.source_record_id): row for row in ledger_rows}

    for row in ledger_rows:
        report.outcomes[row.status] += 1
        report.by_company.setdefault(row.source_company_id, Counter())[row.status] += 1

    if inputs.final:
        report.completeness = await _prove_completeness(
            session_maker,
            inputs,
            report=report,
            ledger_by_identity=by_identity,
            write_client=write_client,
            http_client=http_client,
        )

    return report


async def _prove_completeness(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    report: MigrationReport,
    ledger_by_identity: dict[tuple[int, int], Any],
    write_client: EasyWeekMigrationWriteClient | None,
    http_client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Re-read the live source AND the live targets, and prove the wave landed.

    The earlier version stopped at the ledger: a row saying ``created`` counted
    as an accounted-for booking. That is a statement about our own bookkeeping,
    and it stays true after somebody deletes the booking, cancels it, moves it to
    another day, hands it to another master or reassigns it to another customer.
    A cutover can be reported complete while half of it is no longer there.

    So each accounted booking is now fetched from EasyWeek and proven: the marker
    still names this source identity, the booking is neither cancelled nor
    completed, every write-critical field is present, and the live fingerprint
    equals the one stored when it was written. Anything less makes the row
    unaccounted and the whole reconciliation fail.

    Completeness is judged for **the selected wave only** — a deferred master's
    bookings are not a gap, they are a later wave. An unknown master is still a
    gap, and still fails: that is the difference the selector exists to keep.
    """
    async with session_maker() as session:
        decisions, planned = await build_plan(session, inputs, http_client=http_client)
    report.source_records_fetched = planned.source_records_fetched
    report.wave = planned.wave

    # `skipped` covers the past, the cancelled, the finished — and deferred
    # masters, whose reason code is reported separately below.
    active: list[Decision] = [d for d in decisions if d.outcome != SKIPPED]
    deferred = sum(1 for d in decisions if d.outcome == SKIPPED and d.reason == SKIP_STAFF_DEFERRED)

    unaccounted: list[dict[str, Any]] = []
    reasons: Counter = Counter()
    accounted = 0
    source_changed = 0
    blocked_now = 0
    targets_proven = 0

    def _unaccount(decision: Decision, reason: str) -> None:
        entry = decision.as_safe_dict()
        entry["completeness_reason"] = reason
        reasons[reason] += 1
        unaccounted.append(entry)

    for decision in active:
        if decision.source_record_id is None:
            _unaccount(decision, COMPLETENESS_NO_LEDGER_ROW)
            continue

        if decision.outcome == BLOCKED:
            blocked_now += 1
            if decision.reason == BLOCK_SOURCE_CHANGED:
                source_changed += 1
            _unaccount(decision, decision.reason or COMPLETENESS_LEDGER_NOT_CREATED)
            continue

        row = ledger_by_identity.get((decision.source_company_id, decision.source_record_id))
        if row is None:
            _unaccount(decision, COMPLETENESS_NO_LEDGER_ROW)
            continue
        if row.status != ledger_module.STATUS_CREATED:
            _unaccount(decision, COMPLETENESS_LEDGER_NOT_CREATED)
            continue

        # The ledger says it was created. Now prove it still is.
        if not row.target_booking_uuid:
            _unaccount(decision, TARGET_UUID_MISSING)
            continue
        if not row.target_snapshot_fingerprint:
            # Written before snapshots existed, or by a path that could not take
            # one. Nothing to compare against, and "nothing to compare" is not
            # "unchanged".
            _unaccount(decision, TARGET_SNAPSHOT_MISSING)
            continue
        if write_client is None:
            _unaccount(decision, COMPLETENESS_TARGET_UNPROVEN)
            continue

        marker = ledger_module.migration_marker(
            source_company_id=decision.source_company_id,
            source_record_id=decision.source_record_id,
        )
        proof = await prove_live_target(
            write_client,
            target_booking_uuid=row.target_booking_uuid,
            marker=marker,
            expected_fingerprint=row.target_snapshot_fingerprint,
        )
        if not proof.proven:
            _unaccount(decision, proof.reason)
            continue

        targets_proven += 1
        accounted += 1

    unresolved = report.outcomes.get(ledger_module.STATUS_UNCERTAIN, 0) + report.outcomes.get(
        ledger_module.STATUS_PENDING, 0
    )
    failed = report.outcomes.get(ledger_module.STATUS_FAILED, 0)
    # A source that was never read cannot prove anything. `records_fetched` is
    # the evidence the API actually answered, and it is checked separately from
    # "there were no active bookings", which on its own is not success.
    source_was_read = bool(report.source_records_fetched) and all(
        company_id in report.source_records_fetched for company_id in inputs.manifest.company_ids
    )
    # Proving zero targets is only acceptable when there were none to prove.
    targets_were_checked = write_client is not None

    passed = (
        source_was_read
        and targets_were_checked
        and unresolved == 0
        and failed == 0
        and not unaccounted
        and targets_proven == accounted
    )

    verdict = {
        "passed": passed,
        "source_was_read": source_was_read,
        "targets_were_checked": targets_were_checked,
        "source_active_bookings": len(active),
        "deferred_bookings": deferred,
        "accounted_for": accounted,
        "live_targets_proven": targets_proven,
        "blocked": blocked_now,
        "source_changed": source_changed,
        "uncertain_or_pending": unresolved,
        "failed": failed,
        "unaccounted_reason_codes": dict(sorted(reasons.items())),
        "unaccounted_rows": unaccounted,
    }
    if not passed:
        report.errors.append("final reconciliation did not prove cutover completeness")
    return verdict


async def _prove_uncertain_row_against_target(
    inputs: RunInputs,
    *,
    company_id: int,
    record_id: int,
    stored_source_fingerprint: str,
    target_booking_uuid: str,
    write_client: EasyWeekMigrationWriteClient,
    http_client: httpx.AsyncClient | None = None,
) -> tuple[bool, str, Any]:
    """The shared proof behind ``resolve-created`` and reconcile-with-a-UUID.

    Both commands answer the same question — *is this EasyWeek booking the one
    the migration tried to create for this source row?* — and both used to answer
    it too weakly. Reconcile accepted any 2xx; resolve-created checked the marker
    and the branch but not the staff, service, customer, start time or duration,
    so a booking for the right customer at the wrong time was accepted.

    The proof has two halves, and both must hold:

    1. **The source still describes the attempted booking.** It is re-read and
       re-classified with the same manifest, wave selector, customer directory,
       cutover and price/duration rules, and its fingerprint must equal the one
       recorded before the original POST. Without this the "expected" target
       would be rebuilt from a source that has since moved.
    2. **The live booking matches that expectation exactly** — every
       write-critical field, plus the marker and the active status.

    Returns ``(proven, reason, live_snapshot)``. Never issues a POST.
    """
    if not inputs.manifest.valid:
        return False, GATE_MANIFEST_INVALID, None
    if not inputs.directory.valid or not inputs.directory.by_phone:
        # Without the export there is no way to say which customer this booking
        # was for, and "we could not check the customer" must not pass as "the
        # customer is right".
        return False, RESOLVE_INPUTS_MISSING, None

    branch_identity = verify_branch_identity(inputs.manifest)
    if not branch_identity.proven:
        return False, GATE_BRANCH_IDENTITY_UNPROVEN, None

    reproof, expected_decision = await reclassify_source_for_resolution(
        company_id=company_id,
        record_id=record_id,
        expected_fingerprint=stored_source_fingerprint,
        manifest=inputs.manifest,
        directory=inputs.directory,
        cutover=inputs.cutover,
        http_client=http_client,
    )
    if not reproof.confirmed or expected_decision is None:
        detail = f"{reproof.reason}:{reproof.detail}" if reproof.detail else reproof.reason
        return False, f"{RESOLVE_SOURCE_UNPROVEN}:{detail}", None

    marker = ledger_module.migration_marker(source_company_id=company_id, source_record_id=record_id)
    expected = expected_target_for(expected_decision, booking_uuid=target_booking_uuid, marker=marker)
    proof = await prove_live_target(
        write_client,
        target_booking_uuid=target_booking_uuid,
        marker=marker,
        expected=expected,
    )
    if not proof.proven:
        return False, proof.reason, None
    return True, RESOLVE_CONFIRMED, proof.live


async def run_resolve_created(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    write_client: EasyWeekMigrationWriteClient,
    http_client: httpx.AsyncClient | None = None,
) -> MigrationReport:
    """Resolve ONE unresolved row against an operator-supplied booking UUID.

    The operator finds the booking in the EasyWeek UI — the migration marker
    makes that possible — and names it. The tool does not take their word for it:
    it re-reads the source, rebuilds the booking the migration meant to create,
    fetches the named booking and requires every write-critical field to match.

    The origin run is preserved, so the booking stays in the rollback set of the
    apply that made it. Any mismatch leaves the row exactly as it was — uncertain.
    """
    report = MigrationReport(
        mode=MODE_RESOLVE_CREATED,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )
    company_id = inputs.resolve_company_id
    record_id = inputs.resolve_record_id
    target = inputs.resolve_target_booking_uuid
    if company_id is None or record_id is None or not target:
        report.errors.append("resolve-created requires --resolve-company-id, --resolve-record-id and --target-uuid")
        return report

    async with session_maker() as session:
        row = await ledger_module.get_row(session, source_company_id=company_id, source_record_id=record_id)
        row_snapshot = ledger_module.row_as_safe_dict(row) if row is not None else None
        stored_source_fingerprint = row.source_fingerprint if row is not None else None

    if row is None or row_snapshot is None or stored_source_fingerprint is None:
        report.errors.append(RESOLVE_ROW_MISSING)
        return report
    if row_snapshot["status"] not in (ledger_module.STATUS_UNCERTAIN, ledger_module.STATUS_PENDING):
        report.errors.append(RESOLVE_NOT_UNCERTAIN)
        return report

    proven, reason, live = await _prove_uncertain_row_against_target(
        inputs,
        company_id=company_id,
        record_id=record_id,
        stored_source_fingerprint=stored_source_fingerprint,
        target_booking_uuid=target,
        write_client=write_client,
        http_client=http_client,
    )
    if not proven or live is None:
        # The row stays uncertain. That is the point: a resolution that cannot be
        # proven is not a resolution.
        entry = dict(row_snapshot)
        entry["reconcile_outcome"] = reason
        report.reasons[reason] += 1
        report.uncertain_rows.append(entry)
        report.errors.append(reason)
        return report

    async with session_maker() as session:
        async with session.begin():
            await ledger_module.resolve_uncertain_as_created(
                session,
                run_id=inputs.run_id,
                source_company_id=company_id,
                source_record_id=record_id,
                target_booking_uuid=live.booking_uuid,
                target_snapshot_fingerprint=live.fingerprint,
            )

    entry = dict(row_snapshot)
    entry["reconcile_outcome"] = RESOLVE_CONFIRMED
    entry["target_booking_uuid"] = live.booking_uuid
    report.reasons[RESOLVE_CONFIRMED] += 1
    report.created_rows.append(entry)
    return report


async def run_resolve_absent(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
) -> MigrationReport:
    """Record that an operator checked EasyWeek and the booking is NOT there.

    This is the only path that makes an unresolved row re-claimable, and it is
    the dangerous direction: if the operator is wrong, the next apply creates a
    second appointment for a real person. There is therefore no automatic route
    here and no single boolean — two separate explicit flags are required, and
    the report says plainly what the next apply will do.
    """
    report = MigrationReport(
        mode=MODE_RESOLVE_ABSENT,
        run_id=inputs.run_id,
        cutover=inputs.cutover.as_safe_dict(),
        manifest=inputs.manifest.as_safe_dict(),
        customer_directory=inputs.directory.as_safe_dict(),
    )
    company_id = inputs.resolve_company_id
    record_id = inputs.resolve_record_id
    if company_id is None or record_id is None:
        report.errors.append("resolve-absent requires --resolve-company-id and --resolve-record-id")
        return report
    if not (inputs.resolve_absent_acknowledged and inputs.resolve_absent_confirmed):
        report.errors.append(RESOLVE_ABSENT_UNCONFIRMED)
        return report

    async with session_maker() as session:
        row = await ledger_module.get_row(session, source_company_id=company_id, source_record_id=record_id)
        row_snapshot = ledger_module.row_as_safe_dict(row) if row is not None else None

    if row is None or row_snapshot is None:
        report.errors.append(RESOLVE_ROW_MISSING)
        return report
    if row_snapshot["status"] not in (ledger_module.STATUS_UNCERTAIN, ledger_module.STATUS_PENDING):
        report.errors.append(RESOLVE_NOT_UNCERTAIN)
        return report

    async with session_maker() as session:
        async with session.begin():
            await ledger_module.resolve_uncertain_as_absent(
                session,
                run_id=inputs.run_id,
                source_company_id=company_id,
                source_record_id=record_id,
                reason_code=RESOLVE_ABSENT_CONFIRMED,
            )

    entry = dict(row_snapshot)
    entry["reconcile_outcome"] = RESOLVE_ABSENT_CONFIRMED
    report.reasons[RESOLVE_ABSENT_CONFIRMED] += 1
    report.failed_rows.append(entry)
    report.errors.append("row is now re-claimable; the next apply WILL create this booking")
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

    * **scoped to one run.** Only ledger rows whose ORIGIN ``run_id`` matches, and
      only those with status ``created``. A booking created by a different run, or
      by a person, is not in the set and cannot be.
    * **evidence-based.** Each target is fetched and projected, and its
      fingerprint must equal the one stored when it was written. Time, master,
      service, customer, branch, duration, marker and active status all take part
      — the earlier version compared only the marker and the cancelled/completed
      flags, and both of those survive a booking being moved to another day or
      handed to another customer.
    * **read-only by default.** Without ``--apply --confirm-rollback`` this
      reports what it *would* cancel and returns.
    * **gated like an apply.** Cancelling emits EasyWeek events too, so the write
      path re-checks the same notification fence: a rollback run while
      notifications are back on would tell every one of those customers.
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

    if inputs.rollback_confirmed:
        # A cancellation is a customer-visible write. It passes the same
        # notification fence as a creation — including the operator attestation
        # that EasyWeek's own channels are still off. The canary proof is not
        # required: a rollback creates nothing, so there is no request shape to
        # have proven.
        branch_identity = verify_branch_identity(inputs.manifest)
        gate = evaluate_apply_gate(
            # `rollback_confirmed` is only ever set when `--apply` was given
            # alongside `--confirm-rollback`; the two are one decision here.
            apply_requested=True,
            native_notifications_confirmed=inputs.native_notifications_confirmed,
            cutover_supplied=inputs.cutover_supplied,
            # A rollback is not a plan, so the dry-run digest check does not
            # apply; the run it targets is named explicitly instead.
            verified_dry_run_id=inputs.verified_dry_run_id or "rollback",
            computed_plan_digest=inputs.verified_dry_run_id or "rollback",
            manifest=inputs.manifest,
            directory=inputs.directory,
            canary_notification_observed=inputs.canary_notification_observed,
            branch_identity=branch_identity,
            canary_verdict=None,
            require_canary_proof=False,
        )
        report.gate = gate.as_safe_dict()
        if not gate.passed:
            report.errors.append(ROLLBACK_GATE_REFUSED)
            report.reasons[ROLLBACK_GATE_REFUSED] += 1
            return report

    async with session_maker() as session:
        rows = await ledger_module.rows_for_run(session, run_id=target_run, statuses=(ledger_module.STATUS_CREATED,))
        candidates = [
            (
                ledger_module.row_as_safe_dict(row),
                row.source_company_id,
                row.source_record_id,
                row.target_booking_uuid,
                row.target_snapshot_fingerprint,
            )
            for row in rows
        ]

    for safe_row, company_id, record_id, target, stored_fingerprint in candidates:
        entry = dict(safe_row)

        def _refuse(reason: str) -> None:
            entry["rollback_outcome"] = reason
            report.reasons[reason] += 1
            report.blocked_rows.append(entry)

        if not target:
            _refuse(ROLLBACK_TARGET_MISSING)
            continue
        if not stored_fingerprint:
            # Written before snapshots existed, or by a path that could not take
            # one. There is nothing to compare against, and "nothing to compare"
            # must never read as "unchanged".
            _refuse(ROLLBACK_NO_SNAPSHOT)
            continue

        try:
            booking = await write_client.get_booking(target)
        except EasyWeekNotFoundError:
            # Already gone. Nothing to cancel and nothing to complain about.
            _refuse(ROLLBACK_TARGET_MISSING)
            continue
        except EasyWeekError:
            _refuse(ROLLBACK_TARGET_UNREADABLE)
            continue

        marker = ledger_module.migration_marker(source_company_id=company_id, source_record_id=record_id)
        try:
            live = project_target(booking, expected_marker=marker)
        except TargetSnapshotError:
            # A missing or unreadable field, a rewritten marker, an already
            # cancelled or completed booking. All of them mean "we cannot prove
            # this is untouched", which is treated exactly as "it was touched".
            _refuse(ROLLBACK_TARGET_MODIFIED)
            continue

        if live.fingerprint != stored_fingerprint:
            _refuse(ROLLBACK_TARGET_MODIFIED)
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
                    run_id=inputs.run_id,
                    source_company_id=company_id,
                    source_record_id=record_id,
                )
        entry["rollback_outcome"] = ledger_module.STATUS_ROLLED_BACK
        report.reasons[ledger_module.STATUS_ROLLED_BACK] += 1
        report.created_rows.append(entry)

    return report


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "DEFAULT_HORIZON_DAYS",
    "MODE_APPLY",
    "MODE_CANARY",
    "MODE_DRY_RUN",
    "MODE_INVENTORY",
    "MODE_RECONCILE",
    "MODE_RESOLVE_ABSENT",
    "MODE_RESOLVE_CREATED",
    "MODE_ROLLBACK_APPLY",
    "MODE_ROLLBACK_DRY_RUN",
    "RunInputs",
    "build_plan",
    "new_run_id",
    "run_apply",
    "run_canary",
    "run_inventory_or_dry_run",
    "run_reconcile",
    "run_resolve_absent",
    "run_resolve_created",
    "run_rollback",
    "utcnow",
    "ALREADY_MIGRATED",
    "BLOCKED",
    "SKIPPED",
]
