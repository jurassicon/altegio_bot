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
from collections.abc import Collection
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Final

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import EasyWeekError, EasyWeekNotFoundError
from altegio_bot.easyweek_migration import baseline_store
from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.altegio_source import build_window, fetch_company_records
from altegio_bot.easyweek_migration.baseline_store import load_baselines
from altegio_bot.easyweek_migration.bindings import (
    MUTATION_CART_TWO,
    MUTATION_SINGLE,
    SUPPORTED_MUTATION_KINDS,
)
from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult, verify_branch_identity
from altegio_bot.easyweek_migration.canary import (
    CANARY_IDENTITY_REQUIRED,
    CANARY_NOT_IN_PLAN,
    CANARY_NOT_READY,
    CANARY_POST_FAILED,
    CANARY_POST_UNCERTAIN,
    CANARY_READBACK_FAILED,
    CANARY_REPROOF_FAILED,
    SCOPE_CONTRACTS_UNKNOWN,
    CanaryBinding,
    CanaryVerdict,
    RecoveryAdmission,
    ScopeVerdict,
    build_binding,
    find_licensing_proof,
    find_proven_scope,
    find_recoverable_canary_attempt,
    promote_proof_to_verified,
    record_proof,
)
from altegio_bot.easyweek_migration.classify import (
    ALREADY_MIGRATED,
    BLOCK_CONTRACT_UNSUPPORTED,
    BLOCK_SOURCE_CHANGED,
    BLOCKED,
    READY,
    SKIP_EMPTY_SERVICES,
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
    MIGRATABLE_COMPANY_IDS,
    STAFF_DEFERRED,
    STAFF_SELECTED,
    STAFF_UNKNOWN,
    MigrationManifest,
    ServiceMapping,
)
from altegio_bot.easyweek_migration.previous_wave import (
    PreviousWaveContext,
    prove_previous_wave_context,
)
from altegio_bot.easyweek_migration.proof import (
    GHOST_TARGET_STILL_ACTIVE,
    TARGET_SNAPSHOT_MISSING,
    TARGET_UUID_MISSING,
    expected_target_for,
    prove_live_target,
    prove_staff_assignment,
    prove_target_inactive_or_absent,
)
from altegio_bot.easyweek_migration.report import (
    CREATED,
    FAILED,
    UNCERTAIN,
    MigrationReport,
    plan_digest,
)
from altegio_bot.easyweek_migration.reproof import (
    LIFECYCLE_ACTIVE_UNCHANGED,
    LIFECYCLE_UNPROVABLE,
    reclassify_source_for_resolution,
    reclassify_source_lifecycle,
    reprove_source_booking,
)
from altegio_bot.easyweek_migration.service_catalog import (
    CATALOG_UNREADABLE,
    SERVICE_BASELINE_CONFLICTS_WITH_PLAN,
    SERVICE_BASELINE_MISSING,
    SERVICE_BASELINE_VERSION,
    SERVICE_PROOF_METHOD,
    CatalogSnapshot,
    ServiceBaseline,
    ServiceEvidenceError,
    expectation_from_manifest,
    prove_ordered_service,
    read_full_catalog,
    read_ordered_service,
    verify_baseline,
)
from altegio_bot.easyweek_migration.target_snapshot import (
    TargetSnapshotError,
    project_target,
    prove_canceled_target,
)
from altegio_bot.easyweek_migration.write_client import (
    CancelOutcome,
    EasyWeekCancelNotSent,
    EasyWeekMigrationWriteClient,
    EasyWeekUncertainMutation,
    build_booking_request,
    build_cart_booking_request,
)

logger = logging.getLogger("easyweek_migration.runner")

# The mutation contracts this build can actually write END TO END. A decision
# naming anything else is refused before the ledger claim: a contract with no
# complete path is a row for a person, not an exception thrown half-way through
# a bulk apply.
#
# `cart_two` is deliberately NOT written yet. Its classification, its request
# builder, its write client and its canary isolation are all in place and
# tested — what is missing is the readback: `TargetSnapshot` projects ONE
# service, and proving a two-line booking needs it to carry both signatures,
# which changes the fingerprint the ledger already stores for every migrated
# single booking.
#
# Creating a real appointment we could not then prove is worse than not creating
# it, so the gate stays shut until the readback proves two lines.
#
# The set itself now lives in `bindings`, because the CLASSIFIER enforces it:
# refusing the contract while the plan is built is what makes a dry-run and an
# apply say the same thing about the same booking.

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
# The cancel was attempted and its outcome could not be proven: a timeout, a
# transport failure, a 5xx, or a 2xx the booking did not read back as cancelled.
# The ledger stays `created` — a live booking recorded as rolled back would
# disappear from every later reconciliation.
ROLLBACK_UNCERTAIN: Final = "rollback_uncertain"
# The cancel was refused deterministically: nothing changed, and the ledger is
# untouched.
ROLLBACK_REFUSED: Final = "rollback_refused"
ROLLBACK_NO_SNAPSHOT: Final = "rollback_target_snapshot_missing"
# A durable attempt of OURS exists and the booking now reads as cancelled: the
# PUT did land, we simply never saw it. The row is completed WITHOUT a second
# mutation — this is the recovery the attempt marker exists for.
ROLLBACK_RECOVERED: Final = "rollback_recovered_from_attempt"
# The same situation seen by a dry-run, which proves it and writes nothing.
ROLLBACK_RECOVERY_AVAILABLE: Final = "rollback_recovery_available"
# Our attempt exists and the booking is STILL live. A second PUT is exactly the
# unknown-outcome repeat this design bans, so nothing is sent and an operator
# decides. Deliberately distinct from `rollback_uncertain`, which describes the
# attempt that has just happened rather than one found on a later run.
ROLLBACK_ATTEMPT_UNRESOLVED: Final = "rollback_attempt_unresolved"
# The mutation right went to another run, or the row moved between the candidate
# list and the claim. Nothing was sent, and nothing about the row was changed.
ROLLBACK_CLAIM_LOST: Final = "rollback_claim_not_acquired"
# Proven that the cancel never left: the read immediately before it failed. The
# row keeps no marker, because there is no unknown mutation to keep one for.
ROLLBACK_NOT_SENT: Final = "rollback_cancel_not_sent"
# The booking was already cancelled when this run looked, moments before its own
# PUT. Somebody else did it — another process, another operator, the UI — so it
# is not recorded as this run's rollback.
ROLLBACK_CANCELED_ELSEWHERE: Final = "rollback_target_canceled_elsewhere"
# The booking names no master, so rollback proves one the same way every other
# path does. An unproven master is an unproven target, and an unproven target is
# never cancelled.
ROLLBACK_STAFF_UNPROVEN: Final = "rollback_staff_assignment_unproven"
# Same rule for the service: a target whose service cannot be proven against its
# stored baseline is a target we cannot identify, and an unidentified booking is
# never cancelled.
ROLLBACK_SERVICE_UNPROVEN: Final = "rollback_service_evidence_unproven"
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
# Ledger-side refusals of the narrow canary recovery path.
RECOVERY_RUN_MISMATCH: Final = "canary_recovery_run_id_mismatch"
RECOVERY_ATTEMPTS_UNEXPECTED: Final = "canary_recovery_unexpected_attempt_count"

# Final reconciliation, per row.
COMPLETENESS_TARGET_UNPROVEN: Final = "target_not_proven_in_easyweek"
COMPLETENESS_NO_LEDGER_ROW: Final = "no_ledger_row_for_active_booking"
COMPLETENESS_LEDGER_NOT_CREATED: Final = "ledger_row_not_created"
# The source of an already-migrated row could not be established either way.
COMPLETENESS_SOURCE_UNPROVABLE: Final = "migrated_source_lifecycle_unprovable"


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


# Stable, PII-free reasons a row could not be prepared for writing.
CUSTOMER_UNADDRESSABLE: Final = "customer_transport_fields_missing"
# A booking was created and could not be proven. The run stops there.
APPLY_HALTED_UNPROVEN: Final = "run halted: a created booking could not be proven; no further bookings were created"


class ServiceEvidence:
    """Durable expectations, plus a catalogue observation taken fresh each time.

    The correction this class exists for. Its predecessor cached both halves for
    a whole run: `load()` returned early once a location had been read, and every
    pinned expectation was reused. So a bulk apply proved uniqueness against the
    catalogue as it stood before its FIRST booking, and a service added
    half-way through the run — one that made an earlier service ambiguous — was
    invisible to every remaining POST.

    The two halves are now separated on purpose:

    ``baselines``
        the stored expectations, loaded once because they are immutable by
        construction — nothing but an explicit operator act changes them;
    ``observe()``
        a catalogue read, taken **again at every operation boundary** and never
        remembered. There is no positive result to go stale, and no TTL to tune.

    Re-reading is not a lock and is not claimed to be one: a service can still be
    added between the read and the POST that follows it. What it removes is the
    specific defect — a snapshot from the top of a bulk being treated as evidence
    for its two-hundredth booking.
    """

    def __init__(self, baselines: dict[tuple[str, str], ServiceBaseline] | None = None) -> None:
        self.baselines: dict[tuple[str, str], ServiceBaseline] = dict(baselines or {})
        # Only failures are remembered, and only for the report. A refusal that
        # is re-checked and passes simply produces no new entry; a positive
        # result is never cached, because that is the bug.
        self._failures: dict[tuple[str, str], str] = {}
        self._observations = 0

    async def observe(
        self, write_client: EasyWeekMigrationWriteClient, *, location_uuid: str
    ) -> CatalogSnapshot | None:
        """Read every page of one location's catalogue, now. Never cached.

        Uniqueness is judged over the whole returned catalogue: no filtering by
        the wave's services, by category or by master, because the look-alike
        that would make an attribute match ambiguous is exactly the entry such a
        filter would hide.

        Goes through the shared client, so the existing rate limiter and its
        bounded, safe GET retries still apply. Nothing here fans out.
        """
        try:
            snapshot = await read_full_catalog(write_client, location_uuid=location_uuid)
        except ServiceEvidenceError as exc:
            return self._refuse(location_uuid, exc.reason)
        self._observations += 1
        return snapshot

    def _refuse(self, location_uuid: str, reason: str) -> None:
        self._failures[(location_uuid, "")] = reason
        return None

    async def prove(
        self,
        write_client: EasyWeekMigrationWriteClient,
        decision: Decision,
    ) -> ServiceBaseline | None:
        """Re-read the catalogue and check it still satisfies the stored baseline.

        The only way any path gets a usable expectation. A missing stored
        baseline is a refusal, not an invitation to derive one: deriving it here
        would mean the check compares the catalogue with itself, which is the
        circularity the baseline table exists to break. Establishing one happens
        in exactly one place — beside the ledger claim, before the first POST for
        that service — and never as a side effect of proving.
        """
        location_uuid = decision.easyweek_location_uuid
        service_uuid = decision.easyweek_service_uuid
        if location_uuid is None or service_uuid is None:
            return None
        key = (location_uuid, service_uuid)

        baseline = self.baselines.get(key)
        if baseline is None:
            self._failures[key] = SERVICE_BASELINE_MISSING
            return None

        catalog = await self.observe(write_client, location_uuid=location_uuid)
        if catalog is None:
            return None
        try:
            verify_baseline(catalog, baseline)
        except ServiceEvidenceError as exc:
            self._failures[key] = exc.reason
            return None
        self._failures.pop(key, None)
        return baseline

    def note_failure(self, decision: Decision, reason: str) -> None:
        location_uuid = decision.easyweek_location_uuid or ""
        service_uuid = decision.easyweek_service_uuid or ""
        self._failures[(location_uuid, service_uuid)] = reason

    def failure_for(self, decision: Decision) -> str:
        """The stable reason a service could not be proven."""
        location_uuid = decision.easyweek_location_uuid or ""
        service_uuid = decision.easyweek_service_uuid or ""
        return self._failures.get((location_uuid, service_uuid)) or self._failures.get(
            (location_uuid, ""), CATALOG_UNREADABLE
        )

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "method": SERVICE_PROOF_METHOD,
            "baseline_version": SERVICE_BASELINE_VERSION,
            "stored_baselines": baseline_store.as_safe_dict(self.baselines),
            "catalog_observations": self._observations,
            "refusals": sorted(set(self._failures.values())),
        }


async def load_service_evidence(session_maker: async_sessionmaker[AsyncSession], inputs: RunInputs) -> ServiceEvidence:
    """Load the stored expectations for every branch this manifest names.

    Reads PostgreSQL only. The catalogue itself is deliberately NOT read here:
    an observation taken at the start of a command must never become the evidence
    a later operation in that command leans on.
    """
    locations = tuple(
        branch.easyweek_location_uuid
        for company_id in inputs.manifest.company_ids
        if (branch := inputs.manifest.branch(company_id)) is not None
    )
    async with session_maker() as session:
        return ServiceEvidence(await load_baselines(session, location_uuids=locations))


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

    # `inventory` runs against a manifest that is still being written, so asking
    # it to prove anything cumulative would only produce noise. `dry-run` is the
    # review artefact, and it is where an operator should learn that their wave-2
    # manifest dropped wave 1's mappings — long before the canary refuses.
    if inputs.mode != MODE_INVENTORY:
        context = await prove_previous_wave_context(
            session,
            manifest=inputs.manifest,
            directory=inputs.directory,
            cutover=inputs.cutover,
            decisions=decisions,
            http_client=http_client,
        )
        report.previous_wave_context = context.as_safe_dict()

    return report


def _binding_for(
    inputs: RunInputs,
    branch_identity: BranchIdentityResult,
    *,
    contract_kind: str,
) -> CanaryBinding:
    """The durable identity of the wave these inputs describe, for ONE contract.

    One place, so a command that continues a wave cannot compute the wave's name
    slightly differently from the command that created it.

    ``contract_kind`` is required rather than defaulted. A default would be a
    silent answer to the one question this function exists to keep honest: a
    cart canary stored under `single` would license single bulk applies, and a
    single canary read back as cart would license cart ones. Every caller has to
    say which contract it is talking about.
    """
    return build_binding(
        manifest_digest=inputs.manifest.digest,
        staff_scope_digest=inputs.manifest.staff_scope_digest,
        cutover_at=inputs.cutover.at,
        horizon_days=inputs.horizon_days,
        branch_result=branch_identity,
        contract_kind=contract_kind,
    )


async def _require_proven_scope(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    contract_kinds: Collection[str] = SUPPORTED_MUTATION_KINDS,
) -> tuple[ScopeVerdict, tuple[CanaryBinding, ...]]:
    """Prove that these arguments describe the wave that was actually migrated.

    Every command that *continues* a wave — the reconciliations and the
    resolutions — has to pass this before it may claim anything. Without it the
    scope of the proof was whatever the operator typed, and two edits silently
    narrowed it: omitting ``--cutover-at`` (the code used "now", so earlier
    bookings became out-of-window and their targets were never fetched) and
    moving a migrated master into ``deferred_altegio_staff_ids``.

    Reads only. Never touches EasyWeek and never writes.
    """
    branch_identity = verify_branch_identity(inputs.manifest)
    if not branch_identity.proven:
        return ScopeVerdict(proven=False, reason=GATE_BRANCH_IDENTITY_UNPROVEN), ()

    # One binding per contract, and EVERY one of them has to be proven.
    #
    # The ledger does not record which contract created a row, so a command that
    # continues a wave cannot narrow the question to the contracts it will
    # actually meet — it has to answer for the full set this build can write.
    # That is the conservative direction: it can refuse a wave whose rows all
    # came from one contract, and it can never pass a wave containing a contract
    # no canary ever proved. When the ledger learns to name its contract this
    # narrows to the kinds actually present in scope.
    bindings = tuple(_binding_for(inputs, branch_identity, contract_kind=kind) for kind in sorted(contract_kinds))
    if not bindings:
        # Nothing to prove means nothing was proven.
        return ScopeVerdict(proven=False, reason=SCOPE_CONTRACTS_UNKNOWN), ()

    proven: ScopeVerdict | None = None
    async with session_maker() as session:
        for binding in bindings:
            verdict = replace(
                await find_proven_scope(session, binding=binding),
                contract_kind=binding.contract_kind,
            )
            if not verdict.proven:
                return verdict, bindings
            proven = verdict
    assert proven is not None  # `bindings` is non-empty and every one passed.
    return proven, bindings


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

    # Waves after the first inherit live `created` rows from the waves before
    # them, and this manifest still has to prove those rows. Read-only, and
    # placed here — above the gate, above every write path — so that a manifest
    # which dropped an earlier wave's mapping is refused while EasyWeek is still
    # untouched, rather than after a wave has been migrated that can never be
    # reconciled. Reads Altegio and PostgreSQL; issues no EasyWeek request.
    context: PreviousWaveContext
    async with session_maker() as session:
        context = await prove_previous_wave_context(
            session,
            manifest=inputs.manifest,
            directory=inputs.directory,
            cutover=inputs.cutover,
            decisions=decisions,
            http_client=http_client,
        )
    report.previous_wave_context = context.as_safe_dict()

    canary_verdict: CanaryVerdict | None = None
    if require_canary_proof:
        # A proof licenses ONE contract. The apply therefore asks once per
        # contract it is actually going to execute — the kinds carried by the
        # decisions it will write, not the kinds it could theoretically meet —
        # and the first missing proof shuts the gate.
        #
        # This is what stops a mixed plan from riding on one canary: a wave of
        # single bookings plus one two-service booking needs BOTH proofs, and a
        # verified single canary answers only for the single half.
        # With nothing ready to write — a rerun whose rows are all already
        # migrated — the question falls back to the wave's own contract. That is
        # not a weakening: a plan with no ready row creates nothing whatever the
        # verdict says, and asking about `single` keeps the report carrying a
        # real answer instead of a licence computed from an empty set.
        pending_kinds = sorted({decision.mutation_kind for decision in decisions if decision.outcome == READY}) or [
            MUTATION_SINGLE
        ]
        async with session_maker() as session:
            for kind in pending_kinds:
                binding = _binding_for(inputs, branch_identity, contract_kind=kind)
                verdict = replace(
                    await find_licensing_proof(session, binding=binding),
                    contract_kind=kind,
                )
                canary_verdict = verdict
                if not verdict.licensed:
                    break

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
        previous_wave_context=context,
        require_previous_wave_context=True,
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

    # The stored expectations. Each booking re-reads the catalogue for itself
    # immediately before its own POST — see `_service_baseline_for`.
    evidence = await load_service_evidence(session_maker, inputs)
    report.service_evidence = evidence.as_safe_dict()

    for decision in (d for d in decisions if d.outcome == READY):
        outcome, reason = await _apply_one(
            session_maker,
            inputs,
            decision,
            write_client=write_client,
            evidence=evidence,
            http_client=http_client,
            verify_readback=True,
        )
        if outcome in (CREATED, UNCERTAIN, FAILED):
            report.mutations_attempted += 1
        report.note(decision, outcome=outcome, reason=reason)
        if outcome == UNCERTAIN:
            # Stop. We do not know whether the last write landed, so we cannot
            # reason about the next one either. `reconcile` first.
            report.errors.append("run halted after an uncertain mutation; run reconcile before applying again")
            break
        report.service_evidence = evidence.as_safe_dict()
        if outcome == CREATED and reason is not None:
            # The POST landed and the proof did not. The booking exists, its UUID
            # and the failing reason are recorded, and nothing else is created:
            # whatever made this one unprovable would make the next one
            # unprovable too, and a run that keeps writing while it cannot verify
            # is a run producing bookings nobody has checked. Never a second POST
            # and never an automatic cancellation — a human looks at it.
            report.errors.append(APPLY_HALTED_UNPROVEN)
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

    # The binding is built from the contract the canary is ABOUT TO EXECUTE, not
    # from a default. A cart canary stored under `single` would be evidence
    # about a two-service POST filed under the name of the one-service POST, and
    # every later bulk apply of single bookings would read it as its licence.
    binding = _binding_for(inputs, branch_identity, contract_kind=chosen.mutation_kind)
    report.canary_binding = binding.as_safe_dict()

    evidence = await load_service_evidence(session_maker, inputs)
    report.service_evidence = evidence.as_safe_dict()

    outcome, reason = await _apply_one(
        session_maker,
        inputs,
        chosen,
        write_client=write_client,
        evidence=evidence,
        http_client=http_client,
        verify_readback=True,
        binding_for_proof=binding,
    )
    if outcome in (CREATED, UNCERTAIN, FAILED):
        report.mutations_attempted += 1
    report.note(chosen, outcome=outcome, reason=reason)
    report.service_evidence = evidence.as_safe_dict()

    # A canary is only green when the booking was created AND read back clean.
    # `CREATED` with a reason means the write landed but the verification did
    # not — the booking exists and is rollback-able, and it licenses nothing.
    if outcome != CREATED or reason is not None:
        report.errors.append(reason or CANARY_POST_FAILED)
    return report


async def _service_baseline_for(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    decision: Decision,
    *,
    evidence: ServiceEvidence,
    write_client: EasyWeekMigrationWriteClient,
    wave_identity: str | None = None,
) -> ServiceBaseline | None:
    """The expectation this booking must satisfy, proven against a fresh catalogue.

    Establishes it the first time a service is migrated and verifies it every
    time after. The order matters and is the whole point:

    1. read the catalogue **now**;
    2. if this service has no stored expectation, derive one — which requires the
       manifest's own price and duration to still agree with the catalogue — and
       write it down before anything is created;
    3. verify what is actually stored against that same fresh read.

    Step 3 verifies the STORED row, not the candidate from step 2, because
    another run may have established it first and the row already there wins. A
    service that has drifted since it was reviewed fails here — before the claim,
    so a refusal leaves no `pending` row behind.
    """
    location_uuid = decision.easyweek_location_uuid
    service_uuid = decision.easyweek_service_uuid
    if location_uuid is None or service_uuid is None:
        return None

    key = (location_uuid, service_uuid)

    # 1. The REVIEWED expectation, from the manifest the plan digest covers.
    #    Built first and from the file alone — never from the catalogue — so a
    #    service edited after the review cannot supply its own expectation.
    mapping = _service_mapping_for(inputs.manifest, decision)
    if mapping is None:
        evidence.note_failure(decision, SERVICE_BASELINE_MISSING)
        return None
    try:
        expected = expectation_from_manifest(location_uuid, easyweek_service_uuid=service_uuid, mapping=mapping)
    except ServiceEvidenceError as exc:
        evidence.note_failure(decision, exc.reason)
        return None

    # 2. The STORED expectation, if this service has been migrated before. It and
    #    the reviewed one must agree. Neither is allowed to win automatically:
    #    silently preferring the manifest would be a re-baseline by another name,
    #    and silently preferring the row would let a reviewed change be ignored.
    stored = evidence.baselines.get(key)
    if stored is not None and stored.digest != expected.digest:
        evidence.note_failure(decision, SERVICE_BASELINE_CONFLICTS_WITH_PLAN)
        return None

    if stored is None:
        # 3. First booking for this service. The catalogue is read to CONFIRM the
        #    reviewed expectation still holds — it supplies no values — and the
        #    expectation is written down before anything is created.
        catalog = await evidence.observe(write_client, location_uuid=location_uuid)
        if catalog is None:
            return None
        try:
            verify_baseline(catalog, expected)
        except ServiceEvidenceError as exc:
            evidence.note_failure(decision, exc.reason)
            return None
        async with session_maker() as session:
            async with session.begin():
                written, _outcome = await baseline_store.establish(
                    session, expected, run_id=inputs.run_id, wave_identity=wave_identity
                )
        # A concurrent run may have won the insert with a different expectation.
        if written.digest != expected.digest:
            evidence.note_failure(decision, SERVICE_BASELINE_CONFLICTS_WITH_PLAN)
            return None
        evidence.baselines[key] = written

    # 4. Always a fresh catalogue read for the verification itself.
    return await evidence.prove(write_client, decision)


def _service_mapping_for(manifest: MigrationManifest, decision: Decision) -> ServiceMapping | None:
    """The manifest entry behind a decision's EasyWeek service uuid."""
    branch = manifest.branch(decision.source_company_id)
    if branch is None:
        return None
    return next(
        (entry for entry in branch.services.values() if entry.easyweek_service_uuid == decision.easyweek_service_uuid),
        None,
    )


async def _apply_one(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    decision: Decision,
    *,
    write_client: EasyWeekMigrationWriteClient,
    evidence: ServiceEvidence,
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
    assert decision.easyweek_customer_uuid is not None

    # Which contract writes this booking. The cart endpoint has its own proven
    # body and its own readback shape (plan §30.12), and the two must not be
    # confused: reaching for a single service uuid on a two-service booking
    # would either write half of it or raise mid-apply.
    #
    # An unknown kind refuses HERE, before the ledger is claimed and before any
    # request leaves — a contract this build cannot write is a row for a person,
    # never a crash inside a bulk run.
    if decision.mutation_kind not in SUPPORTED_MUTATION_KINDS:
        return BLOCKED, BLOCK_CONTRACT_UNSUPPORTED
    if decision.mutation_kind == MUTATION_SINGLE:
        assert decision.easyweek_service_uuid is not None

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

    # -- everything that can still refuse, BEFORE the ledger claim ---------
    # A refusal after the claim leaves a `pending` row, and `pending` means "a
    # POST may have been sent". None of these checks sends anything, so all of
    # them belong on this side of the line.

    # The service. The catalogue is read AGAIN here, immediately before this
    # booking — not once at the top of the run. A bulk apply walks its plan for
    # many minutes, and a look-alike service added in those minutes makes an
    # earlier service ambiguous; a snapshot taken before the first POST cannot
    # see that and would license every remaining one.
    expectation = await _service_baseline_for(
        session_maker,
        inputs,
        decision,
        evidence=evidence,
        write_client=write_client,
        wave_identity=getattr(binding_for_proof, "wave_identity", None),
    )
    if expectation is None:
        return BLOCKED, evidence.failure_for(decision)

    # The customer's transport fields, taken from the EasyWeek card we matched.
    # `POST /bookings` rejects `customer_uuid` and requires a phone and a given
    # name, so these have to travel — but only ever as EasyWeek already spells
    # them. Missing means blocked: writing Altegio's spelling over the card that
    # holds the imported visit history is not a fallback, it is damage.
    card = inputs.directory.transport_fields(decision.easyweek_customer_uuid)
    if card is None:
        return BLOCKED, CUSTOMER_UNADDRESSABLE

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

    # The endpoint follows the contract, and only the contract. `single` is the
    # plain `POST /bookings` this migration has always used; `cart_two` is the
    # `POST /bookings/cart` a real canary proved for exactly two services on one
    # master (plan §30.12). Each has its own body builder, and neither builder
    # accepts the other's shape.
    if decision.mutation_kind == MUTATION_CART_TWO:
        body = build_cart_booking_request(
            location_uuid=decision.easyweek_location_uuid,
            customer_phone=card.phone,
            customer_first_name=card.first_name or "",
            datetime_start_utc_iso=starts_at_iso,
            comment=marker,
            # The source's own order, unchanged from the plan the operator
            # reviewed and from the fingerprint that covers it.
            services=[(item.easyweek_service_uuid, item.staffer_uuid) for item in decision.bindings],
        )
    else:
        body = build_booking_request(
            location_uuid=decision.easyweek_location_uuid,
            staffer_uuid=decision.easyweek_staff_uuid,
            service_uuid=decision.easyweek_service_uuid,
            customer_phone=card.phone,
            customer_first_name=card.first_name or "",
            reserved_on_utc_iso=starts_at_iso,
            comment=marker,
        )

    try:
        if decision.mutation_kind == MUTATION_CART_TWO:
            created = await write_client.create_cart_booking(body)
        else:
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

    wanted = expected_target_for(decision, booking_uuid=created.booking_uuid, marker=marker, expectation=expectation)

    readback_failure: str | None = None
    stored_snapshot = wanted
    if verify_readback:
        # The canary reads the booking back through the SAME proof every other
        # path uses: the projection, the catalogue service check and the
        # independent master query. A 2xx alone says the request was accepted,
        # not that it landed where we meant it to — and against this API a 2xx
        # says nothing at all about which master got the appointment.
        # The catalogue is read AGAIN for the readback. The pre-POST proof is not
        # eternal: a look-alike published between the POST and this moment makes
        # the service ambiguous, and reusing the earlier snapshot would call the
        # booking proven on evidence that has since stopped being true.
        readback_baseline = await evidence.prove(write_client, decision)
        proof = await prove_live_target(
            write_client,
            target_booking_uuid=created.booking_uuid,
            marker=marker,
            expected=wanted,
            service_baseline=readback_baseline,
        )
        if not proof.proven:
            readback_failure = f"{CANARY_READBACK_FAILED}:{proof.reason}"
        else:
            assert proof.live is not None
            stored_snapshot = proof.live

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

    # Which wave is this? Asked FIRST, before the live source or any target is
    # read, so a command describing a different wave cannot reach a verdict —
    # let alone a passing one — about this wave's bookings.
    scope, _bindings = await _require_proven_scope(session_maker, inputs)
    report.scope = scope.as_safe_dict()
    if not scope.proven:
        report.errors.append(scope.reason)
        report.reasons[scope.reason] += 1
        if inputs.final:
            report.completeness = {
                "passed": False,
                "scope_proven": False,
                "scope_reason": scope.reason,
                "wave_identity": scope.wave_identity,
            }
        return report

    # The stored expectations, from PostgreSQL. The catalogue itself is read
    # again by each proof below — resolving an uncertain row, and every target
    # the final reconciliation checks — so no observation is reused across them.
    evidence = await load_service_evidence(session_maker, inputs)
    report.service_evidence = evidence.as_safe_dict()

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
            evidence=evidence,
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

    # Both migrating branches, whatever this manifest names. A branch left out of
    # the file must not be a way to narrow the reconciliation: its live targets
    # are exactly the ones nobody would otherwise look at, and a PASS earned by
    # not asking is the failure mode this whole command exists to prevent.
    async with session_maker() as session:
        ledger_rows = await ledger_module.all_rows(session, company_ids=tuple(sorted(MIGRATABLE_COMPANY_IDS)))
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
            evidence=evidence,
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
    evidence: ServiceEvidence,
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
    # Breaks, excluded by owner decision. Reported next to `deferred` because
    # both answer the same operator question — "what did this wave not migrate,
    # and is that a problem?" — and for both the answer is "no".
    empty_services = sum(1 for d in decisions if d.outcome == SKIPPED and d.reason == SKIP_EMPTY_SERVICES)

    unaccounted: list[dict[str, Any]] = []
    reasons: Counter = Counter()
    accounted = 0
    source_changed = 0
    blocked_now = 0
    targets_proven = 0
    # Every ledger identity the active loop reached a verdict on, so the
    # ledger-side sweep below knows which rows it still has to account for.
    judged_identities: set[tuple[int, int]] = set()
    targets_checked = 0
    terminal_targets = 0
    active_ghosts = 0
    # Correct, still-live rows of an earlier confirmed wave, re-proved here
    # rather than assumed.
    earlier_wave_targets = 0

    def _unaccount(decision: Decision, reason: str) -> None:
        entry = decision.as_safe_dict()
        entry["completeness_reason"] = reason
        reasons[reason] += 1
        unaccounted.append(entry)

    for decision in active:
        if decision.source_record_id is None:
            _unaccount(decision, COMPLETENESS_NO_LEDGER_ROW)
            continue

        # Judged here, whatever the verdict. The ledger sweep below is for rows
        # this loop never reached — counting a row in both would report one
        # broken target twice and, worse, ask the source about a booking whose
        # target this loop has already condemned.
        judged_identities.add((decision.source_company_id, decision.source_record_id))

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
        # The service, re-proven against a catalogue read for THIS target. The
        # reconciliation used to skip this entirely: it passed no expectation, so
        # `prove_live_target` had nothing to check the ordered line against and a
        # matching fingerprint over the other fields read as a clean target.
        service_baseline = await evidence.prove(write_client, decision)
        if service_baseline is None:
            _unaccount(decision, evidence.failure_for(decision))
            continue

        proof = await prove_live_target(
            write_client,
            target_booking_uuid=row.target_booking_uuid,
            marker=marker,
            expected_fingerprint=row.target_snapshot_fingerprint,
            # The stored fingerprint binds the master, and the booking payload
            # names none — so the master has to be re-proven by the filtered
            # list before the fingerprints can even be compared.
            expected_staff_uuid=decision.easyweek_staff_uuid,
            expected_location_uuid=decision.easyweek_location_uuid,
            service_baseline=service_baseline,
        )
        if not proof.proven:
            _unaccount(decision, proof.reason)
            continue

        targets_proven += 1
        accounted += 1

    # -- the other direction: targets whose source is no longer active ------
    # A migrated booking whose source was cancelled, deleted, rescheduled into
    # the past or has simply vanished from Altegio classifies as SKIPPED, so the
    # loop above never sees it — and the EasyWeek appointment we created for it
    # kept standing while the reconciliation reported success. That is a real
    # extra appointment in the new schedule that no customer made.
    #
    # So every row the ledger says we CREATED is accounted for from the ledger
    # side too: either it was just proven active against an active source, or its
    # target must be proven gone or finished.
    #
    # Read-only. A ghost is reported and blocks the PASS; cancelling it is a
    # human decision, and this command never makes it.
    ghosts: list[dict[str, Any]] = []
    for (company_id, source_record_id), row in sorted(ledger_by_identity.items()):
        if row.status != ledger_module.STATUS_CREATED:
            continue
        if (company_id, source_record_id) in judged_identities:
            continue

        entry = {
            "source_company_id": company_id,
            "source_record_id": source_record_id,
            "target_booking_uuid": row.target_booking_uuid,
        }
        if write_client is None:
            entry["completeness_reason"] = COMPLETENESS_TARGET_UNPROVEN
            reasons[COMPLETENESS_TARGET_UNPROVEN] += 1
            ghosts.append(entry)
            continue

        # Being absent from THIS wave's active decisions does not mean the source
        # is gone. From the second wave onwards most of these rows belong to an
        # earlier, already-confirmed wave whose masters this manifest defers —
        # their bookings are alive, their targets are correct, and demanding that
        # they be cancelled before wave B can pass is exactly backwards.
        #
        # So the source is asked directly, with the wave selector switched off:
        # a selector says which masters migrate now, never whether a customer
        # still has an appointment.
        lifecycle = await reclassify_source_lifecycle(
            company_id=company_id,
            record_id=source_record_id,
            expected_fingerprint=row.source_fingerprint,
            manifest=inputs.manifest,
            directory=inputs.directory,
            cutover=inputs.cutover,
            http_client=http_client,
        )
        entry.update(lifecycle.as_safe_dict())
        marker = ledger_module.migration_marker(source_company_id=company_id, source_record_id=source_record_id)

        if lifecycle.state == LIFECYCLE_UNPROVABLE:
            # We could not establish what became of the source. Guessing either
            # way is wrong: "gone" would demand a live booking be cancelled,
            # "alive" would hide a real ghost.
            entry["completeness_reason"] = f"{COMPLETENESS_SOURCE_UNPROVABLE}:{lifecycle.detail}"
            reasons[COMPLETENESS_SOURCE_UNPROVABLE] += 1
            ghosts.append(entry)
            continue

        if lifecycle.state == LIFECYCLE_ACTIVE_UNCHANGED:
            # A correct row of an earlier wave. It is not a ghost — but it is
            # not taken on trust either: its target is proven live and still
            # matching the fingerprint stored when it was written.
            # An earlier wave's row: no decision to read the master off, so the
            # expected master comes from the manifest mapping of the Altegio
            # staff id the live source states — and is then PROVEN by the same
            # filtered-list query as everywhere else. The manifest supplies the
            # question; EasyWeek supplies the answer.
            earlier_branch = inputs.manifest.branch(company_id)
            expected_staff = (
                earlier_branch.staff_uuid(lifecycle.altegio_staff_id) if earlier_branch is not None else None
            )
            # An earlier wave's target is proven against ITS OWN stored
            # baseline — the expectation written down when it was created, which
            # no later plan rewrites.
            earlier_baseline = None
            earlier_service = (
                earlier_branch.service(lifecycle.altegio_service_id)
                if earlier_branch is not None and lifecycle.altegio_service_id is not None
                else None
            )
            if earlier_service is not None:
                assert earlier_branch is not None
                earlier_baseline = evidence.baselines.get(
                    (earlier_branch.easyweek_location_uuid, earlier_service.easyweek_service_uuid)
                )
                if earlier_baseline is not None:
                    catalog = await evidence.observe(write_client, location_uuid=earlier_branch.easyweek_location_uuid)
                    try:
                        if catalog is None:
                            raise ServiceEvidenceError(CATALOG_UNREADABLE)
                        verify_baseline(catalog, earlier_baseline)
                    except ServiceEvidenceError as exc:
                        entry["completeness_reason"] = exc.reason
                        reasons[exc.reason] += 1
                        ghosts.append(entry)
                        continue

            proof = await prove_live_target(
                write_client,
                target_booking_uuid=row.target_booking_uuid,
                marker=marker,
                expected_fingerprint=row.target_snapshot_fingerprint,
                expected_staff_uuid=expected_staff,
                expected_location_uuid=earlier_branch.easyweek_location_uuid if earlier_branch is not None else None,
                service_baseline=earlier_baseline,
            )
            targets_checked += 1
            if proof.proven:
                earlier_wave_targets += 1
                continue
            entry["completeness_reason"] = proof.reason
            reasons[proof.reason] += 1
            ghosts.append(entry)
            continue

        # The source was cancelled, deleted, finished, moved into the past, or
        # changed out from under the booking we created. Its target must be gone
        # or finished.
        proof = await prove_target_inactive_or_absent(
            write_client,
            target_booking_uuid=row.target_booking_uuid,
            marker=marker,
        )
        targets_checked += 1
        if proof.proven:
            # The source is gone and so is its booking. A consistent terminal
            # state, not a gap.
            terminal_targets += 1
            continue

        entry["completeness_reason"] = proof.reason
        reasons[proof.reason] += 1
        ghosts.append(entry)
        if proof.reason == GHOST_TARGET_STILL_ACTIVE:
            active_ghosts += 1

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
        and not ghosts
        and targets_proven == accounted
    )

    verdict = {
        "passed": passed,
        # Which wave this verdict is about. Two consecutive waves have different
        # identities, so a PASS can never be read as covering the other one.
        "wave_identity": (report.scope or {}).get("wave_identity"),
        "scope_proven": True,
        "source_was_read": source_was_read,
        "targets_were_checked": targets_were_checked,
        "source_active_bookings": len(active),
        "deferred_bookings": deferred,
        "excluded_empty_services": empty_services,
        "accounted_for": accounted,
        "live_targets_proven": targets_proven,
        "blocked": blocked_now,
        "source_changed": source_changed,
        "uncertain_or_pending": unresolved,
        "failed": failed,
        # Both directions of the check, so an operator can read at a glance what
        # was looked at and what needs a hand.
        "migration_targets_checked": targets_proven + targets_checked,
        "inactive_source_targets_checked": targets_checked,
        "inactive_source_targets_terminal": terminal_targets,
        "earlier_wave_targets_proven": earlier_wave_targets,
        "ghost_targets_active": active_ghosts,
        "unaccounted_reason_codes": dict(sorted(reasons.items())),
        "unaccounted_rows": unaccounted,
        # Source identities a human has to act on: a migrated booking whose
        # source is gone but whose EasyWeek appointment is still standing.
        "manual_action_required": ghosts,
    }
    if not passed:
        report.errors.append("final reconciliation did not prove cutover completeness")
    return verdict


async def _prove_uncertain_row_against_target(
    inputs: RunInputs,
    *,
    evidence: ServiceEvidence,
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

    # The service, re-proven against a catalogue read now, against the baseline
    # stored when the booking was created. A resolution is a claim that a
    # specific booking is the right one, so it carries the same burden a creation
    # does — and never a verdict cached from earlier in this command.
    expectation = await evidence.prove(write_client, expected_decision)
    if expectation is None:
        return False, f"{RESOLVE_SOURCE_UNPROVEN}:{evidence.failure_for(expected_decision)}", None

    marker = ledger_module.migration_marker(source_company_id=company_id, source_record_id=record_id)
    expected = expected_target_for(
        expected_decision, booking_uuid=target_booking_uuid, marker=marker, expectation=expectation
    )
    proof = await prove_live_target(
        write_client,
        target_booking_uuid=target_booking_uuid,
        marker=marker,
        expected=expected,
        service_baseline=expectation,
    )
    if not proof.proven:
        return False, proof.reason, None
    return True, RESOLVE_CONFIRMED, proof.live


async def _admit_canary_recovery(
    session_maker: async_sessionmaker[AsyncSession],
    inputs: RunInputs,
    *,
    binding: CanaryBinding,
    company_id: int,
    record_id: int,
) -> tuple[RecoveryAdmission, int | None]:
    """May this row be resolved against its own unverified canary attempt?

    The canary-side conditions live in :func:`find_recoverable_canary_attempt`;
    the ledger-side ones are here, because this is where that data is:

    * the row exists and is still unresolved (``uncertain`` / ``pending``);
    * its ORIGIN run is the run that wrote the canary proof — a later run's
      uncertain row may not ride on an earlier canary's attempt;
    * exactly one mutation attempt was recorded. More than one would mean the
      POST was sent again, which this design forbids and which would make "the
      booking the operator found" ambiguous between attempts.

    Admission is permission to *start* proving, never a substitute for the proof.
    Nothing outside PostgreSQL is read here.
    """
    async with session_maker() as session:
        admission, proof = await find_recoverable_canary_attempt(
            session,
            binding=binding,
            source_company_id=company_id,
            source_record_id=record_id,
        )
        if not admission.admitted or proof is None:
            return admission, None

        row = await ledger_module.get_row(session, source_company_id=company_id, source_record_id=record_id)
        if row is None:
            return RecoveryAdmission(admitted=False, reason=RESOLVE_ROW_MISSING), None
        if row.status not in (ledger_module.STATUS_UNCERTAIN, ledger_module.STATUS_PENDING):
            return RecoveryAdmission(admitted=False, reason=RESOLVE_NOT_UNCERTAIN), None
        if row.run_id != proof.run_id:
            return RecoveryAdmission(admitted=False, reason=RECOVERY_RUN_MISMATCH), None
        if (row.attempts or 0) != 1:
            return RecoveryAdmission(admitted=False, reason=RECOVERY_ATTEMPTS_UNEXPECTED), None

        return admission, proof.id


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

    # A row is resolved against the wave that tried to create it, never against
    # whatever scope the operator happens to be holding today. A different
    # cutover would re-classify the source and rebuild a different "expected"
    # booking to compare the live one against.
    scope, bindings = await _require_proven_scope(session_maker, inputs)
    report.scope = scope.as_safe_dict()

    recoverable_proof_id: int | None = None
    if not scope.proven:
        # One exception, and only one: the canary whose own POST outcome is
        # unknown. Its proof is the wave's first, it is unverified precisely
        # because the outcome is unknown, and the ordinary gate would therefore
        # refuse to resolve the row that would make it verified — a deadlock with
        # no safe way out, since re-sending the POST could double-break a person's
        # schedule by creating a second booking.
        #
        # The recovery is tried whatever the scope gate's reason was, and that
        # matters from the second wave onwards. Once wave A has a verified proof,
        # wave B's unknown canary no longer produces `migration_scope_missing` —
        # the lookup finds wave A's proof and answers `*_mismatch` or
        # `ambiguous` instead. Keying the recovery on one particular reason
        # therefore locked every wave after the first into the same deadlock.
        #
        # Widening the trigger does not widen the door: `_admit_canary_recovery`
        # still demands an EXACT binding, so wave A's proof can never admit
        # wave B, and every other use of an unverified proof stays shut.
        if not bindings:
            report.errors.append(scope.reason)
            report.reasons[scope.reason] += 1
            return report

        # One attempt per contract. `_admit_canary_recovery` demands an EXACT
        # binding, so at most one of these can match: the contract the stuck
        # canary was actually written under. Trying them in turn is how the
        # recovery finds that contract without being told which one it was, and
        # it opens no door — a proof filed under another contract still fails
        # the exact match.
        admission = None
        for candidate in bindings:
            admission, recoverable_proof_id = await _admit_canary_recovery(
                session_maker,
                inputs,
                binding=candidate,
                company_id=company_id,
                record_id=record_id,
            )
            if admission.admitted:
                break
        assert admission is not None  # `bindings` is non-empty here.
        report.canary_recovery = admission.as_safe_dict()
        if not admission.admitted:
            # Neither an ordinary verified wave nor a recoverable canary attempt.
            # Nothing outside PostgreSQL has been read.
            report.errors.append(scope.reason)
            report.reasons[scope.reason] += 1
            report.errors.append(admission.reason)
            report.reasons[admission.reason] += 1
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

    evidence = await load_service_evidence(session_maker, inputs)
    report.service_evidence = evidence.as_safe_dict()

    proven, reason, live = await _prove_uncertain_row_against_target(
        inputs,
        evidence=evidence,
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

    # One transaction for both verdicts. A canary recovery promotes the ledger row
    # AND its proof, and the two must never disagree: a ledger row that says
    # `created` beside a proof that still says "unknown outcome" would either
    # keep the wave locked out of bulk forever or, worse, be read as an
    # unverified wave that somehow produced bookings. Either both land or
    # neither does.
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
            if recoverable_proof_id is not None:
                await promote_proof_to_verified(
                    session,
                    proof_id=recoverable_proof_id,
                    target_booking_uuid=live.booking_uuid,
                    target_snapshot=live,
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
    http_client: httpx.AsyncClient | None = None,
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

    # The stored expectations. Each row re-reads the catalogue for itself below.
    evidence = await load_service_evidence(session_maker, inputs)
    report.service_evidence = evidence.as_safe_dict()

    async with session_maker() as session:
        rows = await ledger_module.rows_for_run(session, run_id=target_run, statuses=(ledger_module.STATUS_CREATED,))
        candidates = [
            (
                ledger_module.row_as_safe_dict(row),
                row.source_company_id,
                row.source_record_id,
                row.target_booking_uuid,
                row.target_snapshot_fingerprint,
                row.source_fingerprint,
                row.rollback_attempted_at is not None,
                row.rollback_attempt_run_id,
                row.run_id,
            )
            for row in rows
        ]

    for (
        safe_row,
        company_id,
        record_id,
        target,
        stored_fingerprint,
        source_fingerprint,
        attempted_at,
        attempt_run_id,
        origin_run_id,
    ) in candidates:
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

        # -- did OUR cancel already land? -----------------------------------
        # Asked before the projection, because the projection refuses a
        # cancelled booking and cannot tell the two cancellations apart:
        #
        #   * a booking cancelled by a person is a target somebody modified —
        #     unchanged behaviour, and never claimed as our rollback;
        #   * a booking cancelled while OUR attempt marker was standing is our
        #     own PUT whose result we never saw, and finishing that row costs
        #     no mutation at all.
        #
        # The marker is proven inside `prove_canceled_target`, so a booking that
        # is not one this migration wrote can never reach either branch.
        try:
            already_canceled = prove_canceled_target(booking, expected_marker=marker)
        except TargetSnapshotError:
            _refuse(ROLLBACK_TARGET_MODIFIED)
            continue

        if already_canceled:
            # Nothing is weakened by answering here, before the snapshot, staff
            # and service proofs: those exist to decide whether a booking may be
            # CANCELLED, and this branch cancels nothing. They also cannot run
            # on a cancelled booking at all — `project_target` refuses one by
            # design. What licenses the completion instead is the attempt marker
            # itself: it is only ever written after every one of those proofs
            # passed, moments before the PUT that this row is still waiting on.
            if not attempted_at:
                # Cancelled by somebody else, or before this tool ever tried.
                # Not ours to claim, and not a rollback we performed.
                _refuse(ROLLBACK_TARGET_MODIFIED)
                continue
            if not inputs.rollback_confirmed:
                # A dry-run states the finding and changes nothing.
                entry["rollback_outcome"] = ROLLBACK_RECOVERY_AVAILABLE
                report.reasons[ROLLBACK_RECOVERY_AVAILABLE] += 1
                report.created_rows.append(entry)
                continue
            # Finishing SOMEBODY'S attempt — this run's own, or the earlier
            # run's whose result was never seen. Conditional on that exact
            # attempt still standing: if it was released or replaced while this
            # booking was being read, the evidence moved and the conclusion goes
            # with it.
            assert attempt_run_id is not None
            async with session_maker() as session:
                async with session.begin():
                    recorded = await ledger_module.record_rolled_back(
                        session,
                        run_id=inputs.run_id,
                        source_company_id=company_id,
                        source_record_id=record_id,
                        expected_attempt_run_id=attempt_run_id,
                    )
            if not recorded:
                _refuse(ROLLBACK_ATTEMPT_UNRESOLVED)
                continue
            entry["rollback_outcome"] = ROLLBACK_RECOVERED
            report.reasons[ROLLBACK_RECOVERED] += 1
            report.created_rows.append(entry)
            continue

        if attempted_at:
            # Our attempt is on the row and the booking is still live. Either the
            # PUT never left, or it left and did nothing. Both readings forbid an
            # automatic second PUT: EasyWeek publishes no idempotency key, and a
            # blind repeat is the unknown mutation this design refuses to make.
            _refuse(ROLLBACK_ATTEMPT_UNRESOLVED)
            continue

        try:
            live = project_target(booking, expected_marker=marker)
        except TargetSnapshotError:
            # A missing or unreadable field, a rewritten marker, an already
            # cancelled or completed booking. All of them mean "we cannot prove
            # this is untouched", which is treated exactly as "it was touched".
            _refuse(ROLLBACK_TARGET_MODIFIED)
            continue

        # The stored fingerprint binds the master, and the booking payload names
        # none — so rollback proves it the same way every other path does, and
        # refuses when it cannot. That is the contract stated the other way
        # round: a target we cannot prove is a target we must not cancel.
        #
        # The master to ask about comes from re-deriving the source, which is
        # also the check that the appointment still is the one we wrote. A source
        # that has moved on since therefore blocks the cancellation — deliberately:
        # cancelling a booking somebody has deliberately changed is the damage
        # this whole path exists to avoid.
        _reproof, fresh_decision = await reclassify_source_for_resolution(
            company_id=company_id,
            record_id=record_id,
            expected_fingerprint=source_fingerprint,
            manifest=inputs.manifest,
            directory=inputs.directory,
            cutover=inputs.cutover,
            http_client=http_client,
            # Rollback is scoped by run id, not by wave. A master this manifest
            # defers today may well be the master that run created for.
            ignore_wave_scope=True,
        )
        if fresh_decision is None or fresh_decision.easyweek_staff_uuid is None:
            _refuse(ROLLBACK_TARGET_MODIFIED)
            continue

        assignment = await prove_staff_assignment(
            write_client,
            target_booking_uuid=live.booking_uuid,
            location_uuid=live.location_uuid,
            staff_uuid=fresh_decision.easyweek_staff_uuid,
            start_time_utc=live.start_time_utc,
        )
        if not assignment.proven:
            _refuse(ROLLBACK_STAFF_UNPROVEN)
            continue
        live = live.with_proven_staff(fresh_decision.easyweek_staff_uuid)

        # The service, through the SAME proof every other path uses. Rollback had
        # its own weaker check — project the booking, compare the fingerprint —
        # which never looked at the ordered line at all. An ambiguous or drifted
        # service therefore left a target "eligible" for cancellation, and
        # cancelling a booking we cannot identify is the damage this path exists
        # to avoid.
        service_baseline = await evidence.prove(write_client, fresh_decision)
        if service_baseline is None:
            _refuse(ROLLBACK_SERVICE_UNPROVEN)
            continue
        try:
            prove_ordered_service(read_ordered_service(booking), service_baseline)
        except ServiceEvidenceError:
            _refuse(ROLLBACK_SERVICE_UNPROVEN)
            continue

        if live.fingerprint != stored_fingerprint:
            _refuse(ROLLBACK_TARGET_MODIFIED)
            continue

        if not inputs.rollback_confirmed:
            entry["rollback_outcome"] = ROLLBACK_ELIGIBLE
            report.reasons[ROLLBACK_ELIGIBLE] += 1
            report.created_rows.append(entry)
            continue

        # -- take the mutation right, atomically -----------------------------
        # ONE conditional UPDATE is both the marker and the lock. Two rollback
        # runs walking the same wave used to read a NULL marker and both go on
        # to send a cancel; now exactly one of them can win the row, and the
        # loser is out of the mutation path entirely.
        #
        # The claim also re-tests every fact the decision above rested on —
        # status, origin run, target uuid — so a row that moved while these
        # proofs were being fetched is not mutated on stale evidence.
        async with session_maker() as session:
            async with session.begin():
                claim = await ledger_module.claim_rollback_attempt(
                    session,
                    run_id=inputs.run_id,
                    source_company_id=company_id,
                    source_record_id=record_id,
                    origin_run_id=origin_run_id,
                    target_booking_uuid=target,
                )
        if not claim.won:
            # A loser sends nothing, ever. It reports what it found and leaves
            # the row to whoever owns it — including the case where that owner
            # is a run whose own outcome is still unknown.
            entry["rollback_outcome"] = ROLLBACK_CLAIM_LOST
            entry["reason"] = claim.reason
            if claim.owner_run_id is not None:
                # Which run to go and read. Without it `rollback_claim_lost` says
                # only that somebody else got there first, and an operator has no
                # way to find the report that says what happened to this booking.
                #
                # It comes from the claim — the row as it stood at that instant —
                # never from the candidate snapshot, which was read before the
                # race and says nobody owned the row. And it is only set when an
                # owner was actually observed: a row that vanished or moved has
                # no owner to name, and inventing one would send an operator
                # looking for a run that never existed.
                #
                # A run id is a technical identifier: no customer, no booking
                # content, no provider text.
                entry["rollback_claim_owner_run_id"] = claim.owner_run_id
            report.reasons[ROLLBACK_CLAIM_LOST] += 1
            report.created_rows.append(entry)
            continue

        async def _release(entry: dict[str, Any], reason: str) -> None:
            """Hand the mutation right back, then report `reason`.

            Used only where it is PROVEN that no cancel is in flight. If the
            release itself does not take — somebody else owns the marker, or it
            is already gone — the row is reported unresolved instead: claiming a
            retry is safe when the durable state says otherwise is exactly the
            direction this design never goes.
            """
            async with session_maker() as session:
                async with session.begin():
                    released = await ledger_module.release_rollback_attempt(
                        session,
                        run_id=inputs.run_id,
                        source_company_id=company_id,
                        source_record_id=record_id,
                    )
            outcome = reason if released else ROLLBACK_ATTEMPT_UNRESOLVED
            entry["rollback_outcome"] = outcome
            report.reasons[outcome] += 1
            report.created_rows.append(entry)

        report.mutations_attempted += 1
        try:
            outcome = await write_client.cancel_booking(target)
        except EasyWeekCancelNotSent:
            # (A) Proven NOT sent: the read immediately before the PUT failed.
            # There is no unknown mutation, so there must be no marker claiming
            # one — otherwise a cancellation somebody makes by hand tomorrow
            # would be read as this attempt finishing late.
            await _release(entry, ROLLBACK_NOT_SENT)
            continue
        except EasyWeekUncertainMutation:
            # (C) The cancel may or may not have landed, or it landed and the
            # booking would not read back as cancelled. Either way the ledger
            # keeps saying `created`: `rolled_back` is a claim about a real
            # appointment, and recording it on an unproven cancel would hide a
            # live booking from every later reconciliation.
            #
            # The marker STAYS. This is the one state it exists for.
            entry["rollback_outcome"] = ROLLBACK_UNCERTAIN
            report.reasons[ROLLBACK_UNCERTAIN] += 1
            report.created_rows.append(entry)
            continue
        except EasyWeekError as exc:
            # (B) A deterministic refusal: the request was answered, and the
            # answer says nothing changed. That is not an unknown mutation
            # either, so the marker goes back — otherwise the cause could be
            # fixed and the next explicit rollback would find itself locked out
            # by a claim about a cancel that provably never happened.
            entry["reason"] = _safe_error_code(exc)
            await _release(entry, ROLLBACK_REFUSED)
            continue

        if outcome is CancelOutcome.ALREADY_CANCELED_NO_MUTATION:
            # Somebody cancelled it between the proofs above and this run's own
            # PUT. No mutation was sent, so the marker goes back — and the row
            # is NOT recorded as this rollback's work, because it is not.
            await _release(entry, ROLLBACK_CANCELED_ELSEWHERE)
            continue

        # (D) Proven cancelled — `cancel_booking` read it back before returning.
        # Finalised conditionally on this run still owning the attempt, so a
        # marker that changed underneath cannot be finished by the wrong run.
        async with session_maker() as session:
            async with session.begin():
                recorded = await ledger_module.record_rolled_back(
                    session,
                    run_id=inputs.run_id,
                    source_company_id=company_id,
                    source_record_id=record_id,
                    expected_attempt_run_id=inputs.run_id,
                )
        if not recorded:
            entry["rollback_outcome"] = ROLLBACK_ATTEMPT_UNRESOLVED
            report.reasons[ROLLBACK_ATTEMPT_UNRESOLVED] += 1
            report.created_rows.append(entry)
            continue
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
