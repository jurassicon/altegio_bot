"""Post-migration reminder handover (plan §30): one operator process, once.

The gap this closes
-------------------
The cutover writes bookings into EasyWeek and a ledger row for each. It
deliberately creates **no** ``MessageJob`` — planning reminders belongs to the
webhook path, not to a migrator, and a migrator that also planned would be a
second planner.

The consequence is visible the morning after a wave: the customer has an
appointment in EasyWeek, its future reminders are still queued on the Altegio
side pointing at a booking nobody works from any more, and on the EasyWeek side
there are none at all. One reminder aimed at a dead record; none aimed at the
live one.

This module closes that gap once, by hand, under supervision. It is not a
background reconciler, it does not poll Altegio, it plans nothing but
``reminder_24h`` and ``reminder_2h``, and after the handover the ordinary
EasyWeek webhooks are again the only thing that reschedules anything.

Three commands, and only one of them writes
-------------------------------------------
``plan``    reads the ledger, proves every target booking against the live API,
            works out what is owed and what exists, and writes a PII-free
            snapshot. Touches no database row.
``apply``   one PostgreSQL transaction against that exact snapshot: create the
            missing EasyWeek reminders FIRST, then cancel the superseded Altegio
            ones. No API call at all — the live proof was taken in ``plan``,
            while the outbox was still running, which is what keeps the outbox
            stop down to a database transaction.
``verify``  proves the end state.

Why creation comes strictly before cancellation
-----------------------------------------------
Between the two there is a window in which a customer is covered by neither
side. Doing it in this order makes that window empty in the failure case too: if
creation fails, the transaction rolls back and the old Altegio reminders are
still queued — the customer keeps the reminder they had. The other order would
leave a real appointment with no reminder at all if the second half failed.

Three different questions
-------------------------
``easyweek_reminder_preflight`` answers "are the reminders that exist correct?".
That is not "do the reminders that should exist, exist?", and neither is
"can ownership be switched atomically right now?". This module reports all three
separately — ``guard_ready``, ``coverage_ready``, ``cutover_ready`` — because a
green preflight over an EMPTY EasyWeek queue is exactly the state this whole
module exists to fix.

Nothing here calls Meta or Chatwoot, writes an ``OutboxMessage``, or sends
anything.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

from altegio_bot.easyweek_migration.manifest import canonical_uuid as manifest_canonical_uuid
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.easyweek_reminders import (
    REMINDER_OFFSETS,
    PlannedReminder,
    easyweek_reminder_dedupe_key,
    plan_reminders,
    reminder_job_payload,
)
from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK

SNAPSHOT_VERSION: Final = 3
APPLY_REPORT_VERSION: Final = 2
SNAPSHOT_MODE: Final = 0o600
DIR_MODE: Final = 0o700

# How long a plan may sit before it stops describing the world. Reminder
# obligations move with the clock: a two-hour reminder that was owed when the
# plan was written may be in the past by the time an operator gets to apply it.
DEFAULT_MAX_SNAPSHOT_AGE_SEC: Final = 3600

# The confirmation an operator types. It carries the plan digest, so a phrase
# copied out of yesterday's terminal cannot authorise today's plan.
CONFIRMATION_TEMPLATE: Final = "apply reminder handover {digest}"

# Why an old Altegio reminder was withdrawn. Stable, PII-free, and specific
# enough that somebody reading `message_jobs` in six months knows what did it.
CANCEL_REASON: Final = "superseded by migrated EasyWeek booking (reminder handover)"

# What an apply would do to this row's durable ownership marker.
#
# The marker is the durable fact that a booking's future reminders stopped being
# Altegio's. Without it a late Altegio delivery had nothing to consult, and
# `add_job` — which re-queues a cancelled job on conflict — re-opened the very
# reminder the handover had just withdrawn.
MARKER_SET: Final = "set"
MARKER_ALREADY: Final = "already_handed_over"

# Per-obligation outcomes. Only MISSING leads to an insert.
OBLIGATION_MISSING: Final = "missing"
OBLIGATION_PRESENT_OPEN: Final = "already_present_open"
OBLIGATION_DONE: Final = "already_done"
OBLIGATION_PROCESSING: Final = "processing"
OBLIGATION_OCCUPIED_CANCELED: Final = "occupied_by_canceled"
OBLIGATION_OCCUPIED_FAILED: Final = "occupied_by_failed"
OBLIGATION_OCCUPIED_UNKNOWN: Final = "occupied_by_unknown_status"

# Row-level refusals. Every one of them is a row that does NOT enter the wave.
ROW_LEDGER_NOT_CREATED: Final = "ledger_not_created"
ROW_TARGET_UUID_INVALID: Final = "target_uuid_invalid"
ROW_SOURCE_RECORD_MISSING: Final = "source_record_missing"
ROW_TARGET_RECORD_MISSING: Final = "target_record_missing"
ROW_PROVIDER_MISMATCH: Final = "provider_mismatch"
ROW_COMPANY_MISMATCH: Final = "company_mismatch"
ROW_BRANCH_UNPROVEN: Final = "branch_identity_unproven"
ROW_TARGET_UNPROVEN: Final = "target_unproven"
ROW_LOCAL_TARGET_MISMATCH: Final = "local_target_mismatch"
ROW_NO_FUTURE_OBLIGATION: Final = "no_future_obligation"
# Half an ownership marker on the ledger row. The database CHECK forbids it, so
# a row in this state was written outside every supported path and is not
# something to reason about.
ROW_MARKER_INCOMPLETE: Final = "marker_incomplete"

# Statuses an open reminder can hold. `processing` counts as open: a job the
# worker claimed a second ago is still going to fire.
OPEN_STATUSES: Final = ("queued", "processing")
COVERING_STATUSES: Final = ("queued", "processing", "done")

_OBLIGATION_OUTCOMES: Final = frozenset(
    {
        OBLIGATION_MISSING,
        OBLIGATION_PRESENT_OPEN,
        OBLIGATION_DONE,
        OBLIGATION_PROCESSING,
        OBLIGATION_OCCUPIED_CANCELED,
        OBLIGATION_OCCUPIED_FAILED,
        OBLIGATION_OCCUPIED_UNKNOWN,
    }
)
_OUTCOME_STATUS: Final = {
    OBLIGATION_PRESENT_OPEN: "queued",
    OBLIGATION_PROCESSING: "processing",
    OBLIGATION_DONE: "done",
    OBLIGATION_OCCUPIED_CANCELED: "canceled",
    OBLIGATION_OCCUPIED_FAILED: "failed",
}


def canonical_uuid(raw: object) -> uuid_module.UUID | None:
    """A UUID we are willing to act on, or nothing.

    Delegates the acceptance rule to the manifest parser's own
    :func:`~altegio_bot.easyweek_migration.manifest.canonical_uuid` so the two
    cannot disagree about what a canonical identifier is, then returns it parsed.
    A value that had to be normalised — braces, upper case — is a value somebody
    typed, and a booking is not something to act on by approximation.

    A ``uuid.UUID`` straight out of the ``records`` column is already canonical
    by construction and passes through.
    """
    if isinstance(raw, uuid_module.UUID):
        return raw
    text = manifest_canonical_uuid(raw)
    if text is None:
        return None
    try:
        return uuid_module.UUID(text)
    except (ValueError, AttributeError, TypeError):  # pragma: no cover - defensive
        return None


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# What one migrated booking owes, and what it already has
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Obligation:
    """One reminder a live booking owes, and the state of its canonical key."""

    job_type: str
    run_at: datetime
    dedupe_key: str
    outcome: str
    existing_job_id: int | None = None
    existing_job_status: str | None = None

    @property
    def needs_insert(self) -> bool:
        return self.outcome == OBLIGATION_MISSING

    @property
    def is_blocker(self) -> bool:
        """A key held by something that is neither open nor done.

        Re-creating under an occupied key is impossible (the column is unique)
        and re-opening the row that holds it is an operator decision, not a
        tool's: a cancelled reminder was cancelled for a reason nobody here can
        see, and a failed one may have reached Meta.
        """
        return self.outcome in (
            OBLIGATION_OCCUPIED_CANCELED,
            OBLIGATION_OCCUPIED_FAILED,
            OBLIGATION_OCCUPIED_UNKNOWN,
        )

    def as_safe_dict(self) -> dict[str, Any]:
        """The obligation as the snapshot stores it.

        The dedupe key is here because it is what an apply inserts under and
        what a verify looks for. It is a hash of the provider, the booking uuid,
        the job type and the start instant — an identifier, with nothing about a
        customer in it.
        """
        return {
            "job_type": self.job_type,
            "run_at": self.run_at.isoformat().replace("+00:00", "Z"),
            "dedupe_key": self.dedupe_key,
            "outcome": self.outcome,
            "existing_job_id": self.existing_job_id,
            "existing_job_status": self.existing_job_status,
        }


@dataclass(frozen=True)
class HandoverRow:
    """One ledger-proven migration, and the reminder work it implies.

    PII-free by construction: ids, UUIDs, instants and codes. No customer name,
    no phone, no e-mail, no service text. The booking UUID is an EasyWeek
    identifier the reports already carry; the CUSTOMER uuid never appears.
    """

    ledger_id: int
    source_company_id: int
    source_record_id: int
    source_record_pk: int
    target_record_pk: int
    # The EASYWEEK location id, which is what an EasyWeek `Record` and every
    # EasyWeek `MessageJob` carry in their `company_id` column. Deliberately not
    # the Altegio company id: the two number spaces are different, and a job
    # created with the Altegio one would be refused by the runtime guard's
    # `record_company_id` check the moment it came up to send.
    target_company_id: int
    target_booking_uuid: str
    target_starts_at: datetime
    target_is_canceled: bool = False
    target_is_completed: bool = False
    obligations: tuple[Obligation, ...] = ()
    # Altegio reminder job ids that are still queued for the source booking.
    stale_source_job_ids: tuple[int, ...] = ()
    # Source reminder jobs the worker has already claimed. One of these stops
    # the whole apply: a claimed job may be mid-flight to Meta.
    processing_source_job_ids: tuple[int, ...] = ()
    # The durable reminder-ownership marker as it stands right now, and what an
    # apply would do to it. `MARKER_SET` means there is none yet; `MARKER_ALREADY`
    # means this exact handover already ran and carries the digest it ran under,
    # so a repeat of the same snapshot is recognised instead of re-marking.
    marker_action: str = MARKER_SET
    marker_existing_digest: str | None = None
    marker_handed_over_at: str | None = None
    refusal: str | None = None

    @property
    def in_scope(self) -> bool:
        return self.refusal is None

    @property
    def missing(self) -> tuple[Obligation, ...]:
        return tuple(item for item in self.obligations if item.needs_insert)

    @property
    def blockers(self) -> tuple[Obligation, ...]:
        return tuple(item for item in self.obligations if item.is_blocker)

    def identity(self) -> dict[str, Any]:
        """The frozen scope of this row. Compared again before any write."""
        return {
            "ledger_id": self.ledger_id,
            "source_company_id": self.source_company_id,
            "source_record_id": self.source_record_id,
            "source_record_pk": self.source_record_pk,
            "target_record_pk": self.target_record_pk,
            "target_company_id": self.target_company_id,
            "target_booking_uuid": self.target_booking_uuid,
            "target_starts_at": _as_utc(self.target_starts_at).isoformat().replace("+00:00", "Z"),
            "target_is_canceled": self.target_is_canceled,
            "target_is_completed": self.target_is_completed,
        }

    def as_safe_dict(self) -> dict[str, Any]:
        """The row as the snapshot stores it, identity kept as its own object.

        Nested rather than flattened so the frozen scope is one thing that can
        be re-proven, digested and locked as a unit — the digest, the row locks
        and the pre-write re-check all read exactly this sub-object, and a
        flattened shape had them reading three slightly different things.
        """
        return {
            "identity": self.identity(),
            "obligations": [item.as_safe_dict() for item in self.obligations],
            "stale_source_job_ids": list(self.stale_source_job_ids),
            "processing_source_job_ids": list(self.processing_source_job_ids),
            # Digested with everything else, so an edited marker expectation —
            # a row switched from "set" to "already handed over", a swapped
            # digest — invalidates the snapshot rather than authorising a write.
            "marker": {
                "action": self.marker_action,
                "existing_digest": self.marker_existing_digest,
                "handed_over_at": self.marker_handed_over_at,
            },
            "refusal": self.refusal,
        }


def obligations_for(
    *,
    booking_uuid: uuid_module.UUID,
    starts_at: datetime,
    now: datetime,
    is_active: bool,
    existing: dict[str, tuple[int, str]],
) -> tuple[Obligation, ...]:
    """What this booking owes now, matched against the keys already taken.

    ``plan_reminders`` is the canonical source of the windows — more than 24h
    owes both, 2–24h owes only the two-hour one, under 2h owes nothing, and a
    booking that is not active owes nothing at all. Reimplementing those bounds
    here would let the handover disagree with the planner that maintains them.
    """
    if not is_active:
        return ()

    planned: list[PlannedReminder] = plan_reminders(
        booking_uuid=booking_uuid,
        starts_at=starts_at,
        now=now,
        is_deleted=False,
    )

    rows: list[Obligation] = []
    for item in planned:
        held = existing.get(item.dedupe_key)
        if held is None:
            outcome, job_id = OBLIGATION_MISSING, None
        else:
            job_id, status = held
            outcome = {
                "queued": OBLIGATION_PRESENT_OPEN,
                "processing": OBLIGATION_PROCESSING,
                "done": OBLIGATION_DONE,
                "canceled": OBLIGATION_OCCUPIED_CANCELED,
                "failed": OBLIGATION_OCCUPIED_FAILED,
            }.get(status, OBLIGATION_OCCUPIED_UNKNOWN)
        rows.append(
            Obligation(
                job_type=item.job_type,
                run_at=item.run_at,
                dedupe_key=item.dedupe_key,
                outcome=outcome,
                existing_job_id=job_id,
                existing_job_status=status if held is not None else None,
            )
        )
    return tuple(rows)


def insert_values(
    row: HandoverRow,
    obligation: Obligation,
    *,
    client_id: int | None,
) -> dict[str, Any]:
    """The exact ``message_jobs`` row a missing reminder becomes.

    Production identity throughout: the EasyWeek provider, the EasyWeek target
    record, the target's own company, and the canonical dedupe key, ``run_at``
    and payload the webhook planner would have produced for the same booking.
    Anything else here would create a job the runtime guard then refuses.
    """
    booking_uuid = canonical_uuid(row.target_booking_uuid)
    assert booking_uuid is not None  # proven when the row entered scope
    return {
        "provider": PROVIDER_EASYWEEK,
        # The target's own company — the EasyWeek location id — so the created
        # job matches the Record the guard will compare it against.
        "company_id": row.target_company_id,
        "record_id": row.target_record_pk,
        "client_id": client_id,
        "job_type": obligation.job_type,
        "run_at": obligation.run_at,
        "status": "queued",
        "dedupe_key": obligation.dedupe_key,
        "payload": reminder_job_payload(
            booking_uuid=booking_uuid,
            company_id=row.target_company_id,
            starts_at=row.target_starts_at,
            job_type=obligation.job_type,
        ),
    }


# ---------------------------------------------------------------------------
# The plan, and the file it is frozen into
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EligibleRefusal:
    """A created ledger row that had to be proven but was not.

    It remains in the snapshot as PII-free evidence.  A refusal is not a row we
    silently omit; it blocks the complete company/status scope.
    """

    ledger_id: int
    source_company_id: int
    source_record_id: int
    reason: str

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ledger_id": self.ledger_id,
            "source_company_id": self.source_company_id,
            "source_record_id": self.source_record_id,
            "reason": self.reason,
        }


@dataclass
class HandoverPlan:
    """Everything one ``plan`` run proved, and what an apply would do."""

    company_ids: tuple[int, ...]
    created_at: datetime
    rows: tuple[HandoverRow, ...] = ()
    # Historical/non-created rows are information only.  Created rows that
    # could not be proved are durable blockers and retain their safe identity.
    historical_rows: dict[str, int] = field(default_factory=dict)
    eligible_refusals: tuple[EligibleRefusal, ...] = ()
    ledger_rows_seen: int = 0
    eligible_created_rows: int | None = None

    def __post_init__(self) -> None:
        if self.eligible_created_rows is None:
            self.eligible_created_rows = len(self.scoped) + len(self.eligible_refusals)

    @property
    def refused(self) -> dict[str, int]:
        """Compatibility report: eligible refusal counts, never the authority."""
        counts: dict[str, int] = {}
        for item in self.eligible_refusals:
            counts[item.reason] = counts.get(item.reason, 0) + 1
        return counts

    @property
    def scoped(self) -> tuple[HandoverRow, ...]:
        return tuple(row for row in self.rows if row.in_scope)

    @property
    def to_create(self) -> int:
        return sum(len(row.missing) for row in self.scoped)

    @property
    def to_cancel(self) -> int:
        return sum(len(row.stale_source_job_ids) for row in self.scoped)

    @property
    def blocked_rows(self) -> tuple[HandoverRow, ...]:
        return tuple(row for row in self.scoped if row.blockers)

    @property
    def processing_rows(self) -> tuple[HandoverRow, ...]:
        return tuple(row for row in self.scoped if row.processing_source_job_ids)

    @property
    def guard_ready(self) -> bool:
        """Are the EasyWeek reminders that already exist in a sane state?

        Narrow on purpose, and NOT the same as coverage: an empty EasyWeek queue
        satisfies this trivially, which is precisely the trap the standalone
        preflight falls into after a migration.
        """
        return not self.blocked_rows and not self.eligible_refusals

    @property
    def coverage_ready(self) -> bool:
        """Does every obligation already exist? True only after a good apply."""
        return self.guard_ready and self.to_create == 0

    @property
    def cutover_ready(self) -> bool:
        """May ownership be switched atomically right now?"""
        return (
            bool(self.eligible_created_rows)
            and self.eligible_created_rows == len(self.scoped)
            and self.guard_ready
            and not self.processing_rows
        )

    def _snapshot_material(self) -> dict[str, Any]:
        outcomes: dict[str, int] = {}
        for row in self.scoped:
            for item in row.obligations:
                outcomes[item.outcome] = outcomes.get(item.outcome, 0) + 1
        return {
            "version": SNAPSHOT_VERSION,
            "mode": "read-only",
            "created_at": _timestamp(self.created_at),
            "company_ids": sorted(self.company_ids),
            "ledger_rows_seen": self.ledger_rows_seen,
            "eligible_created_rows": self.eligible_created_rows,
            "historical_rows": dict(sorted(self.historical_rows.items())),
            "eligible_refusals": [
                item.as_safe_dict() for item in sorted(self.eligible_refusals, key=lambda item: item.ledger_id)
            ],
            "rows": [row.as_safe_dict() for row in sorted(self.scoped, key=lambda item: item.ledger_id)],
            "obligation_outcomes": dict(sorted(outcomes.items())),
            "readiness": {
                "guard_ready": self.guard_ready,
                "coverage_ready": self.coverage_ready,
                "cutover_ready": self.cutover_ready,
            },
        }

    def digest(self) -> str:
        """Identity of exactly what an apply would do.

        Covers the complete canonical snapshot except the digest field itself.
        This includes ``created_at``: otherwise editing the clock could extend
        an already reviewed snapshot's lifetime.
        """
        return _payload_digest(self._snapshot_material())

    def as_safe_dict(self) -> dict[str, Any]:
        """The report. Counts, codes, ids, UUIDs and instants — nothing else."""
        outcomes: dict[str, int] = {}
        for row in self.scoped:
            for item in row.obligations:
                outcomes[item.outcome] = outcomes.get(item.outcome, 0) + 1

        return {
            "version": SNAPSHOT_VERSION,
            "mode": "read-only",
            "company_ids": sorted(self.company_ids),
            "created_at": _timestamp(self.created_at),
            "plan_digest": self.digest(),
            "ledger_rows_seen": self.ledger_rows_seen,
            "eligible_created_rows": self.eligible_created_rows,
            "rows_in_scope": len(self.scoped),
            "rows_refused": dict(sorted(self.refused.items())),
            "historical_rows": dict(sorted(self.historical_rows.items())),
            "eligible_refusals": [item.as_safe_dict() for item in self.eligible_refusals],
            "obligation_outcomes": dict(sorted(outcomes.items())),
            "rows_without_future_obligation": sum(1 for row in self.scoped if not row.obligations),
            "easyweek_reminders_to_create": self.to_create,
            "altegio_reminders_to_cancel": self.to_cancel,
            "rows_with_blockers": [row.ledger_id for row in self.blocked_rows],
            "rows_with_processing_source_jobs": [row.ledger_id for row in self.processing_rows],
            # Three questions, three answers. See the module docstring.
            "guard_ready": self.guard_ready,
            "coverage_ready": self.coverage_ready,
            "cutover_ready": self.cutover_ready,
        }

    def to_snapshot(self) -> dict[str, Any]:
        payload = self._snapshot_material()
        payload["plan_digest"] = self.digest()
        return payload


def handover_timestamp(value: datetime) -> str:
    """Canonical UTC timestamp, in the one format the snapshot validates."""
    return _timestamp(value)


def _timestamp(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _payload_digest(payload: dict[str, Any]) -> str:
    material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def write_snapshot(plan: HandoverPlan, path: str | Path) -> Path:
    """Freeze the plan to disk, 0600 and atomically.

    Not in the repository, and not world-readable: it names real bookings and
    real job ids, and it is the artefact an apply is authorised against.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(target.parent, DIR_MODE)
    tmp = target.with_suffix(target.suffix + ".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, SNAPSHOT_MODE)
    try:
        os.write(fd, json.dumps(plan.to_snapshot(), ensure_ascii=False, indent=2).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, target)
    os.chmod(target, SNAPSHOT_MODE)
    dir_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return target


class SnapshotError(Exception):
    """The snapshot cannot authorise an apply. Always a full stop."""


@dataclass(frozen=True)
class ApplyReport:
    """Durable, PII-free evidence emitted by one committed apply."""

    snapshot_version: int
    snapshot_digest: str
    company_ids: tuple[int, ...]
    applied_at: datetime
    eligible_created_rows: int
    rows_in_scope: int
    created_job_ids: tuple[int, ...]
    canceled_job_ids: tuple[int, ...]
    already_present_count: int
    # The durable evidence half. `marked` are the rows this apply stamped;
    # `already_marked` are the rows that already carried THIS plan's marker, so
    # an idempotent repeat reports zero mutations without losing the fact that
    # the scope is fully covered.
    marked_ledger_ids: tuple[int, ...]
    already_marked_ledger_ids: tuple[int, ...]
    scoped_outbox_ids_before: tuple[int, ...]
    scoped_outbox_ids_after: tuple[int, ...]

    @property
    def mutation_count(self) -> int:
        return len(self.created_job_ids) + len(self.canceled_job_ids) + len(self.marked_ledger_ids)

    @property
    def marked_ledger_count(self) -> int:
        return len(self.marked_ledger_ids)

    @property
    def created_job_count(self) -> int:
        return len(self.created_job_ids)

    @property
    def canceled_job_count(self) -> int:
        return len(self.canceled_job_ids)

    def material(self) -> dict[str, Any]:
        return {
            "version": APPLY_REPORT_VERSION,
            "mode": "apply-report",
            "snapshot_version": self.snapshot_version,
            "snapshot_digest": self.snapshot_digest,
            "company_ids": list(self.company_ids),
            "applied_at": _timestamp(self.applied_at),
            "eligible_created_rows": self.eligible_created_rows,
            "rows_in_scope": self.rows_in_scope,
            "created_job_ids": list(self.created_job_ids),
            "created_job_count": self.created_job_count,
            "canceled_job_ids": list(self.canceled_job_ids),
            "canceled_job_count": self.canceled_job_count,
            "already_present_count": self.already_present_count,
            "marked_ledger_ids": list(self.marked_ledger_ids),
            "marked_ledger_count": self.marked_ledger_count,
            "already_marked_ledger_ids": list(self.already_marked_ledger_ids),
            "already_marked_ledger_count": len(self.already_marked_ledger_ids),
            "scoped_outbox_ids_before": list(self.scoped_outbox_ids_before),
            "scoped_outbox_ids_after": list(self.scoped_outbox_ids_after),
            "mutation_count": self.mutation_count,
        }

    def to_json(self) -> dict[str, Any]:
        payload = self.material()
        payload["report_digest"] = _payload_digest(payload)
        return payload


def write_apply_report(report: ApplyReport, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(target.parent, DIR_MODE)
    tmp = target.with_suffix(target.suffix + ".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, SNAPSHOT_MODE)
    try:
        os.write(fd, json.dumps(report.to_json(), ensure_ascii=False, indent=2).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, target)
    os.chmod(target, SNAPSHOT_MODE)
    dir_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return target


def read_apply_report(path: str | Path, *, frozen: FrozenPlan | None = None) -> ApplyReport:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as error:
        raise SnapshotError(f"cannot read the apply report: {error.strerror}") from None
    except Exception:
        raise SnapshotError("the apply report is not valid JSON") from None
    expected = {
        "version",
        "mode",
        "snapshot_version",
        "snapshot_digest",
        "company_ids",
        "applied_at",
        "eligible_created_rows",
        "rows_in_scope",
        "created_job_ids",
        "created_job_count",
        "canceled_job_ids",
        "canceled_job_count",
        "already_present_count",
        "marked_ledger_ids",
        "marked_ledger_count",
        "already_marked_ledger_ids",
        "already_marked_ledger_count",
        "scoped_outbox_ids_before",
        "scoped_outbox_ids_after",
        "mutation_count",
        "report_digest",
    }
    _exact_keys(payload, expected, "apply report")
    assert isinstance(payload, dict)
    if payload["version"] != APPLY_REPORT_VERSION or payload["mode"] != "apply-report":
        raise SnapshotError("the apply report has an unexpected version or mode")
    material = dict(payload)
    embedded = material.pop("report_digest")
    if not isinstance(embedded, str) or embedded != _payload_digest(material):
        raise SnapshotError("the apply report content does not match its digest")
    if payload["snapshot_version"] != SNAPSHOT_VERSION:
        raise SnapshotError("the apply report names an unsupported snapshot version")
    snapshot_digest = payload["snapshot_digest"]
    if not isinstance(snapshot_digest, str) or len(snapshot_digest) != 64:
        raise SnapshotError("the apply report snapshot digest is invalid")
    companies = _positive_int_list(payload["company_ids"], "apply report company_ids")
    if list(companies) != sorted(set(companies)):
        raise SnapshotError("apply report company_ids is not canonical")
    created = _canonical_ids(payload["created_job_ids"], "created_job_ids")
    canceled = _canonical_ids(payload["canceled_job_ids"], "canceled_job_ids")
    before = _canonical_ids(payload["scoped_outbox_ids_before"], "scoped_outbox_ids_before")
    after = _canonical_ids(payload["scoped_outbox_ids_after"], "scoped_outbox_ids_after")
    eligible = _non_negative_int(payload["eligible_created_rows"], "apply report eligible_created_rows")
    rows = _non_negative_int(payload["rows_in_scope"], "apply report rows_in_scope")
    already = _non_negative_int(payload["already_present_count"], "already_present_count")
    mutations = _non_negative_int(payload["mutation_count"], "mutation_count")
    created_count = _non_negative_int(payload["created_job_count"], "created_job_count")
    canceled_count = _non_negative_int(payload["canceled_job_count"], "canceled_job_count")
    marked = _canonical_ids(payload["marked_ledger_ids"], "marked_ledger_ids")
    already_marked = _canonical_ids(payload["already_marked_ledger_ids"], "already_marked_ledger_ids")
    marked_count = _non_negative_int(payload["marked_ledger_count"], "marked_ledger_count")
    already_marked_count = _non_negative_int(payload["already_marked_ledger_count"], "already_marked_ledger_count")
    if created_count != len(created) or canceled_count != len(canceled):
        raise SnapshotError("apply report job counts are inconsistent")
    if marked_count != len(marked) or already_marked_count != len(already_marked):
        raise SnapshotError("apply report marker counts are inconsistent")
    if set(marked) & set(already_marked):
        # A row cannot both have been stamped now and have carried the marker
        # already; one of the two lists has been edited.
        raise SnapshotError("apply report marker sets overlap")
    if mutations != created_count + canceled_count + marked_count:
        raise SnapshotError("apply report mutation count is inconsistent")
    report = ApplyReport(
        snapshot_version=payload["snapshot_version"],
        snapshot_digest=snapshot_digest,
        company_ids=companies,
        applied_at=_parse_timestamp(payload["applied_at"], "applied_at"),
        eligible_created_rows=eligible,
        rows_in_scope=rows,
        created_job_ids=created,
        canceled_job_ids=canceled,
        already_present_count=already,
        marked_ledger_ids=marked,
        already_marked_ledger_ids=already_marked,
        scoped_outbox_ids_before=before,
        scoped_outbox_ids_after=after,
    )
    if frozen is not None and (
        report.snapshot_version != frozen.version
        or report.snapshot_digest != frozen.digest
        or report.company_ids != frozen.company_ids
        or report.eligible_created_rows != frozen.eligible_created_rows
        or report.rows_in_scope != len(frozen.rows)
    ):
        raise SnapshotError("the apply report belongs to a different snapshot")
    if frozen is not None:
        obligations = sum(len(row["obligations"]) for row in frozen.rows)
        if len(report.created_job_ids) + report.already_present_count != obligations:
            raise SnapshotError("the apply report does not account for every obligation")
        # Every frozen row must appear in exactly one of the two marker lists.
        # A partial marker apply is the state that would leave some bookings
        # protected against a late Altegio delivery and others not.
        frozen_ledger_ids = {int(row["identity"]["ledger_id"]) for row in frozen.rows}
        if set(report.marked_ledger_ids) | set(report.already_marked_ledger_ids) != frozen_ledger_ids:
            raise SnapshotError("the apply report does not mark every frozen ledger row")
        expected_already = {
            int(row["identity"]["ledger_id"]) for row in frozen.rows if row["marker"]["action"] == MARKER_ALREADY
        }
        if set(report.already_marked_ledger_ids) != expected_already:
            raise SnapshotError("the apply report marker actions disagree with the snapshot")
    return report


def _canonical_ids(value: object, label: str) -> tuple[int, ...]:
    if not isinstance(value, list) or any(type(item) is not int or item <= 0 for item in value):
        raise SnapshotError(f"{label} is not an array of positive integers")
    if value != sorted(set(value)):
        raise SnapshotError(f"{label} contains duplicate or unsorted ids")
    return tuple(value)


@dataclass(frozen=True)
class FrozenPlan:
    """A snapshot read back, validated, and ready to be re-proven."""

    digest: str
    version: int
    created_at: datetime
    company_ids: tuple[int, ...]
    rows: tuple[dict[str, Any], ...]
    ledger_rows_seen: int
    eligible_created_rows: int
    historical_rows: dict[str, int]
    eligible_refusals: tuple[dict[str, Any], ...]
    guard_ready: bool
    coverage_ready: bool
    cutover_ready: bool

    def age_seconds(self, now: datetime) -> float:
        return (_as_utc(now) - _as_utc(self.created_at)).total_seconds()


def freeze_plan(plan: HandoverPlan) -> FrozenPlan:
    """Trusted in-process projection used by DB tests and callers before I/O."""
    material = plan._snapshot_material()
    readiness = material["readiness"]
    return FrozenPlan(
        digest=plan.digest(),
        version=SNAPSHOT_VERSION,
        created_at=_as_utc(plan.created_at),
        company_ids=tuple(sorted(plan.company_ids)),
        rows=tuple(material["rows"]),
        ledger_rows_seen=plan.ledger_rows_seen,
        eligible_created_rows=int(plan.eligible_created_rows or 0),
        historical_rows=dict(material["historical_rows"]),
        eligible_refusals=tuple(material["eligible_refusals"]),
        guard_ready=readiness["guard_ready"],
        coverage_ready=readiness["coverage_ready"],
        cutover_ready=readiness["cutover_ready"],
    )


def read_snapshot(path: str | Path) -> FrozenPlan:
    """Load a snapshot, or refuse. A damaged file never authorises anything."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise SnapshotError(f"cannot read the snapshot: {error.strerror}") from None
    try:
        payload = json.loads(raw)
    except Exception:
        raise SnapshotError("the snapshot is not valid JSON") from None
    if not isinstance(payload, dict) or payload.get("version") != SNAPSHOT_VERSION:
        raise SnapshotError("the snapshot has an unexpected version")
    expected_top = {
        "version",
        "mode",
        "created_at",
        "company_ids",
        "ledger_rows_seen",
        "eligible_created_rows",
        "historical_rows",
        "eligible_refusals",
        "rows",
        "obligation_outcomes",
        "readiness",
        "plan_digest",
    }
    _exact_keys(payload, expected_top, "snapshot")
    if payload["mode"] != "read-only":
        raise SnapshotError("the snapshot mode is invalid")

    created_at = _parse_timestamp(payload["created_at"], "created_at")
    companies = _positive_int_list(payload["company_ids"], "company_ids")
    if list(companies) != sorted(set(companies)) or not companies:
        raise SnapshotError("company_ids must be sorted, unique and non-empty")
    ledger_rows_seen = _non_negative_int(payload["ledger_rows_seen"], "ledger_rows_seen")
    eligible_count = _non_negative_int(payload["eligible_created_rows"], "eligible_created_rows")

    historical = payload["historical_rows"]
    if not isinstance(historical, dict) or not all(
        isinstance(key, str) and key and _is_non_negative_int(value) for key, value in historical.items()
    ):
        raise SnapshotError("historical_rows has an invalid shape")
    if dict(sorted(historical.items())) != historical:
        raise SnapshotError("historical_rows is not canonical")

    refusal_rows = payload["eligible_refusals"]
    if not isinstance(refusal_rows, list):
        raise SnapshotError("eligible_refusals is not an array")
    refusals = tuple(_validate_refusal(item) for item in refusal_rows)
    if [item["ledger_id"] for item in refusals] != sorted({item["ledger_id"] for item in refusals}):
        raise SnapshotError("eligible_refusals contains duplicate or unsorted identity")

    raw_rows = payload["rows"]
    if not isinstance(raw_rows, list):
        raise SnapshotError("rows is not an array")
    rows = tuple(_validate_row(item) for item in raw_rows)
    ledger_ids = [int(item["identity"]["ledger_id"]) for item in rows]
    if ledger_ids != sorted(set(ledger_ids)):
        raise SnapshotError("rows contains duplicate or unsorted identity")
    if set(ledger_ids) & {item["ledger_id"] for item in refusals}:
        raise SnapshotError("one eligible ledger row appears twice")
    if eligible_count != len(rows) + len(refusals):
        raise SnapshotError("eligible_created_rows does not match the frozen scope")
    if ledger_rows_seen != eligible_count + sum(historical.values()):
        raise SnapshotError("ledger_rows_seen does not match eligible and historical rows")

    outcomes = _obligation_counts(rows)
    if payload["obligation_outcomes"] != outcomes:
        raise SnapshotError("obligation_outcomes does not match the frozen obligations")
    readiness = payload["readiness"]
    _exact_keys(readiness, {"guard_ready", "coverage_ready", "cutover_ready"}, "readiness")
    if not all(type(readiness[key]) is bool for key in readiness):
        raise SnapshotError("readiness contains a non-boolean value")
    expected_readiness = _readiness(rows, refusals, eligible_count)
    if readiness != expected_readiness:
        raise SnapshotError("readiness does not match the frozen evidence")

    embedded = payload["plan_digest"]
    if not isinstance(embedded, str) or len(embedded) != 64:
        raise SnapshotError("the snapshot has no canonical plan digest")
    material = dict(payload)
    del material["plan_digest"]
    recomputed = _payload_digest(material)
    if embedded != recomputed:
        raise SnapshotError("the snapshot content does not match its plan digest")

    return FrozenPlan(
        digest=recomputed,
        version=SNAPSHOT_VERSION,
        created_at=created_at,
        company_ids=companies,
        rows=rows,
        ledger_rows_seen=ledger_rows_seen,
        eligible_created_rows=eligible_count,
        historical_rows=dict(historical),
        eligible_refusals=refusals,
        guard_ready=readiness["guard_ready"],
        coverage_ready=readiness["coverage_ready"],
        cutover_ready=readiness["cutover_ready"],
    )


def _exact_keys(value: object, expected: set[str], label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise SnapshotError(f"{label} has missing or unknown fields")


def _is_non_negative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _non_negative_int(value: object, label: str) -> int:
    if not _is_non_negative_int(value):
        raise SnapshotError(f"{label} is not a non-negative integer")
    return value


def _positive_int_list(value: object, label: str) -> tuple[int, ...]:
    if not isinstance(value, list) or any(type(item) is not int or item <= 0 for item in value):
        raise SnapshotError(f"{label} is not an array of positive integers")
    return tuple(value)


def _parse_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SnapshotError(f"{label} is not a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise SnapshotError(f"{label} is not a readable timestamp") from None
    if _timestamp(parsed) != value:
        raise SnapshotError(f"{label} is not canonical")
    return parsed


def _validate_refusal(value: object) -> dict[str, Any]:
    _exact_keys(value, {"ledger_id", "source_company_id", "source_record_id", "reason"}, "eligible refusal")
    assert isinstance(value, dict)
    for key in ("ledger_id", "source_company_id", "source_record_id"):
        if type(value[key]) is not int or value[key] <= 0:
            raise SnapshotError(f"eligible refusal {key} is invalid")
    if not isinstance(value["reason"], str) or not value["reason"]:
        raise SnapshotError("eligible refusal reason is invalid")
    return dict(value)


def _validate_row(value: object) -> dict[str, Any]:
    _exact_keys(
        value,
        {"identity", "obligations", "stale_source_job_ids", "processing_source_job_ids", "marker", "refusal"},
        "row",
    )
    assert isinstance(value, dict)
    if value["refusal"] is not None:
        raise SnapshotError("a scoped row cannot carry a refusal")
    identity = value["identity"]
    identity_keys = {
        "ledger_id",
        "source_company_id",
        "source_record_id",
        "source_record_pk",
        "target_record_pk",
        "target_company_id",
        "target_booking_uuid",
        "target_starts_at",
        "target_is_canceled",
        "target_is_completed",
    }
    _exact_keys(identity, identity_keys, "row identity")
    assert isinstance(identity, dict)
    for key in identity_keys - {
        "target_booking_uuid",
        "target_starts_at",
        "target_is_canceled",
        "target_is_completed",
    }:
        if type(identity[key]) is not int or identity[key] <= 0:
            raise SnapshotError(f"row identity {key} is invalid")
    if type(identity["target_is_canceled"]) is not bool or type(identity["target_is_completed"]) is not bool:
        raise SnapshotError("row target terminal flags are invalid")
    if identity["target_is_canceled"] and identity["target_is_completed"]:
        raise SnapshotError("row target terminal flags contradict each other")
    booking_uuid = canonical_uuid(identity["target_booking_uuid"])
    if booking_uuid is None or str(booking_uuid) != identity["target_booking_uuid"]:
        raise SnapshotError("row target_booking_uuid is not canonical")
    starts_at = _parse_timestamp(identity["target_starts_at"], "target_starts_at")

    stale = _positive_int_list(value["stale_source_job_ids"], "stale_source_job_ids")
    processing = _positive_int_list(value["processing_source_job_ids"], "processing_source_job_ids")
    if list(stale) != sorted(set(stale)) or list(processing) != sorted(set(processing)) or set(stale) & set(processing):
        raise SnapshotError("source job ids are duplicate, unsorted or overlap")

    raw_obligations = value["obligations"]
    if not isinstance(raw_obligations, list):
        raise SnapshotError("obligations is not an array")
    obligations = [
        _validate_obligation(item, booking_uuid=booking_uuid, starts_at=starts_at) for item in raw_obligations
    ]
    order = [(item["run_at"], item["job_type"]) for item in obligations]
    if order != sorted(set(order)):
        raise SnapshotError("obligations are duplicate or not canonical")

    return {
        "identity": dict(identity),
        "obligations": obligations,
        "stale_source_job_ids": list(stale),
        "processing_source_job_ids": list(processing),
        "marker": _validate_marker(value["marker"]),
        "refusal": None,
    }


def _validate_marker(value: object) -> dict[str, Any]:
    """The row's expected ownership-marker state, or refuse the snapshot.

    Strict in both directions. ``set`` means the plan saw no marker and the
    apply will write one, so it must carry neither a digest nor an instant;
    ``already_handed_over`` means the plan read an existing one, so it must
    carry both. A half-stated expectation would let an apply either re-mark a
    row somebody else had already handed over, or skip marking one that needs it.
    """
    _exact_keys(value, {"action", "existing_digest", "handed_over_at"}, "row marker")
    assert isinstance(value, dict)
    action = value["action"]
    if action not in (MARKER_SET, MARKER_ALREADY):
        raise SnapshotError("row marker action is unknown")

    digest = value["existing_digest"]
    handed_over_at = value["handed_over_at"]
    if action == MARKER_SET:
        if digest is not None or handed_over_at is not None:
            raise SnapshotError("a row to be marked cannot already carry a marker")
        return {"action": action, "existing_digest": None, "handed_over_at": None}

    if not isinstance(digest, str) or len(digest) != 64 or not all(c in "0123456789abcdef" for c in digest):
        raise SnapshotError("row marker existing_digest is not a digest")
    _parse_timestamp(handed_over_at, "row marker handed_over_at")
    return {"action": action, "existing_digest": digest, "handed_over_at": handed_over_at}


def _validate_obligation(value: object, *, booking_uuid: uuid_module.UUID, starts_at: datetime) -> dict[str, Any]:
    expected_keys = {"job_type", "run_at", "dedupe_key", "outcome", "existing_job_id", "existing_job_status"}
    _exact_keys(value, expected_keys, "obligation")
    assert isinstance(value, dict)
    job_type = value["job_type"]
    if job_type not in EASYWEEK_REMINDER_JOB_TYPES:
        raise SnapshotError("obligation job_type is unknown")
    run_at = _parse_timestamp(value["run_at"], "obligation run_at")
    if run_at != starts_at - REMINDER_OFFSETS[job_type]:
        raise SnapshotError("obligation run_at does not match job_type and booking start")
    expected_key = easyweek_reminder_dedupe_key(
        booking_uuid=booking_uuid,
        job_type=job_type,
        starts_at=starts_at,
    )
    if value["dedupe_key"] != expected_key:
        raise SnapshotError("obligation dedupe_key is not canonical")
    outcome = value["outcome"]
    if outcome not in _OBLIGATION_OUTCOMES:
        raise SnapshotError("obligation outcome is unknown")
    existing_id = value["existing_job_id"]
    existing_status = value["existing_job_status"]
    if outcome == OBLIGATION_MISSING:
        if existing_id is not None or existing_status is not None:
            raise SnapshotError("missing obligation cannot have an existing job")
    else:
        if type(existing_id) is not int or existing_id <= 0 or not isinstance(existing_status, str):
            raise SnapshotError("existing obligation has no job identity/status")
        expected_status = _OUTCOME_STATUS.get(outcome)
        if expected_status is not None and existing_status != expected_status:
            raise SnapshotError("existing obligation status contradicts its outcome")
        if outcome == OBLIGATION_OCCUPIED_UNKNOWN and existing_status in _OUTCOME_STATUS.values():
            raise SnapshotError("unknown obligation status is actually known")
    return dict(value)


def _obligation_counts(rows: tuple[dict[str, Any], ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for item in row["obligations"]:
            outcome = item["outcome"]
            counts[outcome] = counts.get(outcome, 0) + 1
    return dict(sorted(counts.items()))


def _readiness(
    rows: tuple[dict[str, Any], ...],
    refusals: tuple[dict[str, Any], ...],
    eligible_count: int,
) -> dict[str, bool]:
    obligations = [item for row in rows for item in row["obligations"]]
    blocked_outcomes = {
        OBLIGATION_OCCUPIED_CANCELED,
        OBLIGATION_OCCUPIED_FAILED,
        OBLIGATION_OCCUPIED_UNKNOWN,
    }
    blocker = any(item["outcome"] in blocked_outcomes for item in obligations)
    processing = any(row["processing_source_job_ids"] for row in rows)
    guard = not blocker and not refusals
    return {
        "guard_ready": guard,
        "coverage_ready": guard and not any(item["outcome"] == OBLIGATION_MISSING for item in obligations),
        "cutover_ready": bool(eligible_count) and eligible_count == len(rows) and guard and not processing,
    }


def confirmation_phrase(digest: str) -> str:
    return CONFIRMATION_TEMPLATE.format(digest=digest)


def check_snapshot_usable(
    frozen: FrozenPlan,
    *,
    supplied_digest: str | None,
    supplied_confirmation: str | None,
    now: datetime,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> None:
    """Both permission gates and the age check, or a full stop.

    The digest and the phrase are separate gates on purpose. The digest proves
    the operator is authorising THIS plan; the phrase proves a person typed
    something deliberate rather than a flag surviving in shell history.
    """
    if not supplied_digest:
        raise SnapshotError("apply needs --plan-digest, taken from the plan report")
    if supplied_digest != frozen.digest:
        raise SnapshotError("the supplied plan digest does not match the snapshot; re-run plan")
    expected = confirmation_phrase(frozen.digest)
    if supplied_confirmation != expected:
        raise SnapshotError("the confirmation phrase does not match this plan; re-run plan and read the report")

    age = frozen.age_seconds(now)
    if age < 0:
        raise SnapshotError("the snapshot claims to be from the future; re-run plan")
    if age > max_age_sec:
        # Obligations move with the clock. A plan old enough that a two-hour
        # reminder has fallen past its moment is a plan about a different world.
        raise SnapshotError(f"the snapshot is {int(age)}s old (limit {max_age_sec}s); re-run plan")
    if not frozen.cutover_ready:
        raise SnapshotError("the frozen plan is not cutover-ready")
    if frozen.eligible_refusals or frozen.eligible_created_rows != len(frozen.rows) or not frozen.rows:
        raise SnapshotError("the frozen plan does not prove the complete eligible scope")


def boundary_still_future(rows: tuple[dict[str, Any], ...], *, now: datetime) -> str | None:
    """Refuse if any planned reminder has crossed its moment since the plan.

    Inserting a reminder whose ``run_at`` is already past would queue a message
    the worker either refuses as expired or, worse, sends late — telling a
    customer to come to something they are already at.
    """
    current = _as_utc(now)
    for row in rows:
        for item in row.get("obligations") or ():
            if not isinstance(item, dict) or item.get("outcome") != OBLIGATION_MISSING:
                continue
            try:
                run_at = _as_utc(datetime.fromisoformat(str(item.get("run_at")).replace("Z", "+00:00")))
            except ValueError:
                return "unreadable_run_at"
            if run_at <= current:
                return "reminder_boundary_passed"
    return None


def frozen_scope_identities(rows: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    return [row.get("identity") or {} for row in rows]


__all__ = [
    "CANCEL_REASON",
    "CONFIRMATION_TEMPLATE",
    "DEFAULT_MAX_SNAPSHOT_AGE_SEC",
    "EASYWEEK_REMINDER_JOB_TYPES",
    "OPEN_STATUSES",
    "PROVIDER_ALTEGIO",
    "PROVIDER_EASYWEEK",
    "SNAPSHOT_VERSION",
    "COVERING_STATUSES",
    "EligibleRefusal",
    "ApplyReport",
    "APPLY_REPORT_VERSION",
    "FrozenPlan",
    "HandoverPlan",
    "HandoverRow",
    "Obligation",
    "SnapshotError",
    "boundary_still_future",
    "canonical_uuid",
    "check_snapshot_usable",
    "confirmation_phrase",
    "freeze_plan",
    "insert_values",
    "obligations_for",
    "read_snapshot",
    "read_apply_report",
    "write_apply_report",
    "write_snapshot",
]
