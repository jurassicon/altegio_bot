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
    PlannedReminder,
    plan_reminders,
    reminder_job_payload,
)
from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK

SNAPSHOT_VERSION: Final = 1
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

# Statuses an open reminder can hold. `processing` counts as open: a job the
# worker claimed a second ago is still going to fire.
OPEN_STATUSES: Final = ("queued", "processing")


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
    obligations: tuple[Obligation, ...] = ()
    # Altegio reminder job ids that are still queued for the source booking.
    stale_source_job_ids: tuple[int, ...] = ()
    # Source reminder jobs the worker has already claimed. One of these stops
    # the whole apply: a claimed job may be mid-flight to Meta.
    processing_source_job_ids: tuple[int, ...] = ()
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


@dataclass
class HandoverPlan:
    """Everything one ``plan`` run proved, and what an apply would do."""

    company_ids: tuple[int, ...]
    created_at: datetime
    rows: tuple[HandoverRow, ...] = ()
    # Ledger rows that never entered the wave, by reason. Counts only.
    refused: dict[str, int] = field(default_factory=dict)
    ledger_rows_seen: int = 0

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
        return not self.blocked_rows

    @property
    def coverage_ready(self) -> bool:
        """Does every obligation already exist? True only after a good apply."""
        return self.guard_ready and self.to_create == 0

    @property
    def cutover_ready(self) -> bool:
        """May ownership be switched atomically right now?"""
        return self.guard_ready and not self.processing_rows

    def digest(self) -> str:
        """Identity of exactly what an apply would do.

        Covers the frozen scope and every action. Deliberately excludes
        ``created_at`` — the age is checked separately and explicitly, and
        folding a timestamp in would make the digest unquotable.
        """
        material = json.dumps(
            {
                "version": SNAPSHOT_VERSION,
                "company_ids": sorted(self.company_ids),
                "rows": sorted(
                    (
                        {
                            "identity": row.identity(),
                            "create": sorted(item.dedupe_key for item in row.missing),
                            "cancel": sorted(row.stale_source_job_ids),
                        }
                        for row in self.scoped
                    ),
                    key=lambda item: item["identity"]["ledger_id"],
                ),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

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
            "created_at": _as_utc(self.created_at).isoformat().replace("+00:00", "Z"),
            "plan_digest": self.digest(),
            "ledger_rows_seen": self.ledger_rows_seen,
            "rows_in_scope": len(self.scoped),
            "rows_refused": dict(sorted(self.refused.items())),
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
        payload = self.as_safe_dict()
        payload["rows"] = [row.as_safe_dict() for row in self.scoped]
        return payload


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
    return target


class SnapshotError(Exception):
    """The snapshot cannot authorise an apply. Always a full stop."""


@dataclass(frozen=True)
class FrozenPlan:
    """A snapshot read back, validated, and ready to be re-proven."""

    digest: str
    created_at: datetime
    company_ids: tuple[int, ...]
    rows: tuple[dict[str, Any], ...]

    def age_seconds(self, now: datetime) -> float:
        return (_as_utc(now) - _as_utc(self.created_at)).total_seconds()


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

    digest = payload.get("plan_digest")
    created = payload.get("created_at")
    rows = payload.get("rows")
    companies = payload.get("company_ids")
    if not isinstance(digest, str) or not digest:
        raise SnapshotError("the snapshot has no plan digest")
    if not isinstance(rows, list) or not isinstance(companies, list):
        raise SnapshotError("the snapshot has no frozen scope")
    try:
        created_at = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
    except ValueError:
        raise SnapshotError("the snapshot has no readable creation time") from None

    return FrozenPlan(
        digest=digest,
        created_at=_as_utc(created_at),
        company_ids=tuple(int(item) for item in companies),
        rows=tuple(row for row in rows if isinstance(row, dict)),
    )


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
    "FrozenPlan",
    "HandoverPlan",
    "HandoverRow",
    "Obligation",
    "SnapshotError",
    "boundary_still_future",
    "canonical_uuid",
    "check_snapshot_usable",
    "confirmation_phrase",
    "insert_values",
    "obligations_for",
    "read_snapshot",
    "write_snapshot",
]
