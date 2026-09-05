"""The frozen artefact of the post-booking marketing handover (plan §31.5).

What this phase is
------------------
After a booking has been migrated and its reminders handed over (§30), three
record-bound marketing jobs are still Altegio's: `review_3d`, `repeat_10d` and
`comeback_3d`. Each was planned from an Altegio booking and would be sent with
Altegio's template, sender and booking link — for an appointment that lives in
EasyWeek now.

This phase withdraws them and writes down that the Altegio side gave them up. It
creates NOTHING in their place: a migrated future booking is not evidence of a
completed visit, so only a proven `booking-succeeded` may ever create an
EasyWeek `review_3d` or `repeat_10d`, and only a proven cancellation may create
a `comeback_3d`.

Why a separate artefact from §30
--------------------------------
The reminder snapshot authorises a different transaction: it creates EasyWeek
obligations and cancels Altegio ones. This one cancels only, and it writes a
different marker. Sharing a digest, a phrase or a permission between them would
let a review of one authorise the other, which is exactly the confusion the two
markers exist to prevent. Nothing here is derived from a §30 snapshot; the two
files can sit side by side and neither can stand in for the other.

What is in the file
-------------------
Ids, counts, statuses, digests and instants. No phone, no name, no template, no
URL, no payload, no provider text. It names real bookings by their ids, so it is
written 0600 into a 0700 directory, never committed, and destroyed the moment a
new plan attempt supersedes it.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

# Its own version line. A §30 snapshot can never be read as this one and the
# other way round: the top-level key set differs and both readers are exact.
SNAPSHOT_VERSION: Final = 1
SNAPSHOT_MODE: Final = 0o600
DIR_MODE: Final = 0o700
# One hour, same reasoning as §30: a plan describes a set of open jobs, and a
# stale one describes a world that has moved.
DEFAULT_MAX_SNAPSHOT_AGE_SEC: Final = 3600

# The three record-bound marketing types this phase owns. Imported by the DB
# side and by the tests; the runtime fences carry their own copy so a runtime
# path never depends on an operator module.
POST_BOOKING_JOB_TYPES: Final[tuple[str, ...]] = ("comeback_3d", "repeat_10d", "review_3d")

# Job statuses that mean "still going to happen".
OPEN_JOB_STATUSES: Final[frozenset[str]] = frozenset({"queued", "processing"})
# Outbox statuses that mean "a message may still be in flight". Anything else is
# history: sent, delivered, read, failed, canceled.
NON_TERMINAL_OUTBOX_STATUSES: Final[frozenset[str]] = frozenset({"queued", "sending", "unknown"})

# Its own phrase, naming its own action. An operator who pastes the reminder
# handover's phrase here is refused, and the other way round.
CONFIRMATION_TEMPLATE: Final = "withdraw altegio post-booking jobs {digest}"

# Stable, PII-free STOP codes.
STOP_MANIFEST_SCOPE: Final = "manifest_scope_invalid"
STOP_RUN_SCOPE: Final = "migration_run_scope_invalid"
STOP_PLAN_TRANSACTION: Final = "plan_requires_fresh_transaction"
STOP_REMINDER_HANDOVER_INCOMPLETE: Final = "reminder_handover_incomplete"
STOP_WAVE_NOT_CLOSED: Final = "migration_wave_not_closed"
STOP_WAVE_UNRESOLVED: Final = "migration_wave_unresolved"
STOP_LEDGER_SCOPE_CHANGED: Final = "ledger_scope_changed"
STOP_SOURCE_RECORD: Final = "source_record_unproven"
STOP_TARGET_RECORD: Final = "target_record_unproven"
STOP_SOURCE_PROCESSING: Final = "source_job_processing"
STOP_NON_TERMINAL_OUTBOX: Final = "source_job_outbox_non_terminal"
STOP_SOURCE_JOB_SET_CHANGED: Final = "source_job_set_changed"
STOP_TARGET_JOB_SET_CHANGED: Final = "target_job_set_changed"
STOP_MARKER_CONFLICT: Final = "post_booking_marker_conflict"
STOP_SNAPSHOT_INVALIDATED: Final = "snapshot_invalidated"
STOP_SNAPSHOT_EMPTY: Final = "snapshot_scope_empty"
STOP_CONFIGURATION_CHANGED: Final = "configuration_changed"

TOMBSTONE_MODE: Final = "invalidated"


class PostBookingSnapshotError(Exception):
    """The snapshot cannot authorise an apply. Always a full stop."""


def _timestamp(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _canonical(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _exact_keys(payload: dict[str, Any], expected: set[str], what: str) -> None:
    if set(payload) != expected:
        # Exact, not "at least": an unknown key means the file was written by
        # something else, and a missing one means a check silently did not run.
        raise PostBookingSnapshotError(f"the {what} has an unexpected shape")


@dataclass(frozen=True)
class JobRef:
    """One message job, by identity and state. Never its payload."""

    job_id: int
    job_type: str
    status: str

    def as_safe_dict(self) -> dict[str, Any]:
        return {"job_id": self.job_id, "job_type": self.job_type, "status": self.status}


@dataclass(frozen=True)
class ScopeRow:
    """One ledger row this handover would act on."""

    ledger_id: int
    source_company_id: int
    source_record_id: int
    source_record_pk: int
    target_record_pk: int
    target_company_id: int
    target_booking_uuid: str
    run_id: str
    # §30 must already be finished for this row, and its digest is frozen so a
    # later ownership change is visible as drift rather than silently accepted.
    reminder_handover_digest: str
    # This phase's own marker, when a previous apply already wrote one.
    post_booking_digest: str | None
    source_jobs: tuple[JobRef, ...]
    target_jobs: tuple[JobRef, ...]
    non_terminal_outbox_ids: tuple[int, ...]

    @property
    def queued_source_job_ids(self) -> tuple[int, ...]:
        return tuple(sorted(job.job_id for job in self.source_jobs if job.status == "queued"))

    @property
    def processing_source_job_ids(self) -> tuple[int, ...]:
        return tuple(sorted(job.job_id for job in self.source_jobs if job.status == "processing"))

    @property
    def overlapping_types(self) -> tuple[str, ...]:
        """Types held open on BOTH sides for the same booking.

        Reported rather than refused: the production audit found exactly one
        such pair, and it is the correct end state of this phase — the EasyWeek
        job stays, the Altegio one goes.
        """
        source = {job.job_type for job in self.source_jobs if job.status in OPEN_JOB_STATUSES}
        target = {job.job_type for job in self.target_jobs if job.status in OPEN_JOB_STATUSES}
        return tuple(sorted(source & target))

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ledger_id": self.ledger_id,
            "source_company_id": self.source_company_id,
            "source_record_id": self.source_record_id,
            "source_record_pk": self.source_record_pk,
            "target_record_pk": self.target_record_pk,
            "target_company_id": self.target_company_id,
            "target_booking_uuid": self.target_booking_uuid,
            "run_id": self.run_id,
            "reminder_handover_digest": self.reminder_handover_digest,
            "post_booking_digest": self.post_booking_digest,
            "source_jobs": [job.as_safe_dict() for job in sorted(self.source_jobs, key=lambda j: j.job_id)],
            "target_jobs": [job.as_safe_dict() for job in sorted(self.target_jobs, key=lambda j: j.job_id)],
            "non_terminal_outbox_ids": list(self.non_terminal_outbox_ids),
        }


@dataclass
class PostBookingPlan:
    """Everything one `plan` proved, and what an apply would do."""

    company_ids: tuple[int, ...]
    run_ids: tuple[str, ...]
    created_at: datetime
    manifest_digest: str
    configuration_digest: str
    rows: tuple[ScopeRow, ...] = ()
    # Ledger rows in the selected scope that could NOT be taken, by stable code.
    refusals: dict[str, int] = field(default_factory=dict)
    ledger_rows_seen: int = 0
    eligible_created_rows: int = 0
    # Statuses in the wave that could still become `created`. Any of them is a
    # full stop: §30 already refuses such a wave, and a row that becomes
    # `created` after this handover would carry no marker at all.
    unresolved_rows: dict[str, int] = field(default_factory=dict)
    wave_closed: bool = False

    @property
    def source_queued(self) -> int:
        return sum(len(row.queued_source_job_ids) for row in self.rows)

    @property
    def source_processing(self) -> int:
        return sum(len(row.processing_source_job_ids) for row in self.rows)

    @property
    def source_terminal(self) -> int:
        return sum(1 for row in self.rows for job in row.source_jobs if job.status not in OPEN_JOB_STATUSES)

    @property
    def source_with_non_terminal_outbox(self) -> int:
        return sum(len(row.non_terminal_outbox_ids) for row in self.rows)

    @property
    def target_jobs_present(self) -> int:
        return sum(len(row.target_jobs) for row in self.rows)

    @property
    def rows_without_source_job(self) -> int:
        return sum(1 for row in self.rows if not row.source_jobs)

    @property
    def rows_already_marked(self) -> int:
        return sum(1 for row in self.rows if row.post_booking_digest)

    @property
    def overlapping_rows(self) -> int:
        return sum(1 for row in self.rows if row.overlapping_types)

    @property
    def blockers(self) -> tuple[str, ...]:
        """Everything that makes this plan unable to authorise an apply."""
        found: list[str] = []
        if not self.rows:
            found.append(STOP_SNAPSHOT_EMPTY)
        if self.refusals:
            found.append(STOP_LEDGER_SCOPE_CHANGED)
        if self.unresolved_rows:
            found.append(STOP_WAVE_UNRESOLVED)
        if not self.wave_closed:
            found.append(STOP_WAVE_NOT_CLOSED)
        if self.source_processing:
            found.append(STOP_SOURCE_PROCESSING)
        if self.source_with_non_terminal_outbox:
            found.append(STOP_NON_TERMINAL_OUTBOX)
        return tuple(sorted(set(found)))

    @property
    def apply_ready(self) -> bool:
        """May the withdrawal be authorised right now?

        A zero, partial or unproven wave never is. Note that zero jobs to cancel
        is NOT a blocker: a wave whose rows all lack a marketing job still needs
        the marker, or a late Altegio delivery would create the first one after
        the handover.
        """
        return not self.blockers

    def _snapshot_material(self) -> dict[str, Any]:
        return {
            "version": SNAPSHOT_VERSION,
            "mode": "read-only",
            "kind": "post_booking_handover",
            "created_at": _timestamp(self.created_at),
            "company_ids": sorted(self.company_ids),
            "wave": {
                "run_ids": list(self.run_ids),
                "manifest_digest": self.manifest_digest,
                "configuration_digest": self.configuration_digest,
                "wave_closed": self.wave_closed,
                "unresolved_rows": dict(sorted(self.unresolved_rows.items())),
            },
            "ledger_rows_seen": self.ledger_rows_seen,
            "eligible_created_rows": self.eligible_created_rows,
            "refusals": dict(sorted(self.refusals.items())),
            "rows": [row.as_safe_dict() for row in sorted(self.rows, key=lambda item: item.ledger_id)],
        }

    def digest(self) -> str:
        """Identity of exactly what an apply would do, `created_at` included."""
        return _payload_digest(self._snapshot_material())

    def as_safe_dict(self) -> dict[str, Any]:
        """The operator's report. Counts, codes and ids — nothing else."""
        return {
            "version": SNAPSHOT_VERSION,
            "kind": "post_booking_handover",
            "mode": "read-only",
            "created_at": _timestamp(self.created_at),
            "plan_digest": self.digest(),
            "company_ids": sorted(self.company_ids),
            "run_ids": list(self.run_ids),
            "ledger_rows_seen": self.ledger_rows_seen,
            "eligible_created_rows": self.eligible_created_rows,
            "rows_in_scope": len(self.rows),
            "refusals": dict(sorted(self.refusals.items())),
            "unresolved_rows": dict(sorted(self.unresolved_rows.items())),
            "wave_closed": self.wave_closed,
            "source_jobs_queued": self.source_queued,
            "source_jobs_processing": self.source_processing,
            "source_jobs_terminal": self.source_terminal,
            "source_jobs_with_non_terminal_outbox": self.source_with_non_terminal_outbox,
            "target_easyweek_jobs_present": self.target_jobs_present,
            "rows_without_source_job": self.rows_without_source_job,
            "rows_already_marked": self.rows_already_marked,
            "rows_with_source_target_overlap": self.overlapping_rows,
            "blockers": list(self.blockers),
            "apply_ready": self.apply_ready,
        }

    def to_snapshot(self) -> dict[str, Any]:
        payload = self._snapshot_material()
        payload["plan_digest"] = self.digest()
        return payload


@dataclass(frozen=True)
class FrozenPostBookingPlan:
    """A snapshot read back, validated, and ready to be re-proven."""

    digest: str
    created_at: datetime
    company_ids: tuple[int, ...]
    run_ids: tuple[str, ...]
    manifest_digest: str
    configuration_digest: str
    wave_closed: bool
    unresolved_rows: dict[str, int]
    ledger_rows_seen: int
    eligible_created_rows: int
    refusals: dict[str, int]
    rows: tuple[dict[str, Any], ...]

    def age_seconds(self, now: datetime) -> float:
        return (_as_utc(now) - _as_utc(self.created_at)).total_seconds()

    @property
    def ledger_ids(self) -> tuple[int, ...]:
        return tuple(sorted(int(row["ledger_id"]) for row in self.rows))


def freeze_plan(plan: PostBookingPlan) -> FrozenPostBookingPlan:
    """In-process projection, used by tests and by callers before any file IO."""
    material = plan._snapshot_material()
    return FrozenPostBookingPlan(
        digest=plan.digest(),
        created_at=_as_utc(plan.created_at),
        company_ids=tuple(sorted(plan.company_ids)),
        run_ids=tuple(plan.run_ids),
        manifest_digest=plan.manifest_digest,
        configuration_digest=plan.configuration_digest,
        wave_closed=plan.wave_closed,
        unresolved_rows=dict(material["wave"]["unresolved_rows"]),
        ledger_rows_seen=plan.ledger_rows_seen,
        eligible_created_rows=plan.eligible_created_rows,
        refusals=dict(material["refusals"]),
        rows=tuple(material["rows"]),
    )


# ---------------------------------------------------------------------------
# private artefact IO
# ---------------------------------------------------------------------------


def _atomic_write(target: Path, payload: dict[str, Any]) -> Path:
    """0600 into a 0700 directory, fsynced, then replaced into place."""
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(target.parent, DIR_MODE)
    tmp = target.with_suffix(target.suffix + ".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, SNAPSHOT_MODE)
    try:
        os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
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


def write_snapshot(plan: PostBookingPlan, path: str | Path) -> Path:
    """Freeze the plan to disk, atomically and privately."""
    return _atomic_write(Path(path), plan.to_snapshot())


def invalidate_snapshot(path: str | Path, *, reason: str, now: datetime | None = None) -> Path | None:
    """Destroy an authorisation in place, leaving PII-free evidence it existed.

    The same rule as §30 and for the same reason: renaming leaves the
    authorising bytes readable, so a superseded plan could still be applied.
    The file is OVERWRITTEN by a tombstone no reader accepts, and there is no
    second copy to rename back.
    """
    target = Path(path)
    if not target.exists():
        return None
    moment = now or datetime.now(timezone.utc)
    return _atomic_write(
        target,
        {
            "version": SNAPSHOT_VERSION,
            "kind": "post_booking_handover",
            "mode": TOMBSTONE_MODE,
            "invalidated_at": _timestamp(moment),
            "reason": reason,
        },
    )


def _validate_job(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise PostBookingSnapshotError("a job entry is not an object")
    _exact_keys(payload, {"job_id", "job_type", "status"}, "job entry")
    if type(payload["job_id"]) is not int or payload["job_id"] <= 0:
        raise PostBookingSnapshotError("a job id is not a positive integer")
    if payload["job_type"] not in POST_BOOKING_JOB_TYPES:
        raise PostBookingSnapshotError("a job entry names a type this phase does not own")
    if not isinstance(payload["status"], str) or not payload["status"]:
        raise PostBookingSnapshotError("a job status is not a string")
    return payload


def _validate_row(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise PostBookingSnapshotError("a row is not an object")
    _exact_keys(
        payload,
        {
            "ledger_id",
            "source_company_id",
            "source_record_id",
            "source_record_pk",
            "target_record_pk",
            "target_company_id",
            "target_booking_uuid",
            "run_id",
            "reminder_handover_digest",
            "post_booking_digest",
            "source_jobs",
            "target_jobs",
            "non_terminal_outbox_ids",
        },
        "row",
    )
    for key in (
        "ledger_id",
        "source_company_id",
        "source_record_id",
        "source_record_pk",
        "target_record_pk",
        "target_company_id",
    ):
        if type(payload[key]) is not int or payload[key] <= 0:
            raise PostBookingSnapshotError(f"{key} is not a positive integer")
    if not isinstance(payload["target_booking_uuid"], str) or not payload["target_booking_uuid"]:
        raise PostBookingSnapshotError("target_booking_uuid is missing")
    if not isinstance(payload["run_id"], str) or not payload["run_id"]:
        raise PostBookingSnapshotError("run_id is missing")
    if not isinstance(payload["reminder_handover_digest"], str) or len(payload["reminder_handover_digest"]) != 64:
        # §30 is a prerequisite, not an option. A row without its digest is a
        # row whose reminders may still be Altegio's.
        raise PostBookingSnapshotError("reminder_handover_digest is not a sha256 digest")
    if payload["post_booking_digest"] is not None and (
        not isinstance(payload["post_booking_digest"], str) or len(payload["post_booking_digest"]) != 64
    ):
        raise PostBookingSnapshotError("post_booking_digest is not a sha256 digest")
    for key in ("source_jobs", "target_jobs"):
        if not isinstance(payload[key], list):
            raise PostBookingSnapshotError(f"{key} is not an array")
        for item in payload[key]:
            _validate_job(item)
    outbox = payload["non_terminal_outbox_ids"]
    if not isinstance(outbox, list) or any(type(item) is not int for item in outbox):
        raise PostBookingSnapshotError("non_terminal_outbox_ids is not an array of integers")
    return payload


def read_snapshot(path: str | Path) -> FrozenPostBookingPlan:
    """Load a snapshot, or refuse. A damaged file never authorises anything."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise PostBookingSnapshotError(f"cannot read the snapshot: {error.strerror}") from None
    try:
        payload = json.loads(raw)
    except Exception:
        raise PostBookingSnapshotError("the snapshot is not valid JSON") from None
    if isinstance(payload, dict) and payload.get("mode") == TOMBSTONE_MODE:
        raise PostBookingSnapshotError(STOP_SNAPSHOT_INVALIDATED)
    if not isinstance(payload, dict):
        raise PostBookingSnapshotError("the snapshot is not an object")
    if payload.get("kind") != "post_booking_handover":
        # A reminder-handover snapshot is a different authorisation for a
        # different transaction. It must never be readable here.
        raise PostBookingSnapshotError("the snapshot is not a post-booking handover plan")
    if payload.get("version") != SNAPSHOT_VERSION:
        raise PostBookingSnapshotError("the snapshot has an unexpected version")

    _exact_keys(
        payload,
        {
            "version",
            "mode",
            "kind",
            "created_at",
            "company_ids",
            "wave",
            "ledger_rows_seen",
            "eligible_created_rows",
            "refusals",
            "rows",
            "plan_digest",
        },
        "snapshot",
    )
    if payload["mode"] != "read-only":
        raise PostBookingSnapshotError("the snapshot mode is invalid")
    wave = payload["wave"]
    _exact_keys(
        wave,
        {"run_ids", "manifest_digest", "configuration_digest", "wave_closed", "unresolved_rows"},
        "wave",
    )
    for key in ("manifest_digest", "configuration_digest"):
        if not isinstance(wave[key], str) or len(wave[key]) != 64:
            raise PostBookingSnapshotError(f"{key} is not a sha256 digest")
    if type(wave["wave_closed"]) is not bool:
        raise PostBookingSnapshotError("wave_closed is not boolean")
    if not isinstance(wave["run_ids"], list) or not wave["run_ids"]:
        raise PostBookingSnapshotError("run_ids is empty")
    if not isinstance(wave["unresolved_rows"], dict):
        raise PostBookingSnapshotError("unresolved_rows has an invalid shape")

    companies = payload["company_ids"]
    if not isinstance(companies, list) or not companies or list(companies) != sorted(set(companies)):
        raise PostBookingSnapshotError("company_ids must be sorted, unique and non-empty")
    rows = payload["rows"]
    if not isinstance(rows, list):
        raise PostBookingSnapshotError("rows is not an array")
    for row in rows:
        _validate_row(row)
    ledger_ids = [row["ledger_id"] for row in rows]
    if ledger_ids != sorted(set(ledger_ids)):
        raise PostBookingSnapshotError("rows contain duplicate or unsorted ledger ids")

    material = {key: value for key, value in payload.items() if key != "plan_digest"}
    if _payload_digest(material) != payload["plan_digest"]:
        # The digest covers everything else in the file, so an edited row, an
        # edited count or an edited clock all land here.
        raise PostBookingSnapshotError("the snapshot digest does not match its contents")

    created_at = payload["created_at"]
    if not isinstance(created_at, str):
        raise PostBookingSnapshotError("created_at is not a timestamp")
    try:
        moment = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError:
        raise PostBookingSnapshotError("created_at is not a timestamp") from None

    return FrozenPostBookingPlan(
        digest=str(payload["plan_digest"]),
        created_at=_as_utc(moment),
        company_ids=tuple(int(item) for item in companies),
        run_ids=tuple(str(item) for item in wave["run_ids"]),
        manifest_digest=str(wave["manifest_digest"]),
        configuration_digest=str(wave["configuration_digest"]),
        wave_closed=bool(wave["wave_closed"]),
        unresolved_rows=dict(wave["unresolved_rows"]),
        ledger_rows_seen=int(payload["ledger_rows_seen"]),
        eligible_created_rows=int(payload["eligible_created_rows"]),
        refusals=dict(payload["refusals"]),
        rows=tuple(rows),
    )


def confirmation_phrase(digest: str) -> str:
    return CONFIRMATION_TEMPLATE.format(digest=digest)


def check_snapshot_usable(
    frozen: FrozenPostBookingPlan,
    *,
    supplied_digest: str | None,
    supplied_confirmation: str | None,
    now: datetime,
    max_age_sec: int = DEFAULT_MAX_SNAPSHOT_AGE_SEC,
) -> None:
    """Both permission gates and the age check, or a full stop.

    The digest and the phrase are separate gates on purpose: the digest proves
    the operator is authorising THIS plan, the phrase proves a person typed
    something deliberate rather than a flag surviving in shell history. Neither
    is the §30 phrase, so one authorisation can never be pasted into the other.
    """
    if not supplied_digest:
        raise PostBookingSnapshotError("apply needs --plan-digest, taken from the plan report")
    if supplied_digest != frozen.digest:
        raise PostBookingSnapshotError("the supplied plan digest does not match the snapshot; re-run plan")
    if supplied_confirmation != confirmation_phrase(frozen.digest):
        raise PostBookingSnapshotError("the confirmation phrase does not match this plan; re-run plan")

    age = frozen.age_seconds(now)
    if age < 0:
        raise PostBookingSnapshotError("the snapshot claims to be from the future; re-run plan")
    if age > min(max_age_sec, DEFAULT_MAX_SNAPSHOT_AGE_SEC):
        raise PostBookingSnapshotError(f"the snapshot is {int(age)}s old (limit {max_age_sec}s); re-run plan")
    if not frozen.rows:
        raise PostBookingSnapshotError(STOP_SNAPSHOT_EMPTY)
    if frozen.refusals:
        raise PostBookingSnapshotError(STOP_LEDGER_SCOPE_CHANGED)
    if frozen.unresolved_rows:
        raise PostBookingSnapshotError(STOP_WAVE_UNRESOLVED)
    if not frozen.wave_closed:
        raise PostBookingSnapshotError(STOP_WAVE_NOT_CLOSED)


@dataclass(frozen=True)
class PostBookingApplyReport:
    """Durable, PII-free evidence emitted by one committed apply."""

    snapshot_digest: str
    applied_at: datetime
    company_ids: tuple[int, ...]
    run_ids: tuple[str, ...]
    rows_in_scope: int
    canceled_job_ids: tuple[int, ...]
    already_canceled_job_ids: tuple[int, ...]
    marked_ledger_ids: tuple[int, ...]
    already_marked_ledger_ids: tuple[int, ...]
    target_job_ids: tuple[int, ...]
    scoped_outbox_ids: tuple[int, ...]

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "version": SNAPSHOT_VERSION,
            "kind": "post_booking_handover_apply",
            "snapshot_digest": self.snapshot_digest,
            "applied_at": _timestamp(self.applied_at),
            "company_ids": sorted(self.company_ids),
            "run_ids": list(self.run_ids),
            "rows_in_scope": self.rows_in_scope,
            "canceled_job_ids": list(self.canceled_job_ids),
            "already_canceled_job_ids": list(self.already_canceled_job_ids),
            "marked_ledger_ids": list(self.marked_ledger_ids),
            "already_marked_ledger_ids": list(self.already_marked_ledger_ids),
            "target_job_ids": list(self.target_job_ids),
            "scoped_outbox_ids": list(self.scoped_outbox_ids),
        }


def write_apply_report(report: PostBookingApplyReport, path: str | Path) -> Path:
    return _atomic_write(Path(path), report.as_safe_dict())


def read_apply_report(path: str | Path, *, frozen: FrozenPostBookingPlan) -> PostBookingApplyReport:
    """Load an apply report and prove it belongs to THIS snapshot."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as error:
        raise PostBookingSnapshotError(f"cannot read the apply report: {error.strerror}") from None
    except Exception:
        raise PostBookingSnapshotError("the apply report is not valid JSON") from None
    if not isinstance(payload, dict) or payload.get("kind") != "post_booking_handover_apply":
        raise PostBookingSnapshotError("the apply report is not a post-booking handover report")
    if payload.get("snapshot_digest") != frozen.digest:
        raise PostBookingSnapshotError("the apply report belongs to a different plan")
    try:
        applied_at = datetime.fromisoformat(str(payload["applied_at"]).replace("Z", "+00:00"))
    except Exception:
        raise PostBookingSnapshotError("the apply report has no usable timestamp") from None
    return PostBookingApplyReport(
        snapshot_digest=str(payload["snapshot_digest"]),
        applied_at=_as_utc(applied_at),
        company_ids=tuple(int(item) for item in payload["company_ids"]),
        run_ids=tuple(str(item) for item in payload["run_ids"]),
        rows_in_scope=int(payload["rows_in_scope"]),
        canceled_job_ids=tuple(int(item) for item in payload["canceled_job_ids"]),
        already_canceled_job_ids=tuple(int(item) for item in payload["already_canceled_job_ids"]),
        marked_ledger_ids=tuple(int(item) for item in payload["marked_ledger_ids"]),
        already_marked_ledger_ids=tuple(int(item) for item in payload["already_marked_ledger_ids"]),
        target_job_ids=tuple(int(item) for item in payload["target_job_ids"]),
        scoped_outbox_ids=tuple(int(item) for item in payload["scoped_outbox_ids"]),
    )
