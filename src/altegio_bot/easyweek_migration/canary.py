"""The single controlled booking that licenses a bulk apply (PR-11.1, rev 16).

``POST /bookings`` is a confirmed endpoint with an unconfirmed body schema (plan
§1.1, §21.4). Something has to prove the request shape against the live API
before hundreds of real customers are booked, and that something is the canary.

What the first version did — and why it was not a canary
--------------------------------------------------------
``--limit 1`` took whichever row the Altegio API returned first and stopped after
one POST. Two problems, both fatal:

* **It proved almost nothing.** A 2xx says the request was accepted. It does not
  say the booking landed at the right branch, with the right master, for the
  right customer, at the right time. Every one of those could be wrong and the
  limit would still report success.
* **It was not reproducible.** "First row of the API response" is a different
  live customer on every run, so an operator could not say which booking to go
  and look at, and a second run canaried somebody else.

What a canary is here
---------------------
1. An **explicit source identity** — company id and record id — that the
   operator names and that must be present and ``READY`` in the verified plan.
2. One POST, after the same last-second source re-proof every apply row gets.
3. A **GET of the created booking**, projected and compared field by field
   against what was sent: booking uuid, marker, location, staff, service,
   customer, start time, duration, active status. A missing field is a
   mismatch, not a pass.
4. A **durable proof row**, bound to the manifest digest, the request-schema
   version, the cutover and the branch identity, so it stops applying the moment
   any of those change.

Bulk apply then requires a matching verified proof. An operator boolean is not
enough: the whole point is that a machine checked the booking, not that a person
remembers doing so.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_migration.bindings import MUTATION_KINDS, MUTATION_SINGLE
from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult
from altegio_bot.easyweek_migration.target_snapshot import (
    REQUEST_SCHEMA_VERSION,
    TargetSnapshot,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof

logger = logging.getLogger("easyweek_migration.canary")

# Stable, PII-free reasons a canary did not license a bulk.
CANARY_NOT_IN_PLAN: Final = "canary_source_not_in_verified_plan"
CANARY_NOT_READY: Final = "canary_source_not_ready"
CANARY_REPROOF_FAILED: Final = "canary_source_reproof_failed"
CANARY_POST_UNCERTAIN: Final = "canary_post_uncertain"
CANARY_POST_FAILED: Final = "canary_post_failed"
CANARY_READBACK_FAILED: Final = "canary_readback_failed"
CANARY_MISSING: Final = "canary_proof_missing"
CANARY_NOT_VERIFIED: Final = "canary_proof_not_verified"
CANARY_STALE_MANIFEST: Final = "canary_proof_manifest_changed"
CANARY_STALE_SCHEMA: Final = "canary_proof_request_schema_changed"
CANARY_STALE_BRANCHES: Final = "canary_proof_branch_mapping_changed"
CANARY_STALE_STAFF_SCOPE: Final = "canary_proof_staff_scope_changed"
CANARY_STALE_HORIZON: Final = "canary_proof_horizon_changed"
CANARY_IDENTITY_REQUIRED: Final = "canary_source_identity_required"


def branch_identity_digest(result: BranchIdentityResult) -> str:
    """Digest of the proven company → branch-slug mapping.

    Folded into every canary proof so that re-pointing a branch — the one change
    that would send the whole bulk to the wrong salon — invalidates the proof
    even when the manifest digest happens to be unchanged.
    """
    blob = "|".join(f"{company_id}:{slug}" for company_id, slug in sorted(result.proven_branches.items()))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CanaryBinding:
    """The durable identity of ONE migration wave.

    Written by the canary, required by the bulk apply, and — since revision 18 —
    re-proved by every command that continues the wave. It is what makes "the
    wave you are reconciling" a checkable claim rather than whatever the operator
    typed today.

    Every field is here because changing it silently changes *which bookings get
    proven*:

    ``manifest_digest``          different mapping, so different targets;
    ``staff_scope_digest``       a master moved between waves, so her bookings —
                                 and her EasyWeek targets — drop out of the check;
    ``cutover_at``               a later boundary reclassifies earlier bookings as
                                 ``starts_before_cutover`` and stops checking them;
    ``horizon_days``             a narrower horizon drops the far end of the wave;
    ``branch_identity_digest``   a re-pointed branch proves a different salon;
    ``request_schema_version``   a changed request shape was never canaried;
    ``contract_kind``            a different endpoint and body entirely — a
                                 single-service canary has proven nothing about
                                 the cart path, and vice versa (plan §30.12).
    """

    manifest_digest: str
    staff_scope_digest: str
    request_schema_version: str
    cutover_at: datetime
    horizon_days: int
    branch_identity_digest: str
    contract_kind: str = MUTATION_SINGLE

    @property
    def wave_identity(self) -> str:
        """A short, stable, PII-free name for this wave.

        Two consecutive waves — the first one and the later nail-services one —
        differ in at least the selector, so they get different identities and can
        never be proven against each other's targets.
        """
        blob = "|".join(
            [
                self.manifest_digest,
                self.staff_scope_digest,
                self.request_schema_version,
                self.contract_kind,
                self.cutover_at.isoformat(),
                str(self.horizon_days),
                self.branch_identity_digest,
            ]
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "wave_identity": self.wave_identity,
            "manifest_digest": self.manifest_digest,
            "staff_scope_digest": self.staff_scope_digest,
            "request_schema_version": self.request_schema_version,
            "contract_kind": self.contract_kind,
            "cutover_at": self.cutover_at.isoformat().replace("+00:00", "Z"),
            "horizon_days": self.horizon_days,
            "branch_identity_digest": self.branch_identity_digest,
        }


def build_binding(
    *,
    manifest_digest: str,
    staff_scope_digest: str,
    cutover_at: datetime,
    horizon_days: int,
    branch_result: BranchIdentityResult,
    contract_kind: str = MUTATION_SINGLE,
) -> CanaryBinding:
    if contract_kind not in MUTATION_KINDS:
        raise ValueError(f"unknown mutation contract: {contract_kind!r}")
    return CanaryBinding(
        manifest_digest=manifest_digest,
        staff_scope_digest=staff_scope_digest,
        request_schema_version=REQUEST_SCHEMA_VERSION,
        cutover_at=cutover_at.astimezone(timezone.utc),
        horizon_days=horizon_days,
        branch_identity_digest=branch_identity_digest(branch_result),
        contract_kind=contract_kind,
    )


@dataclass(frozen=True)
class CanaryVerdict:
    """Whether a stored proof licenses a bulk apply, and why not when it does not."""

    licensed: bool
    reason: str | None = None
    source_company_id: int | None = None
    source_record_id: int | None = None
    target_booking_uuid: str | None = None
    verified_at: str | None = None
    # Which mutation contract this verdict is about. A verdict never speaks for
    # a contract it did not look up: the caller asks once per contract it means
    # to execute, and each answer carries the question back with it.
    contract_kind: str = MUTATION_SINGLE

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "licensed": self.licensed,
            "reason": self.reason,
            "contract_kind": self.contract_kind,
            "source_company_id": self.source_company_id,
            "source_record_id": self.source_record_id,
            "target_booking_uuid": self.target_booking_uuid,
            "verified_at": self.verified_at,
        }


async def record_proof(
    session: AsyncSession,
    *,
    run_id: str,
    binding: CanaryBinding,
    source_company_id: int,
    source_record_id: int,
    source_fingerprint: str,
    verified: bool,
    target_booking_uuid: str | None,
    target_snapshot: TargetSnapshot | None,
    failure_reason: str | None,
) -> None:
    """Store (or refresh) the proof for this exact canary attempt.

    A *failed* canary is stored too. It is precisely what an operator needs to
    read, and storing only successes would make a red canary indistinguishable
    from one nobody ran.
    """
    now = datetime.now(timezone.utc)
    values = {
        "source_company_id": source_company_id,
        "source_record_id": source_record_id,
        "source_fingerprint": source_fingerprint,
        "target_booking_uuid": target_booking_uuid,
        "target_snapshot_fingerprint": target_snapshot.fingerprint if target_snapshot is not None else None,
        "manifest_digest": binding.manifest_digest,
        "staff_scope_digest": binding.staff_scope_digest,
        "horizon_days": binding.horizon_days,
        "request_schema_version": binding.request_schema_version,
        "contract_kind": binding.contract_kind,
        "cutover_at": binding.cutover_at,
        "branch_identity_digest": binding.branch_identity_digest,
        "verified": verified,
        "failure_reason": failure_reason,
        "run_id": run_id,
        "verified_at": now if verified else None,
        "created_at": now,
        "updated_at": now,
    }
    stmt = (
        pg_insert(EasyWeekMigrationCanaryProof)
        .values(**values)
        .on_conflict_do_update(
            constraint="uq_easyweek_migration_canary_identity",
            set_={
                "source_fingerprint": values["source_fingerprint"],
                "target_booking_uuid": values["target_booking_uuid"],
                "target_snapshot_fingerprint": values["target_snapshot_fingerprint"],
                "branch_identity_digest": values["branch_identity_digest"],
                "staff_scope_digest": values["staff_scope_digest"],
                "horizon_days": values["horizon_days"],
                "verified": values["verified"],
                "failure_reason": values["failure_reason"],
                "run_id": values["run_id"],
                "verified_at": values["verified_at"],
                "updated_at": values["updated_at"],
            },
        )
    )
    await session.execute(stmt)


async def find_licensing_proof(session: AsyncSession, *, binding: CanaryBinding) -> CanaryVerdict:
    """Is there a verified proof that still applies to THIS run?

    Selects on the full binding, so a changed manifest, request schema, cutover
    or branch mapping simply finds nothing — the proof was evidence about a
    different run.

    The distinction between "no proof at all" and "a proof that went stale" is
    kept, because the operator's next step differs: run the canary, versus work
    out what changed under them.
    """
    exact = (
        await session.execute(
            select(EasyWeekMigrationCanaryProof)
            .where(
                EasyWeekMigrationCanaryProof.manifest_digest == binding.manifest_digest,
                EasyWeekMigrationCanaryProof.request_schema_version == binding.request_schema_version,
                EasyWeekMigrationCanaryProof.contract_kind == binding.contract_kind,
                EasyWeekMigrationCanaryProof.cutover_at == binding.cutover_at,
                EasyWeekMigrationCanaryProof.branch_identity_digest == binding.branch_identity_digest,
                EasyWeekMigrationCanaryProof.staff_scope_digest == binding.staff_scope_digest,
                EasyWeekMigrationCanaryProof.horizon_days == binding.horizon_days,
                EasyWeekMigrationCanaryProof.verified.is_(True),
            )
            .order_by(EasyWeekMigrationCanaryProof.verified_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()

    if exact is not None:
        return CanaryVerdict(
            licensed=True,
            source_company_id=exact.source_company_id,
            source_record_id=exact.source_record_id,
            target_booking_uuid=exact.target_booking_uuid,
            verified_at=exact.verified_at.isoformat().replace("+00:00", "Z") if exact.verified_at else None,
        )

    # Nothing matched. Say WHY as precisely as the stored rows allow.
    any_proof = (
        await session.execute(
            select(EasyWeekMigrationCanaryProof)
            .where(EasyWeekMigrationCanaryProof.verified.is_(True))
            .order_by(EasyWeekMigrationCanaryProof.verified_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()

    if any_proof is None:
        return CanaryVerdict(licensed=False, reason=CANARY_MISSING)
    if any_proof.request_schema_version != binding.request_schema_version:
        return CanaryVerdict(licensed=False, reason=CANARY_STALE_SCHEMA)
    if any_proof.manifest_digest != binding.manifest_digest:
        return CanaryVerdict(licensed=False, reason=CANARY_STALE_MANIFEST)
    if any_proof.staff_scope_digest != binding.staff_scope_digest:
        return CanaryVerdict(licensed=False, reason=CANARY_STALE_STAFF_SCOPE)
    if any_proof.horizon_days != binding.horizon_days:
        return CanaryVerdict(licensed=False, reason=CANARY_STALE_HORIZON)
    if any_proof.branch_identity_digest != binding.branch_identity_digest:
        return CanaryVerdict(licensed=False, reason=CANARY_STALE_BRANCHES)
    return CanaryVerdict(licensed=False, reason=CANARY_NOT_VERIFIED)


# ---------------------------------------------------------------------------
# The durable wave scope, re-proved by everything that continues a wave
# ---------------------------------------------------------------------------
# Stable, PII-free reasons the scope of a command does not match the wave that
# was actually migrated.
SCOPE_MISSING: Final = "migration_scope_missing"
SCOPE_AMBIGUOUS: Final = "migration_scope_ambiguous"
SCOPE_MANIFEST_MISMATCH: Final = "migration_scope_manifest_mismatch"
SCOPE_STAFF_SCOPE_MISMATCH: Final = "migration_scope_staff_scope_mismatch"
SCOPE_CUTOVER_MISMATCH: Final = "migration_scope_cutover_mismatch"
SCOPE_HORIZON_MISMATCH: Final = "migration_scope_horizon_mismatch"
SCOPE_BRANCH_MISMATCH: Final = "migration_scope_branch_mismatch"
SCOPE_SCHEMA_MISMATCH: Final = "migration_scope_schema_mismatch"
# The stored proof exercised a different mutation contract — `single` against
# `POST /bookings`, or `cart_two` against `POST /bookings/cart`. Different
# endpoint, different body, different readback, so it proves nothing here.
SCOPE_CONTRACT_MISMATCH: Final = "migration_scope_contract_mismatch"
SCOPE_PROVEN: Final = "migration_scope_proven"
# A scope check was asked to prove an empty set of contracts. Nothing to prove
# is not the same as proven, and this says so under its own name.
SCOPE_CONTRACTS_UNKNOWN: Final = "migration_scope_contracts_unknown"

# The one narrow admission that lets an UNVERIFIED proof be used — and only
# to recover the very row it belongs to. See `find_recoverable_canary_attempt`.
RECOVERY_ADMITTED: Final = "canary_recovery_admitted"
RECOVERY_NO_ATTEMPT: Final = "canary_recovery_no_matching_attempt"
RECOVERY_NOT_UNCERTAIN_OUTCOME: Final = "canary_recovery_failure_not_an_unknown_outcome"
RECOVERY_ALREADY_VERIFIED: Final = "canary_recovery_proof_already_verified"
RECOVERY_PROOF_CHANGED: Final = "canary_recovery_proof_changed_during_resolution"


class CanaryRecoveryProofChanged(RuntimeError):
    """The admitted canary attempt changed while its live proof was built."""

    def __init__(self, *, source_company_id: int, source_record_id: int) -> None:
        super().__init__(RECOVERY_PROOF_CHANGED)
        self.reason = RECOVERY_PROOF_CHANGED
        self.source_company_id = source_company_id
        self.source_record_id = source_record_id


@dataclass(frozen=True)
class ScopeVerdict:
    """Whether the arguments of this command describe the wave that was migrated."""

    proven: bool
    reason: str
    wave_identity: str | None = None
    # The contract this verdict was asked about; see `CanaryVerdict.contract_kind`.
    contract_kind: str = MUTATION_SINGLE

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "scope_proven": self.proven,
            "scope_reason": self.reason,
            "wave_identity": self.wave_identity,
            "contract_kind": self.contract_kind,
        }


def _stored_binding(row: EasyWeekMigrationCanaryProof) -> CanaryBinding | None:
    """The binding a stored proof represents, or ``None`` if it predates scope.

    A proof written before revision 18 has no selector digest and no horizon, so
    it cannot say which wave it belonged to. That is not a proof of scope, and
    inventing the missing halves would be exactly the guessing this design bans.
    """
    if row.staff_scope_digest is None or row.horizon_days is None:
        return None
    return CanaryBinding(
        manifest_digest=row.manifest_digest,
        staff_scope_digest=row.staff_scope_digest,
        request_schema_version=row.request_schema_version,
        cutover_at=row.cutover_at.astimezone(timezone.utc),
        horizon_days=row.horizon_days,
        branch_identity_digest=row.branch_identity_digest,
        contract_kind=row.contract_kind,
    )


@dataclass(frozen=True)
class RecoveryProofExpectation:
    """Immutable version of the one unverified proof admitted for recovery."""

    proof_id: int
    binding: CanaryBinding
    source_company_id: int
    source_record_id: int
    source_fingerprint: str
    target_booking_uuid: str | None
    target_snapshot_fingerprint: str | None
    verified: bool
    failure_reason: str | None
    run_id: str
    verified_at: datetime | None
    created_at: datetime
    updated_at: datetime


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def recovery_proof_expectation(row: EasyWeekMigrationCanaryProof) -> RecoveryProofExpectation:
    """Freeze the exact proof row before any external recovery proof begins."""
    binding = _stored_binding(row)
    if row.id is None or binding is None or row.created_at is None or row.updated_at is None:
        raise RuntimeError("canary recovery proof has no durable identity/version/binding")
    return RecoveryProofExpectation(
        proof_id=int(row.id),
        binding=binding,
        source_company_id=int(row.source_company_id),
        source_record_id=int(row.source_record_id),
        source_fingerprint=str(row.source_fingerprint),
        target_booking_uuid=str(row.target_booking_uuid) if row.target_booking_uuid is not None else None,
        target_snapshot_fingerprint=(
            str(row.target_snapshot_fingerprint) if row.target_snapshot_fingerprint is not None else None
        ),
        verified=bool(row.verified),
        failure_reason=str(row.failure_reason) if row.failure_reason is not None else None,
        run_id=str(row.run_id),
        verified_at=_as_utc(row.verified_at) if row.verified_at is not None else None,
        created_at=_as_utc(row.created_at),
        updated_at=_as_utc(row.updated_at),
    )


def _first_difference(stored: CanaryBinding, current: CanaryBinding) -> str:
    """Name the field that differs, most-explanatory first.

    The selector is checked before the manifest even though it is *inside* the
    manifest digest: "somebody moved a master to the next wave" is a specific,
    actionable thing to be told, and "the manifest changed" would hide it.
    """
    if stored.staff_scope_digest != current.staff_scope_digest:
        return SCOPE_STAFF_SCOPE_MISMATCH
    if stored.manifest_digest != current.manifest_digest:
        return SCOPE_MANIFEST_MISMATCH
    if stored.cutover_at != current.cutover_at:
        return SCOPE_CUTOVER_MISMATCH
    if stored.horizon_days != current.horizon_days:
        return SCOPE_HORIZON_MISMATCH
    if stored.branch_identity_digest != current.branch_identity_digest:
        return SCOPE_BRANCH_MISMATCH
    if stored.request_schema_version != current.request_schema_version:
        return SCOPE_SCHEMA_MISMATCH
    if stored.contract_kind != current.contract_kind:
        # A single-service canary licensing a cart bulk, or the reverse. Named
        # separately because the fix is a whole extra canary run, not an edit.
        return SCOPE_CONTRACT_MISMATCH
    # Unreachable while `wave_identity` covers exactly these fields; kept so a
    # future field cannot silently pass as "no difference".
    return SCOPE_AMBIGUOUS


async def find_proven_scope(session: AsyncSession, *, binding: CanaryBinding) -> ScopeVerdict:
    """Does a verified canary prove the wave these arguments describe?

    This is what stops a reconciliation from proving a *different* wave than the
    one that was migrated. Two ways that used to be possible, both silent:

    * run ``reconcile --final`` without ``--cutover-at`` and let the code use
      "now" — bookings before that hour become ``starts_before_cutover``, their
      EasyWeek targets are never fetched, and a deleted target cannot fail a
      check that never looks at it;
    * move an already-migrated master into ``deferred_altegio_staff_ids`` — her
      bookings leave the selected wave and take their targets with them.

    Both now change the binding, and a binding that matches nothing is a refusal.

    Outcomes:

    ``proven``            a verified canary carries exactly this binding;
    ``SCOPE_MISSING``     no verified canary at all — no wave has been licensed;
    ``SCOPE_AMBIGUOUS``   several waves exist and none matches, so the tool
                          cannot say which one was meant; the operator must
                          supply that wave's original arguments;
    ``..._MISMATCH``      exactly one wave exists and this is how it differs.
    """
    exact = (
        await session.execute(
            select(EasyWeekMigrationCanaryProof)
            .where(
                EasyWeekMigrationCanaryProof.manifest_digest == binding.manifest_digest,
                EasyWeekMigrationCanaryProof.staff_scope_digest == binding.staff_scope_digest,
                EasyWeekMigrationCanaryProof.request_schema_version == binding.request_schema_version,
                EasyWeekMigrationCanaryProof.contract_kind == binding.contract_kind,
                EasyWeekMigrationCanaryProof.cutover_at == binding.cutover_at,
                EasyWeekMigrationCanaryProof.horizon_days == binding.horizon_days,
                EasyWeekMigrationCanaryProof.branch_identity_digest == binding.branch_identity_digest,
                EasyWeekMigrationCanaryProof.verified.is_(True),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if exact is not None:
        return ScopeVerdict(proven=True, reason=SCOPE_PROVEN, wave_identity=binding.wave_identity)

    rows = list(
        (
            await session.execute(
                select(EasyWeekMigrationCanaryProof)
                .where(EasyWeekMigrationCanaryProof.verified.is_(True))
                .order_by(EasyWeekMigrationCanaryProof.verified_at.desc())
            )
        )
        .scalars()
        .all()
    )
    stored = [b for b in (_stored_binding(row) for row in rows) if b is not None]
    if not stored:
        return ScopeVerdict(proven=False, reason=SCOPE_MISSING)

    distinct = {b.wave_identity: b for b in stored}
    if len(distinct) > 1:
        # Several waves have run. None of them is this one, and picking "the
        # newest" would let a second wave's arguments silently reconcile the
        # first wave's bookings.
        return ScopeVerdict(proven=False, reason=SCOPE_AMBIGUOUS)

    only = next(iter(distinct.values()))
    return ScopeVerdict(proven=False, reason=_first_difference(only, binding), wave_identity=only.wave_identity)


# ---------------------------------------------------------------------------
# Recovering the one canary whose own outcome is unknown
# ---------------------------------------------------------------------------
# A deadlock, and a narrow key for it.
#
# When the canary POST times out (or breaks, or 5xx's, or returns a body with no
# readable uuid) the ledger row is `uncertain` and the proof is stored with
# `verified=false`. The booking may well exist — the operator can find it in the
# EasyWeek UI by its migration marker — but `resolve-created` goes through the
# scope gate, and that gate only accepts a *verified* proof. So the one row that
# would produce the wave's first verified proof is the one row that cannot be
# resolved, and re-sending the POST is forbidden because it may duplicate a real
# customer's appointment.
#
# The key below is deliberately the narrowest thing that opens it: an unverified
# proof may be used to recover **its own** uncertain ledger row, and nothing
# else. Every other use of an unverified proof — licensing a bulk, passing a
# final reconciliation, resolving a different row — stays exactly as closed as it
# was. `find_proven_scope()` and `find_licensing_proof()` are not touched.

# Only an outcome that is genuinely UNKNOWN qualifies. A 4xx proves the booking
# was not created; a source re-proof failure means the POST never went; a
# readback mismatch means something WAS created and it was wrong. None of those
# is a booking waiting to be found, and none of them may take this path.
_RECOVERABLE_FAILURE_REASONS: Final[frozenset[str]] = frozenset({CANARY_POST_UNCERTAIN})


@dataclass(frozen=True)
class RecoveryAdmission:
    """Whether an unverified canary attempt may be used to recover its own row."""

    admitted: bool
    reason: str
    proof_run_id: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {"canary_recovery": self.reason, "canary_recovery_admitted": self.admitted}


async def find_recoverable_canary_attempt(
    session: AsyncSession,
    *,
    binding: CanaryBinding,
    source_company_id: int,
    source_record_id: int,
) -> tuple[RecoveryAdmission, EasyWeekMigrationCanaryProof | None]:
    """Is there an unverified canary attempt that THIS row is allowed to recover?

    Admission requires, all at once:

    * a proof row with ``verified=false``;
    * a ``failure_reason`` that names an unknown mutation outcome — not a 4xx,
      not a source re-proof failure, not a readback mismatch;
    * the **exact** wave binding: manifest, staff scope, cutover, horizon, branch
      identity and request schema all equal to the current run's;
    * the proof's own source identity equal to the row being resolved.

    The ledger-side conditions — the row exists, is unresolved, carries the same
    origin run as the proof, and recorded exactly one mutation attempt — are the
    caller's to check, because they are the caller's data. Admission here is
    necessary, never sufficient.

    Reads only, and reads nothing outside PostgreSQL: a refusal must not cost an
    Altegio or EasyWeek request.
    """
    candidate = (
        await session.execute(
            select(EasyWeekMigrationCanaryProof)
            .where(
                EasyWeekMigrationCanaryProof.source_company_id == source_company_id,
                EasyWeekMigrationCanaryProof.source_record_id == source_record_id,
                EasyWeekMigrationCanaryProof.manifest_digest == binding.manifest_digest,
                EasyWeekMigrationCanaryProof.staff_scope_digest == binding.staff_scope_digest,
                EasyWeekMigrationCanaryProof.request_schema_version == binding.request_schema_version,
                EasyWeekMigrationCanaryProof.contract_kind == binding.contract_kind,
                EasyWeekMigrationCanaryProof.cutover_at == binding.cutover_at,
                EasyWeekMigrationCanaryProof.horizon_days == binding.horizon_days,
                EasyWeekMigrationCanaryProof.branch_identity_digest == binding.branch_identity_digest,
            )
            .limit(1)
        )
    ).scalar_one_or_none()

    if candidate is None:
        # No attempt with this exact binding and this exact source. Nothing to
        # recover, and nothing to read outside the database to find out.
        return RecoveryAdmission(admitted=False, reason=RECOVERY_NO_ATTEMPT), None
    if candidate.verified:
        # Already proven; the ordinary scope gate applies and this path is not
        # needed. Saying so plainly beats silently doing nothing.
        return RecoveryAdmission(admitted=False, reason=RECOVERY_ALREADY_VERIFIED), None
    if candidate.failure_reason not in _RECOVERABLE_FAILURE_REASONS:
        return RecoveryAdmission(admitted=False, reason=RECOVERY_NOT_UNCERTAIN_OUTCOME), None

    return (
        RecoveryAdmission(admitted=True, reason=RECOVERY_ADMITTED, proof_run_id=candidate.run_id),
        candidate,
    )


async def promote_proof_to_verified(
    session: AsyncSession,
    *,
    expected: RecoveryProofExpectation,
    target_booking_uuid: str,
    target_snapshot: TargetSnapshot,
) -> None:
    """Turn one attempted canary into a verified one, on completed evidence.

    Called only after the full target proof has passed: the source was re-read
    and re-classified, the live booking was fetched, and every write-critical
    field matched. This function records that verdict; it never decides it.

    The caller runs it inside the SAME transaction that flips the ledger row, so
    the two can never disagree about whether the canary is proven. ``expected``
    is the exact unverified proof admitted before the external read: refreshing,
    replacing or verifying that attempt while the read is in flight invalidates
    the result instead of attaching it to a different attempt.
    """
    row = (
        await session.execute(
            select(EasyWeekMigrationCanaryProof)
            .where(EasyWeekMigrationCanaryProof.id == expected.proof_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    valid_expectation = (
        not expected.verified
        and expected.failure_reason in _RECOVERABLE_FAILURE_REASONS
        and row is not None
        and recovery_proof_expectation(row) == expected
    )
    if not valid_expectation:
        raise CanaryRecoveryProofChanged(
            source_company_id=expected.source_company_id,
            source_record_id=expected.source_record_id,
        )

    now = datetime.now(timezone.utc)
    row.verified = True
    row.failure_reason = None
    row.target_booking_uuid = target_booking_uuid
    row.target_snapshot_fingerprint = target_snapshot.fingerprint
    row.verified_at = now
    row.updated_at = now
