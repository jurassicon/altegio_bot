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
    """What a proof must match to still license the run being attempted."""

    manifest_digest: str
    request_schema_version: str
    cutover_at: datetime
    branch_identity_digest: str

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "manifest_digest": self.manifest_digest,
            "request_schema_version": self.request_schema_version,
            "cutover_at": self.cutover_at.isoformat().replace("+00:00", "Z"),
            "branch_identity_digest": self.branch_identity_digest,
        }


def build_binding(
    *,
    manifest_digest: str,
    cutover_at: datetime,
    branch_result: BranchIdentityResult,
) -> CanaryBinding:
    return CanaryBinding(
        manifest_digest=manifest_digest,
        request_schema_version=REQUEST_SCHEMA_VERSION,
        cutover_at=cutover_at.astimezone(timezone.utc),
        branch_identity_digest=branch_identity_digest(branch_result),
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

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "licensed": self.licensed,
            "reason": self.reason,
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
        "request_schema_version": binding.request_schema_version,
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
                EasyWeekMigrationCanaryProof.cutover_at == binding.cutover_at,
                EasyWeekMigrationCanaryProof.branch_identity_digest == binding.branch_identity_digest,
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
    if any_proof.branch_identity_digest != binding.branch_identity_digest:
        return CanaryVerdict(licensed=False, reason=CANARY_STALE_BRANCHES)
    return CanaryVerdict(licensed=False, reason=CANARY_NOT_VERIFIED)
