"""Proving a manifest target really is the branch it claims to be (PR-11.1, rev 16).

The manifest parser proves *shape*: a canonical UUID, a positive location id, no
duplicate targets. What it cannot prove is *semantics* — that the EasyWeek
location Karlsruhe is mapped to is actually Karlsruhe. A manifest with the two
branches swapped is internally consistent, passes every syntactic check, and
migrates every Karlsruhe customer into Rastatt.

That is not a hypothetical. Plan §10 records the production incident where
``EASYWEEK_LOCATION_ID`` named one branch and ``EASYWEEK_LOCATION_UUID`` named
another; both values were individually plausible and nothing caught the mismatch
until Rastatt bookings started rendering Durlach templates. The lesson recorded
there is the design here: *one value can be typed wrong and look right; two
values that must agree cannot.* This module adds the third.

The independent authority is the runtime registry the bot already runs on —
``EASYWEEK_LOCATION_MAP``, parsed by :func:`configured_easyweek_locations`. It is
deliberately reused rather than re-parsed: a second, parallel notion of "which
EasyWeek branch is which" is exactly the divergence that would let a swap slip
through one of them.

Four things must line up for every branch, before the first mutation:

1. the runtime registry is configured **and** valid;
2. the manifest's numeric location id exists in it;
3. the manifest's location UUID equals the registry's UUID for that id;
4. the registry's slug for that id equals the slug this Altegio company is
   *expected* to map to — a value that lives in source control
   (:data:`EXPECTED_BRANCH_SLUG`), not in the operator's file.

Plus one cross-branch check: two source branches may not share one target.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Final

from altegio_bot.easyweek_locations import EasyWeekLocationRegistry, configured_easyweek_locations
from altegio_bot.easyweek_migration.manifest import EXPECTED_BRANCH_SLUG, MigrationManifest

# Stable, PII-free reasons. Each names the check that failed, never the values.
BRANCH_REGISTRY_UNCONFIGURED: Final = "easyweek_location_registry_unconfigured"
BRANCH_REGISTRY_INVALID: Final = "easyweek_location_registry_invalid"
BRANCH_LOCATION_ID_UNKNOWN: Final = "target_location_id_not_in_registry"
BRANCH_LOCATION_UUID_MISMATCH: Final = "target_location_uuid_mismatch"
BRANCH_SLUG_MISMATCH: Final = "target_branch_slug_mismatch"
BRANCH_SLUG_UNEXPECTED_SOURCE: Final = "source_company_has_no_expected_branch"
BRANCH_TARGET_REUSED: Final = "target_location_used_by_two_source_branches"
BRANCH_MANIFEST_UNUSABLE: Final = "manifest_has_no_verifiable_branches"


@dataclass(frozen=True)
class BranchIdentityResult:
    """Whether every manifest target is provably the branch it claims to be."""

    proven: bool
    failures: list[str] = field(default_factory=list)
    # company_id → the registry slug it was proven against. Report-safe.
    proven_branches: dict[int, str] = field(default_factory=dict)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "proven": self.proven,
            "failures": list(self.failures),
            "branches": {str(company_id): slug for company_id, slug in sorted(self.proven_branches.items())},
        }


def verify_branch_identity(
    manifest: MigrationManifest,
    *,
    registry: EasyWeekLocationRegistry | None = None,
) -> BranchIdentityResult:
    """Prove every manifest target against the runtime registry and the expected slug.

    Collects **all** failures rather than short-circuiting: an operator with a
    swapped manifest has two problems, and learning about one of them per run is
    how a swap gets half-fixed.

    An unconfigured or invalid registry is a hard failure, not a reason to skip
    the check. "We could not verify the branch" and "the branch is correct" are
    the two answers that must never be confused here.
    """
    live = registry if registry is not None else configured_easyweek_locations()
    failures: list[str] = []
    proven: dict[int, str] = {}

    # A manifest with nothing in it proves nothing. Without this, an invalid or
    # empty manifest would walk an empty loop, collect no failures and come back
    # `proven=True` — a vacuous truth that reads, in a report, exactly like a
    # verified branch mapping.
    if not manifest.valid or not manifest.branches:
        return BranchIdentityResult(proven=False, failures=[BRANCH_MANIFEST_UNUSABLE])

    if not live.configured:
        return BranchIdentityResult(proven=False, failures=[BRANCH_REGISTRY_UNCONFIGURED])
    if not live.valid:
        return BranchIdentityResult(proven=False, failures=[BRANCH_REGISTRY_INVALID])

    seen_location_ids: dict[int, int] = {}

    for company_id, branch in sorted(manifest.branches.items()):
        expected_slug = EXPECTED_BRANCH_SLUG.get(company_id)
        if expected_slug is None:
            # The manifest parser already refuses unknown companies; this is the
            # belt to that braces, and it fails closed rather than assuming.
            failures.append(BRANCH_SLUG_UNEXPECTED_SOURCE)
            continue

        location = live.locations.get(branch.easyweek_location_id)
        if location is None:
            failures.append(BRANCH_LOCATION_ID_UNKNOWN)
            continue

        if location.location_uuid != branch.easyweek_location_uuid:
            failures.append(BRANCH_LOCATION_UUID_MISMATCH)
            continue

        if location.name != expected_slug:
            # The id and the UUID agree with each other and with the registry —
            # and still point at the wrong salon. This is the swap.
            failures.append(BRANCH_SLUG_MISMATCH)
            continue

        previous = seen_location_ids.get(branch.easyweek_location_id)
        if previous is not None and previous != company_id:
            failures.append(BRANCH_TARGET_REUSED)
            continue
        seen_location_ids[branch.easyweek_location_id] = company_id

        proven[company_id] = location.name

    return BranchIdentityResult(proven=not failures, failures=failures, proven_branches=proven)
