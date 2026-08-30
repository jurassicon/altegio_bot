"""The cumulative-manifest guard: previous waves' context, checked before writing.

A wave's manifest is not a description of that wave. It is the accumulated
mapping of every wave so far, and the selector — ``selected_altegio_staff_ids``
/ ``deferred_altegio_staff_ids`` — is the only thing that says which masters the
*current* wave migrates. Those two facts are independent, and the whole class of
defect this module exists to catch comes from confusing them.

The failure it prevents
-----------------------
Wave A migrates the lash master. Her booking is live, her ledger row says
``created``, and nothing about her source or target ever changes. Wave B
migrates the nail master, so the operator builds a manifest describing wave B:
the nail master selected, the nail service mapped, the lash master deferred and
her service dropped as "not this wave's business".

That manifest is syntactically valid, the plan is clean, the canary passes and
the bulk apply succeeds. Then the final reconciliation re-proves wave A's still
live booking — as it must, because a migrated booking that quietly vanished is
the thing reconciliation is for — and cannot: without the LASH mapping the
classifier can no longer normalise the source, so the row is
``migrated_source_lifecycle_unprovable`` and wave B can never be declared
complete. The mutations are already done; the manifest edit that would fix it
happens after the fact. An operator has migrated a wave they cannot close.

So the check has to happen **before the first mutation**, and it does: this
module's verdict feeds the apply gate, which is the single chokepoint canary and
bulk apply both pass through.

What it proves
--------------
For every ``created`` ledger row of **either** migrating branch that is not part
of the current wave's own plan — those are checked the ordinary way, and counting
them twice would only turn one problem into two report entries — and whose source
booking is still live, the current manifest and customer directory must still be
able to:

* prove the source identity,
* apply the stored staff mapping,
* apply the stored service mapping,
* apply the stored catalogue price/duration baseline,
* resolve the customer through the full directory export,
* recompute the same source fingerprint the ledger recorded.

The branch itself is part of that, and it is the half that was missing. A wave
with nothing new in Rastatt could not write ``selected: []`` while the rule was
per-branch, so the only way to a valid file was to delete the Rastatt branch —
which took its live rows out of this check and out of the final reconciliation
together. The parser now allows an empty selector on one branch, and this sweep
reads both branches regardless of what the manifest names, so neither deleting a
branch nor re-selecting an already-migrated master is a way through.

That is exactly the work the final reconciliation will demand later, which is
why it is done with the same :func:`reclassify_source_lifecycle` rather than a
second, subtly different normalisation. There is no new fingerprint model here
and nothing new in the ledger — the contract is enforced with what already
exists.

What it deliberately is NOT
---------------------------
Not a reconciliation. It never reads EasyWeek and never looks at a target. A
previous wave's source that was genuinely cancelled, deleted or has already
happened is *not* a context error and is not reported as one: whether its target
is now a ghost is a question about EasyWeek, it is the final reconciliation's
question, and answering it here would mean fetching every earlier target before
a canary may touch one booking.

Reads Altegio and PostgreSQL. Writes nothing, anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Final

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.classify import (
    BLOCK_CUSTOM_DURATION,
    BLOCK_CUSTOM_PRICE,
    BLOCK_DURATION_UNKNOWN,
    BLOCK_PRICE_BASELINE_MISSING,
    BLOCK_PRICE_MALFORMED,
    BLOCK_SERVICE_MAPPING_MISSING,
    BLOCK_STAFF_MAPPING_MISSING,
    BLOCK_STAFF_NOT_IN_WAVE,
    SKIP_FOREIGN_COMPANY,
    SKIPPED,
    Decision,
)
from altegio_bot.easyweek_migration.customers import (
    CUSTOMER_AMBIGUOUS,
    CUSTOMER_NOT_FOUND,
    CUSTOMER_PHONE_UNUSABLE,
    CustomerDirectory,
)
from altegio_bot.easyweek_migration.cutover import Cutover
from altegio_bot.easyweek_migration.manifest import MIGRATABLE_COMPANY_IDS, MigrationManifest
from altegio_bot.easyweek_migration.reproof import (
    LIFECYCLE_INACTIVE_OR_CHANGED,
    LIFECYCLE_UNPROVABLE,
    reclassify_source_lifecycle,
)

# Per-row reason codes. Stable, machine-readable, PII-free, and deliberately
# prefixed: an operator reading `previous_wave_service_mapping_missing` in a
# canary report should never confuse it with the identically-shaped complaint
# about a booking of the wave they are actually running.
PREV_STAFF_MAPPING_MISSING: Final = "previous_wave_staff_mapping_missing"
PREV_SERVICE_MAPPING_MISSING: Final = "previous_wave_service_mapping_missing"
PREV_CATALOGUE_BASELINE_MISSING: Final = "previous_wave_catalogue_baseline_missing"
PREV_CUSTOMER_UNRESOLVED: Final = "previous_wave_customer_unresolved"
PREV_SOURCE_FINGERPRINT_MISMATCH: Final = "previous_wave_source_fingerprint_mismatch"
# A whole branch was dropped from this wave's manifest while its earlier wave's
# bookings are still live. Deliberately its own code: it is not a foreign
# company, not an inactive source, not a ghost target and not an ordinary
# mapping gap — it is the cumulative contract broken one level up, at the branch.
PREV_BRANCH_MISSING: Final = "previous_wave_branch_missing"
# Everything else that left the row unprovable — an unreadable source, an
# identity that no longer matches. Kept as one code with the classifier's own
# detail alongside, rather than mirroring every reason the classifier has.
PREV_CONTEXT_UNPROVABLE: Final = "previous_wave_context_unprovable"

# Classifier reasons that mean "the manifest no longer carries what this row
# needs", grouped by which half of the cumulative contract was broken.
_REASON_TO_CODE: Final[dict[str, str]] = {
    BLOCK_STAFF_MAPPING_MISSING: PREV_STAFF_MAPPING_MISSING,
    # Can only appear if a caller re-introduces wave scoping here; mapped
    # anyway so the code path cannot fall through to a vaguer answer.
    BLOCK_STAFF_NOT_IN_WAVE: PREV_STAFF_MAPPING_MISSING,
    BLOCK_SERVICE_MAPPING_MISSING: PREV_SERVICE_MAPPING_MISSING,
    # A baseline that is gone, unreadable, or has been edited to a different
    # price/duration than the one this booking was migrated against. All three
    # are the same operator mistake: the earlier wave's baseline was not kept.
    BLOCK_PRICE_BASELINE_MISSING: PREV_CATALOGUE_BASELINE_MISSING,
    BLOCK_PRICE_MALFORMED: PREV_CATALOGUE_BASELINE_MISSING,
    BLOCK_CUSTOM_PRICE: PREV_CATALOGUE_BASELINE_MISSING,
    BLOCK_CUSTOM_DURATION: PREV_CATALOGUE_BASELINE_MISSING,
    BLOCK_DURATION_UNKNOWN: PREV_CATALOGUE_BASELINE_MISSING,
    CUSTOMER_NOT_FOUND: PREV_CUSTOMER_UNRESOLVED,
    CUSTOMER_AMBIGUOUS: PREV_CUSTOMER_UNRESOLVED,
    CUSTOMER_PHONE_UNUSABLE: PREV_CUSTOMER_UNRESOLVED,
    # The classifier's word for "this company is not in the manifest". Rows are
    # only ever read for the two migrating branches, so here it can mean exactly
    # one thing: the operator deleted a branch that still owes live rows.
    SKIP_FOREIGN_COMPANY: PREV_BRANCH_MISSING,
}


@dataclass(frozen=True)
class PreviousWaveContext:
    """Whether earlier waves' live rows survive the current manifest.

    ``proven`` is what the gate consults. ``rows`` is what the operator fixes
    from: one entry per row that failed, carrying the source identity, the reason
    code, and the Altegio staff/service ids whose mapping has to go back into the
    manifest. Ids and codes only — never a name, a phone or a payload.
    """

    proven: bool
    checked: int = 0
    rows: list[dict[str, Any]] = field(default_factory=list)

    def as_safe_dict(self) -> dict[str, Any]:
        return {"proven": self.proven, "checked": self.checked, "rows": list(self.rows)}


def _code_for(detail: str | None) -> str:
    if detail is None:
        return PREV_CONTEXT_UNPROVABLE
    return _REASON_TO_CODE.get(detail, PREV_CONTEXT_UNPROVABLE)


async def prove_previous_wave_context(
    session: AsyncSession,
    *,
    manifest: MigrationManifest,
    directory: CustomerDirectory,
    cutover: Cutover,
    decisions: list[Decision],
    http_client: httpx.AsyncClient | None = None,
) -> PreviousWaveContext:
    """Read-only. Prove the current manifest still covers earlier waves' live rows.

    ``decisions`` is the plan this run just built: any identity it reached a
    non-skipped verdict on belongs to the current wave and is excluded, so the
    guard speaks only about rows the current run would otherwise never look at.
    """
    current_wave: set[tuple[int, int]] = {
        (decision.source_company_id, decision.source_record_id) for decision in decisions if decision.outcome != SKIPPED
    }

    # Every migrating branch, NOT just the ones this manifest happens to name.
    # Scoping the sweep to `manifest.company_ids` made the guard trivially
    # bypassable: deleting a branch from the file deleted its live rows from the
    # check, and a wave could then canary, apply and pass a final reconciliation
    # that had never looked at the other branch at all.
    rows = await ledger_module.all_rows(session, company_ids=tuple(sorted(MIGRATABLE_COMPANY_IDS)))
    failures: list[dict[str, Any]] = []
    checked = 0

    for row in rows:
        if row.status != ledger_module.STATUS_CREATED:
            # `uncertain`, `pending`, `failed` and `rolled_back` rows are not
            # settled facts about a live booking, and resolving them is the
            # reconciliation's job, not a precondition for the next wave.
            continue
        identity = (row.source_company_id, row.source_record_id)
        if identity in current_wave:
            continue

        checked += 1
        lifecycle = await reclassify_source_lifecycle(
            company_id=row.source_company_id,
            record_id=row.source_record_id,
            expected_fingerprint=row.source_fingerprint,
            manifest=manifest,
            directory=directory,
            cutover=cutover,
            http_client=http_client,
        )
        if lifecycle.state == LIFECYCLE_UNPROVABLE:
            code = _code_for(lifecycle.detail)
        elif lifecycle.state == LIFECYCLE_INACTIVE_OR_CHANGED and lifecycle.detail == "fingerprint_changed":
            # The source is still live and still classifiable, but it is no
            # longer the booking that was migrated. The earlier target is now a
            # ghost, and the reconciliation of THIS wave would fail on it — so
            # the wave must not start.
            code = PREV_SOURCE_FINGERPRINT_MISMATCH
        else:
            # Proven unchanged, or genuinely over (cancelled, deleted, past).
            # Neither is a cumulative-context problem.
            continue

        failures.append(
            {
                "source_company_id": row.source_company_id,
                "source_record_id": row.source_record_id,
                "reason": code,
                "detail": lifecycle.detail,
                "altegio_staff_id": lifecycle.altegio_staff_id,
                "altegio_service_id": lifecycle.altegio_service_id,
            }
        )

    return PreviousWaveContext(proven=not failures, checked=checked, rows=failures)
