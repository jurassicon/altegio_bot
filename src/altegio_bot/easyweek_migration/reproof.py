"""The last look at the source, immediately before each POST (PR-11.1, rev 16).

``build_plan`` reads every branch's bookings once and hands back a list. A bulk
apply then walks that list, paced by the EasyWeek rate limit — which means the
last booking in a 400-row run is created from a snapshot taken tens of minutes
earlier. In those minutes a customer can cancel, a salon can reschedule, a
manager can swap the master. The plan does not know, and the old code created
the appointment as planned anyway: at the old time, with the old master, for a
customer who had already called to cancel.

So every booking is re-read from Altegio and re-classified **immediately before
its ledger claim and its POST**. The re-proof is read-only, it costs one request,
and it is the difference between "this was true when we started" and "this is
true now".

Two properties keep the re-proof from becoming a second, sloppier planner:

* **It can confirm or stop. It can never add.** A booking that was not in the
  approved dry-run cannot become migratable here — the fingerprint it is checked
  against comes from the plan the operator reviewed, and anything else is a
  refusal. The apply's set is decided by the reviewed plan, full stop.
* **It fails closed on every ambiguity.** Unreachable, malformed, a 404, a
  changed id: none of those are "probably fine". They all stop this row, and —
  crucially — they stop it *before* a ledger claim exists, so nothing is left
  looking like an unresolved mutation that reconciliation has to chase.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Final

import httpx

from altegio_bot.easyweek_migration.altegio_source import AltegioSourceError, fetch_single_record
from altegio_bot.easyweek_migration.classify import READY, Decision, classify_record
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.cutover import Cutover
from altegio_bot.easyweek_migration.manifest import MigrationManifest

logger = logging.getLogger("easyweek_migration.reproof")

# Stable, PII-free outcomes.
REPROOF_CONFIRMED: Final = "source_reproof_confirmed"
# The booking still exists but is no longer the booking that was approved:
# cancelled, deleted, completed, rescheduled, reassigned, repriced, re-serviced.
REPROOF_SOURCE_CHANGED: Final = "source_changed_after_plan"
# The booking could not be re-read or re-derived at all.
REPROOF_FAILED: Final = "source_reproof_failed"


@dataclass(frozen=True)
class ReproofResult:
    """Whether this exact booking may still be created, right now."""

    confirmed: bool
    reason: str
    # The reason the classifier itself gave, when it had one. Already a stable
    # code (`source_canceled`, `custom_price_unsupported`, …).
    detail: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {"reproof": self.reason, "reproof_detail": self.detail}


CONFIRMED: Final = ReproofResult(confirmed=True, reason=REPROOF_CONFIRMED)


async def reprove_source_booking(
    decision: Decision,
    *,
    manifest: MigrationManifest,
    directory: CustomerDirectory,
    cutover: Cutover,
    http_client: httpx.AsyncClient | None = None,
) -> ReproofResult:
    """Re-read and re-classify one approved booking. Read-only.

    Returns :data:`CONFIRMED` only when the live Altegio record still classifies
    as ``READY`` under the *same* manifest, customer directory and cutover, and
    still produces the *same* source fingerprint the reviewed plan recorded.

    Everything else is a refusal, and the caller must not claim or POST.
    """
    company_id = decision.source_company_id
    record_id = decision.source_record_id
    if record_id is None or decision.source_fingerprint is None:
        # Only a READY decision reaches here, and a READY decision always has
        # both. Refusing rather than asserting keeps a future caller honest.
        return ReproofResult(confirmed=False, reason=REPROOF_FAILED, detail="decision_incomplete")

    try:
        live = await fetch_single_record(company_id=company_id, record_id=record_id, client=http_client)
    except AltegioSourceError:
        # Unreachable or unreadable. "We could not check" is not "it is fine".
        logger.warning(
            "easyweek_migration: re-proof unreadable company_id=%s record_id=%s",
            company_id,
            record_id,
        )
        return ReproofResult(confirmed=False, reason=REPROOF_FAILED, detail="source_unreadable")

    if live is None:
        # Hard-deleted between the plan and now.
        return ReproofResult(confirmed=False, reason=REPROOF_SOURCE_CHANGED, detail="source_absent")

    # The record must still be the record we asked for. Altegio addresses it by
    # both ids, but a proxy, a cache or a future API change must not be able to
    # hand back somebody else's booking and have it migrated.
    live_id = live.get("id")
    if type(live_id) is not int or live_id != record_id:
        return ReproofResult(confirmed=False, reason=REPROOF_FAILED, detail="source_identity_mismatch")

    live_company = live.get("company_id")
    if live_company is not None and (type(live_company) is not int or live_company != company_id):
        return ReproofResult(confirmed=False, reason=REPROOF_FAILED, detail="source_company_mismatch")

    # Same inputs as the plan. A different manifest or cutover here would let the
    # re-proof approve something the operator never reviewed.
    fresh = classify_record(
        live,
        company_id=company_id,
        manifest=manifest,
        directory=directory,
        cutover=cutover,
        ledger=None,
    )

    if fresh.outcome != READY:
        # Cancelled, deleted, completed, moved into the past, newly blocked by a
        # changed price, duration, service or customer — the classifier's own
        # reason code says which, and it is already PII-free.
        return ReproofResult(confirmed=False, reason=REPROOF_SOURCE_CHANGED, detail=fresh.reason)

    if fresh.source_fingerprint != decision.source_fingerprint:
        # Still migratable, but not as the booking that was approved: the time,
        # master, service, duration or customer moved. It needs a new dry-run and
        # a new human look, not a silent write at the new values.
        return ReproofResult(confirmed=False, reason=REPROOF_SOURCE_CHANGED, detail="fingerprint_changed")

    return CONFIRMED


async def reclassify_source_for_resolution(
    *,
    company_id: int,
    record_id: int,
    expected_fingerprint: str,
    manifest: MigrationManifest,
    directory: CustomerDirectory,
    cutover: Cutover,
    http_client: httpx.AsyncClient | None = None,
) -> tuple[ReproofResult, Decision | None]:
    """Rebuild what the migration MEANT to create for one already-attempted row.

    The resolution paths (``resolve-created``, and reconcile when a target UUID
    is known) need the expected staff, service, customer, start time and duration
    so they can compare them against the live booking. Those live in the source,
    not in the ledger — the ledger deliberately stores a digest and no PII.

    Two things make this different from :func:`reprove_source_booking`:

    * it is keyed by the **stored** source fingerprint rather than by a Decision
      from a plan, because a row being resolved has no plan any more; and
    * it classifies with ``ledger=None`` on purpose. The row's own terminal
      status is ``uncertain`` or ``pending``, and passing that in would make the
      classifier return ``blocked: ledger_uncertain_needs_reconcile`` — the
      classifier refusing to help the very command whose job is to resolve it.
      Reading the source content is not the same question as reading the row's
      outcome, and only the first one is being asked here.

    The same manifest, wave selector, customer directory, cutover and price /
    duration rules as every other mode. Returns the fresh Decision only when the
    source still matches the fingerprint recorded before the original POST.
    """
    try:
        live = await fetch_single_record(company_id=company_id, record_id=record_id, client=http_client)
    except AltegioSourceError:
        return ReproofResult(confirmed=False, reason=REPROOF_FAILED, detail="source_unreadable"), None

    if live is None:
        return ReproofResult(confirmed=False, reason=REPROOF_SOURCE_CHANGED, detail="source_absent"), None

    live_id = live.get("id")
    if type(live_id) is not int or live_id != record_id:
        return ReproofResult(confirmed=False, reason=REPROOF_FAILED, detail="source_identity_mismatch"), None

    fresh = classify_record(
        live,
        company_id=company_id,
        manifest=manifest,
        directory=directory,
        cutover=cutover,
        ledger=None,
    )

    if fresh.outcome != READY:
        return ReproofResult(confirmed=False, reason=REPROOF_SOURCE_CHANGED, detail=fresh.reason), None

    if fresh.source_fingerprint != expected_fingerprint:
        # The booking still migrates, but not as the booking that was attempted:
        # resolving it against the old attempt would record a target that does
        # not describe the appointment any more.
        return ReproofResult(confirmed=False, reason=REPROOF_SOURCE_CHANGED, detail="fingerprint_changed"), None

    return CONFIRMED, fresh
