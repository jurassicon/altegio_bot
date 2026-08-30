"""Machine-readable, PII-free reporting for the cutover (PR-11.1).

Every mode ends with one JSON document. It is the artefact an operator reviews
before approving an apply, the evidence that a canary was clean, and the
reconciliation record kept after the migration is over — so it has to be
readable by a person, diffable by a machine, and safe to paste into a ticket.

What may appear here: counts, stable reason codes, Altegio company ids, Altegio
record ids, EasyWeek booking/location/staff/service UUIDs, UTC instants, booleans
and flag names.

What may never appear: a phone number, a customer name, an e-mail, a service or
comment text, a raw Altegio or EasyWeek payload, an API key, a webhook token, or
the contents of the customer directory. That list is not defensive habit — the
report is written to disk, kept, and shared, and there is nothing in a
reconciliation that a phone number would improve.

The ``plan_digest`` is the report's other job: it is what the apply gate compares
against, so a plan an operator reviewed cannot silently become a different plan.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from altegio_bot.easyweek_migration.classify import (
    ALREADY_MIGRATED,
    BLOCKED,
    READY,
    SKIPPED,
    Decision,
)

# Outcomes that only exist once a mutation has been attempted.
CREATED = "created"
UNCERTAIN = "uncertain"
FAILED = "failed"


def plan_digest(decisions: list[Decision], *, cutover_iso: str, manifest_digest: str) -> str:
    """Digest of the exact set of bookings an apply would create.

    Covers only ``ready`` rows and their proven targets: those are what a
    mutation would touch, and they are what an operator actually reviewed. A
    booking that is blocked or already migrated moving in or out of the plan does
    not invalidate the review, but a change to *what would be created* does.

    The cutover and the manifest are folded in because the same ready set under a
    different cutover, or pointing at a different EasyWeek branch, is a different
    plan wearing the same rows.
    """
    entries = sorted(
        (
            (
                decision.source_company_id,
                decision.source_record_id,
                decision.source_fingerprint or "",
            )
            for decision in decisions
            if decision.outcome == READY
        )
    )
    blob = json.dumps(
        {
            "cutover_at": cutover_iso,
            "manifest_digest": manifest_digest,
            "ready": entries,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass
class MigrationReport:
    """The whole run, as counts and codes."""

    mode: str
    run_id: str
    cutover: dict[str, Any] = field(default_factory=dict)
    manifest: dict[str, Any] = field(default_factory=dict)
    customer_directory: dict[str, Any] = field(default_factory=dict)
    gate: dict[str, Any] | None = None
    plan_digest: str = ""
    # What a canary proof would be bound to, for the canary mode's report.
    canary_binding: dict[str, Any] | None = None
    # Completeness verdict of a final reconciliation.
    completeness: dict[str, Any] | None = None
    # Inventory only: which Altegio staff/service ids the source actually uses,
    # and which of them the manifest already covers.
    source_identifiers: dict[str, Any] | None = None
    # Per branch and per Altegio staff id: active bookings, and which wave
    # each master belongs to. The operator's cross-check against Altegio.
    wave: dict[str, Any] | None = None
    # The durable identity of the migration wave this run is about, and
    # whether the run's arguments actually describe it.
    scope: dict[str, Any] | None = None

    # Source-side truth.
    source_records_fetched: Counter = field(default_factory=Counter)
    source_active_bookings: int = 0

    # Outcome tallies.
    outcomes: Counter = field(default_factory=Counter)
    reasons: Counter = field(default_factory=Counter)
    by_company: dict[int, Counter] = field(default_factory=dict)

    # Rows a human has to act on. Ids only.
    blocked_rows: list[dict[str, Any]] = field(default_factory=list)
    uncertain_rows: list[dict[str, Any]] = field(default_factory=list)
    created_rows: list[dict[str, Any]] = field(default_factory=list)
    failed_rows: list[dict[str, Any]] = field(default_factory=list)

    errors: list[str] = field(default_factory=list)
    mutations_attempted: int = 0

    def note_source(self, company_id: int, fetched: int) -> None:
        self.source_records_fetched[company_id] += fetched

    def note(self, decision: Decision, *, outcome: str | None = None, reason: str | None = None) -> None:
        """Record one row's outcome.

        ``outcome``/``reason`` override the classifier's when a mutation moved the
        row on (``ready`` → ``created`` / ``uncertain`` / ``failed``).
        """
        final_outcome = outcome or decision.outcome
        final_reason = reason if reason is not None else decision.reason

        self.outcomes[final_outcome] += 1
        if final_reason:
            self.reasons[final_reason] += 1

        company = self.by_company.setdefault(decision.source_company_id, Counter())
        company[final_outcome] += 1

        # "Active" means the source booking was in scope for this cutover: in a
        # migratable branch, not cancelled, not finished, starting after the
        # cutover. Everything except `skipped` qualifies — including the rows a
        # human has to fix, which is exactly why the count and the blocked list
        # are reported side by side.
        if final_outcome != SKIPPED:
            self.source_active_bookings += 1

        entry = decision.as_safe_dict()
        entry["outcome"] = final_outcome
        entry["reason"] = final_reason
        if final_outcome == BLOCKED:
            self.blocked_rows.append(entry)
        elif final_outcome == UNCERTAIN:
            self.uncertain_rows.append(entry)
        elif final_outcome == CREATED:
            self.created_rows.append(entry)
        elif final_outcome == FAILED:
            self.failed_rows.append(entry)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "run_id": self.run_id,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "plan_digest": self.plan_digest,
            **self.cutover,
            "manifest": self.manifest,
            "customer_directory": self.customer_directory,
            "gate": self.gate,
            "canary_binding": self.canary_binding,
            "completeness": self.completeness,
            "source_identifiers": self.source_identifiers,
            "wave": self.wave,
            "scope": self.scope,
            "mutations_attempted": self.mutations_attempted,
            "source": {
                "provider": "altegio",
                "records_fetched_by_company": {str(k): v for k, v in sorted(self.source_records_fetched.items())},
                "active_bookings_considered": self.source_active_bookings,
            },
            "totals": {
                "ready": self.outcomes.get(READY, 0),
                "created": self.outcomes.get(CREATED, 0),
                "already_migrated": self.outcomes.get(ALREADY_MIGRATED, 0),
                "blocked": self.outcomes.get(BLOCKED, 0),
                "uncertain": self.outcomes.get(UNCERTAIN, 0),
                "failed": self.outcomes.get(FAILED, 0),
                "skipped": self.outcomes.get(SKIPPED, 0),
            },
            "by_company": {
                str(company_id): dict(sorted(counts.items())) for company_id, counts in sorted(self.by_company.items())
            },
            "reason_codes": dict(sorted(self.reasons.items())),
            "blocked_rows": self.blocked_rows,
            "uncertain_rows": self.uncertain_rows,
            "created_rows": self.created_rows,
            "failed_rows": self.failed_rows,
            "errors": list(self.errors),
        }

    def to_json(self) -> str:
        return json.dumps(self.as_safe_dict(), indent=2, sort_keys=False, ensure_ascii=False)


def write_report(report: MigrationReport, directory: str | Path) -> Path:
    """Persist the report under ``<directory>/<mode>-<run_id>.json``.

    The default directory is ``outputs/``, which is git-ignored: a report is safe
    to share but is still an operational artefact of a specific cutover, and the
    repository is not where it belongs.
    """
    target_dir = Path(directory)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{report.mode}-{report.run_id}.json"
    path.write_text(report.to_json() + "\n", encoding="utf-8")
    return path
