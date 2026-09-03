"""Put selected EasyWeek template rows back the way a reconcile apply found them.

``reconcile_easyweek_templates --apply`` overwrites rows, and the state it
overwrote exists nowhere else afterwards. Reverting the code brings back the old
source contract but not the old rows, and running the ordinary apply against the
old code would simply write that contract again — a rollback has to restore what
was there, not re-derive something.

So the apply records a snapshot first, and this command replays it. Nothing else:
it reads one artefact, checks that the rows still look like the ones that
snapshot describes, and writes them back.

**Read-only by default.** ``--apply`` is the only path to a write, and every
restored row is written in one transaction.

**It refuses rather than overwrites.** If a row changed again after the apply —
somebody edited it by hand, a second reconcile ran — this command stops. The
snapshot describes a state that no longer follows from what is in the database,
and quietly stamping the old text over an unrelated edit would destroy work
nobody has looked at.

**A row the apply CREATED is deactivated, never deleted.** Its id is referenced
by outbox rows and by whatever an operator is about to read while investigating;
a deactivated row is inert to the send path and still there to look at.

**What this is not.** It is not a revision manager, it does not execute code from
another git revision, it never touches a sender, another provider, another branch
or another language, and restoring an old body is NOT evidence that the row
matches today's approved Meta content. Re-opening sends stays a separate decision
with its own preflight.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate
from altegio_bot.scripts.reconcile_easyweek_templates import (
    RECONCILABLE_CODES,
    SNAPSHOT_VERSION,
    ReconcileError,
)

ACTION_RESTORE: Final = "restore"
ACTION_DEACTIVATE: Final = "deactivate"
ACTION_UNCHANGED: Final = "unchanged"

BLOCK_SNAPSHOT_UNREADABLE: Final = "snapshot_unreadable"
BLOCK_SNAPSHOT_VERSION: Final = "snapshot_version_unsupported"
BLOCK_SNAPSHOT_SHAPE: Final = "snapshot_shape_invalid"
BLOCK_SNAPSHOT_SCOPE: Final = "snapshot_outside_supported_scope"
BLOCK_ROW_VANISHED: Final = "row_no_longer_exists"
BLOCK_ROW_DUPLICATED: Final = "rows_duplicated"
BLOCK_ROW_CHANGED_SINCE: Final = "row_changed_after_the_apply"

ERROR_CONFIGURATION: Final = "configuration_error"
ERROR_UNEXPECTED: Final = "unexpected_error"


@dataclass(frozen=True)
class RestorePlan:
    company_id: int
    code: str
    language: str
    action: str
    blocked_by: str | None = None

    @property
    def writes(self) -> bool:
        return self.blocked_by is None and self.action != ACTION_UNCHANGED


@dataclass
class RestoreReport:
    apply: bool = False
    snapshot: str = ""
    captured_at_utc: str | None = None
    scope: dict[str, Any] = field(default_factory=dict)
    plans: list[RestorePlan] = field(default_factory=list)
    mutations_attempted: int = 0
    config_error: str | None = None

    @property
    def blockers(self) -> list[str]:
        seen: list[str] = []
        if self.config_error is not None:
            seen.append(self.config_error)
        for plan in self.plans:
            if plan.blocked_by is not None and plan.blocked_by not in seen:
                seen.append(plan.blocked_by)
        return seen

    @property
    def blocked(self) -> bool:
        return bool(self.blockers)

    def as_safe_dict(self) -> dict[str, Any]:
        actions: dict[str, int] = {}
        for plan in self.plans:
            key = plan.blocked_by or plan.action
            actions[key] = actions.get(key, 0) + 1
        return {
            "mode": "apply" if self.apply else "dry-run",
            "send_authorized": False,
            "mutations_attempted": self.mutations_attempted,
            "snapshot": self.snapshot,
            "captured_at_utc": self.captured_at_utc,
            "scope": self.scope,
            "actions": dict(sorted(actions.items())),
            "blockers": self.blockers,
            "rows": [
                {
                    "company_id": plan.company_id,
                    "code": plan.code,
                    "language": plan.language,
                    "action": plan.blocked_by or plan.action,
                }
                for plan in self.plans
            ],
            # A restored body is the body that was there before — not a claim
            # that it matches whatever Meta has approved today.
            "note": "restores previous rows only; not a Meta contract proof and not a send authorization",
        }


def load_snapshot(path: Path) -> dict[str, Any]:
    """Read and structurally validate the artefact. Never trusts its shape."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReconcileError(f"{BLOCK_SNAPSHOT_UNREADABLE}: {path.name}") from exc
    if not isinstance(raw, dict):
        raise ReconcileError(f"{BLOCK_SNAPSHOT_SHAPE}: {path.name}")
    if raw.get("snapshot_version") != SNAPSHOT_VERSION:
        # An older file may simply lack a field this code now relies on.
        # Guessing what it omitted is how a restore writes the wrong row.
        raise ReconcileError(f"{BLOCK_SNAPSHOT_VERSION}: {path.name}")
    rows = raw.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ReconcileError(f"{BLOCK_SNAPSHOT_SHAPE}: {path.name}")
    for row in rows:
        if not isinstance(row, dict):
            raise ReconcileError(f"{BLOCK_SNAPSHOT_SHAPE}: {path.name}")
        if row.get("provider") != PROVIDER_EASYWEEK:
            raise ReconcileError(f"{BLOCK_SNAPSHOT_SCOPE}: {path.name}")
        if row.get("code") not in RECONCILABLE_CODES:
            # The same closed set the apply is limited to. A snapshot naming a
            # lifecycle or reminder code did not come from that command.
            raise ReconcileError(f"{BLOCK_SNAPSHOT_SCOPE}: {path.name}")
        if not isinstance(row.get("company_id"), int) or not isinstance(row.get("language"), str):
            raise ReconcileError(f"{BLOCK_SNAPSHOT_SHAPE}: {path.name}")
        if row.get("existed") is True:
            for key in ("id", "body", "meta_template_name", "is_active"):
                if key not in row:
                    raise ReconcileError(f"{BLOCK_SNAPSHOT_SHAPE}: {path.name}")
        elif row.get("existed") is not False:
            raise ReconcileError(f"{BLOCK_SNAPSHOT_SHAPE}: {path.name}")
    return raw


async def _rows_for(session: AsyncSession, entry: dict[str, Any]) -> list[MessageTemplate]:
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .where(MessageTemplate.company_id == entry["company_id"])
        .where(MessageTemplate.code == entry["code"])
        .where(MessageTemplate.language == entry["language"])
        .order_by(MessageTemplate.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


def _plan_for(entry: dict[str, Any], rows: list[MessageTemplate], *, contract_body: str | None) -> RestorePlan:
    """What this key needs, or why it may not be touched."""
    base = {"company_id": entry["company_id"], "code": entry["code"], "language": entry["language"]}

    if len(rows) > 1:
        return RestorePlan(**base, action=ACTION_UNCHANGED, blocked_by=BLOCK_ROW_DUPLICATED)

    if entry["existed"]:
        if not rows:
            # The row the snapshot describes is gone. Re-creating it would invent
            # an id and hide whatever removed it.
            return RestorePlan(**base, action=ACTION_UNCHANGED, blocked_by=BLOCK_ROW_VANISHED)
        row = rows[0]
        if row.id != entry["id"]:
            return RestorePlan(**base, action=ACTION_UNCHANGED, blocked_by=BLOCK_ROW_CHANGED_SINCE)
        already = (
            row.body == entry["body"]
            and (row.meta_template_name or "") == (entry["meta_template_name"] or "")
            and bool(row.is_active) == bool(entry["is_active"])
        )
        if already:
            # Idempotent: a second restore over a restored row is a no-op.
            return RestorePlan(**base, action=ACTION_UNCHANGED)
        if contract_body is not None and row.body not in (contract_body, entry["body"]):
            # The row holds neither what the apply wrote nor what it replaced.
            # Something else edited it, and that edit is not ours to discard.
            return RestorePlan(**base, action=ACTION_UNCHANGED, blocked_by=BLOCK_ROW_CHANGED_SINCE)
        return RestorePlan(**base, action=ACTION_RESTORE)

    # The key had no row before the apply.
    if not rows:
        return RestorePlan(**base, action=ACTION_UNCHANGED)
    row = rows[0]
    if not bool(row.is_active):
        return RestorePlan(**base, action=ACTION_UNCHANGED)
    if contract_body is not None and row.body != contract_body:
        # A created row that no longer holds what the apply wrote was edited
        # afterwards; deactivating it would silently retire someone's change.
        return RestorePlan(**base, action=ACTION_UNCHANGED, blocked_by=BLOCK_ROW_CHANGED_SINCE)
    return RestorePlan(**base, action=ACTION_DEACTIVATE)


def _contract_body(company_id: int, code: str) -> str | None:
    """What the apply would have written for this key, if it can be derived.

    Used only to tell "still holds what the apply wrote" from "someone changed it
    afterwards". It is never written by this command — the snapshot is the only
    source of a restored value.
    """
    from altegio_bot.easyweek_branches import BRANCH_PROFILES, branch_template_contract
    from altegio_bot.easyweek_locations import configured_easyweek_locations

    registry = configured_easyweek_locations()
    if not registry.ready:
        return None
    location = registry.locations.get(company_id)
    if location is None:
        return None
    profile = BRANCH_PROFILES.get(location.name.strip().casefold())
    if profile is None:
        return None
    contract = branch_template_contract(profile, code)
    return contract.raw_body if contract is not None else None


async def run_restore(session: AsyncSession, *, snapshot_path: Path, apply: bool = False) -> RestoreReport:
    """Plan the restore, and — only with ``apply`` — perform it."""
    snapshot = load_snapshot(snapshot_path)
    report = RestoreReport(
        apply=apply,
        snapshot=snapshot_path.name,
        captured_at_utc=snapshot.get("captured_at_utc"),
        scope=snapshot.get("scope", {}),
    )

    entries = snapshot["rows"]
    # EVERY key is planned before any write, so one unusable row stops the whole
    # restore instead of leaving half of it applied.
    for entry in entries:
        rows = await _rows_for(session, entry)
        report.plans.append(_plan_for(entry, rows, contract_body=_contract_body(entry["company_id"], entry["code"])))

    if not apply or report.blocked:
        return report

    for entry, plan in zip(entries, report.plans, strict=True):
        if not plan.writes:
            continue
        rows = await _rows_for(session, entry)
        row = rows[0]
        if plan.action == ACTION_DEACTIVATE:
            row.is_active = False
        else:
            row.body = entry["body"]
            row.meta_template_name = entry["meta_template_name"]
            row.is_active = bool(entry["is_active"])
        report.mutations_attempted += 1
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Restore EasyWeek template rows from a reconcile snapshot. "
            "Read-only by default; --apply is the only path to a write."
        )
    )
    parser.add_argument("--snapshot", required=True, metavar="PATH", help="Snapshot written by --apply.")
    parser.add_argument("--apply", action="store_true", help="Write the planned restore.")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        async with SessionLocal() as session:
            async with session.begin():
                report = await run_restore(session, snapshot_path=Path(args.snapshot), apply=args.apply)
                if not args.apply or report.blocked:
                    await session.rollback()
    except ReconcileError as exc:
        # Raised from strings written in this repository; no external value is
        # interpolated, so the message is the operator's diagnosis.
        print(
            {
                "mode": "apply" if args.apply else "dry-run",
                "send_authorized": False,
                "mutations_attempted": 0,
                "error": ERROR_CONFIGURATION,
                "detail": str(exc),
            }
        )
        return 1
    except Exception as exc:  # noqa: BLE001 — class name only, never the text
        print(
            {
                "mode": "apply" if args.apply else "dry-run",
                "send_authorized": False,
                "mutations_attempted": 0,
                "error": ERROR_UNEXPECTED,
                "detail": type(exc).__name__,
            }
        )
        return 1

    print(report.as_safe_dict())
    return 1 if report.blocked else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
