"""CLI for the post-migration reminder handover (plan §30).

    python -m altegio_bot.scripts.easyweek_reminder_handover <mode> [options]

Modes::

    plan     read the ledger, prove every migrated booking against the live
             EasyWeek API, work out what is owed, and freeze a snapshot.
             Writes no database row and sends nothing.
    apply    ONE PostgreSQL transaction against that exact snapshot: create the
             missing EasyWeek reminders, then withdraw the superseded Altegio
             ones. Makes no API call at all.
    verify   prove the end state. Read-only.

**Read-only by default.** ``plan`` is the default mode, and the mutation is not
hidden behind it: ``apply`` needs the mode, the ``--apply`` flag, the plan digest
and the exact confirmation phrase, plus an environment authorisation for the
one-off container. Any one of them alone is a refusal.

Why apply makes no API call
---------------------------
The live proof happens in ``plan``, while the outbox worker is still running.
By the time an operator stops the worker there is nothing left but a database
transaction, so the stop is measured in seconds rather than in one paced request
per migrated booking.

What this tool never does
-------------------------
It never calls Meta or Chatwoot, never writes an ``OutboxMessage``, never sends
a message, never plans a lifecycle, review, retention or campaign job, never
touches a job outside the frozen ledger scope, and never re-opens a cancelled or
failed reminder. After the handover, ordinary EasyWeek webhooks are again the
only thing that reschedules anything.

Exit codes::

    0  the mode completed and its report is clean
    1  the mode refused, or something still needs a person
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import text

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_migration.manifest import load_manifest
from altegio_bot.easyweek_migration.reminder_handover import (
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    SnapshotError,
    boundary_still_future,
    check_snapshot_usable,
    confirmation_phrase,
    read_apply_report,
    read_snapshot,
    write_apply_report,
    write_snapshot,
)
from altegio_bot.easyweek_migration.reminder_handover_db import (
    DEFAULT_PAUSE_SEC,
    HandoverError,
    apply_plan,
    build_plan,
    verify_handover,
)

logger = logging.getLogger("easyweek_migration.reminder_handover.cli")

MODE_PLAN: Final = "plan"
MODE_APPLY: Final = "apply"
MODE_VERIFY: Final = "verify"
MODES: Final = (MODE_PLAN, MODE_APPLY, MODE_VERIFY)

DEFAULT_SNAPSHOT: Final = (
    os.environ.get("EASYWEEK_REMINDER_HANDOVER_SNAPSHOT") or "outputs/easyweek_reminder_handover/plan.json"
)
DEFAULT_APPLY_REPORT: Final = (
    os.environ.get("EASYWEEK_REMINDER_HANDOVER_APPLY_REPORT") or "outputs/easyweek_reminder_handover/apply-report.json"
)

# The host-side half of the permission. Checked in addition to the typed flag,
# the digest and the phrase: the flag proves somebody meant it now, this proves
# the host is one where the handover is allowed at all.
APPLY_ENV_FLAG: Final = "EASYWEEK_REMINDER_HANDOVER_ALLOW_APPLY"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_reminder_handover",
        description="Hand future reminders over from migrated Altegio bookings to their EasyWeek twins.",
    )
    parser.add_argument("mode", nargs="?", default=MODE_PLAN, choices=MODES)
    parser.add_argument(
        "--company-id",
        action="append",
        type=int,
        default=[],
        required=True,
        metavar="ALTEGIO_COMPANY_ID",
        help="source Altegio company to include. Repeatable; nothing is in scope by default.",
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help=(
            "the migration manifest for this wave. It is what pairs each Altegio company with its "
            "EasyWeek location; the runtime registry then proves that location IS that branch."
        ),
    )
    parser.add_argument(
        "--snapshot",
        default=DEFAULT_SNAPSHOT,
        help="where the frozen plan lives. Holds real ids; written 0600 and never committed.",
    )
    parser.add_argument(
        "--apply-report",
        default=DEFAULT_APPLY_REPORT,
        help="durable PII-free apply evidence. Required by verify and written 0600 after a committed apply.",
    )
    parser.add_argument(
        "--pause-sec",
        type=float,
        default=DEFAULT_PAUSE_SEC,
        help=f"pause between sequential EasyWeek reads (default {DEFAULT_PAUSE_SEC}); the API allows 60/min.",
    )
    parser.add_argument(
        "--max-snapshot-age-sec",
        type=int,
        default=DEFAULT_MAX_SNAPSHOT_AGE_SEC,
        help=(
            f"how old a snapshot may be at apply time (default {DEFAULT_MAX_SNAPSHOT_AGE_SEC}s). "
            "Reminder obligations move with the clock, so an old plan describes a different world."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write. Without it, apply mode refuses. Never enough on its own.",
    )
    parser.add_argument("--plan-digest", help="the plan_digest printed by the plan run this authorises")
    parser.add_argument(
        "--confirm",
        help=(
            "the exact confirmation phrase for this plan: "
            f"'{confirmation_phrase('<PLAN_DIGEST>')}'. It carries the digest, so a phrase "
            "copied out of an earlier terminal cannot authorise today's plan."
        ),
    )
    return parser


def _fail(message: str) -> int:
    print(f"easyweek_reminder_handover: refused: {message}", file=sys.stderr)
    return 1


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _apply_permitted(args: argparse.Namespace) -> bool:
    """Both halves, or nothing. Neither one alone is authorisation."""
    env = (os.environ.get(APPLY_ENV_FLAG) or "").strip().lower()
    return bool(args.apply) and env == "true"


async def _run(args: argparse.Namespace) -> int:
    companies = tuple(sorted(set(args.company_id)))
    if not companies:
        return _fail("--company-id is required; nothing is in scope by default")

    if args.mode == MODE_PLAN:
        manifest = load_manifest(args.manifest)
        if not manifest.valid:
            return _fail(f"manifest is unusable ({manifest.reason})")

        client = EasyWeekClient()
        try:
            async with SessionLocal() as session:
                plan = await build_plan(
                    session,
                    manifest=manifest,
                    company_ids=companies,
                    client=client,
                    pause_sec=args.pause_sec,
                )
        except HandoverError as error:
            return _fail(str(error))
        finally:
            await client.aclose()

        path = write_snapshot(plan, args.snapshot)
        report = plan.as_safe_dict()
        _print(report)
        print(f"easyweek_reminder_handover: snapshot written to {path}", file=sys.stderr)
        print(
            "easyweek_reminder_handover: to apply this plan, pass\n"
            f"  --plan-digest {report['plan_digest']}\n"
            f"  --confirm '{confirmation_phrase(report['plan_digest'])}'",
            file=sys.stderr,
        )
        # A plan is informational: it exits 0 whenever it could read the world,
        # and says separately whether a cutover is possible.
        return 0 if report["cutover_ready"] else 1

    try:
        frozen = read_snapshot(args.snapshot)
    except SnapshotError as error:
        return _fail(str(error))

    if tuple(sorted(frozen.company_ids)) != companies:
        return _fail("the snapshot was planned for a different set of companies; re-run plan")

    if args.mode == MODE_VERIFY:
        try:
            apply_report = read_apply_report(args.apply_report, frozen=frozen)
        except SnapshotError as error:
            return _fail(str(error))
        async with SessionLocal() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                report = await verify_handover(session, frozen, apply_report)
        _print(report)
        return 0 if report["passed"] else 1

    # -- apply -------------------------------------------------------------
    if not _apply_permitted(args):
        return _fail(
            f"apply needs BOTH --apply and {APPLY_ENV_FLAG}=true. "
            "This permission does not allow sending a message or migrating a booking."
        )
    now = datetime.now(timezone.utc)
    try:
        check_snapshot_usable(
            frozen,
            supplied_digest=args.plan_digest,
            supplied_confirmation=args.confirm,
            now=now,
            max_age_sec=args.max_snapshot_age_sec,
        )
    except SnapshotError as error:
        return _fail(str(error))

    crossed = boundary_still_future(frozen.rows, now=now)
    if crossed is not None:
        return _fail(f"{crossed}: a planned reminder has passed its moment; re-run plan")

    async with SessionLocal() as session:
        transaction = await session.begin()
        try:
            result = await apply_plan(session, frozen, now=now)
            if result.halted is not None:
                await transaction.rollback()
            else:
                await transaction.commit()
        except Exception:
            await transaction.rollback()
            raise
    report = result.as_safe_dict()
    _print(report)
    if result.halted is not None:
        return _fail(f"halted ({result.halted}); nothing was changed")
    durable = result.apply_report(frozen, applied_at=now)
    path = write_apply_report(durable, args.apply_report)
    print(f"easyweek_reminder_handover: apply report written to {path}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    if args.pause_sec < 0:
        return _fail("--pause-sec must not be negative")
    if args.max_snapshot_age_sec < 1:
        return _fail("--max-snapshot-age-sec must be at least 1")
    print(
        f"easyweek_reminder_handover: snapshot path {args.snapshot} names real bookings; "
        "it is not committed and must not be shared",
        file=sys.stderr,
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
