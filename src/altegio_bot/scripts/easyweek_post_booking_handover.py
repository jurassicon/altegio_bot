"""CLI for the post-booking marketing handover (plan §31).

    python -m altegio_bot.scripts.easyweek_post_booking_handover <mode> [options]

Modes::

    plan     read the ledger and the jobs, prove the §30 handover and the wave
             closure, and freeze a snapshot. Writes no database row, sends
             nothing and calls no API at all.
    apply    ONE PostgreSQL transaction against that exact snapshot: withdraw
             the open Altegio `review_3d` / `repeat_10d` / `comeback_3d` of
             migrated bookings and record that their ownership moved. Creates
             NOTHING — no EasyWeek job, no OutboxMessage, no message.
    verify   prove the end state. Read-only.

Why this is a separate tool from the reminder handover
------------------------------------------------------
§30 proves that timed EasyWeek reminders were created. This proves the opposite
kind of fact: that Altegio marketing obligations were given up with nothing
created in their place, because a migrated future booking is not evidence of a
completed visit. The two have separate snapshots, digests, phrases, environment
flags and markers, so a review of one can never authorise the other.

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
from pathlib import Path
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_migration.manifest import load_manifest
from altegio_bot.easyweek_migration.post_booking_handover import (
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    STOP_SNAPSHOT_INVALIDATED,
    PostBookingSnapshotError,
    check_snapshot_usable,
    confirmation_phrase,
    invalidate_snapshot,
    read_apply_report,
    read_snapshot,
    write_apply_report,
    write_snapshot,
)
from altegio_bot.easyweek_migration.post_booking_handover_db import (
    PostBookingHandoverError,
    apply_plan,
    build_plan,
    verify_handover,
)
from altegio_bot.easyweek_migration.reminder_handover import validate_run_ids

logger = logging.getLogger("easyweek_migration.post_booking_handover.cli")

MODE_PLAN: Final = "plan"
MODE_APPLY: Final = "apply"
MODE_VERIFY: Final = "verify"
MODES: Final = (MODE_PLAN, MODE_APPLY, MODE_VERIFY)

DEFAULT_SNAPSHOT: Final = (
    os.environ.get("EASYWEEK_POST_BOOKING_HANDOVER_SNAPSHOT") or "outputs/easyweek_post_booking_handover/plan.json"
)
DEFAULT_APPLY_REPORT: Final = (
    os.environ.get("EASYWEEK_POST_BOOKING_HANDOVER_APPLY_REPORT")
    or "outputs/easyweek_post_booking_handover/apply-report.json"
)

# Its own environment flag. The §30 one must not authorise this write, and this
# one must not authorise §30's: they are different transactions with different
# consequences.
APPLY_ENV_FLAG: Final = "EASYWEEK_POST_BOOKING_HANDOVER_ALLOW_APPLY"

# Suffixes an operator may reach for when a plan has been superseded. Refused by
# name as well as by content.
ARCHIVE_SUFFIXES: Final = (".invalidated", ".tombstone", ".bak", ".old")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_post_booking_handover",
        description="Withdraw the Altegio marketing follow-ups of migrated bookings.",
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
        "--run-id",
        action="append",
        default=[],
        required=True,
        metavar="MIGRATION_RUN_ID",
        help="origin migration run id. Repeatable; nothing is in scope by default.",
    )
    parser.add_argument("--manifest", required=True, help="the migration manifest for this wave")
    parser.add_argument("--snapshot", default=DEFAULT_SNAPSHOT)
    parser.add_argument("--apply-report", default=DEFAULT_APPLY_REPORT)
    parser.add_argument("--plan-digest", default=None, help="the digest printed by plan")
    parser.add_argument("--confirm", default=None, help="the exact confirmation phrase printed by plan")
    parser.add_argument(
        "--max-snapshot-age-sec",
        type=int,
        default=DEFAULT_MAX_SNAPSHOT_AGE_SEC,
        help="refuse a snapshot older than this",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform the withdrawal. Needs the mode, the digest, the phrase and the environment flag.",
    )
    return parser


def build_pre_parser() -> argparse.ArgumentParser:
    """Answers two questions before the real parser runs: is this a plan, and which file.

    It mirrors the real parser's option ARITY, so an option's value can never be
    mistaken for the mode. Everything is optional and nothing is validated: a
    plan whose `--company-id` is malformed is exactly the command that must
    still be recognised as a plan attempt.
    """
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("mode", nargs="?", default=None)
    parser.add_argument("--snapshot", default=None)
    for option in ("--company-id", "--run-id", "--manifest", "--apply-report"):
        parser.add_argument(option, action="append", default=[])
    for option in ("--plan-digest", "--confirm", "--max-snapshot-age-sec"):
        parser.add_argument(option, default=None)
    return parser


def _print(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _fail(message: str) -> int:
    print(f"easyweek_post_booking_handover: refused: {message}", file=sys.stderr)
    return 1


def _apply_permitted(args: argparse.Namespace) -> bool:
    """Both halves, or nothing. Neither one alone is authorisation."""
    env = (os.environ.get(APPLY_ENV_FLAG) or "").strip().lower()
    return bool(args.apply) and env == "true"


def _intended_plan_snapshot(argv: list[str] | None) -> str | None:
    """The snapshot a plan command would replace, or ``None`` if it is not one.

    Answered before `parse_args`, because a plan whose arguments do not parse is
    still a plan attempt: the operator has decided the previous permission is
    superseded. `--help` is not an attempt, and neither is an apply or a verify
    whose option value happens to be the string "plan".
    """
    tokens = list(sys.argv[1:] if argv is None else argv)
    if any(token in ("-h", "--help") for token in tokens):
        return None
    try:
        parsed, _unknown = build_pre_parser().parse_known_args(tokens)
    except SystemExit:
        return None
    mode = parsed.mode if parsed.mode in MODES else (MODE_PLAN if parsed.mode is None else None)
    if mode != MODE_PLAN:
        return None
    return parsed.snapshot or DEFAULT_SNAPSHOT


def _invalidate_previous_plan(path: str, *, reason: str) -> str | None:
    try:
        invalidate_snapshot(path, reason=reason)
    except OSError:
        # Fail closed: an old permission we could not destroy is exactly the one
        # that must not survive quietly.
        return "snapshot_invalidation_failed"
    return None


async def _run(args: argparse.Namespace) -> int:
    if args.mode == MODE_PLAN:
        if failure := _invalidate_previous_plan(args.snapshot, reason="superseded_by_new_plan"):
            return _fail(failure)
    if args.mode in (MODE_APPLY, MODE_VERIFY) and Path(args.snapshot).suffix in ARCHIVE_SUFFIXES:
        # Before the database session, before the manifest, before anything.
        return _fail(STOP_SNAPSHOT_INVALIDATED)
    if not 1 <= args.max_snapshot_age_sec <= DEFAULT_MAX_SNAPSHOT_AGE_SEC:
        return _fail("snapshot_age_limit_invalid")

    companies = tuple(sorted(set(args.company_id)))
    if not companies:
        return _fail("--company-id is required; nothing is in scope by default")
    try:
        runs = tuple(validate_run_ids(sorted(set(args.run_id))))
    except Exception:
        return _fail("migration_run_scope_invalid")

    if args.mode == MODE_PLAN:
        manifest = load_manifest(args.manifest)
        if not manifest.valid:
            return _fail(f"manifest is unusable ({manifest.reason})")
        try:
            async with SessionLocal() as session:
                plan = await build_plan(
                    session,
                    manifest=manifest,
                    company_ids=companies,
                    run_ids=runs,
                )
        except PostBookingHandoverError as error:
            return _fail(str(error))

        path = write_snapshot(plan, args.snapshot)
        report = plan.as_safe_dict()
        _print(report)
        print(f"easyweek_post_booking_handover: snapshot written to {path}", file=sys.stderr)
        if report["apply_ready"]:
            print(
                "easyweek_post_booking_handover: to apply this plan, pass\n"
                f"  --plan-digest {report['plan_digest']}\n"
                f"  --confirm '{confirmation_phrase(report['plan_digest'])}'",
                file=sys.stderr,
            )
        else:
            blockers = ", ".join(report["blockers"]) or "not_apply_ready"
            print(
                f"easyweek_post_booking_handover: this plan cannot be applied ({blockers}); "
                "resolve it and run plan again",
                file=sys.stderr,
            )
        return 0 if report["apply_ready"] else 1

    try:
        frozen = read_snapshot(args.snapshot)
    except PostBookingSnapshotError as error:
        return _fail(str(error))

    if tuple(sorted(frozen.company_ids)) != companies:
        return _fail("the snapshot was planned for a different set of companies; re-run plan")
    manifest = load_manifest(args.manifest)
    if not manifest.valid or manifest.digest != frozen.manifest_digest or tuple(runs) != frozen.run_ids:
        return _fail("migration_wave_changed")

    if args.mode == MODE_VERIFY:
        try:
            report = read_apply_report(args.apply_report, frozen=frozen)
        except PostBookingSnapshotError as error:
            return _fail(str(error))
        async with SessionLocal() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                findings = await verify_handover(session, frozen, report)
        _print(findings)
        return 0 if findings["passed"] else 1

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
    except PostBookingSnapshotError as error:
        return _fail(str(error))

    async with SessionLocal() as session:
        async with session.begin():
            result = await apply_plan(session, frozen, now=now, max_age_sec=args.max_snapshot_age_sec)
            if result.halted is not None:
                # The savepoint already rolled the work back; the outer
                # transaction is rolled back here so nothing at all is written.
                await session.rollback()
    _print(result.as_safe_dict())
    if result.halted is not None:
        return _fail(f"halted ({result.halted}); nothing was changed")
    path = write_apply_report(result.apply_report(frozen, applied_at=now), args.apply_report)
    print(f"easyweek_post_booking_handover: apply report written to {path}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    intended = _intended_plan_snapshot(argv)
    if intended is not None:
        if failure := _invalidate_previous_plan(intended, reason="superseded_by_new_plan"):
            return _fail(failure)
    args = build_parser().parse_args(argv)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    print(
        f"easyweek_post_booking_handover: snapshot path {args.snapshot} names real bookings; "
        "it is not committed and must not be shared",
        file=sys.stderr,
    )
    try:
        return asyncio.run(_run(args))
    except (PostBookingSnapshotError, PostBookingHandoverError) as error:
        return _fail(str(error))
    except SQLAlchemyError:
        return _fail("database_error")
    except OSError:
        return _fail("private_artifact_io_error")
    except Exception:
        return _fail("post_booking_handover_unexpected_error")


if __name__ == "__main__":
    raise SystemExit(main())
