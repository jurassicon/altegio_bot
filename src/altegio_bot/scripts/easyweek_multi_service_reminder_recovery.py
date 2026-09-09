"""Plan/apply/verify CLI for controlled PR-7.4 reminder recovery.

``plan`` is the default and is database read-only.  ``apply`` requires the
frozen snapshot, its exact digest and an exact confirmation phrase.  No mode
can call Meta, Chatwoot or an EasyWeek mutation endpoint.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import shlex
import sys
from datetime import datetime, timezone
from typing import Any, Final

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_multi_service_recovery import (
    DEFAULT_LIMIT,
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    DEFAULT_PAUSE_SEC,
    MAX_SNAPSHOT_AGE_SEC,
    MODE_APPLY,
    MODE_PLAN,
    MODE_VERIFY,
    RecoveryError,
    apply_recovery_plan,
    build_recovery_plan,
    check_apply_authorization,
    confirmation_phrase,
    read_apply_report,
    read_snapshot,
    verify_recovery,
    write_private_json,
    write_snapshot,
)

MODES: Final = (MODE_PLAN, MODE_APPLY, MODE_VERIFY)
DEFAULT_SNAPSHOT: Final = "/recovery/easyweek-multi-service-reminder-plan.json"
DEFAULT_APPLY_REPORT: Final = "/recovery/easyweek-multi-service-reminder-apply.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_multi_service_reminder_recovery",
        description="Recover only current EasyWeek exactly-two-service reminder obligations.",
        allow_abbrev=False,
    )
    parser.add_argument("mode", nargs="?", default=MODE_PLAN, choices=MODES)
    parser.add_argument("--snapshot")
    parser.add_argument("--apply-report", default=DEFAULT_APPLY_REPORT)
    parser.add_argument("--plan-digest")
    parser.add_argument("--confirm")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--pause-sec", type=float, default=DEFAULT_PAUSE_SEC)
    parser.add_argument(
        "--max-snapshot-age-sec",
        type=int,
        default=DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    )
    return parser


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _refuse(reason: str) -> int:
    print(f"easyweek_multi_service_reminder_recovery: refused: {reason}", file=sys.stderr)
    return 1


async def _run(args: argparse.Namespace) -> int:
    if args.limit < 1:
        return _refuse("limit_invalid")
    if not math.isfinite(args.pause_sec) or args.pause_sec < 0:
        return _refuse("api_pacing_invalid")
    if not 1 <= args.max_snapshot_age_sec <= MAX_SNAPSHOT_AGE_SEC:
        return _refuse("snapshot_age_limit_invalid")

    if args.mode == MODE_PLAN:
        snapshot_path = args.snapshot or DEFAULT_SNAPSHOT
        client = EasyWeekClient(max_attempts=1)
        try:
            async with SessionLocal() as session:
                plan = await build_recovery_plan(
                    session,
                    client=client,
                    limit=args.limit,
                    pause_sec=args.pause_sec,
                )
                # Defence in depth: even an accidental dirty ORM object cannot
                # turn plan into a write when the context exits.
                await session.rollback()
        finally:
            await client.aclose()
        path = write_snapshot(plan, snapshot_path)
        report = plan.safe_report()
        _print(report)
        print(f"snapshot written 0600: {path}", file=sys.stderr)
        print(
            "WARNING: snapshot contains real booking UUIDs; do not commit or publish it.",
            file=sys.stderr,
        )
        if report["apply_ready"]:
            print(
                "exact apply command:\n"
                "python -m altegio_bot.scripts.easyweek_multi_service_reminder_recovery apply "
                f"--snapshot {shlex.quote(str(path))} --apply-report {shlex.quote(args.apply_report)} "
                f"--plan-digest {plan.plan_digest} "
                f"--confirm {shlex.quote(confirmation_phrase(plan.plan_digest))}",
                file=sys.stderr,
            )
        return 0 if report["apply_ready"] else 1

    if not args.snapshot:
        return _refuse("--snapshot is required for apply and verify")
    frozen = read_snapshot(args.snapshot)
    if args.mode == MODE_VERIFY:
        apply_report = read_apply_report(args.apply_report, frozen=frozen)
        async with SessionLocal() as session:
            report = await verify_recovery(
                session,
                frozen=frozen,
                apply_report=apply_report,
            )
            await session.rollback()
        _print(report)
        return 0 if report["passed"] else 1

    now = datetime.now(timezone.utc)
    check_apply_authorization(
        frozen,
        supplied_digest=args.plan_digest,
        supplied_confirmation=args.confirm,
        now=now,
        max_age_sec=args.max_snapshot_age_sec,
    )
    client = EasyWeekClient(max_attempts=1)
    try:
        async with SessionLocal() as session:
            result = await apply_recovery_plan(
                session,
                frozen=frozen,
                client=client,
                pause_sec=args.pause_sec,
                max_age_sec=args.max_snapshot_age_sec,
            )
    finally:
        await client.aclose()
    report = result.report()
    write_private_json(report, args.apply_report)
    _print(report)
    print(f"apply report written 0600: {args.apply_report}", file=sys.stderr)
    return 0


async def async_main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return await _run(args)
    except RecoveryError as exc:
        return _refuse(str(exc))


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
