"""Plan/apply/verify CLI for the operator-only v1 -> v2 snapshot recovery.

``plan`` is the default and is database read-only.  ``apply`` requires the
frozen plan, its exact digest and an exact confirmation phrase, and replaces
exactly one JSONB key per proven record.  No mode can create a MessageJob or an
OutboxMessage, and no mode can call Meta, Chatwoot or an EasyWeek mutation
endpoint.

stdout carries aggregates, stable reason codes and technical record ids only.
The private plan file is the only artefact that holds booking UUIDs.
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
    write_private_json,
)
from altegio_bot.easyweek_snapshot_recovery import (
    apply_snapshot_recovery_plan,
    build_snapshot_recovery_plan,
    check_apply_authorization,
    confirmation_phrase,
    read_apply_report,
    read_plan,
    verify_snapshot_recovery,
    write_plan,
)

MODES: Final = (MODE_PLAN, MODE_APPLY, MODE_VERIFY)
DEFAULT_PLAN: Final = "/recovery/easyweek-multi-service-snapshot-plan.json"
DEFAULT_APPLY_REPORT: Final = "/recovery/easyweek-multi-service-snapshot-apply.json"
PROG: Final = "easyweek_multi_service_snapshot_recovery"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Migrate proven EasyWeek pair snapshots from version 1 to the current version 2 projection.",
        allow_abbrev=False,
    )
    parser.add_argument("mode", nargs="?", default=MODE_PLAN, choices=MODES)
    parser.add_argument("--plan")
    parser.add_argument("--apply-report", default=DEFAULT_APPLY_REPORT)
    parser.add_argument("--plan-digest")
    parser.add_argument("--confirm")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--pause-sec", type=float, default=DEFAULT_PAUSE_SEC)
    parser.add_argument("--max-snapshot-age-sec", type=int, default=DEFAULT_MAX_SNAPSHOT_AGE_SEC)
    return parser


COMPOSE_PROJECT: Final = "altegio_bot"
COMPOSE_FILES: Final = ("docker-compose.yml", "docker-compose.chatwoot-internal.yml")
COMPOSE_SERVICE: Final = "easyweek-multi-service-snapshot-recovery"


def apply_command(
    *,
    plan_path: str,
    apply_report: str,
    plan_digest: str,
    max_snapshot_age_sec: int,
) -> str:
    """The one command that actually works on the production host.

    Both production Compose files, the ops profile, a throwaway container and
    the container-side ``/recovery`` paths — nothing here depends on a system
    Python or on a host directory that does not exist.
    """
    files = " ".join(f"-f {name}" for name in COMPOSE_FILES)
    return (
        f"docker compose -p {COMPOSE_PROJECT} {files} --profile ops run --rm --build \\\n"
        f"  {COMPOSE_SERVICE} apply \\\n"
        f"  --plan {shlex.quote(plan_path)} \\\n"
        f"  --apply-report {shlex.quote(apply_report)} \\\n"
        f"  --plan-digest {plan_digest} \\\n"
        f"  --confirm {shlex.quote(confirmation_phrase(plan_digest))} \\\n"
        f"  --max-snapshot-age-sec {int(max_snapshot_age_sec)}"
    )


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _refuse(reason: str) -> int:
    print(f"{PROG}: refused: {reason}", file=sys.stderr)
    return 1


async def _run(args: argparse.Namespace) -> int:
    if args.limit < 1:
        return _refuse("limit_invalid")
    if not math.isfinite(args.pause_sec) or args.pause_sec < 0:
        return _refuse("api_pacing_invalid")
    if not 1 <= args.max_snapshot_age_sec <= MAX_SNAPSHOT_AGE_SEC:
        return _refuse("snapshot_age_limit_invalid")

    if args.mode == MODE_PLAN:
        plan_path = args.plan or DEFAULT_PLAN
        client = EasyWeekClient(max_attempts=1)
        try:
            async with SessionLocal() as session:
                plan = await build_snapshot_recovery_plan(
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
        path = write_plan(plan, plan_path)
        report = plan.safe_report()
        _print(report)
        print(f"plan written 0600: {path}", file=sys.stderr)
        print(
            "WARNING: the plan file contains real booking UUIDs; do not commit or publish it.",
            file=sys.stderr,
        )
        if report["apply_ready"]:
            # A bare `python -m ...` line would be a trap: this process is a
            # one-off container that is already gone, the module is not on the
            # host's Python, and /recovery only exists inside the container.
            # So the command printed here is the real Compose command, with
            # both production files, the ops profile and container paths.
            print(
                "exact apply command (run from the repository root on the host):\n"
                + apply_command(
                    plan_path=str(path),
                    apply_report=args.apply_report,
                    plan_digest=plan.plan_digest,
                    max_snapshot_age_sec=args.max_snapshot_age_sec,
                ),
                file=sys.stderr,
            )
        return 0 if report["apply_ready"] else 1

    if not args.plan:
        return _refuse("--plan is required for apply and verify")
    frozen = read_plan(args.plan)

    if args.mode == MODE_VERIFY:
        apply_report = read_apply_report(args.apply_report, frozen=frozen)
        client = EasyWeekClient(max_attempts=1)
        try:
            async with SessionLocal() as session:
                report = await verify_snapshot_recovery(
                    session,
                    frozen=frozen,
                    apply_report=apply_report,
                    client=client,
                    pause_sec=args.pause_sec,
                )
                await session.rollback()
        finally:
            await client.aclose()
        _print(report)
        return 0 if report["passed"] else 1

    check_apply_authorization(
        frozen,
        supplied_digest=args.plan_digest,
        supplied_confirmation=args.confirm,
        now=datetime.now(timezone.utc),
        max_age_sec=args.max_snapshot_age_sec,
    )
    client = EasyWeekClient(max_attempts=1)
    try:
        async with SessionLocal() as session:
            result = await apply_snapshot_recovery_plan(
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
