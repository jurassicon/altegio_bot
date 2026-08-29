"""CLI for the Altegio → EasyWeek cutover of future active bookings (PR-11.1).

    python -m altegio_bot.scripts.easyweek_migration <mode> [options]

Modes::

    inventory        read-only; what exists in Altegio and what the mapping covers
    dry-run          read-only; the reviewable plan whose digest gates apply
    apply            the ONLY mode that writes, and only through the full gate
    reconcile        resolve uncertain rows by reading EasyWeek; report the state
    rollback-dry-run read-only; what a rollback of one run WOULD cancel

**Dry-run is the default.** Running with no mode, or with a mode but without
``--apply``, cannot issue an EasyWeek mutation — that is a property of the code
path, not of an argument default: ``run_apply`` is the only function that calls
the write client, and it is unreachable without ``--apply`` surviving the gate.

Every mode prints one JSON report to stdout and, unless ``--no-write-report``,
saves it under ``--report-dir`` (default ``outputs/easyweek_migration``, which is
git-ignored). The report is PII-free by construction; see
:mod:`altegio_bot.easyweek_migration.report`.

Exit codes::

    0  the mode completed and its report is ready
    1  the run refused (bad configuration, failed gate, unreadable source)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Final

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_migration.customers import CustomerDirectory, load_customer_directory
from altegio_bot.easyweek_migration.cutover import CutoverError, parse_cutover, run_start_cutover
from altegio_bot.easyweek_migration.gates import ApplyGateError
from altegio_bot.easyweek_migration.manifest import load_manifest
from altegio_bot.easyweek_migration.report import MigrationReport, write_report
from altegio_bot.easyweek_migration.runner import (
    DEFAULT_HORIZON_DAYS,
    MODE_APPLY,
    MODE_DRY_RUN,
    MODE_INVENTORY,
    MODE_RECONCILE,
    MODE_ROLLBACK_DRY_RUN,
    RunInputs,
    new_run_id,
    run_apply,
    run_inventory_or_dry_run,
    run_reconcile,
    run_rollback,
    utcnow,
)
from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient

logger = logging.getLogger("easyweek_migration.cli")

DEFAULT_REPORT_DIR: Final = "outputs/easyweek_migration"

MODES: Final = (
    MODE_INVENTORY,
    MODE_DRY_RUN,
    MODE_APPLY,
    MODE_RECONCILE,
    MODE_ROLLBACK_DRY_RUN,
)

# The operator attestation. Spelled out in full, every time, on purpose: it is a
# claim about a system this process cannot inspect, and a short flag would make
# it feel like a formality.
CONFIRM_NATIVE_FLAG: Final = "--confirm-easyweek-native-notifications-disabled"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_migration",
        description="Migrate FUTURE ACTIVE Altegio bookings into EasyWeek. Dry-run by default.",
    )
    parser.add_argument(
        "mode",
        nargs="?",
        default=MODE_DRY_RUN,
        choices=MODES,
        help="what to do (default: dry-run, which writes nothing)",
    )
    parser.add_argument("--manifest", required=True, help="path to the location/staff/service mapping JSON")
    parser.add_argument(
        "--customer-directory",
        help="path to the EasyWeek customer export (CSV/JSON). PII: never commit it.",
    )
    parser.add_argument(
        "--cutover-at",
        help="immutable cutover instant, ISO-8601 WITH offset (e.g. 2026-09-01T00:00:00+02:00). Required for apply.",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=DEFAULT_HORIZON_DAYS,
        help=f"how far ahead to read Altegio bookings (default {DEFAULT_HORIZON_DAYS})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually issue EasyWeek mutations. Without it nothing is ever written.",
    )
    parser.add_argument(
        "--verified-dry-run-id",
        help="the plan_digest printed by the dry-run that was reviewed. Required for apply.",
    )
    parser.add_argument(
        CONFIRM_NATIVE_FLAG,
        dest="confirm_native_notifications_disabled",
        action="store_true",
        help=(
            "attest that EVERY native EasyWeek customer channel (email, SMS, push, WhatsApp, "
            "automatic confirmations, reminders and change notices) has been turned off by hand "
            "in the EasyWeek UI. Required for apply."
        ),
    )
    parser.add_argument(
        "--canary-notification-observed",
        action="store_true",
        help="declare that the canary produced an unexpected customer notification. Halts every apply.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="create at most N bookings this run. This is how a canary is run.",
    )
    parser.add_argument("--rollback-run-id", help="the run_id whose created bookings a rollback would target")
    parser.add_argument(
        "--confirm-rollback",
        action="store_true",
        help="with --apply, actually cancel the rollback candidates. Without it rollback is read-only.",
    )
    parser.add_argument("--report-dir", default=DEFAULT_REPORT_DIR, help="where the JSON report is saved")
    parser.add_argument("--no-write-report", action="store_true", help="print the report but do not save it")
    return parser


def _fail(message: str) -> int:
    print(f"easyweek_migration: refused: {message}", file=sys.stderr)
    return 1


async def _run(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    if not manifest.valid:
        return _fail(f"manifest is unusable ({manifest.reason})")

    # The customer directory is only needed where a customer must be resolved.
    # `inventory` deliberately runs without it, so an operator can build the
    # mapping before the export exists.
    if args.customer_directory:
        directory = load_customer_directory(args.customer_directory)
        if not directory.valid:
            return _fail(f"customer directory is unusable ({directory.reason})")
    else:
        if args.mode in (MODE_DRY_RUN, MODE_APPLY):
            return _fail("--customer-directory is required for dry-run and apply")
        directory = CustomerDirectory(valid=True, by_phone={})

    try:
        if args.cutover_at:
            cutover = parse_cutover(args.cutover_at)
        elif args.mode == MODE_APPLY:
            # An apply's boundary must be a value a second person can check.
            return _fail("--cutover-at is required for apply")
        else:
            cutover = run_start_cutover(utcnow())
    except CutoverError as exc:
        return _fail(str(exc))

    if args.horizon_days < 1:
        return _fail("--horizon-days must be >= 1")
    if args.limit is not None and args.limit < 1:
        return _fail("--limit must be >= 1")

    run_id = new_run_id()
    inputs = RunInputs(
        mode=args.mode,
        run_id=run_id,
        cutover=cutover,
        manifest=manifest,
        directory=directory,
        horizon_days=args.horizon_days,
        apply_requested=bool(args.apply),
        native_notifications_confirmed=bool(args.confirm_native_notifications_disabled),
        cutover_supplied=bool(args.cutover_at),
        verified_dry_run_id=args.verified_dry_run_id,
        canary_notification_observed=bool(args.canary_notification_observed),
        limit=args.limit,
        rollback_run_id=args.rollback_run_id,
        # A rollback cancels real appointments, so it needs BOTH the general
        # write flag and its own confirmation. Either one alone leaves it
        # read-only.
        rollback_confirmed=bool(args.apply and args.confirm_rollback),
    )

    report: MigrationReport
    try:
        if args.mode in (MODE_INVENTORY, MODE_DRY_RUN):
            async with SessionLocal() as session:
                report = await run_inventory_or_dry_run(session, inputs)
        elif args.mode == MODE_APPLY:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_apply(SessionLocal, inputs, write_client=client)
        elif args.mode == MODE_RECONCILE:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_reconcile(SessionLocal, inputs, write_client=client)
        else:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_rollback(SessionLocal, inputs, write_client=client)
    except ApplyGateError as exc:
        # The gate's refusals are the most important thing this tool ever prints.
        for failure in exc.failures:
            print(f"easyweek_migration: gate refused: {failure}", file=sys.stderr)
        return _fail("apply gate did not pass; no EasyWeek mutation was attempted")
    except Exception as exc:  # noqa: BLE001 — the message is a type name, never a payload
        return _fail(f"{type(exc).__name__}: {exc}")

    print(report.to_json())
    if not args.no_write_report:
        path = write_report(report, args.report_dir)
        print(f"easyweek_migration: report written to {path}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    if args.report_dir:
        Path(args.report_dir).parent.mkdir(parents=True, exist_ok=True)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
