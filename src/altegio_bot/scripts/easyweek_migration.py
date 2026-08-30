"""CLI for the Altegio → EasyWeek cutover of future active bookings (PR-11.1).

    python -m altegio_bot.scripts.easyweek_migration <mode> [options]

Modes::

    inventory        read-only; which Altegio staff/service ids the future
                     bookings use, and which the manifest still misses. Runs on
                     an UNFINISHED manifest — that is what it is for.
    dry-run          read-only; the reviewable plan whose digest gates apply
    canary           creates ONE named booking, reads it back, and records the
                     durable proof a bulk apply requires
    apply            the bulk write, and only through the full gate INCLUDING a
                     matching canary proof
    reconcile        report the state; with --final, re-read the live source and
                     prove the cutover is complete (non-zero exit if it is not)
    resolve-created  resolve one unknown-outcome row against a booking UUID the
                     operator found, after the tool proves it
    resolve-absent   record that an operator verified the booking is NOT there;
                     the next apply will then create it
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
import os
import sys
from pathlib import Path
from typing import Final

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_migration.customers import CustomerDirectory, load_customer_directory
from altegio_bot.easyweek_migration.cutover import CutoverError, parse_cutover, run_start_cutover
from altegio_bot.easyweek_migration.gates import ApplyGateError
from altegio_bot.easyweek_migration.manifest import inventory_manifest, load_manifest
from altegio_bot.easyweek_migration.report import MigrationReport, write_report
from altegio_bot.easyweek_migration.runner import (
    DEFAULT_HORIZON_DAYS,
    MODE_APPLY,
    MODE_CANARY,
    MODE_DRY_RUN,
    MODE_INVENTORY,
    MODE_RECONCILE,
    MODE_RESOLVE_ABSENT,
    MODE_RESOLVE_CREATED,
    MODE_ROLLBACK_DRY_RUN,
    RunInputs,
    new_run_id,
    run_apply,
    run_canary,
    run_inventory_or_dry_run,
    run_reconcile,
    run_resolve_absent,
    run_resolve_created,
    run_rollback,
    utcnow,
)
from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient

logger = logging.getLogger("easyweek_migration.cli")

# Where reports land. The one-off compose service sets this to its mounted
# `/migration/reports`, so a containerised run writes to the host without the
# operator repeating a path on every command; a host run keeps `outputs/`.
DEFAULT_REPORT_DIR: Final = os.environ.get("EASYWEEK_MIGRATION_REPORT_DIR") or "outputs/easyweek_migration"

MODES: Final = (
    MODE_INVENTORY,
    MODE_DRY_RUN,
    MODE_CANARY,
    MODE_APPLY,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    MODE_RESOLVE_ABSENT,
    MODE_ROLLBACK_DRY_RUN,
)

# Modes that must see a COMPLETE manifest. `inventory` is deliberately absent:
# it exists to help build the mapping and so must run before one exists.
STRICT_MANIFEST_MODES: Final = (
    MODE_DRY_RUN,
    MODE_CANARY,
    MODE_APPLY,
    MODE_RECONCILE,
    # `resolve-created` re-classifies the source to rebuild the booking the
    # migration meant to create, so it needs the same complete manifest and
    # the same wave selector every other proving mode uses.
    MODE_RESOLVE_CREATED,
)
# Modes that resolve a customer and therefore need the EasyWeek export.
# `resolve-created` is here because without the directory there is no way to
# say which customer the booking was for — and an unchecked customer must not
# pass as a correct one.
CUSTOMER_DIRECTORY_MODES: Final = (MODE_DRY_RUN, MODE_CANARY, MODE_APPLY, MODE_RESOLVE_CREATED)
# Modes that CONTINUE a wave rather than start one. They prove or resolve
# rows another run created, so their scope is not theirs to choose: the
# cutover must be the exact value that wave was applied with, and a run-start
# default would quietly prove a different window.
CONTINUING_MODES: Final = (MODE_RECONCILE, MODE_RESOLVE_CREATED)

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
        help=(
            "immutable cutover instant, ISO-8601 WITH offset (e.g. 2026-09-01T00:00:00+02:00). "
            "Required for apply, canary, reconcile and resolve-created — for the latter two it must be "
            "the exact value the wave was applied with."
        ),
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=DEFAULT_HORIZON_DAYS,
        help=(
            f"how far ahead to read Altegio bookings (default {DEFAULT_HORIZON_DAYS}). "
            "Part of the wave identity: reconciliation must use the same value the wave was applied with."
        ),
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
        "--canary-company-id",
        type=int,
        help="Altegio company id of the ONE booking the canary creates. Required for canary.",
    )
    parser.add_argument(
        "--canary-record-id",
        type=int,
        help="Altegio record id of the ONE booking the canary creates. Required for canary.",
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="reconcile only: re-read the live source and PROVE the cutover is complete.",
    )
    parser.add_argument("--resolve-company-id", type=int, help="source company id of the row to resolve")
    parser.add_argument("--resolve-record-id", type=int, help="source record id of the row to resolve")
    parser.add_argument(
        "--target-uuid",
        help="resolve-created: the EasyWeek booking UUID the operator found. It is verified, not believed.",
    )
    parser.add_argument(
        "--i-verified-the-booking-does-not-exist-in-easyweek",
        dest="resolve_absent_acknowledged",
        action="store_true",
        help="resolve-absent, step 1 of 2: state that you looked in EasyWeek and it is not there.",
    )
    parser.add_argument(
        "--i-understand-the-next-apply-will-create-it",
        dest="resolve_absent_confirmed",
        action="store_true",
        help="resolve-absent, step 2 of 2: acknowledge that being wrong double-books a real customer.",
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
    # `inventory` reads an unfinished manifest on purpose — it is the mode that
    # tells an operator what still has to go into it. Every writing or reviewing
    # mode gets the strict, all-or-nothing parse.
    if args.mode in STRICT_MANIFEST_MODES or args.mode.startswith("rollback"):
        manifest = load_manifest(args.manifest)
    else:
        manifest = inventory_manifest(_read_text(args.manifest))
    if not manifest.valid:
        return _fail(f"manifest is unusable ({manifest.reason})")

    if args.customer_directory:
        directory = load_customer_directory(args.customer_directory)
        if not directory.valid:
            return _fail(f"customer directory is unusable ({directory.reason})")
    else:
        if args.mode in CUSTOMER_DIRECTORY_MODES:
            return _fail(f"--customer-directory is required for {args.mode}")
        if args.mode == MODE_RECONCILE:
            # A final reconciliation re-classifies the live source, and the
            # everyday one proves any row whose target UUID is known — both need
            # to resolve customers.
            if args.final:
                return _fail("--customer-directory is required for reconcile --final")
            print(
                "easyweek_migration: no --customer-directory: rows with a known target UUID "
                "cannot be proven and will stay uncertain",
                file=sys.stderr,
            )
        directory = CustomerDirectory(valid=True, by_phone={})

    writes = args.mode in (MODE_CANARY, MODE_APPLY) or (args.mode.startswith("rollback") and args.apply)
    try:
        if args.cutover_at:
            cutover = parse_cutover(args.cutover_at)
        elif writes:
            # A write's boundary must be a value a second person can check.
            return _fail(f"--cutover-at is required for {args.mode}")
        elif args.mode in CONTINUING_MODES:
            # These commands continue a wave somebody else started, and the
            # cutover decides which bookings are even in it. Defaulting to "now"
            # was a silent narrowing: a booking earlier today became
            # `starts_before_cutover`, its EasyWeek target was never fetched, and
            # a deleted target could not fail a check that never looked at it.
            return _fail(
                f"--cutover-at is required for {args.mode}: it must be the exact value the wave was applied with"
            )
        else:
            cutover = run_start_cutover(utcnow())
    except CutoverError as exc:
        return _fail(str(exc))

    if args.horizon_days < 1:
        return _fail("--horizon-days must be >= 1")
    if args.mode == MODE_CANARY and (args.canary_company_id is None or args.canary_record_id is None):
        return _fail("canary requires --canary-company-id and --canary-record-id")

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
        canary_company_id=args.canary_company_id,
        canary_record_id=args.canary_record_id,
        rollback_run_id=args.rollback_run_id,
        # A rollback cancels real appointments, so it needs BOTH the general
        # write flag and its own confirmation. Either one alone leaves it
        # read-only.
        rollback_confirmed=bool(args.apply and args.confirm_rollback),
        resolve_company_id=args.resolve_company_id,
        resolve_record_id=args.resolve_record_id,
        resolve_target_booking_uuid=args.target_uuid,
        resolve_absent_acknowledged=bool(args.resolve_absent_acknowledged),
        resolve_absent_confirmed=bool(args.resolve_absent_confirmed),
        final=bool(args.final),
    )

    report: MigrationReport
    try:
        if args.mode in (MODE_INVENTORY, MODE_DRY_RUN):
            async with SessionLocal() as session:
                report = await run_inventory_or_dry_run(session, inputs)
        elif args.mode == MODE_CANARY:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_canary(SessionLocal, inputs, write_client=client)
        elif args.mode == MODE_APPLY:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_apply(SessionLocal, inputs, write_client=client)
        elif args.mode == MODE_RECONCILE:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_reconcile(SessionLocal, inputs, write_client=client)
        elif args.mode == MODE_RESOLVE_CREATED:
            async with EasyWeekMigrationWriteClient() as client:
                report = await run_resolve_created(SessionLocal, inputs, write_client=client)
        elif args.mode == MODE_RESOLVE_ABSENT:
            # Writes nothing to EasyWeek — it only records what a human checked.
            report = await run_resolve_absent(SessionLocal, inputs)
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

    # A mode that could not do its job must SAY so in its exit code. A final
    # reconciliation that did not prove completeness is the one that matters
    # most: it is the command an operator uses to decide the cutover is over.
    if report.errors:
        for message in report.errors:
            print(f"easyweek_migration: {message}", file=sys.stderr)
        return 1
    if inputs.final and (report.completeness is None or not report.completeness.get("passed")):
        return _fail("final reconciliation did not prove cutover completeness")
    return 0


def _read_text(path: str) -> str:
    """Read a manifest file for the lenient inventory parse; unreadable is empty."""
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return ""


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    if args.report_dir:
        Path(args.report_dir).parent.mkdir(parents=True, exist_ok=True)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
