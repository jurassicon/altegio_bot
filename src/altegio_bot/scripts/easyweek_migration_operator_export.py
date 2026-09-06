"""Create the private, clickable operator table for one migration wave."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_migration.cutover import CutoverError, parse_cutover
from altegio_bot.easyweek_migration.manifest import load_manifest
from altegio_bot.easyweek_migration.operator_export import OperatorExportError, export_operator_table
from altegio_bot.easyweek_migration.runner import DEFAULT_HORIZON_DAYS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_migration_operator_export",
        description="Write a PRIVATE CSV and HTML table joining Altegio bookings to their EasyWeek links.",
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--company-id", required=True, type=int)
    parser.add_argument("--staff-id", required=True, type=int)
    parser.add_argument("--cutover-at", required=True)
    parser.add_argument("--horizon-days", type=int, default=DEFAULT_HORIZON_DAYS)
    parser.add_argument("--output-dir", required=True)
    return parser


async def _run(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    if not manifest.valid:
        print(f"easyweek_migration_operator_export: refused: manifest is unusable ({manifest.reason})", file=sys.stderr)
        return 1
    try:
        cutover = parse_cutover(args.cutover_at)
        async with SessionLocal() as session:
            result = await export_operator_table(
                session=session,
                manifest=manifest,
                company_id=args.company_id,
                staff_id=args.staff_id,
                cutover=cutover,
                horizon_days=args.horizon_days,
                output_dir=Path(args.output_dir),
            )
    except (CutoverError, OperatorExportError) as exc:
        print(f"easyweek_migration_operator_export: refused: {exc}", file=sys.stderr)
        return 1

    # PII never reaches stdout.  Only the counts and private file paths do.
    print(f"rows={result.rows}")
    print(f"easyweek_links={result.linked}")
    print(f"csv={result.csv_path}")
    print(f"html={result.html_path}")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
