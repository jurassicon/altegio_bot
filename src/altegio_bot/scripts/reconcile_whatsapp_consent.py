"""Local, read-only-by-default reconciliation of WhatsApp consent (§46.4.4).

Dry run unless ``--apply`` is given, and ``--apply`` is a decision about this
database and nothing else. The command makes no external request of any kind:
no acknowledgement, no Chatwoot note, no Meta or EasyWeek call, no job, no
message. It does not touch ``whatsapp_events.status``, so it cannot replay
anything an inbox worker has or has not processed.

What it prints is safe to paste into a ticket: counts, origins and reason
labels. No phone number, no name, no voucher code, no message body.

    uv run python -m altegio_bot.scripts.reconcile_whatsapp_consent
    uv run python -m altegio_bot.scripts.reconcile_whatsapp_consent --failures
    uv run python -m altegio_bot.scripts.reconcile_whatsapp_consent --apply

Safe to run beside live traffic, and safe to run twice. Every write goes through
the ordinary consent ledger, under its address lock and its monotonicity rules,
so a refusal that lands while this is running wins and a second run is a no-op.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from altegio_bot.db import SessionLocal
from altegio_bot.whatsapp_consent_projection import describe_failures, project


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Project historical WhatsApp opt-outs and provable delivery failures (local only).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the projected refusals and provable failures. Without it nothing is written.",
    )
    parser.add_argument(
        "--failures",
        action="store_true",
        help="Also list the historical delivery outcomes and whether each one can be proven.",
    )
    parser.add_argument(
        "--event-limit",
        type=int,
        default=20000,
        help="How many stored WhatsApp events to read (the report says when it truncated).",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    report = await project(SessionLocal, apply=args.apply, event_limit=args.event_limit)
    print(json.dumps(report.as_safe_dict(), indent=2, sort_keys=True))

    if args.failures:
        candidates = await describe_failures(SessionLocal)
        print("\nhistorical delivery outcomes:")
        for candidate in candidates:
            print(
                f"  batch={candidate.batch_id} slot={candidate.slot} "
                f"provable={candidate.provable} reason={candidate.reason} "
                f"route_proven={candidate.route_proven} recorded={candidate.already_recorded}"
            )
        if not candidates:
            print("  (none)")

    if not args.apply:
        print("\nDry run — nothing was written. Re-run with --apply to write it.")
    if report.truncated:
        print("\nINCOMPLETE: the stored-event scan was truncated; this is not a full historical check.")
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_run(_parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
