"""Manual smoke test for promo discount application via Altegio API.

This script lets an operator verify the apply_discount_program endpoint
on a specific visit without enabling the automatic webhook flow.

Usage (local):
    uv run python -m altegio_bot.scripts.smoke_apply_promo_discount \\
        --location-id 123 \\
        --card-id 456 \\
        --program-id 789 \\
        --record-id 111

Usage (Docker):
    docker compose exec -T altegio-api python -m altegio_bot.scripts.smoke_apply_promo_discount \\
        --location-id 123 \\
        --card-id 456 \\
        --program-id 789 \\
        --record-id 111

By default the script is dry-run only — no Altegio API call is made.
Add --yes-apply to perform the real call.

Before running with --yes-apply, set:
    PROMO_APPLY_DISCOUNT_API_VERIFIED=true

PROMO_APPLY_DISCOUNT_ENABLED is NOT required for this manual smoke script.
The script does not affect the automatic webhook flow.

Exit codes:
    0  — dry-run completed, or API call succeeded
    1  — API not verified, or API call failed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from altegio_bot.promo_discount_apply import PromoDiscountApplyError, apply_promo_discount_to_visit
from altegio_bot.settings import settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke test: apply a promo discount program to an Altegio visit.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--location-id", type=int, required=True, metavar="ID")
    parser.add_argument("--card-id", type=int, required=True, metavar="ID")
    parser.add_argument("--program-id", type=str, required=True, metavar="ID")
    parser.add_argument("--record-id", type=str, required=True, metavar="ID")
    parser.add_argument(
        "--yes-apply",
        action="store_true",
        default=False,
        help="Actually call the Altegio API. Without this flag the script is dry-run only.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    print("Promo discount apply smoke test")
    mode = "apply" if args.yes_apply else "dry-run"
    print(f"mode={mode}")
    print(f"location_id={args.location_id}")
    print(f"card_id={args.card_id}")
    print(f"program_id={args.program_id}")
    print(f"record_id={args.record_id}")

    if not args.yes_apply:
        print("No API call was made. Re-run with --yes-apply to call Altegio.")
        return 0

    if not settings.promo_apply_discount_api_verified:
        print("ERROR: PROMO_APPLY_DISCOUNT_API_VERIFIED=false — set it to true before running with --yes-apply")
        return 1

    try:
        result = await apply_promo_discount_to_visit(
            location_id=args.location_id,
            card_id=args.card_id,
            program_id=args.program_id,
            record_id=args.record_id,
        )
    except PromoDiscountApplyError as exc:
        print(f"ERROR: {exc}")
        return 1

    print("success=true")
    print(f"raw={json.dumps(result.raw)}")
    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
