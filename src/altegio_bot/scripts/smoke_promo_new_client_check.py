"""Manual smoke test for the promo new-client CRM history check.

Read-only: makes no DB writes, creates no PromoLead, sends no WhatsApp
message, issues no loyalty card, and does not modify any Altegio records.
It only calls the read-only Altegio visit-search endpoint and prints the
boolean result.

Run this before enabling PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO=true to verify
that the endpoint works as expected for your location_id:

  - A phone with no prior visits should return has_any_altegio_record=false.
  - A phone with at least one prior visit should return has_any_altegio_record=true.
  - PROMO_LOCATION_ID_BY_COMPANY must map the company_id to the correct location_id
    before you run the real funnel check.

This script does NOT enable PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO and does not
affect the automatic promo funnel.

Usage (local):
    uv run python -m altegio_bot.scripts.smoke_promo_new_client_check \\
        --location-id 9001 \\
        --phone +491234567890

Usage (Docker):
    docker compose exec -T altegio-api \\
        python -m altegio_bot.scripts.smoke_promo_new_client_check \\
        --location-id 9001 \\
        --phone +491234567890

Exit codes:
    0  — check completed (records found or no records)
    1  — API or network error (AltegioNewClientCheckError)
    2  — missing or invalid arguments (argparse)
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from altegio_bot.altegio_records import AltegioNewClientCheckError, check_client_has_any_altegio_record


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Smoke test: check whether Altegio CRM has any visit record for a phone number. "
            "Read-only — no DB or Altegio writes."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--location-id",
        type=int,
        required=True,
        metavar="ID",
        help="Altegio location_id from PROMO_LOCATION_ID_BY_COMPANY.",
    )
    parser.add_argument(
        "--phone",
        type=str,
        required=True,
        metavar="PHONE_E164",
        help="Phone number in E.164 format, e.g. +491234567890.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    print("Promo new-client check smoke test")
    print(f"location_id={args.location_id}")
    print(f"phone={args.phone}")

    try:
        has_records = await check_client_has_any_altegio_record(
            phone_e164=args.phone,
            location_id=args.location_id,
        )
    except AltegioNewClientCheckError as exc:
        print(f"ERROR: {exc}")
        return 1

    result = "true" if has_records else "false"
    print(f"has_any_altegio_record={result}")
    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
