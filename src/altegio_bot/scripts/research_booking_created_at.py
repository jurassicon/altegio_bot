"""Read-only research helper for Altegio appointment creation timestamps."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from altegio_bot.altegio_records import (
    AltegioRecordResearchError,
    fetch_record_details_for_booking_created_at_research,
)
from altegio_bot.settings import settings

_TIMESTAMP_FIELDS: tuple[tuple[str, str], ...] = (
    ("created_at", "candidate only, NOT trusted for auto-apply yet"),
    ("create_date", "candidate only, NOT trusted for auto-apply yet"),
    ("datetime_created", "candidate only, NOT trusted for auto-apply yet"),
    ("date", "appointment start, NOT booking created_at"),
    ("datetime", "appointment start, NOT booking created_at"),
    ("last_change_date", "last change, NOT reliable created_at"),
    ("last_change_at", "last change, NOT reliable created_at"),
)

# No field is trusted by production logic in this PR. Keep this empty until a
# field is verified against real Altegio data and business approval.
_TRUSTED_BOOKING_CREATED_AT_FIELDS: frozenset[str] = frozenset()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Research Altegio appointment timestamp fields. Read-only: no DB writes, "
            "no WhatsApp, no loyalty cards, no discount apply."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--location-id",
        type=int,
        required=True,
        metavar="ID",
        help="Altegio location_id for GET /record/{location_id}/{record_id}.",
    )
    parser.add_argument(
        "--record-id",
        type=int,
        required=True,
        metavar="ID",
        help="Altegio appointment/record id.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="HTTP timeout for the read-only Altegio request.",
    )
    return parser


def _sanitize_error(message: str) -> str:
    sanitized = message
    for token in (settings.altegio_partner_token, settings.altegio_user_token):
        if token:
            sanitized = sanitized.replace(token, "[redacted]")
    return sanitized


def _format_field_value(record: Mapping[str, Any], field: str, note: str) -> str:
    value = record.get(field)
    if value is None or value == "":
        return f"{field}: <missing>"
    return f"{field}: {value}  # {note}"


def _confirmed_booking_created_at(record: Mapping[str, Any]) -> str | None:
    for field in _TRUSTED_BOOKING_CREATED_AT_FIELDS:
        value = record.get(field)
        if value:
            return str(value)
    return None


def build_timestamp_summary(
    *,
    location_id: int,
    record_id: int,
    record: Mapping[str, Any],
) -> str:
    confirmed = _confirmed_booking_created_at(record)
    safe_for_auto_apply = "true" if confirmed else "false"
    confirmed_value = confirmed or "<none>"

    lines = [
        "Booking created-at research",
        f"location_id={location_id}",
        f"record_id={record_id}",
        "",
        "candidate fields:",
    ]
    lines.extend(_format_field_value(record, field, note) for field, note in _TIMESTAMP_FIELDS)
    lines.extend(
        [
            "",
            f"confirmed_booking_created_at={confirmed_value}",
            f"safe_for_auto_apply={safe_for_auto_apply}",
        ]
    )
    return "\n".join(lines)


async def _run(args: argparse.Namespace) -> int:
    try:
        record = await fetch_record_details_for_booking_created_at_research(
            location_id=args.location_id,
            record_id=args.record_id,
            timeout_sec=args.timeout_sec,
        )
    except AltegioRecordResearchError as exc:
        print(f"ERROR: {_sanitize_error(str(exc))}")
        return 1

    print(
        build_timestamp_summary(
            location_id=args.location_id,
            record_id=args.record_id,
            record=record,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
