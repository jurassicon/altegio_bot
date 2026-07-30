"""Operator probe for the EasyWeek Public API v2 (read-only, PR-2).

The single purpose of this script is to let an operator confirm, before any
schema (PR-3) or normalizer (PR-4) depends on them, that:

  1. the API key and workspace slug are accepted (``GET /ping``);
  2. which locations that key can see (``GET /locations``);
  3. which of them is Durlach — so ``EASYWEEK_LOCATION_UUID`` can be recorded by
     hand in the production ``easyweek.env`` (INTEGRATION_PLAN §1.6 p.5: one key
     sees several locations, and the numeric ``location_id`` from a webhook does
     NOT substitute for the UUID);
  4. optionally, that a known booking UUID reads back (``GET /bookings/{uuid}``).

Read-only by construction: it can only call the three GET methods of
:class:`~altegio_bot.easyweek_client.EasyWeekClient`.

Output safety is an **allowlist**, never a redaction pass over a raw response:
only the fields named in ``_LOCATION_FIELDS`` / ``_BOOKING_FIELDS`` below are
ever printed. A new or renamed upstream field therefore cannot leak by default —
including anything under ``customer``, notes, comments, or order/payment totals.
``--redact-pii`` is accepted for explicitness and is always on; PR-2 intentionally
ships no unsafe mode.

Usage::

    python -m altegio_bot.scripts.easyweek_probe --redact-pii
    python -m altegio_bot.scripts.easyweek_probe --booking-uuid <uuid>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from typing import Any

from altegio_bot.easyweek_client import (
    EasyWeekClient,
    EasyWeekConfigError,
    EasyWeekError,
    EasyWeekProtocolError,
)

# Exit codes: distinct so a wrapper script can tell a misconfiguration from an
# API failure without parsing text.
EXIT_OK = 0
EXIT_CONFIG_ERROR = 2
EXIT_API_ERROR = 3

# Allowlists — the ONLY fields that may ever be printed.
_LOCATION_FIELDS = ("uuid", "name", "timezone")
_BOOKING_FIELDS = (
    "uuid",
    "location_uuid",
    "start_time",
    "end_time",
    "start_time_local",
    "end_time_local",
    "timezone",
    "is_canceled",
    "is_completed",
)
# Address sub-fields an operator needs to recognise a branch. Deliberately no
# free-text/"comment"-style keys, which upstream may use for arbitrary content.
_ADDRESS_FIELDS = ("country", "city", "street", "house", "zip_code")


def _safe_scalar(value: Any) -> Any:
    """Pass through only JSON scalars; anything structured becomes a type marker.

    This is what stops an unexpected nested object (a renamed ``customer``, say)
    from being printed just because it appeared under an allowlisted key.
    """
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value
    return f"<{type(value).__name__} omitted>"


def _safe_address(raw: Any) -> dict[str, Any] | None:
    """Project an address down to the few fields an operator needs."""
    if not isinstance(raw, dict):
        return None
    address = {key: _safe_scalar(raw[key]) for key in _ADDRESS_FIELDS if key in raw}
    return address or None


def safe_location_summary(location: dict[str, Any]) -> dict[str, Any]:
    """Allowlisted projection of one ``/locations`` entry."""
    summary: dict[str, Any] = {key: _safe_scalar(location.get(key)) for key in _LOCATION_FIELDS}
    address = _safe_address(location.get("address"))
    if address is not None:
        summary["address"] = address
    return summary


def safe_booking_summary(booking: dict[str, Any]) -> dict[str, Any]:
    """Allowlisted projection of one ``/bookings/{uuid}`` response.

    Never includes ``customer``, notes/comments, order or payment details. The
    services list is reduced to a count plus their names, which is enough for an
    operator to recognise the booking without exposing prices or client data.
    """
    summary: dict[str, Any] = {key: _safe_scalar(booking.get(key)) for key in _BOOKING_FIELDS if key in booking}

    status = booking.get("status")
    if isinstance(status, dict):
        summary["status_type"] = _safe_scalar(status.get("type"))
    elif status is not None:
        summary["status_type"] = _safe_scalar(status)

    services = booking.get("ordered_services")
    if isinstance(services, list):
        names: list[Any] = []
        for item in services:
            if isinstance(item, dict):
                name = item.get("name") or item.get("title")
                if isinstance(name, str):
                    names.append(name)
        summary["services"] = {"count": len(services), "names": names}

    return summary


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_probe",
        description="Read-only EasyWeek Public API v2 probe (ping + locations, optional booking).",
    )
    parser.add_argument(
        "--redact-pii",
        action="store_true",
        default=True,
        help="Always on: output is an allowlist projection and never includes customer data.",
    )
    parser.add_argument(
        "--booking-uuid",
        default=None,
        help="Optional booking UUID to read back via GET /bookings/{uuid}.",
    )
    return parser


async def run_probe(booking_uuid: str | None = None) -> dict[str, Any]:
    """Execute the probe and return a safe, allowlisted summary."""
    async with EasyWeekClient() as client:
        ping = await client.ping()
        locations = await client.list_locations()

        # The whole point of this probe is to identify Durlach, so an empty list
        # is a failed probe, not a successful one with nothing in it. Reporting
        # ok=true here would let an operator believe the key works and then find
        # no UUID to record.
        if not locations:
            raise EasyWeekProtocolError("no locations are visible to this API key", operation="list_locations")

        result: dict[str, Any] = {
            # /ping returns {"ping": "pong", "version": ...} — both safe.
            "ping": {
                "ok": True,
                "version": _safe_scalar(ping.get("version")),
            },
            "locations": {
                "count": len(locations),
                "items": [safe_location_summary(item) for item in locations],
            },
        }

        if booking_uuid:
            booking = await client.get_booking(booking_uuid)
            result["booking"] = safe_booking_summary(booking)

        return result


def main(argv: list[str] | None = None) -> int:
    # Keep library logs off stdout: stdout carries only the safe JSON summary.
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    args = _build_parser().parse_args(argv)

    try:
        summary = asyncio.run(run_probe(args.booking_uuid))
    except EasyWeekError as exc:
        # Metadata only — never a response body, headers, URL or the API key.
        _print_json({"ok": False, **exc.safe_summary})
        return EXIT_CONFIG_ERROR if isinstance(exc, EasyWeekConfigError) else EXIT_API_ERROR

    _print_json({"ok": True, **summary})
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
