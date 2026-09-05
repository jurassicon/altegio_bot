"""Private operator table for one Altegio -> EasyWeek migration wave.

The normal migration report is intentionally PII-free.  That makes it safe to
paste into an incident or a pull request, but not useful to the salon owner who
has to open each migrated booking and adjust a duration by hand.  This module is
the deliberately private companion: it joins the live Altegio booking with the
durable migration ledger and writes a CSV plus a small, clickable HTML table.

It never logs a row, a name or a phone number.  The output directory is 0700 and
both files are 0600, matching the preparation stage's private artefacts.
"""

from __future__ import annotations

import csv
import html
import io
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Iterable

from altegio_bot.easyweek_migration.altegio_source import build_window, fetch_company_records
from altegio_bot.easyweek_migration.classify import classify_source_liveness
from altegio_bot.easyweek_migration.customers import normalized_international_phone
from altegio_bot.easyweek_migration.cutover import Cutover, LocalTimeError, parse_altegio_local_to_utc
from altegio_bot.easyweek_migration.ledger import load_ledger_views
from altegio_bot.easyweek_migration.manifest import BranchMapping, MigrationManifest, canonical_uuid

DIR_MODE: Final = 0o700
FILE_MODE: Final = 0o600
EASYWEEK_BOOKING_URL: Final = "https://my.easyweek.io/bookings/uuid/"

FIELDNAMES: Final = (
    "altegio_record_id",
    "customer_name",
    "customer_phone",
    "date",
    "time",
    "service_name",
    "altegio_duration_minutes",
    "easyweek_standard_duration_minutes",
    "duration_changed",
    "altegio_price_to_pay",
    "migration_status",
    "easyweek_booking_uuid",
    "easyweek_booking_url",
)


class OperatorExportError(ValueError):
    """The requested private export cannot be produced safely."""


@dataclass(frozen=True)
class ExportResult:
    rows: int
    linked: int
    csv_path: Path
    html_path: Path


def _exact_int(value: object) -> int | None:
    return value if type(value) is int else None


def _staff_id(record: dict[str, Any]) -> int | None:
    direct = _exact_int(record.get("staff_id"))
    if direct is not None:
        return direct
    staff = record.get("staff")
    return _exact_int(staff.get("id")) if isinstance(staff, dict) else None


def _text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _customer_name(record: dict[str, Any]) -> str:
    client = record.get("client")
    if not isinstance(client, dict):
        return ""
    first = _text(client.get("first_name") or client.get("name_first"))
    last = _text(client.get("last_name") or client.get("surname"))
    split_name = " ".join(part for part in (first, last) if part)
    return split_name or _text(client.get("name"))


def _customer_phone(record: dict[str, Any]) -> str:
    client = record.get("client")
    if not isinstance(client, dict):
        return ""
    raw = client.get("phone")
    return normalized_international_phone(raw) or _text(raw)


def _services(record: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("services")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _service_names(record: dict[str, Any]) -> str:
    return " + ".join(filter(None, (_text(item.get("title") or item.get("name")) for item in _services(record))))


def _source_duration_minutes(record: dict[str, Any]) -> int | None:
    raw = record.get("seance_length")
    if type(raw) not in (int, float) or isinstance(raw, bool) or raw <= 0 or raw % 60 != 0:
        return None
    return int(raw // 60)


def _target_duration_minutes(record: dict[str, Any], branch: BranchMapping) -> int | None:
    total = 0
    services = _services(record)
    if not services:
        return None
    for service in services:
        mapping = branch.service(service.get("id"))
        if mapping is None or mapping.catalog_duration.minutes is None:
            return None
        total += mapping.catalog_duration.minutes
    return total


def _price_to_pay(record: dict[str, Any]) -> str:
    values: list[str] = []
    for service in _services(record):
        value = service.get("cost_to_pay")
        if value is None:
            value = service.get("cost")
        values.append(_text(value))
    return " + ".join(filter(None, values))


def _local_date_time(record: dict[str, Any]) -> tuple[str, str]:
    raw = record.get("date") or record.get("datetime")
    try:
        parsed = datetime.fromisoformat(_text(raw).replace(" ", "T"))
    except ValueError:
        return _text(raw), ""
    return parsed.date().isoformat(), parsed.strftime("%H:%M")


def _sort_instant(record: dict[str, Any]) -> datetime | None:
    try:
        return parse_altegio_local_to_utc(record.get("date") or record.get("datetime"))
    except LocalTimeError:
        return None


def build_operator_row(
    record: dict[str, Any],
    *,
    branch: BranchMapping,
    ledger_status: str | None,
    target_booking_uuid: str | None,
) -> dict[str, object]:
    """Build one private row.  Pure so field and disclosure rules are testable."""
    record_id = _exact_int(record.get("id"))
    source_duration = _source_duration_minutes(record)
    target_duration = _target_duration_minutes(record, branch)
    date, time = _local_date_time(record)

    target_uuid = canonical_uuid(target_booking_uuid)
    linked = ledger_status == "created" and target_uuid is not None
    return {
        "altegio_record_id": record_id or "",
        "customer_name": _customer_name(record),
        "customer_phone": _customer_phone(record),
        "date": date,
        "time": time,
        "service_name": _service_names(record),
        "altegio_duration_minutes": source_duration or "",
        "easyweek_standard_duration_minutes": target_duration or "",
        "duration_changed": (
            "yes"
            if source_duration is not None and target_duration is not None and source_duration != target_duration
            else "no"
        ),
        "altegio_price_to_pay": _price_to_pay(record),
        "migration_status": ledger_status or "not_migrated",
        "easyweek_booking_uuid": target_uuid if linked else "",
        "easyweek_booking_url": f"{EASYWEEK_BOOKING_URL}{target_uuid}" if linked else "",
    }


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, DIR_MODE)
    temporary = path.with_suffix(path.suffix + ".tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, FILE_MODE)
    try:
        os.write(descriptor, content.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    os.chmod(path, FILE_MODE)


def _csv(rows: Iterable[dict[str, object]]) -> str:
    stream = io.StringIO(newline="")
    # Excel recognises the BOM and opens names in UTF-8 without an import wizard.
    stream.write("\ufeff")
    writer = csv.DictWriter(stream, fieldnames=FIELDNAMES, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def _html(rows: Iterable[dict[str, object]]) -> str:
    headings = {
        "altegio_record_id": "Altegio ID",
        "customer_name": "Name",
        "customer_phone": "Phone",
        "date": "Date",
        "time": "Time",
        "service_name": "Service",
        "altegio_duration_minutes": "Altegio min",
        "easyweek_standard_duration_minutes": "EasyWeek min",
        "duration_changed": "Duration changed",
        "altegio_price_to_pay": "Altegio price",
        "migration_status": "Status",
        "easyweek_booking_uuid": "EasyWeek UUID",
        "easyweek_booking_url": "EasyWeek",
    }
    body: list[str] = []
    for row in rows:
        cells: list[str] = []
        for field in FIELDNAMES:
            value = _text(row.get(field))
            if field == "easyweek_booking_url" and value:
                rendered = (
                    f'<a href="{html.escape(value, quote=True)}" target="_blank" rel="noreferrer">Open booking</a>'
                )
            else:
                rendered = html.escape(value)
            cells.append(f"<td>{rendered}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")

    header = "".join(f"<th>{html.escape(headings[field])}</th>" for field in FIELDNAMES)
    return (
        """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Irina booking migration</title>
<style>
body{font-family:system-ui,sans-serif;margin:24px;color:#17202a}table{border-collapse:collapse;width:100%}
th,td{border:1px solid #d5d8dc;padding:7px;text-align:left;white-space:nowrap}
th{background:#f4f6f7;position:sticky;top:0}
tr:nth-child(even){background:#fafafa}a{color:#0b57d0}td:nth-child(6){white-space:normal;min-width:220px}
</style>
</head>
<body>
<h1>Irina booking migration</h1>
<p>Source details are read from Altegio. A link is shown only for a booking durably recorded as created.</p>
<table><thead><tr>"""
        + header
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></body></html>\n"
    )


async def export_operator_table(
    *,
    session: Any,
    manifest: MigrationManifest,
    company_id: int,
    staff_id: int,
    cutover: Cutover,
    horizon_days: int,
    output_dir: Path,
) -> ExportResult:
    branch = manifest.branch(company_id)
    if branch is None:
        raise OperatorExportError("company is absent from the manifest")
    if staff_id not in branch.selected_staff_ids:
        raise OperatorExportError("staff is not selected in this migration wave")

    records = await fetch_company_records(
        company_id=company_id,
        window=build_window(cutover.at, horizon_days=horizon_days),
    )
    ledger = await load_ledger_views(session, company_ids=(company_id,))

    selected: list[dict[str, Any]] = []
    for record in records:
        if _staff_id(record) != staff_id:
            continue
        if not classify_source_liveness(record, cutover=cutover).alive:
            continue
        selected.append(record)
    selected.sort(key=lambda item: (_sort_instant(item) is None, _sort_instant(item), _exact_int(item.get("id")) or 0))

    rows: list[dict[str, object]] = []
    for record in selected:
        record_id = _exact_int(record.get("id"))
        view = ledger.get((company_id, record_id)) if record_id is not None else None
        rows.append(
            build_operator_row(
                record,
                branch=branch,
                ledger_status=view.status if view is not None else None,
                target_booking_uuid=view.target_booking_uuid if view is not None else None,
            )
        )

    csv_path = output_dir / "irina-bookings.csv"
    html_path = output_dir / "irina-bookings.html"
    _atomic_write(csv_path, _csv(rows))
    _atomic_write(html_path, _html(rows))
    return ExportResult(
        rows=len(rows),
        linked=sum(bool(row["easyweek_booking_url"]) for row in rows),
        csv_path=csv_path,
        html_path=html_path,
    )
