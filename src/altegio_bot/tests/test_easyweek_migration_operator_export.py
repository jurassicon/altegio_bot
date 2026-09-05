from __future__ import annotations

import os
from pathlib import Path

import pytest

from altegio_bot.easyweek_migration import operator_export as export_module
from altegio_bot.easyweek_migration.classify import LedgerView
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import parse_manifest
from altegio_bot.easyweek_migration.operator_export import (
    EASYWEEK_BOOKING_URL,
    _atomic_write,
    _csv,
    _html,
    build_operator_row,
    export_operator_table,
)
from altegio_bot.tests.easyweek_migration_harness import manifest_json
from altegio_bot.tests.test_easyweek_migration_planning import KA_SERVICE_ID, KA_STAFF_ID


def _branch():
    raw = manifest_json()
    manifest = parse_manifest(raw)
    assert manifest.valid
    branch = manifest.branch(758285)
    assert branch is not None
    return branch


def _record() -> dict:
    return {
        "id": 991,
        "date": "2026-09-12 14:30:00",
        "staff_id": KA_STAFF_ID,
        "seance_length": 5400,
        "client": {"first_name": "Irina", "last_name": "Kundin", "phone": "+49 151 12345678"},
        "services": [
            {
                "id": KA_SERVICE_ID,
                "title": "Lashes & <Care>",
                "cost": 90,
                "cost_to_pay": 90,
            }
        ],
    }


def test_row_contains_source_details_and_only_links_created_booking() -> None:
    uuid = "11111111-1111-4111-8111-111111111111"
    row = build_operator_row(
        _record(),
        branch=_branch(),
        ledger_status="created",
        target_booking_uuid=uuid,
    )

    assert row["customer_name"] == "Irina Kundin"
    assert row["customer_phone"] == "+4915112345678"
    assert row["date"] == "2026-09-12"
    assert row["time"] == "14:30"
    assert row["altegio_duration_minutes"] == 90
    assert row["easyweek_standard_duration_minutes"] == 60
    assert row["duration_changed"] == "yes"
    assert row["easyweek_booking_url"] == EASYWEEK_BOOKING_URL + uuid

    uncertain = build_operator_row(
        _record(),
        branch=_branch(),
        ledger_status="uncertain",
        target_booking_uuid=uuid,
    )
    assert uncertain["easyweek_booking_uuid"] == ""
    assert uncertain["easyweek_booking_url"] == ""


def test_html_escapes_source_text_and_csv_is_excel_friendly() -> None:
    row = build_operator_row(
        _record(),
        branch=_branch(),
        ledger_status=None,
        target_booking_uuid=None,
    )

    html = _html([row])
    assert "Lashes &amp; &lt;Care&gt;" in html
    assert "Lashes & <Care>" not in html
    assert _csv([row]).startswith("\ufeffaltegio_record_id,")


def test_private_writer_uses_owner_only_permissions(tmp_path: Path) -> None:
    path = tmp_path / "private" / "report.csv"
    _atomic_write(path, "private")

    assert path.read_text() == "private"
    assert os.stat(path).st_mode & 0o777 == 0o600
    assert os.stat(path.parent).st_mode & 0o777 == 0o700


@pytest.mark.asyncio
async def test_export_keeps_every_active_selected_record_and_marks_unmigrated_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _record()
    second = {**_record(), "id": 992, "date": "2026-09-13 09:00:00"}
    other_staff = {**_record(), "id": 993, "staff_id": 9999}

    async def fetch(**_kwargs):
        return [second, other_staff, source]

    async def ledger(_session, *, company_ids):
        assert company_ids == (758285,)
        return {
            (758285, 991): LedgerView(
                status="created",
                target_booking_uuid="11111111-1111-4111-8111-111111111111",
                source_fingerprint="f" * 64,
            )
        }

    monkeypatch.setattr(export_module, "fetch_company_records", fetch)
    monkeypatch.setattr(export_module, "load_ledger_views", ledger)
    manifest = parse_manifest(manifest_json())
    assert manifest.valid

    result = await export_operator_table(
        session=object(),
        manifest=manifest,
        company_id=758285,
        staff_id=KA_STAFF_ID,
        cutover=parse_cutover("2026-09-05T18:13:06Z"),
        horizon_days=180,
        output_dir=tmp_path / "private",
    )

    assert result.rows == 2
    assert result.linked == 1
    csv_text = result.csv_path.read_text(encoding="utf-8-sig")
    assert "Irina Kundin" in csv_text
    assert "not_migrated" in csv_text
    assert csv_text.index("991") < csv_text.index("992")
