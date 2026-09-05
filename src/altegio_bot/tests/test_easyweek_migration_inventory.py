"""PR-11.1 revision 16: inventory has to work BEFORE the manifest exists.

The first version had a chicken-and-egg bug. Inventory's job is to tell an
operator which Altegio staff and service ids the future bookings use, so they
know what to look up in EasyWeek and write down — but the parser rejected an
empty mapping, so the mode that was supposed to tell you what to fill in refused
to run until you had filled it in. And even when it ran, the report never named
the missing ids.

Staff and service ids are technical source identifiers and belong in this
report. Customer names, phones and payloads do not, and never appear.
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import runner as runner_module
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import (
    KARLSRUHE_COMPANY_ID,
    RASTATT_COMPANY_ID,
    inventory_manifest,
    parse_manifest,
)
from altegio_bot.easyweek_migration.runner import (
    MODE_DRY_RUN,
    MODE_INVENTORY,
    RunInputs,
    new_run_id,
    run_inventory_or_dry_run,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
    RA_LOCATION_UUID,
    directory_with,
    record,
)

UNMAPPED_STAFF_ID = 777001
UNMAPPED_SERVICE_ID = 888001


def unfinished_manifest_text() -> str:
    """What an operator has on day one: branches named, mapping still empty."""
    return json.dumps(
        {
            "manifest_id": "inventory-day-one",
            "branches": {
                str(KARLSRUHE_COMPANY_ID): {
                    "altegio_company_id": KARLSRUHE_COMPANY_ID,
                    "easyweek_location_id": 308001,
                    "easyweek_location_uuid": KA_LOCATION_UUID,
                    "selected_altegio_staff_ids": [],
                    "deferred_altegio_staff_ids": [],
                    "staff": {},
                    "services": {},
                },
                str(RASTATT_COMPANY_ID): {
                    "altegio_company_id": RASTATT_COMPANY_ID,
                    "easyweek_location_id": 315001,
                    "easyweek_location_uuid": RA_LOCATION_UUID,
                    "selected_altegio_staff_ids": [],
                    "deferred_altegio_staff_ids": [],
                    "staff": {},
                    "services": {},
                },
            },
        }
    )


def half_finished_manifest_text() -> str:
    """One master mapped, one not — the state most of the work happens in."""
    payload = json.loads(unfinished_manifest_text())
    payload["manifest_id"] = "inventory-half-way"
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["staff"] = {str(KA_STAFF_ID): KA_STAFF_UUID}
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["selected_altegio_staff_ids"] = [KA_STAFF_ID]
    return json.dumps(payload)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch):
    rows = {
        KARLSRUHE_COMPANY_ID: [
            record(id=900001),
            record(id=900002, date="2026-09-11 10:00:00"),
            record(
                id=900003,
                date="2026-09-12 10:00:00",
                staff_id=UNMAPPED_STAFF_ID,
                services=[{"id": UNMAPPED_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}],
            ),
            # A past booking: skipped, and therefore NOT counted — inventory is
            # about the ids the cutover will actually need.
            record(id=900004, date="2026-08-01 10:00:00", staff_id=999999),
        ],
        RASTATT_COMPANY_ID: [],
    }

    async def _fetch(*, company_id, window, timeout_sec=30.0, client=None):
        return list(rows.get(company_id, []))

    monkeypatch.setattr(runner_module, "fetch_company_records", _fetch)
    return rows


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def inputs_for(mode: str, manifest_text: str) -> RunInputs:
    manifest = inventory_manifest(manifest_text) if mode == MODE_INVENTORY else parse_manifest(manifest_text)
    return RunInputs(
        mode=mode,
        run_id=new_run_id(),
        cutover=parse_cutover("2026-09-01T00:00:00Z"),
        manifest=manifest,
        directory=directory_with(),
    )


async def test_inventory_runs_read_only_on_an_unfinished_manifest(session_local, source):
    """The chicken-and-egg fix: this is the mode that tells you what to fill in."""
    assert not parse_manifest(unfinished_manifest_text()).valid  # strict parse still refuses
    assert inventory_manifest(unfinished_manifest_text()).valid

    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, inputs_for(MODE_INVENTORY, unfinished_manifest_text()))

    assert report.mutations_attempted == 0
    assert report.as_safe_dict()["source"]["records_fetched_by_company"]["758285"] == 4


async def test_inventory_names_the_missing_staff_and_service_ids(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, inputs_for(MODE_INVENTORY, unfinished_manifest_text()))

    identifiers = report.as_safe_dict()["source_identifiers"][str(KARLSRUHE_COMPANY_ID)]
    assert identifiers["staff"]["missing"] == sorted([KA_STAFF_ID, UNMAPPED_STAFF_ID])
    assert identifiers["staff"]["mapped"] == []
    assert identifiers["services"]["missing"] == sorted([KA_SERVICE_ID, UNMAPPED_SERVICE_ID])


async def test_inventory_counts_how_many_bookings_each_id_carries(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, inputs_for(MODE_INVENTORY, unfinished_manifest_text()))

    staff = report.as_safe_dict()["source_identifiers"][str(KARLSRUHE_COMPANY_ID)]["staff"]
    # Two active bookings on the mapped-to-be master, one on the unknown one.
    assert staff["bookings_by_altegio_staff_id"][str(KA_STAFF_ID)] == 2
    assert staff["bookings_by_altegio_staff_id"][str(UNMAPPED_STAFF_ID)] == 1
    # The past booking's master is absent: inventory describes the cutover's work.
    assert "999999" not in staff["bookings_by_altegio_staff_id"]


async def test_inventory_separates_mapped_from_missing_as_the_manifest_fills_up(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, inputs_for(MODE_INVENTORY, half_finished_manifest_text()))

    staff = report.as_safe_dict()["source_identifiers"][str(KARLSRUHE_COMPANY_ID)]["staff"]
    assert staff["mapped"] == [KA_STAFF_ID]
    assert staff["missing"] == [UNMAPPED_STAFF_ID]


async def test_the_inventory_report_carries_no_pii(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, inputs_for(MODE_INVENTORY, unfinished_manifest_text()))

    blob = report.to_json()
    assert CUSTOMER_PHONE not in blob
    assert CUSTOMER_UUID not in blob
    # The ids that SHOULD be there, are.
    assert str(KA_STAFF_ID) in blob


async def test_dry_run_still_refuses_an_incomplete_manifest(session_local, source):
    """Only inventory is lenient. The reviewed plan demands a complete mapping."""
    strict = parse_manifest(unfinished_manifest_text())
    assert not strict.valid
    assert strict.reason == "manifest_empty"

    half = parse_manifest(half_finished_manifest_text())
    assert not half.valid


async def test_dry_run_with_a_complete_manifest_still_reports_identifiers(session_local, source):
    """The aggregate is cheap and useful everywhere; it is surfaced, not special."""
    from altegio_bot.tests.test_easyweek_migration_planning import manifest_text

    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, inputs_for(MODE_DRY_RUN, manifest_text()))
    assert report.as_safe_dict()["source_identifiers"]
