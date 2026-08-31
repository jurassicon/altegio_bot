"""Review of 80e8521: two P2s about what the reviewed plan actually covers.

**P2-A — the expectation only covered the canary's service.** A baseline was
written before the *first booking of each service*, and it was derived from the
live catalogue. So a wave with services A and B, canaried on A, had no reviewed
expectation for B at all: rename B after the canary and the bulk built B's
"expectation" out of the already-renamed catalogue, satisfied it by construction,
created the booking and reconciled clean. The plan digest never moved, because
the name was not in the plan.

The fix makes the manifest carry the whole expectation — ``catalog_service_name``
and ``catalog_currency`` alongside the price and duration it already had — and
folds them into the manifest digest. The manifest is the reviewed artefact; the
catalogue is only ever asked whether the expectation still holds.

**P2-B — the runbook promised a way out that does not exist.** It said an
intentional price change needs a new manifest, a fresh dry-run and a new canary.
That cannot work: the stored baseline is keyed on (location, service) and
survives all three, so the new canary still fails with
``service_attributes_changed``. The tests below pin the honest behaviour — a
named refusal and zero writes — and the runbook now says so instead.

Nothing here tests a baseline-update mechanism, because this hotfix deliberately
does not add one.
"""

from __future__ import annotations

import json
from copy import deepcopy

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.baseline_store import get_baseline
from altegio_bot.easyweek_migration.manifest import (
    INVALID_SERVICE_IDENTITY_MISSING,
    inventory_manifest,
    parse_manifest,
)
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_RECONCILE,
    load_service_evidence,
    run_apply,
    run_canary,
    run_reconcile,
)
from altegio_bot.tests.easyweek_migration_harness import (
    CATALOG_SERVICES,
    CREATED_UUIDS,
    KA_RECORD_A,
    KA_RECORD_B,
    KARLSRUHE_COMPANY_ID,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    make_inputs,
    make_write_client,
    manifest_json,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    record,
)

# A second service in the SAME branch, used only by the second booking. This is
# the service the canary never touches — and the one the old code left without a
# reviewed expectation until its own first POST.
SERVICE_B_ID = 6011
SERVICE_B_UUID = "aaaaaaaa-2222-4222-8222-00000000b001"
SERVICE_B_NAME = "Wimpernlifting B"

# A second, still-unmigrated booking on service A. Needed to canary service A
# twice: the first run stores its expectation, the second meets the conflict.
# Deliberately not 900003: `test_easyweek_migration_apply` registers that id in
# CREATED_UUIDS for one test and pops it again in a `finally`, which would delete
# this module's entry out from under it when both suites run together.
KA_RECORD_C = 900011
CREATED_UUIDS.setdefault(KA_RECORD_C, "aaaaaaaa-0000-4000-8000-000000000011")


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    """Two Karlsruhe bookings: one on service A, one on service B."""
    rows = stub_altegio_source(monkeypatch)
    rows[KARLSRUHE_COMPANY_ID] = [
        record(id=KA_RECORD_A),
        record(
            id=KA_RECORD_B,
            date="2026-09-11 10:00:00",
            staff_id=KA_STAFF_ID,
            services=[{"id": SERVICE_B_ID, "cost": 90.0, "cost_to_pay": 90.0}],
        ),
        record(id=KA_RECORD_C, date="2026-09-12 10:00:00", staff_id=KA_STAFF_ID),
    ]
    return rows


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def two_service_catalog() -> dict[str, list[dict]]:
    catalog = deepcopy(CATALOG_SERVICES)
    catalog[KA_LOCATION_UUID] = [
        catalog[KA_LOCATION_UUID][0],
        {"uuid": SERVICE_B_UUID, "name": SERVICE_B_NAME, "price": 9000, "minutes": 60},
    ]
    return catalog


def two_service_manifest(**service_b):
    """The harness manifest plus service B, both fully reviewed."""
    payload = json.loads(manifest_json())
    entry = {
        "easyweek_service_uuid": SERVICE_B_UUID,
        "catalog_duration_minutes": 60,
        "catalog_price": "90.00",
        "catalog_service_name": SERVICE_B_NAME,
        "catalog_currency": "EUR",
    }
    entry.update(service_b)
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(SERVICE_B_ID)] = entry
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid, manifest.reason
    return manifest


def repriced_manifest():
    """The same wave with service A reviewed at the new price."""
    payload = json.loads(manifest_json())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(KA_SERVICE_ID)]["catalog_price"] = "100.00"
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(SERVICE_B_ID)] = {
        "easyweek_service_uuid": SERVICE_B_UUID,
        "catalog_duration_minutes": 60,
        "catalog_price": "90.00",
        "catalog_service_name": SERVICE_B_NAME,
        "catalog_currency": "EUR",
    }
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid, manifest.reason
    return manifest


def transport() -> RecordingTransport:
    return RecordingTransport(catalog=two_service_catalog())


def rename_in_catalogue(t: RecordingTransport, uuid: str, name: str) -> None:
    """Same uuid, price and duration — only the catalogue name moves."""
    rows = t.catalog[KA_LOCATION_UUID]
    for index, entry in enumerate(rows):
        if entry["uuid"] == uuid:
            rows[index] = {**entry, "name": name}
            return
    raise AssertionError(f"{uuid} not in the fake catalogue")


async def canary_a(session_local, t, manifest):
    plan = await run_dry_run(session_local, manifest=manifest)
    async with make_write_client(t) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                manifest=manifest,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_A,
            ),
            write_client=client,
        )
    assert report.errors == [], report.errors
    return report


async def bulk(session_local, t, manifest):
    plan = await run_dry_run(session_local, manifest=manifest)
    async with make_write_client(t) as client:
        return await run_apply(
            session_local,
            make_inputs(MODE_APPLY, manifest=manifest, verified_dry_run_id=plan.plan_digest),
            write_client=client,
        )


async def final(session_local, t, manifest):
    async with make_write_client(t) as client:
        return await run_reconcile(
            session_local, make_inputs(MODE_RECONCILE, manifest=manifest, final=True), write_client=client
        )


# ---------------------------------------------------------------------------
# P2-A: every service of the plan, not just the canary's
# ---------------------------------------------------------------------------


async def test_a_service_renamed_after_the_canary_stops_the_bulk(session_local, source):
    """The reproduction. Canary on A, rename B, bulk must not create B.

    Before the fix the bulk built B's expectation out of the renamed catalogue,
    created the booking and reconciled clean — because B had never been bound to
    the reviewed plan at all.
    """
    manifest = two_service_manifest()
    t = transport()
    await canary_a(session_local, t, manifest)
    assert t.post_count_for(KA_RECORD_A) == 1

    rename_in_catalogue(t, SERVICE_B_UUID, "Renamed after the review")

    report = await bulk(session_local, t, manifest)
    assert t.post_count_for(KA_RECORD_B) == 0
    assert "service_attributes_changed" in report.as_safe_dict()["reason_codes"]

    verdict = (await final(session_local, t, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False


async def test_a_rename_between_the_dry_run_and_the_canary_is_not_adopted(session_local, source):
    """Changed after the review, before any write. Never becomes the new normal."""
    manifest = two_service_manifest()
    t = transport()
    await run_dry_run(session_local, manifest=manifest)

    rename_in_catalogue(t, KA_SERVICE_UUID, "Renamed before the canary")

    plan = await run_dry_run(session_local, manifest=manifest)
    async with make_write_client(t) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                manifest=manifest,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_A,
            ),
            write_client=client,
        )

    assert t.mutations == 0
    assert any("service_attributes_changed" in str(error) for error in report.errors)
    async with session_local() as session:
        assert await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID) is None


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"catalog_service_name": "Something else"}, id="name"),
        pytest.param({"catalog_price": "100.00"}, id="price"),
        pytest.param({"catalog_duration_minutes": 90}, id="duration"),
    ],
)
async def test_changing_the_reviewed_expectation_moves_the_digest(session_local, override):
    """The identity is part of the plan now, so editing it invalidates approval."""
    baseline = two_service_manifest()
    changed = two_service_manifest(**override)
    assert baseline.digest != changed.digest

    # And the plan digest moves with it, so a reviewed dry-run id stops matching.
    from altegio_bot.easyweek_migration.report import plan_digest

    assert plan_digest([], cutover_iso="2026-09-01T00:00:00Z", manifest_digest=baseline.digest) != plan_digest(
        [], cutover_iso="2026-09-01T00:00:00Z", manifest_digest=changed.digest
    )


async def test_a_currency_the_project_cannot_compare_exactly_is_refused(session_local):
    payload = json.loads(manifest_json())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(KA_SERVICE_ID)]["catalog_currency"] = "CHF"
    assert not parse_manifest(json.dumps(payload)).valid


async def test_a_manifest_without_the_reviewed_identity_is_refused_by_name(session_local, source):
    """An old file is not silently completed from the catalogue."""
    payload = json.loads(manifest_json())
    entry = payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(KA_SERVICE_ID)]
    entry.pop("catalog_service_name")
    entry.pop("catalog_currency")
    raw = json.dumps(payload)

    parsed = parse_manifest(raw)
    assert not parsed.valid
    assert parsed.reason == INVALID_SERVICE_IDENTITY_MISSING

    # The diagnostic names the services an operator has to prepare.
    tolerant = inventory_manifest(raw)
    assert tolerant.valid
    branch = tolerant.as_safe_dict()["branches"][0]
    assert branch["services_missing_identity"] == [KA_SERVICE_ID]


async def test_inventory_still_runs_on_a_half_written_manifest(session_local):
    """The mode whose job is to help fill the file in must not require it filled."""
    payload = json.loads(manifest_json())
    for branch in payload["branches"].values():
        branch["staff"] = {}
        branch["services"] = {}
        branch["selected_altegio_staff_ids"] = []
        branch["deferred_altegio_staff_ids"] = []
    assert inventory_manifest(json.dumps(payload)).valid


async def test_an_unchanged_plan_migrates_both_services_and_reconciles(session_local, source):
    """The positive case: nothing drifted, so everything works end to end."""
    manifest = two_service_manifest()
    t = transport()
    await canary_a(session_local, t, manifest)
    report = await bulk(session_local, t, manifest)

    # Services A (record C) and B here, plus the untouched Rastatt booking.
    assert report.as_safe_dict()["totals"]["created"] == 3
    assert t.post_count_for(KA_RECORD_B) == 1
    verdict = (await final(session_local, t, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True

    # Both services now have a stored expectation, each equal to the manifest's.
    async with session_local() as session:
        for service_uuid in (KA_SERVICE_UUID, SERVICE_B_UUID):
            stored = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=service_uuid)
            assert stored is not None
            assert stored.currency == "EUR"


async def test_a_restart_reuses_the_same_reviewed_expectations(session_local, source):
    manifest = two_service_manifest()
    t = transport()
    await canary_a(session_local, t, manifest)
    await bulk(session_local, t, manifest)

    # A brand-new evidence object, as a later command in a later process gets.
    reloaded = await load_service_evidence(session_local, make_inputs(MODE_APPLY, manifest=manifest))
    for service_uuid in (KA_SERVICE_UUID, SERVICE_B_UUID):
        stored = reloaded.baselines[(KA_LOCATION_UUID, service_uuid)]
        expected = next(
            entry
            for entry in manifest.branch(KARLSRUHE_COMPANY_ID).services.values()
            if entry.easyweek_service_uuid == service_uuid
        )
        assert stored.normalized_name == expected.catalog_service_name
        assert stored.price_minor == 9000


async def test_a_reviewed_plan_that_contradicts_a_stored_baseline_creates_nothing(session_local, source):
    """Neither source wins automatically — an operator decides which is right.

    Run as a canary, because a canary needs no prior proof: this proves the
    refusal comes from the baseline conflict itself and not from the apply gate
    noticing a different manifest digest.
    """
    manifest = two_service_manifest()
    t = transport()
    await canary_a(session_local, t, manifest)
    posts_before = t.mutations

    # A new plan that repriced service A, and a catalogue that agrees with it.
    repriced = repriced_manifest()
    t.catalog[KA_LOCATION_UUID][0] = {**t.catalog[KA_LOCATION_UUID][0], "price": 10000}
    # The source agrees with the new plan too, so the classifier has no quarrel
    # with it and the refusal can only come from the stored expectation.
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_C:
            row["services"] = [{"id": KA_SERVICE_ID, "cost": 100.0, "cost_to_pay": 100.0}]

    plan = await run_dry_run(session_local, manifest=repriced)
    async with make_write_client(t) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                manifest=repriced,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_C,
            ),
            write_client=client,
        )

    assert t.mutations == posts_before
    assert "service_baseline_conflicts_with_plan" in report.as_safe_dict()["reason_codes"]
    async with session_local() as session:
        stored = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID)
    assert stored is not None
    assert stored.price_minor == 9000  # untouched


# ---------------------------------------------------------------------------
# P2-B: the documented way out did not exist
# ---------------------------------------------------------------------------


async def test_an_intentional_reprice_is_refused_even_with_a_fresh_manifest_and_canary(session_local, source):
    """The runbook used to promise this works. It does not, and now it says so.

    Everything the old instruction asked for is done here: the old visit is over,
    the catalogue is repriced, the manifest agrees, a fresh dry-run is taken and a
    NEW canary is run. The stored baseline is keyed on (location, service) and
    outlives all of it, so the canary still refuses — and must, because nothing in
    this hotfix is allowed to overwrite a reviewed expectation.
    """
    manifest = two_service_manifest()
    t = transport()
    await canary_a(session_local, t, manifest)

    async with session_local() as session:
        before = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID)
    assert before is not None and before.price_minor == 9000

    # The old target is gone; a new source booking exists at the new price.
    del t.bookings[next(iter(t.bookings))]
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_A:
            row["services"] = [{"id": KA_SERVICE_ID, "cost": 100.0, "cost_to_pay": 100.0}]
    t.catalog[KA_LOCATION_UUID][0] = {**t.catalog[KA_LOCATION_UUID][0], "price": 10000}

    repriced = repriced_manifest()
    posts_before = t.mutations

    plan = await run_dry_run(session_local, manifest=repriced)
    async with make_write_client(t) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                manifest=repriced,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_A,
            ),
            write_client=client,
        )

    # A named refusal, no writes, and the stored expectation is exactly as it was.
    assert report.errors
    assert t.mutations == posts_before
    async with session_local() as session:
        after = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID)
    assert after is not None
    assert after.digest == before.digest

    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert "created" not in {status for record_id, status in rows.items() if record_id == KA_RECORD_B}


async def test_restoring_the_reviewed_characteristics_lets_the_wave_continue(session_local, source):
    """The accidental-change path the runbook now describes: put it back."""
    manifest = two_service_manifest()
    t = transport()
    await canary_a(session_local, t, manifest)

    original = dict(t.catalog[KA_LOCATION_UUID][1])
    rename_in_catalogue(t, SERVICE_B_UUID, "Fat fingers")
    report = await bulk(session_local, t, manifest)
    assert t.post_count_for(KA_RECORD_B) == 0
    assert "service_attributes_changed" in report.as_safe_dict()["reason_codes"]

    # Restore what was reviewed, re-run: no manual database editing needed.
    t.catalog[KA_LOCATION_UUID][1] = original
    report = await bulk(session_local, t, manifest)
    assert t.post_count_for(KA_RECORD_B) == 1
    assert report.as_safe_dict()["totals"]["created"] == 1
