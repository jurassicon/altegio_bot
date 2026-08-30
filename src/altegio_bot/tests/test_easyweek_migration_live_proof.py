"""PR-11.1 revision 17: proving the booking is still there, and still ours.

Three commands used to accept evidence that did not prove what they claimed:

* ``reconcile --final`` never looked at EasyWeek at all — a ledger row saying
  ``created`` counted as a migrated booking, which stays true after somebody
  deletes it, cancels it, moves it or hands it to another master;
* ``reconcile`` with a known target UUID promoted a row on a bare 2xx, checking
  no fields and storing no fingerprint;
* ``resolve-created`` proved the marker and the branch, but not the staff,
  service, customer, start time or duration.

All three now share one proof, and this file holds it to it. It reuses the apply
suite's harness (real ledger, real gate, stubbed Altegio, MockTransport EasyWeek)
so the proof is exercised through the actual commands, not around them.
"""

from __future__ import annotations

import json

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    run_apply,
    run_reconcile,
    run_resolve_created,
)
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    CUSTOMER_PHONE,
    KA_RECORD_B,
    KARLSRUHE_COMPANY_ID,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    license_bulk,
    make_inputs,
    make_write_client,
    run_dry_run,
    stub_altegio_source,
)


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


# Field → the value that makes the live booking disagree with what we wrote.
TARGET_MUTATIONS = [
    ("location_uuid", "00000000-0000-4000-8000-0000000000d1"),
    ("staff_uuid", "00000000-0000-4000-8000-0000000000d2"),
    ("service_uuid", "00000000-0000-4000-8000-0000000000d3"),
    ("customer_uuid", "00000000-0000-4000-8000-0000000000d4"),
    ("start_time", "2026-09-14T07:00:00Z"),
    ("duration", 120),
    ("comment", "rewritten by hand"),
]


async def applied_run(session_local, source, transport) -> str:
    """Canary + bulk, leaving three proven bookings and one origin run id."""
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, inputs, write_client=client)
    return inputs.run_id


async def final_reconcile(session_local, transport):
    async with make_write_client(transport) as client:
        return await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)


# ---------------------------------------------------------------------------
# Blocker 2 — final reconciliation checks the LIVE targets
# ---------------------------------------------------------------------------


async def test_final_reconciliation_passes_when_every_target_is_still_intact(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)

    report = await final_reconcile(session_local, transport)
    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["targets_were_checked"] is True
    assert verdict["live_targets_proven"] == verdict["accounted_for"] == 3
    assert report.errors == []


async def test_a_ledger_status_alone_no_longer_passes(session_local, source):
    """The regression: `created` in our own table is not evidence about EasyWeek."""
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    # Every booking vanishes from EasyWeek; the ledger still says `created`.
    transport.bookings.clear()

    report = await final_reconcile(session_local, transport)
    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["live_targets_proven"] == 0
    assert verdict["unaccounted_reason_codes"]["target_not_found_in_easyweek"] == 3


async def test_a_deleted_target_fails_the_final_reconciliation(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    del transport.bookings[CREATED_UUIDS[KA_RECORD_B]]

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_not_found_in_easyweek"] == 1


@pytest.mark.parametrize("flag", ["is_canceled", "is_completed"])
async def test_a_cancelled_or_completed_target_fails(session_local, source, flag):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    transport.bookings[CREATED_UUIDS[KA_RECORD_B]][flag] = True

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert any("target_malformed" in reason for reason in verdict["unaccounted_reason_codes"])


async def test_an_unreadable_target_fails(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    transport.get_status_override = {CREATED_UUIDS[KA_RECORD_B]: 503}

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_unreadable"] == 1


async def test_a_malformed_target_fails(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    transport.bookings[CREATED_UUIDS[KA_RECORD_B]].pop("staff_uuid")

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert any("target_malformed" in reason for reason in verdict["unaccounted_reason_codes"])


@pytest.mark.parametrize("field,value", TARGET_MUTATIONS)
async def test_any_changed_critical_field_fails_the_final_reconciliation(session_local, source, field, value):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    transport.bookings[CREATED_UUIDS[KA_RECORD_B]][field] = value

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["live_targets_proven"] == 2


async def test_a_missing_stored_fingerprint_fails(session_local, source):
    """Nothing to compare against is not the same as nothing has changed."""
    from sqlalchemy import text

    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    async with session_local() as session:
        await session.execute(
            text(
                "UPDATE easyweek_migration_ledger SET target_snapshot_fingerprint = NULL WHERE source_record_id = :rid"
            ),
            {"rid": KA_RECORD_B},
        )
        await session.commit()

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_snapshot_fingerprint_missing"] == 1


async def test_a_missing_target_uuid_fails(session_local, source):
    from sqlalchemy import text

    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    async with session_local() as session:
        # The CHECK forbids a created row without a target, so the row is moved
        # to a state that keeps it in scope while losing the identifier.
        await session.execute(
            text(
                "UPDATE easyweek_migration_ledger SET status = 'uncertain', target_booking_uuid = NULL "
                "WHERE source_record_id = :rid"
            ),
            {"rid": KA_RECORD_B},
        )
        await session.commit()

    verdict = (await final_reconcile(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False


async def test_the_completeness_verdict_carries_no_pii(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, source, transport)
    transport.bookings[CREATED_UUIDS[KA_RECORD_B]]["duration"] = 120

    report = await final_reconcile(session_local, transport)
    blob = json.dumps(report.as_safe_dict()["completeness"])
    assert CUSTOMER_PHONE not in blob


# ---------------------------------------------------------------------------
# Blocker 4 — plain reconcile no longer takes a 2xx as proof
# ---------------------------------------------------------------------------


async def uncertain_row_with_known_target(session_local, source, transport) -> str:
    """Leave one row uncertain, but with the target UUID already recorded.

    That is the shape a 2xx-without-a-readable-body leaves behind, and the shape
    the old reconcile promoted on a bare GET.
    """
    from sqlalchemy import text

    run_id = await applied_run(session_local, source, transport)
    async with session_local() as session:
        await session.execute(
            text("UPDATE easyweek_migration_ledger SET status = 'uncertain' WHERE source_record_id = :rid"),
            {"rid": KA_RECORD_B},
        )
        await session.commit()
    return run_id


async def test_a_successful_get_alone_does_not_promote_a_row(session_local, source):
    transport = RecordingTransport()
    await uncertain_row_with_known_target(session_local, source, transport)
    # The booking exists and answers 200 — but for a different appointment.
    transport.bookings[CREATED_UUIDS[KA_RECORD_B]]["start_time"] = "2026-09-14T07:00:00Z"

    async with make_write_client(transport) as client:
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"
    assert transport.mutations == 3  # canary + bulk only; reconcile never POSTs


async def test_a_full_match_promotes_the_row_and_stores_the_fingerprint(session_local, source):
    from sqlalchemy import text

    transport = RecordingTransport()
    await uncertain_row_with_known_target(session_local, source, transport)
    async with session_local() as session:
        await session.execute(
            text(
                "UPDATE easyweek_migration_ledger SET target_snapshot_fingerprint = NULL WHERE source_record_id = :rid"
            ),
            {"rid": KA_RECORD_B},
        )
        await session.commit()

    async with make_write_client(transport) as client:
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    resolved = rows[KA_RECORD_B]
    assert resolved.status == "created"
    # The fingerprint the old path never wrote is now there, so a later rollback
    # and a later final reconciliation both have something to compare against.
    assert resolved.target_snapshot_fingerprint


async def test_an_unreadable_target_leaves_the_row_uncertain(session_local, source):
    transport = RecordingTransport()
    await uncertain_row_with_known_target(session_local, source, transport)
    transport.get_status_override = {CREATED_UUIDS[KA_RECORD_B]: 503}

    async with make_write_client(transport) as client:
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_reconcile_without_a_customer_directory_cannot_promote(session_local, source):
    """No export means no way to say which customer — so no proof, so no promotion."""
    transport = RecordingTransport()
    await uncertain_row_with_known_target(session_local, source, transport)

    async with make_write_client(transport) as client:
        report = await run_reconcile(
            session_local,
            make_inputs(MODE_RECONCILE, directory=CustomerDirectory(valid=True, by_phone={})),
            write_client=client,
        )

    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"
    assert any("resolution_inputs_missing" in reason for reason in report.as_safe_dict()["reason_codes"])


# ---------------------------------------------------------------------------
# Blocker 3 — resolve-created proves the whole expected booking
# ---------------------------------------------------------------------------


async def uncertain_without_target(session_local, source, transport) -> None:
    """The ordinary timeout shape: uncertain, and no target UUID recorded."""
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: timeout}
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    transport.fail_with = {}


def resolve_inputs(**overrides):
    kwargs = {
        "resolve_company_id": KARLSRUHE_COMPANY_ID,
        "resolve_record_id": KA_RECORD_B,
        "resolve_target_booking_uuid": CREATED_UUIDS[KA_RECORD_B],
    }
    kwargs.update(overrides)
    return make_inputs(MODE_RESOLVE_CREATED, **kwargs)


async def test_a_correct_target_is_fully_confirmed(session_local, source):
    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert report.errors == []
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B].status == "created"
    assert rows[KA_RECORD_B].target_snapshot_fingerprint


@pytest.mark.parametrize("field,value", TARGET_MUTATIONS)
async def test_any_mismatched_field_leaves_the_row_uncertain(session_local, source, field, value):
    """The gap this closes: only the marker and the branch used to be checked."""
    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)
    transport.bookings[CREATED_UUIDS[KA_RECORD_B]][field] = value

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_a_source_that_changed_after_the_post_stays_uncertain(session_local, source):
    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)
    # The appointment was moved in Altegio after the attempt.
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = {
        **next(r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] == KA_RECORD_B),
        "date": "2026-09-15 18:00:00",
    }

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert any("source_could_not_be_reproved" in err for err in report.errors)
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_a_source_that_vanished_stays_uncertain(session_local, source):
    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = None

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_an_unreadable_source_stays_uncertain(session_local, source, monkeypatch):
    from altegio_bot.easyweek_migration import reproof as reproof_module
    from altegio_bot.easyweek_migration.altegio_source import AltegioSourceError

    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)

    async def _boom(*, company_id, record_id, timeout_sec=30.0, client=None):
        raise AltegioSourceError("altegio unreachable")

    monkeypatch.setattr(reproof_module, "fetch_single_record", _boom)
    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_a_missing_customer_directory_refuses_to_resolve(session_local, source):
    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            resolve_inputs(directory=CustomerDirectory(valid=True, by_phone={})),
            write_client=client,
        )

    assert any("resolution_inputs_missing" in err for err in report.errors)
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_resolution_preserves_the_origin_run_and_never_posts(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    transport.fail_with = {KA_RECORD_B: timeout}
    plan = await run_dry_run(session_local)
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)
    transport.fail_with = {}
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    resolved = rows[KA_RECORD_B]
    assert resolved.status == "created"
    assert resolved.run_id == apply_inputs.run_id
    assert resolved.last_resolution_run_id != apply_inputs.run_id
    assert transport.mutations == posts_before


def test_the_cli_refuses_resolve_created_without_a_customer_directory(tmp_path):
    """Blocker 3, step 2: the input is mandatory, and the CLI says so up front."""
    from altegio_bot.scripts.easyweek_migration import main

    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps(json.loads(_manifest_for_cli())), encoding="utf-8")
    code = main(
        [
            "resolve-created",
            "--manifest",
            str(manifest),
            "--cutover-at",
            "2026-09-01T00:00:00+02:00",
            "--resolve-company-id",
            str(KARLSRUHE_COMPANY_ID),
            "--resolve-record-id",
            str(KA_RECORD_B),
            "--target-uuid",
            CREATED_UUIDS[KA_RECORD_B],
            "--no-write-report",
        ]
    )
    assert code == 1


def _manifest_for_cli() -> str:
    from altegio_bot.tests.easyweek_migration_harness import manifest_json

    return manifest_json()


async def test_no_resolution_path_ever_creates_a_ledger_row_out_of_nothing(session_local, source):
    """A row that was never attempted cannot be resolved into existence."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert ledger_module.STATUS_CREATED  # sanity: the constant exists
    assert "ledger_row_not_found" in report.errors
    assert KA_RECORD_B not in {row.source_record_id for row in await ledger_rows(session_local)}


# ---------------------------------------------------------------------------
# Blocker 1, end to end — the wave selector through apply and reconciliation
# ---------------------------------------------------------------------------


def _wave_manifest(*, deferred_id: int | None = None, unknown_ok: bool = False) -> str:
    """The harness manifest, with one Karlsruhe master moved to a later wave."""
    from altegio_bot.tests.easyweek_migration_harness import manifest_json

    payload = json.loads(manifest_json())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    if deferred_id is not None:
        branch["deferred_altegio_staff_ids"] = sorted(set(branch["deferred_altegio_staff_ids"]) | {deferred_id})
    if unknown_ok:
        branch["deferred_altegio_staff_ids"] = []
    return json.dumps(payload)


async def test_a_deferred_masters_booking_is_never_created(session_local, source):
    """The nail-service wave: hers is skipped, everybody else's still migrates."""
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.test_easyweek_migration_planning import KA_DEFERRED_STAFF_ID, record

    source[KARLSRUHE_COMPANY_ID].append(record(id=900009, date="2026-09-13 10:00:00", staff_id=KA_DEFERRED_STAFF_ID))
    manifest = parse_manifest(_wave_manifest(deferred_id=KA_DEFERRED_STAFF_ID))
    assert manifest.valid

    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local, manifest=manifest)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local,
            make_inputs(MODE_APPLY, manifest=manifest, verified_dry_run_id=plan.plan_digest),
            write_client=client,
        )

    safe = report.as_safe_dict()
    assert safe["totals"]["created"] == 2
    assert 900009 not in {row["source_record_id"] for row in report.created_rows}
    # And it is counted, not merely absent.
    wave = safe["wave"][str(KARLSRUHE_COMPANY_ID)]
    assert wave["active_bookings_deferred"] == 1
    assert wave["by_altegio_staff_id"][str(KA_DEFERRED_STAFF_ID)]["scope"] == "deferred"


async def test_an_unknown_master_blocks_and_is_counted(session_local, source):
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.test_easyweek_migration_planning import record

    source[KARLSRUHE_COMPANY_ID].append(record(id=900010, date="2026-09-13 11:00:00", staff_id=987654))
    manifest = parse_manifest(_wave_manifest())

    plan = await run_dry_run(session_local, manifest=manifest)
    safe = plan.as_safe_dict()
    blocked = [row for row in plan.blocked_rows if row["source_record_id"] == 900010]
    assert blocked and blocked[0]["reason"] == "staff_not_in_wave_scope"
    assert safe["wave"][str(KARLSRUHE_COMPANY_ID)]["active_bookings_unknown_staff"] == 1


async def test_the_wave_report_counts_bookings_per_branch_and_per_staff_id(session_local, source):
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.test_easyweek_migration_planning import KA_DEFERRED_STAFF_ID, KA_STAFF_ID, record

    source[KARLSRUHE_COMPANY_ID].append(record(id=900009, date="2026-09-13 10:00:00", staff_id=KA_DEFERRED_STAFF_ID))
    manifest = parse_manifest(_wave_manifest(deferred_id=KA_DEFERRED_STAFF_ID))

    report = await run_dry_run(session_local, manifest=manifest)
    wave = report.as_safe_dict()["wave"]
    ka = wave[str(KARLSRUHE_COMPANY_ID)]
    assert ka["active_bookings_total"] == 3
    assert ka["active_bookings_selected"] == 2
    assert ka["active_bookings_deferred"] == 1
    assert ka["by_altegio_staff_id"][str(KA_STAFF_ID)]["active_bookings"] == 2
    # The operator's cross-check numbers are ids and counts only.
    blob = json.dumps(wave)
    assert CUSTOMER_PHONE not in blob


async def test_final_reconciliation_passes_with_deferred_masters_left_behind(session_local, source):
    """A later wave is not an incomplete wave."""
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.test_easyweek_migration_planning import KA_DEFERRED_STAFF_ID, record

    source[KARLSRUHE_COMPANY_ID].append(record(id=900009, date="2026-09-13 10:00:00", staff_id=KA_DEFERRED_STAFF_ID))
    manifest = parse_manifest(_wave_manifest(deferred_id=KA_DEFERRED_STAFF_ID))

    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local, manifest=manifest)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local,
            make_inputs(MODE_APPLY, manifest=manifest, verified_dry_run_id=plan.plan_digest),
            write_client=client,
        )
        report = await run_reconcile(
            session_local, make_inputs(MODE_RECONCILE, manifest=manifest, final=True), write_client=client
        )

    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["deferred_bookings"] == 1


async def test_final_reconciliation_fails_while_an_unknown_master_has_bookings(session_local, source):
    """An unlisted master is still a gap, and still stops the wave being called done."""
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.test_easyweek_migration_planning import record

    manifest = parse_manifest(_wave_manifest())
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local, manifest=manifest)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local,
            make_inputs(MODE_APPLY, manifest=manifest, verified_dry_run_id=plan.plan_digest),
            write_client=client,
        )

    source[KARLSRUHE_COMPANY_ID].append(record(id=900010, date="2026-09-13 11:00:00", staff_id=987654))
    async with make_write_client(transport) as client:
        report = await run_reconcile(
            session_local, make_inputs(MODE_RECONCILE, manifest=manifest, final=True), write_client=client
        )

    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["staff_not_in_wave_scope"] == 1


async def test_the_source_reproof_uses_the_same_wave_selector(session_local, source):
    """A master deferred mid-run must not be written by a plan that predates it."""
    from altegio_bot.easyweek_migration.gates import ApplyGateError
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.test_easyweek_migration_planning import KA_STAFF_ID

    wide = parse_manifest(_wave_manifest())
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local, manifest=wide)
    posts_before = transport.mutations

    # The operator moves the master into the later wave between plan and apply.
    # Karlsruhe now selects nobody, which is allowed — Rastatt still does, and a
    # branch kept as cumulative context is the point of the rule. So the file is
    # valid, and the protection has to come from the plan digest instead.
    narrowed = json.loads(_wave_manifest())
    branch = narrowed["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = []
    branch["deferred_altegio_staff_ids"] = [KA_STAFF_ID]
    narrow = parse_manifest(json.dumps(narrowed))
    assert narrow.valid, narrow.reason

    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local,
                make_inputs(MODE_APPLY, manifest=narrow, verified_dry_run_id=plan.plan_digest),
                write_client=client,
            )
    assert "verified_dry_run_id_mismatch" in exc.value.failures
    assert transport.mutations == posts_before


# ---------------------------------------------------------------------------
# Blocker 2 (rev 18) — the resolution paths prove the source's company too
# ---------------------------------------------------------------------------
# `reclassify_source_for_resolution` checked the record id but not the company
# id, so a payload from a different company could be classified and used to
# resolve a row. The pre-POST re-proof already had the right contract; both now
# share it.

COMPANY_IDENTITY_CASES = [
    ("absent", {}, True),
    ("matching int", {"company_id": KARLSRUHE_COMPANY_ID}, True),
    ("different company", {"company_id": 1271200}, False),
    ("string", {"company_id": str(KARLSRUHE_COMPANY_ID)}, False),
    ("true", {"company_id": True}, False),
    ("false", {"company_id": False}, False),
    ("malformed", {"company_id": {"id": KARLSRUHE_COMPANY_ID}}, False),
]


def _company_case_ids():
    return [name for name, _payload, _ok in COMPANY_IDENTITY_CASES]


@pytest.mark.parametrize(
    "overrides,should_prove",
    [(payload, ok) for _name, payload, ok in COMPANY_IDENTITY_CASES],
    ids=_company_case_ids(),
)
def test_the_shared_source_identity_contract(overrides, should_prove):
    from altegio_bot.easyweek_migration.reproof import DETAIL_COMPANY_MISMATCH, prove_source_identity

    payload = {"id": KA_RECORD_B, **overrides}
    failure = prove_source_identity(payload, company_id=KARLSRUHE_COMPANY_ID, record_id=KA_RECORD_B)
    if should_prove:
        assert failure is None
    else:
        assert failure == DETAIL_COMPANY_MISMATCH


@pytest.mark.parametrize(
    "overrides,should_prove",
    [(payload, ok) for _name, payload, ok in COMPANY_IDENTITY_CASES],
    ids=_company_case_ids(),
)
async def test_resolve_created_honours_the_company_contract(session_local, source, overrides, should_prove):
    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)

    planned = next(r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] == KA_RECORD_B)
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = {**planned, **overrides}
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    row = rows[KA_RECORD_B]
    if should_prove:
        assert report.errors == []
        assert row.status == "created"
    else:
        assert any("source_company_mismatch" in err for err in report.errors)
        # The row is untouched: still uncertain, still no target fingerprint,
        # and its origin run is unchanged.
        assert row.status == "uncertain"
        assert row.target_snapshot_fingerprint is None
    # Never a POST on a resolution path, proven or not.
    assert transport.mutations == posts_before


@pytest.mark.parametrize(
    "overrides,should_prove",
    [(payload, ok) for _name, payload, ok in COMPANY_IDENTITY_CASES],
    ids=_company_case_ids(),
)
async def test_ordinary_reconcile_honours_the_company_contract(session_local, source, overrides, should_prove):
    from sqlalchemy import text

    transport = RecordingTransport()
    await uncertain_row_with_known_target(session_local, source, transport)
    async with session_local() as session:
        await session.execute(
            text(
                "UPDATE easyweek_migration_ledger SET target_snapshot_fingerprint = NULL WHERE source_record_id = :rid"
            ),
            {"rid": KA_RECORD_B},
        )
        await session.commit()

    planned = next(r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] == KA_RECORD_B)
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = {**planned, **overrides}
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    row = rows[KA_RECORD_B]
    if should_prove:
        assert row.status == "created"
        assert row.target_snapshot_fingerprint
    else:
        assert row.status == "uncertain"
        assert row.target_snapshot_fingerprint is None
    assert transport.mutations == posts_before


async def test_a_wrong_company_never_reaches_the_classifier(session_local, source, monkeypatch):
    """Identity first: a payload we cannot vouch for is not classified at all."""
    from altegio_bot.easyweek_migration import reproof as reproof_module

    transport = RecordingTransport()
    await uncertain_without_target(session_local, source, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)

    planned = next(r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] == KA_RECORD_B)
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = {**planned, "company_id": 1271200}

    def _must_not_run(*args, **kwargs):
        raise AssertionError("an unproven payload must never be classified")

    monkeypatch.setattr(reproof_module, "classify_record", _must_not_run)
    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)
    assert any("source_company_mismatch" in err for err in report.errors)
