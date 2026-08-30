"""PR-11.1: two consecutive waves, and the two regressions between them.

Both defects only appear from the **second** wave onwards, which is why the
single-wave suites stayed green through them.

**The canary deadlock came back.** The narrow recovery was keyed on
``scope.reason == SCOPE_MISSING``, which is only what the scope lookup answers
while no verified proof exists at all — i.e. during the first wave. Once wave A
is confirmed, wave B's unknown canary makes the lookup find wave A's proof and
answer ``*_mismatch`` or ``ambiguous`` instead, so wave B's own exact attempt
never reached the admission and wave B could not be started.

**Wave A's correct bookings were called ghosts.** The ledger sweep treated every
created row outside the current wave's active decisions as a row whose source
must be gone. Under wave B's manifest, wave A's masters are deferred — so their
perfectly live bookings, with perfectly correct targets, were reported as
``source_inactive_target_still_active``, and wave B could not pass until an
operator cancelled real customers' appointments.

The fix asks the source directly instead of reading the selector as evidence.
This file proves that, and proves it did not blunt real ghost detection.
"""

from __future__ import annotations

import json

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.canary import RECOVERY_ADMITTED
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.proof import GHOST_TARGET_STILL_ACTIVE
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    run_apply,
    run_canary,
    run_reconcile,
    run_resolve_created,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_RECORD_A,
    RASTATT_COMPANY_ID,
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
    KA_DEFERRED_STAFF_ID,
    KA_SERVICE_ID,
    KA_STAFF_ID,
    record,
)

TIMEOUT = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))

# Wave B's own booking: a deferred-in-wave-A master, selected in wave B.
WAVE_B_RECORD = 900070
DEFERRED_STAFF_UUID = "aaaaaaaa-0000-4000-8000-00000000dddd"
CREATED_UUIDS.setdefault(WAVE_B_RECORD, "cccccccc-0000-4000-8000-000000000070")


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    rows = stub_altegio_source(monkeypatch)
    # Wave B's master has a booking of her own, waiting for her wave.
    rows[KARLSRUHE_COMPANY_ID].append(
        record(id=WAVE_B_RECORD, date="2026-09-16 10:00:00", staff_id=KA_DEFERRED_STAFF_ID)
    )
    return rows


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def wave_b_manifest():
    """Wave B: wave A's master deferred, the nail-services master selected."""
    payload = json.loads(manifest_json())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_DEFERRED_STAFF_ID]
    branch["deferred_altegio_staff_ids"] = [KA_STAFF_ID]
    branch["staff"] = {
        str(KA_STAFF_ID): branch["staff"][str(KA_STAFF_ID)],
        str(KA_DEFERRED_STAFF_ID): DEFERRED_STAFF_UUID,
    }
    # Rastatt migrated with wave A; wave B leaves it alone entirely.
    payload["branches"][str(RASTATT_COMPANY_ID)]["deferred_altegio_staff_ids"] = payload["branches"][
        str(RASTATT_COMPANY_ID)
    ]["selected_altegio_staff_ids"]
    payload["branches"][str(RASTATT_COMPANY_ID)]["selected_altegio_staff_ids"] = []
    manifest = parse_manifest(json.dumps(payload))
    if manifest.valid:
        return manifest
    # A wave that selects nobody in a branch is refused, so Rastatt keeps its
    # own selection and simply has nothing left to migrate.
    payload["branches"][str(RASTATT_COMPANY_ID)] = json.loads(manifest_json())["branches"][str(RASTATT_COMPANY_ID)]
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid, manifest.reason
    return manifest


def wave_b(mode: str, **overrides):
    return make_inputs(mode, manifest=wave_b_manifest(), **overrides)


async def run_wave_a(session_local, transport) -> None:
    """Canary + bulk for wave A, leaving a verified proof and three targets."""
    from altegio_bot.tests.easyweek_migration_harness import license_bulk

    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )


def plant_wave_b_booking(transport) -> None:
    """The booking wave B's canary created before the response was lost.

    Built here rather than via the harness helper because wave B uses a
    different master and a record the shared fixtures do not know about.
    """
    branch = wave_b_manifest().branch(KARLSRUHE_COMPANY_ID)
    assert branch is not None
    service = branch.service(KA_SERVICE_ID)
    assert service is not None
    transport.bookings[CREATED_UUIDS[WAVE_B_RECORD]] = {
        "uuid": CREATED_UUIDS[WAVE_B_RECORD],
        "comment": f"altegio-migration:{KARLSRUHE_COMPANY_ID}:{WAVE_B_RECORD}",
        # 2026-09-16 10:00 local (CEST) is 08:00Z.
        "start_time": "2026-09-16T08:00:00Z",
        "duration": 60,
        "location_uuid": branch.easyweek_location_uuid,
        "staff_uuid": DEFERRED_STAFF_UUID,
        "customer_uuid": CUSTOMER_UUID,
        "service_uuid": service.easyweek_service_uuid,
        "is_canceled": False,
        "is_completed": False,
    }


async def proofs(session_local) -> list[EasyWeekMigrationCanaryProof]:
    async with session_local() as session:
        return list((await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().all())


# ---------------------------------------------------------------------------
# Regression 1: wave B's unknown canary can be recovered
# ---------------------------------------------------------------------------


async def test_wave_b_uncertain_canary_is_recoverable_after_wave_a(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)
    wave_a_proof = (await proofs(session_local))[0]
    assert wave_a_proof.verified is True
    wave_a_verified_at = wave_a_proof.verified_at

    # Wave B's canary POST creates the booking and then times out.
    transport.fail_with = {WAVE_B_RECORD: TIMEOUT}
    plan_b = await run_dry_run(session_local, manifest=wave_b_manifest())
    canary_inputs = wave_b(
        MODE_CANARY,
        verified_dry_run_id=plan_b.plan_digest,
        canary_company_id=KARLSRUHE_COMPANY_ID,
        canary_record_id=WAVE_B_RECORD,
    )
    async with make_write_client(transport) as client:
        await run_canary(session_local, canary_inputs, write_client=client)
    transport.fail_with = {}
    plant_wave_b_booking(transport)

    # Wave B now has its own exact unverified proof and an uncertain row.
    all_proofs = {(p.source_record_id, p.verified) for p in await proofs(session_local)}
    assert (WAVE_B_RECORD, False) in all_proofs
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[WAVE_B_RECORD].status == "uncertain"

    posts_before = transport.mutations
    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            wave_b(
                MODE_RESOLVE_CREATED,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=WAVE_B_RECORD,
                resolve_target_booking_uuid=CREATED_UUIDS[WAVE_B_RECORD],
            ),
            write_client=client,
        )

    # Admitted, proven, and committed — without a second POST.
    assert report.errors == []
    assert report.as_safe_dict()["canary_recovery"]["canary_recovery"] == RECOVERY_ADMITTED
    assert transport.mutations == posts_before

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[WAVE_B_RECORD].status == "created"
    assert rows[WAVE_B_RECORD].target_booking_uuid == CREATED_UUIDS[WAVE_B_RECORD]
    assert rows[WAVE_B_RECORD].target_snapshot_fingerprint

    by_record = {p.source_record_id: p for p in await proofs(session_local)}
    assert by_record[WAVE_B_RECORD].verified is True
    assert by_record[WAVE_B_RECORD].failure_reason is None
    # Wave A's proof is untouched.
    assert by_record[KA_RECORD_A].verified is True
    assert by_record[KA_RECORD_A].verified_at == wave_a_verified_at


async def test_the_recovered_wave_b_proof_licenses_wave_b_bulk(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    transport.fail_with = {WAVE_B_RECORD: TIMEOUT}
    plan_b = await run_dry_run(session_local, manifest=wave_b_manifest())
    async with make_write_client(transport) as client:
        await run_canary(
            session_local,
            wave_b(
                MODE_CANARY,
                verified_dry_run_id=plan_b.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=WAVE_B_RECORD,
            ),
            write_client=client,
        )
    transport.fail_with = {}
    plant_wave_b_booking(transport)
    async with make_write_client(transport) as client:
        await run_resolve_created(
            session_local,
            wave_b(
                MODE_RESOLVE_CREATED,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=WAVE_B_RECORD,
                resolve_target_booking_uuid=CREATED_UUIDS[WAVE_B_RECORD],
            ),
            write_client=client,
        )

    plan_b2 = await run_dry_run(session_local, manifest=wave_b_manifest())
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, wave_b(MODE_APPLY, verified_dry_run_id=plan_b2.plan_digest), write_client=client
        )
    # The point is that the gate LET the bulk run. Everything wave B's manifest
    # selects is already migrated — its own canary booking, plus Rastatt, which
    # wave B still selects and wave A already moved — so nothing new is created.
    safe = report.as_safe_dict()
    assert safe["totals"]["created"] == 0
    assert safe["totals"]["already_migrated"] >= 1
    assert report.errors == []


async def test_wave_a_proof_does_not_license_wave_b(session_local, source):
    """Widening the recovery trigger must not widen the door."""
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    plan_b = await run_dry_run(session_local, manifest=wave_b_manifest())
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local, wave_b(MODE_APPLY, verified_dry_run_id=plan_b.plan_digest), write_client=client
            )
    assert "canary_proof_missing_or_stale" in exc.value.failures


async def test_wave_b_recovery_still_refuses_a_drifted_binding(session_local, source):
    """The exact-attempt conditions are unchanged by the wider trigger."""
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    transport.fail_with = {WAVE_B_RECORD: TIMEOUT}
    plan_b = await run_dry_run(session_local, manifest=wave_b_manifest())
    async with make_write_client(transport) as client:
        await run_canary(
            session_local,
            wave_b(
                MODE_CANARY,
                verified_dry_run_id=plan_b.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=WAVE_B_RECORD,
            ),
            write_client=client,
        )
    transport.fail_with = {}
    plant_wave_b_booking(transport)
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            wave_b(
                MODE_RESOLVE_CREATED,
                horizon_days=90,  # not the wave's horizon
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=WAVE_B_RECORD,
                resolve_target_booking_uuid=CREATED_UUIDS[WAVE_B_RECORD],
            ),
            write_client=client,
        )

    assert "canary_recovery_no_matching_attempt" in report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[WAVE_B_RECORD] == "uncertain"
    assert transport.mutations == posts_before


# ---------------------------------------------------------------------------
# Regression 2: wave A's correct targets are not ghosts of wave B
# ---------------------------------------------------------------------------


async def confirmed_wave_b(session_local, transport):
    """Wave A migrated and confirmed; wave B canaried and applied."""
    await run_wave_a(session_local, transport)
    plan_b = await run_dry_run(session_local, manifest=wave_b_manifest())
    async with make_write_client(transport) as client:
        await run_canary(
            session_local,
            wave_b(
                MODE_CANARY,
                verified_dry_run_id=plan_b.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=WAVE_B_RECORD,
            ),
            write_client=client,
        )


async def final_b(session_local, transport, **overrides):
    async with make_write_client(transport) as client:
        return await run_reconcile(session_local, wave_b(MODE_RECONCILE, final=True, **overrides), write_client=client)


async def test_wave_a_live_targets_are_not_ghosts_of_wave_b(session_local, source):
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)

    report = await final_b(session_local, transport)
    verdict = report.as_safe_dict()["completeness"]

    assert verdict["ghost_targets_active"] == 0
    assert verdict["manual_action_required"] == []
    # Not merely skipped: wave A's two Karlsruhe targets were fetched and proven
    # by the sweep. Rastatt is still selected in wave B, so its row goes through
    # the ordinary active loop instead.
    assert verdict["earlier_wave_targets_proven"] == 2
    assert verdict["live_targets_proven"] >= 1
    assert verdict["passed"] is True


async def test_an_earlier_waves_target_is_proven_live_not_assumed(session_local, source):
    """If wave A's booking has been deleted, wave B must still notice."""
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    del transport.bookings[CREATED_UUIDS[KA_RECORD_A]]

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_not_found_in_easyweek"] == 1


async def test_an_earlier_waves_moved_target_is_caught(session_local, source):
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    transport.bookings[CREATED_UUIDS[KA_RECORD_A]]["start_time"] = "2026-09-20T07:00:00Z"

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False


@pytest.mark.parametrize(
    "make_inactive,label",
    [
        (lambda rows: rows.update({"confirmed": 0}), "cancelled"),
        (lambda rows: rows.update({"deleted": True}), "deleted"),
    ],
)
async def test_a_cancelled_earlier_wave_source_with_a_live_target_is_a_ghost(
    session_local, source, make_inactive, label
):
    """Real ghost detection survives: deferral is not a lifecycle."""
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_A:
            make_inactive(row)

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1
    assert verdict["unaccounted_reason_codes"][GHOST_TARGET_STILL_ACTIVE] == 1
    assert [row["source_record_id"] for row in verdict["manual_action_required"]] == [KA_RECORD_A]


async def test_a_vanished_earlier_wave_source_with_a_live_target_is_a_ghost(session_local, source):
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    source[KARLSRUHE_COMPANY_ID] = [r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] != KA_RECORD_A]
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_A)] = None

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1


async def test_an_earlier_wave_source_changed_beyond_its_fingerprint_is_a_ghost(session_local, source):
    """Still a live booking, but no longer the one we migrated."""
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_A:
            row["date"] = "2026-09-25 09:00:00"

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1


async def test_a_cancelled_earlier_wave_source_with_a_gone_target_is_consistent(session_local, source):
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_A:
            row["confirmed"] = 0
    del transport.bookings[CREATED_UUIDS[KA_RECORD_A]]

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["inactive_source_targets_terminal"] == 1


async def test_an_unreadable_earlier_wave_source_fails_closed(session_local, source, monkeypatch):
    """ "We could not check" is neither "alive" nor "gone"."""
    from altegio_bot.easyweek_migration import reproof as reproof_module
    from altegio_bot.easyweek_migration.altegio_source import AltegioSourceError

    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)

    async def _boom(*, company_id, record_id, timeout_sec=30.0, client=None):
        raise AltegioSourceError("altegio unreachable")

    monkeypatch.setattr(reproof_module, "fetch_single_record", _boom)
    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]

    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["migrated_source_lifecycle_unprovable"] == 2


async def test_the_multi_wave_reconciliation_never_mutates(session_local, source):
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    posts_before = transport.mutations

    await final_b(session_local, transport)
    async with make_write_client(transport) as client:
        await run_reconcile(session_local, wave_b(MODE_RECONCILE), write_client=client)

    assert transport.mutations == posts_before
    assert transport.cancelled == []


async def test_the_multi_wave_report_is_pii_free(session_local, source):
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_A:
            row["confirmed"] = 0

    report = await final_b(session_local, transport)
    blob = json.dumps(report.as_safe_dict()["completeness"])
    assert CUSTOMER_PHONE not in blob
    assert "77777777-7777-4777-8777-777777777777" not in blob
    assert str(KA_RECORD_A) in blob


async def test_a_single_wave_reconciliation_is_unaffected(session_local, source):
    """The first wave keeps behaving exactly as before."""
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    # Wave A's own rows are proven by the ordinary active loop, not the sweep.
    assert verdict["earlier_wave_targets_proven"] == 0
    assert verdict["live_targets_proven"] == 3


async def test_wave_b_still_blocks_an_unknown_master(session_local, source):
    """The selector's own guarantee is untouched by the lifecycle read."""
    transport = RecordingTransport()
    await confirmed_wave_b(session_local, transport)
    source[KARLSRUHE_COMPANY_ID].append(
        record(
            id=900099,
            date="2026-09-18 10:00:00",
            staff_id=987654,
            services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}],
        )
    )

    verdict = (await final_b(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["staff_not_in_wave_scope"] == 1
