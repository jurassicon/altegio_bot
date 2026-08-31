"""PR-11.1: a later wave still needs the earlier waves' mappings.

The defect. Wave A migrates the lash master; her bookings live on, correct, in
both systems. Wave B migrates the nail master, and its manifest — prepared the
way the runbook said, "fill in the mappings for the masters you selected" —
carries only the NAIL staff and service. Wave B canaries and applies happily,
and then its final reconciliation dies:

``reclassify_source_lifecycle`` re-reads wave A's still-active booking and runs
the full classifier over it. Wave B's manifest has no LASH service mapping, so
the classification stops at ``service_mapping_missing``, the lifecycle is
``unprovable``, and completeness fails with
``migrated_source_lifecycle_unprovable`` — about a booking nobody touched.

The fix is the cumulative-manifest contract: each wave's manifest is built on the
previous one's, and mappings of rows that are already migrated and still live may
not be dropped. Because a rule nobody enforces is a rule that gets broken on the
day it matters, a read-only guard proves it before the first mutation of a wave.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import reproof as reproof_module
from altegio_bot.easyweek_migration import runner as runner_module
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_DRY_RUN,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    run_apply,
    run_canary,
    run_inventory_or_dry_run,
    run_reconcile,
    run_resolve_created,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof, EasyWeekMigrationLedger
from altegio_bot.tests.easyweek_migration_harness import (
    KA_LOCATION_ID,
    RecordingTransport,
    apply_production_flags,
    catalog_entry,
    make_inputs,
    make_write_client,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_LOCATION_UUID,
    directory_with,
)

# Two waves, two masters, two genuinely different services.
LASH_STAFF_ID = 7001
NAIL_STAFF_ID = 7002
LASH_STAFF_UUID = "aaaaaaaa-1111-4111-8111-000000000001"
NAIL_STAFF_UUID = "aaaaaaaa-1111-4111-8111-000000000002"

LASH_SERVICE_ID = 8001
NAIL_SERVICE_ID = 8002
LASH_SERVICE_UUID = "bbbbbbbb-2222-4222-8222-000000000001"
NAIL_SERVICE_UUID = "bbbbbbbb-2222-4222-8222-000000000002"

LASH_RECORD = 910101
NAIL_RECORD = 910102

LASH_TARGET = "cccccccc-3333-4333-8333-000000000001"
NAIL_TARGET = "cccccccc-3333-4333-8333-000000000002"

TARGETS = {LASH_RECORD: LASH_TARGET, NAIL_RECORD: NAIL_TARGET}

LASH_ENTRY = {
    "easyweek_service_uuid": LASH_SERVICE_UUID,
    "catalog_duration_minutes": 60,
    "catalog_price": "90.00",
    "catalog_service_name": "Lash Extensions",
    "catalog_currency": "EUR",
}
NAIL_ENTRY = {
    "easyweek_service_uuid": NAIL_SERVICE_UUID,
    "catalog_duration_minutes": 90,
    "catalog_price": "120.00",
    "catalog_service_name": "Nail Modellage",
    "catalog_currency": "EUR",
}


def lash_record() -> dict:
    return {
        "id": LASH_RECORD,
        "date": "2026-09-10 14:00:00",
        "staff_id": LASH_STAFF_ID,
        "seance_length": 3600,
        "client": {"phone": CUSTOMER_PHONE},
        "services": [{"id": LASH_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}],
    }


def nail_record() -> dict:
    return {
        "id": NAIL_RECORD,
        "date": "2026-09-12 11:00:00",
        "staff_id": NAIL_STAFF_ID,
        "seance_length": 5400,
        "client": {"phone": CUSTOMER_PHONE},
        "services": [{"id": NAIL_SERVICE_ID, "cost": 120.0, "cost_to_pay": 120.0}],
    }


def build_manifest(*, manifest_id: str, selected: list[int], deferred: list[int], staff: dict, services: dict):
    payload = {
        "manifest_id": manifest_id,
        "branches": {
            str(KARLSRUHE_COMPANY_ID): {
                "altegio_company_id": KARLSRUHE_COMPANY_ID,
                "easyweek_location_id": KA_LOCATION_ID,
                "easyweek_location_uuid": KA_LOCATION_UUID,
                "selected_altegio_staff_ids": selected,
                "deferred_altegio_staff_ids": deferred,
                "staff": staff,
                "services": services,
            }
        },
    }
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid, manifest.reason
    return manifest


def wave_a_manifest():
    """Lashes now, nails later — and the nail mappings do not exist yet."""
    return build_manifest(
        manifest_id="wave-a-lashes",
        selected=[LASH_STAFF_ID],
        deferred=[NAIL_STAFF_ID],
        staff={str(LASH_STAFF_ID): LASH_STAFF_UUID},
        services={str(LASH_SERVICE_ID): LASH_ENTRY},
    )


def wave_b_cumulative_manifest():
    """Nails now. Lash mappings kept, because wave A's bookings are still live."""
    return build_manifest(
        manifest_id="wave-b-nails",
        selected=[NAIL_STAFF_ID],
        deferred=[LASH_STAFF_ID],
        staff={str(LASH_STAFF_ID): LASH_STAFF_UUID, str(NAIL_STAFF_ID): NAIL_STAFF_UUID},
        services={str(LASH_SERVICE_ID): LASH_ENTRY, str(NAIL_SERVICE_ID): NAIL_ENTRY},
    )


def wave_b_truncated_manifest():
    """The mistake: wave A's mappings dropped because "that wave is done"."""
    return build_manifest(
        manifest_id="wave-b-truncated",
        selected=[NAIL_STAFF_ID],
        deferred=[LASH_STAFF_ID],
        staff={str(NAIL_STAFF_ID): NAIL_STAFF_UUID},
        services={str(NAIL_SERVICE_ID): NAIL_ENTRY},
    )


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict:
    """One Karlsruhe branch holding one lash booking and one nail booking."""
    live_changes: dict[tuple[int, int], dict | None] = {}
    rows = {KARLSRUHE_COMPANY_ID: [lash_record(), nail_record()]}

    async def _fetch(*, company_id, window, timeout_sec=30.0, client=None):
        return list(rows.get(company_id, []))

    async def _fetch_one(*, company_id, record_id, timeout_sec=30.0, client=None):
        if (company_id, record_id) in live_changes:
            return live_changes[(company_id, record_id)]
        for row in rows.get(company_id, []):
            if row.get("id") == record_id:
                return row
        return None

    monkeypatch.setattr(runner_module, "fetch_company_records", _fetch)
    monkeypatch.setattr(reproof_module, "fetch_single_record", _fetch_one)
    rows["live_changes"] = live_changes  # type: ignore[index]
    return rows


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


# This file's two services, in the shape `GET /locations/{uuid}/services` returns.
TWO_SERVICE_CATALOG = {
    KA_LOCATION_UUID: [
        {"uuid": LASH_SERVICE_UUID, "name": "Lash Extensions", "price": 9000, "minutes": 60},
        {"uuid": NAIL_SERVICE_UUID, "name": "Nail Modellage", "price": 12000, "minutes": 90},
    ]
}


class TwoServiceTransport(RecordingTransport):
    """The shared fake, taught about this file's two distinct services."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("catalog", TWO_SERVICE_CATALOG)
        super().__init__(**kwargs)

    def _store(self, body: dict, record_id: int) -> str:  # type: ignore[override]
        uuid = TARGETS[record_id]
        entry = catalog_entry(body["location_uuid"], body["service_uuid"], catalog=self.catalog)
        start = datetime.fromisoformat(body["reserved_on"].replace("Z", "+00:00"))
        booking = {
            "uuid": uuid,
            "location_uuid": body["location_uuid"],
            "start_time": body["reserved_on"],
            "end_time": (start + timedelta(minutes=entry["minutes"])).isoformat().replace("+00:00", "Z"),
            "timezone": body["timezone"],
            "duration": {"value": entry["minutes"], "label": "minutes", "iso_8601": f"PT{entry['minutes']}M"},
            "is_canceled": False,
            "is_completed": False,
            "public_notes": body["booking_comment"],
            "currency": "EUR",
            "customer": {"uuid": CUSTOMER_UUID},
            "ordered_services": [
                {
                    # An order-line uuid, deliberately not the catalogue one.
                    "uuid": f"0de4{uuid[4:]}",
                    "name": entry["name"],
                    "quantity": 1,
                    "currency": "EUR",
                    "price": entry["price"],
                    "original_price": entry["price"],
                    "duration": {"value": entry["minutes"], "label": "minutes"},
                    "original_duration": {"value": entry["minutes"], "label": "minutes"},
                }
            ],
        }
        booking.update(self.readback_override)
        self.bookings[uuid] = booking
        self.assignments[uuid] = body["staffer_uuid"]
        return uuid

    def plant_from_last_post(self, record_id: int) -> str:
        """Store the booking the last CREATE for this record would have made.

        Models the situation behind a timeout precisely: the write landed, the
        response did not. Replaying the recorded request body rather than
        hand-building a booking keeps the planted target byte-identical to the
        one the migration actually sent, which is what the read-back compares.
        """
        for request in reversed(self.requests):
            if request.method != "POST" or request.url.path.endswith("set-booking-cancel"):
                continue
            body = json.loads(request.content.decode())
            if int(body["booking_comment"].rsplit(":", 1)[-1]) == record_id:
                return self._store(body, record_id)
        raise AssertionError(f"no create POST recorded for {record_id}")


def wave_inputs(mode: str, manifest, **overrides):
    return make_inputs(
        mode,
        manifest=manifest,
        directory=directory_with(),
        **overrides,
    )


async def dry_run(session_local, manifest):
    async with session_local() as session:
        return await run_inventory_or_dry_run(session, wave_inputs(MODE_DRY_RUN, manifest))


async def run_wave(session_local, transport, manifest, *, canary_record: int) -> None:
    """Canary the named booking, then apply the rest of the wave."""
    plan = await dry_run(session_local, manifest)
    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            wave_inputs(
                MODE_CANARY,
                manifest,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=canary_record,
            ),
            write_client=client,
        )
    assert report.errors == [], report.errors

    plan2 = await dry_run(session_local, manifest)
    async with make_write_client(transport) as client:
        applied = await run_apply(
            session_local, wave_inputs(MODE_APPLY, manifest, verified_dry_run_id=plan2.plan_digest), write_client=client
        )
    assert applied.errors == [], applied.errors


async def final(session_local, transport, manifest, **overrides):
    async with make_write_client(transport) as client:
        return await run_reconcile(
            session_local, wave_inputs(MODE_RECONCILE, manifest, final=True, **overrides), write_client=client
        )


async def ledger(session_local) -> dict[int, EasyWeekMigrationLedger]:
    async with session_local() as session:
        rows = (await session.execute(select(EasyWeekMigrationLedger))).scalars().all()
    return {row.source_record_id: row for row in rows}


async def proofs(session_local) -> list[EasyWeekMigrationCanaryProof]:
    async with session_local() as session:
        return list((await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().all())


# ---------------------------------------------------------------------------
# 1. Two waves, two different services, one cumulative manifest
# ---------------------------------------------------------------------------


async def test_wave_a_migrates_only_the_lash_booking(session_local, source):
    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    rows = await ledger(session_local)
    assert set(rows) == {LASH_RECORD}
    assert rows[LASH_RECORD].status == "created"

    verdict = (await final(session_local, transport, wave_a_manifest())).as_safe_dict()["completeness"]
    assert verdict["passed"] is True


async def test_a_cumulative_wave_b_passes_with_a_different_service(session_local, source):
    """The defect this file exists for, from the other side: it must work."""
    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)
    posts_after_a = transport.mutations

    manifest_b = wave_b_cumulative_manifest()
    await run_wave(session_local, transport, manifest_b, canary_record=NAIL_RECORD)

    # Wave A's booking was not migrated a second time by wave B.
    rows = await ledger(session_local)
    assert set(rows) == {LASH_RECORD, NAIL_RECORD}
    assert rows[LASH_RECORD].target_booking_uuid == LASH_TARGET
    assert transport.post_count_for(LASH_RECORD) == 1
    assert transport.mutations == posts_after_a + 1

    verdict = (await final(session_local, transport, manifest_b)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    # Wave A's live booking was proven, not skipped and not called a ghost.
    assert verdict["earlier_wave_targets_proven"] == 1
    assert verdict["ghost_targets_active"] == 0
    assert verdict["manual_action_required"] == []


async def test_keeping_the_old_mapping_does_not_reselect_the_old_master(session_local, source):
    """A mapping is not a selector. Wave B must not touch wave A's master."""
    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    manifest_b = wave_b_cumulative_manifest()
    plan = await dry_run(session_local, manifest_b)
    safe = plan.as_safe_dict()
    ready = {row["source_record_id"] for row in plan.blocked_rows}
    assert LASH_RECORD not in ready
    assert safe["wave"][str(KARLSRUHE_COMPANY_ID)]["active_bookings_deferred"] == 1
    assert safe["wave"][str(KARLSRUHE_COMPANY_ID)]["active_bookings_selected"] == 1


# ---------------------------------------------------------------------------
# 2. The reproduction: a truncated manifest must be caught BEFORE any mutation
# ---------------------------------------------------------------------------


async def test_a_truncated_manifest_blocks_the_canary_before_any_post(session_local, source):
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)
    posts_before = transport.mutations
    proof_ids_before = {(p.id, p.verified) for p in await proofs(session_local)}
    ledger_before = {r: (row.status, row.target_booking_uuid) for r, row in (await ledger(session_local)).items()}

    truncated = wave_b_truncated_manifest()
    plan = await dry_run(session_local, truncated)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_canary(
                session_local,
                wave_inputs(
                    MODE_CANARY,
                    truncated,
                    verified_dry_run_id=plan.plan_digest,
                    canary_company_id=KARLSRUHE_COMPANY_ID,
                    canary_record_id=NAIL_RECORD,
                ),
                write_client=client,
            )

    assert "previous_wave_context_unprovable" in exc.value.failures
    # Not one mutation, and nothing about wave A moved.
    assert transport.mutations == posts_before
    assert transport.cancelled == []
    assert {(p.id, p.verified) for p in await proofs(session_local)} == proof_ids_before
    assert {r: (row.status, row.target_booking_uuid) for r, row in (await ledger(session_local)).items()} == (
        ledger_before
    )


async def test_a_truncated_manifest_blocks_the_bulk_apply_before_any_post(session_local, source):
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)
    posts_before = transport.mutations

    truncated = wave_b_truncated_manifest()
    plan = await dry_run(session_local, truncated)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(
                session_local,
                wave_inputs(MODE_APPLY, truncated, verified_dry_run_id=plan.plan_digest),
                write_client=client,
            )
    assert transport.mutations == posts_before


async def test_the_missing_context_report_names_the_ids_and_no_pii(session_local, source):
    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    truncated = wave_b_truncated_manifest()
    report = await dry_run(session_local, truncated)
    context = report.as_safe_dict()["previous_wave_context"]

    assert context["proven"] is False
    row = context["rows"][0]
    assert row["source_record_id"] == LASH_RECORD
    assert row["reason"] == "previous_wave_service_mapping_missing"
    assert row["altegio_service_id"] == LASH_SERVICE_ID
    assert row["altegio_staff_id"] == LASH_STAFF_ID

    blob = json.dumps(context)
    assert CUSTOMER_PHONE not in blob
    assert CUSTOMER_UUID not in blob


async def test_restoring_the_mapping_lets_the_same_wave_through(session_local, source):
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    truncated = wave_b_truncated_manifest()
    plan = await dry_run(session_local, truncated)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_canary(
                session_local,
                wave_inputs(
                    MODE_CANARY,
                    truncated,
                    verified_dry_run_id=plan.plan_digest,
                    canary_company_id=KARLSRUHE_COMPANY_ID,
                    canary_record_id=NAIL_RECORD,
                ),
                write_client=client,
            )

    # The operator puts wave A's mappings back and re-runs. Same wave, no drama.
    manifest_b = wave_b_cumulative_manifest()
    await run_wave(session_local, transport, manifest_b, canary_record=NAIL_RECORD)
    verdict = (await final(session_local, transport, manifest_b)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True


@pytest.mark.parametrize(
    "drop,expected",
    [
        pytest.param("staff", "previous_wave_staff_mapping_missing", id="staff mapping"),
        pytest.param("service", "previous_wave_service_mapping_missing", id="service mapping"),
        pytest.param("customer", "previous_wave_customer_unresolved", id="customer directory"),
    ],
)
async def test_each_kind_of_missing_context_has_its_own_reason(session_local, source, drop, expected):
    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    staff = {str(LASH_STAFF_ID): LASH_STAFF_UUID, str(NAIL_STAFF_ID): NAIL_STAFF_UUID}
    services = {str(LASH_SERVICE_ID): LASH_ENTRY, str(NAIL_SERVICE_ID): NAIL_ENTRY}
    directory = directory_with()
    if drop == "staff":
        staff.pop(str(LASH_STAFF_ID))
    elif drop == "service":
        services.pop(str(LASH_SERVICE_ID))
    else:
        # A customer export that no longer covers the earlier wave's customer.
        directory = replace(directory_with(), by_phone={"+4915100000000": [CUSTOMER_UUID]})

    manifest = build_manifest(
        manifest_id="wave-b-partial",
        selected=[NAIL_STAFF_ID],
        deferred=[LASH_STAFF_ID],
        staff=staff,
        services=services,
    )
    async with session_local() as session:
        report = await run_inventory_or_dry_run(
            session, make_inputs(MODE_DRY_RUN, manifest=manifest, directory=directory)
        )
    context = report.as_safe_dict()["previous_wave_context"]
    assert context["proven"] is False
    assert context["rows"][0]["reason"] == expected


# ---------------------------------------------------------------------------
# 3. The first wave owes nothing to masters it has not migrated yet
# ---------------------------------------------------------------------------


async def test_the_first_wave_needs_no_mapping_for_a_deferred_master(session_local, source):
    """Wave A defers the nail master and has no NAIL mapping at all."""
    manifest_a = wave_a_manifest()
    assert manifest_a.branch(KARLSRUHE_COMPANY_ID).service(NAIL_SERVICE_ID) is None

    transport = TwoServiceTransport()
    # Neither the canary nor the apply is blocked by the cumulative guard.
    await run_wave(session_local, transport, manifest_a, canary_record=LASH_RECORD)

    report = await dry_run(session_local, manifest_a)
    context = report.as_safe_dict()["previous_wave_context"]
    assert context["proven"] is True
    assert context["rows"] == []


async def test_the_guard_ignores_rows_the_current_wave_already_covers(session_local, source):
    """Wave A's own rows are checked by the ordinary path, never counted twice."""
    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    report = await dry_run(session_local, wave_a_manifest())
    context = report.as_safe_dict()["previous_wave_context"]
    assert context["checked"] == 0


async def test_an_earlier_waves_moved_source_stops_the_next_wave_before_it_starts(session_local, source):
    """A wave that could never be reconciled must not be allowed to begin.

    The manifest here is perfectly cumulative — nothing was dropped. What changed
    is the world: wave A's lash booking was rescheduled, so its target is now a
    ghost, and wave B's final reconciliation would fail on it. That verdict is
    the same whether it arrives before wave B's canary or after wave B has been
    migrated, so it arrives before.
    """
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == LASH_RECORD:
            row["date"] = "2026-09-25 09:00:00"

    manifest = wave_b_cumulative_manifest()
    plan = await dry_run(session_local, manifest)
    context = plan.as_safe_dict()["previous_wave_context"]
    assert context["proven"] is False
    assert [row["reason"] for row in context["rows"]] == ["previous_wave_source_fingerprint_mismatch"]

    posts_before = transport.mutations
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_canary(
                session_local,
                wave_inputs(
                    MODE_CANARY,
                    manifest,
                    verified_dry_run_id=plan.plan_digest,
                    canary_company_id=KARLSRUHE_COMPANY_ID,
                    canary_record_id=NAIL_RECORD,
                ),
                write_client=client,
            )
    assert "previous_wave_context_unprovable" in exc.value.failures
    assert transport.mutations == posts_before


# ---------------------------------------------------------------------------
# 4. Real ghost detection is untouched
# ---------------------------------------------------------------------------


async def both_waves(session_local, transport):
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)
    await run_wave(session_local, transport, wave_b_cumulative_manifest(), canary_record=NAIL_RECORD)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda rows: rows[0].update({"confirmed": 0}), id="cancelled"),
        pytest.param(lambda rows: rows[0].update({"deleted": True}), id="deleted"),
        pytest.param(lambda rows: rows[0].update({"date": "2026-09-25 09:00:00"}), id="fingerprint changed"),
    ],
)
async def test_a_broken_earlier_wave_source_with_a_live_target_is_still_a_ghost(session_local, source, mutate):
    transport = TwoServiceTransport()
    await both_waves(session_local, transport)
    mutate([r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] == LASH_RECORD])

    verdict = (await final(session_local, transport, wave_b_cumulative_manifest())).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1
    assert [row["source_record_id"] for row in verdict["manual_action_required"]] == [LASH_RECORD]


async def test_a_vanished_earlier_wave_source_with_a_live_target_is_still_a_ghost(session_local, source):
    transport = TwoServiceTransport()
    await both_waves(session_local, transport)
    source[KARLSRUHE_COMPANY_ID] = [r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] != LASH_RECORD]
    source["live_changes"][(KARLSRUHE_COMPANY_ID, LASH_RECORD)] = None

    verdict = (await final(session_local, transport, wave_b_cumulative_manifest())).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1


async def test_an_inactive_earlier_source_with_a_gone_target_is_consistent(session_local, source):
    transport = TwoServiceTransport()
    await both_waves(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == LASH_RECORD:
            row["confirmed"] = 0
    del transport.bookings[LASH_TARGET]

    verdict = (await final(session_local, transport, wave_b_cumulative_manifest())).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["inactive_source_targets_terminal"] == 1


async def test_an_earlier_waves_deleted_target_still_fails(session_local, source):
    transport = TwoServiceTransport()
    await both_waves(session_local, transport)
    del transport.bookings[LASH_TARGET]

    verdict = (await final(session_local, transport, wave_b_cumulative_manifest())).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_not_found_in_easyweek"] == 1


async def test_an_unreadable_earlier_source_fails_closed(session_local, source, monkeypatch):
    from altegio_bot.easyweek_migration.altegio_source import AltegioSourceError

    transport = TwoServiceTransport()
    await both_waves(session_local, transport)

    async def _boom(*, company_id, record_id, timeout_sec=30.0, client=None):
        raise AltegioSourceError("altegio unreachable")

    monkeypatch.setattr(reproof_module, "fetch_single_record", _boom)
    verdict = (await final(session_local, transport, wave_b_cumulative_manifest())).as_safe_dict()["completeness"]
    assert verdict["passed"] is False


async def test_the_multi_service_reconciliation_never_mutates(session_local, source):
    transport = TwoServiceTransport()
    await both_waves(session_local, transport)
    posts_before = transport.mutations

    await final(session_local, transport, wave_b_cumulative_manifest())

    assert transport.mutations == posts_before
    assert transport.cancelled == []


# ---------------------------------------------------------------------------
# 5. Canary recovery still works — and still refuses — across waves
# ---------------------------------------------------------------------------

TIMEOUT = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))


async def uncertain_wave_b_canary(session_local, transport) -> None:
    """Wave A complete; wave B's canary POST times out on the nail booking."""
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)
    manifest = wave_b_cumulative_manifest()
    plan = await dry_run(session_local, manifest)
    transport.fail_with = {NAIL_RECORD: TIMEOUT}
    async with make_write_client(transport) as client:
        await run_canary(
            session_local,
            wave_inputs(
                MODE_CANARY,
                manifest,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=NAIL_RECORD,
            ),
            write_client=client,
        )
    transport.fail_with = {}


async def test_a_second_waves_uncertain_canary_is_still_recoverable(session_local, source):
    """The cumulative guard must not stand between a wave and its own recovery."""
    transport = TwoServiceTransport()
    await uncertain_wave_b_canary(session_local, transport)

    rows = await ledger(session_local)
    assert rows[NAIL_RECORD].status == "uncertain"
    assert rows[LASH_RECORD].status == "created"

    transport.plant_from_last_post(NAIL_RECORD)
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            wave_inputs(
                MODE_RESOLVE_CREATED,
                wave_b_cumulative_manifest(),
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=NAIL_RECORD,
                resolve_target_booking_uuid=NAIL_TARGET,
            ),
            write_client=client,
        )

    assert report.errors == []
    # Recovery reads; it never re-sends the POST that may already have landed.
    assert transport.mutations == posts_before
    assert transport.post_count_for(NAIL_RECORD) == 1

    rows = await ledger(session_local)
    assert rows[NAIL_RECORD].status == "created"
    assert rows[NAIL_RECORD].target_booking_uuid == NAIL_TARGET
    assert [proof.verified for proof in await proofs(session_local)] == [True, True]


async def test_the_recovered_second_wave_then_reconciles_clean(session_local, source):
    transport = TwoServiceTransport()
    await uncertain_wave_b_canary(session_local, transport)
    transport.plant_from_last_post(NAIL_RECORD)
    manifest = wave_b_cumulative_manifest()
    async with make_write_client(transport) as client:
        await run_resolve_created(
            session_local,
            wave_inputs(
                MODE_RESOLVE_CREATED,
                manifest,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=NAIL_RECORD,
                resolve_target_booking_uuid=NAIL_TARGET,
            ),
            write_client=client,
        )

    verdict = (await final(session_local, transport, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["earlier_wave_targets_proven"] == 1


async def test_wave_a_s_proof_does_not_license_wave_b(session_local, source):
    """A verified canary licenses ITS wave. A new selector is a new wave."""
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = TwoServiceTransport()
    await run_wave(session_local, transport, wave_a_manifest(), canary_record=LASH_RECORD)

    manifest = wave_b_cumulative_manifest()
    plan = await dry_run(session_local, manifest)
    posts_before = transport.mutations
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local,
                wave_inputs(MODE_APPLY, manifest, verified_dry_run_id=plan.plan_digest),
                write_client=client,
            )

    assert "canary_proof_missing_or_stale" in exc.value.failures
    assert transport.mutations == posts_before
    assert NAIL_RECORD not in await ledger(session_local)


async def test_a_drifted_binding_cannot_recover_the_second_wave(session_local, source):
    """Recovering wave B's row under wave A's selector is a different wave's claim.

    Wave A's own proof is verified, so this run gets past the scope gate on wave
    A's identity — and is then refused on the row itself, because under wave A's
    selector the nail master is deferred and the nail booking is not part of the
    wave being claimed. The point is that the drift is caught somewhere, without
    a second POST.
    """
    transport = TwoServiceTransport()
    await uncertain_wave_b_canary(session_local, transport)
    transport.plant_from_last_post(NAIL_RECORD)
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            wave_inputs(
                MODE_RESOLVE_CREATED,
                wave_a_manifest(),
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=NAIL_RECORD,
                resolve_target_booking_uuid=NAIL_TARGET,
            ),
            write_client=client,
        )

    assert report.errors
    assert transport.mutations == posts_before
    assert transport.post_count_for(NAIL_RECORD) == 1
    assert (await ledger(session_local))[NAIL_RECORD].status == "uncertain"
