"""PR-11.1: the cutover end to end, against PostgreSQL.

The Altegio API is stubbed (the source of truth for *what exists* is not what is
under test here) and the EasyWeek write client is driven by a MockTransport, but
the ledger, its unique constraint, the gate and the whole runner are real.

The sequence the runbook prescribes is proven as a sequence, not as isolated
units: inventory → dry-run → canary → reconcile → bulk → dry-run → delta →
reconcile. That ordering is the product; testing each step alone would miss the
one property that matters most, which is that running it twice creates nothing
twice.
"""

from __future__ import annotations

import json
import logging

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration import reproof as reproof_module
from altegio_bot.easyweek_migration import runner as runner_module
from altegio_bot.easyweek_migration.altegio_source import AltegioSourceError
from altegio_bot.easyweek_migration.gates import ApplyGateError
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_DRY_RUN,
    MODE_INVENTORY,
    MODE_RECONCILE,
    MODE_RESOLVE_ABSENT,
    MODE_RESOLVE_CREATED,
    MODE_ROLLBACK_DRY_RUN,
    run_apply,
    run_canary,
    run_inventory_or_dry_run,
    run_reconcile,
    run_resolve_absent,
    run_resolve_created,
    run_rollback,
)
from altegio_bot.models.models import (
    EasyWeekMigrationCanaryProof,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    KA_RECORD_A,
    KA_RECORD_B,
    RA_RECORD_A,
    RecordingTransport,
    _registry_json,
    apply_production_flags,
    ledger_rows,
    license_bulk,
    make_inputs,
    make_write_client,
    manifest_json,
    message_side_effects,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_LOCATION_UUID,
    record,
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


# ---------------------------------------------------------------------------
# Read-only modes
# ---------------------------------------------------------------------------


async def test_inventory_and_dry_run_write_nothing_anywhere(session_local, source):
    transport = RecordingTransport()

    async with session_local() as session:
        inventory = await run_inventory_or_dry_run(session, make_inputs(MODE_INVENTORY))
    async with session_local() as session:
        dry_run = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    assert inventory.as_safe_dict()["totals"]["ready"] == 3
    assert dry_run.as_safe_dict()["totals"]["ready"] == 3
    assert dry_run.mutations_attempted == 0
    # No EasyWeek request of any kind, and no ledger row.
    assert transport.requests == []
    assert await ledger_rows(session_local) == []
    assert await message_side_effects(session_local) == (0, 0)


async def test_the_dry_run_report_is_machine_readable_and_pii_free(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    blob = report.to_json()
    parsed = json.loads(blob)
    assert parsed["mode"] == MODE_DRY_RUN
    assert parsed["cutover_at"] == "2026-09-01T00:00:00Z"
    assert set(parsed["totals"]) == {
        "ready",
        "created",
        "already_migrated",
        "blocked",
        "uncertain",
        "failed",
        "skipped",
    }
    assert set(parsed["by_company"]) == {str(KARLSRUHE_COMPANY_ID), str(RASTATT_COMPANY_ID)}
    assert CUSTOMER_PHONE not in blob
    assert CUSTOMER_UUID not in blob


async def test_durlach_never_appears_in_a_report(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    assert "308697" not in report.as_safe_dict()["by_company"]


async def test_past_cancelled_and_completed_source_rows_are_skipped_not_migrated(session_local, source):
    source[KARLSRUHE_COMPANY_ID].extend(
        [
            record(id=900010, date="2026-08-01 10:00:00"),  # before cutover
            record(id=900011, confirmed=0),  # cancelled
            record(id=900012, attendance=1),  # completed
        ]
    )
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    totals = report.as_safe_dict()["totals"]
    assert totals["ready"] == 3
    assert totals["skipped"] == 3


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


async def test_apply_without_the_gate_makes_no_mutation(session_local, source):
    """No verified dry-run id, no native attestation — nothing leaves."""
    transport = RecordingTransport()
    inputs = make_inputs(MODE_APPLY, native_notifications_confirmed=False, verified_dry_run_id=None)

    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(session_local, inputs, write_client=client)

    assert transport.mutations == 0
    assert await ledger_rows(session_local) == []


@pytest.mark.parametrize("flag", ["easyweek_notifications_enabled", "easyweek_reviews_enabled"])
async def test_apply_is_blocked_while_a_customer_message_flag_is_on(session_local, source, monkeypatch, flag):
    transport = RecordingTransport()
    plan = await run_dry_run(session_local)
    monkeypatch.setattr(settings, flag, True, raising=False)
    inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)

    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(session_local, inputs, write_client=client)

    assert transport.mutations == 0


async def test_a_swapped_branch_mapping_blocks_before_any_mutation(session_local, source, monkeypatch):
    """The manifest is internally consistent and still points at the wrong salon."""
    swapped = json.loads(_registry_json())
    swapped["karlsruhe"], swapped["rastatt"] = swapped["rastatt"], swapped["karlsruhe"]
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(swapped), raising=False)

    transport = RecordingTransport()
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )
    assert "target_branch_identity_unproven" in exc.value.failures
    assert transport.mutations == 0


async def test_an_unconfigured_registry_blocks_before_any_mutation(session_local, source, monkeypatch):
    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)
    transport = RecordingTransport()
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )
    assert transport.mutations == 0


async def test_apply_creates_the_ready_bookings_and_records_them(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert report.as_safe_dict()["totals"]["created"] == 2
    assert report.as_safe_dict()["totals"]["already_migrated"] == 1  # the canary
    rows = await ledger_rows(session_local)
    assert [row.status for row in rows] == ["created", "created", "created"]
    assert all(row.target_booking_uuid for row in rows)
    # Every created row carries the snapshot rollback will later compare against.
    assert all(row.target_snapshot_fingerprint for row in rows)
    # The whole point of PR-11.1: a schedule row, not a conversation.
    assert await message_side_effects(session_local) == (0, 0)


async def test_the_created_booking_carries_a_stable_pii_free_marker(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)

    bodies = [json.loads(r.content.decode()) for r in transport.requests if r.method == "POST"]
    comments = {body["comment"] for body in bodies}
    assert f"altegio-migration:{KARLSRUHE_COMPANY_ID}:{KA_RECORD_A}" in comments
    assert all(CUSTOMER_PHONE not in comment for comment in comments)


async def test_a_second_apply_creates_no_duplicate(session_local, source):
    """The property the ledger exists for. Same source, second run, zero writes."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    first_round = transport.mutations

    plan2 = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        second = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan2.plan_digest), write_client=client
        )

    assert transport.mutations == first_round == 3
    assert second.as_safe_dict()["totals"]["already_migrated"] == 3
    assert second.as_safe_dict()["totals"]["created"] == 0
    assert len(await ledger_rows(session_local)) == 3


async def test_the_ledger_source_identity_is_unique(session_local):
    """A second row for one source booking is a database error, not a policy."""
    async with session_local() as session:
        async with session.begin():
            assert await ledger_module.claim_for_apply(
                session,
                run_id="run-a",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )

    async with session_local() as session:
        async with session.begin():
            await ledger_module.record_created(
                session,
                run_id="run-a",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                target_booking_uuid=CREATED_UUIDS[KA_RECORD_A],
            )

    async with session_local() as session:
        async with session.begin():
            again = await ledger_module.claim_for_apply(
                session,
                run_id="run-b",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )
    assert again is False
    assert len(await ledger_rows(session_local)) == 1


# ---------------------------------------------------------------------------
# Blocker 1 — the last source re-proof before each POST
# ---------------------------------------------------------------------------


def live_change(source, record_id: int, change: dict | None, *, company_id=KARLSRUHE_COMPANY_ID) -> None:
    """Make the per-row re-proof see something the plan did not.

    Models the real window: the plan was built minutes ago and the write loop is
    still walking it when a customer cancels or a salon reschedules.
    """
    planned = next(r for r in source[company_id] if r["id"] == record_id)
    source["live_changes"][(company_id, record_id)] = None if change is None else {**planned, **change}


async def test_a_booking_cancelled_after_the_plan_is_never_created(session_local, source):
    """dry-run saw the booking; the customer cancels before its turn comes round."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    live_change(source, KA_RECORD_B, {"confirmed": 0})

    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    blocked = [row for row in report.blocked_rows if row["source_record_id"] == KA_RECORD_B]
    assert blocked and blocked[0]["reason"].startswith("source_changed_after_plan")
    # No booking, and — just as important — no ledger row claiming otherwise.
    assert CREATED_UUIDS[KA_RECORD_B] not in transport.bookings
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert KA_RECORD_B not in rows
    assert safe["totals"]["created"] == 1  # only the other Karlsruhe/Rastatt row


async def test_a_booking_rescheduled_after_the_plan_is_never_created(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    live_change(source, KA_RECORD_B, {"date": "2026-09-12 18:00:00"})

    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    blocked = [row for row in report.blocked_rows if row["source_record_id"] == KA_RECORD_B]
    assert blocked and blocked[0]["reason"] == "source_changed_after_plan:fingerprint_changed"
    assert CREATED_UUIDS[KA_RECORD_B] not in transport.bookings
    rows = {row.source_record_id for row in await ledger_rows(session_local)}
    assert KA_RECORD_B not in rows


async def test_a_booking_deleted_after_the_plan_is_never_created(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    live_change(source, KA_RECORD_B, None)

    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    blocked = [row for row in report.blocked_rows if row["source_record_id"] == KA_RECORD_B]
    assert blocked and blocked[0]["reason"] == "source_changed_after_plan:source_absent"
    assert CREATED_UUIDS[KA_RECORD_B] not in transport.bookings


async def test_an_unreadable_source_stops_that_row_without_a_claim(session_local, source, monkeypatch):
    """ "We could not check" is not "it is fine" — and leaves nothing to reconcile."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)

    async def _boom(*, company_id, record_id, timeout_sec=30.0, client=None):
        raise AltegioSourceError("altegio unreachable")

    monkeypatch.setattr(reproof_module, "fetch_single_record", _boom)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert report.as_safe_dict()["totals"]["created"] == 0
    assert transport.mutations == 1  # only the canary's, from license_bulk
    rows = {row.source_record_id for row in await ledger_rows(session_local)}
    assert rows == {KA_RECORD_A}  # the canary only; no speculative claims


# ---------------------------------------------------------------------------
# Blocker 2 — an unsafe retry can no longer duplicate a booking
# ---------------------------------------------------------------------------


async def test_a_timeout_becomes_uncertain_and_halts_the_run(session_local, source):
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: timeout}

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert safe["totals"]["uncertain"] == 1
    assert any("reconcile" in err for err in safe["errors"])
    rows = [row for row in await ledger_rows(session_local) if row.status == "uncertain"]
    assert len(rows) == 1
    assert rows[0].target_booking_uuid is None


async def test_a_5xx_with_a_side_effect_is_uncertain_and_never_duplicated(session_local, source):
    """The corrected contract, end to end: one POST, one booking, no retry."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: 503}
    transport.create_side_effect_on_failure = True

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert safe["totals"]["uncertain"] == 1
    # Exactly one POST for the failing row: the side effect happened once.
    assert transport.post_count_for(KA_RECORD_B) == 1
    assert transport.side_effects == 1
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"
    assert any("reconcile" in err for err in safe["errors"])


async def test_an_uncertain_row_blocks_the_next_apply_until_reconciled(session_local, source):
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: timeout}
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    replan = await run_dry_run(session_local)
    blocked = [row for row in replan.blocked_rows if row["source_record_id"] == KA_RECORD_B]
    assert blocked and blocked[0]["reason"] == "ledger_uncertain_needs_reconcile"


async def test_a_permanent_4xx_fails_the_row_and_leaves_the_others_alone(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: 422}

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert safe["totals"]["failed"] == 1
    assert safe["totals"]["created"] == 1  # the run continued
    assert transport.post_count_for(KA_RECORD_B) == 1  # never retried


async def test_a_failed_row_may_be_retried_once_the_cause_is_fixed(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: 422}
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    transport.fail_with = {}
    replan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=replan.plan_digest), write_client=client
        )
    assert report.as_safe_dict()["totals"]["created"] == 1


async def test_a_crashed_claim_is_never_re_sent(session_local, source):
    """A process that died around its POST leaves `pending`, which may have landed."""
    async with session_local() as session:
        async with session.begin():
            assert await ledger_module.claim_for_apply(
                session,
                run_id="crashed-run",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_B,
                source_fingerprint="fp",
            )

    async with session_local() as session:
        async with session.begin():
            reclaimed = await ledger_module.claim_for_apply(
                session,
                run_id="next-run",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_B,
                source_fingerprint="fp",
            )
    assert reclaimed is False

    plan = await run_dry_run(session_local)
    blocked = [row for row in plan.blocked_rows if row["source_record_id"] == KA_RECORD_B]
    assert blocked and blocked[0]["reason"] == "ledger_uncertain_needs_reconcile"


# ---------------------------------------------------------------------------
# Blocker 4 — the canary is named, verified and required
# ---------------------------------------------------------------------------


async def test_the_canary_is_chosen_by_identity_not_by_position(session_local, source):
    """Reordering the source must not change which customer gets canaried."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport, record_id=KA_RECORD_B)
    assert CREATED_UUIDS[KA_RECORD_B] in transport.bookings
    assert CREATED_UUIDS[KA_RECORD_A] not in transport.bookings

    source[KARLSRUHE_COMPANY_ID].reverse()
    async with session_local() as session:
        await session.execute(text("DELETE FROM easyweek_migration_ledger"))
        await session.execute(text("DELETE FROM easyweek_migration_canary_proof"))
        await session.commit()

    reordered = RecordingTransport()
    await license_bulk(session_local, reordered, record_id=KA_RECORD_B)
    assert CREATED_UUIDS[KA_RECORD_B] in reordered.bookings
    assert CREATED_UUIDS[KA_RECORD_A] not in reordered.bookings


async def test_a_canary_naming_a_booking_outside_the_plan_creates_nothing(session_local, source):
    transport = RecordingTransport()
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=987654,
            ),
            write_client=client,
        )
    assert transport.mutations == 0
    assert "canary_source_not_in_verified_plan" in report.errors


async def test_a_verified_canary_stores_a_durable_proof(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    async with session_local() as session:
        rows = (await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().all()
    assert len(rows) == 1
    proof = rows[0]
    assert proof.verified is True
    assert proof.source_record_id == KA_RECORD_A
    assert proof.target_booking_uuid == CREATED_UUIDS[KA_RECORD_A]
    assert proof.target_snapshot_fingerprint
    assert proof.verified_at is not None


async def test_a_canary_whose_readback_disagrees_does_not_license_a_bulk(session_local, source):
    """A 2xx says the request was accepted, not that it landed where we meant."""
    transport = RecordingTransport(readback_override={"staff_uuid": "00000000-0000-4000-8000-0000000000ff"})
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_A,
            ),
            write_client=client,
        )
    assert any("canary_readback_failed" in err for err in report.errors)

    async with session_local() as session:
        proof = (await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().one()
    assert proof.verified is False

    # And the bulk it would have licensed is refused.
    plan2 = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan2.plan_digest), write_client=client
            )
    assert "canary_proof_missing_or_stale" in exc.value.failures


async def test_changing_the_manifest_invalidates_the_canary_proof(session_local, source, monkeypatch):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)

    # A different manifest id is a different manifest digest.
    changed = json.loads(manifest_json())
    changed["manifest_id"] = "changed-manifest"

    def _changed_manifest(mode: str, **overrides):
        inputs = make_inputs(mode, **overrides)
        inputs.manifest = parse_manifest(json.dumps(changed))
        return inputs

    plan_inputs = _changed_manifest(MODE_DRY_RUN)
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, plan_inputs)

    apply_inputs = _changed_manifest(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(session_local, apply_inputs, write_client=client)
    assert "canary_proof_missing_or_stale" in exc.value.failures


async def test_bulk_proceeds_after_a_correct_canary(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    assert report.as_safe_dict()["totals"]["created"] == 2


# ---------------------------------------------------------------------------
# Blocker 5 — an unknown outcome can actually be resolved
# ---------------------------------------------------------------------------


async def _make_uncertain(session_local, source, transport) -> str:
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    await license_bulk(session_local, transport)
    transport.fail_with = {KA_RECORD_B: timeout}
    plan = await run_dry_run(session_local)
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)
    transport.fail_with = {}
    return apply_inputs.run_id


async def test_a_timeout_leaves_uncertain_with_no_uuid_and_plain_reconcile_cannot_resolve_it(session_local, source):
    transport = RecordingTransport()
    await _make_uncertain(session_local, source, transport)

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)
    assert report.as_safe_dict()["reason_codes"]["uncertain_unresolved"] == 1
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_confirmed_created_resolves_an_uncertain_row_and_keeps_its_origin_run(session_local, source):
    transport = RecordingTransport()
    origin_run = await _make_uncertain(session_local, source, transport)

    # The operator finds the booking in the EasyWeek UI by its marker.
    found = CREATED_UUIDS[KA_RECORD_B]
    transport.plant_booking(found, record_id=KA_RECORD_B)

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            make_inputs(
                MODE_RESOLVE_CREATED,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=KA_RECORD_B,
                resolve_target_booking_uuid=found,
            ),
            write_client=client,
        )
    assert report.errors == []

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    resolved = rows[KA_RECORD_B]
    assert resolved.status == "created"
    assert resolved.target_booking_uuid == found
    # The origin run is preserved; only the resolution run id moved.
    assert resolved.run_id == origin_run
    assert resolved.last_resolution_run_id != origin_run
    # No second POST anywhere on the resolution path.
    assert transport.post_count_for(KA_RECORD_B) == 1


async def test_a_rollback_of_the_origin_run_still_sees_a_resolved_booking(session_local, source):
    transport = RecordingTransport()
    origin_run = await _make_uncertain(session_local, source, transport)
    found = CREATED_UUIDS[KA_RECORD_B]
    transport.plant_booking(found, record_id=KA_RECORD_B)
    async with make_write_client(transport) as client:
        await run_resolve_created(
            session_local,
            make_inputs(
                MODE_RESOLVE_CREATED,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=KA_RECORD_B,
                resolve_target_booking_uuid=found,
            ),
            write_client=client,
        )

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=origin_run, rollback_confirmed=False),
            write_client=client,
        )
    targets = {row["target_booking_uuid"] for row in report.created_rows}
    assert found in targets


async def test_a_wrong_marker_does_not_resolve_a_row(session_local, source):
    transport = RecordingTransport()
    await _make_uncertain(session_local, source, transport)
    # A booking that exists but belongs to a DIFFERENT source record.
    stranger = CREATED_UUIDS[RA_RECORD_A]
    transport.plant_booking(stranger, record_id=RA_RECORD_A)

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            make_inputs(
                MODE_RESOLVE_CREATED,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=KA_RECORD_B,
                resolve_target_booking_uuid=stranger,
            ),
            write_client=client,
        )
    # The marker is derived from the source identity, so a stranger's booking
    # cannot carry this row's marker — the strict projection refuses it.
    assert any("target_malformed" in err for err in report.errors)
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_resolve_absent_needs_both_explicit_confirmations(session_local, source):
    transport = RecordingTransport()
    await _make_uncertain(session_local, source, transport)

    report = await run_resolve_absent(
        session_local,
        make_inputs(
            MODE_RESOLVE_ABSENT,
            resolve_company_id=KARLSRUHE_COMPANY_ID,
            resolve_record_id=KA_RECORD_B,
            resolve_absent_acknowledged=True,
        ),
    )
    assert "absent_resolution_not_confirmed" in report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_resolve_absent_makes_the_row_reclaimable_and_says_so(session_local, source):
    transport = RecordingTransport()
    await _make_uncertain(session_local, source, transport)

    report = await run_resolve_absent(
        session_local,
        make_inputs(
            MODE_RESOLVE_ABSENT,
            resolve_company_id=KARLSRUHE_COMPANY_ID,
            resolve_record_id=KA_RECORD_B,
            resolve_absent_acknowledged=True,
            resolve_absent_confirmed=True,
        ),
    )
    assert any("next apply WILL create" in err for err in report.errors)
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "failed"


# ---------------------------------------------------------------------------
# Blocker 6 — a final reconciliation proves completeness
# ---------------------------------------------------------------------------


async def test_final_reconciliation_passes_only_when_everything_is_accounted_for(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)

    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["source_was_read"] is True
    assert verdict["source_active_bookings"] == 3
    assert verdict["accounted_for"] == 3
    assert report.errors == []


async def test_final_reconciliation_fails_while_a_row_is_unresolved(session_local, source):
    transport = RecordingTransport()
    await _make_uncertain(session_local, source, transport)

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)

    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["uncertain_or_pending"] == 1
    assert report.errors


async def test_final_reconciliation_fails_when_a_source_booking_has_no_target(session_local, source):
    """A booking created in Altegio after the bulk is not a complete cutover."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    source[KARLSRUHE_COMPANY_ID].append(record(id=900003, date="2026-09-20 09:00:00"))
    CREATED_UUIDS[900003] = "aaaaaaaa-0000-4000-8000-000000000003"
    try:
        async with make_write_client(transport) as client:
            report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
        verdict = report.as_safe_dict()["completeness"]
        assert verdict["passed"] is False
        assert verdict["unaccounted_rows"]
    finally:
        CREATED_UUIDS.pop(900003, None)


async def test_final_reconciliation_does_not_call_an_empty_source_a_success(session_local, source, monkeypatch):
    """`source_active_bookings == 0` proves nothing if the API was never read."""
    transport = RecordingTransport()
    # A wave has to be licensed before any reconciliation has something to be
    # about; the scope gate is proven separately.
    await license_bulk(session_local, transport)

    async def _empty(*, company_id, window, timeout_sec=30.0, client=None):
        return []

    monkeypatch.setattr(runner_module, "fetch_company_records", _empty)
    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
    verdict = report.as_safe_dict()["completeness"]
    # The stub answers, so the source WAS read — and with nothing outstanding the
    # verdict may pass. What must never happen is passing without a read.
    assert verdict["source_was_read"] is True

    async def _unread(*, company_id, window, timeout_sec=30.0, client=None):
        raise AltegioSourceError("altegio unreachable")

    monkeypatch.setattr(runner_module, "fetch_company_records", _unread)
    async with make_write_client(transport) as client:
        with pytest.raises(AltegioSourceError):
            await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)


# ---------------------------------------------------------------------------
# Blocker 7 — rollback compares a real snapshot, and stays behind the fence
# ---------------------------------------------------------------------------


async def _applied_run(session_local, source, transport) -> str:
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)
    return apply_inputs.run_id


async def test_rollback_is_read_only_by_default(session_local, source):
    transport = RecordingTransport()
    run_id = await _applied_run(session_local, source, transport)

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=False),
            write_client=client,
        )
    assert report.as_safe_dict()["reason_codes"]["rollback_eligible"] == 2
    assert transport.cancelled == []
    assert report.mutations_attempted == 0


async def test_rollback_only_touches_its_own_run(session_local, source):
    transport = RecordingTransport()
    await _applied_run(session_local, source, transport)
    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id="some-other-run", rollback_confirmed=False),
            write_client=client,
        )
    assert report.as_safe_dict()["reason_codes"] == {}


async def test_a_confirmed_rollback_cancels_and_records(session_local, source):
    transport = RecordingTransport()
    run_id = await _applied_run(session_local, source, transport)

    async with make_write_client(transport) as client:
        await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=True),
            write_client=client,
        )
    assert len(transport.cancelled) == 2
    rolled = [row for row in await ledger_rows(session_local) if row.status == "rolled_back"]
    assert len(rolled) == 2
    # The target uuid survives: an operator must still be able to say what was cancelled.
    assert all(row.target_booking_uuid for row in rolled)


@pytest.mark.parametrize(
    "field,value",
    [
        ("start_time", "2026-09-11T09:00:00Z"),
        ("staff_uuid", "00000000-0000-4000-8000-0000000000aa"),
        ("service_uuid", "00000000-0000-4000-8000-0000000000bb"),
        ("customer_uuid", "00000000-0000-4000-8000-0000000000cc"),
        ("location_uuid", "00000000-0000-4000-8000-0000000000dd"),
        ("duration", 90),
        ("comment", "moved to Thursday, called the client"),
    ],
)
async def test_a_target_changed_after_migration_is_never_cancelled(session_local, source, field, value):
    """Marker-plus-status was not enough: all of these survive both checks."""
    transport = RecordingTransport()
    run_id = await _applied_run(session_local, source, transport)
    edited = CREATED_UUIDS[KA_RECORD_B]
    transport.bookings[edited][field] = value

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=True),
            write_client=client,
        )
    assert report.as_safe_dict()["reason_codes"]["rollback_target_modified_after_migration"] == 1
    assert edited not in transport.cancelled


async def test_a_malformed_target_is_never_cancelled(session_local, source):
    transport = RecordingTransport()
    run_id = await _applied_run(session_local, source, transport)
    edited = CREATED_UUIDS[KA_RECORD_B]
    transport.bookings[edited].pop("staff_uuid")

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=True),
            write_client=client,
        )
    assert report.as_safe_dict()["reason_codes"]["rollback_target_modified_after_migration"] == 1
    assert edited not in transport.cancelled


@pytest.mark.parametrize("flag", ["easyweek_notifications_enabled", "easyweek_reviews_enabled"])
async def test_a_rollback_with_notifications_back_on_cancels_nothing(session_local, source, monkeypatch, flag):
    """Cancelling emits EasyWeek events too. Same fence as a creation."""
    transport = RecordingTransport()
    run_id = await _applied_run(session_local, source, transport)
    monkeypatch.setattr(settings, flag, True, raising=False)

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=True),
            write_client=client,
        )
    assert "rollback_notification_gate_refused" in report.errors
    assert transport.cancelled == []


async def test_a_rollback_without_the_native_attestation_cancels_nothing(session_local, source):
    transport = RecordingTransport()
    run_id = await _applied_run(session_local, source, transport)

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(
                MODE_ROLLBACK_DRY_RUN,
                rollback_run_id=run_id,
                rollback_confirmed=True,
                native_notifications_confirmed=False,
            ),
            write_client=client,
        )
    assert "rollback_notification_gate_refused" in report.errors
    assert transport.cancelled == []


# ---------------------------------------------------------------------------
# Log hygiene
# ---------------------------------------------------------------------------


async def test_normal_logs_carry_no_pii(session_local, source, caplog):
    transport = RecordingTransport()
    with caplog.at_level(logging.INFO):
        await license_bulk(session_local, transport)
        plan = await run_dry_run(session_local)
        async with make_write_client(transport) as client:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )

    text_log = "\n".join(record.getMessage() for record in caplog.records)
    assert CUSTOMER_PHONE not in text_log
    assert CUSTOMER_UUID not in text_log
    assert "test-key" not in text_log
    # Not even the target identifiers: a log line is not a report, and the
    # report is where identifiers belong.
    assert KA_LOCATION_UUID not in text_log
    assert CREATED_UUIDS[KA_RECORD_A] not in text_log
