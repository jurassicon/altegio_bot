"""PR-11.1: the cumulative guard was escapable by deleting a whole branch.

The hole was a rule that looked strict. ``selected_altegio_staff_ids`` had to be
non-empty **per branch**, so an operator whose second wave had nothing new in
Rastatt could not write ``"selected_altegio_staff_ids": []`` there. The file was
rejected, and the two ways out were both wrong:

* delete the Rastatt branch — the manifest is then valid, and Rastatt's live
  wave-A rows disappear from the cumulative guard *and* from the final
  reconciliation, because both loaded the ledger by ``manifest.company_ids``.
  Wave B canaries, applies and passes without ever looking at Rastatt;
* keep Rastatt's old selection — the manifest is valid and the guard works, but
  an already-migrated master is selected again, so her NEW bookings are dragged
  into a wave that was never meant to contain them.

The fix is one rule moved up a level and two sweeps widened: an empty selector is
allowed for a branch and forbidden for the whole file, and both the guard and the
final reconciliation read every migrating branch's ledger rows regardless of what
the manifest names.
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.gates import ApplyGateError
from altegio_bot.easyweek_migration.manifest import (
    INVALID_STAFF_SCOPE_EMPTY,
    KARLSRUHE_COMPANY_ID,
    inventory_manifest,
    parse_manifest,
)
from altegio_bot.easyweek_migration.previous_wave import PREV_BRANCH_MISSING
from altegio_bot.easyweek_migration.proof import GHOST_TARGET_STILL_ACTIVE
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_DRY_RUN,
    MODE_RECONCILE,
    MODE_ROLLBACK_DRY_RUN,
    run_apply,
    run_canary,
    run_inventory_or_dry_run,
    run_reconcile,
    run_rollback,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    KA_RECORD_A,
    RA_RECORD_A,
    RASTATT_COMPANY_ID,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    license_bulk,
    make_inputs,
    make_write_client,
    manifest_json,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_multi_wave import (
    DEFERRED_STAFF_UUID,
    WAVE_B_RECORD,
    wave_b_manifest,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    KA_DEFERRED_STAFF_ID,
    KA_STAFF_ID,
    RA_SERVICE_ID,
    RA_STAFF_ID,
    record,
)

# A booking of the already-migrated Rastatt master that appears AFTER wave A.
# It is the thing "just re-select her so the parser is happy" would migrate by
# accident, so wave B has to leave it alone.
NEW_RASTATT_RECORD = 910070
CREATED_UUIDS.setdefault(NEW_RASTATT_RECORD, "bbbbbbbb-0000-4000-8000-000000000070")
assert DEFERRED_STAFF_UUID  # imported for the wave-B manifest it belongs to


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    rows = stub_altegio_source(monkeypatch)
    rows[KARLSRUHE_COMPANY_ID].append(
        record(id=WAVE_B_RECORD, date="2026-09-16 10:00:00", staff_id=KA_DEFERRED_STAFF_ID)
    )
    return rows


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def add_new_rastatt_booking(source) -> None:
    """A fresh booking of the Rastatt master wave A already migrated."""
    source[RASTATT_COMPANY_ID].append(
        record(
            id=NEW_RASTATT_RECORD,
            date="2026-09-18 10:00:00",
            staff_id=RA_STAFF_ID,
            services=[{"id": RA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}],
        )
    )


def wave_b_without_rastatt():
    """The mistake: Rastatt deleted outright rather than kept as context."""
    payload = json.loads(manifest_json())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_DEFERRED_STAFF_ID]
    branch["deferred_altegio_staff_ids"] = [KA_STAFF_ID]
    branch["staff"] = {
        str(KA_STAFF_ID): branch["staff"][str(KA_STAFF_ID)],
        str(KA_DEFERRED_STAFF_ID): DEFERRED_STAFF_UUID,
    }
    del payload["branches"][str(RASTATT_COMPANY_ID)]
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid, manifest.reason
    return manifest


def wave_b(mode: str, manifest, **overrides):
    return make_inputs(mode, manifest=manifest, **overrides)


async def run_wave_a(session_local, transport) -> str:
    """Canary + bulk for wave A: two Karlsruhe bookings and one Rastatt booking."""
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)
    return apply_inputs.run_id


async def dry_run_b(session_local, manifest):
    async with session_local() as session:
        return await run_inventory_or_dry_run(session, wave_b(MODE_DRY_RUN, manifest))


async def canary_b(session_local, transport, manifest, plan_digest):
    async with make_write_client(transport) as client:
        return await run_canary(
            session_local,
            wave_b(
                MODE_CANARY,
                manifest,
                verified_dry_run_id=plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=WAVE_B_RECORD,
            ),
            write_client=client,
        )


async def final_b(session_local, transport, manifest, **overrides):
    async with make_write_client(transport) as client:
        return await run_reconcile(
            session_local, wave_b(MODE_RECONCILE, manifest, final=True, **overrides), write_client=client
        )


async def proofs(session_local) -> list[EasyWeekMigrationCanaryProof]:
    async with session_local() as session:
        return list((await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().all())


# ---------------------------------------------------------------------------
# 1-3. What the parser now accepts, and what it still refuses
# ---------------------------------------------------------------------------


def test_a_branch_with_no_new_masters_may_keep_an_empty_selector():
    """Rastatt stays in the file as pure cumulative context."""
    manifest = wave_b_manifest()
    assert manifest.valid, manifest.reason

    rastatt = manifest.branch(RASTATT_COMPANY_ID)
    assert rastatt is not None
    assert rastatt.selected_staff_ids == frozenset()
    # Not migrating anybody is not the same as carrying nothing: the earlier
    # wave's master, her mapping and her service baseline are all still here.
    assert RA_STAFF_ID in rastatt.deferred_staff_ids
    assert rastatt.staff_uuid(RA_STAFF_ID)
    assert rastatt.service(RA_SERVICE_ID) is not None

    karlsruhe = manifest.branch(KARLSRUHE_COMPANY_ID)
    assert karlsruhe is not None
    assert karlsruhe.selected_staff_ids


def test_a_manifest_that_selects_nobody_anywhere_is_still_refused():
    payload = json.loads(manifest_json())
    for key, branch in payload["branches"].items():
        branch["deferred_altegio_staff_ids"] = sorted(
            set(branch["deferred_altegio_staff_ids"]) | set(branch["selected_altegio_staff_ids"])
        )
        branch["selected_altegio_staff_ids"] = []
        assert key  # every branch, not just one

    manifest = parse_manifest(json.dumps(payload))
    assert not manifest.valid
    assert manifest.reason == INVALID_STAFF_SCOPE_EMPTY


def test_inventory_still_accepts_an_unfinished_manifest():
    """The chicken-and-egg exemption is untouched by the new rule."""
    payload = json.loads(manifest_json())
    for branch in payload["branches"].values():
        branch["selected_altegio_staff_ids"] = []
        branch["deferred_altegio_staff_ids"] = []
        branch["staff"] = {}
        branch["services"] = {}

    raw = json.dumps(payload)
    assert not parse_manifest(raw).valid
    assert inventory_manifest(raw).valid


# ---------------------------------------------------------------------------
# 4-5. Wave A across both branches, then a correct Karlsruhe-only wave B
# ---------------------------------------------------------------------------


async def test_wave_a_migrates_both_branches(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A].status == "created"
    assert rows[RA_RECORD_A].status == "created"
    assert rows[RA_RECORD_A].target_booking_uuid == CREATED_UUIDS[RA_RECORD_A]

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
    assert report.as_safe_dict()["completeness"]["passed"] is True


async def test_a_correct_wave_b_carries_rastatt_without_touching_it(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)
    add_new_rastatt_booking(source)

    manifest = wave_b_manifest()
    plan = await dry_run_b(session_local, manifest)
    context = plan.as_safe_dict()["previous_wave_context"]
    assert context["proven"] is True
    # Rastatt's live wave-A row is checked even though wave B selects nobody there.
    assert context["checked"] >= 1

    posts_for_rastatt = transport.post_count_for(RA_RECORD_A)
    await canary_b(session_local, transport, manifest, plan.plan_digest)
    plan2 = await dry_run_b(session_local, manifest)
    async with make_write_client(transport) as client:
        applied = await run_apply(
            session_local,
            wave_b(MODE_APPLY, manifest, verified_dry_run_id=plan2.plan_digest),
            write_client=client,
        )
    assert applied.errors == []

    # The already-migrated Rastatt booking was not written a second time, and the
    # NEW booking of that same master did not sneak into this wave.
    assert transport.post_count_for(RA_RECORD_A) == posts_for_rastatt
    assert transport.post_count_for(NEW_RASTATT_RECORD) == 0
    assert NEW_RASTATT_RECORD not in {row.source_record_id for row in await ledger_rows(session_local)}

    verdict = (await final_b(session_local, transport, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    # Rastatt's target was fetched and proven, not assumed from the ledger.
    assert verdict["earlier_wave_targets_proven"] == 3


async def test_a_correct_wave_b_cannot_pass_if_the_rastatt_target_is_gone(session_local, source):
    """The PASS above is earned by a target proof, not by carrying the branch."""
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)
    manifest = wave_b_manifest()
    plan = await dry_run_b(session_local, manifest)
    await canary_b(session_local, transport, manifest, plan.plan_digest)

    del transport.bookings[CREATED_UUIDS[RA_RECORD_A]]

    verdict = (await final_b(session_local, transport, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_not_found_in_easyweek"] == 1


# ---------------------------------------------------------------------------
# 6. Deleting the branch is caught before the first mutation
# ---------------------------------------------------------------------------


async def test_deleting_rastatt_is_reported_by_the_dry_run(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    plan = await dry_run_b(session_local, wave_b_without_rastatt())
    context = plan.as_safe_dict()["previous_wave_context"]

    assert context["proven"] is False
    assert [row["reason"] for row in context["rows"]] == [PREV_BRANCH_MISSING]
    row = context["rows"][0]
    assert row["source_company_id"] == RASTATT_COMPANY_ID
    assert row["source_record_id"] == RA_RECORD_A
    # Ids and codes only.
    assert set(row) == {
        "source_company_id",
        "source_record_id",
        "reason",
        "detail",
        "altegio_staff_id",
        "altegio_service_id",
    }
    blob = json.dumps(context)
    for forbidden in ("phone", "+49", "client", "name"):
        assert forbidden not in blob


async def test_deleting_rastatt_blocks_the_canary_before_any_post(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    manifest = wave_b_without_rastatt()
    plan = await dry_run_b(session_local, manifest)
    posts_before = transport.mutations
    ledger_before = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    proofs_before = [(p.id, p.verified) for p in await proofs(session_local)]

    with pytest.raises(ApplyGateError) as exc:
        await canary_b(session_local, transport, manifest, plan.plan_digest)

    assert "previous_wave_context_unprovable" in exc.value.failures
    assert transport.mutations == posts_before
    assert transport.cancelled == []
    assert {row.source_record_id: row.status for row in await ledger_rows(session_local)} == ledger_before
    assert [(p.id, p.verified) for p in await proofs(session_local)] == proofs_before


async def test_deleting_rastatt_blocks_the_bulk_apply_before_any_post(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    manifest = wave_b_without_rastatt()
    plan = await dry_run_b(session_local, manifest)
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local,
                wave_b(MODE_APPLY, manifest, verified_dry_run_id=plan.plan_digest),
                write_client=client,
            )

    assert "previous_wave_context_unprovable" in exc.value.failures
    assert transport.mutations == posts_before
    assert WAVE_B_RECORD not in {row.source_record_id for row in await ledger_rows(session_local)}


async def test_a_final_reconciliation_cannot_pass_with_rastatt_deleted(session_local, source):
    """Even reached by another route, the missing branch cannot buy a PASS."""
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)

    verdict = (await final_b(session_local, transport, wave_b_without_rastatt())).as_safe_dict()["completeness"]
    assert verdict["passed"] is False


# ---------------------------------------------------------------------------
# 7-8. Fail-closed, and what a genuinely terminal source means
# ---------------------------------------------------------------------------


async def test_a_missing_branch_with_an_unreadable_source_fails_closed(session_local, source, monkeypatch):
    from altegio_bot.easyweek_migration import reproof as reproof_module
    from altegio_bot.easyweek_migration.altegio_source import AltegioSourceError

    transport = RecordingTransport()
    await run_wave_a(session_local, transport)
    manifest = wave_b_without_rastatt()

    async def _boom(*, company_id, record_id, timeout_sec=30.0, client=None):
        raise AltegioSourceError("altegio unreachable")

    monkeypatch.setattr(reproof_module, "fetch_single_record", _boom)
    posts_before = transport.mutations

    plan = await dry_run_b(session_local, manifest)
    context = plan.as_safe_dict()["previous_wave_context"]
    assert context["proven"] is False
    # "We could not read it" is never "the branch is fine".
    assert [row["detail"] for row in context["rows"] if row["source_record_id"] == RA_RECORD_A] == ["source_unreadable"]

    with pytest.raises(ApplyGateError) as exc:
        await canary_b(session_local, transport, manifest, plan.plan_digest)
    assert "previous_wave_context_unprovable" in exc.value.failures
    assert transport.mutations == posts_before


@pytest.mark.parametrize(
    "make_terminal",
    [
        pytest.param(lambda row: row.update({"confirmed": 0}), id="cancelled"),
        pytest.param(lambda row: row.update({"deleted": True}), id="deleted"),
    ],
)
async def test_a_terminal_source_is_not_a_live_branch_obligation(session_local, source, make_terminal):
    """A cancelled booking does not oblige anybody to keep its branch.

    Its target is still the final reconciliation's business — and that check runs
    on the widened ledger sweep, so deleting the branch does not hide it either.
    """
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)
    for row in source[RASTATT_COMPANY_ID]:
        if row["id"] == RA_RECORD_A:
            make_terminal(row)

    manifest = wave_b_without_rastatt()
    plan = await dry_run_b(session_local, manifest)
    context = plan.as_safe_dict()["previous_wave_context"]
    assert context["proven"] is True
    assert [row["reason"] for row in context["rows"]] == []

    # The wave may start, and does.
    await canary_b(session_local, transport, manifest, plan.plan_digest)
    plan2 = await dry_run_b(session_local, manifest)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local,
            wave_b(MODE_APPLY, manifest, verified_dry_run_id=plan2.plan_digest),
            write_client=client,
        )

    # The cancelled source still has a standing EasyWeek booking: a real ghost.
    verdict = (await final_b(session_local, transport, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1
    assert verdict["unaccounted_reason_codes"][GHOST_TARGET_STILL_ACTIVE] == 1
    assert [row["source_record_id"] for row in verdict["manual_action_required"]] == [RA_RECORD_A]

    # Once the target is gone too, the pair is a consistent terminal state.
    del transport.bookings[CREATED_UUIDS[RA_RECORD_A]]
    verdict = (await final_b(session_local, transport, manifest)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["inactive_source_targets_terminal"] == 1


# ---------------------------------------------------------------------------
# 9. Rollback is not gated on any of this
# ---------------------------------------------------------------------------


async def test_rollback_still_works_with_a_branch_missing(session_local, source):
    """An emergency undo must not need a well-formed cumulative manifest."""
    transport = RecordingTransport()
    run_id = await run_wave_a(session_local, transport)
    manifest = wave_b_without_rastatt()

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            wave_b(MODE_ROLLBACK_DRY_RUN, manifest, rollback_run_id=run_id, rollback_confirmed=False),
            write_client=client,
        )

    failures = (report.as_safe_dict()["gate"] or {}).get("failures", [])
    assert "previous_wave_context_unprovable" not in failures
    assert "previous_wave_context_unprovable" not in report.errors
    assert report.as_safe_dict()["reason_codes"].get("rollback_eligible")


async def test_the_missing_branch_reconciliation_never_mutates(session_local, source):
    transport = RecordingTransport()
    await run_wave_a(session_local, transport)
    posts_before = transport.mutations

    await final_b(session_local, transport, wave_b_without_rastatt())

    assert transport.mutations == posts_before
    assert transport.cancelled == []
