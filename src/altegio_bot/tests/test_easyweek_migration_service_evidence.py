"""Review of 89401fc: the service proof that could be walked around three ways.

The hotfix added a service check and then left three doors open. Each of the six
scenarios below passed against a live-shaped fake on that commit, and each one is
a way to migrate or cancel a booking whose service nobody proved.

**It was optional where it mattered most.** ``prove_live_target`` took
``service_expectation`` as a keyword defaulting to ``None``, and the final
reconciliation and rollback never passed it — so the check switched itself off
in exactly the two places that decide "this wave is complete" and "cancel this
booking", and a fingerprint match over the remaining fields read as clean.

**The catalogue went stale inside a run.** The evidence book cached both the
catalogue and every pinned expectation for the whole run, so a bulk apply proved
uniqueness against the catalogue as it stood before its *first* booking. A
look-alike service added half-way through was invisible to every POST after it.

**The baseline re-derived itself.** The expectation was rebuilt from the current
catalogue on every run, so renaming a service between the canary and the bulk
produced a new expectation that the new catalogue satisfied by construction. The
plan digest did not move, and the old canary went on licensing the wave.

The fix is one rule in three places: an expectation is **established once,
stored, and only ever verified** — against a catalogue read again at each
operation boundary, by every path that proves an active target.
"""

from __future__ import annotations

from copy import deepcopy

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import reproof as reproof_module
from altegio_bot.easyweek_migration.baseline_store import get_baseline, load_baselines
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    MODE_ROLLBACK_DRY_RUN,
    ServiceEvidence,
    load_service_evidence,
    run_apply,
    run_reconcile,
    run_resolve_created,
    run_rollback,
)
from altegio_bot.models.models import EasyWeekMigrationServiceBaseline
from altegio_bot.tests.easyweek_migration_harness import (
    CATALOG_SERVICES,
    CREATED_UUIDS,
    KA_RECORD_A,
    KA_RECORD_B,
    KARLSRUHE_COMPANY_ID,
    RA_RECORD_A,
    RASTATT_COMPANY_ID,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    license_bulk,
    make_inputs,
    make_write_client,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_live_proof import applied_run
from altegio_bot.tests.test_easyweek_migration_planning import (
    KA_LOCATION_UUID,
    KA_SERVICE_UUID,
    RA_LOCATION_UUID,
)

LOOKALIKE_UUID = "eeeeeeee-0000-4000-8000-000000000001"


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def transport() -> RecordingTransport:
    """A fake with its own catalogue copy, so a test can edit it mid-run."""
    return RecordingTransport(catalog=deepcopy(CATALOG_SERVICES))


def add_lookalike(t: RecordingTransport, location_uuid: str = KA_LOCATION_UUID) -> None:
    """Publish a second service identical to the first in every compared field."""
    original = t.catalog[location_uuid][0]
    t.catalog[location_uuid].append({**original, "uuid": LOOKALIKE_UUID})


def rename_service(t: RecordingTransport, name: str = "A different treatment") -> None:
    """Same uuid, same price, same length — only the name moved."""
    entry = t.catalog[KA_LOCATION_UUID][0]
    t.catalog[KA_LOCATION_UUID][0] = {**entry, "name": name}


async def final(session_local, t: RecordingTransport):
    async with make_write_client(t) as client:
        return await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)


# ---------------------------------------------------------------------------
# 1. The final reconciliation must not skip the service
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("drift", ["lookalike", "unreadable", "direct_conflict"])
async def test_final_reconciliation_refuses_an_unproven_service(session_local, source, drift):
    """All three returned passed=true before the fix.

    A completeness verdict is the sentence "this wave landed". Reaching it
    without ever checking which service is on the bookings makes it a sentence
    about the fields that happened to be easy to compare.
    """
    t = transport()
    await applied_run(session_local, source, t)

    if drift == "lookalike":
        add_lookalike(t)
    elif drift == "unreadable":
        t.catalog_status_override = 503
    else:
        # A direct catalogue link that contradicts us must never be rescued by
        # attributes that match.
        t.bookings[CREATED_UUIDS[KA_RECORD_B]]["ordered_services"][0]["service_uuid"] = LOOKALIKE_UUID

    verdict = (await final(session_local, t)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_rows"] or verdict["manual_action_required"]


async def test_an_unchanged_catalogue_still_reconciles_clean(session_local, source):
    """The refusals above are about drift, not about the check being unpassable."""
    t = transport()
    await applied_run(session_local, source, t)
    verdict = (await final(session_local, t)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True


# ---------------------------------------------------------------------------
# 2. Rollback must not cancel what it cannot identify
# ---------------------------------------------------------------------------


async def test_rollback_refuses_an_ambiguous_service_and_sends_no_cancel(session_local, source):
    t = transport()
    run_id = await applied_run(session_local, source, t)
    add_lookalike(t)

    async with make_write_client(t) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=True),
            write_client=client,
        )

    assert any(row["source_record_id"] == KA_RECORD_B for row in report.blocked_rows)
    assert report.as_safe_dict()["reason_codes"]["rollback_service_evidence_unproven"] >= 1
    # The point of refusing: no cancel was sent for the booking we cannot
    # identify. Rastatt's catalogue is untouched, so its own booking is still
    # provable and is still cancelled — a refusal is per row, not a global stop.
    assert CREATED_UUIDS[KA_RECORD_B] not in t.cancelled


async def test_rollback_still_works_when_the_catalogue_is_unchanged(session_local, source):
    t = transport()
    run_id = await applied_run(session_local, source, t)

    async with make_write_client(t) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=False),
            write_client=client,
        )

    assert report.as_safe_dict()["reason_codes"]["rollback_eligible"] >= 1


# ---------------------------------------------------------------------------
# 3. A rename between canary and bulk must not re-baseline itself
# ---------------------------------------------------------------------------


async def test_a_renamed_service_stops_the_bulk_even_though_the_plan_digest_is_unchanged(session_local, source):
    """The defect in its purest form.

    Nothing about the *plan* changed — same uuid, same price, same length, same
    digest, same reviewed dry-run id — so the old canary still licensed the wave.
    Only the stored baseline can notice, because only it remembers what an
    operator actually reviewed.
    """
    t = transport()
    await license_bulk(session_local, t)
    before = await run_dry_run(session_local)

    rename_service(t)

    plan = await run_dry_run(session_local)
    assert before.plan_digest == plan.plan_digest  # the plan really is identical

    async with make_write_client(t) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert t.post_count_for(KA_RECORD_B) == 0
    assert "service_attributes_changed" in report.as_safe_dict()["reason_codes"]


async def test_a_rename_is_not_repaired_by_running_again(session_local, source):
    """A stored baseline is not regenerated from whatever the catalogue says now."""
    t = transport()
    await license_bulk(session_local, t)
    rename_service(t)

    for _ in range(2):
        plan = await run_dry_run(session_local)
        async with make_write_client(t) as client:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )
    assert t.post_count_for(KA_RECORD_B) == 0

    async with session_local() as session:
        stored = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID)
    assert stored is not None
    assert stored.normalized_name != "a different treatment"


# ---------------------------------------------------------------------------
# 4. The catalogue is re-read at every boundary, not once per run
# ---------------------------------------------------------------------------


async def test_a_lookalike_added_mid_run_stops_the_next_post(session_local, source, monkeypatch):
    """Published between one booking's source re-proof and the next one's POST."""
    t = transport()
    await license_bulk(session_local, t)
    original = reproof_module.fetch_single_record
    changed = False

    async def _fetch_and_publish(*args, **kwargs):
        nonlocal changed
        result = await original(*args, **kwargs)
        if kwargs.get("record_id") == KA_RECORD_B and not changed:
            changed = True
            add_lookalike(t)
        return result

    monkeypatch.setattr(reproof_module, "fetch_single_record", _fetch_and_publish)

    plan = await run_dry_run(session_local)
    async with make_write_client(t) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert changed
    assert t.post_count_for(KA_RECORD_B) == 0


async def test_a_catalogue_failure_before_readback_keeps_the_target_and_stops_the_run(session_local, source):
    """The POST landed and the proof cannot be finished. Keep it, stop, do nothing else."""
    t = transport()
    await license_bulk(session_local, t)
    plan = await run_dry_run(session_local)

    original_store = t._store

    def _store_then_break(body, record_id):
        uuid = original_store(body, record_id)
        add_lookalike(t)
        return uuid

    t._store = _store_then_break  # type: ignore[method-assign]

    async with make_write_client(t) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert any("halted" in error for error in safe["errors"])
    # The booking that broke its own proof is the last one created.
    assert t.post_count_for(KA_RECORD_B) == 1
    assert t.post_count_for(RA_RECORD_A) == 0
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    created = rows[KA_RECORD_B]
    # Kept for a human, never retried, never cancelled.
    assert created.status == "created"
    assert created.target_booking_uuid
    assert t.cancelled == []


# ---------------------------------------------------------------------------
# 5. The baseline is durable and belongs to the wave that established it
# ---------------------------------------------------------------------------


async def test_the_expectation_survives_a_fresh_process(session_local, source):
    """A restart must not lose it — and must not invent a replacement."""
    t = transport()
    await license_bulk(session_local, t)

    async with session_local() as session:
        rows = (await session.execute(select(EasyWeekMigrationServiceBaseline))).scalars().all()
    assert [row.easyweek_service_uuid for row in rows] == [KA_SERVICE_UUID]

    # A brand-new evidence object, as a later command in a later process gets.
    reloaded = await load_service_evidence(session_local, make_inputs(MODE_APPLY))
    assert (KA_LOCATION_UUID, KA_SERVICE_UUID) in reloaded.baselines
    stored = reloaded.baselines[(KA_LOCATION_UUID, KA_SERVICE_UUID)]
    assert (stored.currency, stored.price_minor, stored.duration_minutes) == ("EUR", 9000, 60)


async def test_missing_evidence_is_a_named_refusal_not_a_pass(session_local, source):
    """A target created before this contract existed cannot be waved through."""
    t = transport()
    await applied_run(session_local, source, t)
    async with session_local() as session:
        async with session.begin():
            await session.execute(EasyWeekMigrationServiceBaseline.__table__.delete())

    verdict = (await final(session_local, t)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert "service_baseline_missing" in verdict["unaccounted_reason_codes"]


async def test_evidence_written_under_another_version_is_refused(session_local, source):
    from sqlalchemy import update

    t = transport()
    await applied_run(session_local, source, t)
    async with session_local() as session:
        async with session.begin():
            await session.execute(update(EasyWeekMigrationServiceBaseline).values(proof_version="v0"))

    verdict = (await final(session_local, t)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert "service_baseline_version_unsupported" in verdict["unaccounted_reason_codes"]


async def test_a_stored_baseline_is_never_overwritten_by_a_later_run(session_local, source):
    t = transport()
    await license_bulk(session_local, t)
    async with session_local() as session:
        first = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID)

    rename_service(t)
    plan = await run_dry_run(session_local)
    async with make_write_client(t) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    async with session_local() as session:
        after = await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID)
    assert first is not None and after is not None
    assert after.digest == first.digest


# ---------------------------------------------------------------------------
# 6. Recovery, other branches, and everything that must keep working
# ---------------------------------------------------------------------------


async def test_uncertain_recovery_uses_the_stored_baseline_not_a_new_catalogue_answer(session_local, source):
    """A restart must not make an ordinary recovery impossible — nor easier."""
    import httpx

    from altegio_bot.easyweek_migration.runner import MODE_CANARY, run_canary

    t = transport()
    t.fail_with = {KA_RECORD_A: httpx.ReadTimeout("t", request=httpx.Request("POST", "https://x"))}
    plan = await run_dry_run(session_local)
    async with make_write_client(t) as client:
        await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_A,
            ),
            write_client=client,
        )
    t.fail_with = {}
    t.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)

    # The baseline was written before the POST, so it survived the unknown outcome.
    async with session_local() as session:
        assert await get_baseline(session, location_uuid=KA_LOCATION_UUID, service_uuid=KA_SERVICE_UUID) is not None

    async with make_write_client(t) as client:
        report = await run_resolve_created(
            session_local,
            make_inputs(
                MODE_RESOLVE_CREATED,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=KA_RECORD_A,
                resolve_target_booking_uuid=CREATED_UUIDS[KA_RECORD_A],
            ),
            write_client=client,
        )
    assert report.errors == []

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A].status == "created"


async def test_a_lookalike_in_another_branch_does_not_make_this_one_ambiguous(session_local, source):
    """Uniqueness is judged inside the target location's catalogue, not across them."""
    t = transport()
    add_lookalike(t, location_uuid=RA_LOCATION_UUID)
    # The Rastatt clone shares its attributes with a Rastatt service, so Rastatt
    # itself is ambiguous — Karlsruhe must be unaffected.
    await license_bulk(session_local, t)

    plan = await run_dry_run(session_local)
    async with make_write_client(t) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert t.post_count_for(KA_RECORD_B) == 1
    assert t.post_count_for(RA_RECORD_A) == 0
    codes = report.as_safe_dict()["by_company"]
    assert codes[str(RASTATT_COMPANY_ID)].get("blocked") == 1


async def test_an_empty_evidence_object_proves_nothing(session_local):
    evidence = ServiceEvidence()
    assert evidence.baselines == {}
    assert evidence.as_safe_dict()["catalog_observations"] == 0


async def test_the_previous_wave_is_checked_against_its_own_stored_baseline(session_local, source):
    """An earlier wave's target uses the expectation written when it was created."""
    t = transport()
    await applied_run(session_local, source, t)

    async with session_local() as session:
        baselines = await load_baselines(session, location_uuids=(KA_LOCATION_UUID, RA_LOCATION_UUID))
    # Both branches migrated, so both have their own stored expectation.
    assert {key[0] for key in baselines} == {KA_LOCATION_UUID, RA_LOCATION_UUID}

    verdict = (await final(session_local, t)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
