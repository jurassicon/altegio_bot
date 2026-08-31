"""PR-11.1 revision 19, P1 #2: a target that outlived its source.

The gap. A booking is migrated; the customer then cancels it in Altegio, or it is
deleted, or it simply stops coming back from the list API. ``classify_record()``
returns ``SKIPPED`` before it ever consults the ledger, and the completeness check
only looked at *active* source bookings — so the appointment the migration created
in EasyWeek dropped out of the check entirely and kept standing. An extra
appointment nobody made, in the new schedule, while the reconciliation reported
success.

The fix makes the final reconciliation two-sided: every row the ledger says was
created is accounted for from the ledger side too, and a target whose source is
gone must be proven gone or finished.

Read-only throughout. Reconciliation reports a ghost and refuses to pass; it never
cancels anything.
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.proof import (
    GHOST_TARGET_MALFORMED,
    GHOST_TARGET_STILL_ACTIVE,
    GHOST_TARGET_UNREADABLE,
)
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_RECONCILE,
    run_apply,
    run_reconcile,
)
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    CUSTOMER_PHONE,
    KA_RECORD_B,
    KARLSRUHE_COMPANY_ID,
    RASTATT_COMPANY_ID,
    RecordingTransport,
    apply_production_flags,
    license_bulk,
    make_inputs,
    make_write_client,
    run_dry_run,
    stub_altegio_source,
)

GHOST_UUID = CREATED_UUIDS[KA_RECORD_B]


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


async def migrated_wave(session_local, transport) -> None:
    """Canary + bulk: three bookings created, all proven."""
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )


async def final(session_local, transport):
    async with make_write_client(transport) as client:
        return await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)


def deactivate_source(source, record_id: int, **changes) -> None:
    """Make the source booking inactive the way Altegio would."""
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == record_id:
            row.update(changes)
            return
    raise AssertionError(f"record {record_id} not in the fixture")


def remove_source(source, record_id: int) -> None:
    """The source stops coming back from the list API at all."""
    source[KARLSRUHE_COMPANY_ID] = [r for r in source[KARLSRUHE_COMPANY_ID] if r["id"] != record_id]


# ---------------------------------------------------------------------------
# 1–3. An inactive source with a target still standing is a ghost
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "make_inactive",
    [
        pytest.param(lambda s: deactivate_source(s, KA_RECORD_B, confirmed=0), id="cancelled"),
        pytest.param(lambda s: deactivate_source(s, KA_RECORD_B, deleted=True), id="deleted"),
        pytest.param(lambda s: remove_source(s, KA_RECORD_B), id="absent from list api"),
        pytest.param(lambda s: deactivate_source(s, KA_RECORD_B, attendance=1), id="completed"),
        pytest.param(
            lambda s: deactivate_source(s, KA_RECORD_B, date="2026-08-01 10:00:00"),
            id="rescheduled into the past",
        ),
    ],
)
async def test_an_inactive_source_with_an_active_target_blocks_the_pass(session_local, source, make_inactive):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    make_inactive(source)

    report = await final(session_local, transport)
    verdict = report.as_safe_dict()["completeness"]

    assert verdict["passed"] is False
    assert verdict["ghost_targets_active"] == 1
    assert verdict["unaccounted_reason_codes"][GHOST_TARGET_STILL_ACTIVE] == 1
    # The target really was fetched, and it is named for a human to act on.
    assert verdict["inactive_source_targets_checked"] == 1
    manual = verdict["manual_action_required"]
    assert [row["source_record_id"] for row in manual] == [KA_RECORD_B]
    assert manual[0]["target_booking_uuid"] == GHOST_UUID


async def test_a_deferred_master_is_not_a_ghost(session_local, source):
    """Her bookings were never migrated, so there is no ledger row to sweep."""
    from altegio_bot.easyweek_migration.manifest import parse_manifest
    from altegio_bot.tests.easyweek_migration_harness import manifest_json
    from altegio_bot.tests.test_easyweek_migration_planning import KA_DEFERRED_STAFF_ID, record

    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    # A booking of a deferred master appears; the selector is unchanged.
    source[KARLSRUHE_COMPANY_ID].append(record(id=900050, date="2026-09-14 10:00:00", staff_id=KA_DEFERRED_STAFF_ID))
    payload = json.loads(manifest_json())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["deferred_altegio_staff_ids"] = [KA_DEFERRED_STAFF_ID]
    unchanged = parse_manifest(json.dumps(payload))
    assert unchanged.digest == parse_manifest(manifest_json()).digest

    report = await final(session_local, transport)
    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["ghost_targets_active"] == 0
    assert verdict["deferred_bookings"] == 1


# ---------------------------------------------------------------------------
# 4–5. An inactive source whose target is also gone is consistent
# ---------------------------------------------------------------------------


async def test_a_cancelled_source_with_a_deleted_target_is_consistent(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)
    del transport.bookings[GHOST_UUID]

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["ghost_targets_active"] == 0
    assert verdict["inactive_source_targets_terminal"] == 1


@pytest.mark.parametrize("flag", ["is_canceled", "is_completed"])
async def test_a_cancelled_source_with_a_terminal_target_is_consistent(session_local, source, flag):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, deleted=True)
    transport.bookings[GHOST_UUID][flag] = True

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["inactive_source_targets_terminal"] == 1


async def test_a_terminal_target_is_not_accepted_on_a_malformed_payload(session_local, source):
    """ "We could not read it" is never "it is finished"."""
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)
    transport.bookings[GHOST_UUID].pop("is_canceled")

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert any(GHOST_TARGET_MALFORMED in reason for reason in verdict["unaccounted_reason_codes"])


async def test_a_rewritten_marker_is_not_accepted_as_terminal(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)
    transport.bookings[GHOST_UUID]["public_notes"] = "somebody rewrote this"
    transport.bookings[GHOST_UUID]["is_canceled"] = True

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert any(GHOST_TARGET_MALFORMED in reason for reason in verdict["unaccounted_reason_codes"])


async def test_an_unreadable_target_of_an_inactive_source_fails_closed(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)
    transport.get_status_override = {GHOST_UUID: 503}

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"][GHOST_TARGET_UNREADABLE] == 1


# ---------------------------------------------------------------------------
# 6–7. The active direction is unchanged, and an empty source is not a pass
# ---------------------------------------------------------------------------


async def test_an_active_source_with_a_deleted_target_still_fails(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    del transport.bookings[GHOST_UUID]

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    assert verdict["unaccounted_reason_codes"]["target_not_found_in_easyweek"] == 1


async def test_an_active_source_with_a_cancelled_target_still_fails(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    transport.bookings[GHOST_UUID]["is_canceled"] = True

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False


async def test_zero_active_source_rows_cannot_pass_while_a_target_stands(session_local, source, monkeypatch):
    """The vacuous PASS, closed from the other side.

    Every source vanishes — from the list read AND from the per-record read, the
    way a real disappearance looks — while all three EasyWeek bookings keep
    standing. An empty source is not a finished cutover.
    """
    from altegio_bot.easyweek_migration import runner as runner_module

    transport = RecordingTransport()
    await migrated_wave(session_local, transport)

    async def _empty(*, company_id, window, timeout_sec=30.0, client=None):
        return []

    monkeypatch.setattr(runner_module, "fetch_company_records", _empty)
    for company_id in (KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID):
        for row in list(source.get(company_id, [])):
            source["live_changes"][(company_id, row["id"])] = None

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]

    assert verdict["source_active_bookings"] == 0
    assert verdict["passed"] is False
    # All three migrated bookings are now unaccounted ghosts.
    assert verdict["ghost_targets_active"] == 3


async def test_a_source_404_and_a_target_404_are_different_things(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    # Source gone, target still standing → ghost, not "absent".
    remove_source(source, KA_RECORD_B)
    ghost = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert ghost["unaccounted_reason_codes"][GHOST_TARGET_STILL_ACTIVE] == 1
    assert "target_not_found_in_easyweek" not in ghost["unaccounted_reason_codes"]

    # Source gone AND target gone → consistent.
    del transport.bookings[GHOST_UUID]
    consistent = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert consistent["passed"] is True


# ---------------------------------------------------------------------------
# 8, 11. Still read-only, and still behind the wave scope
# ---------------------------------------------------------------------------


async def test_the_ghost_sweep_never_mutates_easyweek(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)
    posts_before = transport.mutations

    await final(session_local, transport)

    assert transport.mutations == posts_before
    assert transport.cancelled == []


async def test_a_scope_mismatch_still_refuses_before_any_read(session_local, source, monkeypatch):
    from altegio_bot.easyweek_migration import runner as runner_module

    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)
    requests_before = len(transport.requests)

    async def _must_not_run(*args, **kwargs):
        raise AssertionError("a drifted scope must not read the source")

    monkeypatch.setattr(runner_module, "fetch_company_records", _must_not_run)
    async with make_write_client(transport) as client:
        report = await run_reconcile(
            session_local, make_inputs(MODE_RECONCILE, final=True, horizon_days=90), write_client=client
        )

    assert report.as_safe_dict()["completeness"]["passed"] is False
    assert "migration_scope_horizon_mismatch" in report.errors
    assert len(transport.requests) == requests_before


async def test_the_ghost_report_carries_no_pii(session_local, source):
    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    deactivate_source(source, KA_RECORD_B, confirmed=0)

    report = await final(session_local, transport)
    blob = json.dumps(report.as_safe_dict()["completeness"])
    assert CUSTOMER_PHONE not in blob
    assert "77777777-7777-4777-8777-777777777777" not in blob
    # The ids an operator needs ARE there.
    assert str(KA_RECORD_B) in blob


async def test_both_branches_are_swept(session_local, source):
    """The sweep is over the ledger, so it covers Rastatt as well."""
    from altegio_bot.tests.easyweek_migration_harness import RA_RECORD_A

    transport = RecordingTransport()
    await migrated_wave(session_local, transport)
    source[RASTATT_COMPANY_ID] = []

    verdict = (await final(session_local, transport)).as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    manual = {row["source_record_id"] for row in verdict["manual_action_required"]}
    assert RA_RECORD_A in manual
