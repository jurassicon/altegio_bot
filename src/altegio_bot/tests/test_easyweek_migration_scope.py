"""PR-11.1 revision 18: a reconciliation proves the wave that actually ran.

Two silent ways existed to prove a *different* wave than the one that was
migrated, and both of them could end in ``completeness.passed = true``:

* **Drift the window.** ``--cutover-at`` was only mandatory for write modes, so
  ``reconcile --final`` fell back to "now". A booking at 10:00 reconciled at
  12:00 became ``starts_before_cutover``, its EasyWeek target was never fetched,
  and a target that had been deleted could not fail a check that never looked at
  it.
* **Drift the wave.** Move an already-migrated master into
  ``deferred_altegio_staff_ids`` and her bookings leave the selected wave —
  taking their targets out of the proof with them.

The fix binds every continuing command to the durable identity of the wave the
canary licensed. This file holds it to that, and to the rule that a refusal
happens *before* any EasyWeek mutation.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult
from altegio_bot.easyweek_migration.canary import (
    SCOPE_AMBIGUOUS,
    SCOPE_BRANCH_MISMATCH,
    SCOPE_CUTOVER_MISMATCH,
    SCOPE_HORIZON_MISMATCH,
    SCOPE_MANIFEST_MISMATCH,
    SCOPE_MISSING,
    SCOPE_SCHEMA_MISMATCH,
    SCOPE_STAFF_SCOPE_MISMATCH,
    build_binding,
    find_proven_scope,
    record_proof,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    run_apply,
    run_reconcile,
    run_resolve_created,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    CUTOVER,
    KA_RECORD_B,
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
from altegio_bot.tests.test_easyweek_migration_planning import KA_DEFERRED_STAFF_ID, KA_STAFF_ID


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def edited_manifest(**edits) -> str:
    """The harness manifest with one Karlsruhe field changed."""
    payload = json.loads(manifest_json())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)].update(edits)
    return json.dumps(payload)


def moved_to_deferred_manifest() -> str:
    """The migrated master pushed into the next wave — the hiding move."""
    return edited_manifest(
        selected_altegio_staff_ids=[KA_DEFERRED_STAFF_ID],
        deferred_altegio_staff_ids=[KA_STAFF_ID],
        staff={
            str(KA_STAFF_ID): "33333333-3333-4333-8333-333333333333",
            str(KA_DEFERRED_STAFF_ID): "aaaaaaaa-0000-4000-8000-00000000dddd",
        },
    )


async def licensed_wave(session_local, transport) -> None:
    """Canary + bulk, so a real durable wave scope exists to be matched against."""
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )


async def final_with(session_local, transport, **overrides):
    async with make_write_client(transport) as client:
        return await run_reconcile(
            session_local, make_inputs(MODE_RECONCILE, final=True, **overrides), write_client=client
        )


# ---------------------------------------------------------------------------
# The CLI half: an explicit cutover is not optional any more
# ---------------------------------------------------------------------------


def _cli(tmp_path, mode: str, *extra: str) -> int:
    from altegio_bot.scripts.easyweek_migration import main

    manifest = tmp_path / "m.json"
    manifest.write_text(manifest_json(), encoding="utf-8")
    directory = tmp_path / "c.csv"
    directory.write_text("uuid,phone\n77777777-7777-4777-8777-777777777777,+4915112345678\n", encoding="utf-8")
    return main(
        [
            mode,
            "--manifest",
            str(manifest),
            "--customer-directory",
            str(directory),
            "--no-write-report",
            *extra,
        ]
    )


def test_final_reconciliation_without_a_cutover_refuses(tmp_path):
    """It refuses before reading the source or any target."""
    assert _cli(tmp_path, "reconcile", "--final") == 1


def test_ordinary_reconciliation_without_a_cutover_refuses(tmp_path):
    assert _cli(tmp_path, "reconcile") == 1


def test_resolve_created_without_a_cutover_refuses(tmp_path):
    assert (
        _cli(
            tmp_path,
            "resolve-created",
            "--resolve-company-id",
            str(KARLSRUHE_COMPANY_ID),
            "--resolve-record-id",
            str(KA_RECORD_B),
            "--target-uuid",
            CREATED_UUIDS[KA_RECORD_B],
        )
        == 1
    )


# ---------------------------------------------------------------------------
# The durable scope itself
# ---------------------------------------------------------------------------


def binding_for(manifest_text: str, *, cutover: str = CUTOVER, horizon: int = 180, slugs=None):
    manifest = parse_manifest(manifest_text)
    assert manifest.valid
    return build_binding(
        manifest_digest=manifest.digest,
        staff_scope_digest=manifest.staff_scope_digest,
        cutover_at=parse_cutover(cutover).at,
        horizon_days=horizon,
        branch_result=BranchIdentityResult(
            proven=True, proven_branches=slugs or {758285: "karlsruhe", 1271200: "rastatt"}
        ),
    )


async def store_wave(session_local, binding) -> None:
    async with session_local() as session:
        async with session.begin():
            await record_proof(
                session,
                run_id="wave-run",
                binding=binding,
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_B,
                source_fingerprint="fp",
                verified=True,
                target_booking_uuid=CREATED_UUIDS[KA_RECORD_B],
                target_snapshot=None,
                failure_reason=None,
            )


async def scope_reason(session_local, binding) -> str:
    async with session_local() as session:
        return (await find_proven_scope(session, binding=binding)).reason


async def test_no_licensed_wave_at_all_is_scope_missing(session_local):
    assert await scope_reason(session_local, binding_for(manifest_json())) == SCOPE_MISSING


async def test_the_matching_binding_is_proven(session_local):
    binding = binding_for(manifest_json())
    await store_wave(session_local, binding)
    async with session_local() as session:
        verdict = await find_proven_scope(session, binding=binding)
    assert verdict.proven
    assert verdict.wave_identity == binding.wave_identity


async def test_reformatting_the_manifest_does_not_move_the_scope(session_local):
    """Whitespace and key order are not the wave; identifiers are."""
    binding = binding_for(manifest_json())
    await store_wave(session_local, binding)
    reformatted = json.dumps(json.loads(manifest_json()), indent=4, sort_keys=True)
    async with session_local() as session:
        verdict = await find_proven_scope(session, binding=binding_for(reformatted))
    assert verdict.proven


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda: binding_for(edited_manifest(easyweek_location_id=999999)), SCOPE_MANIFEST_MISMATCH),
        (lambda: binding_for(moved_to_deferred_manifest()), SCOPE_STAFF_SCOPE_MISMATCH),
        (lambda: binding_for(manifest_json(), cutover="2026-09-02T00:00:00Z"), SCOPE_CUTOVER_MISMATCH),
        (lambda: binding_for(manifest_json(), horizon=90), SCOPE_HORIZON_MISMATCH),
        (
            lambda: binding_for(manifest_json(), slugs={758285: "rastatt", 1271200: "karlsruhe"}),
            SCOPE_BRANCH_MISMATCH,
        ),
    ],
)
async def test_each_scope_field_has_its_own_refusal(session_local, mutate, expected):
    await store_wave(session_local, binding_for(manifest_json()))
    assert await scope_reason(session_local, mutate()) == expected


async def test_a_changed_request_schema_is_its_own_refusal(session_local):
    import dataclasses

    stored = binding_for(manifest_json())
    await store_wave(session_local, stored)
    assert (
        await scope_reason(session_local, dataclasses.replace(stored, request_schema_version="v2"))
        == SCOPE_SCHEMA_MISMATCH
    )


async def test_two_waves_have_different_identities_and_do_not_merge(session_local):
    """The first wave and the later nail-services wave are separate proofs."""
    first = binding_for(manifest_json())
    second = binding_for(moved_to_deferred_manifest())
    assert first.wave_identity != second.wave_identity

    await store_wave(session_local, first)
    await store_wave(session_local, second)

    async with session_local() as session:
        assert (await find_proven_scope(session, binding=first)).proven
        assert (await find_proven_scope(session, binding=second)).proven
        # A third, unrelated scope cannot be resolved to either of them.
        third = binding_for(manifest_json(), horizon=45)
        verdict = await find_proven_scope(session, binding=third)
    assert not verdict.proven
    assert verdict.reason == SCOPE_AMBIGUOUS


async def test_a_proof_written_before_scope_existed_proves_nothing(session_local):
    """Fail closed: a row with no selector digest cannot name a wave."""
    from sqlalchemy import update

    binding = binding_for(manifest_json())
    await store_wave(session_local, binding)
    async with session_local() as session:
        await session.execute(update(EasyWeekMigrationCanaryProof).values(staff_scope_digest=None, horizon_days=None))
        await session.commit()
    assert await scope_reason(session_local, binding) == SCOPE_MISSING


# ---------------------------------------------------------------------------
# End to end: the wave that ran is the wave that gets proven
# ---------------------------------------------------------------------------


async def test_the_original_wave_reconciles_clean(session_local, source):
    transport = RecordingTransport()
    await licensed_wave(session_local, transport)

    report = await final_with(session_local, transport)
    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is True
    assert verdict["scope_proven"] is True
    assert verdict["wave_identity"]


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"cutover": parse_cutover("2026-09-02T00:00:00Z")}, SCOPE_CUTOVER_MISMATCH),
        ({"horizon_days": 90}, SCOPE_HORIZON_MISMATCH),
        ({"manifest": None}, SCOPE_STAFF_SCOPE_MISMATCH),
    ],
)
async def test_a_drifted_scope_fails_the_final_reconciliation(session_local, source, overrides, expected):
    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    if overrides.get("manifest", "sentinel") is None:
        overrides["manifest"] = parse_manifest(moved_to_deferred_manifest())

    report = await final_with(session_local, transport, **overrides)
    safe = report.as_safe_dict()
    assert safe["completeness"]["passed"] is False
    assert safe["scope"]["scope_reason"] == expected
    assert expected in safe["errors"]


async def test_moving_a_migrated_master_to_deferred_cannot_hide_a_broken_target(session_local, source):
    """The hiding move, end to end: it must fail, not quietly stop checking."""
    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    # The target is destroyed, and the manifest is edited to drop the master.
    transport.bookings.clear()

    report = await final_with(session_local, transport, manifest=parse_manifest(moved_to_deferred_manifest()))
    safe = report.as_safe_dict()
    assert safe["completeness"]["passed"] is False
    assert safe["scope"]["scope_reason"] == SCOPE_STAFF_SCOPE_MISMATCH


async def test_a_drifted_scope_refuses_before_reading_source_or_targets(session_local, source, monkeypatch):
    from altegio_bot.easyweek_migration import runner as runner_module

    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    requests_before = len(transport.requests)

    async def _must_not_run(*args, **kwargs):
        raise AssertionError("the live source must not be read on a drifted scope")

    monkeypatch.setattr(runner_module, "fetch_company_records", _must_not_run)
    report = await final_with(session_local, transport, horizon_days=90)

    assert report.as_safe_dict()["completeness"]["passed"] is False
    # Not one extra EasyWeek request, and certainly no mutation.
    assert len(transport.requests) == requests_before
    assert transport.cancelled == []


async def test_a_drifted_scope_performs_no_mutation(session_local, source):
    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    posts_before = transport.mutations

    await final_with(session_local, transport, horizon_days=90)
    async with make_write_client(transport) as client:
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE, horizon_days=90), write_client=client)
    assert transport.mutations == posts_before
    assert transport.cancelled == []


async def test_a_drifted_scope_does_not_resolve_an_uncertain_row(session_local, source):
    from sqlalchemy import text

    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    async with session_local() as session:
        await session.execute(
            text("UPDATE easyweek_migration_ledger SET status = 'uncertain' WHERE source_record_id = :rid"),
            {"rid": KA_RECORD_B},
        )
        await session.commit()

    async with make_write_client(transport) as client:
        report = await run_resolve_created(
            session_local,
            make_inputs(
                MODE_RESOLVE_CREATED,
                horizon_days=90,
                resolve_company_id=KARLSRUHE_COMPANY_ID,
                resolve_record_id=KA_RECORD_B,
                resolve_target_booking_uuid=CREATED_UUIDS[KA_RECORD_B],
            ),
            write_client=client,
        )

    assert SCOPE_HORIZON_MISMATCH in report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


async def test_the_report_names_the_wave_without_pii(session_local, source):
    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    report = await final_with(session_local, transport)

    scope = report.as_safe_dict()["scope"]
    blob = json.dumps(scope)
    assert scope["wave_identity"]
    assert "+4915112345678" not in blob
    assert "77777777-7777-4777-8777-777777777777" not in blob


async def test_the_stored_proof_carries_the_scope_columns(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    async with session_local() as session:
        proof = (await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().one()
    assert proof.staff_scope_digest
    assert proof.horizon_days == 180


async def test_a_later_cutover_cannot_quietly_drop_earlier_bookings(session_local, source):
    """The original drift: 'now' would push a morning booking out of the window."""
    transport = RecordingTransport()
    await licensed_wave(session_local, transport)
    later = parse_cutover(CUTOVER)
    drifted = parse_cutover((later.at + timedelta(days=30)).isoformat().replace("+00:00", "Z"))

    report = await final_with(session_local, transport, cutover=drifted)
    assert report.as_safe_dict()["scope"]["scope_reason"] == SCOPE_CUTOVER_MISMATCH
    assert report.as_safe_dict()["completeness"]["passed"] is False
