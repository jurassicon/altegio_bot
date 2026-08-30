"""PR-11.1 revision 19, P1 #1: the canary whose own outcome is unknown.

The deadlock. A canary POST that times out (or breaks, or 5xx's, or answers 2xx
with no readable uuid) leaves the ledger row ``uncertain`` and the proof
``verified=false``. The booking may well exist — the operator can find it in the
EasyWeek UI by its migration marker — but ``resolve-created`` goes through the
scope gate, and that gate only accepts a *verified* proof. So the one row that
would produce the wave's first verified proof is the one row that cannot be
resolved, and re-sending the POST is forbidden because it may give a real
customer two appointments.

The key is deliberately narrow: an unverified attempt may recover **its own**
uncertain row, on a full proof, and nothing else. This file is mostly about the
"nothing else".
"""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.canary import (
    CANARY_POST_FAILED,
    CANARY_READBACK_FAILED,
    CANARY_REPROOF_FAILED,
    RECOVERY_ALREADY_VERIFIED,
    RECOVERY_NO_ATTEMPT,
    RECOVERY_NOT_UNCERTAIN_OUTCOME,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_RECONCILE,
    MODE_RESOLVE_CREATED,
    RECOVERY_ATTEMPTS_UNEXPECTED,
    RECOVERY_RUN_MISMATCH,
    run_apply,
    run_canary,
    run_reconcile,
    run_resolve_created,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    KA_RECORD_A,
    KA_RECORD_B,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    make_inputs,
    make_write_client,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_scope import edited_manifest, moved_to_deferred_manifest

TIMEOUT = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


async def uncertain_canary(session_local, transport, *, record_id: int = KA_RECORD_A) -> str:
    """Run the canary and have its POST time out. Returns the canary run id."""
    transport.fail_with = {record_id: TIMEOUT}
    plan = await run_dry_run(session_local)
    inputs = make_inputs(
        MODE_CANARY,
        verified_dry_run_id=plan.plan_digest,
        canary_company_id=KARLSRUHE_COMPANY_ID,
        canary_record_id=record_id,
    )
    async with make_write_client(transport) as client:
        await run_canary(session_local, inputs, write_client=client)
    transport.fail_with = {}
    return inputs.run_id


async def proof_rows(session_local) -> list[EasyWeekMigrationCanaryProof]:
    async with session_local() as session:
        return list((await session.execute(select(EasyWeekMigrationCanaryProof))).scalars().all())


def resolve_inputs(*, record_id: int = KA_RECORD_A, target: str | None = None, **overrides):
    kwargs = {
        "resolve_company_id": KARLSRUHE_COMPANY_ID,
        "resolve_record_id": record_id,
        "resolve_target_booking_uuid": target or CREATED_UUIDS[record_id],
    }
    kwargs.update(overrides)
    return make_inputs(MODE_RESOLVE_CREATED, **kwargs)


# ---------------------------------------------------------------------------
# 1. What the timeout leaves behind
# ---------------------------------------------------------------------------


async def test_a_canary_timeout_leaves_one_post_and_an_unverified_proof(session_local, source):
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)

    assert transport.post_count_for(KA_RECORD_A) == 1
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A].status == "uncertain"

    proofs = await proof_rows(session_local)
    assert len(proofs) == 1
    assert proofs[0].verified is False
    assert proofs[0].failure_reason == "canary_post_uncertain"
    assert proofs[0].target_booking_uuid is None


async def test_before_recovery_bulk_and_final_stay_shut(session_local, source):
    from altegio_bot.easyweek_migration.gates import ApplyGateError

    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    posts_before = transport.mutations

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError) as exc:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )
    assert "canary_proof_missing_or_stale" in exc.value.failures

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
    assert report.as_safe_dict()["completeness"]["passed"] is False
    assert "migration_scope_missing" in report.errors
    assert transport.mutations == posts_before


# ---------------------------------------------------------------------------
# 2–3. The recovery itself, and what it unlocks
# ---------------------------------------------------------------------------


async def test_the_operator_can_recover_the_uncertain_canary(session_local, source):
    transport = RecordingTransport()
    origin_run = await uncertain_canary(session_local, transport)
    # The booking WAS created; the operator finds it by its marker.
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    posts_before = transport.mutations
    gets_before = sum(1 for r in transport.requests if r.method == "GET")

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert report.errors == []
    # No second POST, and the target really was fetched.
    assert transport.mutations == posts_before
    assert sum(1 for r in transport.requests if r.method == "GET") > gets_before

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    row = rows[KA_RECORD_A]
    assert row.status == "created"
    assert row.target_booking_uuid == CREATED_UUIDS[KA_RECORD_A]
    assert row.target_snapshot_fingerprint
    assert row.run_id == origin_run  # origin preserved

    proof = (await proof_rows(session_local))[0]
    assert proof.verified is True
    assert proof.failure_reason is None
    assert proof.target_booking_uuid == CREATED_UUIDS[KA_RECORD_A]
    assert proof.target_snapshot_fingerprint == row.target_snapshot_fingerprint
    assert proof.verified_at is not None


async def test_the_recovered_proof_then_licenses_the_bulk(session_local, source):
    """The point of the whole path: the wave is unblocked, not bypassed."""
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    async with make_write_client(transport) as client:
        await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    assert report.as_safe_dict()["totals"]["created"] == 2
    assert report.as_safe_dict()["totals"]["already_migrated"] == 1


async def test_a_recovered_wave_then_reconciles_clean(session_local, source):
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    async with make_write_client(transport) as client:
        await run_resolve_created(session_local, resolve_inputs(), write_client=client)
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
    assert report.as_safe_dict()["completeness"]["passed"] is True


# ---------------------------------------------------------------------------
# 5–8. Everything the narrow key must NOT open
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"manifest": "moved"}, id="staff selector"),
        pytest.param({"manifest": "edited"}, id="manifest"),
        pytest.param({"cutover": "2026-09-02T00:00:00Z"}, id="cutover"),
        pytest.param({"horizon_days": 90}, id="horizon"),
    ],
)
async def test_a_drifted_scope_is_refused_before_any_external_read(session_local, source, monkeypatch, overrides):
    from altegio_bot.easyweek_migration import reproof as reproof_module

    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    requests_before = len(transport.requests)

    async def _must_not_read(*args, **kwargs):
        raise AssertionError("a refused recovery must not read Altegio")

    monkeypatch.setattr(reproof_module, "fetch_single_record", _must_not_read)

    kwargs = dict(overrides)
    if kwargs.get("manifest") == "moved":
        kwargs["manifest"] = parse_manifest(moved_to_deferred_manifest())
    elif kwargs.get("manifest") == "edited":
        kwargs["manifest"] = parse_manifest(edited_manifest(easyweek_location_id=999999))
    if "cutover" in kwargs:
        kwargs["cutover"] = parse_cutover(kwargs["cutover"])

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(**kwargs), write_client=client)

    assert report.errors
    # Nothing outside PostgreSQL was touched.
    assert len(transport.requests) == requests_before
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A] == "uncertain"
    assert (await proof_rows(session_local))[0].verified is False


async def test_an_unverified_proof_cannot_resolve_a_different_row(session_local, source):
    """The attempt recovers its own row and nothing else."""
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_B], record_id=KA_RECORD_B)
    # Give the OTHER source identity an uncertain ledger row of its own.
    async with session_local() as session:
        await session.execute(
            text(
                "INSERT INTO easyweek_migration_ledger "
                "(source_provider, source_company_id, source_record_id, source_fingerprint, "
                " target_provider, run_id, status, attempts) "
                "VALUES ('altegio', :c, :r, 'fp', 'easyweek', 'other-run', 'uncertain', 1)"
            ),
            {"c": KARLSRUHE_COMPANY_ID, "r": KA_RECORD_B},
        )
        await session.commit()

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(record_id=KA_RECORD_B), write_client=client)

    assert RECOVERY_NO_ATTEMPT in report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_B] == "uncertain"


@pytest.mark.parametrize(
    "failure_reason",
    [
        pytest.param(CANARY_POST_FAILED, id="permanent 4xx"),
        pytest.param(CANARY_REPROOF_FAILED, id="source re-proof failure"),
        pytest.param(f"{CANARY_READBACK_FAILED}:target_field_mismatch:staff_uuid", id="readback mismatch"),
    ],
)
async def test_only_an_unknown_outcome_qualifies(session_local, source, failure_reason):
    """A 4xx proves nothing was created; a mismatch proves the wrong thing was."""
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    async with session_local() as session:
        await session.execute(
            text("UPDATE easyweek_migration_canary_proof SET failure_reason = :r"), {"r": failure_reason}
        )
        await session.commit()

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert RECOVERY_NOT_UNCERTAIN_OUTCOME in report.errors
    rows = {row.source_record_id: row.status for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A] == "uncertain"
    assert (await proof_rows(session_local))[0].verified is False


async def test_a_mismatched_canary_run_is_refused(session_local, source):
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    async with session_local() as session:
        await session.execute(
            text("UPDATE easyweek_migration_ledger SET run_id = 'some-other-run' WHERE source_record_id = :r"),
            {"r": KA_RECORD_A},
        )
        await session.commit()

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert RECOVERY_RUN_MISMATCH in report.errors
    assert (await proof_rows(session_local))[0].verified is False


async def test_more_than_one_attempt_is_refused(session_local, source):
    """Two attempts make "the booking the operator found" ambiguous."""
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    async with session_local() as session:
        await session.execute(
            text("UPDATE easyweek_migration_ledger SET attempts = 2 WHERE source_record_id = :r"),
            {"r": KA_RECORD_A},
        )
        await session.commit()

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert RECOVERY_ATTEMPTS_UNEXPECTED in report.errors
    assert (await proof_rows(session_local))[0].verified is False


async def test_an_already_verified_proof_does_not_take_the_recovery_path(session_local, source):
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    async with session_local() as session:
        await session.execute(
            text(
                "UPDATE easyweek_migration_canary_proof SET verified = true, failure_reason = NULL, "
                "target_booking_uuid = :t"
            ),
            {"t": CREATED_UUIDS[KA_RECORD_A]},
        )
        await session.commit()
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)
    # The ordinary verified-wave path applies; the recovery admission is not used.
    assert RECOVERY_ALREADY_VERIFIED not in report.errors
    assert report.errors == []


# ---------------------------------------------------------------------------
# 9–11. Proof still has to pass, and nothing is ever written twice
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field,value",
    [
        ("staff_uuid", "00000000-0000-4000-8000-0000000000a1"),
        ("service_uuid", "00000000-0000-4000-8000-0000000000a2"),
        ("customer_uuid", "00000000-0000-4000-8000-0000000000a3"),
        ("start_time", "2026-09-14T07:00:00Z"),
        ("duration", 120),
        ("comment", "rewritten by hand"),
    ],
)
async def test_a_mismatched_target_leaves_both_unproven(session_local, source, field, value):
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    transport.bookings[CREATED_UUIDS[KA_RECORD_A]][field] = value

    async with make_write_client(transport) as client:
        report = await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    assert report.errors
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A].status == "uncertain"
    assert rows[KA_RECORD_A].target_snapshot_fingerprint is None
    assert (await proof_rows(session_local))[0].verified is False


async def test_a_failing_transaction_leaves_neither_side_confirmed(session_local, source, monkeypatch):
    """Ledger `created` beside an unverified proof must be impossible."""
    from altegio_bot.easyweek_migration import runner as runner_module

    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)

    async def _boom(*args, **kwargs):
        raise RuntimeError("database went away mid-commit")

    monkeypatch.setattr(runner_module, "promote_proof_to_verified", _boom)
    async with make_write_client(transport) as client:
        with pytest.raises(RuntimeError):
            await run_resolve_created(session_local, resolve_inputs(), write_client=client)

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A].status == "uncertain"
    assert rows[KA_RECORD_A].target_snapshot_fingerprint is None
    assert (await proof_rows(session_local))[0].verified is False


async def test_no_recovery_scenario_ever_mutates_easyweek(session_local, source):
    transport = RecordingTransport()
    await uncertain_canary(session_local, transport)
    transport.plant_booking(CREATED_UUIDS[KA_RECORD_A], record_id=KA_RECORD_A)
    posts_before = transport.mutations

    async with make_write_client(transport) as client:
        await run_resolve_created(session_local, resolve_inputs(), write_client=client)
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    assert transport.mutations == posts_before
    assert transport.cancelled == []
