"""A cancel whose outcome was never seen, and the row it leaves behind (PR-11.2).

`PUT /bookings/{uuid}/status/cancel` can land while its answer does not. From
outside, the result is indistinguishable from a booking a person cancelled by
hand — and the two must never be treated the same: the first is our own rollback
finishing late, the second is somebody else's change that this tool may not
claim.

The difference is made durable, not guessed: the attempt marker is written and
committed BEFORE the request goes out. Everything in this file is about the
three states that follow it, and about the one rule that binds them — after a
PUT may have been sent, no second PUT is ever sent automatically.

The suite also holds the legacy fingerprint compatibility to the end-to-end
paths: an old ledger row has to reconcile and roll back, not merely classify.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
)
from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration.classify import classify_record, legacy_source_fingerprint
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_DRY_RUN,
    MODE_RECONCILE,
    MODE_ROLLBACK_DRY_RUN,
    ROLLBACK_ATTEMPT_UNRESOLVED,
    ROLLBACK_CANCELED_ELSEWHERE,
    ROLLBACK_CLAIM_LOST,
    ROLLBACK_NOT_SENT,
    ROLLBACK_RECOVERED,
    ROLLBACK_RECOVERY_AVAILABLE,
    ROLLBACK_TARGET_MODIFIED,
    ROLLBACK_UNCERTAIN,
    run_apply,
    run_inventory_or_dry_run,
    run_reconcile,
    run_rollback,
)
from altegio_bot.easyweek_migration.write_client import (
    CancelOutcome,
    EasyWeekCancelNotSent,
    EasyWeekUncertainMutation,
)
from altegio_bot.models.models import EasyWeekMigrationLedger
from altegio_bot.tests.easyweek_migration_harness import (
    CREATED_UUIDS,
    KA_RECORD_A,
    KA_RECORD_B,
    RA_RECORD_A,
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


async def applied_run(session_local, transport) -> str:
    """One licensed wave, applied. Returns the run id that created its rows."""
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)
    return apply_inputs.run_id


async def rollback(session_local, transport, *, run_id: str, confirmed: bool):
    async with make_write_client(transport) as client:
        return await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=run_id, rollback_confirmed=confirmed),
            write_client=client,
        )


async def row_for(session_local, record_id: int) -> EasyWeekMigrationLedger:
    rows = await ledger_rows(session_local)
    return next(row for row in rows if row.source_record_id == record_id)


# ---------------------------------------------------------------------------
# The cancel client, one request at a time
# ---------------------------------------------------------------------------


async def test_a_preflight_read_failure_sends_no_put(session_local, source):
    """An error BEFORE the mutation is not uncertain: nothing was sent."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.get_status_override[target] = 503

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    # The other row of the same wave is cancelled normally; this one never got
    # as far as a request.
    assert target not in transport.cancel_puts
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"
    assert row.rollback_attempted_at is None, "no attempt marker for a cancel that was never sent"
    assert report.as_safe_dict()["reason_codes"].get("rollback_target_unreadable") == 1


async def test_a_proven_cancel_sends_exactly_one_put(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert sorted(transport.cancel_puts) == sorted([CREATED_UUIDS[KA_RECORD_B], CREATED_UUIDS[RA_RECORD_A]])
    assert len(transport.cancel_puts) == 2
    assert report.mutations_attempted == len(transport.cancel_puts) == 2
    rolled = [row for row in await ledger_rows(session_local) if row.status == "rolled_back"]
    assert len(rolled) == 2


async def test_a_dry_run_attempts_no_mutation_at_all(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=False)

    assert transport.cancel_puts == []
    assert report.mutations_attempted == 0


async def test_the_counter_equals_the_cancels_that_actually_left(session_local, source, monkeypatch):
    """The invariant, over a wave that exercises three different endings at once.

    One row is refused by its own preflight read, one is cancelled and proven,
    and the wave-level number must equal what the transport really saw. Counting
    on entry to the mutation path instead — the defect — makes this two ahead of
    reality precisely when a run changed less than it looks like it did.
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    stopped = CREATED_UUIDS[KA_RECORD_B]

    async def break_the_preflight(kwargs):
        if kwargs["source_record_id"] == KA_RECORD_B:
            transport.get_status_override[stopped] = 503

    _hook_claim(monkeypatch, after=break_the_preflight)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert stopped not in transport.cancel_puts
    assert CREATED_UUIDS[RA_RECORD_A] in transport.cancel_puts
    assert report.mutations_attempted == len(transport.cancel_puts) == 1


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(httpx.TimeoutException("timeout"), id="timeout"),
        pytest.param(httpx.ConnectError("disconnected"), id="disconnect"),
        pytest.param(500, id="500"),
        pytest.param(503, id="503"),
    ],
)
async def test_a_failing_put_is_uncertain_and_is_never_repeated(session_local, source, failure):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = failure

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == 1
    assert report.as_safe_dict()["reason_codes"][ROLLBACK_UNCERTAIN] == 1
    # The request left, so it is an attempt whatever the answer was.
    assert report.mutations_attempted == len(transport.cancel_puts)
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created", "an unproven cancel must not claim a rollback"
    assert row.rollback_attempted_at is not None, "the attempt is durable even when the answer is not"


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(httpx.TimeoutException("timeout"), id="confirm-timeout"),
        pytest.param(httpx.ConnectError("disconnected"), id="confirm-disconnect"),
        pytest.param(404, id="confirm-404"),
        pytest.param(500, id="confirm-500"),
        pytest.param(503, id="confirm-503"),
        pytest.param(429, id="confirm-rate-limited"),
        pytest.param("malformed", id="confirm-malformed-json"),
        pytest.param("missing", id="confirm-missing-flag"),
        pytest.param("non_bool", id="confirm-non-boolean-flag"),
        pytest.param("false", id="confirm-literally-false"),
    ],
)
async def test_a_2xx_put_with_an_unprovable_readback_is_uncertain(session_local, source, failure):
    """The regression itself: the PUT succeeded, the confirmation did not.

    Every one of these used to escape as its own exception type — a rate limit
    as retryable, a malformed body as a protocol error — and the runner filed
    them as `rollback_refused`, which states that nothing was cancelled. The
    booking may well have been.
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.confirm_get_fail_with[target] = failure

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == 1
    codes = report.as_safe_dict()["reason_codes"]
    assert codes.get(ROLLBACK_UNCERTAIN) == 1
    assert "rollback_refused" not in codes
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"


@pytest.mark.parametrize(
    "status, expected",
    [
        pytest.param(401, EasyWeekAuthError, id="401"),
        pytest.param(403, EasyWeekAuthError, id="403"),
        pytest.param(404, EasyWeekNotFoundError, id="404"),
        pytest.param(422, EasyWeekPermanentError, id="422"),
    ],
)
async def test_a_deterministic_refusal_is_not_a_rollback(session_local, source, status, expected):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = status

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == 1
    assert report.as_safe_dict()["reason_codes"]["rollback_refused"] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"
    assert expected is not EasyWeekUncertainMutation


# ---------------------------------------------------------------------------
# The recovery, end to end
# ---------------------------------------------------------------------------


async def test_a_cancel_that_landed_unseen_is_completed_without_a_second_put(session_local, source):
    """The whole point. One PUT, ever, across both runs."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    # The cancel lands; the answer never arrives.
    transport.cancel_fail_with[target] = httpx.TimeoutException("timeout")
    transport.cancel_side_effect_on_failure = True

    first = await rollback(session_local, transport, run_id=run_id, confirmed=True)
    assert first.as_safe_dict()["reason_codes"][ROLLBACK_UNCERTAIN] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created", "an unproven cancel is not a rollback"
    assert row.rollback_attempted_at is not None
    assert row.rollback_attempt_run_id

    puts_after_first = transport.cancel_puts.count(target)

    second = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == puts_after_first == 1, "no second PUT, ever"
    assert second.as_safe_dict()["reason_codes"][ROLLBACK_RECOVERED] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "rolled_back"
    assert row.target_booking_uuid == target


async def test_the_recovery_is_visible_to_a_dry_run_without_touching_anything(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = 500
    transport.cancel_side_effect_on_failure = True
    await rollback(session_local, transport, run_id=run_id, confirmed=True)

    before = len(transport.cancel_puts)
    report = await rollback(session_local, transport, run_id=run_id, confirmed=False)

    assert report.as_safe_dict()["reason_codes"][ROLLBACK_RECOVERY_AVAILABLE] == 1
    assert len(transport.cancel_puts) == before
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created", "a dry-run states the finding and writes nothing"


async def test_a_cancelled_booking_without_our_attempt_is_not_our_rollback(session_local, source):
    """Somebody cancelled it by hand. That is a modified target, not a rollback."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    # Cancelled in EasyWeek without any request from us — no PUT, no marker.
    transport.canceled_uuids.add(target)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == 0
    assert report.as_safe_dict()["reason_codes"][ROLLBACK_TARGET_MODIFIED] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"
    assert row.rollback_attempted_at is None


async def test_an_attempt_whose_booking_is_still_live_never_retries(session_local, source):
    """The PUT may never have left. A blind repeat is exactly what is banned."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    # The cancel fails and does NOT land: the booking stays active.
    transport.cancel_fail_with[target] = httpx.ConnectError("disconnected")

    await rollback(session_local, transport, run_id=run_id, confirmed=True)
    puts_after_first = transport.cancel_puts.count(target)

    second = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == puts_after_first == 1
    codes = second.as_safe_dict()["reason_codes"]
    assert codes[ROLLBACK_ATTEMPT_UNRESOLVED] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"


async def test_a_finished_rollback_repeats_as_zero_mutations(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    await rollback(session_local, transport, run_id=run_id, confirmed=True)
    puts = list(transport.cancel_puts)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts == puts
    assert report.mutations_attempted == 0
    rolled = [row for row in await ledger_rows(session_local) if row.status == "rolled_back"]
    assert len(rolled) == 2


async def test_the_attempt_marker_is_written_before_the_request_leaves(session_local, source):
    """Crash safety: the marker must survive a process that never got an answer.

    Modelled by a transport that raises — the client's own connection error path
    — and then asserting the marker is already durable. Written after the PUT it
    would be missing in precisely this case.
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = httpx.ConnectError("disconnected")

    await rollback(session_local, transport, run_id=run_id, confirmed=True)

    row = await row_for(session_local, KA_RECORD_B)
    assert row.rollback_attempted_at is not None
    assert row.rollback_attempt_run_id is not None


async def test_no_rollback_reason_code_carries_pii(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.confirm_get_fail_with[target] = "non_bool"

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    blob = report.to_json()
    for leaked in ("+4915112345678", "Testkundin", "not json", "is_canceled is not a boolean"):
        assert leaked not in blob


# ---------------------------------------------------------------------------
# Legacy ledger rows, through the real commands
# ---------------------------------------------------------------------------


async def _make_rows_legacy(session_local, source) -> dict[int, str]:
    """Rewrite every ledger fingerprint to the pre-binding production format.

    Stands in for a database migrated by the old code: same bookings, same
    targets, older hashes.
    """
    inputs = make_inputs(MODE_DRY_RUN)
    payloads = {
        KA_RECORD_A: (KARLSRUHE_COMPANY_ID, source[KARLSRUHE_COMPANY_ID][0]),
        KA_RECORD_B: (KARLSRUHE_COMPANY_ID, source[KARLSRUHE_COMPANY_ID][1]),
        RA_RECORD_A: (RASTATT_COMPANY_ID, source[RASTATT_COMPANY_ID][0]),
    }
    legacy: dict[int, str] = {}
    for record_id, (company_id, payload) in payloads.items():
        decision = classify_record(
            payload,
            company_id=company_id,
            manifest=inputs.manifest,
            directory=inputs.directory,
            cutover=inputs.cutover,
            ledger=None,
        )
        assert decision.starts_at_utc is not None
        assert decision.easyweek_staff_uuid is not None
        assert decision.easyweek_service_uuid is not None
        assert decision.easyweek_customer_uuid is not None
        assert decision.duration_minutes is not None
        legacy[record_id] = legacy_source_fingerprint(
            company_id=company_id,
            record_id=record_id,
            starts_at_utc=decision.starts_at_utc,
            staff_uuid=decision.easyweek_staff_uuid,
            service_uuid=decision.easyweek_service_uuid,
            duration_minutes=decision.duration_minutes,
            customer_uuid=decision.easyweek_customer_uuid,
        )

    async with session_local() as session:
        async with session.begin():
            for record_id, value in legacy.items():
                await session.execute(
                    update(EasyWeekMigrationLedger)
                    .where(EasyWeekMigrationLedger.source_record_id == record_id)
                    .values(source_fingerprint=value)
                )
    return legacy


async def test_a_legacy_wave_still_reads_as_already_migrated(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, transport)
    await _make_rows_legacy(session_local, source)

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    totals = plan.as_safe_dict()["totals"]
    assert totals["already_migrated"] == 3
    assert totals["blocked"] == 0
    assert totals["ready"] == 0


async def test_a_legacy_wave_still_reconciles(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, transport)
    await _make_rows_legacy(session_local, source)

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)

    assert report.completeness is not None
    assert report.completeness["passed"] is True, report.as_safe_dict()["reason_codes"]


async def test_a_legacy_row_can_still_be_rolled_back(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    await _make_rows_legacy(session_local, source)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=False)

    assert report.as_safe_dict()["reason_codes"]["rollback_eligible"] == 2
    assert transport.cancel_puts == []


async def test_a_read_only_run_never_rewrites_a_legacy_fingerprint(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    legacy = await _make_rows_legacy(session_local, source)

    async with session_local() as session:
        await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)
    await rollback(session_local, transport, run_id=run_id, confirmed=False)

    async with session_local() as session:
        stored = {
            row.source_record_id: row.source_fingerprint
            for row in (await session.execute(select(EasyWeekMigrationLedger))).scalars()
        }
    assert stored == legacy, "a read-only command must not convert anything"


async def test_a_legacy_row_whose_source_changed_is_still_blocked(session_local, source):
    transport = RecordingTransport()
    await applied_run(session_local, transport)
    await _make_rows_legacy(session_local, source)

    # The master edits the booking to two units after the migration.
    source[KARLSRUHE_COMPANY_ID][1]["services"][0]["amount"] = 2

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    totals = plan.as_safe_dict()["totals"]
    assert totals["blocked"] == 1
    assert totals["already_migrated"] == 2
    codes = plan.as_safe_dict()["reason_codes"]
    assert codes["source_service_quantity_unsupported"] == 1


# ---------------------------------------------------------------------------
# One owner of the mutation, proven against PostgreSQL
# ---------------------------------------------------------------------------


async def _seed_created_row(session_local, *, run_id: str, record_id: int, target: str) -> None:
    async with session_local() as session:
        async with session.begin():
            await session.execute(
                insert(EasyWeekMigrationLedger).values(
                    source_provider="altegio",
                    source_company_id=KARLSRUHE_COMPANY_ID,
                    source_record_id=record_id,
                    source_fingerprint="f" * 64,
                    target_provider="easyweek",
                    target_booking_uuid=target,
                    target_snapshot_fingerprint="a" * 64,
                    run_id=run_id,
                    status="created",
                    attempts=1,
                )
            )


async def _wait_until_blocked_on_a_lock(session_local) -> None:
    """Return once another session is genuinely waiting on a row lock.

    A barrier, not a delay: it observes the database's own wait state rather
    than guessing how long a statement takes. Without it the two claims can be
    issued in sequence — the first committing before the second even reaches the
    server — and the test would pass against a read-then-write implementation
    that has no atomicity at all.
    """
    for _ in range(20000):
        async with session_local() as probe:
            waiting = (
                await probe.execute(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' AND state = 'active' "
                        "AND query ILIKE '%easyweek_migration_ledger%'"
                    )
                )
            ).scalar_one()
        if waiting:
            return
        await asyncio.sleep(0)
    raise AssertionError("no session ever blocked on the ledger row; the two claims did not overlap")


async def test_two_runs_racing_for_one_row_produce_exactly_one_owner(session_local):
    """Row-level concurrency, against the real database.

    Two rollback runs reach the same ledger row at the same moment, and their
    statements genuinely overlap: the winner holds an uncommitted claim while the
    loser is blocked on the same row. The claim is one conditional UPDATE, so
    when the loser is released it matches nothing — which is the whole
    guarantee, and the reason a read-then-write version cannot pass this test:
    its SELECT would see an uncommitted NULL marker and conclude it may write.
    """
    target = "0e9a1111-2222-4333-8444-555566667777"
    await _seed_created_row(session_local, run_id="origin-run", record_id=900501, target=target)

    first_claimed = asyncio.Event()
    release_first = asyncio.Event()
    results: dict[str, ledger_module.RollbackClaim] = {}

    async def contender(name: str) -> None:
        async with session_local() as session:
            async with session.begin():
                results[name] = await ledger_module.claim_rollback_attempt(
                    session,
                    run_id=name,
                    source_company_id=KARLSRUHE_COMPANY_ID,
                    source_record_id=900501,
                    origin_run_id="origin-run",
                    target_booking_uuid=target,
                )
                if name == "first":
                    first_claimed.set()
                    # Hold the transaction open — and the row locked — until the
                    # loser is provably waiting on it.
                    await release_first.wait()

    async def loser() -> None:
        await first_claimed.wait()
        await contender("second")

    async def referee() -> None:
        await first_claimed.wait()
        await _wait_until_blocked_on_a_lock(session_local)
        release_first.set()

    await asyncio.gather(contender("first"), loser(), referee())

    assert results["first"].won is True
    assert results["second"].won is False
    assert results["second"].reason == ledger_module.CLAIM_HELD_BY_ANOTHER_RUN
    assert results["second"].owner_run_id == "first"

    async with session_local() as session:
        row = (
            await session.execute(
                select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.source_record_id == 900501)
            )
        ).scalar_one()
    assert row.rollback_attempt_run_id == "first"


@pytest.mark.parametrize(
    "mutate, expected",
    [
        pytest.param(
            {"status": "rolled_back"},
            ledger_module.CLAIM_ROW_CHANGED,
            id="status-moved",
        ),
        pytest.param(
            {"target_booking_uuid": "0e9a2222-2222-4333-8444-555566667777"},
            ledger_module.CLAIM_ROW_CHANGED,
            id="target-replaced",
        ),
        pytest.param(
            {"run_id": "another-origin"},
            ledger_module.CLAIM_ROW_CHANGED,
            id="origin-run-changed",
        ),
    ],
)
async def test_a_row_that_moved_under_the_candidate_cannot_be_claimed(session_local, mutate, expected):
    """Every fact the decision rested on is re-tested by the claim itself."""
    target = "0e9a1111-2222-4333-8444-555566667777"
    await _seed_created_row(session_local, run_id="origin-run", record_id=900502, target=target)

    async with session_local() as session:
        async with session.begin():
            await session.execute(
                update(EasyWeekMigrationLedger)
                .where(EasyWeekMigrationLedger.source_record_id == 900502)
                .values(**mutate)
            )

    async with session_local() as session:
        async with session.begin():
            claim = await ledger_module.claim_rollback_attempt(
                session,
                run_id="late-run",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900502,
                origin_run_id="origin-run",
                target_booking_uuid=target,
            )

    assert claim.won is False
    assert claim.reason == expected
    async with session_local() as session:
        row = (
            await session.execute(
                select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.source_record_id == 900502)
            )
        ).scalar_one()
    assert row.rollback_attempted_at is None, "a lost claim must not leave a marker"


async def test_a_release_only_works_for_the_run_that_owns_the_marker(session_local):
    target = "0e9a1111-2222-4333-8444-555566667777"
    await _seed_created_row(session_local, run_id="origin-run", record_id=900503, target=target)
    async with session_local() as session:
        async with session.begin():
            await ledger_module.claim_rollback_attempt(
                session,
                run_id="owner",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900503,
                origin_run_id="origin-run",
                target_booking_uuid=target,
            )

    async with session_local() as session:
        async with session.begin():
            stolen = await ledger_module.release_rollback_attempt(
                session,
                run_id="somebody-else",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900503,
            )
    assert stolen is False, "one run must not clear another run's marker"

    async with session_local() as session:
        async with session.begin():
            released = await ledger_module.release_rollback_attempt(
                session,
                run_id="owner",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900503,
            )
    assert released is True
    async with session_local() as session:
        row = (
            await session.execute(
                select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.source_record_id == 900503)
            )
        ).scalar_one()
    assert row.rollback_attempted_at is None
    assert row.rollback_attempt_run_id is None


async def test_a_finalisation_needs_the_attempt_it_claims_to_finish(session_local):
    target = "0e9a1111-2222-4333-8444-555566667777"
    await _seed_created_row(session_local, run_id="origin-run", record_id=900504, target=target)
    async with session_local() as session:
        async with session.begin():
            await ledger_module.claim_rollback_attempt(
                session,
                run_id="owner",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900504,
                origin_run_id="origin-run",
                target_booking_uuid=target,
            )

    async with session_local() as session:
        async with session.begin():
            wrong = await ledger_module.record_rolled_back(
                session,
                run_id="reporter",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900504,
                expected_attempt_run_id="somebody-else",
            )
    assert wrong is False

    async with session_local() as session:
        async with session.begin():
            right = await ledger_module.record_rolled_back(
                session,
                run_id="reporter",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=900504,
                expected_attempt_run_id="owner",
            )
    assert right is True
    async with session_local() as session:
        row = (
            await session.execute(
                select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.source_record_id == 900504)
            )
        ).scalar_one()
    assert row.status == "rolled_back"
    assert row.last_resolution_run_id == "reporter"
    assert row.run_id == "origin-run", "the origin run is never rewritten"


# ---------------------------------------------------------------------------
# The four states, through the real runner
# ---------------------------------------------------------------------------


def _hook_claim(monkeypatch, *, before=None, after=None):
    """Run `before` / `after` around the runner's real atomic claim.

    Deterministic by construction: the hook fires at the exact instant the race
    it models would happen — after every proof, around the one statement that
    grants the right to mutate. No sleeps, no timing assumptions.
    """
    real = ledger_module.claim_rollback_attempt

    async def wrapper(session, **kwargs):
        if before is not None:
            await before(kwargs)
        claim = await real(session, **kwargs)
        if after is not None and claim.won:
            await after(kwargs)
        return claim

    monkeypatch.setattr(ledger_module, "claim_rollback_attempt", wrapper)


async def test_a_run_that_loses_the_claim_sends_nothing(session_local, source, monkeypatch):
    """Two runs, one row: the loser is out of the mutation path entirely.

    And it says WHOSE row it now is. `rollback_claim_lost` on its own tells an
    operator only that somebody got there first; the owning run id is what turns
    that into an investigation — the report of that run says whether the booking
    was cancelled, refused or left unresolved.
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    real = ledger_module.claim_rollback_attempt
    released: list[int] = []
    finalised: list[int] = []
    real_release = ledger_module.release_rollback_attempt
    real_finalize = ledger_module.record_rolled_back

    async def spy_release(session, **kwargs):
        released.append(kwargs["source_record_id"])
        return await real_release(session, **kwargs)

    async def spy_finalize(session, **kwargs):
        finalised.append(kwargs["source_record_id"])
        return await real_finalize(session, **kwargs)

    monkeypatch.setattr(ledger_module, "release_rollback_attempt", spy_release)
    monkeypatch.setattr(ledger_module, "record_rolled_back", spy_finalize)

    async def competing_run(kwargs):
        if kwargs["source_record_id"] != KA_RECORD_B:
            return
        async with session_local() as other:
            async with other.begin():
                claim = await real(other, **{**kwargs, "run_id": "competing-run"})
        assert claim.won, "the competitor must really own the row"

    _hook_claim(monkeypatch, before=competing_run)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert target not in transport.cancel_puts
    codes = report.as_safe_dict()["reason_codes"]
    assert codes[ROLLBACK_CLAIM_LOST] == 1

    lost = [row for row in report.created_rows if row.get("rollback_outcome") == ROLLBACK_CLAIM_LOST]
    assert len(lost) == 1
    entry = lost[0]
    assert entry["rollback_claim_owner_run_id"] == "competing-run"
    # Holding no claim means sending nothing, and sending nothing means
    # counting nothing — the wave's other row is the only cancel here.
    assert report.mutations_attempted == len(transport.cancel_puts) == 1
    # Not this run, and not the stale snapshot value either: the candidate was
    # read before the race, when nobody owned the row.
    assert entry["rollback_claim_owner_run_id"] != report.run_id
    assert entry["rollback_attempt_run_id"] is None
    assert entry["reason"] == ledger_module.CLAIM_HELD_BY_ANOTHER_RUN

    # The loser acts for nobody on the row it lost: no release, no
    # finalisation, no request. (The other row of the same wave, which this run
    # did win, is cancelled normally — that is what makes the contrast real.)
    assert KA_RECORD_B not in released
    assert KA_RECORD_B not in finalised
    assert RA_RECORD_A in finalised
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"
    assert row.rollback_attempt_run_id == "competing-run", "the marker still names its owner"

    # A run id is technical. Nothing about the customer travels with it.
    blob = report.to_json()
    for leaked in ("+4915112345678", "Testkundin"):
        assert leaked not in blob


@pytest.mark.parametrize(
    "mutate, expected",
    [
        pytest.param({"status": "rolled_back"}, ledger_module.CLAIM_ROW_CHANGED, id="row-changed"),
        pytest.param(None, ledger_module.CLAIM_ROW_MISSING, id="row-missing"),
    ],
)
async def test_a_lost_claim_with_no_proven_owner_names_nobody(session_local, source, monkeypatch, mutate, expected):
    """No owner observed, no owner reported — not even an empty placeholder.

    A row that vanished or moved was never claimed by anyone, and printing a
    made-up id would send an operator looking for a run that does not exist.
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]

    async def move_the_row(kwargs):
        if kwargs["source_record_id"] != KA_RECORD_B:
            return
        async with session_local() as other:
            async with other.begin():
                if mutate is None:
                    await other.execute(
                        delete(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.source_record_id == KA_RECORD_B)
                    )
                else:
                    await other.execute(
                        update(EasyWeekMigrationLedger)
                        .where(EasyWeekMigrationLedger.source_record_id == KA_RECORD_B)
                        .values(**mutate)
                    )

    _hook_claim(monkeypatch, before=move_the_row)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert target not in transport.cancel_puts
    lost = [row for row in report.created_rows if row.get("rollback_outcome") == ROLLBACK_CLAIM_LOST]
    assert len(lost) == 1
    assert lost[0]["reason"] == expected
    assert "rollback_claim_owner_run_id" not in lost[0]


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(httpx.TimeoutException("timeout"), id="preflight-timeout"),
        pytest.param(httpx.ConnectError("disconnected"), id="preflight-disconnect"),
        pytest.param(429, id="preflight-rate-limited"),
        pytest.param(503, id="preflight-503"),
        pytest.param(401, id="preflight-auth"),
        pytest.param(404, id="preflight-not-found"),
    ],
)
async def test_a_read_that_fails_before_the_put_leaves_no_unknown_mutation(session_local, source, monkeypatch, failure):
    """(A) Proven not sent: the marker must not survive it.

    The claim is taken, and then the read the client runs immediately before its
    PUT fails. No request was made, so a marker claiming one would turn any
    later manual cancellation into "our attempt finishing late".
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]

    async def break_the_preflight(kwargs):
        if kwargs["source_record_id"] == KA_RECORD_B:
            transport.get_status_override[target] = failure

    _hook_claim(monkeypatch, after=break_the_preflight)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert target not in transport.cancel_puts
    assert report.as_safe_dict()["reason_codes"][ROLLBACK_NOT_SENT] == 1
    # This row charged the wave nothing: the wave's other row was cancelled
    # normally, and the counter equals the cancels that actually left.
    assert report.mutations_attempted == len(transport.cancel_puts) == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"
    assert row.rollback_attempted_at is None, "no request was sent, so no marker may remain"
    assert row.rollback_attempt_run_id is None


async def test_a_manual_cancel_after_a_pre_put_failure_is_not_our_recovery(session_local, source, monkeypatch):
    """The consequence of the rule above, spelled out end to end."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]

    armed = [True]

    async def break_the_preflight(kwargs):
        # Only the FIRST run's read fails; the hook disarms itself rather than
        # undoing the monkeypatch, which would also unwind the fixtures.
        if kwargs["source_record_id"] == KA_RECORD_B and armed:
            armed.clear()
            transport.get_status_override[target] = 503

    _hook_claim(monkeypatch, after=break_the_preflight)
    await rollback(session_local, transport, run_id=run_id, confirmed=True)

    # An operator cancels the appointment by hand afterwards.
    transport.get_status_override.pop(target, None)
    transport.canceled_uuids.add(target)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert target not in transport.cancel_puts
    codes = report.as_safe_dict()["reason_codes"]
    assert codes.get(ROLLBACK_RECOVERED) is None
    assert codes[ROLLBACK_TARGET_MODIFIED] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"


@pytest.mark.parametrize("status", [401, 403, 404, 422, 429], ids=["401", "403", "404", "422", "429"])
async def test_a_deterministic_refusal_returns_the_mutation_right(session_local, source, status):
    """(B) The request was answered and nothing changed: the claim goes back."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = status

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == 1
    assert report.as_safe_dict()["reason_codes"]["rollback_refused"] == 1
    # Answered by the provider, so the request was made and counts.
    assert report.mutations_attempted == len(transport.cancel_puts)
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created"
    assert row.rollback_attempted_at is None, "a refusal is not an unknown mutation"


async def test_a_refused_row_can_be_cancelled_once_the_cause_is_fixed(session_local, source):
    """And exactly once: the released claim is takeable again, not repeatable."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = 403

    await rollback(session_local, transport, run_id=run_id, confirmed=True)
    assert transport.cancel_puts.count(target) == 1

    # The operator fixes the token and runs the rollback again, explicitly.
    transport.cancel_fail_with.pop(target)
    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == 2, "one deliberate attempt per explicit run"
    assert report.as_safe_dict()["reason_codes"]["rolled_back"] == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "rolled_back"
    assert row.rollback_attempt_run_id is not None


async def test_a_manual_cancel_after_a_deterministic_refusal_is_not_our_recovery(session_local, source):
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = 403

    await rollback(session_local, transport, run_id=run_id, confirmed=True)
    puts = transport.cancel_puts.count(target)
    # Somebody cancels it in the UI afterwards.
    transport.canceled_uuids.add(target)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert transport.cancel_puts.count(target) == puts
    codes = report.as_safe_dict()["reason_codes"]
    assert codes.get(ROLLBACK_RECOVERED) is None
    assert codes[ROLLBACK_TARGET_MODIFIED] == 1


async def test_a_booking_cancelled_between_the_claim_and_the_put_is_not_our_rollback(
    session_local, source, monkeypatch
):
    """The already-cancelled race, at its narrowest.

    Everything above the claim saw a live booking; somebody cancels it in the
    instant before this run's own PUT. The client's read catches it, no request
    is sent, and the row is not credited to this rollback.
    """
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]

    async def cancel_it_elsewhere(kwargs):
        if kwargs["source_record_id"] == KA_RECORD_B:
            transport.canceled_uuids.add(target)

    _hook_claim(monkeypatch, after=cancel_it_elsewhere)

    report = await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert target not in transport.cancel_puts
    codes = report.as_safe_dict()["reason_codes"]
    assert codes[ROLLBACK_CANCELED_ELSEWHERE] == 1
    assert codes.get("rolled_back") is None or codes["rolled_back"] == 1  # the OTHER row of the wave
    # Nothing was sent for this row: the counter matches the wave's real cancels.
    assert report.mutations_attempted == len(transport.cancel_puts) == 1
    row = await row_for(session_local, KA_RECORD_B)
    assert row.status == "created", "somebody else's cancellation is not this run's rollback"
    assert row.rollback_attempted_at is None


async def test_the_marker_survives_only_the_unknown_result(session_local, source):
    """(C) The one state that keeps the marker, next to the ones that release it."""
    transport = RecordingTransport()
    run_id = await applied_run(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_B]
    transport.cancel_fail_with[target] = httpx.TimeoutException("timeout")

    await rollback(session_local, transport, run_id=run_id, confirmed=True)

    row = await row_for(session_local, KA_RECORD_B)
    assert row.rollback_attempted_at is not None
    assert row.rollback_attempt_run_id is not None
    assert row.status == "created"


async def test_no_new_reason_code_carries_pii(session_local, source):
    for code in (ROLLBACK_CLAIM_LOST, ROLLBACK_NOT_SENT, ROLLBACK_CANCELED_ELSEWHERE):
        assert code == code.lower()
        for leaked in ("phone", "+49", "@", "Testkundin"):
            assert leaked not in code


# ---------------------------------------------------------------------------
# The cancel result type
# ---------------------------------------------------------------------------


def test_the_cancel_outcome_enum_has_exactly_two_members():
    """A plain assignment in an Enum body is another member, not a flag.

    `retryable = False` sat in this enum and became `CancelOutcome.retryable`
    with the value `"False"` — a third possible outcome of a cancel that the
    cancel flow can never return and no caller ever handles. It belonged to the
    exception class above it, where the base class already provides it.
    """
    assert list(CancelOutcome) == [
        CancelOutcome.CANCELED_AND_PROVEN,
        CancelOutcome.ALREADY_CANCELED_NO_MUTATION,
    ]
    assert len(CancelOutcome) == 2
    assert CancelOutcome.CANCELED_AND_PROVEN.value == "canceled_and_proven"
    assert CancelOutcome.ALREADY_CANCELED_NO_MUTATION.value == "already_canceled_no_mutation"


def test_retryable_is_not_a_cancel_outcome():
    assert "retryable" not in CancelOutcome.__members__
    assert not hasattr(CancelOutcome, "retryable")
    assert [member.value for member in CancelOutcome] == [
        "canceled_and_proven",
        "already_canceled_no_mutation",
    ]


def test_the_unrepeatable_errors_are_still_not_retryable():
    """Where `retryable` actually belongs, inherited from the base error."""
    assert EasyWeekUncertainMutation.retryable is False
    assert EasyWeekCancelNotSent.retryable is False
    assert EasyWeekUncertainMutation("x", operation="cancel_booking").retryable is False
    assert EasyWeekCancelNotSent("x", operation="cancel_booking").retryable is False
