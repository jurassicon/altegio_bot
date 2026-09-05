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

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
)
from altegio_bot.easyweek_migration.classify import classify_record, legacy_source_fingerprint
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_DRY_RUN,
    MODE_RECONCILE,
    MODE_ROLLBACK_DRY_RUN,
    ROLLBACK_ATTEMPT_UNRESOLVED,
    ROLLBACK_RECOVERED,
    ROLLBACK_RECOVERY_AVAILABLE,
    ROLLBACK_TARGET_MODIFIED,
    ROLLBACK_UNCERTAIN,
    run_apply,
    run_inventory_or_dry_run,
    run_reconcile,
    run_rollback,
)
from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation
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

    await rollback(session_local, transport, run_id=run_id, confirmed=True)

    assert sorted(transport.cancel_puts) == sorted([CREATED_UUIDS[KA_RECORD_B], CREATED_UUIDS[RA_RECORD_A]])
    assert len(transport.cancel_puts) == 2
    rolled = [row for row in await ledger_rows(session_local) if row.status == "rolled_back"]
    assert len(rolled) == 2


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
