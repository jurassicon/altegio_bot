"""Crash, restart, concurrency and migration, on PostgreSQL 16 (§43.5, group D).

The property every test here defends is the one that cannot be recovered by
apologising afterwards: an external request that may have gone out must never be
sent again. So the question is always the same — after this failure, can anything
retry? — and the answer has to be no, with the state readable and a human in the
loop.

Everything runs against a real PostgreSQL: the row locks, the unique constraints,
the ``FOR UPDATE SKIP LOCKED`` claim and the CHECKs are the subject, not an
implementation detail these tests work around.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import uuid as uuid_module
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select, text

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_voucher_mutation import (
    EasyWeekVoucherMutationUnknown,
    VoucherMutationResponse,
)
from altegio_bot.models.models import (
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionOperation,
)
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import (
    FakeReader,
    marker_orders,
    seed_template_and_sender,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    FakeMutator,
    seed_production_preview,
)
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"
STOP_URL = "/ops/voucher-mailings/api/stop"
RECONCILE_URL = "/ops/voucher-mailings/api/reconcile"


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _offer(client, **payload: Any) -> dict[str, Any]:
    answer = (await client.post(PLAN_URL, json=payload)).json()
    assert answer["ready"], answer["reasons"]
    return answer


async def _confirm(client, offer: dict[str, Any]) -> dict[str, Any]:
    response = await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


async def _frozen(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    transports.use(reader=reader)
    offer = await _offer(
        client,
        stage="freeze",
        preview_run_id=run_id,
        expected_recipient_count=count,
        approved_exposure_minor=count * 1000,
    )
    await _confirm(client, offer)
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    batch_id = int(snapshot.batch_id or 0)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    return run_id, batch_id, reader


async def _items(session_maker, batch_id: int) -> dict[int, EasyWeekVoucherProductionBatchItem]:
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem).where(
                        EasyWeekVoucherProductionBatchItem.batch_id == batch_id
                    )
                )
            )
            .scalars()
            .all()
        )
    return {int(row.slot): row for row in rows}


# ===========================================================================
# Concurrent confirmations and concurrent executors
# ===========================================================================


async def test_many_concurrent_confirmations_of_one_offer_make_one_operation(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """Five simultaneous POSTs of the same approval. The unique constraint decides."""
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = {
        "approval_id": offer["approval"]["approval_id"],
        "confirmed_count": offer["targets"]["stage_target_count"],
        "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
    }

    responses = await asyncio.gather(
        *[ui_client.post(CONFIRM_URL, json=body) for _ in range(5)], return_exceptions=True
    )
    ok = [r for r in responses if not isinstance(r, BaseException) and r.status_code == 200]
    assert ok, responses
    created_flags = [r.json()["created"] for r in ok]
    # At most one call reports that it was the one that created the operation.
    assert created_flags.count(True) <= 1, created_flags
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert len([entry for entry in operations if entry.stage == "create"]) == 1


async def test_two_executors_cannot_claim_the_same_operation(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """``FOR UPDATE SKIP LOCKED`` plus a compare-and-set on the status.

    The supported topology has one executor. A mis-deploy produces two, and the
    database — not a convention — is what stops them both running one stage.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)

    first, second = await asyncio.gather(
        operations_module.claim_next_operation(session_maker, owner="executor-a"),
        operations_module.claim_next_operation(session_maker, owner="executor-b"),
    )
    claimed = [entry for entry in (first, second) if entry is not None]
    assert len(claimed) == 1, (first, second)
    assert claimed[0].attempts == 1


async def test_two_concurrent_create_claims_on_one_slot_grant_once(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The per-item claim, raced. One grant, so one possible external request."""
    count = 1
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=count)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()

    first, second = await asyncio.gather(
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=1,
            plan_digest="a" * 64,
            create_window_start=now - timedelta(minutes=5),
            create_window_end=now + timedelta(minutes=5),
        ),
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=1,
            plan_digest="b" * 64,
            create_window_start=now - timedelta(minutes=5),
            create_window_end=now + timedelta(minutes=5),
        ),
    )
    granted = [outcome for outcome in (first, second) if outcome.granted]
    assert len(granted) == 1, (first, second)


async def test_a_stop_racing_a_claim_never_lets_one_more_slot_through(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The stop and the claim both take the header lock, so they serialise.

    Run many times over, because a window this narrow is exactly the kind a single
    lucky pass would hide. Whichever order PostgreSQL picks, the result must be one
    of two consistent answers — never "the stop is active AND a later slot was
    claimed".
    """
    count = 6
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=count)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()

    async def claim(slot: int):
        return await ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=slot,
            plan_digest="c" * 64,
            create_window_start=now - timedelta(minutes=5),
            create_window_end=now + timedelta(minutes=5),
        )

    stop_task = asyncio.create_task(ledger_module.request_stop(session_maker, batch_id=batch_id, requested_by="ops"))
    claims = await asyncio.gather(*[claim(slot) for slot in range(1, count + 1)])
    await stop_task

    # Every refusal after the stop landed is a STOP refusal, not a state one, and
    # no claim was granted once the stop was durable.
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active
    rows = await _items(session_maker, batch_id)
    for outcome, slot in zip(claims, range(1, count + 1), strict=True):
        row = rows[slot]
        if outcome.granted:
            assert row.create_claimed_at is not None
        else:
            # Not claimed means not attempted: no stamp, and nothing in doubt.
            assert row.create_claimed_at is None
            assert row.reconciliation_required is False
    # And from now on nothing more can be claimed at all. The reason may be either
    # refusal and both are correct: a granted claim leaves its slot unresolved,
    # which halts the header in the same transaction, and a halt outranks a stop.
    # What must never happen is a GRANT.
    for slot in (1, count):
        outcome = await claim(slot)
        assert outcome.granted is False
        assert outcome.reason in (
            ledger_module.CLAIM_REFUSED_STOPPED,
            ledger_module.CLAIM_REFUSED_HALTED,
            ledger_module.CLAIM_REFUSED_STATE,
        ), outcome


# ===========================================================================
# §45.4 — the terminal stop is durable, and only execution is terminal
# ===========================================================================


async def test_a_terminal_stop_outlives_a_restart_a_reconcile_and_a_refund(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """§45.4. Nothing in the recovery surface reopens a terminally stopped batch.

    The stop is derived from rows, not from anything held in a process, so the whole
    point is that it cannot be lost: a fresh executor, a readback and an allowed
    refund all run here and the batch is still closed to CREATE, PAY and DELIVER
    afterwards. The three of them staying AVAILABLE is the other half — a stop takes
    away spending, not access to what already happened.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(index) for index in range(count)]))
    await _confirm(ui_client, await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id))
    assert await worker_module.run_once(session_maker, owner="first-executor") is not None
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(index) for index in range(count)], reader=reader, settles=settles),
    )
    await _confirm(ui_client, await _offer(ui_client, stage="pay", preview_run_id=run_id, batch_id=batch_id))
    assert await worker_module.run_once(session_maker, owner="first-executor") is not None

    stop = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    assert stop["stop_active"] is True and stop["stop_terminal"] is True

    # A different executor process, with none of the first one's memory.
    assert await worker_module.run_once(session_maker, owner="restarted-executor") is None
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).terminal is True

    # A readback. It resolves what it can and opens nothing.
    transports.use(reader=reader)
    answer = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
    assert answer.status_code == 200, answer.text
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is True

    # An allowed pre-send refund of one slot: separately confirmed, and permitted.
    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    refunder = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    transports.use(reader=reader, mutator=refunder)
    await _confirm(ui_client, await _offer(ui_client, stage="refund", preview_run_id=run_id, batch_id=batch_id, slot=1))
    finished = await worker_module.run_once(session_maker, owner="restarted-executor")
    assert finished is not None and finished.status == "completed", finished.result
    assert refunder.calls.count("refund") == 1

    # After all three, every spending stage is still refused.
    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    assert state.active is True and state.terminal is True
    for stage in ("create", "pay", "deliver"):
        offer = (
            await ui_client.post(PLAN_URL, json={"stage": stage, "preview_run_id": run_id, "batch_id": batch_id})
        ).json()
        assert not offer["ready"], stage
        assert "voucher_production_stop_terminal" in offer["reasons"], stage
    # The second slot was never sent and is not pretended to be anything else.
    rows = await _items(session_maker, batch_id)
    assert rows[2].status == "paid"
    assert rows[2].send_attempt_count == 0


async def test_closing_the_browser_is_not_a_stop(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """§45.4. Only the explicit press stops a mailing.

    A confirmed stage is durable precisely so that the tab can go away, and that
    must not have become a way to cancel a batch by accident. No client is involved
    in the executor pass below, which is as close to "the browser is gone" as a
    server-side test gets.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    mutator = FakeMutator(create_sequence=[_ok(index) for index in range(count)])
    transports.use(reader=reader, mutator=mutator)
    await _confirm(ui_client, await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id))

    # Nobody is watching any more; the work still happens and no stop appeared.
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.outcome_code == "applied"
    assert mutator.calls.count("create") == count
    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    assert state.active is False and state.terminal is False
    assert all(row.status == "created" for row in (await _items(session_maker, batch_id)).values())


async def test_a_historical_batch_keeps_the_resumable_stop(session_maker, production_configuration, binding_key):
    """§45.4 changes schema 3 only; schemas 1 and 2 keep the §43.6 pause.

    Asserted at the ledger, because the browser only ever creates the new contract:
    a €15 batch's stop is not terminal, and the confirm path's helper still lifts it.
    """
    from altegio_bot.tests import easyweek_voucher_production_fixtures as legacy
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    run_id, _ = await legacy.seed_production_preview(session_maker, count=1)
    await seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, legacy.FakeReader(count=1), legacy.production_request(run_id=run_id), count=1)
    assert frozen.outcome == "frozen", frozen.reasons
    batch_id = int(frozen.batch["batch_id"])
    assert frozen.batch["schema_version"] == "2"

    state = await ledger_module.request_stop(session_maker, batch_id=batch_id, requested_by="ops")
    assert state.active is True and state.terminal is False
    assert (await ledger_module.terminal_stop_active(session_maker, batch_id=batch_id)) is False

    # The §43.6 continuation still works: a confirmation lifts this stop.
    async with session_maker() as session, session.begin():
        lifted = await ledger_module.clear_stop_locked(session, batch_id=batch_id, cleared_by="ops", now=utcnow())
    assert lifted is True
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is False


# ===========================================================================
# Crash, at each of the four moments that matter
# ===========================================================================


async def test_a_crash_before_any_claim_leaves_nothing_to_recover(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The executor dies before it reaches the first slot. No claim, no doubt."""
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="dying-executor")
    assert claimed is not None

    # The process vanishes here. A new executor starts.
    interrupted = await operations_module.interrupt_abandoned(session_maker, include_all_running=True)
    assert [entry.id for entry in interrupted] == [body["operation"]["operation_id"]]

    rows = await _items(session_maker, batch_id)
    assert all(row.create_claimed_at is None for row in rows.values())
    assert all(row.reconciliation_required is False for row in rows.values())
    # Terminal, and never picked up again.
    assert await operations_module.claim_next_operation(session_maker, owner="new-executor") is None


async def test_a_crash_after_a_claim_reads_as_may_have_happened(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The claim is committed before the request leaves, so it is the durable trace.

    A crash between the commit and the socket and a crash after the response are
    indistinguishable afterwards — and both must read as "it may have gone out".
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    class DyingMutator(FakeMutator):
        """The process dies while slot 2's request is in flight."""

        async def create_voucher_order(self, **kwargs: Any) -> Any:
            if len(self.create_calls) == 1:
                self.create_calls.append(kwargs)
                self.calls.append("create")
                raise EasyWeekVoucherMutationUnknown("the process died mid-request")
            return await super().create_voucher_order(**kwargs)

    mutator = DyingMutator(create_sequence=[_ok(0), _ok(1), _ok(2)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None

    rows = await _items(session_maker, batch_id)
    assert rows[1].status == "created"
    # Slot 2's outcome is unknown and stays unknown.
    assert rows[2].status == "create_unknown"
    assert rows[2].reconciliation_required is True
    assert rows[2].manual_cleanup_required is True
    # Slot 3 was never attempted — the suffix stopped.
    assert rows[3].status == "planned"
    assert rows[3].create_claimed_at is None
    # The batch is halted, and the operation is not retryable.
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.halted is True
    assert await operations_module.claim_next_operation(session_maker, owner="again") is None


async def test_a_crash_after_the_external_effect_but_before_the_answer_is_not_retried(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The worst moment: the voucher exists, and the process never wrote it down.

    What makes this survivable is that the claim went in FIRST. The slot reads
    ``create_claimed`` — not ``planned`` — so nothing may claim it again, and the
    operation is interrupted rather than queued.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="dying")
    assert claimed is not None

    # Slot 1's claim is committed, the POST went out, and the process died before
    # recording the answer. Written directly, because the real sequence is exactly
    # "the row is committed and then nothing else happens".
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()
    granted = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=1,
        plan_digest=offer["approval"].get("plan_digest", "d" * 64),
        create_window_start=now - timedelta(minutes=5),
        create_window_end=now + timedelta(minutes=5),
    )
    assert granted.granted

    interrupted = await operations_module.interrupt_abandoned(session_maker, include_all_running=True)
    assert len(interrupted) == 1
    assert interrupted[0].status == "interrupted"
    assert interrupted[0].outcome_code == "voucher_production_execution_interrupted"

    rows = await _items(session_maker, batch_id)
    # NOT reset to planned. The one reading that cannot create a second voucher.
    assert rows[1].status == "create_claimed"
    assert rows[1].reconciliation_required is True
    # Nothing is queued, so nothing can be retried.
    assert await operations_module.claim_next_operation(session_maker, owner="new") is None
    # And a fresh plan refuses, because the batch is halted by the claim.
    transports.use(reader=reader)
    blocked = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert blocked["ready"] is False
    assert "voucher_production_batch_halted" in blocked["reasons"]


async def test_a_restart_never_returns_an_operation_to_the_queue(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """There is no path from ``running`` or ``interrupted`` back to ``queued``.

    Asserted over the whole status vocabulary rather than one case, because a future
    "helpful" requeue is exactly the change this guards against.
    """
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    assert await operations_module.claim_next_operation(session_maker, owner="first") is not None

    for _ in range(3):
        await worker_module.run_worker(session_maker, owner="restarted", poll_sec=0.01, max_iterations=1)
        async with session_maker() as session:
            statuses = list((await session.execute(select(EasyWeekVoucherProductionOperation.status))).scalars().all())
        assert "queued" not in statuses, statuses
        assert "interrupted" in statuses, statuses


async def test_an_expired_lease_interrupts_rather_than_retries(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """A worker killed between the claim and the next heartbeat."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="killed", lease=timedelta(seconds=-1))
    assert claimed is not None

    swept = await operations_module.interrupt_abandoned(session_maker)
    assert [entry.status for entry in swept] == ["interrupted"]
    assert await operations_module.claim_next_operation(session_maker, owner="next") is None


async def test_a_live_executor_is_not_swept(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """A slow stage must not be called abandoned while its worker is renewing."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="busy", lease=timedelta(minutes=10))
    assert claimed is not None

    assert await operations_module.interrupt_abandoned(session_maker) == []
    assert await operations_module.renew_lease(session_maker, operation_id=claimed.id, owner="busy") is True
    # A different owner cannot renew it.
    assert await operations_module.renew_lease(session_maker, operation_id=claimed.id, owner="other") is False
    assert await operations_module.interrupt_abandoned(session_maker) == []


async def test_a_finished_operation_is_never_overwritten(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """A late writer must not replace an interrupted row with a tidier-looking one."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)
    operation_id = body["operation"]["operation_id"]
    await operations_module.claim_next_operation(session_maker, owner="dying")
    await operations_module.interrupt_abandoned(session_maker, include_all_running=True)

    late = await operations_module.finish_operation(
        session_maker,
        operation_id=operation_id,
        owner="dying",
        status="completed",
        outcome_code="applied",
    )
    assert late is not None
    assert late.status == "interrupted"
    assert late.outcome_code == "voucher_production_execution_interrupted"


async def test_approvals_and_audit_survive_a_crash(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The record of who authorised what is not lost with the process that ran it."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    await operations_module.claim_next_operation(session_maker, owner="dying")
    await operations_module.interrupt_abandoned(session_maker, include_all_running=True)

    approval = await operations_module.load_approval(session_maker, approval_id=offer["approval"]["approval_id"])
    assert approval is not None
    assert approval.status == "consumed"
    assert approval.target_slots == (1,)
    async with session_maker() as session:
        audit = list(
            (
                await session.execute(
                    text("SELECT action, principal FROM easyweek_voucher_production_audit ORDER BY id")
                )
            ).all()
        )
    actions = [row[0] for row in audit]
    assert "plan" in actions and "confirm" in actions
    assert {row[1] for row in audit} == {"ops-operator"}


# ===========================================================================
# Schema guarantees
# ===========================================================================


async def test_the_database_refuses_a_second_operation_for_one_approval(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """Not the button, not the handler: the constraint."""
    from sqlalchemy.exc import IntegrityError

    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)
    existing = await operations_module.load_operation(session_maker, operation_id=body["operation"]["operation_id"])
    assert existing is not None

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionOperation(
                        approval_id=offer["approval"]["approval_id"],
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        provider="easyweek",
                        company_id=322579,
                        stage="create",
                        campaign_run_id=run_id,
                        batch_id=batch_id,
                        principal="ops-operator",
                        session_fingerprint="f" * 64,
                        identification_limit="shared_ops_account",
                        status="queued",
                        queued_at=utcnow(),
                    )
                )


async def test_a_queued_operation_cannot_hold_a_lease(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """A CHECK, so a sweep can never mistake a waiting row for an abandoned one."""
    from sqlalchemy.exc import IntegrityError

    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _offer(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    body = await _confirm(ui_client, offer)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                row = await session.get(EasyWeekVoucherProductionOperation, body["operation"]["operation_id"])
                assert row is not None
                row.lease_owner = "somebody"
                row.lease_expires_at = utcnow()


async def test_a_stop_row_cannot_exist_twice_for_one_batch(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import EasyWeekVoucherProductionStopRequest

    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    await ledger_module.request_stop(session_maker, batch_id=batch_id, requested_by="ops")

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionStopRequest(batch_id=batch_id, requested_by="ops", requested_at=utcnow())
                )


async def test_an_approval_cannot_cover_no_slots(session_maker):
    """An empty slot list is what an executor could read as "no restriction"."""
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import EasyWeekVoucherProductionApproval

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionApproval(
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        request_schema_version="1",
                        provider="easyweek",
                        company_id=322579,
                        stage="create",
                        principal="ops",
                        session_fingerprint="f" * 64,
                        identification_limit="shared_ops_account",
                        campaign_run_id=1,
                        batch_id=None,
                        target_slots=[],
                        target_slot_count=0,
                        stage_target_count=1,
                        stage_amount_minor=1500,
                        batch_recipient_count=1,
                        batch_exposure_minor=1500,
                        campaign_period_start=utcnow(),
                        campaign_period_end=utcnow() + timedelta(days=1),
                        plan_digest="a" * 64,
                        plan_issued_at=utcnow(),
                        expires_at=utcnow() + timedelta(minutes=30),
                        issuer_pinned=True,
                        issuer_membership_proven=True,
                        runtime_identity_bound=True,
                        baseline_version="x",
                        frozen_digest=None,
                        status="pending",
                    )
                )


async def test_an_approval_expiry_must_be_after_its_issue(session_maker):
    """Queue time is not reading time, and a window has to be a window."""
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import EasyWeekVoucherProductionApproval

    moment = utcnow()
    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionApproval(
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        request_schema_version="1",
                        provider="easyweek",
                        company_id=322579,
                        stage="freeze",
                        principal="ops",
                        session_fingerprint="f" * 64,
                        identification_limit="shared_ops_account",
                        campaign_run_id=1,
                        batch_id=None,
                        target_slots=[1],
                        target_slot_count=1,
                        stage_target_count=1,
                        stage_amount_minor=1500,
                        batch_recipient_count=1,
                        batch_exposure_minor=1500,
                        campaign_period_start=moment,
                        campaign_period_end=moment + timedelta(days=1),
                        plan_digest="a" * 64,
                        plan_issued_at=moment,
                        expires_at=moment - timedelta(minutes=1),
                        issuer_pinned=True,
                        issuer_membership_proven=True,
                        runtime_identity_bound=True,
                        baseline_version="x",
                        frozen_digest=None,
                        status="pending",
                    )
                )


# ===========================================================================
# Migration: upgrade, downgrade, re-upgrade, one head
# ===========================================================================

# Set this to "1" to turn "this environment cannot create a disposable database"
# from a skip into a failure, exactly as ALTEGIO_REQUIRE_MIGTEST does for the §42
# migration suite. The required workflow sets it: this module is one of the dedicated
# gates in docs/ops/test_suite_tiers.md, so a machine without a usable PostgreSQL
# fails the gate rather than reporting a green skip for the migration cycle.
#
# The invariant that must never be able to skip — exactly one Alembic head — does
# not use this fixture at all and runs unconditionally in the rest shard.
REQUIRE_MIGRATION = os.getenv("ALTEGIO_REQUIRE_VOUCHER_MAILING_MIGTEST") == "1"

# The revision these four tables were created ON TOP OF. Named, so "undo the
# migration that created them" keeps meaning that however many revisions follow.
OPERATIONS_PARENT_REVISION = "e2c7b4f16a83"


def _alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run alembic against one database.

    ``heads`` needs no database and is given the repo path as a harmless value, so
    that the single-head check can run with no server at all.
    """
    env = dict(os.environ)
    env["DATABASE_URL"] = database_url
    return subprocess.run(  # noqa: S603 - fixed command, generated args
        ["uv", "run", "alembic", *args],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=_repo_root(),
    )


def _repo_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.exists(os.path.join(here, "alembic.ini")):
            return here
        here = os.path.dirname(here)
    raise AssertionError("alembic.ini not found above the test file")


@pytest.fixture
async def disposable_database(session_maker) -> Any:
    """A throwaway database for the migration tests, or a skip.

    Created next to the configured test database, through the server the suite is
    already connected to, and dropped afterwards. Never the suite's own database:
    these tests run DDL from scratch.
    """
    from altegio_bot.settings import Settings

    base = Settings().database_url
    name = f"altegio_mig_{uuid_module.uuid4().hex[:12]}"
    admin_url = base.rsplit("/", 1)[0] + "/postgres"
    target = base.rsplit("/", 1)[0] + "/" + name

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as exc:  # noqa: BLE001 - the environment, not the code
        await engine.dispose()
        if REQUIRE_MIGRATION:
            raise
        pytest.skip(f"cannot create a disposable database: {exc}")
    try:
        yield target
    finally:
        async with engine.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await engine.dispose()


def test_the_chain_has_exactly_one_head():
    """A second head is a deployment that cannot be upgraded.

    Deliberately NOT behind the disposable-database fixture. ``alembic heads``
    reads the revision files and needs no database at all, and this is the one
    invariant that must never be able to skip: a second head is caught here, in
    the required gate, rather than by a deploy that stops half-way.
    """
    result = _alembic(_repo_root(), "heads")
    assert result.returncode == 0, result.stderr
    heads = [line for line in result.stdout.splitlines() if "(head)" in line]
    assert len(heads) == 1, result.stdout


async def test_upgrade_downgrade_and_re_upgrade_on_an_empty_database(disposable_database: str):
    """The new tables appear, disappear and reappear, and the head stays single.

    Addressed by REVISION rather than by "head" and "-1", for the reason §42's own
    migration tests had to learn the hard way: a later revision — including this
    PR's own follow-up — makes "one step back" mean something else, and a test that
    said "-1" would then be checking a different migration than the one it names.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    new_tables = (
        "easyweek_voucher_production_approvals",
        "easyweek_voucher_production_operations",
        "easyweek_voucher_production_stop_requests",
        "easyweek_voucher_production_audit",
    )
    assert await _tables(disposable_database, new_tables) == set(new_tables)

    # Back past the revision that created them, by name.
    assert _alembic(disposable_database, "downgrade", OPERATIONS_PARENT_REVISION).returncode == 0
    assert await _tables(disposable_database, new_tables) == set()
    # The §42 tables are untouched by the downgrade.
    historical = (
        "easyweek_voucher_production_batches",
        "easyweek_voucher_production_batch_items",
        "easyweek_voucher_production_batch_attempts",
        "easyweek_voucher_snapshot_batches",
        "easyweek_voucher_canary_ledger",
    )
    assert await _tables(disposable_database, historical) == set(historical)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, new_tables) == set(new_tables)
    current = _alembic(disposable_database, "current")
    assert current.returncode == 0
    assert "(head)" in current.stdout
    # The stop generation the review's R1 fix added is part of the upgraded schema.
    assert await _columns(disposable_database, "easyweek_voucher_production_approvals") >= {"stop_generation_at_plan"}


async def test_a_downgrade_keeps_historical_ledger_rows(disposable_database: str):
    """An upgrade/downgrade cycle over a populated historical ledger loses nothing.

    The §36 canary ledger is used because it is the oldest voucher ledger in the
    chain and has no foreign keys of its own, so one row can be written as the old
    phase would have written it. Its columns are spelled out here rather than built
    from the ORM: the point is that a REAL historical row survives, and a row built
    from today's model would prove only that today's model round-trips.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_canary_ledger ("
                    " canary_scope, request_schema_version, template_config_digest,"
                    " create_plan_digest, customer_fingerprint, staffer_fingerprint,"
                    " account_fingerprint, reconciliation_marker, status,"
                    " create_claimed_at, create_attempted_at,"
                    " create_window_start, create_window_end"
                    ") VALUES ("
                    " :scope, '1', :digest, :digest, :fp, :fp, :fp, :marker, 'create_claimed',"
                    " now() - interval '30 minutes', now() - interval '30 minutes',"
                    " now() - interval '1 hour', now() + interval '1 hour')"
                ),
                {
                    "scope": "historical_canary_v1",
                    "digest": "a" * 64,
                    "fp": "b" * 64,
                    "marker": "historical-" + uuid_module.uuid4().hex[:10],
                },
            )
        async with engine.connect() as conn:
            before = (
                await conn.execute(text("SELECT count(*), max(canary_scope) FROM easyweek_voucher_canary_ledger"))
            ).one()
        assert before[0] == 1 and before[1] == "historical_canary_v1"

        # Down and back up again. The historical row is not the new migration's to
        # touch, in either direction.
        assert _alembic(disposable_database, "downgrade", OPERATIONS_PARENT_REVISION).returncode == 0
        async with engine.connect() as conn:
            midway = (await conn.execute(text("SELECT count(*) FROM easyweek_voucher_canary_ledger"))).scalar_one()
        assert midway == 1

        assert _alembic(disposable_database, "upgrade", "head").returncode == 0
        async with engine.connect() as conn:
            after = (
                await conn.execute(text("SELECT count(*), max(canary_scope) FROM easyweek_voucher_canary_ledger"))
            ).one()
        assert after == before
    finally:
        await engine.dispose()


async def test_upgrading_a_database_that_already_holds_a_production_batch(
    disposable_database: str,
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The §42 tables the new foreign keys point at may already have rows.

    A real deployment upgrades a database with production batches in it, so the
    migration is applied here to a schema that holds one — written through the UI,
    with its campaign run, its recipients and its slots — and the batch has to come
    out of the cycle unchanged.
    """
    # Build a real batch in the suite's own database first, then prove the same
    # migration sequence applies cleanly to a schema shaped like that one.
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=2)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.exists and snapshot.recipient_count == 2

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert _alembic(disposable_database, "downgrade", OPERATIONS_PARENT_REVISION).returncode == 0
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    current = _alembic(disposable_database, "current")
    assert current.returncode == 0 and "(head)" in current.stdout

    # And the batch in the live database is untouched by any of it.
    after = await ledger_module.load(session_maker, batch_id=batch_id)
    assert after.recipient_count == snapshot.recipient_count
    assert after.frozen_digest == snapshot.frozen_digest
    assert after.staffer_uuid == snapshot.staffer_uuid


async def _columns(database_url: str, table: str) -> set[str]:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_name = :t"),
                    {"t": table},
                )
            ).all()
        return {row[0] for row in rows}
    finally:
        await engine.dispose()


async def _tables(database_url: str, names: tuple[str, ...]) -> set[str]:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT tablename FROM pg_tables WHERE tablename = ANY(:names)"),
                    {"names": list(names)},
                )
            ).all()
        return {row[0] for row in rows}
    finally:
        await engine.dispose()


# ===========================================================================
# The rollback the runbook actually documents (review F3)
# ===========================================================================

RUNBOOK = os.path.join(_repo_root(), "docs", "easyweek", "VOUCHER_PRODUCTION_MAILING_RUNBOOK.md")

# The revision the stop-generation fix was added ON TOP OF: one step back from the
# head, and the target of a rollback of the FIX alone.
STOP_GENERATION_PARENT_REVISION = "a4f1c9d26b70"
STOP_GENERATION_COLUMN = "stop_generation_at_plan"
STOP_GENERATION_CHECK = "ck_ew_voucher_production_approval_stop_gen"

PHASE_TABLES = (
    "easyweek_voucher_production_approvals",
    "easyweek_voucher_production_operations",
    "easyweek_voucher_production_stop_requests",
    "easyweek_voucher_production_audit",
)
HISTORICAL_TABLES = (
    "easyweek_voucher_production_batches",
    "easyweek_voucher_production_batch_items",
    "easyweek_voucher_production_batch_attempts",
    "easyweek_voucher_snapshot_batches",
    "easyweek_voucher_canary_ledger",
)


def _rollback_section() -> str:
    text_body = open(RUNBOOK, encoding="utf-8").read()
    assert "### 12.2 Rollback" in text_body, "the runbook has no rollback section"
    return text_body.split("### 12.2 Rollback", 1)[1].split("\n## ", 1)[0]


def _rollback_commands() -> list[str]:
    """The COMMANDS of the rollback section, without the prose that explains them.

    The prose names ``alembic downgrade -1`` on purpose — to say that it is not what
    the section used to claim it was — so a scan of the whole text would flag the very
    sentence that fixes the defect.
    """
    inside = False
    commands: list[str] = []
    for line in _rollback_section().splitlines():
        if line.startswith("```"):
            inside = line.startswith("```bash")
            continue
        if inside and line.strip():
            commands.append(line.strip())
    return commands


def _documented_downgrade_targets() -> dict[str, str]:
    """The revisions the runbook tells an administrator to downgrade TO.

    Read out of the document rather than written here, because the point of these
    tests is that the documented command does what the document says it does. A test
    with its own hard-coded revision would stay green while the runbook drifted — and
    that drift is exactly the defect under repair.
    """
    import re as re_module

    scope = None
    found: dict[str, str] = {}
    for line in _rollback_section().splitlines():
        if line.startswith("Scope A"):
            scope = "A"
        elif line.startswith("Scope B"):
            scope = "B"
        match = re_module.search(r"alembic downgrade (\S+)", line)
        if match and scope is not None:
            found.setdefault(scope, match.group(1))
    return found


def test_the_runbook_names_explicit_rollback_revisions():
    """Review F3: ``downgrade -1`` was documented as dropping the four tables.

    It never did once a second §43 revision existed. One step back from the head
    removes the stop-generation column and leaves every table in place, so an
    administrator following the old instruction would have reported a rollback that
    had not happened and left the new application over a schema missing a column it
    reads.

    No database is needed for this one, and that is deliberate: a wrong instruction
    is caught by the required gate even on a machine with no PostgreSQL at all.
    """
    section = _rollback_section()
    commands = _rollback_commands()
    assert commands, "the rollback section has no commands at all"
    assert not [line for line in commands if "downgrade -1" in line], (
        f"the runbook still tells an operator to step back one revision: {commands}"
    )
    # And it says so in words, so the next reader does not reintroduce it.
    assert "`alembic downgrade -1` is scope A" in section

    targets = _documented_downgrade_targets()
    assert targets == {"A": STOP_GENERATION_PARENT_REVISION, "B": OPERATIONS_PARENT_REVISION}, targets

    # The two scopes are named and distinguished, with what each one costs.
    assert "Scope A" in section and "Scope B" in section
    assert STOP_GENERATION_COLUMN in section
    assert "pg_dump" in section, "a rollback without a backup is not documented as needing one"
    # The ordering rule the defect would otherwise invite: new code, old schema.
    assert "stop altegio-api altegio-easyweek-voucher-executor" in section
    # And it does not read as routine maintenance.
    assert "not a maintenance step" in section
    # The CLI stays shut either way.
    assert "no supported way to run a mailing" in section


async def test_the_documented_fix_rollback_keeps_the_four_tables(disposable_database: str):
    """Scope A, run exactly as the runbook spells it.

    The column and its CHECK go; the four tables, the historical ledgers and every
    other revision stay. Then forward again, because a rollback nobody can come back
    from is not one.
    """
    target = _documented_downgrade_targets()["A"]
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, PHASE_TABLES) == set(PHASE_TABLES)
    assert STOP_GENERATION_COLUMN in await _columns(disposable_database, PHASE_TABLES[0])

    assert _alembic(disposable_database, "downgrade", target).returncode == 0

    current = _alembic(disposable_database, "current")
    assert current.returncode == 0
    assert target in current.stdout, current.stdout
    # The documented outcome: the tables are still there.
    assert await _tables(disposable_database, PHASE_TABLES) == set(PHASE_TABLES)
    assert STOP_GENERATION_COLUMN not in await _columns(disposable_database, PHASE_TABLES[0])
    assert STOP_GENERATION_CHECK not in await _check_constraints(disposable_database, PHASE_TABLES[0])
    assert await _tables(disposable_database, HISTORICAL_TABLES) == set(HISTORICAL_TABLES)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert STOP_GENERATION_COLUMN in await _columns(disposable_database, PHASE_TABLES[0])
    assert STOP_GENERATION_CHECK in await _check_constraints(disposable_database, PHASE_TABLES[0])
    back = _alembic(disposable_database, "current")
    assert back.returncode == 0 and "(head)" in back.stdout


async def test_the_documented_phase_rollback_drops_the_four_tables(disposable_database: str):
    """Scope B, run exactly as the runbook spells it.

    This is the transition the old instruction CLAIMED to perform, so it is checked
    under the revision the document now names — including the historical ledgers it
    promises not to touch, with a real row in one of them.
    """
    target = _documented_downgrade_targets()["B"]
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    from sqlalchemy.ext.asyncio import create_async_engine

    marker = "historical-" + uuid_module.uuid4().hex[:10]
    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_canary_ledger ("
                    " canary_scope, request_schema_version, template_config_digest,"
                    " create_plan_digest, customer_fingerprint, staffer_fingerprint,"
                    " account_fingerprint, reconciliation_marker, status,"
                    " create_claimed_at, create_attempted_at,"
                    " create_window_start, create_window_end"
                    ") VALUES ("
                    " 'historical_canary_v1', '1', :digest, :digest, :fp, :fp, :fp, :marker,"
                    " 'create_claimed', now(), now(), now() - interval '1 hour', now() + interval '1 hour')"
                ),
                {"digest": "c" * 64, "fp": "d" * 64, "marker": marker},
            )

        assert _alembic(disposable_database, "downgrade", target).returncode == 0

        current = _alembic(disposable_database, "current")
        assert current.returncode == 0 and target in current.stdout, current.stdout
        assert await _tables(disposable_database, PHASE_TABLES) == set()
        assert await _tables(disposable_database, HISTORICAL_TABLES) == set(HISTORICAL_TABLES)
        async with engine.connect() as conn:
            kept = (
                await conn.execute(
                    text("SELECT count(*) FROM easyweek_voucher_canary_ledger WHERE reconciliation_marker = :m"),
                    {"m": marker},
                )
            ).scalar_one()
        assert kept == 1, "the rollback took a historical ledger row with it"

        assert _alembic(disposable_database, "upgrade", "head").returncode == 0
        assert await _tables(disposable_database, PHASE_TABLES) == set(PHASE_TABLES)
        assert STOP_GENERATION_COLUMN in await _columns(disposable_database, PHASE_TABLES[0])
        heads = _alembic(_repo_root(), "heads")
        assert len([line for line in heads.stdout.splitlines() if "(head)" in line]) == 1, heads.stdout
    finally:
        await engine.dispose()


async def _check_constraints(database_url: str, table: str) -> set[str]:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid"
                        " WHERE t.relname = :t AND c.contype = 'c'"
                    ),
                    {"t": table},
                )
            ).all()
        return {row[0] for row in rows}
    finally:
        await engine.dispose()


async def test_pr21_uuid_migration_refuses_loss_and_preserves_legacy_rows(disposable_database: str):
    """PG16: nullable numeric is UUID-only; populated downgrade refuses atomically."""
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "c7e3b8a14f29").returncode == 0
    engine = create_async_engine(disposable_database, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            version = int(await conn.scalar(text("SHOW server_version_num")))
            assert 160000 <= version < 170000
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, raw) "
                    "VALUES ('altegio', 322579, 91000001, '{}'::jsonb)"
                )
            )
        upgraded = _alembic(disposable_database, "upgrade", "f6a8d2c91b47")
        assert upgraded.returncode == 0, upgraded.stderr
        async with engine.connect() as conn:
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, easyweek_customer_uuid, raw) "
                    "VALUES ('easyweek', 322579, NULL, '91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                )
            )
            with pytest.raises(Exception):
                await conn.execute(
                    text(
                        "INSERT INTO clients (provider, company_id, altegio_client_id, raw) "
                        "VALUES ('altegio', 322579, NULL, '{}'::jsonb)"
                    )
                )
            with pytest.raises(Exception):
                await conn.execute(
                    text(
                        "INSERT INTO clients (provider, company_id, altegio_client_id, easyweek_customer_uuid, raw) "
                        "VALUES ('easyweek', 315607, NULL, '91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                    )
                )
        refused = _alembic(disposable_database, "downgrade", "c7e3b8a14f29")
        assert refused.returncode != 0 and "PR-21 downgrade refused" in refused.stderr
        async with engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM clients")) == 2
            assert await conn.scalar(text("SELECT version_num FROM alembic_version")) == "f6a8d2c91b47"
            assert (
                await conn.scalar(text("SELECT altegio_client_id FROM clients WHERE provider = 'altegio'")) == 91000001
            )
            assert "manual_policy" in await _columns(disposable_database, "campaign_recipients")
    finally:
        await engine.dispose()


async def test_pr21_empty_upgrade_downgrade_reupgrade_preserves_constraints(disposable_database: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert _alembic(disposable_database, "downgrade", "c7e3b8a14f29").returncode == 0
    assert "easyweek_customer_uuid" not in await _columns(disposable_database, "clients")
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    engine = create_async_engine(disposable_database)
    try:
        async with engine.connect() as conn:
            constraints = set((await conn.execute(text("SELECT conname FROM pg_constraint"))).scalars())
            assert {
                "ck_clients_external_identity",
                "uq_clients_provider_company_easyweek_uuid",
                "ck_campaign_recipients_manual_policy",
                "ck_campaign_recipients_manual_policy_proof",
                "ck_ew_voucher_production_item_source",
                "ck_ew_voucher_production_item_policy",
                "uq_ew_voucher_production_item_entitlement",
                "ck_ew_voucher_production_item_single_attempt",
                "ck_ew_voucher_production_item_refund_is_pre_send",
                "ck_ew_manual_plan_applied",
            } <= constraints
    finally:
        await engine.dispose()


async def test_pr21_branch_uuid_upgrade_preserves_rows_and_refuses_lossy_downgrade(disposable_database: str):
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "f6a8d2c91b47").returncode == 0
    engine = create_async_engine(disposable_database, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            assert 160000 <= int(await conn.scalar(text("SHOW server_version_num"))) < 170000
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, raw) "
                    "VALUES ('altegio', 322579, 91000001, '{}'::jsonb)"
                )
            )
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, "
                    "easyweek_customer_uuid, wa_opted_out, raw) VALUES ('easyweek', 322579, 17, "
                    "'91919191-1212-4343-8787-565656565656', true, '{}'::jsonb)"
                )
            )
            before = (await conn.execute(text("SELECT * FROM clients ORDER BY id"))).all()
        upgraded = _alembic(disposable_database, "upgrade", "head")
        assert upgraded.returncode == 0, upgraded.stderr
        async with engine.connect() as conn:
            upgraded_revision = await conn.scalar(text("SELECT version_num FROM alembic_version"))
            assert (await conn.execute(text("SELECT * FROM clients ORDER BY id"))).all() == before
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, "
                    "easyweek_customer_uuid, raw) VALUES ('easyweek', 315607, 17, "
                    "'91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                )
            )
            with pytest.raises(Exception):
                await conn.execute(
                    text(
                        "INSERT INTO clients (provider, company_id, altegio_client_id, "
                        "easyweek_customer_uuid, raw) VALUES ('easyweek', 315607, NULL, "
                        "'91919191-1212-4343-8787-565656565656', '{}'::jsonb)"
                    )
                )
        refused = _alembic(disposable_database, "downgrade", "f6a8d2c91b47")
        assert refused.returncode != 0 and "PR-21 branch downgrade refused" in refused.stderr
        async with engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM clients")) == 3
            assert await conn.scalar(text("SELECT version_num FROM alembic_version")) == upgraded_revision
            assert (
                await conn.execute(text("SELECT * FROM clients WHERE company_id=322579 ORDER BY id"))
            ).all() == before
            # Synthetic fixture cleanup permits exercising the supported reverse path.
            await conn.execute(text("DELETE FROM clients WHERE company_id=315607"))
        assert _alembic(disposable_database, "downgrade", "f6a8d2c91b47").returncode == 0
        assert _alembic(disposable_database, "upgrade", "head").returncode == 0
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT * FROM clients ORDER BY id"))).all() == before
    finally:
        await engine.dispose()


# ===========================================================================
# §46.4 / PR-22.1 — the consent and delivery-evidence migration
# ===========================================================================

# The revision the consent and evidence tables were created ON TOP OF. Named,
# so "undo the migration that created them" keeps meaning that however many
# revisions follow it.
CONSENT_PARENT_REVISION = "c9e7a34d6210"

CONSENT_TABLES = (
    "whatsapp_inbound_receipts",
    "whatsapp_consent_suppressions",
    "easyweek_voucher_production_delivery_failures",
)


async def test_the_consent_migration_cycles_on_an_empty_database(disposable_database: str):
    """The three tables and three columns appear, disappear and reappear.

    Addressed by revision rather than by ``-1`` for the same reason the §43
    tests are: the next revision would silently make "one step back" mean a
    different migration than the one this test names.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, CONSENT_TABLES) == set(CONSENT_TABLES)
    assert await _columns(disposable_database, "whatsapp_events") >= {"inbound_authenticity"}
    assert await _columns(disposable_database, "easyweek_voucher_production_batch_attempts") >= {
        "sender_phone_number_id_digest",
        "recipient_digest",
    }

    assert _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION).returncode == 0
    assert await _tables(disposable_database, CONSENT_TABLES) == set()
    assert "inbound_authenticity" not in await _columns(disposable_database, "whatsapp_events")
    # Everything the migration does NOT own is untouched by the rollback.
    assert await _tables(disposable_database, HISTORICAL_TABLES) == set(HISTORICAL_TABLES)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, CONSENT_TABLES) == set(CONSENT_TABLES)
    current = _alembic(disposable_database, "current")
    assert current.returncode == 0 and "(head)" in current.stdout


async def test_the_consent_migration_upgrades_a_populated_historical_database(disposable_database: str):
    """A real deployment upgrades a database that already holds history.

    The rows are written the way the OLD code wrote them — a client card with an
    opt-out flag, a stored WhatsApp event, an attempt row with no route — and
    the cycle has to leave every one of them exactly as it found it, including
    the two things that must stay NULL: an authenticity nobody recorded and a
    route nobody froze.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", CONSENT_PARENT_REVISION).returncode == 0

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO clients (provider, company_id, altegio_client_id, phone_e164,"
                    " display_name, raw, wa_opted_out, wa_opted_out_at, wa_opt_out_reason)"
                    " VALUES ('altegio', 1, 918273, '+4915177993001', 'Historical', '{}'::jsonb,"
                    " true, now() - interval '100 days', 'wa:stop')"
                )
            )
            await conn.execute(
                text(
                    "INSERT INTO whatsapp_events (dedupe_key, status, query, headers, payload)"
                    " VALUES ('wa:historical-1', 'processed', '{}'::jsonb, '{}'::jsonb,"
                    " CAST(:payload AS jsonb))"
                ),
                {
                    "dedupe": "wa:historical-1",
                    "payload": (
                        '{"entry": [{"changes": [{"value": {"metadata": {"phone_number_id": "OLD"},'
                        ' "messages": [{"from": "4915177993001", "id": "wamid.OLD",'
                        ' "timestamp": "1700000000", "type": "text", "text": {"body": "\\u0441\\u0442\\u043e\\u043f"}}]'
                        "}}]}]}"
                    ),
                },
            )

        assert _alembic(disposable_database, "upgrade", "head").returncode == 0

        async with engine.connect() as conn:
            card = (
                await conn.execute(
                    text("SELECT wa_opted_out, wa_opt_out_reason FROM clients WHERE altegio_client_id = 918273")
                )
            ).one()
            assert card[0] is True and card[1] == "wa:stop"
            # An event stored before this phase existed proves no signature, and
            # the upgrade does not invent one for it.
            authenticity = (
                await conn.execute(
                    text("SELECT inbound_authenticity FROM whatsapp_events WHERE dedupe_key = 'wa:historical-1'")
                )
            ).scalar_one()
            assert authenticity is None
            # And the upgrade creates no receipt and no refusal by itself: the
            # historical reconciliation is an explicit, separate action.
            assert (await conn.execute(text("SELECT count(*) FROM whatsapp_inbound_receipts"))).scalar_one() == 0
            assert (await conn.execute(text("SELECT count(*) FROM whatsapp_consent_suppressions"))).scalar_one() == 0

        assert _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION).returncode == 0
        assert _alembic(disposable_database, "upgrade", "head").returncode == 0

        async with engine.connect() as conn:
            after = (
                await conn.execute(
                    text("SELECT wa_opted_out, wa_opt_out_reason FROM clients WHERE altegio_client_id = 918273")
                )
            ).one()
            assert after[0] is True and after[1] == "wa:stop"
            assert (await conn.execute(text("SELECT count(*) FROM whatsapp_events"))).scalar_one() == 1
    finally:
        await engine.dispose()


async def test_a_destructive_downgrade_refuses_and_keeps_every_refusal(disposable_database: str):
    """§46.9 — a refusal cannot be deleted to make a rollback possible.

    The downgrade is attempted with one consent decision on disk. It must fail,
    and — the half that matters more — it must fail BEFORE any DDL, so the
    operator who is told the rollback is unsafe finds the refusal still there and
    the schema still whole.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO whatsapp_consent_suppressions ("
                    " wa_address, state, consent_version, decision_command, decision_source,"
                    " decision_authenticity, decided_observed_at, opted_out_at, last_stop_observed_at)"
                    " VALUES ('+4915177994001', 'opted_out', 1, 'stop', 'inbound_command',"
                    " 'unverified', now(), now(), now())"
                )
            )

        attempt = _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION)
        assert attempt.returncode != 0
        assert "downgrade refused before DDL" in (attempt.stderr + attempt.stdout)

        # Nothing was dropped, and nothing was lost.
        assert await _tables(disposable_database, CONSENT_TABLES) == set(CONSENT_TABLES)
        async with engine.connect() as conn:
            assert (
                await conn.execute(text("SELECT count(*) FROM whatsapp_consent_suppressions WHERE state = 'opted_out'"))
            ).scalar_one() == 1
        current = _alembic(disposable_database, "current")
        assert "(head)" in current.stdout
    finally:
        await engine.dispose()


async def test_a_destructive_downgrade_refuses_while_delivery_evidence_exists(disposable_database: str):
    """The same refusal for a proven failure fact, which is also undeletable."""
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_production_delivery_failures ("
                    " fact_key, source, reason_code, match_state, failure_class, authenticity,"
                    " provider_message_id, observed_at)"
                    " VALUES (:key, 'status_callback', 'meta_callback_failed', 'unmatched',"
                    " 'unknown', 'unverified', 'wamid.KEEPME', now())"
                ),
                {"key": "c" * 64},
            )

        attempt = _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION)
        assert attempt.returncode != 0
        assert await _tables(disposable_database, CONSENT_TABLES) == set(CONSENT_TABLES)
        async with engine.connect() as conn:
            assert (
                await conn.execute(text("SELECT count(*) FROM easyweek_voucher_production_delivery_failures"))
            ).scalar_one() == 1
    finally:
        await engine.dispose()


async def test_the_orm_and_the_migration_agree_about_the_new_schema(disposable_database: str):
    """What Alembic builds is what the ORM expects, column for column.

    Compared against the ORM's own metadata rather than a hand-written list, so
    a column added to a model without a migration — the drift this project has
    already been bitten by — fails here instead of in a deploy.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    from altegio_bot.models.models import Base

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    engine = create_async_engine(disposable_database)
    try:
        for table_name in (*CONSENT_TABLES, "whatsapp_events", "easyweek_voucher_production_batch_attempts"):
            expected = {column.name for column in Base.metadata.tables[table_name].columns}
            actual = await _columns(disposable_database, table_name)
            assert expected <= actual, (table_name, sorted(expected - actual))
    finally:
        await engine.dispose()


async def test_the_consent_constraints_and_dedupe_hold_in_postgres(disposable_database: str):
    """The uniqueness and CHECK rules are the database's, not the writer's."""
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO whatsapp_consent_suppressions ("
                    " wa_address, state, consent_version, decision_command, decision_source,"
                    " decision_authenticity, decided_observed_at, opted_out_at)"
                    " VALUES ('+4915177994002', 'opted_out', 1, 'stop', 'inbound_command',"
                    " 'unverified', now(), now())"
                )
            )
        # One row per address, table-wide.
        with pytest.raises(Exception):
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO whatsapp_consent_suppressions ("
                        " wa_address, state, consent_version, decision_command, decision_source,"
                        " decision_authenticity, decided_observed_at, opted_out_at)"
                        " VALUES ('+4915177994002', 'opted_out', 1, 'stop', 'inbound_command',"
                        " 'unverified', now(), now())"
                    )
                )
        # An unverified opt-in is unrepresentable, not merely unreachable.
        with pytest.raises(Exception):
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO whatsapp_consent_suppressions ("
                        " wa_address, state, consent_version, decision_command, decision_source,"
                        " decision_authenticity, decided_observed_at, decided_event_at)"
                        " VALUES ('+4915177994003', 'opted_in', 1, 'start', 'inbound_command',"
                        " 'unverified', now(), now())"
                    )
                )
        # And a confirmed failure fact must name the slot it is about.
        with pytest.raises(Exception):
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO easyweek_voucher_production_delivery_failures ("
                        " fact_key, source, reason_code, match_state, failure_class, authenticity,"
                        " provider_message_id, observed_at)"
                        " VALUES (:key, 'status_callback', 'meta_callback_failed', 'confirmed',"
                        " 'unknown', 'unverified', 'wamid.NOSLOT', now())"
                    ),
                    {"key": "d" * 64},
                )
    finally:
        await engine.dispose()


async def test_concurrent_first_refusals_for_one_address_do_not_race(session_maker):
    """PostgreSQL 16, two transactions, one address, no row to lock yet.

    The absent-row race §46.4.6 names. Both writers decide to create the first
    refusal for an address nobody has a row for; the advisory address lock makes
    the read-decide-write sequence atomic, so one applies and the other reads
    what it wrote instead of failing on the unique index.
    """
    from altegio_bot.models.models import WhatsAppConsentSuppression
    from altegio_bot.whatsapp_consent import ProjectedRefusal, apply_projected_refusal

    address = "+4915177994010"
    moment = utcnow()

    async def _write(origin: str) -> str:
        async with session_maker() as session:
            async with session.begin():
                outcome = await apply_projected_refusal(
                    session,
                    ProjectedRefusal(wa_address=address, origin=origin, observed_at=moment),
                )
                return outcome.resolution

    results = await asyncio.gather(*[_write(f"writer-{index}") for index in range(4)])

    assert results.count("applied") == 1
    assert results.count("noop") == 3
    async with session_maker() as session:
        rows = (
            (
                await session.execute(
                    select(WhatsAppConsentSuppression).where(WhatsAppConsentSuppression.wa_address == address)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].state == "opted_out"


async def test_a_refusal_racing_the_final_send_guard_has_one_checkable_order(session_maker):
    """§46.4.6 — the guard and the refusal serialise on the same address lock.

    The guard holds its transaction open; a refusal for the same address then
    tries to commit. It cannot overtake the guard, and once the guard's
    transaction ends the refusal lands — after which the fingerprint the guard
    handed out no longer matches, so a claim that re-checks it refuses.
    """
    from altegio_bot.whatsapp_consent import (
        GUARD_CONSENT_MOVED,
        ProjectedRefusal,
        apply_projected_refusal,
        final_send_guard,
        recheck_consent_boundary,
    )

    address = "+4915177994011"
    moment = utcnow()
    guard_taken = asyncio.Event()
    release_guard = asyncio.Event()
    fingerprints: list[str] = []

    async def _guard() -> None:
        async with session_maker() as session:
            async with session.begin():
                verdict = await final_send_guard(session, phone=address)
                assert verdict.allowed is True
                fingerprints.append(verdict.fingerprint)
                guard_taken.set()
                await release_guard.wait()

    async def _refuse() -> str:
        await guard_taken.wait()
        release_guard.set()
        async with session_maker() as session:
            async with session.begin():
                outcome = await apply_projected_refusal(
                    session,
                    ProjectedRefusal(wa_address=address, origin="live", observed_at=moment),
                )
                return outcome.resolution

    _, resolution = await asyncio.gather(_guard(), _refuse())

    assert resolution == "applied"
    async with session_maker() as session:
        recheck = await recheck_consent_boundary(session, phone=address, expected_fingerprint=fingerprints[0])
        assert recheck.allowed is False
        # It is the refusal itself that stops the claim, not merely a moved
        # fingerprint: the re-check asks the same question a fresh guard asks.
        assert recheck.reason != GUARD_CONSENT_MOVED
        assert (await final_send_guard(session, phone=address)).allowed is False


# ===========================================================================
# Review fix 8 — the downgrade protects the provenance in OLD tables too
# ===========================================================================


async def _seed_one_attempt(db_url: str) -> tuple[int, int]:
    """One batch, one item and one attempt, through the §42 raw-SQL seeder.

    Imported rather than hand-rolled: that module owns the exact column list
    these tables require, and a second copy of it here would drift from the
    schema the migrations actually produce.
    """
    from altegio_bot.tests.test_easyweek_voucher_production_migration import (
        _add_item,
        _execute,
        _fetch,
        _seed_batch,
    )

    run_id, recipient_ids, batch_id = await _seed_batch(db_url, recipient_count=1)
    customer = str(uuid_module.UUID(int=0xC0FFEE))
    await _add_item(
        db_url,
        batch_id=batch_id,
        batch_count=1,
        slot=1,
        run_id=run_id,
        recipient_id=recipient_ids[0],
        customer=customer,
        marker="marker-route-1",
    )
    item_id = (await _fetch(db_url, "SELECT id FROM easyweek_voucher_production_batch_items ORDER BY id DESC LIMIT 1"))[
        0
    ][0]
    await _execute(
        db_url,
        "INSERT INTO easyweek_voucher_production_batch_attempts ("
        " item_id, intent_uuid, outcome, template_code, meta_template_name, template_language,"
        " campaign_recipient_id, batch_id, slot, claimed_at)"
        " VALUES (:item, :intent, 'claimed', 'new_client_voucher_10eur_v2', 'meta_template', 'de',"
        " :recipient, :batch, 1, now())",
        {
            "item": item_id,
            "intent": str(uuid_module.uuid4()),
            "recipient": recipient_ids[0],
            "batch": batch_id,
        },
    )
    return int(batch_id), int(item_id)


async def test_a_downgrade_refuses_while_a_frozen_send_route_exists(disposable_database: str):
    """§46.3 — the route of a real send is not traded for a rollback.

    The reproduction from the review: a mailing went out, its route digests were
    frozen with the one attempt it got, and no consent row or failure fact
    exists yet. The downgrade used to pass straight through that state and drop
    both digest columns, which cannot be recomputed — the sender configuration
    and the client's number are both mutable, which is why they were frozen in
    the first place — so a re-upgrade would bring them back as NULL and every
    later callback about those sends would be unprovable.
    """
    from altegio_bot.tests.test_easyweek_voucher_production_migration import _execute, _fetch

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    _batch_id, item_id = await _seed_one_attempt(disposable_database)
    await _execute(
        disposable_database,
        "UPDATE easyweek_voucher_production_batch_attempts"
        " SET sender_phone_number_id_digest = :sender, recipient_digest = :recipient"
        " WHERE item_id = :item",
        {"sender": "a" * 64, "recipient": "b" * 64, "item": item_id},
    )

    attempt = _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION)
    assert attempt.returncode != 0
    assert "downgrade refused before DDL" in (attempt.stderr + attempt.stdout)

    # Nothing dropped, nothing lost, still at head.
    columns = await _columns(disposable_database, "easyweek_voucher_production_batch_attempts")
    assert {"sender_phone_number_id_digest", "recipient_digest"} <= columns
    rows = await _fetch(
        disposable_database,
        "SELECT sender_phone_number_id_digest, recipient_digest"
        " FROM easyweek_voucher_production_batch_attempts WHERE item_id = :item",
        {"item": item_id},
    )
    assert rows == [("a" * 64, "b" * 64)]
    assert await _tables(disposable_database, CONSENT_TABLES) == set(CONSENT_TABLES)
    assert "(head)" in _alembic(disposable_database, "current").stdout


async def test_a_downgrade_refuses_while_a_recorded_authenticity_exists(disposable_database: str):
    """The same for a status-only delivery, whose proof lives on the event.

    An event carrying no messages has no receipt to record authenticity on, so
    ``whatsapp_events.inbound_authenticity`` is the only place the fact that a
    signature WAS checked survives — the header itself is stripped before the
    row is written. Dropping the column turns every one of those into
    "unknown", and §46.4.7 reads unknown as unverified for good.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0

    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO whatsapp_events (dedupe_key, status, query, headers, payload,"
                    " inbound_authenticity)"
                    " VALUES ('wa:status-only-1', 'processed', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,"
                    " 'signature_verified')"
                )
            )

        attempt = _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION)
        assert attempt.returncode != 0
        assert "downgrade refused before DDL" in (attempt.stderr + attempt.stdout)

        assert "inbound_authenticity" in await _columns(disposable_database, "whatsapp_events")
        async with engine.connect() as conn:
            stored = (
                await conn.execute(
                    text("SELECT inbound_authenticity FROM whatsapp_events WHERE dedupe_key = 'wa:status-only-1'")
                )
            ).scalar_one()
        assert stored == "signature_verified"
        assert "(head)" in _alembic(disposable_database, "current").stdout
    finally:
        await engine.dispose()


async def test_a_downgrade_cannot_overtake_a_route_being_frozen(disposable_database: str):
    """The check and the DROP are one atomic decision, under the table lock.

    An acceptance freezes a route in an open transaction while the downgrade
    runs. Without the ACCESS EXCLUSIVE lock on that old table the downgrade
    could count zero digests and drop the columns a moment before the insert
    commits — the race the new tables were already protected from, on data that
    is exactly as unrecoverable. So the downgrade must WAIT (asserted here) and
    then refuse on what it finds.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    from altegio_bot.tests.test_easyweek_voucher_production_migration import _fetch

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    _batch_id, item_id = await _seed_one_attempt(disposable_database)

    engine = create_async_engine(disposable_database)
    blocked: list[bool] = []
    try:
        async with engine.connect() as conn:
            transaction = await conn.begin()
            await conn.execute(
                text(
                    "UPDATE easyweek_voucher_production_batch_attempts"
                    " SET sender_phone_number_id_digest = :sender, recipient_digest = :recipient"
                    " WHERE item_id = :item"
                ),
                {"sender": "c" * 64, "recipient": "d" * 64, "item": item_id},
            )

            task = asyncio.create_task(
                asyncio.to_thread(_alembic, disposable_database, "downgrade", CONSENT_PARENT_REVISION)
            )
            done, _pending = await asyncio.wait({task}, timeout=3.0)
            blocked.append(not done)
            await transaction.commit()
            attempt = await task

        assert blocked == [True], "the downgrade must wait for the writer, not read around it"
        assert attempt.returncode != 0
        assert "downgrade refused before DDL" in (attempt.stderr + attempt.stdout)
        rows = await _fetch(
            disposable_database,
            "SELECT sender_phone_number_id_digest FROM easyweek_voucher_production_batch_attempts"
            " WHERE item_id = :item",
            {"item": item_id},
        )
        assert rows == [("c" * 64,)]
    finally:
        await engine.dispose()


async def test_an_empty_database_still_downgrades_and_re_upgrades(disposable_database: str):
    """The guard is about data, not about the migration being irreversible.

    With no consent row, no failure fact, no frozen route and no recorded
    authenticity, the rollback is exactly as available as it was — and the
    re-upgrade leaves one head.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    await _seed_one_attempt(disposable_database)

    assert _alembic(disposable_database, "downgrade", CONSENT_PARENT_REVISION).returncode == 0
    assert await _tables(disposable_database, CONSENT_TABLES) == set()
    columns = await _columns(disposable_database, "easyweek_voucher_production_batch_attempts")
    assert "sender_phone_number_id_digest" not in columns
    assert "recipient_digest" not in columns

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, CONSENT_TABLES) == set(CONSENT_TABLES)
    current = _alembic(disposable_database, "current")
    assert "(head)" in current.stdout
    heads = [line for line in _alembic(_repo_root(), "heads").stdout.splitlines() if "(head)" in line]
    assert len(heads) == 1


# ---------------------------------------------------------------------------
# §46.7–46.8 — the follow-up SEND ledger: concurrency and migration (PR-22.3)
# ---------------------------------------------------------------------------
# The property defended here is the one that cannot be apologised for
# afterwards: a person who already received the one extra message must never
# receive a second one. So every test below ends in a count — how many claims
# were granted, how many POSTs were made — rather than in a status.

FOLLOWUP_SEND_PARENT_REVISION = "b7d41f9c0a52"

FOLLOWUP_SEND_TABLES = (
    "easyweek_voucher_followup_approvals",
    "easyweek_voucher_followup_runs",
    "easyweek_voucher_followup_sends",
    "easyweek_voucher_followup_callbacks",
)


@pytest.fixture
def followup_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """The follow-up's own fence open, the primary mailing's pinned closed."""
    from pydantic import SecretStr

    from altegio_bot.settings import settings
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import location_map

    monkeypatch.setattr(settings, "easyweek_voucher_followup_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_followup_executor_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)
    monkeypatch.setattr(settings, "ops_secret", "synthetic-ops-signing-secret", raising=False)
    monkeypatch.setattr(settings, "ops_pass", "synthetic-ops-password", raising=False)


async def _followup_ready(session_maker, *, slots: list[dict[str, Any]] | None = None) -> tuple[int, Any]:
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import build_preview, seed_ready_batch
    from altegio_bot.tests.test_easyweek_voucher_followup_preview import FakeHistoryReader

    batch_id = await seed_ready_batch(session_maker, slots=slots)
    reader = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    return batch_id, reader


async def test_two_concurrent_confirmations_of_one_offer_produce_one_run(session_maker, followup_environment) -> None:
    """The double click, raced for real on PostgreSQL.

    Both coroutines read a pending approval and both try to consume it. The
    approval's row lock plus the UNIQUE on ``approval_id`` decide it: one creates
    the run, the other is handed the same run back, and the ledger holds one.
    """
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import confirm, offer

    batch_id, _reader = await _followup_ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    assert plan.ready

    first, second = await asyncio.gather(
        confirm(session_maker, plan=plan),
        confirm(session_maker, plan=plan),
        return_exceptions=True,
    )
    outcomes = [value for value in (first, second) if not isinstance(value, BaseException)]
    assert len(outcomes) == 2, (first, second)
    assert sum(1 for value in outcomes if value.created) == 1
    runs = [value.run.id for value in outcomes if value.run is not None]
    assert len(set(runs)) == 1
    async with session_maker() as session:
        count = (await session.execute(text("SELECT count(*) FROM easyweek_voucher_followup_runs"))).scalar_one()
    assert int(count) == 1


async def test_two_executors_racing_one_row_grant_exactly_one_attempt(session_maker, followup_environment) -> None:
    """Two claims for the same recipient, concurrently. One attempt, never two.

    Both tasks hold a valid lease for the same run — which the supported
    topology does not produce and a mis-deploy can — and both re-prove consent
    and ask for the row. The row lock serialises them and the second finds the
    attempt already accounted for.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as send_ids
    from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as followup_ledger
    from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import consent_snapshot
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import SENDER_DIGEST
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
        confirm,
        offer,
        person_phone,
        person_uuid,
        rows_of,
    )
    from altegio_bot.whatsapp_consent import recipient_route_digest

    batch_id, _reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await followup_ledger.claim_next_run(session_maker, owner="executor-a")
    assert claimed is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    send_id = int(rows[1].id)

    async with session_maker() as session:
        snapshot = await consent_snapshot(session, phone=person_phone(1), easyweek_customer_uuid=person_uuid(1))

    async def claim() -> Any:
        return await followup_ledger.claim_send(
            session_maker,
            run_id=outcome.run.id,
            send_id=send_id,
            owner="executor-a",
            address=person_phone(1),
            easyweek_customer_uuid=person_uuid(1),
            expected_consent_fingerprint=snapshot.fingerprint,
            sender_digest=SENDER_DIGEST,
            recipient_digest=str(recipient_route_digest(person_phone(1))),
            source_batch_id=batch_id,
        )

    first, second = await asyncio.gather(claim(), claim())
    granted = [value for value in (first, second) if value.granted]
    assert len(granted) == 1
    refused = [value for value in (first, second) if not value.granted]
    assert refused[0].reason == send_ids.ATTEMPT_ALREADY_USED
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    assert int(rows[1].attempt_count) == 1


async def test_a_stop_racing_the_boundary_never_lets_two_things_happen(session_maker, followup_environment) -> None:
    """A stop and a claim, concurrently. Either zero attempts or one, never both.

    The ordering is single and checkable because both take the run's row lock:
    whichever commits first is seen by the other. What must never happen is a
    tombstone AND a spent attempt for the same recipient, which would mean the
    stop was recorded as having prevented a message that went out.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as followup_ledger
    from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import consent_snapshot
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import SENDER_DIGEST
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
        confirm,
        offer,
        person_phone,
        person_uuid,
        rows_of,
    )
    from altegio_bot.whatsapp_consent import recipient_route_digest

    batch_id, _reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    claimed = await followup_ledger.claim_next_run(session_maker, owner="executor-a")
    assert claimed is not None
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    send_id = int(rows[1].id)
    async with session_maker() as session:
        snapshot = await consent_snapshot(session, phone=person_phone(1), easyweek_customer_uuid=person_uuid(1))

    claim, stop = await asyncio.gather(
        followup_ledger.claim_send(
            session_maker,
            run_id=outcome.run.id,
            send_id=send_id,
            owner="executor-a",
            address=person_phone(1),
            easyweek_customer_uuid=person_uuid(1),
            expected_consent_fingerprint=snapshot.fingerprint,
            sender_digest=SENDER_DIGEST,
            recipient_digest=str(recipient_route_digest(person_phone(1))),
            source_batch_id=batch_id,
        ),
        followup_ledger.request_terminal_stop(session_maker, run_id=outcome.run.id, requested_by="operator"),
    )
    assert stop.applied is True
    rows = await rows_of(session_maker, run_id=outcome.run.id)
    row = rows[1]
    if claim.granted:
        # The claim won: the attempt is spent and the stop found nothing left to
        # tombstone. The stop is still durable for every other row.
        assert int(row.attempt_count) == 1
        assert stop.tombstoned == 0
    else:
        # The stop won: zero attempts and a durable tombstone.
        assert int(row.attempt_count) == 0
        assert stop.tombstoned == 1
        assert str(row.status) == "stopped"


async def test_a_stop_that_commits_before_the_boundary_yields_zero_meta_requests(
    session_maker, followup_environment
) -> None:
    """The one-sided guarantee of §46.4.6, with the ordering forced rather than raced.

    The stop is committed while the executor is between rows, so it is durable
    BEFORE the next boundary. The counted sender proves the consequence.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch as followup_dispatch
    from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as followup_ledger
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
        CountingSender,
        StubTransports,
        confirm,
        offer,
    )

    batch_id, reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}, {"state": "delivered"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    await followup_ledger.request_terminal_stop(session_maker, run_id=outcome.run.id, requested_by="operator")
    # The stop finished the queued run, so the executor finds nothing at all.
    assert await followup_ledger.claim_next_run(session_maker, owner="executor-a") is None
    sender = CountingSender()
    report = await followup_dispatch.execute_run(
        session_maker,
        run=outcome.run,
        owner="executor-a",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0
    assert report.attempted == 0


async def test_the_followup_send_ledger_upgrades_downgrades_and_re_upgrades_when_empty(
    disposable_database: str,
) -> None:
    """The four tables appear, disappear and reappear, and the head stays single.

    Addressed by REVISION rather than by "-1", for the reason this file's other
    migration tests already learned: a later revision makes one step back mean
    something else.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, FOLLOWUP_SEND_TABLES) == set(FOLLOWUP_SEND_TABLES)

    assert _alembic(disposable_database, "downgrade", FOLLOWUP_SEND_PARENT_REVISION).returncode == 0
    assert await _tables(disposable_database, FOLLOWUP_SEND_TABLES) == set()
    # Everything the follow-up refers to is untouched by the rollback.
    historical = (
        "easyweek_voucher_production_batches",
        "easyweek_voucher_production_batch_items",
        "easyweek_voucher_production_batch_attempts",
        "easyweek_voucher_followup_previews",
        "easyweek_voucher_followup_preview_rows",
        "whatsapp_consent_suppressions",
    )
    assert await _tables(disposable_database, historical) == set(historical)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert await _tables(disposable_database, FOLLOWUP_SEND_TABLES) == set(FOLLOWUP_SEND_TABLES)
    current = _alembic(disposable_database, "current")
    assert "(head)" in current.stdout
    heads = [line for line in _alembic(_repo_root(), "heads").stdout.splitlines() if "(head)" in line]
    assert len(heads) == 1


async def _seed_followup_send_row(database_url: str, *, status: str) -> None:
    """One follow-up row in a given state, written in SQL rather than the ORM.

    Spelled out in SQL for the same reason this file's other migration seeds
    are: the point is that a REAL row blocks the rollback, and a row built from
    today's model would prove only that today's model round-trips.

    The two foreign keys to the approval and the preview are dropped first, and
    said out loud rather than worked around: this seed is about the SEND row's
    guard, and satisfying the whole chain — an approval, a preview, a batch, an
    item — would make the test about the fixtures instead.
    """
    from datetime import datetime, timezone

    from sqlalchemy.ext.asyncio import create_async_engine

    attempted = status != "stopped"
    moment = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("ALTER TABLE easyweek_voucher_followup_runs ALTER COLUMN approval_id DROP NOT NULL")
            )
            await conn.execute(text("ALTER TABLE easyweek_voucher_followup_runs ALTER COLUMN preview_id DROP NOT NULL"))
            await conn.execute(
                text("ALTER TABLE easyweek_voucher_followup_sends ALTER COLUMN source_item_id DROP NOT NULL")
            )
            run_id = (
                await conn.execute(
                    text(
                        "INSERT INTO easyweek_voucher_followup_runs ("
                        " approval_id, preview_id, preview_version, source_batch_id, campaign_run_id,"
                        " company_id, composition_digest, principal, session_fingerprint,"
                        " identification_limit, status, planned_count, queued_at)"
                        " VALUES (NULL, NULL, 1, 1, 1, 322579, :digest, 'ops', :fp, 'shared_ops_account',"
                        " 'completed', 1, :moment) RETURNING id"
                    ),
                    {"digest": "d" * 64, "fp": "a" * 64, "moment": moment},
                )
            ).scalar_one()
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_followup_sends ("
                    " run_id, source_batch_id, source_item_id, source_slot, campaign_recipient_id,"
                    " easyweek_customer_uuid, source_entitlement_digest, group_code, status, attempt_count,"
                    " intent_uuid, claimed_at, stopped_at, sender_digest, recipient_digest, template_code,"
                    " meta_template_name, template_language, template_body_digest)"
                    " VALUES (:run, 1, NULL, 1, 1, :uuid, :ent, 'delivered_unread', :status, :attempts,"
                    " :intent, :claimed, :stopped, :sender, :recipient,"
                    " 'new_client_voucher_followup_v1', 'kitilash_ka_new_client_voucher_followup_v1',"
                    " 'de', :body)"
                ),
                {
                    "run": run_id,
                    "uuid": "00000001-1111-4111-8111-111111111111",
                    "ent": "e" * 64,
                    "status": status,
                    "attempts": 1 if attempted else 0,
                    "intent": str(uuid_module.uuid4()) if attempted else None,
                    "claimed": moment if attempted else None,
                    "stopped": None if attempted else moment,
                    "sender": ("s" * 64) if attempted else None,
                    "recipient": ("r" * 64) if attempted else None,
                    "body": "b" * 64,
                },
            )
    finally:
        await engine.dispose()


async def test_a_destructive_downgrade_refuses_while_a_spent_attempt_exists(
    disposable_database: str,
) -> None:
    """§46.9: the facts whose loss would re-open an attempt are not droppable.

    A spent attempt is the only record that stops a new run claiming the same
    recipient again, so the rollback refuses BEFORE any DDL and the row is still
    there afterwards.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    await _seed_followup_send_row(disposable_database, status="unknown")

    attempt = _alembic(disposable_database, "downgrade", FOLLOWUP_SEND_PARENT_REVISION)
    assert attempt.returncode != 0
    assert "spent follow-up attempt" in (attempt.stderr + attempt.stdout)
    assert await _tables(disposable_database, FOLLOWUP_SEND_TABLES) == set(FOLLOWUP_SEND_TABLES)
    from altegio_bot.tests.test_easyweek_voucher_production_migration import _fetch

    rows = await _fetch(disposable_database, "SELECT status, attempt_count FROM easyweek_voucher_followup_sends")
    assert rows == [("unknown", 1)]


async def test_a_destructive_downgrade_refuses_while_a_terminal_tombstone_exists(
    disposable_database: str,
) -> None:
    """A tombstone spends nothing and is just as undroppable.

    Dropping it would let a new run send to somebody an operator stopped for
    good, which §46.7 forbids whatever the attempt count says.
    """
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    await _seed_followup_send_row(disposable_database, status="stopped")

    attempt = _alembic(disposable_database, "downgrade", FOLLOWUP_SEND_PARENT_REVISION)
    assert attempt.returncode != 0
    assert "terminal stop tombstone" in (attempt.stderr + attempt.stdout)
    from altegio_bot.tests.test_easyweek_voucher_production_migration import _fetch

    rows = await _fetch(disposable_database, "SELECT status, attempt_count FROM easyweek_voucher_followup_sends")
    assert rows == [("stopped", 0)]


# ---------------------------------------------------------------------------
# §46.8 review round 1 — competing approvals, and a stop that cannot be lost
# ---------------------------------------------------------------------------


async def test_two_different_approvals_of_one_batch_create_at_most_one_active_run(
    session_maker, followup_environment
) -> None:
    """Two offers, two confirmations, one run — decided by a lock, not a read.

    Both confirmations are started while a third transaction holds the ORIGINAL
    batch header, so neither can run its admission check until that is released:
    the barrier is asserted rather than hoped for. On the reviewed commit both
    passed an unserialised ``busy`` SELECT and two queued runs were created with
    the same recipients, which is also what later made a terminal stop abort.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as send_ids
    from altegio_bot.models.models import EasyWeekVoucherProductionBatch
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import confirm, offer

    batch_id, _reader = await _followup_ready(session_maker)
    first = await offer(session_maker, batch_id=batch_id)
    second = await offer(session_maker, batch_id=batch_id)
    assert first.ready and second.ready
    assert first.approval is not None and second.approval is not None
    assert first.approval.id != second.approval.id

    blocked: list[bool] = []
    async with session_maker() as holder:
        transaction = await holder.begin()
        await holder.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)

        tasks = [
            asyncio.create_task(confirm(session_maker, plan=first)),
            asyncio.create_task(confirm(session_maker, plan=second)),
        ]
        for _ in range(20):
            await asyncio.sleep(0.01)
            if all(task.done() for task in tasks):
                break
        blocked.append(not any(task.done() for task in tasks))
        await transaction.commit()
        outcomes = await asyncio.gather(*tasks)

    assert blocked == [True], "both confirmations must wait for the source batch header"
    created = [outcome for outcome in outcomes if outcome.created]
    refused = [outcome for outcome in outcomes if not outcome.created]
    assert len(created) == 1
    assert len(refused) == 1
    assert refused[0].reasons == (send_ids.RUN_ALREADY_ACTIVE,)
    async with session_maker() as session:
        count = (await session.execute(text("SELECT count(*) FROM easyweek_voucher_followup_runs"))).scalar_one()
    assert int(count) == 1


async def test_repeating_one_approval_returns_the_same_run(session_maker, followup_environment) -> None:
    """The loser of a double click is not an error, and not a second run."""
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import confirm, offer

    batch_id, _reader = await _followup_ready(session_maker)
    plan = await offer(session_maker, batch_id=batch_id)
    first, second = await asyncio.gather(
        confirm(session_maker, plan=plan),
        confirm(session_maker, plan=plan),
    )
    runs = {outcome.run.id for outcome in (first, second) if outcome.run is not None}
    assert len(runs) == 1
    assert sum(1 for outcome in (first, second) if outcome.created) == 1


async def test_a_terminal_stop_survives_a_recipient_another_run_already_accounts_for(
    session_maker, followup_environment
) -> None:
    """A unique-index conflict must never be able to cancel an operator's stop.

    On the reviewed commit the stop tombstoned every planned row in one
    statement set, so a recipient another run had already tombstoned or sent to
    raised a partial-index violation — and the whole transaction rolled back,
    leaving ``stop_requested_at`` NULL while the operator had been told the run
    was stopped.

    The admission fixed above makes two ACTIVE runs impossible, so this state is
    now only reachable by editing a row by hand — which is exactly why the stop
    has to survive it rather than rely on the admission.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as send_ids
    from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as followup_ledger
    from altegio_bot.models.models import EasyWeekVoucherFollowupSend
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
        CountingSender,
        build_preview,
        confirm,
        offer,
        rows_of,
    )

    batch_id, reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}])

    # Run one: confirmed, then stopped before it started. Its row is a tombstone.
    first = await offer(session_maker, batch_id=batch_id)
    first_run = await confirm(session_maker, plan=first)
    assert first_run.run is not None
    stop_one = await followup_ledger.request_terminal_stop(
        session_maker, run_id=first_run.run.id, requested_by="operator"
    )
    assert stop_one.applied and stop_one.tombstoned == 1

    # Run two: confirmed afterwards. Its row is correctly SKIPPED, because the
    # tombstone already accounts for this recipient.
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    second = await offer(session_maker, batch_id=batch_id)
    second_run = await confirm(session_maker, plan=second)
    assert second_run.run is not None
    rows = await rows_of(session_maker, run_id=second_run.run.id)
    assert rows[1].reason_code == send_ids.TERMINALLY_STOPPED

    # Forced back to a reservation, which is the state the admission no longer
    # allows the application to produce.
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherFollowupSend, int(rows[1].id))
            assert row is not None
            row.status = "planned"
            row.reason_code = None

    stop_two = await followup_ledger.request_terminal_stop(
        session_maker, run_id=second_run.run.id, requested_by="operator"
    )
    assert stop_two.applied is True
    assert stop_two.tombstoned == 0
    assert stop_two.already_accounted == 1

    # The stop is durable, which is the whole point.
    stored = await followup_ledger.load_run(session_maker, run_id=second_run.run.id)
    assert stored is not None
    assert stored.stop_requested_at is not None
    assert stored.status == "stopped"

    # And no claim can reach that recipient afterwards.
    sender = CountingSender()
    assert await followup_ledger.claim_next_run(session_maker, owner="executor-after-stop") is None
    rows = await rows_of(session_maker, run_id=second_run.run.id)
    assert rows[1].status in {"skipped", "stopped"}
    assert int(rows[1].attempt_count) == 0
    assert sender.count == 0


async def test_a_stopped_run_is_not_bypassed_by_a_later_confirmation(session_maker, followup_environment) -> None:
    """§46.7: a new preview, a new offer and a new run reach nobody who was stopped."""
    from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch as followup_dispatch
    from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as followup_ledger
    from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
        CountingSender,
        StubTransports,
        build_preview,
        confirm,
        offer,
    )

    batch_id, reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}])
    first = await offer(session_maker, batch_id=batch_id)
    first_run = await confirm(session_maker, plan=first)
    assert first_run.run is not None
    await followup_ledger.request_terminal_stop(session_maker, run_id=first_run.run.id, requested_by="operator")

    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    again = await offer(session_maker, batch_id=batch_id)
    assert again.approval is not None and again.approval.approved_recipient_count == 0
    later = await confirm(session_maker, plan=again)
    assert later.run is not None and later.planned == 0

    claimed = await followup_ledger.claim_next_run(session_maker, owner="executor-bypass")
    assert claimed is not None
    sender = CountingSender()
    report = await followup_dispatch.execute_run(
        session_maker,
        run=claimed,
        owner="executor-bypass",
        transports=StubTransports(reader=reader, sender=sender),
    )
    assert sender.count == 0
    assert report.attempted == 0


FOLLOWUP_OFFER_COLUMN_PARENT = "c3f8a95d2e14"


async def test_the_offer_identity_column_upgrades_downgrades_and_re_upgrades(
    disposable_database: str,
) -> None:
    """One nullable column, added and removed, with the head staying single."""
    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert "offered_source_item_ids" in await _columns(disposable_database, "easyweek_voucher_followup_approvals")

    assert _alembic(disposable_database, "downgrade", FOLLOWUP_OFFER_COLUMN_PARENT).returncode == 0
    assert "offered_source_item_ids" not in await _columns(disposable_database, "easyweek_voucher_followup_approvals")
    # The ledger itself is untouched by this rollback.
    assert await _tables(disposable_database, FOLLOWUP_SEND_TABLES) == set(FOLLOWUP_SEND_TABLES)

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    assert "offered_source_item_ids" in await _columns(disposable_database, "easyweek_voucher_followup_approvals")
    heads = [line for line in _alembic(_repo_root(), "heads").stdout.splitlines() if "(head)" in line]
    assert len(heads) == 1


async def test_dropping_the_offer_identities_refuses_while_a_pending_offer_carries_them(
    disposable_database: str,
) -> None:
    """§46.8: an offer whose identities are dropped could reach people it never covered.

    So the rollback refuses before any DDL while such an offer is still
    pending. A consumed one is history — its run and its rows already exist, and
    the single-attempt key is what protects those.
    """
    from datetime import datetime, timezone

    from sqlalchemy.ext.asyncio import create_async_engine

    assert _alembic(disposable_database, "upgrade", "head").returncode == 0
    moment = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
    engine = create_async_engine(disposable_database)
    try:
        async with engine.begin() as conn:
            # The approval's own foreign key is to a preview, which this seed has
            # no business building: the guard is about the column, so the key is
            # relaxed for this one insert exactly as the seeds above do.
            await conn.execute(
                text("ALTER TABLE easyweek_voucher_followup_approvals ALTER COLUMN preview_id DROP NOT NULL")
            )
            await conn.execute(
                text(
                    "INSERT INTO easyweek_voucher_followup_approvals ("
                    " action, preview_id, preview_version, source_batch_id, campaign_run_id, company_id,"
                    " location_uuid, source_request_schema_version, source_frozen_digest,"
                    " source_stop_generation, composition_digest, selected_groups, min_interval_seconds,"
                    " approved_recipient_count, recipient_digest, offered_source_item_ids, template_code,"
                    " meta_template_name, template_language, template_category, template_parameter_order,"
                    " template_body_digest, contract_version, failure_policy_version, sender_digest,"
                    " principal, session_fingerprint, identification_limit, nonce, plan_signature,"
                    " issued_at, expires_at, status)"
                    " VALUES ('followup_send', NULL, 1, 1, 1, 322579, :loc, '4', :frozen, 0, :comp,"
                    " '[\"delivered_unread\"]'::jsonb, 1209600, 1, :recip, '[1]'::jsonb,"
                    " 'new_client_voucher_followup_v1', 'kitilash_ka_new_client_voucher_followup_v1',"
                    " 'de', 'MARKETING', '[\"client_name\", \"booking_link\"]'::jsonb, :body,"
                    " 'followup-send-v1', 'followup-failure-policy-v1', :sender, 'ops', :fp,"
                    " 'shared_ops_account', :nonce, :sig, :issued, :expires, 'pending')"
                ),
                {
                    "loc": "8395fab6-7ee8-4702-88d9-fd78f92539c3",
                    "frozen": "f" * 64,
                    "comp": "c" * 64,
                    "recip": "r" * 64,
                    "body": "b" * 64,
                    "sender": "s" * 64,
                    "fp": "a" * 64,
                    "nonce": "n" * 32,
                    "sig": "g" * 64,
                    "issued": moment,
                    "expires": moment + timedelta(minutes=10),
                },
            )
    finally:
        await engine.dispose()

    attempt = _alembic(disposable_database, "downgrade", FOLLOWUP_OFFER_COLUMN_PARENT)
    assert attempt.returncode != 0
    assert "pending offer" in (attempt.stderr + attempt.stdout)
    assert "offered_source_item_ids" in await _columns(disposable_database, "easyweek_voucher_followup_approvals")
