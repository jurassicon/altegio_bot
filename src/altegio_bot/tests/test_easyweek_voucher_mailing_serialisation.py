"""A batch does one thing at a time (review R1 and R2).

Two confirmed review findings, both of which let a second actor interfere with a
stage that was already running, and both reproduced here through the real API and
the real executor before being asserted fixed.

**R1.** A stop pressed during a running CREATE could be lifted by a second tab
confirming an approval prepared *before* it, and the stopped slots were then
created after all. The scenario is scripted exactly as reported.

**R2.** A reconcile run during an active DELIVER reinterpreted the live
``send_claimed`` row as abandoned, so when Meta's success came back the
compare-and-set that records ``provider_message_id`` no longer matched and the
acceptance was lost — while the operation still reported success.

**§45.4.** The stop a confirmation races is now TERMINAL on the fixed €10
contract, so the pair has two orders rather than one outcome set, and each is
pinned by its own test instead of being left to whichever order the machine
happens to win. ``applied`` is not an acceptable result in either of them: the
stop is durable before the executor is started, so a stage that went on to buy
would be a violation, not an interleaving. ``stopped`` is not either — that is
what the per-slot guard reports when a stop lands DURING a stage, and here the
stage has not begun.

Only EasyWeek and Meta are faked. The HTTP layer, the session, the CSRF check, the
approvals, the operations, the per-item ledger and the executor are the real ones.
Where an order has to be fixed, it is fixed by holding one real call at its entry
until the other has committed — never by substituting a result.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.models.models import (
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import (
    FakeReader,
    marker_orders,
    seed_template_and_sender,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    PROVIDER_MESSAGE_IDS,
    FakeMutator,
    FakeSender,
    seed_production_preview,
    unknown_outcome,
)
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"
STOP_URL = "/ops/voucher-mailings/api/stop"
RECONCILE_URL = "/ops/voucher-mailings/api/reconcile"
STATUS_URL = "/ops/voucher-mailings/api/status"


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _plan(client, **payload: Any) -> dict[str, Any]:
    return (await client.post(PLAN_URL, json=payload)).json()


async def _confirm(client, offer: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    response = await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    return response.status_code, response.json()


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


async def _frozen(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    """A real frozen batch, reached only through the UI."""
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    transports.use(reader=reader)
    offer = await _plan(
        client,
        stage="freeze",
        preview_run_id=run_id,
        expected_recipient_count=count,
        approved_exposure_minor=count * 1000,
    )
    assert offer["ready"], offer["reasons"]
    status, _ = await _confirm(client, offer)
    assert status == 200
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    batch_id = int(snapshot.batch_id or 0)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    return run_id, batch_id, reader


async def _created(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    run_id, batch_id, reader = await _frozen(client, session_maker, transports, count=count)
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    offer = await _plan(client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(client, offer)
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    return run_id, batch_id, reader


async def _paid(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    run_id, batch_id, reader = await _created(client, session_maker, transports, count=count)
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    offer = await _plan(client, stage="pay", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(client, offer)
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    return run_id, batch_id, reader


# ===========================================================================
# R1 — a plan prepared before a stop must not resume after it
# ===========================================================================


async def test_a_stale_approval_in_a_second_tab_cannot_lift_a_live_stop(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The reported scenario, step for step.

    Two CREATE approvals prepared; the first confirmed and executing; the operator
    presses stop during its first external request; the second tab confirms the
    approval it prepared earlier. Before the fix that cleared the stop and the
    remaining two vouchers were created.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    # 1–2. Two approvals prepared up front. The second one is the stale plan.
    transports.use(reader=reader)
    first_offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    second_offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert first_offer["ready"] and second_offer["ready"]
    assert first_offer["approval"]["approval_id"] != second_offer["approval"]["approval_id"], (
        "the two tabs must hold two different approvals"
    )

    stop_landed: list[bool] = []
    second_confirm: list[tuple[int, dict[str, Any]]] = []

    class StoppingMutator(FakeMutator):
        """Presses stop, then confirms the stale plan — during the first request."""

        async def create_voucher_order(self, **kwargs: Any) -> Any:
            result = await super().create_voucher_order(**kwargs)
            if len(self.create_calls) == 1:
                # 3. Stop, while this very request is in flight.
                answer = await ui_client.post(STOP_URL, json={"batch_id": batch_id})
                stop_landed.append(bool(answer.json()["stop_active"]))
                # 4. The other tab confirms the approval it prepared BEFORE the stop.
                second_confirm.append(await _confirm(ui_client, second_offer))
            return result

    mutator = StoppingMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    status, _ = await _confirm(ui_client, first_offer)
    assert status == 200
    finished = await worker_module.run_once(session_maker, owner="test-executor")

    assert stop_landed == [True]
    # The stale confirmation is refused, and says why.
    assert second_confirm, "the second tab never confirmed"
    second_status, second_body = second_confirm[0]
    assert second_status == 409, second_body
    assert set(second_body["reasons"]) & {
        "voucher_production_operation_in_flight",
        # §45.4 answers with the terminal code on this contract; either refusal is a
        # refusal, and which one depends only on which gate the second tab hit first.
        "voucher_production_stop_terminal",
        "voucher_production_stop_active",
    }, second_body["reasons"]

    # 5. The outcome the finding described must NOT happen.
    assert mutator.calls.count("create") == 1, "the stop did not hold"
    items = await _items(session_maker, batch_id)
    assert items[1].status == "created"
    assert [items[s].status for s in (2, 3)] == [VOUCHER_PRODUCTION_ITEM_PLANNED] * 2
    assert all(items[s].create_claimed_at is None for s in (2, 3))
    # Nothing about the untouched slots is in doubt.
    assert all(items[s].reconciliation_required is False for s in (2, 3))
    # The stop is still in force, and the stale approval is still unspent.
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is True
    stale = await operations_module.load_approval(session_maker, approval_id=second_offer["approval"]["approval_id"])
    assert stale is not None and stale.pending is True
    assert finished is not None and finished.outcome_code == "stopped"


async def test_a_fresh_plan_after_a_terminal_stop_does_not_resume(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """§45.4. Inverts ``test_a_fresh_plan_after_the_stop_is_what_resumes``.

    The §43.6 rule was that a plan built in knowledge of the stop IS the decision to
    carry on. The owner removed that for this contract: the stop ends the batch, so
    the plan is refused where it used to be the way back in, and nothing a browser
    can do spends money afterwards.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    stop = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    assert stop["stop_active"] is True and stop["stop_terminal"] is True

    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert not offer["ready"]
    assert "voucher_production_stop_terminal" in offer["reasons"]
    # No approval was even offered, so there is nothing to confirm and nothing queued.
    assert offer["approval"] is None
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    assert mutator.calls == []
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is True
    assert all(
        row.status == VOUCHER_PRODUCTION_ITEM_PLANNED for row in (await _items(session_maker, batch_id)).values()
    )


async def test_an_offer_held_open_across_a_terminal_stop_is_refused_at_confirm(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """§45.4, the second gate. The tab that was already open cannot spend its offer.

    Supersedes ``test_a_second_stop_invalidates_a_plan_made_after_the_first``: the
    generation counter decided which stop a plan had seen, and under a terminal stop
    there is no plan that may carry on, so the confirmation is refused without
    consulting it. The counter still governs schemas 1 and 2.

    Pressing stop twice is still one stop, and still escalates to nothing.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]

    first = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    second = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    assert first["stop_terminal"] is True
    assert second["stop_count"] == 2 and second["stop_terminal"] is True

    status, body = await _confirm(ui_client, offer)
    assert status == 409
    assert body["reasons"] == ["voucher_production_stop_terminal"]
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    assert all(
        row.status == VOUCHER_PRODUCTION_ITEM_PLANNED for row in (await _items(session_maker, batch_id)).values()
    )
    # The approval stays unspent rather than being consumed by the refusal.
    stale = await operations_module.load_approval(session_maker, approval_id=offer["approval"]["approval_id"])
    assert stale is not None and stale.pending is True


async def test_no_second_operation_is_admitted_while_one_is_queued(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """One batch, one operation — checked before the executor even starts."""
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    first = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    second = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)

    assert (await _confirm(ui_client, first))[0] == 200
    status, body = await _confirm(ui_client, second)
    assert status == 409
    assert body["reasons"] == ["voucher_production_operation_in_flight"]
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert len([entry for entry in operations if entry.stage == "create"]) == 1


# How long a gated order waits for the other request to reach its own entry. A
# regression that stops routing through the gated call must FAIL here, loudly and
# quickly, rather than hang a required CI job until the runner kills it.
GATE_TIMEOUT = 30.0


async def _reached(event: asyncio.Event, what: str) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=GATE_TIMEOUT)
    except TimeoutError as error:  # pragma: no cover - only on a regression
        raise AssertionError(f"{what} never reached its gate, so this order was not exercised") from error


@contextlib.asynccontextmanager
async def _gate(module: Any, name: str, release: asyncio.Event, arrived: asyncio.Event):
    """Hold one real call at its entry until ``release`` is set.

    The ordering control these two tests need, and nothing more: the wrapped
    function is the real one, it is called with the real arguments, and its result
    is returned untouched. Only WHEN it runs is decided here, which is what makes
    "stop first" and "confirm first" reproducible instead of a coin toss the
    machine wins differently on each run.

    ``arrived`` fires as the gated call enters, so the other request can be
    launched knowing this one is genuinely in flight — both requests are open at
    the same time, against the same batch, exactly as two operators would be.
    """
    real = getattr(module, name)

    async def gated(*args: Any, **kwargs: Any):
        arrived.set()
        await release.wait()
        return await real(*args, **kwargs)

    setattr(module, name, gated)
    try:
        yield
    finally:
        setattr(module, name, real)


async def _assert_nothing_was_spent(session_maker, batch_id: int, mutator: FakeMutator) -> None:
    """The invariant both orders share: a refused stage costs nothing, anywhere."""
    assert mutator.calls == [], mutator.calls
    for slot, row in (await _items(session_maker, batch_id)).items():
        assert row.status == VOUCHER_PRODUCTION_ITEM_PLANNED, slot
        assert row.create_claimed_at is None, slot
        assert row.send_attempt_count == 0, slot
        assert row.reconciliation_required is False, slot
        assert row.voucher_code_hmac is None, slot
    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    assert state.active is True and state.terminal is True


async def test_a_stop_committing_first_refuses_a_confirmation_already_in_flight(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """Order A of the §45.4 race, made deterministic: the stop reaches the batch first.

    The confirmation is a real request, already inside its transaction and holding
    its approval row, when the stop commits. It must be refused outright — the
    terminal stop is not something a confirmation that was already under way gets
    to outrun — and no operation may be queued for an executor to pick up.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)

    confirm_arrived = asyncio.Event()
    stop_committed = asyncio.Event()

    async def drive_stop():
        # Only once the confirmation is provably in flight at its admission read.
        await _reached(confirm_arrived, "the confirmation")
        response = await ui_client.post(STOP_URL, json={"batch_id": batch_id})
        stop_committed.set()
        return response

    # The confirmation's admission read is what the stop has to beat, so that is
    # where it waits. Its approval row lock is held throughout; the stop takes the
    # batch header's lock, which is why the two can interleave at all.
    async with _gate(ledger_module, "admission_locked", stop_committed, confirm_arrived):
        confirm_result, stop_response = await asyncio.gather(_confirm(ui_client, offer), drive_stop())

    stop_body = stop_response.json()
    assert stop_body["stop_active"] is True and stop_body["stop_terminal"] is True
    confirm_status, confirm_body = confirm_result
    assert confirm_status == 409, confirm_body
    assert confirm_body["reasons"] == ["voucher_production_stop_terminal"]

    # Nothing was queued, so there is nothing for the executor to do.
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert [entry for entry in operations if entry.stage == "create"] == []
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    await _assert_nothing_was_spent(session_maker, batch_id, mutator)

    # The approval is spent by nothing and is not a second chance either: a fresh
    # plan for any spending stage is refused too.
    stale = await operations_module.load_approval(session_maker, approval_id=offer["approval"]["approval_id"])
    assert stale is not None and stale.pending is True
    for stage in ("create", "pay", "deliver"):
        again = await _plan(ui_client, stage=stage, preview_run_id=run_id, batch_id=batch_id)
        assert not again["ready"], stage
        assert "voucher_production_stop_terminal" in again["reasons"], stage


async def test_a_confirmation_committing_first_is_still_refused_by_the_executor(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """Order B of the same race, and the one the old test got wrong.

    It allowed ``applied`` here, which cannot be right: the stop is durable before
    the executor is ever started, so a stage that went on to buy three vouchers
    would be a §45.4 violation rather than an accepted interleaving. It also
    allowed ``stopped``, which the per-slot guard produces when a stop lands DURING
    a stage — not when the stage has not begun.

    What actually happens, and what is asserted: the queued operation is refused as
    a whole, because the executor rebuilds the plan live and the plan sees the
    terminal stop. Nothing is claimed and nothing is bought.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)

    stop_arrived = asyncio.Event()
    confirm_committed = asyncio.Event()

    async def drive_confirm():
        # Only once the stop request is provably in flight at its own write.
        await _reached(stop_arrived, "the stop")
        result = await _confirm(ui_client, offer)
        confirm_committed.set()
        return result

    async with _gate(ledger_module, "request_stop", confirm_committed, stop_arrived):
        confirm_result, stop_response = await asyncio.gather(
            drive_confirm(), ui_client.post(STOP_URL, json={"batch_id": batch_id})
        )

    confirm_status, confirm_body = confirm_result
    assert confirm_status == 200, confirm_body
    stop_body = stop_response.json()
    assert stop_body["stop_active"] is True and stop_body["stop_terminal"] is True

    # The operation really was queued — this order is not the other one in disguise.
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert len([entry for entry in operations if entry.stage == "create"]) == 1

    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None
    assert finished.outcome_code == "refused", finished.result
    assert finished.status == "refused"
    assert finished.finished_at is not None
    assert "voucher_production_stop_terminal" in (finished.reason_codes or [])
    # Terminal, and specifically not the two outcomes the old expectation allowed.
    assert finished.outcome_code not in ("applied", "stopped")
    await _assert_nothing_was_spent(session_maker, batch_id, mutator)

    # And the refusal is not a licence to try again from anywhere.
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    for stage in ("create", "pay", "deliver"):
        again = await _plan(ui_client, stage=stage, preview_run_id=run_id, batch_id=batch_id)
        assert not again["ready"], stage
        assert "voucher_production_stop_terminal" in again["reasons"], stage
    await _assert_nothing_was_spent(session_maker, batch_id, mutator)


async def test_a_stop_does_not_permanently_block_status_or_reconcile_or_refund(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The admission gate must not take away recovery (§43.6)."""
    count = 2
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)
    await ui_client.post(STOP_URL, json={"batch_id": batch_id})

    # Status: readable.
    assert (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).status_code == 200
    # Reconcile: available, because nothing is executing.
    transports.use(reader=reader)
    answer = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
    assert answer.status_code == 200, answer.text

    # An allowed refund: still plannable and still executable. A refund never
    # clears a stop, so the stop is intact afterwards.
    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    transports.use(reader=reader, mutator=mutator)
    offer = await _plan(ui_client, stage="refund", preview_run_id=run_id, batch_id=batch_id, slot=1)
    assert offer["ready"], offer["reasons"]
    status, body = await _confirm(ui_client, offer)
    assert status == 200, body
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.status == "completed", finished.result
    assert mutator.calls.count("refund") == 1
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is True


async def test_pay_and_deliver_honour_the_same_stop_rules(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The guarantee is per claim, so it is the same for every acting stage."""
    count = 3
    run_id, batch_id, reader = await _created(ui_client, session_maker, transports, count=count)

    # PAY: stop during the first payment.
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")

    class StoppingPayer(FakeMutator):
        async def pay_voucher_order(self, **kwargs: Any) -> Any:
            result = await super().pay_voucher_order(**kwargs)
            if len(self.pay_calls) == 1:
                await ui_client.post(STOP_URL, json={"batch_id": batch_id})
            return result

    payer = StoppingPayer(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles)
    transports.use(reader=reader, mutator=payer)
    offer = await _plan(ui_client, stage="pay", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.outcome_code == "stopped"
    assert payer.calls.count("pay") == 1
    items = await _items(session_maker, batch_id)
    assert items[1].status == "paid"
    assert [items[s].status for s in (2, 3)] == ["created", "created"]

    # §45.4: the DELIVER half used to be a fresh plan for the one paid slot. Under a
    # terminal stop there is no such plan, and the paid slot keeps its unspent
    # attempt — which is exactly what makes it still refundable.
    sender = FakeSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert not offer["ready"]
    assert "voucher_production_stop_terminal" in offer["reasons"]
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    assert sender.calls == 0
    items = await _items(session_maker, batch_id)
    assert items[1].status == "paid"
    assert items[1].send_attempt_count == 0
    assert items[1].reconciliation_required is False


async def test_a_terminal_stop_survives_a_send_whose_result_was_a_success(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """§45.4. A stop pressed mid-send never rewrites the request already on the wire.

    Meta accepted slot 1 while the operator was pressing stop. That acceptance is
    recorded as the success it was — not as unsent, not as cancelled — and the
    remaining slots are never claimed. Honesty in both directions is the point.
    """
    count = 3
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    class StoppingSender(FakeSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            result = await super().send_voucher_template(**kwargs)
            await ui_client.post(STOP_URL, json={"batch_id": batch_id})
            return result

    sender = StoppingSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    assert (await _confirm(ui_client, offer))[0] == 200
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.outcome_code == "stopped"

    assert sender.calls == 1
    items = await _items(session_maker, batch_id)
    assert items[1].status == "provider_accepted"
    assert items[1].send_attempt_count == 1
    assert items[1].reconciliation_required is False
    # The slots behind it were never claimed, and nothing about them is in doubt.
    assert [items[slot].status for slot in (2, 3)] == ["paid", "paid"]
    assert all(items[slot].send_attempt_count == 0 for slot in (2, 3))
    assert all(items[slot].reconciliation_required is False for slot in (2, 3))
    # The stop is terminal, so there is no second DELIVER for the two paid slots.
    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    assert state.active is True and state.terminal is True
    again = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert not again["ready"]
    assert "voucher_production_stop_terminal" in again["reasons"]


async def test_a_stop_during_an_unknown_send_leaves_the_unknown_unknown(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """§45.4. A stop is not an answer about the request that was already in flight.

    Meta's reply never arrived for slot 1 and the operator stopped. The terminal stop
    must not turn that into a safe "not sent": the slot stays unresolved and
    reconcilable, its one attempt is spent, and no second attempt exists anywhere.
    """
    count = 3
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    class StoppingUnknownSender(FakeSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            await ui_client.post(STOP_URL, json={"batch_id": batch_id})
            return await super().send_voucher_template(**kwargs)

    sender = StoppingUnknownSender(outcomes=[unknown_outcome()])
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    assert (await _confirm(ui_client, offer))[0] == 200
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None

    assert sender.calls == 1
    items = await _items(session_maker, batch_id)
    # Unknown stays unknown: not sent, not unsent, and flagged for a human.
    assert items[1].send_attempt_count == 1
    assert items[1].reconciliation_required is True
    assert items[1].status not in ("paid", "refunded")
    # The slots behind it were never claimed, so nothing about THEM is in doubt.
    assert [items[slot].status for slot in (2, 3)] == ["paid", "paid"]
    assert all(items[slot].send_attempt_count == 0 for slot in (2, 3))
    assert all(items[slot].reconciliation_required is False for slot in (2, 3))
    # And no retry is reachable: the stop is terminal and the unknown blocks anyway.
    again = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert not again["ready"]
    assert sender.calls == 1


async def test_delivery_webhooks_still_land_after_a_terminal_stop(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """§45.4. Stopping execution does not stop the truth arriving afterwards.

    Slot 1 was accepted by Meta before the stop. The `delivered` and `read` callbacks
    for it land afterwards and are recorded — a stopped batch must not start lying
    about what happened to a message that really went out.
    """
    count = 2
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    class StoppingSender(FakeSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            result = await super().send_voucher_template(**kwargs)
            await ui_client.post(STOP_URL, json={"batch_id": batch_id})
            return result

    sender = StoppingSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert (await _confirm(ui_client, offer))[0] == 200
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    assert sender.calls == 1
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).terminal is True

    for status in ("delivered", "read"):
        outcome = await ledger_module.record_webhook_transition(
            session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=status
        )
        assert outcome.applied is True, (status, outcome)

    items = await _items(session_maker, batch_id)
    assert items[1].status == "read"
    assert items[1].send_attempt_count == 1
    # The unsent slot is untouched by any of it, and still not resumable.
    assert items[2].status == "paid"
    body = (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).json()
    assert body["stop_terminal"] is True
    assert body["batch"]["webhook_delivered_count"] == 1
    assert body["batch"]["webhook_read_count"] == 1


# ===========================================================================
# R2 — a readback must not reinterpret a live claim
# ===========================================================================


async def test_a_reconcile_during_an_active_send_does_not_lose_the_success(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The reported scenario: reconcile mid-send, then Meta answers successfully.

    Before the fix the reconcile moved the live ``send_claimed`` row to
    ``send_unknown``, the acceptance write no longer matched, and the result was one
    real Meta call with no ``provider_message_id`` recorded while the operation
    reported success.
    """
    count = 1
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)
    reconcile_answers: list[tuple[int, dict[str, Any]]] = []

    class ReconcilingSender(FakeSender):
        """Runs a real reconcile while its own request is in flight."""

        async def send_voucher_template(self, **kwargs: Any) -> Any:
            response = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
            reconcile_answers.append((response.status_code, response.json()))
            # ... and only then does Meta answer, successfully.
            return await super().send_voucher_template(**kwargs)

    sender = ReconcilingSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")

    # The reconcile was refused while the stage was executing, and said why.
    assert reconcile_answers, "the reconcile never ran"
    code, body = reconcile_answers[0]
    assert code == 409, body
    assert body["reasons"] == ["voucher_production_reconcile_busy"]

    # One Meta call, and its success is recorded in full.
    assert sender.calls == 1
    items = await _items(session_maker, batch_id)
    assert items[1].status == "provider_accepted"
    assert items[1].provider_message_id == PROVIDER_MESSAGE_IDS[0]
    assert items[1].send_attempt_count == 1
    assert items[1].provider_accepted_at is not None
    assert finished is not None and finished.status == "completed"

    # And a later callback finds the slot by that id.
    async with session_maker() as session:
        located = await ledger_module._locate_message(session, PROVIDER_MESSAGE_IDS[0])
    assert located == (batch_id, 1)


async def test_a_displaced_row_still_absorbs_the_providers_success(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """Belt and braces: even if a row IS parked, the acceptance is not thrown away.

    The admission guard is the fix; this is the second layer. A ``send_unknown`` row
    is a legal source state for the acceptance write, because ``provider_accepted``
    outranks it and the alternative is discarding a proven success together with the
    only identifier a delivered/read callback could use.
    """
    count = 1
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    class DisplacingSender(FakeSender):
        """Parks its own row mid-flight, bypassing the admission guard entirely."""

        async def send_voucher_template(self, **kwargs: Any) -> Any:
            await ledger_module.record_item_outcome(
                session_maker,
                batch_id=batch_id,
                slot=1,
                status="send_unknown",
                expected_statuses=frozenset({"send_claimed"}),
                reason_code="voucher_production_mutation_unknown",
                reconciliation_required=True,
            )
            return await super().send_voucher_template(**kwargs)

    sender = DisplacingSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")

    items = await _items(session_maker, batch_id)
    assert items[1].status == "provider_accepted"
    assert items[1].provider_message_id == PROVIDER_MESSAGE_IDS[0]
    assert items[1].send_attempt_count == 1
    assert finished is not None


async def test_a_lost_ledger_write_is_never_reported_as_success(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """If the acceptance genuinely cannot be recorded, say so — do not claim success.

    Forced by moving the row somewhere the acceptance write may not come back from,
    which is what "the record did not land" looks like from the runner's side.
    """
    count = 1
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    class VanishingSender(FakeSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            # `read` outranks `provider_accepted`, so the acceptance write is
            # refused as a regression — the ledger will not take it.
            await ledger_module.record_item_outcome(
                session_maker,
                batch_id=batch_id,
                slot=1,
                status="read",
                expected_statuses=frozenset({"send_claimed"}),
                reconciliation_required=False,
            )
            return await super().send_voucher_template(**kwargs)

    sender = VanishingSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")

    assert sender.calls == 1
    assert finished is not None
    # Never "applied": the message is real and the ledger does not record it.
    assert finished.outcome_code == "unknown"
    assert "voucher_production_ledger_write_lost" in finished.reason_codes
    # And no second attempt was made to "fix" it.
    assert (await _items(session_maker, batch_id))[1].send_attempt_count == 1


async def test_reconcile_stays_available_after_a_genuine_interruption(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The guard must not block the case it exists to serve.

    An operation whose executor died is ``interrupted``, which is terminal — so it is
    not "in flight" and a readback passes straight through.
    """
    count = 2
    run_id, batch_id, reader = await _created(ui_client, session_maker, transports, count=count)
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    offer = await _plan(ui_client, stage="pay", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)

    # The executor claims it and dies.
    assert await operations_module.claim_next_operation(session_maker, owner="dying") is not None
    blocked = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
    assert blocked.status_code == 409
    assert blocked.json()["reasons"] == ["voucher_production_reconcile_busy"]

    interrupted = await operations_module.interrupt_abandoned(session_maker, include_all_running=True)
    assert [entry.status for entry in interrupted] == ["interrupted"]

    transports.use(reader=reader)
    allowed = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
    assert allowed.status_code == 200, allowed.text


async def test_a_reconcile_racing_a_worker_claim_cannot_park_a_live_row(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
):
    """The write-time guard, not just the admission one.

    Admission is a courtesy; this is the guarantee. The reconcile write is attempted
    directly against a batch whose operation is already ``running``, which is the
    state the race would produce, and the ledger refuses it under the header lock.
    """
    count = 1
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader, sender=FakeSender())
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    await _confirm(ui_client, offer)
    claimed = await operations_module.claim_next_operation(session_maker, owner="busy")
    assert claimed is not None

    # Put the row in the state a live send holds, then try to park it.
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    from altegio_bot.campaigns.easyweek_voucher_production.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(snapshot)
    assert identity is not None
    granted = await ledger_module.claim_send(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=1,
        plan_digest="d" * 64,
        live_guard_reproven_at=utcnow(),
        template_code="new_client_voucher",
        meta_template_name="kitilash_ka_new_client_voucher_v1",
        template_language="de",
        sender_id=None,
    )
    assert granted.granted

    refused = await ledger_module.record_item_outcome(
        session_maker,
        batch_id=batch_id,
        slot=1,
        status="send_unknown",
        expected_statuses=frozenset({"send_claimed"}),
        reason_code="voucher_production_mutation_unknown",
        reconciliation_required=True,
        require_idle=True,
    )
    assert refused.applied is False
    assert refused.reason == ledger_module.RECORD_REFUSED_BUSY
    # The live claim is untouched, so the answer still has somewhere to land.
    assert (await _items(session_maker, batch_id))[1].status == "send_claimed"
