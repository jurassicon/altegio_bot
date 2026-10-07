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

Only EasyWeek and Meta are faked. The HTTP layer, the session, the CSRF check, the
approvals, the operations, the per-item ledger and the executor are the real ones.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as production_runner
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.models.models import (
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import (
    FakeReader,
    marker_orders,
    model_issued_validity_capability,
    seed_template_and_sender,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    PROVIDER_MESSAGE_IDS,
    FakeMutator,
    FakeSender,
    seed_production_preview,
)
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module


@pytest.fixture
def issued_validity_capability(monkeypatch):
    """§45.2 (a) modelled as answered, so a stage of the new contract may buy.

    This module's subject is one batch doing one thing at a time.
    With the real default the fixed €10 contract refuses CREATE and PAY
    outright, before any order exists. That refusal is proven WITHOUT this
    fixture in ``test_easyweek_voucher_10eur_lifecycle.py``; nothing here
    weakens it. This models question (a) only — whether any issued term could
    be proven at all — and never question (b) about one particular voucher.
    """
    model_issued_validity_capability(monkeypatch)


@pytest.fixture
def synthetic_validity_proven(monkeypatch):
    """Send concurrency acceptance assumes independently proven validity; rollout remains blocked."""
    monkeypatch.setattr(production_runner, "issued_voucher_validity_reason", lambda payload, *, now: None)


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
    issued_validity_capability,
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


async def test_a_fresh_plan_after_the_stop_is_what_resumes(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """Continuing is possible, and it is a decision taken in knowledge of the stop."""
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    await ui_client.post(STOP_URL, json={"batch_id": batch_id})

    # A plan built now sees the stop, so confirming it IS the decision to carry on.
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    status, body = await _confirm(ui_client, offer)
    assert status == 200, body
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.outcome_code == "applied"
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is False
    assert all(row.status == "created" for row in (await _items(session_maker, batch_id)).values())


async def test_a_second_stop_invalidates_a_plan_made_after_the_first(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """The generation is a counter, not a flag.

    A plan built under stop #1 is a legitimate continuation of stop #1 — and not of
    stop #2. Without a counter, "a stop is active and the plan knew about a stop"
    would be true for both.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    await ui_client.post(STOP_URL, json={"batch_id": batch_id})

    transports.use(reader=reader)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"]

    # The operator changes their mind and stops again before confirming.
    second = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    assert second["stop_count"] == 2

    status, body = await _confirm(ui_client, offer)
    assert status == 409
    assert body["reasons"] == ["voucher_production_stop_active"]
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    assert all(
        row.status == VOUCHER_PRODUCTION_ITEM_PLANNED for row in (await _items(session_maker, batch_id)).values()
    )


async def test_no_second_operation_is_admitted_while_one_is_queued(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
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


async def test_concurrent_stop_and_confirm_never_both_win(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """Whichever order PostgreSQL picks, the pair is consistent.

    Either the confirmation landed first and the stop then blocks the claims, or the
    stop landed first and the confirmation is refused. What must never happen is a
    confirmed operation running with the stop considered lifted.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)

    confirm_result, _stop_result = await asyncio.gather(
        _confirm(ui_client, offer),
        ui_client.post(STOP_URL, json={"batch_id": batch_id}),
    )
    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    confirm_status, _ = confirm_result

    finished = await worker_module.run_once(session_maker, owner="test-executor")
    if confirm_status == 409:
        # The stop won admission: nothing was queued at all.
        assert finished is None
        assert state.active is True
    else:
        # The confirmation won admission. The stop then governs the claims, so the
        # stage either stops immediately or runs — never half of each inconsistently.
        assert finished is not None
        assert finished.outcome_code in ("stopped", "applied")
        if finished.outcome_code == "stopped":
            assert all(row.create_claimed_at is None for row in (await _items(session_maker, batch_id)).values())


async def test_a_stop_does_not_permanently_block_status_or_reconcile_or_refund(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
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
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    synthetic_validity_proven,
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

    # DELIVER: a fresh plan for the one paid slot, stopped during its send.
    class StoppingSender(FakeSender):
        async def send_voucher_template(self, **kwargs: Any) -> Any:
            result = await super().send_voucher_template(**kwargs)
            await ui_client.post(STOP_URL, json={"batch_id": batch_id})
            return result

    sender = StoppingSender()
    transports.use(reader=reader, sender=sender)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    await _confirm(ui_client, offer)
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None
    assert sender.calls == 1
    items = await _items(session_maker, batch_id)
    assert items[1].status == "provider_accepted"
    assert items[1].send_attempt_count == 1


# ===========================================================================
# R2 — a readback must not reinterpret a live claim
# ===========================================================================


async def test_a_reconcile_during_an_active_send_does_not_lose_the_success(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    synthetic_validity_proven,
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
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    synthetic_validity_proven,
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
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    synthetic_validity_proven,
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
    issued_validity_capability,
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
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    synthetic_validity_proven,
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
