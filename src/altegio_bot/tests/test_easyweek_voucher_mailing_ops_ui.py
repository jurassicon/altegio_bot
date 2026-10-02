"""The operator's whole path, in the browser (§43.3, §43.7 group A).

What this file proves is the product requirement, not a layer of it: an operator
who only has a browser can prepare an audience, check it, freeze it, create, pay
and deliver as three separately confirmed stages, watch progress, stop, reconcile,
refund an allowed slot and continue — and never needs a command, a digest, a
timestamp, a typed id or an ``.env`` edit anywhere in that path.

How it is driven
----------------
Through the real app, over HTTP, with a genuine signed session cookie and this
session's CSRF token, against the real pages and the real JSON API. The shipped
page JavaScript is executed under ``node``, so the decisions an operator depends
on — which stage is offered next, what the confirmation says, whether a slot may
be refunded — are tested as the bytes the browser receives rather than as a copy.

The only things faked are EasyWeek, Meta and the single narrow issuer seam, and
they are installed where production builds a transport — so the plan endpoint and
the executor both get the fake, and neither can quietly reach the real API.

The executor is the real one. Every stage below is run by ``run_once`` picking up
a durable operation, which is the only way a confirmed stage ever executes.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.models.models import (
    VOUCHER_PRODUCTION_ITEM_PAID,
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import StubTransports, session_cookie
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    VOUCHER_CODE_SENTINELS,
    FakeMutator,
    FakeReader,
    FakeSender,
    accepted_outcome,
    marker_orders,
    seed_production_preview,
    seed_template_and_sender,
    unknown_outcome,
)
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed; JS execution tests need it")

PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"
STOP_URL = "/ops/voucher-mailings/api/stop"
RECONCILE_URL = "/ops/voucher-mailings/api/reconcile"
STATUS_URL = "/ops/voucher-mailings/api/status"


# ===========================================================================
# Helpers that drive the UI exactly as the page script does
# ===========================================================================


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _plan(client, **payload: Any) -> dict[str, Any]:
    response = await client.post(PLAN_URL, json=payload)
    return response.json()


async def _confirm(client, offer: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Confirm an offer the way the page does: echo back the server's own numbers.

    The page never invents these. It reads them off the offer it was given, which
    is exactly why the server comparing them catches a tampered client.
    """
    response = await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    return response.status_code, response.json()


async def _run_executor(session_maker) -> operations_module.StoredOperation | None:
    """One pass of the real executor over the real queue.

    No transports argument: the stub is installed where production builds one, so
    the executor constructs it exactly as the deployed worker does.
    """
    return await worker_module.run_once(session_maker, owner="test-executor")


async def _stage(
    client,
    session_maker,
    transports: StubTransports,
    *,
    stage: str,
    preview_run_id: int,
    reader: Any,
    batch_id: int | None = None,
    mutator: Any = None,
    sender: Any = None,
    slot: int | None = None,
    count: int | None = None,
    minor: int | None = None,
) -> tuple[dict[str, Any], operations_module.StoredOperation | None]:
    """Plan, confirm and let the executor run one stage. The whole UI round trip."""
    transports.use(reader=reader, mutator=mutator, sender=sender)
    payload: dict[str, Any] = {"stage": stage, "preview_run_id": preview_run_id}
    if batch_id is not None:
        payload["batch_id"] = batch_id
    if slot is not None:
        payload["slot"] = slot
    if count is not None:
        payload["expected_recipient_count"] = count
        payload["approved_exposure_minor"] = minor
    offer = await _plan(client, **payload)
    assert offer["ready"], offer["reasons"]
    status, body = await _confirm(client, offer)
    assert status == 200, body
    return offer, await _run_executor(session_maker)


async def _batch_id(session_maker, *, run_id: int) -> int | None:
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    return snapshot.batch_id


async def _items(session_maker, batch_id: int) -> list[EasyWeekVoucherProductionBatchItem]:
    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                    .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
                )
            )
            .scalars()
            .all()
        )


async def _frozen(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    """A preview curated, checked and frozen entirely through the browser."""
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    _offer, frozen = await _stage(
        client,
        session_maker,
        transports,
        stage="freeze",
        preview_run_id=run_id,
        reader=reader,
        count=count,
        minor=count * 1500,
    )
    assert frozen is not None and frozen.status == "completed", frozen
    batch_id = await _batch_id(session_maker, run_id=run_id)
    assert batch_id is not None
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    return run_id, batch_id, reader


async def _created(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    run_id, batch_id, reader = await _frozen(client, session_maker, transports, count=count)
    _offer, created = await _stage(
        client,
        session_maker,
        transports,
        stage="create",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]),
    )
    assert created is not None and created.status == "completed", created.result
    return run_id, batch_id, reader


async def _paid(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    run_id, batch_id, reader = await _created(client, session_maker, transports, count=count)
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    _offer, paid = await _stage(
        client,
        session_maker,
        transports,
        stage="pay",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    assert paid is not None and paid.status == "completed", paid.result
    return run_id, batch_id, reader


# ===========================================================================
# The whole path: preview → freeze → create → pay → deliver → progress
# ===========================================================================


async def test_an_operator_runs_the_whole_mailing_from_the_browser(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """One operator, one browser, four confirmations, three recipients paid and sent.

    The acceptance criterion of §43.7, executed. Note what never appears below: a
    command, a digest, a timestamp, a typed batch id, a staffer, an ``.env`` value.
    """
    count = 3
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)

    # 1. The list page offers this preview, with a link that carries the id so the
    #    operator never has to.
    index = await ui_client.get("/ops/voucher-mailings")
    assert index.status_code == 200
    assert f"/ops/voucher-mailings/prepare?preview_run_id={run_id}" in index.text
    assert "Юлии Мюллер" in index.text

    # 2. The preparation page shows the message with a PLACEHOLDER code.
    prepare = await ui_client.get(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    assert prepare.status_code == 200
    assert "XXXX-XXXX-XXXX" in prepare.text

    # 3. FREEZE — the operator states the count and the money.
    freeze_offer, frozen = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="freeze",
        preview_run_id=run_id,
        reader=reader,
        count=count,
        minor=count * 1500,
    )
    assert freeze_offer["targets"]["stage_target_count"] == count
    assert freeze_offer["targets"]["stage_amount_minor"] == count * 1500
    assert frozen is not None and frozen.status == "completed"
    batch_id = await _batch_id(session_maker, run_id=run_id)
    assert batch_id is not None

    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    assert page.status_code == 200
    assert "Ваучеры оформляются от" in page.text

    # 4. CREATE — its own confirmation.
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    create_offer, created = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="create",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]),
    )
    assert create_offer["targets"]["stage_target_count"] == count
    assert created is not None and created.status == "completed"
    assert created.result["external_calls"]["create"] == count

    # 5. PAY — its own confirmation, and its own money line.
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    pay_offer, paid = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="pay",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    assert pay_offer["targets"]["stage_amount_minor"] == count * 1500
    assert paid is not None and paid.status == "completed"
    assert paid.result["external_calls"]["pay"] == count

    # 6. DELIVER — its own confirmation, and no money line at all.
    sender = FakeSender()
    deliver_offer, delivered = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="deliver",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        sender=sender,
    )
    assert deliver_offer["targets"]["stage_amount_minor"] == 0
    assert delivered is not None and delivered.status == "completed"
    assert sender.calls == count
    assert all(sender.saw_codes)

    # 7. Progress, read from the browser. "Meta accepted" is not "delivered".
    status = (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).json()
    assert status["batch"]["provider_accepted_count"] == count
    assert status["batch"]["webhook_delivered_count"] == 0
    assert status["batch"]["webhook_read_count"] == 0
    assert status["active_operation"] is None
    assert [entry["stage"] for entry in status["operations"]] == ["deliver", "pay", "create", "freeze"]


async def test_no_single_control_walks_create_pay_and_deliver(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Finishing a stage offers the next one; it never authorises it.

    Proven by state rather than by reading the HTML: after CREATE completes the
    queue is empty and no payment approval exists, so nothing could have been spent
    without a second confirmation.
    """
    count = 2
    _run_id, batch_id, reader = await _created(ui_client, session_maker, transports, count=count)

    transports.use(reader=reader)
    assert await _run_executor(session_maker) is None
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert [entry.stage for entry in operations] == ["create", "freeze"]
    assert all(row.status == "created" for row in await _items(session_maker, batch_id))


async def test_a_refresh_and_a_second_tab_do_not_create_a_second_effect(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """One offer, two confirmations, one operation, one set of vouchers.

    The two POSTs are what a double-click, a retried request whose response was
    lost, a refreshed form and a second tab all look like on the wire.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"]

    first_status, first = await _confirm(ui_client, offer)
    second_status, second = await _confirm(ui_client, offer)

    assert first_status == 200 and first["created"] is True
    # The second press is told the truth — it worked, once — rather than failing.
    assert second_status == 200 and second["created"] is False
    assert first["operation"]["operation_id"] == second["operation"]["operation_id"]

    await _run_executor(session_maker)
    assert await _run_executor(session_maker) is None
    assert mutator.calls.count("create") == count


async def test_work_survives_the_tab_that_started_it(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The operation is durable before the browser is answered.

    So a client that vanishes right after the confirmation changes nothing, and a
    later page load shows the same operation rather than a lost one.
    """
    count = 2
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    transports.use(reader=reader)
    offer = await _plan(
        ui_client,
        stage="freeze",
        preview_run_id=run_id,
        expected_recipient_count=count,
        approved_exposure_minor=count * 1500,
    )
    status, body = await _confirm(ui_client, offer)
    assert status == 200

    # Before the executor has run at all, the operation is already readable.
    queued = await operations_module.load_operation(session_maker, operation_id=body["operation"]["operation_id"])
    assert queued is not None and queued.status == "queued"

    finished = await _run_executor(session_maker)
    assert finished is not None and finished.status == "completed"
    batch_id = await _batch_id(session_maker, run_id=run_id)
    seen = (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).json()
    assert [entry["operation_id"] for entry in seen["operations"]] == [queued.id]


async def test_readiness_explains_a_closed_fence_without_naming_a_secret(
    session_maker, ops_credentials, monkeypatch, ui_client
):
    """With the fence shut the pages still render and say why nothing may run."""
    from altegio_bot.settings import settings as live

    monkeypatch.setattr(live, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    monkeypatch.setattr(live, "easyweek_voucher_production_mailing_staffer_uuid", "", raising=False)
    monkeypatch.setattr(live, "easyweek_voucher_production_mailing_account_uuid", "", raising=False)

    page = await ui_client.get("/ops/voucher-mailings")
    assert page.status_code == 200
    assert "voucher_production_disabled" in page.text
    assert "voucher_production_staffer_unconfigured" in page.text
    assert "voucher_production_account_unconfigured" in page.text
    # Reasons, never values.
    assert "EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY=" not in page.text

    run_id, _ = await seed_production_preview(session_maker, count=1)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=1)
    assert offer["ready"] is False
    assert "voucher_production_disabled" in offer["reasons"]


async def test_the_status_page_stays_readable_with_the_fence_closed(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """Shutting the fence must not take the state away from the operator."""
    run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)

    from altegio_bot.settings import settings as live

    monkeypatch.setattr(live, "easyweek_voucher_production_mailing_enabled", False, raising=False)

    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    assert page.status_code == 200
    state = (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).json()
    assert state["batch"]["exists"] is True
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"] is False and "voucher_production_disabled" in offer["reasons"]


async def test_a_get_never_starts_a_stage(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client
):
    """Loading any page or the status endpoint creates no approval and no operation."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)

    for path in (
        "/ops/voucher-mailings",
        f"/ops/voucher-mailings/prepare?preview_run_id={run_id}",
        f"{STATUS_URL}?preview_run_id={run_id}",
    ):
        assert (await ui_client.get(path)).status_code == 200

    assert await operations_module.list_operations(session_maker) == []
    assert await _batch_id(session_maker, run_id=run_id) is None


# ===========================================================================
# Stop, unknown, reconcile, continue, refund — all from the same interface
# ===========================================================================


async def test_a_stop_ends_the_stage_after_the_current_request(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """One voucher is created, the stop lands, and the rest are never claimed.

    The stop is pressed from the browser while the stage runs — modelled by a
    mutator that posts it as its own first CREATE returns, which is exactly the
    race the in-transaction check exists for.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    class StoppingMutator(FakeMutator):
        """Presses stop in the browser during its own first request."""

        async def create_voucher_order(self, **kwargs: Any) -> Any:
            result = await super().create_voucher_order(**kwargs)
            if len(self.create_calls) == 1:
                response = await ui_client.post(STOP_URL, json={"batch_id": batch_id})
                assert response.json()["stop_active"] is True
            return result

    mutator = StoppingMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"]
    status, _body = await _confirm(ui_client, offer)
    assert status == 200
    finished = await _run_executor(session_maker)

    assert finished is not None
    # The stage stopped; it did not fail, and nothing is unknown.
    assert finished.outcome_code == "stopped"
    assert finished.result["stopped_by_operator"] is True
    # Exactly one request left the process — the one already in flight.
    assert mutator.calls.count("create") == 1
    items = await _items(session_maker, batch_id)
    assert items[0].status == "created"
    # The slots behind it were never claimed, so nothing about them is in doubt.
    assert [row.status for row in items[1:]] == [VOUCHER_PRODUCTION_ITEM_PLANNED] * (count - 1)
    assert all(row.create_claimed_at is None for row in items[1:])
    assert all(row.reconciliation_required is False for row in items[1:])


async def test_a_second_stop_is_the_same_stop_and_refunds_nothing(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Pressing stop twice is one request, and it never moves money."""
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=2)

    first = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    second = (await ui_client.post(STOP_URL, json={"batch_id": batch_id})).json()
    assert first["stop_active"] and second["stop_active"]
    assert second["stop_count"] == 2
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active

    # A stop does not halt the batch and does not refund anything.
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.halted is False
    assert all(entry.status == VOUCHER_PRODUCTION_ITEM_PLANNED for entry in snapshot.items)


async def test_a_stop_does_not_block_status_or_reconciliation(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Stopping stops spending, not looking."""
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=2)
    await ui_client.post(STOP_URL, json={"batch_id": batch_id})

    assert (await ui_client.get(f"/ops/voucher-mailings/{batch_id}")).status_code == 200
    state = (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).json()
    assert state["stop_active"] is True

    transports.use(reader=reader)
    answer = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
    assert answer.status_code == 200 and answer.json()["accepted"] is True


async def test_continuing_after_a_stop_needs_a_fresh_confirmation_and_repeats_nothing(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A stop blocks the stage it was pressed during; continuing is a new decision.

    Two things are proven here. The stop really does block a stage confirmed before
    it — zero CREATEs. And the way back is a fresh plan and a fresh confirmation,
    which is the only thing that lifts a stop.
    """
    count = 3
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    # Confirm a stage, then stop before the executor reaches it.
    blocked = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=blocked)
    offer = await _plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    status, _ = await _confirm(ui_client, offer)
    assert status == 200
    await ui_client.post(STOP_URL, json={"batch_id": batch_id})

    stopped = await _run_executor(session_maker)
    assert stopped is not None and stopped.outcome_code == "stopped"
    assert blocked.calls.count("create") == 0

    # Continuing: a fresh plan, confirmed. That confirmation lifts the stop.
    resumed = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    _offer, finished = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="create",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        mutator=resumed,
    )
    assert finished is not None and finished.outcome_code == "applied"
    assert resumed.calls.count("create") == count
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is False


async def test_an_unknown_send_stops_the_rest_and_the_ui_offers_reconciliation(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """One ambiguous Meta answer halts the suffix, and the browser says so.

    No retry is offered and no new batch is offered. What the operator gets is a
    readback.
    """
    count = 3
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    sender = FakeSender(outcomes=[accepted_outcome(0), unknown_outcome(), accepted_outcome(2)])
    _offer, delivered = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="deliver",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        sender=sender,
    )
    assert delivered is not None
    assert delivered.outcome_code == "unknown"
    # Two sends: the proven one and the ambiguous one. The third was not attempted.
    assert sender.calls == 2

    state = (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).json()
    assert state["batch"]["reconciliation_required"] is True
    assert state["batch"]["halted"] is True

    # The stage is not on offer again while an outcome is in doubt.
    transports.use(reader=reader)
    offer = await _plan(ui_client, stage="deliver", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"] is False
    assert "voucher_production_batch_halted" in offer["reasons"]

    # And the readback is available from the same interface.
    answer = await ui_client.post(RECONCILE_URL, json={"batch_id": batch_id, "preview_run_id": run_id})
    assert answer.status_code == 200


async def test_an_allowed_pre_send_refund_runs_from_the_browser(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """One named paid slot, nothing ever sent for it, money back — one confirmation."""
    count = 2
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)

    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    offer, finished = await _stage(
        ui_client,
        session_maker,
        transports,
        stage="refund",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        mutator=mutator,
        slot=1,
    )
    assert offer["targets"]["stage_target_count"] == 1
    assert offer["targets"]["stage_amount_minor"] == 1500
    assert finished is not None and finished.status == "completed", finished.result
    assert mutator.calls.count("refund") == 1
    items = await _items(session_maker, batch_id)
    assert items[0].status == "refunded"
    # The other slot is untouched.
    assert items[1].status == VOUCHER_PRODUCTION_ITEM_PAID


async def test_the_draft_cleanup_requirement_is_stated_in_the_ui(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """When a draft may be open in EasyWeek, the page says so and offers a readback.

    No cancellation API is invented: this project has proven none, so the honest
    answer is to tell the operator and give them the reconcile.
    """
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    assert "Нужно закрыть draft в EasyWeek" in page.text
    assert "Сверить с EasyWeek" in page.text


# ===========================================================================
# The shipped JavaScript, executed
# ===========================================================================


def _page_script(page: str) -> str:
    blocks = re.findall(r"<script>(.*?)</script>", page, re.DOTALL)
    assert blocks, "the page serves no script"
    return max(blocks, key=len)


def _function_source(script: str, name: str) -> str:
    """One function declaration, sliced by matching braces — the shipped bytes."""
    marker = f"function {name}("
    start = script.find(marker)
    assert start != -1, f"{name} is not in the page script"
    prefix = "async "
    if script[start - len(prefix) : start] == prefix:
        start -= len(prefix)
    open_brace = script.index("{", start)
    depth = 0
    for index in range(open_brace, len(script)):
        if script[index] == "{":
            depth += 1
        elif script[index] == "}":
            depth -= 1
            if depth == 0:
                return script[start : index + 1]
    raise AssertionError(f"unbalanced braces in {name}")


def _parses(script: str) -> None:
    assert NODE is not None
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "page.mjs"
        target.write_text(script, encoding="utf-8")
        result = subprocess.run(  # noqa: S603 - fixed interpreter, generated file
            [NODE, "--check", str(target)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    assert result.returncode == 0, result.stderr


def _run_node(source: str, driver: str) -> Any:
    assert NODE is not None
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "case.mjs"
        script.write_text(source + "\n" + driver, encoding="utf-8")
        result = subprocess.run(  # noqa: S603 - fixed interpreter, generated file
            [NODE, str(script)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


_DECISION_FUNCTIONS = (
    "moneyLabel",
    "euroToMinor",
    "stageLabel",
    "confirmSummary",
    "nextAction",
    "slotCounts",
    "stateBanner",
    "deliveryFacts",
    "mayRefund",
)


async def _decisions(ui_client, session_maker, transports) -> str:
    """The shipped decision functions of the real mailing page, ready for node."""
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    script = _page_script(page.text)
    return "\n".join(_function_source(script, name) for name in _DECISION_FUNCTIONS)


@needs_node
async def test_the_preparation_page_javascript_parses(ui_client, ops_credentials) -> None:
    """A page whose script does not parse is a page with no working buttons."""
    page = await ui_client.get("/ops/voucher-mailings/prepare?preview_run_id=1")
    assert page.status_code == 200
    _parses(_page_script(page.text))


@needs_node
async def test_the_mailing_page_javascript_parses(
    ui_client, session_maker, production_configuration, binding_key, executor_enabled, transports
) -> None:
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    _parses(_page_script(page.text))


@needs_node
async def test_the_confirmation_names_this_stage_not_only_the_batch_total(
    ui_client, session_maker, production_configuration, binding_key, executor_enabled, transports
) -> None:
    """A payment for three remaining slots must not read as the batch total."""
    source = await _decisions(ui_client, session_maker, transports)
    driver = """
function amounts(text) {
  return (text.match(/\\d+\\.\\d\\d \\u20ac/g) || []);
}
const pay = confirmSummary({stage: "pay", targets: {
  stage_target_count: 3, stage_amount_minor: 4500,
  batch_recipient_count: 20, batch_exposure_minor: 30000
}});
const deliver = confirmSummary({stage: "deliver", targets: {
  stage_target_count: 2, stage_amount_minor: 0,
  batch_recipient_count: 2, batch_exposure_minor: 3000}});
console.log(JSON.stringify({
  payAmounts: amounts(pay),
  payCount: pay.includes("3"),
  deliverAmounts: amounts(deliver),
  deliverCount: deliver.includes("2")
}));
"""
    answer = _run_node(source, driver)
    # A payment names TWO amounts and they are different: what this press moves,
    # and what the whole mailing cost. Showing only the total would invite an
    # operator to think they were authorising €300 rather than €45 — or the other
    # way round after a partial run.
    assert answer["payAmounts"] == ["45.00 €", "300.00 €"]
    assert answer["payCount"] is True
    # A send is not a purchase, so the only amount beside it is the batch total —
    # never a "€0.00" of its own, which would read as a charge.
    assert answer["deliverAmounts"] == ["30.00 €"]
    assert answer["deliverCount"] is True


@needs_node
async def test_the_next_action_is_one_stage_and_never_two(
    ui_client, session_maker, production_configuration, binding_key, executor_enabled, transports
) -> None:
    """The page offers exactly one next step, and none while work is in flight."""
    source = await _decisions(ui_client, session_maker, transports)
    driver = """
function withItems(statuses, extra) {
  const base = {exists: true, reconciliation_required: false, halted: false,
    items: statuses.map((s, i) => ({slot: i + 1, status: s, reconciliation_required: false}))};
  const over = extra || {};
  return Object.assign({batch: Object.assign(base, over.batch || {})},
                       over.active_operation ? {active_operation: over.active_operation} : {});
}
console.log(JSON.stringify({
  planned: nextAction(withItems(["planned", "planned"])),
  created: nextAction(withItems(["created", "created"])),
  paid: nextAction(withItems(["paid"])),
  sent: nextAction(withItems(["provider_accepted"])),
  mixedPrefersEarliest: nextAction(withItems(["planned", "paid"])),
  whileRunning: nextAction(withItems(["planned"], {active_operation: {stage: "create"}})),
  whileUnresolved: nextAction(withItems(["paid"], {batch: {reconciliation_required: true}})),
  whileHalted: nextAction(withItems(["paid"], {batch: {halted: true}})),
  noBatch: nextAction({batch: {exists: false}})
}));
"""
    answer = _run_node(source, driver)
    assert answer["planned"] == "create"
    assert answer["created"] == "pay"
    assert answer["paid"] == "deliver"
    assert answer["sent"] is None
    # A partially created batch is offered CREATE, never a jump to the send.
    assert answer["mixedPrefersEarliest"] == "create"
    assert answer["whileRunning"] is None
    assert answer["whileUnresolved"] is None
    assert answer["whileHalted"] is None
    assert answer["noBatch"] is None


@needs_node
async def test_the_ui_distinguishes_a_stop_from_an_unknown(
    ui_client, session_maker, production_configuration, binding_key, executor_enabled, transports
) -> None:
    """Two different situations, two different next steps, two different banners."""
    source = await _decisions(ui_client, session_maker, transports)
    driver = """
const stopped = stateBanner({stop_active: true, batch: {reconciliation_required: false, halted: false}});
const unknown = stateBanner({stop_active: false, batch: {reconciliation_required: true, halted: true}});
const halted = stateBanner({stop_active: false, batch: {reconciliation_required: false, halted: true}});
console.log(JSON.stringify({
  stopKind: stopped.kind, stopText: stopped.text,
  unknownKind: unknown.kind, unknownText: unknown.text,
  haltedKind: halted.kind,
  quiet: stateBanner({stop_active: false, batch: {reconciliation_required: false, halted: false}})
}));
"""
    answer = _run_node(source, driver)
    assert answer["stopKind"] == "warning"
    assert "Остановлено оператором" in answer["stopText"]
    assert answer["unknownKind"] == "danger"
    assert "сверка" in answer["unknownText"]
    assert answer["haltedKind"] == "danger"
    assert answer["quiet"] is None


@needs_node
async def test_the_four_delivery_facts_are_never_merged_in_the_ui(
    ui_client, session_maker, production_configuration, binding_key, executor_enabled, transports
) -> None:
    """ "Execution completed" must not be able to read as "delivered"."""
    source = await _decisions(ui_client, session_maker, transports)
    driver = """
const facts = deliveryFacts({batch: {
  recipient_count: 4, execution_completed: true,
  provider_accepted_count: 4, webhook_delivered_count: 1, webhook_read_count: 0
}});
console.log(JSON.stringify({rows: facts.length, values: facts.map(p => p[1])}));
"""
    answer = _run_node(source, driver)
    assert answer["rows"] == 4
    assert answer["values"] == ["да", "4 из 4", "1 из 4", "0 из 4"]


def test_the_server_action_table_is_the_ledgers_own_contract():
    """``available_item_actions`` is derived from the claim sets, not re-typed (R6).

    The UI used to keep its own list of refundable statuses and it had drifted both
    ways: it hid ``pay_unknown`` and ``refund_rejected``, where a refund is allowed,
    and offered ``send_rejected``, where it is forbidden because the one attempt was
    already spent. This pins the single table against the ledger's own sets, so the
    two cannot drift again.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.ledger import (
        CREATE_CLAIMABLE_FROM,
        PAY_CLAIMABLE_FROM,
        REFUND_CLAIMABLE_FROM,
        SEND_CLAIMABLE_FROM,
        SENT_ITEM_STATUSES,
        ItemSnapshot,
    )
    from altegio_bot.campaigns.easyweek_voucher_production.runner import available_item_actions

    def snapshot(status: str, attempts: int = 0) -> ItemSnapshot:
        return ItemSnapshot(
            slot=1,
            campaign_recipient_id=1,
            campaign_run_id=1,
            easyweek_customer_uuid="synthetic",
            reconciliation_marker="marker",
            status=status,
            send_attempt_count=attempts,
        )

    for status in CREATE_CLAIMABLE_FROM:
        assert "create" in available_item_actions(snapshot(status)), status
    for status in PAY_CLAIMABLE_FROM:
        assert "pay" in available_item_actions(snapshot(status)), status
    for status in SEND_CLAIMABLE_FROM:
        assert "deliver" in available_item_actions(snapshot(status)), status
    for status in REFUND_CLAIMABLE_FROM:
        assert "refund" in available_item_actions(snapshot(status)), status

    # And never after a send, by either test: the state, or a spent attempt.
    for status in SENT_ITEM_STATUSES:
        assert "refund" not in available_item_actions(snapshot(status, attempts=1)), status
    assert "refund" not in available_item_actions(snapshot("paid", attempts=1))


@needs_node
async def test_the_ui_offers_a_refund_from_the_servers_list_not_a_status_name(
    ui_client, session_maker, production_configuration, binding_key, executor_enabled, transports
) -> None:
    """``mayRefund`` reads ``available_actions`` and ignores the status entirely (R6).

    Asserted by giving it contradictory inputs: a status that sounds refundable with
    no action offered, and one that sounds final with the action present. The server's
    list wins both times, which is what makes it the only copy of the rule.
    """
    source = await _decisions(ui_client, session_maker, transports)
    driver = """
console.log(JSON.stringify({
  offered: mayRefund({status: "paid", available_actions: ["deliver", "refund"]}),
  notOffered: mayRefund({status: "paid", available_actions: ["deliver"]}),
  payUnknownOffered: mayRefund({status: "pay_unknown", available_actions: ["refund"]}),
  refundRejectedOffered: mayRefund({status: "refund_rejected", available_actions: ["refund"]}),
  sendRejectedNotOffered: mayRefund({status: "send_rejected", available_actions: []}),
  soundsFinalButOffered: mayRefund({status: "refund_rejected", available_actions: ["refund"]}),
  missingField: mayRefund({status: "paid"}),
  nothing: mayRefund(null)
}));
"""
    answer = _run_node(source, driver)
    assert answer["offered"] is True
    assert answer["notOffered"] is False
    # The two states the old UI list wrongly hid.
    assert answer["payUnknownOffered"] is True
    assert answer["refundRejectedOffered"] is True
    # The state the old UI list wrongly offered.
    assert answer["sendRejectedNotOffered"] is False
    assert answer["soundsFinalButOffered"] is True
    # No list means no offer: a missing field is never read as permission.
    assert answer["missingField"] is False
    assert answer["nothing"] is False


@needs_node
async def test_checking_the_list_is_not_one_click_from_a_freeze(ui_client, ops_credentials) -> None:
    """ "Проверить состав" must not arm the confirm button.

    Executed against the shipped ``renderOffer``, because this is the one place
    where a careless refactor would turn a read into an action.
    """
    page = await ui_client.get("/ops/voucher-mailings/prepare?preview_run_id=1")
    script = _page_script(page.text)
    source = "\n".join(
        _function_source(script, name)
        for name in (
            "renderOffer",
            "renderComposition",
            "hideConfirm",
            "moneyLabel",
            "escapeHtml",
            "confirmSummary",
            "stageLabel",
            "setAlert",
        )
    )
    driver = """
let OFFER = {sentinel: true};
const PANELS = {};
globalThis.document = {
  getElementById: (id) => (PANELS[id] = PANELS[id] || {
    innerHTML: "", value: "",
    classList: {_s: new Set(), add(c){this._s.add(c);}, remove(c){this._s.delete(c);},
                contains(c){return this._s.has(c);}}
  })
};
const UNIT_PRICE_MINOR = 1500;
const ready = {ready: true, stage: "freeze", approval: {approval_id: 7},
               targets: {stage_target_count: 2, stage_amount_minor: 3000,
                         batch_recipient_count: 2, batch_exposure_minor: 3000},
               plan: {snapshot: {composition: {recipient_count: 2, total_exposure_minor: 3000, slots: []}}}};
renderOffer("freeze", {status: 200, data: ready}, {preview: true});
const afterPreview = {offer: OFFER, hidden: PANELS["confirm-panel"].classList.contains("d-none")};
renderOffer("freeze", {status: 200, data: ready}, {});
const afterPlan = {armed: OFFER !== null, hidden: PANELS["confirm-panel"].classList.contains("d-none")};
console.log(JSON.stringify({afterPreview: afterPreview, afterPlan: afterPlan}));
"""
    answer = _run_node(source, driver)
    # Checking the list clears any armed offer and keeps the confirmation hidden.
    assert answer["afterPreview"]["offer"] is None
    assert answer["afterPreview"]["hidden"] is True
    # Only an explicit plan arms it.
    assert answer["afterPlan"]["armed"] is True
    assert answer["afterPlan"]["hidden"] is False


# ===========================================================================
# Secrecy
# ===========================================================================


async def test_no_page_or_api_answer_carries_a_code_or_an_identity(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Every sentinel this suite can leak, hunted across the whole operator surface."""
    from altegio_bot.tests.easyweek_voucher_production_fixtures import (
        ACCOUNT_UUID,
        CUSTOMER_UUIDS,
        PHONES,
        STAFFER_UUID,
    )

    count = 2
    run_id, batch_id, reader = await _paid(ui_client, session_maker, transports, count=count)
    sender = FakeSender()
    await _stage(
        ui_client,
        session_maker,
        transports,
        stage="deliver",
        preview_run_id=run_id,
        batch_id=batch_id,
        reader=reader,
        sender=sender,
    )

    surfaces = [
        (await ui_client.get("/ops/voucher-mailings")).text,
        (await ui_client.get(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")).text,
        (await ui_client.get(f"/ops/voucher-mailings/{batch_id}")).text,
        (await ui_client.get(f"{STATUS_URL}?batch_id={batch_id}")).text,
    ]
    stored = await operations_module.list_operations(session_maker, batch_id=batch_id)
    assert stored, "the stages left no operations to inspect"
    surfaces.append(json.dumps([entry.as_safe_dict() for entry in stored]))

    forbidden = [
        *VOUCHER_CODE_SENTINELS[:count],
        *CUSTOMER_UUIDS[:count],
        *PHONES[:count],
        *ORDER_UUIDS[:count],
        STAFFER_UUID,
        ACCOUNT_UUID,
        "wamid.SYNTHETICPROD",
    ]
    # The audit and approval rows too. They are the tables most likely to grow a
    # "just for debugging" column, and the only place an operator's session could
    # be written down by accident.
    from sqlalchemy import text

    async with session_maker() as session:
        audit = (
            await session.execute(
                text(
                    "SELECT action, outcome, detail::text, session_fingerprint,"
                    " identification_limit FROM easyweek_voucher_production_audit ORDER BY id"
                )
            )
        ).all()
        approvals = (
            await session.execute(
                text(
                    "SELECT stage, principal, session_fingerprint, target_slots::text,"
                    " plan_digest FROM easyweek_voucher_production_approvals ORDER BY id"
                )
            )
        ).all()
    assert audit, "no audit rows were written"
    surfaces.append(json.dumps([[str(value) for value in row] for row in audit]))
    surfaces.append(json.dumps([[str(value) for value in row] for row in approvals]))

    # And the audit says what it can and cannot prove about who acted.
    assert {row[4] for row in audit} == {"shared_ops_account"}
    # The session is a digest, never the token itself.
    cookie = session_cookie()
    assert all(row[3] != cookie for row in audit)
    assert all(cookie not in row[2] for row in audit)

    for surface in surfaces:
        for secret in forbidden:
            assert secret not in surface, f"leaked {secret!r}"
