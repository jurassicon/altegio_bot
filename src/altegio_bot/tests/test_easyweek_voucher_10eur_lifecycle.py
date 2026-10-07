"""§45 full fixed-product lifecycle; real expiry evidence remains a rollout gate."""

from __future__ import annotations

from dataclasses import replace

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production import ledger, runner
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    BASELINE_DRIFT,
    IDENTITY_BINDING_MISMATCH,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
)
from altegio_bot.campaigns.easyweek_voucher_production.validity import VALIDITY_CAPABILITY_UNPROVEN
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown
from altegio_bot.easyweek_voucher_production_contract import CURRENT_PRODUCTION_CONTRACT as CONTRACT
from altegio_bot.models.models import EasyWeekVoucherProductionBatchAttempt, MessageTemplate
from altegio_bot.tests import easyweek_voucher_10eur_fixtures as new
from altegio_bot.tests import easyweek_voucher_production_fixtures as old
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import ui_confirm, ui_execute, ui_frozen, ui_plan
from altegio_bot.tests.test_easyweek_voucher_production_mailing import _apply, _ok_response, _plan


@pytest.fixture
def issued_validity_capability(monkeypatch):
    """§45.2 (a) modelled as answered, so a stage may buy at all.

    Required by every test below that creates or pays: with the real default the
    new contract refuses both outright, which is what
    ``test_unproven_validity_capability_refuses_create_and_pay_before_any_money``
    covers. Modelling (a) does NOT model (b): delivery still refuses unless a
    test patches ``issued_voucher_validity_reason`` as well.
    """
    new.model_issued_validity_capability(monkeypatch)


async def prepared(session_maker, *, paid=False, reader=None, count=1):
    """Freeze, create and optionally pay. Needs ``issued_validity_capability``."""
    run_id, ids = await old.seed_production_preview(session_maker, count=count)
    await new.seed_template_and_sender(session_maker)
    reader = reader or new.FakeReader(count=count)
    request = new.production_request(run_id=run_id)
    frozen = await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=new.approval_for(count))
    assert frozen.outcome == "frozen", frozen.reasons
    request = replace(request, batch_id=frozen.batch["batch_id"])
    reader.orders.update(await new.marker_orders(session_maker, batch_id=request.batch_id))
    create = old.FakeMutator(create_sequence=[_ok_response(index) for index in range(count)])
    created = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=create)
    assert created.outcome == "applied", created.reasons
    assert all(call["price_minor"] == 1000 for call in create.create_calls)
    assert all(call["voucher_template_uuid"] == CONTRACT.template_uuid for call in create.create_calls)
    assert all(call["product_contract_version"] == CONTRACT.version for call in create.create_calls)
    if paid:
        pay = old.FakeMutator(
            pay_sequence=[_ok_response(index) for index in range(count)],
            reader=reader,
            settles=await new.marker_orders(session_maker, batch_id=request.batch_id, status="paid"),
        )
        report = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=pay)
        assert report.outcome == "applied", report.reasons
    return request, reader, ids


async def test_new_lifecycle_uses_exact_product_and_scoped_message_with_mocked_expiry_evidence(
    session_maker, production_configuration, binding_key, issued_validity_capability, monkeypatch
):
    # Test the full delivery machinery under a separately mocked positive evidence
    # result. This is not a claim that the real API expiry schema is established.
    monkeypatch.setattr(runner, "issued_voucher_validity_reason", lambda payload, *, now: None)
    request, reader, _ = await prepared(session_maker, paid=True, count=2)
    reader.template["vouchers_count"] = reader.template["activated_vouchers_count"] = 2
    sender = old.FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert delivered.outcome == "applied"
    assert delivered.batch["total_exposure_minor"] == 2000
    assert delivered.batch["voucher_unit_price_minor"] == 1000
    assert sender.calls == 2
    assert set(reader.product_reads) == {CONTRACT.template_uuid}
    assert reader.meta_reads >= 4
    async with session_maker() as session:
        items = list((await session.scalars(select(EasyWeekVoucherProductionBatchAttempt))).all())
        assert all(item.template_code == CONTRACT.message_code for item in items)
        rows = list((await session.scalars(select(MessageTemplate))).all())
        assert {row.code for row in rows} >= {CONTRACT.message_code, "new_client_voucher"}
    # Sending consumes the one-attempt protection even under the new version.
    replay = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not replay.ready
    refund = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1)
    assert not refund.ready


async def test_unproven_validity_capability_refuses_create_and_pay_before_any_money(
    session_maker, production_configuration, binding_key
):
    """F1. The real default, with every other prerequisite in order.

    Supersedes the reviewed ``test_missing_expiry_evidence_blocks_delivery_but_not_refund``,
    which proved only that DELIVER refused — and got there through a CREATE and a
    PAY that it asserted were correct. They were not: the contract could not have
    delivered any of it, and that was knowable before the first order existed. So
    this test asks the question at the stage where the answer is still free.

    The freeze is deliberately still allowed. It is local, it buys nothing, and
    finding out at freeze time that the batch is undeliverable is the point of
    freezing first — so it reports the blocker rather than hiding it.
    """
    run_id, _ = await old.seed_production_preview(session_maker, count=2)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader(count=2)
    request = new.production_request(run_id=run_id)

    frozen = await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=new.approval_for(2))
    assert frozen.outcome == "frozen", frozen.reasons
    # Composed and approved, and explicitly NOT a launch-ready mailing.
    assert frozen.as_safe_dict()["delivery_blockers"] == [VALIDITY_CAPABILITY_UNPROVEN]
    assert frozen.as_safe_dict()["ready_for_send"] is False
    request = replace(request, batch_id=frozen.batch["batch_id"])
    reader.orders.update(await new.marker_orders(session_maker, batch_id=request.batch_id))

    # Every acting stage refuses, and none of them is reached through a payment.
    mutator = old.FakeMutator(create_sequence=[_ok_response(index) for index in range(2)])
    for stage in (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER):
        plan = await _plan(session_maker, reader, stage=stage, request=request)
        assert not plan.ready, stage
        assert VALIDITY_CAPABILITY_UNPROVEN in plan.reasons, stage
        assert plan.snapshot["prerequisites"]["issued_validity_capability_proven"] is False
    created = await _apply(
        session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator, expect_ready=False
    )
    assert created.outcome == "refused"
    assert VALIDITY_CAPABILITY_UNPROVEN in created.reasons
    # No create, no pay, no send, and no claim pretending one may have happened.
    assert mutator.create_calls == [] and mutator.pay_calls == []
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert {item.status for item in snapshot.items} == {"planned"}
    assert not snapshot.halted and not snapshot.reconciliation_required

    # The blocker closes buying, not looking. Read-only status, the batch's own
    # diagnostics and the readback all keep answering, which is what keeps an
    # existing object recoverable while this stands.
    status = await runner.run_status(session_maker, batch_id=request.batch_id)
    assert status.outcome == "observed"
    assert status.as_safe_dict()["voucher_unit_price_minor"] == 1000
    readback = await runner.run_reconcile(session_maker, request=request, order_reader=reader)
    assert readback.outcome == "observed", readback.reasons
    assert mutator.create_calls == [] and mutator.pay_calls == []


async def test_an_approval_taken_while_the_capability_held_cannot_execute_without_it(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """F1. The guard is the executor's, not the button's.

    A whole browser round trip — plan, confirm, a durable operation — taken
    while the capability was modelled as answered, and then executed after it is
    not. The stored approval does not carry permission: the executor rebuilds the
    plan live, finds the blocker, and finishes the operation without buying
    anything.
    """
    with monkeypatch.context() as held:
        new.model_issued_validity_capability(held)
        run_id, batch_id, reader = await ui_frozen(ui_client, session_maker, transports, count=1)
        mutator = old.FakeMutator(create_sequence=[_ok_response(0)])
        transports.use(reader=reader, mutator=mutator)
        offer = await ui_plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
        assert offer["ready"], offer["reasons"]
        assert (await ui_confirm(ui_client, offer))[0] == 200

    # The capability is gone again before the executor ever claims the row.
    finished = await ui_execute(session_maker)
    assert finished is not None
    assert finished.status == "refused", finished.result
    assert finished.finished_at is not None
    assert VALIDITY_CAPABILITY_UNPROVEN in (finished.reason_codes or [])
    assert mutator.create_calls == [] and mutator.pay_calls == []
    snapshot = await ledger.load(session_maker, batch_id=batch_id)
    assert {item.status for item in snapshot.items} == {"planned"}
    assert not snapshot.reconciliation_required


async def test_an_issued_voucher_with_an_unproven_term_blocks_delivery_but_not_refund(
    session_maker, production_configuration, binding_key, issued_validity_capability
):
    """F1, question (b): the capability exists and THIS voucher still cannot be sent.

    The half of the reviewed test that was always right, with the premature
    payment removed from under it: these slots were bought while (a) held, so the
    refusal here is about one artifact rather than about the whole contract. The
    pre-send refund stays available, which is what keeps real money recoverable.
    """
    request, reader, _ = await prepared(session_maker, paid=True)
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert "voucher_production_validity_unproven" in plan.reasons
    assert VALIDITY_CAPABILITY_UNPROVEN not in plan.reasons
    reader.template["is_enabled"] = False
    reader.template["validity"] = None
    reader.meta_templates = [new.meta_template(status="REJECTED")]
    mutator = old.FakeMutator(
        refund=_ok_response(0),
        reader=reader,
        settles=await new.marker_orders(session_maker, batch_id=request.batch_id, status="refunded"),
    )
    refunded = await _apply(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1, mutator=mutator)
    assert refunded.outcome == "applied"
    assert len(mutator.refund_calls) == 1


async def test_a_historical_batch_is_not_moved_under_the_new_blocker(
    session_maker, production_configuration, binding_key
):
    """F1. §45.1: the €15 contracts keep their own terms and their own recovery."""
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze, _full_create

    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    reader = old.FakeReader(count=1)
    request = old.production_request(run_id=run_id)
    frozen = await _freeze(session_maker, reader, request, count=1)
    assert frozen.outcome == "frozen"
    assert frozen.as_safe_dict()["delivery_blockers"] == []
    request = replace(request, batch_id=frozen.batch["batch_id"])
    report, mutator = await _full_create(session_maker, reader, request, count=1, batch_id=request.batch_id)
    assert report.outcome == "applied", report.reasons
    assert len(mutator.create_calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"is_single_charge": False},
        {"validity": None},
        {"validity": 2},
        {"validity": True},
        {"validity": "1"},
        {"uuid": old.EASYWEEK_VOUCHER_TEMPLATE_UUID},
        {"is_enabled": False},
        {"forces_activation": False},
        {"activate_after": 1},
        {"activate_at": "2026-10-12T00:00:00Z"},
        {"is_connected_all_services": False},
        {"services_count": 42},
        {"cost": 1500},
        {"value": 1500},
    ],
)
async def test_new_contract_drift_refuses_before_any_external_mutation(
    session_maker, production_configuration, binding_key, changes
):
    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader(template=new.template_payload(**changes))
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=new.production_request(run_id=run_id),
        approval=new.approval_for(1),
    )
    assert not plan.ready and BASELINE_DRIFT in plan.reasons
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists


@pytest.mark.parametrize("status", ["PENDING", "REJECTED"])
async def test_live_meta_refuses_pending_or_rejected(session_maker, production_configuration, binding_key, status):
    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader(meta_templates=[new.meta_template(status=status)])
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=new.production_request(run_id=run_id),
        approval=new.approval_for(1),
    )
    assert not plan.ready
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists


@pytest.mark.parametrize("where", ["total", "subtotal", "artifact_price", "artifact_value"])
async def test_wrong_1500_minor_units_cannot_be_paid(
    session_maker, production_configuration, binding_key, issued_validity_capability, where
):
    request, reader, _ = await prepared(session_maker)
    order = reader.orders[old.ORDER_UUIDS[0]]
    if where.startswith("artifact_"):
        order["vouchers"][0][where.removeprefix("artifact_")] = 1500
    else:
        order[where] = 1500
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)
    assert not plan.ready


async def test_historical_request_cannot_authorize_new_batch(
    session_maker, production_configuration, binding_key, issued_validity_capability
):
    request, reader, _ = await prepared(session_maker)
    legacy = old.production_request(run_id=request.preview_run_id, batch_id=request.batch_id)
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=legacy)
    assert not plan.ready and IDENTITY_BINDING_MISMATCH in plan.reasons


async def test_unknown_payment_reconciles_new_amount_with_no_second_pay(
    session_maker, production_configuration, binding_key, issued_validity_capability
):
    request, reader, _ = await prepared(session_maker)
    mutator = old.FakeMutator(pay=EasyWeekVoucherMutationUnknown("timeout"))
    report = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=mutator)
    assert report.outcome == "unknown" and report.halted
    assert len(mutator.pay_calls) == 1
    reader.orders.update(await new.marker_orders(session_maker, batch_id=request.batch_id, status="paid"))
    reader.template["is_enabled"] = False
    report = await runner.run_reconcile(session_maker, request=request, order_reader=reader)
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert snapshot.items[0].status == "paid"
    assert not snapshot.halted
    assert len(mutator.pay_calls) == 1
    assert not report.reconciliation_required


@pytest.mark.parametrize("change", ["amount_paid", "amount_due", "total", "subtotal"])
async def test_wrong_paid_invoice_blocks_send_and_stays_refundable(
    session_maker, production_configuration, binding_key, issued_validity_capability, monkeypatch, change
):
    monkeypatch.setattr(runner, "issued_voucher_validity_reason", lambda payload, *, now: None)
    request, reader, _ = await prepared(session_maker, paid=True)
    reader.orders[old.ORDER_UUIDS[0]]["invoice"][change] = 1500
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert "voucher_production_order_not_paid" in plan.reasons
    refund = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1)
    assert refund.ready, refund.reasons


async def test_new_contract_does_not_reset_historical_customer_entitlement(
    session_maker, production_configuration, binding_key
):
    # A legacy frozen batch already owns the period entitlement. Schema 3 must
    # not turn a second preview for the same customer into a second gift.
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    old_run, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, old.FakeReader(), old.production_request(run_id=old_run), count=1)
    assert frozen.outcome == "frozen"
    new_run, _ = await old.seed_production_preview(session_maker, count=1)
    plan = await _plan(
        session_maker,
        new.FakeReader(),
        stage=STAGE_FREEZE,
        request=new.production_request(run_id=new_run),
        approval=new.approval_for(1),
    )
    assert not plan.ready
    assert "voucher_production_entitlement_already_exists" in plan.reasons
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=new_run)).exists


async def test_optout_after_pay_still_refuses_new_message(
    session_maker, production_configuration, binding_key, issued_validity_capability, monkeypatch
):
    from altegio_bot.models.models import CampaignRecipient, Client

    monkeypatch.setattr(runner, "issued_voucher_validity_reason", lambda payload, *, now: None)
    request, reader, ids = await prepared(session_maker, paid=True)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, ids[0])
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert "voucher_production_recipient_opted_out" in plan.reasons
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert snapshot.items[0].send_attempt_count == 0


async def test_stop_preserves_new_paid_slots_without_sending(
    session_maker, production_configuration, binding_key, issued_validity_capability, monkeypatch
):
    monkeypatch.setattr(runner, "issued_voucher_validity_reason", lambda payload, *, now: None)
    request, reader, _ = await prepared(session_maker, paid=True)
    await ledger.request_stop(session_maker, batch_id=request.batch_id, requested_by="synthetic-operator")
    sender = old.FakeSender()
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender, honour_stop=True)
    assert report.stopped
    assert sender.calls == 0
    assert report.batch["items"][0]["send_attempt_count"] == 0


async def test_expiry_rechecked_after_plan_before_send_claim(
    session_maker, production_configuration, binding_key, issued_validity_capability, monkeypatch
):
    request, reader, _ = await prepared(session_maker, paid=True)
    checks = 0

    def evidence(payload, *, now):
        nonlocal checks
        checks += 1
        # Both the offered and rebuilt approval have positive synthetic evidence;
        # time changes before the final exact-order read preceding the send claim.
        return "voucher_production_voucher_expired" if checks >= 3 else None

    monkeypatch.setattr(runner, "issued_voucher_validity_reason", evidence)
    sender = old.FakeSender()
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert sender.calls == 0
    assert "voucher_production_voucher_expired" in report.reasons
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert snapshot.items[0].send_attempt_count == 0


async def test_new_create_unknown_recovers_only_matching_1000_order(
    session_maker, production_configuration, binding_key, issued_validity_capability
):
    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader()
    request = new.production_request(run_id=run_id)
    frozen = await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=new.approval_for(1))
    request = replace(request, batch_id=frozen.batch["batch_id"])
    mutator = old.FakeMutator(create=EasyWeekVoucherMutationUnknown("timeout"))
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    assert report.outcome == "unknown"
    orders = await new.marker_orders(session_maker, batch_id=request.batch_id)
    for order in orders.values():
        order["created_at"] = runner.utcnow().isoformat()
    reader.orders = orders
    reader.order_pages = [old.orders_page(list(orders.values()))]
    reader.template["is_enabled"] = False
    report = await runner.run_reconcile(session_maker, request=request, order_reader=reader)
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert snapshot.items[0].status == "created", report.reasons
    assert snapshot.items[0].voucher_binding_recorded
    assert len(mutator.create_calls) == 1


@pytest.mark.parametrize(
    "drift",
    [
        "uuid",
        "slug",
        "currency",
        "missing_currency",
        "unreadable_workspace",
        "missing_location",
        "duplicate_location",
        "malformed_location",
    ],
)
async def test_live_workspace_currency_and_location_evidence_required(
    session_maker, production_configuration, binding_key, drift
):
    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader()
    if drift in ("uuid", "slug", "currency"):
        reader.workspace[drift] = "different"
    elif drift == "missing_currency":
        del reader.workspace["currency"]
    elif drift == "unreadable_workspace":
        reader.workspace = TimeoutError()
    elif drift == "missing_location":
        reader.locations = []
    elif drift == "duplicate_location":
        reader.locations *= 2
    else:
        reader.locations.append("malformed")
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=new.production_request(run_id=run_id),
        approval=new.approval_for(1),
    )
    assert not plan.ready
    assert set(plan.reasons) & {"voucher_production_workspace_unproven", "voucher_production_location_unproven"}
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists


async def test_environment_drift_blocks_new_payment_but_not_frozen_refund(
    session_maker, production_configuration, binding_key, issued_validity_capability
):
    request, reader, _ = await prepared(session_maker, paid=True)
    reader.workspace = TimeoutError()
    reader.locations = TimeoutError()
    reads = len(reader.environment_reads)
    deliver = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not deliver.ready
    assert "voucher_production_workspace_unproven" in deliver.reasons
    assert len(reader.environment_reads) == reads + 3
    reads = len(reader.environment_reads)
    refund = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1)
    assert refund.ready, refund.reasons
    assert len(reader.environment_reads) == reads


@pytest.mark.parametrize(
    "drift", ["wrong_configured_uuid", "missing", "duplicate", "error", "malformed", "partial_collection"]
)
async def test_current_product_requires_approved_account_and_live_branch_membership(
    session_maker, production_configuration, binding_key, drift
):
    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader()
    request = new.production_request(run_id=run_id)
    if drift == "wrong_configured_uuid":
        request = replace(request, payment_account_uuid="00000000-0000-4000-8000-000000000001")
        reader.accounts = [{"uuid": request.payment_account_uuid}]
    elif drift == "missing":
        reader.accounts = []
    elif drift == "duplicate":
        reader.accounts *= 2
    elif drift == "error":
        reader.accounts = TimeoutError()
    elif drift == "partial_collection":
        reader.accounts = {"data": reader.accounts, "meta": {"next_page": 2}}
    else:
        reader.accounts.append("malformed")
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=new.approval_for(1))
    assert not plan.ready
    assert "voucher_production_account_unproven" in plan.reasons
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists


async def test_new_environment_reads_and_message_send_hold_no_database_transaction(
    session_maker, production_configuration, binding_key, issued_validity_capability, monkeypatch
):
    from altegio_bot.tests.test_easyweek_voucher_production_read_sessions import _ObservedTransport, _tracked_factory

    tracked, sessions = _tracked_factory(session_maker)
    calls = []
    actual_reader = new.FakeReader()
    reader = _ObservedTransport(actual_reader, sessions, calls)
    request, _, _ = await prepared(tracked, paid=True, reader=reader)
    monkeypatch.setattr(runner, "issued_voucher_validity_reason", lambda payload, *, now: None)
    sender = _ObservedTransport(old.FakeSender(), sessions, calls)
    report = await _apply(tracked, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert report.outcome == "applied"
    assert {
        "get_workspace",
        "list_locations",
        "list_location_accounts",
        "list_meta_templates",
        "send_voucher_template",
    } <= set(calls)


@pytest.mark.parametrize("recover_unknown", [False, True])
async def test_documented_paid_status_and_exact_total_prove_payment_without_invoice(
    session_maker, production_configuration, binding_key, issued_validity_capability, recover_unknown
):
    request, reader, _ = await prepared(session_maker)
    paid_orders = await new.marker_orders(session_maker, batch_id=request.batch_id, status="paid")
    for order in paid_orders.values():
        del order["invoice"]
        # A documented status plus exact order subtotal/total and the bound
        # 1000-minor-unit artifact is a complete proof path. Do not require API
        # invoice fields the provider does not publish for this response shape.
        order.pop("is_paid")
    mutator = old.FakeMutator(
        pay=EasyWeekVoucherMutationUnknown("timeout") if recover_unknown else _ok_response(0),
        reader=reader,
        settles=paid_orders,
    )
    report = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=mutator)
    if recover_unknown:
        assert report.outcome == "unknown"
        reader.orders.update(paid_orders)
        report = await runner.run_reconcile(session_maker, request=request, order_reader=reader)
        assert not report.reconciliation_required
    else:
        assert report.outcome == "applied"
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert snapshot.items[0].status == "paid"
    assert snapshot.items[0].voucher_binding_recorded
    assert not snapshot.halted
    assert len(mutator.pay_calls) == 1
