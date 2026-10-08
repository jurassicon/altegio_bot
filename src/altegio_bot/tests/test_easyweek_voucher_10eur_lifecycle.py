"""§45 full fixed-product lifecycle, with the voucher term answered by EasyWeek.

§45.4. The ordinary issued artifact these fixtures build carries NO activation and
NO expiry field, because that is what EasyWeek returns. Under this contract that is
the normal shape of a correct voucher, so the positive lifecycle below runs on the
real production functions with no capability modelled and no validity guard
replaced — the only mocks are the external APIs themselves. What still refuses is
EasyWeek SAYING a voucher is unusable, which the negative tests here exercise
through the artifact rather than through a patched boundary.
"""

from __future__ import annotations

import copy
import json
from dataclasses import replace

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_manual_batch import MANUAL_POLICY
from altegio_bot.campaigns.easyweek_voucher_production import ledger, runner
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    BASELINE_DRIFT,
    IDENTITY_BINDING_MISMATCH,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
    STOP_TERMINAL,
)
from altegio_bot.campaigns.easyweek_voucher_production.validity import (
    PROVIDER_MANAGED_VALIDITY,
    VOUCHER_EXPIRED,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown
from altegio_bot.easyweek_voucher_production_contract import CURRENT_PRODUCTION_CONTRACT as CONTRACT
from altegio_bot.models.models import CampaignRecipient, EasyWeekVoucherProductionBatchAttempt, MessageTemplate
from altegio_bot.settings import settings
from altegio_bot.tests import easyweek_voucher_10eur_fixtures as new
from altegio_bot.tests import easyweek_voucher_delivery_fixtures as earned
from altegio_bot.tests import easyweek_voucher_production_fixtures as old
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import ui_confirm, ui_execute, ui_frozen, ui_plan
from altegio_bot.tests.test_easyweek_voucher_production_mailing import _apply, _ok_response, _plan


async def prepared(session_maker, *, paid=False, reader=None, count=1):
    """Freeze, create and optionally pay, on the real production path."""
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
    session_maker, production_configuration, binding_key, monkeypatch
):
    # Test the full delivery machinery under a separately mocked positive evidence
    # result. This is not a claim that the real API expiry schema is established.
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


async def test_an_artifact_without_provider_dates_completes_the_whole_lifecycle(
    session_maker, production_configuration, binding_key
):
    """§45.4, the main positive scenario. No capability modelled, no guard replaced.

    Supersedes ``test_unproven_validity_capability_refuses_create_and_pay_before_any_money``,
    which asserted that this exact artifact closed CREATE and PAY. The evidence has
    not changed — EasyWeek still publishes no activation or expiry field, and these
    orders still carry none — the OWNER'S DECISION has: the provider answers for the
    term, so an artifact that simply says nothing about dates is a correct one.

    Everything else about the contract is still proven here, on the real functions:
    the exact product, the exact nominal, one voucher and one payment per recipient,
    one Meta attempt per recipient, and the money being N × 1000 and nothing else.
    """
    run_id, _ = await old.seed_production_preview(session_maker, count=2)
    await new.seed_template_and_sender(session_maker)
    reader = new.FakeReader(count=2)
    request = new.production_request(run_id=run_id)

    frozen = await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=new.approval_for(2))
    assert frozen.outcome == "frozen", frozen.reasons
    # Nothing contract-level stands between this composition and a send any more.
    assert frozen.as_safe_dict()["delivery_blockers"] == []
    request = replace(request, batch_id=frozen.batch["batch_id"])
    orders = await new.marker_orders(session_maker, batch_id=request.batch_id)
    # The premise, stated rather than assumed: not one artifact carries a date.
    for order in orders.values():
        for voucher in order["vouchers"]:
            assert not {"activated_at", "expires_at", "valid_until", "is_expired"} & set(voucher)
    reader.orders.update(orders)

    create = old.FakeMutator(create_sequence=[_ok_response(index) for index in range(2)])
    created = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=create)
    assert created.outcome == "applied", created.reasons
    pay = old.FakeMutator(
        pay_sequence=[_ok_response(index) for index in range(2)],
        reader=reader,
        settles=await new.marker_orders(session_maker, batch_id=request.batch_id, status="paid"),
    )
    paid = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=pay)
    assert paid.outcome == "applied", paid.reasons
    sender = old.FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert delivered.outcome == "applied", delivered.reasons

    # Exactly one voucher and one payment per recipient, and exactly one message.
    assert len(create.create_calls) == len(pay.pay_calls) == sender.calls == 2
    assert not pay.refund_calls
    # The exact product and the exact nominal, on every call.
    assert {call["voucher_template_uuid"] for call in create.create_calls} == {CONTRACT.template_uuid}
    assert {call["price_minor"] for call in create.create_calls} == {1000}
    assert delivered.batch["total_exposure_minor"] == 2 * 1000
    assert delivered.batch["voucher_unit_price_minor"] == 1000
    # One Meta attempt per recipient, under the new contract's own message code.
    async with session_maker() as session:
        attempts = list((await session.scalars(select(EasyWeekVoucherProductionBatchAttempt))).all())
    assert len(attempts) == 2
    assert {attempt.template_code for attempt in attempts} == {CONTRACT.message_code}
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert {item.send_attempt_count for item in snapshot.items} == {1}
    assert not snapshot.reconciliation_required


class _MixedTenEuroReader(new.FakeReader):
    """The €10 product, read by a preview holding one earned and one manual row."""

    def __init__(self) -> None:
        super().__init__(count=2)
        self.booking = earned.booking_payload(customer={"uuid": old.CUSTOMER_UUIDS[0]})
        self.histories = {old.CUSTOMER_UUIDS[0]: [self.booking], old.CUSTOMER_UUIDS[1]: []}

    async def get_booking(self, booking_uuid):
        assert booking_uuid == str(earned.BOOKING_UUID)
        return self.booking

    async def list_customer_bookings(self, customer_uuid, page, per_page=100):
        assert page == 1 and per_page == 100
        return earned.history_page(self.histories[customer_uuid])


async def _mixed_preview(session_maker, monkeypatch):
    """One preview whose two slots are proven in the two different ways (§44)."""
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Wimpernverlängerung"]')
    run_id, earned_id = await earned.seed_recipient(session_maker, phone=old.PHONES[0])
    _, manual_ids = await old.seed_production_preview(session_maker, count=1, offset=1)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, manual_ids[0])
        recipient.campaign_run_id = run_id
        recipient.manual_policy = MANUAL_POLICY
        recipient.manual_policy_checked_at = runner.utcnow()
        recipient.manual_operator_attested_at = runner.utcnow()
    await new.seed_template_and_sender(session_maker)
    return run_id, _MixedTenEuroReader()


async def test_a_mixed_earned_and_manual_audience_completes_the_new_contract(
    session_maker, production_configuration, binding_key, monkeypatch
):
    """§45.4 with §44's audience: both bases buy and send under the €10 contract.

    The two slots are proven in different ways — one by a live booking, one by an
    operator's attested manual decision — and neither of them carries an issued
    voucher date. Both complete, each costs exactly 1000 minor units, and the money
    is N × 1000 and nothing else.

    It also proves the thing no report may ever contain: the voucher code goes to
    Meta and appears nowhere else — not in the stage report, not in the ledger
    snapshot, not in the batch's own diagnostics.
    """
    run_id, reader = await _mixed_preview(session_maker, monkeypatch)
    request = new.production_request(run_id=run_id)
    frozen = await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=new.approval_for(2))
    assert frozen.outcome == "frozen", frozen.reasons
    assert frozen.batch["recipient_basis"] == "mixed"
    assert frozen.batch["earned_recipient_count"] == frozen.batch["manual_recipient_count"] == 1
    assert frozen.batch["voucher_unit_price_minor"] == 1000
    assert frozen.as_safe_dict()["delivery_blockers"] == []

    request = replace(request, batch_id=frozen.batch["batch_id"])
    reader.orders.update(await new.marker_orders(session_maker, batch_id=request.batch_id))
    create = old.FakeMutator(create_sequence=[_ok_response(index) for index in range(2)])
    created = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=create)
    assert created.outcome == "applied", created.reasons
    pay = old.FakeMutator(
        pay_sequence=[_ok_response(index) for index in range(2)],
        reader=reader,
        settles=await new.marker_orders(session_maker, batch_id=request.batch_id, status="paid"),
    )
    settled = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=pay)
    assert settled.outcome == "applied", settled.reasons
    sender = old.FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    assert delivered.outcome == "applied", delivered.reasons
    assert len(create.create_calls) == len(pay.pay_calls) == sender.calls == 2
    assert {call["price_minor"] for call in create.create_calls} == {1000}
    assert delivered.batch["total_exposure_minor"] == 2 * 1000
    assert delivered.batch["provider_accepted_count"] == 2
    # Both messages carried a real code to Meta...
    assert sender.saw_codes == [True, True]
    # ...and no surface an operator or a log can read contains one.
    status = await runner.run_status(session_maker, batch_id=request.batch_id)
    surfaces = json.dumps([delivered.as_safe_dict(), status.as_safe_dict()], default=str)
    assert not [code for code in old.VOUCHER_CODE_SENTINELS if code in surfaces]
    assert "voucher_code_omitted" in surfaces


async def test_the_whole_report_surface_names_easyweek_and_never_a_proof(
    session_maker, production_configuration, binding_key
):
    """§45.4. No report may say this application proved an issued voucher's dates.

    The old ``issued_validity_capability_proven`` boolean is gone rather than pinned
    to ``true``, which is the point: there is no field left that could carry a
    fictional proof, and the one that replaced it names the responsible party.
    """
    request, reader, _ = await prepared(session_maker, paid=True)
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert plan.ready, plan.reasons
    prerequisites = plan.snapshot["prerequisites"]
    assert prerequisites["issued_voucher_validity"] == PROVIDER_MANAGED_VALIDITY
    assert "issued_validity_capability_proven" not in prerequisites
    assert prerequisites["delivery_blockers"] == []
    assert "issued_validity_proven" not in json.dumps(plan.as_safe_dict())
    # A historical contract is described as out of scope, not as a passed check.
    legacy = await _plan(
        session_maker,
        old.FakeReader(count=1),
        stage=STAGE_FREEZE,
        request=old.production_request(run_id=request.preview_run_id),
        approval=old.approval_for(1),
    )
    assert legacy.snapshot["prerequisites"]["issued_voucher_validity"] == "not_applicable"


async def test_an_operation_queued_before_a_terminal_stop_cannot_execute_after_it(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """§45.4. A stop reached an already-confirmed operation; nothing is bought.

    Re-points ``test_an_approval_taken_while_the_capability_held_cannot_execute_without_it``
    at the guard that still matters. The shape is the one that test established —
    the executor rebuilds the plan live and the stored approval carries no
    permission — applied to the owner's new rule instead of a capability that no
    longer exists.
    """
    run_id, batch_id, reader = await ui_frozen(ui_client, session_maker, transports, count=1)
    mutator = old.FakeMutator(create_sequence=[_ok_response(0)])
    transports.use(reader=reader, mutator=mutator)
    offer = await ui_plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    assert (await ui_confirm(ui_client, offer))[0] == 200

    # Stopped after the operation was durable and before the executor claimed it.
    state = await ledger.request_stop(session_maker, batch_id=batch_id, requested_by="synthetic-operator")
    assert state.active and state.terminal

    finished = await ui_execute(session_maker)
    assert finished is not None
    assert finished.status == "refused", finished.result
    assert finished.finished_at is not None
    assert STOP_TERMINAL in (finished.reason_codes or [])
    assert mutator.create_calls == [] and mutator.pay_calls == []
    snapshot = await ledger.load(session_maker, batch_id=batch_id)
    assert {item.status for item in snapshot.items} == {"planned"}
    assert not snapshot.reconciliation_required


async def test_a_voucher_easyweek_calls_expired_blocks_delivery_but_not_refund(
    session_maker, production_configuration, binding_key
):
    """§45.4. The supported invalidity signal still refuses, and money still returns.

    Replaces ``test_an_issued_voucher_with_an_unproven_term_blocks_delivery_but_not_refund``,
    whose premise was the absence of a date. The invariant it was really protecting
    survives unchanged and is asserted here through the signal the provider actually
    publishes: EasyWeek saying the voucher is expired.
    """
    request, reader, _ = await prepared(session_maker, paid=True)
    for order in reader.orders.values():
        if isinstance(order, dict):
            for voucher in order.get("vouchers") or []:
                voucher["is_expired"] = True
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert VOUCHER_EXPIRED in plan.reasons
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


async def test_an_unreadable_artifact_is_not_a_missing_date(session_maker, production_configuration, binding_key):
    """§45.4. "No optional date" and "cannot read the code" are different answers.

    The first is now allowed; the second never was and still is not. The refusal
    comes from the artifact and binding checks, which is where it belongs — the
    validity answer deliberately says nothing about an artifact it cannot see.
    """
    request, reader, _ = await prepared(session_maker, paid=True)
    for order in reader.orders.values():
        if isinstance(order, dict):
            order["vouchers"] = []
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert "voucher_production_artifact_unproven" in plan.reasons
    assert VOUCHER_EXPIRED not in plan.reasons
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert {item.send_attempt_count for item in snapshot.items} == {0}


async def test_a_historical_batch_keeps_its_own_contract(session_maker, production_configuration, binding_key):
    """§45.1 and §45.4: the €15 contracts keep their own terms and their own recovery."""
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
async def test_wrong_1500_minor_units_cannot_be_paid(session_maker, production_configuration, binding_key, where):
    request, reader, _ = await prepared(session_maker)
    order = reader.orders[old.ORDER_UUIDS[0]]
    if where.startswith("artifact_"):
        order["vouchers"][0][where.removeprefix("artifact_")] = 1500
    else:
        order[where] = 1500
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)
    assert not plan.ready


async def test_historical_request_cannot_authorize_new_batch(session_maker, production_configuration, binding_key):
    request, reader, _ = await prepared(session_maker)
    legacy = old.production_request(run_id=request.preview_run_id, batch_id=request.batch_id)
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=legacy)
    assert not plan.ready and IDENTITY_BINDING_MISMATCH in plan.reasons


async def test_unknown_payment_reconciles_new_amount_with_no_second_pay(
    session_maker, production_configuration, binding_key
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
    session_maker, production_configuration, binding_key, monkeypatch, change
):
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
    session_maker, production_configuration, binding_key, monkeypatch
):
    from altegio_bot.models.models import CampaignRecipient, Client

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


async def test_a_terminal_stop_preserves_new_paid_slots_without_sending(
    session_maker, production_configuration, binding_key
):
    """§45.4. A stopped €10 batch is refused where it used to pause.

    Under §43.6 this plan was still built and the stage reported ``stopped`` per
    slot. The owner's rule makes the stop terminal, so the refusal now comes first
    — and the paid slots are left exactly as they are. A stop marks nothing unsent,
    refunded or cancelled on EasyWeek's side.
    """
    request, reader, _ = await prepared(session_maker, paid=True)
    state = await ledger.request_stop(session_maker, batch_id=request.batch_id, requested_by="synthetic-operator")
    assert state.active and state.terminal
    sender = old.FakeSender()
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert STOP_TERMINAL in plan.reasons
    report = await _apply(
        session_maker,
        reader,
        stage=STAGE_DELIVER,
        request=request,
        sender=sender,
        honour_stop=True,
        expect_ready=False,
    )
    assert report.outcome == "refused"
    assert STOP_TERMINAL in report.reasons
    assert sender.calls == 0
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert {item.status for item in snapshot.items} == {"paid"}
    assert {item.send_attempt_count for item in snapshot.items} == {0}
    assert not snapshot.reconciliation_required


class _ExpiresAfterReads(new.FakeReader):
    """EasyWeek marks the voucher expired between the plan and the send claim.

    The flip is in the MOCKED PROVIDER, not in the guard: §45.4's boundary two is
    only worth testing if the same real function answers both times and the WORLD
    is what changed in between.

    ``expire_after`` is how many order reads still see a usable voucher once it is
    set; ``None`` is never. The flip is returned as a copy so the stored order is
    untouched and the count stays the only thing that decides the answer.
    """

    def __init__(self, *, count: int = 1) -> None:
        super().__init__(count=count)
        self.expire_after: int | None = None
        self.reads = 0

    async def get_order(self, order_uuid: str):
        payload = await super().get_order(order_uuid)
        self.reads += 1
        if self.expire_after is None or self.reads <= self.expire_after:
            return payload
        payload = copy.deepcopy(payload)
        for voucher in payload.get("vouchers") or []:
            voucher["is_expired"] = True
        return payload


async def test_invalidity_rechecked_after_plan_before_send_claim(session_maker, production_configuration, binding_key):
    """§45.4. Both DELIVER boundaries ask the same question of the same artifact.

    The plan is built against a usable voucher and accepted; EasyWeek then calls
    that voucher expired, and the final read before the claim refuses it. One
    attempt is never spent, and no message leaves.
    """
    reader = _ExpiresAfterReads()
    request, reader, _ = await prepared(session_maker, paid=True, reader=reader)
    # Counting from here: the offered DELIVER plan reads the order once, the plan
    # the stage rebuilds reads it again, and the third read is the one the send
    # claim is about to act on.
    reader.reads, reader.expire_after = 0, 2
    sender = old.FakeSender()
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert sender.calls == 0
    assert VOUCHER_EXPIRED in report.reasons
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert snapshot.items[0].send_attempt_count == 0


async def test_new_create_unknown_recovers_only_matching_1000_order(
    session_maker, production_configuration, binding_key
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
    session_maker, production_configuration, binding_key
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
    session_maker, production_configuration, binding_key, monkeypatch
):
    from altegio_bot.tests.test_easyweek_voucher_production_read_sessions import _ObservedTransport, _tracked_factory

    tracked, sessions = _tracked_factory(session_maker)
    calls = []
    actual_reader = new.FakeReader()
    reader = _ObservedTransport(actual_reader, sessions, calls)
    request, _, _ = await prepared(tracked, paid=True, reader=reader)
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
    session_maker, production_configuration, binding_key, recover_unknown
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
