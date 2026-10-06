"""PR-21 production proofs keep earned evidence and manual decisions distinct."""

from __future__ import annotations

import copy
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_manual_batch import MANUAL_POLICY
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import voucher_code_mac
from altegio_bot.campaigns.easyweek_voucher_production import ledger, runner
from altegio_bot.campaigns.easyweek_voucher_production.composition import (
    prove_production_composition,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    CUSTOMER_IDENTITY_NOT_CURRENT,
    FROZEN_DIGEST_MISMATCH,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
    binding_material,
)
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID
from altegio_bot.models.models import (
    CampaignRecipient,
    Client,
    EasyWeekEvent,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.settings import settings
from altegio_bot.tests import easyweek_voucher_delivery_fixtures as earned
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    CUSTOMER_UUIDS,
    ORDER_UUIDS,
    PHONES,
    VOUCHER_CODE_SENTINELS,
    FakeMutator,
    FakeReader,
    FakeSender,
    approval_for,
    customer_payload,
    customers_page,
    marker_orders,
    production_request,
    seed_production_preview,
    seed_template_and_sender,
)
from altegio_bot.tests.test_easyweek_voucher_production_mailing import (
    _apply,
    _freeze,
    _full_create,
    _full_pay,
    _ok_response,
    _plan,
)
from altegio_bot.utils import utcnow


class MixedReader(FakeReader):
    def __init__(self):
        super().__init__(count=2)
        self.booking = earned.booking_payload(customer={"uuid": CUSTOMER_UUIDS[0]})
        self.histories = {CUSTOMER_UUIDS[0]: [self.booking], CUSTOMER_UUIDS[1]: []}

    async def get_booking(self, booking_uuid):
        assert booking_uuid == str(earned.BOOKING_UUID)
        return self.booking

    async def list_customer_bookings(self, customer_uuid, page, per_page=100):
        assert page == 1 and per_page == 100
        answer = self.histories[customer_uuid]
        if isinstance(answer, Exception):
            raise answer
        return earned.history_page(answer)


async def mixed_preview(session_maker, monkeypatch, *, policy=True):
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Wimpernverlängerung"]')
    run_id, earned_id = await earned.seed_recipient(session_maker, phone=PHONES[0])
    _, manual_ids = await seed_production_preview(session_maker, count=1, offset=1)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, manual_ids[0])
        recipient.campaign_run_id = run_id
        if policy:
            recipient.manual_policy = MANUAL_POLICY
            recipient.manual_policy_checked_at = utcnow()
            recipient.manual_operator_attested_at = utcnow()
    await seed_template_and_sender(session_maker)
    return run_id, [earned_id, manual_ids[0]], MixedReader()


async def test_mixed_freeze_create_pay_deliver_keeps_separate_proofs(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, _, reader = await mixed_preview(session_maker, monkeypatch)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    assert frozen.batch["recipient_basis"] == "mixed"
    assert frozen.batch["first_visit_proof"] == "per_recipient"
    assert frozen.batch["earned_recipient_count"] == frozen.batch["manual_recipient_count"] == 1
    assert frozen.batch["items"][0]["source_proof_digest"]
    assert frozen.batch["items"][1]["manual_policy"] == MANUAL_POLICY
    request = production_request(run_id=run_id, batch_id=batch_id)
    created, _ = await _full_create(session_maker, reader, request, count=2, batch_id=batch_id)
    paid, _ = await _full_pay(session_maker, reader, request, count=2, batch_id=batch_id)
    sender = FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert created.outcome == paid.outcome == delivered.outcome == "applied"
    assert delivered.batch["provider_accepted_count"] == 2
    assert delivered.batch["webhook_delivered_count"] == delivered.batch["webhook_read_count"] == 0
    assert sender.calls == 2


async def test_earned_only_freezes_with_live_source_evidence(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    async with session_maker() as session, session.begin():
        manual = await session.get(CampaignRecipient, ids[1])
        manual.status = "skipped"
    report = await _freeze(session_maker, reader, production_request(run_id=run_id), count=1)
    assert report.batch["recipient_basis"] == "earned_first_visit"
    assert report.batch["items"][0]["source_proof_digest"]


@pytest.mark.parametrize("failure", ["canceled", "future", "source", "history_timeout"])
async def test_earned_live_failures_refuse_entire_composition(
    session_maker, production_configuration, binding_key, monkeypatch, failure
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    if failure == "canceled":
        reader.booking["is_canceled"] = True
    elif failure == "future":
        reader.histories[CUSTOMER_UUIDS[0]].append(
            {
                **reader.booking,
                "uuid": str(uuid.uuid4()),
                "is_completed": False,
                "start_time": (utcnow() + timedelta(days=3)).isoformat(),
            }
        )
    elif failure == "history_timeout":
        reader.histories[CUSTOMER_UUIDS[0]] = TimeoutError()
    else:
        async with session_maker() as session, session.begin():
            recipient = await session.get(CampaignRecipient, ids[0])
            event = await session.get(EasyWeekEvent, recipient.source_easyweek_event_id)
            event.status = "failed"
    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )
    assert not plan.ready
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_DELIVER])
@pytest.mark.parametrize("booking_status", ["completed", "future", "cancelled"])
async def test_new_manual_history_blocks_every_irreversible_stage(
    session_maker, production_configuration, binding_key, monkeypatch, stage, booking_status
):
    run_id, _, reader = await mixed_preview(session_maker, monkeypatch)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    if stage in (STAGE_PAY, STAGE_DELIVER):
        await _full_create(session_maker, reader, request, count=2, batch_id=batch_id)
    if stage == STAGE_DELIVER:
        await _full_pay(session_maker, reader, request, count=2, batch_id=batch_id)
    reader.histories[CUSTOMER_UUIDS[1]] = [
        {
            **reader.booking,
            "uuid": str(uuid.uuid4()),
            "customer": {"uuid": CUSTOMER_UUIDS[1]},
            "is_completed": booking_status == "completed",
            "is_canceled": booking_status == "cancelled",
            "start_time": (utcnow() + timedelta(days=2)).isoformat(),
        }
    ]
    plan = await _plan(session_maker, reader, stage=stage, request=request)
    assert not plan.ready
    assert "manual_recipient_history_nonempty" in plan.reasons
    snapshot = await ledger.load(session_maker, batch_id=batch_id)
    assert snapshot.recipient_count == 2 and snapshot.total_exposure_minor == 3000
    assert all(item.send_attempt_count == 0 for item in snapshot.items)
    if stage == STAGE_DELIVER:
        refunds = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
        mutator = FakeMutator(refund=_ok_response(1), reader=reader, settles=refunds)
        refund = await _apply(session_maker, reader, stage=STAGE_REFUND, request=request, slot=2, mutator=mutator)
        assert refund.outcome == "applied"


async def test_legacy_manual_policy_does_not_silently_gain_zero_history_requirement(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, _, reader = await mixed_preview(session_maker, monkeypatch, policy=False)
    reader.histories[CUSTOMER_UUIDS[1]] = TimeoutError()
    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )
    assert plan.ready, plan.reasons


async def test_changed_manual_attestation_after_freeze_is_drift(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    async with session_maker() as session, session.begin():
        manual = await session.get(CampaignRecipient, ids[1])
        manual.manual_operator_attested_at += timedelta(seconds=1)
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_CREATE,
        request=production_request(run_id=run_id, batch_id=frozen.batch["batch_id"]),
    )
    assert FROZEN_DIGEST_MISMATCH in plan.reasons


async def test_old_batch_keeps_its_composition_and_voucher_hmac(session_maker, production_configuration, binding_key):
    run_id, _ = await seed_production_preview(session_maker, count=1)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=1)
    async with session_maker() as session:
        composition = await prove_production_composition(
            session,
            preview_run_id=run_id,
            client_reader=reader,
            now=utcnow(),
            approval=approval_for(1),
            schema_version="1",
        )
    identity = runner._identity_from_composition(production_request(run_id=run_id), composition)
    outcome = await ledger.freeze_batch(
        session_maker,
        identity=identity,
        approved_recipient_count=1,
        approved_exposure_minor=1500,
        freeze_plan_digest="a" * 64,
    )
    batch_id = outcome.snapshot.batch_id
    assert outcome.snapshot.schema_version == "1"
    request = production_request(run_id=run_id, batch_id=batch_id)
    report, _ = await _full_create(session_maker, reader, request, count=1, batch_id=batch_id)
    assert report.outcome == "applied"
    _, mac = voucher_code_mac(
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        ledger_uuid=binding_material(batch_id=batch_id, slot=1),
        target_order_uuid=ORDER_UUIDS[0],
        voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        domain=ledger.VOUCHER_PRODUCTION_DOMAIN,
    )
    async with session_maker() as session:
        stored = await session.scalar(select(EasyWeekVoucherProductionBatchItem.voucher_code_hmac))
    assert stored == mac
    assert await ledger.binding_matches(
        session_maker,
        batch_id=batch_id,
        slot=1,
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        target_order_uuid=ORDER_UUIDS[0],
    )


def test_v2_hmac_binds_basis_policy_and_source_without_changing_v1():
    base = dict(
        batch_id=9,
        slot=1,
        schema_version="2",
        frozen_digest="a" * 64,
        recipient_basis="earned_first_visit",
        source_proof_digest="b" * 64,
        customer_uuid=CUSTOMER_UUIDS[0],
    )
    current = binding_material(**base)
    for change in (
        {"recipient_basis": "operator_manual_selection"},
        {"manual_policy": MANUAL_POLICY},
        {"source_proof_digest": "c" * 64},
        {"customer_uuid": CUSTOMER_UUIDS[1]},
    ):
        assert binding_material(**{**base, **change}) != current
    assert binding_material(batch_id=9, slot=1) == "easyweek_voucher_production_mailing_v1:9:1"


@pytest.mark.parametrize("field", ["basis", "policy", "source", "customer"])
async def test_frozen_item_proof_tampering_cannot_borrow_live_recipient_proof(
    session_maker, production_configuration, binding_key, monkeypatch, field
):
    run_id, _, reader = await mixed_preview(session_maker, monkeypatch)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    async with session_maker() as session, session.begin():
        item = await session.scalar(
            select(EasyWeekVoucherProductionBatchItem).where(
                EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                EasyWeekVoucherProductionBatchItem.slot == (2 if field == "policy" else 1),
            )
        )
        if field == "basis":
            item.recipient_basis = "operator_manual_selection"
            item.source_booking_uuid = item.source_proof_digest = None
        elif field == "policy":
            item.manual_policy = None
        elif field == "source":
            item.source_proof_digest = "e" * 64
        else:
            item.easyweek_customer_uuid = uuid.UUID(CUSTOMER_UUIDS[2])
    plan = await _plan(
        session_maker, reader, stage=STAGE_CREATE, request=production_request(run_id=run_id, batch_id=batch_id)
    )
    assert not plan.ready
    assert FROZEN_DIGEST_MISMATCH in plan.reasons


async def test_an_equally_valid_earned_source_substitution_changes_frozen_proof(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, ids[0])
        source = await session.get(EasyWeekEvent, recipient.source_easyweek_event_id)
        replacement = EasyWeekEvent(
            status=source.status,
            event_hint=source.event_hint,
            body_truncated=False,
            payload=dict(source.payload),
            payload_hash="different-event",
        )
        session.add(replacement)
        await session.flush()
        recipient.source_easyweek_event_id = replacement.id
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_CREATE,
        request=production_request(run_id=run_id, batch_id=frozen.batch["batch_id"]),
    )
    assert plan.snapshot["composition"]["composition_proven"]
    assert FROZEN_DIGEST_MISMATCH in plan.reasons


def test_pending_v1_stage_plan_does_not_gain_v2_permission(monkeypatch):
    from altegio_bot.campaigns.easyweek_voucher_production import authorisation

    now = utcnow()
    material = dict(stage=STAGE_FREEZE, snapshot={"target_slots": [1]}, ledger_state={}, issued_at=now)
    with monkeypatch.context() as old:
        old.setattr(authorisation, "PRODUCTION_SCHEMA_VERSION", "1")
        old_digest = authorisation.stage_digest(**material)
    plan = authorisation.StagePlan(ready=True, reasons=(), digest=authorisation.stage_digest(**material), **material)
    reasons = authorisation.verify_plan_authorisation(
        plan,
        supplied_digest=old_digest,
        supplied_issued_at=now,
        supplied_phrase=authorisation.phrase_for_digest(STAGE_FREEZE, old_digest),
        now=now,
    )
    assert "voucher_production_plan_digest_mismatch" in reasons


@pytest.mark.parametrize("drift", ["basis", "source", "opt_out", "manual_policy"])
async def test_local_drift_after_live_proof_is_refused_inside_durable_claim(
    session_maker, production_configuration, binding_key, monkeypatch, drift
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    if drift == "manual_policy":
        async with session_maker() as session, session.begin():
            recipient = await session.get(CampaignRecipient, ids[0])
            recipient.status = "skipped"
    count = 1 if drift == "manual_policy" else 2
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    original = ledger.claim_create

    async def racing_claim(*args, **kwargs):
        async with session_maker() as session, session.begin():
            recipient = await session.get(CampaignRecipient, ids[1] if drift == "manual_policy" else ids[0])
            if drift == "basis":
                recipient.recipient_basis = "operator_manual_selection"
                recipient.easyweek_customer_uuid = uuid.UUID(CUSTOMER_UUIDS[0])
                recipient.source_booking_uuid = None
                recipient.source_easyweek_event_id = recipient.source_record_id = None
                recipient.source_visits_total = recipient.source_visits_total_updated_at = None
            elif drift == "source":
                recipient.source_booking_uuid = uuid.uuid4()
            elif drift == "opt_out":
                recipient.is_opted_out = True
            else:
                recipient.manual_policy = recipient.manual_policy_checked_at = recipient.manual_operator_attested_at = (
                    None
                )
        return await original(*args, **kwargs)

    monkeypatch.setattr(ledger, "claim_create", racing_claim)
    mutator = FakeMutator(create=_ok_response(0))
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    assert report.outcome == "refused"
    assert mutator.calls == []
    current = await ledger.load(session_maker, batch_id=batch_id)
    assert current.recipient_count == count
    assert all(item.status == "planned" for item in current.items)


async def _assert_legacy_earned_identity(session_maker, recipient_id):
    async with session_maker() as session:
        recipient = await session.get(CampaignRecipient, recipient_id)
        client = await session.get(Client, recipient.client_id)
        assert client.easyweek_customer_uuid is None


def _change_second_booking_read(reader, monkeypatch):
    calls = 0

    async def get_booking(booking_uuid):
        nonlocal calls
        assert booking_uuid == str(earned.BOOKING_UUID)
        calls += 1
        payload = copy.deepcopy(reader.booking)
        if calls % 2 == 0:
            payload["customer"] = {"uuid": CUSTOMER_UUIDS[2]}
        return payload

    monkeypatch.setattr(reader, "get_booking", get_booking)


async def test_earned_second_booking_read_cannot_spend_another_customers_history(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    await _assert_legacy_earned_identity(session_maker, ids[0])
    _change_second_booking_read(reader, monkeypatch)
    report = await _apply(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=production_request(run_id=run_id),
        approval=approval_for(2),
        expect_ready=False,
    )
    assert report.outcome == "refused"
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists


async def test_earned_second_read_customer_drift_after_approval_causes_zero_mutations(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    await _assert_legacy_earned_identity(session_maker, ids[0])
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    request = production_request(run_id=run_id, batch_id=frozen.batch["batch_id"])
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert plan.ready
    _change_second_booking_read(reader, monkeypatch)
    mutator = FakeMutator()
    async with session_maker() as session:
        report = await runner.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "refused" and mutator.calls == []
    snapshot = await ledger.load(session_maker, batch_id=request.batch_id)
    assert all(item.status == "planned" for item in snapshot.items)


async def test_reordered_booking_dictionary_keys_preserve_the_same_earned_proof(
    session_maker, production_configuration, binding_key, monkeypatch
):
    run_id, _, reader = await mixed_preview(session_maker, monkeypatch)
    calls = 0

    def reorder(value):
        if isinstance(value, dict):
            return {key: reorder(child) for key, child in reversed(list(value.items()))}
        if isinstance(value, list):
            return [reorder(child) for child in value]
        return value

    async def get_booking(booking_uuid):
        nonlocal calls
        assert booking_uuid == str(earned.BOOKING_UUID)
        calls += 1
        return reorder(reader.booking) if calls % 2 == 0 else copy.deepcopy(reader.booking)

    monkeypatch.setattr(reader, "get_booking", get_booking)
    result = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    assert result.outcome == "frozen"


@pytest.mark.parametrize("failure", ["card_phone", "stable_customer_swap", "ambiguous", "incomplete"])
async def test_earned_customer_must_be_proven_for_the_destination_phone(
    session_maker, production_configuration, binding_key, monkeypatch, failure
):
    run_id, ids, reader = await mixed_preview(session_maker, monkeypatch)
    await _assert_legacy_earned_identity(session_maker, ids[0])
    if failure == "card_phone":
        reader.customers[CUSTOMER_UUIDS[0]] = customer_payload(0, phone=PHONES[2])
    elif failure == "stable_customer_swap":
        reader.booking = {**reader.booking, "customer": {"uuid": CUSTOMER_UUIDS[2]}}
        reader.histories[CUSTOMER_UUIDS[2]] = [reader.booking]
        reader.customers[CUSTOMER_UUIDS[2]] = customer_payload(2)
    elif failure == "ambiguous":
        reader.customer_pages[PHONES[0]] = [
            customers_page(
                [
                    customer_payload(0),
                    customer_payload(2, phone=PHONES[0]),
                ]
            )
        ]
    else:
        reader.customer_pages[PHONES[0]] = [customers_page([], total=1)]
    report = await _apply(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=production_request(run_id=run_id),
        approval=approval_for(2),
        expect_ready=False,
    )
    assert report.outcome == "refused" and CUSTOMER_IDENTITY_NOT_CURRENT in report.reasons
    assert not (await ledger.load_for_preview(session_maker, campaign_run_id=run_id)).exists
