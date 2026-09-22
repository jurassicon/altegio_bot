"""The controlled EasyWeek voucher snapshot batch (§41 / PR-18).

§37.2 proved the whole irreversible sequence for one manually selected person.
This phase repeats it for a bounded handful, and everything these tests guard
follows from that one change:

* the batch is one to five people and never six, never zero and never twice;
* €15 each and €75 in total are arithmetic the database enforces, not policy;
* the composition is frozen once, deterministically ordered, and any drift
  afterwards costs zero new external calls;
* nothing external happens before a claim is committed;
* the first unknown stops every slot after it, and is never retried;
* a delivery is one attempt per slot, for the lifetime of the row;
* and no voucher code, phone number, name or customer UUID appears anywhere
  except in memory.

Every identity here is synthetic, and the voucher codes are sentinels the
secrecy tests hunt for by name.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid as uuid_module
from datetime import timedelta

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from altegio_bot.campaigns.configuration import (
    CAMPAIGN_EXECUTION_NOT_AUTHORIZED,
    resolve_campaign_readiness,
)
from altegio_bot.campaigns.easyweek_voucher_batch import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_batch import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_batch.composition import prove_batch_composition
from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    BASELINE_DRIFT,
    BATCH_ALREADY_EXISTS,
    BATCH_DISABLED,
    BATCH_HALTED,
    BATCH_SCOPE,
    BOOKING_LINK_UNPROVEN,
    COMPOSITION_EMPTY,
    COMPOSITION_MIXED_BASIS,
    COMPOSITION_TOO_LARGE,
    CONFIRMATION_MISMATCH,
    ENTITLEMENT_ALREADY_EXISTS,
    FROZEN_DIGEST_MISMATCH,
    HALTED_BY_PREDECESSOR,
    LEDGER_STATE_UNEXPECTED,
    MAX_EXPOSURE_MINOR,
    MAX_RECIPIENTS,
    PLAN_DIGEST_MISMATCH,
    PLAN_EXPIRED,
    PREVIEW_ALREADY_CONSUMED,
    RECIPIENT_OPTED_OUT,
    REFUND_FORBIDDEN_AFTER_SEND,
    RUN_UNPROVEN,
    SENDER_UNPROVEN,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
    TEMPLATE_UNPROVEN,
    UNIT_PRICE_MINOR,
    batch_marker,
)
from altegio_bot.campaigns.provider import (
    EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED,
    CampaignProviderRefusal,
    require_campaign_execution_provider,
)
from altegio_bot.easyweek_client import EasyWeekPermanentError
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_TEST,
    VOUCHER_BATCH_COMPLETED,
    VOUCHER_BATCH_HALTED,
    VOUCHER_BATCH_IN_PROGRESS,
    VOUCHER_BATCH_ITEM_CREATE_CLAIMED,
    VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
    VOUCHER_BATCH_ITEM_CREATED,
    VOUCHER_BATCH_ITEM_DELIVERED,
    VOUCHER_BATCH_ITEM_PAID,
    VOUCHER_BATCH_ITEM_PLANNED,
    VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_BATCH_ITEM_READ,
    CampaignRecipient,
    EasyWeekManualVoucherDeliveryLedger,
    EasyWeekVoucherSnapshotBatch,
    EasyWeekVoucherSnapshotBatchAttempt,
    EasyWeekVoucherSnapshotBatchItem,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.tests.easyweek_voucher_batch_fixtures import (  # noqa: F401 - fixtures
    ACCOUNT_UUID,
    BOOKING_LINK,
    COMPANY_ID,
    CUSTOMER_NAMES,
    CUSTOMER_UUIDS,
    ORDER_UUIDS,
    PERIOD_END,
    PERIOD_START,
    PHONES,
    PROVIDER_MESSAGE_IDS,
    STAFFER_UUID,
    VOUCHER_CODE_SENTINELS,
    FakeMutator,
    FakeReader,
    FakeSender,
    accepted_outcome,
    batch_request,
    customer_payload,
    customers_page,
    issued_voucher,
    location_map,
    marker_orders,
    markers_for,
    rejected_outcome,
    seed_batch_preview,
    seed_template_and_sender,
    template_payload,
    unknown_outcome,
    voucher_order,
)
from altegio_bot.utils import utcnow

_RUNNERS = {
    STAGE_FREEZE: runner_module.run_freeze,
    STAGE_CREATE: runner_module.run_create,
    STAGE_PAY: runner_module.run_pay,
    STAGE_DELIVER: runner_module.run_deliver,
    STAGE_REFUND: runner_module.run_refund,
}


async def _plan(session_maker, reader, *, stage, request, slot=None):
    async with session_maker() as session:
        plan, *_ = await runner_module.build_stage_plan(
            session,
            session_maker,
            stage=stage,
            request=request,
            reader=reader,
            order_reader=reader,
            slot=slot,
        )
    return plan


async def _apply(session_maker, reader, *, stage, request, slot=None, expect_ready=True, **extra):
    """Plan the stage live, then apply it with that plan's own approval.

    Exactly what the runbook asks an operator to do, and the only path any of
    these tests ever uses to make something happen.
    """
    plan = await _plan(session_maker, reader, stage=stage, request=request, slot=slot)
    if expect_ready:
        assert plan.ready, plan.reasons
    common = {
        "request": request,
        "reader": reader,
        "order_reader": reader,
        "apply": True,
        "supplied_digest": plan.digest,
        "supplied_issued_at": plan.issued_at,
        "supplied_phrase": plan.confirmation_phrase,
    }
    if stage == STAGE_REFUND:
        common["slot"] = slot
    async with session_maker() as session:
        return await _RUNNERS[stage](session, session_maker, **common, **extra)


async def _freeze(session_maker, reader, request):
    return await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request)


def _ok_response(index: int) -> VoucherMutationResponse:
    """A 2xx whose body names the order. A claim, never a proof."""
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _full_create(session_maker, reader, request, *, count):
    """Freeze and create every slot, leaving the batch at ``created``."""
    await _freeze(session_maker, reader, request)
    mutator = FakeMutator(create_sequence=[_ok_response(index) for index in range(count)])
    reader.orders = await marker_orders(session_maker, count=count)
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    return report, mutator


async def _full_pay(session_maker, reader, request, *, count):
    """Pay every created slot, with the orders open until the POST settles them.

    The plan requires an OPEN order and the readback requires a PAID one, which
    is the real sequence; the mutator flips each order as it pays it.
    """
    paid = await marker_orders(session_maker, count=count, status="paid")
    mutator = FakeMutator(
        pay_sequence=[_ok_response(index) for index in range(count)],
        reader=reader,
        settles=paid,
    )
    report = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=mutator)
    return report, mutator


# ===========================================================================
# Composition: one to five, and nothing else
# ===========================================================================


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5])
async def test_a_batch_of_one_to_five_freezes_with_deterministic_slots(
    session_maker, batch_configuration, binding_key, count
) -> None:
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)

    report = await _freeze(session_maker, reader, batch_request(run_id=run_id))

    assert report.outcome == "frozen"
    assert report.external_effect_attempted is False
    batch = report.batch
    assert batch["recipient_count"] == count
    assert batch["voucher_unit_price_minor"] == UNIT_PRICE_MINOR
    assert batch["total_exposure_minor"] == UNIT_PRICE_MINOR * count
    assert batch["total_exposure_minor"] <= MAX_EXPOSURE_MINOR
    # Slots are 1..N in preview row order, and the pairing is derived, never
    # chosen: two operators freezing the same snapshot get the same batch.
    assert [entry["slot"] for entry in batch["items"]] == list(range(1, count + 1))
    assert [entry["campaign_recipient_id"] for entry in batch["items"]] == recipient_ids
    assert [entry["reconciliation_marker"] for entry in batch["items"]] == [
        batch_marker(preview_run_id=run_id, campaign_recipient_id=rid, slot=slot)
        for slot, rid in enumerate(recipient_ids, start=1)
    ]


async def test_an_empty_snapshot_is_not_a_small_batch(session_maker, batch_configuration, binding_key) -> None:
    run_id, _ = await seed_batch_preview(session_maker, count=0)
    await seed_template_and_sender(session_maker)

    plan = await _plan(session_maker, FakeReader(count=0), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert COMPOSITION_EMPTY in plan.reasons


async def test_a_sixth_recipient_refuses_the_whole_batch(session_maker, batch_configuration, binding_key) -> None:
    """Refused whole, never truncated to five.

    A tool that quietly took the first five would be deciding who gets €15 and
    who does not.
    """
    run_id, _ = await seed_batch_preview(session_maker, count=MAX_RECIPIENTS + 1)
    await seed_template_and_sender(session_maker)

    plan = await _plan(
        session_maker,
        FakeReader(count=MAX_RECIPIENTS + 1),
        stage=STAGE_FREEZE,
        request=batch_request(run_id=run_id),
    )

    assert not plan.ready
    assert COMPOSITION_TOO_LARGE in plan.reasons
    # And nothing was written, so the batch table is still empty.
    assert not (await ledger_module.load(session_maker)).exists


async def test_a_second_batch_is_refused_and_then_impossible(session_maker, batch_configuration, binding_key) -> None:
    run_a, _ = await seed_batch_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    await _freeze(session_maker, FakeReader(count=2), batch_request(run_id=run_a))

    # A second, entirely different preview.
    run_b, _ = await seed_batch_preview(session_maker, count=2, customer_uuids=[CUSTOMER_UUIDS[5], CUSTOMER_UUIDS[6]])
    plan = await _plan(session_maker, FakeReader(count=2), stage=STAGE_FREEZE, request=batch_request(run_id=run_b))
    assert not plan.ready
    assert BATCH_ALREADY_EXISTS in plan.reasons

    # And the schema says the same thing without relying on that check at all:
    # the scope is unique AND pinned to one literal, so the table holds one row.
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherSnapshotBatch))).scalar_one()
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO easyweek_voucher_snapshot_batches "
                        "(batch_scope, request_schema_version, baseline_version, provider, company_id, "
                        " campaign_code, recipient_basis, campaign_run_id, campaign_period_start, "
                        " campaign_period_end, location_uuid, staffer_uuid, payment_account_uuid, "
                        " voucher_template_uuid, frozen_digest, recipient_count, voucher_unit_price_minor, "
                        " total_exposure_minor, status, frozen_at) "
                        "VALUES (:scope, '1', :baseline, 'easyweek', 322579, 'new_clients_monthly', "
                        " 'operator_manual_selection', :run, :start, :end, :loc, :staffer, :account, "
                        " :template, 'x', 1, 1500, 1500, 'frozen', now())"
                    ),
                    {
                        "scope": BATCH_SCOPE,
                        "baseline": row.baseline_version,
                        "run": row.campaign_run_id,
                        "start": PERIOD_START,
                        "end": PERIOD_END,
                        "loc": str(row.location_uuid),
                        "staffer": str(row.staffer_uuid),
                        "account": str(row.payment_account_uuid),
                        "template": str(row.voucher_template_uuid),
                    },
                )


async def test_a_mixed_snapshot_is_refused_rather_than_filtered(
    session_maker, batch_configuration, binding_key
) -> None:
    """An earned candidate beside a manual one refuses the batch.

    Serving "the manual ones" would be this tool deciding which of the
    operator's rows counted.
    """
    run_id, _ = await seed_batch_preview(
        session_maker,
        count=2,
        bases=["operator_manual_selection", RECIPIENT_BASIS_TEST],
        customer_uuids=[CUSTOMER_UUIDS[0], None],
    )
    await seed_template_and_sender(session_maker)

    plan = await _plan(session_maker, FakeReader(count=2), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert COMPOSITION_MIXED_BASIS in plan.reasons


async def test_one_human_cannot_occupy_two_slots(session_maker, batch_configuration, binding_key) -> None:
    """Guarded twice, in two different tables, and neither relies on the other.

    The shared preview table already refuses two active rows for one EasyWeek
    customer in one run, which is why a composition can never be built that way
    in the first place. This phase does not depend on remembering that: its own
    items carry the same rule again, per batch. The composition's
    ``COMPOSITION_DUPLICATE_CUSTOMER`` reason is the third layer, and it is
    deliberately unreachable while the first two hold.
    """
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=1)
    await seed_template_and_sender(session_maker)

    # 1. The preview cannot hold the same customer twice.
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    CampaignRecipient(
                        campaign_run_id=run_id,
                        provider=PROVIDER_EASYWEEK,
                        company_id=COMPANY_ID,
                        phone_e164=PHONES[1],
                        display_name=CUSTOMER_NAMES[1],
                        status="candidate",
                        recipient_basis="operator_manual_selection",
                        easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[0]),
                    )
                )

    # 2. And neither can the batch, whatever a preview might one day allow.
    await _freeze(session_maker, FakeReader(count=1), batch_request(run_id=run_id))
    async with session_maker() as session:
        batch = (await session.execute(select(EasyWeekVoucherSnapshotBatch))).scalar_one()
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO easyweek_voucher_snapshot_batch_items "
                        "(batch_id, batch_recipient_count, slot, provider, company_id, campaign_code, "
                        " recipient_basis, campaign_run_id, campaign_recipient_id, easyweek_customer_uuid, "
                        " campaign_period_start, campaign_period_end, voucher_value_minor, voucher_quantity, "
                        " reconciliation_marker, status) "
                        "VALUES (:batch, 1, 1, 'easyweek', 322579, 'new_clients_monthly', "
                        " 'operator_manual_selection', :run, :recipient, :customer, :start, :end, "
                        " 1500, 1, 'ewvb1-duplicate', 'planned')"
                    ),
                    {
                        "batch": batch.id,
                        "run": run_id,
                        "recipient": recipient_ids[0],
                        "customer": CUSTOMER_UUIDS[0],
                        "start": PERIOD_START,
                        "end": PERIOD_END,
                    },
                )


async def test_a_recipient_of_the_historical_canary_cannot_be_reused(
    session_maker, batch_configuration, binding_key
) -> None:
    """The §37.2 canary's preview and person are history, not a fresh snapshot."""
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekManualVoucherDeliveryLedger(
                    canary_scope="easyweek_manual_voucher_canary_v1",
                    request_schema_version="1",
                    baseline_version="2026-09-15-42",
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    campaign_code="new_clients_monthly",
                    recipient_basis="operator_manual_selection",
                    campaign_run_id=run_id,
                    campaign_recipient_id=recipient_ids[0],
                    easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[0]),
                    campaign_period_start=PERIOD_START,
                    campaign_period_end=PERIOD_END,
                    location_uuid=uuid_module.UUID("8395fab6-7ee8-4702-88d9-fd78f92539c1"),
                    staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                    payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                    voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                    reconciliation_marker="ewmv1-deadbeefcafe",
                    status="read",
                    evidence={},
                    created_at=utcnow(),
                    updated_at=utcnow(),
                )
            )

    plan = await _plan(session_maker, FakeReader(count=2), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert PREVIEW_ALREADY_CONSUMED in plan.reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"provider": PROVIDER_ALTEGIO},
        {"company_id": 758285},
        {"campaign_code": "newsletter_new_clients_monthly"},
        {"run_mode": "send-real"},
        {"run_status": "running"},
    ],
)
async def test_a_foreign_run_is_refused_before_any_live_read(
    session_maker, batch_configuration, binding_key, changes
) -> None:
    run_id, _ = await seed_batch_preview(session_maker, count=1, **changes)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=1)

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert RUN_UNPROVEN in plan.reasons
    # Refused from the database alone: no customer was read for any of them.
    assert reader.customer_calls == []
    assert reader.listing_calls == []


async def test_an_opted_out_member_refuses_the_whole_batch(session_maker, batch_configuration, binding_key) -> None:
    run_id, _ = await seed_batch_preview(session_maker, count=3, opted_out=[False, True, False])
    await seed_template_and_sender(session_maker)

    plan = await _plan(session_maker, FakeReader(count=3), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert RECIPIENT_OPTED_OUT in plan.reasons


# ===========================================================================
# What the schema itself refuses
# ===========================================================================


async def _frozen_batch(session_maker, *, count=3):
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    await _freeze(session_maker, FakeReader(count=count), batch_request(run_id=run_id))
    return run_id, recipient_ids


async def test_the_schema_refuses_a_sixth_slot_and_a_slot_outside_the_size(
    session_maker, batch_configuration, binding_key
) -> None:
    run_id, recipient_ids = await _frozen_batch(session_maker, count=2)
    async with session_maker() as session:
        batch = (await session.execute(select(EasyWeekVoucherSnapshotBatch))).scalar_one()

    async def _insert(slot: int, declared: int) -> None:
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    EasyWeekVoucherSnapshotBatchItem(
                        batch_id=batch.id,
                        batch_recipient_count=declared,
                        slot=slot,
                        provider=PROVIDER_EASYWEEK,
                        company_id=COMPANY_ID,
                        campaign_code="new_clients_monthly",
                        recipient_basis="operator_manual_selection",
                        campaign_run_id=run_id,
                        campaign_recipient_id=recipient_ids[0],
                        easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[7]),
                        campaign_period_start=PERIOD_START,
                        campaign_period_end=PERIOD_END,
                        voucher_value_minor=UNIT_PRICE_MINOR,
                        voucher_quantity=1,
                        reconciliation_marker=f"ewvb1-extra{slot}{declared}",
                        status=VOUCHER_BATCH_ITEM_PLANNED,
                        evidence={},
                        created_at=utcnow(),
                        updated_at=utcnow(),
                    )
                )

    # A slot beyond the batch's own declared size: the CHECK refuses it.
    with pytest.raises(IntegrityError):
        await _insert(3, 2)
    # Claiming a bigger size to make room: the composite FK has no such batch.
    with pytest.raises(IntegrityError):
        await _insert(6, 6)


async def test_the_schema_refuses_a_wrong_price_or_quantity(session_maker, batch_configuration, binding_key) -> None:
    await _frozen_batch(session_maker, count=1)
    for column, value in (("voucher_value_minor", 1400), ("voucher_quantity", 2)):
        async with session_maker() as session:
            with pytest.raises(IntegrityError):
                async with session.begin():
                    await session.execute(
                        text(f"UPDATE easyweek_voucher_snapshot_batch_items SET {column} = :value"),
                        {"value": value},
                    )


async def test_the_schema_refuses_an_exposure_that_is_not_the_product(
    session_maker, batch_configuration, binding_key
) -> None:
    await _frozen_batch(session_maker, count=2)
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(text("UPDATE easyweek_voucher_snapshot_batches SET total_exposure_minor = 1500"))
    # And the count itself cannot exceed five.
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE easyweek_voucher_snapshot_batches SET recipient_count = 6, total_exposure_minor = 9000"
                    )
                )


async def test_the_schema_refuses_an_impossible_state_transition(
    session_maker, batch_configuration, binding_key
) -> None:
    await _frozen_batch(session_maker, count=1)
    statements = [
        # Paid before the order was proven to exist.
        "UPDATE easyweek_voucher_snapshot_batch_items SET pay_claimed_at = now(), pay_attempted_at = now()",
        # Sent before the payment was verified.
        "UPDATE easyweek_voucher_snapshot_batch_items "
        "SET send_claimed_at = now(), send_attempted_at = now(), send_attempt_count = 1",
        # Delivered without acceptance.
        "UPDATE easyweek_voucher_snapshot_batch_items SET delivered_at = now()",
        # Read without delivery.
        "UPDATE easyweek_voucher_snapshot_batch_items SET read_at = now()",
        # A status outside the closed vocabulary.
        "UPDATE easyweek_voucher_snapshot_batch_items SET status = 'almost_paid'",
        # A second delivery attempt.
        "UPDATE easyweek_voucher_snapshot_batch_items SET send_attempt_count = 2",
        # A MAC without the key that made it.
        "UPDATE easyweek_voucher_snapshot_batch_items SET voucher_code_hmac = 'abc'",
    ]
    for statement in statements:
        async with session_maker() as session:
            with pytest.raises(IntegrityError):
                async with session.begin():
                    await session.execute(text(statement))


async def test_the_schema_refuses_another_branch_a_basis_and_a_provider(
    session_maker, batch_configuration, binding_key
) -> None:
    await _frozen_batch(session_maker, count=1)
    statements = [
        "UPDATE easyweek_voucher_snapshot_batches SET company_id = 758285",
        "UPDATE easyweek_voucher_snapshot_batches SET provider = 'altegio'",
        "UPDATE easyweek_voucher_snapshot_batches SET campaign_code = 'newsletter_new_clients_monthly'",
        "UPDATE easyweek_voucher_snapshot_batches SET recipient_basis = 'earned_first_visit'",
        "UPDATE easyweek_voucher_snapshot_batch_items SET company_id = 758285",
        "UPDATE easyweek_voucher_snapshot_batch_items SET recipient_basis = 'owner_test_account'",
    ]
    for statement in statements:
        async with session_maker() as session:
            with pytest.raises(IntegrityError):
                async with session.begin():
                    await session.execute(text(statement))


async def test_one_person_one_period_one_voucher(session_maker, batch_configuration, binding_key) -> None:
    """A fresh preview of the same wave cannot hand the same person a second €15."""
    run_id, recipient_ids = await _frozen_batch(session_maker, count=1)
    async with session_maker() as session:
        batch = (await session.execute(select(EasyWeekVoucherSnapshotBatch))).scalar_one()
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                # A different batch id is impossible, so the entitlement rule is
                # exercised by a second slot naming the same human being.
                await session.execute(
                    text(
                        "INSERT INTO easyweek_voucher_snapshot_batch_items "
                        "(batch_id, batch_recipient_count, slot, provider, company_id, campaign_code, "
                        " recipient_basis, campaign_run_id, campaign_recipient_id, easyweek_customer_uuid, "
                        " campaign_period_start, campaign_period_end, voucher_value_minor, voucher_quantity, "
                        " reconciliation_marker, status) "
                        "VALUES (:batch, 1, 1, 'easyweek', 322579, 'new_clients_monthly', "
                        " 'operator_manual_selection', :run, :recipient, :customer, :start, :end, "
                        " 1500, 1, 'ewvb1-second', 'planned')"
                    ),
                    {
                        "batch": batch.id,
                        "run": run_id,
                        "recipient": recipient_ids[0],
                        "customer": CUSTOMER_UUIDS[0],
                        "start": PERIOD_START,
                        "end": PERIOD_END,
                    },
                )


async def test_an_entitlement_already_in_the_batch_refuses_a_later_freeze(
    session_maker, batch_configuration, binding_key
) -> None:
    """The named refusal, so an operator reads a reason and not a constraint."""
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=1)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=1)
    async with session_maker() as session:
        composition = await prove_batch_composition(session, preview_run_id=run_id, client_reader=reader, now=utcnow())
    assert composition.proven

    # The composition proves out today; once a batch item holds that customer
    # for that period, the same proof refuses.
    await _freeze(session_maker, reader, batch_request(run_id=run_id))
    async with session_maker() as session:
        again = await prove_batch_composition(session, preview_run_id=run_id, client_reader=reader, now=utcnow())
    assert not again.proven
    assert ENTITLEMENT_ALREADY_EXISTS in again.reasons


# ===========================================================================
# Authorisation
# ===========================================================================


async def test_a_closed_fence_refuses_before_any_read(session_maker, binding_key, monkeypatch) -> None:
    from altegio_bot.settings import settings

    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_account_uuid", ACCOUNT_UUID, raising=False)
    run_id, _ = await seed_batch_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)

    plan = await _plan(session_maker, FakeReader(count=2), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert BATCH_DISABLED in plan.reasons


async def test_a_stage_without_apply_does_nothing(session_maker, batch_configuration, binding_key) -> None:
    run_id, _ = await seed_batch_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=batch_request(run_id=run_id),
            reader=reader,
            order_reader=reader,
            apply=False,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert not (await ledger_module.load(session_maker)).exists


@pytest.mark.parametrize(
    "mutate,expected",
    [
        ("digest", PLAN_DIGEST_MISMATCH),
        ("phrase", CONFIRMATION_MISMATCH),
        ("age", PLAN_EXPIRED),
    ],
)
async def test_a_tampered_or_stale_approval_authorises_nothing(
    session_maker, batch_configuration, binding_key, mutate, expected
) -> None:
    run_id, _ = await _frozen_batch(session_maker, count=2)
    reader = FakeReader(count=2)
    reader.orders = {}
    request = batch_request(run_id=run_id)
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    mutator = FakeMutator(create=_ok_response(0))

    digest, issued_at, phrase = plan.digest, plan.issued_at, plan.confirmation_phrase
    if mutate == "digest":
        digest = "0" * 64
    elif mutate == "phrase":
        phrase = phrase + "x"
    else:
        issued_at = issued_at - timedelta(hours=2)

    async with session_maker() as session:
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=digest,
            supplied_issued_at=issued_at,
            supplied_phrase=phrase,
        )

    assert report.outcome == "refused"
    assert expected in report.reasons
    # The whole point: a refusal is worth nothing if something still went out.
    assert mutator.calls == []
    assert report.external_calls == {"create": 0, "pay": 0, "refund": 0, "meta": 0}


async def test_a_stage_digest_never_authorises_another_stage(session_maker, batch_configuration, binding_key) -> None:
    run_id, _ = await _frozen_batch(session_maker, count=1)
    reader = FakeReader(count=1)
    request = batch_request(run_id=run_id)
    create_plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    mutator = FakeMutator(create=_ok_response(0))

    async with session_maker() as session:
        report = await runner_module.run_pay(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=create_plan.digest,
            supplied_issued_at=create_plan.issued_at,
            supplied_phrase=create_plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert mutator.calls == []


# ===========================================================================
# Prerequisites, proven before the first voucher exists
# ===========================================================================


async def test_a_missing_template_or_sender_stops_the_freeze_and_the_create(
    session_maker, batch_configuration, binding_key
) -> None:
    """Asked before the money moves, when the answer is still free."""
    run_id, _ = await seed_batch_preview(session_maker, count=2)
    # Deliberately no template and no sender row.
    plan = await _plan(session_maker, FakeReader(count=2), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert TEMPLATE_UNPROVEN in plan.reasons
    assert SENDER_UNPROVEN in plan.reasons


async def test_an_unproven_booking_link_stops_the_freeze(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    from altegio_bot.settings import settings

    monkeypatch.setattr(settings, "easyweek_location_map", location_map(booking_link="notalink"), raising=False)
    run_id, _ = await seed_batch_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)

    plan = await _plan(session_maker, FakeReader(count=2), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert BOOKING_LINK_UNPROVEN in plan.reasons


async def test_a_drifted_baseline_stops_the_create(session_maker, batch_configuration, binding_key) -> None:
    run_id, _ = await _frozen_batch(session_maker, count=2)
    reader = FakeReader(count=2, template=template_payload(services=41, all_services=41))
    request = batch_request(run_id=run_id)

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)

    assert not plan.ready
    assert BASELINE_DRIFT in plan.reasons


# ===========================================================================
# The batch itself: five calls at most, and the first unknown stops the rest
# ===========================================================================


async def test_five_slots_cost_exactly_five_creates_five_pays_and_five_sends(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 5
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    create_report, create_mutator = await _full_create(session_maker, reader, request, count=count)
    assert create_report.outcome == "applied"
    assert len(create_mutator.create_calls) == count
    assert create_report.external_calls["create"] == count

    pay_report, pay_mutator = await _full_pay(session_maker, reader, request, count=count)
    assert pay_report.outcome == "applied"
    assert len(pay_mutator.pay_calls) == count

    sender = FakeSender()
    deliver_report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert deliver_report.outcome == "applied"
    assert sender.calls == count
    assert deliver_report.external_calls["meta"] == count
    # Accepted is not delivered. That word belongs to a webhook.
    assert {entry.outcome for entry in deliver_report.slots} == {"provider_accepted"}

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.recipient_count == count
    assert snapshot.total_exposure_minor == UNIT_PRICE_MINOR * count == MAX_EXPOSURE_MINOR
    assert {entry.status for entry in snapshot.items} == {VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED}
    assert {entry.send_attempt_count for entry in snapshot.items} == {1}


async def test_the_first_unknown_create_stops_every_slot_after_it(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 4
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)

    mutator = FakeMutator(
        create_sequence=[
            _ok_response(0),
            EasyWeekVoucherMutationUnknown("timeout"),
            _ok_response(2),
            _ok_response(3),
        ]
    )
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    assert report.outcome == "unknown"
    # Exactly two POSTs: one that worked and one whose answer was lost.
    assert len(mutator.create_calls) == 2
    outcomes = {entry.slot: entry.outcome for entry in report.slots}
    assert outcomes == {1: "created", 2: "unknown", 3: "not_attempted", 4: "not_attempted"}
    assert all(HALTED_BY_PREDECESSOR in entry.reasons for entry in report.slots if entry.slot > 2)

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_HALTED
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_CREATED
    assert snapshot.item(2).status == VOUCHER_BATCH_ITEM_CREATE_UNKNOWN
    assert snapshot.item(3).status == VOUCHER_BATCH_ITEM_PLANNED
    assert snapshot.item(4).status == VOUCHER_BATCH_ITEM_PLANNED


async def test_an_unknown_is_never_retried_by_a_second_command(session_maker, batch_configuration, binding_key) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)
    mutator = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    assert len(mutator.create_calls) == 1

    # The batch is halted, so the very next plan refuses and nothing can be
    # claimed — including the slot that was never attempted.
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert not plan.ready
    assert BATCH_HALTED in plan.reasons

    again = FakeMutator(create=_ok_response(0))
    async with session_maker() as session:
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=again,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "refused"
    assert again.calls == []


async def test_a_proven_rejection_does_not_stop_the_rest_of_the_batch(
    session_maker, batch_configuration, binding_key
) -> None:
    """A refusal the API decided before acting says nothing about the next person."""
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)

    mutator = FakeMutator(create_sequence=[_ok_response(0), EasyWeekPermanentError("refused"), _ok_response(2)])
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    assert len(mutator.create_calls) == count
    assert report.outcome == "partial"
    outcomes = {entry.slot: entry.outcome for entry in report.slots}
    assert outcomes == {1: "created", 2: "rejected", 3: "created"}
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_IN_PROGRESS
    # A proven pre-action refusal leaves nothing to clean up.
    assert snapshot.item(2).manual_cleanup_required is False


async def test_a_crash_after_the_claim_reads_as_it_may_have_gone_out(
    session_maker, batch_configuration, binding_key
) -> None:
    """The claim is committed before the request leaves, and that is visible."""
    run_id, _ = await _frozen_batch(session_maker, count=2)
    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None

    now = utcnow()
    claim = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        slot=1,
        plan_digest="d" * 64,
        create_window_start=now - timedelta(minutes=30),
        create_window_end=now + timedelta(minutes=30),
    )
    assert claim.granted

    # Nothing further ran. What a later process sees is exactly "claimed,
    # outcome unknown" — and a claimed slot is an unresolved one, so the batch
    # is halted and the suffix cannot be claimed.
    after = await ledger_module.load(session_maker)
    assert after.item(1).status == VOUCHER_BATCH_ITEM_CREATE_CLAIMED
    assert after.item(1).manual_cleanup_required is True
    assert after.item(1).reconciliation_required is True
    assert after.status == VOUCHER_BATCH_IN_PROGRESS

    second = await ledger_module.claim_create(
        session_maker,
        identity=identity,
        slot=1,
        plan_digest="e" * 64,
        create_window_start=now,
        create_window_end=now,
    )
    assert not second.granted
    assert second.reason == ledger_module.CLAIM_REFUSED_STATE


# ===========================================================================
# Drift between the freeze and the stage
# ===========================================================================


async def test_a_recipient_removed_after_the_freeze_costs_zero_calls(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 3
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)

    # The editor is blocked once a batch exists, but a direct write models any
    # way the snapshot could stop matching what was approved.
    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_ids[1])
            recipient.status = "skipped"

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert not plan.ready
    assert FROZEN_DIGEST_MISMATCH in plan.reasons

    mutator = FakeMutator(create=_ok_response(0))
    async with session_maker() as session:
        report = await runner_module.run_create(
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
    assert report.outcome == "refused"
    assert mutator.calls == []


async def test_an_opt_out_after_the_freeze_costs_zero_calls(session_maker, batch_configuration, binding_key) -> None:
    count = 2
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    _, create_mutator = await _full_create(session_maker, reader, request, count=count)
    assert len(create_mutator.create_calls) == count

    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_ids[1])
            recipient.is_opted_out = True

    reader.orders = await marker_orders(session_maker, count=count, status="paid")
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)
    assert not plan.ready
    assert RECIPIENT_OPTED_OUT in plan.reasons

    mutator = FakeMutator(pay=_ok_response(0))
    async with session_maker() as session:
        report = await runner_module.run_pay(
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
    assert report.outcome == "refused"
    assert mutator.calls == []


async def test_a_changed_number_before_the_send_costs_zero_meta_calls(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    # The card now answers with a different number than the preview recorded.
    reader.customers[CUSTOMER_UUIDS[1]] = customer_payload(1, phone="+4915199999999")

    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready

    sender = FakeSender()
    async with session_maker() as session:
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=sender,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "refused"
    assert sender.calls == 0


async def test_a_template_switched_off_before_the_send_costs_zero_meta_calls(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    async with session_maker() as session:
        async with session.begin():
            await session.execute(text("UPDATE whatsapp_senders SET is_active = false"))

    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert SENDER_UNPROVEN in plan.reasons


# ===========================================================================
# The voucher artifact
# ===========================================================================


@pytest.mark.parametrize(
    "order_changes",
    [
        {"vouchers": [issued_voucher(0, price=1400)]},
        {"vouchers": [issued_voucher(0), issued_voucher(0)]},
        {"vouchers": []},
        {"vouchers": [issued_voucher(0, quantity=2)]},
        {"vouchers": [issued_voucher(0, code="")]},
    ],
)
async def test_an_unprovable_voucher_line_never_becomes_created(
    session_maker, batch_configuration, binding_key, order_changes
) -> None:
    run_id, _ = await _frozen_batch(session_maker, count=1)
    reader = FakeReader(count=1)
    request = batch_request(run_id=run_id)
    reader.orders = await marker_orders(session_maker, count=1, **order_changes)
    mutator = FakeMutator(create_sequence=[_ok_response(0)])

    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    assert report.outcome == "unknown"
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_CREATE_UNKNOWN
    assert snapshot.item(1).manual_cleanup_required is True
    assert snapshot.item(1).voucher_binding_recorded is False


async def test_the_binding_is_bound_to_this_slot_and_this_order(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)

    assert await ledger_module.binding_matches(
        session_maker, slot=1, voucher_code=VOUCHER_CODE_SENTINELS[0], target_order_uuid=ORDER_UUIDS[0]
    )
    # Another slot's code, the same slot's order: a MAC is only meaningful in
    # the exact place it was made.
    assert not await ledger_module.binding_matches(
        session_maker, slot=1, voucher_code=VOUCHER_CODE_SENTINELS[1], target_order_uuid=ORDER_UUIDS[0]
    )
    # The right code, another slot's order.
    assert not await ledger_module.binding_matches(
        session_maker, slot=1, voucher_code=VOUCHER_CODE_SENTINELS[0], target_order_uuid=ORDER_UUIDS[1]
    )
    # And the same code under another slot's binding.
    assert not await ledger_module.binding_matches(
        session_maker, slot=2, voucher_code=VOUCHER_CODE_SENTINELS[0], target_order_uuid=ORDER_UUIDS[1]
    )


def test_the_batch_binds_a_code_differently_from_the_two_canaries() -> None:
    """One key, three domains. No stored MAC can verify another phase's code."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
        _DOMAIN,
        MANUAL_VOUCHER_DOMAIN,
    )

    assert ledger_module.VOUCHER_BATCH_DOMAIN not in (_DOMAIN, MANUAL_VOUCHER_DOMAIN)
    assert _DOMAIN == b"altegio_bot/easyweek_voucher_delivery/voucher_code/v1"
    assert MANUAL_VOUCHER_DOMAIN == b"altegio_bot/easyweek_manual_voucher/voucher_code/v1"


# ===========================================================================
# Delivery, webhooks and the refund
# ===========================================================================


async def test_the_delivery_attempt_is_one_per_slot_for_the_lifetime_of_the_row(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert sender.calls == count

    # There is no state a second deliver can be planned from.
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    second = FakeSender()
    async with session_maker() as session:
        report = await runner_module.run_deliver(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            sender=second,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "refused"
    assert second.calls == 0


async def test_an_unknown_send_halts_the_batch_and_forbids_a_refund(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    sender = FakeSender(outcomes=[accepted_outcome(0), unknown_outcome(), accepted_outcome(2)])
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    assert report.outcome == "unknown"
    assert sender.calls == 2
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_HALTED
    assert snapshot.item(3).status == VOUCHER_BATCH_ITEM_PAID

    # The customer may already be holding the code. The money stays where it is.
    plan = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=2)
    assert not plan.ready
    assert REFUND_FORBIDDEN_AFTER_SEND in plan.reasons
    # And the database says so independently of the plan.
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text("UPDATE easyweek_voucher_snapshot_batch_items SET refund_claimed_at = now() WHERE slot = 2")
                )


async def test_a_refund_is_one_named_slot_and_only_before_a_send(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    refunded = await marker_orders(session_maker, count=count, status="refunded")
    # Paid when the plan is built and when the claim is taken; reverted only
    # once the POST has actually been made. Anything else would be the test
    # describing a world the runner never sees.
    mutator = FakeMutator(
        refund=_ok_response(1),
        reader=reader,
        settles={ORDER_UUIDS[1]: refunded[ORDER_UUIDS[1]]},
    )

    report = await _apply(session_maker, reader, stage=STAGE_REFUND, request=request, slot=2, mutator=mutator)

    assert report.outcome == "applied"
    assert mutator.calls == ["refund"]
    assert report.external_calls["refund"] == 1
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(2).status == "refunded"
    # The other slot is untouched: there is no refund-everything.
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_PAID


async def test_a_halt_does_not_shut_the_refund_for_an_untouched_slot(
    session_maker, batch_configuration, binding_key
) -> None:
    """The cleanup path must survive the condition that makes it necessary.

    One slot's send goes unknown, which halts the batch. The slot behind it is
    sitting paid and can no longer be delivered — which is precisely when its
    €15 should come back. Blocking the refund because the batch is halted would
    have it exactly the wrong way round.
    """
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    sender = FakeSender(outcomes=[accepted_outcome(0), unknown_outcome()])
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    halted = await ledger_module.load(session_maker)
    assert halted.status == VOUCHER_BATCH_HALTED
    assert halted.item(3).status == VOUCHER_BATCH_ITEM_PAID

    # Slot 3 was never sent for, so its refund is still reachable.
    refunded = await marker_orders(session_maker, count=count, status="refunded")
    mutator = FakeMutator(
        refund=_ok_response(2),
        reader=reader,
        settles={ORDER_UUIDS[2]: refunded[ORDER_UUIDS[2]]},
    )
    report = await _apply(session_maker, reader, stage=STAGE_REFUND, request=request, slot=3, mutator=mutator)

    assert report.outcome == "applied"
    assert mutator.calls == ["refund"]
    after = await ledger_module.load(session_maker)
    assert after.item(3).status == "refunded"
    # The halt itself is untouched: resolving slot 2 is still a human's job.
    assert after.status == VOUCHER_BATCH_HALTED
    # And the slot that WAS sent for stays forbidden, halt or no halt.
    blocked = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=2)
    assert not blocked.ready
    assert REFUND_FORBIDDEN_AFTER_SEND in blocked.reasons

    # Every other stage is still shut while the batch is halted.
    for stage in (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER):
        plan = await _plan(session_maker, reader, stage=stage, request=request)
        assert not plan.ready
        assert BATCH_HALTED in plan.reasons, stage


async def test_a_stage_that_attempted_nothing_does_not_report_partial(
    session_maker, batch_configuration, binding_key
) -> None:
    """ "Partial" would read as though some of it had worked."""
    run_id, _ = await _frozen_batch(session_maker, count=3)
    reader = FakeReader(count=3)
    request = batch_request(run_id=run_id)

    # Every slot is already past this stage, so there is no work at all.
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)
    assert not plan.ready
    assert LEDGER_STATE_UNEXPECTED in plan.reasons

    # And a stage whose first slot cannot be claimed reports a refusal, not a
    # partial success, with no external call behind it.
    report = runner_module._stage_report(
        STAGE_CREATE,
        [
            runner_module.SlotResult(slot=1, outcome="refused", reasons=["x"]),
            runner_module.SlotResult(slot=2, outcome="not_attempted", reasons=[HALTED_BY_PREDECESSOR]),
        ],
        await ledger_module.load(session_maker),
        runner_module.prove_baseline(template_payload()),
        external_calls={"create": 0, "pay": 0, "refund": 0, "meta": 0},
    )
    assert report.outcome == "refused"
    assert report.external_effect_attempted is False


async def test_webhooks_move_a_slot_forward_only_and_never_backwards(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=FakeSender())

    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_BATCH_ITEM_DELIVERED
    )
    assert (await ledger_module.load(session_maker)).item(1).status == VOUCHER_BATCH_ITEM_DELIVERED

    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_BATCH_ITEM_READ
    )
    assert (await ledger_module.load(session_maker)).item(1).status == VOUCHER_BATCH_ITEM_READ

    # A late `delivered` is refused outright: acceptance is what Meta said and a
    # later callback cannot unsay it.
    backwards = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_BATCH_ITEM_DELIVERED
    )
    assert not backwards.applied
    assert backwards.reason == ledger_module.RECORD_WOULD_REGRESS

    # An exact duplicate is idempotent rather than refused, and changes nothing
    # an operator or a report can see — including when it was read.
    before = (await ledger_module.load(session_maker)).item(1)
    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_BATCH_ITEM_READ
    )
    after = (await ledger_module.load(session_maker)).item(1)
    assert after.status == VOUCHER_BATCH_ITEM_READ
    assert after.read_at == before.read_at
    assert after.delivered_at == before.delivered_at
    assert after.provider_accepted_at == before.provider_accepted_at

    # A `read` alone implies `delivered`, because the schema refuses otherwise.
    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[1], status=VOUCHER_BATCH_ITEM_READ
    )
    slot_two = (await ledger_module.load(session_maker)).item(2)
    assert slot_two.status == VOUCHER_BATCH_ITEM_READ
    assert slot_two.delivered_at is not None

    # A callback for a message nobody here sent is not ours.
    foreign = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id="wamid.SOMEONE_ELSE", status=VOUCHER_BATCH_ITEM_READ
    )
    assert not foreign.applied
    assert foreign.reason == ledger_module.RECORD_MISSING_ROW


async def test_a_fully_read_batch_reports_completed(session_maker, batch_configuration, binding_key) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=FakeSender())
    for index in range(count):
        await ledger_module.record_webhook_transition(
            session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[index], status=VOUCHER_BATCH_ITEM_READ
        )

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_COMPLETED
    assert snapshot.reconciliation_required is False


# ===========================================================================
# Reconcile
# ===========================================================================


async def test_reconcile_recovers_a_known_order_and_lifts_the_halt(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)
    mutator = FakeMutator(create_sequence=[_ok_response(0), EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    assert (await ledger_module.load(session_maker)).status == VOUCHER_BATCH_HALTED

    # The order DOES exist; the answer was simply lost. A complete marker walk
    # finds it, and the exact readback proves it.
    #
    # Rebuilt here rather than before the stage: the bounded create window is
    # anchored on the clock at claim time, and a row dated from before that
    # would fall outside it and be correctly ignored.
    reader.orders = await marker_orders(session_maker, count=count)
    reader.order_pages = [
        {
            "data": [reader.orders[ORDER_UUIDS[1]]],
            "meta": {"current_page": 1, "last_page": 1, "per_page": 100},
        }
    ]
    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "observed"
    assert report.external_effect_attempted is False
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(2).status == VOUCHER_BATCH_ITEM_CREATED
    assert snapshot.status == VOUCHER_BATCH_IN_PROGRESS


async def test_reconcile_never_sends_a_second_create_and_stays_unknown(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 1
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = {}
    mutator = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    # The walk completes and finds nothing. Zero matches is NOT "it was not
    # created": the batch stays halted and a human decides.
    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "unknown"
    assert mutator.calls == ["create"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_HALTED
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_CREATE_UNKNOWN
    assert snapshot.item(1).manual_cleanup_required is True


# ===========================================================================
# Concurrency, on the real database
# ===========================================================================


async def test_two_concurrent_freezes_produce_exactly_one_batch(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    request = batch_request(run_id=run_id)

    async def freeze() -> str:
        try:
            report = await _freeze(session_maker, FakeReader(count=count), request)
            return report.outcome
        except IntegrityError:
            # The loser of the race, told by the database rather than by a check.
            return "refused"

    outcomes = await asyncio.gather(freeze(), freeze(), return_exceptions=True)
    assert all(not isinstance(entry, BaseException) for entry in outcomes), outcomes
    async with session_maker() as session:
        assert (await session.scalar(select(func.count()).select_from(EasyWeekVoucherSnapshotBatch))) == 1
        assert (await session.scalar(select(func.count()).select_from(EasyWeekVoucherSnapshotBatchItem))) == count


async def test_two_concurrent_claims_of_one_slot_produce_one_winner(
    session_maker, batch_configuration, binding_key
) -> None:
    run_id, _ = await _frozen_batch(session_maker, count=2)
    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()

    async def claim() -> bool:
        outcome = await ledger_module.claim_create(
            session_maker,
            identity=identity,
            slot=1,
            plan_digest="d" * 64,
            create_window_start=now - timedelta(minutes=30),
            create_window_end=now + timedelta(minutes=30),
        )
        return outcome.granted

    results = await asyncio.gather(claim(), claim())
    assert sorted(results) == [False, True]


async def test_a_freeze_that_wins_the_race_blocks_the_editor(session_maker, batch_configuration, binding_key) -> None:
    from altegio_bot.campaigns.preview_freeze import preview_is_locked_by_any_canary

    run_id, _ = await _frozen_batch(session_maker, count=2)
    async with session_maker() as session:
        assert await preview_is_locked_by_any_canary(session, campaign_run_id=run_id)
        assert await ledger_module.preview_is_locked_by_voucher_batch(session, campaign_run_id=run_id)
        assert not await ledger_module.preview_is_locked_by_voucher_batch(session, campaign_run_id=run_id + 999)


async def test_a_removal_that_wins_the_race_stops_the_freeze(session_maker, batch_configuration, binding_key) -> None:
    """The run lock is taken first, and the recipients are re-read under it."""
    count = 2
    run_id, recipient_ids = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request)
    assert plan.ready

    # The Remove lands between the plan and the transaction.
    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_ids[1])
            recipient.status = "skipped"

    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )
    assert report.outcome == "refused"
    assert not (await ledger_module.load(session_maker)).exists


# ===========================================================================
# Secrecy
# ===========================================================================


async def test_no_report_or_log_ever_carries_a_code_a_phone_or_a_name(
    session_maker, batch_configuration, binding_key, caplog
) -> None:
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    secrets = [
        *VOUCHER_CODE_SENTINELS[:count],
        *PHONES[:count],
        *CUSTOMER_NAMES[:count],
        *CUSTOMER_UUIDS[:count],
        *ORDER_UUIDS[:count],
    ]

    with caplog.at_level(logging.DEBUG):
        freeze_report = await _freeze(session_maker, reader, request)
        mutator = FakeMutator(create_sequence=[_ok_response(index) for index in range(count)])
        reader.orders = await marker_orders(session_maker, count=count)
        create_report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
        pay_report, _ = await _full_pay(session_maker, reader, request, count=count)
        sender = FakeSender()
        deliver_report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
        status_report = await runner_module.run_status(session_maker)
        reconcile_report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
        plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)

    # The code did reach Meta — that is the whole point of the phase.
    assert sender.saw_codes == [True] * count

    printed = json.dumps(
        [
            freeze_report.as_safe_dict(),
            create_report.as_safe_dict(),
            pay_report.as_safe_dict(),
            deliver_report.as_safe_dict(),
            status_report.as_safe_dict(),
            reconcile_report.as_safe_dict(),
            plan.as_safe_dict(),
        ],
        ensure_ascii=False,
        default=str,
    )
    logged = "\n".join(record.getMessage() for record in caplog.records)
    for secret in secrets:
        assert secret not in printed, secret
        assert secret not in logged, secret


async def test_the_database_stores_no_code_and_no_personal_field(
    session_maker, batch_configuration, binding_key
) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=FakeSender())

    async with session_maker() as session:
        items = list((await session.execute(select(EasyWeekVoucherSnapshotBatchItem))).scalars().all())
        attempts = list((await session.execute(select(EasyWeekVoucherSnapshotBatchAttempt))).scalars().all())
    dump = json.dumps(
        [
            {column.name: str(getattr(row, column.name)) for column in row.__table__.columns}
            for row in [*items, *attempts]
        ]
    )
    for secret in [*VOUCHER_CODE_SENTINELS[:count], *PHONES[:count], *CUSTOMER_NAMES[:count]]:
        assert secret not in dump, secret
    # The attempt rows exist, and carry which template was used and nothing of
    # what it said.
    assert len(attempts) == count
    assert {entry.outcome for entry in attempts} == {"provider_accepted"}
    assert all(entry.slot in (1, 2) for entry in attempts)


async def test_the_ops_page_shows_state_and_never_a_person(session_maker, batch_configuration, binding_key) -> None:
    from altegio_bot.ops import router as ops_router

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)

    html = await ops_router.ops_voucher_snapshot_batch_page()

    assert "Controlled voucher snapshot batch" in html
    assert "VOUCHER_SNAPSHOT_BATCH_RUNBOOK" in html
    # Read-only on purpose. The shared layout owns exactly one form — Logout —
    # and this page adds no form, no button and no link that could act on the
    # batch. A button here would be precisely the one-click "pay and send" the
    # phase exists to avoid.
    assert html.count("<form") == 1
    assert 'action="/ops/logout"' in html
    assert "easyweek_voucher_snapshot_batch " not in html
    assert "campaign_send_authorized=false" in html
    for secret in [*VOUCHER_CODE_SENTINELS[:count], *PHONES[:count], *CUSTOMER_NAMES[:count], *CUSTOMER_UUIDS[:count]]:
        assert secret not in html, secret


# ===========================================================================
# Nothing else in the system moved
# ===========================================================================


async def test_the_global_easyweek_guards_are_unchanged(session_maker, batch_configuration, binding_key) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=FakeSender())

    with pytest.raises(CampaignProviderRefusal) as excinfo:
        require_campaign_execution_provider(PROVIDER_EASYWEEK)
    assert excinfo.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED

    async with session_maker() as session:
        readiness = await resolve_campaign_readiness(
            session,
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            sender_code="default",
            template_code="new_client_voucher",
        )
    assert readiness.supported_job_types == ()
    assert CAMPAIGN_EXECUTION_NOT_AUTHORIZED in readiness.reasons
    assert readiness.ready_for_send is False


async def test_a_whole_batch_creates_no_job_and_no_outbox_row(session_maker, batch_configuration, binding_key) -> None:
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=FakeSender())

    async with session_maker() as session:
        assert (await session.scalar(select(func.count()).select_from(MessageJob))) == 0
        assert (await session.scalar(select(func.count()).select_from(OutboxMessage))) == 0
        # And the recipients themselves are untouched: this phase never
        # advances a campaign row's own status.
        statuses = set(
            (await session.execute(select(CampaignRecipient.status).where(CampaignRecipient.campaign_run_id == run_id)))
            .scalars()
            .all()
        )
    assert statuses == {"candidate"}


async def test_every_report_repeats_that_nothing_is_authorised(session_maker, batch_configuration, binding_key) -> None:
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    freeze_report = await _freeze(session_maker, reader, request)
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)

    for payload in (freeze_report.as_safe_dict(), plan.as_safe_dict()):
        assert payload["campaign_send_authorized"] is False
        assert payload["bulk_delivery_authorized"] is False
        assert payload["global_ready_for_send"] is False
        assert payload["max_recipients"] == MAX_RECIPIENTS
        assert payload["max_exposure_minor"] == MAX_EXPOSURE_MINOR
    assert freeze_report.as_safe_dict()["ready_for_send"] is False


def test_the_exit_code_mapping_is_never_optimistic() -> None:
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    def code(**changes):
        return cli._exit_for(runner_module.StageReport(stage="create", **changes))

    assert code(outcome="applied") == cli.EXIT_OK
    assert code(outcome="unknown") == cli.EXIT_UNKNOWN
    assert code(outcome="applied", reconciliation_required=True) == cli.EXIT_UNKNOWN
    assert code(outcome="applied", halted=True) == cli.EXIT_UNKNOWN
    assert code(outcome="refused") == cli.EXIT_CONTRACT_MISMATCH
    assert code(outcome="partial") == cli.EXIT_CONTRACT_MISMATCH
    assert code(outcome="applied", manual_cleanup_required=True) == cli.EXIT_MANUAL_CLEANUP


def test_the_cli_has_no_command_that_runs_two_external_stages() -> None:
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    parser = cli._build_parser()
    action = next(entry for entry in parser._actions if isinstance(entry, type(parser._subparsers._group_actions[0])))
    commands = set(action.choices)
    assert commands == {"status", "plan", "reconcile", "freeze", "create", "pay", "deliver", "refund"}
    # `--help` and a mistyped flag must never look like a batch that worked.
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--help"])
    assert excinfo.value.code == cli.EXIT_ARGUMENTS
