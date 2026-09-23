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
import dataclasses
import json
import logging
import uuid as uuid_module
from datetime import datetime, timedelta

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
    DATABASE_UNAVAILABLE,
    ENTITLEMENT_ALREADY_EXISTS,
    EXECUTION_INTERRUPTED,
    FROZEN_DIGEST_MISMATCH,
    HALTED_BY_PREDECESSOR,
    IDENTITY_BINDING_MISMATCH,
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
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationUnknown, VoucherMutationResponse
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_TEST,
    VOUCHER_BATCH_COMPLETED,
    VOUCHER_BATCH_HALTED,
    VOUCHER_BATCH_IN_PROGRESS,
    VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
    VOUCHER_BATCH_ITEM_CREATED,
    VOUCHER_BATCH_ITEM_DELIVERED,
    VOUCHER_BATCH_ITEM_PAID,
    VOUCHER_BATCH_ITEM_PLANNED,
    VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_BATCH_ITEM_READ,
    CampaignRecipient,
    CampaignRun,
    EasyWeekManualVoucherDeliveryLedger,
    EasyWeekVoucherSnapshotBatch,
    EasyWeekVoucherSnapshotBatchAttempt,
    EasyWeekVoucherSnapshotBatchItem,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.settings import Settings, settings
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
    orders_page,
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


async def _advance_to(session_maker, reader, request, *, stage, count):
    """Bring the batch to the state *stage* is claimed from, honestly.

    Through the real commands, not by writing rows: a crash test that set up
    its own ledger would be proving something about its own fixture.
    """
    await _full_create(session_maker, reader, request, count=count)
    if stage in (STAGE_PAY,):
        return
    await _full_pay(session_maker, reader, request, count=count)


async def _claim_for(session_maker, identity, *, stage, slot):
    """Take the durable claim *stage* takes, and then stop — as a crash would."""
    now = utcnow()
    if stage == STAGE_CREATE:
        return await ledger_module.claim_create(
            session_maker,
            identity=identity,
            slot=slot,
            plan_digest="d" * 64,
            create_window_start=now - timedelta(minutes=30),
            create_window_end=now + timedelta(minutes=30),
        )
    if stage == STAGE_PAY:
        return await ledger_module.claim_pay(session_maker, identity=identity, slot=slot, plan_digest="d" * 64)
    if stage == STAGE_REFUND:
        return await ledger_module.claim_refund(session_maker, identity=identity, slot=slot, plan_digest="d" * 64)
    snapshot = await ledger_module.load(session_maker)
    item = snapshot.item(slot)
    assert item is not None and item.pay_verified_at is not None
    return await ledger_module.claim_send(
        session_maker,
        identity=identity,
        slot=slot,
        plan_digest="d" * 64,
        # The CHECK requires a guard taken after the payment; a crash test must
        # not be the thing that relaxes it.
        live_guard_reproven_at=datetime.fromisoformat(item.pay_verified_at) + timedelta(seconds=1),
        template_code="new_client_voucher",
        meta_template_name="kitilash_ka_new_client_voucher_v1",
        template_language="de",
        sender_id=None,
    )


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND])
async def test_a_crash_after_any_claim_halts_the_batch_and_shuts_the_suffix(
    session_maker, batch_configuration, binding_key, stage
) -> None:
    """A process that died after the claim must read as "it may have gone out".

    The claim is committed before the request leaves, so what a later reader
    sees is a slot that may or may not have reached EasyWeek or Meta. The part
    this guards is what that does to the SLOTS BEHIND IT: until somebody proves
    what happened, nothing else may be claimed. Testing only that the same slot
    refuses would miss the whole failure — a second command claiming slot 2
    while slot 1's request is still in flight.
    """
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    if stage != STAGE_CREATE:
        await _advance_to(session_maker, reader, request, stage=stage, count=count)
    else:
        await _freeze(session_maker, reader, request)
        reader.orders = await marker_orders(session_maker, count=count)

    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None

    claim = await _claim_for(session_maker, identity, stage=stage, slot=1)
    assert claim.granted

    # 1. The header itself is halted, in the very transaction that claimed.
    after = await ledger_module.load(session_maker)
    assert after.status == VOUCHER_BATCH_HALTED
    assert after.halted_reason_code is not None
    assert after.item(1).reconciliation_required is True

    # 2. The same slot cannot be claimed again.
    again = await _claim_for(session_maker, identity, stage=stage, slot=1)
    assert not again.granted
    assert again.reason in (ledger_module.CLAIM_REFUSED_STATE, ledger_module.CLAIM_REFUSED_HALTED)

    # 3. And neither can the NEXT one — the point of the whole exercise.
    #
    # The refund is the deliberate exception and stays reachable: it returns
    # money rather than spending it, and a halt is exactly when an untouched
    # paid slot most needs it. Every stage that could SPEND or SEND is shut.
    successor = await _claim_for(session_maker, identity, stage=stage, slot=2)
    if stage == STAGE_REFUND:
        assert successor.granted
    else:
        assert not successor.granted
        assert successor.reason == ledger_module.CLAIM_REFUSED_HALTED

    # 4. A fresh plan for any spending or sending stage refuses, so no operator
    # and no wrapper can walk past it.
    for blocked in (STAGE_CREATE, STAGE_PAY, STAGE_DELIVER):
        plan = await _plan(session_maker, reader, stage=blocked, request=request)
        assert not plan.ready, blocked
        assert BATCH_HALTED in plan.reasons, blocked


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_DELIVER])
async def test_a_command_run_after_a_crashed_claim_makes_no_external_call(
    session_maker, batch_configuration, binding_key, stage
) -> None:
    """The refusal is worth nothing if the suffix still went out."""
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    if stage == STAGE_CREATE:
        await _freeze(session_maker, reader, request)
        reader.orders = await marker_orders(session_maker, count=count)
    else:
        await _advance_to(session_maker, reader, request, stage=stage, count=count)

    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    assert (await _claim_for(session_maker, identity, stage=stage, slot=1)).granted

    plan = await _plan(session_maker, reader, stage=stage, request=request)
    assert not plan.ready
    mutator = FakeMutator(create=_ok_response(0), pay=_ok_response(0))
    sender = FakeSender()
    common = {
        "request": request,
        "reader": reader,
        "order_reader": reader,
        "apply": True,
        "supplied_digest": plan.digest,
        "supplied_issued_at": plan.issued_at,
        "supplied_phrase": plan.confirmation_phrase,
    }
    async with session_maker() as session:
        if stage == STAGE_CREATE:
            report = await runner_module.run_create(session, session_maker, mutator=mutator, **common)
        elif stage == STAGE_PAY:
            report = await runner_module.run_pay(session, session_maker, mutator=mutator, **common)
        else:
            report = await runner_module.run_deliver(session, session_maker, sender=sender, **common)

    assert report.outcome == "refused"
    assert mutator.calls == []
    assert sender.calls == 0
    assert report.external_calls == {"create": 0, "pay": 0, "refund": 0, "meta": 0}


async def test_a_proven_outcome_lifts_the_halt_the_claim_raised(
    session_maker, batch_configuration, binding_key
) -> None:
    """Halting at claim time must not strand a batch that then worked.

    The halt is derived, not latched: recording a proven result re-derives the
    header, and with nothing unresolved left the next slot becomes claimable
    again. That is what lets a whole three-slot stage run to the end.
    """
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    report, mutator = await _full_create(session_maker, reader, request, count=count)

    assert report.outcome == "applied"
    assert len(mutator.create_calls) == count
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_IN_PROGRESS
    assert {entry.status for entry in snapshot.items} == {VOUCHER_BATCH_ITEM_CREATED}


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


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_REFUND])
async def test_reconcile_resolves_a_crashed_claim_whose_effect_did_happen(
    session_maker, batch_configuration, binding_key, stage
) -> None:
    """The process died after the claim; the request had in fact gone out.

    The order exists and proves out, so a GET-only reconcile is allowed to say
    so and move the slot forward — which is what lifts the halt and lets the
    rest of the batch continue.
    """
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    if stage == STAGE_CREATE:
        await _freeze(session_maker, reader, request)
    else:
        await _full_create(session_maker, reader, request, count=count)
    if stage == STAGE_REFUND:
        await _full_pay(session_maker, reader, request, count=count)

    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    assert (await _claim_for(session_maker, identity, stage=stage, slot=1)).granted
    assert (await ledger_module.load(session_maker)).status == VOUCHER_BATCH_HALTED

    # The world as it really is: the request DID leave, and the order shows it.
    if stage == STAGE_CREATE:
        reader.orders = await marker_orders(session_maker, count=count)
        reader.order_pages = [
            {
                "data": [reader.orders[ORDER_UUIDS[0]]],
                "meta": {"current_page": 1, "last_page": 1, "per_page": 100},
            }
        ]
        expected = VOUCHER_BATCH_ITEM_CREATED
    elif stage == STAGE_PAY:
        reader.orders = await marker_orders(session_maker, count=count, status="paid")
        expected = VOUCHER_BATCH_ITEM_PAID
    else:
        reader.orders = await marker_orders(session_maker, count=count, status="refunded")
        expected = "refunded"

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    # A reconcile never sends anything, whatever it proves.
    assert report.external_effect_attempted is False
    assert report.external_calls == {"create": 0, "pay": 0, "refund": 0, "meta": 0}
    after = await ledger_module.load(session_maker)
    assert after.item(1).status == expected
    assert after.status != VOUCHER_BATCH_HALTED
    assert after.item(1).reconciliation_required is False


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_DELIVER])
async def test_reconcile_leaves_the_halt_when_it_cannot_prove_the_outcome(
    session_maker, batch_configuration, binding_key, stage
) -> None:
    """Looked, found nothing, changed nothing that could be re-sent.

    Absence is not proof the request never left. The slot stays unresolved, the
    batch stays halted, the suffix stays shut, and no second POST is possible —
    the whole point being that "we could not find it" must never become
    permission to try again.
    """
    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    if stage == STAGE_CREATE:
        await _freeze(session_maker, reader, request)
    else:
        await _full_create(session_maker, reader, request, count=count)
    if stage == STAGE_DELIVER:
        await _full_pay(session_maker, reader, request, count=count)

    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    assert (await _claim_for(session_maker, identity, stage=stage, slot=1)).granted

    # The search finds nothing: an empty listing for CREATE, an order that is
    # still open for PAY, and for DELIVER nothing a POS order could ever say.
    if stage == STAGE_CREATE:
        reader.orders = {}
        reader.order_pages = [orders_page()]

    report = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert report.outcome == "unknown"
    assert report.halted is True
    after = await ledger_module.load(session_maker)
    assert after.status == VOUCHER_BATCH_HALTED
    # Parked as "looked, still unknown" — never back to a claimable state.
    assert after.item(1).status in (
        VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
        "pay_unknown",
        "send_unknown",
    )
    assert after.item(1).status not in (VOUCHER_BATCH_ITEM_PLANNED, VOUCHER_BATCH_ITEM_CREATED)

    # The suffix is still shut, and re-running the stage sends nothing.
    plan = await _plan(session_maker, reader, stage=stage, request=request)
    assert not plan.ready
    assert BATCH_HALTED in plan.reasons
    mutator = FakeMutator(create=_ok_response(0), pay=_ok_response(0))
    sender = FakeSender()
    common = {
        "request": request,
        "reader": reader,
        "order_reader": reader,
        "apply": True,
        "supplied_digest": plan.digest,
        "supplied_issued_at": plan.issued_at,
        "supplied_phrase": plan.confirmation_phrase,
    }
    async with session_maker() as session:
        if stage == STAGE_CREATE:
            await runner_module.run_create(session, session_maker, mutator=mutator, **common)
        elif stage == STAGE_PAY:
            await runner_module.run_pay(session, session_maker, mutator=mutator, **common)
        else:
            await runner_module.run_deliver(session, session_maker, sender=sender, **common)
    assert mutator.calls == []
    assert sender.calls == 0


async def test_a_crashed_send_is_never_declared_unsent_by_a_reconcile(
    session_maker, batch_configuration, binding_key
) -> None:
    """No provider message id means nothing to ask Meta about.

    A send claim that never recorded an identifier cannot be proven, cannot be
    retried — the attempt counter is already spent — and cannot be refunded.
    The one honest answer is that a human has to look.
    """
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    snapshot = await ledger_module.load(session_maker)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    assert (await _claim_for(session_maker, identity, stage=STAGE_DELIVER, slot=1)).granted

    await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    after = await ledger_module.load(session_maker)

    assert after.item(1).status == "send_unknown"
    assert after.item(1).send_attempt_count == 1
    assert after.item(1).provider_message_id_recorded is False
    assert after.status == VOUCHER_BATCH_HALTED

    # A refund is forbidden: the customer may be holding the code already.
    refund_plan = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1)
    assert not refund_plan.ready
    assert REFUND_FORBIDDEN_AFTER_SEND in refund_plan.reasons
    # And a second reconcile still resolves nothing and sends nothing.
    again = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    assert again.outcome == "unknown"
    assert again.external_effect_attempted is False


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


# ===========================================================================
# The runtime identity a batch was frozen with
# ===========================================================================


async def test_a_changed_runtime_identity_refuses_before_any_post(
    session_maker, batch_configuration, binding_key
) -> None:
    """Four UUIDs decide where a real €15 goes, and all four come from the env.

    Nothing stops an operator restarting the container with a different payment
    account between the freeze and the payment. The batch already stores what
    was approved, so the comparison is cheap — and the consequence of skipping
    it is a voucher sold from the wrong branch or charged to the wrong account.
    """
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    frozen = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, frozen)
    reader.orders = await marker_orders(session_maker, count=count)

    # The same process, restarted against a different POS account.
    drifted = dataclasses.replace(frozen, payment_account_uuid=str(uuid_module.uuid4()))

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=drifted)
    assert not plan.ready
    assert IDENTITY_BINDING_MISMATCH in plan.reasons
    assert plan.snapshot["runtime_identity_matches_frozen"] is False

    mutator = FakeMutator(create=_ok_response(0))
    async with session_maker() as session:
        report = await runner_module.run_create(
            session,
            session_maker,
            request=drifted,
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

    # The unchanged identity still works, and what goes on the wire is the
    # frozen identity rather than whatever the environment currently holds.
    created = FakeMutator(create_sequence=[_ok_response(index) for index in range(count)])
    good = await _apply(session_maker, reader, stage=STAGE_CREATE, request=frozen, mutator=created)
    assert good.outcome == "applied"
    assert {call["staffer_uuid"] for call in created.create_calls} == {STAFFER_UUID}
    assert {call["location_uuid"] for call in created.create_calls} == {KARLSRUHE_LOCATION_UUID}


async def test_a_changed_payment_account_refuses_before_the_charge(
    session_maker, batch_configuration, binding_key
) -> None:
    """The one that costs money: PAY must never charge an unapproved account."""
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    frozen = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, frozen, count=count)

    drifted = dataclasses.replace(frozen, payment_account_uuid=str(uuid_module.uuid4()))
    reader.orders = await marker_orders(session_maker, count=count)

    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=drifted)
    assert not plan.ready
    assert IDENTITY_BINDING_MISMATCH in plan.reasons

    mutator = FakeMutator(pay=_ok_response(0))
    async with session_maker() as session:
        report = await runner_module.run_pay(
            session,
            session_maker,
            request=drifted,
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

    # And the account actually charged on the good path is the frozen one.
    _, paid = await _full_pay(session_maker, reader, frozen, count=count)
    assert {call["account_uuid"] for call in paid.pay_calls} == {ACCOUNT_UUID}


def test_no_report_prints_the_operational_identities() -> None:
    """The staffer and the account are operational identities, not report fields."""
    safe = ledger_module.BatchSnapshot(
        exists=True,
        campaign_period_start="2026-08-01T00:00:00+00:00",
        campaign_period_end="2026-08-31T23:59:59+00:00",
        staffer_uuid=STAFFER_UUID,
        payment_account_uuid=ACCOUNT_UUID,
    ).as_safe_dict()
    printed = json.dumps(safe)
    assert STAFFER_UUID not in printed
    assert ACCOUNT_UUID not in printed


# ===========================================================================
# The campaign period an operator is approving
# ===========================================================================


async def test_the_campaign_period_is_visible_before_the_freeze(
    session_maker, batch_configuration, binding_key
) -> None:
    """Which wave, in one glance, while the approval can still be withheld.

    The period is the entitlement key, not a send date: a transitional August
    audience mailed in October is still an August entitlement. An operator who
    cannot see which month they are approving cannot notice that they are about
    to hand a second €15 to people the August batch already served.
    """
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request)

    composition = plan.snapshot["composition"]
    assert composition["campaign_period"] == "2026-08-01..2026-08-31"
    assert composition["campaign_period_start"] == PERIOD_START.isoformat()
    assert composition["campaign_period_end"] == PERIOD_END.isoformat()

    # And it keeps saying so after the freeze, in the status and on the page.
    await _freeze(session_maker, reader, request)
    status = (await runner_module.run_status(session_maker)).as_safe_dict()
    assert status["batch"]["campaign_period"] == "2026-08-01..2026-08-31"

    from altegio_bot.ops import router as ops_router

    html = await ops_router.ops_voucher_snapshot_batch_page()
    assert "2026-08-01..2026-08-31" in html
    assert "Период кампании" in html


async def test_a_different_period_is_a_different_approval(session_maker, batch_configuration, binding_key) -> None:
    """Change the wave and the digest changes, so the old approval dies with it."""
    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    before = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request)
    assert before.ready

    # The same preview, the same people, a different entitlement period.
    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                text("UPDATE campaign_runs SET period_start = :start, period_end = :end WHERE id = :run"),
                {
                    "start": PERIOD_START.replace(month=10),
                    "end": PERIOD_END.replace(month=10),
                    "run": run_id,
                },
            )

    after = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request)
    assert after.snapshot["composition"]["campaign_period"] == "2026-10-01..2026-10-31"
    assert after.digest_for(before.issued_at) != before.digest

    # The approval taken for August authorises nothing in October.
    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            apply=True,
            supplied_digest=before.digest,
            supplied_issued_at=before.issued_at,
            supplied_phrase=before.confirmation_phrase,
        )
    assert report.outcome == "refused"
    assert PLAN_DIGEST_MISMATCH in report.reasons
    assert not (await ledger_module.load(session_maker)).exists


# ===========================================================================
# One lock order, on the real database
# ===========================================================================


async def _delivered_batch(session_maker, *, count):
    """A batch whose slots all carry a provider message id."""
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=FakeSender())
    return run_id, reader, request


async def test_a_blocked_webhook_holds_no_item_lock(session_maker, batch_configuration, binding_key) -> None:
    """The webhook takes the header FIRST, exactly like every stage writer.

    This is the deadlock in one assertion. A webhook that locked its item and
    then waited for the header, while a stage held the header and waited for
    that item, is a textbook cycle — and both orders are individually
    reasonable, which is why having two of them is the bug.

    So: hold the header elsewhere, let a webhook block on it, and then ask
    whether item 1 is still lockable by somebody else. Under one shared order
    it is, because the blocked webhook is waiting at the header and owns
    nothing. Under the old order the answer would be no.
    """
    count = 2
    await _delivered_batch(session_maker, count=count)

    barrier = asyncio.Event()
    released = asyncio.Event()

    async def hold_the_header() -> None:
        async with session_maker() as session:
            async with session.begin():
                await session.execute(text("SELECT id FROM easyweek_voucher_snapshot_batches FOR UPDATE"))
                barrier.set()
                await released.wait()

    holder = asyncio.create_task(hold_the_header())
    await asyncio.wait_for(barrier.wait(), timeout=15)

    webhook = asyncio.create_task(
        ledger_module.record_webhook_transition(
            session_maker,
            provider_message_id=PROVIDER_MESSAGE_IDS[0],
            status=VOUCHER_BATCH_ITEM_DELIVERED,
        )
    )
    # Give it long enough to have taken whatever locks it is going to take
    # before the header stops it.
    await asyncio.sleep(0.5)
    assert not webhook.done()

    async with session_maker() as probe:
        async with probe.begin():
            # NOWAIT: this either gets the lock immediately or raises. A
            # webhook that already held item 1 would make this fail.
            rows = (
                (
                    await probe.execute(
                        select(EasyWeekVoucherSnapshotBatchItem)
                        .where(EasyWeekVoucherSnapshotBatchItem.slot == 1)
                        .with_for_update(nowait=True)
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1

    released.set()
    await asyncio.wait_for(holder, timeout=15)
    result = await asyncio.wait_for(webhook, timeout=15)
    assert result.applied
    assert (await ledger_module.load(session_maker)).item(1).status == VOUCHER_BATCH_ITEM_DELIVERED


async def test_a_webhook_and_a_stage_write_do_not_deadlock(session_maker, batch_configuration, binding_key) -> None:
    """Slot 1's callback beside slot 2's recorded Meta outcome, repeatedly.

    Two independent sessions, bounded by a timeout so a deadlock or a lock wait
    fails the test instead of hanging it, and an assertion that the accepted
    provider message id is still there afterwards — a lost one would mean a
    delivered message nobody can ever observe.
    """
    count = 2
    await _delivered_batch(session_maker, count=count)

    for _ in range(6):
        webhook = ledger_module.record_webhook_transition(
            session_maker,
            provider_message_id=PROVIDER_MESSAGE_IDS[0],
            status=VOUCHER_BATCH_ITEM_READ,
        )
        stage = ledger_module.record_item_outcome(
            session_maker,
            slot=2,
            status=VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
            expected_statuses=frozenset({VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED}),
            provider_message_id=PROVIDER_MESSAGE_IDS[1],
            reconciliation_required=False,
        )
        await asyncio.wait_for(asyncio.gather(webhook, stage), timeout=30)

    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_READ
    assert snapshot.item(1).provider_message_id_recorded is True
    assert snapshot.item(2).provider_message_id_recorded is True
    # Neither path relaxed what it guards.
    assert snapshot.item(1).send_attempt_count == 1
    assert snapshot.item(2).send_attempt_count == 1


# ===========================================================================
# Freeze against discard and delete
# ===========================================================================


async def test_a_frozen_batch_refuses_discard_and_delete(session_maker, batch_configuration, binding_key) -> None:
    from altegio_bot.campaigns.runner import delete_preview_run, discard_preview_run

    run_id, _ = await _frozen_batch(session_maker, count=2)

    with pytest.raises(ValueError):
        await discard_preview_run(run_id)
    with pytest.raises(ValueError):
        await delete_preview_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        assert run.status == "completed"
    assert (await ledger_module.load(session_maker)).exists


async def test_a_discarded_preview_cannot_be_frozen(session_maker, batch_configuration, binding_key) -> None:
    from altegio_bot.campaigns.runner import discard_preview_run

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    await discard_preview_run(run_id)

    plan = await _plan(session_maker, FakeReader(count=count), stage=STAGE_FREEZE, request=batch_request(run_id=run_id))

    assert not plan.ready
    assert RUN_UNPROVEN in plan.reasons
    assert not (await ledger_module.load(session_maker)).exists


@pytest.mark.parametrize("operation", ["discard", "delete"])
async def test_a_concurrent_freeze_and_discard_leave_no_impossible_state(
    session_maker, batch_configuration, binding_key, operation
) -> None:
    """Whoever wins, the pair stays consistent.

    The state this forbids is a batch bound to a preview that has since been
    discarded or deleted: the voucher would be real and the snapshot proving
    whose it is would be gone. Both paths take the same ``CampaignRun`` lock
    first and re-check their guards after the wait, so one of them always loses
    cleanly.
    """
    from altegio_bot.campaigns.runner import delete_preview_run, discard_preview_run

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request)
    assert plan.ready

    async def freeze() -> str:
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
        return report.outcome

    async def edit() -> str:
        try:
            if operation == "discard":
                await discard_preview_run(run_id)
            else:
                await delete_preview_run(run_id)
        except ValueError:
            return "refused"
        return "applied"

    outcomes = await asyncio.wait_for(asyncio.gather(freeze(), edit()), timeout=30)
    frozen_outcome, edit_outcome = outcomes

    batch = await ledger_module.load(session_maker)
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        status = run.status

    if batch.exists:
        # The freeze won: the preview must still be the snapshot it points at.
        assert frozen_outcome == "frozen"
        assert edit_outcome == "refused"
        assert status == "completed"
    else:
        # The edit won: nothing was frozen onto a snapshot that is now gone.
        assert edit_outcome == "applied"
        assert frozen_outcome == "refused"
        assert status in ("discarded", "deleted")


# ===========================================================================
# The operator CLI: what it says when something unplanned happens
# ===========================================================================


class _AsyncCM:
    """An ``async with`` wrapper around an already-built fake transport."""

    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *exc) -> bool:
        return False


def _forbidden_transport(*args, **kwargs):  # pragma: no cover - only called on failure
    raise AssertionError("no transport may be constructed for this command")


async def _run_cli(argv: list[str]) -> tuple[dict, int]:
    """Drive the real entry point in a worker thread, and read what it printed.

    A thread because ``main`` owns its event loop, and the point of these tests
    is the entry point an operator actually types — including its exit code.

    The command gets its own ``NullPool`` engine on the same database. Without
    it the CLI would borrow the module-global pooled engine, leave a connection
    bound to the thread's event loop, and poison later tests the moment that
    loop closed — the failure mode ``conftest`` documents at length. NullPool
    opens and closes a connection per checkout, so nothing outlives a loop.
    """
    import io
    from contextlib import redirect_stdout

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    engine = create_async_engine(Settings().database_url, poolclass=NullPool)
    original = cli.SessionLocal
    cli.SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    def run() -> tuple[str, int]:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = cli.main(argv)
        return buffer.getvalue(), code

    try:
        printed, code = await asyncio.to_thread(run)
    finally:
        cli.SessionLocal = original
        await engine.dispose()
    return json.loads(printed), code


async def test_status_answers_with_the_fence_closed(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """A closed fence must not make the durable state unreadable.

    `status` reads the ledger and nothing else — no HTTP, no approval, no
    mutation. After an emergency `false` the operator's question is exactly
    what the halted batch left behind and whether a draft is still open in the
    POS, and refusing to answer that would not be safety: it would be somebody
    running SQL by hand instead.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 3
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)
    mutator = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    # The fence goes down, as it would after an incident.
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_enabled", False, raising=False)
    # And nothing may open a socket.
    monkeypatch.setattr(cli, "EasyWeekClient", _forbidden_transport)
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", _forbidden_transport)
    monkeypatch.setattr(cli, "VoucherDeliveryClient", _forbidden_transport)

    payload, code = await _run_cli(["status"])

    assert code == cli.EXIT_OK
    assert payload["stage"] == "status"
    # The REAL state, not a refusal.
    batch = payload["batch"]
    assert batch["exists"] is True
    assert batch["halted"] is True
    assert batch["recipient_count"] == count
    assert payload["reconciliation_required"] is True
    assert payload["manual_cleanup_required"] is True
    assert BATCH_DISABLED not in payload["reasons"]

    # Every other command stays behind the fence.
    for argv in (
        ["plan", "--stage", "create", "--preview-run-id", str(run_id)],
        ["create", "--preview-run-id", str(run_id), "--apply"],
        ["reconcile", "--preview-run-id", str(run_id)],
    ):
        refused, refused_code = await _run_cli(argv)
        assert refused_code == cli.EXIT_CONTRACT_MISMATCH, argv
        assert BATCH_DISABLED in refused["reasons"], argv


async def test_an_exception_before_any_claim_is_still_a_safe_refusal(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """Exit 4 is a promise that nothing started, and here it is provable.

    The claim is committed before any request leaves, so a ledger that still
    answers and holds no unresolved slot IS the proof. Only then may the CLI
    say "refused".
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)

    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(FakeMutator()))

    def explode(*args, **kwargs):
        raise RuntimeError("synthetic failure before anything was claimed")

    monkeypatch.setattr(runner_module, "build_stage_plan", explode)

    payload, code = await _run_cli(
        ["create", "--preview-run-id", str(run_id), "--apply", "--plan-digest", "x", "--confirm", "y"]
    )

    assert code == cli.EXIT_CONTRACT_MISMATCH
    assert payload["outcome"] == "refused"
    assert payload["external_effect_attempted"] is False
    # And the ledger agrees: nothing was claimed.
    snapshot = await ledger_module.load(session_maker)
    assert {entry.status for entry in snapshot.items} == {VOUCHER_BATCH_ITEM_PLANNED}
    assert snapshot.status != VOUCHER_BATCH_HALTED


@pytest.mark.parametrize("stage", [STAGE_PAY, STAGE_DELIVER])
async def test_a_failure_after_the_effect_never_reports_that_nothing_happened(
    session_maker, batch_configuration, binding_key, monkeypatch, stage
) -> None:
    """The money moved, or Meta took the message, and then the write failed.

    The old blanket handler answered `refused`, `external_effect_attempted=false`
    and exit 4 for any exception — including this one. That is a promise that
    nothing started, made over a €15 charge that already happened or a message
    a customer may already be reading.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)

    if stage == STAGE_PAY:
        paid = await marker_orders(session_maker, count=count, status="paid")
        mutator = FakeMutator(pay_sequence=[_ok_response(index) for index in range(count)], reader=reader, settles=paid)
        sender = FakeSender()
    else:
        await _full_pay(session_maker, reader, request, count=count)
        mutator = FakeMutator()
        sender = FakeSender()

    plan = await _plan(session_maker, reader, stage=stage, request=request)
    assert plan.ready, plan.reasons

    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))
    monkeypatch.setattr(cli, "VoucherDeliveryClient", lambda *a, **k: _AsyncCM(sender))

    # The external effect happens; writing down what it was does not.
    def explode(*args, **kwargs):
        raise RuntimeError("synthetic failure while recording a proven outcome")

    monkeypatch.setattr(ledger_module, "record_item_outcome", explode)

    payload, code = await _run_cli(
        [
            stage,
            "--preview-run-id",
            str(run_id),
            "--apply",
            "--plan-digest",
            plan.digest,
            "--plan-issued-at",
            plan.issued_at.isoformat(),
            "--confirm",
            plan.confirmation_phrase,
        ]
    )

    # The effect really did happen.
    if stage == STAGE_PAY:
        assert len(mutator.pay_calls) == 1
    else:
        assert sender.calls == 1

    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "unknown"
    assert payload["external_effect_attempted"] is True
    assert payload["reconciliation_required"] is True
    assert payload["halted"] is True
    if stage == STAGE_DELIVER:
        assert payload["external_send_attempted"] is True

    # Nothing personal, and no traceback, reached the operator's report.
    printed = json.dumps(payload)
    for secret in [*VOUCHER_CODE_SENTINELS[:count], *PHONES[:count], *CUSTOMER_NAMES[:count], *ORDER_UUIDS[:count]]:
        assert secret not in printed, secret
    assert "Traceback" not in printed
    assert "synthetic failure" not in printed

    # And the durable record is the claim, which is what reconcile will resolve.
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_BATCH_HALTED
    assert snapshot.item(1).status in ("pay_claimed", "send_claimed")


# ===========================================================================
# The runbook, as a contract
# ===========================================================================


def _runbook() -> str:
    from pathlib import Path

    path = Path(__file__).resolve().parents[3] / "docs" / "easyweek" / "VOUCHER_SNAPSHOT_BATCH_RUNBOOK.md"
    assert path.exists(), path
    return path.read_text(encoding="utf-8")


def _command_blocks(text: str) -> list[str]:
    """Every ```bash block, as one command string each."""
    blocks: list[str] = []
    inside = False
    current: list[str] = []
    for line in text.splitlines():
        if line.strip().startswith("```bash"):
            inside, current = True, []
            continue
        if inside and line.strip().startswith("```"):
            blocks.append(" ".join(current).strip())
            inside = False
            continue
        if inside:
            current.append(line.strip())
    return blocks


def test_the_runbook_recreates_the_api_service_instead_of_restarting_it() -> None:
    """A `restart` keeps the environment the container was created with.

    Which means the fence would still read `true` inside the container while
    the file on disk says `false` — the operator believes the batch is shut and
    it is not. The instruction has to recreate the service, and it has to end
    with a check of the value INSIDE the container. Pinned here so it cannot
    quietly go back to a restart.
    """
    text = _runbook()
    blocks = _command_blocks(text)

    recreate = [
        block for block in blocks if "up -d" in block and "--force-recreate" in block and "altegio-api" in block
    ]
    assert recreate, "the runbook must recreate the API service, not restart it"
    # Both compose files, or the command runs against a different topology.
    for block in recreate:
        assert "-f docker-compose.yml" in block
        assert "-f docker-compose.chatwoot-internal.yml" in block
        assert "--no-deps" in block

    # No command block may tell an operator to restart the service instead.
    for block in blocks:
        assert not (block.startswith("docker compose") and " restart" in f" {block} "), block

    # And the fence-off section must end by reading the value back from inside
    # the container, with `false` named as the expected output.
    afterwards = text.split("## 12.")[1].split("## 13.")[0]
    verify = [
        block
        for block in _command_blocks(afterwards)
        if "EASYWEEK_VOUCHER_SNAPSHOT_BATCH_ENABLED" in block and "exec altegio-api" in block
    ]
    assert verify, "the runbook must verify the fence value inside the container"
    assert "printenv" in " ".join(verify)
    assert "\n```\nfalse\n```" in afterwards


def test_the_runbook_makes_the_operator_check_the_campaign_period() -> None:
    """The period is the entitlement key, and it is checkable before the freeze.

    An operator who cannot see which wave they are approving cannot notice that
    a transitional August audience is about to be frozen against October.
    """
    text = _runbook()

    assert "campaign_period" in text
    assert "Check the campaign period" in text
    # The worked example, by name, so the trap is stated rather than implied.
    assert "August" in text and "October" in text
    assert "2026-08-01..2026-08-31" in text
    assert "send date never replaces the entitlement period" in text


def test_the_runbook_still_states_the_limits_it_is_built_on() -> None:
    text = _runbook()

    assert "€75" in text
    assert "at most five" in text.lower() or "Maximum vouchers | 5" in text
    # No command that would run two external stages in one go.
    for block in _command_blocks(text):
        assert block.count("easyweek_voucher_snapshot_batch ") <= 1, block


# ===========================================================================
# A command that died AFTER an external effect of its own
# ===========================================================================


def _raise_on_call(monkeypatch, module, name: str, *, nth: int) -> dict[str, int]:
    """Let the real function run, then blow up on its *nth* call.

    Models the failure the blocker is about: slot 1's request really went out
    and was really recorded, and the command then stopped before reaching
    slot 2. The counter is returned so a test can assert where it happened.
    """
    original = getattr(module, name)
    seen = {"calls": 0}

    async def flaky(*args, **kwargs):
        seen["calls"] += 1
        if seen["calls"] >= nth:
            raise RuntimeError("synthetic failure before the next slot was claimed")
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, name, flaky)
    return seen


async def _cli_stage(session_maker, reader, request, *, stage, slot=None):
    """A fresh plan plus the argv an operator would type for that stage."""
    plan = await _plan(session_maker, reader, stage=stage, request=request, slot=slot)
    assert plan.ready, plan.reasons
    argv = [stage, "--preview-run-id", str(request.preview_run_id)]
    if slot is not None:
        argv += ["--slot", str(slot)]
    argv += [
        "--apply",
        "--plan-digest",
        plan.digest,
        "--plan-issued-at",
        plan.issued_at.isoformat(),
        "--confirm",
        plan.confirmation_phrase,
    ]
    return argv


def _assert_pii_free(payload: dict, *, count: int) -> None:
    printed = json.dumps(payload, ensure_ascii=False, default=str)
    for secret in [
        *VOUCHER_CODE_SENTINELS[:count],
        *PHONES[:count],
        *CUSTOMER_NAMES[:count],
        *CUSTOMER_UUIDS[:count],
        *ORDER_UUIDS[:count],
    ]:
        assert secret not in printed, secret
    assert "Traceback" not in printed
    assert "synthetic failure" not in printed


async def test_a_multi_slot_create_that_died_after_slot_one_never_reports_a_refusal(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """The blocker, in the shape that makes it dangerous.

    Slot 1 creates a real voucher and is recorded as ``created`` — proven,
    resolved, terminal. The command then dies before claiming slot 2. The
    ledger holds nothing unresolved, so the old handler read that as "nothing
    started" and printed `refused`, `external_effect_attempted=false`, exit 4,
    over an order that exists in the POS.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)

    mutator = FakeMutator(create_sequence=[_ok_response(index) for index in range(count)])
    argv = await _cli_stage(session_maker, reader, request, stage=STAGE_CREATE)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))
    # Slot 1 runs for real; the second claim is where it stops.
    _raise_on_call(monkeypatch, ledger_module, "claim_create", nth=2)

    payload, code = await _run_cli(argv)

    # Exactly one voucher was created, and it is on the record.
    assert len(mutator.create_calls) == 1
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_CREATED
    assert snapshot.item(2).status == VOUCHER_BATCH_ITEM_PLANNED

    assert code != cli.EXIT_CONTRACT_MISMATCH
    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "interrupted"
    assert payload["external_effect_attempted"] is True
    assert EXECUTION_INTERRUPTED in payload["reasons"]
    # The durable state, as it really is — not forced.
    assert payload["batch"]["items"][0]["status"] == VOUCHER_BATCH_ITEM_CREATED
    _assert_pii_free(payload, count=count)


async def test_a_multi_slot_pay_that_died_after_slot_one_never_denies_the_charge(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """€15 left the account and the command stopped. It may not say otherwise."""
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)

    paid = await marker_orders(session_maker, count=count, status="paid")
    mutator = FakeMutator(pay_sequence=[_ok_response(index) for index in range(count)], reader=reader, settles=paid)
    argv = await _cli_stage(session_maker, reader, request, stage=STAGE_PAY)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))
    _raise_on_call(monkeypatch, ledger_module, "claim_pay", nth=2)

    payload, code = await _run_cli(argv)

    assert len(mutator.pay_calls) == 1
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_PAID
    assert snapshot.item(2).status == VOUCHER_BATCH_ITEM_CREATED

    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "interrupted"
    assert payload["external_effect_attempted"] is True
    _assert_pii_free(payload, count=count)


async def test_a_multi_slot_deliver_that_died_after_slot_one_never_reports_a_refusal(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """Meta took the message. One attempt is spent and a person may be reading it."""
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    sender = FakeSender()
    argv = await _cli_stage(session_maker, reader, request, stage=STAGE_DELIVER)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "VoucherDeliveryClient", lambda *a, **k: _AsyncCM(sender))
    _raise_on_call(monkeypatch, ledger_module, "claim_send", nth=2)

    payload, code = await _run_cli(argv)

    assert sender.calls == 1
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(1).status == VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED
    assert snapshot.item(1).send_attempt_count == 1
    assert snapshot.item(1).provider_message_id_recorded is True
    assert snapshot.item(2).send_attempt_count == 0

    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "interrupted"
    assert payload["external_effect_attempted"] is True
    assert payload["external_send_attempted"] is True
    _assert_pii_free(payload, count=count)


async def test_a_refund_that_died_while_printing_still_says_it_happened(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """A single-slot terminal effect, and the crash is in the report itself."""
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _full_create(session_maker, reader, request, count=count)
    await _full_pay(session_maker, reader, request, count=count)

    refunded = await marker_orders(session_maker, count=count, status="refunded")
    mutator = FakeMutator(refund=_ok_response(1), reader=reader, settles={ORDER_UUIDS[1]: refunded[ORDER_UUIDS[1]]})
    argv = await _cli_stage(session_maker, reader, request, stage=STAGE_REFUND, slot=2)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))

    # The refund is made and recorded; building the final report is not.
    def explode(*args, **kwargs):
        raise RuntimeError("synthetic failure while building the final report")

    monkeypatch.setattr(runner_module, "_stage_report", explode)

    payload, code = await _run_cli(argv)

    assert mutator.calls == ["refund"]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.item(2).status == "refunded"

    assert code == cli.EXIT_UNKNOWN
    assert payload["external_effect_attempted"] is True
    assert payload["outcome"] == "interrupted"
    _assert_pii_free(payload, count=count)


async def test_a_create_that_died_reading_its_own_final_state_still_says_it_happened(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """The terminal outcome is written; the read that follows it is not.

    The durable state survives, so the fallback has everything it needs — and
    what it must not do is mistake "I could not finish looking" for "nothing
    happened".
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 1
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)
    reader.orders = await marker_orders(session_maker, count=count)

    mutator = FakeMutator(create_sequence=[_ok_response(0)])
    argv = await _cli_stage(session_maker, reader, request, stage=STAGE_CREATE)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))

    # The first read taken once a slot is `created` fails — exactly the final
    # `load` of the stage. Later reads, including the failure handler's own,
    # work again, so this is not a database outage.
    original_load = ledger_module.load
    tripped = {"done": False}

    async def failing_load(session_maker_arg):
        snapshot = await original_load(session_maker_arg)
        created = any(entry.status == VOUCHER_BATCH_ITEM_CREATED for entry in snapshot.items)
        if created and not tripped["done"]:
            tripped["done"] = True
            raise RuntimeError("synthetic failure reading the final state")
        return snapshot

    monkeypatch.setattr(ledger_module, "load", failing_load)

    payload, code = await _run_cli(argv)

    assert tripped["done"] is True
    assert len(mutator.create_calls) == 1
    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "interrupted"
    assert payload["external_effect_attempted"] is True
    # Built from the durable ledger, which did survive.
    assert payload["batch"]["items"][0]["status"] == VOUCHER_BATCH_ITEM_CREATED
    _assert_pii_free(payload, count=count)


async def test_an_unreadable_ledger_fails_closed_for_an_effectful_command(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """A ledger that will not answer proves nothing, so exit 4 is not available."""
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)

    mutator = FakeMutator(create=_ok_response(0))
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))

    def explode(*args, **kwargs):
        raise RuntimeError("synthetic database failure")

    # Both the command and every attempt to read the durable state fail.
    monkeypatch.setattr(runner_module, "build_stage_plan", explode)
    monkeypatch.setattr(runner_module, "run_status", explode)

    payload, code = await _run_cli(
        ["create", "--preview-run-id", str(run_id), "--apply", "--plan-digest", "x", "--confirm", "y"]
    )

    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "unknown"
    assert payload["external_effect_attempted"] is True
    assert payload["reconciliation_required"] is True
    assert DATABASE_UNAVAILABLE in payload["reasons"]
    _assert_pii_free(payload, count=count)


async def test_a_freeze_that_died_after_committing_does_not_read_as_refused(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """A freeze reaches nobody, and it still must not deny what it wrote.

    `external_effect_attempted` stays false here, and provably so: the freeze
    path never constructs a mutation transport at all.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)

    argv = await _cli_stage(session_maker, reader, request, stage=STAGE_FREEZE)
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", _forbidden_transport)

    # The freeze runs for real and commits; the command then dies on its way
    # back out. Wrapped rather than replaced, so what is being tested is a
    # crash AFTER durable work, not instead of it.
    original_freeze = runner_module.run_freeze

    async def freeze_then_die(*args, **kwargs):
        await original_freeze(*args, **kwargs)
        raise RuntimeError("synthetic failure after the freeze committed")

    monkeypatch.setattr(runner_module, "run_freeze", freeze_then_die)

    payload, code = await _run_cli(argv)

    assert (await ledger_module.load(session_maker)).exists
    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "interrupted"
    assert payload["external_effect_attempted"] is False
    _assert_pii_free(payload, count=count)


async def test_a_missing_baseline_is_as_disqualifying_as_an_unreadable_ledger(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """ "Unchanged" is a comparison, and it needs something to compare against.

    If the reading taken BEFORE the command fails — a momentary database
    hiccup, say — then a later ledger with nothing unresolved in it proves
    nothing at all about what this invocation did. Falling through to a
    refusal there would reintroduce the same false "nothing happened" by a
    different door.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)

    mutator = FakeMutator(create=_ok_response(0))
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(mutator))

    # The FIRST read — the baseline — fails; everything afterwards works.
    original_status = runner_module.run_status
    first = {"seen": False}

    async def status_once(*args, **kwargs):
        if not first["seen"]:
            first["seen"] = True
            raise RuntimeError("synthetic failure taking the baseline")
        return await original_status(*args, **kwargs)

    monkeypatch.setattr(runner_module, "run_status", status_once)

    def explode(*args, **kwargs):
        raise RuntimeError("synthetic failure before anything was claimed")

    monkeypatch.setattr(runner_module, "build_stage_plan", explode)

    payload, code = await _run_cli(
        ["create", "--preview-run-id", str(run_id), "--apply", "--plan-digest", "x", "--confirm", "y"]
    )

    assert first["seen"] is True
    # Nothing was actually claimed — but this run cannot prove that, and it
    # does not pretend to.
    assert code == cli.EXIT_UNKNOWN
    assert payload["outcome"] == "unknown"
    assert payload["external_effect_attempted"] is True
    assert DATABASE_UNAVAILABLE in payload["reasons"]
    _assert_pii_free(payload, count=count)


def test_a_terminal_slot_alone_is_not_proof_that_nothing_happened() -> None:
    """The blocker, stated as the unit it lives in.

    Without a baseline to compare against, a batch whose every slot is in a
    proven terminal state is indistinguishable from one this command never
    touched — which is precisely how a created voucher came to be reported as
    `refused`. The classifier must refuse to guess.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    settled = {
        "exists": True,
        "halted": False,
        "reconciliation_required": False,
        "items": [
            {"slot": 1, "status": VOUCHER_BATCH_ITEM_CREATED},
            {"slot": 2, "status": VOUCHER_BATCH_ITEM_PLANNED},
        ],
    }
    # Nothing unresolved, so the old rule read this as "nothing started".
    assert cli._has_unresolved_slot(settled) is False

    before = {**settled, "items": [{"slot": 1, "status": VOUCHER_BATCH_ITEM_PLANNED}, settled["items"][1]]}
    assert before != settled


# ===========================================================================
# One CLI invocation is one event loop
# ===========================================================================


async def _run_cli_pooled(argv: list[str]) -> tuple[dict, int]:
    """Drive the entry point against the REAL pooled engine, as production does.

    The sibling helper above swaps in a ``NullPool`` engine, which is right for
    isolating the other tests and wrong for this one: NullPool opens and closes
    a connection per checkout, so it cannot reproduce a pooled connection being
    handed to a second event loop. That is the whole bug, so this helper leaves
    ``altegio_bot.db.SessionLocal`` exactly as the deployed CLI finds it.

    Why the cleanup is safe
    -----------------------
    After the invocation the pool holds a connection belonging to the loop that
    ``main`` created and then closed. ``dispose(close=False)`` ABANDONS those
    connections instead of closing them: nothing is scheduled on the dead loop,
    so there is no ``Event loop is closed`` and no "coroutine was never awaited"
    warning to leak into unrelated tests, and the next checkout opens a fresh
    connection. The abandoned socket is released when this test process exits —
    at most one per invocation here, which is why this is affordable for a
    couple of tests and not a pattern to spread.
    """
    import io
    from contextlib import redirect_stdout

    import altegio_bot.db as app_db
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    assert cli.SessionLocal is app_db.SessionLocal, "this test must use the production engine"

    def run() -> tuple[str, int]:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = cli.main(argv)
        return buffer.getvalue(), code

    try:
        printed, code = await asyncio.to_thread(run)
    finally:
        await app_db.engine.dispose(close=False)
    return json.loads(printed), code


async def test_the_cli_survives_the_production_pooled_engine(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """A baseline read and then a command, over one pooled engine.

    ``SessionLocal`` pools, and a pooled asyncpg connection belongs to the loop
    that opened it. When the baseline read, the command and the failure read
    each ran in their own ``asyncio.run``, the first read poisoned the pool for
    everything after it: the second checkout died with *Event loop is closed* /
    *got Future attached to a different loop*, the broad handler swallowed it,
    and a perfectly healthy database was reported to the operator as
    unreadable.

    So this drives the real entry point against the real engine and asserts the
    database was readable the whole way through.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)

    # `create` is a durable command, so it takes a baseline read first and then
    # reads again inside the dispatch — two checkouts in one invocation, which
    # is exactly the shape that used to break.
    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(FakeMutator()))

    payload, code = await _run_cli_pooled(
        [
            "create",
            "--preview-run-id",
            str(run_id),
            "--apply",
            "--plan-digest",
            "0" * 64,
            "--plan-issued-at",
            utcnow().isoformat(),
            "--confirm",
            "wrong-phrase",
        ]
    )

    # A stale approval, refused on its merits — NOT a database failure.
    assert DATABASE_UNAVAILABLE not in payload["reasons"], payload["reasons"]
    assert payload["outcome"] == "refused"
    assert PLAN_DIGEST_MISMATCH in payload["reasons"]
    assert code == cli.EXIT_CONTRACT_MISMATCH
    # The ledger really was read, in both directions.
    assert payload["batch"]["exists"] is True
    assert payload["batch"]["recipient_count"] == count
    _assert_pii_free(payload, count=count)


async def test_the_cli_survives_a_pooled_engine_on_the_failure_path(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """The same, for the path that reads the ledger a SECOND time.

    The failure handler takes its own reading to classify what happened. Under
    the old split that was a third event loop over the same pool, so the
    classification itself could only ever come back "unreadable" — turning a
    proven pre-claim refusal into a fail-closed unknown.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = batch_request(run_id=run_id)
    await _freeze(session_maker, reader, request)

    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(FakeMutator()))

    def explode(*args, **kwargs):
        raise RuntimeError("synthetic failure before anything was claimed")

    monkeypatch.setattr(runner_module, "build_stage_plan", explode)

    payload, code = await _run_cli_pooled(
        ["create", "--preview-run-id", str(run_id), "--apply", "--plan-digest", "x", "--confirm", "y"]
    )

    # Nothing was claimed and the ledger said so, so the refusal is provable
    # and exit 4 is honest. Under the old split this could not be established
    # at all and the command fell back to a fail-closed unknown.
    assert code == cli.EXIT_CONTRACT_MISMATCH
    assert payload["outcome"] == "refused"
    assert payload["external_effect_attempted"] is False
    assert payload["batch"]["exists"] is True
    _assert_pii_free(payload, count=count)


async def test_one_invocation_enters_exactly_one_event_loop(
    session_maker, batch_configuration, binding_key, monkeypatch
) -> None:
    """The structural half of the same guarantee.

    A pooled connection belongs to one loop, so the number of loops an
    invocation enters is the number of times it can poison its own pool. One
    CLI process is one invocation is one event loop; ``main`` is the only place
    that enters it.
    """
    from altegio_bot.scripts import easyweek_voucher_snapshot_batch as cli

    count = 2
    run_id, _ = await seed_batch_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    await _freeze(session_maker, reader, batch_request(run_id=run_id))

    monkeypatch.setattr(cli, "EasyWeekClient", lambda *a, **k: _AsyncCM(reader))
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", lambda *a, **k: _AsyncCM(FakeMutator()))

    entered: list[str] = []

    def run() -> int:
        # Counted inside the worker thread and restored before it returns, so
        # nothing about the test's own loop is disturbed.
        original = asyncio.run

        def counting_run(coro, **kwargs):
            entered.append("loop")
            return original(coro, **kwargs)

        asyncio.run = counting_run  # type: ignore[assignment]
        try:
            import io
            from contextlib import redirect_stdout

            with redirect_stdout(io.StringIO()):
                return cli.main(
                    [
                        "create",
                        "--preview-run-id",
                        str(run_id),
                        "--apply",
                        "--plan-digest",
                        "x",
                        "--confirm",
                        "y",
                    ]
                )
        finally:
            asyncio.run = original  # type: ignore[assignment]

    import altegio_bot.db as app_db

    try:
        await asyncio.to_thread(run)
    finally:
        await app_db.engine.dispose(close=False)

    assert entered == ["loop"], f"expected exactly one event loop, entered {len(entered)}"


def test_the_module_enters_the_event_loop_in_exactly_one_place() -> None:
    """Read from the source, so a future helper cannot quietly add a second.

    Docstrings mention ``asyncio.run`` on purpose — the rule is worth
    explaining where it is enforced — so only executable statements are
    counted.
    """
    import ast
    from pathlib import Path

    source = Path("src/altegio_bot/scripts/easyweek_voucher_snapshot_batch.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    sites = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
    ]
    assert len(sites) == 1, f"expected one asyncio.run call site, found {len(sites)}"

    # And it is in the sync entry point, not buried in a helper.
    enclosing = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and any(call is sites[0] for call in ast.walk(node))
    ]
    assert enclosing == ["main"], enclosing
