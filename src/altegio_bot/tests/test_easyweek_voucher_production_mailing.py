"""The production EasyWeek voucher mailing (§42 / PR-19).

§41 proved the whole irreversible sequence for a bounded handful of manually
selected people. This phase is the working mode, and everything these tests
guard follows from the two things that changed:

* **how many** is the operator's to state, not the schema's to cap. A freeze
  needs an explicit count and exposure, both must describe the full active
  snapshot exactly, and there is no ceiling and no truncation — six recipients
  work, twelve work, and a snapshot of six approved as four refuses whole.
* **which batch** is always named. Mailings are plural, slot numbers repeat
  across them, and one batch's approval can never authorise another's stage.

Everything §41 earned is still guarded here: nothing external happens before a
claim is committed; the first unknown stops every later slot of its own batch
and is never retried; a delivery is one attempt per slot for the lifetime of the
row; a refund is pre-send only; one person gets one voucher per campaign period;
and no voucher code, phone number, name or customer UUID appears anywhere except
in memory.

Every identity here is synthetic, and the voucher codes are sentinels the
secrecy tests hunt for by name.
"""

from __future__ import annotations

import asyncio
import dataclasses
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
from altegio_bot.campaigns.easyweek_voucher_batch.ledger import VOUCHER_BATCH_DOMAIN
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import voucher_code_mac
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_production.baseline import (
    PRODUCTION_BASELINE_TEMPLATE_FACTS,
    PRODUCTION_BASELINE_VERSION,
    prove_production_baseline,
)
from altegio_bot.campaigns.easyweek_voucher_production.composition import (
    BatchApproval,
    prove_production_composition,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPROVAL_COUNT_MISMATCH,
    APPROVAL_COUNT_MISSING,
    APPROVAL_EXPOSURE_MISMATCH,
    APPROVAL_EXPOSURE_MISSING,
    BASELINE_DRIFT,
    BATCH_HALTED,
    BATCH_PREVIEW_MISMATCH,
    BATCH_UNKNOWN,
    BOOKING_LINK_UNPROVEN,
    COMPOSITION_DRIFTED,
    COMPOSITION_EMPTY,
    COMPOSITION_MIXED_BASIS,
    CONFIRMATION_MISMATCH,
    ENTITLEMENT_ALREADY_EXISTS,
    FROZEN_DIGEST_MISMATCH,
    HALTED_BY_PREDECESSOR,
    IDENTITY_BINDING_MISMATCH,
    LEDGER_STATE_UNEXPECTED,
    PLAN_DIGEST_MISMATCH,
    PLAN_EXPIRED,
    PREVIEW_ALREADY_CONSUMED,
    PREVIEW_ALREADY_FROZEN,
    PRODUCTION_DISABLED,
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
    binding_material,
    production_marker,
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
    VOUCHER_PRODUCTION_COMPLETED,
    VOUCHER_PRODUCTION_HALTED,
    VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_CREATED,
    VOUCHER_PRODUCTION_ITEM_DELIVERED,
    VOUCHER_PRODUCTION_ITEM_PAID,
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_READ,
    CampaignRecipient,
    EasyWeekManualVoucherDeliveryLedger,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherSnapshotBatch,
    EasyWeekVoucherSnapshotBatchItem,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_production_fixtures import (  # noqa: F401 - fixtures
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
    SEPTEMBER_END,
    SEPTEMBER_START,
    STAFFER_UUID,
    VOUCHER_CODE_SENTINELS,
    FakeMutator,
    FakeReader,
    FakeSender,
    accepted_outcome,
    approval_for,
    customer_payload,
    customers_page,
    issued_voucher,
    location_map,
    marker_orders,
    markers_for,
    orders_page,
    production_request,
    rejected_outcome,
    seed_production_preview,
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


async def _plan(session_maker, reader, *, stage, request, approval=None, slot=None):
    async with session_maker() as session:
        plan, *_ = await runner_module.build_stage_plan(
            session,
            session_maker,
            stage=stage,
            request=request,
            reader=reader,
            order_reader=reader,
            approval=approval,
            slot=slot,
        )
    return plan


async def _apply(session_maker, reader, *, stage, request, approval=None, slot=None, expect_ready=True, **extra):
    """Plan the stage live, then apply it with that plan's own approval.

    Exactly what the runbook asks an operator to do, and the only path any of
    these tests ever uses to make something happen.
    """
    plan = await _plan(session_maker, reader, stage=stage, request=request, approval=approval, slot=slot)
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
    if stage == STAGE_FREEZE:
        common["approval"] = approval if approval is not None else BatchApproval()
    async with session_maker() as session:
        return await _RUNNERS[stage](session, session_maker, **common, **extra)


async def _freeze(session_maker, reader, request, *, count):
    """Freeze the whole active snapshot, approved correctly, and return the id."""
    report = await _apply(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(count))
    return report


async def _batch_id(session_maker, *, run_id):
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    return snapshot.batch_id


def _ok_response(index: int) -> VoucherMutationResponse:
    """A 2xx whose body names the order. A claim, never a proof."""
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _full_create(session_maker, reader, request, *, count, batch_id, offset=0):
    """Create every slot of one batch, leaving it at ``created``."""
    mutator = FakeMutator(create_sequence=[_ok_response(offset + index) for index in range(count)])
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id, offset=offset))
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    return report, mutator


async def _full_pay(session_maker, reader, request, *, count, batch_id, offset=0):
    """Pay every created slot, with the orders open until the POST settles them.

    The plan requires an OPEN order and the readback requires a PAID one, which
    is the real sequence; the mutator flips each order as it pays it.
    """
    paid = await marker_orders(session_maker, batch_id=batch_id, offset=offset, status="paid")
    mutator = FakeMutator(
        pay_sequence=[_ok_response(offset + index) for index in range(count)],
        reader=reader,
        settles=paid,
    )
    report = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=mutator)
    return report, mutator


# ===========================================================================
# The size and the money: stated by the operator, checked by the schema
# ===========================================================================


@pytest.mark.parametrize("count", [1, 2, 6, 12])
async def test_a_mailing_of_any_stated_size_freezes(session_maker, production_configuration, binding_key, count):
    """One, two, six and twelve all freeze. §41 could not hold the last two."""
    run_id, recipient_ids = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request = production_request(run_id=run_id)

    report = await _freeze(session_maker, reader, request, count=count)

    assert report.outcome == "frozen"
    batch = report.batch
    assert batch["recipient_count"] == count
    assert batch["approved_recipient_count"] == count
    assert batch["total_exposure_minor"] == count * UNIT_PRICE_MINOR
    assert batch["approved_exposure_minor"] == count * UNIT_PRICE_MINOR
    assert [entry["slot"] for entry in batch["items"]] == list(range(1, count + 1))
    # Slot order is the operator's own add order, deterministically.
    assert [entry["campaign_recipient_id"] for entry in batch["items"]] == recipient_ids


async def test_no_recipient_ceiling_is_reported_anywhere(session_maker, production_configuration, binding_key):
    """The reports say there is no ceiling rather than leaving a reader to guess."""
    run_id, _ = await seed_production_preview(session_maker, count=6)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=6)
    request = production_request(run_id=run_id)

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(6))
    composition = plan.snapshot["composition"]
    assert composition["max_recipients"] is None
    assert composition["recipient_ceiling_applies"] is False
    # And what DOES bound the money is stated, as a relationship.
    assert composition["approval_arithmetic"] == "approved_exposure_minor = expected_recipient_count * 1500"


async def test_a_freeze_without_an_approval_refuses(session_maker, production_configuration, binding_key):
    """No count and no exposure is not a small mailing: it is an absent approval."""
    run_id, _ = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    request = production_request(run_id=run_id)

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=BatchApproval())

    assert not plan.ready
    assert APPROVAL_COUNT_MISSING in plan.reasons
    assert APPROVAL_EXPOSURE_MISSING in plan.reasons
    assert await _batch_id(session_maker, run_id=run_id) is None


@pytest.mark.parametrize(
    ("approved_count", "approved_exposure", "expected"),
    [
        # Four approved for a snapshot of six: the classic "I'll do the rest
        # later" mistake, and the one that must never quietly take the first
        # four.
        (4, 4 * UNIT_PRICE_MINOR, APPROVAL_COUNT_MISMATCH),
        # More than are there.
        (8, 8 * UNIT_PRICE_MINOR, APPROVAL_COUNT_MISMATCH),
        # The right count with the wrong money.
        (6, 4 * UNIT_PRICE_MINOR, APPROVAL_EXPOSURE_MISMATCH),
        (6, 6 * UNIT_PRICE_MINOR + 1, APPROVAL_EXPOSURE_MISMATCH),
        # Zero and negative are absent approvals, not wrong ones.
        (0, 0, APPROVAL_COUNT_MISSING),
        (-1, -1500, APPROVAL_COUNT_MISSING),
    ],
)
async def test_a_mismatched_approval_refuses_the_whole_freeze(
    session_maker, production_configuration, binding_key, approved_count, approved_exposure, expected
):
    """Nothing is truncated, nothing is dropped, and no number is invented."""
    run_id, _ = await seed_production_preview(session_maker, count=6)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=6)
    request = production_request(run_id=run_id)

    report = await _apply(
        session_maker,
        reader,
        stage=STAGE_FREEZE,
        request=request,
        approval=BatchApproval(approved_count, approved_exposure),
        expect_ready=False,
    )

    assert report.outcome == "refused"
    assert expected in report.reasons
    # And above all: no batch, so no partial mailing of the convenient four.
    assert await _batch_id(session_maker, run_id=run_id) is None


async def test_an_empty_snapshot_refuses_however_it_is_approved(session_maker, production_configuration, binding_key):
    """An empty mailing is not a small one: there is nothing to approve."""
    run_id, _ = await seed_production_preview(session_maker, count=0)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=0)
    request = production_request(run_id=run_id)

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(1))

    assert not plan.ready
    assert COMPOSITION_EMPTY in plan.reasons


async def test_the_database_refuses_an_approval_that_does_not_match_its_batch(session_maker):
    """The arithmetic is a CHECK, not only a branch in the composition."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    now = utcnow()
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionBatch(
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        request_schema_version="1",
                        baseline_version=PRODUCTION_BASELINE_VERSION,
                        provider=PROVIDER_EASYWEEK,
                        company_id=COMPANY_ID,
                        campaign_code="new_clients_monthly",
                        recipient_basis="operator_manual_selection",
                        campaign_run_id=run_id,
                        campaign_period_start=PERIOD_START,
                        campaign_period_end=PERIOD_END,
                        location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                        staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                        payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                        voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                        frozen_digest="f" * 64,
                        recipient_count=2,
                        voucher_unit_price_minor=UNIT_PRICE_MINOR,
                        total_exposure_minor=2 * UNIT_PRICE_MINOR,
                        # The lie: approved for one person, frozen for two.
                        approved_recipient_count=1,
                        approved_exposure_minor=UNIT_PRICE_MINOR,
                        status="frozen",
                        frozen_at=now,
                        evidence={},
                        created_at=now,
                        updated_at=now,
                    )
                )


async def test_the_database_refuses_an_empty_batch(session_maker):
    """``recipient_count >= 1``. There is no zero-recipient mailing."""
    run_id, _ = await seed_production_preview(session_maker, count=1)
    now = utcnow()
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    EasyWeekVoucherProductionBatch(
                        batch_scope="easyweek_voucher_production_mailing_v1",
                        request_schema_version="1",
                        baseline_version=PRODUCTION_BASELINE_VERSION,
                        provider=PROVIDER_EASYWEEK,
                        company_id=COMPANY_ID,
                        campaign_code="new_clients_monthly",
                        recipient_basis="operator_manual_selection",
                        campaign_run_id=run_id,
                        campaign_period_start=PERIOD_START,
                        campaign_period_end=PERIOD_END,
                        location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                        staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                        payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                        voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                        frozen_digest="f" * 64,
                        recipient_count=0,
                        voucher_unit_price_minor=UNIT_PRICE_MINOR,
                        total_exposure_minor=0,
                        approved_recipient_count=0,
                        approved_exposure_minor=0,
                        status="frozen",
                        frozen_at=now,
                        evidence={},
                        created_at=now,
                        updated_at=now,
                    )
                )


# ===========================================================================
# Many batches, each addressed by its own id
# ===========================================================================


async def test_two_independent_batches_coexist_with_the_same_slot_numbers(
    session_maker, production_configuration, binding_key
):
    """Slot 1 exists in both. A slot is a position, never a global identifier."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=2, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=3, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 13)))

    report_a = await _freeze(session_maker, reader, production_request(run_id=run_a), count=2)
    report_b = await _freeze(session_maker, reader, production_request(run_id=run_b), count=3)

    assert report_a.outcome == "frozen"
    assert report_b.outcome == "frozen"
    batch_a, batch_b = report_a.batch["batch_id"], report_b.batch["batch_id"]
    assert batch_a != batch_b
    # Both hold a slot 1, and neither hides the other's.
    assert [entry["slot"] for entry in report_a.batch["items"]] == [1, 2]
    assert [entry["slot"] for entry in report_b.batch["items"]] == [1, 2, 3]
    async with session_maker() as session:
        total = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatchItem))
        slot_ones = await session.scalar(
            select(func.count())
            .select_from(EasyWeekVoucherProductionBatchItem)
            .where(EasyWeekVoucherProductionBatchItem.slot == 1)
        )
    assert total == 5
    assert slot_ones == 2


async def test_one_preview_cannot_be_frozen_twice(session_maker, production_configuration, binding_key):
    """The second freeze of a preview finds a wall, not a second mailing."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    request = production_request(run_id=run_id)

    first = await _freeze(session_maker, reader, request, count=2)
    assert first.outcome == "frozen"

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(2))
    assert not plan.ready
    assert PREVIEW_ALREADY_FROZEN in plan.reasons

    async with session_maker() as session:
        headers = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatch))
    assert headers == 1


async def test_the_database_refuses_a_second_batch_on_one_preview(session_maker):
    """``campaign_run_id`` is UNIQUE. Not a branch somebody could forget."""
    run_id, _ = await seed_production_preview(session_maker, count=1)
    now = utcnow()

    def header() -> EasyWeekVoucherProductionBatch:
        return EasyWeekVoucherProductionBatch(
            batch_scope="easyweek_voucher_production_mailing_v1",
            request_schema_version="1",
            baseline_version=PRODUCTION_BASELINE_VERSION,
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            campaign_code="new_clients_monthly",
            recipient_basis="operator_manual_selection",
            campaign_run_id=run_id,
            campaign_period_start=PERIOD_START,
            campaign_period_end=PERIOD_END,
            location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
            staffer_uuid=uuid_module.UUID(STAFFER_UUID),
            payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
            voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
            frozen_digest="f" * 64,
            recipient_count=1,
            voucher_unit_price_minor=UNIT_PRICE_MINOR,
            total_exposure_minor=UNIT_PRICE_MINOR,
            approved_recipient_count=1,
            approved_exposure_minor=UNIT_PRICE_MINOR,
            status="frozen",
            frozen_at=now,
            evidence={},
            created_at=now,
            updated_at=now,
        )

    async with session_maker() as session:
        async with session.begin():
            session.add(header())
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(header())


async def test_a_stage_without_a_batch_id_refuses(session_maker, production_configuration, binding_key):
    """There is no "latest batch". A stage that is not told which one refuses."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)

    # The same preview, and a request that names no batch at all.
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=production_request(run_id=run_id))

    assert not plan.ready
    assert BATCH_UNKNOWN in plan.reasons


async def test_an_unknown_batch_id_refuses(session_maker, production_configuration, binding_key):
    """A digit slip is a refusal, not a stage against something else."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    real = await _batch_id(session_maker, run_id=run_id)

    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_CREATE,
        request=production_request(run_id=run_id, batch_id=real + 999),
    )

    assert not plan.ready
    assert BATCH_UNKNOWN in plan.reasons


async def test_a_batch_id_from_another_preview_refuses(session_maker, production_configuration, binding_key):
    """Both arguments are required AND compared. One month cannot act on another's."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=2, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=2, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 12)))
    await _freeze(session_maker, reader, production_request(run_id=run_a), count=2)
    await _freeze(session_maker, reader, production_request(run_id=run_b), count=2)
    batch_b = await _batch_id(session_maker, run_id=run_b)

    # Preview A, batch B. Each exists; together they are wrong.
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_CREATE,
        request=production_request(run_id=run_a, batch_id=batch_b),
    )

    assert not plan.ready
    assert BATCH_PREVIEW_MISMATCH in plan.reasons


async def test_an_approval_for_one_batch_cannot_authorise_another(session_maker, production_configuration, binding_key):
    """Cross-batch replay. The batch identity is inside the signed snapshot."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=2, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=2, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 12)))
    await _freeze(session_maker, reader, production_request(run_id=run_a), count=2)
    await _freeze(session_maker, reader, production_request(run_id=run_b), count=2)
    batch_a = await _batch_id(session_maker, run_id=run_a)
    batch_b = await _batch_id(session_maker, run_id=run_b)
    request_a = production_request(run_id=run_a, batch_id=batch_a)
    request_b = production_request(run_id=run_b, batch_id=batch_b)

    reader.orders.update(await marker_orders(session_maker, batch_id=batch_a, offset=0))
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_b, offset=10))

    # A perfectly good CREATE plan for batch A...
    plan_a = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request_a)
    assert plan_a.ready, plan_a.reasons

    # ...pasted into batch B's create command.
    mutator = FakeMutator(create_sequence=[_ok_response(10), _ok_response(11)])
    async with session_maker() as session:
        report = await runner_module.run_create(
            session,
            session_maker,
            request=request_b,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=plan_a.digest,
            supplied_issued_at=plan_a.issued_at,
            supplied_phrase=plan_a.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert PLAN_DIGEST_MISMATCH in report.reasons
    assert mutator.calls == []


async def test_a_halt_in_one_batch_does_not_stop_another(session_maker, production_configuration, binding_key):
    """Separate mailings, separate approvals, separate state. August is not September."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=2, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=2, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 12)))
    await _freeze(session_maker, reader, production_request(run_id=run_a), count=2)
    await _freeze(session_maker, reader, production_request(run_id=run_b), count=2)
    batch_a = await _batch_id(session_maker, run_id=run_a)
    batch_b = await _batch_id(session_maker, run_id=run_b)
    request_a = production_request(run_id=run_a, batch_id=batch_a)
    request_b = production_request(run_id=run_b, batch_id=batch_b)

    # Batch A's first CREATE times out, which halts A.
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_a, offset=0))
    mutator_a = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    report_a = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request_a, mutator=mutator_a)
    assert report_a.outcome == "unknown"
    assert report_a.halted

    # Batch B is untouched and still actable.
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_b, offset=10))
    report_b, mutator_b = await _full_create(session_maker, reader, request_b, count=2, batch_id=batch_b, offset=10)
    assert report_b.outcome == "applied"
    assert not report_b.halted
    assert len(mutator_b.create_calls) == 2

    snapshot_a = await ledger_module.load(session_maker, batch_id=batch_a)
    assert snapshot_a.halted


async def test_status_lists_every_batch_and_prints_one_in_full(session_maker, production_configuration, binding_key):
    """The listing is how an operator finds the id they then have to type."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=2, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=3, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 13)))
    await _freeze(session_maker, reader, production_request(run_id=run_a), count=2)
    await _freeze(session_maker, reader, production_request(run_id=run_b), count=3)
    batch_a = await _batch_id(session_maker, run_id=run_a)

    listing = await runner_module.run_status(session_maker)
    payload = listing.as_safe_dict()
    assert len(payload["batches"]) == 2
    assert {entry["recipient_count"] for entry in payload["batches"]} == {2, 3}
    # The listing itself carries no composition: it is a finding aid.
    assert payload["batch"]["exists"] is False

    detail = (await runner_module.run_status(session_maker, batch_id=batch_a)).as_safe_dict()
    assert detail["batch"]["batch_id"] == batch_a
    assert len(detail["batch"]["items"]) == 2


# ===========================================================================
# The whole sequence, on a mailing bigger than §41 could hold
# ===========================================================================


async def test_the_full_sequence_on_six_recipients(session_maker, production_configuration, binding_key):
    """Freeze, create, pay, deliver — six people, one call each, in slot order."""
    count = 6
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    request_freeze = production_request(run_id=run_id)

    frozen = await _freeze(session_maker, reader, request_freeze, count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)

    created, create_mutator = await _full_create(session_maker, reader, request, count=count, batch_id=batch_id)
    assert created.outcome == "applied"
    assert created.external_calls == {"create": count, "pay": 0, "refund": 0, "meta": 0}
    assert len(create_mutator.create_calls) == count
    # Every CREATE carried the FROZEN identity, never the environment's.
    for call in create_mutator.create_calls:
        assert call["price_minor"] == UNIT_PRICE_MINOR
        assert call["staffer_uuid"] == STAFFER_UUID
        assert call["location_uuid"] == KARLSRUHE_LOCATION_UUID

    paid, pay_mutator = await _full_pay(session_maker, reader, request, count=count, batch_id=batch_id)
    assert paid.outcome == "applied"
    assert paid.external_calls["pay"] == count
    for call in pay_mutator.pay_calls:
        assert call["account_uuid"] == ACCOUNT_UUID

    sender = FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert delivered.outcome == "applied"
    assert delivered.external_calls["meta"] == count
    assert sender.calls == count
    # The code really did reach the template parameters, and only there.
    assert all(sender.saw_codes)
    assert sender.destinations == list(PHONES[:count])

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.status == VOUCHER_PRODUCTION_COMPLETED
    assert all(entry.status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED for entry in snapshot.items)
    assert all(entry.send_attempt_count == 1 for entry in snapshot.items)


async def test_a_twelve_recipient_mailing_makes_exactly_twelve_calls(
    session_maker, production_configuration, binding_key
):
    """One external call per slot, at a size where an accidental N-squared shows.

    The guard is the CALL COUNT, not the clock: a per-item re-proof of the whole
    snapshot would show up here as twelve times the customer reads, which is
    exactly what §41's shape would have done.
    """
    count = 12
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)

    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)

    # Two whole-composition proofs per operator stage, and only two: the plan
    # the operator reads, and the rebuild the apply does live before the first
    # claim. So 2N live customer reads per stage — linear in the size of the
    # mailing, and independent of how many slots the stage then claims.
    #
    # This is the anti-quadratic guard, and it is a call count rather than a
    # clock reading. §41's shape re-proved the entire composition inside the
    # per-item loop; here that would be N + N*N = 156 reads for twelve people
    # instead of 24.
    reads_after_freeze = len(reader.customer_calls)
    assert reads_after_freeze == 2 * count

    created, mutator = await _full_create(session_maker, reader, request, count=count, batch_id=batch_id)
    assert created.outcome == "applied"
    assert len(mutator.create_calls) == count
    assert len(reader.customer_calls) == reads_after_freeze + 2 * count

    paid, pay_mutator = await _full_pay(session_maker, reader, request, count=count, batch_id=batch_id)
    assert paid.outcome == "applied"
    assert len(pay_mutator.pay_calls) == count

    sender = FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert delivered.outcome == "applied"
    assert sender.calls == count

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.recipient_count == count
    assert snapshot.total_exposure_minor == count * UNIT_PRICE_MINOR


# ===========================================================================
# The first unknown stops the suffix, and continuation is operator-driven
# ===========================================================================


async def test_the_first_unknown_create_stops_every_later_slot(session_maker, production_configuration, binding_key):
    """Slot 1 created, slot 2 unknown, slots 3-6 never attempted at all."""
    count = 6
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    mutator = FakeMutator(create_sequence=[_ok_response(0), EasyWeekVoucherMutationUnknown("timeout")])
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    assert report.outcome == "unknown"
    assert report.halted
    # Exactly two POSTs: the one that worked and the one that went unknown.
    assert len(mutator.create_calls) == 2
    assert report.external_calls["create"] == 2

    outcomes = {entry.slot: entry.outcome for entry in report.slots}
    assert outcomes[1] == "created"
    assert outcomes[2] == "unknown"
    assert all(outcomes[slot] == "not_attempted" for slot in range(3, count + 1))
    assert all(HALTED_BY_PREDECESSOR in entry.reasons for entry in report.slots if entry.slot >= 3)

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.status == VOUCHER_PRODUCTION_HALTED
    assert snapshot.item(1).status == VOUCHER_PRODUCTION_ITEM_CREATED
    assert snapshot.item(2).status == VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN
    assert all(snapshot.item(slot).status == VOUCHER_PRODUCTION_ITEM_PLANNED for slot in range(3, count + 1))


async def test_a_halted_batch_refuses_the_next_plan(session_maker, production_configuration, binding_key):
    """The suffix is not claimable until a human has resolved what went wrong."""
    run_id, _ = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=3)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    mutator = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert not plan.ready
    assert BATCH_HALTED in plan.reasons


async def test_an_unknown_create_is_recovered_by_reading_not_by_repeating(
    session_maker, production_configuration, binding_key
):
    """Reconcile proves the order exists. It never sends a second CREATE."""
    run_id, _ = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=3)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)

    # Slot 1's CREATE times out, but the order really was created.
    mutator = FakeMutator(create_sequence=[EasyWeekVoucherMutationUnknown("timeout")])
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
    assert report.outcome == "unknown"

    markers = await markers_for(session_maker, batch_id=batch_id)
    # Rebuilt AFTER the claim, so the order is dated inside the create window
    # the ledger actually wrote — which is what the marker search bounds itself
    # by, and what an operator looking in the dashboard would really find.
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    reader.order_pages = [orders_page([reader.orders[ORDER_UUIDS[0]]])]

    reconciled = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)

    assert reconciled.external_calls == {"create": 0, "pay": 0, "refund": 0, "meta": 0}
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).status == VOUCHER_PRODUCTION_ITEM_CREATED
    # The halt lifts as a consequence of the slot resolving, not by decree.
    assert not snapshot.halted
    assert markers[1] == production_marker(
        preview_run_id=run_id, campaign_recipient_id=snapshot.item(1).campaign_recipient_id, slot=1
    )


async def test_after_a_recovery_the_operator_continues_the_same_batch(
    session_maker, production_configuration, binding_key
):
    """A fresh plan, a new confirmation, the SAME batch — and no repeated effects."""
    count = 4
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    # Slots 1-2 succeed, slot 2's answer is lost, 3-4 untouched.
    first = FakeMutator(create_sequence=[_ok_response(0), EasyWeekVoucherMutationUnknown("timeout")])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=first)

    # The operator reconciles slot 2 by looking; the order is there, dated
    # inside the window the claim recorded.
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    reader.order_pages = [orders_page([reader.orders[ORDER_UUIDS[1]]])]
    await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert not snapshot.halted
    assert snapshot.item(2).status == VOUCHER_PRODUCTION_ITEM_CREATED

    # A fresh plan now covers ONLY the slots that were never started.
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert plan.ready, plan.reasons
    assert plan.snapshot["target_slots"] == [3, 4]

    second = FakeMutator(create_sequence=[_ok_response(2), _ok_response(3)])
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=second)
    assert report.outcome == "applied"
    # Two more calls, for the two untouched slots. Nothing was repeated.
    assert len(second.create_calls) == 2
    markers = await markers_for(session_maker, batch_id=batch_id)
    assert {call["marker"] for call in second.create_calls} == {markers[3], markers[4]}

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert all(entry.status == VOUCHER_PRODUCTION_ITEM_CREATED for entry in snapshot.items)


async def test_a_proven_rejection_does_not_halt_the_rest(session_maker, production_configuration, binding_key):
    """A validation refusal about one person says nothing about the next."""
    count = 4
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    mutator = FakeMutator(
        create_sequence=[
            _ok_response(0),
            EasyWeekPermanentError("422", status_code=422),
            _ok_response(2),
            _ok_response(3),
        ]
    )
    report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)

    assert report.outcome == "partial"
    assert not report.halted
    assert len(mutator.create_calls) == count
    outcomes = {entry.slot: entry.outcome for entry in report.slots}
    assert outcomes == {1: "created", 2: "rejected", 3: "created", 4: "created"}


# ===========================================================================
# One person, one voucher per campaign period
# ===========================================================================


async def test_the_same_person_in_two_previews_of_one_period_refuses(
    session_maker, production_configuration, binding_key
):
    """A fresh preview is not a fresh entitlement. Same wave, same person, no."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=2, offset=0)
    # A second preview for the SAME August wave holding person 1 again.
    run_b, _ = await seed_production_preview(
        session_maker, count=2, offset=0, period_start=PERIOD_START, period_end=PERIOD_END
    )
    reader = FakeReader(count=2)

    first = await _freeze(session_maker, reader, production_request(run_id=run_a), count=2)
    assert first.outcome == "frozen"

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_b), approval=approval_for(2)
    )
    assert not plan.ready
    assert ENTITLEMENT_ALREADY_EXISTS in plan.reasons


async def test_the_same_person_in_a_different_period_is_allowed(session_maker, production_configuration, binding_key):
    """A new wave IS a new entitlement. That is what a monthly campaign means."""
    await seed_template_and_sender(session_maker)
    run_aug, _ = await seed_production_preview(session_maker, count=2, offset=0)
    run_sep, _ = await seed_production_preview(
        session_maker, count=2, offset=0, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    reader = FakeReader(count=2)

    assert (await _freeze(session_maker, reader, production_request(run_id=run_aug), count=2)).outcome == "frozen"
    assert (await _freeze(session_maker, reader, production_request(run_id=run_sep), count=2)).outcome == "frozen"

    async with session_maker() as session:
        rows = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatchItem))
    assert rows == 4


async def test_the_database_enforces_the_entitlement_across_batches(session_maker):
    """The unique index, not the check in the composition, is the arbiter."""
    run_a, recipients_a = await seed_production_preview(session_maker, count=1, offset=0)
    run_b, recipients_b = await seed_production_preview(session_maker, count=1, offset=0)
    now = utcnow()

    async def header(run_id: int) -> int:
        async with session_maker() as session:
            async with session.begin():
                row = EasyWeekVoucherProductionBatch(
                    batch_scope="easyweek_voucher_production_mailing_v1",
                    request_schema_version="1",
                    baseline_version=PRODUCTION_BASELINE_VERSION,
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    campaign_code="new_clients_monthly",
                    recipient_basis="operator_manual_selection",
                    campaign_run_id=run_id,
                    campaign_period_start=PERIOD_START,
                    campaign_period_end=PERIOD_END,
                    location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                    staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                    payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                    voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                    frozen_digest="f" * 64,
                    recipient_count=1,
                    voucher_unit_price_minor=UNIT_PRICE_MINOR,
                    total_exposure_minor=UNIT_PRICE_MINOR,
                    approved_recipient_count=1,
                    approved_exposure_minor=UNIT_PRICE_MINOR,
                    status="frozen",
                    frozen_at=now,
                    evidence={},
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
                await session.flush()
                return int(row.id)

    def item(batch_id: int, run_id: int, recipient_id: int, marker: str):
        return EasyWeekVoucherProductionBatchItem(
            batch_id=batch_id,
            batch_recipient_count=1,
            slot=1,
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            campaign_code="new_clients_monthly",
            recipient_basis="operator_manual_selection",
            campaign_run_id=run_id,
            campaign_recipient_id=recipient_id,
            # The SAME human being.
            easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[0]),
            campaign_period_start=PERIOD_START,
            campaign_period_end=PERIOD_END,
            voucher_value_minor=UNIT_PRICE_MINOR,
            voucher_quantity=1,
            reconciliation_marker=marker,
            status=VOUCHER_PRODUCTION_ITEM_PLANNED,
            evidence={},
            created_at=now,
            updated_at=now,
        )

    batch_a = await header(run_a)
    batch_b = await header(run_b)
    async with session_maker() as session:
        async with session.begin():
            session.add(item(batch_a, run_a, recipients_a[0], "marker-a"))
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(item(batch_b, run_b, recipients_b[0], "marker-b"))


async def test_a_recipient_of_the_historical_manual_canary_is_excluded(
    session_maker, production_configuration, binding_key
):
    """§37.2's person is spent, blanket, whatever the period says.

    Stricter than this phase's own entitlement rule and deliberately so: the
    canary people hold a real €15 code from this exact campaign, and "the old
    ledger is completed", "it was a different preview" and "we are sending in a
    different month" are none of them reasons to give them a second one.
    """
    run_id, recipient_ids = await seed_production_preview(session_maker, count=2)
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
                    location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                    staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                    payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                    voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                    reconciliation_marker="ewmv1-historical",
                    status="read",
                )
            )

    reader = FakeReader(count=2)
    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert PREVIEW_ALREADY_CONSUMED in plan.reasons


async def test_a_recipient_of_the_pr18_batch_is_excluded(session_maker, production_configuration, binding_key):
    """And §41's two people are spent too, by the same conservative rule."""
    run_id, recipient_ids = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            header = EasyWeekVoucherSnapshotBatch(
                batch_scope="easyweek_voucher_snapshot_batch_v1",
                request_schema_version="1",
                baseline_version="2026-09-27-43",
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                campaign_code="new_clients_monthly",
                recipient_basis="operator_manual_selection",
                campaign_run_id=run_id,
                campaign_period_start=PERIOD_START,
                campaign_period_end=PERIOD_END,
                location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                frozen_digest="a" * 64,
                recipient_count=1,
                voucher_unit_price_minor=UNIT_PRICE_MINOR,
                total_exposure_minor=UNIT_PRICE_MINOR,
                status="completed",
                frozen_at=now,
                evidence={},
                created_at=now,
                updated_at=now,
            )
            session.add(header)
            await session.flush()
            session.add(
                EasyWeekVoucherSnapshotBatchItem(
                    batch_id=header.id,
                    batch_recipient_count=1,
                    slot=1,
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    campaign_code="new_clients_monthly",
                    recipient_basis="operator_manual_selection",
                    campaign_run_id=run_id,
                    campaign_recipient_id=recipient_ids[1],
                    easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[1]),
                    campaign_period_start=PERIOD_START,
                    campaign_period_end=PERIOD_END,
                    voucher_value_minor=UNIT_PRICE_MINOR,
                    voucher_quantity=1,
                    reconciliation_marker="ewvb1-historical",
                    status="read",
                    evidence={},
                    created_at=now,
                    updated_at=now,
                )
            )

    reader = FakeReader(count=2)
    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert PREVIEW_ALREADY_CONSUMED in plan.reasons


async def test_one_preview_cannot_hold_the_same_customer_twice(session_maker):
    """Upstream of this phase entirely: the shared preview index forbids it.

    Worth asserting rather than assuming. The composition still carries a
    ``COMPOSITION_DUPLICATE_CUSTOMER`` refusal, and this is why that refusal is
    defence in depth rather than the only thing standing between one human and
    two vouchers in a single mailing.
    """
    run_id, _ = await seed_production_preview(session_maker, count=1)
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
                        # The customer person 1 already occupies in this run.
                        easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[0]),
                    )
                )


async def test_one_batch_cannot_hold_the_same_customer_twice(session_maker):
    """And the ledger says it again, per batch, in its own table."""
    run_id, recipient_ids = await seed_production_preview(session_maker, count=2)
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            header = EasyWeekVoucherProductionBatch(
                batch_scope="easyweek_voucher_production_mailing_v1",
                request_schema_version="1",
                baseline_version=PRODUCTION_BASELINE_VERSION,
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                campaign_code="new_clients_monthly",
                recipient_basis="operator_manual_selection",
                campaign_run_id=run_id,
                campaign_period_start=PERIOD_START,
                campaign_period_end=PERIOD_END,
                location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
                staffer_uuid=uuid_module.UUID(STAFFER_UUID),
                payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
                voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
                frozen_digest="f" * 64,
                recipient_count=2,
                voucher_unit_price_minor=UNIT_PRICE_MINOR,
                total_exposure_minor=2 * UNIT_PRICE_MINOR,
                approved_recipient_count=2,
                approved_exposure_minor=2 * UNIT_PRICE_MINOR,
                status="frozen",
                frozen_at=now,
                evidence={},
                created_at=now,
                updated_at=now,
            )
            session.add(header)
            await session.flush()
            batch_id = int(header.id)

    def item(slot: int, recipient_id: int, marker: str):
        return EasyWeekVoucherProductionBatchItem(
            batch_id=batch_id,
            batch_recipient_count=2,
            slot=slot,
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            campaign_code="new_clients_monthly",
            recipient_basis="operator_manual_selection",
            campaign_run_id=run_id,
            campaign_recipient_id=recipient_id,
            easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUIDS[0]),
            campaign_period_start=PERIOD_START,
            campaign_period_end=PERIOD_END,
            voucher_value_minor=UNIT_PRICE_MINOR,
            voucher_quantity=1,
            reconciliation_marker=marker,
            status=VOUCHER_PRODUCTION_ITEM_PLANNED,
            evidence={},
            created_at=now,
            updated_at=now,
        )

    async with session_maker() as session:
        async with session.begin():
            session.add(item(1, recipient_ids[0], "marker-slot-1"))
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(item(2, recipient_ids[1], "marker-slot-2"))


# ===========================================================================
# Wrong topology, wrong basis, wrong preview
# ===========================================================================


async def test_a_mixed_basis_snapshot_refuses_whole(session_maker, production_configuration, binding_key):
    """An owner-test row in the same preview cancels the composition."""
    run_id, _ = await seed_production_preview(
        session_maker, count=2, bases=["operator_manual_selection", RECIPIENT_BASIS_TEST]
    )
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert COMPOSITION_MIXED_BASIS in plan.reasons


@pytest.mark.parametrize(
    ("kwargs", "label"),
    [
        ({"provider": PROVIDER_ALTEGIO}, "another provider"),
        ({"company_id": 999999}, "another branch"),
        ({"campaign_code": "some_other_campaign"}, "another campaign"),
        ({"run_status": "running"}, "an unfinished preview"),
        ({"run_mode": "send-real"}, "a send-real run"),
    ],
)
async def test_a_foreign_run_refuses(session_maker, production_configuration, binding_key, kwargs, label):
    """One branch, one campaign, one basis, one completed preview."""
    run_id, _ = await seed_production_preview(session_maker, count=2, **kwargs)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready, label
    assert RUN_UNPROVEN in plan.reasons, label


# ===========================================================================
# Drift: every stage re-proves, and a drift costs zero external calls
# ===========================================================================


async def test_a_recipient_removed_after_the_freeze_stops_the_next_stage(
    session_maker, production_configuration, binding_key
):
    """The frozen digest is compared with the live composition, every stage."""
    run_id, recipient_ids = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=3)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)

    # The operator removes somebody after freezing — which changes nothing that
    # was frozen and everything that can be proven.
    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_ids[2])
            recipient.status = "skipped"

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert not plan.ready
    assert FROZEN_DIGEST_MISMATCH in plan.reasons or COMPOSITION_DRIFTED in plan.reasons

    mutator = FakeMutator(create_sequence=[_ok_response(0)])
    report = await _apply(
        session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator, expect_ready=False
    )
    assert report.outcome == "refused"
    assert mutator.calls == []


async def test_an_opt_out_after_the_freeze_stops_the_next_stage(session_maker, production_configuration, binding_key):
    """Somebody who opted out cannot be reached, and that refuses the stage."""
    run_id, recipient_ids = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)

    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_ids[1])
            recipient.is_opted_out = True

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert not plan.ready
    assert RECIPIENT_OPTED_OUT in plan.reasons


@pytest.mark.parametrize(
    ("break_it", "expected"),
    [
        ("template", TEMPLATE_UNPROVEN),
        ("sender", SENDER_UNPROVEN),
        ("booking_link", BOOKING_LINK_UNPROVEN),
    ],
)
async def test_the_delivery_surface_is_proven_before_the_first_create(
    session_maker, production_configuration, binding_key, monkeypatch, break_it, expected
):
    """Buying vouchers that could not be delivered is refused while it is free."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)

    if break_it == "template":
        async with session_maker() as session:
            async with session.begin():
                await session.execute(text("UPDATE message_templates SET is_active = false"))
    elif break_it == "sender":
        async with session_maker() as session:
            async with session.begin():
                await session.execute(text("UPDATE whatsapp_senders SET is_active = false"))
    else:
        monkeypatch.setattr(
            settings, "easyweek_location_map", location_map(booking_link="http://insecure"), raising=False
        )

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert expected in plan.reasons


async def test_a_baseline_drift_stops_the_stage(session_maker, production_configuration, binding_key):
    """42/42 is not close enough to 43/43. It is a product somebody edited."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2, template=template_payload(services=42, all_services=42))

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert BASELINE_DRIFT in plan.reasons


async def test_the_baseline_is_this_phases_own_and_reads_43(session_maker):
    """Its own constant, structurally separate from §41's and §37.2's."""
    assert PRODUCTION_BASELINE_VERSION == "2026-09-27-43"
    assert PRODUCTION_BASELINE_TEMPLATE_FACTS["services_count"] == 43
    assert PRODUCTION_BASELINE_TEMPLATE_FACTS["all_services_count"] == 43
    assert prove_production_baseline(template_payload()).proven
    assert not prove_production_baseline(template_payload(services=42, all_services=42)).proven
    # The two counts must AGREE, not merely each match a literal.
    assert not prove_production_baseline(template_payload(services=43, all_services=42)).proven


async def test_a_restarted_container_cannot_pay_from_another_account(
    session_maker, production_configuration, binding_key
):
    """Four UUIDs decide where real money goes. A drift in any of them refuses."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]

    drifted = dataclasses.replace(
        production_request(run_id=run_id, batch_id=batch_id),
        payment_account_uuid="99999999-9999-4999-8999-999999999999",
    )
    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=drifted)

    assert not plan.ready
    assert IDENTITY_BINDING_MISMATCH in plan.reasons


# ===========================================================================
# Approval: per stage, per batch, per moment
# ===========================================================================


async def test_an_expired_plan_authorises_nothing(session_maker, production_configuration, binding_key):
    """Thirty minutes, and not one scaled up for a bigger mailing."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    request = production_request(run_id=run_id)

    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(2))
    stale = plan.issued_at - timedelta(hours=2)
    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            approval=approval_for(2),
            apply=True,
            supplied_digest=plan.digest_for(stale),
            supplied_issued_at=stale,
            supplied_phrase=plan.phrase_for(plan.digest_for(stale)),
        )

    assert report.outcome == "refused"
    assert PLAN_EXPIRED in report.reasons
    assert await _batch_id(session_maker, run_id=run_id) is None


async def test_a_tampered_digest_authorises_nothing(session_maker, production_configuration, binding_key):
    """The digest is the token. A different one is not an approval."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    request = production_request(run_id=run_id)
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(2))

    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            approval=approval_for(2),
            apply=True,
            supplied_digest="0" * 64,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert PLAN_DIGEST_MISMATCH in report.reasons
    assert await _batch_id(session_maker, run_id=run_id) is None


async def test_a_wrong_confirmation_phrase_authorises_nothing(session_maker, production_configuration, binding_key):
    """And the phrase names this phase, so a §41 one cannot be pasted in."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    request = production_request(run_id=run_id)
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(2))
    assert plan.confirmation_phrase.startswith("freeze-voucher-production-")

    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            approval=approval_for(2),
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            # §41's spelling of the same stage.
            supplied_phrase=f"freeze-voucher-batch-{plan.digest[:12]}",
        )

    assert report.outcome == "refused"
    assert CONFIRMATION_MISMATCH in report.reasons


async def test_a_create_approval_cannot_authorise_a_pay(session_maker, production_configuration, binding_key):
    """One plan per stage. A create digest is useless for a payment."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    await _full_create(session_maker, reader, request, count=2, batch_id=batch_id)

    create_plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    mutator = FakeMutator(pay_sequence=[_ok_response(0), _ok_response(1)])
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
    assert PLAN_DIGEST_MISMATCH in report.reasons
    assert mutator.calls == []


async def test_without_apply_nothing_happens(session_maker, production_configuration, binding_key):
    """The default is that nothing happens, and it is not an error to explain."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    request = production_request(run_id=run_id)
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(2))

    async with session_maker() as session:
        report = await runner_module.run_freeze(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            approval=approval_for(2),
            apply=False,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert await _batch_id(session_maker, run_id=run_id) is None


async def test_the_fence_closes_every_stage_including_the_plan(session_maker, binding_key, monkeypatch):
    """A closed fence means the command does nothing at all."""
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_account_uuid", ACCOUNT_UUID, raising=False)
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert PRODUCTION_DISABLED in plan.reasons


async def test_the_pr18_fence_does_not_open_this_phase(session_maker, binding_key, monkeypatch):
    """§41's fence is not this one's, in either direction."""
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)
    # §41 wide open, §42 shut.
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_account_uuid", ACCOUNT_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_account_uuid", ACCOUNT_UUID, raising=False)
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)

    plan = await _plan(
        session_maker, reader, stage=STAGE_FREEZE, request=production_request(run_id=run_id), approval=approval_for(2)
    )

    assert not plan.ready
    assert PRODUCTION_DISABLED in plan.reasons


async def test_status_works_with_the_fence_closed(session_maker, production_configuration, binding_key, monkeypatch):
    """After an emergency `false`, the state must still be readable."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]

    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    report = await runner_module.run_status(session_maker, batch_id=batch_id)

    assert report.outcome == "observed"
    assert report.batch["batch_id"] == batch_id
    assert report.batch["recipient_count"] == 2


# ===========================================================================
# Delivery: one attempt per slot, ever
# ===========================================================================


async def _paid_batch(session_maker, reader, *, count, offset=0, run_id=None):
    """A batch with every slot proven paid, ready for DELIVER."""
    if run_id is None:
        run_id, _ = await seed_production_preview(session_maker, count=count, offset=offset)
    await seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    await _full_create(session_maker, reader, request, count=count, batch_id=batch_id, offset=offset)
    await _full_pay(session_maker, reader, request, count=count, batch_id=batch_id, offset=offset)
    return run_id, batch_id, request


async def test_a_delivered_slot_can_never_be_sent_again(session_maker, production_configuration, binding_key):
    """One attempt for the lifetime of the row. There is no second plan for it."""
    reader = FakeReader(count=2)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=2)

    sender = FakeSender()
    first = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert first.outcome == "applied"
    assert sender.calls == 2

    # A second DELIVER has no work, and says so rather than sending again.
    plan = await _plan(session_maker, reader, stage=STAGE_DELIVER, request=request)
    assert not plan.ready
    assert LEDGER_STATE_UNEXPECTED in plan.reasons

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert all(entry.send_attempt_count == 1 for entry in snapshot.items)


async def test_the_database_caps_the_attempt_counter_at_one(session_maker, production_configuration, binding_key):
    """Not a retry budget: a counter that can only be zero or one."""
    reader = FakeReader(count=1)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=1)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE easyweek_voucher_production_batch_items SET send_attempt_count = 2 WHERE batch_id = :b"
                    ),
                    {"b": batch_id},
                )


async def test_a_deliver_plan_refuses_a_slot_that_was_already_attempted(
    session_maker, production_configuration, binding_key
):
    """``DELIVERY_ALREADY_ATTEMPTED``, before anything reaches Meta."""
    reader = FakeReader(count=2)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=2)

    # Slot 1 attempted and rejected by Meta: proven, terminal, and still spent.
    sender = FakeSender(outcomes=[rejected_outcome(), accepted_outcome(1)])
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert report.outcome == "partial"
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).send_attempt_count == 1
    assert snapshot.item(2).send_attempt_count == 1


async def test_an_unknown_send_halts_and_is_never_reconciled_away(session_maker, production_configuration, binding_key):
    """A POS order cannot tell us whether Meta delivered. It waits for a human."""
    reader = FakeReader(count=3)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=3)

    sender = FakeSender(outcomes=[accepted_outcome(0), unknown_outcome()])
    report = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    assert report.outcome == "unknown"
    assert report.halted
    assert sender.calls == 2
    outcomes = {entry.slot: entry.outcome for entry in report.slots}
    assert outcomes == {1: "provider_accepted", 2: "unknown", 3: "not_attempted"}

    # A reconcile looks, records that it looked, and changes nothing about it.
    reconciled = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    assert reconciled.outcome == "unknown"
    assert reconciled.external_calls == {"create": 0, "pay": 0, "refund": 0, "meta": 0}
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.halted
    assert snapshot.item(2).status == "send_unknown"
    assert snapshot.item(2).send_attempt_count == 1


# ===========================================================================
# Refund: one named slot of one named batch, pre-send only
# ===========================================================================


async def test_a_refund_returns_one_paid_slot(session_maker, production_configuration, binding_key):
    """One slot, named. There is no refund-everything, at any batch size."""
    reader = FakeReader(count=3)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=3)

    refunded_orders = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok_response(1), reader=reader, settles=refunded_orders)
    report = await _apply(session_maker, reader, stage=STAGE_REFUND, request=request, slot=2, mutator=mutator)

    # The STAGE applied; the SLOT was refunded. Two different statements, and
    # the report keeps them apart exactly as it does for delivery.
    assert report.outcome == "applied"
    assert [entry.outcome for entry in report.slots] == ["refunded"]
    assert report.external_calls == {"create": 0, "pay": 0, "refund": 1, "meta": 0}
    assert len(mutator.refund_calls) == 1
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(2).status == "refunded"
    # The other two are untouched and still deliverable.
    assert snapshot.item(1).status == VOUCHER_PRODUCTION_ITEM_PAID
    assert snapshot.item(3).status == VOUCHER_PRODUCTION_ITEM_PAID


async def test_a_refund_after_a_send_is_refused(session_maker, production_configuration, binding_key):
    """Taking the money back for a code somebody holds is worse than losing it."""
    reader = FakeReader(count=2)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=2)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    plan = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1)
    assert not plan.ready
    assert REFUND_FORBIDDEN_AFTER_SEND in plan.reasons

    mutator = FakeMutator(refund=_ok_response(0))
    report = await _apply(
        session_maker,
        reader,
        stage=STAGE_REFUND,
        request=request,
        slot=1,
        mutator=mutator,
        expect_ready=False,
    )
    assert report.outcome == "refused"
    assert mutator.calls == []


async def test_the_database_forbids_a_refund_claim_after_a_send(session_maker, production_configuration, binding_key):
    """A CHECK constraint says it again, independently of the plan and the claim."""
    reader = FakeReader(count=1)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=1)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE easyweek_voucher_production_batch_items "
                        "SET refund_claimed_at = now() WHERE batch_id = :b"
                    ),
                    {"b": batch_id},
                )


async def test_a_refund_stays_available_while_the_batch_is_halted(session_maker, production_configuration, binding_key):
    """A halt is exactly when an untouched paid slot most needs its money back."""
    reader = FakeReader(count=3)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=3)

    # Slot 1's send goes unknown, halting the batch and stranding 2 and 3.
    sender = FakeSender(outcomes=[unknown_outcome()])
    halted = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert halted.halted

    plan = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=3)
    assert plan.ready, plan.reasons

    refunded_orders = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok_response(2), reader=reader, settles=refunded_orders)
    report = await _apply(session_maker, reader, stage=STAGE_REFUND, request=request, slot=3, mutator=mutator)
    assert [entry.outcome for entry in report.slots] == ["refunded"]
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(3).status == "refunded"
    # And the batch is still halted: the slot that went unknown is untouched.
    assert snapshot.halted


async def test_a_refund_for_a_slot_of_another_batch_refuses(session_maker, production_configuration, binding_key):
    """Slot 1 exists in both mailings. Naming the batch is what disambiguates."""
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 12)))
    run_a, batch_a, request_a = await _paid_batch(session_maker, reader, count=2, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=2, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    frozen_b = await _freeze(session_maker, reader, production_request(run_id=run_b), count=2)
    batch_b = frozen_b.batch["batch_id"]

    # Batch B's slot 1 is only `planned`, so it is not refundable — and asking
    # for it through batch A's request would be asking about a different row.
    plan = await _plan(
        session_maker,
        reader,
        stage=STAGE_REFUND,
        request=production_request(run_id=run_b, batch_id=batch_b),
        slot=1,
    )
    assert not plan.ready
    assert LEDGER_STATE_UNEXPECTED in plan.reasons


# ===========================================================================
# Webhooks: the right slot of the right batch, monotonically
# ===========================================================================


async def test_a_webhook_finds_exactly_the_slot_that_owns_the_message_id(
    session_maker, production_configuration, binding_key
):
    """Unique table-wide, so it resolves across every mailing ever run."""
    reader = FakeReader(indices=list(range(0, 2)) + list(range(10, 12)))
    run_a, batch_a, request_a = await _paid_batch(session_maker, reader, count=2, offset=0)
    sender_a = FakeSender(first_index=0)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request_a, sender=sender_a)

    run_b, _ = await seed_production_preview(
        session_maker, count=2, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    _run, batch_b, request_b = await _paid_batch(session_maker, reader, count=2, offset=10, run_id=run_b)
    sender_b = FakeSender(first_index=10)
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request_b, sender=sender_b)

    # Batch B slot 2's own identifier.
    outcome = await ledger_module.record_webhook_transition(
        session_maker,
        provider_message_id=PROVIDER_MESSAGE_IDS[11],
        status=VOUCHER_PRODUCTION_ITEM_DELIVERED,
    )
    assert outcome.applied

    snapshot_b = await ledger_module.load(session_maker, batch_id=batch_b)
    snapshot_a = await ledger_module.load(session_maker, batch_id=batch_a)
    assert snapshot_b.item(2).status == VOUCHER_PRODUCTION_ITEM_DELIVERED
    assert snapshot_b.item(1).status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED
    # Nothing in the other mailing moved.
    assert all(entry.status == VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED for entry in snapshot_a.items)


async def test_webhook_statuses_are_monotonic_and_idempotent(session_maker, production_configuration, binding_key):
    """read implies delivered; a duplicate changes nothing; delivered cannot undo read."""
    reader = FakeReader(count=1)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=1)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    # read arrives first — and stamps delivered too, because the schema
    # requires it and refusing would silently lose a status.
    assert (
        await ledger_module.record_webhook_transition(
            session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_PRODUCTION_ITEM_READ
        )
    ).applied
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    item = snapshot.item(1)
    assert item.status == VOUCHER_PRODUCTION_ITEM_READ
    assert item.delivered_at is not None
    assert item.provider_accepted_at is not None
    first_read_at = item.read_at

    # A late `delivered` cannot walk it back.
    regressed = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_PRODUCTION_ITEM_DELIVERED
    )
    assert not regressed.applied
    assert regressed.reason == ledger_module.RECORD_WOULD_REGRESS

    # A duplicate `read` is idempotent.
    duplicate = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_PRODUCTION_ITEM_READ
    )
    assert duplicate.applied
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).read_at == first_read_at


async def test_a_webhook_for_an_unknown_message_id_touches_nothing(
    session_maker, production_configuration, binding_key
):
    """Not ours. No lock taken, nothing written, and the caller falls through."""
    reader = FakeReader(count=1)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=1)

    outcome = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id="wamid.SOMEBODY.ELSES", status=VOUCHER_PRODUCTION_ITEM_DELIVERED
    )

    assert not outcome.applied
    assert outcome.reason == ledger_module.RECORD_MISSING_ROW


async def test_the_status_worker_routes_production_callbacks(session_maker, production_configuration, binding_key):
    """The one place a production slot's delivered/read can ever be observed."""
    from altegio_bot.workers.whatsapp_inbox_worker import _apply_voucher_production_status

    reader = FakeReader(count=1)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=1)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    async with session_maker() as session:
        async with session.begin():
            mine = await _apply_voucher_production_status(session, PROVIDER_MESSAGE_IDS[0], "delivered")
            theirs = await _apply_voucher_production_status(session, "wamid.NOTOURS", "read")
    assert mine is True
    assert theirs is False

    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).status == VOUCHER_PRODUCTION_ITEM_DELIVERED


async def test_the_webhook_still_lands_with_the_fence_closed(
    session_maker, production_configuration, binding_key, monkeypatch
):
    """A status about a message already sent is not a new effect to fence off."""
    reader = FakeReader(count=1)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=1)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    outcome = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_PRODUCTION_ITEM_READ
    )

    assert outcome.applied
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).status == VOUCHER_PRODUCTION_ITEM_READ


async def test_completed_is_not_delivered_and_not_read(session_maker, production_configuration, binding_key):
    """The four facts are reported separately, and only one of them means read."""
    reader = FakeReader(count=3)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=3)
    sender = FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert delivered.outcome == "applied"

    # Every stage finished. No webhook has said anything yet.
    payload = (await runner_module.run_status(session_maker, batch_id=batch_id)).as_safe_dict()
    assert payload["batch"]["status"] == VOUCHER_PRODUCTION_COMPLETED
    assert payload["batch"]["execution_completed"] is True
    assert payload["batch"]["provider_accepted_count"] == 3
    assert payload["batch"]["webhook_delivered_count"] == 0
    assert payload["batch"]["webhook_read_count"] == 0

    # One delivered, one read, one still only accepted — exactly the shape the
    # §41 production run ended in, and the report must not blur it.
    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_PRODUCTION_ITEM_READ
    )
    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[1], status=VOUCHER_PRODUCTION_ITEM_DELIVERED
    )
    payload = (await runner_module.run_status(session_maker, batch_id=batch_id)).as_safe_dict()
    assert payload["batch"]["status"] == VOUCHER_PRODUCTION_COMPLETED
    assert payload["batch"]["provider_accepted_count"] == 3
    assert payload["batch"]["webhook_delivered_count"] == 2
    assert payload["batch"]["webhook_read_count"] == 1
    slots = {entry["slot"]: entry for entry in payload["batch"]["items"]}
    assert slots[3]["provider_accepted"] is True
    assert slots[3]["webhook_delivered"] is False
    assert slots[3]["webhook_read"] is False


# ===========================================================================
# HMAC isolation: a MAC is only meaningful in the exact place it was made
# ===========================================================================


async def test_the_binding_material_names_the_batch_and_the_slot():
    """Neither half is enough alone, which is why both are in the material."""
    assert binding_material(batch_id=7, slot=1) != binding_material(batch_id=8, slot=1)
    assert binding_material(batch_id=7, slot=1) != binding_material(batch_id=7, slot=2)
    # And it is this phase's scope, so it cannot collide with §41's.
    assert binding_material(batch_id=7, slot=1).startswith("easyweek_voucher_production_mailing_v1:")


async def test_a_mac_from_another_slot_does_not_verify(session_maker, production_configuration, binding_key):
    """Slot 1's code cannot be presented as slot 2's, even inside one batch."""
    reader = FakeReader(count=2)
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    await _full_create(session_maker, reader, request, count=2, batch_id=batch_id)

    # Slot 1's own code against slot 1: proven.
    assert await ledger_module.binding_matches(
        session_maker,
        batch_id=batch_id,
        slot=1,
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        target_order_uuid=ORDER_UUIDS[0],
    )
    # Slot 1's code offered for slot 2, and slot 2's for slot 1: neither.
    assert not await ledger_module.binding_matches(
        session_maker,
        batch_id=batch_id,
        slot=2,
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        target_order_uuid=ORDER_UUIDS[1],
    )
    assert not await ledger_module.binding_matches(
        session_maker,
        batch_id=batch_id,
        slot=1,
        voucher_code=VOUCHER_CODE_SENTINELS[1],
        target_order_uuid=ORDER_UUIDS[0],
    )


async def test_a_mac_from_another_batch_does_not_verify(session_maker, production_configuration, binding_key):
    """Slot 1 exists in every mailing. A MAC from one must not verify another's."""
    reader = FakeReader(indices=[0, 10])
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=1, offset=0)
    run_b, _ = await seed_production_preview(
        session_maker, count=1, offset=10, period_start=SEPTEMBER_START, period_end=SEPTEMBER_END
    )
    frozen_a = await _freeze(session_maker, reader, production_request(run_id=run_a), count=1)
    frozen_b = await _freeze(session_maker, reader, production_request(run_id=run_b), count=1)
    batch_a, batch_b = frozen_a.batch["batch_id"], frozen_b.batch["batch_id"]
    await _full_create(
        session_maker, reader, production_request(run_id=run_a, batch_id=batch_a), count=1, batch_id=batch_a, offset=0
    )
    await _full_create(
        session_maker,
        reader,
        production_request(run_id=run_b, batch_id=batch_b),
        count=1,
        batch_id=batch_b,
        offset=10,
    )

    # Each batch's slot 1 verifies its own code...
    assert await ledger_module.binding_matches(
        session_maker,
        batch_id=batch_a,
        slot=1,
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        target_order_uuid=ORDER_UUIDS[0],
    )
    assert await ledger_module.binding_matches(
        session_maker,
        batch_id=batch_b,
        slot=1,
        voucher_code=VOUCHER_CODE_SENTINELS[10],
        target_order_uuid=ORDER_UUIDS[10],
    )
    # ...and not the other's, even though both are "slot 1".
    assert not await ledger_module.binding_matches(
        session_maker,
        batch_id=batch_a,
        slot=1,
        voucher_code=VOUCHER_CODE_SENTINELS[10],
        target_order_uuid=ORDER_UUIDS[0],
    )


async def test_a_pr18_mac_does_not_verify_here(session_maker, production_configuration, binding_key):
    """Shared secret, different domain label. §41's MAC is not §42's."""
    reader = FakeReader(count=1)
    run_id, _ = await seed_production_preview(session_maker, count=1)
    await seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=1)
    batch_id = frozen.batch["batch_id"]
    await _full_create(
        session_maker, reader, production_request(run_id=run_id, batch_id=batch_id), count=1, batch_id=batch_id
    )

    # The same code, the same order, the same key — under §41's domain.
    _foreign_key, foreign_mac = voucher_code_mac(
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        ledger_uuid="easyweek_voucher_snapshot_batch_v1:1",
        target_order_uuid=ORDER_UUIDS[0],
        voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        domain=VOUCHER_BATCH_DOMAIN,
    )
    _own_key, own_mac = voucher_code_mac(
        voucher_code=VOUCHER_CODE_SENTINELS[0],
        ledger_uuid=await ledger_module.load_binding_material(session_maker, batch_id=batch_id, slot=1),
        target_order_uuid=ORDER_UUIDS[0],
        voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        domain=ledger_module.VOUCHER_PRODUCTION_DOMAIN,
    )
    assert foreign_mac != own_mac
    assert ledger_module.VOUCHER_PRODUCTION_DOMAIN != VOUCHER_BATCH_DOMAIN

    # And the row really stores the §42 one.
    async with session_maker() as session:
        stored = await session.scalar(
            select(EasyWeekVoucherProductionBatchItem.voucher_code_hmac).where(
                EasyWeekVoucherProductionBatchItem.batch_id == batch_id
            )
        )
    assert stored == own_mac
    assert stored != foreign_mac


async def test_a_changed_code_between_create_and_pay_refuses(session_maker, production_configuration, binding_key):
    """Paying for an order whose voucher changed underneath us buys something else."""
    reader = FakeReader(count=2)
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    await _full_create(session_maker, reader, request, count=2, batch_id=batch_id)

    # Somebody reissued slot 1's voucher between the CREATE and the PAY.
    swapped = dict(reader.orders[ORDER_UUIDS[0]])
    swapped["vouchers"] = [issued_voucher(0, code="SOMETHING-ELSE-ENTIRELY")]
    reader.orders[ORDER_UUIDS[0]] = swapped

    mutator = FakeMutator(pay_sequence=[_ok_response(0), _ok_response(1)])
    report = await _apply(session_maker, reader, stage=STAGE_PAY, request=request, mutator=mutator)

    assert report.outcome == "refused"
    # Nothing was paid: the binding is checked BEFORE the claim.
    assert mutator.calls == []


# ===========================================================================
# Nothing leaks: no code, no phone, no name, no customer UUID
# ===========================================================================


async def test_no_report_or_log_carries_a_secret(session_maker, production_configuration, binding_key, caplog):
    """Whole-payload search, over every report of a complete mailing."""
    caplog.set_level(logging.DEBUG)
    count = 3
    reader = FakeReader(count=count)
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)

    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    created, _ = await _full_create(session_maker, reader, request, count=count, batch_id=batch_id)
    paid, _ = await _full_pay(session_maker, reader, request, count=count, batch_id=batch_id)
    sender = FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    status = await runner_module.run_status(session_maker, batch_id=batch_id)
    reconciled = await runner_module.run_reconcile(session_maker, request=request, order_reader=reader)
    refund_plan = await _plan(session_maker, reader, stage=STAGE_REFUND, request=request, slot=1)

    blob = json.dumps(
        [
            frozen.as_safe_dict(),
            created.as_safe_dict(),
            paid.as_safe_dict(),
            delivered.as_safe_dict(),
            status.as_safe_dict(),
            reconciled.as_safe_dict(),
            refund_plan.as_safe_dict(),
        ],
        default=str,
    )
    logs = caplog.text

    secrets = [
        *VOUCHER_CODE_SENTINELS[:count],
        *PHONES[:count],
        *CUSTOMER_NAMES[:count],
        *CUSTOMER_UUIDS[:count],
        *ORDER_UUIDS[:count],
        STAFFER_UUID,
        ACCOUNT_UUID,
        BOOKING_LINK,
        "k" * 48,
    ]
    for secret in secrets:
        assert secret not in blob, secret
        assert secret not in logs, secret

    # And the database stores a keyed MAC rather than anything reversible.
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(
                        EasyWeekVoucherProductionBatchItem.voucher_code_hmac,
                        EasyWeekVoucherProductionBatchItem.hmac_key_id,
                    )
                )
            ).all()
        )
    assert len(rows) == count
    for mac, key_id in rows:
        assert mac is not None and key_id is not None
        assert all(code not in mac for code in VOUCHER_CODE_SENTINELS)


async def test_the_attempt_row_stores_no_message(session_maker, production_configuration, binding_key):
    """Enough to audit which template was used, and nothing about what it said."""
    reader = FakeReader(count=2)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=2)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    async with session_maker() as session:
        attempts = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchAttempt).order_by(
                        EasyWeekVoucherProductionBatchAttempt.slot.asc()
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(attempts) == 2
    for attempt in attempts:
        assert attempt.batch_id == batch_id
        assert attempt.outcome == "provider_accepted"
        assert attempt.template_code == "new_client_voucher"
        assert attempt.meta_template_name == "kitilash_ka_new_client_voucher_v1"
        assert attempt.template_language == "de"

    # No column exists that could hold a rendered body or a parameter, so
    # nothing downstream could re-render the message even if it tried.
    columns = set(EasyWeekVoucherProductionBatchAttempt.__table__.columns.keys())
    assert not columns & {"body", "params", "parameters", "phone_e164", "to_e164", "voucher_code"}


async def test_no_outbox_row_or_message_job_is_created(session_maker, production_configuration, binding_key):
    """No generic runner, no Outbox, no worker that could ever re-send this."""
    reader = FakeReader(count=2)
    _run_id, _batch_id, request = await _paid_batch(session_maker, reader, count=2)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)

    async with session_maker() as session:
        outbox = await session.scalar(select(func.count()).select_from(OutboxMessage))
        jobs = await session.scalar(select(func.count()).select_from(MessageJob))
    assert outbox == 0
    assert jobs == 0


# ===========================================================================
# Concurrency, on real PostgreSQL
# ===========================================================================


async def _composition_identity(session_maker, reader, *, run_id, count):
    """The identity a freeze would write, built the way the runner builds it."""
    request = production_request(run_id=run_id)
    async with session_maker() as session:
        composition = await prove_production_composition(
            session,
            preview_run_id=run_id,
            client_reader=reader,
            now=utcnow(),
            approval=approval_for(count),
        )
    return runner_module._identity_from_composition(request, composition)


async def test_a_removal_racing_the_freeze_cannot_be_lost(session_maker, production_configuration, binding_key):
    """A stale approval does not authorise a batch that no longer matches."""
    run_id, recipient_ids = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    request = production_request(run_id=run_id)

    # A plan built when all three were active...
    plan = await _plan(session_maker, reader, stage=STAGE_FREEZE, request=request, approval=approval_for(3))
    assert plan.ready, plan.reasons

    # ...then the operator removes one in the editor.
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
            approval=approval_for(3),
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    assert report.outcome == "refused"
    assert await _batch_id(session_maker, run_id=run_id) is None


async def test_the_freeze_transaction_itself_refuses_a_removed_recipient(
    session_maker, production_configuration, binding_key
):
    """Not only the plan: the insert re-reads every row under the run's lock.

    This is the race the plan alone cannot close — an approval taken while the
    world was right, applied a moment after a Remove won.
    """
    run_id, recipient_ids = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    identity = await _composition_identity(session_maker, reader, run_id=run_id, count=3)
    assert identity is not None

    async with session_maker() as session:
        async with session.begin():
            recipient = await session.get(CampaignRecipient, recipient_ids[2])
            recipient.status = "skipped"

    outcome = await ledger_module.freeze_batch(
        session_maker,
        identity=identity,
        approved_recipient_count=3,
        approved_exposure_minor=3 * UNIT_PRICE_MINOR,
        freeze_plan_digest="d" * 64,
    )

    assert not outcome.applied
    assert outcome.reason == ledger_module.FREEZE_REFUSED_SNAPSHOT
    async with session_maker() as session:
        headers = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatch))
    assert headers == 0


async def test_the_freeze_transaction_refuses_a_stale_approved_count(
    session_maker, production_configuration, binding_key
):
    """Re-checked inside the transaction, against the rows it can actually see."""
    run_id, _ = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    identity = await _composition_identity(session_maker, reader, run_id=run_id, count=3)
    assert identity is not None

    outcome = await ledger_module.freeze_batch(
        session_maker,
        identity=identity,
        # Three people, approved as two.
        approved_recipient_count=2,
        approved_exposure_minor=2 * UNIT_PRICE_MINOR,
        freeze_plan_digest="d" * 64,
    )

    assert not outcome.applied
    assert outcome.reason == ledger_module.FREEZE_REFUSED_APPROVAL


async def test_two_concurrent_freezes_of_one_preview_produce_one_batch(
    session_maker, production_configuration, binding_key
):
    """Real row locks, real unique constraint. Exactly one batch exists after."""
    run_id, _ = await seed_production_preview(session_maker, count=3)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=3)
    identity = await _composition_identity(session_maker, reader, run_id=run_id, count=3)
    assert identity is not None

    async def freeze_once():
        return await ledger_module.freeze_batch(
            session_maker,
            identity=identity,
            approved_recipient_count=3,
            approved_exposure_minor=3 * UNIT_PRICE_MINOR,
            freeze_plan_digest="d" * 64,
        )

    results = await asyncio.gather(freeze_once(), freeze_once(), return_exceptions=True)

    # One applied; the other is idempotent for the identical composition rather
    # than a second mailing.
    applied = [r for r in results if not isinstance(r, Exception) and r.applied]
    assert len(applied) == 1, results
    async with session_maker() as session:
        headers = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatch))
        items = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatchItem))
    assert headers == 1
    assert items == 3


async def test_two_concurrent_freezes_for_one_person_leave_one_entitlement(
    session_maker, production_configuration, binding_key
):
    """The entitlement index decides the race, not whoever checked first."""
    await seed_template_and_sender(session_maker)
    run_a, _ = await seed_production_preview(session_maker, count=1, offset=0)
    run_b, _ = await seed_production_preview(session_maker, count=1, offset=0)
    reader = FakeReader(count=1)

    identity_a = await _composition_identity(session_maker, reader, run_id=run_a, count=1)
    identity_b = await _composition_identity(session_maker, reader, run_id=run_b, count=1)
    assert identity_a is not None and identity_b is not None

    async def freeze(identity):
        return await ledger_module.freeze_batch(
            session_maker,
            identity=identity,
            approved_recipient_count=1,
            approved_exposure_minor=UNIT_PRICE_MINOR,
            freeze_plan_digest="d" * 64,
        )

    results = await asyncio.gather(freeze(identity_a), freeze(identity_b), return_exceptions=True)

    succeeded = [r for r in results if not isinstance(r, Exception) and r.applied]
    assert len(succeeded) == 1, results
    async with session_maker() as session:
        items = await session.scalar(select(func.count()).select_from(EasyWeekVoucherProductionBatchItem))
    assert items == 1


async def test_two_concurrent_create_claims_on_one_slot_grant_once(
    session_maker, production_configuration, binding_key
):
    """The claim is the gate, and it is a row lock rather than a check."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    identity = runner_module._identity_from_snapshot(snapshot)
    assert identity is not None
    now = utcnow()

    async def claim():
        return await ledger_module.claim_create(
            session_maker,
            identity=identity,
            batch_id=batch_id,
            slot=1,
            plan_digest="c" * 64,
            create_window_start=now - timedelta(minutes=30),
            create_window_end=now + timedelta(minutes=30),
        )

    results = await asyncio.gather(claim(), claim(), return_exceptions=True)
    granted = [r for r in results if not isinstance(r, Exception) and r.granted]

    assert len(granted) == 1, results


async def test_a_frozen_preview_is_locked_against_the_editor(session_maker, production_configuration, binding_key):
    """After the freeze the preview is locked, and the guard says which phase."""
    from altegio_bot.campaigns.preview_freeze import preview_is_locked_by_any_canary

    run_id, _ = await seed_production_preview(session_maker, count=2)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=2)

    async with session_maker() as session:
        assert not await preview_is_locked_by_any_canary(session, campaign_run_id=run_id)

    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=2)
    batch_id = frozen.batch["batch_id"]

    async with session_maker() as session:
        assert await preview_is_locked_by_any_canary(session, campaign_run_id=run_id)
        assert await ledger_module.preview_is_locked_by_voucher_production(session, campaign_run_id=run_id)
        assert await ledger_module.production_batch_id_for_preview(session, campaign_run_id=run_id) == batch_id


# ===========================================================================
# What this phase does NOT open
# ===========================================================================


async def test_generic_easyweek_campaign_execution_stays_closed(session_maker, production_configuration, binding_key):
    """A mailing of forty proven sends is not a campaign permission.

    Asserted with a complete, delivered mailing already in the database, which
    is the only state in which anybody could be tempted to read these guards as
    having moved.
    """
    reader = FakeReader(count=2)
    _run_id, _batch_id, request = await _paid_batch(session_maker, reader, count=2)
    sender = FakeSender()
    assert (
        await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    ).outcome == "applied"

    with pytest.raises(CampaignProviderRefusal) as refusal:
        require_campaign_execution_provider(PROVIDER_EASYWEEK)
    assert refusal.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED

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


async def test_every_report_repeats_that_nothing_is_authorised(session_maker, production_configuration, binding_key):
    """On a green stage too, which is the only time it could be misread."""
    reader = FakeReader(count=2)
    _run_id, _batch_id, request = await _paid_batch(session_maker, reader, count=2)
    sender = FakeSender()
    delivered = await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    assert delivered.outcome == "applied"

    payload = delivered.as_safe_dict()
    assert payload["campaign_send_authorized"] is False
    assert payload["bulk_delivery_authorized"] is False
    assert payload["global_ready_for_send"] is False
    assert payload["ready_for_send"] is False
    assert payload["recipient_basis"] == "operator_manual_selection"
    assert payload["first_visit_proof"] == "not_applicable"
    assert payload["voucher_unit_price_minor"] == UNIT_PRICE_MINOR


async def test_the_pr18_singleton_constraints_are_untouched(session_maker):
    """§41 is still a singleton, and still capped at five. Nothing was widened."""
    run_id, _ = await seed_production_preview(session_maker, count=2)
    now = utcnow()

    def batch_header(scope: str) -> EasyWeekVoucherSnapshotBatch:
        return EasyWeekVoucherSnapshotBatch(
            batch_scope=scope,
            request_schema_version="1",
            baseline_version="2026-09-27-43",
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            campaign_code="new_clients_monthly",
            recipient_basis="operator_manual_selection",
            campaign_run_id=run_id,
            campaign_period_start=PERIOD_START,
            campaign_period_end=PERIOD_END,
            location_uuid=uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
            staffer_uuid=uuid_module.UUID(STAFFER_UUID),
            payment_account_uuid=uuid_module.UUID(ACCOUNT_UUID),
            voucher_template_uuid=uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
            frozen_digest="a" * 64,
            recipient_count=1,
            voucher_unit_price_minor=UNIT_PRICE_MINOR,
            total_exposure_minor=UNIT_PRICE_MINOR,
            status="frozen",
            frozen_at=now,
            evidence={},
            created_at=now,
            updated_at=now,
        )

    # One §41 batch is fine...
    async with session_maker() as session:
        async with session.begin():
            session.add(batch_header("easyweek_voucher_snapshot_batch_v1"))
    # ...a second is impossible, and so is a different scope literal.
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(batch_header("easyweek_voucher_snapshot_batch_v1"))
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(batch_header("easyweek_voucher_snapshot_batch_v2"))

    # And §41 still caps itself at five recipients, which this phase does not
    # borrow and has not relaxed.
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(text("UPDATE easyweek_voucher_snapshot_batches SET recipient_count = 6"))


async def test_the_production_scope_is_not_pinned_to_one_row(session_maker):
    """The one §41 constraint this phase deliberately does NOT have."""
    constraints = {constraint.name for constraint in EasyWeekVoucherProductionBatch.__table__.constraints}
    # There is a scope CHECK — the phase label is still pinned...
    assert "ck_ew_voucher_production_batch_scope" in constraints
    # ...but the uniqueness is on the PREVIEW, not on the scope, which is what
    # makes more than one mailing representable at all.
    assert "uq_ew_voucher_production_batch_run" in constraints
    assert "uq_ew_voucher_production_batch_scope" not in constraints


async def test_the_altegio_path_is_untouched(session_maker):
    """This phase's tables reference only the shared campaign tables."""
    production_tables = {
        "easyweek_voucher_production_batches",
        "easyweek_voucher_production_batch_items",
        "easyweek_voucher_production_batch_attempts",
    }
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    text(
                        "SELECT tc.table_name, ccu.table_name AS target "
                        "FROM information_schema.table_constraints tc "
                        "JOIN information_schema.constraint_column_usage ccu "
                        "  ON tc.constraint_name = ccu.constraint_name "
                        "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = ANY(:names)"
                    ),
                    {"names": list(production_tables)},
                )
            ).all()
        )
    targets = {row[1] for row in rows}
    assert targets <= {"campaign_runs", "campaign_recipients"} | production_tables


# ===========================================================================
# The Ops page: read-only, and it never shows a person
# ===========================================================================


async def test_the_ops_page_lists_the_mailings_and_shows_one_in_full(
    session_maker, production_configuration, binding_key
):
    """A finding aid and a state view. No buttons, and nobody's name."""
    from altegio_bot.ops import router as ops_router

    count = 3
    reader = FakeReader(count=count)
    _run_id, batch_id, request = await _paid_batch(session_maker, reader, count=count)
    sender = FakeSender()
    await _apply(session_maker, reader, stage=STAGE_DELIVER, request=request, sender=sender)
    await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_IDS[0], status=VOUCHER_PRODUCTION_ITEM_READ
    )

    listing = await ops_router.ops_voucher_production_mailing_page()
    detail = await ops_router.ops_voucher_production_mailing_page(batch_id=batch_id)

    for html in (listing, detail):
        assert "Production voucher mailing" in html
        assert "VOUCHER_PRODUCTION_MAILING_RUNBOOK" in html
        assert "campaign_send_authorized=false" in html
        # Read-only on purpose. The shared layout owns exactly one form —
        # Logout — and this page adds none. A button here would be precisely
        # the one-click "pay and send" the phase exists to avoid.
        assert html.count("<form") == 1
        assert 'action="/ops/logout"' in html
        # And it says out loud that there is no ceiling, so a reader cannot
        # assume §41's five still applies.
        assert "Потолок получателей" in html

    # The listing carries the id an operator has to type next.
    assert f"?batch_id={batch_id}" in listing

    # The detail view keeps the four delivery facts apart.
    assert "Meta приняла (provider_accepted)" in detail
    assert "Webhook подтвердил delivered" in detail
    assert "Webhook подтвердил read" in detail
    assert "Исполнение стадий завершено" in detail

    for secret in [
        *VOUCHER_CODE_SENTINELS[:count],
        *PHONES[:count],
        *CUSTOMER_NAMES[:count],
        *CUSTOMER_UUIDS[:count],
        *ORDER_UUIDS[:count],
        *PROVIDER_MESSAGE_IDS[:count],
        STAFFER_UUID,
        ACCOUNT_UUID,
    ]:
        assert secret not in detail, secret
        assert secret not in listing, secret


async def test_the_ops_page_survives_an_empty_phase_and_an_unknown_id(
    session_maker, production_configuration, binding_key
):
    """Both edges render rather than raising: a page that 500s says less."""
    from altegio_bot.ops import router as ops_router

    empty = await ops_router.ops_voucher_production_mailing_page()
    assert "Batches нет" in empty

    missing = await ops_router.ops_voucher_production_mailing_page(batch_id=4242)
    assert "не найден" in missing


# ===========================================================================
# The approved plan bounds what a stage may act on
# ===========================================================================


class _CreateRacingReader(FakeReader):
    """A reader that lets a real CREATE land at one exact moment.

    The moment is the template read inside :func:`build_stage_plan`, which
    happens AFTER that function has loaded the ledger snapshot it will build its
    ``target_slots`` from and BEFORE the acting loop claims anything. That is the
    window the fixed code has to survive: the approved plan says one thing and
    the ledger has since grown another eligible slot.

    Armed explicitly, so the plan an operator reads is built against a quiet
    world and only the live rebuild inside the apply sees the race. Fires once —
    the CREATE it triggers builds a plan of its own and would otherwise recurse.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._armed = False
        self._fired = False
        self._on_template = None

    def arm(self, on_template) -> None:
        self._armed = True
        self._on_template = on_template

    async def get_voucher_template(self, template_uuid: str):
        answer = await super().get_voucher_template(template_uuid)
        if self._armed and not self._fired:
            self._fired = True
            assert self._on_template is not None
            await self._on_template()
        return answer


async def test_the_plan_hands_back_the_snapshot_it_was_proven_against():
    """The contract the fix rests on, pinned so a refactor cannot quietly undo it.

    ``build_stage_plan`` returns five values, the last being the ledger snapshot
    it built its ``target_slots`` from. Callers act on THAT snapshot rather than
    loading their own — the second load is what let a PAY reach an unapproved
    slot — so the arity and the pairing are worth asserting directly. The
    operator CLI unpacks the same five values.
    """
    import inspect

    signature = inspect.signature(runner_module.build_stage_plan)
    annotation = str(signature.return_annotation)
    assert "BatchSnapshot" in annotation, annotation
    # And the only place that rebuilds a plan for an apply must not read the
    # ledger again: the whole blocker was a second read widening the targets.
    source = inspect.getsource(runner_module._authorise)
    assert "ledger_module.load" not in source, "_authorise must not re-read the ledger"


async def test_a_create_landing_after_the_pay_plan_is_not_paid(session_maker, production_configuration, binding_key):
    """A slot that became payable after the approval is not paid by it.

    The confirmed P1: the plan was built and digest-checked against one read of
    the ledger, then a second read decided what to act on. A CREATE completing
    in between made its slot payable, and the PAY charged a slot the approved
    ``target_slots`` never contained.

    Real freeze, real claims, real outcomes, real HMAC and real constraints
    throughout; only the two transports are fakes, and the pay sequence
    deliberately has TWO answers ready so that the old behaviour would succeed
    at paying twice rather than erroring on a short fixture.
    """
    count = 2
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = _CreateRacingReader(count=count)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    # Slot 1 is created; slot 2's CREATE is refused by the endpoint's own
    # validation. A proven pre-action refusal, so the batch is NOT halted and
    # slot 2 stays re-claimable from a fresh plan — which is what makes the race
    # reachable at all.
    first = FakeMutator(create_sequence=[_ok_response(0), EasyWeekPermanentError("422", status_code=422)])
    created = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=first)
    assert created.outcome == "partial"
    assert not created.halted
    before = await ledger_module.load(session_maker, batch_id=batch_id)
    assert before.item(1).status == VOUCHER_PRODUCTION_ITEM_CREATED
    assert before.item(2).status == "create_rejected"

    # The PAY plan an operator reads and the owner approves: slot 1 only.
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)
    assert plan.ready, plan.reasons
    assert plan.authorised_slots == (1,)
    assert plan.snapshot["target_slots"] == [1]

    # Now arm the race: during the live rebuild inside the apply, slot 2's
    # CREATE completes for real.
    async def land_a_create_for_slot_two() -> None:
        mutator = FakeMutator(create_sequence=[_ok_response(1)])
        report = await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=mutator)
        assert report.outcome == "applied", report.reasons
        assert len(mutator.create_calls) == 1

    reader.arm(land_a_create_for_slot_two)

    paid_orders = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    mutator = FakeMutator(
        # TWO answers ready on purpose: the pre-fix code would have used both.
        pay_sequence=[_ok_response(0), _ok_response(1)],
        reader=reader,
        settles=paid_orders,
    )
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

    # The race really did happen: slot 2 is created, by a CREATE of its own.
    after = await ledger_module.load(session_maker, batch_id=batch_id)
    assert after.item(2).target_order_uuid is not None

    # And exactly one payment left the process, for the one approved slot.
    assert len(mutator.pay_calls) == 1, mutator.pay_calls
    assert mutator.pay_calls[0]["order_uuid"] == ORDER_UUIDS[0]
    assert report.external_calls["pay"] == 1
    assert [entry.slot for entry in report.slots] == [1]

    assert after.item(1).status == VOUCHER_PRODUCTION_ITEM_PAID
    # Slot 2 was created and NOT paid. It needs a new plan and a new approval.
    assert after.item(2).status == VOUCHER_PRODUCTION_ITEM_CREATED
    assert after.item(2).pay_verified_at is None


async def test_the_slot_the_race_created_is_payable_under_a_fresh_plan(
    session_maker, production_configuration, binding_key
):
    """Deferred, not lost. The next plan sees it and the owner approves it.

    The fix must not strand the slot — that would be a different bug. What it
    must do is make the owner see it before any money moves.
    """
    count = 2
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = _CreateRacingReader(count=count)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    first = FakeMutator(create_sequence=[_ok_response(0), EasyWeekPermanentError("422", status_code=422)])
    await _apply(session_maker, reader, stage=STAGE_CREATE, request=request, mutator=first)
    plan = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)

    async def land_a_create_for_slot_two() -> None:
        await _apply(
            session_maker,
            reader,
            stage=STAGE_CREATE,
            request=request,
            mutator=FakeMutator(create_sequence=[_ok_response(1)]),
        )

    reader.arm(land_a_create_for_slot_two)
    paid_orders = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    async with session_maker() as session:
        await runner_module.run_pay(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=FakeMutator(pay_sequence=[_ok_response(0)], reader=reader, settles=paid_orders),
            apply=True,
            supplied_digest=plan.digest,
            supplied_issued_at=plan.issued_at,
            supplied_phrase=plan.confirmation_phrase,
        )

    # A fresh plan now offers exactly the slot the race created.
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    reader.orders[ORDER_UUIDS[0]] = paid_orders[ORDER_UUIDS[0]]
    second = await _plan(session_maker, reader, stage=STAGE_PAY, request=request)
    assert second.ready, second.reasons
    assert second.authorised_slots == (2,)

    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    mutator = FakeMutator(pay_sequence=[_ok_response(1)], reader=reader, settles=settles)
    async with session_maker() as session:
        report = await runner_module.run_pay(
            session,
            session_maker,
            request=request,
            reader=reader,
            order_reader=reader,
            mutator=mutator,
            apply=True,
            supplied_digest=second.digest,
            supplied_issued_at=second.issued_at,
            supplied_phrase=second.confirmation_phrase,
        )

    assert report.outcome == "applied", report.reasons
    assert len(mutator.pay_calls) == 1
    assert mutator.pay_calls[0]["order_uuid"] == ORDER_UUIDS[1]
    final = await ledger_module.load(session_maker, batch_id=batch_id)
    assert final.item(1).status == VOUCHER_PRODUCTION_ITEM_PAID
    assert final.item(2).status == VOUCHER_PRODUCTION_ITEM_PAID


async def test_a_create_landing_after_the_create_plan_is_not_created_twice(
    session_maker, production_configuration, binding_key
):
    """The same bound on CREATE, which shares ``_authorise`` with PAY.

    The reviewer asked for the invariant to be checked on the other stages that
    go through the same helper, not only on the one where it was demonstrated.
    Here the race makes a slot LEAVE the eligible set rather than join it, and
    the intersection has to narrow without disturbing the rest of the stage.
    """
    count = 3
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = _CreateRacingReader(count=count)
    frozen = await _freeze(session_maker, reader, production_request(run_id=run_id), count=count)
    batch_id = frozen.batch["batch_id"]
    request = production_request(run_id=run_id, batch_id=batch_id)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    plan = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
    assert plan.authorised_slots == (1, 2, 3)

    # Slot 1 gets created by somebody else between the approval and the act.
    async def land_a_create_for_slot_one() -> None:
        single = await _plan(session_maker, reader, stage=STAGE_CREATE, request=request)
        async with session_maker() as session:
            await runner_module.run_create(
                session,
                session_maker,
                request=request,
                reader=reader,
                order_reader=reader,
                mutator=FakeMutator(create_sequence=[_ok_response(0), _ok_response(1), _ok_response(2)]),
                apply=True,
                supplied_digest=single.digest,
                supplied_issued_at=single.issued_at,
                supplied_phrase=single.confirmation_phrase,
            )

    reader.arm(land_a_create_for_slot_one)
    mutator = FakeMutator(create_sequence=[_ok_response(0), _ok_response(1), _ok_response(2)])
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

    # Every slot was created exactly once, by the inner run. The outer run's
    # claims all refuse, because the rows have moved on — no second CREATE.
    assert len(mutator.create_calls) == 0, mutator.create_calls
    assert report.external_calls["create"] == 0
    final = await ledger_module.load(session_maker, batch_id=batch_id)
    assert [entry.status for entry in final.items] == [VOUCHER_PRODUCTION_ITEM_CREATED] * count
    assert all(entry.target_order_uuid is not None for entry in final.items)
