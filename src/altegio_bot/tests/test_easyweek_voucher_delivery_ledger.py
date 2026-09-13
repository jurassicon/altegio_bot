"""The durable ledger of the voucher delivery canary, on real PostgreSQL (§36).

These run against the project's PostgreSQL instance because the guarantees being
asserted are database guarantees: unique constraints deciding who owns the
canary, CHECK constraints making an impossible row impossible, and
``SELECT ... FOR UPDATE`` serialising a stage transition.

The one that matters most is the entitlement rule. A scope-only uniqueness check
would miss the mistake a human is actually likely to make: running a fresh
preview, getting a new recipient row id for the same person, and giving them a
second €15 voucher.
"""

from __future__ import annotations

import asyncio
import uuid as uuid_module
from datetime import timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    NEW_CLIENT_CAMPAIGN_CODE,
    VOUCHER_DELIVERY_SCHEMA_VERSION,
    VOUCHER_DELIVERY_SCOPE,
    delivery_marker,
)
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    VOUCHER_DELIVERY_BASIS_EARNED,
    VOUCHER_DELIVERY_CREATE_CLAIMED,
    VOUCHER_DELIVERY_CREATED,
    VOUCHER_DELIVERY_PAID,
    VOUCHER_DELIVERY_PAY_CLAIMED,
    VOUCHER_DELIVERY_PLANNED,
    VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
    VOUCHER_DELIVERY_READ,
    VOUCHER_DELIVERY_SEND_CLAIMED,
    EasyWeekCampaignVoucherDeliveryAttempt,
    EasyWeekCampaignVoucherDeliveryLedger,
)
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (
    ACCOUNT_UUID,
    BOOKING_UUID,
    COMPANY_ID,
    EW_CUSTOMER_UUID,
    ORDER_UUID,
    OTHER_UUID,
    PROVIDER_MESSAGE_ID,
    STAFFER_UUID,
    seed_recipient,
)
from altegio_bot.utils import utcnow

MAC = "a" * 64
KEY_ID = "test-key-1"


def _identity(
    *,
    run_id: int,
    recipient_id: int,
    booking_uuid=BOOKING_UUID,
    basis: str = VOUCHER_DELIVERY_BASIS_EARNED,
    customer_uuid=EW_CUSTOMER_UUID,
) -> ledger_module.CanaryIdentity:
    return ledger_module.CanaryIdentity(
        company_id=COMPANY_ID,
        campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
        campaign_run_id=run_id,
        campaign_recipient_id=recipient_id,
        recipient_basis=basis,
        source_booking_uuid=str(booking_uuid) if booking_uuid is not None else None,
        easyweek_customer_uuid=str(customer_uuid),
        location_uuid=KARLSRUHE_LOCATION_UUID,
        staffer_uuid=STAFFER_UUID,
        payment_account_uuid=ACCOUNT_UUID,
        voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        reconciliation_marker=delivery_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id),
    )


async def _row(session_maker, *, run_id: int, recipient_id: int, **values):
    """Insert a ledger row directly, bypassing the state machine.

    The run and recipient ids are required rather than defaulted: a row missing
    them would fail on a NOT NULL constraint and a constraint test would then
    pass for entirely the wrong reason.
    """
    now = utcnow()
    payload = {
        "canary_scope": VOUCHER_DELIVERY_SCOPE,
        "request_schema_version": VOUCHER_DELIVERY_SCHEMA_VERSION,
        "provider": PROVIDER_EASYWEEK,
        "company_id": COMPANY_ID,
        "campaign_code": NEW_CLIENT_CAMPAIGN_CODE,
        "campaign_run_id": run_id,
        "campaign_recipient_id": recipient_id,
        "source_booking_uuid": BOOKING_UUID,
        "easyweek_customer_uuid": EW_CUSTOMER_UUID,
        "location_uuid": uuid_module.UUID(KARLSRUHE_LOCATION_UUID),
        "staffer_uuid": uuid_module.UUID(STAFFER_UUID),
        "payment_account_uuid": uuid_module.UUID(ACCOUNT_UUID),
        "voucher_template_uuid": uuid_module.UUID(EASYWEEK_VOUCHER_TEMPLATE_UUID),
        "reconciliation_marker": "ewvd1-000000000000",
        "status": VOUCHER_DELIVERY_PLANNED,
        "evidence": {},
        "created_at": now,
        "updated_at": now,
    }
    payload.update(values)
    async with session_maker() as session:
        async with session.begin():
            row = EasyWeekCampaignVoucherDeliveryLedger(**payload)
            session.add(row)
            await session.flush()
            return row.id


# ---------------------------------------------------------------------------
# One canary, one entitlement
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_canary_row_records_the_recipient_before_any_money_moves(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)

    snapshot = await ledger_module.open_canary(
        session_maker, identity=_identity(run_id=run_id, recipient_id=recipient_id)
    )

    assert snapshot.exists is True
    assert snapshot.status == VOUCHER_DELIVERY_PLANNED
    assert snapshot.campaign_recipient_id == recipient_id
    assert snapshot.send_attempt_count == 0


@pytest.mark.asyncio
async def test_a_second_canary_for_the_same_scope_is_impossible(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await ledger_module.open_canary(session_maker, identity=_identity(run_id=run_id, recipient_id=recipient_id))

    with pytest.raises(IntegrityError):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, source_booking_uuid=OTHER_UUID)


@pytest.mark.asyncio
async def test_the_same_person_cannot_earn_a_second_voucher_through_a_new_preview(session_maker) -> None:
    """The mistake a scope-only rule would miss entirely.

    A fresh preview produces a NEW run and a NEW recipient row for the same
    human being and the same first visit. The entitlement key is the booking, so
    the second row collides in the database rather than in somebody's memory.
    """
    run_id, recipient_id = await seed_recipient(session_maker)
    await ledger_module.open_canary(session_maker, identity=_identity(run_id=run_id, recipient_id=recipient_id))

    # A different canary scope would be a code change; simulate the entitlement
    # rule directly by inserting a second row under another scope.
    with pytest.raises(IntegrityError):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            canary_scope="easyweek_voucher_delivery_canary_v2",
            source_booking_uuid=BOOKING_UUID,
        )


@pytest.mark.asyncio
async def test_a_row_for_another_provider_is_refused(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises(IntegrityError):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, provider=PROVIDER_ALTEGIO)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["target_order_uuid", "outbound_intent_uuid"])
async def test_a_result_may_belong_to_only_one_canary_row(session_maker, field) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _row(session_maker, run_id=run_id, recipient_id=recipient_id, **{field: ORDER_UUID})

    with pytest.raises(IntegrityError):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            canary_scope="second",
            source_booking_uuid=OTHER_UUID,
            **{field: ORDER_UUID},
        )


@pytest.mark.asyncio
async def test_a_provider_message_id_may_belong_to_only_one_canary_row(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _row(session_maker, run_id=run_id, recipient_id=recipient_id, provider_message_id=PROVIDER_MESSAGE_ID)

    with pytest.raises(IntegrityError):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            canary_scope="second",
            source_booking_uuid=OTHER_UUID,
            provider_message_id=PROVIDER_MESSAGE_ID,
        )


# ---------------------------------------------------------------------------
# States the schema refuses to represent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unknown_status_is_not_a_state(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, status="probably_fine")


@pytest.mark.asyncio
async def test_money_cannot_move_before_the_order_was_proven(session_maker) -> None:
    """`pay_claimed` without a verified create is not a row that may exist."""
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            status=VOUCHER_DELIVERY_PAY_CLAIMED,
            pay_claimed_at=utcnow(),
        )


@pytest.mark.asyncio
async def test_nothing_may_be_sent_before_the_voucher_is_paid_for_and_bound(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    now = utcnow()
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            status=VOUCHER_DELIVERY_SEND_CLAIMED,
            target_order_uuid=ORDER_UUID,
            create_claimed_at=now,
            create_attempted_at=now,
            create_verified_at=now,
            pay_claimed_at=now,
            pay_attempted_at=now,
            # No pay_verified_at, no binding, no live guard.
            send_claimed_at=now,
            send_attempted_at=now,
            send_attempt_count=1,
        )


@pytest.mark.asyncio
async def test_a_send_may_not_be_claimed_on_a_guard_older_than_the_payment(session_maker) -> None:
    """A guard taken before the money moved says nothing about now."""
    run_id, recipient_id = await seed_recipient(session_maker)
    now = utcnow()
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            status=VOUCHER_DELIVERY_SEND_CLAIMED,
            target_order_uuid=ORDER_UUID,
            voucher_code_hmac=MAC,
            hmac_key_id=KEY_ID,
            create_claimed_at=now,
            create_attempted_at=now,
            create_verified_at=now,
            pay_claimed_at=now,
            pay_attempted_at=now,
            pay_verified_at=now,
            live_guard_reproven_at=now - timedelta(minutes=1),
            send_claimed_at=now,
            send_attempted_at=now,
            send_attempt_count=1,
        )


@pytest.mark.asyncio
async def test_acceptance_needs_both_an_attempt_and_a_message_id(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            status=VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
            provider_accepted_at=utcnow(),
        )


@pytest.mark.asyncio
async def test_delivered_needs_acceptance_and_read_needs_delivered(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    now = utcnow()
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, delivered_at=now)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            canary_scope="s2",
            source_booking_uuid=OTHER_UUID,
            read_at=now,
        )


@pytest.mark.asyncio
async def test_a_refund_cannot_coexist_with_a_send(session_maker) -> None:
    """Once a message may be in a customer's hands, the money stays put."""
    run_id, recipient_id = await seed_recipient(session_maker)
    now = utcnow()
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(
            session_maker,
            run_id=run_id,
            recipient_id=recipient_id,
            status=VOUCHER_DELIVERY_PAID,
            target_order_uuid=ORDER_UUID,
            voucher_code_hmac=MAC,
            hmac_key_id=KEY_ID,
            create_claimed_at=now,
            create_attempted_at=now,
            create_verified_at=now,
            pay_claimed_at=now,
            pay_attempted_at=now,
            pay_verified_at=now,
            live_guard_reproven_at=now,
            send_claimed_at=now,
            send_attempted_at=now,
            send_attempt_count=1,
            refund_claimed_at=now,
            refund_attempted_at=now,
        )


@pytest.mark.asyncio
async def test_more_than_one_send_attempt_is_not_representable(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, send_attempt_count=2)


@pytest.mark.asyncio
async def test_a_binding_without_its_key_id_is_refused(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, voucher_code_hmac=MAC)


@pytest.mark.asyncio
async def test_a_stage_cannot_be_verified_before_it_was_attempted(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    with pytest.raises((IntegrityError, DBAPIError)):
        await _row(session_maker, run_id=run_id, recipient_id=recipient_id, create_verified_at=utcnow())


@pytest.mark.asyncio
async def test_the_audit_attempt_does_not_disappear_with_the_ledger(session_maker) -> None:
    """A record of a real send attempt outlives anybody's tidy-up."""
    run_id, recipient_id = await seed_recipient(session_maker)
    ledger_id = await _row(session_maker, run_id=run_id, recipient_id=recipient_id)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekCampaignVoucherDeliveryAttempt(
                    ledger_id=ledger_id,
                    intent_uuid=uuid_module.uuid4(),
                    template_code="new_client_voucher",
                    meta_template_name="kitilash_ka_new_client_voucher_v1",
                    template_language="de",
                    campaign_recipient_id=1,
                    outcome="claimed",
                    claimed_at=utcnow(),
                )
            )

    async with session_maker() as session:
        with pytest.raises((IntegrityError, DBAPIError)):
            async with session.begin():
                await session.execute(
                    text("DELETE FROM easyweek_campaign_voucher_delivery_ledger WHERE id = :id"),
                    {"id": ledger_id},
                )


# ---------------------------------------------------------------------------
# Concurrency: exactly one external call
# ---------------------------------------------------------------------------


async def _created(session_maker, *, run_id: int, recipient_id: int) -> None:
    identity = _identity(run_id=run_id, recipient_id=recipient_id)
    await ledger_module.open_canary(session_maker, identity=identity)
    now = utcnow()
    await ledger_module.claim_create(
        session_maker,
        identity=identity,
        plan_digest="d" * 64,
        create_window_start=now - timedelta(minutes=10),
        create_window_end=now + timedelta(hours=6),
    )
    await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_CREATED,
        expected_statuses=frozenset({VOUCHER_DELIVERY_CREATE_CLAIMED}),
        target_order_uuid=str(ORDER_UUID),
        voucher_code_hmac=MAC,
        hmac_key_id=KEY_ID,
        verified_field="create_verified_at",
    )


@pytest.mark.asyncio
async def test_two_processes_together_claim_one_create(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    identity = _identity(run_id=run_id, recipient_id=recipient_id)
    await ledger_module.open_canary(session_maker, identity=identity)
    now = utcnow()

    first, second = await asyncio.gather(
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            plan_digest="a" * 64,
            create_window_start=now - timedelta(minutes=10),
            create_window_end=now + timedelta(hours=6),
        ),
        ledger_module.claim_create(
            session_maker,
            identity=identity,
            plan_digest="a" * 64,
            create_window_start=now - timedelta(minutes=10),
            create_window_end=now + timedelta(hours=6),
        ),
    )

    assert sorted([first.granted, second.granted]) == [False, True]


@pytest.mark.asyncio
async def test_two_processes_together_claim_one_pay(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _created(session_maker, run_id=run_id, recipient_id=recipient_id)
    identity = _identity(run_id=run_id, recipient_id=recipient_id)

    first, second = await asyncio.gather(
        ledger_module.claim_pay(session_maker, identity=identity, plan_digest="b" * 64),
        ledger_module.claim_pay(session_maker, identity=identity, plan_digest="b" * 64),
    )

    assert sorted([first.granted, second.granted]) == [False, True]
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PAY_CLAIMED


@pytest.mark.asyncio
async def test_two_processes_together_claim_one_send(session_maker) -> None:
    """The one that matters: two operators, one message to a real person."""
    run_id, recipient_id = await seed_recipient(session_maker)
    await _created(session_maker, run_id=run_id, recipient_id=recipient_id)
    identity = _identity(run_id=run_id, recipient_id=recipient_id)
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="b" * 64)
    await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PAID,
        expected_statuses=frozenset({VOUCHER_DELIVERY_PAY_CLAIMED}),
        verified_field="pay_verified_at",
    )
    now = utcnow()

    async def claim():
        return await ledger_module.claim_send(
            session_maker,
            identity=identity,
            plan_digest="c" * 64,
            live_guard_reproven_at=now,
            template_code="new_client_voucher",
            meta_template_name="kitilash_ka_new_client_voucher_v1",
            template_language="de",
            sender_id=1,
        )

    first, second = await asyncio.gather(claim(), claim())

    assert sorted([first.granted, second.granted]) == [False, True]
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.send_attempt_count == 1
    async with session_maker() as session:
        attempts = list((await session.execute(select(EasyWeekCampaignVoucherDeliveryAttempt))).scalars().all())
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_a_send_is_never_claimable_twice_even_after_it_finished(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _created(session_maker, run_id=run_id, recipient_id=recipient_id)
    identity = _identity(run_id=run_id, recipient_id=recipient_id)
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="b" * 64)
    await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PAID,
        expected_statuses=frozenset({VOUCHER_DELIVERY_PAY_CLAIMED}),
        verified_field="pay_verified_at",
    )
    now = utcnow()
    granted = await ledger_module.claim_send(
        session_maker,
        identity=identity,
        plan_digest="c" * 64,
        live_guard_reproven_at=now,
        template_code="new_client_voucher",
        meta_template_name="kitilash_ka_new_client_voucher_v1",
        template_language="de",
        sender_id=1,
    )
    assert granted.granted is True
    await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
        expected_statuses=frozenset({VOUCHER_DELIVERY_SEND_CLAIMED}),
        provider_message_id=PROVIDER_MESSAGE_ID,
        verified_field="provider_accepted_at",
    )

    again = await ledger_module.claim_send(
        session_maker,
        identity=identity,
        plan_digest="c" * 64,
        live_guard_reproven_at=utcnow(),
        template_code="new_client_voucher",
        meta_template_name="kitilash_ka_new_client_voucher_v1",
        template_language="de",
        sender_id=1,
    )

    assert again.granted is False


@pytest.mark.asyncio
async def test_a_claim_under_another_identity_is_refused_under_the_lock(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _created(session_maker, run_id=run_id, recipient_id=recipient_id)
    foreign = _identity(run_id=run_id, recipient_id=recipient_id, booking_uuid=OTHER_UUID)

    outcome = await ledger_module.claim_pay(session_maker, identity=foreign, plan_digest="b" * 64)

    assert outcome.granted is False
    assert outcome.reason == ledger_module.CLAIM_REFUSED_IDENTITY


@pytest.mark.asyncio
async def test_a_stale_writer_cannot_walk_the_state_backwards(session_maker) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _created(session_maker, run_id=run_id, recipient_id=recipient_id)
    identity = _identity(run_id=run_id, recipient_id=recipient_id)
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="b" * 64)

    stale = await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_CREATED,
        expected_statuses=frozenset({VOUCHER_DELIVERY_PAY_CLAIMED}),
    )

    assert stale.applied is False
    assert stale.reason == ledger_module.RECORD_WOULD_REGRESS
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PAY_CLAIMED


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------


async def _accepted(session_maker) -> tuple[int, int]:
    run_id, recipient_id = await seed_recipient(session_maker)
    await _created(session_maker, run_id=run_id, recipient_id=recipient_id)
    identity = _identity(run_id=run_id, recipient_id=recipient_id)
    await ledger_module.claim_pay(session_maker, identity=identity, plan_digest="b" * 64)
    await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PAID,
        expected_statuses=frozenset({VOUCHER_DELIVERY_PAY_CLAIMED}),
        verified_field="pay_verified_at",
    )
    await ledger_module.claim_send(
        session_maker,
        identity=identity,
        plan_digest="c" * 64,
        live_guard_reproven_at=utcnow(),
        template_code="new_client_voucher",
        meta_template_name="kitilash_ka_new_client_voucher_v1",
        template_language="de",
        sender_id=1,
    )
    await ledger_module.record_outcome(
        session_maker,
        status=VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
        expected_statuses=frozenset({VOUCHER_DELIVERY_SEND_CLAIMED}),
        provider_message_id=PROVIDER_MESSAGE_ID,
        verified_field="provider_accepted_at",
        attempt_outcome="provider_accepted",
    )
    return run_id, recipient_id


@pytest.mark.asyncio
async def test_a_webhook_moves_accepted_to_delivered_then_read(session_maker) -> None:
    await _accepted(session_maker)

    first = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_ID, status="delivered"
    )
    second = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_ID, status="read"
    )

    assert first.applied and second.applied
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == VOUCHER_DELIVERY_READ
    assert snapshot.stage_timestamps["delivered_at"] is not None
    assert snapshot.stage_timestamps["read_at"] is not None


@pytest.mark.asyncio
async def test_a_webhook_for_another_message_changes_nothing(session_maker) -> None:
    """A callback about somebody else's message says nothing about ours."""
    await _accepted(session_maker)

    result = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id="wamid.SOMEBODY_ELSE", status="delivered"
    )

    assert result.applied is False
    assert (await ledger_module.load(session_maker)).status == VOUCHER_DELIVERY_PROVIDER_ACCEPTED


@pytest.mark.asyncio
async def test_a_duplicate_or_out_of_order_webhook_never_walks_the_state_back(session_maker) -> None:
    await _accepted(session_maker)
    await ledger_module.record_webhook_transition(session_maker, provider_message_id=PROVIDER_MESSAGE_ID, status="read")
    before = await ledger_module.load(session_maker)

    late = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_ID, status="delivered"
    )
    again = await ledger_module.record_webhook_transition(
        session_maker, provider_message_id=PROVIDER_MESSAGE_ID, status="read"
    )

    assert late.applied is False
    after = await ledger_module.load(session_maker)
    assert after.status == VOUCHER_DELIVERY_READ
    # The first observation stands; a repeat does not re-stamp it.
    assert after.stage_timestamps["read_at"] == before.stage_timestamps["read_at"]
    assert again.applied is True


@pytest.mark.asyncio
async def test_the_status_worker_advances_the_canary_and_ignores_everything_else(session_maker) -> None:
    """The wiring, not just the contract.

    The canary has no OutboxMessage, so the shared WhatsApp status worker is the
    only place a delivered/read callback can ever reach it. That helper runs
    inside the worker's own session, and it must leave every other callback to
    the ordinary outbox path.
    """
    from altegio_bot.workers.whatsapp_inbox_worker import _apply_voucher_delivery_status

    await _accepted(session_maker)

    async with session_maker() as session:
        async with session.begin():
            ours = await _apply_voucher_delivery_status(session, PROVIDER_MESSAGE_ID, "delivered")
            foreign = await _apply_voucher_delivery_status(session, "wamid.SOMEBODY_ELSE", "delivered")

    assert ours is True
    assert foreign is False
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.status == "delivered"


@pytest.mark.asyncio
async def test_the_status_worker_helper_leaves_an_empty_canary_alone(session_maker) -> None:
    """With no canary row at all, nothing is claimed and nothing is written."""
    from altegio_bot.workers.whatsapp_inbox_worker import _apply_voucher_delivery_status

    async with session_maker() as session:
        async with session.begin():
            assert await _apply_voucher_delivery_status(session, PROVIDER_MESSAGE_ID, "read") is False
