"""Booking evidence committed before the follow-up attempt boundary."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch, send_ledger
from altegio_bot.models.models import RECIPIENT_BASIS_MANUAL, EasyWeekEvent, EasyWeekVoucherProductionBatch
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    ATTEMPTED_AT,
    NOW,
    person,
    seed_booking_created,
    seed_booking_created_without_a_customer,
    seed_booking_created_without_a_date,
    seed_booking_lifecycle,
)
from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
    CountingSender,
    StubTransports,
    build_preview,
    confirm,
    offer,
    rows_of,
)
from altegio_bot.tests.test_easyweek_voucher_followup_preview import booking
from altegio_bot.tests.test_easyweek_voucher_mailing_durability import (
    _await_lock_waiter,
    _followup_ready,
)
from altegio_bot.tests.test_easyweek_voucher_mailing_durability import (
    followup_environment as followup_environment,
)

BOOKING_UUID = "aa770000-0000-0000-0000-000000000001"
pytestmark = pytest.mark.usefixtures("followup_environment")


async def _capture(session_maker, *, kind: str, received_at) -> None:
    if kind == "none":
        return
    if kind == "ownerless":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=BOOKING_UUID,
            created_at=ATTEMPTED_AT + timedelta(days=1),
            received_at=received_at,
        )
        return
    if kind == "undated":
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=BOOKING_UUID,
            numeric_customer_id=person(1)["numeric_id"],
            received_at=received_at,
        )
        return
    await seed_booking_created(
        session_maker,
        booking_uuid=BOOKING_UUID,
        numeric_customer_id=person(2 if kind == "other" else 1)["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
        received_at=received_at,
    )
    if kind in {"conflicting", "changed_owner"}:
        await seed_booking_created(
            session_maker,
            booking_uuid=BOOKING_UUID,
            numeric_customer_id=person(2)["numeric_id"],
            created_at=ATTEMPTED_AT + timedelta(days=1),
            received_at=received_at,
        )
    if kind == "truncated":
        async with session_maker() as session:
            async with session.begin():
                event = await session.scalar(select(EasyWeekEvent).order_by(EasyWeekEvent.id.desc()).limit(1))
                assert event is not None
                event.body_truncated = True


@pytest.mark.parametrize(
    ("kind", "during_wait", "manual_uuid_only", "expected_reason"),
    [
        ("own", False, False, "followup_new_booking_after_attempt"),
        ("own", True, False, "followup_new_booking_after_attempt"),
        ("undated", True, False, "followup_booking_capture_unproven"),
        ("conflicting", True, False, "followup_booking_capture_unproven"),
        ("other", True, False, None),
        ("none", True, False, None),
        ("own", True, True, "followup_booking_capture_unproven"),
        ("late", True, True, "followup_booking_capture_unproven"),
        ("other", True, True, None),
        ("none", True, True, None),
        ("duplicate", True, True, None),
        ("changed_owner", True, True, "followup_booking_capture_unproven"),
        ("ownerless", True, True, "followup_booking_capture_unproven"),
        ("truncated", True, True, "followup_booking_capture_unproven"),
    ],
)
async def test_booking_capture_at_the_claim_boundary(
    session_maker, kind: str, during_wait: bool, manual_uuid_only: bool, expected_reason: str | None
) -> None:
    source = {"state": "delivered"}
    if manual_uuid_only:
        source.update(basis=RECIPIENT_BASIS_MANUAL, numeric_id=None)
    batch_id, reader = await _followup_ready(session_maker, slots=[source, {"state": "accepted"}])
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = "booking-boundary-test"
    run = await send_ledger.claim_next_run(session_maker, owner=owner, now=NOW)
    assert run is not None
    sender = CountingSender()
    clock = [NOW]
    if kind in {"duplicate", "changed_owner"}:
        await _capture(session_maker, kind="own", received_at=NOW - timedelta(seconds=2))

    async def execute():
        return await send_dispatch.execute_run(
            session_maker,
            run=run,
            owner=owner,
            transports=StubTransports(reader=reader, sender=sender),
            now=lambda: clock[0],
        )

    if during_wait:
        async with session_maker() as holder:
            transaction = await holder.begin()
            await holder.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
            task = asyncio.create_task(execute())
            try:
                assert await _await_lock_waiter(session_maker), "the claim must wait on the original batch"
                assert sender.count == 0
                # This event is newer than the external proof and its local
                # scan, but is committed before the claim obtains its locks.
                clock[0] += timedelta(seconds=2)
                received_at = NOW - timedelta(seconds=1) if kind == "late" else clock[0] - timedelta(seconds=1)
                await _capture(session_maker, kind=kind, received_at=received_at)
            finally:
                await transaction.commit()
                await task
    else:
        await _capture(session_maker, kind=kind, received_at=NOW)
        await execute()

    rows = await rows_of(session_maker, run_id=outcome.run.id)
    row = rows[1]
    if expected_reason is not None:
        assert sender.count == 0
        assert row.status == "skipped"
        assert row.reason_code == expected_reason
        assert row.attempt_count == 0
    else:
        assert sender.count == 1
        assert row.status == "accepted"
        assert row.attempt_count == 1
        assert sender.calls[0]["to_e164"] == person(1)["phone"]


async def test_rescheduling_a_known_booking_invalidates_the_cached_live_proof(session_maker) -> None:
    batch_id, reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    old = booking(
        index=773,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        completed=False,
    )
    reader.bookings = {who["customer_uuid"]: [old]}
    await seed_booking_created(
        session_maker,
        booking_uuid=old["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=2),
    )
    await build_preview(session_maker, batch_id=batch_id, reader=reader)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = "booking-rescheduled-test"
    run = await send_ledger.claim_next_run(session_maker, owner=owner, now=NOW)
    assert run is not None
    sender = CountingSender()
    clock = [NOW]
    async with session_maker() as holder:
        transaction = await holder.begin()
        await holder.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
        task = asyncio.create_task(
            send_dispatch.execute_run(
                session_maker,
                run=run,
                owner=owner,
                transports=StubTransports(reader=reader, sender=sender),
                now=lambda: clock[0],
            )
        )
        try:
            assert await _await_lock_waiter(session_maker)
            assert sender.count == 0
            clock[0] += timedelta(seconds=2)
            reader.bookings = {who["customer_uuid"]: [{**old, "start_time": (NOW + timedelta(days=3)).isoformat()}]}
            await seed_booking_lifecycle(
                session_maker,
                booking_uuid=old["uuid"],
                received_at=clock[0] - timedelta(seconds=1),
                event_hint="booking-rescheduled",
                numeric_customer_id=who["numeric_id"],
            )
        finally:
            await transaction.commit()
            await task
    row = (await rows_of(session_maker, run_id=outcome.run.id))[1]
    assert sender.count == 0
    assert row.attempt_count == 0
    assert row.status == "skipped"
    assert row.reason_code == "followup_booking_capture_unproven"
