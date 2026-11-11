"""Booking deliveries that commit while the follow-up executor waits for its claim."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch, send_ledger
from altegio_bot.main import app
from altegio_bot.models.models import EasyWeekEvent, EasyWeekVoucherProductionBatch
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    ATTEMPTED_AT,
    COMPANY_ID,
    KARLSRUHE_LOCATION_UUID,
    NOW,
    person,
    seed_booking_created,
)
from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
    CountingSender,
    StubTransports,
    confirm,
    offer,
    rows_of,
)
from altegio_bot.tests.test_easyweek_voucher_followup_preview import booking
from altegio_bot.tests.test_easyweek_voucher_mailing_durability import _await_lock_waiter, _followup_ready
from altegio_bot.tests.test_easyweek_voucher_mailing_durability import followup_environment as followup_environment
from altegio_bot.tests.test_easyweek_webhook import SECRET, URL, _capture_env

pytestmark = pytest.mark.usefixtures("followup_environment")


async def _receive(session_maker, event: str, payload: dict[str, Any], received_at: datetime) -> EasyWeekEvent:
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(URL, params={"event": event, "token": SECRET}, json=payload)
    assert response.status_code == 200 and response.json() == {"ok": True}
    async with session_maker.begin() as session:
        row = await session.scalar(select(EasyWeekEvent).order_by(EasyWeekEvent.id.desc()).limit(1))
        assert row is not None and row.event_hint == event and row.status == "captured"
        # Align the database receipt clock with the executor's deterministic clock.
        row.received_at = received_at
    return row


async def _execute_with_delivery(
    session_maker, batch_id: int, reader, deliver: Callable[[datetime], Awaitable[None]], *, during_wait: bool = True
):
    plan = await offer(session_maker, batch_id=batch_id)
    confirmation = await confirm(session_maker, plan=plan)
    assert confirmation.run is not None
    owner = "booking-delivery-test"
    run = await send_ledger.claim_next_run(session_maker, owner=owner, now=NOW)
    assert run is not None
    sender = CountingSender()
    now = NOW

    async def execute() -> None:
        await send_dispatch.execute_run(
            session_maker,
            run=run,
            owner=owner,
            transports=StubTransports(reader=reader, sender=sender),
            now=lambda: now,
        )

    if during_wait:
        async with asyncio.timeout(15), asyncio.TaskGroup() as tasks:
            async with session_maker.begin() as holder:
                await holder.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
                tasks.create_task(execute())
                assert await _await_lock_waiter(session_maker), "claim must be waiting for the source batch header"
                assert sender.count == 0
                now += timedelta(seconds=2)
                await deliver(now - timedelta(seconds=1))
    else:
        await deliver(now)
        await execute()

    rows = await rows_of(session_maker, run_id=confirmation.run.id)
    assert all(row.attempt_count == 0 for slot, row in rows.items() if slot != 1)
    return sender, rows[1]


def _assert_outcome(sender: CountingSender, row, *, should_send: bool) -> None:
    if should_send:
        assert sender.count == 1
        assert sender.calls[0]["to_e164"] == person(1)["phone"]
        assert (row.status, row.reason_code, row.attempt_count) == ("accepted", None, 1)
    else:
        assert sender.count == 0
        assert (row.status, row.reason_code, row.attempt_count) == ("skipped", "followup_booking_capture_unproven", 0)


@pytest.mark.parametrize(
    ("kind", "previous_delivery"),
    [
        pytest.param("oversized", True, id="two-oversized-creations"),
        pytest.param("unrepresentable", True, id="two-creations-without-a-jsonb-projection"),
        pytest.param("oversized", False, id="first-oversized-creation"),
        pytest.param("unrepresentable", False, id="first-creation-without-a-jsonb-projection"),
        pytest.param("valid", True, id="retry-of-a-proven-foreign-creation"),
    ],
)
async def test_new_creations_with_the_same_stored_payload(session_maker, kind: str, previous_delivery: bool) -> None:
    batch_id, reader = await _followup_ready(session_maker, slots=[{"state": "delivered"}, {"state": "accepted"}])
    recipient, other = person(1), person(2)
    appointment = booking(
        index=944, customer_uuid=recipient["customer_uuid"], start=NOW + timedelta(days=3), completed=False
    )
    original = {
        "uid": "69efe8af-0b0d-4751-b5f3-390b2636bb31",
        "location_id": COMPANY_ID,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "customer_id": other["numeric_id"],
        "booking_created_at": (ATTEMPTED_AT - timedelta(days=2)).isoformat(),
    }
    if kind == "oversized":
        original["comment"] = "x" * (129 * 1024)
    elif kind == "unrepresentable":
        original["comment"] = "appointment\x00note"

    valid_retry = kind == "valid"
    previous = (
        await _receive(session_maker, "booking-created", original, NOW - timedelta(seconds=2))
        if previous_delivery
        else None
    )

    async def deliver(received_at: datetime) -> None:
        body = original
        if not valid_retry:
            reader.bookings[recipient["customer_uuid"]] = [appointment]
            body = {
                **original,
                "uid": appointment["uuid"],
                "customer_id": recipient["numeric_id"],
                "booking_created_at": NOW.isoformat(),
            }
        arrived = await _receive(session_maker, "booking-created", body, received_at)
        assert arrived.body_truncated == (kind == "oversized")
        if valid_retry:
            assert arrived.payload == original and str(arrived.booking_uuid) == original["uid"]
            assert arrived.payload_hash is not None
        else:
            assert (arrived.payload, arrived.payload_hash, arrived.booking_uuid) == ({}, None, None)
        if previous is not None:
            assert previous.id != arrived.id
            assert previous.payload == arrived.payload and previous.payload_hash == arrived.payload_hash
            assert (previous.body_raw == arrived.body_raw) == valid_retry

    sender, row = await _execute_with_delivery(session_maker, batch_id, reader, deliver)
    _assert_outcome(sender, row, should_send=valid_retry)


@pytest.mark.parametrize(
    "case",
    [
        "foreign-reschedule",
        "before-worker",
        "exact-retry",
        "unrelated-creator",
        "earlier-consistent-sighting",
        "sighting-before-creation",
        "conflicting-creation",
    ],
)
async def test_rescheduling_after_transfer_off_the_creators_list(session_maker, case: str) -> None:
    batch_id, reader = await _followup_ready(
        session_maker, slots=[{"state": "delivered"}, {"state": "accepted"}, {"state": "accepted"}]
    )
    creator = person(2 if case == "unrelated-creator" else 1)
    holder = person(3)
    created_at = ATTEMPTED_AT - timedelta(days=2)
    old_start = NOW + timedelta(days=3)
    appointment = booking(index=992, customer_uuid=holder["customer_uuid"], start=old_start, completed=False)
    reader.bookings[holder["customer_uuid"]] = [appointment]
    await seed_booking_created(
        session_maker,
        booking_uuid=appointment["uuid"],
        numeric_customer_id=creator["numeric_id"],
        created_at=created_at,
        received_at=created_at + timedelta(hours=2),
    )

    def reschedule(start: datetime) -> dict:
        return {
            "uid": appointment["uuid"],
            "location_id": COMPANY_ID,
            "location_uuid": KARLSRUHE_LOCATION_UUID,
            "customer_id": holder["numeric_id"],
            "booking_date_start": start.isoformat(),
            "booking_date_end": (start + timedelta(minutes=30)).isoformat(),
        }

    previous = await _receive(session_maker, "booking-rescheduled", reschedule(old_start), NOW - timedelta(seconds=3))

    async def deliver(received_at: datetime) -> None:
        start = old_start if case == "exact-retry" else old_start + timedelta(days=1)
        reader.bookings[holder["customer_uuid"]] = [{**appointment, "start_time": start.isoformat()}]
        # A receipt can become visible now but move the earliest known sighting backwards.
        if case == "earlier-consistent-sighting":
            received_at = created_at + timedelta(hours=1)
        elif case == "sighting-before-creation":
            received_at = created_at - timedelta(hours=1)
        arrived = await _receive(session_maker, "booking-rescheduled", reschedule(start), received_at)
        assert arrived.id != previous.id
        assert arrived.payload_hash is not None and previous.payload_hash is not None
        assert (arrived.payload_hash == previous.payload_hash) == (case == "exact-retry")
        if case == "conflicting-creation":
            await seed_booking_created(
                session_maker,
                booking_uuid=appointment["uuid"],
                numeric_customer_id=creator["numeric_id"],
                created_at=NOW,
                received_at=received_at,
            )

    sender, row = await _execute_with_delivery(
        session_maker, batch_id, reader, deliver, during_wait=case != "before-worker"
    )
    _assert_outcome(sender, row, should_send=case not in {"sighting-before-creation", "conflicting-creation"})
