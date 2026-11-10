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
    COMPANY_ID,
    KARLSRUHE_LOCATION_UUID,
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
from altegio_bot.webhooks.common import postgres_safe_json_hash

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


@pytest.mark.parametrize("manual_uuid_only", [False, True], ids=["numeric-bridge", "uuid-only"])
@pytest.mark.parametrize("during_wait", [False, True], ids=["before-worker", "waiting-claim"])
@pytest.mark.parametrize(
    ("change", "undated_creation", "prior_holder"),
    [
        pytest.param("recipient", False, None, id="recipient"),
        pytest.param("ownerless", False, None, id="ownerless"),
        pytest.param("same-foreign", False, None, id="same-foreign"),
        pytest.param("foreign-transfer", False, None, id="foreign-transfer"),
        pytest.param("none", False, None, id="none"),
        pytest.param("recipient", True, None, id="undated-recipient"),
        pytest.param("ownerless", True, None, id="undated-ownerless"),
        pytest.param("foreign-transfer", False, "recipient", id="foreign-after-old-recipient"),
        pytest.param("foreign-transfer", False, "ownerless", id="foreign-after-old-ownerless"),
    ],
)
async def test_changed_booking_holder_is_not_decided_by_its_original_creator(
    session_maker,
    manual_uuid_only: bool,
    during_wait: bool,
    change: str,
    undated_creation: bool,
    prior_holder: str | None,
) -> None:
    """A foreign creation does not prove who holds the future appointment now."""
    source = {"state": "delivered"}
    if manual_uuid_only:
        source.update(basis=RECIPIENT_BASIS_MANUAL, numeric_id=None)
    batch_id, reader = await _followup_ready(
        session_maker, slots=[source, {"state": "accepted"}, {"state": "accepted"}]
    )
    recipient = person(1)
    creator = person(2)
    other_holder = person(3)
    appointment = booking(
        index=994,
        customer_uuid=creator["customer_uuid"],
        start=NOW + timedelta(days=3),
        completed=False,
    )
    if undated_creation:
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=appointment["uuid"],
            numeric_customer_id=creator["numeric_id"],
            received_at=ATTEMPTED_AT - timedelta(days=1),
        )
    else:
        await seed_booking_created(
            session_maker,
            booking_uuid=appointment["uuid"],
            numeric_customer_id=creator["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=2),
        )
    if prior_holder is not None:
        # Already present when the full provider history is read. A later
        # change proven to involve only somebody else must not revive this
        # old uncertainty as if it had arrived during the claim wait.
        await seed_booking_lifecycle(
            session_maker,
            booking_uuid=appointment["uuid"],
            received_at=NOW - timedelta(seconds=3),
            event_hint="booking-updated",
            numeric_customer_id=recipient["numeric_id"] if prior_holder == "recipient" else None,
        )
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = "booking-holder-boundary-test"
    run = await send_ledger.claim_next_run(session_maker, owner=owner, now=NOW)
    assert run is not None
    sender = CountingSender()
    clock = [NOW]

    async def change_holder() -> None:
        if change == "none":
            return
        if change in {"recipient", "ownerless"}:
            # The provider's live list changes as well as the local capture.
            # Reading this before the worker must exclude a future booking;
            # seeing only the new capture must invalidate its cached history.
            reader.bookings = {
                recipient["customer_uuid"]: [{**appointment, "customer": {"uuid": recipient["customer_uuid"]}}]
            }
        customer_id = {
            "recipient": recipient["numeric_id"],
            "ownerless": None,
            "same-foreign": creator["numeric_id"],
            "foreign-transfer": other_holder["numeric_id"],
        }[change]
        await seed_booking_lifecycle(
            session_maker,
            booking_uuid=appointment["uuid"],
            received_at=clock[0] - timedelta(seconds=1),
            event_hint="booking-updated",
            numeric_customer_id=customer_id,
        )

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
                assert await _await_lock_waiter(session_maker), "claim must wait for the source batch header"
                assert sender.count == 0
                clock[0] += timedelta(seconds=2)
                await change_holder()
            finally:
                await transaction.commit()
                await asyncio.wait_for(task, timeout=10)
    else:
        await change_holder()
        await execute()

    rows = await rows_of(session_maker, run_id=outcome.run.id)
    row = rows[1]
    if change in {"recipient", "ownerless"}:
        assert sender.count == 0
        assert row.status == "skipped"
        assert row.attempt_count == 0
        assert row.reason_code == (
            "followup_booking_capture_unproven" if during_wait else "followup_future_booking_active"
        )
    else:
        assert sender.count == 1
        assert row.status == "accepted"
        assert row.attempt_count == 1
        assert sender.calls[0]["to_e164"] == recipient["phone"]
    assert all(other.attempt_count == 0 for slot, other in rows.items() if slot != 1)


@pytest.mark.parametrize("kind", ["malformed", "truncated", "foreign-without-creation", "foreign-recipient-created"])
async def test_repeated_lifecycle_delivery_keeps_unknown_and_proven_foreign_changes_distinct(
    session_maker, kind: str
) -> None:
    batch_id, reader = await _followup_ready(
        session_maker, slots=[{"state": "delivered"}, {"state": "accepted"}, {"state": "accepted"}]
    )
    recipient, other = person(1), person(3)
    appointment = booking(
        index=990,
        customer_uuid=recipient["customer_uuid"],
        start=NOW + timedelta(days=3),
        completed=False,
    )
    if kind == "foreign-recipient-created":
        await seed_booking_created(
            session_maker,
            booking_uuid=appointment["uuid"],
            numeric_customer_id=recipient["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=2),
        )
    payload = {
        "uid": "not-a-booking-uuid" if kind == "malformed" else appointment["uuid"],
        "location_id": COMPANY_ID,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "customer_id": other["numeric_id"],
    }

    async def capture(received_at) -> int:
        async with session_maker() as session:
            async with session.begin():
                event = EasyWeekEvent(
                    event_hint="booking-updated",
                    query={},
                    headers={},
                    payload=dict(payload),
                    payload_hash=postgres_safe_json_hash(payload),
                    body_truncated=kind == "truncated",
                    received_at=received_at,
                    status="processed",
                )
                session.add(event)
                await session.flush()
                return int(event.id)

    # With no captured creation, a sighting before the original mailing proves
    # that this was already an existing booking; no creation date is invented.
    old_received_at = (
        ATTEMPTED_AT - timedelta(days=1) if kind == "foreign-without-creation" else NOW - timedelta(seconds=3)
    )
    old_id = await capture(old_received_at)
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = "booking-repeated-lifecycle-test"
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
            assert await _await_lock_waiter(session_maker), "claim must wait for the source batch header"
            assert sender.count == 0
            clock[0] += timedelta(seconds=2)
            new_id = await capture(clock[0] - timedelta(seconds=1))
            async with session_maker() as session:
                old = await session.get(EasyWeekEvent, old_id)
                new = await session.get(EasyWeekEvent, new_id)
                assert old is not None and new is not None
                assert old.id != new.id
                assert old.received_at < new.received_at
                assert old.payload == new.payload
                assert old.payload_hash == new.payload_hash == postgres_safe_json_hash(new.payload)
        finally:
            await transaction.commit()
            await asyncio.wait_for(task, timeout=10)

    rows = await rows_of(session_maker, run_id=outcome.run.id)
    row = rows[1]
    if kind in {"malformed", "truncated"}:
        assert sender.count == 0
        assert row.status == "skipped"
        assert row.attempt_count == 0
        assert row.reason_code == "followup_booking_capture_unproven"
    else:
        # The latest delivery still names another person. In particular, an
        # original creation by this recipient must not revive past ownership.
        assert sender.count == 1
        assert row.status == "accepted"
        assert row.attempt_count == 1
        assert sender.calls[0]["to_e164"] == recipient["phone"]
    assert all(other.attempt_count == 0 for slot, other in rows.items() if slot != 1)


async def _capture_hashed_lifecycle(session_maker, *, booking_uuid: str, numeric_customer_id, received_at) -> int:
    event_id = await seed_booking_lifecycle(
        session_maker,
        booking_uuid=booking_uuid,
        received_at=received_at,
        event_hint="booking-updated",
        numeric_customer_id=numeric_customer_id,
    )
    async with session_maker() as session:
        async with session.begin():
            event = await session.get(EasyWeekEvent, event_id)
            assert event is not None
            event.payload_hash = postgres_safe_json_hash(event.payload)
    return event_id


@pytest.mark.parametrize("target_kind", ["recipient", "ownerless", "foreign"])
@pytest.mark.parametrize("during_wait", [False, True], ids=["before-worker", "waiting-claim"])
@pytest.mark.parametrize("repeat_payload", [False, True], ids=["new-payload", "repeated-payload"])
async def test_returning_to_a_previous_booking_holder_invalidates_the_cached_history(
    session_maker, target_kind: str, during_wait: bool, repeat_payload: bool
) -> None:
    batch_id, reader = await _followup_ready(
        session_maker, slots=[{"state": "delivered"}, {"state": "accepted"}, {"state": "accepted"}]
    )
    recipient, creator, other = person(1), person(2), person(3)
    appointment = booking(
        index=988,
        customer_uuid=creator["customer_uuid"],
        start=NOW + timedelta(days=3),
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=appointment["uuid"],
        numeric_customer_id=creator["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=2),
    )
    target_id = {"recipient": recipient["numeric_id"], "ownerless": None, "foreign": creator["numeric_id"]}[target_kind]
    old_id = None
    if repeat_payload:
        old_id = await _capture_hashed_lifecycle(
            session_maker,
            booking_uuid=appointment["uuid"],
            numeric_customer_id=target_id,
            received_at=NOW - timedelta(seconds=3),
        )
    # The recipient's live history is empty after the appointment moves away.
    # A later return can have exactly the same payload as its previous stay.
    await _capture_hashed_lifecycle(
        session_maker,
        booking_uuid=appointment["uuid"],
        numeric_customer_id=other["numeric_id"],
        received_at=NOW - timedelta(seconds=2),
    )
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = "booking-returned-holder-test"
    run = await send_ledger.claim_next_run(session_maker, owner=owner, now=NOW)
    assert run is not None
    sender = CountingSender()
    clock = [NOW]

    async def transfer_back() -> None:
        if target_kind != "foreign":
            reader.bookings = {
                recipient["customer_uuid"]: [{**appointment, "customer": {"uuid": recipient["customer_uuid"]}}]
            }
        new_id = await _capture_hashed_lifecycle(
            session_maker,
            booking_uuid=appointment["uuid"],
            numeric_customer_id=target_id,
            received_at=clock[0] - timedelta(seconds=1),
        )
        if old_id is not None:
            async with session_maker() as session:
                old = await session.get(EasyWeekEvent, old_id)
                new = await session.get(EasyWeekEvent, new_id)
                assert old is not None and new is not None
                assert old.id != new.id
                assert old.received_at < new.received_at
                assert old.payload == new.payload
                assert old.payload_hash == new.payload_hash == postgres_safe_json_hash(new.payload)

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
                assert await _await_lock_waiter(session_maker), "claim must wait for the source batch header"
                assert sender.count == 0
                clock[0] += timedelta(seconds=2)
                await transfer_back()
            finally:
                await transaction.commit()
                await asyncio.wait_for(task, timeout=10)
    else:
        await transfer_back()
        await execute()

    rows = await rows_of(session_maker, run_id=outcome.run.id)
    row = rows[1]
    if target_kind != "foreign":
        assert sender.count == 0
        assert row.status == "skipped"
        assert row.attempt_count == 0
        assert row.reason_code == (
            "followup_booking_capture_unproven" if during_wait else "followup_future_booking_active"
        )
    else:
        assert sender.count == 1
        assert row.status == "accepted"
        assert row.attempt_count == 1
        assert sender.calls[0]["to_e164"] == recipient["phone"]
    assert all(other.attempt_count == 0 for slot, other in rows.items() if slot != 1)


@pytest.mark.parametrize("target_kind", ["recipient", "ownerless", "foreign"])
async def test_late_commit_of_a_lower_id_lifecycle_delivery_rechecks_cached_history(
    session_maker, target_kind: str
) -> None:
    batch_id, reader = await _followup_ready(
        session_maker, slots=[{"state": "delivered"}, {"state": "accepted"}, {"state": "accepted"}]
    )
    recipient, creator, other = person(1), person(2), person(3)
    appointment = booking(
        index=989,
        customer_uuid=creator["customer_uuid"],
        start=NOW + timedelta(days=3),
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=appointment["uuid"],
        numeric_customer_id=creator["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=2),
    )
    target_id = {"recipient": recipient["numeric_id"], "ownerless": None, "foreign": creator["numeric_id"]}[target_kind]
    old_id = await _capture_hashed_lifecycle(
        session_maker,
        booking_uuid=appointment["uuid"],
        numeric_customer_id=target_id,
        received_at=NOW - timedelta(seconds=4),
    )
    plan = await offer(session_maker, batch_id=batch_id)
    outcome = await confirm(session_maker, plan=plan)
    assert outcome.run is not None
    owner = "booking-late-commit-test"
    run = await send_ledger.claim_next_run(session_maker, owner=owner, now=NOW)
    assert run is not None
    sender = CountingSender()
    clock = [NOW]

    async with session_maker() as incoming:
        incoming_transaction = await incoming.begin()
        old = await incoming.get(EasyWeekEvent, old_id)
        assert old is not None
        pending = EasyWeekEvent(
            event_hint="booking-updated",
            query={},
            headers={},
            payload=dict(old.payload),
            payload_hash=postgres_safe_json_hash(old.payload),
            received_at=NOW - timedelta(seconds=2),
            status="processed",
        )
        incoming.add(pending)
        await incoming.flush()
        # Allocate a lower ID without committing. Another connection commits
        # a higher ID before the worker reads its complete booking history.
        newer_visible_id = await _capture_hashed_lifecycle(
            session_maker,
            booking_uuid=appointment["uuid"],
            numeric_customer_id=other["numeric_id"],
            received_at=NOW - timedelta(seconds=1),
        )
        assert old.id < pending.id < newer_visible_id
        assert old.received_at < pending.received_at < NOW
        assert old.payload == pending.payload
        assert old.payload_hash == pending.payload_hash == postgres_safe_json_hash(pending.payload)
        async with session_maker() as observer:
            assert await observer.get(EasyWeekEvent, pending.id) is None
            assert await observer.get(EasyWeekEvent, newer_visible_id) is not None

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
                assert await _await_lock_waiter(session_maker), "claim must wait for the source batch header"
                assert sender.count == 0
                clock[0] += timedelta(seconds=2)
                if target_kind != "foreign":
                    reader.bookings = {
                        recipient["customer_uuid"]: [{**appointment, "customer": {"uuid": recipient["customer_uuid"]}}]
                    }
                # This is a real visibility change, not a backdated receipt in
                # an otherwise new row. ID and receipt-time watermarks miss it.
                await incoming_transaction.commit()
                async with session_maker() as observer:
                    assert await observer.get(EasyWeekEvent, pending.id) is not None
            finally:
                await transaction.commit()
                await asyncio.wait_for(task, timeout=10)

    rows = await rows_of(session_maker, run_id=outcome.run.id)
    row = rows[1]
    if target_kind != "foreign":
        assert sender.count == 0
        assert row.status == "skipped"
        assert row.attempt_count == 0
        assert row.reason_code == "followup_booking_capture_unproven"
    else:
        assert sender.count == 1
        assert row.status == "accepted"
        assert row.attempt_count == 1
        assert sender.calls[0]["to_e164"] == recipient["phone"]
    assert all(other.attempt_count == 0 for slot, other in rows.items() if slot != 1)
