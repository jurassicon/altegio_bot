"""The first real booking adopts a UUID-only Client, without making another."""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from sqlalchemy import func, select

from altegio_bot import easyweek_uuid_identity as identity
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import TEST_CUSTOMER_ID, TEST_LOCATION_ID, TEST_LOCATION_UUID, booking_created
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_inbox_worker as worker

CUSTOMER = "21212121-3434-4567-8901-121212121212"
PHONE = "+4915100000821"


@pytest.fixture(autouse=True)
def configuration(monkeypatch, session_maker):
    monkeypatch.setattr(worker, "SessionLocal", session_maker)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False)
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://example.invalid/booking",
                }
            }
        ),
    )
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Fixture Category"]')


async def seed(session_maker, *, opted_out=False):
    payload = booking_created()
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = PHONE
    async with session_maker() as session, session.begin():
        client = Client(
            provider="easyweek",
            company_id=TEST_LOCATION_ID,
            altegio_client_id=None,
            easyweek_customer_uuid=uuid.UUID(CUSTOMER),
            easyweek_identity_assigned_at=utcnow(),
            phone_e164=PHONE,
            display_name="Synthetic",
            wa_opted_out=opted_out,
        )
        event = EasyWeekEvent(
            status="captured",
            event_hint="booking-created",
            auth_via="query",
            payload_hash="synthetic-hash",
            payload=payload,
            body_truncated=False,
            booking_uuid=uuid.UUID(payload["uid"]),
        )
        session.add_all([client, event])
        await session.flush()
        return client.id, event.id, payload


class Reader:
    def __init__(self, payload, session_maker, *, mismatch=False, error=False, check_lock=True):
        self.payload = payload
        self.session_maker = session_maker
        self.mismatch = mismatch
        self.error = error
        self.calls = 0
        self.check_lock = check_lock

    async def get_booking(self, booking_uuid):
        self.calls += 1
        # The claim transaction must have been rolled back before GET.
        if self.check_lock:
            async with self.session_maker() as session, session.begin():
                event = await session.scalar(select(EasyWeekEvent).with_for_update(nowait=True))
                assert event.status == "captured"
        if self.error:
            raise TimeoutError
        return {
            "uuid": booking_uuid,
            "location_uuid": TEST_LOCATION_UUID,
            "customer": {"uuid": str(uuid.uuid4()) if self.mismatch else CUSTOMER},
        }

    async def list_customers(self, *, params):
        return {
            "data": [{"uuid": CUSTOMER, "phone": PHONE, "first_name": "Synthetic"}],
            "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": 1},
        }

    async def get_customer(self, customer_uuid):
        return {"uuid": CUSTOMER, "phone": PHONE, "first_name": "Synthetic"}

    async def aclose(self):
        pass


@pytest.mark.parametrize("opted_out", [False, True])
async def test_actual_webhook_adopts_same_client_and_preserves_optout(session_maker, monkeypatch, opted_out):
    client_id, event_id, payload = await seed(session_maker, opted_out=opted_out)
    reader = Reader(payload, session_maker)
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: reader)
    assert await worker.process_one() is True  # proof outside the rolled-back event transaction
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(Record)) == 0
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == 0
        client = await session.get(Client, client_id)
        assert client.altegio_client_id == TEST_CUSTOMER_ID
        assert client.wa_opted_out is opted_out and client.easyweek_visits_total is None
    assert await worker.process_one() is True
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        record = await session.scalar(select(Record).where(Record.provider == "easyweek"))
        assert record.client_id == client_id
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 1
    assert reader.calls == 1


@pytest.mark.parametrize("failure", ["mismatch", "timeout"])
async def test_unproven_webhook_never_creates_conflicting_identity(session_maker, monkeypatch, failure):
    client_id, event_id, payload = await seed(session_maker)
    reader = Reader(payload, session_maker, mismatch=failure == "mismatch", error=failure == "timeout")
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: reader)
    assert await worker.process_one() is False
    async with session_maker() as session:
        assert (await session.get(Client, client_id)).altegio_client_id is None
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert await session.scalar(select(func.count()).select_from(Record)) == 0
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 1


async def test_concurrent_proofs_converge_to_same_identity(session_maker):
    client_id, event_id, payload = await seed(session_maker)
    registry = identity.configured_easyweek_locations()
    booking = worker.normalize_event(
        event_hint="booking-created", payload=payload, body_truncated=False, location_registry=registry.locations
    )
    results = await asyncio.gather(
        *[
            identity.resolve_webhook_identity(
                session_maker,
                event_id=event_id,
                booking=booking,
                reader=Reader(payload, session_maker, check_lock=False),
            )
            for _ in range(2)
        ]
    )
    assert all(results)
    async with session_maker() as session:
        assert (await session.get(Client, client_id)).altegio_client_id == TEST_CUSTOMER_ID
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 1


async def test_numeric_collision_cannot_bypass_uuid_identity(session_maker, monkeypatch):
    client_id, event_id, payload = await seed(session_maker)
    async with session_maker() as session, session.begin():
        collision = Client(
            provider="easyweek",
            company_id=TEST_LOCATION_ID,
            altegio_client_id=TEST_CUSTOMER_ID,
            phone_e164="+4915100000999",
            display_name="Other",
        )
        session.add(collision)
        await session.flush()
        collision_id = collision.id
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    assert await worker.process_one() is False
    async with session_maker() as session:
        assert (await session.get(Client, client_id)).altegio_client_id is None
        assert (await session.get(Client, collision_id)).phone_e164 == "+4915100000999"
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert await session.scalar(select(func.count()).select_from(Record)) == 0


async def test_uuid_bound_numeric_card_phone_change_requires_proof(session_maker, monkeypatch):
    client_id, event_id, payload = await seed(session_maker)
    async with session_maker() as session, session.begin():
        client = await session.get(Client, client_id)
        client.altegio_client_id = TEST_CUSTOMER_ID
        client.phone_e164 = "+4915100000999"
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    assert await worker.process_one() is False
    async with session_maker() as session:
        assert (await session.get(Client, client_id)).phone_e164 == "+4915100000999"
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert await session.scalar(select(func.count()).select_from(Record)) == 0


async def test_first_karlsruhe_booking_without_phone_and_concurrent_operator_add_converge(session_maker, monkeypatch):
    """Pause provider proof, add the operator identity, then resume the webhook."""
    from altegio_bot.campaigns import easyweek_manual_identity as manual_identity

    monkeypatch.setattr(identity, "KARLSRUHE_COMPANY_ID", TEST_LOCATION_ID)
    monkeypatch.setattr(manual_identity, "KARLSRUHE_COMPANY_ID", TEST_LOCATION_ID)
    client_id, event_id, payload = await seed(session_maker)
    payload.pop("customer_phone")
    payload.pop("customer_attributes.customer_phone")
    async with session_maker() as session, session.begin():
        await session.delete(await session.get(Client, client_id))
        (await session.get(EasyWeekEvent, event_id)).payload = payload
    entered = asyncio.Event()
    release = asyncio.Event()
    base = Reader(payload, session_maker)

    class PausedReader(Reader):
        async def get_booking(self, booking_uuid):
            entered.set()
            await release.wait()
            return await base.get_booking(booking_uuid)

    monkeypatch.setattr(identity, "EasyWeekClient", lambda: PausedReader(payload, session_maker))
    task = asyncio.create_task(worker.process_one())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        async with session_maker() as session, session.begin():
            assigned, reason = await manual_identity.ensure_local_identity(
                session,
                company_id=TEST_LOCATION_ID,
                phone=PHONE,
                customer_uuid=CUSTOMER,
                first_name="Synthetic",
                assign_karlsruhe=True,
            )
            assert reason is None
            assigned_id = assigned.id
        release.set()
        assert await task is True
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert await worker.process_one() is True
    async with session_maker() as session:
        rows = list((await session.scalars(select(Client).where(Client.provider == "easyweek"))).all())
        assert len(rows) == 1 and rows[0].id == assigned_id
        assert rows[0].altegio_client_id == TEST_CUSTOMER_ID
        assert (await session.scalar(select(Record))).client_id == assigned_id


async def test_explicitly_unusable_phone_does_not_hot_loop_identity_proof(session_maker, monkeypatch):
    from dataclasses import replace

    _client_id, event_id, payload = await seed(session_maker)
    registry = identity.configured_easyweek_locations()
    booking = worker.normalize_event(
        event_hint="booking-created", payload=payload, body_truncated=False, location_registry=registry.locations
    )
    booking = replace(booking, phone_e164=None)
    assert booking.carries("phone_e164")
    reader = Reader(payload, session_maker)
    assert (
        await identity.resolve_webhook_identity(session_maker, event_id=event_id, booking=booking, reader=reader)
        is False
    )
    assert reader.calls == 0


async def test_unnamed_real_booking_identity_does_not_require_voucher_template_name(session_maker, monkeypatch):
    client_id, event_id, payload = await seed(session_maker)

    class UnnamedReader(Reader):
        async def list_customers(self, *, params):
            result = await super().list_customers(params=params)
            result["data"][0]["first_name"] = ""
            return result

        async def get_customer(self, customer_uuid):
            result = await super().get_customer(customer_uuid)
            result["first_name"] = ""
            return result

    monkeypatch.setattr(identity, "EasyWeekClient", lambda: UnnamedReader(payload, session_maker))
    assert await worker.process_one() is True
    assert await worker.process_one() is True
    async with session_maker() as session:
        assert (await session.get(Client, client_id)).altegio_client_id == TEST_CUSTOMER_ID
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        assert (await session.scalar(select(Record))).client_id == client_id
