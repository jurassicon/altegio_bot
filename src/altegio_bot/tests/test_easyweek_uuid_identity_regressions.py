"""Separate regression reproductions for the PR-21 identity review."""

import json
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from altegio_bot import easyweek_uuid_identity as identity
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, OutboxMessage, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import TEST_CUSTOMER_ID, TEST_LOCATION_ID
from altegio_bot.tests.test_easyweek_inbox_worker_integration import _at, _capture, _future
from altegio_bot.tests.test_easyweek_uuid_identity_worker import (
    CUSTOMER,
    PHONE,
    Reader,
    seed,
)
from altegio_bot.tests.test_easyweek_uuid_identity_worker import (
    configuration as configuration,
)
from altegio_bot.workers import easyweek_inbox_worker as worker

OTHER_COMPANY = TEST_LOCATION_ID + 1
OTHER_UUID = "41414141-3434-4567-8901-121212121212"
OTHER_PHONE = "+4915100000998"


def configure_second_branch(monkeypatch):
    locations = json.loads(settings.easyweek_location_map)
    locations["second"] = {
        "location_id": OTHER_COMPANY,
        "location_uuid": "51515151-3434-4567-8901-121212121212",
        "meta_template_prefix": "second",
        "booking_page_url": "https://example.invalid/second",
    }
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(locations))


async def test_r1_captured_numeric_a_never_attaches_to_reassigned_customer_b(session_maker, monkeypatch):
    client_b_id, event_id, payload = await seed(session_maker, opted_out=True)
    configure_second_branch(monkeypatch)
    payload.pop("customer_phone")
    payload.pop("customer_attributes.customer_phone")
    async with session_maker() as session, session.begin():
        session.add(
            Client(
                provider="easyweek",
                company_id=OTHER_COMPANY,
                altegio_client_id=TEST_CUSTOMER_ID,
                easyweek_customer_uuid=uuid.UUID(OTHER_UUID),
                phone_e164=OTHER_PHONE,
                display_name="Synthetic A",
                wa_opted_out=True,
                wa_opt_out_reason="Synthetic A opt-out",
            )
        )
        event = await session.get(EasyWeekEvent, event_id)
        event.payload = payload
    preserved = select(
        Client.id,
        Client.company_id,
        Client.altegio_client_id,
        Client.easyweek_customer_uuid,
        Client.phone_e164,
        Client.display_name,
        Client.wa_opted_out,
        Client.wa_opted_out_at,
        Client.wa_opt_out_reason,
    ).order_by(Client.id)
    async with session_maker() as session:
        before = (await session.execute(preserved)).all()
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    await worker.process_one()
    await worker.process_one()
    async with session_maker() as session:
        client_b = await session.get(Client, client_b_id)
        records = list((await session.scalars(select(Record))).all())
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0
        assert client_b.altegio_client_id is None, "B borrowed captured numeric ID belonging to A"
        assert not records, "An uncertain identity must not create a Record"
        assert (await session.get(EasyWeekEvent, event_id)).status != "processed"
        assert (await session.execute(preserved)).all() == before


async def test_r2_proven_phone_change_on_same_identity_is_processed(session_maker, monkeypatch):
    client_id, event_id, payload = await seed(session_maker, opted_out=True)
    async with session_maker() as session, session.begin():
        client = await session.get(Client, client_id)
        client.altegio_client_id = TEST_CUSTOMER_ID
        client.phone_e164 = OTHER_PHONE
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    await worker.process_one()
    await worker.process_one()
    async with session_maker() as session:
        client = await session.get(Client, client_id)
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        assert client.phone_e164 == PHONE and client.wa_opted_out is True
        assert (await session.scalar(select(Record))).client_id == client_id
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 1


async def test_r2_explicit_clear_on_known_identity_is_processed_without_send(session_maker, monkeypatch):
    client_id, event_id, payload = await seed(session_maker)
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = ""
    async with session_maker() as session, session.begin():
        (await session.get(Client, client_id)).altegio_client_id = TEST_CUSTOMER_ID
        (await session.get(EasyWeekEvent, event_id)).payload = payload

    class ClearedReader(Reader):
        async def get_customer(self, customer_uuid):
            return {"uuid": CUSTOMER, "phone": None, "first_name": "Synthetic"}

    monkeypatch.setattr(identity, "EasyWeekClient", lambda: ClearedReader(payload, session_maker))
    await worker.process_one()
    await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        assert (await session.get(Client, client_id)).phone_e164 is None
        assert (await session.scalar(select(Record))).client_id == client_id
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0


async def test_r3_real_booking_in_second_branch_keeps_branch_scoped_clients(session_maker, monkeypatch):
    first_client_id, event_id, payload = await seed(session_maker)
    configure_second_branch(monkeypatch)
    async with session_maker() as session, session.begin():
        first = await session.get(Client, first_client_id)
        first.company_id = OTHER_COMPANY
        first.altegio_client_id = TEST_CUSTOMER_ID
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    await worker.process_one()
    await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        first = await session.get(Client, first_client_id)
        assert first.company_id == OTHER_COMPANY and first.easyweek_customer_uuid == uuid.UUID(CUSTOMER)
        record = await session.scalar(select(Record))
        assert record.company_id == TEST_LOCATION_ID and record.client_id != first_client_id
        assert (await session.get(Client, record.client_id)).company_id == TEST_LOCATION_ID


@pytest.mark.parametrize("phone_present", [True, False])
async def test_r4_ordinary_new_customer_without_phone_does_not_need_voucher_api(
    session_maker, monkeypatch, phone_present
):
    client_id, event_id, payload = await seed(session_maker)
    monkeypatch.setattr(identity, "KARLSRUHE_COMPANY_ID", TEST_LOCATION_ID)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False)
    if phone_present:
        payload["customer_phone"] = payload["customer_attributes.customer_phone"] = ""
    else:
        payload.pop("customer_phone")
        payload.pop("customer_attributes.customer_phone")
    async with session_maker() as session, session.begin():
        await session.delete(await session.get(Client, client_id))
        (await session.get(EasyWeekEvent, event_id)).payload = payload

    def no_identity_api():
        raise AssertionError("Ordinary numeric ingestion must not need the voucher customer API")

    monkeypatch.setattr(identity, "EasyWeekClient", no_identity_api)
    await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        record = await session.scalar(select(Record))
        client = await session.get(Client, record.client_id)
        assert client.altegio_client_id == TEST_CUSTOMER_ID and client.phone_e164 is None
        assert client.easyweek_customer_uuid is None
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0


@pytest.mark.parametrize("hint", ["booking-canceled", "booking-rescheduled"])
@pytest.mark.parametrize("cleared", [False, True])
async def test_r2_contact_change_preserves_record_and_reminder_lifecycle(session_maker, monkeypatch, hint, cleared):
    client_id, event_id, payload = await seed(session_maker)
    start = _future(days=3)
    payload = _at(payload, start)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True)
    async with session_maker() as session, session.begin():
        (await session.get(Client, client_id)).altegio_client_id = TEST_CUSTOMER_ID
        (await session.get(EasyWeekEvent, event_id)).payload = payload
    assert await worker.process_one()
    async with session_maker() as session:
        jobs = list((await session.scalars(select(MessageJob).where(MessageJob.job_type.like("reminder_%")))).all())
        original_ids = {job.id for job in jobs}
        assert len(original_ids) == 2
    changed = dict(payload)
    changed["customer_phone"] = changed["customer_attributes.customer_phone"] = "" if cleared else OTHER_PHONE
    changed["booking_status"] = "Canceled appointment" if hint == "booking-canceled" else "Rescheduled appointment"
    if hint == "booking-rescheduled":
        changed = _at(changed, start + timedelta(days=1))
    async with session_maker() as session, session.begin():
        changed_id = await _capture(session, changed, event_hint=hint, payload_hash="changed-contact")

    class ChangedReader(Reader):
        async def get_customer(self, customer_uuid):
            return {"uuid": CUSTOMER, "phone": None if cleared else OTHER_PHONE, "first_name": "Synthetic"}

        async def list_customers(self, *, params):
            return {
                "data": [await self.get_customer(CUSTOMER)],
                "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": 1},
            }

    monkeypatch.setattr(identity, "EasyWeekClient", lambda: ChangedReader(changed, session_maker, check_lock=False))
    assert await worker.process_one()
    assert await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, changed_id)).status == "processed"
        assert (await session.get(Client, client_id)).phone_e164 == (None if cleared else OTHER_PHONE)
        record = await session.scalar(select(Record))
        assert record.client_id == client_id
        if hint == "booking-canceled":
            assert record.is_deleted
        else:
            assert record.starts_at == start + timedelta(days=1)
        jobs = list((await session.scalars(select(MessageJob).where(MessageJob.job_type.like("reminder_%")))).all())
        assert all(job.status == "canceled" for job in jobs if job.id in original_ids)
        if hint == "booking-rescheduled" and not cleared:
            assert len([job for job in jobs if job.status == "queued"]) == 2
        else:
            assert not [job for job in jobs if job.status == "queued"]
        before = len(list((await session.scalars(select(MessageJob))).all()))
    async with session_maker() as session, session.begin():
        await _capture(session, changed, event_hint=hint, payload_hash="changed-contact")
    assert await worker.process_one()
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == before


async def test_r4_unaddressable_numeric_ingestion_cannot_race_a_duplicate_manual_card(session_maker, monkeypatch):
    from altegio_bot.campaigns import easyweek_manual_identity as manual

    client_id, event_id, payload = await seed(session_maker)
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = ""
    monkeypatch.setattr(manual, "KARLSRUHE_COMPANY_ID", TEST_LOCATION_ID)
    async with session_maker() as session, session.begin():
        await session.delete(await session.get(Client, client_id))
        (await session.get(EasyWeekEvent, event_id)).payload = payload
    assert await worker.process_one()
    async with session_maker() as session, session.begin():
        assigned, reason = await manual.ensure_local_identity(
            session,
            company_id=TEST_LOCATION_ID,
            phone=PHONE,
            customer_uuid=CUSTOMER,
            first_name="Synthetic",
            assign_karlsruhe=True,
        )
        assert assigned is None and reason == manual.IDENTITY_CONFLICT
    # Later captured contact evidence updates the same numeric card; a proven
    # manual UUID binding then reuses it, never manufacturing a second Client.
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = PHONE
    async with session_maker() as session, session.begin():
        await _capture(session, payload, event_hint="booking-updated", payload_hash="now-addressable")
    assert await worker.process_one()
    async with session_maker() as session, session.begin():
        assigned, reason = await manual.ensure_local_identity(
            session,
            company_id=TEST_LOCATION_ID,
            phone=PHONE,
            customer_uuid=CUSTOMER,
            first_name="Synthetic",
            assign_karlsruhe=True,
        )
        assert reason is None
        assert assigned.id == (await session.scalar(select(Record))).client_id
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 1


@pytest.mark.parametrize("existing_numeric", [False, True])
@pytest.mark.parametrize("opted_out", [False, True])
async def test_r3_branch_binding_keeps_cards_audit_and_jobs_separate(
    session_maker, monkeypatch, existing_numeric, opted_out
):
    from altegio_bot.utils import utcnow

    first_id, event_id, payload = await seed(session_maker, opted_out=opted_out)
    configure_second_branch(monkeypatch)
    payload = _at(payload, _future(days=3))
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True)
    audit_at = utcnow() if opted_out else None
    async with session_maker() as session, session.begin():
        first = await session.get(Client, first_id)
        first.company_id = OTHER_COMPANY
        first.altegio_client_id = TEST_CUSTOMER_ID
        first.wa_opted_out_at = audit_at
        first.wa_opt_out_reason = "synthetic audit" if opted_out else None
        first.easyweek_visits_total = 7
        first.easyweek_visits_total_updated_at = utcnow()
        (await session.get(EasyWeekEvent, event_id)).payload = payload
        if existing_numeric:
            second = Client(
                provider="easyweek", company_id=TEST_LOCATION_ID, altegio_client_id=TEST_CUSTOMER_ID, phone_e164=PHONE
            )
            session.add(second)
            await session.flush()
            second_id = second.id
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    assert await worker.process_one()
    assert await worker.process_one()
    async with session_maker() as session:
        first = await session.get(Client, first_id)
        record = await session.scalar(select(Record))
        second = await session.get(Client, record.client_id)
        assert first.company_id == OTHER_COMPANY and first.easyweek_visits_total == 7
        assert first.phone_e164 == PHONE and first.wa_opted_out_at == audit_at
        assert second.id != first_id and second.company_id == TEST_LOCATION_ID
        assert second.easyweek_customer_uuid == first.easyweek_customer_uuid
        assert second.wa_opted_out is opted_out and second.wa_opted_out_at == audit_at
        assert second.wa_opt_out_reason == first.wa_opt_out_reason
        if existing_numeric:
            assert second.id == second_id
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 2
        jobs = list((await session.scalars(select(MessageJob))).all())
        assert all(job.company_id == TEST_LOCATION_ID and job.client_id == second.id for job in jobs)
        assert not await session.scalar(select(func.count()).select_from(OutboxMessage))


async def test_r1_known_numeric_uuid_bridge_allows_absent_phone_without_borrowing_remote_contact(
    session_maker, monkeypatch
):
    first_id, event_id, payload = await seed(session_maker)
    configure_second_branch(monkeypatch)
    payload.pop("customer_phone")
    payload.pop("customer_attributes.customer_phone")
    async with session_maker() as session, session.begin():
        first = await session.get(Client, first_id)
        first.company_id = OTHER_COMPANY
        first.altegio_client_id = TEST_CUSTOMER_ID
        (await session.get(EasyWeekEvent, event_id)).payload = payload
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker))
    assert await worker.process_one()
    assert await worker.process_one()
    async with session_maker() as session:
        record = await session.scalar(select(Record))
        target = await session.get(Client, record.client_id)
        assert target.id != first_id and target.phone_e164 is None
        assert target.easyweek_customer_uuid == uuid.UUID(CUSTOMER)
        assert (await session.get(Client, first_id)).phone_e164 == PHONE
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"


@pytest.mark.parametrize("failure", ["uuid_mismatch", "occupied_phone", "ambiguous_phone"])
async def test_r2_phone_change_rejects_conflicting_evidence_without_identity_damage(
    session_maker, monkeypatch, failure
):
    client_id, event_id, payload = await seed(session_maker, opted_out=True)
    async with session_maker() as session, session.begin():
        client = await session.get(Client, client_id)
        client.altegio_client_id = TEST_CUSTOMER_ID
        client.phone_e164 = OTHER_PHONE
        if failure == "occupied_phone":
            session.add(
                Client(
                    provider="easyweek",
                    company_id=TEST_LOCATION_ID,
                    altegio_client_id=TEST_CUSTOMER_ID + 1,
                    easyweek_customer_uuid=uuid.UUID(OTHER_UUID),
                    phone_e164=PHONE,
                )
            )

    class ConflictingReader(Reader):
        async def list_customers(self, *, params):
            result = await super().list_customers(params=params)
            if failure == "ambiguous_phone":
                result["data"].append({"uuid": OTHER_UUID, "phone": PHONE, "first_name": "Other"})
                result["meta"]["total"] = 2
            return result

    monkeypatch.setattr(
        identity,
        "EasyWeekClient",
        lambda: ConflictingReader(payload, session_maker, mismatch=failure == "uuid_mismatch"),
    )
    assert not await worker.process_one()
    async with session_maker() as session:
        client = await session.get(Client, client_id)
        assert client.phone_e164 == OTHER_PHONE and client.wa_opted_out
        assert client.easyweek_customer_uuid == uuid.UUID(CUSTOMER)
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert not await session.scalar(select(func.count()).select_from(Record))


async def test_r3_concurrent_two_branch_adoptions_and_manual_add_keep_one_card_per_branch(session_maker, monkeypatch):
    import asyncio

    from altegio_bot.campaigns import easyweek_manual_identity as manual

    first_id, event_id, payload = await seed(session_maker)
    configure_second_branch(monkeypatch)
    monkeypatch.setattr(manual, "KARLSRUHE_COMPANY_ID", TEST_LOCATION_ID)
    second_payload = dict(
        payload, uid=OTHER_UUID, location_id=OTHER_COMPANY, location_uuid="51515151-3434-4567-8901-121212121212"
    )
    async with session_maker() as session, session.begin():
        second_event = await _capture(
            session, second_payload, event_hint="booking-created", payload_hash="second-branch"
        )
    registry = identity.configured_easyweek_locations()

    class BranchReader(Reader):
        async def get_booking(self, booking_uuid):
            body = await super().get_booking(booking_uuid)
            body["location_uuid"] = self.payload["location_uuid"]
            return body

    async def resolve(event_id, payload):
        booking = worker.normalize_event(
            event_hint="booking-created", payload=payload, body_truncated=False, location_registry=registry.locations
        )
        return await identity.resolve_webhook_identity(
            session_maker,
            event_id=event_id,
            booking=booking,
            reader=BranchReader(payload, session_maker, check_lock=False),
        )

    async def manual_add():
        async with session_maker() as session, session.begin():
            client, reason = await manual.ensure_local_identity(
                session,
                company_id=TEST_LOCATION_ID,
                phone=PHONE,
                customer_uuid=CUSTOMER,
                first_name="Synthetic",
                assign_karlsruhe=True,
            )
            assert reason is None and client.id == first_id
            return True

    assert all(await asyncio.gather(resolve(event_id, payload), resolve(second_event, second_payload), manual_add()))
    assert await worker.process_one()
    assert await worker.process_one()
    async with session_maker() as session:
        cards = list((await session.scalars(select(Client).where(Client.provider == "easyweek"))).all())
        records = list((await session.scalars(select(Record))).all())
        assert len(cards) == len(records) == 2
        assert {card.company_id for card in cards} == {TEST_LOCATION_ID, OTHER_COMPANY}
        assert {card.easyweek_customer_uuid for card in cards} == {uuid.UUID(CUSTOMER)}
        assert all(
            next(card for card in cards if card.id == record.client_id).company_id == record.company_id
            for record in records
        )
