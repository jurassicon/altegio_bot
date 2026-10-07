"""PR-21 review A/B/C: partial events and obsolete contact snapshots."""

import uuid
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from altegio_bot import easyweek_uuid_identity as identity
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, OutboxMessage, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import TEST_CUSTOMER_ID, TEST_LOCATION_ID
from altegio_bot.tests.test_easyweek_inbox_worker_integration import _at, _capture, _future
from altegio_bot.tests.test_easyweek_uuid_identity_worker import CUSTOMER, PHONE, Reader, seed
from altegio_bot.tests.test_easyweek_uuid_identity_worker import configuration as configuration
from altegio_bot.workers import easyweek_inbox_worker as worker

B_UUID = "41414141-3434-4567-8901-121212121212"
B_PHONE = "+4915100000998"
C_PHONE = "+4915100000997"


async def established(session_maker, monkeypatch, *, bound=False):
    client_id, event_id, payload = await seed(session_maker)
    start = _future(days=3)
    payload = _at(payload, start)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True)
    async with session_maker() as session, session.begin():
        client = await session.get(Client, client_id)
        client.altegio_client_id = TEST_CUSTOMER_ID
        if not bound:
            client.easyweek_customer_uuid = None
            client.easyweek_identity_assigned_at = None
        (await session.get(EasyWeekEvent, event_id)).payload = payload
    assert await worker.process_one()
    async with session_maker() as session:
        record = await session.scalar(select(Record))
        jobs = await reminders(session)
        assert len(jobs) == 2 and all(job.status == "queued" for job in jobs)
        return client_id, record.id, payload, start, {job.id for job in jobs}


async def reminders(session):
    return list((await session.scalars(select(MessageJob).where(MessageJob.job_type.like("reminder_%")))).all())


def without_phone(payload):
    payload = dict(payload)
    payload.pop("customer_phone", None)
    payload.pop("customer_attributes.customer_phone", None)
    return payload


async def capture(session_maker, payload, hint="booking-updated", key="partial"):
    async with session_maker() as session, session.begin():
        return await _capture(session, payload, event_hint=hint, payload_hash=key)


@pytest.mark.parametrize("hint", ["booking-canceled", "booking-updated", "booking-rescheduled"])
async def test_a_unrelated_manual_identity_does_not_block_established_numeric_lifecycle(
    session_maker, monkeypatch, hint
):
    client_id, record_id, payload, start, job_ids = await established(session_maker, monkeypatch)
    async with session_maker() as session, session.begin():
        other = Client(
            provider="easyweek",
            company_id=TEST_LOCATION_ID,
            easyweek_customer_uuid=uuid.UUID(B_UUID),
            phone_e164=B_PHONE,
            display_name="Unrelated",
            wa_opted_out=True,
            wa_opt_out_reason="original",
        )
        session.add(other)
        await session.flush()
        other_id = other.id
    payload = without_phone(payload)
    payload["booking_comment"] = "changed comment"
    if hint == "booking-rescheduled":
        payload = _at(payload, start + timedelta(days=1))
    event_id = await capture(session_maker, payload, hint)

    def no_api():
        pytest.fail("An unrelated manual identity must not require contact API reads")

    monkeypatch.setattr(identity, "EasyWeekClient", no_api)
    assert await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        client = await session.get(Client, client_id)
        other = await session.get(Client, other_id)
        assert client.phone_e164 == PHONE and client.easyweek_customer_uuid is None
        assert other.altegio_client_id is None and other.phone_e164 == B_PHONE
        assert other.wa_opted_out and other.wa_opt_out_reason == "original" and other.display_name == "Unrelated"
        record = await session.get(Record, record_id)
        assert record.client_id == client_id
        jobs = await reminders(session)
        if hint == "booking-updated":
            assert {job.id for job in jobs if job.status == "queued"} == job_ids
        else:
            assert all(job.status == "canceled" for job in jobs if job.id in job_ids)
            assert len([job for job in jobs if job.status == "queued"]) == (2 if hint == "booking-rescheduled" else 0)
        assert record.is_deleted == (hint == "booking-canceled")
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 2
        assert await session.scalar(select(func.count()).select_from(Record)) == 1


@pytest.mark.parametrize("reschedule", [False, True])
async def test_b_partial_event_uses_the_stored_client_for_reminders(session_maker, monkeypatch, reschedule):
    client_id, record_id, payload, start, job_ids = await established(session_maker, monkeypatch)
    payload = without_phone(payload)
    payload.pop("customer_id")
    if reschedule:
        payload = _at(payload, start + timedelta(days=1))
    event_id = await capture(session_maker, payload, "booking-rescheduled" if reschedule else "booking-updated")
    assert await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        assert (await session.get(Record, record_id)).client_id == client_id
        assert (await session.get(Client, client_id)).phone_e164 == PHONE
        jobs = await reminders(session)
        queued = {job.id for job in jobs if job.status == "queued"}
        assert len(queued) == 2
        assert queued.isdisjoint(job_ids) if reschedule else queued == job_ids
        if reschedule:
            assert all(job.status == "canceled" for job in jobs if job.id in job_ids)
        count = await session.scalar(select(func.count()).select_from(MessageJob))
    await capture(session_maker, payload, "booking-rescheduled" if reschedule else "booking-updated")
    assert await worker.process_one()
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == count


class CurrentReader(Reader):
    async def get_customer(self, customer_uuid):
        return {"uuid": CUSTOMER, "phone": C_PHONE, "first_name": "Synthetic"}

    async def list_customers(self, *, params):
        rows = [await self.get_customer(CUSTOMER)] if params["phone"] == C_PHONE else []
        return {"data": rows, "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": len(rows)}}


@pytest.mark.parametrize("first_phone", [B_PHONE, ""])
@pytest.mark.parametrize("first_hint", ["booking-rescheduled", "booking-canceled"])
async def test_c_stale_contact_chain_applies_business_changes_in_order(
    session_maker, monkeypatch, first_phone, first_hint
):
    client_id, record_id, payload, start, job_ids = await established(session_maker, monkeypatch, bound=True)
    first = _at(payload, start + timedelta(days=1))
    first["customer_phone"] = first["customer_attributes.customer_phone"] = first_phone
    first_id = await capture(session_maker, first, first_hint, "old-contact")
    # The newer contact update carries no appointment times: only the earlier
    # event can apply its reschedule. Cancellation retains its terminal rule.
    second = dict(payload)
    for field in ("booking_date_start", "booking_date_start_tz", "booking_date_end", "booking_duration"):
        second.pop(field, None)
    second["customer_phone"] = second["customer_attributes.customer_phone"] = C_PHONE
    second_id = await capture(session_maker, second, key="current-contact")
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: CurrentReader(payload, session_maker, check_lock=False))
    for _ in range(6):
        await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, first_id)).status == "processed"
        assert (await session.get(EasyWeekEvent, second_id)).status == "processed"
        record = await session.get(Record, record_id)
        assert record.client_id == client_id and record.starts_at == start + timedelta(days=1)
        assert record.is_deleted == (first_hint == "booking-canceled")
        # Current contact proof already converges even if the later event is
        # terminal/no-op under the existing cancellation rule.
        assert (await session.get(Client, client_id)).phone_e164 == C_PHONE
        assert (await session.get(EasyWeekEvent, first_id)).payload == first
        assert await session.scalar(select(func.count()).select_from(Client).where(Client.provider == "easyweek")) == 1
        assert await session.scalar(select(func.count()).select_from(Record)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0
        jobs = await reminders(session)
        assert all(job.status == "canceled" for job in jobs if job.id in job_ids)
        assert len([job for job in jobs if job.status == "queued"]) == (0 if record.is_deleted else 2)


@pytest.mark.parametrize("conflict", ["phone", "numeric_uuid"])
async def test_a_real_identity_conflict_still_blocks_partial_lifecycle(session_maker, monkeypatch, conflict):
    client_id, record_id, payload, _, job_ids = await established(session_maker, monkeypatch)
    async with session_maker() as session, session.begin():
        session.add(
            Client(
                provider="easyweek",
                company_id=TEST_LOCATION_ID + 1,
                altegio_client_id=TEST_CUSTOMER_ID if conflict == "numeric_uuid" else None,
                easyweek_customer_uuid=uuid.UUID(B_UUID),
                phone_e164=PHONE,
            )
        )
    event_id = await capture(session_maker, without_phone(payload), "booking-canceled")
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: Reader(payload, session_maker, check_lock=False))
    assert not await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert not (await session.get(Record, record_id)).is_deleted
        assert (await session.get(Client, client_id)).phone_e164 == PHONE
        assert {job.id for job in await reminders(session) if job.status == "queued"} == job_ids


@pytest.mark.parametrize("provider,company", [("altegio", TEST_LOCATION_ID), ("easyweek", TEST_LOCATION_ID + 1)])
async def test_b_saved_client_link_must_match_provider_and_branch(session_maker, monkeypatch, provider, company):
    _, record_id, payload, _, job_ids = await established(session_maker, monkeypatch)
    async with session_maker() as session, session.begin():
        foreign = Client(
            provider=provider, company_id=company, altegio_client_id=TEST_CUSTOMER_ID + 123, phone_e164=B_PHONE
        )
        session.add(foreign)
        await session.flush()
        (await session.get(Record, record_id)).client_id = foreign.id
    payload = without_phone(payload)
    payload.pop("customer_id")
    event_id = await capture(session_maker, payload)
    assert await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "failed"
        assert {job.id for job in await reminders(session) if job.status == "queued"} == job_ids
        assert not await session.scalar(select(func.count()).select_from(OutboxMessage))


async def test_b_partial_cancel_with_sent_reminder_never_reopens_jobs(session_maker, monkeypatch):
    _, _, payload, _, job_ids = await established(session_maker, monkeypatch)
    sent_id = min(job_ids)
    async with session_maker() as session, session.begin():
        (await session.get(MessageJob, sent_id)).status = "sent"
    payload = without_phone(payload)
    payload.pop("customer_id")
    await capture(session_maker, payload, "booking-canceled")
    assert await worker.process_one()
    await capture(session_maker, payload, "booking-canceled")
    assert await worker.process_one()
    async with session_maker() as session:
        jobs = await reminders(session)
        assert {job.id for job in jobs} == job_ids
        assert (await session.get(MessageJob, sent_id)).status == "sent"
        assert all(job.status == "canceled" for job in jobs if job.id != sent_id)


async def test_c_absent_contact_after_stale_snapshot_keeps_proven_phone_and_optout_audit(session_maker, monkeypatch):
    from altegio_bot.utils import utcnow

    client_id, _, payload, _, _ = await established(session_maker, monkeypatch, bound=True)
    audit = utcnow()
    async with session_maker() as session, session.begin():
        client = await session.get(Client, client_id)
        client.wa_opted_out = True
        client.wa_opted_out_at = audit
        client.wa_opt_out_reason = "Synthetic audit"
    first = dict(payload)
    first["customer_phone"] = first["customer_attributes.customer_phone"] = B_PHONE
    ids = [
        await capture(session_maker, first, key="stale"),
        await capture(session_maker, without_phone(payload), key="absent"),
    ]
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: CurrentReader(payload, session_maker, check_lock=False))
    for _ in range(4):
        await worker.process_one()
    async with session_maker() as session:
        for event_id in ids:
            assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        client = await session.get(Client, client_id)
        assert client.phone_e164 == C_PHONE and client.wa_opted_out_at == audit
        assert client.wa_opted_out and client.wa_opt_out_reason == "Synthetic audit"


@pytest.mark.parametrize("failure", ["uuid", "phone_owner", "lookup_ambiguous", "api_unavailable"])
async def test_c_current_contact_requires_proof_and_never_touches_other_identities(session_maker, monkeypatch, failure):
    client_id, record_id, payload, start, job_ids = await established(session_maker, monkeypatch, bound=True)
    changed = _at(payload, start + timedelta(days=1))
    changed["customer_phone"] = changed["customer_attributes.customer_phone"] = B_PHONE
    event_id = await capture(session_maker, changed, "booking-rescheduled")
    if failure == "phone_owner":
        async with session_maker() as session, session.begin():
            session.add(
                Client(
                    provider="easyweek",
                    company_id=TEST_LOCATION_ID,
                    easyweek_customer_uuid=uuid.UUID(B_UUID),
                    phone_e164=C_PHONE,
                )
            )

    class FailingReader(CurrentReader):
        async def list_customers(self, *, params):
            body = await super().list_customers(params=params)
            if failure == "lookup_ambiguous":
                body["data"].append({"uuid": B_UUID, "phone": C_PHONE, "first_name": "Other"})
                body["meta"]["total"] = len(body["data"])
            return body

    monkeypatch.setattr(
        identity,
        "EasyWeekClient",
        lambda: FailingReader(
            payload, session_maker, check_lock=False, mismatch=failure == "uuid", error=failure == "api_unavailable"
        ),
    )
    assert not await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert (await session.get(Client, client_id)).phone_e164 == PHONE
        assert (await session.get(Record, record_id)).starts_at == start
        assert {job.id for job in await reminders(session) if job.status == "queued"} == job_ids


async def test_c_stale_contact_and_business_changes_rollback_together(session_maker, monkeypatch):
    client_id, record_id, payload, start, job_ids = await established(session_maker, monkeypatch, bound=True)
    changed = _at(payload, start + timedelta(days=1))
    changed["customer_phone"] = changed["customer_attributes.customer_phone"] = B_PHONE
    event_id = await capture(session_maker, changed, "booking-rescheduled")
    monkeypatch.setattr(identity, "EasyWeekClient", lambda: CurrentReader(payload, session_maker, check_lock=False))

    async def fail(*args, **kwargs):
        raise RuntimeError("synthetic apply failure")

    monkeypatch.setattr(worker, "sync_reminder_jobs", fail)
    assert await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "captured"
        assert (await session.get(Client, client_id)).phone_e164 == PHONE
        assert (await session.get(Record, record_id)).starts_at == start
        assert {job.id for job in await reminders(session) if job.status == "queued"} == job_ids


@pytest.mark.parametrize("missing", ["client", "phone"])
async def test_b_truly_missing_destination_withdraws_reminders(session_maker, monkeypatch, missing):
    client_id, record_id, payload, _, job_ids = await established(session_maker, monkeypatch)
    async with session_maker() as session, session.begin():
        if missing == "client":
            (await session.get(Record, record_id)).client_id = None
        else:
            (await session.get(Client, client_id)).phone_e164 = None
    payload = without_phone(payload)
    payload.pop("customer_id")
    event_id = await capture(session_maker, payload)
    assert await worker.process_one()
    async with session_maker() as session:
        assert (await session.get(EasyWeekEvent, event_id)).status == "processed"
        assert {job.id for job in await reminders(session)} == job_ids
        assert all(job.status == "canceled" for job in await reminders(session))
        assert not await session.scalar(select(func.count()).select_from(OutboxMessage))
