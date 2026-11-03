"""Consent projection must not cross a booking writer's marketing job locks."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest
import respx
from sqlalchemy import select, text

from altegio_bot import service_filter
from altegio_bot.models.models import (
    CONSENT_SOURCE_INBOUND,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    AltegioEvent,
    Client,
    MessageJob,
    Record,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
)
from altegio_bot.settings import settings
from altegio_bot.tests.test_whatsapp_consent_card_locks import (
    ALTEGIO_PHONE,
    LASH_COMPANY,
    _CaptureProvider,
    _ingest_stop,
    _marketing_verdict,
    _seed_sender,
)
from altegio_bot.whatsapp_consent import (
    apply_consent_decision,
    apply_inbound_consent,
    lock_consent_address,
)
from altegio_bot.workers import inbox_worker as altegio_worker
from altegio_bot.workers import whatsapp_inbox_worker as wa_worker


@pytest.fixture(autouse=True)
def isolated_http(monkeypatch):
    """Every external request is intercepted, including visit-count lookups."""
    base = "https://altegio.test.invalid/api/v1"
    monkeypatch.setattr(settings, "altegio_api_base_url", base)
    monkeypatch.setattr(settings, "altegio_partner_token", "synthetic-partner")
    monkeypatch.setattr(settings, "altegio_user_token", "synthetic-user")
    monkeypatch.setattr(settings, "chatwoot_enabled", False)
    monkeypatch.setattr(settings, "promo_apply_discount_enabled", False)
    monkeypatch.setenv("ALTEGIO_API_BASE_URL", base)
    monkeypatch.setenv("ALTEGIO_PARTNER_TOKEN", "synthetic-partner")
    monkeypatch.setenv("ALTEGIO_USER_TOKEN", "synthetic-user")
    saved_cache = service_filter._LRU_CACHE.copy()
    service_filter._LRU_CACHE.clear()
    try:
        with respx.mock(assert_all_mocked=True, assert_all_called=False) as http:
            http.get(f"{base}/company/{LASH_COMPANY}/services/111").respond(
                200,
                json={"data": {"category_id": min(service_filter.LASH_CATEGORY_IDS_BY_COMPANY[LASH_COMPANY])}},
            )
            http.get(f"{base}/records/{LASH_COMPANY}").respond(200, json={"data": []})
            yield http
    finally:
        service_filter._LRU_CACHE.clear()
        service_filter._LRU_CACHE.update(saved_cache)


@dataclass(frozen=True)
class Reassignment:
    stable_card: int
    moved_card: int
    record: int
    job: int
    event: int


async def _reassignment(session_maker) -> Reassignment:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    async with session_maker() as session, session.begin():
        stable = Client(company_id=LASH_COMPANY, altegio_client_id=71, phone_e164=ALTEGIO_PHONE, raw={})
        moved = Client(company_id=LASH_COMPANY, altegio_client_id=72, phone_e164="+491600000098", raw={})
        session.add_all([stable, moved])
        await session.flush()
        record = Record(
            company_id=LASH_COMPANY,
            altegio_record_id=711,
            client_id=stable.id,
            starts_at=now - timedelta(days=3),
            raw={},
        )
        session.add(record)
        await session.flush()
        job = MessageJob(
            company_id=LASH_COMPANY,
            record_id=record.id,
            client_id=stable.id,
            job_type="repeat_10d",
            status="queued",
            run_at=now + timedelta(days=7),
            dedupe_key="consent-jobs:original-repeat",
            payload={"kind": "repeat_10d"},
        )
        event = AltegioEvent(
            company_id=LASH_COMPANY,
            resource="record",
            resource_id=711,
            event_status="update",
            status="received",
            dedupe_key="consent-jobs:reassignment",
            payload={
                "company_id": LASH_COMPANY,
                "resource": "record",
                "status": "update",
                "data": {
                    "id": 711,
                    "client": {"id": 72, "name": "Moved", "phone": ALTEGIO_PHONE},
                    "services": [{"id": 111, "title": "Allowed"}],
                    "date": (now - timedelta(days=2)).isoformat(),
                    "staff_id": 5,
                },
            },
        )
        session.add_all([job, event])
        await session.flush()
        return Reassignment(stable.id, moved.id, record.id, job.id, event.id)


async def _wait_for_sql_lock(session_maker, pid: int) -> None:
    deadline = asyncio.get_running_loop().time() + 10
    while True:
        async with session_maker() as session:
            waiting = await session.scalar(
                text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted)"), {"pid": pid}
            )
        if waiting:
            return
        assert asyncio.get_running_loop().time() < deadline, "the projection did not reach the held job"
        await asyncio.sleep(0.01)


async def _assert_finished(session_maker, scenario: Reassignment, stop_event: int) -> None:
    async with session_maker() as session:
        for card_id in (scenario.stable_card, scenario.moved_card):
            card = await session.get(Client, card_id)
            assert card.phone_e164 == ALTEGIO_PHONE
            assert card.wa_opted_out is True
            assert _marketing_verdict(card) == "opted_out"
        assert (await session.get(Record, scenario.record)).client_id == scenario.moved_card
        assert (await session.get(MessageJob, scenario.job)).status == "canceled"
        assert (await session.get(AltegioEvent, scenario.event)).status == "processed"
        event = await session.get(WhatsAppEvent, stop_event)
        assert event.status == "processed"
        assert event.error is None
        receipts = list(
            (
                await session.scalars(
                    select(WhatsAppInboundReceipt).where(WhatsAppInboundReceipt.whatsapp_event_id == stop_event)
                )
            ).all()
        )
        assert len(receipts) == 1
        assert receipts[0].resolved_at is not None and receipts[0].resolution == "applied"
        refusal = await session.scalar(
            select(WhatsAppConsentSuppression).where(WhatsAppConsentSuppression.wa_address == ALTEGIO_PHONE)
        )
        assert refusal.state == "opted_out"
    async with session_maker() as session, session.begin():
        assert list(await wa_worker.lock_next_batch(session, 50)) == []


async def test_record_reassignment_and_claimed_stop_finish_without_a_job_consent_cycle(
    session_maker, monkeypatch
) -> None:
    """Real booking writes hold a job that the STOP projection must cancel.

    C is locked by the projection before B moves onto its phone. The booking
    planner cancels C's old job, then reaches its final consent section. A
    projection taking consent before that job would close the opposite edge.
    """
    scenario = await _reassignment(session_maker)
    await _seed_sender(session_maker)
    stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)
    async with session_maker() as session, session.begin():
        assert [event.id for event in await wa_worker.lock_next_batch(session, 50)] == [stop_event]
    monkeypatch.setattr(wa_worker, "SessionLocal", session_maker)
    monkeypatch.setattr(altegio_worker, "SessionLocal", session_maker)
    card_read = asyncio.Event()
    resume_projection = asyncio.Event()
    writer_ready = asyncio.Event()
    resume_writer = asyncio.Event()
    projection_pid = None
    real_card_read = wa_worker._card_ids_matching
    real_settle = altegio_worker.settle_card_writes

    async def after_card_read(session, phones, *, lock):
        nonlocal projection_pid
        ids = await real_card_read(session, phones, lock=lock)
        if lock:
            assert ids == {scenario.stable_card}
            projection_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            card_read.set()
            await resume_projection.wait()
        return ids

    async def before_writer_consent(session, *args, **kwargs):
        assert await session.scalar(select(MessageJob.status).where(MessageJob.id == scenario.job)) == "canceled"
        writer_ready.set()
        await resume_writer.wait()
        return await real_settle(session, *args, **kwargs)

    monkeypatch.setattr(wa_worker, "_card_ids_matching", after_card_read)
    monkeypatch.setattr(altegio_worker, "settle_card_writes", before_writer_consent)
    provider = _CaptureProvider()
    tasks = [asyncio.create_task(wa_worker.process_one_event(stop_event, provider))]
    try:
        await asyncio.wait_for(card_read.wait(), 10)
        tasks.append(asyncio.create_task(altegio_worker.process_one_event(scenario.event)))
        await asyncio.wait_for(writer_ready.wait(), 10)
        resume_projection.set()
        await _wait_for_sql_lock(session_maker, projection_pid)
        resume_writer.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 20)
    finally:
        resume_projection.set()
        resume_writer.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await _assert_finished(session_maker, scenario, stop_event)
    assert len(provider.sent) == 1
    assert "abgemeldet" in provider.sent[0][2]


async def test_settled_stop_projection_is_idempotent_after_ordinary_reassignment(session_maker, monkeypatch) -> None:
    scenario = await _reassignment(session_maker)
    await _seed_sender(session_maker)
    monkeypatch.setattr(wa_worker, "SessionLocal", session_maker)
    monkeypatch.setattr(altegio_worker, "SessionLocal", session_maker)
    await altegio_worker.process_one_event(scenario.event)
    stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)
    async with session_maker() as session, session.begin():
        await wa_worker.lock_next_batch(session, 50)
    provider = _CaptureProvider()
    await wa_worker.process_one_event(stop_event, provider)
    await _assert_finished(session_maker, scenario, stop_event)

    async with session_maker() as session:
        before = await session.scalar(select(WhatsAppConsentSuppression.consent_version))
        jobs_before = list(
            (await session.execute(select(MessageJob.id, MessageJob.status).order_by(MessageJob.id))).all()
        )
    await apply_inbound_consent(session_maker, event_id=stop_event)
    repeated = await wa_worker.project_inbound_commands(session_maker, event_id=stop_event)
    assert repeated[ALTEGIO_PHONE].jobs_canceled == 0
    async with session_maker() as session:
        assert await session.scalar(select(WhatsAppConsentSuppression.consent_version)) == before
        assert (
            list((await session.execute(select(MessageJob.id, MessageJob.status).order_by(MessageJob.id))).all())
            == jobs_before
        )
    assert len(provider.sent) == 1, "local projection recovery must not replay acknowledgements"


@pytest.mark.parametrize("proven_start", [True, False])
async def test_cancellation_uses_the_ledger_after_waiting_for_jobs(session_maker, monkeypatch, proven_start) -> None:
    """A job lock is not permission to decide consent from an earlier read."""
    scenario = await _reassignment(session_maker)
    moment = datetime.now(timezone.utc) - timedelta(minutes=2)
    async with session_maker() as session, session.begin():
        await lock_consent_address(session, ALTEGIO_PHONE)
        outcome = await apply_consent_decision(
            session,
            wa_address=ALTEGIO_PHONE,
            command="stop",
            authenticity=INBOUND_AUTHENTICITY_SIGNED,
            source=CONSENT_SOURCE_INBOUND,
            observed_at=moment,
            event_at=moment,
        )
        assert outcome.applied
    real_card_read = wa_worker._card_ids_matching
    card_read = asyncio.Event()
    projection_pid = None

    async def after_card_read(session, phones, *, lock):
        nonlocal projection_pid
        ids = await real_card_read(session, phones, lock=lock)
        if lock:
            projection_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            card_read.set()
        return ids

    monkeypatch.setattr(wa_worker, "_card_ids_matching", after_card_read)

    async def project():
        async with session_maker() as session, session.begin():
            return await wa_worker._apply_inbound_command_flags(
                session, [(ALTEGIO_PHONE, "stop" if proven_start else "start")], serialised=True
            )

    task = None
    try:
        async with session_maker() as holder, holder.begin():
            await holder.get(MessageJob, scenario.job, with_for_update=True)
            task = asyncio.create_task(project())
            await asyncio.wait_for(card_read.wait(), 10)
            await _wait_for_sql_lock(session_maker, projection_pid)
            async with session_maker() as decision, decision.begin():
                await decision.execute(text("SET LOCAL lock_timeout = '1000ms'"))
                await lock_consent_address(decision, ALTEGIO_PHONE)
                outcome = await apply_consent_decision(
                    decision,
                    wa_address=ALTEGIO_PHONE,
                    command="start",
                    authenticity=INBOUND_AUTHENTICITY_SIGNED if proven_start else INBOUND_AUTHENTICITY_UNVERIFIED,
                    source=CONSENT_SOURCE_INBOUND,
                    observed_at=moment + timedelta(minutes=1),
                    event_at=moment + timedelta(minutes=1),
                )
                assert outcome.applied is proven_start
        projected = (await asyncio.wait_for(task, 10))[ALTEGIO_PHONE]
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with session_maker() as session:
        assert (await session.get(MessageJob, scenario.job)).status == ("queued" if proven_start else "canceled")
        assert (await session.get(Client, scenario.stable_card)).wa_opted_out is (not proven_start)
    assert projected.jobs_canceled == (0 if proven_start else 1)
