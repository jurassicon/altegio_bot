"""Consent projection must not cross a booking writer's marketing job locks."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest
import respx
from sqlalchemy import select, text

from altegio_bot import message_planner, service_filter
from altegio_bot.models.models import (
    CONSENT_SOURCE_INBOUND,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    AltegioEvent,
    Client,
    MessageJob,
    OutboxMessage,
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
from altegio_bot.workers import outbox_worker
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


async def _add_review_job(session_maker, scenario: Reassignment) -> int:
    async with session_maker() as session, session.begin():
        record = await session.get(Record, scenario.record)
        job = MessageJob(
            company_id=LASH_COMPANY,
            record_id=scenario.record,
            client_id=scenario.stable_card,
            job_type="review_3d",
            status="queued",
            run_at=record.starts_at + timedelta(days=3),
            dedupe_key="consent-jobs:original-review",
            payload={"kind": "review_3d"},
        )
        session.add(job)
        await session.flush()
        return job.id


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
    review_job = await _add_review_job(session_maker, scenario)
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
        assert (await session.get(MessageJob, review_job)).status == "canceled"
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


@pytest.mark.parametrize("move_first_job", [False, True], ids=["original-tuples", "updated-first-tuple"])
async def test_record_reassignment_and_stop_order_all_marketing_job_locks(
    session_maker, monkeypatch, move_first_job
) -> None:
    """Both real workers must finish even when tuple order differs from ids.

    The trigger only pauses the writer after it locks the second job. The
    barrier is released before the workers finish, so it is not part of the
    potential cycle. On an unordered UPDATE, the moved first tuple is reached
    after the second, opposite to the projection's ascending-id selection.
    """
    scenario = await _reassignment(session_maker)
    review_job = await _add_review_job(session_maker, scenario)
    assert scenario.job < review_job
    if move_first_job:
        async with session_maker() as session, session.begin():
            await session.execute(
                text("UPDATE message_jobs SET run_at=run_at + interval '1 second' WHERE id=:id"),
                {"id": scenario.job},
            )
    barrier_key = 938100
    async with session_maker() as session, session.begin():
        await session.execute(
            text(f"""
                CREATE FUNCTION consent_test_pause_reschedule() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.id={review_job} AND NEW.status='canceled'
                     AND NEW.last_error='Canceled: rescheduled' THEN
                    PERFORM pg_advisory_xact_lock({barrier_key});
                  END IF;
                  RETURN NEW;
                END $$
            """)
        )
        await session.execute(
            text("""
                CREATE TRIGGER consent_test_pause_reschedule BEFORE UPDATE ON message_jobs
                FOR EACH ROW EXECUTE FUNCTION consent_test_pause_reschedule()
            """)
        )

    cards_locked = asyncio.Event()
    resume_projection = asyncio.Event()
    writer_started = asyncio.Event()
    projection_pid = None
    writer_pid = None
    real_card_read = wa_worker._card_ids_matching
    real_handle = altegio_worker.handle_event

    async def after_card_read(session, phones, *, lock):
        nonlocal projection_pid
        ids = await real_card_read(session, phones, lock=lock)
        if lock:
            assert ids == {scenario.stable_card}
            projection_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            cards_locked.set()
            await resume_projection.wait()
        return ids

    async def capture_writer(session, event):
        nonlocal writer_pid
        writer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
        writer_started.set()
        return await real_handle(session, event)

    monkeypatch.setattr(wa_worker, "SessionLocal", session_maker)
    monkeypatch.setattr(altegio_worker, "SessionLocal", session_maker)
    monkeypatch.setattr(wa_worker, "_card_ids_matching", after_card_read)
    monkeypatch.setattr(altegio_worker, "handle_event", capture_writer)
    provider = _CaptureProvider()
    tasks = []
    try:
        await _seed_sender(session_maker)
        stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)
        async with session_maker() as session, session.begin():
            assert [event.id for event in await wa_worker.lock_next_batch(session, 50)] == [stop_event]
            assert [event.id for event in await altegio_worker.lock_next_batch(session, 50)] == [scenario.event]
        async with session_maker() as barrier, barrier.begin():
            await barrier.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": barrier_key})
            tasks.append(asyncio.create_task(wa_worker.process_one_event(stop_event, provider)))
            await asyncio.wait_for(cards_locked.wait(), 10)
            tasks.append(asyncio.create_task(altegio_worker.process_one_event(scenario.event)))
            await asyncio.wait_for(writer_started.wait(), 10)
            await _wait_for_sql_lock(session_maker, writer_pid)
            async with session_maker() as session:
                assert await session.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted AND locktype='advisory')"
                    ),
                    {"pid": writer_pid},
                )
            resume_projection.set()
            await _wait_for_sql_lock(session_maker, projection_pid)
        await asyncio.wait_for(asyncio.gather(*tasks), 20)
        await _assert_finished(session_maker, scenario, stop_event)
        async with session_maker() as session:
            assert (await session.get(MessageJob, review_job)).status == "canceled"
        async with session_maker() as session, session.begin():
            assert list(await altegio_worker.lock_next_batch(session, 50)) == []
        assert len(provider.sent) == 1
        assert "abgemeldet" in provider.sent[0][2]
    finally:
        resume_projection.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        async with session_maker() as session, session.begin():
            await session.execute(text("DROP TRIGGER consent_test_pause_reschedule ON message_jobs"))
            await session.execute(text("DROP FUNCTION consent_test_pause_reschedule()"))


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


async def test_record_cancellation_keeps_its_full_system_scope(session_maker) -> None:
    scenario = await _reassignment(session_maker)
    system_types = (
        "record_created",
        "record_updated",
        "record_canceled",
        "reminder_24h",
        "reminder_2h",
        "review_3d",
        "repeat_10d",
        "comeback_3d",
    )
    async with session_maker() as session, session.begin():
        original = await session.get(MessageJob, scenario.job)
        other_record = Record(
            company_id=LASH_COMPANY,
            altegio_record_id=712,
            client_id=scenario.stable_card,
            starts_at=original.run_at,
            raw={},
        )
        session.add(other_record)
        await session.flush()
        matching = [original]
        untouched = []

        def job(label, *, kind="review_3d", status="queued", company=LASH_COMPANY, record=scenario.record):
            return MessageJob(
                company_id=company,
                record_id=record,
                client_id=scenario.stable_card,
                job_type=kind,
                status=status,
                run_at=original.run_at,
                dedupe_key=f"consent-jobs:scope:{label}",
                payload={"kind": kind},
                last_error="original reason",
            )

        for kind in system_types:
            if kind != original.job_type:
                matching.append(job(kind, kind=kind))
        untouched.extend(
            [
                job("other-record", record=other_record.id),
                job("other-company", company=LASH_COMPANY + 1),
                job("other-kind", kind="custom_marketing"),
                *(job(state, status=state) for state in ("running", "done", "failed", "canceled")),
            ]
        )
        session.add_all(matching[1:] + untouched)
        await session.flush()
        expected_untouched = {row.id: (row.status, row.last_error) for row in untouched}
        matching_ids = [row.id for row in matching]

    async with session_maker() as session, session.begin():
        changed = await message_planner.cancel_queued_jobs(
            session, company_id=LASH_COMPANY, record_id=scenario.record, reason="Canceled: rescheduled"
        )
        assert changed == len(system_types)
    async with session_maker() as session:
        for job_id in matching_ids:
            row = await session.get(MessageJob, job_id)
            assert row.status == "canceled"
            assert row.last_error == "Canceled: rescheduled"
        for job_id, expected in expected_untouched.items():
            row = await session.get(MessageJob, job_id)
            assert (row.status, row.last_error) == expected


async def test_record_cancellation_does_not_expand_an_empty_locked_set(session_maker) -> None:
    scenario = await _reassignment(session_maker)
    async with session_maker() as session, session.begin():
        assert (
            await message_planner.cancel_queued_jobs(
                session,
                company_id=LASH_COMPANY,
                record_id=scenario.record,
                reason="Canceled: rescheduled",
                locked_job_ids=set(),
            )
            == 0
        )
    async with session_maker() as session:
        assert (await session.get(MessageJob, scenario.job)).status == "queued"


@pytest.mark.parametrize("status", ["canceled", "failed", "queued", "running", "done"])
async def test_create_planning_preserves_dedupe_and_resurrection_rules(session_maker, status) -> None:
    scenario = await _reassignment(session_maker)
    async with session_maker() as session, session.begin():
        original = await session.get(MessageJob, scenario.job)
        original.dedupe_key = message_planner.make_dedupe_key(
            job_type=original.job_type,
            company_id=original.company_id,
            record_id=original.record_id,
            run_at=original.run_at,
        )
        key = original.dedupe_key
        original.status = status
        original.payload = {"kind": "repeat_10d", "previous": True}
        original.last_error = "previous reason"
    async with session_maker() as session, session.begin():
        await message_planner.plan_jobs_for_record_event(
            session, record_id=scenario.record, client_id=scenario.stable_card, event_status="create"
        )
    async with session_maker() as session:
        matching = list((await session.scalars(select(MessageJob).where(MessageJob.dedupe_key == key))).all())
        assert len(matching) == 1
        row = matching[0]
        assert row.id == scenario.job
        if status in ("canceled", "failed"):
            assert row.status == "queued"
            assert row.payload == {"kind": "repeat_10d"}
            assert row.last_error is None
        else:
            assert row.status == status
            assert row.payload == {"kind": "repeat_10d", "previous": True}
            assert row.last_error == "previous reason"


async def test_reassignment_and_stop_cover_a_canceled_dedupe_row_before_resurrection(
    session_maker, monkeypatch
) -> None:
    """A lower-id canceled job can become queued before a later dedupe write.

    The booking writer must include it in its initial order, even though its
    cancellation does not change that row. A second real add_job call races
    with planning and can otherwise expose it to STOP after the writer already
    holds a higher queued job.
    """
    scenario = await _reassignment(session_maker)
    review_job = await _add_review_job(session_maker, scenario)
    async with session_maker() as session, session.begin():
        event = await session.get(AltegioEvent, scenario.event)
        target = datetime.fromisoformat(event.payload["data"]["date"]) + timedelta(days=10)
        original = await session.get(MessageJob, scenario.job)
        original.run_at = target
        original.status = "canceled"
        original.dedupe_key = message_planner.make_dedupe_key(
            job_type="repeat_10d", company_id=LASH_COMPANY, record_id=scenario.record, run_at=target
        )
        dedupe_key = original.dedupe_key
    await _seed_sender(session_maker)
    stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)
    async with session_maker() as session, session.begin():
        await wa_worker.lock_next_batch(session, 50)
        await altegio_worker.lock_next_batch(session, 50)
    monkeypatch.setattr(wa_worker, "SessionLocal", session_maker)
    monkeypatch.setattr(altegio_worker, "SessionLocal", session_maker)
    cards_locked = asyncio.Event()
    resume_projection = asyncio.Event()
    writer_ready = asyncio.Event()
    resume_writer = asyncio.Event()
    resurrection_started = asyncio.Event()
    projection_pid = None
    resurrection_pid = None
    real_card_read = wa_worker._card_ids_matching
    real_cancel = message_planner.cancel_queued_jobs

    async def after_card_read(session, phones, *, lock):
        nonlocal projection_pid
        ids = await real_card_read(session, phones, lock=lock)
        if lock:
            projection_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            cards_locked.set()
            await resume_projection.wait()
        return ids

    async def after_cancel(session, **kwargs):
        result = await real_cancel(session, **kwargs)
        writer_ready.set()
        await resume_writer.wait()
        return result

    async def resurrect():
        nonlocal resurrection_pid
        async with session_maker() as session, session.begin():
            resurrection_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            resurrection_started.set()
            await message_planner.add_job(
                session,
                company_id=LASH_COMPANY,
                record_id=scenario.record,
                client_id=scenario.stable_card,
                job_type="repeat_10d",
                run_at=target,
                payload={"kind": "repeat_10d"},
            )

    monkeypatch.setattr(wa_worker, "_card_ids_matching", after_card_read)
    monkeypatch.setattr(message_planner, "cancel_queued_jobs", after_cancel)
    provider = _CaptureProvider()
    tasks = [asyncio.create_task(wa_worker.process_one_event(stop_event, provider))]
    try:
        await asyncio.wait_for(cards_locked.wait(), 10)
        tasks.append(asyncio.create_task(altegio_worker.process_one_event(scenario.event)))
        await asyncio.wait_for(writer_ready.wait(), 10)
        resurrection = asyncio.create_task(resurrect())
        tasks.append(resurrection)
        await asyncio.wait_for(resurrection_started.wait(), 10)
        deadline = asyncio.get_running_loop().time() + 10
        while not resurrection.done():
            async with session_maker() as session:
                waiting = await session.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted)"),
                    {"pid": resurrection_pid},
                )
            if waiting:
                break
            assert asyncio.get_running_loop().time() < deadline, "the dedupe write neither waited nor finished"
            await asyncio.sleep(0.01)
        if resurrection.done():
            resurrection.result()
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

    async with session_maker() as session:
        for card_id in (scenario.stable_card, scenario.moved_card):
            assert _marketing_verdict(await session.get(Client, card_id)) == "opted_out"
        assert (await session.get(MessageJob, review_job)).status == "canceled"
        assert list(
            (await session.scalars(select(MessageJob.id).where(MessageJob.dedupe_key == dedupe_key))).all()
        ) == [scenario.job]
    # Jobs made queued after the projection's selection are protected by the
    # saved card veto. Local projection can cancel them without replaying ACK.
    await wa_worker.project_inbound_commands(session_maker, event_id=stop_event)
    await _assert_finished(session_maker, scenario, stop_event)
    repeated = await wa_worker.project_inbound_commands(session_maker, event_id=stop_event)
    assert repeated[ALTEGIO_PHONE].jobs_canceled == 0
    assert len(provider.sent) == 1


async def test_running_job_can_save_its_outbox_audit_while_stop_and_planning_wait(session_maker, monkeypatch) -> None:
    """A running job must be able to finish its foreign-key-linked audit.

    STOP owns C and waits for the planner's queued job. The planner waits for a
    running job, whose Outbox row refers back to C. Saving that audit needs a
    key-share card lock, not permission to overwrite the consent flags.
    """
    scenario = await _reassignment(session_maker)
    running_id = await _add_review_job(session_maker, scenario)
    async with session_maker() as session, session.begin():
        running = await session.get(MessageJob, running_id)
        running.status = "running"
    await _seed_sender(session_maker)
    stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)
    async with session_maker() as session, session.begin():
        assert [event.id for event in await wa_worker.lock_next_batch(session, 50)] == [stop_event]
        assert [event.id for event in await altegio_worker.lock_next_batch(session, 50)] == [scenario.event]
    monkeypatch.setattr(wa_worker, "SessionLocal", session_maker)
    monkeypatch.setattr(altegio_worker, "SessionLocal", session_maker)
    cards_locked = asyncio.Event()
    resume_projection = asyncio.Event()
    running_locked = asyncio.Event()
    finish_outbox = asyncio.Event()
    writer_started = asyncio.Event()
    projection_pid = None
    writer_pid = None
    real_card_read = wa_worker._card_ids_matching
    real_handle = altegio_worker.handle_event

    async def after_card_read(session, phones, *, lock):
        nonlocal projection_pid
        ids = await real_card_read(session, phones, lock=lock)
        if lock:
            assert ids == {scenario.stable_card}
            projection_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            cards_locked.set()
            await resume_projection.wait()
        return ids

    async def capture_writer(session, event):
        nonlocal writer_pid
        writer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
        writer_started.set()
        return await real_handle(session, event)

    async def finish_running_job():
        async with session_maker() as session, session.begin():
            running = await outbox_worker._load_job(session, running_id)
            assert running.status == "running"
            running_locked.set()
            await finish_outbox.wait()
            session.add(
                OutboxMessage(
                    company_id=LASH_COMPANY,
                    client_id=scenario.stable_card,
                    record_id=scenario.record,
                    job_id=running.id,
                    phone_e164=ALTEGIO_PHONE,
                    template_code="review_3d",
                    language="de",
                    body="synthetic audit",
                    status="failed",
                    error="synthetic provider refusal",
                    scheduled_at=running.run_at,
                    sent_at=datetime.now(timezone.utc),
                )
            )
            await session.flush()
            running.status = "failed"

    monkeypatch.setattr(wa_worker, "_card_ids_matching", after_card_read)
    monkeypatch.setattr(altegio_worker, "handle_event", capture_writer)
    provider = _CaptureProvider()
    tasks = [asyncio.create_task(finish_running_job())]
    try:
        await asyncio.wait_for(running_locked.wait(), 10)
        tasks.append(asyncio.create_task(wa_worker.process_one_event(stop_event, provider)))
        await asyncio.wait_for(cards_locked.wait(), 10)
        tasks.append(asyncio.create_task(altegio_worker.process_one_event(scenario.event)))
        await asyncio.wait_for(writer_started.wait(), 10)
        await _wait_for_sql_lock(session_maker, writer_pid)
        resume_projection.set()
        await _wait_for_sql_lock(session_maker, projection_pid)
        finish_outbox.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 20)
    finally:
        resume_projection.set()
        finish_outbox.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await _assert_finished(session_maker, scenario, stop_event)
    async with session_maker() as session:
        assert (await session.get(MessageJob, running_id)).status == "failed"
        audits = list((await session.scalars(select(OutboxMessage).where(OutboxMessage.job_id == running_id))).all())
        assert len(audits) == 1
        assert audits[0].status == "failed"
        assert audits[0].client_id == scenario.stable_card
    assert len(provider.sent) == 1


async def test_projection_card_lock_still_blocks_a_phone_writer(session_maker) -> None:
    """Allowing an audit FK must not let a phone update pass a held card."""
    scenario = await _reassignment(session_maker)
    writer_started = asyncio.Event()
    writer_pid = None
    new_phone = "+491600000097"

    async def move_phone():
        nonlocal writer_pid
        async with session_maker() as session, session.begin():
            writer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            writer_started.set()
            await altegio_worker.upsert_client(session, LASH_COMPANY, {"id": 71, "phone": new_phone})
            await altegio_worker.settle_card_writes(session)

    writer = None
    try:
        async with session_maker() as projection, projection.begin():
            assert await wa_worker._card_ids_matching(projection, [ALTEGIO_PHONE], lock=True) == {scenario.stable_card}
            writer = asyncio.create_task(move_phone())
            await asyncio.wait_for(writer_started.wait(), 10)
            await _wait_for_sql_lock(session_maker, writer_pid)
            assert not writer.done()
            assert (await projection.get(Client, scenario.stable_card)).phone_e164 == ALTEGIO_PHONE
        await asyncio.wait_for(writer, 10)
    finally:
        if writer is not None and not writer.done():
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
    async with session_maker() as session:
        assert (await session.get(Client, scenario.stable_card)).phone_e164 == new_phone
