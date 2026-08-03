"""PostgreSQL contract for the EasyWeek inbox worker (PR-4).

Exercises the real transactional lifecycle against a real database: the claim,
the provider-scoped upserts, the UUID-first identity, the fail-closed gates and
the guarantee that none of it disturbs the Altegio path.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.easyweek_normalizer import NormalizationError
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, Record, RecordService
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    FOREIGN_LOCATION_ID,
    TEST_BOOKING_HASH_ID,
    TEST_BOOKING_ID,
    TEST_BOOKING_PAGE,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    booking_canceled,
    booking_created,
    booking_created_multi_service,
    booking_created_resend,
    booking_rescheduled,
    booking_updated,
)
from altegio_bot.workers import easyweek_inbox_worker as worker

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _enable_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Processing on, notifications OFF — the production default for PR-4."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_location_id", TEST_LOCATION_ID, raising=False)


@pytest_asyncio.fixture
async def bound_session_local(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> async_sessionmaker[AsyncSession]:
    """Point the worker's module-global SessionLocal at the test database."""
    monkeypatch.setattr(app_db, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(worker, "SessionLocal", session_maker, raising=False)
    return session_maker


async def _capture(
    session: AsyncSession,
    payload: dict[str, Any],
    *,
    event_hint: str = "booking-created",
    payload_hash: str = "hash-1",
    truncated: bool = False,
) -> int:
    event = EasyWeekEvent(
        status="captured",
        event_hint=event_hint,
        auth_via="query",
        payload_hash=payload_hash,
        payload=payload,
        body_truncated=truncated,
    )
    session.add(event)
    await session.flush()
    return int(event.id)


async def _run_until_idle(limit: int = 20) -> int:
    processed = 0
    for _ in range(limit):
        if not await worker.process_one():
            break
        processed += 1
    return processed


async def _counts(session: AsyncSession) -> tuple[int, int, int]:
    """Count only EasyWeek-owned rows.

    conftest seeds two baseline Altegio clients, so a bare table count would
    never be zero and would hide exactly the leaks these tests look for.
    """
    records = (
        await session.execute(select(func.count()).select_from(Record).where(Record.provider == "easyweek"))
    ).scalar_one()
    clients = (
        await session.execute(select(func.count()).select_from(Client).where(Client.provider == "easyweek"))
    ).scalar_one()
    jobs = (
        await session.execute(select(func.count()).select_from(MessageJob).where(MessageJob.provider == "easyweek"))
    ).scalar_one()
    return int(records), int(clients), int(jobs)


# ===========================================================================
# Full lifecycle collapses onto ONE record
# ===========================================================================


async def test_create_update_rescheduled_cancel_touch_one_record(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), event_hint="booking-created", payload_hash="h1")
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="h2")
            await _capture(session, booking_rescheduled(), event_hint="booking-rescheduled", payload_hash="h3")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h4")

    assert await _run_until_idle() == 4

    async with bound_session_local() as session:
        records = list((await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().all())
        assert len(records) == 1, "the lifecycle must collapse onto a single Record"
        record = records[0]
        assert record.provider == "easyweek"
        assert record.company_id == TEST_LOCATION_ID
        assert record.easyweek_booking_uuid == uuid.UUID(TEST_BOOKING_UUID)
        assert record.altegio_record_id == TEST_BOOKING_ID
        assert record.is_deleted is True, "cancel must mark the record deleted"

        clients = list((await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().all())
        assert len(clients) == 1
        assert clients[0].provider == "easyweek"
        assert clients[0].altegio_client_id == TEST_CUSTOMER_ID

        statuses = list((await session.execute(select(EasyWeekEvent.status))).scalars().all())
        assert statuses == ["processed"] * 4


async def test_reschedule_moves_the_stored_start(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        before = (await session.execute(select(Record.starts_at).where(Record.provider == "easyweek"))).scalar_one()

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_rescheduled(), event_hint="booking-rescheduled", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        after = (await session.execute(select(Record.starts_at).where(Record.provider == "easyweek"))).scalar_one()
    assert after > before


async def test_manage_link_and_hash_are_stored_from_the_proven_pair(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.short_link == TEST_BOOKING_PAGE
        assert record.easyweek_booking_hash_id == TEST_BOOKING_HASH_ID


async def test_unproven_new_hash_clears_the_stored_link(bound_session_local) -> None:
    """A stale link must never sit next to an unproven new hash."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    broken = booking_updated()
    broken["booking_hash_id"] = "90000999"  # page still points at the old hash
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, broken, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.short_link is None
        assert record.easyweek_booking_hash_id is None


async def test_event_without_link_fields_keeps_the_last_proven_link(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    silent = booking_updated()
    del silent["booking_page"]
    del silent["booking_hash_id"]
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.short_link == TEST_BOOKING_PAGE
        assert record.easyweek_booking_hash_id == TEST_BOOKING_HASH_ID


# ===========================================================================
# Idempotency: Resend
# ===========================================================================


async def test_exact_resend_gives_two_terminal_events_but_one_domain_result(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="same-hash")
            await _capture(session, booking_created_resend(), payload_hash="same-hash")

    assert await _run_until_idle() == 2

    async with bound_session_local() as session:
        statuses = list((await session.execute(select(EasyWeekEvent.status))).scalars().all())
        assert statuses == ["processed", "processed"], "both raw rows must reach a terminal status"
        records, clients, jobs = await _counts(session)
        assert records == 1, "a Resend must not create a second Record"
        assert clients == 1, "a Resend must not create a second Client"
        assert jobs == 0


async def test_resend_with_notifications_on_creates_one_job(
    bound_session_local, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="same-hash")
            await _capture(session, booking_created_resend(), payload_hash="same-hash")

    await _run_until_idle()

    async with bound_session_local() as session:
        jobs = list(
            (await session.execute(select(MessageJob).where(MessageJob.provider == "easyweek"))).scalars().all()
        )
        assert len(jobs) == 1, "a Resend must not create a second lifecycle job"
        assert jobs[0].provider == "easyweek"
        assert jobs[0].job_type == "record_created"


# ===========================================================================
# Fail-closed gates
# ===========================================================================


async def test_processing_disabled_claims_nothing(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    assert worker.processing_is_configured() is False

    async with bound_session_local() as session:
        status = (await session.execute(select(EasyWeekEvent.status))).scalar_one()
        assert status == "captured", "a disabled worker must leave the backlog untouched"


@pytest.mark.parametrize("location", [0, -5])
async def test_unconfigured_location_claims_nothing(bound_session_local, monkeypatch, location: int) -> None:
    monkeypatch.setattr(settings, "easyweek_location_id", location, raising=False)
    assert worker.processing_is_configured() is False


async def test_notifications_disabled_creates_zero_jobs(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        records, clients, jobs = await _counts(session)
        assert records == 1 and clients == 1
        assert jobs == 0, "notifications=false must not fill the queue"


async def test_notifications_enabled_creates_only_lifecycle_types(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), event_hint="booking-created", payload_hash="h1")
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="h2")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h3")
    await _run_until_idle()

    async with bound_session_local() as session:
        job_types = sorted((await session.execute(select(MessageJob.job_type))).scalars().all())
        assert job_types == ["record_canceled", "record_created", "record_updated"]
        providers = set((await session.execute(select(MessageJob.provider))).scalars().all())
        assert providers == {"easyweek"}


async def test_no_marketing_or_reminder_jobs_are_ever_planned(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            for index, (payload, hint) in enumerate(
                [
                    (booking_created(), "booking-created"),
                    (booking_updated(), "booking-updated"),
                    (booking_rescheduled(), "booking-rescheduled"),
                    (booking_canceled(), "booking-canceled"),
                ]
            ):
                await _capture(session, payload, event_hint=hint, payload_hash=f"h{index}")
    await _run_until_idle()

    forbidden = {"reminder_24h", "reminder_2h", "review_3d", "repeat_10d", "comeback_3d"}
    async with bound_session_local() as session:
        job_types = set((await session.execute(select(MessageJob.job_type))).scalars().all())
        assert not (job_types & forbidden), f"phase-2 job types leaked: {job_types & forbidden}"


# ===========================================================================
# Deterministic rejections
# ===========================================================================


@pytest.mark.parametrize(
    ("mutate", "hint", "expected_code"),
    [
        (lambda p: p.update({"location_id": FOREIGN_LOCATION_ID}), "booking-created", "foreign_location"),
        (lambda p: p.update({"uid": "not-a-uuid"}), "booking-created", "invalid_booking_uuid"),
        (lambda p: p.pop("uid"), "booking-created", "missing_booking_uuid"),
        (lambda p: p.update({"id": "nope"}), "booking-created", "missing_booking_id"),
        (lambda p: None, "created", "invalid_event_hint"),
        (lambda p: None, "smoke-test", "invalid_event_hint"),
        (lambda p: p.update({"booking_date_start": "garbage"}), "booking-created", "invalid_datetime"),
    ],
)
async def test_invalid_payloads_become_failed_with_a_safe_code(
    bound_session_local, mutate, hint: str, expected_code: str
) -> None:
    payload = booking_created()
    mutate(payload)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, payload, event_hint=hint, payload_hash="h1")

    assert await _run_until_idle() == 1

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "failed"
        assert event.error_code == expected_code
        assert event.error_code in NormalizationError.ALL_CODES
        assert event.processed_at is not None
        records, clients, jobs = await _counts(session)
        assert (records, clients, jobs) == (0, 0, 0), "a rejection must leave no partial writes"


async def test_foreign_location_leaves_no_partial_writes(bound_session_local) -> None:
    foreign = booking_created()
    foreign["location_id"] = FOREIGN_LOCATION_ID
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, foreign, payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        records, clients, jobs = await _counts(session)
        assert (records, clients, jobs) == (0, 0, 0)
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.error_code == "foreign_location"


async def test_truncated_body_is_failed_not_processed(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1", truncated=True)
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "failed"
        assert event.error_code == "truncated_payload"


async def test_booking_succeeded_is_processed_without_side_effects(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), event_hint="booking-succeeded", payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "processed"
        assert event.error_code is None
        assert await _counts(session) == (0, 0, 0)


async def test_error_code_never_contains_payload_content(bound_session_local) -> None:
    payload = booking_created()
    payload["location_id"] = FOREIGN_LOCATION_ID
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, payload, payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        code = (await session.execute(select(EasyWeekEvent.error_code))).scalar_one()
        assert code is not None
        for pii in (payload["customer_phone"], payload["customer_email"], payload["customer_full_name"]):
            assert pii not in code


# ===========================================================================
# Transactional safety
# ===========================================================================


async def test_unexpected_failure_leaves_the_event_captured(bound_session_local, monkeypatch) -> None:
    """A transient/infrastructure error must not burn the event."""

    async def _boom(*args, **kwargs):
        raise RuntimeError("transient database blip")

    monkeypatch.setattr(worker, "apply_booking", _boom)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    assert await worker.process_one() is True

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "captured", "the claim must roll back with the transaction"
        assert event.processed_at is None
        assert event.error_code is None
        # Re-queued for a later attempt rather than burned.
        assert event.processing_attempts == 1
        assert event.next_retry_at is not None
        assert await _counts(session) == (0, 0, 0)


async def test_a_crash_never_commits_a_processing_row(bound_session_local, monkeypatch) -> None:
    """`processing` is only ever visible inside the open transaction."""

    async def _boom(*args, **kwargs):
        raise RuntimeError("crash mid-flight")

    monkeypatch.setattr(worker, "apply_booking", _boom)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    assert await worker.process_one() is True

    async with bound_session_local() as session:
        statuses = set((await session.execute(select(EasyWeekEvent.status))).scalars().all())
        assert "processing" not in statuses


async def test_concurrent_workers_do_not_process_one_event_twice(bound_session_local) -> None:
    """SKIP LOCKED means the second claimer sees the queue as empty."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    async with bound_session_local() as first:
        async with first.begin():
            claimed = await worker.claim_next_event(first)
            assert claimed is not None

            async with bound_session_local() as second:
                async with second.begin():
                    assert await worker.claim_next_event(second) is None

    async with bound_session_local() as session:
        assert (await session.execute(select(func.count()).select_from(EasyWeekEvent))).scalar_one() == 1


# ===========================================================================
# The Altegio path is untouched
# ===========================================================================


async def test_numeric_id_collision_does_not_touch_the_altegio_row(bound_session_local) -> None:
    """Same numeric ids, different provider — two independent rows."""
    async with bound_session_local() as session:
        async with session.begin():
            altegio_client = Client(
                provider="altegio",
                company_id=TEST_LOCATION_ID,
                altegio_client_id=TEST_CUSTOMER_ID,
                display_name="Altegio Client",
                phone_e164="+49111111111",
            )
            session.add(altegio_client)
            await session.flush()
            session.add(
                Record(
                    provider="altegio",
                    company_id=TEST_LOCATION_ID,
                    altegio_record_id=TEST_BOOKING_ID,
                    client_id=altegio_client.id,
                    comment="altegio record",
                )
            )
            await _capture(session, booking_created(), payload_hash="h1")

    await _run_until_idle()

    async with bound_session_local() as session:
        altegio_record = (
            (
                await session.execute(
                    select(Record).where(Record.provider == "altegio").where(Record.company_id == TEST_LOCATION_ID)
                )
            )
            .scalars()
            .one()
        )
        assert altegio_record.comment == "altegio record", "the Altegio row was modified"
        assert altegio_record.easyweek_booking_uuid is None
        assert altegio_record.short_link is None

        altegio_client_row = (
            (
                await session.execute(
                    select(Client)
                    .where(Client.provider == "altegio")
                    .where(Client.company_id == TEST_LOCATION_ID)
                    .where(Client.altegio_client_id == TEST_CUSTOMER_ID)
                )
            )
            .scalars()
            .one()
        )
        assert altegio_client_row.display_name == "Altegio Client"
        assert altegio_client_row.phone_e164 == "+49111111111"

        easyweek_record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert easyweek_record.easyweek_booking_uuid == uuid.UUID(TEST_BOOKING_UUID)
        assert easyweek_record.id != altegio_record.id


async def test_existing_altegio_jobs_are_untouched(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            session.add(
                MessageJob(
                    provider="altegio",
                    company_id=TEST_LOCATION_ID,
                    job_type="record_created",
                    run_at=func.now(),
                    status="queued",
                    dedupe_key="record_created:999001:1:2026-01-01T00:00:00+00:00",
                    payload={},
                )
            )
            await _capture(session, booking_created(), payload_hash="h1")

    await _run_until_idle()

    async with bound_session_local() as session:
        altegio_jobs = list(
            (await session.execute(select(MessageJob).where(MessageJob.provider == "altegio"))).scalars().all()
        )
        assert len(altegio_jobs) == 1
        assert altegio_jobs[0].dedupe_key == "record_created:999001:1:2026-01-01T00:00:00+00:00"
        assert altegio_jobs[0].status == "queued"

        easyweek_jobs = list(
            (await session.execute(select(MessageJob).where(MessageJob.provider == "easyweek"))).scalars().all()
        )
        assert len(easyweek_jobs) == 1
        assert easyweek_jobs[0].dedupe_key.startswith("easyweek:")


# ===========================================================================
# Stale replay must not resurrect a cancelled booking (review fix 1)
# ===========================================================================


async def test_resend_of_create_after_cancel_does_not_resurrect_the_booking(bound_session_local) -> None:
    """create -> update -> reschedule -> cancel -> Resend(create).

    The Resend arrives LAST, so arrival order alone would make it look newest.
    It must not un-delete the booking or restore the original start time.
    """
    created = booking_created()
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, event_hint="booking-created", payload_hash="h-create")
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="h-update")
            await _capture(session, booking_rescheduled(), event_hint="booking-rescheduled", payload_hash="h-resched")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h-cancel")
    assert await _run_until_idle() == 4

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        cancelled_start = record.starts_at
        assert record.is_deleted is True

    # The Resend: byte-identical body, same hash, delivered after the cancel.
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created_resend(), event_hint="booking-created", payload_hash="h-create")
    assert await _run_until_idle() == 1

    async with bound_session_local() as session:
        records = list((await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().all())
        assert len(records) == 1, "the Resend created a second Record"
        record = records[0]
        assert record.is_deleted is True, "the Resend resurrected a cancelled booking"
        assert record.starts_at == cancelled_start, "the Resend rolled the time back to the original create"

        clients = list((await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().all())
        assert len(clients) == 1

        statuses = list((await session.execute(select(EasyWeekEvent.status))).scalars().all())
        assert statuses == ["processed"] * 5, "every delivery must still reach a terminal status"


async def test_resend_after_cancel_creates_no_duplicate_job(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), event_hint="booking-created", payload_hash="h-create")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h-cancel")
    await _run_until_idle()

    async with bound_session_local() as session:
        before = sorted((await session.execute(select(MessageJob.job_type))).scalars().all())

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created_resend(), event_hint="booking-created", payload_hash="h-create")
    await _run_until_idle()

    async with bound_session_local() as session:
        after = sorted((await session.execute(select(MessageJob.job_type))).scalars().all())
    assert after == before == ["record_canceled", "record_created"]


async def test_replay_does_not_touch_service_or_client_rows(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-create")
    await _run_until_idle()

    async with bound_session_local() as session:
        async with session.begin():
            record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
            record.comment = "operator edit"

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created_resend(), payload_hash="h-create")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.comment == "operator edit", "the replay re-applied domain writes"


# ===========================================================================
# UUID identity is authoritative (review fix 2)
# ===========================================================================


async def test_a_different_uuid_on_the_same_numeric_id_fails_closed(bound_session_local) -> None:
    """One booking must never seize the row of another."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    other = booking_created()
    other["uid"] = "99999999-2222-4333-8444-555555555555"  # same numeric id
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, other, payload_hash="h2")
    assert await _run_until_idle() == 1

    async with bound_session_local() as session:
        records = list((await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().all())
        assert len(records) == 1, "a conflicting UUID must not create a second row"
        assert records[0].easyweek_booking_uuid == uuid.UUID(TEST_BOOKING_UUID), "the original UUID was overwritten"
        assert records[0].altegio_record_id == TEST_BOOKING_ID

        conflicting = (
            (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h2"))).scalars().one()
        )
        assert conflicting.status == "failed"
        assert conflicting.error_code == "identity_conflict"
        assert conflicting.error_code in NormalizationError.ALL_CODES


async def test_identity_conflict_leaves_the_original_record_intact(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        original = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        snapshot = (original.starts_at, original.short_link, original.comment, original.total_cost)

    other = booking_created()
    other["uid"] = "99999999-2222-4333-8444-555555555555"
    other["booking_attributes.booking_comment"] = "hijack attempt"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, other, payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert (record.starts_at, record.short_link, record.comment, record.total_cost) == snapshot


async def test_identity_conflict_does_not_touch_an_altegio_row(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            session.add(
                Record(
                    provider="altegio",
                    company_id=TEST_LOCATION_ID,
                    altegio_record_id=TEST_BOOKING_ID,
                    comment="altegio record",
                )
            )
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        altegio = (await session.execute(select(Record).where(Record.provider == "altegio"))).scalars().one()
        assert altegio.comment == "altegio record"
        assert altegio.easyweek_booking_uuid is None


# ===========================================================================
# Service and price are persisted (review fix 7)
# ===========================================================================


async def test_create_persists_the_service_and_total_cost(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.total_cost == Decimal("35.00"), "PR-5 templates would render 0.00"
        services = list(
            (await session.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars().all()
        )
        assert len(services) == 1
        assert services[0].service_id == 5100003
        assert services[0].title == "Fixture Service"
        assert services[0].cost_to_pay == Decimal("35.00")


async def test_update_synchronises_a_changed_service_and_price(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    changed = booking_updated()
    changed["service_id"] = 5100099
    changed["service_name"] = "Different Service"
    changed["services_description"] = "Different Service"
    changed["booking_price_int"] = 5000
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, changed, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.total_cost == Decimal("50.00")
        services = list(
            (await session.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars().all()
        )
        assert len(services) == 1, "the old service row was not replaced"
        assert services[0].service_id == 5100099
        assert services[0].title == "Different Service"


async def test_resend_does_not_create_a_second_service_row(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-create")
            await _capture(session, booking_created_resend(), payload_hash="h-create")
    await _run_until_idle()

    async with bound_session_local() as session:
        services = list((await session.execute(select(RecordService))).scalars().all())
        assert len(services) == 1


async def test_partial_event_without_service_fields_keeps_the_service(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    silent = booking_updated()
    for key in (
        "service_id",
        "service_name",
        "services_description",
        "services_count",
        "quantity",
        "booking_price_int",
    ):
        silent.pop(key, None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.total_cost == Decimal("35.00"), "a silent delivery blanked the price"
        services = list((await session.execute(select(RecordService))).scalars().all())
        assert len(services) == 1, "a silent delivery deleted the known service"
        assert services[0].title == "Fixture Service"


async def test_altegio_record_services_are_untouched(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            altegio = Record(provider="altegio", company_id=TEST_LOCATION_ID, altegio_record_id=777001)
            session.add(altegio)
            await session.flush()
            session.add(RecordService(record_id=altegio.id, service_id=5100003, title="Altegio Service"))
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        altegio = (await session.execute(select(Record).where(Record.provider == "altegio"))).scalars().one()
        services = list(
            (await session.execute(select(RecordService).where(RecordService.record_id == altegio.id))).scalars().all()
        )
        assert len(services) == 1
        assert services[0].title == "Altegio Service"


# ===========================================================================
# Partial deliveries preserve known fields (review fix 6)
# ===========================================================================


async def test_update_without_phone_keeps_the_known_phone(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    silent = booking_updated()
    del silent["customer_phone"]
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
        assert client.phone_e164 == "+49000000000", "a partial delivery blanked the phone"


async def test_cancel_without_email_or_name_keeps_them(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    silent = booking_canceled()
    for key in ("customer_email", "customer_full_name", "customer_name", "customer_first_name", "customer_last_name"):
        silent.pop(key, None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint="booking-canceled", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
        assert client.email == "test.person@example.invalid"
        assert client.display_name == "Test Person"


async def test_partial_event_does_not_blank_times_or_staff(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        before = (await session.execute(select(Record.starts_at).where(Record.provider == "easyweek"))).scalar_one()

    silent = booking_updated()
    for key in ("booking_date_start", "booking_date_end", "booking_duration", "users_description", "user_name"):
        silent.pop(key, None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.starts_at == before
        assert record.staff_name == "Fixture Specialist"
        assert record.duration_sec == 3600


# ===========================================================================
# Deterministic failure is atomic with the claim (review fix 3)
# ===========================================================================


async def test_deterministic_failure_never_releases_the_row_mid_flight(bound_session_local) -> None:
    """The rejection must not publish the row as `captured` again.

    The old design rolled the outer transaction back and then failed the row in
    a second transaction. In that window another worker could claim it, process
    it successfully and mark it `processed` — only to have the first worker
    overwrite that with `failed`.
    """
    bad = booking_created()
    bad["location_id"] = FOREIGN_LOCATION_ID
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, bad, payload_hash="h1")

    observed: list[str | None] = []

    original_claim = worker.claim_next_event

    async def _claim_and_peek(session):
        event = await original_claim(session)
        if event is not None:
            # A CONCURRENT session must never see this row as claimable while
            # the first worker still owns it.
            async with bound_session_local() as other:
                async with other.begin():
                    stolen = await original_claim(other)
                    observed.append(None if stolen is None else "stolen")
        return event

    monkey = pytest.MonkeyPatch()
    monkey.setattr(worker, "claim_next_event", _claim_and_peek)
    try:
        assert await worker.process_one() is True
    finally:
        monkey.undo()

    assert observed == [None], "a second worker claimed the row during the rejection"

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
        assert event.status == "failed"
        assert event.error_code == "foreign_location"


async def test_a_processed_result_is_never_overwritten_by_a_failure(bound_session_local) -> None:
    """Two workers, one event: exactly one terminal result, and it stands."""
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, booking_created(), payload_hash="h1")

    assert await worker.process_one() is True
    assert await worker.process_one() is False, "the event was claimable twice"

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
        assert event.status == "processed"
        assert event.error_code is None


async def test_domain_writes_and_event_status_never_disagree(bound_session_local) -> None:
    """A failed event must leave no Record; a processed one must leave one."""
    bad = booking_created()
    bad["location_id"] = FOREIGN_LOCATION_ID
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, bad, payload_hash="h-bad")
            await _capture(session, booking_created(), payload_hash="h-good")
    await _run_until_idle()

    async with bound_session_local() as session:
        rows = {
            hash_: (status, code)
            for hash_, status, code in (
                await session.execute(
                    select(EasyWeekEvent.payload_hash, EasyWeekEvent.status, EasyWeekEvent.error_code)
                )
            ).all()
        }
        assert rows["h-bad"] == ("failed", "foreign_location")
        assert rows["h-good"] == ("processed", None)
        records, clients, _ = await _counts(session)
        assert (records, clients) == (1, 1), "the rejected event left partial writes"


# ===========================================================================
# Terminal-state invariants (review fix 11)
# ===========================================================================


async def test_processed_rows_have_a_timestamp_and_no_error_code(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
            await _capture(session, booking_created(), event_hint="booking-succeeded", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        for status, processed_at, code in (
            await session.execute(select(EasyWeekEvent.status, EasyWeekEvent.processed_at, EasyWeekEvent.error_code))
        ).all():
            assert status == "processed"
            assert processed_at is not None
            assert code is None


async def test_failed_rows_have_a_timestamp_and_a_safe_code(bound_session_local) -> None:
    bad = booking_created()
    bad["location_id"] = FOREIGN_LOCATION_ID
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, bad, payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        status, processed_at, code = (
            await session.execute(select(EasyWeekEvent.status, EasyWeekEvent.processed_at, EasyWeekEvent.error_code))
        ).one()
        assert status == "failed"
        assert processed_at is not None
        assert code in NormalizationError.ALL_CODES


async def test_captured_rows_after_a_transient_fault_stay_pristine(bound_session_local, monkeypatch) -> None:
    async def _boom(*args, **kwargs):
        raise RuntimeError("transient")

    monkeypatch.setattr(worker, "apply_booking", _boom)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    assert await worker.process_one() is True

    async with bound_session_local() as session:
        status, processed_at, code = (
            await session.execute(select(EasyWeekEvent.status, EasyWeekEvent.processed_at, EasyWeekEvent.error_code))
        ).one()
        assert (status, processed_at, code) == ("captured", None, None)
        assert await _counts(session) == (0, 0, 0)


# ===========================================================================
# booking-succeeded shares validation and isolation (review fix 8)
# ===========================================================================


async def test_succeeded_for_our_location_is_processed_without_side_effects(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), event_hint="booking-succeeded", payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "processed"
        assert event.error_code is None
        assert await _counts(session) == (0, 0, 0)


@pytest.mark.parametrize(
    ("mutate", "truncated", "expected"),
    [
        (lambda p: p.update({"location_id": FOREIGN_LOCATION_ID}), False, "foreign_location"),
        (lambda p: p.clear(), False, "invalid_payload"),
        (lambda p: None, True, "truncated_payload"),
        (lambda p: p.pop("location_id"), False, "invalid_location_id"),
    ],
)
async def test_invalid_succeeded_events_fail_closed(bound_session_local, mutate, truncated, expected) -> None:
    """A foreign or malformed `booking-succeeded` must NOT be marked processed."""
    payload = booking_created()
    mutate(payload)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, payload, event_hint="booking-succeeded", payload_hash="h1", truncated=truncated)
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "failed"
        assert event.error_code == expected
        assert await _counts(session) == (0, 0, 0)


# ===========================================================================
# Per-event retry: a poisoned row must not block the backlog (review fix 1)
# ===========================================================================


async def test_a_poisoned_event_does_not_block_newer_ones(bound_session_local, monkeypatch) -> None:
    """The head-of-line case. A is oldest and always explodes; B must still run."""
    other = booking_created()
    other["uid"] = "22222222-3333-4444-8555-666666666666"
    other["id"] = TEST_BOOKING_ID + 1

    async with bound_session_local() as session:
        async with session.begin():
            poisoned_id = await _capture(session, booking_created(), payload_hash="h-poison")
            healthy_id = await _capture(session, other, payload_hash="h-ok")

    real_apply = worker.apply_booking

    async def _explode_for_the_poisoned_one(session, booking, **kwargs):
        if str(booking.booking_uuid) == TEST_BOOKING_UUID:
            raise RuntimeError("simulated database fault")
        return await real_apply(session, booking, **kwargs)

    monkeypatch.setattr(worker, "apply_booking", _explode_for_the_poisoned_one)

    # First cycle claims the OLDEST row (the poisoned one) and reschedules it.
    assert await worker.process_one() is True
    # Second cycle must reach the newer, healthy event — not retry the poison.
    assert await worker.process_one() is True
    # Nothing eligible left: the poisoned row is waiting out its delay.
    assert await worker.process_one() is False

    async with bound_session_local() as session:
        poisoned = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == poisoned_id))).scalars().one()
        assert poisoned.status == "captured"
        assert poisoned.processing_attempts == 1
        assert poisoned.next_retry_at is not None, "the poisoned row was not rescheduled"
        assert poisoned.error_code is None
        assert poisoned.processed_at is None

        healthy = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == healthy_id))).scalars().one()
        assert healthy.status == "processed", "a healthy event was blocked behind the poisoned one"

        # Only the healthy booking produced domain rows.
        records = list((await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().all())
        assert len(records) == 1
        assert records[0].easyweek_booking_uuid == uuid.UUID(other["uid"])


async def test_retry_delay_grows_and_is_bounded() -> None:
    delays = [worker.retry_delay_for(n) for n in range(1, 12)]
    assert delays[0] == worker.RETRY_BASE_SEC
    assert delays == sorted(delays), "backoff must be monotonic"
    assert max(delays) <= worker.MAX_RETRY_DELAY_SEC


async def test_exhausted_retries_quarantine_the_event(bound_session_local, monkeypatch) -> None:
    async def _boom(*args, **kwargs):
        raise RuntimeError("permanent infrastructure fault")

    monkeypatch.setattr(worker, "apply_booking", _boom)
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, booking_created(), payload_hash="h1")

    for _ in range(worker.MAX_PROCESSING_ATTEMPTS):
        # Clear the delay so the next attempt is eligible immediately.
        async with bound_session_local() as session:
            async with session.begin():
                await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))
                row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
                if row.status == "captured":
                    row.next_retry_at = None
        await worker.process_one()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
        assert event.status == "failed", "the row was never quarantined"
        assert event.error_code == worker.RETRY_EXHAUSTED_CODE
        assert event.processed_at is not None
        assert event.next_retry_at is None
        assert event.processing_attempts >= worker.MAX_PROCESSING_ATTEMPTS
        assert await _counts(session) == (0, 0, 0), "a quarantined event left domain writes"


async def test_transient_failure_logs_no_pii(bound_session_local, monkeypatch, caplog) -> None:
    phone = "+4915199988877"
    email = "real.person@example.com"

    async def _boom(*args, **kwargs):
        raise RuntimeError(f"[parameters: ('{phone}', '{email}')]")

    monkeypatch.setattr(worker, "apply_booking", _boom)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    with caplog.at_level("DEBUG", logger="easyweek_inbox_worker"):
        await worker.process_one()

    text = "\n".join(record.getMessage() for record in caplog.records)
    for secret in (phone, email, "parameters:"):
        assert secret not in text, f"PII leaked into the log: {secret!r}"
    assert "processing_error" in text
    assert "RuntimeError" in text
    for record in caplog.records:
        assert record.exc_info is None, "a traceback was logged"


async def test_a_poisoned_numeric_event_does_not_block_the_next_one(bound_session_local) -> None:
    """An out-of-range number is deterministic, not transient."""
    poisoned = booking_created()
    poisoned["booking_price_int"] = 10**30
    healthy = booking_created()
    healthy["uid"] = "33333333-4444-4555-8666-777777777777"
    healthy["id"] = TEST_BOOKING_ID + 2

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, poisoned, payload_hash="h-bad")
            await _capture(session, healthy, payload_hash="h-ok")

    assert await _run_until_idle() == 2

    async with bound_session_local() as session:
        rows = {
            h: (status, code)
            for h, status, code in (
                await session.execute(
                    select(EasyWeekEvent.payload_hash, EasyWeekEvent.status, EasyWeekEvent.error_code)
                )
            ).all()
        }
        assert rows["h-bad"] == ("failed", "invalid_numeric_range")
        assert rows["h-ok"][0] == "processed"


# ===========================================================================
# UUID / numeric ownership (review fix 2)
# ===========================================================================


async def _seed_created(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-seed")
    await _run_until_idle()


async def _snapshot(bound_session_local) -> list[tuple]:
    async with bound_session_local() as session:
        return [
            tuple(row)
            for row in (
                await session.execute(
                    select(
                        Record.id,
                        Record.easyweek_booking_uuid,
                        Record.altegio_record_id,
                        Record.starts_at,
                        Record.comment,
                        Record.total_cost,
                        Record.is_deleted,
                    ).order_by(Record.id)
                )
            ).all()
        ]


async def test_existing_uuid_with_the_same_numeric_id_is_an_ordinary_update(bound_session_local) -> None:
    await _seed_created(bound_session_local)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h2"))).scalars().one()
        assert event.status == "processed"
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.comment == "fixture comment edited"


async def test_uuid_b_on_numeric_n_owned_by_uuid_a_conflicts(bound_session_local) -> None:
    await _seed_created(bound_session_local)
    before = await _snapshot(bound_session_local)

    intruder = booking_created()
    intruder["uid"] = "99999999-2222-4333-8444-555555555555"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, intruder, payload_hash="h-conflict")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (
            (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h-conflict")))
            .scalars()
            .one()
        )
        assert event.status == "failed"
        assert event.error_code == "identity_conflict"
    assert await _snapshot(bound_session_local) == before, "the conflict mutated existing rows"


async def test_uuid_a_moving_onto_a_numeric_id_owned_by_uuid_b_conflicts(bound_session_local) -> None:
    """UUID A exists; the delivery claims a numeric id that belongs to UUID B."""
    await _seed_created(bound_session_local)

    second = booking_created()
    second["uid"] = "44444444-5555-4666-8777-888888888888"
    second["id"] = TEST_BOOKING_ID + 50
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, second, payload_hash="h-second")
    await _run_until_idle()

    before = await _snapshot(bound_session_local)

    # Now UUID A tries to take over B's numeric id.
    hijack = booking_updated()
    hijack["id"] = TEST_BOOKING_ID + 50
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, hijack, event_hint="booking-updated", payload_hash="h-hijack")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (
            (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h-hijack")))
            .scalars()
            .one()
        )
        assert event.status == "failed", "a numeric-id takeover must not succeed"
        assert event.error_code == "identity_conflict"
    assert await _snapshot(bound_session_local) == before


async def test_numeric_id_owned_by_a_null_uuid_row_conflicts(bound_session_local) -> None:
    """Ownership of a UUID-less EasyWeek row was never proven; do not claim it."""
    async with bound_session_local() as session:
        async with session.begin():
            session.add(
                Record(
                    provider="easyweek",
                    company_id=TEST_LOCATION_ID,
                    altegio_record_id=TEST_BOOKING_ID,
                    easyweek_booking_uuid=None,
                    comment="pre-existing, unproven",
                )
            )
    before = await _snapshot(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "failed"
        assert event.error_code == "identity_conflict"
        assert await _counts(session) == (1, 0, 0), "no Client should have been created"
    assert await _snapshot(bound_session_local) == before


async def test_an_altegio_row_with_the_same_numeric_id_does_not_conflict(bound_session_local) -> None:
    """Identity resolution is provider-scoped; Altegio is a different namespace."""
    async with bound_session_local() as session:
        async with session.begin():
            session.add(
                Record(
                    provider="altegio",
                    company_id=TEST_LOCATION_ID,
                    altegio_record_id=TEST_BOOKING_ID,
                    comment="altegio record",
                )
            )
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "processed", "an Altegio row must not block EasyWeek identity"
        altegio = (await session.execute(select(Record).where(Record.provider == "altegio"))).scalars().one()
        assert altegio.comment == "altegio record"
        assert altegio.easyweek_booking_uuid is None


async def test_identity_conflict_creates_no_client_service_or_job(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _seed_created(bound_session_local)

    async with bound_session_local() as session:
        jobs_before = (
            await session.execute(select(func.count()).select_from(MessageJob).where(MessageJob.provider == "easyweek"))
        ).scalar_one()
        services_before = (await session.execute(select(func.count()).select_from(RecordService))).scalar_one()

    intruder = booking_created()
    intruder["uid"] = "99999999-2222-4333-8444-555555555555"
    intruder["customer_id"] = TEST_CUSTOMER_ID + 999
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, intruder, payload_hash="h-conflict")
    await _run_until_idle()

    async with bound_session_local() as session:
        jobs_after = (
            await session.execute(select(func.count()).select_from(MessageJob).where(MessageJob.provider == "easyweek"))
        ).scalar_one()
        services_after = (await session.execute(select(func.count()).select_from(RecordService))).scalar_one()
        clients = list((await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().all())
    assert jobs_after == jobs_before
    assert services_after == services_before
    assert all(c.altegio_client_id != TEST_CUSTOMER_ID + 999 for c in clients), "conflict created a Client"


# ===========================================================================
# Cancel is terminal (review fix 6)
# ===========================================================================


async def _state(bound_session_local) -> Record:
    async with bound_session_local() as session:
        return (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()


@pytest.mark.parametrize(
    ("hint", "build"),
    [
        ("booking-updated", booking_updated),
        ("booking-rescheduled", booking_rescheduled),
        ("booking-created", booking_created),
    ],
)
async def test_no_post_cancel_delivery_resurrects_the_booking(bound_session_local, hint, build) -> None:
    """Cancel is terminal in PR-4: no confirmed un-cancel signal exists."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-create")
            await _capture(session, booking_rescheduled(), event_hint="booking-rescheduled", payload_hash="h-resched")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h-cancel")
    await _run_until_idle()

    before = await _state(bound_session_local)
    snapshot = (before.starts_at, before.ends_at, before.total_cost, before.client_id, before.comment)
    assert before.is_deleted is True

    stale = build()
    stale["booking_attributes.booking_comment"] = "stale edit"
    stale["booking_price_int"] = 111
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, stale, event_hint=hint, payload_hash="h-stale-new-hash")
    await _run_until_idle()

    after = await _state(bound_session_local)
    assert after.is_deleted is True, f"{hint} after cancel resurrected the booking"
    assert (after.starts_at, after.ends_at, after.total_cost, after.client_id, after.comment) == snapshot, (
        f"{hint} after cancel mutated domain state"
    )


async def test_post_cancel_delivery_plans_no_job(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-create")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h-cancel")
    await _run_until_idle()

    async with bound_session_local() as session:
        before = sorted((await session.execute(select(MessageJob.job_type))).scalars().all())

    stale = booking_updated()
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, stale, event_hint="booking-updated", payload_hash="h-stale")
    await _run_until_idle()

    async with bound_session_local() as session:
        after = sorted((await session.execute(select(MessageJob.job_type))).scalars().all())
        event = (
            (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h-stale")))
            .scalars()
            .one()
        )
    assert after == before, "a post-cancel delivery created a lifecycle job"
    assert event.status == "processed", "the stale event must still reach a terminal status"


async def test_post_cancel_service_snapshot_is_not_rolled_back(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-create")
            await _capture(session, booking_canceled(), event_hint="booking-canceled", payload_hash="h-cancel")
    await _run_until_idle()

    async with bound_session_local() as session:
        before = [
            (s.service_id, s.title, s.cost_to_pay)
            for s in (await session.execute(select(RecordService))).scalars().all()
        ]

    stale = booking_updated()
    stale["service_id"] = 5100777
    stale["services_description"] = "Stale Service"
    stale["booking_price_int"] = 100
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, stale, event_hint="booking-updated", payload_hash="h-stale")
    await _run_until_idle()

    async with bound_session_local() as session:
        after = [
            (s.service_id, s.title, s.cost_to_pay)
            for s in (await session.execute(select(RecordService))).scalars().all()
        ]
    assert after == before, "a post-cancel delivery rewrote the service snapshot"


# ===========================================================================
# Client link survives partial deliveries (review fix 4)
# ===========================================================================


@pytest.mark.parametrize(
    ("hint", "build"), [("booking-updated", booking_updated), ("booking-canceled", booking_canceled)]
)
async def test_a_delivery_without_customer_id_keeps_the_client_link(bound_session_local, hint, build) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        linked_client_id = record.client_id
        assert linked_client_id is not None

    silent = build()
    del silent["customer_id"]
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint=hint, payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.client_id == linked_client_id, "the FK to the known Client was cleared"
        assert record.altegio_client_id == TEST_CUSTOMER_ID, "the external customer id was cleared"


async def test_an_explicit_null_customer_id_does_not_unlink(bound_session_local) -> None:
    """Unproven contract: null is treated as "not stated", never as "unlink"."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    silent = booking_updated()
    silent["customer_id"] = None
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, silent, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.client_id is not None
        assert record.altegio_client_id == TEST_CUSTOMER_ID


async def test_a_new_customer_id_rebinds_the_booking(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    moved = booking_updated()
    moved["customer_id"] = TEST_CUSTOMER_ID + 1
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, moved, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.altegio_client_id == TEST_CUSTOMER_ID + 1
        client = (await session.execute(select(Client).where(Client.id == record.client_id))).scalars().one()
        assert client.altegio_client_id == TEST_CUSTOMER_ID + 1


# ===========================================================================
# Service snapshot (review fix 5)
# ===========================================================================


async def _snapshot_row(bound_session_local) -> tuple:
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        service = (
            (await session.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars().one()
        )
        return (record.total_cost, service.service_id, service.title, service.amount, service.cost_to_pay)


async def test_multi_service_snapshot_uses_the_set_description(bound_session_local) -> None:
    """A two-service booking must not be described by one service's name."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created_multi_service(), payload_hash="h1")
    await _run_until_idle()

    total, _service_id, title, amount, cost = await _snapshot_row(bound_session_local)
    assert title == "Fixture Service, Second Fixture Service", "the singular name would mislead the customer"
    assert amount == 2
    assert total == Decimal("80.00")
    assert cost == total


async def test_price_only_update_keeps_the_service_row_consistent(bound_session_local) -> None:
    """Record.total_cost and RecordService.cost_to_pay must never diverge."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    price_only = booking_updated()
    for key in ("service_id", "service_name", "services_description", "services_count", "quantity"):
        price_only.pop(key, None)
    price_only["booking_price_int"] = 4200
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, price_only, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, _sid, _title, _amount, cost = await _snapshot_row(bound_session_local)
    assert total == Decimal("42.00")
    assert cost == total, "a price-only edit left a stale RecordService.cost_to_pay"


async def test_description_only_update_is_not_lost_without_a_service_id(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    description_only = booking_updated()
    for key in ("service_id", "service_name", "quantity", "booking_price_int"):
        description_only.pop(key, None)
    description_only["services_description"] = "Renamed Service Set"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, description_only, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _total, _sid, title, _amount, _cost = await _snapshot_row(bound_session_local)
    assert title == "Renamed Service Set"


async def test_the_pr5_facing_query_returns_a_usable_snapshot(bound_session_local) -> None:
    """What PR-5 will read to render a lifecycle template."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    async with bound_session_local() as session:
        row = (
            await session.execute(
                select(Record.total_cost, RecordService.title, RecordService.cost_to_pay)
                .join(RecordService, RecordService.record_id == Record.id)
                .where(Record.provider == "easyweek")
            )
        ).one()
    total, title, cost = row
    assert title, "PR-5 would render an empty service"
    assert total == Decimal("35.00") and cost == total, "PR-5 would render 0.00"
