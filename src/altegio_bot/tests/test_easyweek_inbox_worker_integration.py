"""PostgreSQL contract for the EasyWeek inbox worker (PR-4).

Exercises the real transactional lifecycle against a real database: the claim,
the provider-scoped upserts, the UUID-first identity, the fail-closed gates and
the guarantee that none of it disturbs the Altegio path.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.easyweek_normalizer import NormalizationError
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, Record
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

    with pytest.raises(RuntimeError):
        await worker.process_one()

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert event.status == "captured", "the claim must roll back with the transaction"
        assert event.processed_at is None
        assert event.error_code is None
        assert await _counts(session) == (0, 0, 0)


async def test_a_crash_never_commits_a_processing_row(bound_session_local, monkeypatch) -> None:
    """`processing` is only ever visible inside the open transaction."""

    async def _boom(*args, **kwargs):
        raise RuntimeError("crash mid-flight")

    monkeypatch.setattr(worker, "apply_booking", _boom)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")

    with pytest.raises(RuntimeError):
        await worker.process_one()

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
