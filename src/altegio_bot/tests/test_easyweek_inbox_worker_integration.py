"""PostgreSQL contract for the EasyWeek inbox worker (PR-4).

Exercises the real transactional lifecycle against a real database: the claim,
the provider-scoped upserts, the UUID-first identity, the fail-closed gates and
the guarantee that none of it disturbs the Altegio path.
"""

from __future__ import annotations

import asyncio
import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.easyweek_normalizer import NormalizationError, canonical_booking_uuid
from altegio_bot.easyweek_service_category import (
    EASYWEEK_RAW_NAMESPACE,
    SERVICE_CATEGORY_SNAPSHOT_KEY,
    SERVICES_COUNT_SNAPSHOT_KEY,
    service_category_from_record_raw,
    services_count_from_record_raw,
)
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
    TEST_LOCATION_UUID,
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
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test-branch": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )


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
        # Exactly what the capture endpoint stores: the canonical booking UUID,
        # or NULL when the id is missing/malformed. The claim orders on this
        # column, so a fixture that skipped it would not be testing production.
        booking_uuid=canonical_booking_uuid(payload),
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


@pytest.mark.parametrize("location_map", ["{}", "", "{not json"])
async def test_unconfigured_location_claims_nothing(bound_session_local, monkeypatch, location_map: str) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", location_map, raising=False)
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


async def test_two_registry_locations_write_their_own_company_ids(bound_session_local, monkeypatch) -> None:
    second_id = 999002
    second_uuid = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
    location_map = json.loads(settings.easyweek_location_map)
    location_map["second-branch"] = {
        "location_id": second_id,
        "location_uuid": second_uuid,
        "meta_template_prefix": "sb",
        "booking_page_url": "https://booking.example.invalid/second",
    }
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(location_map), raising=False)

    first = booking_created()
    second = booking_created()
    second.update(
        {
            "location_id": second_id,
            "location_uuid": second_uuid,
            "uid": "22222222-3333-4444-8555-666666666666",
            "id": TEST_BOOKING_ID + 1,
            "customer_id": TEST_CUSTOMER_ID + 1,
            "service_id": 5100004,
        }
    )
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, first, payload_hash="location-a")
            await _capture(session, second, payload_hash="location-b")

    assert await _run_until_idle() == 2
    async with bound_session_local() as session:
        record_companies = set(
            (await session.execute(select(Record.company_id).where(Record.provider == "easyweek"))).scalars().all()
        )
        client_companies = set(
            (await session.execute(select(Client.company_id).where(Client.provider == "easyweek"))).scalars().all()
        )
        assert record_companies == client_companies == {TEST_LOCATION_ID, second_id}


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


# ===========================================================================
# PR-7.1 category snapshot and planner guard
# ===========================================================================


@pytest.mark.parametrize("broken_allowlist", ["", "[]", "{invalid", '["Fixture Category", 7]'])
async def test_unavailable_category_configuration_leaves_event_captured_until_fixed(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
    broken_allowlist: str,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", broken_allowlist, raising=False)
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, booking_created(), payload_hash="config-recovery")

    assert await worker.process_one() is False
    async with bound_session_local() as session:
        event = await session.get(EasyWeekEvent, event_id)
        assert event is not None and event.status == "captured"
        assert await _counts(session) == (0, 0, 0)

    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    assert await worker.process_one() is True
    async with bound_session_local() as session:
        event = await session.get(EasyWeekEvent, event_id)
        assert event is not None and event.status == "processed"
        assert await _counts(session) == (1, 1, 1)


async def _capture_and_process(
    session_maker,
    payload: dict[str, Any],
    *,
    event_hint: str,
    payload_hash: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _capture(session, payload, event_hint=event_hint, payload_hash=payload_hash)
    assert await _run_until_idle() == 1


async def _easyweek_jobs(session_maker) -> list[MessageJob]:
    async with session_maker() as session:
        return list(
            (await session.execute(select(MessageJob).where(MessageJob.provider == "easyweek").order_by(MessageJob.id)))
            .scalars()
            .all()
        )


async def test_allowed_create_persists_minimal_snapshot_and_plans_one_job(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    payload = booking_created()
    payload["service_category"] = "  FIXTURE\u00a0CATEGORY  "
    await _capture_and_process(
        bound_session_local,
        payload,
        event_hint="booking-created",
        payload_hash="category-allowed-create",
    )

    jobs = await _easyweek_jobs(bound_session_local)
    assert [job.job_type for job in jobs] == ["record_created"]
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert record.raw == {
            EASYWEEK_RAW_NAMESPACE: {
                SERVICE_CATEGORY_SNAPSHOT_KEY: "FIXTURE CATEGORY",
                SERVICES_COUNT_SNAPSHOT_KEY: 1,
            }
        }


@pytest.mark.parametrize(
    ("payload_builder", "event_hint"),
    [
        (booking_created, "booking-created"),
        (booking_updated, "booking-updated"),
        (booking_rescheduled, "booking-rescheduled"),
        (booking_canceled, "booking-canceled"),
    ],
)
async def test_multi_service_is_terminal_business_suppression_for_every_lifecycle_event(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
    payload_builder,
    event_hint: str,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    payload = payload_builder()
    payload["services_count"] = 2
    await _capture_and_process(
        bound_session_local,
        payload,
        event_hint=event_hint,
        payload_hash=f"multi-{event_hint}",
    )

    assert await _easyweek_jobs(bound_session_local) == []
    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert event.status == "processed"
        assert services_count_from_record_raw(record.raw) == 2


async def test_absent_service_count_update_preserves_single_service_proof_and_plans(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="count-one-create",
    )
    update_payload = booking_updated()
    update_payload.pop("services_count")
    await _capture_and_process(
        bound_session_local,
        update_payload,
        event_hint="booking-updated",
        payload_hash="count-absent-update",
    )

    assert [job.job_type for job in await _easyweek_jobs(bound_session_local)] == [
        "record_created",
        "record_updated",
    ]
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert services_count_from_record_raw(record.raw) == 1


@pytest.mark.parametrize("cleared", [None, 0, -1, "1", 1.5, True, {}])
async def test_explicit_unusable_service_count_clears_proof_and_suppresses(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
    cleared: object,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="count-before-clear",
    )
    update_payload = booking_updated()
    update_payload["services_count"] = cleared
    await _capture_and_process(
        bound_session_local,
        update_payload,
        event_hint="booking-updated",
        payload_hash=f"count-clear-{cleared!r}",
    )

    assert [job.job_type for job in await _easyweek_jobs(bound_session_local)] == ["record_created"]
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert services_count_from_record_raw(record.raw) is None


async def test_service_count_transition_one_to_multi_to_one_reopens_planning(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="count-transition-one",
    )
    multi = booking_updated()
    multi["services_count"] = 2
    await _capture_and_process(
        bound_session_local,
        multi,
        event_hint="booking-updated",
        payload_hash="count-transition-two",
    )
    single = booking_updated()
    single["services_count"] = 1
    await _capture_and_process(
        bound_session_local,
        single,
        event_hint="booking-updated",
        payload_hash="count-transition-back-one",
    )

    assert [job.job_type for job in await _easyweek_jobs(bound_session_local)] == [
        "record_created",
        "record_updated",
    ]


@pytest.mark.parametrize("category", ["Other Category", None])
async def test_disallowed_or_missing_create_is_processed_and_persisted_without_job(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
    category: object,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    payload = booking_created()
    if category is None:
        payload.pop("service_category")
    else:
        payload["service_category"] = category
    await _capture_and_process(
        bound_session_local,
        payload,
        event_hint="booking-created",
        payload_hash=f"category-suppressed-{category is None}",
    )

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent))).scalars().one()
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert event.status == "processed"
        assert service_category_from_record_raw(record.raw) == category
        assert await _counts(session) == (1, 1, 0)


async def test_absent_update_preserves_allowed_category_and_plans_update(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="allowed-create-before-patch",
    )
    update_payload = booking_updated()
    update_payload.pop("service_category")
    await _capture_and_process(
        bound_session_local,
        update_payload,
        event_hint="booking-updated",
        payload_hash="allowed-absent-update",
    )

    jobs = await _easyweek_jobs(bound_session_local)
    assert [job.job_type for job in jobs] == ["record_created", "record_updated"]
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert service_category_from_record_raw(record.raw) == "Fixture Category"


async def test_disallowed_patch_state_survives_absent_update_and_cancel(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    disallowed = booking_created()
    disallowed["service_category"] = "Other Category"
    await _capture_and_process(
        bound_session_local,
        disallowed,
        event_hint="booking-created",
        payload_hash="disallowed-create",
    )
    update_payload = booking_updated()
    update_payload.pop("service_category")
    await _capture_and_process(
        bound_session_local,
        update_payload,
        event_hint="booking-updated",
        payload_hash="disallowed-absent-update",
    )
    cancel_payload = booking_canceled()
    cancel_payload.pop("service_category")
    await _capture_and_process(
        bound_session_local,
        cancel_payload,
        event_hint="booking-canceled",
        payload_hash="disallowed-absent-cancel",
    )

    assert await _easyweek_jobs(bound_session_local) == []
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert service_category_from_record_raw(record.raw) == "Other Category"
        assert record.is_deleted is True


async def test_allowed_disallowed_allowed_transitions_gate_each_update(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="transition-create",
    )
    disallowed = booking_updated()
    disallowed["service_category"] = "Other Category"
    await _capture_and_process(
        bound_session_local,
        disallowed,
        event_hint="booking-updated",
        payload_hash="transition-disallowed",
    )
    allowed = booking_updated()
    allowed["service_category"] = "fixture category"
    await _capture_and_process(
        bound_session_local,
        allowed,
        event_hint="booking-updated",
        payload_hash="transition-allowed",
    )

    jobs = await _easyweek_jobs(bound_session_local)
    assert [job.job_type for job in jobs] == ["record_created", "record_updated"]


async def test_cancel_uses_persisted_allowed_category(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="allowed-before-cancel",
    )
    cancel_payload = booking_canceled()
    cancel_payload.pop("service_category")
    cancel_payload.pop("services_count")
    await _capture_and_process(
        bound_session_local,
        cancel_payload,
        event_hint="booking-canceled",
        payload_hash="allowed-cancel",
    )
    jobs = await _easyweek_jobs(bound_session_local)
    assert [job.job_type for job in jobs] == ["record_created", "record_canceled"]


async def test_cancel_without_count_uses_persisted_multi_service_suppression(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    created = booking_created()
    created["services_count"] = 2
    await _capture_and_process(
        bound_session_local,
        created,
        event_hint="booking-created",
        payload_hash="multi-before-cancel",
    )
    canceled = booking_canceled()
    canceled.pop("service_category")
    canceled.pop("services_count")
    await _capture_and_process(
        bound_session_local,
        canceled,
        event_hint="booking-canceled",
        payload_hash="multi-absent-cancel",
    )

    assert await _easyweek_jobs(bound_session_local) == []
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert services_count_from_record_raw(record.raw) == 2
        assert record.is_deleted is True


@pytest.mark.parametrize("cleared", [None, "", "  ", 42, "bad\nvalue"])
async def test_explicit_unusable_category_clears_eligibility(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
    cleared: object,
) -> None:
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    await _capture_and_process(
        bound_session_local,
        booking_created(),
        event_hint="booking-created",
        payload_hash="allowed-before-clear",
    )
    update_payload = booking_updated()
    update_payload["service_category"] = cleared
    await _capture_and_process(
        bound_session_local,
        update_payload,
        event_hint="booking-updated",
        payload_hash=f"clear-{repr(cleared)}",
    )

    jobs = await _easyweek_jobs(bound_session_local)
    assert [job.job_type for job in jobs] == ["record_created"]
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert service_category_from_record_raw(record.raw) is None


async def test_notifications_disabled_processes_snapshot_even_with_invalid_allowlist(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", "{invalid", raising=False)
    payload = booking_created()
    payload["service_category"] = "Other Category"
    await _capture_and_process(
        bound_session_local,
        payload,
        event_hint="booking-created",
        payload_hash="notifications-disabled-category",
    )
    async with bound_session_local() as session:
        record = (await session.execute(select(Record).where(Record.provider == "easyweek"))).scalars().one()
        assert service_category_from_record_raw(record.raw) == "Other Category"
        assert services_count_from_record_raw(record.raw) == 1
        assert await _counts(session) == (1, 1, 0)


async def test_durlach_and_rastatt_share_identical_category_and_count_semantics(
    bound_session_local,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_id = TEST_LOCATION_ID + 1
    second_uuid = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": "https://booking.example.invalid/durlach",
                },
                "rastatt": {
                    "location_id": second_id,
                    "location_uuid": second_uuid,
                    "meta_template_prefix": "ra",
                    "booking_page_url": "https://booking.example.invalid/rastatt",
                },
            }
        ),
        raising=False,
    )

    durlach = booking_created()
    rastatt_allowed = booking_created()
    rastatt_allowed.update(
        {
            "uid": "22222222-3333-4444-8555-666666666666",
            "id": TEST_BOOKING_ID + 1,
            "customer_id": TEST_CUSTOMER_ID + 1,
            "location_id": second_id,
            "location_uuid": second_uuid,
        }
    )
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, durlach, payload_hash="du-allowed")
            await _capture(session, rastatt_allowed, payload_hash="ra-allowed")
    assert await _run_until_idle() == 2

    jobs = await _easyweek_jobs(bound_session_local)
    assert {(job.company_id, job.job_type) for job in jobs} == {
        (TEST_LOCATION_ID, "record_created"),
        (second_id, "record_created"),
    }

    durlach_update = booking_updated()
    durlach_update["services_count"] = 2
    rastatt_update = booking_updated()
    rastatt_update.update(
        {
            "uid": rastatt_allowed["uid"],
            "id": rastatt_allowed["id"],
            "customer_id": rastatt_allowed["customer_id"],
            "location_id": second_id,
            "location_uuid": second_uuid,
            "services_count": 2,
        }
    )
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(
                session,
                durlach_update,
                event_hint="booking-updated",
                payload_hash="du-disallowed",
            )
            await _capture(
                session,
                rastatt_update,
                event_hint="booking-updated",
                payload_hash="ra-disallowed",
            )
    assert await _run_until_idle() == 2
    assert len(await _easyweek_jobs(bound_session_local)) == 2

    async with bound_session_local() as session:
        records = list(
            (await session.execute(select(Record).where(Record.provider == "easyweek").order_by(Record.company_id)))
            .scalars()
            .all()
        )
        assert {record.company_id for record in records} == {TEST_LOCATION_ID, second_id}
        assert {service_category_from_record_raw(record.raw) for record in records} == {"Fixture Category"}
        assert {services_count_from_record_raw(record.raw) for record in records} == {2}


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
        (
            lambda p: p.update({"location_uuid": "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"}),
            "booking-created",
            "location_identity_mismatch",
        ),
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


async def test_repeated_transient_failures_never_become_terminal(bound_session_local, monkeypatch) -> None:
    """An UNCLASSIFIED fault must stay recoverable, however many times it fires.

    Turning it into `failed` on attempt count alone would discard a real webhook
    because PostgreSQL was briefly unreachable or a deploy was mid-flight.
    """

    async def _boom(*args, **kwargs):
        raise RuntimeError("infrastructure outage")

    monkeypatch.setattr(worker, "apply_booking", _boom)
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, booking_created(), payload_hash="h1")

    delays: list[float] = []
    for _ in range(8):  # well past the old 5-attempt quarantine threshold
        async with bound_session_local() as session:
            async with session.begin():
                row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
                assert row.status == "captured", "an unclassified fault was made terminal"
                row.next_retry_at = None
        assert await worker.process_one() is True
        async with bound_session_local() as session:
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
            delays.append(worker.retry_delay_for(row.processing_attempts))

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
        assert event.status == "captured"
        assert event.error_code is None
        assert event.processed_at is None
        assert event.processing_attempts >= 8
        assert event.next_retry_at is not None
        assert await _counts(session) == (0, 0, 0), "a retrying event left domain writes"

    assert delays == sorted(delays), "backoff must be monotonic"
    assert max(delays) <= worker.MAX_RETRY_DELAY_SEC, "backoff must stay bounded"
    assert max(delays) == worker.MAX_RETRY_DELAY_SEC, "backoff must actually reach its ceiling"


async def test_an_event_recovers_once_the_dependency_is_healthy(bound_session_local, monkeypatch) -> None:
    """After the fault clears, the event processes exactly once."""
    real_apply = worker.apply_booking
    broken = {"yes": True}

    async def _sometimes(session, booking, **kwargs):
        if broken["yes"]:
            raise RuntimeError("dependency down")
        return await real_apply(session, booking, **kwargs)

    monkeypatch.setattr(worker, "apply_booking", _sometimes)
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, booking_created(), payload_hash="h1")

    for _ in range(3):
        async with bound_session_local() as session:
            async with session.begin():
                row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
                row.next_retry_at = None
        await worker.process_one()

    broken["yes"] = False
    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
            row.next_retry_at = None
    assert await worker.process_one() is True

    async with bound_session_local() as session:
        event = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
        assert event.status == "processed"
        assert event.error_code is None
        assert event.processed_at is not None
        assert event.next_retry_at is None, "a terminal row must not keep a future retry timestamp"
        # Applied exactly once despite the earlier failures.
        assert await _counts(session) == (1, 1, 0)


async def test_a_stalled_event_does_not_hold_up_other_bookings(bound_session_local, monkeypatch) -> None:
    other = booking_created()
    other["uid"] = "77777777-8888-4999-8aaa-bbbbbbbbbbbb"
    other["id"] = TEST_BOOKING_ID + 77

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-stalled")
            await _capture(session, other, payload_hash="h-other")

    real_apply = worker.apply_booking

    async def _explode_for_the_stalled_one(session, booking, **kwargs):
        if str(booking.booking_uuid) == TEST_BOOKING_UUID:
            raise RuntimeError("dependency down")
        return await real_apply(session, booking, **kwargs)

    monkeypatch.setattr(worker, "apply_booking", _explode_for_the_stalled_one)

    assert await worker.process_one() is True  # stalled one, rescheduled
    assert await worker.process_one() is True  # the OTHER booking still flows

    async with bound_session_local() as session:
        rows = {
            h: status
            for h, status in (await session.execute(select(EasyWeekEvent.payload_hash, EasyWeekEvent.status))).all()
        }
        assert rows["h-stalled"] == "captured"
        assert rows["h-other"] == "processed"


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


# ===========================================================================
# Causal order within one booking UUID (review fix 1)
# ===========================================================================


async def test_a_stalled_earlier_event_holds_back_only_its_own_booking(bound_session_local, monkeypatch) -> None:
    """The ordering case per-event retry would otherwise break.

    A (early reschedule, UUID X) fails transiently. B (later reschedule, same X)
    must NOT jump ahead — applying it and then letting A land on top would
    revert the times. A different booking Y keeps flowing throughout.
    """
    early = booking_rescheduled()
    early["booking_date_start"] = "2026-08-03T12:00:00+0000"
    early["booking_date_end"] = "2026-08-03T13:00:00+0000"

    later = booking_rescheduled()
    later["booking_date_start"] = "2026-08-03T16:00:00+0000"
    later["booking_date_end"] = "2026-08-03T17:00:00+0000"

    unrelated = booking_created()
    unrelated["uid"] = "aaaaaaaa-1111-4222-8333-444444444444"
    unrelated["id"] = TEST_BOOKING_ID + 900

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-create")
            await _capture(session, early, event_hint="booking-rescheduled", payload_hash="h-early")
            await _capture(session, later, event_hint="booking-rescheduled", payload_hash="h-later")
            await _capture(session, unrelated, payload_hash="h-other")

    fail_early = {"yes": True}
    real_apply = worker.apply_booking

    async def _fail_the_early_reschedule(session, booking, **kwargs):
        if fail_early["yes"] and kwargs.get("payload_hash") == "h-early":
            raise RuntimeError("dependency down")
        return await real_apply(session, booking, **kwargs)

    monkeypatch.setattr(worker, "apply_booking", _fail_the_early_reschedule)

    # create -> ok; early -> transient, rescheduled; other booking -> ok.
    assert await _run_until_idle() >= 3

    async with bound_session_local() as session:
        rows = {
            h: (status, retry is not None)
            for h, status, retry in (
                await session.execute(
                    select(EasyWeekEvent.payload_hash, EasyWeekEvent.status, EasyWeekEvent.next_retry_at)
                )
            ).all()
        }
        assert rows["h-create"][0] == "processed"
        assert rows["h-early"] == ("captured", True), "the early event should be waiting on a retry"
        assert rows["h-later"][0] == "captured", "the LATER event of the same booking jumped the queue"
        assert rows["h-other"][0] == "processed", "an unrelated booking was blocked"

        record = (
            (
                await session.execute(
                    select(Record)
                    .where(Record.provider == "easyweek")
                    .where(Record.easyweek_booking_uuid == uuid.UUID(TEST_BOOKING_UUID))
                )
            )
            .scalars()
            .one()
        )
        assert record.starts_at == datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc), (
            "a later delivery was applied out of order"
        )

    # The dependency recovers: A applies, then B, and B wins.
    fail_early["yes"] = False
    async with bound_session_local() as session:
        async with session.begin():
            row = (
                (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h-early")))
                .scalars()
                .one()
            )
            row.next_retry_at = None
    assert await _run_until_idle() == 2

    async with bound_session_local() as session:
        statuses = set((await session.execute(select(EasyWeekEvent.status))).scalars().all())
        assert statuses == {"processed"}
        record = (
            (
                await session.execute(
                    select(Record)
                    .where(Record.provider == "easyweek")
                    .where(Record.easyweek_booking_uuid == uuid.UUID(TEST_BOOKING_UUID))
                )
            )
            .scalars()
            .one()
        )
        assert record.starts_at == datetime(2026, 8, 3, 16, 0, tzinfo=timezone.utc), (
            "final state must be the LAST delivery, not the replayed earlier one"
        )


async def test_a_terminal_predecessor_does_not_block_its_successor(bound_session_local) -> None:
    """Only non-terminal predecessors hold the line."""
    bad = booking_created()
    bad["booking_date_start"] = "garbage"

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, bad, payload_hash="h-bad")
            await _capture(session, booking_created(), payload_hash="h-good")

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
        assert rows["h-bad"] == ("failed", "invalid_datetime")
        assert rows["h-good"][0] == "processed", "a failed predecessor blocked its successor"


async def test_a_processed_predecessor_does_not_block_either(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="h2")
    assert await _run_until_idle() == 2

    async with bound_session_local() as session:
        statuses = set((await session.execute(select(EasyWeekEvent.status))).scalars().all())
        assert statuses == {"processed"}


async def test_rows_without_a_usable_uid_neither_block_nor_are_blocked(bound_session_local) -> None:
    """A malformed capture row must not stall well-formed bookings."""
    async with bound_session_local() as session:
        async with session.begin():
            # Oldest row: no `uid` at all.
            await _capture(session, {"location_id": TEST_LOCATION_ID}, payload_hash="h-nouid")
            await _capture(session, booking_created(), payload_hash="h-good")

    assert await _run_until_idle() == 2

    async with bound_session_local() as session:
        rows = {
            h: status
            for h, status in (await session.execute(select(EasyWeekEvent.payload_hash, EasyWeekEvent.status))).all()
        }
        assert rows["h-nouid"] == "failed", "the malformed row must reach its deterministic failure"
        assert rows["h-good"] == "processed", "a malformed row blocked a real booking"


async def test_two_workers_never_process_one_booking_out_of_order(bound_session_local) -> None:
    """Concurrent claims: same UUID serialises, different UUIDs run in parallel."""
    other = booking_created()
    other["uid"] = "bbbbbbbb-1111-4222-8333-444444444444"
    other["id"] = TEST_BOOKING_ID + 800

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-x1")
            await _capture(session, booking_updated(), event_hint="booking-updated", payload_hash="h-x2")
            await _capture(session, other, payload_hash="h-y1")

    # Worker 1 holds the first event of booking X inside an open transaction.
    async with bound_session_local() as first:
        async with first.begin():
            claimed = await worker.claim_next_event(first)
            assert claimed is not None
            assert claimed.payload_hash == "h-x1"

            # Worker 2 must NOT be able to take the second event of booking X,
            # but SHOULD be able to take the unrelated booking Y.
            async with bound_session_local() as second:
                async with second.begin():
                    concurrent = await worker.claim_next_event(second)
                    assert concurrent is not None, "an unrelated booking was blocked"
                    assert concurrent.payload_hash == "h-y1", (
                        f"a second worker claimed {concurrent.payload_hash} out of order"
                    )


# ===========================================================================
# Identity is validated BEFORE the cancel no-op (review fix 3)
# ===========================================================================


async def test_identity_conflict_wins_over_the_cancel_no_op(bound_session_local, monkeypatch) -> None:
    """A cancelled UUID must not mask a numeric-id takeover as `processed`."""
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)

    # Booking A: created then cancelled, numeric id 100.
    a_created = booking_created()
    a_created["id"] = 100
    a_cancel = booking_canceled()
    a_cancel["id"] = 100

    # Booking B: active, numeric id 200.
    b = booking_created()
    b["uid"] = "cccccccc-1111-4222-8333-444444444444"
    b["id"] = 200
    b["customer_id"] = TEST_CUSTOMER_ID + 5

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, a_created, payload_hash="h-a1")
            await _capture(session, a_cancel, event_hint="booking-canceled", payload_hash="h-a2")
            await _capture(session, b, payload_hash="h-b1")
    await _run_until_idle()

    before = await _snapshot(bound_session_local)
    async with bound_session_local() as session:
        jobs_before = (
            await session.execute(select(func.count()).select_from(MessageJob).where(MessageJob.provider == "easyweek"))
        ).scalar_one()
        services_before = (await session.execute(select(func.count()).select_from(RecordService))).scalar_one()
        clients_before = (
            await session.execute(select(func.count()).select_from(Client).where(Client.provider == "easyweek"))
        ).scalar_one()

    # Contradictory: UUID A (cancelled) claiming B's numeric id.
    contradiction = booking_updated()
    contradiction["id"] = 200
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, contradiction, event_hint="booking-updated", payload_hash="h-conflict")
    await _run_until_idle()

    async with bound_session_local() as session:
        event = (
            (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.payload_hash == "h-conflict")))
            .scalars()
            .one()
        )
        assert event.status == "failed", "the cancel guard masked an identity conflict as processed"
        assert event.error_code == "identity_conflict"
        assert event.processed_at is not None

        jobs_after = (
            await session.execute(select(func.count()).select_from(MessageJob).where(MessageJob.provider == "easyweek"))
        ).scalar_one()
        services_after = (await session.execute(select(func.count()).select_from(RecordService))).scalar_one()
        clients_after = (
            await session.execute(select(func.count()).select_from(Client).where(Client.provider == "easyweek"))
        ).scalar_one()

    assert await _snapshot(bound_session_local) == before, "both records must be untouched"
    assert (jobs_after, services_after, clients_after) == (jobs_before, services_before, clients_before)


# ===========================================================================
# Explicit-empty service semantics (review fix 4)
# ===========================================================================


@pytest.mark.parametrize("cleared", ["", None])
async def test_an_explicitly_cleared_description_clears_the_title(bound_session_local, cleared) -> None:
    """Present-but-empty is authoritative; the name fallback must not fire."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    _t, _s, title, _a, _c = await _snapshot_row(bound_session_local)
    assert title == "Fixture Service"

    cleared_event = booking_updated()
    cleared_event["services_description"] = cleared
    cleared_event["service_name"] = cleared
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, cleared_event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, title, _a, _c = await _snapshot_row(bound_session_local)
    assert title is None, "an explicitly cleared description left a stale title"


@pytest.mark.parametrize("cleared", ["", None])
async def test_a_cleared_set_description_wins_over_a_carried_name(bound_session_local, cleared) -> None:
    """The whole-set field is authoritative even when it is explicitly empty.

    ``services_description`` describes ALL services; ``service_name`` names one
    of them. A delivery that sends both has said "the set description is now
    empty" — falling back to the singular name would resurrect a title the
    delivery did not confirm, and for a multi-service booking it would be
    actively wrong. Presence decides which field wins, not truthiness.
    """
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["services_description"] = cleared
    event["service_name"] = "Single Service"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, title, _a, _c = await _snapshot_row(bound_session_local)
    assert title is None


@pytest.mark.parametrize("cleared", [None, 0])
async def test_an_explicitly_cleared_count_clears_the_amount(bound_session_local, cleared) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["services_count"] = cleared
    event["quantity"] = cleared
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, _title, amount, _c = await _snapshot_row(bound_session_local)
    assert amount == (None if cleared is None else 0)


async def test_an_explicitly_null_price_clears_both_costs(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["booking_price_int"] = None
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, _s, _title, _a, cost = await _snapshot_row(bound_session_local)
    assert total is None
    assert cost is None, "Record.total_cost and RecordService.cost_to_pay diverged"


async def test_absent_service_fields_still_keep_the_snapshot(bound_session_local) -> None:
    """The absent/empty distinction must remain, not collapse into one rule."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()
    before = await _snapshot_row(bound_session_local)

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

    assert await _snapshot_row(bound_session_local) == before


# ===========================================================================
# Review fix 1 — causal ordering keys on the CANONICAL booking UUID
# ===========================================================================
#
# The normalizer collapses every textual spelling of one UUID to a single value.
# Ordering therefore has to key on that canonical value: on the raw
# `payload ->> 'uid'` text, an EARLIER delivery still retrying would not be seen
# as the predecessor of a LATER delivery written in a different case or format,
# so the later one would be applied first and the recovered earlier one would
# then overwrite the newer times, service snapshot, price or client link.

_SAME_UUID_SPELLINGS = {
    "uppercase": TEST_BOOKING_UUID.upper(),
    "braced": "{" + TEST_BOOKING_UUID + "}",
    "compact": TEST_BOOKING_UUID.replace("-", ""),
    "padded": f"  {TEST_BOOKING_UUID}  ",
    "urn": f"urn:uuid:{TEST_BOOKING_UUID}",
}


@pytest.mark.parametrize("spelling", sorted(_SAME_UUID_SPELLINGS))
async def test_alternative_uuid_spellings_share_one_causal_chain(bound_session_local, spelling) -> None:
    """A later delivery must wait behind an earlier one however the id is written."""
    later = booking_updated()
    later["uid"] = _SAME_UUID_SPELLINGS[spelling]

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-early")
            await _capture(session, later, event_hint="booking-updated", payload_hash="h-late")

    # The earlier row is claimed and held, exactly like a worker mid-flight.
    async with bound_session_local() as first:
        async with first.begin():
            claimed = await worker.claim_next_event(first)
            assert claimed is not None
            assert claimed.payload_hash == "h-early"

            async with bound_session_local() as second:
                async with second.begin():
                    concurrent = await worker.claim_next_event(second)
                    assert concurrent is None, (
                        f"the {spelling} spelling of the same booking was claimed out of order "
                        f"({concurrent.payload_hash if concurrent else None})"
                    )


@pytest.mark.parametrize("spelling", sorted(_SAME_UUID_SPELLINGS))
async def test_a_retrying_earlier_event_blocks_every_spelling(bound_session_local, spelling) -> None:
    """The real regression: transient retry must not let a later delivery pass."""
    later = booking_updated()
    later["uid"] = _SAME_UUID_SPELLINGS[spelling]

    async with bound_session_local() as session:
        async with session.begin():
            early_id = await _capture(session, booking_created(), payload_hash="h-early")
            await _capture(session, later, event_hint="booking-updated", payload_hash="h-late")

    # The earlier row failed transiently and is waiting for its retry window.
    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == early_id))).scalars().one()
            row.next_retry_at = worker.utcnow() + timedelta(minutes=5)
            row.processing_attempts = 1

    async with bound_session_local() as session:
        async with session.begin():
            claimed = await worker.claim_next_event(session)
            assert claimed is None, "a later delivery overtook an earlier retrying one"


async def test_recovered_earlier_event_is_applied_before_the_later_one(bound_session_local) -> None:
    """After recovery the two deliveries apply in CAPTURE order, not retry order."""
    later = booking_updated()
    later["uid"] = TEST_BOOKING_UUID.upper()
    later["service_id"] = 777
    later["services_description"] = "Later description"

    async with bound_session_local() as session:
        async with session.begin():
            early_id = await _capture(session, booking_created(), payload_hash="h-early")
            await _capture(session, later, event_hint="booking-updated", payload_hash="h-late")

    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == early_id))).scalars().one()
            row.next_retry_at = worker.utcnow() - timedelta(seconds=1)  # window elapsed
            row.processing_attempts = 2

    await _run_until_idle()

    async with bound_session_local() as session:
        rows = (await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))).scalars().all()
        assert [r.status for r in rows] == ["processed", "processed"]
        # Terminal rows never keep a retry timestamp.
        assert all(r.next_retry_at is None for r in rows)
        # The attempt history of the recovered row is preserved.
        assert rows[0].processing_attempts == 2

    # Final state reflects the LAST delivery.
    _total, service_id, title, _amount, _cost = await _snapshot_row(bound_session_local)
    assert service_id == 777
    assert title == "Later description"


async def test_a_malformed_uid_neither_blocks_nor_is_blocked(bound_session_local) -> None:
    """A row with no canonical UUID must not stall the backlog."""
    broken = booking_created()
    broken["uid"] = "not-a-uuid"
    other = booking_created()
    other["uid"] = "bbbbbbbb-1111-4222-8333-444444444444"
    other["id"] = TEST_BOOKING_ID + 900

    async with bound_session_local() as session:
        async with session.begin():
            broken_id = await _capture(session, broken, payload_hash="h-broken")
            await _capture(session, other, payload_hash="h-other")

    async with bound_session_local() as session:
        row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == broken_id))).scalars().one()
        assert row.booking_uuid is None, "a malformed uid must not produce a canonical key"

    await _run_until_idle()

    async with bound_session_local() as session:
        rows = {r.payload_hash: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        # The malformed one reached its deterministic failure ...
        assert rows["h-broken"].status == "failed"
        assert rows["h-broken"].error_code == NormalizationError.INVALID_BOOKING_UUID
        # ... and the unrelated booking was processed regardless.
        assert rows["h-other"].status == "processed"
        assert all(r.next_retry_at is None for r in rows.values())


async def test_different_bookings_still_claim_in_parallel(bound_session_local) -> None:
    """The serialisation is per booking, never global."""
    other = booking_created()
    other["uid"] = "cccccccc-1111-4222-8333-444444444444"
    other["id"] = TEST_BOOKING_ID + 901

    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-x")
            await _capture(session, other, payload_hash="h-y")

    async with bound_session_local() as first:
        async with first.begin():
            claimed = await worker.claim_next_event(first)
            assert claimed is not None and claimed.payload_hash == "h-x"
            async with bound_session_local() as second:
                async with second.begin():
                    concurrent = await worker.claim_next_event(second)
                    assert concurrent is not None and concurrent.payload_hash == "h-y"


# ===========================================================================
# Review fix 2 — a service-id-only update keeps the price consistent
# ===========================================================================


async def _assert_price_invariant(bound_session_local) -> None:
    """Record.total_cost and RecordService.cost_to_pay are ONE snapshot."""
    async with bound_session_local() as session:
        rows = (
            await session.execute(
                select(Record.total_cost, RecordService.cost_to_pay)
                .join(RecordService, RecordService.record_id == Record.id)
                .where(Record.provider == "easyweek")
            )
        ).all()
    for total_cost, cost_to_pay in rows:
        assert total_cost == cost_to_pay, f"price snapshot diverged: record={total_cost} service={cost_to_pay}"


async def test_service_id_change_without_a_price_keeps_the_known_total(bound_session_local) -> None:
    """The regression: a new service row must not start with an unknown price.

    PR-5 renders from the service row, so a NULL there next to a known
    Record.total_cost would print 0.00 for a booking whose price is proven.
    """
    created = booking_created()
    created["service_id"] = 111
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    updated = booking_updated()
    updated["service_id"] = 222
    updated.pop("booking_price_int", None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, updated, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, service_id, _title, _amount, cost = await _snapshot_row(bound_session_local)
    assert service_id == 222, "the new service identity was not adopted"
    assert total == Decimal("35.00")
    assert cost == Decimal("35.00"), "the proven booking total was not carried to the new service row"
    await _assert_price_invariant(bound_session_local)

    # Exactly one snapshot row: the old service was removed, not duplicated.
    async with bound_session_local() as session:
        services = (await session.execute(select(RecordService))).scalars().all()
        assert len(services) == 1


async def test_service_id_change_with_a_new_price_updates_both(bound_session_local) -> None:
    created = booking_created()
    created["service_id"] = 111
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    updated = booking_updated()
    updated["service_id"] = 222
    updated["booking_price_int"] = 5000
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, updated, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, service_id, _title, _amount, cost = await _snapshot_row(bound_session_local)
    assert service_id == 222
    assert total == Decimal("50.00")
    assert cost == Decimal("50.00")
    await _assert_price_invariant(bound_session_local)


async def test_service_id_change_with_an_explicit_price_clear_clears_both(bound_session_local) -> None:
    created = booking_created()
    created["service_id"] = 111
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    updated = booking_updated()
    updated["service_id"] = 222
    updated["booking_price_int"] = None
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, updated, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, service_id, _title, _amount, cost = await _snapshot_row(bound_session_local)
    assert service_id == 222
    assert total is None
    assert cost is None
    await _assert_price_invariant(bound_session_local)


async def test_service_id_change_without_any_known_total_stays_null(bound_session_local) -> None:
    created = booking_created()
    created["service_id"] = 111
    created.pop("booking_price_int", None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    updated = booking_updated()
    updated["service_id"] = 222
    updated.pop("booking_price_int", None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, updated, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, _service_id, _title, _amount, cost = await _snapshot_row(bound_session_local)
    assert total is None and cost is None
    await _assert_price_invariant(bound_session_local)


async def test_a_service_id_only_resend_does_not_duplicate_the_snapshot(bound_session_local) -> None:
    created = booking_created()
    created["service_id"] = 111
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    updated = booking_updated()
    updated["service_id"] = 222
    updated.pop("booking_price_int", None)
    for payload_hash in ("h2", "h3"):
        async with bound_session_local() as session:
            async with session.begin():
                await _capture(session, updated, event_hint="booking-updated", payload_hash=payload_hash)
        await _run_until_idle()

    async with bound_session_local() as session:
        services = (await session.execute(select(RecordService))).scalars().all()
        assert len(services) == 1
    await _assert_price_invariant(bound_session_local)


# ===========================================================================
# Review fixes 3 and 4 — presence decides the fallback, not truthiness
# ===========================================================================


async def test_a_present_set_description_wins_even_when_a_name_is_carried(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["services_description"] = "Set description"
    event["service_name"] = "Single"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, title, _a, _c = await _snapshot_row(bound_session_local)
    assert title == "Set description"


async def test_a_singular_name_is_used_only_when_the_set_field_is_absent(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event.pop("services_description", None)
    event["service_name"] = "Single"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, title, _a, _c = await _snapshot_row(bound_session_local)
    assert title == "Single"


async def test_both_title_fields_absent_preserves_the_known_title(bound_session_local) -> None:
    created = booking_created()
    created["services_description"] = "Original set"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event.pop("services_description", None)
    event.pop("service_name", None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, title, _a, _c = await _snapshot_row(bound_session_local)
    assert title == "Original set"


@pytest.mark.parametrize("cleared", [None, 0])
async def test_a_cleared_set_count_wins_over_a_carried_quantity(bound_session_local, cleared) -> None:
    """`services_count` is the whole-set field; an explicit clear is authoritative."""
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["services_count"] = cleared
    event["quantity"] = 1
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, _title, amount, _c = await _snapshot_row(bound_session_local)
    assert amount == cleared


async def test_a_present_set_count_wins_over_quantity(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["services_count"] = 2
    event["quantity"] = 1
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, _title, amount, _c = await _snapshot_row(bound_session_local)
    assert amount == 2


async def test_quantity_is_used_only_when_the_set_count_is_absent(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event.pop("services_count", None)
    event["quantity"] = 1
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, _title, amount, _c = await _snapshot_row(bound_session_local)
    assert amount == 1


async def test_both_count_fields_absent_preserves_the_known_amount(bound_session_local) -> None:
    created = booking_created()
    created["services_count"] = 3
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event.pop("services_count", None)
    event.pop("quantity", None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    _t, _s, _title, amount, _c = await _snapshot_row(bound_session_local)
    assert amount == 3


# ===========================================================================
# Review fix 5 — every terminal transition clears next_retry_at
# ===========================================================================


async def _mark_transiently_failed(bound_session_local, event_id: int) -> None:
    """Put a row in the state a transient failure leaves behind."""
    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
            row.processing_attempts = 3
            row.next_retry_at = worker.utcnow() - timedelta(seconds=1)


async def _terminal_row(bound_session_local, event_id: int) -> EasyWeekEvent:
    async with bound_session_local() as session:
        return (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()


async def test_recovered_normal_processing_clears_the_retry_timestamp(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, booking_created(), payload_hash="h1")
    await _mark_transiently_failed(bound_session_local, event_id)
    await _run_until_idle()

    row = await _terminal_row(bound_session_local, event_id)
    assert row.status == "processed"
    assert row.processed_at is not None
    assert row.error_code is None
    assert row.next_retry_at is None
    assert row.processing_attempts == 3, "attempt history is audit data and is kept"


async def test_recovered_booking_succeeded_clears_the_retry_timestamp(bound_session_local) -> None:
    """An early-return terminal branch used to keep a future retry timestamp."""
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(
                session, booking_created(), event_hint="booking-succeeded", payload_hash="h-succeeded"
            )
    await _mark_transiently_failed(bound_session_local, event_id)
    await _run_until_idle()

    row = await _terminal_row(bound_session_local, event_id)
    assert row.status == "processed"
    assert row.processed_at is not None
    assert row.error_code is None
    assert row.next_retry_at is None


async def test_recovered_exact_replay_clears_the_retry_timestamp(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, booking_created(), payload_hash="h-same")
    await _run_until_idle()

    async with bound_session_local() as session:
        async with session.begin():
            replay_id = await _capture(session, booking_created(), payload_hash="h-same")
    await _mark_transiently_failed(bound_session_local, replay_id)
    await _run_until_idle()

    row = await _terminal_row(bound_session_local, replay_id)
    assert row.status == "processed"
    assert row.next_retry_at is None
    assert row.error_code is None


async def test_recovered_deterministic_failure_clears_the_retry_timestamp(bound_session_local) -> None:
    broken = booking_created()
    broken["uid"] = "not-a-uuid"
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, broken, payload_hash="h-broken")
    await _mark_transiently_failed(bound_session_local, event_id)
    await _run_until_idle()

    row = await _terminal_row(bound_session_local, event_id)
    assert row.status == "failed"
    assert row.processed_at is not None
    assert row.error_code == NormalizationError.INVALID_BOOKING_UUID
    assert row.next_retry_at is None


async def test_no_terminal_row_ever_looks_like_it_is_waiting(bound_session_local) -> None:
    """The runbook query for "terminal but waiting" must return nothing.

    SELECT ... WHERE status IN ('processed','failed') AND next_retry_at IS NOT NULL
    """
    broken = booking_created()
    broken["uid"] = "not-a-uuid"
    succeeded = booking_created()
    succeeded["uid"] = "dddddddd-1111-4222-8333-444444444444"

    async with bound_session_local() as session:
        async with session.begin():
            ids = [
                await _capture(session, booking_created(), payload_hash="h1"),
                await _capture(session, broken, payload_hash="h2"),
                await _capture(session, succeeded, event_hint="booking-succeeded", payload_hash="h3"),
            ]
    for event_id in ids:
        await _mark_transiently_failed(bound_session_local, event_id)
    await _run_until_idle()

    async with bound_session_local() as session:
        waiting = (
            await session.execute(
                select(EasyWeekEvent.id)
                .where(EasyWeekEvent.status.in_(("processed", "failed")))
                .where(EasyWeekEvent.next_retry_at.is_not(None))
            )
        ).all()
    assert waiting == [], "a terminal row still carries next_retry_at"


# ===========================================================================
# P1 — the rollout gap between the migration backfill and the new API image
# ===========================================================================
#
# Production applies the migration while the OLD `altegio-api` container is
# still accepting webhooks. That image has no `booking_uuid` in its model, so a
# delivery landing after the backfill but before the API is recreated is stored
# with a valid `uid` and a NULL canonical key — invisible to causal ordering.


async def _capture_legacy(session: AsyncSession, payload: dict[str, Any], **kwargs: Any) -> int:
    """A row exactly as the PRE-PR-4 image would have written it: key NULL."""
    event_id = await _capture(session, payload, **kwargs)
    await session.execute(update(EasyWeekEvent).where(EasyWeekEvent.id == event_id).values(booking_uuid=None))
    return event_id


async def _reconcile(bound_session_local) -> int:
    async with bound_session_local() as session:
        async with session.begin():
            return await worker.reconcile_missing_booking_uuid(session)


async def test_reconciliation_gives_legacy_rows_the_canonical_key(bound_session_local) -> None:
    """A legacy row and a later, differently-spelled one must share one key."""
    later = booking_updated()
    later["uid"] = TEST_BOOKING_UUID.upper()

    async with bound_session_local() as session:
        async with session.begin():
            legacy_id = await _capture_legacy(session, booking_created(), payload_hash="h-legacy")
            later_id = await _capture(session, later, event_hint="booking-updated", payload_hash="h-later")

    async with bound_session_local() as session:
        rows = {r.id: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        assert rows[legacy_id].booking_uuid is None, "precondition: the legacy row has no key"
        assert rows[later_id].booking_uuid is not None

    repaired = await _reconcile(bound_session_local)
    assert repaired == 1

    async with bound_session_local() as session:
        rows = {r.id: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        assert rows[legacy_id].booking_uuid == rows[later_id].booking_uuid, (
            "the legacy row and the later spelling must land in ONE causal chain"
        )
        # Capture data is untouched: exactly one column was written.
        assert rows[legacy_id].payload == booking_created()
        assert rows[legacy_id].payload_hash == "h-legacy"
        assert rows[legacy_id].status == "captured"


async def test_a_reconciled_legacy_row_blocks_its_later_sibling(bound_session_local) -> None:
    """The whole point: after reconciliation the ordering guard sees the row."""
    later = booking_updated()
    later["uid"] = "{" + TEST_BOOKING_UUID + "}"

    async with bound_session_local() as session:
        async with session.begin():
            legacy_id = await _capture_legacy(session, booking_created(), payload_hash="h-legacy")
            await _capture(session, later, event_hint="booking-updated", payload_hash="h-later")

    # BEFORE reconciliation the legacy row does not participate in ordering:
    # the later delivery is claimable even while the legacy one is pending.
    async with bound_session_local() as session:
        async with session.begin():
            unordered = await worker.claim_next_event(session)
            assert unordered is not None
            # Roll the claim back so the state is untouched for the real check.
            await session.rollback()

    await _reconcile(bound_session_local)

    # The legacy row failed transiently and is waiting for its retry window.
    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == legacy_id))).scalars().one()
            row.next_retry_at = worker.utcnow() + timedelta(minutes=5)
            row.processing_attempts = 1

    async with bound_session_local() as session:
        async with session.begin():
            blocked = await worker.claim_next_event(session)
            assert blocked is None, "the later delivery overtook a reconciled legacy row"


async def test_after_recovery_the_later_delivery_wins(bound_session_local) -> None:
    """Final domain state must reflect the LAST delivery, not the recovered one."""
    later = booking_updated()
    later["uid"] = TEST_BOOKING_UUID.upper()
    later["service_id"] = 909
    later["services_description"] = "Later set"

    async with bound_session_local() as session:
        async with session.begin():
            legacy_id = await _capture_legacy(session, booking_created(), payload_hash="h-legacy")
            await _capture(session, later, event_hint="booking-updated", payload_hash="h-later")

    await _reconcile(bound_session_local)

    async with bound_session_local() as session:
        async with session.begin():
            row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == legacy_id))).scalars().one()
            row.next_retry_at = worker.utcnow() - timedelta(seconds=1)
            row.processing_attempts = 2

    await _run_until_idle()

    async with bound_session_local() as session:
        rows = (await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))).scalars().all()
        assert [r.status for r in rows] == ["processed", "processed"]
        assert all(r.next_retry_at is None for r in rows)

    _total, service_id, title, _amount, _cost = await _snapshot_row(bound_session_local)
    assert service_id == 909
    assert title == "Later set"


async def test_reconciliation_leaves_malformed_rows_null(bound_session_local) -> None:
    broken = booking_created()
    broken["uid"] = "not-a-uuid"
    missing = booking_created()
    missing.pop("uid", None)

    async with bound_session_local() as session:
        async with session.begin():
            broken_id = await _capture_legacy(session, broken, payload_hash="h-broken")
            missing_id = await _capture_legacy(session, missing, payload_hash="h-missing")

    repaired = await _reconcile(bound_session_local)
    assert repaired == 0, "nothing parseable, so nothing to repair"

    async with bound_session_local() as session:
        rows = {r.id: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        assert rows[broken_id].booking_uuid is None
        assert rows[missing_id].booking_uuid is None

    # They still reach their deterministic failure rather than stalling.
    await _run_until_idle()
    async with bound_session_local() as session:
        rows = {r.id: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        assert rows[broken_id].status == "failed"
        assert rows[broken_id].error_code == NormalizationError.INVALID_BOOKING_UUID
        assert rows[missing_id].status == "failed"
        assert rows[missing_id].error_code == NormalizationError.MISSING_BOOKING_UUID
        assert all(r.next_retry_at is None for r in rows.values())


async def test_reconciliation_is_idempotent_and_bounded(bound_session_local) -> None:
    """A second pass finds nothing, and the scan is keyset-paginated."""
    async with bound_session_local() as session:
        async with session.begin():
            for index in range(5):
                payload = booking_created()
                payload["uid"] = f"aaaaaaaa-0000-4000-8000-00000000000{index}"
                payload["id"] = TEST_BOOKING_ID + 1000 + index
                await _capture_legacy(session, payload, payload_hash=f"h{index}")

    assert await _reconcile(bound_session_local) == 5
    assert await _reconcile(bound_session_local) == 0

    # The scan must not load the table whole.
    import inspect

    source = inspect.getsource(worker.reconcile_missing_booking_uuid)
    assert "EasyWeekEvent.id > last_id" in source or "id > last_id" in source
    assert ".limit(RECONCILE_BATCH)" in source
    assert "offset" not in source.lower()


async def test_the_loop_refuses_to_claim_until_reconciliation_succeeds(bound_session_local, monkeypatch) -> None:
    """Fail-closed: a broken reconciliation must not let claiming start."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture_legacy(session, booking_created(), payload_hash="h1")

    claims: list[int] = []

    async def never_called() -> bool:
        claims.append(1)
        return False

    async def boom(session: AsyncSession) -> int:
        raise RuntimeError("reconciliation unavailable")

    monkeypatch.setattr(worker, "process_one", never_called)
    monkeypatch.setattr(worker, "reconcile_missing_booking_uuid", boom)

    stop_event = asyncio.Event()

    async def stop_soon() -> None:
        await asyncio.sleep(0.15)
        stop_event.set()

    await asyncio.wait_for(
        asyncio.gather(worker.run_loop(poll_sec=0.01, stop_event=stop_event), stop_soon()),
        timeout=10,
    )

    assert claims == [], "the worker claimed events despite a failed reconciliation"
    # And the row was left exactly as captured.
    async with bound_session_local() as session:
        row = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert row.status == "captured"
        assert row.booking_uuid is None


async def test_the_loop_reconciles_once_then_claims(bound_session_local, monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture_legacy(session, booking_created(), payload_hash="h1")

    passes: list[int] = []
    real_reconcile = worker.reconcile_missing_booking_uuid

    async def counting(session: AsyncSession) -> int:
        passes.append(1)
        return await real_reconcile(session)

    monkeypatch.setattr(worker, "reconcile_missing_booking_uuid", counting)

    stop_event = asyncio.Event()

    async def stop_soon() -> None:
        await asyncio.sleep(0.3)
        stop_event.set()

    await asyncio.wait_for(
        asyncio.gather(worker.run_loop(poll_sec=0.01, stop_event=stop_event), stop_soon()),
        timeout=10,
    )

    assert len(passes) == 1, f"reconciliation ran {len(passes)} times; it must run once per process"
    async with bound_session_local() as session:
        row = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert row.booking_uuid is not None
        assert row.status == "processed"


# ===========================================================================
# P2 — `service_id` is identity and has no explicit-clear semantics
# ===========================================================================


@pytest.mark.parametrize(
    ("bad_value", "expected_code"),
    [
        # "not a number at all" -> invalid_payload
        (None, NormalizationError.INVALID_PAYLOAD),
        (True, NormalizationError.INVALID_PAYLOAD),
        (False, NormalizationError.INVALID_PAYLOAD),
        ("10", NormalizationError.INVALID_PAYLOAD),
        ("", NormalizationError.INVALID_PAYLOAD),
        (1.5, NormalizationError.INVALID_PAYLOAD),
        # "a number, but out of the column's range" -> invalid_numeric_range.
        # The same distinction every other id uses (`id`, `customer_id`,
        # `location_id`); service_id must not invent a different convention.
        (0, NormalizationError.INVALID_NUMERIC_RANGE),
        (-1, NormalizationError.INVALID_NUMERIC_RANGE),
    ],
    ids=["null", "true", "false", "string", "empty", "fraction", "zero", "negative"],
)
async def test_an_unusable_service_id_fails_deterministically(bound_session_local, bad_value, expected_code) -> None:
    """No captured payload proves what `service_id: null` means, so refuse it.

    Silently keeping the old identity would attach the NEW title, amount and
    price to the OLD service_id; deleting the snapshot would destroy a proven
    one. Both are guesses, so the delivery is rejected and nothing is written.
    """
    created = booking_created()
    created["service_id"] = 10
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    before = await _snapshot_row(bound_session_local)
    async with bound_session_local() as session:
        service_count_before = len((await session.execute(select(RecordService))).scalars().all())

    event = booking_updated()
    event["service_id"] = bad_value
    event["services_description"] = "Should never be applied"
    event["services_count"] = 9
    event["booking_price_int"] = 9900
    async with bound_session_local() as session:
        async with session.begin():
            event_id = await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    async with bound_session_local() as session:
        row = (await session.execute(select(EasyWeekEvent).where(EasyWeekEvent.id == event_id))).scalars().one()
        assert row.status == "failed"
        assert row.error_code == expected_code
        assert row.next_retry_at is None
        assert row.processed_at is not None

    # Nothing in the domain moved.
    assert await _snapshot_row(bound_session_local) == before
    async with bound_session_local() as session:
        assert len((await session.execute(select(RecordService))).scalars().all()) == service_count_before


async def test_an_absent_service_id_still_preserves_identity(bound_session_local) -> None:
    created = booking_created()
    created["service_id"] = 10
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event.pop("service_id", None)
    event["services_description"] = "Same service, new text"
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, service_id, title, _amount, cost = await _snapshot_row(bound_session_local)
    assert service_id == 10, "an absent service_id must keep the known identity"
    assert title == "Same service, new text"
    assert total == cost == Decimal("35.00")


async def test_a_valid_service_id_change_still_works(bound_session_local) -> None:
    created = booking_created()
    created["service_id"] = 10
    created["booking_price_int"] = 3500
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, created, payload_hash="h1")
    await _run_until_idle()

    event = booking_updated()
    event["service_id"] = 20
    event.pop("booking_price_int", None)
    async with bound_session_local() as session:
        async with session.begin():
            await _capture(session, event, event_hint="booking-updated", payload_hash="h2")
    await _run_until_idle()

    total, service_id, _title, _amount, cost = await _snapshot_row(bound_session_local)
    assert service_id == 20
    assert total == cost == Decimal("35.00")
    await _assert_price_invariant(bound_session_local)


# ===========================================================================
# Loop-level backoff: the fail-closed loop regression
# ===========================================================================


async def test_a_persistently_failing_reconciliation_never_overflows_or_claims(
    bound_session_local, monkeypatch
) -> None:
    """Fail-closed AND crash-free, however long the outage lasts."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)

    async with bound_session_local() as session:
        async with session.begin():
            await _capture_legacy(session, booking_created(), payload_hash="h1")

    claims: list[int] = []
    delays: list[float] = []

    async def never_called() -> bool:
        claims.append(1)
        return False

    async def always_broken(session: AsyncSession) -> int:
        raise RuntimeError("dependency down")

    real_sleep = worker._sleep_unless_stopping

    async def record_sleep(delay: float, stop_event) -> None:
        delays.append(delay)
        await real_sleep(0, stop_event)

    monkeypatch.setattr(worker, "process_one", never_called)
    monkeypatch.setattr(worker, "reconcile_missing_booking_uuid", always_broken)
    monkeypatch.setattr(worker, "_sleep_unless_stopping", record_sleep)

    stop_event = asyncio.Event()

    async def stop_soon() -> None:
        await asyncio.sleep(0.25)
        stop_event.set()

    # No OverflowError even after many consecutive failures.
    await asyncio.wait_for(
        asyncio.gather(worker.run_loop(poll_sec=0.001, stop_event=stop_event), stop_soon()),
        timeout=15,
    )

    assert claims == [], "the worker claimed events despite a failing reconciliation"
    assert delays, "the error path never backed off"
    assert all(math.isfinite(d) and 0 < d <= worker.MAX_ERROR_BACKOFF_SEC for d in delays)

    async with bound_session_local() as session:
        row = (await session.execute(select(EasyWeekEvent))).scalars().one()
        assert row.status == "captured"
        assert row.booking_uuid is None


# ===========================================================================
# Malformed string UIDs are candidates, not repairable rows
# ===========================================================================


async def test_a_smoke_uid_is_a_candidate_but_is_never_repaired(bound_session_local) -> None:
    """The real class of value that broke the operator's candidate == repaired check.

    `public-deploy-smoke-<uuid>` is a JSON string, so it is counted by the
    step-3c candidate query, but `uuid.UUID()` cannot parse it — so it is never
    repaired. The runbook must not present those two numbers as equal.
    """
    smoke = booking_created()
    smoke["uid"] = f"public-deploy-smoke-{TEST_BOOKING_UUID}"
    good = booking_created()
    good["uid"] = "eeeeeeee-1111-4222-8333-444444444444"
    good["id"] = TEST_BOOKING_ID + 1500

    async with bound_session_local() as session:
        async with session.begin():
            smoke_id = await _capture_legacy(session, smoke, payload_hash="h-smoke")
            good_id = await _capture_legacy(session, good, payload_hash="h-good")

    # Both are string candidates by the step-3c query ...
    async with bound_session_local() as session:
        candidates = (
            await session.execute(
                select(func.count())
                .select_from(EasyWeekEvent)
                .where(EasyWeekEvent.booking_uuid.is_(None))
                .where(func.jsonb_typeof(EasyWeekEvent.payload["uid"]) == "string")
            )
        ).scalar_one()
    assert candidates == 2

    # ... but only one of them can be repaired.
    repaired = await _reconcile(bound_session_local)
    assert repaired == 1, "a malformed string uid must not be counted as repaired"
    assert repaired < candidates

    async with bound_session_local() as session:
        rows = {r.id: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        assert rows[smoke_id].booking_uuid is None
        assert rows[good_id].booking_uuid is not None
        # Raw capture untouched.
        assert rows[smoke_id].payload["uid"] == smoke["uid"]

    # The smoke row still reaches a safe deterministic failure, and the valid
    # delivery behind it keeps being processed.
    await _run_until_idle()
    async with bound_session_local() as session:
        rows = {r.id: r for r in (await session.execute(select(EasyWeekEvent))).scalars().all()}
        assert rows[smoke_id].status == "failed"
        assert rows[smoke_id].error_code == NormalizationError.INVALID_BOOKING_UUID
        assert rows[smoke_id].next_retry_at is None
        assert rows[good_id].status == "processed"


async def test_reconciliation_survives_a_batch_of_only_malformed_strings(bound_session_local) -> None:
    async with bound_session_local() as session:
        async with session.begin():
            for index in range(4):
                payload = booking_created()
                payload["uid"] = f"public-deploy-smoke-{index}"
                payload["id"] = TEST_BOOKING_ID + 1600 + index
                await _capture_legacy(session, payload, payload_hash=f"hs{index}")

    assert await _reconcile(bound_session_local) == 0

    async with bound_session_local() as session:
        rows = (await session.execute(select(EasyWeekEvent))).scalars().all()
        assert all(r.booking_uuid is None for r in rows)
