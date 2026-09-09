"""PostgreSQL/read-only contract for the PR-7.4 aggregate preflight."""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_multi_service import (
    WebhookServicePair,
    clear_multi_service_catalog_cache,
    multi_service_job_payload,
    prove_exactly_two_service_snapshot,
    record_raw_with_multi_service_snapshot,
)
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_service_category import record_raw_with_services_count
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, OutboxMessage, Record
from altegio_bot.scripts.easyweek_multi_service_preflight import (
    MultiServicePreflightReport,
    run_preflight,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_BOOKING_ID,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_created_multi_service,
    set_booking_price,
)
from altegio_bot.utils import utcnow

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_multi_service_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    clear_multi_service_catalog_cache()


def _webhook() -> dict[str, Any]:
    payload = booking_created_multi_service()
    payload["service_related"] = "Second Fixture Service"
    payload["services_description"] = "Fixture Service, Second Fixture Service"
    payload["services_count"] = 2
    payload["quantity"] = 2
    payload["booking_price_currency"] = "EUR"
    set_booking_price(payload, 8000)
    return payload


def _line(line_uuid: str, name: str, price: int, duration: int) -> dict[str, Any]:
    return {
        "uuid": line_uuid,
        "name": name,
        "currency": "EUR",
        "price": price,
        "original_price": price,
        "discount": 0,
        "quantity": 1,
        "duration": {"value": duration, "label": "minutes"},
        "original_duration": {"value": duration, "label": "minutes"},
    }


def _api() -> dict[str, Any]:
    return {
        "uuid": TEST_BOOKING_UUID,
        "location_uuid": TEST_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": 8000, "total": 8000},
        "ordered_services": [
            _line("11111111-1111-4111-8111-111111111111", "Fixture Service", 3500, 30),
            _line("22222222-2222-4222-8222-222222222222", "Second Fixture Service", 4500, 45),
        ],
    }


def _catalog() -> list[dict[str, Any]]:
    return [
        {
            "uuid": "aaaaaaaa-1111-4111-8111-111111111111",
            "name": "Fixture Service",
            "currency": "EUR",
            "price": 3500,
            "duration": {"value": 30, "label": "minutes"},
            "category": {"name": "Fixture Category"},
        },
        {
            "uuid": "aaaaaaaa-2222-4222-8222-222222222222",
            "name": "Second Fixture Service",
            "currency": "EUR",
            "price": 4500,
            "duration": {"value": 45, "label": "minutes"},
            "category": {"name": "Fixture Category"},
        },
    ]


class FakeReader:
    def __init__(self) -> None:
        self.booking_calls = 0
        self.catalog_calls = 0

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        assert booking_uuid == TEST_BOOKING_UUID
        self.booking_calls += 1
        return _api()

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert (location_uuid, page) == (TEST_LOCATION_UUID, 1)
        self.catalog_calls += 1
        rows = _catalog()
        return {
            "data": rows,
            "meta": {"current_page": 1, "last_page": 1, "total": len(rows)},
        }


async def _seed_active_pair(session, *, with_snapshot: bool = False, stale_job: bool = False) -> None:
    client = Client(
        provider="easyweek",
        company_id=TEST_LOCATION_ID,
        altegio_client_id=TEST_CUSTOMER_ID,
        display_name="Preflight fixture",
        phone_e164="+49000000000",
        raw={},
    )
    session.add(client)
    await session.flush()

    raw = record_raw_with_services_count({}, 2)
    snapshot = prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
            location_uuid=TEST_LOCATION_UUID,
            service_name="Fixture Service",
            service_related="Second Fixture Service",
            services_description="Fixture Service, Second Fixture Service",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=Decimal("80.00"),
        ),
        booking_payload=_api(),
        catalog_rows=_catalog(),
    )
    if with_snapshot:
        raw = record_raw_with_multi_service_snapshot(raw, snapshot)
    record = Record(
        provider="easyweek",
        company_id=TEST_LOCATION_ID,
        altegio_record_id=TEST_BOOKING_ID,
        easyweek_booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        client_id=client.id,
        starts_at=utcnow() + timedelta(days=3),
        total_cost=Decimal("80.00"),
        is_deleted=False,
        raw=raw,
    )
    session.add(record)
    await session.flush()

    payload = _webhook()
    session.add(
        EasyWeekEvent(
            status="processed",
            event_hint="booking-created",
            auth_via="query",
            payload_hash="multi-preflight-fixture",
            payload=payload,
            booking_uuid=canonical_booking_uuid(payload),
            body_truncated=False,
        )
    )
    if with_snapshot:
        job_payload = multi_service_job_payload(snapshot)
        if stale_job:
            job_payload["multi_service_snapshot_digest"] = "0" * 64
        session.add(
            MessageJob(
                provider="easyweek",
                company_id=TEST_LOCATION_ID,
                record_id=record.id,
                client_id=client.id,
                job_type="record_created",
                run_at=utcnow(),
                status="queued",
                dedupe_key=f"multi-preflight-job-{stale_job}",
                payload=job_payload,
            )
        )
    await session.flush()


async def _row_counts(session) -> tuple[int, int, int, int]:
    values = []
    for model in (Record, EasyWeekEvent, MessageJob, OutboxMessage):
        values.append(int((await session.execute(select(func.count()).select_from(model))).scalar_one()))
    return tuple(values)  # type: ignore[return-value]


async def _no_sleep(_seconds: float) -> None:
    return None


async def test_historical_pair_without_snapshot_is_proven_and_preflight_is_read_only(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session)
        before = await _row_counts(session)

    reader = FakeReader()
    async with session_maker() as session:
        report = await run_preflight(session, client=reader, sleep=_no_sleep)

    async with session_maker() as session:
        after = await _row_counts(session)
    assert before == after
    assert report.as_safe_dict() == {
        "mode": "read-only",
        "active_multi_service": 1,
        "checked": 1,
        "structurally_proven": 1,
        "allowed": 1,
        "disallowed_by_category": 0,
        "ambiguous": 0,
        "open_jobs": 0,
        "jobs_held_by_send_fence": 0,
        "stale_snapshot_digest": 0,
        "unexplained": 0,
        "truncated": False,
        "reasons": {},
        "ready": True,
    }
    assert (reader.booking_calls, reader.catalog_calls) == (1, 1)


async def test_matching_controlled_job_is_counted_as_held_by_send_fence(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session, with_snapshot=True)

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(), sleep=_no_sleep)

    assert report.open_jobs == report.jobs_held_by_send_fence == 1
    assert report.stale_snapshot_digest == report.unexplained == 0
    assert report.ready is True


async def test_stale_job_digest_is_counted_and_never_green(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session, with_snapshot=True, stale_job=True)

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(), sleep=_no_sleep)

    assert report.open_jobs == report.jobs_held_by_send_fence == 1
    assert report.stale_snapshot_digest == report.unexplained == 1
    assert report.reasons == {"multi_service_snapshot_digest_mismatch": 1}
    assert report.ready is False


async def test_empty_truncated_or_unexplained_reports_never_go_green() -> None:
    assert MultiServicePreflightReport().ready is False
    assert (
        MultiServicePreflightReport(
            active_multi_service=1,
            checked=1,
            structurally_proven=1,
            allowed=1,
            truncated=True,
        ).ready
        is False
    )
    assert (
        MultiServicePreflightReport(
            active_multi_service=1,
            checked=1,
            structurally_proven=1,
            allowed=1,
            unexplained=1,
        ).ready
        is False
    )
