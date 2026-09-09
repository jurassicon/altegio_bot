"""PostgreSQL/read-only contract for the PR-7.4 aggregate preflight."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_CATALOG_MATCH_MISSING,
    MULTI_SERVICE_CATEGORY_AMBIGUOUS,
    MULTI_SERVICE_CURRENCY_MISMATCH,
    MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED,
    MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED,
    MULTI_SERVICE_DISCOUNT_UNSUPPORTED,
    MULTI_SERVICE_DUPLICATE_AMBIGUOUS,
    MULTI_SERVICE_ORDERED_SERVICES_MALFORMED,
    MULTI_SERVICE_QUANTITY_UNSUPPORTED,
    MULTI_SERVICE_TOTAL_MISMATCH,
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


def _api(
    *,
    custom_duration: bool = False,
    malformed: bool = False,
    resource_shadow: bool = False,
) -> dict[str, Any]:
    first = _line("11111111-1111-4111-8111-111111111111", "Fixture Service", 3500, 30)
    second = _line("22222222-2222-4222-8222-222222222222", "Second Fixture Service", 4500, 45)
    if custom_duration:
        second["duration"] = {"value": 75, "label": "minutes"}
    ordered_services: object = [first, second]
    if resource_shadow:
        shadow = copy.deepcopy(second)
        shadow["uuid"] = "33333333-3333-4333-8333-333333333333"
        ordered_services.insert(1, shadow)
    if malformed:
        ordered_services = "not-a-list"
    return {
        "uuid": TEST_BOOKING_UUID,
        "location_uuid": TEST_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": 8000, "total": 8000},
        "ordered_services": ordered_services,
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
    def __init__(
        self,
        *,
        custom_duration: bool = False,
        malformed: bool = False,
        booking: dict[str, Any] | None = None,
        catalog_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.booking_calls = 0
        self.catalog_calls = 0
        self.custom_duration = custom_duration
        self.malformed = malformed
        self.booking = booking
        self.catalog_rows = catalog_rows

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        assert booking_uuid == TEST_BOOKING_UUID
        self.booking_calls += 1
        return self.booking or _api(custom_duration=self.custom_duration, malformed=self.malformed)

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert (location_uuid, page) == (TEST_LOCATION_UUID, 1)
        self.catalog_calls += 1
        rows = self.catalog_rows or _catalog()
        return {
            "data": rows,
            "meta": {"current_page": 1, "last_page": 1, "total": len(rows)},
        }


async def _seed_active_pair(
    session,
    *,
    with_snapshot: bool = False,
    embedded_snapshot: bool = False,
    malformed_embedded: bool = False,
    stale_job: bool = False,
    unsafe_open_job: bool = False,
    non_terminal_outbox: bool = False,
) -> None:
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
    if with_snapshot or embedded_snapshot:
        job_payload = multi_service_job_payload(snapshot, include_snapshot=embedded_snapshot)
        if embedded_snapshot:
            job_payload.update(
                {
                    "multi_service_recovery_plan_digest": "a" * 64,
                    "multi_service_recovery_snapshot_version": 1,
                }
            )
        if malformed_embedded:
            job_payload["multi_service_snapshot"] = None
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
    if unsafe_open_job or non_terminal_outbox:
        unsafe_job = MessageJob(
            provider="easyweek",
            company_id=TEST_LOCATION_ID,
            record_id=record.id,
            client_id=client.id,
            job_type="reminder_24h",
            run_at=utcnow(),
            status="queued",
            dedupe_key=f"unsafe-contract-excluded-{record.id}",
            payload={},
        )
        session.add(unsafe_job)
        await session.flush()
        if non_terminal_outbox:
            session.add(
                OutboxMessage(
                    company_id=TEST_LOCATION_ID,
                    client_id=client.id,
                    record_id=record.id,
                    job_id=unsafe_job.id,
                    phone_e164="+49000000000",
                    template_code="reminder_24h",
                    body="fixture",
                    status="queued",
                    scheduled_at=utcnow(),
                    meta={},
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
        "contract_excluded": 0,
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


async def test_embedded_only_recovery_job_is_ready_without_record_backfill(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session, embedded_snapshot=True)

    async with session_maker() as session:
        record = (await session.execute(select(Record))).scalar_one()
        assert "multi_service_snapshot" not in record.raw["easyweek"]
        report = await run_preflight(session, client=FakeReader(), sleep=_no_sleep)

    assert report.open_jobs == report.jobs_held_by_send_fence == 1
    assert report.stale_snapshot_digest == 0
    assert report.unexplained == 0
    assert report.ready is True


async def test_malformed_embedded_recovery_snapshot_is_stale_and_not_ready(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session, embedded_snapshot=True, malformed_embedded=True)

    async with session_maker() as session:
        report = await run_preflight(session, client=FakeReader(), sleep=_no_sleep)

    assert report.open_jobs == report.jobs_held_by_send_fence == 1
    assert report.stale_snapshot_digest == report.unexplained == 1
    assert report.reasons == {"multi_service_snapshot_digest_mismatch": 1}
    assert report.ready is False


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


async def test_custom_duration_is_a_clean_contract_exclusion(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session)
        report = await run_preflight(
            session,
            client=FakeReader(custom_duration=True),
            sleep=_no_sleep,
        )

    assert report.contract_excluded == 1
    assert report.structurally_proven == report.allowed == report.disallowed_by_category == 0
    assert report.ambiguous == report.unexplained == 0
    assert report.reasons == {MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED: 1}
    assert report.ready is True


@pytest.mark.parametrize(
    ("damage", "expected_reason"),
    [
        ("total", MULTI_SERVICE_TOTAL_MISMATCH),
        ("currency", MULTI_SERVICE_CURRENCY_MISMATCH),
        ("custom_price", MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED),
        ("discount", MULTI_SERVICE_DISCOUNT_UNSUPPORTED),
        ("quantity", MULTI_SERVICE_QUANTITY_UNSUPPORTED),
        ("missing_catalog", MULTI_SERVICE_CATALOG_MATCH_MISSING),
        ("ambiguous_catalog", MULTI_SERVICE_CATEGORY_AMBIGUOUS),
        ("malformed", MULTI_SERVICE_ORDERED_SERVICES_MALFORMED),
        ("duplicate", MULTI_SERVICE_DUPLICATE_AMBIGUOUS),
        ("third_service", MULTI_SERVICE_DUPLICATE_AMBIGUOUS),
    ],
)
async def test_custom_duration_with_a_second_error_remains_unexplained(
    session_maker,
    damage: str,
    expected_reason: str,
) -> None:
    booking = _api()
    services = booking["ordered_services"]
    assert isinstance(services, list)
    services[0]["duration"] = {"value": 60, "label": "minutes"}
    catalog = _catalog()
    if damage == "total":
        booking["order"]["total"] = 7900
    elif damage == "currency":
        booking["currency"] = "USD"
    elif damage == "custom_price":
        services[1]["original_price"] = 4600
    elif damage == "discount":
        services[1]["discount"] = 1
    elif damage == "quantity":
        services[1]["quantity"] = 2
    elif damage == "missing_catalog":
        catalog = catalog[:1]
    elif damage == "ambiguous_catalog":
        duplicate = copy.deepcopy(catalog[1])
        duplicate["uuid"] = "aaaaaaaa-3333-4333-8333-333333333333"
        duplicate["category"] = {"name": "Other"}
        catalog.append(duplicate)
    elif damage == "malformed":
        services[1] = "not-a-service-line"
    elif damage == "duplicate":
        services[1] = copy.deepcopy(services[0])
        services[1]["uuid"] = "22222222-2222-4222-8222-222222222222"
    else:
        services.append(_line("33333333-3333-4333-8333-333333333333", "Third Fixture Service", 0, 15))

    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session)
        report = await run_preflight(
            session,
            client=FakeReader(booking=booking, catalog_rows=catalog),
            sleep=_no_sleep,
        )

    assert report.contract_excluded == 0
    assert report.ambiguous == report.unexplained == 1
    assert report.reasons == {expected_reason: 1}
    assert report.ready is False


async def test_resource_shadow_with_only_custom_duration_is_a_clean_exclusion(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session)
        report = await run_preflight(
            session,
            client=FakeReader(booking=_api(custom_duration=True, resource_shadow=True)),
            sleep=_no_sleep,
        )

    assert report.contract_excluded == 1
    assert report.ambiguous == report.unexplained == 0
    assert report.reasons == {MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED: 1}
    assert report.ready is True


@pytest.mark.parametrize("with_outbox", [False, True])
async def test_contract_exclusion_with_open_queue_is_not_ready(session_maker, with_outbox: bool) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(
                session,
                unsafe_open_job=True,
                non_terminal_outbox=with_outbox,
            )
        report = await run_preflight(
            session,
            client=FakeReader(custom_duration=True),
            sleep=_no_sleep,
        )

    assert report.contract_excluded == 1
    assert report.open_jobs == 1
    assert report.unexplained >= 1
    assert report.reasons["contract_excluded_open_job"] == 1
    assert report.reasons["contract_excluded_non_terminal_outbox"] == int(with_outbox)
    assert report.ready is False


async def test_non_custom_duration_proof_error_remains_unexplained(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session)
        report = await run_preflight(session, client=FakeReader(malformed=True), sleep=_no_sleep)

    assert report.contract_excluded == 0
    assert report.ambiguous == report.unexplained == 1
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
