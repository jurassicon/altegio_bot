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
from altegio_bot.easyweek_resource_shadow_contract import (
    KARLSRUHE_COMPANY_ID,
    KARLSRUHE_LOCATION_UUID,
    KARLSRUHE_NUMERIC_SERVICE_NAMES,
    KARLSRUHE_SERVICE_CATEGORY,
)
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


# ---------------------------------------------------------------------------
# PR-7.5: the Karlsruhe resource-shadow transition.
#
# These are the production shapes that today report
# `multi_service_duplicate_ambiguous`.  With the new fence open they must reach
# ordinary all-categories eligibility and end as
# `multi_service_category_not_allowed`, with zero jobs and zero outbox rows.
# ---------------------------------------------------------------------------

KARLSRUHE_BOOKING_UUID = "77777777-2222-4333-8444-555555555555"
KARLSRUHE_BOOKING_ID = 4200777
KARLSRUHE_CUSTOMER_ID = 7300777
KARLSRUHE_SHELLAC = "Maniküre mit Gel-Lack / Shellac"
KARLSRUHE_PEDIKUERE_GEL = "Pediküre mit Gel-Lack"
_KARLSRUHE_SHELLAC_ID = 1030234
_KARLSRUHE_PRICES = {KARLSRUHE_SHELLAC: 4200, KARLSRUHE_PEDIKUERE_GEL: 5100}
_KARLSRUHE_TOTAL = sum(_KARLSRUHE_PRICES.values())


def _karlsruhe_location_map() -> str:
    return json.dumps(
        {
            "test-branch": {
                "location_id": TEST_LOCATION_ID,
                "location_uuid": TEST_LOCATION_UUID,
                "meta_template_prefix": "tb",
                "booking_page_url": "https://booking.example.invalid/test",
            },
            "karlsruhe": {
                "location_id": KARLSRUHE_COMPANY_ID,
                "location_uuid": KARLSRUHE_LOCATION_UUID,
                "meta_template_prefix": "ka",
                "booking_page_url": "https://booking.example.invalid/karlsruhe",
            },
        }
    )


def _karlsruhe_webhook() -> dict[str, Any]:
    payload = booking_created_multi_service()
    payload["uid"] = KARLSRUHE_BOOKING_UUID
    payload["id"] = KARLSRUHE_BOOKING_ID
    payload["customer_id"] = KARLSRUHE_CUSTOMER_ID
    payload["location_id"] = KARLSRUHE_COMPANY_ID
    payload["location_uuid"] = KARLSRUHE_LOCATION_UUID
    payload["service_id"] = _KARLSRUHE_SHELLAC_ID
    payload["service_name"] = KARLSRUHE_SHELLAC
    payload["service_related"] = KARLSRUHE_PEDIKUERE_GEL
    payload["services_description"] = f"{KARLSRUHE_SHELLAC}, {KARLSRUHE_PEDIKUERE_GEL}"
    payload["service_category"] = KARLSRUHE_SERVICE_CATEGORY
    payload["services_count"] = 2
    payload["quantity"] = 2
    payload["booking_price_currency"] = "EUR"
    set_booking_price(payload, _KARLSRUHE_TOTAL)
    return payload


def _karlsruhe_api() -> dict[str, Any]:
    """Three ordered rows whose two pedicure copies differ technically."""
    rows = []
    for index, name in enumerate([KARLSRUHE_SHELLAC, KARLSRUHE_PEDIKUERE_GEL, KARLSRUHE_PEDIKUERE_GEL]):
        row = _line(f"5000000{index}-0000-4000-8000-000000000001", name, _KARLSRUHE_PRICES[name], 60)
        row["resource"] = {"uuid": f"5100000{index}-0000-4000-8000-000000000001"}
        rows.append(row)
    return {
        "uuid": KARLSRUHE_BOOKING_UUID,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": _KARLSRUHE_TOTAL, "total": _KARLSRUHE_TOTAL},
        "ordered_services": rows,
    }


def _karlsruhe_catalog() -> list[dict[str, Any]]:
    return [
        {
            "uuid": f"40000000-0000-4000-8000-{index:012d}",
            "name": name,
            "currency": "EUR",
            "price": _KARLSRUHE_PRICES.get(name, 3000),
            "duration": {"value": 60, "label": "minutes"},
            "category": {"name": KARLSRUHE_SERVICE_CATEGORY},
        }
        for index, name in enumerate(sorted(KARLSRUHE_NUMERIC_SERVICE_NAMES.values()))
    ]


class KarlsruheReader:
    def __init__(self) -> None:
        self.booking_calls = 0
        self.catalog_calls = 0

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        assert booking_uuid == KARLSRUHE_BOOKING_UUID
        self.booking_calls += 1
        return _karlsruhe_api()

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert (location_uuid, page) == (KARLSRUHE_LOCATION_UUID, 1)
        self.catalog_calls += 1
        rows = _karlsruhe_catalog()
        return {"data": rows, "meta": {"current_page": 1, "last_page": 1, "total": len(rows)}}


async def _seed_karlsruhe_resource_shadow_record(session) -> None:
    client = Client(
        provider="easyweek",
        company_id=KARLSRUHE_COMPANY_ID,
        altegio_client_id=KARLSRUHE_CUSTOMER_ID,
        display_name="Karlsruhe fixture",
        phone_e164="+49000000777",
        raw={},
    )
    session.add(client)
    await session.flush()

    record = Record(
        provider="easyweek",
        company_id=KARLSRUHE_COMPANY_ID,
        altegio_record_id=KARLSRUHE_BOOKING_ID,
        easyweek_booking_uuid=uuid.UUID(KARLSRUHE_BOOKING_UUID),
        client_id=client.id,
        starts_at=utcnow() + timedelta(days=4),
        total_cost=Decimal(_KARLSRUHE_TOTAL) / Decimal(100),
        is_deleted=False,
        raw=record_raw_with_services_count({}, 2),
    )
    session.add(record)
    await session.flush()

    payload = _karlsruhe_webhook()
    session.add(
        EasyWeekEvent(
            status="processed",
            event_hint="booking-created",
            auth_via="query",
            payload_hash="karlsruhe-resource-shadow-fixture",
            payload=payload,
            booking_uuid=canonical_booking_uuid(payload),
            body_truncated=False,
        )
    )
    await session.flush()


async def test_karlsruhe_resource_shadow_is_ambiguous_while_the_new_fence_is_closed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", _karlsruhe_location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", False, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_karlsruhe_resource_shadow_record(session)
        before = await _row_counts(session)

    async with session_maker() as session:
        report = await run_preflight(session, client=KarlsruheReader(), sleep=_no_sleep)

    async with session_maker() as session:
        assert await _row_counts(session) == before

    assert report.active_multi_service == 1
    assert report.structurally_proven == 0
    assert report.ambiguous == 1
    assert report.unexplained == 1
    # A stable, PII-free reason, not a pretend readiness.
    assert report.reasons[MULTI_SERVICE_DUPLICATE_AMBIGUOUS] == 1
    assert report.ready is False


async def test_karlsruhe_resource_shadow_becomes_proven_and_category_suppressed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", _karlsruhe_location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_karlsruhe_resource_shadow_record(session)
        before = await _row_counts(session)

    reader = KarlsruheReader()
    async with session_maker() as session:
        report = await run_preflight(session, client=reader, sleep=_no_sleep)

    async with session_maker() as session:
        # The preflight stays read-only: no Record, job or outbox row appears.
        assert await _row_counts(session) == before

    assert reader.booking_calls == 1
    assert report.active_multi_service == 1
    assert report.checked == 1
    assert report.structurally_proven == 1
    assert report.allowed == 0
    assert report.disallowed_by_category == 1
    assert report.contract_excluded == 0
    assert report.ambiguous == 0
    assert report.stale_snapshot_digest == 0
    assert report.unexplained == 0
    assert report.open_jobs == 0
    assert report.truncated is False
    assert report.reasons["multi_service_category_not_allowed"] == 1
    assert report.ready is True


async def test_the_observed_baseline_mixes_one_ordinary_pair_with_resource_shadows(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reported baseline shape: 1 ordinary pair + Karlsruhe resource shadows.

    Production saw 17 active records: one ordinary structurally proven and
    category-suppressed pair, and 16 Karlsruhe resource shadows.  The counts are
    rollout evidence, not a runtime constant, so the invariant asserted here is
    the RELATION between them: everything is proven, nothing is allowed, and
    everything is suppressed by category.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", _karlsruhe_location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Wimpernverlängerung"]),
        raising=False,
    )
    async with session_maker() as session:
        async with session.begin():
            await _seed_active_pair(session)
            await _seed_karlsruhe_resource_shadow_record(session)

    class BothReader:
        def __init__(self) -> None:
            self.fixture = FakeReader()
            self.karlsruhe = KarlsruheReader()

        async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
            if booking_uuid == KARLSRUHE_BOOKING_UUID:
                return await self.karlsruhe.get_booking(booking_uuid)
            return await self.fixture.get_booking(booking_uuid)

        async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
            if location_uuid == KARLSRUHE_LOCATION_UUID:
                return await self.karlsruhe.list_location_services(location_uuid, page=page)
            return await self.fixture.list_location_services(location_uuid, page=page)

    async with session_maker() as session:
        report = await run_preflight(session, client=BothReader(), sleep=_no_sleep)

    assert report.active_multi_service == report.checked == 2
    assert report.structurally_proven == 2
    assert report.allowed == 0
    assert report.disallowed_by_category == 2
    assert report.ambiguous == 0
    assert report.unexplained == 0
    assert report.ready is True


# ===========================================================================
# §38.8: the rescheduled Karlsruhe pair must reach the category, not ambiguity
#
# Two events for one booking: booking-created with services_count=2 and a
# top-level quantity of 2, then the later booking-rescheduled with the same
# authoritative count and a top-level quantity of 1. The proof used to refuse
# the second shape outright, so one fully provable forbidden record turned the
# whole preflight red with ambiguous=1 / unexplained=1.
# ===========================================================================


def _karlsruhe_rescheduled_webhook(*, quantity: int) -> dict[str, Any]:
    payload = _karlsruhe_webhook()
    payload["quantity"] = quantity
    return payload


async def _seed_rescheduled_karlsruhe_record(session, *, quantity: int = 1) -> None:
    """The production shape, with no production identifiers."""
    client = Client(
        provider="easyweek",
        company_id=KARLSRUHE_COMPANY_ID,
        altegio_client_id=KARLSRUHE_CUSTOMER_ID,
        display_name="Karlsruhe reschedule fixture",
        phone_e164="+49000000778",
        raw={},
    )
    session.add(client)
    await session.flush()

    record = Record(
        provider="easyweek",
        company_id=KARLSRUHE_COMPANY_ID,
        altegio_record_id=KARLSRUHE_BOOKING_ID,
        easyweek_booking_uuid=uuid.UUID(KARLSRUHE_BOOKING_UUID),
        client_id=client.id,
        starts_at=utcnow() + timedelta(days=6),
        total_cost=Decimal(_KARLSRUHE_TOTAL) / Decimal(100),
        is_deleted=False,
        raw=record_raw_with_services_count({}, 2),
    )
    session.add(record)
    await session.flush()

    created = _karlsruhe_rescheduled_webhook(quantity=2)
    session.add(
        EasyWeekEvent(
            status="processed",
            event_hint="booking-created",
            auth_via="query",
            payload_hash="karlsruhe-reschedule-created",
            payload=created,
            booking_uuid=canonical_booking_uuid(created),
            body_truncated=False,
            received_at=utcnow() - timedelta(hours=2),
        )
    )
    rescheduled = _karlsruhe_rescheduled_webhook(quantity=quantity)
    session.add(
        EasyWeekEvent(
            status="processed",
            event_hint="booking-rescheduled",
            auth_via="query",
            payload_hash="karlsruhe-reschedule-later",
            payload=rescheduled,
            booking_uuid=canonical_booking_uuid(rescheduled),
            body_truncated=False,
            received_at=utcnow() - timedelta(minutes=5),
        )
    )
    await session.flush()


async def test_a_rescheduled_pair_with_a_lower_envelope_quantity_is_structurally_proven(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", _karlsruhe_location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_rescheduled_karlsruhe_record(session)
        before = await _row_counts(session)

    reader = KarlsruheReader()
    async with session_maker() as session:
        report = await run_preflight(session, client=reader, sleep=_no_sleep)

    async with session_maker() as session:
        # Read-only: not one row moved.
        assert await _row_counts(session) == before

    assert report.active_multi_service == report.checked == 1
    assert report.structurally_proven == 1
    assert report.allowed == 0
    assert report.disallowed_by_category == 1
    assert report.contract_excluded == 0
    assert report.ambiguous == 0
    assert report.unexplained == 0
    assert report.stale_snapshot_digest == 0
    assert report.truncated is False
    assert report.reasons["multi_service_category_not_allowed"] == 1
    assert report.ready is True


async def test_the_later_service_bearing_event_is_the_one_that_is_proved(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A status-only later delivery must not hide the proof-bearing one."""
    monkeypatch.setattr(settings, "easyweek_location_map", _karlsruhe_location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_rescheduled_karlsruhe_record(session)
            status_only = {
                "uid": KARLSRUHE_BOOKING_UUID,
                "id": KARLSRUHE_BOOKING_ID,
                "location_id": KARLSRUHE_COMPANY_ID,
                "location_uuid": KARLSRUHE_LOCATION_UUID,
                "booking_status": "Confirmed",
            }
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-updated",
                    auth_via="query",
                    payload_hash="karlsruhe-reschedule-status-only",
                    payload=status_only,
                    booking_uuid=uuid.UUID(KARLSRUHE_BOOKING_UUID),
                    body_truncated=False,
                    received_at=utcnow(),
                )
            )

    async with session_maker() as session:
        report = await run_preflight(session, client=KarlsruheReader(), sleep=_no_sleep)

    assert report.structurally_proven == 1
    assert report.disallowed_by_category == 1
    assert report.ambiguous == 0
    assert report.ready is True


@pytest.mark.parametrize("quantity", [0, 3, "1", None])
async def test_an_unusable_envelope_quantity_still_keeps_the_preflight_red(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    quantity: object,
) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", _karlsruhe_location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_rescheduled_karlsruhe_record(session, quantity=quantity)  # type: ignore[arg-type]

    async with session_maker() as session:
        report = await run_preflight(session, client=KarlsruheReader(), sleep=_no_sleep)

    assert report.structurally_proven == 0
    assert report.ambiguous == 1
    assert report.unexplained == 1
    assert report.ready is False
