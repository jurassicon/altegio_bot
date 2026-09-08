"""PR-14 EasyWeek local proven subset, partial re-proof and read-only preflight."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

import altegio_bot.campaigns.configuration as configuration
import altegio_bot.campaigns.easyweek_segment as segment
import altegio_bot.campaigns.runner as runner
import altegio_bot.scripts.easyweek_campaign_preflight as preflight
from altegio_bot.campaigns.configuration import CAMPAIGN_LIVE_GUARD_UNPROVEN, resolve_campaign_readiness
from altegio_bot.campaigns.easyweek_eligibility import (
    BOOKING_CANCELED,
    BOOKING_LOCATION_MISMATCH,
    BOOKING_NOT_FOUND,
    BOOKING_RESPONSE_MALFORMED,
    BOOKING_RETRYABLE_UNAVAILABLE,
    BOOKING_SERVICE_COUNT_UNPROVEN,
    BOOKING_UUID_MISMATCH,
    CONFLICTING_FIRST_VISIT_EVIDENCE,
    evaluate_booking_response,
    evaluate_local_evidence,
)
from altegio_bot.campaigns.easyweek_segment import (
    SEGMENT_SOURCE,
    EasyWeekSegmentUnavailable,
    build_easyweek_segment,
)
from altegio_bot.campaigns.provider import EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED
from altegio_bot.campaigns.runner import RunParams
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_locations import EasyWeekLocation, EasyWeekLocationRegistry
from altegio_bot.easyweek_normalizer import SucceededVisit
from altegio_bot.easyweek_service_category import (
    record_raw_with_service_category,
    record_raw_with_services_count,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.settings import settings

COMPANY_ID = 322579
OTHER_COMPANY_ID = 308697
LOCATION_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
OTHER_LOCATION_UUID = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
BOOKING_UUID = uuid.UUID("11111111-2222-4333-8444-555555555555")
BOOKING_ID = 1811630
CUSTOMER_ID = 323876
START = datetime(2026, 8, 15, 10, tzinfo=timezone.utc)
PERIOD_START = datetime(2026, 8, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 9, 1, tzinfo=timezone.utc)
STAMP = datetime(2026, 8, 15, 12, tzinfo=timezone.utc)
NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)


def _registry() -> EasyWeekLocationRegistry:
    return EasyWeekLocationRegistry(
        configured=True,
        valid=True,
        locations={
            COMPANY_ID: EasyWeekLocation(
                name="karlsruhe",
                location_id=COMPANY_ID,
                location_uuid=LOCATION_UUID,
                meta_template_prefix="ka",
                booking_page_url="https://kitilash.easyweek.de/",
            ),
            OTHER_COMPANY_ID: EasyWeekLocation(
                name="durlach",
                location_id=OTHER_COMPANY_ID,
                location_uuid=OTHER_LOCATION_UUID,
                meta_template_prefix="du",
                booking_page_url="https://durlach.example.invalid/",
            ),
        },
    )


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _registry()
    raw = {
        location.name: {
            "location_id": location.location_id,
            "location_uuid": location.location_uuid,
            "meta_template_prefix": location.meta_template_prefix,
            "booking_page_url": location.booking_page_url,
        }
        for location in registry.locations.values()
    }
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(raw), raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Wimpernverlängerung"]),
        raising=False,
    )


def _payload(**changes: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "uid": str(BOOKING_UUID),
        "id": BOOKING_ID,
        "location_id": COMPANY_ID,
        "location_uuid": LOCATION_UUID,
        "customer_id": CUSTOMER_ID,
        "visits_total": 1,
    }
    payload.update(changes)
    return payload


def _record_raw(*, category: object = "Wimpernverlängerung", services_count: object = 1) -> dict:
    raw = record_raw_with_service_category({}, category if isinstance(category, str) else None)
    return record_raw_with_services_count(raw, services_count if isinstance(services_count, int) else None)


async def _seed(
    session_maker,
    *,
    payload: dict[str, Any] | None = None,
    event_status: str = "processed",
    truncated: bool = False,
    visits_total: int | None = 1,
    stamp: datetime | None = STAMP,
    record_provider: str = PROVIDER_EASYWEEK,
    record_company: int = COMPANY_ID,
    record_booking_uuid: uuid.UUID = BOOKING_UUID,
    record_booking_id: int = BOOKING_ID,
    record_customer_id: int = CUSTOMER_ID,
    client_provider: str = PROVIDER_EASYWEEK,
    client_company: int = COMPANY_ID,
    client_customer_id: int = CUSTOMER_ID,
    starts_at: datetime = START,
    deleted: bool = False,
    raw: dict | None = None,
    phone: str | None = "+4915112345678",
    opted_out: bool = False,
) -> tuple[EasyWeekEvent, Record, Client]:
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider=client_provider,
                company_id=client_company,
                altegio_client_id=client_customer_id,
                phone_e164=phone,
                display_name="Test Client",
                raw={},
                wa_opted_out=opted_out,
                easyweek_visits_total=visits_total if client_provider == PROVIDER_EASYWEEK else None,
                easyweek_visits_total_updated_at=stamp if client_provider == PROVIDER_EASYWEEK else None,
            )
            session.add(client)
            await session.flush()
            record = Record(
                provider=record_provider,
                company_id=record_company,
                altegio_record_id=record_booking_id,
                easyweek_booking_uuid=record_booking_uuid,
                client_id=client.id,
                altegio_client_id=record_customer_id,
                starts_at=starts_at,
                is_deleted=deleted,
                raw=raw if raw is not None else _record_raw(),
            )
            session.add(record)
            event = EasyWeekEvent(
                status=event_status,
                event_hint="booking-succeeded",
                body_truncated=truncated,
                payload=payload if payload is not None else _payload(),
                payload_hash=f"event-{uuid.uuid4()}",
            )
            session.add(event)
            await session.flush()
        return event, record, client


async def _build(session_maker, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    return await build_easyweek_segment(
        company_id=COMPANY_ID,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        registry=_registry(),
        allowed_categories_raw=json.dumps(["Wimpernverlängerung"]),
        now=NOW,
    )


async def test_one_fully_proven_candidate_has_durable_source_proof(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    event, record, client = await _seed(session_maker)
    result = await _build(session_maker, monkeypatch)

    assert result.coverage["eligible_proven_rows"] == 1
    assert result.coverage["eligible_unique_clients"] == 1
    assert len(result.candidates) == 1
    item = result.candidates[0]
    assert item.candidate.is_eligible
    assert item.candidate.client.id == client.id
    assert item.proof is not None
    assert item.proof.event_id == event.id
    assert item.proof.record_id == record.id
    assert item.proof.booking_uuid == BOOKING_UUID
    assert item.proof.visits_total == 1
    assert item.proof.visits_total_updated_at == STAMP


@pytest.mark.parametrize(
    ("source_value", "reason"),
    [
        (2, "easyweek_campaign_source_visits_total_not_one"),
        (4, "easyweek_campaign_source_visits_total_not_one"),
        (True, "invalid_visits_total"),
        (1.0, "invalid_visits_total"),
        ("1", "invalid_visits_total"),
        (0, "visits_total_out_of_range"),
        (-1, "visits_total_out_of_range"),
    ],
)
async def test_source_visit_shapes_fail_closed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    source_value: object,
    reason: str,
) -> None:
    await _seed(session_maker, payload=_payload(visits_total=source_value))
    result = await _build(session_maker, monkeypatch)
    assert result.coverage["eligible_unique_clients"] == 0
    assert result.reason_counts[reason] == 1


@pytest.mark.parametrize(
    ("current", "stamp", "reason"),
    [
        (None, None, "easyweek_campaign_current_visits_total_missing"),
        (2, STAMP, "easyweek_campaign_current_visits_total_not_one"),
        (4, STAMP, "easyweek_campaign_current_visits_total_not_one"),
    ],
)
async def test_current_visit_snapshot_must_be_exactly_one_and_stamped(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    current: int | None,
    stamp: datetime | None,
    reason: str,
) -> None:
    await _seed(session_maker, visits_total=current, stamp=stamp)
    result = await _build(session_maker, monkeypatch)
    assert result.reason_counts[reason] == 1
    assert result.coverage["eligible_unique_clients"] == 0


def test_unstamped_current_snapshot_is_refused_even_though_db_prevents_it() -> None:
    client = Client(
        id=100,
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        altegio_client_id=CUSTOMER_ID,
        phone_e164="+4915112345678",
        raw={},
        wa_opted_out=False,
        easyweek_visits_total=1,
        easyweek_visits_total_updated_at=None,
    )
    record = Record(
        id=200,
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        altegio_record_id=BOOKING_ID,
        easyweek_booking_uuid=BOOKING_UUID,
        client_id=client.id,
        altegio_client_id=CUSTOMER_ID,
        starts_at=START,
        is_deleted=False,
        raw=_record_raw(),
    )
    local = evaluate_local_evidence(
        visit=SucceededVisit(
            booking_uuid=BOOKING_UUID,
            booking_id=BOOKING_ID,
            company_id=COMPANY_ID,
            customer_id=CUSTOMER_ID,
            visits_total=1,
        ),
        record=record,
        client=client,
        company_id=COMPANY_ID,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        allowed_categories_raw=json.dumps(["Wimpernverlängerung"]),
        has_future_booking=False,
    )
    assert local.reason == "easyweek_campaign_current_visits_total_unstamped"


@pytest.mark.parametrize("current", [True, 1.0, "1", 0, -1])
def test_malformed_current_visit_snapshot_is_refused(current: object) -> None:
    client = Client(
        id=100,
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        altegio_client_id=CUSTOMER_ID,
        phone_e164="+4915112345678",
        raw={},
        wa_opted_out=False,
        easyweek_visits_total=current,  # type: ignore[arg-type]
        easyweek_visits_total_updated_at=STAMP,
    )
    record = Record(
        id=200,
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        altegio_record_id=BOOKING_ID,
        easyweek_booking_uuid=BOOKING_UUID,
        client_id=client.id,
        altegio_client_id=CUSTOMER_ID,
        starts_at=START,
        is_deleted=False,
        raw=_record_raw(),
    )
    local = evaluate_local_evidence(
        visit=SucceededVisit(
            booking_uuid=BOOKING_UUID,
            booking_id=BOOKING_ID,
            company_id=COMPANY_ID,
            customer_id=CUSTOMER_ID,
            visits_total=1,
        ),
        record=record,
        client=client,
        company_id=COMPANY_ID,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        allowed_categories_raw=json.dumps(["Wimpernverlängerung"]),
        has_future_booking=False,
    )
    assert local.reason == "easyweek_campaign_current_visits_total_not_one"


async def test_unconfigured_category_contract_fails_preview_source(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed(session_maker)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    with pytest.raises(EasyWeekSegmentUnavailable) as exc_info:
        await build_easyweek_segment(
            company_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            registry=_registry(),
            allowed_categories_raw="",
            now=NOW,
        )
    assert exc_info.value.reason == "easyweek_campaign_segment_contract_unavailable"


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"event_status": "captured"}, "easyweek_campaign_event_not_processed"),
        ({"truncated": True}, "truncated_payload"),
        (
            {"record_booking_uuid": uuid.UUID("22222222-2222-4333-8444-555555555555")},
            "easyweek_campaign_booking_identity_mismatch",
        ),
        ({"record_booking_id": BOOKING_ID + 1}, "easyweek_campaign_booking_identity_mismatch"),
        ({"record_provider": PROVIDER_ALTEGIO}, "easyweek_campaign_provider_mismatch"),
        ({"record_company": OTHER_COMPANY_ID}, "easyweek_campaign_company_mismatch"),
        ({"record_customer_id": CUSTOMER_ID + 1}, "easyweek_campaign_customer_identity_mismatch"),
        ({"client_customer_id": CUSTOMER_ID + 1}, "easyweek_campaign_customer_identity_mismatch"),
        ({"deleted": True}, "easyweek_campaign_record_deleted"),
        ({"raw": _record_raw(category="Other")}, "category_not_allowed"),
        ({"raw": _record_raw(category=None)}, "category_missing"),
        ({"raw": _record_raw(services_count=None)}, "service_count_unproven"),
        ({"raw": _record_raw(services_count=2)}, "category_ambiguous_multi_service"),
        ({"phone": None}, "easyweek_campaign_phone_unproven"),
        ({"phone": "+49abc"}, "easyweek_campaign_phone_unproven"),
        ({"opted_out": True}, "easyweek_campaign_opted_out"),
    ],
)
async def test_local_contract_refusals(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, Any],
    reason: str,
) -> None:
    await _seed(session_maker, **changes)
    result = await _build(session_maker, monkeypatch)
    assert result.reason_counts[reason] == 1
    assert result.coverage["eligible_unique_clients"] == 0


async def test_missing_record_and_client_are_counted(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    _event, record, client = await _seed(session_maker)
    async with session_maker() as session:
        async with session.begin():
            await session.execute(delete(Record).where(Record.id == record.id))
    result = await _build(session_maker, monkeypatch)
    assert result.reason_counts["easyweek_campaign_record_missing"] == 1

    # A fresh event/record with a deliberately absent client link.
    await _seed(
        session_maker,
        record_booking_uuid=uuid.UUID("33333333-2222-4333-8444-555555555555"),
        record_booking_id=BOOKING_ID + 2,
        record_customer_id=CUSTOMER_ID + 2,
        client_customer_id=CUSTOMER_ID + 2,
        payload=_payload(
            uid="33333333-2222-4333-8444-555555555555",
            id=BOOKING_ID + 2,
            customer_id=CUSTOMER_ID + 2,
        ),
    )
    async with session_maker() as session:
        async with session.begin():
            fresh = await session.scalar(select(Record).where(Record.altegio_record_id == BOOKING_ID + 2))
            assert fresh is not None
            fresh.client_id = None
    result = await _build(session_maker, monkeypatch)
    assert result.reason_counts["easyweek_campaign_client_missing"] == 1
    assert client.id is not None


@pytest.mark.parametrize("linked", [True, False])
async def test_future_booking_blocks_same_scoped_client_even_without_record_link(
    session_maker, monkeypatch: pytest.MonkeyPatch, linked: bool
) -> None:
    _event, _record, client = await _seed(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Record(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_record_id=BOOKING_ID + 1,
                    easyweek_booking_uuid=uuid.UUID("44444444-2222-4333-8444-555555555555"),
                    client_id=client.id if linked else None,
                    altegio_client_id=CUSTOMER_ID,
                    starts_at=NOW + timedelta(days=1),
                    is_deleted=False,
                    raw=_record_raw(),
                )
            )
    result = await _build(session_maker, monkeypatch)
    assert result.reason_counts["easyweek_campaign_future_booking_known_locally"] == 1


async def test_duplicate_delivery_collapses_and_conflicting_first_visits_exclude(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(EasyWeekEvent(status="processed", event_hint="booking-succeeded", payload=_payload()))
    duplicate = await _build(session_maker, monkeypatch)
    assert len(duplicate.candidates) == 1
    assert duplicate.coverage["duplicate_evidence_for_client"] == 1
    assert duplicate.coverage["eligible_unique_clients"] == 1

    second_uuid = uuid.UUID("55555555-2222-4333-8444-555555555555")
    async with session_maker() as session:
        async with session.begin():
            client = await session.scalar(
                select(Client).where(Client.provider == PROVIDER_EASYWEEK, Client.company_id == COMPANY_ID)
            )
            assert client is not None
            session.add(
                Record(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_record_id=BOOKING_ID + 1,
                    easyweek_booking_uuid=second_uuid,
                    client_id=client.id,
                    altegio_client_id=CUSTOMER_ID,
                    starts_at=START + timedelta(days=1),
                    is_deleted=False,
                    raw=_record_raw(),
                )
            )
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-succeeded",
                    payload=_payload(uid=str(second_uuid), id=BOOKING_ID + 1),
                )
            )
    conflict = await _build(session_maker, monkeypatch)
    assert len(conflict.candidates) == 1
    assert conflict.candidates[0].candidate.excluded_reason == CONFLICTING_FIRST_VISIT_EVIDENCE
    assert conflict.coverage["eligible_unique_clients"] == 0


async def test_provider_and_company_numeric_collisions_do_not_change_decision(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_ALTEGIO,
                    company_id=COMPANY_ID,
                    altegio_client_id=CUSTOMER_ID,
                    phone_e164="+4915000000000",
                    raw={},
                )
            )
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=OTHER_COMPANY_ID,
                    altegio_client_id=CUSTOMER_ID,
                    phone_e164="+4915000000001",
                    raw={},
                )
            )
    result = await _build(session_maker, monkeypatch)
    assert result.coverage["eligible_unique_clients"] == 1


@pytest.mark.parametrize(
    ("start", "eligible"),
    [(PERIOD_START, True), (PERIOD_END - timedelta(microseconds=1), True), (PERIOD_END, False)],
)
async def test_period_is_half_open_and_order_is_deterministic(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    start: datetime,
    eligible: bool,
) -> None:
    await _seed(session_maker, starts_at=start)
    first = await _build(session_maker, monkeypatch)
    second = await _build(session_maker, monkeypatch)
    assert (first.coverage["eligible_unique_clients"] == 1) is eligible
    assert first.safe_meta() == second.safe_meta()
    assert [x.candidate.client.id for x in first.candidates] == [x.candidate.client.id for x in second.candidates]


async def test_preview_persists_subset_without_jobs_or_outbox(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await _seed(session_maker)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
        )
    )
    assert run.status == "completed"
    assert run.meta["discovery_source"] == SEGMENT_SOURCE
    assert run.meta["segment_completeness"] == "proven_subset"
    async with session_maker() as session:
        recipient = await session.scalar(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id))
        assert recipient is not None and recipient.source_easyweek_event_id is not None
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    # A repeat preview is a new immutable snapshot; it does not edit the first.
    second = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
        )
    )
    assert second.id != run.id
    async with session_maker() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)
            )
            == 1
        )


def _api(**changes: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "uuid": str(BOOKING_UUID),
        "location_uuid": LOCATION_UUID,
        "is_canceled": False,
        "ordered_services": [{}],
    }
    payload.update(changes)
    return payload


def test_get_parser_accepts_only_confirmed_projection_and_ignores_untrusted_fields() -> None:
    result = evaluate_booking_response(
        _api(
            customer={"uuid": "not-linked", "phone": "+490000000"},
            status={"name": "Cancelled", "uuid": "not-authoritative"},
            links={"manage": "https://evil.invalid"},
            ordered_services=[{"name": "Other category", "price": 1}],
        ),
        expected_booking_uuid=BOOKING_UUID,
        location=_registry().locations[COMPANY_ID],
    )
    assert result.current is True
    assert result.reason is None


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"uuid": str(uuid.UUID("66666666-2222-4333-8444-555555555555"))}, BOOKING_UUID_MISMATCH),
        ({"location_uuid": OTHER_LOCATION_UUID}, BOOKING_LOCATION_MISMATCH),
        ({"is_canceled": True}, BOOKING_CANCELED),
        ({"is_canceled": None}, BOOKING_RESPONSE_MALFORMED),
        ({"is_canceled": 0}, BOOKING_RESPONSE_MALFORMED),
        ({"ordered_services": None}, BOOKING_SERVICE_COUNT_UNPROVEN),
        ({"ordered_services": {}}, BOOKING_SERVICE_COUNT_UNPROVEN),
        ({"ordered_services": []}, BOOKING_SERVICE_COUNT_UNPROVEN),
        ({"ordered_services": [{}, {}]}, BOOKING_SERVICE_COUNT_UNPROVEN),
    ],
)
def test_get_parser_refuses_unproven_shapes(changes: dict[str, Any], reason: str) -> None:
    result = evaluate_booking_response(
        _api(**changes),
        expected_booking_uuid=BOOKING_UUID,
        location=_registry().locations[COMPANY_ID],
    )
    assert result.current is False
    assert result.reason == reason


class _Reader:
    def __init__(self, payload: object = None, error: Exception | None = None) -> None:
        self.payload = payload if payload is not None else _api()
        self.error = error
        self.calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(booking_uuid)
        if self.error is not None:
            raise self.error
        return self.payload  # type: ignore[return-value]


async def _preview(session_maker, monkeypatch: pytest.MonkeyPatch) -> CampaignRun:
    await _seed(session_maker)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    return await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
        )
    )


@pytest.mark.parametrize(
    ("error", "reason", "retryable"),
    [
        (EasyWeekNotFoundError("hidden", operation="get_booking", status_code=404), BOOKING_NOT_FOUND, 0),
        (EasyWeekRetryableError("hidden", operation="get_booking", status_code=429), BOOKING_RETRYABLE_UNAVAILABLE, 1),
        (EasyWeekRetryableError("hidden", operation="get_booking", status_code=500), BOOKING_RETRYABLE_UNAVAILABLE, 1),
        (EasyWeekRetryableError("hidden", operation="get_booking"), BOOKING_RETRYABLE_UNAVAILABLE, 1),
        (
            EasyWeekConfigError("hidden", operation="get_booking"),
            "easyweek_campaign_booking_configuration_unavailable",
            0,
        ),
        (
            EasyWeekAuthError("hidden", operation="get_booking", status_code=401),
            "easyweek_campaign_booking_configuration_unavailable",
            0,
        ),
    ],
)
async def test_preflight_classifies_api_without_writes_or_attempts(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    reason: str,
    retryable: int,
) -> None:
    run = await _preview(session_maker, monkeypatch)
    monkeypatch.setattr(preflight, "configured_easyweek_locations", _registry)
    reader = _Reader(error=error)
    async with session_maker() as session:
        before = (
            await session.scalar(select(func.count()).select_from(MessageJob)),
            await session.scalar(select(func.count()).select_from(OutboxMessage)),
        )
        report = await preflight.run_preflight(
            session,
            run_id=run.id,
            client=reader,
            limit=10,
            pause_sec=0,
        )
        after = (
            await session.scalar(select(func.count()).select_from(MessageJob)),
            await session.scalar(select(func.count()).select_from(OutboxMessage)),
        )
    assert report.reasons[reason] == 1
    assert report.reasons[CAMPAIGN_LIVE_GUARD_UNPROVEN] == 1
    assert report.retryable_uncertainty_count == retryable
    assert report.send_ready_count == 0
    assert report.ready_for_send is False
    assert before == after == (0, 0)


async def test_preflight_success_is_partial_truncatable_and_never_send_ready(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = await _preview(session_maker, monkeypatch)
    monkeypatch.setattr(preflight, "configured_easyweek_locations", _registry)
    reader = _Reader()
    async with session_maker() as session:
        source = await session.scalar(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id))
        assert source is not None
        async with session.begin_nested():
            session.add(
                CampaignRecipient(
                    provider=source.provider,
                    campaign_run_id=source.campaign_run_id,
                    company_id=source.company_id,
                    client_id=source.client_id,
                    altegio_client_id=source.altegio_client_id,
                    phone_e164=source.phone_e164,
                    display_name=source.display_name,
                    status="candidate",
                    source_easyweek_event_id=source.source_easyweek_event_id,
                    source_record_id=source.source_record_id,
                    source_booking_uuid=source.source_booking_uuid,
                    source_visits_total=source.source_visits_total,
                    source_visits_total_updated_at=source.source_visits_total_updated_at,
                )
            )
        await session.commit()
        report = await preflight.run_preflight(session, run_id=run.id, client=reader, limit=1, pause_sec=0)
    assert report.candidate_count == 2
    assert report.checked_count == 1
    assert report.truncated is True
    assert report.local_eligible_count == 1
    assert report.source_booking_current_count == 1
    assert report.send_ready_count == 0
    assert report.reasons == {CAMPAIGN_LIVE_GUARD_UNPROVEN: 1}
    assert reader.calls == [str(BOOKING_UUID)]


async def test_durable_source_proof_constraints_are_provider_scoped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=PROVIDER_ALTEGIO,
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY_ID],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
            )
            session.add(run)
            await session.flush()
            session.add(
                CampaignRecipient(
                    provider=PROVIDER_ALTEGIO,
                    campaign_run_id=run.id,
                    company_id=COMPANY_ID,
                    source_easyweek_event_id=1,
                    status="candidate",
                )
            )
            with pytest.raises(IntegrityError):
                await session.flush()


async def test_durable_source_proof_requires_first_visit(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run = await _preview(session_maker, monkeypatch)
    async with session_maker() as session:
        recipient = await session.scalar(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id))
        assert recipient is not None
        recipient.source_visits_total = 2
        with pytest.raises(IntegrityError):
            await session.flush()


async def test_readiness_exposes_subset_but_keeps_send_blocked(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(configuration, "configured_easyweek_locations", _registry)
    monkeypatch.setattr(configuration, "validate_static_booking_page", lambda value: value)
    async with session_maker() as session:
        readiness = await resolve_campaign_readiness(
            session,
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            sender_code="campaign",
            template_code="newsletter_new_clients_monthly",
        )
    assert readiness.segment_source == SEGMENT_SOURCE
    assert EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED not in readiness.reasons
    assert CAMPAIGN_LIVE_GUARD_UNPROVEN in readiness.reasons
    assert readiness.supported_job_types == ()
    assert readiness.ready_for_send is False
