"""Synthetic seeding for the zero-counter diagnostic suites.

Shared by the preview regression suite and the operator-check suite so both
describe the same world. Everything here is invented: no real booking number,
phone, UUID, name or captured payload appears anywhere in this file.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from altegio_bot.easyweek_locations import EasyWeekLocation, EasyWeekLocationRegistry
from altegio_bot.easyweek_service_category import (
    record_raw_with_service_category,
    record_raw_with_services_count,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    Record,
)
from altegio_bot.settings import settings

COMPANY_ID = 322579
OTHER_COMPANY_ID = 308697
LOCATION_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
OTHER_LOCATION_UUID = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
CATEGORY = "Wimpernverlängerung"

PERIOD_START = datetime(2026, 10, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 11, 1, tzinfo=timezone.utc)
START = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
STAMP = datetime(2026, 10, 14, 11, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 5, tzinfo=timezone.utc)

PG_INT_MAX = 2147483647


def registry() -> EasyWeekLocationRegistry:
    return EasyWeekLocationRegistry(
        configured=True,
        valid=True,
        locations={
            COMPANY_ID: EasyWeekLocation(
                name="karlsruhe",
                location_id=COMPANY_ID,
                location_uuid=LOCATION_UUID,
                meta_template_prefix="ka",
                booking_page_url="https://karlsruhe.example.invalid/",
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


@pytest.fixture
def easyweek_zero_visit_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    locations = registry()
    raw = {
        location.name: {
            "location_id": location.location_id,
            "location_uuid": location.location_uuid,
            "meta_template_prefix": location.meta_template_prefix,
            "booking_page_url": location.booking_page_url,
        }
        for location in locations.locations.values()
    }
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(raw), raising=False)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps([CATEGORY]), raising=False)


def record_raw() -> dict:
    return record_raw_with_services_count(record_raw_with_service_category({}, CATEGORY), 1)


def succeeded_payload(
    *,
    booking_uuid: uuid.UUID,
    booking_id: int,
    customer_id: int,
    visits_total: Any,
    location_id: int = COMPANY_ID,
    location_uuid: str = LOCATION_UUID,
    omit_visits_total: bool = False,
    customer_phone: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "uid": str(booking_uuid),
        "id": booking_id,
        "location_id": location_id,
        "location_uuid": location_uuid,
        "customer_id": customer_id,
    }
    if customer_phone is not None:
        # The contact the delivery itself captured. §44.2 allows an identity to
        # be proven from it when no numeric-to-UUID correspondence was stored;
        # off by default so a test says explicitly which world it is in.
        payload["customer_phone"] = customer_phone
    if not omit_visits_total:
        payload["visits_total"] = visits_total
    return payload


class Person:
    """One synthetic customer: a local card, its booking and its delivery."""

    def __init__(self, index: int) -> None:
        self.booking_uuid = uuid.UUID(int=0x1100_0000 + index, version=4)
        self.booking_id = 900_000 + index
        self.customer_id = 500_000 + index
        self.phone = f"+4915100{index:05d}"
        self.name = f"Synthetic {index}"
        self.client_id: int | None = None
        self.record_id: int | None = None
        self.event_ids: list[int] = []


async def seed_person(
    session_maker,
    index: int,
    *,
    visits_total: Any = 0,
    omit_visits_total: bool = False,
    deliveries: int = 1,
    event_status: str = "processed",
    truncated: bool = False,
    local_counter: int | None = None,
    local_stamp: datetime | None = None,
    starts_at: datetime = START,
    record_provider: str = PROVIDER_EASYWEEK,
    record_company: int = COMPANY_ID,
    record_booking_id: int | None = None,
    record_customer_id: int | None = None,
    client_provider: str = PROVIDER_EASYWEEK,
    client_company: int = COMPANY_ID,
    client_customer_id: int | None = None,
    record_points_at_other_client: bool = False,
    deleted: bool = False,
    opted_out: bool = False,
    payload_location_id: int = COMPANY_ID,
    payload_location_uuid: str = LOCATION_UUID,
    customer_uuid: uuid.UUID | None = None,
    capture_phone: bool = False,
    card_phone: str | None = None,
) -> Person:
    person = Person(index)
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider=client_provider,
                company_id=client_company,
                altegio_client_id=person.customer_id if client_customer_id is None else client_customer_id,
                easyweek_customer_uuid=customer_uuid if client_provider == PROVIDER_EASYWEEK else None,
                easyweek_identity_assigned_at=(
                    NOW if customer_uuid is not None and client_provider == PROVIDER_EASYWEEK else None
                ),
                phone_e164=person.phone if card_phone is None else card_phone,
                display_name=person.name,
                raw={},
                wa_opted_out=opted_out,
                easyweek_visits_total=local_counter if client_provider == PROVIDER_EASYWEEK else None,
                easyweek_visits_total_updated_at=local_stamp if client_provider == PROVIDER_EASYWEEK else None,
            )
            session.add(client)
            await session.flush()
            owner_id = client.id
            if record_points_at_other_client:
                other = Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_client_id=person.customer_id + 90_000,
                    phone_e164=f"{person.phone}9",
                    display_name="Synthetic other",
                    raw={},
                    wa_opted_out=False,
                )
                session.add(other)
                await session.flush()
                owner_id = other.id
            record = Record(
                provider=record_provider,
                company_id=record_company,
                altegio_record_id=person.booking_id if record_booking_id is None else record_booking_id,
                easyweek_booking_uuid=person.booking_uuid,
                client_id=owner_id,
                altegio_client_id=person.customer_id if record_customer_id is None else record_customer_id,
                starts_at=starts_at,
                is_deleted=deleted,
                raw=record_raw(),
            )
            session.add(record)
            await session.flush()
            person.client_id = client.id
            person.record_id = record.id
            for _delivery in range(deliveries):
                event = EasyWeekEvent(
                    status=event_status,
                    event_hint="booking-succeeded",
                    body_truncated=truncated,
                    payload=succeeded_payload(
                        booking_uuid=person.booking_uuid,
                        booking_id=person.booking_id,
                        customer_id=person.customer_id,
                        visits_total=visits_total,
                        location_id=payload_location_id,
                        location_uuid=payload_location_uuid,
                        omit_visits_total=omit_visits_total,
                        customer_phone=person.phone if capture_phone else None,
                    ),
                    payload_hash=f"event-{uuid.uuid4()}",
                )
                session.add(event)
                await session.flush()
                person.event_ids.append(event.id)
    return person


__all__ = [
    "CATEGORY",
    "COMPANY_ID",
    "LOCATION_UUID",
    "NOW",
    "OTHER_COMPANY_ID",
    "OTHER_LOCATION_UUID",
    "PERIOD_END",
    "PERIOD_START",
    "PG_INT_MAX",
    "STAMP",
    "START",
    "Person",
    "easyweek_zero_visit_configuration",
    "record_raw",
    "registry",
    "seed_person",
    "succeeded_payload",
]
