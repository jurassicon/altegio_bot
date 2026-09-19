"""PR-7.5 operator-only snapshot recovery, including PostgreSQL mutations.

The production state these tests reproduce: an active future Karlsruhe record
whose durable projection is still the correct version 1 written before the
resource-shadow proof existed, while the same booking re-proved today yields
the version 2 projection.  The multi-service preflight reports that as
``stale_snapshot_digest``, and only this operator recovery may resolve it.
"""

from __future__ import annotations

import copy
import hashlib
import inspect
import json
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_client import EasyWeekRetryableError
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_SNAPSHOT_KEY,
    MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION,
    MULTI_SERVICE_SNAPSHOT_VERSION,
    WebhookServicePair,
    clear_multi_service_catalog_cache,
    multi_service_snapshot_from_record_raw,
    prove_exactly_two_service_snapshot,
    record_raw_with_multi_service_snapshot,
)
from altegio_bot.easyweek_multi_service_recovery import _digest, write_private_json
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_resource_shadow_contract import (
    KARLSRUHE_COMPANY_ID,
    KARLSRUHE_CONTRACT_REVISION,
    KARLSRUHE_LOCATION_UUID,
    KARLSRUHE_NUMERIC_SERVICE_NAMES,
    KARLSRUHE_SERVICE_CATEGORY,
    RESOURCE_SHADOW_PROOF_KIND,
)
from altegio_bot.easyweek_service_category import record_raw_with_services_count
from altegio_bot.easyweek_snapshot_recovery import (
    _SEMANTIC_LINE_FIELDS,
    BLOCKED,
    CATEGORY_NOW_ALLOWED,
    JOBS_PRESENT,
    LIVE_BOOKING_NOT_ACTIVE,
    MIGRATE,
    OUTBOX_PRESENT,
    OUTCOME_ALREADY_APPLIED,
    OUTCOME_APPLIED,
    SNAPSHOT_CURRENT,
    SOURCE_BUSINESS_MISMATCH,
    SOURCE_IDENTITY_MISMATCH,
    RecoveryError,
    _ordered_semantic_projection,
    apply_snapshot_recovery_plan,
    build_snapshot_recovery_plan,
    check_apply_authorization,
    confirmation_phrase,
    read_apply_report,
    read_plan,
    verify_snapshot_recovery,
    write_plan,
)
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, OutboxMessage, Record, RecordService
from altegio_bot.scripts import easyweek_multi_service_snapshot_recovery as cli
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import booking_created_multi_service, set_booking_price

pytestmark = pytest.mark.asyncio

RUNBOOK = Path(__file__).resolve().parents[3] / "docs/easyweek/pr7_4_two_service_notifications_runbook.md"

NOW = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)
STARTS_AT = NOW + timedelta(days=4)

SHELLAC = "Maniküre mit Gel-Lack / Shellac"
PEDIKUERE_GEL = "Pediküre mit Gel-Lack"
SHELLAC_ID = 1030234
PRICES = {SHELLAC: 4200, PEDIKUERE_GEL: 5100}
TOTAL = sum(PRICES.values())

BOOKING_UUID = "77777777-2222-4333-8444-555555555555"
BOOKING_ID = 4200777
CUSTOMER_ID = 7300777

OTHER_COMPANY_ID = 308697
OTHER_LOCATION_UUID = "b9d689f2-0e41-47cd-812a-e3c33197753d"


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "karlsruhe": {
                    "location_id": KARLSRUHE_COMPANY_ID,
                    "location_uuid": KARLSRUHE_LOCATION_UUID,
                    "meta_template_prefix": "ka",
                    "booking_page_url": "https://booking.example.invalid/karlsruhe",
                },
                "durlach": {
                    "location_id": OTHER_COMPANY_ID,
                    "location_uuid": OTHER_LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": "https://booking.example.invalid/durlach",
                },
            }
        ),
        raising=False,
    )
    # Production-shaped: Nagelservice is deliberately NOT allowed.
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Wimpernverlängerung"]),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_multi_service_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    clear_multi_service_catalog_cache()


# ---------------------------------------------------------------------------
# Fixtures shaped like the confirmed production records
# ---------------------------------------------------------------------------


def _webhook(*, booking_uuid: str = BOOKING_UUID, booking_id: int = BOOKING_ID) -> dict[str, Any]:
    payload = booking_created_multi_service()
    payload["uid"] = booking_uuid
    payload["id"] = booking_id
    payload["customer_id"] = CUSTOMER_ID
    payload["location_id"] = KARLSRUHE_COMPANY_ID
    payload["location_uuid"] = KARLSRUHE_LOCATION_UUID
    payload["service_id"] = SHELLAC_ID
    payload["service_name"] = SHELLAC
    payload["service_related"] = PEDIKUERE_GEL
    payload["services_description"] = f"{SHELLAC}, {PEDIKUERE_GEL}"
    payload["service_category"] = KARLSRUHE_SERVICE_CATEGORY
    payload["services_count"] = 2
    payload["quantity"] = 2
    payload["booking_price_currency"] = "EUR"
    set_booking_price(payload, TOTAL)
    return payload


def _line(
    index: int,
    name: str,
    *,
    price: int | None = None,
    minutes: int = 60,
    technical: str = "0",
) -> dict[str, Any]:
    return {
        "uuid": f"5000000{index}-0000-4000-8000-000000000001",
        "name": name,
        "currency": "EUR",
        "price": PRICES[name] if price is None else price,
        "original_price": PRICES[name] if price is None else price,
        "discount": 0,
        "quantity": 1,
        "duration": {"value": minutes, "label": "minutes"},
        "original_duration": {"value": minutes, "label": "minutes"},
        # A technical API field the proof does not model; it differs between
        # the pedicure row and its resource copy, which is exactly why the old
        # full-row signature could not collapse them.  ``technical`` also lets
        # a test give two live observations of the SAME booking different
        # technical values, which is what §38.6 says must not matter.
        "resource": {"uuid": f"5100000{index}-{technical}000-4000-8000-000000000001"[:36]},
    }


def _api(
    *,
    starts_at: datetime = STARTS_AT,
    booking_uuid: str = BOOKING_UUID,
    resource_shadow: bool = True,
    canceled: bool = False,
    completed: bool = False,
    second_price: int | None = None,
    technical: str = "0",
) -> dict[str, Any]:
    rows = [
        _line(0, SHELLAC, technical=technical),
        _line(1, PEDIKUERE_GEL, price=second_price, technical=technical),
    ]
    if resource_shadow:
        rows.append(_line(2, PEDIKUERE_GEL, price=second_price, technical=technical))
    total = rows[0]["price"] + rows[1]["price"]
    status = "canceled" if canceled else "completed" if completed else "active"
    return {
        "uuid": booking_uuid,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "start_time": starts_at.isoformat(),
        "is_canceled": canceled,
        "is_completed": completed,
        "status": {"type": status},
        "currency": "EUR",
        "order": {"subtotal": total, "total": total},
        "ordered_services": rows,
    }


def _catalog(
    *,
    rename: tuple[str, str] | None = None,
    drop: str | None = None,
    duplicate: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, name in enumerate(sorted(KARLSRUHE_NUMERIC_SERVICE_NAMES.values())):
        if name == drop:
            continue
        display = rename[1] if rename is not None and rename[0] == name else name
        rows.append(
            {
                "uuid": f"40000000-0000-4000-8000-{index:012d}",
                "name": display,
                "currency": "EUR",
                "price": PRICES.get(name, 3000),
                "duration": {"value": 60, "label": "minutes"},
                "category": {"name": KARLSRUHE_SERVICE_CATEGORY},
            }
        )
    if duplicate is not None:
        rows.append(
            {
                "uuid": "40000000-0000-4000-8000-000000000099",
                "name": duplicate,
                "currency": "EUR",
                "price": PRICES.get(duplicate, 3000),
                "duration": {"value": 60, "label": "minutes"},
                "category": {"name": KARLSRUHE_SERVICE_CATEGORY},
            }
        )
    return rows


class FakeReader:
    """The two GET-only endpoints, with every answer stated explicitly."""

    def __init__(
        self,
        *,
        bookings: dict[str, dict[str, Any]] | None = None,
        catalog_rows: list[dict[str, Any]] | None = None,
        booking_error: Exception | None = None,
        catalog_error: Exception | None = None,
    ) -> None:
        self.bookings = bookings
        self.catalog_rows = catalog_rows
        self.booking_error = booking_error
        self.catalog_error = catalog_error
        self.booking_calls: list[str] = []
        self.catalog_calls: list[tuple[str, int]] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.booking_calls.append(booking_uuid)
        if self.booking_error is not None:
            raise self.booking_error
        if self.bookings is not None:
            return self.bookings[booking_uuid]
        return _api(booking_uuid=booking_uuid)

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        self.catalog_calls.append((location_uuid, page))
        if self.catalog_error is not None:
            raise self.catalog_error
        rows = self.catalog_rows if self.catalog_rows is not None else _catalog()
        return {"data": rows, "meta": {"current_page": 1, "last_page": 1, "total": len(rows)}}

    async def aclose(self) -> None:
        return None


def _stored_v1_snapshot(
    booking_uuid: str = BOOKING_UUID,
    *,
    starts_at: datetime = STARTS_AT,
    technical: str = "0",
):
    """The projection these records really carry: proven, correct, version 1.

    Built from the two-row form the booking had when it was first proved, which
    is why it is version 1 and has no resource-shadow proof.
    """
    snapshot = prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=uuid.UUID(booking_uuid),
            location_uuid=KARLSRUHE_LOCATION_UUID,
            service_name=SHELLAC,
            service_related=PEDIKUERE_GEL,
            services_description=f"{SHELLAC}, {PEDIKUERE_GEL}",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=Decimal(TOTAL) / Decimal(100),
            company_id=KARLSRUHE_COMPANY_ID,
            service_id=SHELLAC_ID,
        ),
        booking_payload=_api(
            booking_uuid=booking_uuid, starts_at=starts_at, resource_shadow=False, technical=technical
        ),
        catalog_rows=_catalog(),
    )
    assert snapshot.version == MULTI_SERVICE_SNAPSHOT_VERSION
    assert snapshot.resource_shadow_proof is None
    return snapshot


async def _seed(
    session,
    *,
    record_id: int | None = None,
    booking_uuid: str = BOOKING_UUID,
    booking_id: int = BOOKING_ID,
    company_id: int = KARLSRUHE_COMPANY_ID,
    provider: str = "easyweek",
    starts_at: datetime = STARTS_AT,
    is_deleted: bool = False,
    services_count: int = 2,
    with_snapshot: bool = True,
    snapshot_override: Any = None,
    stored_technical: str = "0",
    extra_raw: dict[str, Any] | None = None,
) -> Record:
    client = Client(
        provider="easyweek",
        company_id=company_id,
        altegio_client_id=CUSTOMER_ID + booking_id,
        display_name="Snapshot recovery fixture",
        phone_e164="+49000000777",
        raw={},
    )
    session.add(client)
    await session.flush()

    raw: dict[str, Any] = record_raw_with_services_count({}, services_count)
    if extra_raw:
        raw = {**raw, **extra_raw}
    if with_snapshot:
        raw = record_raw_with_multi_service_snapshot(
            raw,
            snapshot_override
            if snapshot_override is not None
            else _stored_v1_snapshot(booking_uuid, starts_at=starts_at, technical=stored_technical),
        )
    record = Record(
        id=record_id,
        provider=provider,
        company_id=company_id,
        altegio_record_id=booking_id,
        easyweek_booking_uuid=uuid.UUID(booking_uuid) if provider == "easyweek" else None,
        client_id=client.id,
        starts_at=starts_at,
        total_cost=Decimal(TOTAL) / Decimal(100),
        is_deleted=is_deleted,
        raw=raw,
    )
    session.add(record)
    await session.flush()

    payload = _webhook(booking_uuid=booking_uuid, booking_id=booking_id)
    session.add(
        EasyWeekEvent(
            status="processed",
            event_hint="booking-created",
            auth_via="query",
            payload_hash=f"snapshot-recovery-{booking_id}",
            payload=payload,
            booking_uuid=canonical_booking_uuid(payload),
            body_truncated=False,
        )
    )
    await session.flush()
    return record


async def _plan(session, *, reader: FakeReader | None = None, limit: int = 500):
    return await build_snapshot_recovery_plan(
        session,
        client=reader or FakeReader(),
        now=NOW,
        limit=limit,
        pause_sec=0,
    )


async def _freeze(tmp_path, plan):
    path = write_plan(plan, tmp_path / "plan.json")
    return read_plan(path), path


async def _row_counts(session) -> tuple[int, int, int, int]:
    values = []
    for model in (Record, Client, MessageJob, OutboxMessage):
        values.append(int((await session.execute(select(func.count()).select_from(model))).scalar_one()))
    return tuple(values)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


async def test_a_production_shaped_v1_record_is_planned_for_migration(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.summary["candidates"] == 1
    assert plan.summary["source_version_1"] == 1
    assert plan.summary["target_version_2"] == 1
    assert plan.summary["blocked"] == 0
    assert plan.summary["truncated"] is False
    assert plan.summary["apply_ready"] is True

    row = plan.records[0]
    assert row["disposition"] == MIGRATE
    assert row["source_snapshot_version"] == MULTI_SERVICE_SNAPSHOT_VERSION
    assert row["target_snapshot_version"] == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
    assert row["target_contract"]["proof_kind"] == RESOURCE_SHADOW_PROOF_KIND
    assert row["target_contract"]["contract_revision"] == KARLSRUHE_CONTRACT_REVISION
    assert len(row["target_contract"]["contract_digest"]) == 64
    assert row["source_snapshot_digest"] != row["target_snapshot_digest"]


async def test_plan_is_read_only_and_leaves_no_dirty_orm_state(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        before = await _row_counts(session)
        record = (await session.execute(select(Record))).scalars().one()
        raw_before = copy.deepcopy(record.raw)

    async with session_maker() as session:
        await _plan(session)
        assert not session.dirty
        assert not session.new
        assert not session.deleted
        await session.rollback()

    async with session_maker() as session:
        assert await _row_counts(session) == before
        record = (await session.execute(select(Record))).scalars().one()
        assert record.raw == raw_before


async def test_five_candidates_are_planned_as_one_wave(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            for index in range(5):
                await _seed(
                    session,
                    booking_uuid=f"7777777{index}-2222-4333-8444-555555555555",
                    booking_id=BOOKING_ID + index,
                )

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.summary["candidates"] == 5
    assert plan.summary["target_version_2"] == 5
    assert plan.summary["blocked"] == 0
    assert plan.summary["apply_ready"] is True
    assert len({row["target_snapshot_digest"] for row in plan.records}) == 5


@pytest.mark.parametrize(
    "seed_kwargs",
    [
        {"provider": "altegio"},
        {"company_id": OTHER_COMPANY_ID},
        {"services_count": 1},
        {"is_deleted": True},
        {"starts_at": NOW - timedelta(days=1)},
    ],
    ids=["altegio", "other-company", "single-service", "deleted", "past"],
)
async def test_records_outside_the_contract_are_not_candidates_and_not_blockers(
    session_maker,
    seed_kwargs: dict[str, Any],
) -> None:
    """Out of scope means invisible, never "blocked": their v1 is correct."""
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, **seed_kwargs)

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.records == ()
    assert plan.summary["candidates"] == 0
    assert plan.summary["blocked"] == 0
    # An empty scope is a safe, idempotent steady state.
    assert plan.summary["apply_ready"] is True


async def test_an_already_correct_version_2_record_is_never_selected(session_maker) -> None:
    live = prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=uuid.UUID(BOOKING_UUID),
            location_uuid=KARLSRUHE_LOCATION_UUID,
            service_name=SHELLAC,
            service_related=PEDIKUERE_GEL,
            services_description=f"{SHELLAC}, {PEDIKUERE_GEL}",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=Decimal(TOTAL) / Decimal(100),
            company_id=KARLSRUHE_COMPANY_ID,
            service_id=SHELLAC_ID,
        ),
        booking_payload=_api(),
        catalog_rows=_catalog(),
    )
    assert live.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION

    async with session_maker() as session:
        async with session.begin():
            await _seed(session, snapshot_override=live)

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.records == ()


async def test_a_two_row_booking_that_still_proves_version_1_is_current_not_blocked(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    reader = FakeReader(bookings={BOOKING_UUID: _api(resource_shadow=False)})
    async with session_maker() as session:
        plan = await _plan(session, reader=reader)

    assert plan.records[0]["disposition"] == SNAPSHOT_CURRENT
    assert plan.summary["target_version_2"] == 0
    assert plan.summary["blocked"] == 0
    assert plan.summary["apply_ready"] is True


@pytest.mark.parametrize("status", ["queued", "processing", "done", "canceled", "failed"])
async def test_any_message_job_of_any_status_blocks_the_record(session_maker, status: str) -> None:
    async with session_maker() as session:
        async with session.begin():
            record = await _seed(session)
            session.add(
                MessageJob(
                    provider="easyweek",
                    company_id=KARLSRUHE_COMPANY_ID,
                    record_id=record.id,
                    client_id=record.client_id,
                    job_type="record_created",
                    run_at=NOW,
                    status=status,
                    dedupe_key=f"snapshot-recovery-block-{status}",
                    payload={},
                )
            )

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.records[0]["disposition"] == BLOCKED
    assert plan.records[0]["refusal_reason"] == JOBS_PRESENT
    assert plan.summary["apply_ready"] is False


@pytest.mark.parametrize("status", ["queued", "sending", "sent", "failed"])
async def test_any_outbox_message_of_any_status_blocks_the_record(session_maker, status: str) -> None:
    async with session_maker() as session:
        async with session.begin():
            record = await _seed(session)
            session.add(
                OutboxMessage(
                    company_id=KARLSRUHE_COMPANY_ID,
                    client_id=record.client_id,
                    record_id=record.id,
                    phone_e164="+49000000777",
                    template_code="record_created",
                    body="fixture",
                    status=status,
                    scheduled_at=NOW,
                    meta={},
                )
            )

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.records[0]["disposition"] == BLOCKED
    assert plan.records[0]["refusal_reason"] == OUTBOX_PRESENT
    assert plan.summary["apply_ready"] is False


async def test_an_allowed_category_blocks_this_suppression_only_recovery(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps([KARLSRUHE_SERVICE_CATEGORY]),
        raising=False,
    )
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.records[0]["disposition"] == BLOCKED
    assert plan.records[0]["refusal_reason"] == CATEGORY_NOW_ALLOWED
    assert plan.summary["apply_ready"] is False


@pytest.mark.parametrize(
    ("reader_kwargs", "expected"),
    [
        ({"bookings": {BOOKING_UUID: _api(canceled=True)}}, LIVE_BOOKING_NOT_ACTIVE),
        ({"bookings": {BOOKING_UUID: _api(completed=True)}}, LIVE_BOOKING_NOT_ACTIVE),
        ({"bookings": {BOOKING_UUID: _api(starts_at=STARTS_AT + timedelta(hours=3))}}, "identity_mismatch"),
        ({"booking_error": TimeoutError()}, "multi_service_api_unavailable"),
        (
            {"catalog_error": EasyWeekRetryableError("unavailable", operation="list_location_services")},
            "multi_service_catalog_unavailable",
        ),
        ({"catalog_rows": _catalog(rename=(PEDIKUERE_GEL, "Pediküre mit Gel"))}, "multi_service_duplicate_ambiguous"),
        ({"catalog_rows": _catalog(drop=PEDIKUERE_GEL)}, "multi_service_duplicate_ambiguous"),
        ({"catalog_rows": _catalog(duplicate=PEDIKUERE_GEL)}, "multi_service_duplicate_ambiguous"),
        ({"bookings": {BOOKING_UUID: _api(second_price=5200)}}, "multi_service_total_mismatch"),
    ],
    ids=[
        "canceled",
        "completed",
        "rescheduled",
        "booking-api-failure",
        "catalog-api-failure",
        "catalog-rename",
        "catalog-missing",
        "catalog-duplicate",
        "price-drift",
    ],
)
async def test_live_drift_blocks_the_record_with_a_stable_reason(
    session_maker,
    reader_kwargs: dict[str, Any],
    expected: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session, reader=FakeReader(**reader_kwargs))

    row = plan.records[0]
    assert row["disposition"] == BLOCKED
    assert row["refusal_reason"] == expected
    assert row["target_snapshot"] is None
    assert plan.summary["apply_ready"] is False


async def test_a_malformed_stored_snapshot_blocks_the_wave(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            record = await _seed(session)
            raw = copy.deepcopy(record.raw)
            raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY]["digest"] = "0" * 64
            record.raw = raw

    async with session_maker() as session:
        plan = await _plan(session)

    assert plan.records[0]["disposition"] == BLOCKED
    assert plan.summary["apply_ready"] is False


async def test_a_custom_duration_pair_is_never_migrated(session_maker) -> None:
    booking = _api()
    for row in booking["ordered_services"]:
        if row["name"] == PEDIKUERE_GEL:
            row["duration"] = {"value": 75, "label": "minutes"}
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session, reader=FakeReader(bookings={BOOKING_UUID: booking}))

    assert plan.records[0]["disposition"] == BLOCKED
    assert plan.records[0]["refusal_reason"] == "multi_service_custom_duration_unsupported"
    assert plan.summary["apply_ready"] is False


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


async def test_apply_replaces_only_the_snapshot_and_keeps_every_neighbour_key(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(
                session,
                extra_raw={"altegio": {"legacy": True}, "unrelated_top_level": [1, 2, 3]},
            )
        before_counts = await _row_counts(session)
        record = (await session.execute(select(Record))).scalars().one()
        raw_before = copy.deepcopy(record.raw)

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(
            session,
            frozen=frozen,
            client=FakeReader(),
            now=NOW,
            pause_sec=0,
        )

    assert len(result.migrated) == 1
    migrated = result.migrated[0]
    assert migrated["old_snapshot_version"] == MULTI_SERVICE_SNAPSHOT_VERSION
    assert migrated["new_snapshot_version"] == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
    assert migrated["old_snapshot_digest"] != migrated["new_snapshot_digest"]
    assert migrated["proof_kind"] == RESOURCE_SHADOW_PROOF_KIND
    assert migrated["contract_revision"] == KARLSRUHE_CONTRACT_REVISION

    async with session_maker() as session:
        assert await _row_counts(session) == before_counts
        record = (await session.execute(select(Record))).scalars().one()

    # Every neighbour key survives byte for byte.
    assert record.raw["altegio"] == raw_before["altegio"]
    assert record.raw["unrelated_top_level"] == raw_before["unrelated_top_level"]
    assert record.raw["easyweek"]["services_count"] == raw_before["easyweek"]["services_count"]
    assert set(record.raw) == set(raw_before)
    assert set(record.raw["easyweek"]) == set(raw_before["easyweek"])

    stored, error = multi_service_snapshot_from_record_raw(record.raw)
    assert error is None and stored is not None
    assert stored.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
    assert stored.resource_shadow_proof is not None
    assert stored.resource_shadow_proof.proof_kind == RESOURCE_SHADOW_PROOF_KIND
    assert stored.digest == migrated["new_snapshot_digest"]


async def test_five_records_migrate_in_one_atomic_wave(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        async with session.begin():
            for index in range(5):
                await _seed(
                    session,
                    booking_uuid=f"7777777{index}-2222-4333-8444-555555555555",
                    booking_id=BOOKING_ID + index,
                )

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)

    assert len(result.migrated) == 5
    report = result.report()
    assert report["mutation_counts"]["records_snapshot_migrated"] == 5
    assert report["mutation_counts"]["message_jobs_created"] == 0
    assert report["mutation_counts"]["outbox_messages_created"] == 0

    async with session_maker() as session:
        records = (await session.execute(select(Record).order_by(Record.id))).scalars().all()
        for record in records:
            stored, error = multi_service_snapshot_from_record_raw(record.raw)
            assert error is None and stored is not None
            assert stored.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
        assert (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one() == 0


@pytest.mark.parametrize("intruder", ["job", "outbox"])
async def test_a_row_created_between_plan_and_apply_rolls_back_the_whole_wave(
    session_maker,
    tmp_path,
    intruder: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            for index in range(2):
                await _seed(
                    session,
                    booking_uuid=f"7777777{index}-2222-4333-8444-555555555555",
                    booking_id=BOOKING_ID + index,
                )

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record).order_by(Record.id))).scalars().first()
            assert record is not None
            if intruder == "job":
                session.add(
                    MessageJob(
                        provider="easyweek",
                        company_id=KARLSRUHE_COMPANY_ID,
                        record_id=record.id,
                        client_id=record.client_id,
                        job_type="record_created",
                        run_at=NOW,
                        status="queued",
                        dedupe_key="snapshot-recovery-intruder",
                        payload={},
                    )
                )
            else:
                session.add(
                    OutboxMessage(
                        company_id=KARLSRUHE_COMPANY_ID,
                        client_id=record.client_id,
                        record_id=record.id,
                        phone_e164="+49000000777",
                        template_code="record_created",
                        body="fixture",
                        status="queued",
                        scheduled_at=NOW,
                        meta={},
                    )
                )

    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)

    async with session_maker() as session:
        records = (await session.execute(select(Record).order_by(Record.id))).scalars().all()
        for record in records:
            stored, _error = multi_service_snapshot_from_record_raw(record.raw)
            assert stored is not None
            # Not one record was partially migrated.
            assert stored.version == MULTI_SERVICE_SNAPSHOT_VERSION


@pytest.mark.parametrize(
    "mutate",
    ["record_raw", "record_field", "client", "record_service"],
)
async def test_local_state_drift_between_plan_and_apply_blocks_the_wave(
    session_maker,
    tmp_path,
    mutate: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record))).scalars().one()
            if mutate == "record_raw":
                record.raw = {**copy.deepcopy(record.raw), "drifted": True}
            elif mutate == "record_field":
                record.comment = "changed after the plan"
            elif mutate == "client":
                client = await session.get(Client, record.client_id)
                assert client is not None
                client.display_name = "changed after the plan"
            else:
                session.add(
                    RecordService(
                        record_id=record.id,
                        service_id=SHELLAC_ID,
                        title=f"{SHELLAC}, {PEDIKUERE_GEL}",
                        amount=2,
                        cost_to_pay=Decimal(TOTAL) / Decimal(100),
                        raw={},
                    )
                )

    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)

    async with session_maker() as session:
        record = (await session.execute(select(Record))).scalars().one()
        stored, _error = multi_service_snapshot_from_record_raw(record.raw)
        assert stored is not None and stored.version == MULTI_SERVICE_SNAPSHOT_VERSION


@pytest.mark.parametrize(
    "reader_kwargs",
    [
        {"bookings": {BOOKING_UUID: _api(canceled=True)}},
        {"bookings": {BOOKING_UUID: _api(starts_at=STARTS_AT + timedelta(hours=2))}},
        {"catalog_rows": _catalog(rename=(PEDIKUERE_GEL, "Pediküre mit Gel"))},
        {"booking_error": TimeoutError()},
    ],
    ids=["canceled", "rescheduled", "catalog-rename", "api-failure"],
)
async def test_live_drift_at_apply_time_blocks_without_mutation(
    session_maker,
    tmp_path,
    reader_kwargs: dict[str, Any],
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    # Apply is its own container run in production, so its catalogue cache is
    # cold; inheriting the plan phase's cache would hide a live rename.
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(
                session,
                frozen=frozen,
                client=FakeReader(**reader_kwargs),
                now=NOW,
                pause_sec=0,
            )

    async with session_maker() as session:
        record = (await session.execute(select(Record))).scalars().one()
        stored, _error = multi_service_snapshot_from_record_raw(record.raw)
        assert stored is not None and stored.version == MULTI_SERVICE_SNAPSHOT_VERSION


async def test_contract_drift_blocks_apply(session_maker, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    # A contract revision bump is exactly what a code-side table change looks
    # like: the frozen target no longer matches what the resolver proves.
    monkeypatch.setattr(
        "altegio_bot.easyweek_resource_shadow_contract._KARLSRUHE_CONTRACT",
        None,
        raising=False,
    )
    clear_multi_service_catalog_cache()

    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)

    async with session_maker() as session:
        record = (await session.execute(select(Record))).scalars().one()
        stored, _error = multi_service_snapshot_from_record_raw(record.raw)
        assert stored is not None and stored.version == MULTI_SERVICE_SNAPSHOT_VERSION


async def test_configuration_drift_blocks_apply(
    session_maker,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)

    async with session_maker() as session:
        plan = await _plan(session)
        frozen, path = await _freeze(tmp_path, plan)

    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Wimpernverlängerung", "Kosmetik"]),
        raising=False,
    )
    with pytest.raises(RecoveryError, match="configuration_digest_changed"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW + timedelta(seconds=5),
        )
    assert path.exists()


@pytest.mark.parametrize(
    ("digest", "confirm", "age_sec", "expected"),
    [
        ("0" * 64, None, 5, "plan_digest_mismatch"),
        (None, "wrong phrase", 5, "confirmation_mismatch"),
        (None, None, 100000, "plan_expired"),
    ],
    ids=["digest", "confirmation", "expired"],
)
async def test_apply_authorization_refuses_bad_inputs(
    session_maker,
    tmp_path,
    digest: str | None,
    confirm: str | None,
    age_sec: int,
    expected: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    with pytest.raises(RecoveryError, match=expected):
        check_apply_authorization(
            frozen,
            supplied_digest=digest if digest is not None else frozen.digest,
            supplied_confirmation=confirm if confirm is not None else confirmation_phrase(frozen.digest),
            now=NOW + timedelta(seconds=age_sec),
        )


@pytest.mark.parametrize(
    ("flag", "value", "expected"),
    [
        ("easyweek_multi_service_send_enabled", True, "multi_service_send_fence_open"),
        ("easyweek_resource_shadow_proof_enabled", False, "resource_shadow_fence_closed"),
        ("easyweek_multi_service_notifications_enabled", False, "multi_service_planning_fence_closed"),
    ],
    ids=["send-fence-open", "resource-fence-closed", "planning-fence-closed"],
)
async def test_the_fences_gate_apply(
    session_maker,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    flag: str,
    value: bool,
    expected: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    monkeypatch.setattr(settings, flag, value, raising=False)
    with pytest.raises(RecoveryError, match=expected):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW + timedelta(seconds=5),
        )


async def test_an_empty_scope_applies_nothing_and_stays_idempotent(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    assert frozen.apply_ready is True
    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)
    assert result.migrated == ()
    assert result.report()["mutation_counts"]["records_snapshot_migrated"] == 0


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


async def _plan_apply_verify(session_maker, tmp_path, *, records: int = 1):
    async with session_maker() as session:
        async with session.begin():
            for index in range(records):
                await _seed(
                    session,
                    booking_uuid=f"7777777{index}-2222-4333-8444-555555555555",
                    booking_id=BOOKING_ID + index,
                )
    async with session_maker() as session:
        plan = await _plan(session)
        frozen, path = await _freeze(tmp_path, plan)
    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)
    return frozen, path, result


async def test_verify_passes_after_a_clean_wave(session_maker, tmp_path) -> None:
    frozen, _path, result = await _plan_apply_verify(session_maker, tmp_path, records=5)

    async with session_maker() as session:
        report = await verify_snapshot_recovery(
            session,
            frozen=frozen,
            apply_report=result.report(),
            client=FakeReader(),
            pause_sec=0,
        )
        await session.rollback()

    assert report["passed"] is True
    assert report["verified_records"] == 5
    assert report["missing_record_ids"] == []
    assert report["snapshot_mismatch_record_ids"] == []
    assert report["live_proof_mismatch_record_ids"] == []
    assert report["non_target_raw_changed_record_ids"] == []
    assert report["unexpected_job_ids"] == []
    assert report["unexpected_outbox_ids"] == []
    assert report["still_version_1_record_ids"] == []
    assert report["counts_match"] is True


async def test_verify_detects_a_changed_snapshot(session_maker, tmp_path) -> None:
    frozen, _path, result = await _plan_apply_verify(session_maker, tmp_path)

    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record))).scalars().one()
            record.raw = record_raw_with_multi_service_snapshot(record.raw, _stored_v1_snapshot())

    async with session_maker() as session:
        report = await verify_snapshot_recovery(
            session, frozen=frozen, apply_report=result.report(), client=FakeReader(), pause_sec=0
        )

    assert report["passed"] is False
    assert report["snapshot_mismatch_record_ids"]
    assert report["still_version_1_record_ids"]


@pytest.mark.parametrize("intruder", ["job", "outbox"])
async def test_verify_detects_unexpected_jobs_or_outbox(session_maker, tmp_path, intruder: str) -> None:
    frozen, _path, result = await _plan_apply_verify(session_maker, tmp_path)

    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record))).scalars().one()
            if intruder == "job":
                session.add(
                    MessageJob(
                        provider="easyweek",
                        company_id=KARLSRUHE_COMPANY_ID,
                        record_id=record.id,
                        client_id=record.client_id,
                        job_type="record_created",
                        run_at=NOW,
                        status="queued",
                        dedupe_key="snapshot-recovery-verify-intruder",
                        payload={},
                    )
                )
            else:
                session.add(
                    OutboxMessage(
                        company_id=KARLSRUHE_COMPANY_ID,
                        client_id=record.client_id,
                        record_id=record.id,
                        phone_e164="+49000000777",
                        template_code="record_created",
                        body="fixture",
                        status="queued",
                        scheduled_at=NOW,
                        meta={},
                    )
                )

    async with session_maker() as session:
        report = await verify_snapshot_recovery(
            session, frozen=frozen, apply_report=result.report(), client=FakeReader(), pause_sec=0
        )

    assert report["passed"] is False
    assert report["unexpected_job_ids"] or report["unexpected_outbox_ids"]


async def test_verify_detects_a_changed_neighbour_raw_key(session_maker, tmp_path) -> None:
    frozen, _path, result = await _plan_apply_verify(session_maker, tmp_path)

    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record))).scalars().one()
            record.raw = {**copy.deepcopy(record.raw), "tampered": True}

    async with session_maker() as session:
        report = await verify_snapshot_recovery(
            session, frozen=frozen, apply_report=result.report(), client=FakeReader(), pause_sec=0
        )

    assert report["passed"] is False
    assert report["non_target_raw_changed_record_ids"]


async def test_a_second_plan_after_recovery_is_empty_and_a_repeat_is_safe(session_maker, tmp_path) -> None:
    await _plan_apply_verify(session_maker, tmp_path)

    async with session_maker() as session:
        second = await _plan(session)
    assert second.records == ()
    assert second.summary["candidates"] == 0
    assert second.summary["apply_ready"] is True

    frozen, _path = await _freeze(tmp_path, second)
    async with session_maker() as session:
        repeat = await apply_snapshot_recovery_plan(session, frozen=frozen, client=FakeReader(), now=NOW, pause_sec=0)
    assert repeat.migrated == ()


# ---------------------------------------------------------------------------
# Artefacts, CLI surface and output hygiene
# ---------------------------------------------------------------------------


async def test_plan_and_apply_report_files_are_private_and_digest_bound(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        plan = await _plan(session)
        frozen, path = await _freeze(tmp_path, plan)

    assert (path.stat().st_mode & 0o777) == 0o600
    assert (path.parent.stat().st_mode & 0o777) == 0o700

    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["records"][0]["target_snapshot_digest"] = "0" * 64
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(RecoveryError, match="plan_digest_mismatch"):
        read_plan(path)

    # And an apply report has to belong to this exact plan.
    from altegio_bot.easyweek_multi_service_recovery import write_private_json

    report_path = tmp_path / "apply.json"
    write_private_json({"version": 1, "mode": "apply-report", "plan_digest": "x"}, report_path)
    with pytest.raises(RecoveryError):
        read_apply_report(report_path, frozen=frozen)


async def test_plan_stdout_carries_no_pii_booking_uuid_or_service_name(session_maker, tmp_path, capsys) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        plan = await _plan(session)

    printed = json.dumps(plan.safe_report(), ensure_ascii=False)
    for forbidden in (BOOKING_UUID, SHELLAC, PEDIKUERE_GEL, "+49000000777", "Snapshot recovery fixture"):
        assert forbidden not in printed
    assert plan.safe_report()["candidate_record_ids"]

    # The private file is the only artefact allowed to name the booking.
    path = write_plan(plan, tmp_path / "plan.json")
    assert BOOKING_UUID in path.read_text(encoding="utf-8")


async def test_the_cli_defaults_to_plan_and_requires_an_explicit_plan_for_apply() -> None:
    args = cli.build_parser().parse_args([])
    assert args.mode == "plan"
    apply_args = cli.build_parser().parse_args(["apply"])
    assert apply_args.plan is None
    assert apply_args.plan_digest is None
    assert apply_args.confirm is None


async def test_the_confirmation_phrase_is_bound_to_the_plan_digest() -> None:
    assert confirmation_phrase("a" * 64) == f"migrate easyweek multi-service snapshots {'a' * 64}"
    assert confirmation_phrase("a" * 64) != confirmation_phrase("b" * 64)


async def test_the_recovery_module_has_no_send_or_mutation_capability() -> None:
    import inspect

    from altegio_bot import easyweek_snapshot_recovery as module

    source = inspect.getsource(module)
    for forbidden in (
        "pg_insert",
        "session.delete",
        "MessageJob(",
        "OutboxMessage(",
        "safe_send",
        "ChatwootClient",
        "post_booking",
        "cancel_booking",
        "meta_",
    ):
        assert forbidden not in source
    # Exactly one write, through the canonical helper.
    assert source.count("record.raw = record_raw_with_multi_service_snapshot") == 1


# ===========================================================================
# Blocker 1: one canonical apply-report order
#
# The plan is ordered by starts_at so the operator reads the queue in time
# order. In production that is NOT numeric id order, and serialising the
# report in plan order while validating it in numeric order made a successful
# apply guarantee a later verify refusal.
# ===========================================================================

# The confirmed production shape: ids ascend, start times do not.
PRODUCTION_WAVE = (
    (8205, datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)),
    (8206, datetime(2026, 11, 4, 9, 0, tzinfo=timezone.utc)),
    (8207, datetime(2026, 12, 3, 9, 0, tzinfo=timezone.utc)),
    (8208, datetime(2026, 12, 21, 9, 0, tzinfo=timezone.utc)),
    (8238, datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)),
)
PLAN_ORDER = [8205, 8238, 8206, 8207, 8208]
CANONICAL_ORDER = [8205, 8206, 8207, 8208, 8238]


def _wave_uuid(offset: int) -> str:
    return f"7777777{offset}-2222-4333-8444-555555555555"


def _wave_reader(**kwargs: Any) -> FakeReader:
    """Live answers whose start time matches each record's own start."""
    bookings = {
        _wave_uuid(offset): _api(booking_uuid=_wave_uuid(offset), starts_at=starts_at, **kwargs)
        for offset, (_record_id, starts_at) in enumerate(PRODUCTION_WAVE)
    }
    return FakeReader(bookings=bookings)


async def _seed_production_wave(session) -> None:
    for offset, (record_id, starts_at) in enumerate(PRODUCTION_WAVE):
        await _seed(
            session,
            record_id=record_id,
            booking_uuid=_wave_uuid(offset),
            booking_id=BOOKING_ID + offset,
            starts_at=starts_at,
        )


async def test_the_plan_keeps_start_time_order_and_the_report_is_canonical(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)

    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())
        frozen, path = await _freeze(tmp_path, plan)

    # The plan really does present the queue in start-time order.
    assert [int(row["record_id"]) for row in plan.records] == PLAN_ORDER
    assert [int(row["record_id"]) for row in frozen.migrate_rows] == PLAN_ORDER

    # Apply is its own container run, so its catalogue cache starts cold.
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)

    report = result.report()
    assert [int(row["record_id"]) for row in report["migrated"]] == CANONICAL_ORDER
    assert report["migrated_record_ids"] == CANONICAL_ORDER
    assert report["migrated_this_run_record_ids"] == CANONICAL_ORDER
    assert report["already_applied_record_ids"] == []

    # The whole documented path, end to end.
    report_path = write_private_json(report, tmp_path / "apply.json")
    reread = read_apply_report(report_path, frozen=frozen)
    assert reread["migrated_record_ids"] == CANONICAL_ORDER

    async with session_maker() as session:
        verified = await verify_snapshot_recovery(
            session, frozen=frozen, apply_report=reread, client=_wave_reader(), pause_sec=0
        )
    assert verified["passed"] is True


async def _report_for_wave(session_maker, tmp_path):
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)
    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())
        frozen, _path = await _freeze(tmp_path, plan)
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)
    return frozen, result.report()


@pytest.mark.parametrize(
    "corrupt",
    ["duplicate", "unsorted", "mismatch", "missing", "unexpected", "bool", "not-a-list"],
)
async def test_the_reader_refuses_an_ambiguous_apply_report(session_maker, tmp_path, corrupt: str) -> None:
    frozen, report = await _report_for_wave(session_maker, tmp_path)
    ids = list(report["migrated_record_ids"])

    if corrupt == "duplicate":
        report["migrated_record_ids"] = [ids[0], *ids]
    elif corrupt == "unsorted":
        report["migrated_record_ids"] = PLAN_ORDER
        report["migrated"] = sorted(report["migrated"], key=lambda row: PLAN_ORDER.index(int(row["record_id"])))
    elif corrupt == "mismatch":
        report["migrated"] = report["migrated"][:-1]
    elif corrupt == "missing":
        report["migrated_record_ids"] = ids[:-1]
        report["migrated"] = report["migrated"][:-1]
        report["migrated_this_run_record_ids"] = ids[:-1]
    elif corrupt == "unexpected":
        report["migrated_record_ids"] = [*ids, 99999]
        report["migrated"] = [*report["migrated"], {**report["migrated"][0], "record_id": 99999}]
        report["migrated_this_run_record_ids"] = [*ids, 99999]
    elif corrupt == "bool":
        report["migrated_record_ids"] = [True, *ids[1:]]
    else:
        report["migrated_record_ids"] = {"ids": ids}

    # Re-sign so only the semantic check can catch it.
    unsigned = {key: value for key, value in report.items() if key != "report_digest"}
    report["report_digest"] = _digest(unsigned)
    path = write_private_json(report, tmp_path / "corrupt.json")

    with pytest.raises(RecoveryError):
        read_apply_report(path, frozen=frozen)


# ===========================================================================
# Blocker 2: the documented jq check has to actually run
# ===========================================================================


def _runbook_jq_command() -> str:
    text = RUNBOOK.read_text(encoding="utf-8")
    section = text.split("## 25. Проверка ожидаемых technical Record IDs", 1)[1].split("## 26.", 1)[0]
    blocks = [part for index, part in enumerate(section.split("```")) if index % 2 == 1]
    assert len(blocks) == 1, "section 25 must document exactly one command block"
    body = blocks[0]
    assert body.startswith("bash\n")
    return body[len("bash\n") :]


def _run_runbook_jq(plan_payload: dict[str, Any], tmp_path, *, expected_ids: list[int]) -> subprocess.CompletedProcess:
    command = _runbook_jq_command()
    # Only the operator-supplied inputs are substituted; the jq program itself
    # is executed exactly as the runbook prints it.
    command = command.replace("cd /opt/altegio_bot\n", "")
    command = command.replace(
        "EXPECTED_MIGRATE_IDS='[8205,8206,8207,8208,8238]'",
        f"EXPECTED_MIGRATE_IDS='{json.dumps(expected_ids, separators=(',', ':'))}'",
    )
    plan_file = tmp_path / "runbook-plan.json"
    plan_file.write_text(json.dumps(plan_payload, ensure_ascii=False), encoding="utf-8")
    command = command.replace(
        "outputs/easyweek_multi_service_recovery/snapshot-plan.json",
        shlex.quote(str(plan_file)),
    )
    return subprocess.run(["bash", "-c", command], capture_output=True, text=True, check=False)


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
async def test_the_documented_jq_check_passes_on_a_real_frozen_plan(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)
    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())

    result = _run_runbook_jq(plan.snapshot(), tmp_path, expected_ids=CANONICAL_ORDER)

    assert result.returncode == 0, result.stderr
    printed = json.loads(result.stdout)
    assert printed["candidates"] == 5
    assert printed["source_version_1"] == 5
    assert printed["target_version_2"] == 5
    assert printed["blocked"] == 0
    assert printed["truncated"] is False
    assert printed["apply_ready"] is True
    assert printed["migrate_record_ids"] == CANONICAL_ORDER
    assert printed["blocked_record_ids"] == []
    # PII-free: no booking uuid, no service name, no customer data.
    for forbidden in (BOOKING_UUID[:8], SHELLAC, PEDIKUERE_GEL, "+49000000777"):
        assert forbidden not in result.stdout


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
@pytest.mark.parametrize(
    "damage",
    ["no-summary", "no-records", "blocked-record", "truncated", "wrong-ids"],
)
async def test_the_documented_jq_check_fails_loudly_on_a_bad_plan(session_maker, tmp_path, damage: str) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)
    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())
    payload = plan.snapshot()
    expected = CANONICAL_ORDER

    if damage == "no-summary":
        payload.pop("summary")
    elif damage == "no-records":
        payload.pop("records")
    elif damage == "blocked-record":
        payload["summary"]["blocked"] = 1
        payload["records"][0]["disposition"] = "blocked"
    elif damage == "truncated":
        payload["summary"]["truncated"] = True
    else:
        expected = [1, 2, 3]

    result = _run_runbook_jq(payload, tmp_path, expected_ids=expected)
    assert result.returncode != 0, result.stdout


# ===========================================================================
# Blocker 3: the printed apply command must be runnable where it is printed
# ===========================================================================


async def test_the_cli_prints_a_real_compose_apply_command() -> None:
    command = cli.apply_command(
        plan_path="/recovery/snapshot-plan.json",
        apply_report="/recovery/snapshot-apply.json",
        plan_digest="a" * 64,
        max_snapshot_age_sec=600,
    )
    for required in (
        "docker compose",
        "-p altegio_bot",
        "-f docker-compose.yml",
        "-f docker-compose.chatwoot-internal.yml",
        "--profile ops",
        "run --rm --build",
        "easyweek-multi-service-snapshot-recovery apply",
        "--plan /recovery/snapshot-plan.json",
        "--apply-report /recovery/snapshot-apply.json",
        f"--plan-digest {'a' * 64}",
        "--max-snapshot-age-sec 600",
    ):
        assert required in command, f"printed command is missing {required}"
    assert confirmation_phrase("a" * 64) in command
    # The old trap: a host command that needs a system Python and a host
    # /recovery directory that does not exist.
    assert "python -m altegio_bot" not in command


async def test_the_cli_never_calls_an_incomplete_command_exact() -> None:
    source = inspect.getsource(cli)
    exact_lines = [line for line in source.splitlines() if "exact apply command" in line]
    assert exact_lines, "the CLI should still offer the operator an exact command"
    assert "python -m altegio_bot.scripts" not in source.split("def apply_command", 1)[0]
    # Whatever it labels exact is produced by the Compose builder.
    assert "+ apply_command(" in source


# ===========================================================================
# Blocker 4: the replaced version 1 must be the same booking and the same pair
# ===========================================================================


def _tampered_v1(**overrides: Any):
    """A digest-valid version 1 snapshot that is NOT the same projection."""
    base = _stored_v1_snapshot()
    lines = [copy.deepcopy(line.as_dict()) for line in base.lines]
    booking_uuid = overrides.pop("booking_uuid", base.booking_uuid)
    location_uuid = overrides.pop("location_uuid", base.location_uuid)
    if overrides.pop("reverse_lines", False):
        lines.reverse()
    both_lines = overrides.pop("both_lines", False)
    targets = range(len(lines)) if both_lines else (overrides.pop("line_index", 1),)
    for index in targets:
        for key, value in overrides.items():
            lines[index][key] = value
    unsigned = {
        "version": MULTI_SERVICE_SNAPSHOT_VERSION,
        "provider": "easyweek",
        "booking_uuid": booking_uuid,
        "location_uuid": location_uuid,
        "services_count": 2,
        "lines": lines,
    }
    encoded = json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {**unsigned, "digest": hashlib.sha256(encoded).hexdigest()}


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"booking_uuid": "99999999-2222-4333-8444-555555555555"}, SOURCE_IDENTITY_MISMATCH),
        ({"location_uuid": OTHER_LOCATION_UUID}, SOURCE_IDENTITY_MISMATCH),
        ({"reverse_lines": True}, SOURCE_BUSINESS_MISMATCH),
        ({"display_name": "Pediküre Mit French", "normalized_name": "pediküre mit french"}, SOURCE_BUSINESS_MISMATCH),
        ({"category": "Wimpernverlängerung"}, SOURCE_BUSINESS_MISMATCH),
        ({"currency": "CHF", "both_lines": True}, SOURCE_BUSINESS_MISMATCH),
        ({"actual_price_minor": 5200}, SOURCE_BUSINESS_MISMATCH),
        ({"actual_duration_minutes": 75, "original_duration_minutes": 75}, SOURCE_BUSINESS_MISMATCH),
        # The canonical parser already forbids duration != original_duration,
        # so this shape never reaches the provenance check.
        ({"original_duration_minutes": 75}, "multi_service_snapshot_digest_mismatch"),
    ],
    ids=[
        "other-booking",
        "other-location",
        "reversed-lines",
        "other-service",
        "other-category",
        "other-currency",
        "other-price",
        "other-duration",
        "other-original-duration",
    ],
)
async def test_a_stored_v1_from_a_different_projection_is_never_upgraded(
    session_maker,
    overrides: dict[str, Any],
    expected: str,
) -> None:
    tampered = _tampered_v1(**overrides)
    async with session_maker() as session:
        async with session.begin():
            record = await _seed(session)
            raw = copy.deepcopy(record.raw)
            raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY] = tampered
            record.raw = raw
        before = await _row_counts(session)

    async with session_maker() as session:
        plan = await _plan(session)

    row = plan.records[0]
    assert row["disposition"] == BLOCKED
    assert row["refusal_reason"] == expected
    assert plan.summary["apply_ready"] is False

    async with session_maker() as session:
        assert await _row_counts(session) == before
        record = (await session.execute(select(Record))).scalars().one()
        assert record.raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY] == tampered
        assert (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one() == 0


async def test_the_production_shaped_pair_has_one_ordered_semantic_projection(session_maker) -> None:
    """The positive control: only version, digest and proof may differ."""
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        plan = await _plan(session)

    row = plan.records[0]
    assert row["disposition"] == MIGRATE
    stored = _stored_v1_snapshot()
    target = plan.proven[int(row["record_id"])]
    assert [line.as_dict() for line in stored.lines] == [line.as_dict() for line in target.lines]
    assert stored.booking_uuid == target.booking_uuid
    assert stored.location_uuid == target.location_uuid
    assert (stored.version, target.version) == (
        MULTI_SERVICE_SNAPSHOT_VERSION,
        MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION,
    )
    assert stored.digest != target.digest


async def test_provenance_is_re_proved_under_the_record_lock(session_maker, tmp_path) -> None:
    """A plan-time proof alone would leave a window before the write."""
    async with session_maker() as session:
        async with session.begin():
            await _seed(session)
    async with session_maker() as session:
        plan = await _plan(session)
        frozen, _path = await _freeze(tmp_path, plan)

    # Swap the stored snapshot for a different, digest-valid projection after
    # the plan was frozen.
    async with session_maker() as session:
        async with session.begin():
            record = (await session.execute(select(Record))).scalars().one()
            raw = copy.deepcopy(record.raw)
            raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY] = _tampered_v1(actual_price_minor=5200)
            record.raw = raw

    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)

    async with session_maker() as session:
        record = (await session.execute(select(Record))).scalars().one()
        assert record.raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY]["version"] == MULTI_SERVICE_SNAPSHOT_VERSION


# ===========================================================================
# Blocker 5: repeating the SAME frozen plan after an undetermined result
# ===========================================================================


async def test_repeating_the_same_frozen_plan_reconciles_without_mutating(session_maker, tmp_path) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)
    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())
        frozen, _path = await _freeze(tmp_path, plan)
    clear_multi_service_catalog_cache()

    async with session_maker() as session:
        first = await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)
    assert first.outcome == OUTCOME_APPLIED

    # The operator never saw the first report: the container died after commit.
    async with session_maker() as session:
        after_first = await _row_counts(session)
        raws_before = {
            record.id: copy.deepcopy(record.raw)
            for record in (await session.execute(select(Record).order_by(Record.id))).scalars()
        }

    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        second = await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)

    assert second.outcome == OUTCOME_ALREADY_APPLIED
    report = second.report()
    assert report["migrated_record_ids"] == CANONICAL_ORDER
    assert report["migrated_this_run_record_ids"] == []
    assert report["already_applied_record_ids"] == CANONICAL_ORDER
    assert report["mutation_counts"]["records_snapshot_migrated"] == 0

    async with session_maker() as session:
        assert await _row_counts(session) == after_first
        raws_after = {
            record.id: record.raw for record in (await session.execute(select(Record).order_by(Record.id))).scalars()
        }
    assert raws_after == raws_before

    # The regenerated report is a valid input for verify.
    report_path = write_private_json(report, tmp_path / "second-apply.json")
    reread = read_apply_report(report_path, frozen=frozen)
    async with session_maker() as session:
        verified = await verify_snapshot_recovery(
            session, frozen=frozen, apply_report=reread, client=_wave_reader(), pause_sec=0
        )
    assert verified["passed"] is True
    assert verified["outcome"] == OUTCOME_ALREADY_APPLIED


async def _applied_wave(session_maker, tmp_path):
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)
    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())
        frozen, _path = await _freeze(tmp_path, plan)
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)
    clear_multi_service_catalog_cache()
    return frozen


async def test_a_partially_applied_wave_is_never_mistaken_for_idempotency(session_maker, tmp_path) -> None:
    frozen = await _applied_wave(session_maker, tmp_path)

    # Put one record back to its version 1 projection: a partial state.
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, PRODUCTION_WAVE[0][0])
            assert record is not None
            raw = copy.deepcopy(record.raw)
            raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY] = _stored_v1_snapshot(
                _wave_uuid(0), starts_at=PRODUCTION_WAVE[0][1]
            ).as_dict()
            record.raw = raw

    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0)


@pytest.mark.parametrize("drift", ["neighbour-raw", "job", "outbox", "live", "new-candidate"])
async def test_a_reconcile_refuses_any_drift(session_maker, tmp_path, drift: str) -> None:
    frozen = await _applied_wave(session_maker, tmp_path)
    reader = _wave_reader()

    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, PRODUCTION_WAVE[0][0])
            assert record is not None
            if drift == "neighbour-raw":
                record.raw = {**copy.deepcopy(record.raw), "tampered": True}
            elif drift == "job":
                session.add(
                    MessageJob(
                        provider="easyweek",
                        company_id=KARLSRUHE_COMPANY_ID,
                        record_id=record.id,
                        client_id=record.client_id,
                        job_type="record_created",
                        run_at=NOW,
                        status="queued",
                        dedupe_key="snapshot-recovery-reconcile-job",
                        payload={},
                    )
                )
            elif drift == "outbox":
                session.add(
                    OutboxMessage(
                        company_id=KARLSRUHE_COMPANY_ID,
                        client_id=record.client_id,
                        record_id=record.id,
                        phone_e164="+49000000777",
                        template_code="record_created",
                        body="fixture",
                        status="queued",
                        scheduled_at=NOW,
                        meta={},
                    )
                )
            elif drift == "new-candidate":
                await _seed(
                    session,
                    record_id=8999,
                    booking_uuid="78888888-2222-4333-8444-555555555555",
                    booking_id=BOOKING_ID + 900,
                    starts_at=NOW + timedelta(days=40),
                )

    if drift == "live":
        reader = _wave_reader(canceled=True)

    async with session_maker() as session:
        with pytest.raises(RecoveryError):
            await apply_snapshot_recovery_plan(session, frozen=frozen, client=reader, now=NOW, pause_sec=0)


# ===========================================================================
# Review finding 1: the raw-row signature digest is not a business field
#
# A stored version 1 was proved from a two-row response; the version 2 that
# would replace it is proved from the same booking after EasyWeek started
# returning the resource row. §38.6: unknown technical fields may differ as
# long as every known business field matches. Comparing the whole
# MultiServiceLine — which carries a digest over the raw row minus its UUID —
# refused exactly that legitimate upgrade.
# ===========================================================================

STORED_TECHNICAL = "a"
LIVE_TECHNICAL = "b"


async def test_a_technical_only_difference_between_the_two_observations_still_migrates(
    session_maker,
    tmp_path,
) -> None:
    stored = _stored_v1_snapshot(technical=STORED_TECHNICAL)
    live_payload = _api(technical=LIVE_TECHNICAL)
    target = prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=uuid.UUID(BOOKING_UUID),
            location_uuid=KARLSRUHE_LOCATION_UUID,
            service_name=SHELLAC,
            service_related=PEDIKUERE_GEL,
            services_description=f"{SHELLAC}, {PEDIKUERE_GEL}",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=Decimal(TOTAL) / Decimal(100),
            company_id=KARLSRUHE_COMPANY_ID,
            service_id=SHELLAC_ID,
        ),
        booking_payload=live_payload,
        catalog_rows=_catalog(),
    )

    # Both projections come from the shared production resolver, and each is
    # independently digest-valid.
    assert stored.version == MULTI_SERVICE_SNAPSHOT_VERSION
    assert target.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
    assert stored.booking_uuid == target.booking_uuid
    assert stored.location_uuid == target.location_uuid

    # Every business field agrees...
    for field in (
        "display_name",
        "normalized_name",
        "category",
        "currency",
        "actual_price_minor",
        "actual_duration_minutes",
        "original_duration_minutes",
    ):
        assert [getattr(line, field) for line in stored.lines] == [getattr(line, field) for line in target.lines]

    # ...and the raw-row signature digests genuinely do NOT, because only a
    # technical field moved between the two live observations.
    assert [line.business_signature_digest for line in stored.lines] != [
        line.business_signature_digest for line in target.lines
    ]

    async with session_maker() as session:
        async with session.begin():
            await _seed(session, stored_technical=STORED_TECHNICAL)
        before_counts = await _row_counts(session)
        record = (await session.execute(select(Record))).scalars().one()
        raw_before = copy.deepcopy(record.raw)

    reader = FakeReader(bookings={BOOKING_UUID: live_payload})
    async with session_maker() as session:
        plan = await _plan(session, reader=reader)

    assert plan.records[0]["disposition"] == MIGRATE
    assert plan.records[0]["refusal_reason"] is None
    assert plan.summary["apply_ready"] is True

    frozen, _path = await _freeze(tmp_path, plan)
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        result = await apply_snapshot_recovery_plan(
            session,
            frozen=frozen,
            client=FakeReader(bookings={BOOKING_UUID: live_payload}),
            now=NOW,
            pause_sec=0,
        )
    assert len(result.migrated) == 1

    report_path = write_private_json(result.report(), tmp_path / "apply.json")
    reread = read_apply_report(report_path, frozen=frozen)

    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        verified = await verify_snapshot_recovery(
            session,
            frozen=frozen,
            apply_report=reread,
            client=FakeReader(bookings={BOOKING_UUID: live_payload}),
            pause_sec=0,
        )
    assert verified["passed"] is True
    assert verified["still_version_1_record_ids"] == []

    async with session_maker() as session:
        assert await _row_counts(session) == before_counts
        record = (await session.execute(select(Record))).scalars().one()
        stored_after, error = multi_service_snapshot_from_record_raw(record.raw)
        assert error is None and stored_after is not None
        assert stored_after.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
        assert stored_after.digest == target.digest
        # The record is no longer a stale digest for the preflight.
        assert (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one() == 0

    # Every neighbouring key of Record.raw survived untouched.
    assert set(record.raw) == set(raw_before)
    assert set(record.raw["easyweek"]) == set(raw_before["easyweek"])
    assert record.raw["easyweek"]["services_count"] == raw_before["easyweek"]["services_count"]

    # A second plan no longer sees it as version 1 work.
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        second = await _plan(session, reader=FakeReader(bookings={BOOKING_UUID: live_payload}))
    assert second.records == ()


async def test_the_semantic_projection_excludes_only_the_raw_row_signature() -> None:
    """The contract of the comparison, stated once and pinned."""
    assert set(_SEMANTIC_LINE_FIELDS) == {
        "display_name",
        "normalized_name",
        "category",
        "currency",
        "actual_price_minor",
        "actual_duration_minutes",
        "original_duration_minutes",
    }
    # `as_dict()` is untouched and still carries the digest the snapshot needs.
    line = _stored_v1_snapshot().lines[0]
    assert "business_signature_digest" in line.as_dict()
    assert "business_signature_digest" not in _ordered_semantic_projection(_stored_v1_snapshot())[0]


# ===========================================================================
# Review finding 2: the operator's report check must be bound to THIS plan
#
# A refused apply does not rewrite the report file, so the permanent path can
# still hold a report from an earlier plan. A check that only validates the
# report's internal lists would give that stale file a clean exit code.
# ===========================================================================


def _runbook_report_command() -> str:
    text = RUNBOOK.read_text(encoding="utf-8")
    section = text.split("## 27a.", 1)[1].split("## 28.", 1)[0]
    blocks = [part for index, part in enumerate(section.split("```")) if index % 2 == 1]
    assert len(blocks) == 1, "section 27a must document exactly one command block"
    body = blocks[0]
    assert body.startswith("bash\n")
    return body[len("bash\n") :]


def _run_runbook_report_check(plan_payload: object, report_payload: object, tmp_path) -> subprocess.CompletedProcess:
    command = _runbook_report_command().replace("cd /opt/altegio_bot\n", "")
    plan_file = tmp_path / "rb-plan.json"
    report_file = tmp_path / "rb-apply.json"
    for path, payload in ((plan_file, plan_payload), (report_file, report_payload)):
        if payload is None:
            path.unlink(missing_ok=True)
        elif isinstance(payload, str):
            path.write_text(payload, encoding="utf-8")
        else:
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    command = command.replace(
        "PLAN=outputs/easyweek_multi_service_recovery/snapshot-plan.json",
        f"PLAN={shlex.quote(str(plan_file))}",
    )
    command = command.replace(
        "REPORT=outputs/easyweek_multi_service_recovery/snapshot-apply.json",
        f"REPORT={shlex.quote(str(report_file))}",
    )
    return subprocess.run(["bash", "-c", command], capture_output=True, text=True, check=False)


async def _plan_and_reports(session_maker, tmp_path):
    """One real frozen plan plus its genuine applied and reconciled reports."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_production_wave(session)
    async with session_maker() as session:
        plan = await _plan(session, reader=_wave_reader())
        frozen, _path = await _freeze(tmp_path, plan)
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        applied = await apply_snapshot_recovery_plan(
            session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0
        )
    clear_multi_service_catalog_cache()
    async with session_maker() as session:
        reconciled = await apply_snapshot_recovery_plan(
            session, frozen=frozen, client=_wave_reader(), now=NOW, pause_sec=0
        )
    return plan.snapshot(), applied.report(), reconciled.report()


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
async def test_the_documented_report_check_accepts_both_real_outcomes(session_maker, tmp_path) -> None:
    plan_payload, applied, reconciled = await _plan_and_reports(session_maker, tmp_path)

    for report, outcome, this_run, already in (
        (applied, OUTCOME_APPLIED, CANONICAL_ORDER, []),
        (reconciled, OUTCOME_ALREADY_APPLIED, [], CANONICAL_ORDER),
    ):
        result = _run_runbook_report_check(plan_payload, report, tmp_path)
        assert result.returncode == 0, result.stderr
        printed = json.loads(result.stdout)
        assert printed["outcome"] == outcome
        assert printed["migrated_record_ids"] == CANONICAL_ORDER
        assert printed["migrated_this_run_record_ids"] == this_run
        assert printed["already_applied_record_ids"] == already
        for forbidden in (BOOKING_UUID[:8], SHELLAC, PEDIKUERE_GEL, "+49000000777"):
            assert forbidden not in result.stdout


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
async def test_a_report_from_an_earlier_plan_is_refused(session_maker, tmp_path) -> None:
    """The exact production trap: same record ids, different plan."""
    plan_payload, applied, _reconciled = await _plan_and_reports(session_maker, tmp_path)

    stale = {**applied, "plan_digest": "f" * 64}
    unsigned = {key: value for key, value in stale.items() if key != "report_digest"}
    stale["report_digest"] = _digest(unsigned)

    # The stale report is internally perfectly consistent.
    assert stale["migrated_record_ids"] == applied["migrated_record_ids"]
    result = _run_runbook_report_check(plan_payload, stale, tmp_path)
    assert result.returncode != 0, result.stdout


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "mode",
        "halted",
        "duplicate-id",
        "non-integer-id",
        "missing-id",
        "extra-id",
        "overlapping-partition",
        "applied-with-already",
        "already-with-this-run",
        "wrong-migrated-count",
        "job-created",
        "outbox-changed",
        "missing-counts",
    ],
)
async def test_the_documented_report_check_refuses_every_inconsistency(
    session_maker,
    tmp_path,
    damage: str,
) -> None:
    plan_payload, applied, reconciled = await _plan_and_reports(session_maker, tmp_path)
    report = copy.deepcopy(applied)
    ids = list(report["migrated_record_ids"])

    if damage == "version":
        report["version"] = 1
    elif damage == "mode":
        report["mode"] = "plan"
    elif damage == "halted":
        report["halted"] = True
    elif damage == "duplicate-id":
        report["migrated_record_ids"] = [ids[0], *ids]
        report["migrated_this_run_record_ids"] = [ids[0], *ids]
    elif damage == "non-integer-id":
        report["migrated_record_ids"] = [f"{ids[0]}", *ids[1:]]
    elif damage == "missing-id":
        report["migrated_record_ids"] = ids[:-1]
        report["migrated_this_run_record_ids"] = ids[:-1]
    elif damage == "extra-id":
        report["migrated_record_ids"] = [*ids, 99999]
        report["migrated_this_run_record_ids"] = [*ids, 99999]
    elif damage == "overlapping-partition":
        report["migrated_this_run_record_ids"] = ids
        report["already_applied_record_ids"] = [ids[0]]
    elif damage == "applied-with-already":
        report["migrated_this_run_record_ids"] = ids[:-1]
        report["already_applied_record_ids"] = ids[-1:]
    elif damage == "already-with-this-run":
        report = copy.deepcopy(reconciled)
        report["migrated_this_run_record_ids"] = list(report["already_applied_record_ids"])
        report["already_applied_record_ids"] = []
    elif damage == "wrong-migrated-count":
        report["mutation_counts"]["records_snapshot_migrated"] = 0
    elif damage == "job-created":
        report["mutation_counts"]["message_jobs_created"] = 1
    elif damage == "outbox-changed":
        report["mutation_counts"]["outbox_messages_changed"] = 1
    else:
        report.pop("mutation_counts")

    unsigned = {key: value for key, value in report.items() if key != "report_digest"}
    report["report_digest"] = _digest(unsigned)

    result = _run_runbook_report_check(plan_payload, report, tmp_path)
    assert result.returncode != 0, f"{damage} was accepted: {result.stdout}"


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
@pytest.mark.parametrize("missing", ["plan", "report"])
async def test_the_documented_report_check_refuses_a_missing_or_malformed_file(
    session_maker,
    tmp_path,
    missing: str,
) -> None:
    plan_payload, applied, _reconciled = await _plan_and_reports(session_maker, tmp_path)

    absent = _run_runbook_report_check(
        None if missing == "plan" else plan_payload,
        None if missing == "report" else applied,
        tmp_path,
    )
    assert absent.returncode != 0, absent.stdout

    malformed = _run_runbook_report_check(
        "{not json" if missing == "plan" else plan_payload,
        "{not json" if missing == "report" else applied,
        tmp_path,
    )
    assert malformed.returncode != 0, malformed.stdout
