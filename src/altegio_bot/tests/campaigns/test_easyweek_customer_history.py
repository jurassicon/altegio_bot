"""PR-15 strict EasyWeek customer booking-history live guard."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from altegio_bot.campaigns.easyweek_customer_history import (
    API_CONFIGURATION_UNAVAILABLE,
    API_RETRYABLE_UNCERTAINTY,
    BOOKING_CANCELED,
    BOOKING_LOCATION_MISMATCH,
    BOOKING_SERVICE_COUNT_UNPROVEN,
    CUSTOMER_NOT_FOUND,
    CUSTOMER_RESPONSE_MALFORMED,
    CUSTOMER_UUID_MISMATCH,
    HISTORY_ACTIVE_FUTURE_BOOKING,
    HISTORY_COMPLETED_VISITS_NOT_ONE,
    HISTORY_DUPLICATE_BOOKING_UUID,
    HISTORY_IDENTITY_MISMATCH,
    HISTORY_PAGINATION_INCOMPLETE,
    HISTORY_PAGINATION_INCONSISTENT,
    HISTORY_PAGINATION_UNBOUNDED,
    HISTORY_ROW_MALFORMED,
    HISTORY_SOURCE_BOOKING_ABSENT,
    HISTORY_SOURCE_BOOKING_STATE_MISMATCH,
    HISTORY_UNREADABLE,
    MAX_CUSTOMER_HISTORY_PAGES,
    SOURCE_BOOKING_MALFORMED,
    SOURCE_BOOKING_NOT_COMPLETED,
    SOURCE_CUSTOMER_UUID_UNPROVEN,
    LiveProofDisposition,
    prove_customer_booking_history,
)
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekNotFoundError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_locations import EasyWeekLocation
from altegio_bot.scripts.easyweek_campaign_preflight import CampaignPreflightReport

BOOKING_UUID = uuid.UUID("11111111-2222-4333-8444-555555555555")
CUSTOMER_UUID = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
OTHER_UUID = uuid.UUID("99999999-8888-4777-8666-555555555555")
NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
LOCATION = EasyWeekLocation(
    name="test",
    location_id=1,
    location_uuid="bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
    meta_template_prefix="test",
    booking_page_url="https://example.invalid/",
)


def _booking(booking_uuid: uuid.UUID = BOOKING_UUID, **changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "uuid": str(booking_uuid),
        "location_uuid": LOCATION.location_uuid,
        "customer": {"uuid": str(CUSTOMER_UUID)},
        "is_canceled": False,
        "is_completed": True,
        "start_time": "2026-08-01T10:00:00+00:00",
        "ordered_services": [{}],
    }
    value.update(changes)
    return value


def _page(rows: list[dict[str, Any]], *, current: int = 1, last: int = 1, total: int | None = None):
    return {
        "data": rows,
        "meta": {
            "current_page": current,
            "last_page": last,
            "per_page": 100,
            "total": len(rows) if total is None else total,
        },
    }


class _Reader:
    def __init__(
        self,
        *,
        source: object | Exception | None = None,
        customer: object | Exception | None = None,
        pages: dict[int, object | Exception] | None = None,
    ) -> None:
        self.source = _booking() if source is None else source
        self.customer = {"uuid": str(CUSTOMER_UUID)} if customer is None else customer
        self.pages = {1: _page([_booking()])} if pages is None else pages
        self.calls: list[tuple[str, object]] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(("booking", booking_uuid))
        if isinstance(self.source, Exception):
            raise self.source
        return self.source  # type: ignore[return-value]

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.calls.append(("customer", customer_uuid))
        if isinstance(self.customer, Exception):
            raise self.customer
        return self.customer  # type: ignore[return-value]

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        self.calls.append(("history", (customer_uuid, page, per_page)))
        value = self.pages[page]
        if isinstance(value, Exception):
            raise value
        return value  # type: ignore[return-value]


async def _prove(reader: _Reader, **kwargs: Any):
    return await prove_customer_booking_history(
        reader,
        expected_booking_uuid=BOOKING_UUID,
        location=LOCATION,
        now=NOW,
        **kwargs,
    )


async def test_happy_path_is_live_guard_ready_but_not_delivery_authority() -> None:
    result = await _prove(_Reader())
    assert result.live_guard_ready is True
    assert result.source_booking_current is True
    assert result.customer_identity_current is True
    assert result.history_complete is True
    assert result.completed_visit_count == 1
    assert result.first_visit_current is True
    assert result.no_active_future_booking is True
    assert result.reason is None
    assert result.disposition is LiveProofDisposition.READY
    assert result.pages_read == 1


def test_preflight_report_is_pii_free_and_never_authorizes_delivery() -> None:
    report = CampaignPreflightReport(
        run_id=36,
        candidate_count=1,
        checked_count=1,
        local_eligible_count=1,
        source_booking_current_count=1,
        customer_identity_current_count=1,
        history_complete_count=1,
        first_visit_current_count=1,
        no_active_future_booking_count=1,
        live_guard_ready_count=1,
        pages_read=1,
    )
    safe = report.as_safe_dict()
    rendered = repr(safe)
    assert safe["live_guard_ready"] is True
    assert safe["delivery_authorized"] is False
    assert safe["ready_for_send"] is False
    for secret in (str(BOOKING_UUID), str(CUSTOMER_UUID), "+4915112345678", "person@example.test"):
        assert secret not in rendered


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"customer": None}, SOURCE_CUSTOMER_UUID_UNPROVEN),
        ({"customer": {}}, SOURCE_CUSTOMER_UUID_UNPROVEN),
        ({"customer": {"uuid": "bad"}}, SOURCE_CUSTOMER_UUID_UNPROVEN),
        ({"is_completed": False}, SOURCE_BOOKING_NOT_COMPLETED),
        ({"is_completed": 1}, SOURCE_BOOKING_MALFORMED),
        ({"is_canceled": 0}, SOURCE_BOOKING_MALFORMED),
        ({"is_canceled": True}, BOOKING_CANCELED),
        ({"location_uuid": "wrong"}, BOOKING_LOCATION_MISMATCH),
        ({"ordered_services": []}, BOOKING_SERVICE_COUNT_UNPROVEN),
        ({"start_time": "2026-08-01T10:00:00"}, SOURCE_BOOKING_MALFORMED),
    ],
)
async def test_source_booking_failures_stop_before_customer(change: dict[str, Any], reason: str) -> None:
    reader = _Reader(source=_booking(**change))
    result = await _prove(reader)
    assert result.reason == reason
    assert result.live_guard_ready is False
    assert [kind for kind, _ in reader.calls] == ["booking"]


@pytest.mark.parametrize(
    ("customer", "reason", "retryable"),
    [
        (EasyWeekNotFoundError("safe"), CUSTOMER_NOT_FOUND, False),
        (EasyWeekAuthError("safe"), API_CONFIGURATION_UNAVAILABLE, False),
        (EasyWeekRetryableError("safe"), API_RETRYABLE_UNCERTAINTY, True),
        (EasyWeekProtocolError("safe"), CUSTOMER_RESPONSE_MALFORMED, False),
        ({"data": []}, CUSTOMER_RESPONSE_MALFORMED, False),
        ({"uuid": str(OTHER_UUID)}, CUSTOMER_UUID_MISMATCH, False),
    ],
)
async def test_customer_card_failures(customer: object, reason: str, retryable: bool) -> None:
    result = await _prove(_Reader(customer=customer))
    assert result.reason == reason
    assert result.retryable_uncertainty is retryable
    assert result.live_guard_ready is False


@pytest.mark.parametrize(
    ("page", "reason"),
    [
        ({"data": []}, HISTORY_PAGINATION_INCOMPLETE),
        (_page([_booking()], current=2), HISTORY_PAGINATION_INCONSISTENT),
        (_page([_booking()], last=MAX_CUSTOMER_HISTORY_PAGES + 1, total=5001), HISTORY_PAGINATION_UNBOUNDED),
        (_page([], total=0), HISTORY_SOURCE_BOOKING_ABSENT),
        (_page([_booking(customer={"uuid": str(OTHER_UUID)})]), HISTORY_IDENTITY_MISMATCH),
        (_page([_booking(customer={})]), HISTORY_ROW_MALFORMED),
        (_page([_booking(is_completed=1)]), HISTORY_ROW_MALFORMED),
        (_page([_booking(start_time="2026-08-01T10:00:00")]), HISTORY_ROW_MALFORMED),
        (_page([_booking(is_completed=False)]), HISTORY_SOURCE_BOOKING_STATE_MISMATCH),
    ],
)
async def test_history_contract_refusals(page: object, reason: str) -> None:
    result = await _prove(_Reader(pages={1: page}))
    assert result.reason == reason
    assert result.live_guard_ready is False


async def test_duplicate_booking_uuid_is_refused() -> None:
    result = await _prove(_Reader(pages={1: _page([_booking(), _booking()], total=2)}))
    assert result.reason == HISTORY_DUPLICATE_BOOKING_UUID


async def test_two_completed_visits_are_business_exclusion() -> None:
    result = await _prove(_Reader(pages={1: _page([_booking(), _booking(OTHER_UUID)], total=2)}))
    assert result.reason == HISTORY_COMPLETED_VISITS_NOT_ONE
    assert result.completed_visit_count == 2
    assert result.disposition is LiveProofDisposition.BUSINESS_EXCLUSION


async def test_active_future_booking_blocks_even_when_not_completed() -> None:
    future = _booking(OTHER_UUID, is_completed=False, start_time=(NOW + timedelta(days=1)).isoformat())
    result = await _prove(_Reader(pages={1: _page([_booking(), future], total=2)}))
    assert result.reason == HISTORY_ACTIVE_FUTURE_BOOKING
    assert result.first_visit_current is True


async def test_canceled_future_booking_does_not_block() -> None:
    future = _booking(
        OTHER_UUID,
        is_canceled=True,
        is_completed=False,
        start_time=(NOW + timedelta(days=1)).isoformat(),
    )
    result = await _prove(_Reader(pages={1: _page([_booking(), future], total=2)}))
    assert result.live_guard_ready is True


async def test_all_pages_are_read_sequentially_with_pacing() -> None:
    rows = [_booking()]
    rows.extend(_booking(uuid.uuid4(), is_canceled=True, is_completed=False) for _ in range(100))
    reader = _Reader(
        pages={
            1: _page(rows[:100], current=1, last=2, total=101),
            2: _page(rows[100:], current=2, last=2, total=101),
        }
    )
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = await _prove(reader, pause=sleep, pause_sec=0.25)
    assert result.live_guard_ready is True
    assert result.pages_read == 2
    assert [kind for kind, _ in reader.calls] == ["booking", "customer", "history", "history"]
    assert sleeps == [0.25, 0.25, 0.25]


async def test_changed_pagination_and_second_page_error_fail_closed() -> None:
    rows = [_booking()]
    rows.extend(_booking(uuid.uuid4(), is_canceled=True, is_completed=False) for _ in range(99))
    changed = _Reader(
        pages={
            1: _page(rows, current=1, last=2, total=101),
            2: _page([_booking(uuid.uuid4())], current=2, last=2, total=102),
        }
    )
    assert (await _prove(changed)).reason == HISTORY_PAGINATION_INCONSISTENT

    failed = _Reader(
        pages={
            1: _page(rows, current=1, last=2, total=101),
            2: EasyWeekProtocolError("safe"),
        }
    )
    result = await _prove(failed)
    assert result.reason == HISTORY_UNREADABLE
    assert result.pages_read == 1
