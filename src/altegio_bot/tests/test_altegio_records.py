from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
import pytest
import respx

from altegio_bot import altegio_records as records_mod
from altegio_bot.altegio_records import AmbiguousRecordError

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_BASE = "https://api.test/api/v1"
_NOW = datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc)

# A date clearly in the future from _NOW (after Belgrade DST, UTC+2 in April).
# "2026-04-15 18:30:00" Belgrade = 16:30 UTC, which is > 10:00 UTC Apr 10.
_FUTURE_DATE = "2026-04-15 18:30:00"

# A date clearly in the past from _NOW.
# "2026-04-01 10:00:00" Belgrade = 08:00 UTC Apr 1 < 10:00 UTC Apr 10.
_PAST_DATE = "2026-04-01 10:00:00"


def _mock_settings(monkeypatch) -> None:
    monkeypatch.setattr(records_mod.settings, "altegio_api_base_url", _BASE)
    monkeypatch.setattr(records_mod.settings, "altegio_partner_token", "partner-token")
    monkeypatch.setattr(records_mod.settings, "altegio_user_token", "user-token")


def _single_page(records: list[dict]) -> dict:
    return {"data": records}


# ---------------------------------------------------------------------------
# client_has_future_appointments — basic cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_detects_future_record(
    monkeypatch,
) -> None:
    """A non-deleted, confirmed future record → True."""
    _mock_settings(monkeypatch)

    route = respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 10, "deleted": False, "date": _FUTURE_DATE}]),
        )
    )

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is True
    assert route.called
    request = route.calls[0].request
    assert request.url.params["client_id"] == "9001"
    assert request.url.params["page"] == "1"
    assert request.url.params["count"] == "200"


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_ignores_past_record(
    monkeypatch,
) -> None:
    """A past record (not deleted) → False."""
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 20, "deleted": False, "date": _PAST_DATE}]),
        )
    )

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_ignores_deleted_future_record(
    monkeypatch,
) -> None:
    """A future record marked deleted → False (deleted records never count)."""
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 30, "deleted": True, "date": _FUTURE_DATE}]),
        )
    )

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_ignores_cancelled_future_record(
    monkeypatch,
) -> None:
    """A future record with confirmed=0 (cancelled in Altegio) → False.

    Assumption: confirmed==0 means the appointment was cancelled, consistent
    with CONFIRMED_FLAG=1 in campaigns/segment.py and how inbox_worker stores
    the confirmed field from the Altegio API.
    """
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 40, "deleted": False, "confirmed": 0, "date": _FUTURE_DATE}]),
        )
    )

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_treats_confirmed_1_as_active(
    monkeypatch,
) -> None:
    """A future record with confirmed=1 → True (explicitly confirmed)."""
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 50, "deleted": False, "confirmed": 1, "date": _FUTURE_DATE}]),
        )
    )

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is True


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_treats_absent_confirmed_as_active(
    monkeypatch,
) -> None:
    """A future record with no confirmed field → True (fail-safe: treat as active).

    Better to suppress repeat_10d than to send it to a client who might have
    an upcoming visit.
    """
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page(
                # No "confirmed" key at all.
                [{"id": 60, "deleted": False, "date": _FUTURE_DATE}]
            ),
        )
    )

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is True


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_returns_false_for_empty_list(
    monkeypatch,
) -> None:
    """No records in API response → False."""
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(return_value=httpx.Response(200, json={"data": []}))

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False


# ---------------------------------------------------------------------------
# Fail-safe: unparseable date on an active record → AmbiguousRecordError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_active_record_with_bad_date_raises_ambiguous_error(
    monkeypatch,
    caplog,
) -> None:
    """Active record (not deleted, no confirmed=0) with unparseable date raises
    AmbiguousRecordError.

    This is fail-safe: we cannot silently treat "unknown date" as "not future".
    The caller (repeat_10d guard) must requeue instead of sending.
    The parse failure is also logged as a warning for production visibility.
    """
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 77, "deleted": False, "date": "not-a-date", "datetime": "also-bad"}]),
        )
    )

    with caplog.at_level(logging.WARNING, logger="altegio_bot.altegio_records"):
        with pytest.raises(AmbiguousRecordError) as exc_info:
            await records_mod.client_has_future_appointments(
                company_id=1,
                altegio_client_id=9001,
                now_dt=_NOW,
            )

    assert "77" in str(exc_info.value)
    # _parse_record_starts_at also logged a warning for the non-empty bad data
    assert any("could not parse" in r.message for r in caplog.records)


@pytest.mark.asyncio
@respx.mock
async def test_active_record_with_no_date_fields_raises_ambiguous_error(
    monkeypatch,
) -> None:
    """Active record with no date fields at all → AmbiguousRecordError.

    A record without any date is indistinguishable from a future record
    with missing data, so we must not assume it is safe to send.
    """
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page(
                # No "date" or "datetime" keys at all.
                [{"id": 88, "deleted": False}]
            ),
        )
    )

    with pytest.raises(AmbiguousRecordError):
        await records_mod.client_has_future_appointments(
            company_id=1,
            altegio_client_id=9001,
            now_dt=_NOW,
        )


@pytest.mark.asyncio
@respx.mock
async def test_deleted_record_with_bad_date_does_not_raise(
    monkeypatch,
) -> None:
    """Deleted record with unparseable date → False, no AmbiguousRecordError.

    The deleted check short-circuits before any date parsing.
    """
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 99, "deleted": True, "date": "not-a-date"}]),
        )
    )

    # Must not raise
    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False


@pytest.mark.asyncio
@respx.mock
async def test_cancelled_record_with_bad_date_does_not_raise(
    monkeypatch,
) -> None:
    """Cancelled (confirmed=0) record with unparseable date → False, no raise.

    The confirmed==0 check short-circuits before any date parsing.
    """
    _mock_settings(monkeypatch)

    respx.get(f"{_BASE}/records/1").mock(
        return_value=httpx.Response(
            200,
            json=_single_page([{"id": 100, "deleted": False, "confirmed": 0, "date": "not-a-date"}]),
        )
    )

    # Must not raise
    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_finds_record_on_second_page(
    monkeypatch,
) -> None:
    """Future record on page 2 (page 1 is full of past records) → True.

    The first page returns exactly _PAGE_SIZE (200) past records, triggering a
    second request. The future record is on page 2.
    """
    _mock_settings(monkeypatch)

    page_size = records_mod._PAGE_SIZE
    past_records = [{"id": i, "deleted": False, "date": _PAST_DATE} for i in range(page_size)]
    future_record = {"id": 9999, "deleted": False, "date": _FUTURE_DATE}

    call_count = 0

    def _side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        page = int(request.url.params.get("page", 1))
        if page == 1:
            return httpx.Response(200, json={"data": past_records})
        return httpx.Response(200, json={"data": [future_record]})

    respx.get(f"{_BASE}/records/1").mock(side_effect=_side_effect)

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is True
    assert call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_client_has_future_appointments_stops_after_last_page(
    monkeypatch,
) -> None:
    """Two pages of past records → False; exactly 2 API calls made."""
    _mock_settings(monkeypatch)

    page_size = records_mod._PAGE_SIZE
    past_records = [{"id": i, "deleted": False, "date": _PAST_DATE} for i in range(page_size)]

    call_count = 0

    def _side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        page = int(request.url.params.get("page", 1))
        if page == 1:
            return httpx.Response(200, json={"data": past_records})
        # Page 2 is empty → no more pages.
        return httpx.Response(200, json={"data": []})

    respx.get(f"{_BASE}/records/1").mock(side_effect=_side_effect)

    has_future = await records_mod.client_has_future_appointments(
        company_id=1,
        altegio_client_id=9001,
        now_dt=_NOW,
    )

    assert has_future is False
    assert call_count == 2
