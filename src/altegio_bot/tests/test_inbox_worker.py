from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from altegio_bot.altegio_records import AltegioRecordResearchError
from altegio_bot.promo_discount_apply import is_promo_origin_comment
from altegio_bot.settings import settings
from altegio_bot.workers.inbox_worker import (
    _is_noop_update,
    _normalize_phone,
    _parse_starts_at,
    handle_event,
    parse_dt,
    resolve_booking_created_at_for_record_create,
)


class TestParseDt:
    """Tests for parse_dt DST handling."""

    def test_none_returns_none(self):
        assert parse_dt(None) is None

    def test_empty_string_returns_none(self):
        assert parse_dt("") is None

    def test_invalid_string_returns_none(self):
        assert parse_dt("not-a-date") is None

    def test_winter_time_utc_offset(self):
        """In winter (CET) Europe/Belgrade is UTC+1."""
        dt = parse_dt("2024-01-15 10:00:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 3600  # +01:00

    def test_summer_time_utc_offset(self):
        """In summer (CEST) Europe/Belgrade is UTC+2."""
        dt = parse_dt("2024-07-15 10:00:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 7200  # +02:00

    def test_aware_string_unchanged(self):
        """A string with explicit offset is returned as-is (no normalization applied)."""
        dt = parse_dt("2024-07-15T10:00:00+05:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 5 * 3600

    def test_dst_spring_forward(self):
        """Last moment of winter time: 2024-03-31 01:59 → UTC+1."""
        dt = parse_dt("2024-03-31 01:59:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 3600  # still CET

    def test_dst_first_summer_moment(self):
        """First moment of summer time: 2024-03-31 03:00 → UTC+2."""
        dt = parse_dt("2024-03-31 03:00:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 7200  # now CEST

    def test_dst_fall_back(self):
        """First moment of winter time after fallback: 2024-10-27 03:00 → UTC+1."""
        dt = parse_dt("2024-10-27 03:00:00")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 3600  # back to CET

    def test_isoformat_with_space_separator(self):
        """Altegio sometimes sends 'YYYY-MM-DD HH:MM:SS' with space instead of T."""
        dt = parse_dt("2024-06-01 12:30:00")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 6
        assert dt.day == 1

    def test_offset_without_colon_normalized(self):
        """Offsets like +0200 (no colon) should be parsed correctly."""
        dt = parse_dt("2024-07-15T10:00:00+0200")
        assert dt is not None
        assert dt.utcoffset().total_seconds() == 7200


class TestStartsAtParsing:
    """Tests for _parse_starts_at DST and field-priority logic."""

    def test_date_field_winter(self):
        """date present, winter (CET UTC+1): 10:30 local → 09:30 UTC."""
        result = _parse_starts_at({"date": "2026-01-15 10:30:00"})
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 9
        assert result.minute == 30

    def test_date_field_summer(self):
        """date present, summer (CEST UTC+2): 10:30 local → 08:30 UTC."""
        result = _parse_starts_at({"date": "2026-07-15 10:30:00"})
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 8
        assert result.minute == 30

    def test_date_field_preferred_over_datetime(self):
        """date is always used even when datetime is also present."""
        result = _parse_starts_at(
            {
                "date": "2026-07-15 10:30:00",
                # Wrong offset (+01:00 instead of +02:00) — must be ignored.
                "datetime": "2026-07-15T10:30:00+01:00",
            }
        )
        assert result is not None
        assert result.tzinfo == timezone.utc
        # Should be 08:30 UTC (Europe/Belgrade summer = UTC+2), not 09:30 UTC.
        assert result.hour == 8
        assert result.minute == 30

    def test_datetime_fallback_strips_bad_offset(self):
        """date absent: datetime fallback strips the wrong offset and applies TZ."""
        # datetime carries a wrong +01:00 offset (should be +02:00 in summer).
        result = _parse_starts_at({"datetime": "2026-07-15T10:30:00+01:00"})
        assert result is not None
        assert result.tzinfo == timezone.utc
        # Local wall-clock 10:30 in Europe/Belgrade summer (UTC+2) → 08:30 UTC.
        assert result.hour == 8
        assert result.minute == 30

    def test_both_absent_returns_none(self):
        """Neither date nor datetime present → None."""
        assert _parse_starts_at({}) is None
        assert _parse_starts_at({"date": None, "datetime": None}) is None

    def test_dst_spring_forward(self):
        """2026-03-29 03:00 is summer time (CEST UTC+2): → 01:00 UTC."""
        result = _parse_starts_at({"date": "2026-03-29 03:00:00"})
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 1
        assert result.minute == 0

    def test_invalid_date_falls_back_to_datetime(self):
        """Malformed date value falls back to datetime field."""
        result = _parse_starts_at({"date": "not-a-date", "datetime": "2026-01-15T10:30:00+01:00"})
        assert result is not None
        assert result.hour == 9
        assert result.minute == 30

    def test_invalid_date_and_short_datetime_returns_none(self):
        """Malformed date and too-short datetime → None."""
        assert _parse_starts_at({"date": "bad", "datetime": "2026"}) is None

    def test_invalid_both_returns_none(self):
        """Both fields malformed → None."""
        assert _parse_starts_at({"date": "bad", "datetime": "also-bad"}) is None

    def test_upsert_record_uses_date_field_not_datetime(self):
        """date field (local naive) must be used; datetime offset is ignored."""
        # Local time 10:30 in Europe/Belgrade winter (UTC+1) → 09:30 UTC.
        # The wrong +03:00 offset in datetime must be completely ignored.
        TZ = ZoneInfo("Europe/Belgrade")
        naive = datetime(2026, 1, 15, 10, 30, 0)
        expected_utc = naive.replace(tzinfo=TZ).astimezone(timezone.utc)

        result = _parse_starts_at(
            {
                "date": "2026-01-15 10:30:00",
                "datetime": "2026-01-15T10:30:00+03:00",  # wrong offset — must be IGNORED
            }
        )

        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result == expected_utc  # 09:30 UTC

    def test_dst_fall_back(self):
        """DST fallback: 2026-10-25 10:30 → CET (UTC+1) → 09:30 UTC."""
        result = _parse_starts_at({"date": "2026-10-25 10:30:00"})
        assert result is not None
        assert result == datetime(2026, 10, 25, 9, 30, tzinfo=timezone.utc)

    def test_upsert_record_fallback_to_datetime_strips_offset(self):
        """When 'date' is absent, fallback uses 'datetime' but strips its offset."""
        # datetime carries +03:00 — entirely wrong. The bare wall-clock time
        # 10:30 should be interpreted as Europe/Belgrade (UTC+1 in January).
        TZ = ZoneInfo("Europe/Belgrade")
        naive = datetime(2026, 1, 15, 10, 30, 0)
        expected_utc = naive.replace(tzinfo=TZ).astimezone(timezone.utc)

        result = _parse_starts_at(
            {
                "datetime": "2026-01-15T10:30:00+03:00",  # offset must be stripped
            }
        )

        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result == expected_utc  # 09:30 UTC


class TestNormalizePhone:
    """Tests for _normalize_phone."""

    def test_none_returns_none(self):
        assert _normalize_phone(None) is None

    def test_empty_string_returns_none(self):
        assert _normalize_phone("") is None

    def test_digits_only_adds_plus(self):
        assert _normalize_phone("4917637706557") == "+4917637706557"

    def test_already_has_plus(self):
        assert _normalize_phone("+4917637706557") == "+4917637706557"

    def test_strips_spaces_and_dashes(self):
        assert _normalize_phone("+49 176-3770-6557") == "+4917637706557"

    def test_whitespace_only_returns_none(self):
        assert _normalize_phone("   ") is None


class TestBookingCreatedAtResolver:
    """Tests for record-create booking_created_at resolution."""

    async def test_payload_create_date_is_used_without_get_record(self):
        record = MagicMock(altegio_record_id=123456789)
        fetch_mock = AsyncMock(side_effect=AssertionError("GET /record must not be called"))

        with patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock):
            result = await resolve_booking_created_at_for_record_create(
                company_id=758285,
                record_data={"id": 123456789, "create_date": "2026-05-10 14:22:00"},
                record=record,
            )

        fetch_mock.assert_not_called()
        assert result == datetime(2026, 5, 10, 12, 22, tzinfo=timezone.utc)

    async def test_missing_payload_timestamp_fetches_get_record_create_date(self):
        record = MagicMock(altegio_record_id=123456789)
        fetch_mock = AsyncMock(return_value={"id": 123456789, "create_date": "2026-05-10 14:22:00"})

        with (
            patch.object(settings, "promo_location_id_by_company", '{"758285": 9001}'),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
        ):
            result = await resolve_booking_created_at_for_record_create(
                company_id=758285,
                record_data={"id": 123456789},
                record=record,
            )

        fetch_mock.assert_awaited_once_with(location_id=9001, record_id=123456789)
        assert result == datetime(2026, 5, 10, 12, 22, tzinfo=timezone.utc)

    async def test_get_record_without_create_date_returns_none(self):
        record = MagicMock(altegio_record_id=123456789)
        fetch_mock = AsyncMock(return_value={"id": 123456789, "date": "2026-05-20 12:00:00"})

        with (
            patch.object(settings, "promo_location_id_by_company", '{"758285": 9001}'),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
        ):
            result = await resolve_booking_created_at_for_record_create(
                company_id=758285,
                record_data={"id": 123456789},
                record=record,
            )

        assert result is None

    async def test_get_record_http_error_returns_none(self):
        record = MagicMock(altegio_record_id=123456789)
        fetch_mock = AsyncMock(side_effect=AltegioRecordResearchError("HTTP 500: location_id=9001 record_id=123456789"))

        with (
            patch.object(settings, "promo_location_id_by_company", '{"758285": 9001}'),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
        ):
            result = await resolve_booking_created_at_for_record_create(
                company_id=758285,
                record_data={"id": 123456789},
                record=record,
            )

        assert result is None

    async def test_received_at_is_not_a_fallback(self):
        record = MagicMock(altegio_record_id=123456789)
        fetch_mock = AsyncMock(return_value={"id": 123456789})

        with (
            patch.object(settings, "promo_location_id_by_company", '{"758285": 9001}'),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
        ):
            result = await resolve_booking_created_at_for_record_create(
                company_id=758285,
                record_data={
                    "id": 123456789,
                    "date": "2026-05-20 12:00:00",
                    "last_change_date": "2026-05-11 09:00:00",
                },
                record=record,
            )

        assert result is None


class TestHandleEventVisitAttendance:
    """Tests that visit_attendance update events are skipped without creating jobs."""

    def _make_event(self, visit_attendance: int, event_status: str = "update") -> MagicMock:
        event = MagicMock()
        event.id = 1
        event.company_id = 123
        event.resource = "record"
        event.resource_id = 42
        event.event_status = event_status
        event.payload = {
            "data": {
                "id": 42,
                "client": {"id": 7, "display_name": "Test Client", "phone": "+79001234567"},
                "services": [],
                "visit_attendance": visit_attendance,
                "date": "2026-01-15 10:30:00",
                "staff_id": 5,
            }
        }
        return event

    async def _run_handle(self, event: MagicMock) -> bool:
        """Run handle_event with mocked DB and return True if plan_jobs was called."""
        session = AsyncMock()

        with (
            patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=7)),
            patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
            patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker._load_existing_record_and_services", new=AsyncMock(return_value=(None, []))),
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()) as mock_plan,
            patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", new=AsyncMock()),
        ):
            session.get = AsyncMock(return_value=MagicMock(id=99, company_id=123, comment=None))
            await handle_event(session, event)
            return mock_plan.called

    async def test_visit_attendance_minus_one_skipped(self):
        """visit_attendance=-1 (not arrived) must not trigger job creation."""
        event = self._make_event(visit_attendance=-1)
        called = await self._run_handle(event)
        assert not called, "plan_jobs should NOT be called for visit_attendance=-1"

    async def test_visit_attendance_one_skipped(self):
        """visit_attendance=1 (arrived) must not trigger job creation."""
        event = self._make_event(visit_attendance=1)
        called = await self._run_handle(event)
        assert not called, "plan_jobs should NOT be called for visit_attendance=1"

    async def test_visit_attendance_zero_processed(self):
        """visit_attendance=0 (pending) must proceed to job creation."""
        event = self._make_event(visit_attendance=0)
        called = await self._run_handle(event)
        assert called, "plan_jobs SHOULD be called for visit_attendance=0"

    async def test_non_update_status_not_skipped(self):
        """create events must not be skipped regardless of visit_attendance."""
        event = self._make_event(visit_attendance=-1, event_status="create")
        called = await self._run_handle(event)
        assert called, "plan_jobs SHOULD be called for event_status=create"

    async def test_delete_event_passes_last_change_date_as_source_cancelled_at(self):
        event = self._make_event(visit_attendance=0, event_status="delete")
        event.payload["data"]["last_change_date"] = "2026-04-01T14:00:00+0200"
        session = AsyncMock()

        with (
            patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=7)),
            patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
            patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()) as mock_plan,
            patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", new=AsyncMock()),
        ):
            session.get = AsyncMock(return_value=MagicMock(id=99, company_id=123, comment=None))
            await handle_event(session, event)

        mock_plan.assert_awaited_once()
        assert mock_plan.await_args.kwargs["source_cancelled_at"] == datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)

    async def test_update_event_does_not_resolve_booking_created_at(self):
        event = self._make_event(visit_attendance=0, event_status="update")
        session = AsyncMock()
        resolver = AsyncMock(side_effect=AssertionError("timestamp resolver must not run for update"))

        with (
            patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=7)),
            patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
            patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker._load_existing_record_and_services", new=AsyncMock(return_value=(None, []))),
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", resolver),
            patch.object(settings, "promo_apply_discount_enabled", True),
        ):
            session.get = AsyncMock(return_value=MagicMock(id=99, company_id=123, is_deleted=False, comment=None))
            await handle_event(session, event)

        resolver.assert_not_called()


class TestHandleEventPromoBookingCreatedAt:
    def _make_create_event(self, data: dict | None = None) -> MagicMock:
        event = MagicMock()
        event.id = 2
        event.company_id = 758285
        event.resource = "record"
        event.resource_id = 123456789
        event.event_status = "create"
        event.received_at = datetime(2026, 5, 12, 9, 0, tzinfo=timezone.utc)
        payload_data = {
            "id": 123456789,
            "client": {"id": 7, "display_name": "Test Client", "phone": "+491600000099"},
            "services": [{"id": 111, "title": "Allowed"}],
            "date": "2026-05-20 12:00:00",
            "staff_id": 5,
        }
        if data:
            payload_data.update(data)
        event.payload = {"data": payload_data}
        return event

    async def test_create_with_apply_disabled_does_not_create_expensive_resolver(self):
        event = self._make_create_event()
        session = AsyncMock()
        record = MagicMock(id=99, company_id=758285, altegio_record_id=123456789, is_deleted=False)
        try_apply = AsyncMock()
        fetch_mock = AsyncMock(side_effect=AssertionError("GET /record must not be called"))
        resolver = AsyncMock(side_effect=AssertionError("resolver must not be called"))

        with (
            patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=7)),
            patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
            patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
            patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", resolver),
            patch("altegio_bot.workers.inbox_worker.try_apply_promo_discount", try_apply),
            patch.object(settings, "promo_apply_discount_enabled", False),
        ):
            session.get = AsyncMock(return_value=record)
            await handle_event(session, event)

        resolver.assert_not_called()
        fetch_mock.assert_not_called()
        try_apply.assert_awaited_once()
        assert try_apply.await_args.kwargs["booking_created_at_resolver"] is None

    async def test_create_payload_create_date_passes_lazy_resolver_to_try_apply(self):
        event = self._make_create_event({"create_date": "2026-05-10 14:22:00"})
        session = AsyncMock()
        record = MagicMock(id=99, company_id=758285, altegio_record_id=123456789, is_deleted=False)
        try_apply = AsyncMock()
        fetch_mock = AsyncMock(side_effect=AssertionError("GET /record must not be called"))

        with (
            patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=7)),
            patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
            patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
            patch("altegio_bot.workers.inbox_worker.try_apply_promo_discount", try_apply),
            patch.object(settings, "promo_apply_discount_enabled", True),
        ):
            session.get = AsyncMock(return_value=record)
            await handle_event(session, event)

        fetch_mock.assert_not_called()
        try_apply.assert_awaited_once()
        resolver = try_apply.await_args.kwargs["booking_created_at_resolver"]
        assert resolver is not None
        assert await resolver() == datetime(2026, 5, 10, 12, 22, tzinfo=timezone.utc)
        fetch_mock.assert_not_called()

    async def test_create_missing_payload_timestamp_passes_lazy_get_record_resolver(self):
        event = self._make_create_event()
        session = AsyncMock()
        record = MagicMock(id=99, company_id=758285, altegio_record_id=123456789, is_deleted=False)
        try_apply = AsyncMock()
        fetch_mock = AsyncMock(return_value={"id": 123456789, "create_date": "2026-05-10 14:22:00"})

        with (
            patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=7)),
            patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
            patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.fetch_record_details_for_booking_created_at", fetch_mock),
            patch("altegio_bot.workers.inbox_worker.try_apply_promo_discount", try_apply),
            patch.object(settings, "promo_apply_discount_enabled", True),
            patch.object(settings, "promo_location_id_by_company", '{"758285": 758285}'),
        ):
            session.get = AsyncMock(return_value=record)
            await handle_event(session, event)

            fetch_mock.assert_not_called()
            resolver = try_apply.await_args.kwargs["booking_created_at_resolver"]
            assert resolver is not None
            assert await resolver() == datetime(2026, 5, 10, 12, 22, tzinfo=timezone.utc)
            fetch_mock.assert_awaited_once_with(location_id=758285, record_id=123456789)


# =============================================================================
# Tests D, E, F — promo origin comment suppression for record_updated events
# =============================================================================

_PHONE = "+4916099887766"
_COMPANY = 1
_ALLOWED_SERVICE = 12345


def _make_update_event(*, comment: str | None) -> MagicMock:
    """Build a fake record_updated AltegioEvent for suppression tests."""
    event = MagicMock()
    event.id = 1
    event.company_id = _COMPANY
    event.resource = "record"
    event.event_status = "update"
    event.resource_id = None
    event.received_at = datetime(2026, 5, 8, 20, 0, 0, tzinfo=timezone.utc)
    event.payload = {
        "data": {
            "id": 424242,
            "client": {"id": 100, "display_name": "Test", "phone": _PHONE},
            "services": [{"id": _ALLOWED_SERVICE, "title": "Test", "cost_to_pay": 50}],
            "date": "2026-05-08 12:00:00",
            "staff_id": 5,
            "comment": comment,
            "visit_attendance": 0,
        }
    }
    return event


def _make_mock_record(*, comment: str | None) -> MagicMock:
    record = MagicMock()
    record.id = 99
    record.company_id = _COMPANY
    record.is_deleted = False
    record.comment = comment
    return record


# Unit tests for is_promo_origin_comment helper


class TestIsPromoOriginComment:
    def test_none_returns_false(self):
        assert is_promo_origin_comment(None) is False

    def test_empty_string_returns_false(self):
        assert is_promo_origin_comment("") is False

    def test_no_marker_returns_false(self):
        assert is_promo_origin_comment("Normal appointment note") is False

    def test_simple_marker_detected(self):
        assert is_promo_origin_comment("[PromoLead:42]") is True

    def test_manual_marker_detected(self):
        assert is_promo_origin_comment("Some note\n[PromoLead:42:manual]") is True

    def test_marker_mid_text_detected(self):
        assert is_promo_origin_comment("Text before [PromoLead:999] text after") is True

    def test_partial_marker_not_detected(self):
        assert is_promo_origin_comment("[PromoLead:]") is False

    def test_wrong_format_not_detected(self):
        assert is_promo_origin_comment("PromoLead:42") is False


# Integration tests: handle_event suppression on record_updated


@pytest.mark.asyncio
async def test_record_updated_simple_promo_marker_suppresses_plan_jobs() -> None:
    """
    D. record_updated event whose comment contains [PromoLead:<id>] (simple marker)
    must NOT call plan_jobs_for_record_event when suppress helper returns True.
    The suppress helper itself is tested separately (TestShouldSuppressPromoOrigin*).
    """
    event = _make_update_event(comment="Note\n[PromoLead:42]")
    session = AsyncMock()
    mock_record = _make_mock_record(comment="Note\n[PromoLead:42]")
    mock_plan = AsyncMock()
    suppress_mock = AsyncMock(return_value=True)

    with (
        patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=100)),
        patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
        patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
        patch("altegio_bot.workers.inbox_worker._load_existing_record_and_services", new=AsyncMock(return_value=(None, []))),
        patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", mock_plan),
        patch("altegio_bot.workers.inbox_worker.should_suppress_promo_origin_record_update", suppress_mock),
    ):
        session.get = AsyncMock(return_value=mock_record)
        await handle_event(session, event)

    suppress_mock.assert_awaited_once()
    mock_plan.assert_not_called()


@pytest.mark.asyncio
async def test_record_updated_manual_promo_marker_suppresses_plan_jobs() -> None:
    """
    E. record_updated event whose comment contains [PromoLead:<id>:manual] (manual marker)
    must NOT call plan_jobs_for_record_event when suppress helper returns True.
    """
    comment = "Promo welcome_discount: Neukundenrabatt reserviert.\n[PromoLead:55:manual]"
    event = _make_update_event(comment=comment)
    session = AsyncMock()
    mock_record = _make_mock_record(comment=comment)
    mock_plan = AsyncMock()
    suppress_mock = AsyncMock(return_value=True)

    with (
        patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=100)),
        patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
        patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
        patch("altegio_bot.workers.inbox_worker._load_existing_record_and_services", new=AsyncMock(return_value=(None, []))),
        patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", mock_plan),
        patch("altegio_bot.workers.inbox_worker.should_suppress_promo_origin_record_update", suppress_mock),
    ):
        session.get = AsyncMock(return_value=mock_record)
        await handle_event(session, event)

    suppress_mock.assert_awaited_once()
    mock_plan.assert_not_called()


@pytest.mark.asyncio
async def test_record_updated_no_promo_marker_calls_plan_jobs() -> None:
    """
    F. record_updated event with a normal comment (no promo marker) must call
    plan_jobs_for_record_event normally when suppress helper returns False.
    """
    comment = "Normale Buchungsnotiz"
    event = _make_update_event(comment=comment)
    session = AsyncMock()
    mock_record = _make_mock_record(comment=comment)
    mock_plan = AsyncMock()
    suppress_mock = AsyncMock(return_value=False)

    with (
        patch("altegio_bot.workers.inbox_worker.upsert_client", new=AsyncMock(return_value=100)),
        patch("altegio_bot.workers.inbox_worker.upsert_record", new=AsyncMock(return_value=99)),
        patch("altegio_bot.workers.inbox_worker.replace_record_services", new=AsyncMock()),
        patch("altegio_bot.workers.inbox_worker._load_existing_record_and_services", new=AsyncMock(return_value=(None, []))),
        patch(
            "altegio_bot.workers.inbox_worker.record_has_allowed_service",
            new=AsyncMock(return_value=True),
        ),
        patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", mock_plan),
        patch("altegio_bot.workers.inbox_worker.should_suppress_promo_origin_record_update", suppress_mock),
    ):
        session.get = AsyncMock(return_value=mock_record)
        await handle_event(session, event)

    suppress_mock.assert_awaited_once()
    mock_plan.assert_awaited_once()


# =============================================================================
# Part 3 — No-op record update guard
# =============================================================================

# Shared fixtures for no-op tests
_NP_COMPANY = 1
_NP_RECORD_ID = 424242
_NP_CLIENT_ID = 100
_NP_SERVICE_ID = 9001
_NP_STAFF_ID = 5
_NP_STAFF_NAME = "Tanja"
_NP_SHORT_LINK = "https://alteg.io/xyz"
_NP_DATE = "2026-06-10 10:30:00"  # Europe/Belgrade → UTC 08:30


def _make_noop_event(data_overrides: dict | None = None) -> MagicMock:
    event = MagicMock()
    event.id = 77
    event.company_id = _NP_COMPANY
    event.resource = "record"
    event.event_status = "update"
    event.resource_id = _NP_RECORD_ID
    event.received_at = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    data: dict = {
        "id": _NP_RECORD_ID,
        "client": {
            "id": _NP_CLIENT_ID,
            "display_name": "Anna",
            "phone": "+491234567890",
        },
        "staff_id": _NP_STAFF_ID,
        "staff": {"id": _NP_STAFF_ID, "name": _NP_STAFF_NAME},
        "short_link": _NP_SHORT_LINK,
        "date": _NP_DATE,
        "services": [
            {
                "id": _NP_SERVICE_ID,
                "title": "Wimpernverlängerung",
                "amount": 1,
                "cost_to_pay": 80,
            }
        ],
        "visit_attendance": 0,
    }
    if data_overrides:
        data.update(data_overrides)
    event.payload = {"data": data}
    return event


def _make_existing_record() -> MagicMock:
    from zoneinfo import ZoneInfo

    TZ = ZoneInfo("Europe/Belgrade")
    from datetime import datetime as _dt

    naive = _dt(2026, 6, 10, 10, 30, 0)
    starts_at_utc = naive.replace(tzinfo=TZ).astimezone(timezone.utc)

    rec = MagicMock()
    rec.id = 999
    rec.company_id = _NP_COMPANY
    rec.altegio_record_id = _NP_RECORD_ID
    rec.starts_at = starts_at_utc
    rec.staff_id = _NP_STAFF_ID
    rec.staff_name = _NP_STAFF_NAME
    rec.short_link = _NP_SHORT_LINK
    rec.total_cost = None  # sum_total_cost will compute from services
    rec.is_deleted = False
    return rec


def _make_existing_service() -> MagicMock:
    from decimal import Decimal

    svc = MagicMock()
    svc.service_id = _NP_SERVICE_ID
    svc.title = "Wimpernverlängerung"
    svc.amount = 1
    svc.cost_to_pay = Decimal("80")
    return svc


async def _noop_handle(
    event: MagicMock,
    existing_rec: MagicMock,
    existing_svcs: list,
) -> tuple[bool, bool]:
    """Run handle_event; return (plan_called, noop_triggered).

    noop_triggered is True when plan_jobs was NOT called due to no-op guard
    (different from service-filter skip or promo suppression).
    We infer noop_triggered = not plan_called when other skips are absent.
    """
    mock_record_obj = MagicMock()
    mock_record_obj.id = 999
    mock_record_obj.company_id = _NP_COMPANY
    mock_record_obj.is_deleted = False
    mock_record_obj.comment = None

    mock_plan = AsyncMock()
    mock_load = AsyncMock(return_value=(existing_rec, existing_svcs))

    session = AsyncMock()
    session.get = AsyncMock(return_value=mock_record_obj)
    with (
        patch(
            "altegio_bot.workers.inbox_worker.upsert_client",
            new=AsyncMock(return_value=_NP_CLIENT_ID),
        ),
        patch(
            "altegio_bot.workers.inbox_worker.upsert_record",
            new=AsyncMock(return_value=999),
        ),
        patch(
            "altegio_bot.workers.inbox_worker.replace_record_services",
            new=AsyncMock(),
        ),
        patch(
            "altegio_bot.workers.inbox_worker._load_existing_record_and_services",
            mock_load,
        ),
        patch(
            "altegio_bot.workers.inbox_worker.record_has_allowed_service",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "altegio_bot.workers.inbox_worker.should_suppress_promo_origin_record_update",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "altegio_bot.workers.inbox_worker.plan_jobs_for_record_event",
            mock_plan,
        ),
    ):
        await handle_event(session, event)

    return mock_plan.called, not mock_plan.called


@pytest.mark.asyncio
async def test_noop_update_same_snapshot_skips_plan_jobs() -> None:
    """No-op update: identical visible snapshot → plan_jobs NOT called."""
    event = _make_noop_event()
    existing_rec = _make_existing_record()
    # total_cost must match sum_total_cost(services)
    from decimal import Decimal

    existing_rec.total_cost = Decimal("80")
    existing_svcs = [_make_existing_service()]

    plan_called, _ = await _noop_handle(event, existing_rec, existing_svcs)
    assert not plan_called, 'plan_jobs must NOT be called for no-op update'


@pytest.mark.asyncio
async def test_noop_update_date_change_calls_plan_jobs() -> None:
    """Real starts_at change → plan_jobs IS called."""
    # Change the date to a different time
    event = _make_noop_event({"date": "2026-06-11 10:30:00"})  # 1 day later
    existing_rec = _make_existing_record()
    from decimal import Decimal

    existing_rec.total_cost = Decimal("80")
    existing_svcs = [_make_existing_service()]

    plan_called, _ = await _noop_handle(event, existing_rec, existing_svcs)
    assert plan_called, 'plan_jobs must be called when starts_at changes'


@pytest.mark.asyncio
async def test_noop_update_staff_change_calls_plan_jobs() -> None:
    """Real staff change → plan_jobs IS called."""
    event = _make_noop_event({
        "staff_id": 99,
        "staff": {"id": 99, "name": "New Staff"},
    })
    existing_rec = _make_existing_record()
    from decimal import Decimal

    existing_rec.total_cost = Decimal("80")
    existing_svcs = [_make_existing_service()]

    plan_called, _ = await _noop_handle(event, existing_rec, existing_svcs)
    assert plan_called, 'plan_jobs must be called when staff changes'


@pytest.mark.asyncio
async def test_noop_update_services_change_calls_plan_jobs() -> None:
    """Real service change → plan_jobs IS called."""
    event = _make_noop_event({
        "services": [
            {
                "id": 9002,  # different service_id
                "title": "Anderer Service",
                "amount": 1,
                "cost_to_pay": 80,
            }
        ]
    })
    existing_rec = _make_existing_record()
    from decimal import Decimal

    existing_rec.total_cost = Decimal("80")
    existing_svcs = [_make_existing_service()]

    plan_called, _ = await _noop_handle(event, existing_rec, existing_svcs)
    assert plan_called, 'plan_jobs must be called when services change'


@pytest.mark.asyncio
async def test_noop_update_total_cost_change_calls_plan_jobs() -> None:
    """Real cost change → plan_jobs IS called."""
    event = _make_noop_event({
        "services": [
            {
                "id": _NP_SERVICE_ID,
                "title": "Wimpernverlängerung",
                "amount": 1,
                "cost_to_pay": 100,  # changed from 80 to 100
            }
        ]
    })
    existing_rec = _make_existing_record()
    from decimal import Decimal

    existing_rec.total_cost = Decimal("80")
    existing_svcs = [_make_existing_service()]

    plan_called, _ = await _noop_handle(event, existing_rec, existing_svcs)
    assert plan_called, 'plan_jobs must be called when cost changes'


@pytest.mark.asyncio
async def test_noop_update_missing_existing_record_processes_normally() -> None:
    """No existing record on update → process normally, not a no-op."""
    event = _make_noop_event()
    # existing_rec=None means record was not in DB before this event
    plan_called, _ = await _noop_handle(event, None, [])
    assert plan_called, 'must process normally when no existing record'


@pytest.mark.asyncio
async def test_visit_attendance_update_still_skipped() -> None:
    """visit_attendance-only skip is preserved regardless of no-op guard."""
    event = _make_noop_event({"visit_attendance": 1})
    existing_rec = _make_existing_record()
    from decimal import Decimal

    existing_rec.total_cost = Decimal("80")
    existing_svcs = [_make_existing_service()]

    plan_called, _ = await _noop_handle(event, existing_rec, existing_svcs)
    assert not plan_called, 'visit_attendance skip must remain'


# ---------------------------------------------------------------------------
# Unit tests for _is_noop_update helper
# ---------------------------------------------------------------------------


class TestIsNoopUpdate:
    """Unit tests for _is_noop_update helper."""

    def _make_rec(self, **overrides: object) -> MagicMock:
        from decimal import Decimal
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt

        TZ = ZoneInfo("Europe/Belgrade")
        naive = _dt(2026, 6, 10, 10, 30, 0)
        starts_at_utc = naive.replace(tzinfo=TZ).astimezone(timezone.utc)

        rec = MagicMock()
        rec.starts_at = starts_at_utc
        rec.staff_id = _NP_STAFF_ID
        rec.staff_name = _NP_STAFF_NAME
        rec.short_link = _NP_SHORT_LINK
        rec.total_cost = Decimal("80")
        for k, v in overrides.items():
            setattr(rec, k, v)
        return rec

    def _make_svc(self, **overrides: object) -> MagicMock:
        from decimal import Decimal

        svc = MagicMock()
        svc.service_id = _NP_SERVICE_ID
        svc.title = "Wimpernverlängerung"
        svc.amount = 1
        svc.cost_to_pay = Decimal("80")
        for k, v in overrides.items():
            setattr(svc, k, v)
        return svc

    def _base_data(self) -> dict:
        return {
            "id": _NP_RECORD_ID,
            "staff_id": _NP_STAFF_ID,
            "staff": {"id": _NP_STAFF_ID, "name": _NP_STAFF_NAME},
            "short_link": _NP_SHORT_LINK,
            "date": _NP_DATE,
            "services": [
                {
                    "id": _NP_SERVICE_ID,
                    "title": "Wimpernverlängerung",
                    "amount": 1,
                    "cost_to_pay": 80,
                }
            ],
        }

    def test_identical_snapshot_is_noop(self) -> None:
        assert _is_noop_update(self._make_rec(), [self._make_svc()], self._base_data())

    def test_services_absent_not_noop(self) -> None:
        data = self._base_data()
        del data["services"]
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_staff_id_change_not_noop(self) -> None:
        data = self._base_data()
        data["staff_id"] = 999
        data["staff"] = {"id": 999, "name": _NP_STAFF_NAME}
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_staff_name_change_not_noop(self) -> None:
        data = self._base_data()
        data["staff"] = {"id": _NP_STAFF_ID, "name": "Different"}
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_short_link_change_not_noop(self) -> None:
        data = self._base_data()
        data["short_link"] = "https://other.link"
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_starts_at_change_not_noop(self) -> None:
        data = self._base_data()
        data["date"] = "2026-06-11 10:30:00"
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_service_cost_change_not_noop(self) -> None:
        data = self._base_data()
        data["services"][0]["cost_to_pay"] = 100
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_service_added_not_noop(self) -> None:
        data = self._base_data()
        data["services"].append(
            {"id": 9999, "title": "Extra", "amount": 1, "cost_to_pay": 20}
        )
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_service_removed_not_noop(self) -> None:
        data = self._base_data()
        data["services"] = []
        assert not _is_noop_update(self._make_rec(), [self._make_svc()], data)

    def test_empty_existing_services_same_incoming_empty_is_noop(self) -> None:
        from decimal import Decimal

        rec = self._make_rec(total_cost=None)
        data = self._base_data()
        data["services"] = []
        assert _is_noop_update(rec, [], data)
