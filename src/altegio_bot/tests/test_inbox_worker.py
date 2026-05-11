from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from altegio_bot.altegio_records import AltegioRecordResearchError
from altegio_bot.settings import settings
from altegio_bot.workers.inbox_worker import (
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
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()) as mock_plan,
            patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", new=AsyncMock()),
        ):
            session.get = AsyncMock(return_value=MagicMock(id=99, company_id=123))
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
            session.get = AsyncMock(return_value=MagicMock(id=99, company_id=123))
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
            patch("altegio_bot.workers.inbox_worker.record_has_allowed_service", new=AsyncMock(return_value=True)),
            patch("altegio_bot.workers.inbox_worker.plan_jobs_for_record_event", new=AsyncMock()),
            patch("altegio_bot.workers.inbox_worker.resolve_booking_created_at_for_record_create", resolver),
            patch.object(settings, "promo_apply_discount_enabled", True),
        ):
            session.get = AsyncMock(return_value=MagicMock(id=99, company_id=123, is_deleted=False))
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

    async def test_create_payload_create_date_passed_to_try_apply(self):
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
        assert try_apply.await_args.kwargs["booking_created_at"] == datetime(2026, 5, 10, 12, 22, tzinfo=timezone.utc)

    async def test_create_missing_payload_timestamp_uses_get_record_create_date(self):
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

        fetch_mock.assert_awaited_once_with(location_id=758285, record_id=123456789)
        assert try_apply.await_args.kwargs["booking_created_at"] == datetime(2026, 5, 10, 12, 22, tzinfo=timezone.utc)
