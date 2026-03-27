from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from altegio_bot.workers.inbox_worker import _normalize_phone, _parse_starts_at, parse_dt


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
