from __future__ import annotations

from altegio_bot.workers.inbox_worker import parse_dt


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
