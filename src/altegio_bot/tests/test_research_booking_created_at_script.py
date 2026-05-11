from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from altegio_bot.altegio_records import AltegioRecordResearchError
from altegio_bot.scripts import research_booking_created_at as script
from altegio_bot.settings import settings


def test_missing_required_args_exits_non_zero() -> None:
    with pytest.raises(SystemExit) as exc:
        script.main([])

    assert exc.value.code != 0


def test_no_created_fields_reports_none_and_not_safe(capsys: pytest.CaptureFixture[str]) -> None:
    fetch_mock = AsyncMock(return_value={})

    with patch.object(script, "fetch_record_details_for_booking_created_at_research", fetch_mock):
        code = script.main(["--location-id", "9001", "--record-id", "123456789"])

    out = capsys.readouterr().out
    assert code == 0
    assert "Booking created-at research" in out
    assert "location_id=9001" in out
    assert "record_id=123456789" in out
    assert "created_at: <missing>" in out
    assert "create_date: <missing>" in out
    assert "datetime_created: <missing>" in out
    assert "confirmed_booking_created_at=<none>" in out
    assert "safe_for_auto_apply=false" in out
    fetch_mock.assert_awaited_once_with(location_id=9001, record_id=123456789, timeout_sec=15.0)


def test_appointment_and_last_change_fields_are_not_reliable(capsys: pytest.CaptureFixture[str]) -> None:
    fetch_mock = AsyncMock(
        return_value={
            "date": "2026-05-20 12:00:00",
            "datetime": "2026-05-20T12:00:00+02:00",
            "last_change_date": "2026-05-10 14:22:00",
        }
    )

    with patch.object(script, "fetch_record_details_for_booking_created_at_research", fetch_mock):
        code = script.main(["--location-id", "9001", "--record-id", "123456789"])

    out = capsys.readouterr().out
    assert code == 0
    assert "date: 2026-05-20 12:00:00  # appointment start, NOT booking created_at" in out
    assert "datetime: 2026-05-20T12:00:00+02:00  # appointment start, NOT booking created_at" in out
    assert "last_change_date: 2026-05-10 14:22:00  # last change, NOT reliable created_at" in out
    assert "confirmed_booking_created_at=<none>" in out
    assert "safe_for_auto_apply=false" in out


def test_created_at_field_is_reported_and_trusted(capsys: pytest.CaptureFixture[str]) -> None:
    fetch_mock = AsyncMock(return_value={"created_at": "2026-05-10T14:22:00+02:00"})

    with patch.object(script, "fetch_record_details_for_booking_created_at_research", fetch_mock):
        code = script.main(["--location-id", "9001", "--record-id", "123456789"])

    out = capsys.readouterr().out
    assert code == 0
    assert "created_at: 2026-05-10T14:22:00+02:00  # trusted booking creation timestamp" in out
    assert "confirmed_booking_created_at=2026-05-10T12:22:00+00:00" in out
    assert "safe_for_auto_apply=true" in out


def test_api_error_returns_one_and_redacts_tokens(capsys: pytest.CaptureFixture[str]) -> None:
    with (
        patch.object(settings, "altegio_partner_token", "partner-secret"),
        patch.object(settings, "altegio_user_token", "user-secret"),
    ):
        fetch_mock = AsyncMock(side_effect=AltegioRecordResearchError("failed with partner-secret and user-secret"))

        with patch.object(script, "fetch_record_details_for_booking_created_at_research", fetch_mock):
            code = script.main(["--location-id", "9001", "--record-id", "123456789"])

    out = capsys.readouterr().out
    assert code == 1
    assert "ERROR:" in out
    assert "partner-secret" not in out
    assert "user-secret" not in out
    assert "[redacted]" in out


def test_passes_location_id_record_id_and_timeout_to_wrapper() -> None:
    fetch_mock = AsyncMock(return_value={})

    with patch.object(script, "fetch_record_details_for_booking_created_at_research", fetch_mock):
        code = script.main(
            [
                "--location-id",
                "9001",
                "--record-id",
                "123456789",
                "--timeout-sec",
                "3.5",
            ]
        )

    assert code == 0
    fetch_mock.assert_awaited_once_with(location_id=9001, record_id=123456789, timeout_sec=3.5)
