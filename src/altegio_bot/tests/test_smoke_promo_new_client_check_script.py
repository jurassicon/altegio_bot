"""Tests: smoke_promo_new_client_check CLI script.

Covers:
1. No records: mocked wrapper returns False → has_any_altegio_record=false, exit 0.
2. Records found: mocked wrapper returns True → has_any_altegio_record=true, exit 0.
3. Wrapper error: raises AltegioNewClientCheckError → ERROR in output, exit 1.
4. No token leakage on wrapper error.
5. Script passes location_id and phone correctly to wrapper.
6a. Missing --location-id → argparse exits non-zero.
6b. Missing --phone → argparse exits non-zero.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import altegio_bot.altegio_records as records_mod
from altegio_bot.altegio_records import AltegioNewClientCheckError
from altegio_bot.scripts.smoke_promo_new_client_check import _build_parser, _run

_PATCH = "altegio_bot.scripts.smoke_promo_new_client_check.check_client_has_any_altegio_record"
_ARGS = ["--location-id", "9001", "--phone", "+491234567890"]


def _parse(*extra: str) -> object:
    return _build_parser().parse_args([*_ARGS, *extra])


# ---------------------------------------------------------------------------
# 1. No records → has_any_altegio_record=false, exit 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_records_prints_false_exit_0(capsys) -> None:
    mock_wrapper = AsyncMock(return_value=False)
    with patch(_PATCH, mock_wrapper):
        exit_code = await _run(_parse())
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "has_any_altegio_record=false" in out
    assert "location_id=9001" in out
    assert "phone=+491234567890" in out


# ---------------------------------------------------------------------------
# 2. Records found → has_any_altegio_record=true, exit 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_records_found_prints_true_exit_0(capsys) -> None:
    mock_wrapper = AsyncMock(return_value=True)
    with patch(_PATCH, mock_wrapper):
        exit_code = await _run(_parse())
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "has_any_altegio_record=true" in out


# ---------------------------------------------------------------------------
# 3. Wrapper error → ERROR in output, exit 1
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wrapper_error_prints_error_exit_1(capsys) -> None:
    mock_wrapper = AsyncMock(
        side_effect=AltegioNewClientCheckError("promo new-client check HTTP 503: location_id=9001")
    )
    with patch(_PATCH, mock_wrapper):
        exit_code = await _run(_parse())
    assert exit_code == 1
    out = capsys.readouterr().out
    assert "ERROR" in out
    assert "503" in out


# ---------------------------------------------------------------------------
# 4. No token leakage on wrapper error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_token_leakage_on_error(capsys, monkeypatch) -> None:
    monkeypatch.setattr(records_mod.settings, "altegio_partner_token", "secret-partner-token")
    monkeypatch.setattr(records_mod.settings, "altegio_user_token", "secret-user-token")
    mock_wrapper = AsyncMock(side_effect=AltegioNewClientCheckError("HTTP 500: location_id=9001"))
    with patch(_PATCH, mock_wrapper):
        exit_code = await _run(_parse())
    assert exit_code == 1
    out = capsys.readouterr().out
    assert "secret-partner-token" not in out
    assert "secret-user-token" not in out


# ---------------------------------------------------------------------------
# 5. Script passes location_id and phone correctly to wrapper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wrapper_called_with_correct_args(capsys) -> None:
    mock_wrapper = AsyncMock(return_value=False)
    with patch(_PATCH, mock_wrapper):
        await _run(_parse())
    mock_wrapper.assert_awaited_once_with(
        phone_e164="+491234567890",
        location_id=9001,
    )


# ---------------------------------------------------------------------------
# 6. Missing required args → argparse exits non-zero
# ---------------------------------------------------------------------------


def test_missing_location_id_exits_nonzero() -> None:
    with pytest.raises(SystemExit) as exc_info:
        _build_parser().parse_args(["--phone", "+491234567890"])
    assert exc_info.value.code != 0


def test_missing_phone_exits_nonzero() -> None:
    with pytest.raises(SystemExit) as exc_info:
        _build_parser().parse_args(["--location-id", "9001"])
    assert exc_info.value.code != 0
