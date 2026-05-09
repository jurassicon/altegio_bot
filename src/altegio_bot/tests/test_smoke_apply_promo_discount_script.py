"""Tests: smoke_apply_promo_discount CLI script.

Covers:
1. dry-run: --yes-apply not passed → wrapper not called, exit 0, output contains mode=dry-run.
2. apply mode success: --yes-apply + api_verified=True + mocked wrapper → exit 0, output shows success.
3. apply mode API not verified: --yes-apply + api_verified=False → exit 1, wrapper not called.
4. apply mode wrapper error: wrapper raises PromoDiscountApplyError → exit 1, error in output.
5. missing required args: argparse exits non-zero.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from altegio_bot.promo_discount_apply import PromoDiscountApplyError, PromoDiscountApplyResult
from altegio_bot.scripts.smoke_apply_promo_discount import _build_parser, _run
from altegio_bot.settings import settings

_ARGS = ["--location-id", "123", "--card-id", "456", "--program-id", "789", "--record-id", "111"]
_PATCH = "altegio_bot.scripts.smoke_apply_promo_discount.apply_promo_discount_to_visit"


def _parse(*extra: str) -> object:
    return _build_parser().parse_args([*_ARGS, *extra])


# ---------------------------------------------------------------------------
# 1. Dry-run: no --yes-apply → wrapper not called, exit 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_no_api_call(capsys) -> None:
    mock_wrapper = AsyncMock(side_effect=RuntimeError("must not be called"))
    with patch(_PATCH, mock_wrapper):
        exit_code = await _run(_parse())
    mock_wrapper.assert_not_called()
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "mode=dry-run" in out
    assert "No API call was made" in out


# ---------------------------------------------------------------------------
# 2. Apply success: api_verified=True, mocked wrapper → exit 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_mode_success(capsys) -> None:
    mock_wrapper = AsyncMock(return_value=PromoDiscountApplyResult(applied=True, raw={"success": True}))
    with patch.object(settings, "promo_apply_discount_api_verified", True):
        with patch(_PATCH, mock_wrapper):
            exit_code = await _run(_parse("--yes-apply"))
    assert exit_code == 0
    mock_wrapper.assert_called_once_with(
        location_id=123,
        card_id=456,
        program_id="789",
        record_id="111",
    )
    out = capsys.readouterr().out
    assert "mode=apply" in out
    assert "success=true" in out


# ---------------------------------------------------------------------------
# 3. Apply mode: api_verified=False → exit 1, wrapper not called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_mode_api_not_verified(capsys) -> None:
    mock_wrapper = AsyncMock(side_effect=RuntimeError("must not be called"))
    with patch.object(settings, "promo_apply_discount_api_verified", False):
        with patch(_PATCH, mock_wrapper):
            exit_code = await _run(_parse("--yes-apply"))
    mock_wrapper.assert_not_called()
    assert exit_code == 1
    out = capsys.readouterr().out
    assert "PROMO_APPLY_DISCOUNT_API_VERIFIED" in out


# ---------------------------------------------------------------------------
# 4. Apply mode: wrapper raises PromoDiscountApplyError → exit 1
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_mode_wrapper_error(capsys) -> None:
    mock_wrapper = AsyncMock(side_effect=PromoDiscountApplyError("Altegio 503 server error"))
    with patch.object(settings, "promo_apply_discount_api_verified", True):
        with patch(_PATCH, mock_wrapper):
            exit_code = await _run(_parse("--yes-apply"))
    assert exit_code == 1
    out = capsys.readouterr().out
    assert "ERROR" in out
    assert "Altegio 503" in out


# ---------------------------------------------------------------------------
# 5. Missing required args: argparse exits non-zero
# ---------------------------------------------------------------------------


def test_missing_required_args_exits_nonzero() -> None:
    with pytest.raises(SystemExit) as exc_info:
        _build_parser().parse_args(["--location-id", "123"])
    assert exc_info.value.code != 0
