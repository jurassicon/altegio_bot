"""Юнит-тесты функции _is_eligible_for_followup (без обращения к БД).

Проверяем все ветки логики:
  - unread_only: отправлять только тем, кто не прочитал
  - unread_or_not_booked: тем, кто не прочитал ИЛИ не записался
  - cleanup_failed: всегда исключать
  - skipped: всегда исключать
"""

from __future__ import annotations

from datetime import datetime, timezone

from altegio_bot.campaigns.followup import _is_eligible_for_followup
from altegio_bot.models.models import CampaignRecipient

_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _recipient(**kw) -> CampaignRecipient:
    """Создать CampaignRecipient с дефолтными полями."""
    defaults = dict(
        campaign_run_id=1,
        company_id=758285,
        status="queued",
        excluded_reason=None,
        read_at=None,
        booked_after_at=None,
        followup_status=None,
    )
    defaults.update(kw)
    return CampaignRecipient(**defaults)


# ---------------------------------------------------------------------------
# Политика unread_only
# ---------------------------------------------------------------------------


def test_unread_only_eligible_when_no_read_at() -> None:
    """При unread_only клиент без read_at должен получить follow-up."""
    r = _recipient(status="queued", read_at=None)
    assert _is_eligible_for_followup(r, "unread_only") is True


def test_unread_only_excluded_when_read() -> None:
    """При unread_only клиент, уже прочитавший сообщение, исключается."""
    r = _recipient(status="read", read_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_only") is False


# ---------------------------------------------------------------------------
# Политика unread_or_not_booked
# ---------------------------------------------------------------------------


def test_unread_or_not_booked_eligible_unread() -> None:
    """unread_or_not_booked: не прочитал — eligible (даже если записан)."""
    r = _recipient(status="queued", read_at=None, booked_after_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is True


def test_unread_or_not_booked_eligible_not_booked() -> None:
    """unread_or_not_booked: прочитал, но не записался — eligible."""
    r = _recipient(status="read", read_at=_NOW, booked_after_at=None)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is True


def test_unread_or_not_booked_excluded_when_read_and_booked() -> None:
    """unread_or_not_booked: прочитал И записался — исключить."""
    r = _recipient(status="booked_after_campaign", read_at=_NOW, booked_after_at=_NOW)
    assert _is_eligible_for_followup(r, "unread_or_not_booked") is False


# ---------------------------------------------------------------------------
# Hard-failure статусы и причины
# ---------------------------------------------------------------------------


def test_cleanup_failed_status_excluded() -> None:
    """Клиент со статусом cleanup_failed не должен получать follow-up."""
    r = _recipient(status="cleanup_failed")
    assert _is_eligible_for_followup(r, "unread_only") is False


def test_skipped_status_excluded() -> None:
    """Клиент, не прошедший сегментацию (skipped), не должен получать follow-up."""
    r = _recipient(status="skipped", excluded_reason="opted_out")
    assert _is_eligible_for_followup(r, "unread_only") is False


def test_hard_failure_reason_excluded() -> None:
    """Клиент с excluded_reason='no_whatsapp' не должен получать follow-up."""
    r = _recipient(status="queued", excluded_reason="no_whatsapp")
    assert _is_eligible_for_followup(r, "unread_only") is False
