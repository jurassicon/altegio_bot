"""Юнит-тесты классификатора follow-up кандидатов (без обращения к БД).

Проверяем classify_followup_candidate() и тонкую обёртку
_is_eligible_for_followup() для всех веток логики:
  - unread_only / unread_or_not_booked: планируем только реальных кандидатов
    (в pipeline доставки, не прочитал/не ответил/не записался);
  - read / replied / booked_after: НЕ планируем (финальный guard их отсечёт);
  - hard_failure / skipped / excluded / not_sent_pipeline: НЕ планируем;
  - unknown_policy: НЕ планируем.
"""

from __future__ import annotations

from datetime import datetime, timezone

from altegio_bot.campaigns.followup import (
    _is_eligible_for_followup,
    classify_followup_candidate,
)
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
        replied_at=None,
        booked_after_at=None,
        followup_status=None,
    )
    defaults.update(kw)
    return CampaignRecipient(**defaults)


# ---------------------------------------------------------------------------
# Политика unread_only
# ---------------------------------------------------------------------------


def test_unread_only_eligible_when_no_read_at() -> None:
    """При unread_only доставленный клиент без read_at должен получить follow-up."""
    r = _recipient(status="delivered", read_at=None)
    assert _is_eligible_for_followup(r, "unread_only") is True


def test_unread_only_excluded_when_read() -> None:
    """При unread_only клиент, уже прочитавший сообщение, исключается."""
    r = _recipient(status="read", read_at=_NOW)
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "read"


def test_unread_only_excluded_when_booked() -> None:
    """unread_only тоже не планирует записавшихся (финальный guard их пропустит)."""
    r = _recipient(status="queued", read_at=None, booked_after_at=_NOW)
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "booked_after"


# ---------------------------------------------------------------------------
# Политика unread_or_not_booked
# ---------------------------------------------------------------------------


def test_unread_or_not_booked_eligible_unread_not_booked() -> None:
    """unread_or_not_booked: не прочитал и не записался — eligible."""
    r = _recipient(status="delivered", read_at=None, booked_after_at=None)
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is True
    assert result.reason == "eligible"
    assert result.followup_status is None


def test_unread_or_not_booked_unread_but_booked_not_planned() -> None:
    """unread_or_not_booked: не прочитал, но УЖЕ записался — НЕ планировать.

    Регрессия: раньше `not is_read or booked_after is None` планировал таких
    получателей, хотя финальный guard их отсекает (booked_after).
    """
    r = _recipient(status="queued", read_at=None, booked_after_at=_NOW)
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "booked_after"
    assert result.followup_status == "skipped_booked_after"


def test_unread_or_not_booked_read_but_not_booked_not_planned() -> None:
    """unread_or_not_booked: прочитал, но не записался — НЕ планировать.

    Это основное исправление: UI и финальный guard считают read-получателей
    неподходящими, значит plan_followup тоже не должен их планировать.
    """
    r = _recipient(status="read", read_at=_NOW, booked_after_at=None)
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "read"
    assert result.followup_status == "skipped_read"


def test_unread_or_not_booked_excluded_when_read_and_booked() -> None:
    """unread_or_not_booked: прочитал И записался — исключить (booked приоритетнее)."""
    r = _recipient(status="booked_after_campaign", read_at=_NOW, booked_after_at=_NOW)
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "booked_after"


# ---------------------------------------------------------------------------
# replied получатели
# ---------------------------------------------------------------------------


def test_replied_recipient_not_planned() -> None:
    """Ответивший получатель не планируется (reason=replied)."""
    r = _recipient(status="delivered", replied_at=_NOW)
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "replied"
    assert result.followup_status == "skipped_replied"


def test_replied_status_not_planned() -> None:
    """Статус replied тоже отсекается."""
    r = _recipient(status="replied")
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "replied"


# ---------------------------------------------------------------------------
# Original delivery required → only status="delivered" is eligible
# ---------------------------------------------------------------------------


def test_delivered_unread_eligible() -> None:
    r = _recipient(status="delivered")
    assert classify_followup_candidate(r, "unread_or_not_booked").eligible is True


def test_provider_accepted_not_delivered_not_eligible() -> None:
    """provider_accepted does not prove original delivery → not eligible."""
    r = _recipient(status="provider_accepted")
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "not_delivered"
    assert result.followup_status == "skipped_not_delivered"


def test_queued_not_delivered_not_eligible() -> None:
    """queued does not prove original delivery → not eligible."""
    r = _recipient(status="queued")
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "not_delivered"
    assert result.followup_status == "skipped_not_delivered"


# ---------------------------------------------------------------------------
# Hard-failure статусы и причины, skipped, excluded, not-in-pipeline
# ---------------------------------------------------------------------------


def test_cleanup_failed_status_excluded() -> None:
    """Клиент со статусом cleanup_failed не должен получать follow-up."""
    r = _recipient(status="cleanup_failed")
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "hard_failure"


def test_skipped_status_excluded() -> None:
    """Клиент, не прошедший сегментацию (skipped), не должен получать follow-up."""
    r = _recipient(status="skipped", excluded_reason="opted_out")
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "skipped"


def test_hard_failure_reason_excluded() -> None:
    """Клиент с excluded_reason='no_whatsapp' не должен получать follow-up."""
    r = _recipient(status="queued", excluded_reason="no_whatsapp")
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "hard_failure"
    # excluded_reason уже фиксирует причину — followup_status не выставляем.
    assert result.followup_status is None


def test_soft_excluded_reason_excluded() -> None:
    """Несработавшая (не hard-failure) excluded_reason → reason=excluded."""
    r = _recipient(status="card_issued", excluded_reason="returned_after_first_visit")
    result = classify_followup_candidate(r, "unread_only")
    assert result.eligible is False
    assert result.reason == "excluded"


def test_not_in_sent_pipeline_excluded() -> None:
    """Статус вне sent pipeline (например, candidate) → not_sent_pipeline."""
    r = _recipient(status="candidate")
    result = classify_followup_candidate(r, "unread_or_not_booked")
    assert result.eligible is False
    assert result.reason == "not_sent_pipeline"


def test_unknown_policy_not_eligible() -> None:
    """Неизвестная политика → reason=unknown_policy, не eligible."""
    r = _recipient(status="delivered")
    result = classify_followup_candidate(r, "some_unknown_policy")
    assert result.eligible is False
    assert result.reason == "unknown_policy"
