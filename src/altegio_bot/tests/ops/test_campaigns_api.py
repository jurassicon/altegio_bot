"""Unit tests for helpers in ops/campaigns_api.py.

These tests are pure Python — no database or async required.
"""

from __future__ import annotations

from altegio_bot.models.models import CampaignRecipient
from altegio_bot.ops.campaigns_api import _followup_reason


def _r(**kw) -> CampaignRecipient:
    """Minimal CampaignRecipient for unit tests (no DB session needed)."""
    defaults = dict(campaign_run_id=1, company_id=1, status="delivered")
    defaults.update(kw)
    return CampaignRecipient(**defaults)


# ---------------------------------------------------------------------------
# _followup_reason — timestamp-based (existing behaviour)
# ---------------------------------------------------------------------------


def test_followup_reason_booked_after_timestamp() -> None:
    from datetime import datetime, timezone

    r = _r(booked_after_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert _followup_reason(r) == "booked_after"


def test_followup_reason_replied_timestamp() -> None:
    from datetime import datetime, timezone

    r = _r(replied_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert _followup_reason(r) == "replied"


def test_followup_reason_read_timestamp() -> None:
    from datetime import datetime, timezone

    r = _r(read_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert _followup_reason(r) == "read"


def test_followup_reason_queued() -> None:
    r = _r(followup_status="followup_queued")
    assert _followup_reason(r) == "queued"


def test_followup_reason_eligible_returns_none() -> None:
    r = _r(status="delivered")
    assert _followup_reason(r) is None


def test_followup_reason_not_eligible_skipped() -> None:
    r = _r(status="skipped", excluded_reason="opted_out")
    assert _followup_reason(r) == "not eligible"


# ---------------------------------------------------------------------------
# _followup_reason — status-based (новое поведение: статус без timestamp)
# ---------------------------------------------------------------------------


def test_followup_reason_status_read_no_timestamp() -> None:
    """status='read' без read_at → 'read' (зеркалирует followup.py)."""
    r = _r(status="read", read_at=None)
    assert _followup_reason(r) == "read"


def test_followup_reason_status_booked_after_campaign_no_timestamp() -> None:
    """status='booked_after_campaign' без booked_after_at → 'booked_after'."""
    r = _r(status="booked_after_campaign", booked_after_at=None)
    assert _followup_reason(r) == "booked_after"


def test_followup_reason_status_replied_no_timestamp() -> None:
    """status='replied' без replied_at → 'replied'."""
    r = _r(status="replied", replied_at=None)
    assert _followup_reason(r) == "replied"


# ---------------------------------------------------------------------------
# _followup_reason — приоритет (booked_after > replied > read)
# ---------------------------------------------------------------------------


def test_followup_reason_priority_booked_over_read() -> None:
    """Когда есть и booked_after_at, и read_at — возвращает 'booked_after'."""
    from datetime import datetime, timezone

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    r = _r(booked_after_at=ts, read_at=ts)
    assert _followup_reason(r) == "booked_after"


def test_followup_reason_priority_replied_over_read() -> None:
    """Когда есть и replied_at, и read_at — возвращает 'replied'."""
    from datetime import datetime, timezone

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    r = _r(replied_at=ts, read_at=ts)
    assert _followup_reason(r) == "replied"
