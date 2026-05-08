"""Tests: cleanup of expired promo loyalty cards.

Covers:
1.  Expired issued PromoLead with card → delete_card called, status=expired, meta set.
2.  Not-expired PromoLead → delete_card not called, status unchanged.
3.  Already deleted (meta.promo_card_deleted_at set) → skip, delete_card not called.
4.  delete_card failure → status unchanged, meta.promo_card_delete_result='failed', retryable.
5.  status='used' PromoLead → skipped, delete_card not called.
6.  status='cancelled' PromoLead → skipped, delete_card not called.
7.  PromoLead without loyalty_card_id → not fetched, delete_card not called.
8.  PromoLead without location_id → not fetched, delete_card not called.
9.  meta.loyalty_card_issued is not True → skipped, delete_card not called.
10. limit parameter caps the number of processed rows.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from altegio_bot.models.models import PromoLead
from altegio_bot.promo_loyalty_cleanup import cleanup_expired_promo_loyalty_cards

_UTC = timezone.utc
_EXPIRED = datetime(2025, 1, 1, tzinfo=_UTC)
_FUTURE = datetime(2099, 1, 1, tzinfo=_UTC)
_NOW = datetime(2026, 5, 1, tzinfo=_UTC)

_PHONE = "+4916099887766"


def _lead(
    *,
    phone: str = _PHONE,
    status: str = "issued",
    expires_at: datetime = _EXPIRED,
    loyalty_card_id: str | None = "555",
    location_id: int | None = 9001,
    meta: dict | None = None,
) -> PromoLead:
    return PromoLead(
        company_id=1,
        phone_e164=phone,
        campaign_name="welcome_discount",
        secret_code="aktion",
        discount_amount=Decimal("15"),
        discount_type="fixed",
        status=status,
        issued_at=datetime(2024, 12, 1, tzinfo=_UTC),
        expires_at=expires_at,
        loyalty_card_id=loyalty_card_id,
        location_id=location_id,
        meta=meta if meta is not None else {"loyalty_card_issued": True},
    )


@contextlib.contextmanager
def _mock_loyalty(*, side_effect=None):
    """Patch AltegioLoyaltyClient so no real HTTP calls are made."""
    mock_instance = MagicMock()
    mock_instance.delete_card = AsyncMock(side_effect=side_effect)
    mock_instance.aclose = AsyncMock()
    with patch(
        "altegio_bot.promo_loyalty_cleanup.AltegioLoyaltyClient",
        return_value=mock_instance,
    ):
        yield mock_instance


# ---------------------------------------------------------------------------
# 1. Expired issued lead with card → delete_card called, status/meta updated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_lead_card_deleted(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead()
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_awaited_once_with(9001, 555)
    assert result.found == 1
    assert result.deleted == 1
    assert result.failed == 0
    assert result.skipped == 0

    assert lead.status == "expired"
    meta = lead.meta or {}
    assert meta.get("promo_card_deleted_at") is not None
    assert meta.get("promo_card_delete_attempted_at") is not None
    assert meta.get("promo_card_delete_result") == "deleted"
    assert meta.get("promo_card_delete_error") is None


# ---------------------------------------------------------------------------
# 2. Not-expired lead → not fetched, delete_card not called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_not_expired_lead_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead(expires_at=_FUTURE)
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 0
    assert result.deleted == 0
    assert result.skipped == 0
    assert lead.status == "issued"


# ---------------------------------------------------------------------------
# 3. Already deleted (promo_card_deleted_at set) → skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_deleted_is_skipped(session_maker) -> None:
    already_deleted_meta = {
        "loyalty_card_issued": True,
        "promo_card_deleted_at": "2026-04-01T00:00:00+00:00",
        "promo_card_delete_result": "deleted",
    }

    async with session_maker() as session:
        async with session.begin():
            lead = _lead(meta=already_deleted_meta)
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 1
    assert result.skipped == 1
    assert result.deleted == 0


# ---------------------------------------------------------------------------
# 4. delete_card raises → status unchanged, meta.promo_card_delete_result='failed'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_card_failure_marks_failed(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead()
            session.add(lead)
            await session.flush()

            with _mock_loyalty(side_effect=RuntimeError("Altegio 503")) as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_awaited_once()
    assert result.found == 1
    assert result.failed == 1
    assert result.deleted == 0

    assert lead.status == "issued", "status must not change on delete failure"
    meta = lead.meta or {}
    assert meta.get("promo_card_delete_result") == "failed"
    assert "Altegio 503" in (meta.get("promo_card_delete_error") or "")
    assert meta.get("promo_card_delete_attempted_at") is not None
    assert meta.get("promo_card_deleted_at") is None, "promo_card_deleted_at must not be set on failure"


# ---------------------------------------------------------------------------
# 4b. Failed lead is retryable — promo_card_deleted_at absent means next run retries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_lead_is_retryable(session_maker) -> None:
    failed_meta = {
        "loyalty_card_issued": True,
        "promo_card_delete_attempted_at": "2026-04-30T00:00:00+00:00",
        "promo_card_delete_result": "failed",
        "promo_card_delete_error": "timeout",
    }

    async with session_maker() as session:
        async with session.begin():
            lead = _lead(meta=failed_meta)
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_awaited_once(), "Retry must call delete_card again"
    assert result.deleted == 1
    assert lead.status == "expired"
    assert (lead.meta or {}).get("promo_card_delete_result") == "deleted"


# ---------------------------------------------------------------------------
# 5. status='used' → not fetched by SQL (not in cleanup statuses)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_used_lead_not_processed(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead(status="used")
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 0
    assert result.deleted == 0
    assert lead.status == "used"


# ---------------------------------------------------------------------------
# 6. status='cancelled' → not fetched by SQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_lead_not_processed(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead(status="cancelled")
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 0
    assert lead.status == "cancelled"


# ---------------------------------------------------------------------------
# 7. No loyalty_card_id → not fetched by SQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lead_without_card_id_not_processed(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead(loyalty_card_id=None)
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 0


# ---------------------------------------------------------------------------
# 8. No location_id → not fetched by SQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lead_without_location_id_not_processed(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            lead = _lead(location_id=None)
            session.add(lead)
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 0


# ---------------------------------------------------------------------------
# 9. meta.loyalty_card_issued is not True → skipped (Python guard)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_loyalty_card_issued_not_true_skipped(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            # Covers both missing key and explicit False
            lead_missing = _lead(phone="+49100000091", meta={})
            lead_false = _lead(phone="+49100000092", meta={"loyalty_card_issued": False})
            session.add_all([lead_missing, lead_false])
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW)

    mock_client.delete_card.assert_not_awaited()
    assert result.found == 2
    assert result.skipped == 2
    assert result.deleted == 0


# ---------------------------------------------------------------------------
# 10. limit caps the number of rows processed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_limit_caps_processed_rows(session_maker) -> None:
    phones = [f"+491600000{i:02d}" for i in range(5)]

    async with session_maker() as session:
        async with session.begin():
            for phone in phones:
                session.add(_lead(phone=phone))
            await session.flush()

            with _mock_loyalty() as mock_client:
                result = await cleanup_expired_promo_loyalty_cards(session, now=_NOW, limit=3)

    assert mock_client.delete_card.await_count == 3, "Only limit=3 rows must be processed"
    assert result.found == 3
    assert result.deleted == 3
