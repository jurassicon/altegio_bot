"""Tests for find_outstanding_campaign_cards and bulk_delete_outstanding_cards.

Business rule
-------------
Loyalty cards issued in month N are NOT automatically deleted when month N+1
campaign runs, because former "new clients" are excluded by has_records_before_period
and never enter the candidate loop.  A dedicated cross-period cleanup is needed.

Coverage
--------
1. find_outstanding_campaign_cards returns nothing when no cards issued.
2. Returns cards from a send-real run.
3. Excludes cards that are already recorded in cleanup_card_ids.
4. Ignores preview runs (only send-real cards are outstanding).
5. Ignores cards from a different company_id.
6. bulk_delete_outstanding_cards calls delete_card and persists cleanup_card_ids.
7. bulk_delete_outstanding_cards skips excluded recipient_ids.
8. bulk_delete_outstanding_cards records a failed delete without aborting others.
9. find_outstanding_campaign_cards returns nothing after bulk delete removes all.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.loyalty_cleanup import (
    bulk_delete_outstanding_cards,
    find_outstanding_campaign_cards,
)
from altegio_bot.models.models import CampaignRecipient, CampaignRun

CAMPAIGN_CODE = "new_clients_monthly"
COMPANY_ID = 758285
LOCATION_ID = 758285
PERIOD_MARCH = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_APRIL = datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_run(
    session: AsyncSession,
    *,
    campaign_code: str = CAMPAIGN_CODE,
    mode: str = "send-real",
    company_id: int = COMPANY_ID,
    location_id: int = LOCATION_ID,
    period_start: datetime = PERIOD_MARCH,
) -> CampaignRun:
    run = CampaignRun(
        campaign_code=campaign_code,
        mode=mode,
        company_ids=[company_id],
        location_id=location_id,
        period_start=period_start,
        period_end=period_start.replace(month=period_start.month % 12 + 1, day=1)
        if period_start.month < 12
        else period_start.replace(year=period_start.year + 1, month=1, day=1),
        status="completed",
    )
    session.add(run)
    return run


def _make_recipient(
    session: AsyncSession,
    run: CampaignRun,
    *,
    company_id: int = COMPANY_ID,
    phone_e164: str = "+49123456789",
    display_name: str = "Test Client",
    loyalty_card_id: str | None = "card-001",
    loyalty_card_number: str | None = "0049123456789000",
    cleanup_card_ids: list | None = None,
) -> CampaignRecipient:
    r = CampaignRecipient(
        campaign_run_id=run.id,
        company_id=company_id,
        phone_e164=phone_e164,
        display_name=display_name,
        status="provider_accepted",
        loyalty_card_id=loyalty_card_id,
        loyalty_card_number=loyalty_card_number,
        cleanup_card_ids=cleanup_card_ids or [],
    )
    session.add(r)
    return r


# ---------------------------------------------------------------------------
# Tests: find_outstanding_campaign_cards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_cards_issued(session_maker):
    async with session_maker() as session:
        async with session.begin():
            _make_run(session)
        # No recipients added → no cards issued
        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)
    assert cards == []


@pytest.mark.asyncio
async def test_returns_card_from_send_real_run(session_maker):
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            _make_recipient(session, run, loyalty_card_id="card-001")

        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)

    assert len(cards) == 1
    assert cards[0]["loyalty_card_id"] == "card-001"
    assert cards[0]["phone_e164"] == "+49123456789"
    assert cards[0]["location_id"] == LOCATION_ID


@pytest.mark.asyncio
async def test_excludes_already_deleted_card(session_maker):
    """A card listed in cleanup_card_ids of any recipient must not be returned."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            # Recipient with card already deleted
            _make_recipient(
                session,
                run,
                loyalty_card_id="card-001",
                cleanup_card_ids=["card-001"],
            )

        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)

    assert cards == []


@pytest.mark.asyncio
async def test_ignores_preview_run_cards(session_maker):
    """Cards from preview runs must never appear in the outstanding list."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, mode="preview")
            await session.flush()
            _make_recipient(session, run, loyalty_card_id="card-preview")

        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)

    assert cards == []


@pytest.mark.asyncio
async def test_ignores_different_company(session_maker):
    """Cards scoped to a different company_id must not be returned."""
    other_company = 999999
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code=CAMPAIGN_CODE,
                mode="send-real",
                company_ids=[other_company],
                location_id=other_company,
                period_start=PERIOD_MARCH,
                period_end=PERIOD_APRIL,
                status="completed",
            )
            session.add(run)
            await session.flush()
            _make_recipient(session, run, company_id=other_company, loyalty_card_id="card-other")

        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)

    assert cards == []


@pytest.mark.asyncio
async def test_multiple_runs_returns_all_outstanding(session_maker):
    """Cards from multiple runs for same campaign+company all appear."""
    async with session_maker() as session:
        async with session.begin():
            run1 = _make_run(session, period_start=PERIOD_MARCH)
            run2 = _make_run(session, period_start=PERIOD_APRIL)
            await session.flush()
            _make_recipient(session, run1, phone_e164="+49111", loyalty_card_id="card-march")
            _make_recipient(session, run2, phone_e164="+49222", loyalty_card_id="card-april")

        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)

    card_ids = {c["loyalty_card_id"] for c in cards}
    assert card_ids == {"card-march", "card-april"}


# ---------------------------------------------------------------------------
# Tests: bulk_delete_outstanding_cards
# ---------------------------------------------------------------------------


def _mock_loyalty(delete_raises: dict[int, Exception] | None = None) -> MagicMock:
    loyalty = MagicMock()
    delete_raises = delete_raises or {}

    async def _delete(location_id: int, card_id: int) -> None:
        if card_id in delete_raises:
            raise delete_raises[card_id]

    loyalty.delete_card = AsyncMock(side_effect=_delete)
    return loyalty


@pytest.mark.asyncio
async def test_bulk_delete_calls_api_and_persists(session_maker):
    """Successful delete must call loyalty.delete_card and update cleanup_card_ids."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            recipient = _make_recipient(session, run, loyalty_card_id="111")
            await session.flush()
            recipient_id = recipient.id

    outstanding = [
        {
            "recipient_id": recipient_id,
            "run_id": run.id,
            "client_id": None,
            "phone_e164": "+49123456789",
            "display_name": "Test",
            "loyalty_card_id": "111",
            "loyalty_card_number": "0049123456789000",
            "period_start": PERIOD_MARCH.isoformat(),
            "location_id": LOCATION_ID,
        }
    ]
    loyalty = _mock_loyalty()

    result = await bulk_delete_outstanding_cards(
        loyalty,
        outstanding,
        exclude_recipient_ids=set(),
        session_factory=session_maker,
    )

    assert result.deleted == ["111"]
    assert result.failed == []
    assert result.skipped == 0
    loyalty.delete_card.assert_awaited_once_with(LOCATION_ID, 111)

    # Verify cleanup_card_ids persisted in DB
    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)
        assert r is not None
        assert "111" in [str(x) for x in (r.cleanup_card_ids or [])]


@pytest.mark.asyncio
async def test_bulk_delete_skips_excluded(session_maker):
    """Recipients in exclude_recipient_ids must not have their cards deleted."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            recipient = _make_recipient(session, run, loyalty_card_id="222")
            await session.flush()
            recipient_id = recipient.id

    outstanding = [
        {
            "recipient_id": recipient_id,
            "run_id": run.id,
            "client_id": None,
            "phone_e164": "+49000000000",
            "display_name": "Skip Me",
            "loyalty_card_id": "222",
            "loyalty_card_number": "",
            "period_start": PERIOD_MARCH.isoformat(),
            "location_id": LOCATION_ID,
        }
    ]
    loyalty = _mock_loyalty()

    result = await bulk_delete_outstanding_cards(
        loyalty,
        outstanding,
        exclude_recipient_ids={recipient_id},
        session_factory=session_maker,
    )

    assert result.skipped == 1
    assert result.deleted == []
    loyalty.delete_card.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_delete_records_failure_and_continues(session_maker):
    """A failed delete must be recorded; the loop must continue with the next card."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            r1 = _make_recipient(session, run, phone_e164="+491", loyalty_card_id="50001")
            r2 = _make_recipient(session, run, phone_e164="+492", loyalty_card_id="50002")
            await session.flush()
            r1_id, r2_id = r1.id, r2.id

    outstanding: list[dict[str, Any]] = [
        {
            "recipient_id": r1_id,
            "run_id": run.id,
            "client_id": None,
            "phone_e164": "+491",
            "display_name": "Bad",
            "loyalty_card_id": "50001",
            "loyalty_card_number": "",
            "period_start": PERIOD_MARCH.isoformat(),
            "location_id": LOCATION_ID,
        },
        {
            "recipient_id": r2_id,
            "run_id": run.id,
            "client_id": None,
            "phone_e164": "+492",
            "display_name": "Good",
            "loyalty_card_id": "50002",
            "loyalty_card_number": "",
            "period_start": PERIOD_MARCH.isoformat(),
            "location_id": LOCATION_ID,
        },
    ]
    loyalty = _mock_loyalty(delete_raises={50001: RuntimeError("API error")})

    result = await bulk_delete_outstanding_cards(
        loyalty,
        outstanding,
        exclude_recipient_ids=set(),
        session_factory=session_maker,
    )

    assert result.deleted == ["50002"]
    assert len(result.failed) == 1
    assert result.failed[0]["card_id"] == "50001"

    # 50002 must be persisted; 50001 must NOT be in cleanup_card_ids
    async with session_maker() as session:
        good = await session.get(CampaignRecipient, r2_id)
        bad = await session.get(CampaignRecipient, r1_id)
        assert "50002" in [str(x) for x in (good.cleanup_card_ids or [])]
        assert "50001" not in [str(x) for x in (bad.cleanup_card_ids or [])]


@pytest.mark.asyncio
async def test_find_outstanding_empty_after_bulk_delete(session_maker):
    """After bulk delete, find_outstanding_campaign_cards must return empty."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            recipient = _make_recipient(session, run, loyalty_card_id="333")
            await session.flush()
            recipient_id = recipient.id

    outstanding = [
        {
            "recipient_id": recipient_id,
            "run_id": run.id,
            "client_id": None,
            "phone_e164": "+49333",
            "display_name": "C",
            "loyalty_card_id": "333",
            "loyalty_card_number": "",
            "period_start": PERIOD_MARCH.isoformat(),
            "location_id": LOCATION_ID,
        }
    ]
    loyalty = _mock_loyalty()

    await bulk_delete_outstanding_cards(
        loyalty,
        outstanding,
        exclude_recipient_ids=set(),
        session_factory=session_maker,
    )

    async with session_maker() as session:
        cards = await find_outstanding_campaign_cards(session, campaign_code=CAMPAIGN_CODE, company_id=COMPANY_ID)
    assert cards == []
