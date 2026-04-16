"""Tests: loyalty card resolve/reuse/conflict for CRM-only campaign recipients.

Root cause context
------------------
Previously, CRM-only send-real runs called loyalty.issue_card() unconditionally
after cleanup (which always returns ok for client_id=None). If a card already
existed for the same phone (e.g. from a prior queue_failed run), the API
returned a 409 Conflict or created a duplicate, requiring manual cleanup.

New behaviour
-------------
resolve_or_issue_loyalty_card() checks CampaignRecipient records first:
  0 prior cards → issued_new   (API called, cards_issued counter +1)
  1 prior card  → reused_existing  (no API call, cards_reused in meta)
  2+ prior cards → failed_conflict (no API call, recipient → card_conflict)

Test coverage
-------------
1. No existing card → new card issued, loyalty.issue_card called once.
2. One existing card → reused, loyalty.issue_card NOT called.
3. Two existing cards → failed_conflict, loyalty.issue_card NOT called.
4. cards_issued_count correct for new issuances (no increment for reuse).
5. cards_reused_count stored in run.meta when a card is reused.
6. Existing successful send path (fresh run, no prior cards) still works.
7. find_existing_campaign_card_for_phone only matches same campaign_code.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import select

import altegio_bot.campaigns.runner as runner_module
from altegio_bot.campaigns.loyalty_cleanup import (
    find_existing_campaign_card_for_phone,
    resolve_or_issue_loyalty_card,
)
from altegio_bot.campaigns.runner import RunParams, run_send_real
from altegio_bot.campaigns.segment import ClientCandidate, ClientSnapshot
from altegio_bot.models.models import CampaignRecipient, CampaignRun

COMPANY = 758285
LOCATION = 1
CARD_TYPE = "type-res"
CAMPAIGN_CODE = "new_clients_monthly"
PHONE = "+4915123456789"
PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _MockLoyalty:
    def __init__(self) -> None:
        self.issued: list[dict] = []

    async def get_card_types(self, location_id: int):  # noqa: ANN201
        return [{"id": CARD_TYPE}]

    async def issue_card(
        self,
        location_id: int,
        *,
        loyalty_card_number: str,
        loyalty_card_type_id: str,
        phone: int,
    ) -> dict:
        self.issued.append({"number": loyalty_card_number, "phone": phone})
        return {
            "loyalty_card_number": loyalty_card_number,
            "id": f"mock-{loyalty_card_number[-4:]}",
        }

    async def delete_card(self, location_id: int, card_id: int) -> None:
        pass

    async def aclose(self) -> None:
        pass


def _make_run_params(*, source_preview_run_id: int | None = None) -> RunParams:
    return RunParams(
        company_id=COMPANY,
        location_id=LOCATION,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        mode="send-real",
        card_type_id=CARD_TYPE,
        source_preview_run_id=source_preview_run_id,
    )


def _crm_candidate(phone: str = PHONE) -> ClientCandidate:
    snap = ClientSnapshot(
        id=None,
        company_id=COMPANY,
        altegio_client_id=99999,
        display_name="Lalka Marinova",
        phone_e164=phone,
        wa_opted_out=False,
    )
    return ClientCandidate(
        client=snap,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=["Wimpernverlängerung"],
        records_before_period=0,
        local_client_found=False,
        excluded_reason=None,
    )


async def _create_run_with_card(
    session_maker,
    *,
    phone: str = PHONE,
    card_id: str,
    campaign_code: str = CAMPAIGN_CODE,
) -> int:
    """Create a CampaignRun with one recipient that already has a loyalty card."""
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code=campaign_code,
                mode="send-real",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
            )
            session.add(run)
            await session.flush()
            r = CampaignRecipient(
                campaign_run_id=run.id,
                company_id=COMPANY,
                client_id=None,
                phone_e164=phone,
                display_name="Lalka Marinova",
                local_client_found=False,
                total_records_in_period=1,
                confirmed_records_in_period=1,
                lash_records_in_period=1,
                confirmed_lash_records_in_period=1,
                records_before_period=0,
                service_titles_in_period=[],
                status="queued",
                excluded_reason=None,
                loyalty_card_id=card_id,
                loyalty_card_number="0049151234567890",
                loyalty_card_type_id=CARD_TYPE,
            )
            session.add(r)
    return run.id


@pytest_asyncio.fixture
def runner_with_mock_loyalty(session_maker, monkeypatch) -> _MockLoyalty:
    loyalty = _MockLoyalty()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


# ---------------------------------------------------------------------------
# Unit tests: find_existing_campaign_card_for_phone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_existing_returns_empty_when_no_prior_cards(
    session_maker,
) -> None:
    """No prior recipients → empty list returned."""
    async with session_maker() as session:
        result = await find_existing_campaign_card_for_phone(session, phone_e164=PHONE, campaign_code=CAMPAIGN_CODE)
    assert result == []


@pytest.mark.asyncio
async def test_find_existing_returns_card_when_present(session_maker) -> None:
    """One prior recipient with a card → that card returned."""
    await _create_run_with_card(session_maker, card_id="card-abc")
    async with session_maker() as session:
        result = await find_existing_campaign_card_for_phone(session, phone_e164=PHONE, campaign_code=CAMPAIGN_CODE)
    assert len(result) == 1
    assert result[0][0] == "card-abc"


@pytest.mark.asyncio
async def test_find_existing_ignores_different_campaign_code(
    session_maker,
) -> None:
    """Card in a different campaign_code is NOT returned for this campaign."""
    await _create_run_with_card(session_maker, card_id="card-other", campaign_code="other_campaign")
    async with session_maker() as session:
        result = await find_existing_campaign_card_for_phone(session, phone_e164=PHONE, campaign_code=CAMPAIGN_CODE)
    assert result == [], "Cards from a different campaign must not be returned"


@pytest.mark.asyncio
async def test_find_existing_ignores_different_phone(session_maker) -> None:
    """Card for a different phone is NOT returned."""
    await _create_run_with_card(session_maker, phone="+4915999888777", card_id="card-other-phone")
    async with session_maker() as session:
        result = await find_existing_campaign_card_for_phone(session, phone_e164=PHONE, campaign_code=CAMPAIGN_CODE)
    assert result == []


# ---------------------------------------------------------------------------
# Unit tests: resolve_or_issue_loyalty_card
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_issues_new_card_when_no_prior(session_maker) -> None:
    """Test 1: no existing card → issued_new, loyalty.issue_card called once."""
    loyalty = _MockLoyalty()
    async with session_maker() as session:
        res = await resolve_or_issue_loyalty_card(
            session,
            loyalty,
            phone_e164=PHONE,
            location_id=LOCATION,
            card_type_id=CARD_TYPE,
            campaign_code=CAMPAIGN_CODE,
        )

    assert res.outcome == "issued_new"
    assert res.loyalty_card_id != ""
    assert len(loyalty.issued) == 1, "issue_card must be called exactly once"


@pytest.mark.asyncio
async def test_resolve_reuses_existing_card(session_maker) -> None:
    """Test 2: one existing card → reused_existing, loyalty.issue_card NOT called."""
    await _create_run_with_card(session_maker, card_id="prior-card-id")

    loyalty = _MockLoyalty()
    async with session_maker() as session:
        res = await resolve_or_issue_loyalty_card(
            session,
            loyalty,
            phone_e164=PHONE,
            location_id=LOCATION,
            card_type_id=CARD_TYPE,
            campaign_code=CAMPAIGN_CODE,
        )

    assert res.outcome == "reused_existing", f"Expected reused_existing, got {res.outcome!r}"
    assert res.loyalty_card_id == "prior-card-id"
    assert len(loyalty.issued) == 0, "loyalty.issue_card must NOT be called when reusing existing card"


@pytest.mark.asyncio
async def test_resolve_conflict_when_two_cards_exist(session_maker) -> None:
    """Test 3: two existing cards → failed_conflict, loyalty.issue_card NOT called."""
    await _create_run_with_card(session_maker, card_id="card-a")
    await _create_run_with_card(session_maker, card_id="card-b")

    loyalty = _MockLoyalty()
    async with session_maker() as session:
        res = await resolve_or_issue_loyalty_card(
            session,
            loyalty,
            phone_e164=PHONE,
            location_id=LOCATION,
            card_type_id=CARD_TYPE,
            campaign_code=CAMPAIGN_CODE,
        )

    assert res.outcome == "failed_conflict", f"Expected failed_conflict, got {res.outcome!r}"
    assert res.reason is not None
    assert "card-a" in res.reason or "card-b" in res.reason
    assert len(loyalty.issued) == 0, "loyalty.issue_card must NOT be called on conflict"


# ---------------------------------------------------------------------------
# Integration tests: full pipeline via run_send_real
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_run_no_prior_cards_issues_new_and_queues(runner_with_mock_loyalty, session_maker) -> None:
    """Test 6: fresh run with no prior cards → issued_new → recipient queued."""
    loyalty = runner_with_mock_loyalty
    candidate = _crm_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_run_params())

    assert run.status == "completed", f"Expected completed, got {run.status!r}"
    assert run.queued_count == 1
    assert run.failed_count == 0
    assert run.cards_issued_count == 1, f"Expected cards_issued_count=1, got {run.cards_issued_count}"
    assert len(loyalty.issued) == 1, "issue_card must be called for new card"
    # No reuse → cards_reused_count should not appear in meta
    assert not run.meta.get("cards_reused_count"), "No reused cards: cards_reused_count must not be set in meta"


@pytest.mark.asyncio
async def test_run_reuses_existing_card_and_queues(runner_with_mock_loyalty, session_maker) -> None:
    """Test 1+4+5: prior card exists → reused_existing → recipient queued,
    cards_issued_count stays 0, cards_reused_count stored in meta."""
    loyalty = runner_with_mock_loyalty

    # Create a prior run with a card for PHONE
    await _create_run_with_card(session_maker, phone=PHONE, card_id="existing-card-xyz")

    candidate = _crm_candidate(phone=PHONE)
    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_run_params())

    assert run.status == "completed", f"Expected completed, got {run.status!r}"
    assert run.queued_count == 1, f"Recipient must be queued, queued_count={run.queued_count}"
    assert run.failed_count == 0
    assert run.cards_issued_count == 0, (
        f"No new card issued: cards_issued_count must be 0, got {run.cards_issued_count}"
    )
    assert len(loyalty.issued) == 0, "loyalty.issue_card must NOT be called when reusing existing card"
    assert run.meta.get("cards_reused_count") == 1, f"cards_reused_count must be 1 in meta, got {run.meta}"

    # Verify the recipient has the reused card_id
    async with session_maker() as session:
        recipients = (
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )
    assert len(recipients) == 1
    r = recipients[0]
    assert r.loyalty_card_id == "existing-card-xyz", f"Recipient must have reused card_id, got {r.loyalty_card_id!r}"
    assert r.status == "queued"


@pytest.mark.asyncio
async def test_run_fails_safely_on_card_conflict(runner_with_mock_loyalty, session_maker) -> None:
    """Test 3+4: two prior cards → failed_conflict → recipient skipped,
    run fails, loyalty.issue_card NOT called."""
    loyalty = runner_with_mock_loyalty

    await _create_run_with_card(session_maker, phone=PHONE, card_id="card-x")
    await _create_run_with_card(session_maker, phone=PHONE, card_id="card-y")

    candidate = _crm_candidate(phone=PHONE)
    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_run_params())

    assert run.failed_count == 1, f"Conflict must count as failure, failed_count={run.failed_count}"
    assert run.queued_count == 0
    assert len(loyalty.issued) == 0, "loyalty.issue_card must NOT be called on conflict"
    assert run.cards_issued_count == 0

    # Verify recipient has excluded_reason='card_conflict'
    async with session_maker() as session:
        recipients = (
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )
    assert len(recipients) == 1
    r = recipients[0]
    assert r.excluded_reason == "card_conflict", f"Expected card_conflict, got {r.excluded_reason!r}"
    assert r.cleanup_failed_reason is not None, "cleanup_failed_reason must store the conflict details"


@pytest.mark.asyncio
async def test_cards_issued_count_correct_for_multiple_recipients(runner_with_mock_loyalty, session_maker) -> None:
    """Test 4: mix of new + reuse → counters accurate.

    Recipient A (phone1): no prior card → issued_new → cards_issued +1
    Recipient B (phone2): has prior card → reused_existing → cards_reused +1
    """
    loyalty = runner_with_mock_loyalty
    phone1 = "+4915111000001"
    phone2 = "+4915111000002"

    # Create prior card only for phone2
    await _create_run_with_card(session_maker, phone=phone2, card_id="prior-card-phone2")

    candidates = [_crm_candidate(phone=phone1), _crm_candidate(phone=phone2)]
    with patch.object(runner_module, "find_candidates", return_value=candidates):
        run = await run_send_real(_make_run_params())

    assert run.queued_count == 2, f"Both recipients must be queued, got {run.queued_count}"
    assert run.failed_count == 0
    assert run.cards_issued_count == 1, f"Only phone1 should have new card, got {run.cards_issued_count}"
    assert run.meta.get("cards_reused_count") == 1, (
        f"phone2 card reused, expected meta cards_reused_count=1, got {run.meta}"
    )
    assert len(loyalty.issued) == 1, "Only one new API call expected"


@pytest.mark.asyncio
async def test_recompute_not_broken_after_reuse(runner_with_mock_loyalty, session_maker, monkeypatch) -> None:
    """Test 6: recompute_campaign_run_stats can be called after a run with reuse.

    We only verify the run transitions to 'completed' and that the recompute
    helper (mocked) is not called during the send-real itself — it is invoked
    later by the outbox worker, not during campaign execution.
    """
    await _create_run_with_card(session_maker, phone=PHONE, card_id="existing-for-recompute")
    candidate = _crm_candidate()

    recompute_calls: list = []

    async def _fake_recompute(run_id: int, session) -> None:  # noqa: ANN001
        recompute_calls.append(run_id)

    monkeypatch.setattr(runner_module, "recompute_campaign_run_stats", _fake_recompute)

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_run_params())

    assert run.status == "completed"
    # recompute is NOT triggered by send-real itself
    assert recompute_calls == [], "recompute_campaign_run_stats must not be called during send-real"
