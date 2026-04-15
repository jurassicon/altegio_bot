"""Tests: send-real run status when card issuance fails.

Context (run 12 silent-success bug):
  - preview run 11: eligible=1
  - send-real run 12: completed, but cards_issued=0, queued=0, failed=1
  - root cause: _execute_send_real_for_existing_run() always set status='completed'
    regardless of stats['failed'] and stats['queued']
  - UI showed 100% completed with no visible error

Fix:
  - if eligible_count > 0 and queued == 0 and failed > 0:
      run.status = 'failed', meta['last_error'] = descriptive message
  - if queued > 0 and failed > 0 (partial):
      run.status = 'completed', meta['partial_failure'] = True
  - normal success: run.status = 'completed', no last_error

Coverage:
  1. issue_card fails for the only eligible → run becomes failed, not completed
  2. run.meta contains last_error with card failure count
  3. run.failed_count == 1, run.queued_count == 0
  4. when some recipients queue and some fail → partial-success: completed + partial_failure meta
  5. normal success: completed with no last_error in meta
  6. resume_send_real does not silently re-process card_issue_failed recipients
     (_is_manual_action_recipient guards them correctly)
  7. zero eligible candidates (all pre-excluded) → completed with no error
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

import altegio_bot.campaigns.runner as runner_module
from altegio_bot.campaigns.runner import RunParams, run_send_real
from altegio_bot.campaigns.segment import ClientCandidate, ClientSnapshot
from altegio_bot.models.models import CampaignRecipient

COMPANY = 758285
LOCATION = 1
CARD_TYPE = "type-abc"
PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_params() -> RunParams:
    return RunParams(
        company_id=COMPANY,
        location_id=LOCATION,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        mode="send-real",
        card_type_id=CARD_TYPE,
    )


def _eligible_candidate(phone_e164: str = "+4915111111111") -> ClientCandidate:
    """CRM-only eligible candidate (id=None).

    All run-status tests use CRM-only candidates to avoid FK constraints on
    client_id in the test DB.  The status/meta behavior under test is the same
    regardless of whether the client is local or CRM-only.
    """
    snapshot = ClientSnapshot(
        id=None,
        company_id=COMPANY,
        altegio_client_id=9001,
        display_name="Test Kunde",
        phone_e164=phone_e164,
        wa_opted_out=False,
    )
    return ClientCandidate(
        client=snapshot,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=["Wimpern"],
        records_before_period=0,
        local_client_found=False,
        excluded_reason=None,
    )


def _excluded_candidate(reason: str = "opted_out") -> ClientCandidate:
    snapshot = ClientSnapshot(
        id=10,
        company_id=COMPANY,
        altegio_client_id=9002,
        display_name="Opted Out",
        phone_e164="+4915999999999",
        wa_opted_out=True,
    )
    return ClientCandidate(
        client=snapshot,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=[],
        records_before_period=0,
        local_client_found=True,
        excluded_reason=reason,
    )


class _LoyaltyOK:
    """Mock loyalty that always issues cards successfully."""

    def __init__(self) -> None:
        self.issued: list[dict] = []

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(self, location_id, *, loyalty_card_number, loyalty_card_type_id, phone):
        self.issued.append({"number": loyalty_card_number, "phone": phone})
        return {"loyalty_card_number": loyalty_card_number, "id": "card-ok"}

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


class _LoyaltyFail:
    """Mock loyalty that always raises on issue_card."""

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(self, location_id, *, loyalty_card_number, loyalty_card_type_id, phone):
        raise RuntimeError("Altegio API: card already exists for this phone")

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


class _LoyaltyPartial:
    """Mock loyalty that fails on the second issue_card call."""

    def __init__(self) -> None:
        self.call_count = 0
        self.issued: list[dict] = []

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(self, location_id, *, loyalty_card_number, loyalty_card_type_id, phone):
        self.call_count += 1
        if self.call_count >= 2:
            raise RuntimeError("Altegio API: duplicate card")
        self.issued.append({"number": loyalty_card_number, "phone": phone})
        return {"loyalty_card_number": loyalty_card_number, "id": f"card-{self.call_count}"}

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


@pytest_asyncio.fixture
def runner_ok(session_maker, monkeypatch) -> _LoyaltyOK:
    loyalty = _LoyaltyOK()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


@pytest_asyncio.fixture
def runner_fail(session_maker, monkeypatch) -> _LoyaltyFail:
    loyalty = _LoyaltyFail()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


@pytest_asyncio.fixture
def runner_partial(session_maker, monkeypatch) -> _LoyaltyPartial:
    loyalty = _LoyaltyPartial()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


# ---------------------------------------------------------------------------
# 1. issue_card fails for the only eligible → run becomes failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_eligible_failed_run_becomes_failed(runner_fail, monkeypatch) -> None:
    """When the only eligible recipient fails card issuance, run.status must be 'failed'.

    This is the exact silent-success bug from run 12: cards_issued=0, queued=0, failed=1,
    but run was reported as 'completed'. After the fix, run.status == 'failed'.
    """
    candidate = _eligible_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.status == "failed", f"Expected run.status='failed' when all card issuances fail, got {run.status!r}"
    assert run.queued_count == 0
    assert run.failed_count == 1
    assert run.cards_issued_count == 0


@pytest.mark.asyncio
async def test_all_eligible_failed_last_error_in_meta(runner_fail, monkeypatch) -> None:
    """run.meta['last_error'] must contain a descriptive message when all recipients fail."""
    candidate = _eligible_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.meta is not None, "run.meta must not be None"
    last_error = run.meta.get("last_error", "")
    assert last_error, "run.meta['last_error'] must be set when run fails due to card issuance"
    assert "0 queued" in last_error, f"last_error should mention '0 queued', got: {last_error!r}"
    assert "1 recipient" in last_error, f"last_error should mention recipient count, got: {last_error!r}"


@pytest.mark.asyncio
async def test_all_eligible_failed_counters_correct(runner_fail, monkeypatch) -> None:
    """failed_count and queued_count are correctly recorded on the failed run."""
    candidate = _eligible_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.failed_count == 1
    assert run.queued_count == 0
    assert run.cards_issued_count == 0
    assert run.sent_count == 0


# ---------------------------------------------------------------------------
# 2. Partial success: some queued, some failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_success_run_is_completed(runner_partial, monkeypatch) -> None:
    """When some recipients queue and some fail, run.status == 'completed' (partial success)."""
    c1 = _eligible_candidate(phone_e164="+4915111111111")
    c2 = _eligible_candidate(phone_e164="+4915222222222")

    with patch.object(runner_module, "find_candidates", return_value=[c1, c2]):
        run = await run_send_real(_make_params())

    assert run.status == "completed", f"Partial success must keep run.status='completed', got {run.status!r}"
    assert run.queued_count == 1
    assert run.failed_count == 1


@pytest.mark.asyncio
async def test_partial_success_meta_has_partial_failure_flag(runner_partial, monkeypatch) -> None:
    """Partial success sets meta['partial_failure'] = True and 'partial_failure_count'."""
    c1 = _eligible_candidate(phone_e164="+4915111111111")
    c2 = _eligible_candidate(phone_e164="+4915222222222")

    with patch.object(runner_module, "find_candidates", return_value=[c1, c2]):
        run = await run_send_real(_make_params())

    assert run.meta is not None
    assert run.meta.get("partial_failure") is True, (
        f"meta['partial_failure'] must be True for partial success, got {run.meta!r}"
    )
    assert run.meta.get("partial_failure_count") == 1, (
        f"meta['partial_failure_count'] must be 1, got {run.meta.get('partial_failure_count')!r}"
    )
    assert "last_error" not in run.meta, "Partial success must NOT set last_error (run is completed, not failed)"


# ---------------------------------------------------------------------------
# 3. Normal success: completed with no error meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_normal_success_run_is_completed(runner_ok, monkeypatch) -> None:
    """Normal success: all eligible recipients queued → run.status == 'completed', no last_error."""
    candidate = _eligible_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.status == "completed"
    assert run.queued_count == 1
    assert run.failed_count == 0
    assert run.meta is not None
    assert "last_error" not in run.meta, f"No last_error expected for normal success, meta={run.meta!r}"
    assert "partial_failure" not in run.meta


# ---------------------------------------------------------------------------
# 4. Zero eligible candidates (all pre-excluded) → completed, no error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zero_eligible_all_excluded_run_is_completed(runner_ok, monkeypatch) -> None:
    """When all candidates are pre-excluded (no eligible), run must be completed (not failed).

    The all-failed guard only fires when eligible_count > 0. If every candidate
    was excluded before _process_eligible, stats['failed'] == 0 and the guard
    does not trigger — run stays completed.
    """
    candidates = [
        _excluded_candidate("opted_out"),
        _excluded_candidate("has_records_before_period"),
    ]

    with patch.object(runner_module, "find_candidates", return_value=candidates):
        run = await run_send_real(_make_params())

    assert run.status == "completed", f"Run with zero eligible (all excluded) must be completed, got {run.status!r}"
    assert run.failed_count == 0
    assert run.queued_count == 0
    assert run.meta is not None
    assert "last_error" not in run.meta


# ---------------------------------------------------------------------------
# 5. resume_send_real does not silently re-process card_issue_failed recipients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_card_issue_failed_recipient_is_manual_action(runner_fail, monkeypatch, session_maker) -> None:
    """card_issue_failed recipient is marked as manual-action — resume does not retry it.

    _is_manual_action_recipient returns True for status='skipped' + excluded_reason='card_issue_failed',
    so resume_send_real puts it in remaining_manual_count, not remaining_pending_count.
    The recipient must NOT be requeued silently by resume.
    """
    from altegio_bot.campaigns.runner import _is_manual_action_recipient, _is_resume_pending_recipient

    candidate = _eligible_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.status == "failed"
    assert run.failed_count == 1

    # Check that the recipient is marked as manual-action in the DB
    async with session_maker() as session:
        from sqlalchemy import select

        recipients = (
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )

    assert len(recipients) == 1
    r = recipients[0]
    assert r.status == "skipped", f"Expected status='skipped', got {r.status!r}"
    assert r.excluded_reason == "card_issue_failed", (
        f"Expected excluded_reason='card_issue_failed', got {r.excluded_reason!r}"
    )
    assert _is_manual_action_recipient(r), "card_issue_failed recipient must be classified as manual-action"
    assert not _is_resume_pending_recipient(r), (
        "card_issue_failed recipient must NOT be resume-pending (would silently retry)"
    )
