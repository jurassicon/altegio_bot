"""Tests for mandatory card_type_id validation on send-real new-clients campaign.

Business rule: send-real MUST NOT be created without an explicitly selected
loyalty card type. The backend must reject the request with HTTP 422 before
creating any CampaignRun or execution job.

Test coverage:
  - send-real forbidden without card_type_id (None, empty string, whitespace)
  - no CampaignRun created when card is missing
  - no execution job created when card is missing
  - valid send-real with card_type_id still works (HTTP 202)
  - error message is understandable
  - repeated invalid attempts do not accumulate orphan queued runs/jobs
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRun, MessageJob
from altegio_bot.ops.auth import require_ops_auth

COMPANY_ID = 758285
LOCATION_ID = 758285
PERIOD_START = "2026-03-01T00:00:00Z"
PERIOD_END = "2026-03-31T23:59:59Z"
VALID_CARD_TYPE = "card-type-abc123"

_BASE_PAYLOAD: dict = {
    "company_id": COMPANY_ID,
    "location_id": LOCATION_ID,
    "period_start": PERIOD_START,
    "period_end": PERIOD_END,
    "attribution_window_days": 30,
    "followup_enabled": False,
}


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _count_runs(session_maker, *, mode: str = "send-real") -> int:
    async with session_maker() as session:
        result = await session.execute(select(CampaignRun).where(CampaignRun.mode == mode))
        return len(result.scalars().all())


async def _count_jobs(session_maker, *, job_type: str = "campaign_execute_new_clients_monthly") -> int:
    async with session_maker() as session:
        result = await session.execute(select(MessageJob).where(MessageJob.job_type == job_type))
        return len(result.scalars().all())


# ---------------------------------------------------------------------------
# 1. send-real rejected without card_type_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_real_rejected_without_card_type_id(
    http_client: AsyncClient,
) -> None:
    """card_type_id=None → HTTP 422 with understandable error message."""
    payload = {**_BASE_PAYLOAD, "card_type_id": None}
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    assert resp.status_code == 422
    detail = resp.json().get("detail", "")
    assert "карты лояльности" in detail.lower() or "лояльности" in detail.lower()


@pytest.mark.asyncio
async def test_send_real_rejected_with_empty_card_type_id(
    http_client: AsyncClient,
) -> None:
    """card_type_id='' → HTTP 422."""
    payload = {**_BASE_PAYLOAD, "card_type_id": ""}
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_send_real_rejected_with_whitespace_card_type_id(
    http_client: AsyncClient,
) -> None:
    """card_type_id='   ' (spaces only) → HTTP 422."""
    payload = {**_BASE_PAYLOAD, "card_type_id": "   "}
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_send_real_missing_card_type_id_field(
    http_client: AsyncClient,
) -> None:
    """Payload without card_type_id key at all → HTTP 422."""
    payload = {k: v for k, v in _BASE_PAYLOAD.items()}  # no card_type_id key
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 2. No CampaignRun and no execution job created on invalid request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_campaign_run_created_when_card_missing(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """No CampaignRun(mode=send-real) must be created when card_type_id is absent."""
    payload = {**_BASE_PAYLOAD, "card_type_id": None}

    before = await _count_runs(session_maker)
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    after = await _count_runs(session_maker)

    assert resp.status_code == 422
    assert after == before, f"Expected no new runs; got {after - before} new run(s)"


@pytest.mark.asyncio
async def test_no_execution_job_created_when_card_missing(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """No campaign_execute_new_clients_monthly job must be queued when card is absent."""
    payload = {**_BASE_PAYLOAD, "card_type_id": None}

    before = await _count_jobs(session_maker)
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    after = await _count_jobs(session_maker)

    assert resp.status_code == 422
    assert after == before, f"Expected no new jobs; got {after - before} new job(s)"


# ---------------------------------------------------------------------------
# 3. Valid send-real with card_type_id still works
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_send_real_with_card_type_id_accepted(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """send-real with explicit card_type_id → HTTP 202 + run created."""
    payload = {**_BASE_PAYLOAD, "card_type_id": VALID_CARD_TYPE}

    before = await _count_runs(session_maker)
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    after = await _count_runs(session_maker)

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data.get("accepted") is True
    assert data.get("mode") == "send-real"
    assert after == before + 1, "Expected exactly one new CampaignRun"


@pytest.mark.asyncio
async def test_valid_send_real_creates_execution_job(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """send-real with explicit card_type_id → execution job queued."""
    payload = {**_BASE_PAYLOAD, "card_type_id": VALID_CARD_TYPE}

    before = await _count_jobs(session_maker)
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    after = await _count_jobs(session_maker)

    assert resp.status_code == 202
    assert after == before + 1, "Expected exactly one new execution job"


@pytest.mark.asyncio
async def test_valid_send_real_run_has_correct_card_type_id(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """card_type_id must be stored on the created CampaignRun."""
    payload = {**_BASE_PAYLOAD, "card_type_id": VALID_CARD_TYPE}
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    assert resp.status_code == 202
    run_id = resp.json()["id"]

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run is not None
    assert run.card_type_id == VALID_CARD_TYPE


# ---------------------------------------------------------------------------
# 4. Error message is understandable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_error_message_mentions_loyalty_card(http_client: AsyncClient) -> None:
    """The 422 error detail must mention loyalty card selection."""
    payload = {**_BASE_PAYLOAD, "card_type_id": None}
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
    assert resp.status_code == 422
    body = resp.json()
    detail = body.get("detail", "")
    # Must mention loyalty card in a human-readable way (Russian text)
    assert "карт" in detail.lower() or "лояльност" in detail.lower()


# ---------------------------------------------------------------------------
# 5. Repeated invalid attempts do not accumulate orphan queued runs/jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_invalid_attempts_no_orphan_accumulation(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Three consecutive invalid requests must not create any run or job."""
    payload = {**_BASE_PAYLOAD, "card_type_id": None}

    for _ in range(3):
        resp = await http_client.post("/ops/campaigns/new-clients/run", json=payload)
        assert resp.status_code == 422

    runs_count = await _count_runs(session_maker)
    jobs_count = await _count_jobs(session_maker)
    assert runs_count == 0, f"Expected 0 orphan runs, found {runs_count}"
    assert jobs_count == 0, f"Expected 0 orphan jobs, found {jobs_count}"


@pytest.mark.asyncio
async def test_valid_run_after_invalid_attempts_is_sole_run(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """After N invalid attempts, a valid run must be the only one created."""
    invalid_payload = {**_BASE_PAYLOAD, "card_type_id": None}
    valid_payload = {**_BASE_PAYLOAD, "card_type_id": VALID_CARD_TYPE}

    # Two invalid attempts
    for _ in range(2):
        resp = await http_client.post("/ops/campaigns/new-clients/run", json=invalid_payload)
        assert resp.status_code == 422

    # One valid run
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=valid_payload)
    assert resp.status_code == 202

    runs_count = await _count_runs(session_maker)
    jobs_count = await _count_jobs(session_maker)
    assert runs_count == 1, f"Expected exactly 1 valid run, found {runs_count}"
    assert jobs_count == 1, f"Expected exactly 1 execution job, found {jobs_count}"


# ---------------------------------------------------------------------------
# 6. Preview is not affected by send-real card validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_still_works_without_card_type_id(
    http_client: AsyncClient,
    session_maker,
    monkeypatch,
) -> None:
    """Preview creation (mode=preview) must NOT be blocked by card_type_id absence.

    card_type_id is optional for preview — it is only mandatory for send-real.
    """
    # Mock find_candidates to avoid real CRM calls
    from altegio_bot.campaigns import runner as runner_mod

    monkeypatch.setattr(
        runner_mod,
        "find_candidates",
        AsyncMock(return_value=[]),
    )

    payload = {
        "company_id": COMPANY_ID,
        "location_id": LOCATION_ID,
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "attribution_window_days": 30,
        "followup_enabled": False,
        # card_type_id intentionally omitted
    }
    resp = await http_client.post("/ops/campaigns/new-clients/preview", json=payload)
    assert resp.status_code == 200, f"Preview should succeed without card: {resp.text}"
    data = resp.json()
    assert data.get("mode") == "preview"
