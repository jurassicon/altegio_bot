"""Tests for the ops campaign HTML routes (/ops/campaigns/*)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun
from altegio_bot.ops.auth import require_ops_auth


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def sample_run(session_maker) -> int:
    """Создаёт тестовый CampaignRun и возвращает его id."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                total_clients_seen=100,
                candidates_count=20,
                queued_count=15,
                cards_issued_count=10,
                sent_count=15,
                delivered_count=12,
                read_count=8,
                followup_enabled=False,
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    return run_id


@pytest_asyncio.fixture
async def sample_run_with_recipients(session_maker, sample_run) -> int:
    """Добавляет получателей к тестовому run."""
    run_id = sample_run
    async with session_maker() as session:
        async with session.begin():
            for i in range(3):
                recipient = CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    client_id=1,
                    altegio_client_id=i + 1,
                    phone_e164=f"+4915112345{i:02d}",
                    display_name=f"Test Client {i + 1}",
                    status="queued",
                )
                session.add(recipient)

    return run_id


# ---------------------------------------------------------------------------
# /ops/campaigns
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_campaigns_list_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/campaigns")
    assert response.status_code == 200
    assert "Campaign Runs" in response.text
    assert "Campaigns" in response.text


@pytest.mark.asyncio
async def test_ops_campaigns_list_shows_run(
    http_client: AsyncClient,
    sample_run: int,
) -> None:
    response = await http_client.get("/ops/campaigns")
    assert response.status_code == 200
    assert str(sample_run) in response.text
    assert "completed" in response.text


@pytest.mark.asyncio
async def test_ops_campaigns_list_filters(http_client: AsyncClient, sample_run: int) -> None:
    response = await http_client.get("/ops/campaigns?mode=send-real&status=completed")
    assert response.status_code == 200
    assert str(sample_run) in response.text


@pytest.mark.asyncio
async def test_ops_campaigns_list_empty_filter(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/campaigns?status=failed")
    assert response.status_code == 200
    assert "Нет рассылок" in response.text


# ---------------------------------------------------------------------------
# /ops/campaigns/{run_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_campaign_run_detail_returns_200(
    http_client: AsyncClient,
    sample_run: int,
) -> None:
    response = await http_client.get(f"/ops/campaigns/{sample_run}")
    assert response.status_code == 200
    assert f"Run #{sample_run}" in response.text
    assert "Delivery" in response.text
    assert "Loyalty" in response.text
    assert "Excluded" in response.text
    assert "Follow-up" in response.text


@pytest.mark.asyncio
async def test_ops_campaign_run_detail_not_found(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/campaigns/9999999")
    assert response.status_code == 200
    assert "not found" in response.text.lower()


@pytest.mark.asyncio
async def test_ops_campaign_run_detail_shows_json_links(
    http_client: AsyncClient,
    sample_run: int,
) -> None:
    response = await http_client.get(f"/ops/campaigns/{sample_run}")
    assert response.status_code == 200
    assert "JSON report" in response.text
    assert "JSON recipients" in response.text
    assert "JSON progress" in response.text


@pytest.mark.asyncio
async def test_ops_campaign_run_detail_shows_last_error(
    http_client: AsyncClient,
    session_maker,
) -> None:
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="failed",
                meta={"last_error": "Something went wrong during execution"},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert "Something went wrong during execution" in response.text
    assert "alert-danger" in response.text


# ---------------------------------------------------------------------------
# /ops/campaigns/{run_id}/recipients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_campaign_recipients_returns_200(
    http_client: AsyncClient,
    sample_run_with_recipients: int,
) -> None:
    run_id = sample_run_with_recipients
    response = await http_client.get(f"/ops/campaigns/{run_id}/recipients")
    assert response.status_code == 200
    assert f"Run #{run_id}" in response.text
    assert "Recipients" in response.text


@pytest.mark.asyncio
async def test_ops_campaign_recipients_shows_data(
    http_client: AsyncClient,
    sample_run_with_recipients: int,
) -> None:
    run_id = sample_run_with_recipients
    response = await http_client.get(f"/ops/campaigns/{run_id}/recipients")
    assert response.status_code == 200
    assert "Test Client 1" in response.text
    assert "+4915112345" in response.text


@pytest.mark.asyncio
async def test_ops_campaign_recipients_not_found(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/campaigns/9999999/recipients")
    assert response.status_code == 200
    assert "not found" in response.text.lower()


@pytest.mark.asyncio
async def test_ops_campaign_recipients_filter_by_status(
    http_client: AsyncClient,
    sample_run_with_recipients: int,
) -> None:
    run_id = sample_run_with_recipients
    response = await http_client.get(f"/ops/campaigns/{run_id}/recipients?status=queued")
    assert response.status_code == 200
    assert "Test Client 1" in response.text

    response_empty = await http_client.get(f"/ops/campaigns/{run_id}/recipients?status=skipped")
    assert response_empty.status_code == 200
    assert "Нет получателей" in response_empty.text


# ---------------------------------------------------------------------------
# /ops/campaigns/dashboard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_campaigns_dashboard_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/campaigns/dashboard?year=2026&month=4")
    assert response.status_code == 200
    assert "Dashboard" in response.text
    assert "2026" in response.text


@pytest.mark.asyncio
async def test_ops_campaigns_dashboard_default_params(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/campaigns/dashboard")
    assert response.status_code == 200
    assert "Dashboard" in response.text
