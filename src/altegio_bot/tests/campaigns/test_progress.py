"""Тесты observability-endpoint'ов кампаний.

Четыре сценария:
  1. GET /runs/{id} включает last_error из run.meta['last_error'].
  2. GET /runs/{id}/progress возвращает live-счётчики получателей.
  3. GET /runs/{id}/progress включает execution_job, если он есть.
  4. GET /runs возвращает last_error в сводке, если run завершился с ошибкой.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.ops.campaigns_api as campaigns_api_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun, MessageJob
from altegio_bot.ops.auth import require_ops_auth

COMPANY = 758285
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _make_run(
    session, *, mode: str = "send-real", status: str = "completed", meta: dict | None = None, **kw
) -> CampaignRun:
    defaults = dict(
        campaign_code="new_clients_monthly",
        mode=mode,
        status=status,
        company_ids=[COMPANY],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        total_clients_seen=0,
        candidates_count=0,
        meta=meta or {},
    )
    defaults.update(kw)
    run = CampaignRun(**defaults)
    session.add(run)
    return run


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Тест 1: GET /runs/{id} включает last_error из meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_run_includes_last_error_from_meta(http_client, session_maker) -> None:
    """get_run должен вернуть last_error из run.meta['last_error']."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(
                session,
                status="failed",
                meta={"last_error": "Altegio API timeout after 3 retries"},
            )
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/runs/{run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["last_error"] == "Altegio API timeout after 3 retries"


# ---------------------------------------------------------------------------
# Тест 2: GET /runs/{id}/progress возвращает live-счётчики
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_progress_counts_for_running_run(http_client, session_maker) -> None:
    """GET /runs/{id}/progress должен вернуть актуальные счётчики из CampaignRecipient."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, status="running", total_clients_seen=5)
            await session.flush()
            run_id = run.id

            # 2 queued, 1 skipped, 1 candidate, 1 card_issued
            for st in ("queued", "queued", "skipped", "candidate", "card_issued"):
                session.add(
                    CampaignRecipient(
                        campaign_run_id=run_id,
                        company_id=COMPANY,
                        status=st,
                    )
                )

    response = await http_client.get(f"/ops/campaigns/runs/{run_id}/progress")
    assert response.status_code == 200
    p = response.json()["progress"]

    assert p["recipients_total"] == 5
    assert p["recipients_queued"] == 2
    assert p["recipients_skipped"] == 1
    assert p["recipients_candidate"] == 1
    assert p["recipients_card_issued"] == 1
    assert p["recipients_done"] == 3  # queued + skipped
    assert p["recipients_in_progress"] == 2  # candidate + card_issued
    assert p["progress_pct"] == round(3 / 5, 4)


# ---------------------------------------------------------------------------
# Тест 3: GET /runs/{id}/progress включает execution_job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_progress_endpoint_returns_execution_job(http_client, session_maker) -> None:
    """GET /runs/{id}/progress должен включать execution_job, если он есть."""
    from altegio_bot.campaigns.runner import CAMPAIGN_EXECUTION_JOB_TYPE

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, status="running", total_clients_seen=1)
            await session.flush()
            run_id = run.id

            job = MessageJob(
                company_id=COMPANY,
                job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
                run_at=PERIOD_START,
                status="running",
                attempts=1,
                max_attempts=3,
                dedupe_key=f"test-exec-job-run-{run_id}",
                payload={"kind": CAMPAIGN_EXECUTION_JOB_TYPE, "campaign_run_id": run_id},
            )
            session.add(job)

    response = await http_client.get(f"/ops/campaigns/runs/{run_id}/progress")
    assert response.status_code == 200
    data = response.json()

    assert data["run_id"] == run_id
    assert data["status"] == "running"

    ej = data["execution_job"]
    assert ej is not None
    assert ej["status"] == "running"
    assert ej["attempts"] == 1
    assert ej["max_attempts"] == 3


# ---------------------------------------------------------------------------
# Тест 4: GET /runs включает last_error в сводке при failed-статусе
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_runs_includes_last_error_when_failed(http_client, session_maker) -> None:
    """list_runs должен включать last_error в каждой сводке."""
    async with session_maker() as session:
        async with session.begin():
            _make_run(
                session,
                status="failed",
                meta={"last_error": "cleanup step timed out"},
            )
            _make_run(session, status="completed")

    response = await http_client.get("/ops/campaigns/runs")
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 2

    failed = next(i for i in items if i["status"] == "failed")
    ok = next(i for i in items if i["status"] == "completed")

    assert failed["last_error"] == "cleanup step timed out"
    assert ok["last_error"] is None


@pytest.mark.asyncio
async def test_progress_does_not_count_queue_failed_as_done(
    http_client,
    session_maker,
) -> None:
    """queue_failed не должен считаться done в progress.

    Такой recipient ещё можно резюмить, значит он pending/in_progress,
    а не завершённый.
    """
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(
                session,
                status="failed",
                total_clients_seen=2,
                candidates_count=2,
            )
            await session.flush()
            run_id = run.id

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=COMPANY,
                    client_id=1,
                    phone_e164="+10000000001",
                    status="skipped",
                    excluded_reason="queue_failed",
                )
            )
            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=COMPANY,
                    client_id=10,
                    phone_e164="+10000000010",
                    status="queued",
                )
            )

    response = await http_client.get(f"/ops/campaigns/runs/{run_id}/progress")
    assert response.status_code == 200
    data = response.json()

    assert data["progress"]["recipients_done"] == 1
    assert data["progress"]["recipients_queue_failed_pending"] == 1
    assert data["progress"]["recipients_in_progress"] == 1
    assert data["progress"]["progress_pct"] == 0.5
