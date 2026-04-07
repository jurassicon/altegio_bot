"""Тесты followup_worker: авто-планирование и авто-запуск follow-up.

Шесть сценариев:
  1. Due completed run с followup_enabled → auto plan + execute.
  2. Run с followup_enabled=False → игнорируется.
  3. Run ещё не due → игнорируется.
  4. Run с meta['followup_auto_status']='completed' → не трогается повторно.
  5. Ошибка в plan_followup → записывается в meta['followup_auto_last_error'].
  6. GET /ops/campaigns/runs/{run_id} включает followup_auto блок.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.followup as followup_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.workers.followup_worker as followup_worker_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.workers.followup_worker import _claim_due_runs, process_run

COMPANY = 758285
LOCATION = 1
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
CLIENT_ID = 1
PHONE = "+10000000001"
TEMPLATE = "test_followup_template"


def _make_run(
    session,
    *,
    mode: str = "send-real",
    status: str = "completed",
    followup_enabled: bool = True,
    followup_delay_days: int | None = 7,
    followup_template_name: str | None = TEMPLATE,
    followup_policy: str | None = "unread_only",
    completed_at: datetime | None = None,
    meta: dict | None = None,
    **kw,
) -> CampaignRun:
    if completed_at is None:
        # По умолчанию — завершился 8 дней назад, задержка 7 дней → уже due
        completed_at = datetime.now(timezone.utc) - timedelta(days=8)

    defaults = dict(
        campaign_code="new_clients_monthly",
        mode=mode,
        status=status,
        company_ids=[COMPANY],
        location_id=LOCATION,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        total_clients_seen=0,
        candidates_count=0,
        followup_enabled=followup_enabled,
        followup_delay_days=followup_delay_days,
        followup_template_name=followup_template_name,
        followup_policy=followup_policy,
        completed_at=completed_at,
        meta=meta if meta is not None else {},
    )
    defaults.update(kw)
    run = CampaignRun(**defaults)
    session.add(run)
    return run


def _make_recipient(session, run_id: int, **kw) -> CampaignRecipient:
    defaults = dict(
        campaign_run_id=run_id,
        company_id=COMPANY,
        client_id=CLIENT_ID,
        phone_e164=PHONE,
        status="queued",
        followup_status=None,
        read_at=None,
    )
    defaults.update(kw)
    r = CampaignRecipient(**defaults)
    session.add(r)
    return r


@pytest.fixture
def worker_session(session_maker, monkeypatch):
    """Патчим SessionLocal во всех модулях на тестовый session_maker."""
    monkeypatch.setattr(followup_worker_module, "SessionLocal", session_maker)
    monkeypatch.setattr(followup_module, "SessionLocal", session_maker)
    return session_maker


@pytest_asyncio.fixture
async def worker_http_client(
    session_maker,
    monkeypatch,
) -> AsyncGenerator[AsyncClient, None]:
    """HTTP-клиент + патч SessionLocal для API-тестов."""
    monkeypatch.setattr(followup_worker_module, "SessionLocal", session_maker)
    monkeypatch.setattr(followup_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setitem(
        app.dependency_overrides,
        require_ops_auth,
        lambda: None,
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Тест 1: due run → auto plan + execute
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_due_run_gets_auto_planned_and_queued(
    worker_session,
    session_maker,
) -> None:
    """Completed run с прошедшим followup_delay_days должен получить авто-follow-up."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id
            recipient = _make_recipient(session, run_id)
            await session.flush()
            recipient_id = recipient.id

    await process_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        r = await session.get(CampaignRecipient, recipient_id)

    assert run is not None
    meta = run.meta or {}
    assert meta.get("followup_auto_status") == "completed"
    assert meta.get("followup_auto_planned_count") == 1
    assert meta.get("followup_auto_queued_count") == 1
    assert "followup_auto_completed_at" in meta
    assert "followup_auto_last_error" not in meta

    assert r is not None
    assert r.followup_status == "followup_queued"


# ---------------------------------------------------------------------------
# Тест 2: followup_enabled=False → не попадает в _claim_due_runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_without_followup_enabled_ignored(
    worker_session,
    session_maker,
) -> None:
    """Run с followup_enabled=False не должен попасть в авто-follow-up."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, followup_enabled=False)
            await session.flush()
            run_id = run.id

    async with session_maker() as session:
        async with session.begin():
            claimed = await _claim_due_runs(session)

    assert run_id not in claimed


# ---------------------------------------------------------------------------
# Тест 3: run ещё не due → не попадает в _claim_due_runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_not_yet_due_ignored(
    worker_session,
    session_maker,
) -> None:
    """Run, у которого не истёк followup_delay_days, не должен забираться worker'ом."""
    async with session_maker() as session:
        async with session.begin():
            # Завершился 1 час назад, задержка 7 дней — ещё не due
            run = _make_run(
                session,
                completed_at=datetime.now(timezone.utc) - timedelta(hours=1),
                followup_delay_days=7,
            )
            await session.flush()
            run_id = run.id

    async with session_maker() as session:
        async with session.begin():
            claimed = await _claim_due_runs(session)

    assert run_id not in claimed


# ---------------------------------------------------------------------------
# Тест 4: уже processed → не попадает повторно
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_completed_auto_followup_ignored(
    worker_session,
    session_maker,
) -> None:
    """Run с meta['followup_auto_status']='completed' не обрабатывается повторно."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(
                session,
                meta={"followup_auto_status": "completed"},
            )
            await session.flush()
            run_id = run.id

    async with session_maker() as session:
        async with session.begin():
            claimed = await _claim_due_runs(session)

    assert run_id not in claimed


# ---------------------------------------------------------------------------
# Тест 5: ошибка → meta['followup_auto_last_error'] и status='failed'
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_stores_followup_auto_last_error_on_failure(
    worker_session,
    session_maker,
) -> None:
    """При ошибке в plan_followup worker должен записать ошибку в run.meta."""
    async with session_maker() as session:
        async with session.begin():
            # followup_policy=None → plan_followup выбросит ValueError
            run = _make_run(session, followup_policy=None)
            await session.flush()
            run_id = run.id

    # process_run не должен пробрасывать исключение
    await process_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run is not None
    meta = run.meta or {}
    assert meta.get("followup_auto_status") == "failed"
    assert meta.get("followup_auto_last_error") is not None
    assert len(meta["followup_auto_last_error"]) > 0


# ---------------------------------------------------------------------------
# Тест 6: GET /ops/campaigns/runs/{run_id} включает followup_auto блок
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_run_includes_followup_auto_block(
    worker_http_client,
    session_maker,
) -> None:
    """GET /ops/campaigns/runs/{run_id} должен включать followup_auto блок из meta."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(
                session,
                meta={
                    "followup_auto_status": "completed",
                    "followup_auto_started_at": "2026-04-01T12:00:00+00:00",
                    "followup_auto_completed_at": "2026-04-01T12:00:05+00:00",
                    "followup_auto_planned_count": 10,
                    "followup_auto_queued_count": 8,
                },
            )
            await session.flush()
            run_id = run.id

    response = await worker_http_client.get(f"/ops/campaigns/runs/{run_id}")
    assert response.status_code == 200

    data = response.json()
    fa = data.get("followup_auto")
    assert fa is not None
    assert fa["followup_auto_status"] == "completed"
    assert fa["followup_auto_planned_count"] == 10
    assert fa["followup_auto_queued_count"] == 8
    assert fa["followup_auto_completed_at"] == "2026-04-01T12:00:05+00:00"
    assert fa["followup_auto_last_error"] is None
