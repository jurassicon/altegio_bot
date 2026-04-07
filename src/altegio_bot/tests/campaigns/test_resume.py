"""Тесты endpoint"а resume для failed send-real campaign run.

Семь сценариев:
  1. Resume candidate recipient — полный flow, recipient → queued.
  2. Resume card_issued recipient — только queue, карта не перевыпускается.
  3. Resume skipped+queue_failed recipient — только queue.
  4. Resume не трогает cleanup_failed.
  5. Resume не трогает card_issue_failed.
  6. Resume запрещён для preview run (400).
  7. Resume запрещён для non-failed run (400).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun
from altegio_bot.ops.auth import require_ops_auth

COMPANY = 758285
LOCATION = 1
CARD_TYPE = "type-1"
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)

# client_id из conftest: клиенты 1 и 10
CLIENT_ID = 1
PHONE = "+10000000001"


class _MockLoyalty:
    """Заглушка AltegioLoyaltyClient для тестов resume."""

    def __init__(self) -> None:
        self.issue_card_call_count = 0

    async def issue_card(
        self,
        location_id,
        *,
        loyalty_card_number,
        loyalty_card_type_id,
        phone,
    ):
        self.issue_card_call_count += 1
        return {
            "loyalty_card_number": loyalty_card_number,
            "id": "mock-card-id",
        }

    async def delete_card(self, location_id, card_id):
        pass

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def aclose(self):
        pass


def _make_run(
    session,
    *,
    mode: str = "send-real",
    status: str = "failed",
    meta: dict | None = None,
    **kw,
) -> CampaignRun:
    defaults = dict(
        campaign_code="new_clients_monthly",
        mode=mode,
        status=status,
        company_ids=[COMPANY],
        location_id=LOCATION,
        card_type_id=CARD_TYPE,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        total_clients_seen=0,
        candidates_count=0,
        meta=meta or {"last_error": "timeout"},
    )
    defaults.update(kw)
    run = CampaignRun(**defaults)
    session.add(run)
    return run


def _make_recipient(
    session, run_id: int, *, recipient_status: str, excluded_reason: str | None = None, **kw
) -> CampaignRecipient:
    defaults = dict(
        campaign_run_id=run_id,
        company_id=COMPANY,
        client_id=CLIENT_ID,
        phone_e164=PHONE,
        status=recipient_status,
        excluded_reason=excluded_reason,
    )
    defaults.update(kw)
    r = CampaignRecipient(**defaults)
    session.add(r)
    return r


@pytest_asyncio.fixture
async def resume_client(
    session_maker,
    monkeypatch,
) -> AsyncGenerator[tuple[AsyncClient, _MockLoyalty], None]:
    """HTTP-клиент с test DB и замоканным loyalty-клиентом."""
    loyalty = _MockLoyalty()

    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(
        runner_module,
        "AltegioLoyaltyClient",
        lambda: loyalty,
    )
    monkeypatch.setitem(
        app.dependency_overrides,
        require_ops_auth,
        lambda: None,
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client, loyalty


# ---------------------------------------------------------------------------
# Тест 1: candidate → полный flow → queued
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_candidate_recipient(
    resume_client,
    session_maker,
) -> None:
    """Resume candidate recipient должен пройти полный flow и стать queued."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id

            recipient = _make_recipient(
                session,
                run_id,
                recipient_status="candidate",
            )
            await session.flush()
            recipient_id = recipient.id

    client, _ = resume_client
    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 200
    data = response.json()

    assert data["accepted"] is True
    assert data["summary"]["resumed_count"] == 1
    assert data["summary"]["skipped_count"] == 0
    assert data["summary"]["remaining_manual_count"] == 0
    assert data["status"] == "completed"

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)

    assert r is not None
    assert r.status == "queued"
    assert r.message_job_id is not None


# ---------------------------------------------------------------------------
# Тест 2: card_issued → только queue → queued
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_card_issued_recipient(
    resume_client,
    session_maker,
) -> None:
    """Resume card_issued не выпускает новую карту — только допоставляет job."""
    client, loyalty = resume_client

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id
            recipient = _make_recipient(
                session,
                run_id,
                recipient_status="card_issued",
                loyalty_card_number="0010000000010",
                loyalty_card_id="existing-card-id",
                loyalty_card_type_id=CARD_TYPE,
            )
            await session.flush()
            recipient_id = recipient.id

    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["resumed_count"] == 1
    assert data["status"] == "completed"
    assert loyalty.issue_card_call_count == 0

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)

    assert r.status == "queued"
    assert r.loyalty_card_id == "existing-card-id"
    assert r.loyalty_card_number == "0010000000010"


# ---------------------------------------------------------------------------
# Тест 3: skipped + queue_failed → только queue → queued
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_queue_failed_recipient(
    resume_client,
    session_maker,
) -> None:
    """Resume skipped+queue_failed допоставляет job без повторного выпуска карты."""
    client, loyalty = resume_client

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id
            recipient = _make_recipient(
                session,
                run_id,
                recipient_status="skipped",
                excluded_reason="queue_failed",
                loyalty_card_number="0010000000010",
                loyalty_card_id="qf-card-id",
                loyalty_card_type_id=CARD_TYPE,
            )
            await session.flush()
            recipient_id = recipient.id

    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["resumed_count"] == 1
    assert data["status"] == "completed"
    assert loyalty.issue_card_call_count == 0

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)

    assert r.status == "queued"
    assert r.excluded_reason is None
    assert r.loyalty_card_id == "qf-card-id"


# ---------------------------------------------------------------------------
# Тест 4: cleanup_failed — не трогается
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_does_not_touch_cleanup_failed(
    resume_client,
    session_maker,
) -> None:
    """Resume не должен трогать cleanup_failed получателей."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id

            _make_recipient(
                session,
                run_id,
                recipient_status="cleanup_failed",
                excluded_reason="cleanup_failed",
            )

    client, _ = resume_client
    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["resumed_count"] == 0
    assert data["summary"]["remaining_manual_count"] == 1
    assert data["status"] == "failed"

    async with session_maker() as session:
        result = await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id))
        r = result.scalar_one()

    assert r.status == "cleanup_failed"


# ---------------------------------------------------------------------------
# Тест 5: card_issue_failed — не трогается
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_does_not_touch_card_issue_failed(resume_client, session_maker) -> None:
    """Resume не должен трогать card_issue_failed получателей."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id
            recipient = _make_recipient(
                session,
                run_id,
                recipient_status="skipped",
                excluded_reason="card_issue_failed",
            )
            await session.flush()
            recipient_id = recipient.id

    client, _ = resume_client
    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["resumed_count"] == 0
    assert data["summary"]["remaining_manual_count"] == 1
    assert data["status"] == "failed"

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)
    assert r.status == "skipped"
    assert r.excluded_reason == "card_issue_failed"  # Не изменился


# ---------------------------------------------------------------------------
# Тест 6: preview run — 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_forbidden_for_preview(resume_client, session_maker) -> None:
    """Resume должен возвращать 400 для preview run."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, mode="preview", status="completed")
            await session.flush()
            run_id = run.id

    client, _ = resume_client
    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 400
    assert "send-real" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Тест 7: non-failed run — 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_forbidden_for_non_failed_run(resume_client, session_maker) -> None:
    """Resume должен возвращать 400 для run со статусом не failed."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, status="completed")
            await session.flush()
            run_id = run.id

    client, _ = resume_client
    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 400
    assert "failed" in response.json()["detail"]


@pytest.mark.asyncio
async def test_resume_recalculates_run_counters(
    resume_client,
    session_maker,
) -> None:
    """Resume должен пересчитывать агрегаты CampaignRun."""
    client, _ = resume_client

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            run_id = run.id

            _make_recipient(
                session,
                run_id,
                recipient_status="card_issued",
                loyalty_card_number="0010000000010",
                loyalty_card_id="existing-card-id",
                loyalty_card_type_id=CARD_TYPE,
            )

    response = await client.post(f"/ops/campaigns/runs/{run_id}/resume")
    assert response.status_code == 200

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run is not None
    assert run.status == "completed"
    assert run.queued_count == 1
    assert run.sent_count == 1
    assert run.cards_issued_count == 1
    assert run.failed_count == 0
