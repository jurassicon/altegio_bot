"""Тесты защиты run-from-preview от рассинхрона параметров.

Проверяет:
- Backend validation: несовпадение company_id / period / campaign_code → 400.
- Discarded preview → 400.
- Совпадающие параметры → запрос проходит валидацию (202).
- UI: страница с from_preview содержит JS prefill и lock логику.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRun
from altegio_bot.ops.auth import require_ops_auth


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch):
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 1, 31, hour=23, minute=59, second=59, tzinfo=timezone.utc)
COMPANY = 758285


async def _create_preview_run(session_maker, **kw) -> int:
    """Создать preview CampaignRun для тестов."""
    defaults = dict(
        campaign_code="new_clients_monthly",
        mode="preview",
        company_ids=[COMPANY],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        status="completed",
    )
    defaults.update(kw)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(**defaults)
            session.add(run)
            await session.flush()
            return run.id


# ---------------------------------------------------------------------------
# Тест: несовпадение company_id → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_preview_company_mismatch_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """company_id в запросе не совпадает с preview → 400."""
    preview_run_id = await _create_preview_run(session_maker)

    body = {
        "company_id": 1271200,  # ДРУГАЯ компания
        "location_id": 1271200,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "Company ID mismatch" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Тест: несовпадение period_start → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_preview_period_start_mismatch_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """period_start в запросе не совпадает с preview → 400."""
    preview_run_id = await _create_preview_run(session_maker)

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-02-01T00:00:00Z",  # ДРУГОЙ период
        "period_end": "2026-02-28T23:59:59Z",
        "source_preview_run_id": preview_run_id,
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "Period start mismatch" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Тест: discarded preview → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_discarded_preview_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Discarded preview не может быть источником send-real → 400."""
    preview_run_id = await _create_preview_run(session_maker, status="discarded")

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "discarded" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Тест: несуществующий preview → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_nonexistent_preview_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Несуществующий preview_run_id → 400."""
    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": 99999999,  # не существует
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Тест: совпадающие параметры → 202 (валидация проходит)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_preview_matching_params_accepted(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Совпадающие параметры → запрос принят (202)."""
    preview_run_id = await _create_preview_run(session_maker)

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
    }

    # Мокируем enqueue_send_real — создаём реальный CampaignRun в БД
    async with session_maker() as session:
        async with session.begin():
            queued_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="queued",
                source_preview_run_id=preview_run_id,
            )
            session.add(queued_run)
            await session.flush()
            queued_run_id = queued_run.id

    async def mock_enqueue(params):
        async with session_maker() as s:
            return await s.get(CampaignRun, queued_run_id)

    with patch("altegio_bot.ops.campaigns_api.enqueue_send_real", new=AsyncMock(side_effect=mock_enqueue)):
        resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)

    assert resp.status_code == 202
    data = resp.json()
    assert data.get("accepted") is True


# ---------------------------------------------------------------------------
# Тест: UI содержит prefill и lock JS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_from_preview_page_has_prefill_js(http_client: AsyncClient) -> None:
    """Страница /ops/campaigns/new-clients?from_preview=123 содержит JS prefill логику."""
    response = await http_client.get("/ops/campaigns/new-clients?from_preview=123")
    assert response.status_code == 200
    text = response.text
    # JS переменная previewRunId должна быть установлена
    assert "previewRunId = 123" in text
    # Должна быть функция prefill
    assert "loadPreviewAndPrefill" in text
    # Disabled поля
    assert "disabled = true" in text


# ---------------------------------------------------------------------------
# Тест: preview endpoint не раскрывает str(exc) в detail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_endpoint_hides_internal_error(
    http_client: AsyncClient,
    monkeypatch,
) -> None:
    """При внутренней ошибке preview возвращает безопасный detail, а не str(exc)."""
    import altegio_bot.ops.campaigns_api as api_mod

    original_run_preview = api_mod.run_preview

    async def failing_preview(params):
        raise RuntimeError("SECRET_DB_SCHEMA_INFO: table campaign_runs column xyz")

    monkeypatch.setattr(api_mod, "run_preview", failing_preview)

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
    }
    resp = await http_client.post("/ops/campaigns/new-clients/preview", json=body)
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    # Не должен содержать внутренние детали
    assert "SECRET_DB_SCHEMA_INFO" not in detail
    assert "campaign_runs" not in detail
    # Должен быть безопасный текст
    assert "internal error" in detail.lower() or "failed" in detail.lower()

    monkeypatch.setattr(api_mod, "run_preview", original_run_preview)
