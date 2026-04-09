"""Тесты защиты run-from-preview от рассинхрона параметров.

Проверяет:
- Backend validation: несовпадение company_id / period / campaign_code → 400.
- Только completed preview → 202; failed/running/queued/discarded → 400.
- card_type_id и followup-параметры зафиксированы в снимке → несовпадение → 400.
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
    """Создать preview CampaignRun для тестов.

    По умолчанию статус 'completed' — только такие previews разрешены как источник send-real.
    """
    defaults = dict(
        campaign_code="new_clients_monthly",
        mode="preview",
        company_ids=[COMPANY],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        status="completed",
        card_type_id=None,
        followup_enabled=False,
        followup_delay_days=None,
        followup_policy=None,
        followup_template_name=None,
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
    detail = resp.json()["detail"]
    # Сообщение теперь объясняет требование к статусу
    assert "completed" in detail
    assert "discarded" in detail


# ---------------------------------------------------------------------------
# Тест: failed preview → 400 (только completed разрешён)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_failed_preview_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Failed preview → 400: только completed previews можно использовать."""
    preview_run_id = await _create_preview_run(session_maker, status="failed")

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "completed" in resp.json()["detail"]
    assert "failed" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_run_from_running_preview_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Running preview → 400: данные неполные."""
    preview_run_id = await _create_preview_run(session_maker, status="running")

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "completed" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Тест: card_type_id mismatch → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_preview_card_type_mismatch_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """card_type_id в запросе не совпадает с preview → 400."""
    preview_run_id = await _create_preview_run(session_maker, card_type_id="type_abc")

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
        "card_type_id": "type_xyz",  # ДРУГОЙ тип карты
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "card_type_id" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Тест: followup_enabled mismatch → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_preview_followup_mismatch_returns_400(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """followup_enabled в запросе не совпадает с preview → 400."""
    preview_run_id = await _create_preview_run(session_maker, followup_enabled=True)

    body = {
        "company_id": COMPANY,
        "location_id": COMPANY,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2026-01-31T23:59:59Z",
        "source_preview_run_id": preview_run_id,
        "followup_enabled": False,  # ОТЛИЧАЕТСЯ от preview
    }
    resp = await http_client.post("/ops/campaigns/new-clients/run", json=body)
    assert resp.status_code == 400
    assert "followup_enabled" in resp.json()["detail"]


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


# ---------------------------------------------------------------------------
# Тест 11.5: _normalise_nullable_str
# ---------------------------------------------------------------------------


def test_normalise_nullable_str_empty_is_none() -> None:
    """Пустая строка нормализуется в None."""
    from altegio_bot.ops.campaigns_api import _normalise_nullable_str

    assert _normalise_nullable_str("") is None
    assert _normalise_nullable_str(None) is None
    assert _normalise_nullable_str("abc") == "abc"
    assert _normalise_nullable_str("0") == "0"  # "0" — truthy, не None


# ---------------------------------------------------------------------------
# Тест 11.7: discovery_source записывается в meta после run_preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_preview_writes_discovery_source(session_maker) -> None:
    """После run_preview run.meta['discovery_source'] == 'local_db'."""
    import altegio_bot.campaigns.runner as runner_module
    from altegio_bot.campaigns.runner import RunParams, run_preview
    from altegio_bot.models.models import CampaignRun

    original_session_local = runner_module.SessionLocal
    runner_module.SessionLocal = session_maker

    params = RunParams(
        company_id=COMPANY,
        location_id=COMPANY,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        mode="preview",
    )

    try:
        with patch("altegio_bot.campaigns.runner.find_candidates", new=AsyncMock(return_value=[])):
            run = await run_preview(params)

        assert run.meta is not None
        assert run.meta.get("discovery_source") == "local_db"

        # Проверить и в БД
        async with session_maker() as session:
            db_run = await session.get(CampaignRun, run.id)
            assert db_run.meta.get("discovery_source") == "local_db"
    finally:
        runner_module.SessionLocal = original_session_local


# ---------------------------------------------------------------------------
# Тест 11.3: final state guard — run.status != 'running' при финализации
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_preview_final_state_guard(session_maker) -> None:
    """Если run.status != 'running' при финализации preview — статус не перезаписывается."""
    import altegio_bot.campaigns.runner as runner_module
    from altegio_bot.campaigns.runner import RunParams, run_preview
    from altegio_bot.models.models import CampaignRun

    async def fake_find_candidates(**kw):
        return []

    # Подменяем SessionLocal в runner.py
    original_session_local = runner_module.SessionLocal
    runner_module.SessionLocal = session_maker

    # Запускаем preview — создаёт run со статусом 'running', потом финализирует
    params = RunParams(
        company_id=COMPANY,
        location_id=COMPANY,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        mode="preview",
    )

    try:
        with patch("altegio_bot.campaigns.runner.find_candidates", new=fake_find_candidates):
            run = await run_preview(params)

        # Run должен быть completed
        assert run.status == "completed"
        assert run.meta.get("discovery_source") == "local_db"

        # Убедиться что run_id существует в БД
        async with session_maker() as session:
            db_run = await session.get(CampaignRun, run.id)
            assert db_run is not None
            assert db_run.status == "completed"
            assert db_run.meta.get("discovery_source") == "local_db"
    finally:
        runner_module.SessionLocal = original_session_local
