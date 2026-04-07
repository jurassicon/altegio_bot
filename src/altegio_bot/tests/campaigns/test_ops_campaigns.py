"""Tests for the ops campaign HTML routes (/ops/campaigns/*)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun
from altegio_bot.ops.auth import require_ops_auth


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
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


# ---------------------------------------------------------------------------
# /ops/campaigns/new-clients  – страница запуска кампании
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_new_clients_page_returns_200(http_client: AsyncClient) -> None:
    """Страница запуска кампании открывается без ошибок."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "New Clients Campaign" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_default_period_is_last_month(
    http_client: AsyncClient,
) -> None:
    """По умолчанию форма заполнена прошлым месяцем."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_prev = first_of_this_month - timedelta(days=1)
    first_of_prev = last_day_prev.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    expected_start = first_of_prev.strftime("%Y-%m-%d")
    expected_end = last_day_prev.strftime("%Y-%m-%d")

    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert expected_start in response.text, f"Ожидался period_start={expected_start}"
    assert expected_end in response.text, f"Ожидался period_end={expected_end}"


@pytest.mark.asyncio
async def test_ops_new_clients_page_shows_company_names(http_client: AsyncClient) -> None:
    """На странице отображаются human-readable названия компаний."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "Karlsruhe" in response.text
    assert "Rastatt" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_shows_template_block(http_client: AsyncClient) -> None:
    """На странице есть read-only блок с информацией о Meta-шаблоне."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "kitilash_ka_newsletter_new_clients_monthly_v2" in response.text
    assert "newsletter_new_clients_monthly" in response.text
    # Язык шаблона
    assert "de" in response.text
    # Блок помечен как только для просмотра
    assert "только для просмотра" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_company_template_mapping(http_client: AsyncClient) -> None:
    """JS-маппинг COMPANY_TEMPLATES содержит template_name для каждой компании."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    # Маппинг присутствует в JS
    assert "COMPANY_TEMPLATES" in text
    # Karlsruhe и Rastatt имеют template в маппинге
    assert "758285" in text
    assert "1271200" in text
    # Для Karlsruhe — KA-шаблон
    assert "kitilash_ka_newsletter_new_clients_monthly_v2" in text
    # loadTemplateText принимает companyId из select
    assert "loadTemplateText(companySelect.value)" in text


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_preview_button(http_client: AsyncClient) -> None:
    """На странице есть кнопка Create Preview."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "Create Preview" in response.text
    assert "btn-preview" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_run_button(http_client: AsyncClient) -> None:
    """На странице есть кнопка Run Campaign (изначально disabled)."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "Run Campaign" in response.text
    assert "btn-run" in response.text
    # Кнопка disabled до preview
    assert "disabled" in response.text


@pytest.mark.asyncio
async def test_ops_campaigns_list_has_new_clients_button(http_client: AsyncClient) -> None:
    """Список кампаний содержит кнопку перехода к запуску кампании новых клиентов."""
    response = await http_client.get("/ops/campaigns")
    assert response.status_code == 200
    assert "New Clients Campaign" in response.text
    assert "/ops/campaigns/new-clients" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_recipients_section(http_client: AsyncClient) -> None:
    """На странице есть секция для отображения получателей после preview."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "Только eligible" in response.text
    assert "Все записи" in response.text
    assert "recipients-table" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_progress_section(http_client: AsyncClient) -> None:
    """На странице есть секция прогресса (скрытая до запуска)."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "progress-section" in response.text
    assert "progress-bar" in response.text
    assert "pollProgress" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_run_uses_source_preview_run_id(
    http_client: AsyncClient,
) -> None:
    """JavaScript-код содержит передачу source_preview_run_id в payload."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    assert "source_preview_run_id" in response.text
    assert "previewRunId" in response.text


# ---------------------------------------------------------------------------
# /ops/campaigns/new-clients/card-types  – JSON endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_card_types_endpoint_returns_data(
    http_client: AsyncClient,
    monkeypatch,
) -> None:
    """Endpoint возвращает список типов карт от Altegio."""
    from altegio_bot.altegio_loyalty import AltegioLoyaltyClient

    fake_types = [
        {"id": 46657, "title": "Kundenkarte - 10 %"},
        {"id": 46658, "title": "Kundenkarte - 20 %"},
    ]

    async def mock_get_card_types(self, location_id: int):
        return fake_types

    async def mock_aclose(self):
        pass

    monkeypatch.setattr(AltegioLoyaltyClient, "get_card_types", mock_get_card_types)
    monkeypatch.setattr(AltegioLoyaltyClient, "aclose", mock_aclose)

    response = await http_client.get("/ops/campaigns/new-clients/card-types?location_id=758285")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["title"] == "Kundenkarte - 10 %"
    assert data[0]["id"] == 46657


@pytest.mark.asyncio
async def test_card_types_endpoint_requires_location_id(
    http_client: AsyncClient,
) -> None:
    """Endpoint возвращает 422, если location_id не передан."""
    response = await http_client.get("/ops/campaigns/new-clients/card-types")
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Follow-up поля на странице
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_followup_fields(http_client: AsyncClient) -> None:
    """На странице есть follow-up поля: checkbox, delay, policy, template."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    assert "f-followup-enabled" in text
    assert "f-followup-delay" in text
    assert "f-followup-policy" in text
    assert "f-followup-template" in text
    # Follow-up включён checkbox
    assert "Follow-up включён" in text


@pytest.mark.asyncio
async def test_ops_new_clients_buildpayload_contains_followup(http_client: AsyncClient) -> None:
    """buildPayload() в JS содержит followup_enabled, followup_delay_days, followup_policy, followup_template_name."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    assert "followup_enabled" in text
    assert "followup_delay_days" in text
    assert "followup_policy" in text
    assert "followup_template_name" in text


# ---------------------------------------------------------------------------
# Location — human-readable select
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_location_select(http_client: AsyncClient) -> None:
    """Location теперь human-readable select, а не numeric input."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    # Это select, а не input type=number
    assert 'id="f-location"' in text
    assert "form-select" in text.split('id="f-location"')[1][:200]
    # Human-readable названия филиалов
    assert "Karlsruhe (location_id=758285)" in text
    assert "Rastatt (location_id=1271200)" in text


# ---------------------------------------------------------------------------
# Template text блок (с AJAX загрузкой)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_template_text_block(http_client: AsyncClient) -> None:
    """На странице есть блок для загрузки реального текста шаблона из БД."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    assert "template-text-block" in text
    assert "loadTemplateText" in text
    # id для динамического обновления имени шаблона
    assert "template-name-display" in text
    # Должен быть fallback alert текст в JS
    assert "Не удалось загрузить текст шаблона из Meta" in text


@pytest.mark.asyncio
async def test_template_text_endpoint_returns_data(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Endpoint /template-text возвращает данные шаблона из БД (KA, с company_id)."""
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            tpl = MessageTemplate(
                company_id=758285,
                code="newsletter_new_clients_monthly",
                language="de",
                body="Hallo {{1}}, buche jetzt: {{2}}. Deine Karte: {{3}}",
                is_active=True,
            )
            session.add(tpl)

    response = await http_client.get(
        "/ops/campaigns/new-clients/template-text"
        "?template_name=kitilash_ka_newsletter_new_clients_monthly_v2"
        "&company_id=758285"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["language"] == "de"
    assert "Hallo" in data["body"]
    assert data["code"] == "newsletter_new_clients_monthly"


@pytest.mark.asyncio
async def test_template_text_endpoint_returns_404_if_not_found(
    http_client: AsyncClient,
) -> None:
    """Endpoint /template-text возвращает 404 если шаблон не найден."""
    response = await http_client.get("/ops/campaigns/new-clients/template-text?template_name=nonexistent_template")
    assert response.status_code == 404
    data = response.json()
    assert "не найден" in data["detail"]


# ---------------------------------------------------------------------------
# template-text: per-company детерминированный поиск
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_template_text_endpoint_karlsruhe(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Endpoint возвращает шаблон для Karlsruhe (company_id=758285)."""
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            session.add(
                MessageTemplate(
                    company_id=758285,
                    code="newsletter_new_clients_monthly",
                    language="de",
                    body="KA: Hallo {{1}}",
                    is_active=True,
                )
            )

    response = await http_client.get(
        "/ops/campaigns/new-clients/template-text"
        "?template_name=kitilash_ka_newsletter_new_clients_monthly_v2"
        "&company_id=758285"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == "newsletter_new_clients_monthly"
    assert data["company_id"] == 758285
    assert "KA:" in data["body"]


@pytest.mark.asyncio
async def test_template_text_endpoint_rastatt_falls_back_to_ka(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Rastatt использует KA-шаблон. Endpoint находит его через fallback."""
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            # Только KA-запись в БД (Rastatt uses KA template per META_TEMPLATE_MAP)
            session.add(
                MessageTemplate(
                    company_id=758285,
                    code="newsletter_new_clients_monthly",
                    language="de",
                    body="Universal: Hallo {{1}}",
                    is_active=True,
                )
            )

    # RA campaign company, но template_name — KA (оба используют kitilash_ka_... шаблон)
    response = await http_client.get(
        "/ops/campaigns/new-clients/template-text"
        "?template_name=kitilash_ka_newsletter_new_clients_monthly_v2"
        "&company_id=1271200"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["code"] == "newsletter_new_clients_monthly"
    # Нашли KA-запись через fallback (RA не имеет своей записи с этим кодом)
    assert data["company_id"] == 758285
    assert "Universal" in data["body"]


@pytest.mark.asyncio
async def test_template_text_no_substring_ambiguity(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Exact match: запрос на newsletter_new_clients_monthly не захватывает newsletter_new_clients."""
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            # Короткий код — потенциальная ловушка substring matching
            session.add(
                MessageTemplate(
                    company_id=758285,
                    code="newsletter_new_clients",
                    language="de",
                    body="SHORT: body without monthly",
                    is_active=True,
                )
            )
            # Полный код — именно его ищем
            session.add(
                MessageTemplate(
                    company_id=758285,
                    code="newsletter_new_clients_monthly",
                    language="de",
                    body="FULL: body with monthly",
                    is_active=True,
                )
            )

    response = await http_client.get(
        "/ops/campaigns/new-clients/template-text?template_name=kitilash_ka_newsletter_new_clients_monthly_v2"
    )
    assert response.status_code == 200
    data = response.json()
    # Строгий exact match: должен вернуть именно newsletter_new_clients_monthly
    assert data["code"] == "newsletter_new_clients_monthly"
    assert "FULL" in data["body"]
    assert "SHORT" not in data["body"]


# ---------------------------------------------------------------------------
# Excluded breakdown после preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_excluded_breakdown_section(
    http_client: AsyncClient,
) -> None:
    """На странице есть секция excluded breakdown после preview."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    assert "excluded-breakdown" in text
    # JS содержит рендеринг excluded reasons
    assert "opted_out" in text
    assert "no_phone" in text
    assert "invalid_phone" in text
    assert "no_whatsapp" in text
    assert "multiple_records_in_period" in text
    assert "no_confirmed_record_in_period" in text
    assert "has_records_before_period" in text
    assert "Причины исключения" in text
