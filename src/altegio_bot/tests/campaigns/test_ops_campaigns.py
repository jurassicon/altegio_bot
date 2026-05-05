"""Tests for the ops campaign HTML routes (/ops/campaigns/*)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun, MessageJob
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
    assert "kitilash_ka_newsletter_new_clients_monthly_v1" in response.text
    assert "newsletter_new_clients_monthly" in response.text
    # Язык шаблона
    assert "de" in response.text
    # Блок помечен как только для просмотра
    assert "только для просмотра" in response.text


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_company_template_mapping(http_client: AsyncClient) -> None:
    """JS-маппинг COMPANY_TEMPLATES содержит template_name для каждой компании."""
    import json
    import re

    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    # Маппинг присутствует в JS
    assert "COMPANY_TEMPLATES" in text
    # Извлекаем JSON из строки "const COMPANY_TEMPLATES = {...};"
    m = re.search(r"const COMPANY_TEMPLATES = (\{.*?\});", text)
    assert m is not None, "COMPANY_TEMPLATES JSON не найден в тексте страницы"
    mapping = json.loads(m.group(1))
    # Оба company_id обязаны быть ключами маппинга с непустым template_name
    assert "758285" in mapping, "Karlsruhe (758285) отсутствует в COMPANY_TEMPLATES"
    assert "1271200" in mapping, "Rastatt (1271200) отсутствует в COMPANY_TEMPLATES"
    assert mapping["758285"], "template_name для Karlsruhe пустой"
    assert mapping["1271200"], "template_name для Rastatt пустой"
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
# Company / филиал select (единый, заменяет отдельный location)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ops_new_clients_page_has_company_select(http_client: AsyncClient) -> None:
    """Один dropdown id="f-company" выбирает филиал (company_id = location_id)."""
    response = await http_client.get("/ops/campaigns/new-clients")
    assert response.status_code == 200
    text = response.text
    # Единый select для кабинета/филиала
    assert 'id="f-company"' in text
    assert "form-select" in text.split('id="f-company"')[1][:200]
    # Отдельного f-location быть не должно
    assert 'id="f-location"' not in text
    # Опции содержат человекочитаемые названия филиалов
    assert "Karlsruhe" in text
    assert "Rastatt" in text


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
        "?template_name=kitilash_ka_newsletter_new_clients_monthly_v1"
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
        "?template_name=kitilash_ka_newsletter_new_clients_monthly_v1"
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
        "?template_name=kitilash_ka_newsletter_new_clients_monthly_v1"
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
        "/ops/campaigns/new-clients/template-text?template_name=kitilash_ka_newsletter_new_clients_monthly_v1"
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


# ---------------------------------------------------------------------------
# POST /ops/campaigns/runs/{run_id}/recompute
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def stale_send_real_run(session_maker) -> int:
    """Production-realistic run with stale counters.

    Reproduces the actual production path:
    - CampaignRecipient has message_job_id set (job was created)
    - outbox_message_id is NULL (back-reference never filled by worker)
    - OutboxMessage exists only via job_id FK
    - CampaignRun counters are stale (queued_count=0)

    recompute должен найти outbox через job_id, заполнить ссылку,
    обновить счётчики и синхронизировать статус получателя.
    """
    from altegio_bot.models.models import OutboxMessage

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
                # Stale counters — queued=0 even though one message was sent
                total_clients_seen=3,
                candidates_count=1,
                queued_count=0,
                sent_count=0,
                provider_accepted_count=0,
                delivered_count=0,
                read_count=0,
                cards_issued_count=0,
                cleanup_failed_count=0,
                cards_deleted_count=0,
                followup_enabled=False,
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

            # Create the MessageJob that triggered the send
            job = MessageJob(
                company_id=758285,
                job_type="newsletter_new_clients_monthly",
                run_at=now,
                status="done",
                dedupe_key=f"test-recompute-stale-{run_id}",
                payload={
                    "campaign_run_id": run_id,
                    "campaign_recipient_id": 0,  # patched below
                },
            )
            session.add(job)
            await session.flush()
            job_id = job.id

            # OutboxMessage linked via job_id ONLY — no back-ref on recipient
            outbox = OutboxMessage(
                company_id=758285,
                job_id=job_id,
                phone_e164="+49151000001",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test body",
                status="sent",
                scheduled_at=now,
            )
            session.add(outbox)

            # Excluded recipient (opted_out)
            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    altegio_client_id=10,
                    phone_e164="+49151000000",
                    display_name="Opted Out Client",
                    status="skipped",
                    excluded_reason="opted_out",
                )
            )
            # Excluded recipient (no_phone)
            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    altegio_client_id=11,
                    phone_e164=None,
                    display_name="No Phone Client",
                    status="skipped",
                    excluded_reason="no_phone",
                )
            )
            # Eligible recipient: has message_job_id but outbox_message_id=NULL
            # This is the real production gap we're testing.
            eligible = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=758285,
                altegio_client_id=12,
                phone_e164="+49151000001",
                display_name="Success Client",
                status="queued",
                loyalty_card_id="CARD-001",
                message_job_id=job_id,
                outbox_message_id=None,  # gap: never backfilled
            )
            session.add(eligible)

    return run_id


@pytest.mark.asyncio
async def test_recompute_endpoint_returns_200(
    http_client: AsyncClient,
    stale_send_real_run: int,
) -> None:
    """POST /recompute возвращает 200 с JSON summary."""
    run_id = stale_send_real_run
    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200
    data = response.json()
    assert data["recomputed"] is True
    assert data["run_id"] == run_id
    assert "stats" in data


@pytest.mark.asyncio
async def test_recompute_fixes_stale_counters(
    http_client: AsyncClient,
    stale_send_real_run: int,
    session_maker,
) -> None:
    """После recompute счётчики в БД соответствуют реальному состоянию.

    Production regression scenario:
    - CampaignRecipient has message_job_id, outbox_message_id=NULL
    - OutboxMessage exists via job_id only
    - recompute должен найти outbox и выставить queued_count=1
    """
    run_id = stale_send_real_run

    # Counters are stale before recompute
    async with session_maker() as session:
        run_before = await session.get(CampaignRun, run_id)
    assert run_before.queued_count == 0

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    async with session_maker() as session:
        run_after = await session.get(CampaignRun, run_id)

    assert run_after.queued_count == 1, "queued_count должен стать 1 после recompute"
    assert run_after.sent_count == 1
    assert run_after.provider_accepted_count == 1  # outbox.status='sent'
    assert run_after.cards_issued_count == 1
    assert run_after.candidates_count == 1
    assert run_after.total_clients_seen == 3
    assert run_after.excluded_opted_out == 1
    assert run_after.excluded_no_phone == 1


@pytest.mark.asyncio
async def test_recompute_backfills_outbox_link_via_job_id(
    http_client: AsyncClient,
    stale_send_real_run: int,
    session_maker,
) -> None:
    """recompute заполняет outbox_message_id на recipient через job_id.

    Это основной regression test для production gap: outbox_worker создаёт
    OutboxMessage с job_id но не записывает обратную ссылку в recipient.
    После recompute recipient должен иметь outbox_message_id заполненным.
    """
    from altegio_bot.models.models import OutboxMessage

    run_id = stale_send_real_run

    # Verify precondition: eligible recipient has no outbox_message_id
    async with session_maker() as session:
        stmt = (
            select(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.message_job_id.is_not(None))
        )
        recip_before = (await session.execute(stmt)).scalars().first()
        assert recip_before is not None
        assert recip_before.outbox_message_id is None, "precondition: outbox_message_id should be NULL before recompute"

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    # After recompute: outbox_message_id must be backfilled
    async with session_maker() as session:
        stmt = (
            select(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.message_job_id.is_not(None))
        )
        recip_after = (await session.execute(stmt)).scalars().first()
        assert recip_after is not None
        assert recip_after.outbox_message_id is not None, "outbox_message_id should be backfilled by recompute"
        # Verify the linked outbox is real
        ob = await session.get(OutboxMessage, recip_after.outbox_message_id)
        assert ob is not None
        assert ob.status == "sent"


@pytest.mark.asyncio
async def test_recompute_syncs_recipient_status_from_outbox(
    http_client: AsyncClient,
    stale_send_real_run: int,
    session_maker,
) -> None:
    """После recompute статус получателя синхронизируется с outbox.

    Variant A: если outbox.status='sent', recipient.status должен стать
    'provider_accepted' (не оставаться 'queued').
    Это исправляет misleading recipients summary на detail page.
    """
    run_id = stale_send_real_run

    # Precondition: recipient status is 'queued'
    async with session_maker() as session:
        stmt = (
            select(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.message_job_id.is_not(None))
        )
        r = (await session.execute(stmt)).scalars().first()
        assert r is not None
        assert r.status == "queued"

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    # After recompute: status should be 'provider_accepted' (outbox='sent')
    async with session_maker() as session:
        stmt = (
            select(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.message_job_id.is_not(None))
        )
        r_after = (await session.execute(stmt)).scalars().first()
        assert r_after is not None

    assert r_after.status == "provider_accepted", (
        "recipient status должен быть 'provider_accepted' после recompute когда outbox.status='sent'"
    )


@pytest.mark.asyncio
async def test_recompute_delivery_counters_from_outbox(
    http_client: AsyncClient,
    stale_send_real_run: int,
    session_maker,
) -> None:
    """provider_accepted, delivered, read берутся из OutboxMessage.status."""
    from altegio_bot.models.models import OutboxMessage

    run_id = stale_send_real_run

    # Update the outbox to 'read' via job_id lookup (production path)
    async with session_maker() as session:
        async with session.begin():
            stmt = (
                select(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.message_job_id.is_not(None))
            )
            recip = (await session.execute(stmt)).scalars().first()
            assert recip is not None
            # find outbox via job_id (production path)
            ob_stmt = (
                select(OutboxMessage)
                .where(OutboxMessage.job_id == recip.message_job_id)
                .order_by(OutboxMessage.id.desc())
                .limit(1)
            )
            ob = (await session.execute(ob_stmt)).scalars().first()
            assert ob is not None
            ob.status = "read"

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    # Cumulative: read => also delivered and provider_accepted
    assert run.read_count == 1
    assert run.delivered_count == 1
    assert run.provider_accepted_count == 1


@pytest.mark.asyncio
async def test_recompute_is_idempotent(
    http_client: AsyncClient,
    stale_send_real_run: int,
    session_maker,
) -> None:
    """Повторный вызов recompute даёт тот же результат."""
    run_id = stale_send_real_run

    resp1 = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    resp2 = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")

    assert resp1.status_code == 200
    assert resp2.status_code == 200

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.queued_count == 1
    assert run.candidates_count == 1


@pytest.mark.asyncio
async def test_recompute_not_found(http_client: AsyncClient) -> None:
    """POST /recompute для несуществующего run возвращает 404."""
    response = await http_client.post("/ops/campaigns/runs/9999999/recompute")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_recompute_button_present_for_send_real(
    http_client: AsyncClient,
    sample_run: int,
) -> None:
    """Кнопка Recompute stats доступна на detail page send-real run."""
    response = await http_client.get(f"/ops/campaigns/{sample_run}")
    assert response.status_code == 200
    assert "Recompute stats" in response.text
    assert "recomputeStats" in response.text


@pytest.mark.asyncio
async def test_recompute_button_disabled_for_preview(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Для preview run кнопка Recompute stats disabled."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert "Recompute stats" in response.text
    # Button must be disabled for preview
    assert "disabled" in response.text


@pytest.mark.asyncio
async def test_recompute_attribution_counters(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """replied / booked_after / opted_out_after берутся из CampaignRecipient timestamps."""
    from altegio_bot.models.models import OutboxMessage

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
                replied_count=0,
                booked_after_count=0,
                opted_out_after_count=0,
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

            outbox = OutboxMessage(
                company_id=758285,
                phone_e164="+49151000099",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test",
                status="read",
                scheduled_at=now,
            )
            session.add(outbox)
            await session.flush()

            session.add(
                CampaignRecipient(
                    campaign_run_id=run_id,
                    company_id=758285,
                    altegio_client_id=20,
                    phone_e164="+49151000099",
                    display_name="Attribution Client",
                    status="queued",
                    outbox_message_id=outbox.id,
                    replied_at=now,
                    booked_after_at=now,
                    opted_out_after_at=now,
                )
            )

    response = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")
    assert response.status_code == 200

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run.replied_count == 1
    assert run.booked_after_count == 1
    assert run.opted_out_after_count == 1


@pytest.mark.asyncio
async def test_recompute_warning_js_present_in_page(
    http_client: AsyncClient,
    sample_run: int,
) -> None:
    """JS warning handler for service lookup failures is present in the run detail page."""
    response = await http_client.get(f"/ops/campaigns/{sample_run}")
    assert response.status_code == 200
    assert "booked_after_service_lookup_failed_count" in response.text
    assert "alert-warning" in response.text
    assert "may be undercounted" in response.text


@pytest.mark.asyncio
async def test_persistent_recompute_warning_shown_when_last_recompute_has_failures(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Persistent server-rendered warning appears on page load when last_recompute has failures (Case A)."""
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
                followup_enabled=False,
                meta={
                    "last_recompute": {
                        "booked_after_service_lookup_failed_count": 2,
                        "booked_after_service_lookup_failed_service_ids": [111, 222],
                        "booked_after_service_lookup_failed_record_count": 3,
                        "booked_after_service_lookup_checked_at": "2026-05-05T10:00:00+00:00",
                    }
                },
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert "alert-warning" in response.text
    assert "Booked after может быть занижен" in response.text
    assert "111" in response.text
    assert "222" in response.text
    assert "3" in response.text


@pytest.mark.asyncio
async def test_persistent_recompute_warning_absent_when_last_recompute_clean(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Persistent warning is NOT shown when last_recompute has no failures (Case B)."""
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
                followup_enabled=False,
                meta={
                    "last_recompute": {
                        "booked_after_service_lookup_failed_count": 0,
                        "booked_after_service_lookup_failed_service_ids": [],
                        "booked_after_service_lookup_failed_record_count": 0,
                    }
                },
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert "Booked after может быть занижен" not in response.text


# ---------------------------------------------------------------------------
# Preview runs hidden from list / accessible via direct link
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_run_hidden_from_campaign_list(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Preview runs do not appear in the default campaign list; send-real runs do (Case A)."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            preview_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                followup_enabled=False,
                meta={},
            )
            send_real_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                followup_enabled=False,
                meta={},
            )
            session.add(preview_run)
            session.add(send_real_run)
            await session.flush()
            preview_id = preview_run.id
            send_real_id = send_real_run.id

    response = await http_client.get("/ops/campaigns")
    assert response.status_code == 200
    # Send-real run must appear (its detail link)
    assert f'href="/ops/campaigns/{send_real_id}"' in response.text
    # Preview run detail link must NOT appear in default list
    assert f'href="/ops/campaigns/{preview_id}"' not in response.text

    # Explicit mode=preview filter must still show it
    response_preview = await http_client.get("/ops/campaigns?mode=preview")
    assert response_preview.status_code == 200
    assert f'href="/ops/campaigns/{preview_id}"' in response_preview.text


@pytest.mark.asyncio
async def test_preview_run_detail_accessible_via_direct_link(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Preview run detail page is accessible by direct URL even though hidden from list (Case B)."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                followup_enabled=False,
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert f"Run #{run_id}" in response.text
    assert "превью" in response.text.lower()


@pytest.mark.asyncio
async def test_send_real_detail_links_to_source_preview(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Send-real run detail shows link to its source preview run (Case C)."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            preview_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                followup_enabled=False,
                meta={},
            )
            session.add(preview_run)
            await session.flush()
            preview_id = preview_run.id

            send_real_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[758285],
                period_start=now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                period_end=now,
                status="completed",
                source_preview_run_id=preview_id,
                followup_enabled=False,
                meta={},
            )
            session.add(send_real_run)
            await session.flush()
            send_real_id = send_real_run.id

    response = await http_client.get(f"/ops/campaigns/{send_real_id}")
    assert response.status_code == 200
    assert f"/ops/campaigns/{preview_id}" in response.text
    assert "превью" in response.text.lower()


@pytest.mark.asyncio
async def test_send_real_without_preview_shows_no_empty_preview_block(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Send-real run without source_preview_run_id shows no broken or empty preview link (Case D)."""
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
                source_preview_run_id=None,
                followup_enabled=False,
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

    response = await http_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert "Открыть превью" not in response.text
    assert "Связанное превью" not in response.text
