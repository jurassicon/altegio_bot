"""Интеграционные тесты: CRM-only clients в полном pipeline preview → send-real.

Сценарии:
  A. Preview с CRM-only eligible client (phone_raw fallback) → eligible.
  B. Send-real from preview snapshot: CRM-only eligible → queued (не no_local_client).
  C. Fresh send-real: CRM-only eligible → queued.
  D. CRM-only client с client.id=None не silently skipped.
  E. discovery_source = "crm_api" в preview и fresh send-real.
  F. no_local_client НЕ назначается CRM-only eligible preview recipient при snapshot load.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

import altegio_bot.campaigns.runner as runner_module
from altegio_bot.campaigns.runner import RunParams, run_preview, run_send_real
from altegio_bot.campaigns.segment import ClientCandidate, ClientSnapshot
from altegio_bot.models.models import CampaignRecipient, CampaignRun

COMPANY = 758285
LOCATION = 1
CARD_TYPE = "type-crm"
PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)

LASH_SVC_ID = 99001


# ---------------------------------------------------------------------------
# Вспомогательные объекты и моки
# ---------------------------------------------------------------------------


class _MockLoyalty:
    """Заглушка loyalty-клиента для интеграционных тестов."""

    def __init__(self) -> None:
        self.issued: list[dict] = []

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(self, location_id, *, loyalty_card_number, loyalty_card_type_id, phone):
        self.issued.append({"number": loyalty_card_number, "phone": phone})
        return {"loyalty_card_number": loyalty_card_number, "id": "mock-card-id"}

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


def _crm_only_candidate(*, phone_e164: str | None = "+4915123456789") -> ClientCandidate:
    """ClientCandidate для CRM-only eligible клиента (без local client.id)."""
    snapshot = ClientSnapshot(
        id=None,
        company_id=COMPANY,
        altegio_client_id=99999,
        display_name="Lalka Marinova",
        phone_e164=phone_e164,
        wa_opted_out=False,
    )
    return ClientCandidate(
        client=snapshot,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=["Wimpernverlängerung"],
        records_before_period=0,
        local_client_found=False,
        excluded_reason=None,  # eligible
    )


def _make_params(*, source_preview_run_id: int | None = None, mode: str = "send-real") -> RunParams:
    return RunParams(
        company_id=COMPANY,
        location_id=LOCATION,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        mode=mode,
        card_type_id=CARD_TYPE,
        source_preview_run_id=source_preview_run_id,
    )


@pytest_asyncio.fixture
def runner_with_mock_loyalty(session_maker, monkeypatch) -> _MockLoyalty:
    """Подключить тестовую БД и mock loyalty к runner_module."""
    loyalty = _MockLoyalty()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


# ---------------------------------------------------------------------------
# Тест A: Preview с CRM-only eligible client → eligible в результатах
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_crm_only_eligible(runner_with_mock_loyalty, session_maker, monkeypatch) -> None:
    """Preview возвращает CRM-only eligible client с phone_e164 из phone_raw fallback.

    Сценарий: клиент не в локальной БД, CRM discovery возвращает phone_raw,
    после нормализации клиент становится eligible.
    """
    candidate = _crm_only_candidate(phone_e164="+4915123456789")

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_preview(_make_params(mode="preview"))

    assert run.status == "completed"
    assert run.candidates_count == 1
    assert run.excluded_no_phone == 0

    # Проверяем сохранённые recipients
    async with session_maker() as session:
        from sqlalchemy import select

        recipients = (
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )

    assert len(recipients) == 1
    r = recipients[0]
    assert r.client_id is None, "CRM-only: client_id в recipient должен быть None"
    assert r.phone_e164 == "+4915123456789", "phone из CRM fallback должен быть сохранён"
    assert r.status == "candidate", f"CRM-only eligible должен быть candidate, не {r.status!r}"
    assert r.excluded_reason is None
    assert r.local_client_found is False


# ---------------------------------------------------------------------------
# Тест B: Send-real from preview snapshot: CRM-only eligible → queued
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_real_from_preview_snapshot_crm_only(runner_with_mock_loyalty, session_maker, monkeypatch) -> None:
    """Send-real из preview snapshot корректно обрабатывает CRM-only eligible client.

    Проблема до фикса: _load_candidates_from_preview_snapshot назначал no_local_client
    для recipient с client_id=None, даже если excluded_reason был None (eligible).
    После фикса: excluded_reason сохраняется, клиент доходит до _process_eligible.
    """
    loyalty = runner_with_mock_loyalty

    # Создать preview run с CRM-only eligible recipient вручную
    async with session_maker() as session:
        async with session.begin():
            preview_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                candidates_count=1,
            )
            session.add(preview_run)
            await session.flush()
            preview_run_id = preview_run.id

            # CRM-only eligible recipient: client_id=None, phone_e164 из CRM fallback
            recipient = CampaignRecipient(
                campaign_run_id=preview_run_id,
                company_id=COMPANY,
                client_id=None,  # CRM-only
                altegio_client_id=99999,
                phone_e164="+4915123456789",
                display_name="Lalka Marinova",
                local_client_found=False,
                total_records_in_period=1,
                confirmed_records_in_period=1,
                lash_records_in_period=1,
                confirmed_lash_records_in_period=1,
                records_before_period=0,
                service_titles_in_period=["Wimpernverlängerung"],
                status="candidate",
                excluded_reason=None,  # eligible
            )
            session.add(recipient)

    # Send-real из этого preview snapshot
    params = _make_params(source_preview_run_id=preview_run_id)

    run = await run_send_real(params)

    assert run.status == "completed", f"Run должен быть completed, но: {run.status}"
    assert run.queued_count == 1, (
        f"CRM-only eligible client должен быть queued, а не пропущен. queued={run.queued_count}"
    )
    assert run.failed_count == 0

    # Проверяем recipient в send-real run
    async with session_maker() as session:
        from sqlalchemy import select

        send_real_recipients = (
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )

    assert len(send_real_recipients) == 1
    r = send_real_recipients[0]
    assert r.status == "queued", f"Ожидали queued, получили {r.status!r}"
    assert r.excluded_reason not in ("no_local_client", "no_phone"), (
        f"CRM-only eligible не должен быть excluded с {r.excluded_reason!r}"
    )
    assert len(loyalty.issued) == 1
    issued_phone = loyalty.issued[0]["phone"]
    assert issued_phone == 4915123456789, (
        f"Loyalty card должен быть выпущен на правильный номер, получили {issued_phone}"
    )


# ---------------------------------------------------------------------------
# Тест C: Fresh send-real с CRM-only eligible client → queued
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_send_real_crm_only_eligible(runner_with_mock_loyalty, session_maker, monkeypatch) -> None:
    """Свежий send-real (без preview snapshot) с CRM-only eligible client → queued.

    Проверяет полный pipeline: find_candidates возвращает CRM-only eligible,
    _process_eligible обрабатывает его без ошибки из-за client.id=None.
    """
    loyalty = runner_with_mock_loyalty
    candidate = _crm_only_candidate(phone_e164="+4915987654321")

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.status == "completed", f"Run должен быть completed: {run.status}"
    assert run.queued_count == 1, (
        f"CRM-only eligible должен быть queued. queued={run.queued_count}, failed={run.failed_count}"
    )
    assert run.failed_count == 0

    assert len(loyalty.issued) == 1


# ---------------------------------------------------------------------------
# Тест D: CRM-only eligible не silently skipped при client.id=None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crm_only_not_silently_skipped(runner_with_mock_loyalty, session_maker, monkeypatch) -> None:
    """CRM-only eligible client (id=None) попадает в queued, а не в failed и не пропускается.

    До фикса: guard в _process_eligible делал stats['failed'] += 1 и return.
    После фикса: guard удалён, loyalty.issue_card вызывается.
    """
    loyalty = runner_with_mock_loyalty
    candidate = _crm_only_candidate(phone_e164="+4915111111111")

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    assert run.failed_count == 0, "CRM-only eligible НЕ должен попасть в failed (старый guard был удалён)"
    assert run.queued_count == 1
    assert len(loyalty.issued) == 1, "loyalty.issue_card должен быть вызван для CRM-only клиента"


# ---------------------------------------------------------------------------
# Тест E: discovery_source = "crm_api" в preview и fresh send-real
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discovery_source_crm_api_in_preview(runner_with_mock_loyalty, session_maker, monkeypatch) -> None:
    """Preview сохраняет discovery_source='crm_api' в meta (не 'local_db')."""
    candidate = _crm_only_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_preview(_make_params(mode="preview"))

    assert run.meta is not None
    assert run.meta.get("discovery_source") == "crm_api", (
        f"Ожидали discovery_source='crm_api', получили {run.meta.get('discovery_source')!r}"
    )


@pytest.mark.asyncio
async def test_discovery_source_crm_api_in_fresh_send_real(
    runner_with_mock_loyalty, session_maker, monkeypatch
) -> None:
    """Fresh send-real сохраняет discovery_source='crm_api' в meta (не 'local_db')."""
    candidate = _crm_only_candidate()

    with patch.object(runner_module, "find_candidates", return_value=[candidate]):
        run = await run_send_real(_make_params())

    async with session_maker() as session:
        db_run = await session.get(CampaignRun, run.id)

    assert db_run.meta is not None
    assert db_run.meta.get("discovery_source") == "crm_api", (
        f"Ожидали discovery_source='crm_api', получили {db_run.meta.get('discovery_source')!r}"
    )


# ---------------------------------------------------------------------------
# Тест F: no_local_client НЕ назначается CRM-only eligible при snapshot load
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_local_client_not_assigned_to_crm_only_eligible(
    runner_with_mock_loyalty, session_maker, monkeypatch
) -> None:
    """_load_candidates_from_preview_snapshot не назначает no_local_client CRM-only eligible.

    Регрессионный тест на конкретный баг:
    - CRM-only eligible имеет recipient.client_id=None и excluded_reason=None.
    - Старая логика: excluded = r.excluded_reason or 'no_local_client' = 'no_local_client'.
    - Новая логика: проверяем r.client_id is None → сохраняем excluded_reason=None.
    """
    from altegio_bot.campaigns.runner import _load_candidates_from_preview_snapshot

    async with session_maker() as session:
        async with session.begin():
            preview_run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
            )
            session.add(preview_run)
            await session.flush()
            preview_run_id = preview_run.id

            # CRM-only eligible: client_id=None, excluded_reason=None
            r = CampaignRecipient(
                campaign_run_id=preview_run_id,
                company_id=COMPANY,
                client_id=None,
                altegio_client_id=99999,
                phone_e164="+4915123456789",
                display_name="Lalka Marinova",
                local_client_found=False,
                total_records_in_period=1,
                confirmed_records_in_period=1,
                lash_records_in_period=1,
                confirmed_lash_records_in_period=1,
                records_before_period=0,
                service_titles_in_period=[],
                status="candidate",
                excluded_reason=None,
            )
            session.add(r)

    async with session_maker() as session:
        candidates = await _load_candidates_from_preview_snapshot(session, preview_run_id)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.excluded_reason is None, (
        f"CRM-only eligible не должен получить excluded_reason, а получил: {c.excluded_reason!r}"
    )
    assert c.is_eligible, "CRM-only eligible из snapshot должен оставаться eligible"
    assert c.client.id is None, "Snapshot id должен быть None для CRM-only"
    assert c.client.phone_e164 == "+4915123456789", "phone из snapshot должен сохраниться"
    assert c.local_client_found is False
