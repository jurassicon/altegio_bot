"""Тесты модуля reports.py.

Сценарии:
  1. Preview-runs НЕ попадают в monthly_dashboard (только mode='send-real').
  2. Кумулятивная воронка — один OutboxMessage 'read':
       _fetch_attribution возвращает provider_accepted=1, delivered=1, read=1.
  3. Смешанные статусы (sent + delivered + read):
       _fetch_attribution возвращает provider_accepted=3, delivered=2, read=1.
  4. run_report() использует счётчики CampaignRun (is not None, а не falsy or).
  5. Фильтр по company_id в monthly_dashboard через SQL JSONB-containment.
  6. run_report() для несуществующего run_id выбрасывает ValueError.
  7. monthly_dashboard() без явного company_ids использует COMPANIES по умолчанию.
  8. Тест на company filter: явно проверяется что чужой филиал не попал в totals.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from altegio_bot.campaigns.reports import (
    COMPANIES,
    _fetch_attribution,
    monthly_dashboard,
    run_report,
)
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    OutboxMessage,
)

COMPANY = 758285
YEAR = 2026
MONTH = 1
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _make_run(session, *, mode: str, status: str = "completed", **kw) -> CampaignRun:
    """Создать и добавить CampaignRun в сессию (не flush)."""
    defaults = dict(
        campaign_code="new_clients_monthly",
        mode=mode,
        status=status,
        company_ids=[COMPANY],
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        total_clients_seen=0,
        candidates_count=0,
    )
    defaults.update(kw)
    run = CampaignRun(**defaults)
    session.add(run)
    return run


async def _add_outbox_with_recipient(
    session,
    *,
    msg_status: str,
    run: CampaignRun,
    phone_suffix: str = "0099",
) -> OutboxMessage:
    """Создать OutboxMessage + CampaignRecipient, привязанный к run."""
    msg = OutboxMessage(
        company_id=COMPANY,
        status=msg_status,
        template_code="new_clients",
        phone_e164=f"+49123456{phone_suffix}",
        body="",
        scheduled_at=PERIOD_START,
    )
    session.add(msg)
    await session.flush()

    recipient = CampaignRecipient(
        campaign_run_id=run.id,
        company_id=COMPANY,
        status=msg_status,
        outbox_message_id=msg.id,
    )
    session.add(recipient)
    return msg


# ---------------------------------------------------------------------------
# Тест 1: preview-runs не в monthly_dashboard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_excluded_from_monthly_dashboard(session_maker) -> None:
    """monthly_dashboard должен возвращать только mode='send-real' runs."""
    async with session_maker() as session:
        async with session.begin():
            _make_run(session, mode="preview")
            _make_run(session, mode="send-real", queued_count=5)

        result = await monthly_dashboard(
            session,
            year=YEAR,
            month=MONTH,
            company_ids=[COMPANY],
        )

    company_data = next(c for c in result["companies"] if c["company_id"] == COMPANY)
    # Только один send-real run
    assert company_data["runs_count"] == 1
    # queued_count из send-real run
    assert company_data["totals"]["queued"] == 5


# ---------------------------------------------------------------------------
# Тест 2: кумулятивная воронка — один OutboxMessage со статусом 'read'
#
# Тестируем _fetch_attribution напрямую, потому что поля CampaignRun
# (provider_accepted_count и др.) — nullable=False int с server_default=0.
# После flush они всегда int (никогда не None), поэтому fallback на attr
# в run_report() корректно не срабатывает для существующих счётчиков.
# Логика кумулятивного подсчёта живёт именно в _fetch_attribution.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_attribution_read_counted_cumulatively(session_maker) -> None:
    """Статус 'read' у OutboxMessage учитывается в delivered и provider_accepted.

    Точные ожидаемые значения:
      provider_accepted == 1
      delivered == 1
      read == 1
    """
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, mode="send-real")
            await session.flush()
            await _add_outbox_with_recipient(session, msg_status="read", run=run)

        attr = await _fetch_attribution(session, run.id)

    assert attr["read"] == 1, f"read должен быть 1, получили {attr['read']}"
    assert attr["delivered"] == 1, f"delivered должен быть 1, получили {attr['delivered']}"
    assert attr["provider_accepted"] == 1, f"provider_accepted должен быть 1, получили {attr['provider_accepted']}"


# ---------------------------------------------------------------------------
# Тест 3: смешанные статусы — sent + delivered + read
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_attribution_mixed_statuses(session_maker) -> None:
    """Смешанные статусы OutboxMessage считаются корректно в кумулятивной воронке.

    Три сообщения:
      - один 'sent'      → попадает только в provider_accepted
      - один 'delivered' → попадает в provider_accepted и delivered
      - один 'read'      → попадает в provider_accepted, delivered и read

    Ожидается:
      provider_accepted == 3
      delivered == 2
      read == 1
    """
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, mode="send-real")
            await session.flush()

            for i, msg_status in enumerate(("sent", "delivered", "read")):
                await _add_outbox_with_recipient(
                    session,
                    msg_status=msg_status,
                    run=run,
                    phone_suffix=f"{i:04d}",
                )

        attr = await _fetch_attribution(session, run.id)

    assert attr["provider_accepted"] == 3, f"provider_accepted должен быть 3, получили {attr['provider_accepted']}"
    assert attr["delivered"] == 2, f"delivered должен быть 2, получили {attr['delivered']}"
    assert attr["read"] == 1, f"read должен быть 1, получили {attr['read']}"


# ---------------------------------------------------------------------------
# Тест 4: run_report возвращает ноль из CampaignRun, даже если outbox даёт данные
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_report_zero_counters_not_replaced_by_attr(session_maker) -> None:
    """run_report() возвращает ноль из CampaignRun, если счётчик записан как 0.

    Сценарий специально построен для проверки фикса `is not None`:
      - CampaignRun имеет явные нули: provider_accepted_count=0, delivered_count=0 и т.д.
      - В outbox / recipients есть сообщение со статусом 'read', поэтому
        _fetch_attribution вернул бы: provider_accepted=1, delivered=1, read=1.
      - Старый код с `or` вернул бы 1 (0 or 1 → 1) — это неверно.
      - Новый код с `is not None` возвращает 0 — счётчик записан, пусть он и нулевой.
    """
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(
                session,
                mode="send-real",
                queued_count=5,
                provider_accepted_count=0,
                delivered_count=0,
                read_count=0,
                replied_count=0,
                booked_after_count=0,
            )
            await session.flush()
            # Добавляем outbox 'read' — _fetch_attribution вернул бы 1 для этих полей.
            # Но run_report должен взять явный ноль из CampaignRun.
            await _add_outbox_with_recipient(session, msg_status="read", run=run)

        report = await run_report(session, run.id)

    # queued берётся из CampaignRun напрямую
    assert report["queued"] == 5
    # Нули из CampaignRun НЕ должны подменяться значениями из outbox (1).
    # Именно это и отличает `is not None` от старого `or`.
    assert report["provider_accepted"] == 0, (
        f"Ожидали 0 из CampaignRun, получили {report['provider_accepted']} — возможно, сработал старый or-fallback"
    )
    assert report["delivered"] == 0, f"Ожидали 0 из CampaignRun, получили {report['delivered']}"
    assert report["read"] == 0, f"Ожидали 0 из CampaignRun, получили {report['read']}"
    assert report["replied"] == 0
    assert report["booked_after_campaign"] == 0


# ---------------------------------------------------------------------------
# Тест 5: run_report() для несуществующего run_id → ValueError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_report_raises_for_unknown_run_id(session_maker) -> None:
    """run_report() должен выбрасывать ValueError при несуществующем run_id."""
    async with session_maker() as session:
        with pytest.raises(ValueError, match="not found"):
            await run_report(session, 9_999_999)


# ---------------------------------------------------------------------------
# Тест 6: monthly_dashboard() без явного company_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monthly_dashboard_default_company_ids(session_maker) -> None:
    """monthly_dashboard без company_ids использует COMPANIES по умолчанию.

    Проверяем:
      - в ответе есть все company_id из COMPANIES;
      - данные по известному run корректно попадают в summary.
    """
    async with session_maker() as session:
        async with session.begin():
            _make_run(
                session,
                mode="send-real",
                company_ids=[COMPANY],
                queued_count=7,
                total_clients_seen=10,
                candidates_count=7,
            )

        # company_ids=None — дефолтная ветка
        result = await monthly_dashboard(session, year=YEAR, month=MONTH, company_ids=None)

    returned_company_ids = {c["company_id"] for c in result["companies"]}
    # Все филиалы из COMPANIES должны присутствовать в ответе
    assert returned_company_ids == set(COMPANIES.keys()), (
        f"Ожидали company_ids={set(COMPANIES.keys())}, получили {returned_company_ids}"
    )

    # Run с COMPANY попал в summary
    assert result["summary"]["queued_count"] == 7
    assert result["summary"]["total_clients_seen"] == 10


# ---------------------------------------------------------------------------
# Тест 7: фильтрация по company_ids в monthly_dashboard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monthly_dashboard_company_filter(session_maker) -> None:
    """monthly_dashboard должен фильтровать runs по запрошенному company_id.

    Явно проверяем:
      - в ответе только запрошенный company_id;
      - run другого филиала не попал в totals.
    """
    other_company = 1271200  # Rastatt — второй филиал из COMPANIES

    async with session_maker() as session:
        async with session.begin():
            # Run для COMPANY — 3 в очереди
            _make_run(session, mode="send-real", company_ids=[COMPANY], queued_count=3)
            # Run для другого филиала — 10 в очереди (не должны попасть)
            _make_run(session, mode="send-real", company_ids=[other_company], queued_count=10)

        result = await monthly_dashboard(
            session,
            year=YEAR,
            month=MONTH,
            company_ids=[COMPANY],  # запрашиваем только один филиал
        )

    # В ответе только COMPANY
    assert len(result["companies"]) == 1
    company_data = result["companies"][0]
    assert company_data["company_id"] == COMPANY, f"Ожидали company_id={COMPANY}, получили {company_data['company_id']}"
    # queued от чужого филиала (10) не должен попасть в totals
    assert company_data["totals"]["queued"] == 3, (
        f"Ожидали queued=3 (только COMPANY), получили {company_data['totals']['queued']}"
    )
    # summary тоже должен отражать только COMPANY
    assert result["summary"]["queued_count"] == 3


# ---------------------------------------------------------------------------
# Тест 8: monthly_dashboard отдаёт summary и by_company для HTML
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monthly_dashboard_exposes_summary_and_by_company(
    session_maker,
) -> None:
    """monthly_dashboard должен отдавать summary и by_company для HTML."""

    async with session_maker() as session:
        async with session.begin():
            _make_run(
                session,
                mode="send-real",
                company_ids=[COMPANY],
                total_clients_seen=11,
                candidates_count=7,
                queued_count=5,
                cards_issued_count=2,
                cards_deleted_count=1,
                opted_out_after_count=1,
            )

        result = await monthly_dashboard(
            session,
            year=YEAR,
            month=MONTH,
            company_ids=[COMPANY],
        )

    assert "summary" in result
    assert "by_company" in result

    assert result["summary"]["runs_count"] == 1
    assert result["summary"]["total_clients_seen"] == 11
    assert result["summary"]["candidates_count"] == 7
    assert result["summary"]["queued_count"] == 5
    assert result["summary"]["cards_issued_count"] == 2
    assert result["summary"]["cards_deleted_count"] == 1
    assert result["summary"]["opted_out_after_count"] == 1

    company_row = result["by_company"][0]
    assert company_row["company_id"] == COMPANY
    assert company_row["total_clients_seen"] == 11
    assert company_row["candidates_count"] == 7
    assert company_row["queued_count"] == 5
    assert company_row["cards_issued_count"] == 2
    assert company_row["cards_deleted_count"] == 1
