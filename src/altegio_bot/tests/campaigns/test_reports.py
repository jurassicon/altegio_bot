"""Тесты модуля reports.py.

Три сценария:
  1. Preview-runs НЕ попадают в monthly_dashboard (только mode='send-real').
  2. Сообщение со статусом 'read' считается и в delivered, и в provider_accepted
     (кумулятивная воронка).
  3. Фильтр по company_id в list_runs работает корректно через SQL JSONB-containment.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from altegio_bot.campaigns.reports import monthly_dashboard, run_report
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
    assert company_data["totals"]["sent"] == 5


# ---------------------------------------------------------------------------
# Тест 2: кумулятивная воронка — read учитывается в delivered и provider_accepted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_counted_in_delivered_and_provider_accepted(session_maker) -> None:
    """Статус 'read' у OutboxMessage должен учитываться кумулятивно.

    Ожидается:
      provider_accepted >= delivered >= read
      Если 1 сообщение = read: provider_accepted=1, delivered=1, read=1.
    """
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session, mode="send-real")
            await session.flush()

            # OutboxMessage с финальным статусом 'read'
            msg = OutboxMessage(
                company_id=COMPANY,
                status="read",
                template_code="new_clients",
                phone_e164="+491234560099",
                body="",
                scheduled_at=PERIOD_START,
            )
            session.add(msg)
            await session.flush()

            recipient = CampaignRecipient(
                campaign_run_id=run.id,
                company_id=COMPANY,
                status="read",
                outbox_message_id=msg.id,
            )
            session.add(recipient)

        report = await run_report(session, run.id)

    assert report["read"] >= 1, "read должен быть >= 1"
    assert report["delivered"] >= report["read"], "delivered должен быть >= read (кумулятивно)"
    assert report["provider_accepted"] >= report["delivered"], (
        "provider_accepted должен быть >= delivered (кумулятивно)"
    )


# ---------------------------------------------------------------------------
# Тест 3: фильтрация completed_at / company_ids в monthly_dashboard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monthly_dashboard_company_filter(session_maker) -> None:
    """monthly_dashboard должен фильтровать runs по запрошенному company_id."""
    other_company = 1271200  # Rastatt — второй филиал из COMPANIES

    async with session_maker() as session:
        async with session.begin():
            # Run для COMPANY
            _make_run(session, mode="send-real", company_ids=[COMPANY], queued_count=3)
            # Run для другого филиала
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
    assert company_data["company_id"] == COMPANY
    assert company_data["totals"]["sent"] == 3
