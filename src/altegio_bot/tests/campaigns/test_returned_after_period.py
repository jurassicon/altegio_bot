"""Тесты: новое правило исключения — клиент уже вернулся после периода.

Бизнес-правило (добавлено после первого запуска кампании March 2026):
  Если у клиента есть НЕ УДАЛЁННАЯ запись ПОСЛЕ окончания периода кампании
  (starts_at >= period_end), его нужно исключить из рассылки.
  Рассылка нужна только тем, кто впервые пришёл в периоде И больше не вернулся.

Причина исключения: 'returned_after_first_visit'

Пример из реального кейса: Tamara Gocevska (491728079002)
  - первая запись в марте 2026 → попала в выборку;
  - запись в апреле 2026 → должна быть ИСКЛЮЧЕНА.

Тесты:
  A. Клиент с записью в апреле → excluded с returned_after_first_visit.
  B. Клиент без записей после периода → eligible (как раньше).
  C. Любой статус future-записи исключает (confirmed=0, attendance=0).
  D. УДАЛЁННАЯ будущая запись не исключает (deleted=True).
  E. classify_crm_records правильно считает count_after.
  F. compute_excluded_reason с count_after > 0 → returned_after_first_visit.
  G. compute_excluded_reason с count_after = 0 → не возвращает returned_after_first_visit.
  H. Приоритет: has_records_before_period раньше returned_after_first_visit.
  I. Preview показывает новую причину в breakdown.
  J. Send-real: snapshot корректно восстанавливает records_after_period.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

import altegio_bot.campaigns.segment as segment_module
from altegio_bot.campaigns.altegio_crm import CrmClientRef, CrmRecord, classify_crm_records
from altegio_bot.campaigns.segment import compute_excluded_reason, find_candidates
from altegio_bot.models.models import CampaignRecipient, CampaignRun

PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)
COMPANY = 758285
LASH_SVC_ID = 99001

_NEXT_ID = 50_000


def _next_id() -> int:
    global _NEXT_ID
    _NEXT_ID += 1
    return _NEXT_ID


def _crm_rec(
    *,
    days_offset: int,
    confirmed: int = 1,
    attendance: int = 1,
    deleted: bool = False,
    service_ids: list[int] | None = None,
) -> CrmRecord:
    """Построить CrmRecord с заданным смещением от PERIOD_START."""
    return CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START + timedelta(days=days_offset),
        confirmed=confirmed,
        attendance=attendance,
        deleted=deleted,
        service_ids=service_ids or [LASH_SVC_ID],
        service_titles=["Wimpernverlängerung"],
    )


def _crm_after(
    *,
    confirmed: int = 1,
    attendance: int = 1,
    deleted: bool = False,
) -> CrmRecord:
    """Запись ПОСЛЕ периода (в апреле)."""
    return _crm_rec(days_offset=35, confirmed=confirmed, attendance=attendance, deleted=deleted)


# ===========================================================================
# E. classify_crm_records — unit tests для count_after
# ===========================================================================


def test_classify_counts_after_period() -> None:
    """classify_crm_records правильно считает count_after для записи после периода."""
    in_period_rec = _crm_rec(days_offset=5)  # в периоде
    after_rec = _crm_after()  # после периода

    in_period, count_before, count_after = classify_crm_records([in_period_rec, after_rec], PERIOD_START, PERIOD_END)

    assert len(in_period) == 1
    assert count_before == 0
    assert count_after == 1


def test_classify_deleted_after_not_counted() -> None:
    """Удалённая запись после периода НЕ считается в count_after."""
    in_period_rec = _crm_rec(days_offset=5)
    after_deleted = _crm_after(deleted=True)  # удалена — не считаем

    in_period, count_before, count_after = classify_crm_records(
        [in_period_rec, after_deleted], PERIOD_START, PERIOD_END
    )

    assert count_after == 0


def test_classify_count_after_any_status() -> None:
    """count_after считает записи независимо от confirmed/attendance статуса."""
    after_unconfirmed = _crm_after(confirmed=0, attendance=0)  # отменена, не пришёл
    after_confirmed = _crm_after(confirmed=1, attendance=0)  # не пришёл
    after_attended = _crm_after(confirmed=1, attendance=1)  # пришёл

    _, _, count_after = classify_crm_records(
        [after_unconfirmed, after_confirmed, after_attended], PERIOD_START, PERIOD_END
    )

    assert count_after == 3, "Все три не-удалённые записи после периода должны быть посчитаны"


def test_classify_backward_compat_returns_three_tuple() -> None:
    """classify_crm_records возвращает 3-tuple (не 2-tuple как раньше)."""
    result = classify_crm_records([], PERIOD_START, PERIOD_END)
    assert len(result) == 3


# ===========================================================================
# F, G. compute_excluded_reason — unit tests
# ===========================================================================


def test_compute_excluded_reason_returned_after_first_visit() -> None:
    """count_after > 0 → returned_after_first_visit."""
    reason = compute_excluded_reason(
        wa_opted_out=False,
        phone_e164="+4915123456789",
        count_before=0,
        count_after=1,
        lash_count=1,
        attended_lash_count=1,
    )
    assert reason == "returned_after_first_visit"


def test_compute_excluded_reason_no_future_records_eligible() -> None:
    """count_after = 0 → не добавляет returned_after_first_visit, клиент eligible."""
    reason = compute_excluded_reason(
        wa_opted_out=False,
        phone_e164="+4915123456789",
        count_before=0,
        count_after=0,
        lash_count=1,
        attended_lash_count=1,
    )
    assert reason is None


# ===========================================================================
# H. Приоритет: has_records_before_period раньше returned_after_first_visit
# ===========================================================================


def test_compute_priority_before_over_after() -> None:
    """has_records_before_period имеет приоритет над returned_after_first_visit."""
    reason = compute_excluded_reason(
        wa_opted_out=False,
        phone_e164="+4915123456789",
        count_before=1,  # есть история до периода
        count_after=1,  # И вернулся после
        lash_count=1,
        attended_lash_count=1,
    )
    assert reason == "has_records_before_period"


# ===========================================================================
# Интеграционные тесты через find_candidates
# ===========================================================================


def _make_client(**kw):
    """Создать модель Client."""
    from altegio_bot.models.models import Client

    defaults = dict(
        company_id=COMPANY,
        altegio_client_id=_next_id(),
        phone_e164="+4915199000001",
        raw={},
    )
    defaults.update(kw)
    return Client(**defaults)


def _patch_crm(records: list[CrmRecord]):
    return patch(
        "altegio_bot.campaigns.segment.get_client_crm_records",
        new=AsyncMock(return_value=records),
    )


def _patch_discovery(altegio_id: int):
    return patch(
        "altegio_bot.campaigns.segment.get_company_period_client_refs",
        new=AsyncMock(return_value=[CrmClientRef(altegio_client_id=altegio_id)]),
    )


def _patch_lash():
    return patch(
        "altegio_bot.campaigns.segment.is_lash_service",
        new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
    )


@pytest_asyncio.fixture
async def patched_db(session_maker, monkeypatch):
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)
    return session_maker


# ===========================================================================
# A. Клиент с записью после периода → excluded с returned_after_first_visit
# ===========================================================================


@pytest.mark.asyncio
async def test_client_with_april_record_excluded(patched_db) -> None:
    """Tamara Gocevska scenario: первая запись в марте, но уже запись в апреле.

    Такой клиент должен быть ИСКЛЮЧЁН с returned_after_first_visit.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)

    # CRM: 1 мартовская запись (eligible по остальным критериям) + 1 апрельская
    march_rec = _crm_rec(days_offset=5, confirmed=1, attendance=1)
    april_rec = _crm_after(confirmed=1, attendance=0)  # статус не важен

    with _patch_crm([march_rec, april_rec]), _patch_lash(), _patch_discovery(client.altegio_client_id):
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert len(result) == 1
    match = result[0]
    assert not match.is_eligible, "Клиент с апрельской записью не должен быть eligible"
    assert match.excluded_reason == "returned_after_first_visit", (
        f"Ожидали returned_after_first_visit, получили: {match.excluded_reason!r}"
    )
    assert match.records_after_period == 1


# ===========================================================================
# B. Клиент без записей после периода → eligible
# ===========================================================================


@pytest.mark.asyncio
async def test_client_without_future_records_eligible(patched_db) -> None:
    """Клиент, который пришёл в марте и больше не вернулся → eligible."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)

    # CRM: только одна мартовская запись, нет истории до периода, нет записей после
    march_rec = _crm_rec(days_offset=5, confirmed=1, attendance=1)

    with _patch_crm([march_rec]), _patch_lash(), _patch_discovery(client.altegio_client_id):
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert len(result) == 1
    match = result[0]
    assert match.is_eligible, f"Клиент без будущих записей должен быть eligible: {match.excluded_reason}"
    assert match.records_after_period == 0


# ===========================================================================
# C. Любой статус будущей записи исключает (confirmed=0, attendance=0)
# ===========================================================================


@pytest.mark.asyncio
async def test_future_record_any_status_excludes(patched_db) -> None:
    """Запись после периода с confirmed=0, attendance=0 тоже исключает клиента."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)

    march_rec = _crm_rec(days_offset=5, confirmed=1, attendance=1)
    # Отменённая запись в апреле (confirmed=0) — не важен статус
    cancelled_april = _crm_after(confirmed=0, attendance=0, deleted=False)

    with _patch_crm([march_rec, cancelled_april]), _patch_lash(), _patch_discovery(client.altegio_client_id):
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert len(result) == 1
    match = result[0]
    assert not match.is_eligible
    assert match.excluded_reason == "returned_after_first_visit"
    assert match.records_after_period == 1


# ===========================================================================
# D. Удалённая будущая запись НЕ исключает клиента
# ===========================================================================


@pytest.mark.asyncio
async def test_deleted_future_record_does_not_exclude(patched_db) -> None:
    """Если запись после периода УДАЛЕНА — клиент не исключается.

    deleted=True означает, что запись была полностью удалена из системы
    (не просто отменена). Такой «возврат» не считается.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)

    march_rec = _crm_rec(days_offset=5, confirmed=1, attendance=1)
    deleted_april = _crm_after(deleted=True)  # удалена — не считаем как возврат

    with _patch_crm([march_rec, deleted_april]), _patch_lash(), _patch_discovery(client.altegio_client_id):
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert len(result) == 1
    match = result[0]
    assert match.is_eligible, f"Удалённая будущая запись не должна исключать клиента: {match.excluded_reason}"
    assert match.records_after_period == 0


# ===========================================================================
# I. Preview breakdown: records_after_period сохраняется в recipient
# ===========================================================================


@pytest.mark.asyncio
async def test_preview_saves_records_after_period(patched_db, session_maker) -> None:
    """Preview сохраняет records_after_period в CampaignRecipient.

    После фикса runner._build_recipient передаёт records_after_period в recipient.
    """
    import altegio_bot.campaigns.runner as runner_module
    from altegio_bot.campaigns.runner import RunParams, run_preview
    from altegio_bot.campaigns.segment import ClientCandidate, ClientSnapshot

    # Создаём candidate с records_after_period=2
    snapshot = ClientSnapshot(
        id=1,
        company_id=COMPANY,
        altegio_client_id=99001,
        display_name="Tamara Gocevska",
        phone_e164="+491728079002",
        wa_opted_out=False,
    )
    candidate = ClientCandidate(
        client=snapshot,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=["Wimpernverlängerung"],
        records_before_period=0,
        records_after_period=2,  # уже вернулась дважды!
        local_client_found=True,
        excluded_reason="returned_after_first_visit",
    )

    orig_session_local = runner_module.SessionLocal
    runner_module.SessionLocal = session_maker
    try:
        with patch.object(runner_module, "find_candidates", return_value=[candidate]):
            run = await run_preview(
                RunParams(
                    company_id=COMPANY,
                    location_id=COMPANY,
                    period_start=PERIOD_START,
                    period_end=PERIOD_END,
                    mode="preview",
                )
            )
    finally:
        runner_module.SessionLocal = orig_session_local

    assert run.status == "completed"
    # excluded_returned_after_visit счётчик должен быть 1
    assert run.excluded_returned_after_visit == 1

    # Проверяем recipient в БД
    from sqlalchemy import select

    async with session_maker() as session:
        recipients = (
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )

    assert len(recipients) == 1
    r = recipients[0]
    assert r.excluded_reason == "returned_after_first_visit"
    assert r.records_after_period == 2


# ===========================================================================
# J. Send-real snapshot восстанавливает records_after_period
# ===========================================================================


@pytest.mark.asyncio
async def test_snapshot_restores_records_after_period(session_maker) -> None:
    """_load_candidates_from_preview_snapshot корректно восстанавливает records_after_period."""
    from altegio_bot.campaigns.runner import _load_candidates_from_preview_snapshot
    from altegio_bot.models.models import CampaignRecipient as R

    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
            )
            session.add(run)
            await session.flush()

            recipient = R(
                campaign_run_id=run.id,
                company_id=COMPANY,
                client_id=None,  # CRM-only
                altegio_client_id=99999,
                phone_e164="+491728079002",
                display_name="Tamara Gocevska",
                local_client_found=False,
                total_records_in_period=1,
                confirmed_records_in_period=1,
                lash_records_in_period=1,
                confirmed_lash_records_in_period=1,
                records_before_period=0,
                records_after_period=1,  # была в апреле
                service_titles_in_period=[],
                status="skipped",
                excluded_reason="returned_after_first_visit",
            )
            session.add(recipient)

    async with session_maker() as session:
        candidates = await _load_candidates_from_preview_snapshot(session, run.id)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.excluded_reason == "returned_after_first_visit"
    assert c.records_after_period == 1
    assert not c.is_eligible
