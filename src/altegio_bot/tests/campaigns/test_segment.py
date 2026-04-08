"""Тесты сегментации новых клиентов (campaigns/segment.py).

Новая логика (Вариант А):
- Источник истины для «до периода»: Altegio CRM API (мокируется).
- Ресничные записи в периоде: local RecordService + LASH_CATEGORY_IDS_BY_COMPANY.
- Классификация:
    eligible: ровно 1 подтверждённая ресничная запись + 0 записей до периода в CRM
    opted_out: wa_opted_out=True
    no_phone: нет phone_e164
    has_records_before_period: CRM вернул > 0 записей до периода
    no_lash_record_in_period: нет ресничных записей в периоде
    no_confirmed_lash_record_in_period: нет подтверждённой ресничной записи
    multiple_lash_records_in_period: 2+ подтверждённых ресничных

Мокирование:
- count_client_records_before_period → int (мок возвращает 0 или N)
- is_lash_service → bool (мок: True для service_id в LASH_IDS)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from altegio_bot.campaigns.segment import find_candidates
from altegio_bot.models.models import Client, Record, RecordService
from altegio_bot.service_filter import LASH_CATEGORY_IDS_BY_COMPANY

PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPANY = 758285

# Возьмём первую известную lash category_id для тестов
_LASH_CATEGORIES = LASH_CATEGORY_IDS_BY_COMPANY.get(COMPANY, set())
# Фиктивный service_id, который мок пометит как «ресничный»
LASH_SVC_ID = 99001
NON_LASH_SVC_ID = 99002

_NEXT_REC_ID = 7_000
_NEXT_CLI_ID = 6_000
_NEXT_ALT_CLI_ID = 5_000


def _record_id() -> int:
    global _NEXT_REC_ID
    _NEXT_REC_ID += 1
    return _NEXT_REC_ID


def _client_id() -> int:
    global _NEXT_CLI_ID
    _NEXT_CLI_ID += 1
    return _NEXT_CLI_ID


def _alt_id() -> int:
    global _NEXT_ALT_CLI_ID
    _NEXT_ALT_CLI_ID += 1
    return _NEXT_ALT_CLI_ID


def _make_client(**kw) -> Client:
    defaults = dict(
        company_id=COMPANY,
        altegio_client_id=_alt_id(),
        phone_e164="+491234567890",
        raw={},
    )
    defaults.update(kw)
    return Client(**defaults)


def _make_record(client_id: int, *, days_offset: int = 5, confirmed: int = 1, **kw) -> Record:
    defaults = dict(
        company_id=COMPANY,
        altegio_record_id=_record_id(),
        client_id=client_id,
        starts_at=PERIOD_START + timedelta(days=days_offset),
        confirmed=confirmed,
        is_deleted=False,
    )
    defaults.update(kw)
    return Record(**defaults)


def _make_lash_service(record_id: int, service_id: int = LASH_SVC_ID) -> RecordService:
    return RecordService(
        record_id=record_id,
        service_id=service_id,
        title="Wimpernverlängerung",
    )


def _mock_lash(svc_id: int) -> bool:
    """Мок для is_lash_service: True только для LASH_SVC_ID."""
    return svc_id == LASH_SVC_ID


def _patch_crm(records_before: int = 0):
    """Мок для count_client_records_before_period."""
    return patch(
        "altegio_bot.campaigns.segment.count_client_records_before_period",
        new=AsyncMock(return_value=records_before),
    )


def _patch_lash(is_lash: bool = True):
    """Мок для is_lash_service."""
    return patch(
        "altegio_bot.campaigns.segment.is_lash_service",
        new=AsyncMock(side_effect=lambda company_id, svc_id: _mock_lash(svc_id)),
    )


# ---------------------------------------------------------------------------
# Тест 1: eligible-клиент (одна подтверждённая ресничная запись, нет истории)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eligible_client(session_maker) -> None:
    """Ровно 1 подтверждённая ресничная запись + нет истории → eligible."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            session.add(_make_lash_service(rec.id))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None, "Клиент должен быть найден"
    assert match.is_eligible, f"Должен быть eligible, но: {match.excluded_reason}"
    assert match.lash_records_in_period == 1
    assert match.confirmed_lash_records_in_period == 1
    assert match.records_before_period == 0


# ---------------------------------------------------------------------------
# Тест 2: opted_out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_opted_out_excluded(session_maker) -> None:
    """wa_opted_out=True → excluded с причиной 'opted_out'."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id(), wa_opted_out=True)
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            session.add(_make_lash_service(rec.id))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "opted_out"


# ---------------------------------------------------------------------------
# Тест 3: no_phone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_phone_excluded(session_maker) -> None:
    """phone_e164=None → excluded с причиной 'no_phone'."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id(), phone_e164=None)
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            session.add(_make_lash_service(rec.id))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "no_phone"


# ---------------------------------------------------------------------------
# Тест 4: has_records_before_period (из Altegio CRM)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_records_before_period_excluded_from_crm(session_maker) -> None:
    """CRM вернул записи до периода → excluded 'has_records_before_period'.

    Источник правды — Altegio CRM API (мокируем count_client_records_before_period).
    Даже если в локальной БД нет записей до периода — решение принимается по CRM.
    """
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            session.add(_make_lash_service(rec.id))

    # CRM говорит: у клиента есть 1 запись до периода
    with _patch_crm(1), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "has_records_before_period"
    assert match.records_before_period == 1


# ---------------------------------------------------------------------------
# Тест 5: no_lash_record_in_period
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_lash_record_excluded(session_maker) -> None:
    """Только не-ресничные услуги в периоде → excluded 'no_lash_record_in_period'."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            # Добавить NON-lash service
            session.add(RecordService(record_id=rec.id, service_id=NON_LASH_SVC_ID, title="Massage"))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "no_lash_record_in_period"


# ---------------------------------------------------------------------------
# Тест 6: no_confirmed_lash_record_in_period
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_confirmed_lash_excluded(session_maker) -> None:
    """Ресничная запись есть, но неподтверждённая → excluded."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id, confirmed=0)  # не подтверждена
            session.add(rec)
            await session.flush()
            session.add(_make_lash_service(rec.id))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "no_confirmed_lash_record_in_period"


# ---------------------------------------------------------------------------
# Тест 7: multiple_lash_records_in_period
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_lash_records_excluded(session_maker) -> None:
    """2+ подтверждённых ресничных записей → excluded 'multiple_lash_records_in_period'."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec1 = _make_record(client.id, days_offset=3)
            rec2 = _make_record(client.id, days_offset=15)
            session.add(rec1)
            session.add(rec2)
            await session.flush()
            session.add(_make_lash_service(rec1.id))
            session.add(_make_lash_service(rec2.id))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "multiple_lash_records_in_period"
    assert match.confirmed_lash_records_in_period == 2


# ---------------------------------------------------------------------------
# Тест 8: local_client_found flag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_client_found_flag(session_maker) -> None:
    """local_client_found=True для клиента из локальной БД."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            session.add(_make_lash_service(rec.id))

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert match.local_client_found is True


# ---------------------------------------------------------------------------
# Тест 9: service_titles_in_period собираются корректно
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_titles_collected(session_maker) -> None:
    """service_titles_in_period содержит названия услуг из периода."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)
            await session.flush()
            session.add(
                RecordService(
                    record_id=rec.id,
                    service_id=LASH_SVC_ID,
                    title="Wimpernverlängerung",
                )
            )

    with _patch_crm(0), _patch_lash():
        async with session_maker() as session:
            result = await find_candidates(
                session,
                company_id=COMPANY,
                period_start=PERIOD_START,
                period_end=PERIOD_END,
            )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert "Wimpernverlängerung" in match.service_titles_in_period
