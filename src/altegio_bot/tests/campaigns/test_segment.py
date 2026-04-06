"""Тесты сегментации новых клиентов (campaigns/segment.py).

Покрывают основные ветки классификации:
  - eligible: 1 подтверждённая запись в периоде, нет записей до периода
  - opted_out: клиент отказался от рассылки
  - no_phone: нет phone_e164
  - has_records_before_period: есть запись до начала периода
  - multiple_records_in_period: 2+ записи в периоде
  - no_confirmed_record_in_period: запись есть, но не подтверждена
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from altegio_bot.campaigns.segment import find_candidates
from altegio_bot.models.models import Client, Record

PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPANY = 758285

# Счётчик для уникальных altegio_record_id / altegio_client_id
_NEXT_REC_ID = 9_000
_NEXT_CLI_ID = 8_000


def _record_id() -> int:
    global _NEXT_REC_ID
    _NEXT_REC_ID += 1
    return _NEXT_REC_ID


def _client_id() -> int:
    global _NEXT_CLI_ID
    _NEXT_CLI_ID += 1
    return _NEXT_CLI_ID


# ---------------------------------------------------------------------------
# Вспомогательные фабрики
# ---------------------------------------------------------------------------


def _make_client(**kw) -> Client:
    defaults = dict(
        company_id=COMPANY,
        altegio_client_id=_client_id(),
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
    )
    defaults.update(kw)
    return Record(**defaults)


# ---------------------------------------------------------------------------
# Тест 1: eligible-клиент
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eligible_client(session_maker) -> None:
    """Клиент с одной подтверждённой записью в периоде — eligible."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_client_id())
            session.add(client)
            await session.flush()
            session.add(_make_record(client.id))

        result = await find_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    eligible = [c for c in result if c.is_eligible]
    assert any(c.client.id == client.id for c in eligible), "eligible-клиент должен попасть в список"


# ---------------------------------------------------------------------------
# Тест 2: opted_out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_opted_out_excluded(session_maker) -> None:
    """Клиент с wa_opted_out=True должен быть исключён с причиной 'opted_out'."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_client_id(), wa_opted_out=True)
            session.add(client)
            await session.flush()
            session.add(_make_record(client.id))

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
    """Клиент без phone_e164 должен быть исключён с причиной 'no_phone'."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_client_id(), phone_e164=None)
            session.add(client)
            await session.flush()
            session.add(_make_record(client.id))

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
# Тест 4: has_records_before_period
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_records_before_period_excluded(session_maker) -> None:
    """Клиент с записью ДО начала периода должен быть исключён."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_client_id())
            session.add(client)
            await session.flush()
            # Запись в периоде
            session.add(_make_record(client.id, days_offset=5))
            # Запись ДО периода
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_record_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START - timedelta(days=10),
                    confirmed=1,
                )
            )

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


# ---------------------------------------------------------------------------
# Тест 5: multiple_records_in_period
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_records_in_period_excluded(session_maker) -> None:
    """Клиент с двумя и более записями в периоде должен быть исключён."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_client_id())
            session.add(client)
            await session.flush()
            session.add(_make_record(client.id, days_offset=3))
            session.add(_make_record(client.id, days_offset=15))

        result = await find_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "multiple_records_in_period"


# ---------------------------------------------------------------------------
# Тест 6: no_confirmed_record_in_period
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_confirmed_record_excluded(session_maker) -> None:
    """Клиент с одной НЕ подтверждённой записью должен быть исключён."""
    async with session_maker() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_client_id())
            session.add(client)
            await session.flush()
            # confirmed=0 — не подтверждена
            session.add(_make_record(client.id, confirmed=0))

        result = await find_candidates(
            session,
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "no_confirmed_record_in_period"
