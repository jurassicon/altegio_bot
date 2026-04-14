"""Тесты сегментации новых клиентов (campaigns/segment.py).

Архитектура «Вариант А» (CRM как единый источник истины):
  - Локальная БД: только для обнаружения candidate client_id и wa_opted_out / phone_e164.
  - Altegio CRM API: записи в периоде, услуги, подтверждённость, история до периода.
  - Мокируем get_client_crm_records + is_lash_service.

Строгое правило множественности:
  - lash_records >= 2 → multiple_lash_records_in_period (даже если подтверждена только одна)
  - lash_records == 1 и не подтверждена → no_confirmed_lash_record_in_period
  - lash_records == 0 → no_lash_record_in_period

CRM недоступность:
  - CrmUnavailableError → crm_history_unavailable (клиент не считается новым)

Service lookup недоступность:
  - ServiceLookupError → service_category_unavailable (не считать non-lash)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

import altegio_bot.campaigns.segment as segment_module
from altegio_bot.campaigns.altegio_crm import CrmRecord, CrmUnavailableError
from altegio_bot.campaigns.segment import find_candidates
from altegio_bot.models.models import Client, Record
from altegio_bot.service_filter import ServiceLookupError

PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
COMPANY = 758285

# Фиктивные service_id для тестов
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


def _crm_record(
    *,
    days_offset: int = 5,
    confirmed: int = 1,
    attendance: int = 1,
    deleted: bool = False,
    service_ids: list[int] | None = None,
    service_titles: list[str] | None = None,
    before_period: bool = False,
) -> CrmRecord:
    """Создать CrmRecord для тестов.

    По умолчанию confirmed=1 AND attendance=1 — клиент пришёл («Пришел»).
    Для записей до периода или без явки передай attendance=0.
    """
    if before_period:
        starts_at = PERIOD_START - timedelta(days=30)
    else:
        starts_at = PERIOD_START + timedelta(days=days_offset)
    return CrmRecord(
        crm_id=_record_id(),
        starts_at=starts_at,
        confirmed=confirmed,
        attendance=attendance,
        deleted=deleted,
        service_ids=service_ids or [LASH_SVC_ID],
        service_titles=service_titles or ["Wimpernverlängerung"],
    )


def _patch_crm(records: list[CrmRecord] | None = None, *, raise_error: bool = False):
    """Мок для get_client_crm_records."""
    if raise_error:
        return patch(
            "altegio_bot.campaigns.segment.get_client_crm_records",
            new=AsyncMock(side_effect=CrmUnavailableError("CRM недоступен (тест)")),
        )
    return patch(
        "altegio_bot.campaigns.segment.get_client_crm_records",
        new=AsyncMock(return_value=records or []),
    )


def _patch_lash(lash_ids: set[int] | None = None, *, raise_error: bool = False):
    """Мок для is_lash_service: True только если service_id в lash_ids.

    raise_error=True — имитирует ServiceLookupError (API недоступен).
    """
    if raise_error:
        return patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=ServiceLookupError("service API недоступен (тест)")),
        )
    ids = lash_ids if lash_ids is not None else {LASH_SVC_ID}
    return patch(
        "altegio_bot.campaigns.segment.is_lash_service",
        new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id in ids),
    )


@pytest_asyncio.fixture
async def patched_db(session_maker, monkeypatch):
    """Патчит SessionLocal в segment.py на тестовую session_maker.

    find_candidates теперь создаёт свои собственные сессии через SessionLocal,
    поэтому тесты должны подменить SessionLocal на тестовую фабрику.
    """
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)
    return session_maker


# ---------------------------------------------------------------------------
# Тест 1: eligible-клиент
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eligible_client(patched_db) -> None:
    """1 подтверждённая lash-запись из CRM + нет истории → eligible."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [_crm_record(confirmed=1, service_ids=[LASH_SVC_ID])]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
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
async def test_opted_out_excluded(patched_db) -> None:
    """wa_opted_out=True → excluded 'opted_out'."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id(), wa_opted_out=True)
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [_crm_record(confirmed=1)]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
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
async def test_no_phone_excluded(patched_db) -> None:
    """phone_e164=None → excluded 'no_phone'."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id(), phone_e164=None)
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [_crm_record(confirmed=1)]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "no_phone"


# ---------------------------------------------------------------------------
# Тест 4: has_records_before_period (из CRM, не из локальной БД)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_records_before_period_excluded_from_crm(patched_db) -> None:
    """CRM вернул запись до периода → excluded 'has_records_before_period'.

    Источник истины — CRM API. Локальная БД может быть пустой для этого периода.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    # CRM возвращает: 1 запись в периоде + 1 запись ДО периода
    crm_records = [
        _crm_record(confirmed=1, service_ids=[LASH_SVC_ID]),
        _crm_record(before_period=True, service_ids=[LASH_SVC_ID]),
    ]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
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
async def test_no_lash_record_excluded(patched_db) -> None:
    """Только не-ресничные услуги в CRM → excluded 'no_lash_record_in_period'."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    # CRM запись с NON-lash сервисом
    crm_records = [_crm_record(confirmed=1, service_ids=[NON_LASH_SVC_ID])]

    with _patch_crm(crm_records), _patch_lash({LASH_SVC_ID}):
        result = await find_candidates(
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
async def test_no_confirmed_lash_excluded(patched_db) -> None:
    """Ровно 1 lash-запись, но клиент не пришёл (attendance=0) → excluded.

    Критерий eligible — attendance=1 («Пришел»), а не просто confirmed=1.
    confirmed=1 означает только «запись не отменена», но не факт явки.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id, confirmed=1)
            session.add(rec)

    # CRM: 1 lash-запись, confirmed=1 (не отменена), но attendance=0 (не пришёл)
    crm_records = [_crm_record(confirmed=1, attendance=0, service_ids=[LASH_SVC_ID])]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "no_confirmed_lash_record_in_period"


# ---------------------------------------------------------------------------
# Тест 7: multiple_lash_records — строгое правило (2 подтверждённые)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_confirmed_lash_excluded(patched_db) -> None:
    """2 подтверждённые lash-записи → excluded 'multiple_lash_records_in_period'."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec1 = _make_record(client.id, days_offset=3)
            rec2 = _make_record(client.id, days_offset=15)
            session.add(rec1)
            session.add(rec2)

    # CRM: 2 подтверждённые lash-записи
    crm_records = [
        _crm_record(days_offset=3, confirmed=1, service_ids=[LASH_SVC_ID]),
        _crm_record(days_offset=15, confirmed=1, service_ids=[LASH_SVC_ID]),
    ]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    assert match.excluded_reason == "multiple_lash_records_in_period"
    assert match.lash_records_in_period == 2
    assert match.confirmed_lash_records_in_period == 2


# ---------------------------------------------------------------------------
# Тест 8: strict multiplicity — 1 confirmed + 1 unconfirmed → excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_strict_multiplicity_one_confirmed_one_unconfirmed(patched_db) -> None:
    """1 подтверждённая + 1 неподтверждённая lash-запись → excluded.

    Правило строгое: 2+ lash-записей в периоде вообще — excluded,
    даже если подтверждена только одна.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec1 = _make_record(client.id, days_offset=3, confirmed=1)
            rec2 = _make_record(client.id, days_offset=20, confirmed=0)
            session.add(rec1)
            session.add(rec2)

    # CRM: 1 attended lash + 1 not-attended lash
    # confirmed=1 + attendance=1 → пришёл; confirmed=0 + attendance=0 → не пришёл
    crm_records = [
        _crm_record(days_offset=3, confirmed=1, attendance=1, service_ids=[LASH_SVC_ID]),
        _crm_record(days_offset=20, confirmed=0, attendance=0, service_ids=[LASH_SVC_ID]),
    ]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert not match.is_eligible
    # Строгое правило: 2+ lash-записей вообще
    assert match.excluded_reason == "multiple_lash_records_in_period"
    assert match.lash_records_in_period == 2
    # Только 1 из 2 имеет attendance=1
    assert match.confirmed_lash_records_in_period == 1


# ---------------------------------------------------------------------------
# Тест 9: CRM error → crm_history_unavailable (клиент не становится eligible)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crm_error_client_not_eligible(patched_db) -> None:
    """При ошибке CRM клиент исключается с причиной 'crm_history_unavailable'.

    Нельзя считать клиента новым, если не удалось проверить его историю.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    with _patch_crm(raise_error=True), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None, "Клиент должен присутствовать в результатах (как excluded)"
    assert not match.is_eligible, "Клиент с недоступным CRM не может быть eligible"
    assert match.excluded_reason == "crm_history_unavailable"


# ---------------------------------------------------------------------------
# Тест 10: CRM = источник истины — локальная БД игнорируется для records
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crm_source_of_truth_overrides_local_db(patched_db) -> None:
    """CRM говорит «есть запись до периода» → excluded, даже если в локальной БД истории нет."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            # Локальная БД: только in-period запись, до периода ничего нет
            rec = _make_record(client.id, days_offset=5)
            session.add(rec)
            # Намеренно НЕ добавляем записей до периода в локальную БД

    # CRM: in-period + before-period
    crm_records = [
        _crm_record(days_offset=5, confirmed=1, service_ids=[LASH_SVC_ID]),
        _crm_record(before_period=True, service_ids=[NON_LASH_SVC_ID]),
    ]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
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
# Тест 11: local_client_found flag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_client_found_flag(patched_db) -> None:
    """local_client_found=True для клиента из локальной БД."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [_crm_record(confirmed=1)]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert match.local_client_found is True


# ---------------------------------------------------------------------------
# Тест 12: service_titles_in_period из CRM
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_titles_from_crm(patched_db) -> None:
    """service_titles_in_period заполняются из данных CRM."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [
        _crm_record(
            confirmed=1,
            service_ids=[LASH_SVC_ID],
            service_titles=["Wimpernverlängerung"],
        )
    ]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert "Wimpernverlängerung" in match.service_titles_in_period


# ---------------------------------------------------------------------------
# Тест 13: service lookup failure → service_category_unavailable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_lookup_failure_excluded(patched_db) -> None:
    """ServiceLookupError → excluded 'service_category_unavailable'.

    При ошибке Altegio service category API нельзя считать услугу non-lash
    и тихо пропускать клиента. Клиент должен быть исключён явно.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_alt_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [_crm_record(confirmed=1, service_ids=[LASH_SVC_ID])]

    with _patch_crm(crm_records), _patch_lash(raise_error=True):
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None, "Клиент должен присутствовать (как excluded)"
    assert not match.is_eligible, "Клиент с недоступным service API не может быть eligible"
    assert match.excluded_reason == "service_category_unavailable"
