"""Тесты новых защитных механизмов сегментации (Session 3 fixes).

Проверяет:
11.1 outer except Exception в _process_one_client: RuntimeError → crm_history_unavailable
11.2 return_exceptions=True: один таск c RuntimeError не прерывает остальные
11.4 ClientSnapshot: candidate.client — ClientSnapshot, а не Client ORM
11.6 settings.campaign_crm_max_concurrency: семафор создаётся с правильным значением
11.8 _make_excluded_candidate: единый helper корректно создаёт ClientCandidate
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import altegio_bot.campaigns.segment as segment_module
from altegio_bot.campaigns.altegio_crm import CrmRecord, CrmUnavailableError
from altegio_bot.campaigns.segment import (
    ClientCandidate,
    ClientSnapshot,
    _make_excluded_candidate,
    _process_one_client,
    find_candidates,
)
from altegio_bot.models.models import Client, Record
from altegio_bot.service_filter import ServiceLookupError

PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)
COMPANY = 758285

_NEXT_ID = 9_000


def _next_id() -> int:
    global _NEXT_ID
    _NEXT_ID += 1
    return _NEXT_ID


def _make_snapshot(**kw) -> ClientSnapshot:
    defaults = dict(
        id=_next_id(),
        company_id=COMPANY,
        altegio_client_id=_next_id(),
        display_name="Test Client",
        phone_e164="+491234567890",
        wa_opted_out=False,
    )
    defaults.update(kw)
    return ClientSnapshot(**defaults)


def _crm_record(*, days_offset: int = 5, confirmed: int = 1, attendance: int = 1, service_ids=None) -> CrmRecord:
    """По умолчанию confirmed=1 AND attendance=1 — клиент пришёл («Пришел»)."""
    return CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START + timedelta(days=days_offset),
        confirmed=confirmed,
        attendance=attendance,
        deleted=False,
        service_ids=service_ids or [99001],
        service_titles=["Wimpernverlängerung"],
    )


# ---------------------------------------------------------------------------
# Тест 11.8: _make_excluded_candidate — единый helper
# ---------------------------------------------------------------------------


def test_make_excluded_candidate_crm_unavailable() -> None:
    """_make_excluded_candidate создаёт ClientCandidate с указанным reason."""
    snapshot = _make_snapshot()
    candidate = _make_excluded_candidate(snapshot, "crm_history_unavailable")

    assert candidate.excluded_reason == "crm_history_unavailable"
    assert candidate.client is snapshot
    assert candidate.total_records_in_period == 0
    assert candidate.lash_records_in_period == 0
    assert candidate.records_before_period == 0
    assert not candidate.is_eligible


def test_make_excluded_candidate_service_unavailable() -> None:
    """_make_excluded_candidate работает для любого reason."""
    snapshot = _make_snapshot()
    candidate = _make_excluded_candidate(snapshot, "service_category_unavailable")

    assert candidate.excluded_reason == "service_category_unavailable"
    assert candidate.client is snapshot
    assert not candidate.is_eligible


# ---------------------------------------------------------------------------
# Тест 11.1: outer except Exception в _process_one_client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_one_client_outer_except_catches_runtime_error() -> None:
    """RuntimeError внутри _process_one_client → crm_history_unavailable (не пробрасывается)."""
    snapshot = _make_snapshot()
    sem = asyncio.Semaphore(8)
    http = MagicMock()

    # Мокируем get_client_crm_records так, чтобы он бросал RuntimeError (не CrmUnavailableError)
    with patch(
        "altegio_bot.campaigns.segment.get_client_crm_records",
        new=AsyncMock(side_effect=RuntimeError("unexpected internal error")),
    ):
        result = await _process_one_client(http, sem, snapshot, COMPANY, PERIOD_START, PERIOD_END)

    assert isinstance(result, ClientCandidate)
    assert result.excluded_reason == "crm_history_unavailable"
    assert result.client is snapshot


@pytest.mark.asyncio
async def test_process_one_client_crm_unavailable_caught() -> None:
    """CrmUnavailableError → crm_history_unavailable (штатная обработка)."""
    snapshot = _make_snapshot()
    sem = asyncio.Semaphore(8)
    http = MagicMock()

    with patch(
        "altegio_bot.campaigns.segment.get_client_crm_records",
        new=AsyncMock(side_effect=CrmUnavailableError("CRM down")),
    ):
        result = await _process_one_client(http, sem, snapshot, COMPANY, PERIOD_START, PERIOD_END)

    assert result.excluded_reason == "crm_history_unavailable"


@pytest.mark.asyncio
async def test_process_one_client_service_lookup_error_caught() -> None:
    """ServiceLookupError → service_category_unavailable."""
    snapshot = _make_snapshot()
    sem = asyncio.Semaphore(8)
    http = MagicMock()

    crm_records = [_crm_record()]

    with (
        patch(
            "altegio_bot.campaigns.segment.get_client_crm_records",
            new=AsyncMock(return_value=crm_records),
        ),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=ServiceLookupError("service API down")),
        ),
    ):
        result = await _process_one_client(http, sem, snapshot, COMPANY, PERIOD_START, PERIOD_END)

    assert result.excluded_reason == "service_category_unavailable"


# ---------------------------------------------------------------------------
# Тест 11.2: return_exceptions=True — один сбой не прерывает остальные таски
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_candidates_return_exceptions_resilience(session_maker, monkeypatch) -> None:
    """RuntimeError в одном таске → остальные завершаются нормально."""
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)

    call_count = 0
    lash_svc_id = 99001

    async def selective_crm(http_client, *, company_id, altegio_client_id):
        """Первый клиент → RuntimeError; второй → нормальный ответ."""
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated unexpected crash")
        return [_crm_record(service_ids=[lash_svc_id])]

    async with session_maker() as session:
        async with session.begin():
            # Два клиента с записями в периоде
            for _ in range(2):
                client = Client(
                    company_id=COMPANY,
                    altegio_client_id=_next_id(),
                    phone_e164="+491234567890",
                    raw={},
                )
                session.add(client)
                await session.flush()
                rec = Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=5),
                    confirmed=1,
                    is_deleted=False,
                )
                session.add(rec)

    with (
        patch("altegio_bot.campaigns.segment.get_client_crm_records", new=AsyncMock(side_effect=selective_crm)),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == lash_svc_id),
        ),
    ):
        results = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    # Два клиента обнаружены: первый → crm_history_unavailable, второй → eligible
    assert len(results) == 2
    reasons = {c.excluded_reason for c in results}
    assert "crm_history_unavailable" in reasons
    # Хотя бы один eligible (второй клиент с нормальным ответом)
    assert any(c.is_eligible for c in results)


# ---------------------------------------------------------------------------
# Тест 11.4: candidate.client — ClientSnapshot, а не Client ORM
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_candidates_client_is_snapshot(session_maker, monkeypatch) -> None:
    """find_candidates возвращает ClientCandidate с ClientSnapshot, а не Client ORM."""
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)

    lash_svc_id = 99001

    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=_next_id(),
                phone_e164="+491234567890",
                raw={},
            )
            session.add(client)
            await session.flush()
            rec = Record(
                company_id=COMPANY,
                altegio_record_id=_next_id(),
                client_id=client.id,
                starts_at=PERIOD_START + timedelta(days=5),
                confirmed=1,
                is_deleted=False,
            )
            session.add(rec)
            client_db_id = client.id

    with (
        patch(
            "altegio_bot.campaigns.segment.get_client_crm_records",
            new=AsyncMock(return_value=[_crm_record(service_ids=[lash_svc_id])]),
        ),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == lash_svc_id),
        ),
    ):
        results = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert len(results) == 1
    candidate = results[0]

    # Ключевая проверка: client — ClientSnapshot, не Client ORM
    assert isinstance(candidate.client, ClientSnapshot)
    # Данные совпадают с тем, что было в БД
    assert candidate.client.id == client_db_id
    assert candidate.client.company_id == COMPANY
    assert candidate.client.phone_e164 == "+491234567890"


# ---------------------------------------------------------------------------
# Тест 11.6: settings.campaign_crm_max_concurrency используется для семафора
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_candidates_uses_settings_concurrency(session_maker, monkeypatch) -> None:
    """find_candidates создаёт Semaphore с settings.campaign_crm_max_concurrency."""
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)

    captured_concurrency: list[int] = []
    original_semaphore = asyncio.Semaphore

    def mock_semaphore(n):
        captured_concurrency.append(n)
        return original_semaphore(n)

    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=_next_id(),
                phone_e164="+491234567890",
                raw={},
            )
            session.add(client)
            await session.flush()
            rec = Record(
                company_id=COMPANY,
                altegio_record_id=_next_id(),
                client_id=client.id,
                starts_at=PERIOD_START + timedelta(days=5),
                confirmed=1,
                is_deleted=False,
            )
            session.add(rec)

    fake_settings = SimpleNamespace(campaign_crm_max_concurrency=3)

    with (
        patch("altegio_bot.campaigns.segment.settings", fake_settings),
        patch("altegio_bot.campaigns.segment.asyncio.Semaphore", side_effect=mock_semaphore),
        patch(
            "altegio_bot.campaigns.segment.get_client_crm_records",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(return_value=False),
        ),
    ):
        await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    assert captured_concurrency == [3], f"Ожидался Semaphore(3), вызвано с {captured_concurrency}"


# ---------------------------------------------------------------------------
# Тест 11.6-extra: валидатор campaign_crm_max_concurrency в settings
# ---------------------------------------------------------------------------


def test_campaign_crm_max_concurrency_validator_rejects_zero() -> None:
    """Settings отвергают campaign_crm_max_concurrency=0 при инициализации."""
    import pytest
    from pydantic import ValidationError

    # Создаём изолированный Settings только с нужным полем, чтобы не зависеть
    # от DATABASE_URL и других обязательных полей продакшн-класса.
    from altegio_bot.settings import Settings

    with pytest.raises((ValidationError, ValueError)):
        Settings(
            database_url="postgresql+asyncpg://test/test",
            altegio_webhook_secret="secret",
            campaign_crm_max_concurrency=0,
        )


def test_campaign_crm_max_concurrency_validator_rejects_negative() -> None:
    """Settings отвергают отрицательные значения campaign_crm_max_concurrency."""
    import pytest
    from pydantic import ValidationError

    from altegio_bot.settings import Settings

    with pytest.raises((ValidationError, ValueError)):
        Settings(
            database_url="postgresql+asyncpg://test/test",
            altegio_webhook_secret="secret",
            campaign_crm_max_concurrency=-5,
        )


def test_campaign_crm_max_concurrency_validator_accepts_one() -> None:
    """campaign_crm_max_concurrency=1 — минимально допустимое значение."""
    from altegio_bot.settings import Settings

    s = Settings(
        database_url="postgresql+asyncpg://test/test",
        altegio_webhook_secret="secret",
        campaign_crm_max_concurrency=1,
    )
    assert s.campaign_crm_max_concurrency == 1


# ---------------------------------------------------------------------------
# Тест: gather резервная ветка — BaseException превращается в excluded candidate
# ---------------------------------------------------------------------------


class _TestBaseException(BaseException):
    """Тестовый BaseException, который не поймает except Exception."""


@pytest.mark.asyncio
async def test_gather_base_exception_produces_excluded_candidate(session_maker, monkeypatch) -> None:
    """Если BaseException пробивается через _process_one_client, клиент не теряется.

    Ожидаемое поведение:
    - gather с return_exceptions=True ловит BaseException в results
    - task_snapshots[i] известен → создаётся excluded candidate
    - клиент присутствует в итоге как crm_history_unavailable, не дропается
    """
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)

    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=_next_id(),
                phone_e164="+491234567890",
                raw={},
            )
            session.add(client)
            await session.flush()
            rec = Record(
                company_id=COMPANY,
                altegio_record_id=_next_id(),
                client_id=client.id,
                starts_at=PERIOD_START + timedelta(days=5),
                confirmed=1,
                is_deleted=False,
            )
            session.add(rec)

    # Патчим _process_one_client так, чтобы он бросал BaseException.
    # outer except Exception не поймает _TestBaseException.
    # asyncio.gather с return_exceptions=True положит его в results.
    async def raises_base_exception(http, sem, snapshot, company_id, period_start, period_end):
        raise _TestBaseException("simulated unhandled base error")

    monkeypatch.setattr(segment_module, "_process_one_client", raises_base_exception)

    results = await find_candidates(
        company_id=COMPANY,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
    )

    # Клиент не должен быть потерян — должен стать excluded
    assert len(results) == 1
    assert results[0].excluded_reason == "crm_history_unavailable"
    assert isinstance(results[0].client, ClientSnapshot)


# ---------------------------------------------------------------------------
# Тест: _normalise_nullable_str — trim и пробельные строки
# ---------------------------------------------------------------------------


def test_normalise_nullable_str_with_whitespace() -> None:
    """_normalise_nullable_str: trim + пробельные строки → None."""
    from altegio_bot.ops.campaigns_api import _normalise_nullable_str

    assert _normalise_nullable_str("") is None
    assert _normalise_nullable_str("   ") is None
    assert _normalise_nullable_str(" abc ") == "abc"
    assert _normalise_nullable_str("0") == "0"
    assert _normalise_nullable_str(None) is None
    assert _normalise_nullable_str("abc") == "abc"
