"""Тесты: attendance («Пришел») как критерий eligible в сегментации.

Ключевые сценарии:
  A. confirmed=1, attendance=0 → НЕ eligible (false positive fix — кейс Sabrina Richter).
     Клиент с подтверждённой, но непосещённой записью НЕ должен получать рассылку.

  B. confirmed=1, attendance=1 → eligible.
     Реально новый клиент с одним мартовским визитом («Пришел»).

  C. confirmed=0, attendance=0 → НЕ eligible (no_confirmed_lash_record_in_period).
     Отменённая запись тоже не даёт права на рассылку.

  D. Debug endpoint: возвращает понятную диагностику.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.segment as segment_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.campaigns.altegio_crm import CrmRecord, CrmUnavailableError
from altegio_bot.campaigns.segment import find_candidates
from altegio_bot.main import app
from altegio_bot.models.models import Client, Record
from altegio_bot.ops.auth import require_ops_auth

PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)
COMPANY = 758285

LASH_SVC_ID = 99001
NON_LASH_SVC_ID = 99002

_NEXT_ID = 20_000


def _next_id() -> int:
    global _NEXT_ID
    _NEXT_ID += 1
    return _NEXT_ID


def _make_client(**kw) -> Client:
    defaults = dict(
        company_id=COMPANY,
        altegio_client_id=_next_id(),
        phone_e164="+4915100000001",
        raw={},
    )
    defaults.update(kw)
    return Client(**defaults)


def _make_record(client_id: int, *, days_offset: int = 5, **kw) -> Record:
    defaults = dict(
        company_id=COMPANY,
        altegio_record_id=_next_id(),
        client_id=client_id,
        starts_at=PERIOD_START + timedelta(days=days_offset),
        confirmed=1,
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
) -> CrmRecord:
    return CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START + timedelta(days=days_offset),
        confirmed=confirmed,
        attendance=attendance,
        deleted=deleted,
        service_ids=service_ids or [LASH_SVC_ID],
        service_titles=["Wimpernverlängerung"],
    )


def _patch_crm(records: list[CrmRecord] | None = None, *, raise_error: bool = False):
    if raise_error:
        return patch(
            "altegio_bot.campaigns.segment.get_client_crm_records",
            new=AsyncMock(side_effect=CrmUnavailableError("CRM down")),
        )
    return patch(
        "altegio_bot.campaigns.segment.get_client_crm_records",
        new=AsyncMock(return_value=records or []),
    )


def _patch_lash(lash_ids: set[int] | None = None):
    ids = lash_ids if lash_ids is not None else {LASH_SVC_ID}
    return patch(
        "altegio_bot.campaigns.segment.is_lash_service",
        new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id in ids),
    )


@pytest_asyncio.fixture
async def patched_db(session_maker, monkeypatch):
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)
    return session_maker


# ---------------------------------------------------------------------------
# Тест A: confirmed=1, attendance=0 → НЕ eligible (кейс Sabrina Richter)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_confirmed_but_not_attended_is_not_eligible(patched_db) -> None:
    """Клиент с confirmed=1 и attendance=0 НЕ должен получать рассылку.

    Это исправление false positive: confirmed=1 означает только «запись не
    отменена», а НЕ «клиент пришёл». Пример: Sabrina Richter — запись в CRM
    активна (confirmed=1), но «Пришел» не отмечен (attendance=0).
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    # CRM: запись подтверждена, но клиент не пришёл
    crm_records = [_crm_record(confirmed=1, attendance=0, service_ids=[LASH_SVC_ID])]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None, "Клиент должен присутствовать в results (как excluded)"
    assert not match.is_eligible, (
        "Клиент с confirmed=1, attendance=0 НЕ должен быть eligible — он не пришёл на приём (нет статуса «Пришел»)"
    )
    assert match.excluded_reason == "no_confirmed_lash_record_in_period"
    assert match.lash_records_in_period == 1
    assert match.confirmed_lash_records_in_period == 0


# ---------------------------------------------------------------------------
# Тест B: confirmed=1, attendance=1 → eligible (реально новый клиент)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_attended_new_client_is_eligible(patched_db) -> None:
    """Реально новый клиент с одной мартовской lash-записью со статусом «Пришел»."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    # CRM: ровно 1 запись, клиент пришёл, нет истории до периода
    crm_records = [_crm_record(confirmed=1, attendance=1, service_ids=[LASH_SVC_ID])]

    with _patch_crm(crm_records), _patch_lash():
        result = await find_candidates(
            company_id=COMPANY,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
        )

    match = next((c for c in result if c.client.id == client.id), None)
    assert match is not None
    assert match.is_eligible, f"Новый клиент с attendance=1 должен быть eligible, но: {match.excluded_reason}"
    assert match.lash_records_in_period == 1
    assert match.confirmed_lash_records_in_period == 1
    assert match.records_before_period == 0


# ---------------------------------------------------------------------------
# Тест C: confirmed=0, attendance=0 → НЕ eligible (отменённая запись)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_record_is_not_eligible(patched_db) -> None:
    """Отменённая запись (confirmed=0) тоже не даёт eligibility."""
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    crm_records = [_crm_record(confirmed=0, attendance=0, service_ids=[LASH_SVC_ID])]

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
# Тест D: debug endpoint показывает правильную диагностику
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def debug_http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_debug_endpoint_not_attended(session_maker, debug_http_client) -> None:
    """Debug endpoint корректно показывает, почему клиент без «Пришел» не eligible."""
    altegio_id = _next_id()
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_id,
                phone_e164="+4915199999901",
                display_name="Sabrina Richter",
                raw={},
            )
            session.add(client)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=5),
                    confirmed=1,
                    is_deleted=False,
                )
            )

    crm_resp = [
        CrmRecord(
            crm_id=_next_id(),
            starts_at=PERIOD_START + timedelta(days=5),
            confirmed=1,
            attendance=0,
            deleted=False,
            service_ids=[LASH_SVC_ID],
            service_titles=["Wimpernverlängerung"],
        )
    ]

    with (
        patch("altegio_bot.ops.campaigns_api.get_client_crm_records", new=AsyncMock(return_value=crm_resp)),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
        ),
    ):
        resp = await debug_http_client.get(
            "/ops/campaigns/debug-client",
            params={
                "company_id": COMPANY,
                "phone": "+4915199999901",
                "period_start": PERIOD_START.isoformat(),
                "period_end": PERIOD_END.isoformat(),
            },
        )

    assert resp.status_code == 200
    data = resp.json()

    assert data["phone_e164"] == "+4915199999901"
    assert data["local_client"] is not None
    assert data["local_client"]["altegio_client_id"] == altegio_id

    cl = data["classification"]
    assert cl is not None
    assert cl["count_before_period"] == 0
    assert cl["lash_records_in_period"] == 1
    assert cl["attended_lash_records_in_period"] == 0  # attendance=0

    assert data["eligibility"]["is_eligible"] is False
    assert data["eligibility"]["excluded_reason"] == "no_confirmed_lash_record_in_period"


@pytest.mark.asyncio
async def test_debug_endpoint_attended(session_maker, debug_http_client) -> None:
    """Debug endpoint подтверждает eligible для клиента со статусом «Пришел»."""
    altegio_id = _next_id()
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_id,
                phone_e164="+4915199999902",
                display_name="Anna Müller",
                raw={},
            )
            session.add(client)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=10),
                    confirmed=1,
                    is_deleted=False,
                )
            )

    crm_resp = [
        CrmRecord(
            crm_id=_next_id(),
            starts_at=PERIOD_START + timedelta(days=10),
            confirmed=1,
            attendance=1,
            deleted=False,
            service_ids=[LASH_SVC_ID],
            service_titles=["Wimpernverlängerung"],
        )
    ]

    with (
        patch("altegio_bot.ops.campaigns_api.get_client_crm_records", new=AsyncMock(return_value=crm_resp)),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
        ),
    ):
        resp = await debug_http_client.get(
            "/ops/campaigns/debug-client",
            params={
                "company_id": COMPANY,
                "phone": "+4915199999902",
                "period_start": PERIOD_START.isoformat(),
                "period_end": PERIOD_END.isoformat(),
            },
        )

    assert resp.status_code == 200
    data = resp.json()

    cl = data["classification"]
    assert cl["lash_records_in_period"] == 1
    assert cl["attended_lash_records_in_period"] == 1

    assert data["eligibility"]["is_eligible"] is True
    assert data["eligibility"]["excluded_reason"] is None


@pytest.mark.asyncio
async def test_debug_endpoint_client_not_found(debug_http_client) -> None:
    """Debug endpoint возвращает понятную ошибку для несуществующего клиента."""
    resp = await debug_http_client.get(
        "/ops/campaigns/debug-client",
        params={
            "company_id": COMPANY,
            "phone": "+4915100000000",
            "period_start": PERIOD_START.isoformat(),
            "period_end": PERIOD_END.isoformat(),
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["local_client"] is None
    assert "не найден" in data["error"]
    assert data["eligibility"]["is_eligible"] is False


# ---------------------------------------------------------------------------
# Тест E: visit_attendance fallback → eligible
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_visit_attendance_fallback_makes_eligible(patched_db) -> None:
    """Если attendance отсутствует в CRM, но visit_attendance=1 — клиент eligible.

    Проверяет полный pipeline сегментации: fallback в парсинге CRM
    корректно поднимает attendance=1, что даёт eligible через is_attended.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    # CrmRecord с attendance=1 из visit_attendance fallback (attendance_source="visit_attendance")
    crm_records = [
        CrmRecord(
            crm_id=_next_id(),
            starts_at=PERIOD_START + timedelta(days=5),
            confirmed=1,
            attendance=1,  # уже нормализовано через fallback из visit_attendance
            attendance_source="visit_attendance",
            deleted=False,
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
    assert match.is_eligible, (
        f"Клиент с visit_attendance=1 (fallback) должен быть eligible, но: {match.excluded_reason}"
    )
    assert match.confirmed_lash_records_in_period == 1
    assert match.lash_records_in_period == 1


# ---------------------------------------------------------------------------
# Тест F: visit_attendance=0, attendance отсутствует → excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_visit_attendance_fallback_excluded_when_zero(patched_db) -> None:
    """Если attendance отсутствует и visit_attendance=0 — клиент НЕ eligible.

    Fallback на visit_attendance=0 означает «не пришёл» — excluded.
    """
    async with patched_db() as session:
        async with session.begin():
            client = _make_client(altegio_client_id=_next_id())
            session.add(client)
            await session.flush()
            rec = _make_record(client.id)
            session.add(rec)

    # CrmRecord с attendance=0 из visit_attendance fallback
    crm_records = [
        CrmRecord(
            crm_id=_next_id(),
            starts_at=PERIOD_START + timedelta(days=5),
            confirmed=1,
            attendance=0,  # нормализовано через fallback: visit_attendance=0
            attendance_source="visit_attendance",
            deleted=False,
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
    assert not match.is_eligible
    assert match.excluded_reason == "no_confirmed_lash_record_in_period"
    assert match.confirmed_lash_records_in_period == 0


# ---------------------------------------------------------------------------
# Тест G: debug endpoint показывает raw_crm_fields и attendance_source
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_debug_endpoint_shows_raw_crm_fields(session_maker, debug_http_client) -> None:
    """Debug endpoint раскрывает сырые CRM-поля и attendance_source для каждой записи.

    Это ключевое для расследования mismatch «в CRM 15 клиентов, в preview 3»:
    нужно видеть не только нормализованные данные, но и сырые поля из API.
    """
    altegio_id = _next_id()
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_id,
                phone_e164="+4915199999903",
                display_name="Maria Schmidt",
                raw={},
            )
            session.add(client)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=7),
                    confirmed=1,
                    is_deleted=False,
                )
            )

    # CrmRecord с raw_debug, имитирующим ответ CRM где attendance=None
    # но visit_attendance=1 (fallback сработал)
    crm_resp = [
        CrmRecord(
            crm_id=_next_id(),
            starts_at=PERIOD_START + timedelta(days=7),
            confirmed=1,
            attendance=1,
            attendance_source="visit_attendance",
            deleted=False,
            service_ids=[LASH_SVC_ID],
            service_titles=["Wimpernverlängerung"],
            raw_debug={
                "confirmed": 1,
                "attendance": None,  # сырое поле из CRM — отсутствовало
                "visit_attendance": 1,  # fallback-источник
                "deleted": False,
                "service_ids": [LASH_SVC_ID],
                "service_titles": ["Wimpernverlängerung"],
                "starts_at": "2026-03-08 10:00:00",
            },
        )
    ]

    with (
        patch("altegio_bot.ops.campaigns_api.get_client_crm_records", new=AsyncMock(return_value=crm_resp)),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
        ),
    ):
        resp = await debug_http_client.get(
            "/ops/campaigns/debug-client",
            params={
                "company_id": COMPANY,
                "phone": "+4915199999903",
                "period_start": PERIOD_START.isoformat(),
                "period_end": PERIOD_END.isoformat(),
            },
        )

    assert resp.status_code == 200
    data = resp.json()

    # Классификация должна быть успешной
    assert data["eligibility"]["is_eligible"] is True

    # in_period_records должны содержать raw_crm_fields и attendance_source
    cl = data["classification"]
    assert cl is not None
    assert len(cl["in_period_records"]) == 1

    rec_out = cl["in_period_records"][0]
    assert "raw_crm_fields" in rec_out, "debug output должен содержать raw_crm_fields"
    assert "attendance_source" in rec_out, "debug output должен содержать attendance_source"
    assert rec_out["attendance_source"] == "visit_attendance"

    raw = rec_out["raw_crm_fields"]
    assert "confirmed" in raw
    assert "attendance" in raw
    assert "visit_attendance" in raw
    assert raw["visit_attendance"] == 1
    assert raw["attendance"] is None  # сырое: поле отсутствовало → None

    # attendance_notes должны объяснять, откуда взят attended flag
    assert "attendance_notes" in cl
    notes = cl["attendance_notes"]
    assert len(notes) == 1
    assert notes[0]["attendance_source"] == "visit_attendance"
    assert notes[0]["is_attended"] is True
    assert "visit_attendance" in notes[0]["explanation"]


# ---------------------------------------------------------------------------
# Тест H: invalid datetime format → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_debug_endpoint_invalid_datetime_returns_400(debug_http_client) -> None:
    """Debug endpoint возвращает 400 при невалидном формате datetime.

    Проверяет обработку некорректного периода (не ISO 8601) без паники.
    """
    resp = await debug_http_client.get(
        "/ops/campaigns/debug-client",
        params={
            "company_id": COMPANY,
            "phone": "+4915100000001",
            "period_start": "not-a-date",
            "period_end": PERIOD_END.isoformat(),
        },
    )
    assert resp.status_code == 400
    detail = resp.json().get("detail", "")
    assert "datetime" in detail.lower() or "невалидный" in detail.lower()


# ---------------------------------------------------------------------------
# Тест I: decision_notes присутствует для eligible клиента
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_debug_endpoint_happy_path_has_decision_notes(session_maker, debug_http_client) -> None:
    """Debug endpoint включает decision_notes в ответ для eligible клиента.

    Проверяет полноту ответа: raw_crm_fields, attendance_notes, decision_notes.
    Это «golden path» для диагностики new-clients сегментации.
    """
    altegio_id = _next_id()
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_id,
                phone_e164="+4915199999904",
                display_name="Lisa Weber",
                raw={},
            )
            session.add(client)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=3),
                    confirmed=1,
                    is_deleted=False,
                )
            )

    crm_resp = [
        CrmRecord(
            crm_id=_next_id(),
            starts_at=PERIOD_START + timedelta(days=3),
            confirmed=1,
            attendance=1,
            attendance_source="attendance",
            deleted=False,
            service_ids=[LASH_SVC_ID],
            service_titles=["Wimpernverlängerung"],
            raw_debug={
                "confirmed": 1,
                "attendance": 1,
                "visit_attendance": None,
                "deleted": False,
                "service_ids": [LASH_SVC_ID],
                "service_titles": ["Wimpernverlängerung"],
                "starts_at": "2026-03-04 10:00:00",
            },
        )
    ]

    with (
        patch("altegio_bot.ops.campaigns_api.get_client_crm_records", new=AsyncMock(return_value=crm_resp)),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
        ),
    ):
        resp = await debug_http_client.get(
            "/ops/campaigns/debug-client",
            params={
                "company_id": COMPANY,
                "phone": "+4915199999904",
                "period_start": PERIOD_START.isoformat(),
                "period_end": PERIOD_END.isoformat(),
            },
        )

    assert resp.status_code == 200
    data = resp.json()

    # --- eligibility ---
    elig = data["eligibility"]
    assert elig["is_eligible"] is True
    assert elig["excluded_reason"] is None
    assert "decision_notes" in elig, "eligibility должен содержать decision_notes"
    assert "eligible" in elig["decision_notes"]

    # --- classification ---
    cl = data["classification"]
    assert cl is not None
    assert cl["lash_records_in_period"] == 1
    assert cl["attended_lash_records_in_period"] == 1
    assert cl["count_before_period"] == 0

    # attendance_notes присутствует и информативен
    assert "attendance_notes" in cl
    assert len(cl["attendance_notes"]) == 1
    note = cl["attendance_notes"][0]
    assert note["is_attended"] is True
    assert note["attendance_source"] == "attendance"
    assert "attendance" in note["explanation"]

    # in_period_records содержит raw_crm_fields
    assert len(cl["in_period_records"]) == 1
    rec_out = cl["in_period_records"][0]
    assert "raw_crm_fields" in rec_out
    assert "attendance_source" in rec_out
    raw = rec_out["raw_crm_fields"]
    assert raw["confirmed"] == 1
    assert raw["attendance"] == 1


# ---------------------------------------------------------------------------
# Тест J: decision_notes для excluded клиента
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_debug_endpoint_excluded_has_decision_notes(session_maker, debug_http_client) -> None:
    """Debug endpoint включает понятный decision_notes для excluded клиента.

    Позволяет оператору без чтения кода понять, почему клиент не eligible.
    """
    altegio_id = _next_id()
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_id,
                phone_e164="+4915199999905",
                display_name="Petra König",
                raw={},
            )
            session.add(client)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=client.id,
                    starts_at=PERIOD_START + timedelta(days=2),
                    confirmed=1,
                    is_deleted=False,
                )
            )

    # Клиент пришёл, но у него есть история до периода
    crm_resp_before = CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START - timedelta(days=40),  # до периода
        confirmed=1,
        attendance=1,
        attendance_source="attendance",
        deleted=False,
        service_ids=[LASH_SVC_ID],
        service_titles=["Wimpernverlängerung"],
    )
    crm_resp_in = CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START + timedelta(days=2),
        confirmed=1,
        attendance=1,
        attendance_source="attendance",
        deleted=False,
        service_ids=[LASH_SVC_ID],
        service_titles=["Wimpernverlängerung"],
    )

    with (
        patch(
            "altegio_bot.ops.campaigns_api.get_client_crm_records",
            new=AsyncMock(return_value=[crm_resp_before, crm_resp_in]),
        ),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
        ),
    ):
        resp = await debug_http_client.get(
            "/ops/campaigns/debug-client",
            params={
                "company_id": COMPANY,
                "phone": "+4915199999905",
                "period_start": PERIOD_START.isoformat(),
                "period_end": PERIOD_END.isoformat(),
            },
        )

    assert resp.status_code == 200
    data = resp.json()

    elig = data["eligibility"]
    assert elig["is_eligible"] is False
    assert elig["excluded_reason"] == "has_records_before_period"
    assert "decision_notes" in elig
    assert "excluded" in elig["decision_notes"]
    assert "has_records_before_period" in elig["decision_notes"]


# ===========================================================================
# Batch debug endpoint: /ops/campaigns/debug-clients-batch
# ===========================================================================


def _batch_body(phones: list[str]) -> dict:
    return {
        "company_id": COMPANY,
        "period_start": PERIOD_START.isoformat(),
        "period_end": PERIOD_END.isoformat(),
        "phones": phones,
    }


def _crm_record_eligible() -> CrmRecord:
    """CrmRecord, который даёт eligible при одной lash-записи за период."""
    return CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START + timedelta(days=5),
        confirmed=1,
        attendance=1,
        attendance_source="attendance",
        deleted=False,
        service_ids=[LASH_SVC_ID],
        service_titles=["Wimpernverlängerung"],
    )


# ---------------------------------------------------------------------------
# K: основной сценарий расследования — mix клиентов
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_debug_mix_of_clients(session_maker, debug_http_client) -> None:
    """Batch endpoint корректно диагностирует клиентов трёх категорий одновременно:

      1. Eligible клиент — в локальной БД, запись за период есть, CRM подтверждает.
      2. not_in_local_db — телефон не известен локальной БД (Discovery Gap!).
      3. Excluded — в локальной БД, но history до периода есть.

    Это ключевой сценарий расследования mismatch «в CRM 15, в preview 3».
    """
    altegio_eligible = _next_id()
    altegio_excluded = _next_id()

    async with session_maker() as session:
        async with session.begin():
            # Клиент 1: eligible (новый, пришёл, нет истории)
            c_eligible = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_eligible,
                phone_e164="+4915100000010",
                display_name="Anna Neu",
                raw={},
            )
            session.add(c_eligible)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=c_eligible.id,
                    starts_at=PERIOD_START + timedelta(days=5),
                    confirmed=1,
                    is_deleted=False,
                )
            )

            # Клиент 2: в локальной БД, есть запись, но history до периода
            c_excluded = Client(
                company_id=COMPANY,
                altegio_client_id=altegio_excluded,
                phone_e164="+4915100000011",
                display_name="Petra Alt",
                raw={},
            )
            session.add(c_excluded)
            await session.flush()
            session.add(
                Record(
                    company_id=COMPANY,
                    altegio_record_id=_next_id(),
                    client_id=c_excluded.id,
                    starts_at=PERIOD_START + timedelta(days=3),
                    confirmed=1,
                    is_deleted=False,
                )
            )

    # Клиент 3: +4915100000012 — в БД нет вообще (Discovery Gap)

    # CRM mocks:
    # - eligible: одна запись в периоде, attendance=1, нет истории до периода
    crm_eligible = [_crm_record_eligible()]
    # - excluded: одна запись в периоде + одна до периода
    crm_before = CrmRecord(
        crm_id=_next_id(),
        starts_at=PERIOD_START - timedelta(days=30),
        confirmed=1,
        attendance=1,
        attendance_source="attendance",
        deleted=False,
        service_ids=[LASH_SVC_ID],
        service_titles=["Wimpernverlängerung"],
    )
    crm_in = _crm_record_eligible()
    crm_excluded = [crm_before, crm_in]

    async def _crm_side_effect(http, *, company_id, altegio_client_id):
        if altegio_client_id == altegio_eligible:
            return crm_eligible
        return crm_excluded

    with (
        patch(
            "altegio_bot.ops.campaigns_api.get_client_crm_records",
            side_effect=_crm_side_effect,
        ),
        patch(
            "altegio_bot.campaigns.segment.is_lash_service",
            new=AsyncMock(side_effect=lambda company_id, svc_id, http_client=None: svc_id == LASH_SVC_ID),
        ),
    ):
        resp = await debug_http_client.post(
            "/ops/campaigns/debug-clients-batch",
            json=_batch_body(["+4915100000010", "+4915100000011", "+4915100000012"]),
        )

    assert resp.status_code == 200
    data = resp.json()

    summary = data["summary"]
    assert summary["total_phones"] == 3
    assert summary["eligible"] == 1
    assert summary["excluded"] == 2

    # Discovery gap правильно считается
    disc = summary["discovery_status_counts"]
    assert disc.get("in_local_db_with_records", 0) == 2
    assert disc.get("not_in_local_db", 0) == 1

    # not_in_local_db должна быть одной из причин исключения в excluded_by_reason
    exc_reasons = summary["excluded_by_reason"]
    assert exc_reasons.get("not_in_local_db", 0) == 1
    assert exc_reasons.get("has_records_before_period", 0) == 1

    results = {r["phone_e164"]: r for r in data["results"] if r.get("phone_e164")}

    # Клиент 1: eligible
    r1 = results["+4915100000010"]
    assert r1["is_eligible"] is True
    assert r1["discovery_status"] == "in_local_db_with_records"
    assert r1["local_records_in_period"] == 1
    assert r1["crm_in_period"] == 1
    assert r1["crm_before_period"] == 0
    assert r1["lash_in_period"] == 1
    assert r1["attended_lash_in_period"] == 1

    # Клиент 2: excluded по истории
    r2 = results["+4915100000011"]
    assert r2["is_eligible"] is False
    assert r2["excluded_reason"] == "has_records_before_period"
    assert r2["discovery_status"] == "in_local_db_with_records"
    assert r2["crm_before_period"] == 1

    # Клиент 3: Discovery Gap
    r3 = results["+4915100000012"]
    assert r3["is_eligible"] is False
    assert r3["excluded_reason"] == "not_in_local_db"
    assert r3["discovery_status"] == "not_in_local_db"
    assert r3["local_client_found"] is False
    assert r3["crm_records_total"] is None  # CRM не вызывался
    assert "не обнаружен" in r3["decision_notes"] or "not_in_local_db" in r3["excluded_reason"]


# ---------------------------------------------------------------------------
# L: невалидный телефон в батче
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_debug_invalid_phone_handled_gracefully(debug_http_client) -> None:
    """Невалидный телефон в батче не ронит весь запрос.

    Возвращает discovery_status='invalid_phone' для проблемного номера,
    остальные обрабатываются нормально.
    """
    with patch(
        "altegio_bot.ops.campaigns_api.get_client_crm_records",
        new=AsyncMock(return_value=[]),
    ):
        resp = await debug_http_client.post(
            "/ops/campaigns/debug-clients-batch",
            json=_batch_body(["not-a-phone", "+4915100000099"]),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["summary"]["total_phones"] == 2

    results_by_input = {r["phone_input"]: r for r in data["results"]}

    invalid = results_by_input["not-a-phone"]
    assert invalid["discovery_status"] == "invalid_phone"
    assert invalid["is_eligible"] is False
    assert invalid["excluded_reason"] == "invalid_phone"

    # Валидный телефон обработан (клиент не найден в БД — not_in_local_db)
    valid = results_by_input["+4915100000099"]
    assert valid["phone_e164"] == "+4915100000099"
    assert valid["discovery_status"] == "not_in_local_db"


# ---------------------------------------------------------------------------
# M: невалидный datetime → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_debug_invalid_period_returns_400(debug_http_client) -> None:
    """Batch endpoint возвращает 400 при невалидном формате datetime."""
    resp = await debug_http_client.post(
        "/ops/campaigns/debug-clients-batch",
        json={
            "company_id": COMPANY,
            "period_start": "not-a-date",
            "period_end": PERIOD_END.isoformat(),
            "phones": ["+4915100000001"],
        },
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# N: порядок результатов совпадает с порядком входных телефонов
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_debug_preserves_phone_order(session_maker, debug_http_client) -> None:
    """Результаты возвращаются в том же порядке, что входные телефоны.

    Важно для UI: оператор видит ту же последовательность, что и в CRM-скрине.
    """
    phones_input = [
        "+4915100000020",  # не в БД
        "+4915100000021",  # не в БД
        "+4915100000022",  # не в БД
    ]

    with patch(
        "altegio_bot.ops.campaigns_api.get_client_crm_records",
        new=AsyncMock(return_value=[]),
    ):
        resp = await debug_http_client.post(
            "/ops/campaigns/debug-clients-batch",
            json=_batch_body(phones_input),
        )

    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 3
    for i, phone in enumerate(phones_input):
        assert results[i]["phone_input"] == phone, (
            f"Позиция {i}: ожидали {phone!r}, получили {results[i]['phone_input']!r}"
        )
