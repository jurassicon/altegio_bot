"""Тесты парсинга и обработки ответов Altegio CRM API.

Проверяет:
- Невалидный JSON → CrmUnavailableError (Fix 6)
- Payload не dict → CrmUnavailableError
- data не list (неожиданный тип) → CrmUnavailableError
- Пустой data (None) → пустой список (не ошибка)
- Нормальный ответ → список CrmRecord
- attendance из основного поля → attendance_source="attendance"
- visit_attendance fallback при отсутствии attendance → attendance_source="visit_attendance"
- оба поля отсутствуют → attendance=0, attendance_source="default"
- raw_debug содержит сырые CRM-поля
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from altegio_bot.campaigns.altegio_crm import CrmUnavailableError, get_client_crm_records


def _make_http_client() -> httpx.AsyncClient:
    """Фиктивный AsyncClient для unit-тестов (не делает реальных запросов)."""
    return MagicMock(spec=httpx.AsyncClient)


def _mock_response(*, json_data=None, status_code: int = 200, raises_json: bool = False):
    """Создать mock-ответ httpx."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code

    if raises_json:
        resp.json.side_effect = ValueError("invalid json")
    else:
        resp.json.return_value = json_data

    resp.raise_for_status.return_value = None
    return resp


async def _call_get_crm_records(mock_response) -> list:
    """Вызвать get_client_crm_records с заданным mock-ответом."""
    http_client = _make_http_client()
    http_client.get = AsyncMock(return_value=mock_response)
    return await get_client_crm_records(http_client, company_id=1271200, altegio_client_id=12345)


# ---------------------------------------------------------------------------
# Тест 1: Невалидный JSON → CrmUnavailableError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_json_raises_crm_unavailable() -> None:
    """resp.json() бросает ValueError → CrmUnavailableError."""
    resp = _mock_response(raises_json=True)
    with pytest.raises(CrmUnavailableError, match="invalid JSON"):
        await _call_get_crm_records(resp)


# ---------------------------------------------------------------------------
# Тест 2: Payload не dict → CrmUnavailableError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_payload_not_dict_raises_crm_unavailable() -> None:
    """CRM вернул не словарь (например, список) → CrmUnavailableError."""
    resp = _mock_response(json_data=[{"id": 1}])  # список, не dict
    with pytest.raises(CrmUnavailableError, match="unexpected payload type"):
        await _call_get_crm_records(resp)


# ---------------------------------------------------------------------------
# Тест 3: data не list → CrmUnavailableError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_data_not_list_raises_crm_unavailable() -> None:
    """CRM вернул payload с data не-список → CrmUnavailableError."""
    resp = _mock_response(json_data={"data": "unexpected_string", "meta": {}})
    with pytest.raises(CrmUnavailableError, match="unexpected 'data' type"):
        await _call_get_crm_records(resp)


# ---------------------------------------------------------------------------
# Тест 4: data = None → пустой список (не ошибка)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_data_none_returns_empty_list() -> None:
    """CRM вернул payload без поля data → пустой список записей."""
    resp = _mock_response(json_data={"success": True})  # нет поля data
    records = await _call_get_crm_records(resp)
    assert records == []


# ---------------------------------------------------------------------------
# Тест 5: Нормальный ответ → список CrmRecord
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_response_returns_crm_records() -> None:
    """Нормальный ответ → список CrmRecord с правильными данными."""
    resp = _mock_response(
        json_data={
            "data": [
                {
                    "id": 101,
                    "date": "2026-01-15 10:00:00",
                    "confirmed": 1,
                    "deleted": False,
                    "services": [{"id": 99001, "title": "Wimpernverlängerung"}],
                }
            ]
        }
    )
    records = await _call_get_crm_records(resp)
    assert len(records) == 1
    assert records[0].crm_id == 101
    assert records[0].is_confirmed is True
    assert records[0].service_ids == [99001]
    assert records[0].service_titles == ["Wimpernverlängerung"]


# ---------------------------------------------------------------------------
# Тест 6: HTTP-ошибка → CrmUnavailableError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_error_raises_crm_unavailable() -> None:
    """httpx.HTTPError → CrmUnavailableError."""
    http_client = _make_http_client()
    http_client.get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
    with pytest.raises(CrmUnavailableError):
        await get_client_crm_records(http_client, company_id=1271200, altegio_client_id=12345)


# ---------------------------------------------------------------------------
# Тесты 7-10: attendance parsing + fallback на visit_attendance
# ---------------------------------------------------------------------------


def _crm_response_with_record(record_fields: dict) -> dict:
    """Сформировать CRM-ответ с одной записью из заданных полей."""
    base = {
        "id": 999,
        "date": "2026-03-15 10:00:00",
        "confirmed": 1,
        "deleted": False,
        "services": [],
    }
    base.update(record_fields)
    return {"data": [base]}


@pytest.mark.asyncio
async def test_attendance_field_is_primary_source() -> None:
    """Если attendance присутствует в ответе CRM — оно используется напрямую.

    attendance_source должен быть "attendance".
    """
    resp = _mock_response(json_data=_crm_response_with_record({"attendance": 1}))
    records = await _call_get_crm_records(resp)
    assert len(records) == 1
    assert records[0].attendance == 1
    assert records[0].attendance_source == "attendance"


@pytest.mark.asyncio
async def test_visit_attendance_fallback_when_attendance_absent() -> None:
    """Если attendance отсутствует, но visit_attendance=1 — клиент считается «Пришел».

    attendance_source должен быть "visit_attendance".
    Это критичный fallback: в ответе Altegio «Пришел» может приходить
    через разные поля в зависимости от версии или типа записи.
    """
    resp = _mock_response(json_data=_crm_response_with_record({"visit_attendance": 1}))
    # Поле attendance явно отсутствует в записи
    records = await _call_get_crm_records(resp)
    assert len(records) == 1
    assert records[0].attendance == 1, "visit_attendance=1 должен давать attendance=1 через fallback"
    assert records[0].attendance_source == "visit_attendance"
    assert records[0].is_attended is True


@pytest.mark.asyncio
async def test_visit_attendance_fallback_zero_when_attendance_absent() -> None:
    """Если attendance отсутствует и visit_attendance=0 — клиент НЕ «Пришел».

    attendance_source = "visit_attendance", attendance = 0.
    """
    resp = _mock_response(json_data=_crm_response_with_record({"visit_attendance": 0}))
    records = await _call_get_crm_records(resp)
    assert len(records) == 1
    assert records[0].attendance == 0
    assert records[0].attendance_source == "visit_attendance"
    assert records[0].is_attended is False


@pytest.mark.asyncio
async def test_default_attendance_when_both_fields_absent() -> None:
    """Если ни attendance, ни visit_attendance не присутствуют — attendance=0, source="default"."""
    resp = _mock_response(json_data=_crm_response_with_record({}))
    # В записи нет ни attendance, ни visit_attendance
    records = await _call_get_crm_records(resp)
    assert len(records) == 1
    assert records[0].attendance == 0
    assert records[0].attendance_source == "default"
    assert records[0].is_attended is False


@pytest.mark.asyncio
async def test_raw_debug_contains_crm_fields() -> None:
    """raw_debug содержит сырые CRM-поля для диагностики mismatch.

    Минимальный набор: confirmed, attendance, visit_attendance, deleted, starts_at.
    """
    resp = _mock_response(
        json_data=_crm_response_with_record(
            {
                "confirmed": 1,
                "attendance": 0,
                "visit_attendance": 1,
                "deleted": False,
                "services": [{"id": 99001, "title": "Wimpernverlängerung"}],
            }
        )
    )
    records = await _call_get_crm_records(resp)
    assert len(records) == 1
    rd = records[0].raw_debug

    assert "confirmed" in rd
    assert "attendance" in rd
    assert "visit_attendance" in rd
    assert "deleted" in rd
    assert "starts_at" in rd
    assert "service_ids" in rd

    # Проверяем, что именно сырые значения (до нормализации)
    assert rd["confirmed"] == 1
    assert rd["attendance"] == 0  # сырое поле — 0, несмотря на visit_attendance=1
    assert rd["visit_attendance"] == 1  # fallback-поле присутствует в raw_debug
