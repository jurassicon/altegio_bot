"""Altegio CRM API: чтение истории клиентов для сегментации.

Используется в campaigns/segment.py для проверки наличия записей ДО
начала периода — именно это и есть «старый клиент» по бизнес-правилу.

Источник истины: Altegio CRM API (endpoint /records/{company_id}).
Локальная БД records не используется для этого шага:
  - bot работает меньше месяца → история до его запуска в локальной БД отсутствует.

Authorization: Bearer {partner_token},{user_token}
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

_PAGE_SIZE = 200
_ALTEGIO_LOCAL_TZ = ZoneInfo("Europe/Belgrade")


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


def _parse_record_starts_at(record_data: dict[str, Any]) -> datetime | None:
    """Распарсить дату начала записи из ответа Altegio API → UTC.

    Приоритет: поле date (наивное локальное время Europe/Belgrade).
    Fallback: первые 19 символов поля datetime.
    None при любой ошибке парсинга.
    """
    raw_date = record_data.get("date")
    if isinstance(raw_date, str) and raw_date.strip():
        try:
            naive_dt = datetime.fromisoformat(raw_date.strip().replace(" ", "T"))
            return naive_dt.replace(tzinfo=_ALTEGIO_LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    raw_datetime = record_data.get("datetime")
    if isinstance(raw_datetime, str) and len(raw_datetime) >= 19:
        try:
            naive_dt = datetime.fromisoformat(raw_datetime[:19])
            return naive_dt.replace(tzinfo=_ALTEGIO_LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    if raw_date or raw_datetime:
        logger.warning(
            "crm record id=%s: cannot parse date=%r / datetime=%r",
            record_data.get("id"),
            raw_date,
            raw_datetime,
        )
    return None


async def count_client_records_before_period(
    *,
    company_id: int,
    altegio_client_id: int,
    period_start: datetime,
    timeout_sec: float = 25.0,
) -> int:
    """Подсчитать все записи клиента ДО period_start из Altegio CRM API.

    Считаются ВСЕ записи — любого статуса, включая удалённые и
    неподтверждённые.  Любая запись до периода означает «старый клиент».

    Возвращает 0 при сетевых ошибках (conservative fallback): если мы не
    можем проверить историю, безопаснее считать клиента новым, чем
    заблокировать его.  Ошибка логируется как WARNING.

    При явном HTTP 4xx (кроме 429) — также возвращает 0 с WARNING.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/records/{company_id}"
    page = 1
    total = 0

    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as http:
            while True:
                params: dict[str, Any] = {
                    "client_id": altegio_client_id,
                    "count": _PAGE_SIZE,
                    "page": page,
                }
                resp = await http.get(url, headers=_headers(), params=params)
                resp.raise_for_status()

                payload = resp.json()
                records: list[dict[str, Any]] = []
                if isinstance(payload, dict):
                    data = payload.get("data")
                    if isinstance(data, list):
                        records = data

                for rec in records:
                    starts_at = _parse_record_starts_at(rec)
                    if starts_at is not None and starts_at < period_start:
                        total += 1

                if len(records) < _PAGE_SIZE:
                    break

                logger.debug(
                    "crm history page=%d company=%d client=%d",
                    page,
                    company_id,
                    altegio_client_id,
                )
                page += 1

    except httpx.HTTPError as exc:
        logger.warning(
            "crm history fetch failed company=%d client=%d: %s — treating as 0 prior records",
            company_id,
            altegio_client_id,
            exc,
        )
        return 0

    return total
