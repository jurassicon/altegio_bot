"""Фильтрация услуг по категориям (lash / не lash).

Использует Altegio API для получения category_id услуги.
Результаты кешируются в LRU-кеше (OrderedDict) для снижения нагрузки на API.

LRU-стратегия вытеснения: при достижении _CACHE_MAX_SIZE вытесняется
самая редко используемая запись (не весь кеш). Это предотвращает
cache stampede, при котором полный .clear() мог бы внезапно вызвать
массовые запросы к API для всех service_id одновременно.
"""

from __future__ import annotations

import logging
import os
from collections import OrderedDict
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import RecordService

logger = logging.getLogger(__name__)


class ServiceLookupError(Exception):
    """Altegio service category lookup failed (network or HTTP error).

    Отличается от «услуга не является ресничной»:
    - ServiceLookupError = API недоступен, ответ не получен.
    - False из is_lash_service = ответ получен, услуга не ресничная.

    Caller в segment.py обязан обработать это исключение и пометить клиента
    excluded_reason='service_category_unavailable', а НЕ считать услугу non-lash.
    """


LASH_CATEGORY_IDS_BY_COMPANY: dict[int, set[int]] = {
    1271200: {10707687, 12414859, 13329127, 13351976},
    758285: {10707687, 12414859, 13329127, 13351956},
}

# ---------------------------------------------------------------------------
# LRU-кеш: (company_id, service_id) → category_id
# ---------------------------------------------------------------------------

_LRU_CACHE: OrderedDict[tuple[int, int], int] = OrderedDict()
_CACHE_MAX_SIZE = 5000


def _cache_get(key: tuple[int, int]) -> int | None:
    """Получить значение из LRU-кеша; обновляет порядок (MRU)."""
    if key in _LRU_CACHE:
        _LRU_CACHE.move_to_end(key)
        return _LRU_CACHE[key]
    return None


def _cache_put(key: tuple[int, int], category_id: int) -> None:
    """Сохранить значение в LRU-кеше.

    Если ключ уже есть — обновляет позицию (MRU).
    При переполнении вытесняет один самый старый элемент (LRU),
    а не очищает весь кеш целиком.
    """
    if key in _LRU_CACHE:
        _LRU_CACHE.move_to_end(key)
        _LRU_CACHE[key] = category_id
        return

    if len(_LRU_CACHE) >= _CACHE_MAX_SIZE:
        # Вытеснить самый старый (наименее используемый) элемент
        _LRU_CACHE.popitem(last=False)

    _LRU_CACHE[key] = category_id


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def _get_api_base_url() -> str:
    base = os.getenv("ALTEGIO_API_BASE_URL", "https://api.alteg.io/api/v1")
    return base.rstrip("/")


def _get_api_accept() -> str:
    return os.getenv("ALTEGIO_API_ACCEPT", "application/vnd.api.v2+json")


def _get_altegio_tokens() -> tuple[str, str] | None:
    partner = (os.getenv("ALTEGIO_PARTNER_TOKEN") or "").strip()
    user = (os.getenv("ALTEGIO_USER_TOKEN") or "").strip()
    if not partner or not user:
        return None
    return partner, user


async def _fetch_service_category_id(
    *,
    company_id: int,
    service_id: int,
    http_client: httpx.AsyncClient | None = None,
) -> int | None:
    """Получить category_id услуги через Altegio API.

    Args:
        http_client: если передан — переиспользуется (keep-alive соединения).
                     Если None — создаётся новый клиент для одного запроса.
    """
    tokens = _get_altegio_tokens()
    if tokens is None:
        return None

    partner_token, user_token = tokens
    url = f"{_get_api_base_url()}/company/{company_id}/services/{service_id}"
    headers = {
        "Accept": _get_api_accept(),
        "Content-Type": "application/json",
        "Authorization": f"Bearer {partner_token}, User {user_token}",
    }

    async def _do_request(client: httpx.AsyncClient) -> httpx.Response:
        return await client.get(url, headers=headers)

    try:
        if http_client is not None:
            resp = await _do_request(http_client)
        else:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await _do_request(client)
    except httpx.HTTPError as exc:
        raise ServiceLookupError(
            f"altegio service lookup network error: company={company_id} service={service_id}: {exc}"
        ) from exc

    if resp.status_code == 404:
        # Service not found — definitively not lash, not an error
        return None

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise ServiceLookupError(
            f"altegio service lookup bad status {resp.status_code}: company={company_id} service={service_id}"
        ) from exc

    try:
        payload: Any = resp.json()
    except ValueError:
        logger.warning(
            "altegio service lookup invalid json company_id=%s service_id=%s",
            company_id,
            service_id,
        )
        return None

    if not isinstance(payload, dict):
        return None

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    category_id = data.get("category_id")
    if category_id is None:
        category_id = data.get("categoryId")

    if isinstance(category_id, int):
        return category_id
    if isinstance(category_id, str) and category_id.isdigit():
        return int(category_id)

    return None


async def is_lash_service(
    company_id: int,
    service_id: int,
    http_client: httpx.AsyncClient | None = None,
) -> bool:
    """Вернуть True, если service_id принадлежит категории ресниц для компании.

    Использует LRU-кеш и Altegio API для получения category_id.

    Args:
        http_client: если передан — переиспользуется для API-запроса.
                     Позволяет избежать лишних TCP/TLS handshake при массовом
                     обходе services (батч сегментации кампании).

    Raises:
        ServiceLookupError: при сетевой ошибке или HTTP-ошибке (не 404).
            Caller обязан обработать это как 'service_category_unavailable',
            а НЕ молча считать услугу non-lash.
    """
    allowed = LASH_CATEGORY_IDS_BY_COMPANY.get(company_id)
    if not allowed:
        return False

    key = (company_id, service_id)
    category_id = _cache_get(key)
    if category_id is None:
        # Может выбросить ServiceLookupError — propagate to caller
        fetched = await _fetch_service_category_id(
            company_id=company_id,
            service_id=service_id,
            http_client=http_client,
        )
        if fetched is None:
            # 404 or no category_id in response — definitively not lash
            return False
        _cache_put(key, fetched)
        category_id = fetched

    return category_id in allowed


async def record_has_allowed_service(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
) -> bool:
    allowed_categories = LASH_CATEGORY_IDS_BY_COMPANY.get(company_id)
    if not allowed_categories:
        return False

    stmt = select(RecordService.service_id).where(RecordService.record_id == record_id)
    res = await session.execute(stmt)
    service_ids = res.scalars().all()

    for sid in service_ids:
        key = (company_id, sid)

        category_id = _cache_get(key)
        if category_id is None:
            fetched = await _fetch_service_category_id(
                company_id=company_id,
                service_id=sid,
            )
            if fetched is None:
                continue
            _cache_put(key, fetched)
            category_id = fetched

        if category_id in allowed_categories:
            return True

    return False
