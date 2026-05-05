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
from dataclasses import dataclass, field
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


@dataclass
class LashRecordFilterResult:
    """Result of filter_lash_record_ids, including service lookup diagnostics.

    lookup_failed_service_ids: service IDs for which Altegio API lookup failed.
    lookup_failed_record_ids: records excluded because lookup failed for their
        service(s) and none of their services were confirmed lash.
    """

    lash_record_ids: set[int] = field(default_factory=set)
    lookup_failed_service_ids: set[int] = field(default_factory=set)
    lookup_failed_record_ids: set[int] = field(default_factory=set)


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
    strict: bool = False,
) -> int | None:
    """Получить category_id услуги через Altegio API.

    Args:
        http_client: если передан — переиспользуется (keep-alive соединения).
                     Если None — создаётся новый клиент для одного запроса.
        strict:      если True и токены не настроены — raise ServiceLookupError
                     вместо тихого None. Используется в recompute attribution,
                     где отсутствующие credentials должны быть явно видны
                     в diagnostics, а не маскироваться как «не lash».
    """
    tokens = _get_altegio_tokens()
    if tokens is None:
        if strict:
            raise ServiceLookupError(
                f"altegio service lookup unavailable: credentials missing "
                f"(ALTEGIO_PARTNER_TOKEN or ALTEGIO_USER_TOKEN not set) "
                f"company={company_id} service={service_id}"
            )
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

    # Невалидный JSON или неожиданная структура ответа — это ServiceLookupError,
    # а не «услуга не lash». Нельзя допускать путь:
    #   malformed response → return None → is_lash=False → ложно-eligible клиент.
    try:
        payload: Any = resp.json()
    except Exception as exc:
        raise ServiceLookupError(
            f"altegio service lookup invalid JSON: company={company_id} service={service_id}: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise ServiceLookupError(
            f"altegio service lookup unexpected payload type {type(payload).__name__}: "
            f"company={company_id} service={service_id}"
        )

    data = payload.get("data")
    if data is not None and not isinstance(data, dict):
        raise ServiceLookupError(
            f"altegio service lookup unexpected 'data' type {type(data).__name__}: "
            f"company={company_id} service={service_id}"
        )

    if data is None:
        # Ответ валидный, но поле data отсутствует — услуга не найдена.
        # Трактуем как «не lash» (не ошибка lookup).
        return None

    category_id = data.get("category_id")
    if category_id is None:
        category_id = data.get("categoryId")

    if isinstance(category_id, int):
        return category_id
    if isinstance(category_id, str) and category_id.isdigit():
        return int(category_id)

    # category_id отсутствует или непарсируемый — услуга не имеет категории.
    # Это валидный ответ API: услуга существует, но категория не назначена → не lash.
    return None


async def is_lash_service(
    company_id: int,
    service_id: int,
    http_client: httpx.AsyncClient | None = None,
    *,
    strict_lookup: bool = False,
) -> bool:
    """Вернуть True, если service_id принадлежит категории ресниц для компании.

    Использует LRU-кеш и Altegio API для получения category_id.

    Args:
        http_client:    если передан — переиспользуется для API-запроса.
                        Позволяет избежать лишних TCP/TLS handshake при массовом
                        обходе services (батч сегментации кампании).
        strict_lookup:  если True, отсутствующие API-credentials поднимают
                        ServiceLookupError вместо тихого False.
                        Используется из filter_lash_record_ids (recompute path)
                        чтобы missing credentials были явно видны в diagnostics.

    Raises:
        ServiceLookupError: при сетевой ошибке, HTTP-ошибке (не 404),
            или при strict_lookup=True когда credentials отсутствуют.
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
            strict=strict_lookup,
        )
        if fetched is None:
            # 404 or no category_id in response — definitively not lash
            return False
        _cache_put(key, fetched)
        category_id = fetched

    return category_id in allowed


async def filter_lash_record_ids(
    session: AsyncSession,
    *,
    company_id: int,
    record_ids: list[int],
    http_client: httpx.AsyncClient | None = None,
) -> LashRecordFilterResult:
    """Return lash record_ids and service lookup diagnostics.

    Queries RecordService in one batch, then resolves service categories via
    the process-local LRU cache; cache misses trigger an Altegio API call
    (using http_client if provided, otherwise a single-shot client per call).

    Booked-after semantics: a booking counts only when it contains ≥1 lash
    service. This function is the authoritative lash gate for recompute
    attribution — it does NOT use title substrings.

    ServiceLookupError for a service_id:
    - That service is treated as 'unknown'. A record is included only if
      another of its services is confirmed lash.
    - A warning is logged per affected service_id; the error is not propagated.
    - Affected service_ids and record_ids are returned in LashRecordFilterResult
      for caller diagnostics.

    Args:
        session:    async DB session (read-only use within existing transaction).
        company_id: Altegio company ID (key for LASH_CATEGORY_IDS_BY_COMPANY).
        record_ids: Record.id (PK) values to inspect.
        http_client: if provided, reused for Altegio API calls (keep-alive).

    Returns:
        LashRecordFilterResult with confirmed lash record_ids plus lookup failure
        diagnostics (failed service_ids and the record_ids affected by them).
    """
    empty = LashRecordFilterResult()
    if not record_ids:
        return empty

    allowed = LASH_CATEGORY_IDS_BY_COMPANY.get(company_id)
    if not allowed:
        return empty

    # Batch query: all services for the candidate records
    stmt = select(RecordService.record_id, RecordService.service_id).where(RecordService.record_id.in_(record_ids))
    rows = (await session.execute(stmt)).all()

    # Build record → service_ids mapping and collect all unique service_ids
    record_svcs: dict[int, set[int]] = {}
    all_svc_ids: set[int] = set()
    for row in rows:
        record_svcs.setdefault(row.record_id, set()).add(row.service_id)
        all_svc_ids.add(row.service_id)

    # Resolve each unique service_id once (cache + optional API).
    # strict_lookup=True: missing credentials surface as ServiceLookupError
    # instead of silent False, so operators see diagnostics rather than
    # an authoritative-looking undercount.
    lash_svc: set[int] = set()
    unknown_svc: set[int] = set()
    for svc_id in all_svc_ids:
        try:
            if await is_lash_service(company_id, svc_id, http_client, strict_lookup=True):
                lash_svc.add(svc_id)
        except ServiceLookupError:
            unknown_svc.add(svc_id)
            logger.warning(
                "recompute booked-after: service category lookup failed "
                "company_id=%d service_id=%d — record excluded from lash attribution",
                company_id,
                svc_id,
            )

    # Classify each record:
    # - confirmed lash if any service is in lash_svc
    # - lookup-failed if none confirmed lash and at least one service in unknown_svc
    lash_record_ids: set[int] = set()
    failed_record_ids: set[int] = set()
    for rec_id in record_ids:
        svc_ids = record_svcs.get(rec_id, set())
        if any(s in lash_svc for s in svc_ids):
            lash_record_ids.add(rec_id)
        elif any(s in unknown_svc for s in svc_ids):
            failed_record_ids.add(rec_id)

    return LashRecordFilterResult(
        lash_record_ids=lash_record_ids,
        lookup_failed_service_ids=set(unknown_svc),
        lookup_failed_record_ids=failed_record_ids,
    )


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
