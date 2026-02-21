from __future__ import annotations

from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import RecordService
from altegio_bot.settings import settings

# Категории, которые считаем 'ресницы' (category_id, не service_id).
# IDs категорий одинаковые в сети, но оставляем маппинг по company_id,
# чтобы при необходимости легко разделить правила по филиалам.
LASH_CATEGORY_IDS_BY_COMPANY: dict[int, set[int]] = {
    # 10707687 Wimpernverlängerung
    # 12414859 Happy Monday
    # 13329127 Black Friday
    1271200: {10707687, 12414859, 13329127},
    758285: {10707687, 12414859, 13329127},
}

# Кэш: (company_id, service_id) -> category_id
# _NOT_FOUND означает: сервис не найден или category_id не удалось получить.
_NOT_FOUND = -1
_SERVICE_CATEGORY_CACHE: dict[tuple[int, int], int] = {}


def _get_setting(name: str, default: str = '') -> str:
    value = getattr(settings, name, None)
    if value is None:
        return default
    return str(value)


def _has_altegio_tokens() -> bool:
    partner = _get_setting('altegio_partner_token')
    user = _get_setting('altegio_user_token')
    return bool(partner and user)


def _get_altegio_headers() -> dict[str, str]:
    partner = _get_setting('altegio_partner_token')
    user = _get_setting('altegio_user_token')

    accept = _get_setting('altegio_api_accept', 'application/json')

    return {
        'Accept': accept,
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {partner}, User {user}',
    }


def _get_altegio_base_url() -> str:
    # Ожидаем, что в settings есть altegio_api_base_url.
    # Если нет — возвращаем пустую строку, и запросы не выполняем.
    return _get_setting('altegio_api_base_url').rstrip('/')


async def _fetch_service_category_id(
    *,
    company_id: int,
    service_id: int,
) -> int | None:
    if not _has_altegio_tokens():
        return None

    base_url = _get_altegio_base_url()
    if not base_url:
        return None

    url = f'{base_url}/company/{company_id}/services/{service_id}'
    headers = _get_altegio_headers()

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    payload: Any = resp.json()
    data = payload.get('data') or {}
    category_id = data.get('category_id')

    if isinstance(category_id, int):
        return category_id
    if isinstance(category_id, str) and category_id.isdigit():
        return int(category_id)

    return None


async def record_has_allowed_service(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
) -> bool:
    allowed_categories = LASH_CATEGORY_IDS_BY_COMPANY.get(company_id)
    if not allowed_categories:
        return False

    stmt = select(RecordService.service_id).where(
        RecordService.record_id == record_id
    )
    res = await session.execute(stmt)
    service_ids = res.scalars().all()

    if not service_ids:
        return False

    for service_id in service_ids:
        key = (company_id, int(service_id))
        cached = _SERVICE_CATEGORY_CACHE.get(key)

        if cached is None:
            fetched = await _fetch_service_category_id(
                company_id=company_id,
                service_id=int(service_id),
            )
            if fetched is None:
                _SERVICE_CATEGORY_CACHE[key] = _NOT_FOUND
                continue
            _SERVICE_CATEGORY_CACHE[key] = fetched
            category_id = fetched
        else:
            if cached == _NOT_FOUND:
                continue
            category_id = cached

        if category_id in allowed_categories:
            return True

    return False