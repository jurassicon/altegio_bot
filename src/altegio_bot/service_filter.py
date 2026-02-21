from __future__ import annotations

from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import RecordService
from altegio_bot.settings import settings

LASH_CATEGORY_IDS_BY_COMPANY: dict[int, set[int]] = {
    # 10707687 Wimpernverlängerung
    # 12414859 Happy Monday
    # 13329127 Black Friday
    1271200: {10707687, 12414859, 13329127},
    758285: {10707687, 12414859, 13329127},
}

_SERVICE_CATEGORY_CACHE: dict[tuple[int, int], int] = {}


def _has_altegio_tokens() -> bool:
    return bool(settings.altegio_partner_token and settings.altegio_user_token)


async def _fetch_service_category_id(
        *,
        company_id: int,
        service_id: int,
) -> int | None:
    if not _has_altegio_tokens():
        return None

    url = (
        f'{settings.altegio_api_base_url}/company/{company_id}/services/'
        f'{service_id}'
    )
    headers = {
        'Accept': settings.altegio_api_accept,
        'Content-Type': 'application/json',
        'Authorization': (
            f'Bearer {settings.altegio_partner_token}, '
            f'User {settings.altegio_user_token}'
        ),
    }

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
    stmt = (
        select(RecordService.service_id)
        .where(RecordService.record_id == record_id)
    )
    res = await session.execute(stmt)
    service_ids = res.scalars().all()

    allowed_categories = LASH_CATEGORY_IDS_BY_COMPANY.get(company_id)
    if allowed_categories:
        for sid in service_ids:
            key = (company_id, sid)
            category_id = _SERVICE_CATEGORY_CACHE.get(key)
            if category_id is None:
                fetched = await _fetch_service_category_id(
                    company_id=company_id,
                    service_id=sid,
                )
                if fetched is None:
                    continue
                _SERVICE_CATEGORY_CACHE[key] = fetched
                category_id = fetched

            if category_id in allowed_categories:
                return True

    return False
