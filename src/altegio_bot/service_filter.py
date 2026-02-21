from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import RecordService

logger = logging.getLogger(__name__)

LASH_CATEGORY_IDS_BY_COMPANY: dict[int, set[int]] = {
    # 10707687 Wimpernverlängerung
    # 12414859 Happy Monday
    # 13329127 Black Friday
    1271200: {10707687, 12414859, 13329127},
    758285: {10707687, 12414859, 13329127},
}

_SERVICE_CATEGORY_CACHE: dict[tuple[int, int], int] = {}


def _get_api_base_url() -> str:
    base = os.getenv('ALTEGIO_API_BASE_URL', 'https://api.alteg.io/api/v1')
    return base.rstrip('/')


def _get_api_accept() -> str:
    return os.getenv('ALTEGIO_API_ACCEPT', 'application/vnd.api.v2+json')


def _get_altegio_tokens() -> tuple[str, str] | None:
    partner = (os.getenv('ALTEGIO_PARTNER_TOKEN') or '').strip()
    user = (os.getenv('ALTEGIO_USER_TOKEN') or '').strip()
    if not partner or not user:
        return None
    return partner, user


async def _fetch_service_category_id(
    *,
    company_id: int,
    service_id: int,
) -> int | None:
    tokens = _get_altegio_tokens()
    if tokens is None:
        return None

    partner_token, user_token = tokens

    url = (
        f'{_get_api_base_url()}/company/{company_id}/services/{service_id}'
    )
    headers = {
        'Accept': _get_api_accept(),
        'Content-Type': 'application/json',
        'Authorization': (
            f'Bearer {partner_token}, '
            f'User {user_token}'
        ),
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        logger.warning(
            'alteg.io service lookup failed company_id=%s service_id=%s: %s',
            company_id,
            service_id,
            exc,
        )
        return None

    if resp.status_code == 404:
        return None

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        logger.warning(
            'alteg.io service lookup bad status company_id=%s service_id=%s: %s',
            company_id,
            service_id,
            exc,
        )
        return None

    payload: Any = resp.json()
    if not isinstance(payload, dict):
        return None

    data = payload.get('data')
    if not isinstance(data, dict):
        return None

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

    stmt = (
        select(RecordService.service_id)
        .where(RecordService.record_id == record_id)
    )
    res = await session.execute(stmt)
    service_ids = res.scalars().all()

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