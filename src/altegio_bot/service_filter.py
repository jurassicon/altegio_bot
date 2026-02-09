from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import RecordService

# Пока хардкодим список “ресниц”.
# Позже вынесем в таблицу/конфиг.
LASH_SERVICE_IDS_BY_COMPANY: dict[int, set[int]] = {
    758285: {12859821},  # пример: nails/ресницы (переименуешь как нужно)
    # 1271200: {...},
}


async def record_has_allowed_service(
    session: AsyncSession,
    *,
    company_id: int,
    record_id: int,
) -> bool:
    allowed = LASH_SERVICE_IDS_BY_COMPANY.get(company_id)
    if not allowed:
        return False

    stmt = (
        select(RecordService.service_id)
        .where(RecordService.record_id == record_id)
    )
    res = await session.execute(stmt)
    service_ids = res.scalars().all()

    return any(sid in allowed for sid in service_ids)
