from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import RecordService, ServiceSenderRule


async def pick_sender_code_for_record(
        session: AsyncSession, company_id: int, record_id: int
) -> str:
    stmp = (
        select(RecordService.service_id)
        .where(RecordService.record_id == record_id)
        .limit(1)
    )
    res = await session.execute(stmp)
    service_id = res.scalar_one_or_none()

    if service_id is None:
        return "default"

    stmt = (
        select(ServiceSenderRule.sender_code)
        .where(ServiceSenderRule.company_id == company_id)
        .where(ServiceSenderRule.service_id == service_id)
    )
    res = await session.execute(stmt)
    sender_code = res.scalar_one_or_none()
    return sender_code or "default"
