from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import (
    RecordService,
    ServiceSenderRule,
    WhatsAppSender
)


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
        return 'default'

    stmt = (
        select(ServiceSenderRule.sender_code)
        .where(ServiceSenderRule.company_id == company_id)
        .where(ServiceSenderRule.service_id == service_id)
    )
    res = await session.execute(stmt)
    sender_code = res.scalar_one_or_none()
    return sender_code or 'default'


async def pick_sender_code(
        session: AsyncSession, company_id: int, sender_code: str = 'default'
) -> int | None:
    stmp = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.company_id == company_id)
        .where(WhatsAppSender.sender_code == sender_code)
        .where(WhatsAppSender.is_active.is_(True))
    )

    res = await session.execute(stmp)
    default = res.scalar_one_or_none()

    if default is not None:
        return int(default)
    return None


async def pick_sender_id(
        session: AsyncSession, company_id: int, sender_code: str
) -> int | None:
    stmp = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.company_id == company_id)
        .where(WhatsAppSender.sender_code == sender_code)
        .where(WhatsAppSender.is_active.is_(True))
    )

    res = await session.execute(stmp)
    sender_id = res.scalar_one_or_none()

    if sender_id is not None:
        return int(sender_id)
    if sender_code == 'default':
        return None

    return await pick_sender_code(session, company_id, 'default')
