from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from altegio_bot.models.models import (
    RecordService,
    ServiceSenderRule,
    WhatsAppSender
)

logger = logging.getLogger(__name__)
logger.info('Starting inbox worker')


async def pick_sender_code_for_record(
        session: AsyncSession, company_id: int, record_id: int
) -> str:
    stmp = (
        select(RecordService.service_id)
        .where(RecordService.record_id == record_id)
        .order_by(RecordService.service_id.asc())
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

    logger.info(
        "Sender code for record_id=%s service_id=%s: %s",
        record_id,
        service_id,
        sender_code,
    )

    return sender_code or 'default'


async def pick_sender_id_by_code(
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
    session: AsyncSession,
    company_id: int,
    sender_code: str,
) -> int | None:
    stmt = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.company_id == company_id)
        .where(WhatsAppSender.sender_code == sender_code)
        .where(WhatsAppSender.is_active.is_(True))
        .limit(1)
    )
    res = await session.execute(stmt)
    sender_id = res.scalar_one_or_none()
    if sender_id is not None:
        return int(sender_id)

    if sender_code == "default":
        return None

    stmt = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.company_id == company_id)
        .where(WhatsAppSender.sender_code == "default")
        .where(WhatsAppSender.is_active.is_(True))
        .limit(1)
    )
    res = await session.execute(stmt)
    default_id = res.scalar_one_or_none()
    return int(default_id) if default_id is not None else None
