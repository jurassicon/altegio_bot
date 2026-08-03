import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import PROVIDER_ALTEGIO, RecordService, ServiceSenderRule, WhatsAppSender

logger = logging.getLogger(__name__)
logger.info("Starting inbox worker")


async def pick_sender_code_for_record(session: AsyncSession, company_id: int, record_id: int) -> str:
    stmp = (
        select(RecordService.service_id)
        .where(RecordService.record_id == record_id)
        .order_by(RecordService.service_id.asc())
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

    logger.info(
        "Sender code for record_id=%s service_id=%s: %s",
        record_id,
        service_id,
        sender_code,
    )

    return sender_code or "default"


async def pick_sender_id_by_code(
    session: AsyncSession,
    company_id: int,
    sender_code: str = "default",
    *,
    provider: str = PROVIDER_ALTEGIO,
) -> int | None:
    stmp = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.provider == provider)
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
    *,
    provider: str = PROVIDER_ALTEGIO,
) -> int | None:
    """Return the active sender id for *provider* / *company_id* / *sender_code*.

    ``provider`` is part of sender identity, not a decoration. EasyWeek's
    ``company_id`` is the numeric EasyWeek ``:location_id`` and lives in the same
    integer space as an Altegio company id, so ``(company_id, sender_code)``
    alone can collide across CRMs. Sending an EasyWeek booking confirmation from
    the Altegio WABA number would be a cross-tenant leak, so every lookup —
    including the ``default`` fallback below — is bounded to one provider.

    The default parameter keeps existing Altegio, promo and campaign call sites
    on exactly the behaviour they had; only callers that pass ``provider``
    explicitly can reach another provider's rows.
    """
    stmt = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.provider == provider)
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

    # Fallback stays INSIDE the same provider and company: "no dedicated sender
    # for this code" must never widen into "any sender that happens to match".
    stmt = (
        select(WhatsAppSender.id)
        .where(WhatsAppSender.provider == provider)
        .where(WhatsAppSender.company_id == company_id)
        .where(WhatsAppSender.sender_code == "default")
        .where(WhatsAppSender.is_active.is_(True))
        .limit(1)
    )
    res = await session.execute(stmt)
    default_id = res.scalar_one_or_none()
    return int(default_id) if default_id is not None else None
