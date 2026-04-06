"""Безопасная очистка и выпуск loyalty-карт для кампании.

Правила безопасности:
  * Удаляем ТОЛЬКО карты, которые были выпущены этой кампанией
    (loyalty_card_id в CampaignRecipient с нужным campaign_code).
  * Если удаление хотя бы одной карты не удалось — не выпускаем
    новую карту и не отправляем сообщение.
  * Помечаем клиента cleanup_failed с причиной.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.models.models import CampaignRecipient, CampaignRun

logger = logging.getLogger(__name__)


@dataclass
class CleanupResult:
    ok: bool
    deleted_ids: list[str] = field(default_factory=list)
    failed_card_id: str | None = None
    reason: str | None = None


async def find_campaign_card_ids(
    session: AsyncSession,
    *,
    client_id: int,
    campaign_code: str,
) -> list[str]:
    """Найти loyalty_card_id, выпущенные для client_id этой кампанией.

    Ищем только те записи, где loyalty_card_id не пустой.
    Это гарантирует, что удаляем только campaign-generated карты.
    """
    stmt = (
        select(CampaignRecipient.loyalty_card_id)
        .join(
            CampaignRun,
            CampaignRun.id == CampaignRecipient.campaign_run_id,
        )
        .where(CampaignRecipient.client_id == client_id)
        .where(CampaignRun.campaign_code == campaign_code)
        .where(CampaignRecipient.loyalty_card_id.is_not(None))
        .where(CampaignRecipient.loyalty_card_id != '')
    )
    rows = (await session.execute(stmt)).scalars().all()
    return [r for r in rows if r]


async def cleanup_campaign_cards(
    session: AsyncSession,
    loyalty: AltegioLoyaltyClient,
    *,
    location_id: int,
    client_id: int,
    campaign_code: str,
) -> CleanupResult:
    """Удалить все loyalty-карты, выпущенные кампанией для client_id.

    Если удаление любой карты не удалось — возвращает ok=False.
    В этом случае не выпускать новую карту и не отправлять сообщение.
    """
    card_ids = await find_campaign_card_ids(
        session,
        client_id=client_id,
        campaign_code=campaign_code,
    )

    if not card_ids:
        logger.debug(
            'cleanup client_id=%d: no previous campaign cards',
            client_id,
        )
        return CleanupResult(ok=True, deleted_ids=[])

    deleted: list[str] = []
    for card_id in card_ids:
        try:
            await loyalty.delete_card(location_id, int(card_id))
            deleted.append(card_id)
            logger.info(
                'cleanup deleted card_id=%s client_id=%d',
                card_id,
                client_id,
            )
        except Exception as exc:
            logger.error(
                'cleanup FAILED card_id=%s client_id=%d: %s',
                card_id,
                client_id,
                exc,
            )
            return CleanupResult(
                ok=False,
                deleted_ids=deleted,
                failed_card_id=card_id,
                reason=f'delete_card failed: {exc}',
            )

    return CleanupResult(ok=True, deleted_ids=deleted)


def make_card_number(phone_e164: str) -> str:
    """Сформировать номер карты из телефона (16 цифр, ведущие нули)."""
    digits = phone_e164.lstrip('+')
    return digits.zfill(16)


def make_card_text(card_number: str) -> str:
    """Форматировать текст карты для шаблона сообщения."""
    return f'Kundenkarte #{card_number}'
