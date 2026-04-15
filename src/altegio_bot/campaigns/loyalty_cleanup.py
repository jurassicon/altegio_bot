"""Безопасная очистка и выпуск loyalty-карт для кампании.

Правила безопасности:
  * Удаляем ТОЛЬКО карты, которые были выпущены этой кампанией
    (loyalty_card_id в CampaignRecipient с нужным campaign_code).
  * Не пытаемся повторно удалять карты, которые уже удалялись ранее
    (они присутствуют в cleanup_card_ids других записей).
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
    client_id: int | None,
    campaign_code: str,
) -> list[str]:
    """Найти loyalty_card_id, выпущенные кампанией для client_id.

    Возвращает только те card_id, которые ещё не были удалены
    в предыдущих cleanup-проходах. Если card_id уже присутствует
    в cleanup_card_ids любой записи этого клиента, он пропускается —
    это предотвращает повторный вызов delete для уже удалённых карт.

    client_id=None (CRM-only client) → всегда [] (нет локальной записи,
    значит нет и предыдущих campaign cards для очистки).
    """
    if client_id is None:
        return []

    # Карты, выпущенные этой кампанией для клиента
    issued_stmt = (
        select(CampaignRecipient.loyalty_card_id)
        .join(CampaignRun, CampaignRun.id == CampaignRecipient.campaign_run_id)
        .where(CampaignRecipient.client_id == client_id)
        .where(CampaignRun.campaign_code == campaign_code)
        .where(CampaignRecipient.loyalty_card_id.is_not(None))
        .where(CampaignRecipient.loyalty_card_id != "")
    )
    issued_rows = (await session.execute(issued_stmt)).scalars().all()
    issued_ids = {r for r in issued_rows if r}

    if not issued_ids:
        return []

    # Карты, которые уже удалялись ранее (в cleanup_card_ids любого recipient)
    cleaned_stmt = (
        select(CampaignRecipient.cleanup_card_ids)
        .where(CampaignRecipient.client_id == client_id)
        .where(CampaignRecipient.cleanup_card_ids != [])
    )
    cleaned_rows = (await session.execute(cleaned_stmt)).scalars().all()
    already_deleted: set[str] = set()
    for cleanup_ids in cleaned_rows:
        if isinstance(cleanup_ids, list):
            already_deleted.update(str(x) for x in cleanup_ids)

    # Возвращаем только те, что ещё не удалены
    pending = [cid for cid in issued_ids if cid not in already_deleted]
    if already_deleted & issued_ids:
        logger.debug(
            "cleanup client_id=%d: skipping %d already-deleted cards",
            client_id,
            len(already_deleted & issued_ids),
        )
    return pending


async def cleanup_campaign_cards(
    session: AsyncSession,
    loyalty: AltegioLoyaltyClient,
    *,
    location_id: int,
    client_id: int | None,
    campaign_code: str,
) -> CleanupResult:
    """Удалить все loyalty-карты, выпущенные кампанией для client_id.

    Если удаление любой карты не удалось — возвращает ok=False.
    В этом случае не выпускать новую карту и не отправлять сообщение.

    client_id=None (CRM-only client) → CleanupResult(ok=True, deleted_ids=[])
    (нет локальной записи → нет предыдущих campaign cards → очистка не нужна).
    """
    card_ids = await find_campaign_card_ids(
        session,
        client_id=client_id,
        campaign_code=campaign_code,
    )

    if not card_ids:
        logger.debug("cleanup client_id=%d: no pending campaign cards", client_id)
        return CleanupResult(ok=True, deleted_ids=[])

    deleted: list[str] = []
    for card_id in card_ids:
        try:
            await loyalty.delete_card(location_id, int(card_id))
            deleted.append(card_id)
            logger.info("cleanup deleted card_id=%s client_id=%d", card_id, client_id)
        except Exception as exc:
            logger.error(
                "cleanup FAILED card_id=%s client_id=%d: %s",
                card_id,
                client_id,
                exc,
            )
            return CleanupResult(
                ok=False,
                deleted_ids=deleted,
                failed_card_id=card_id,
                reason=f"delete_card failed: {exc}",
            )

    return CleanupResult(ok=True, deleted_ids=deleted)


def make_card_number(phone_e164: str) -> str:
    """Сформировать номер карты из телефона (16 цифр, ведущие нули)."""
    digits = phone_e164.lstrip("+")
    return digits.zfill(16)


def make_card_text(card_number: str) -> str:
    """Форматировать текст карты для шаблона сообщения."""
    return f"#{card_number}"
