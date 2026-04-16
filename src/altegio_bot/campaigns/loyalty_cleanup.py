"""Безопасная очистка и выпуск loyalty-карт для кампании.

Правила безопасности:
  * Удаляем ТОЛЬКО карты, которые были выпущены этой кампанией
    (loyalty_card_id в CampaignRecipient с нужным campaign_code).
  * Не пытаемся повторно удалять карты, которые уже удалялись ранее
    (они присутствуют в cleanup_card_ids других записей).
  * Если удаление хотя бы одной карты не удалось — не выпускаем
    новую карту и не отправляем сообщение.
  * Помечаем клиента cleanup_failed с причиной.

resolve_or_issue_loyalty_card (для CRM-only, client_id=None):
  * 0 существующих карт в том же филиале → issue_new
    (API-запрос, может поднять исключение).
  * 1 существующая карта в том же филиале → reused_existing
    (без API-запроса).
  * 2+ карт в том же филиале → failed_conflict
    (без API-запроса, требует ручного разбора).
  * Карты другого филиала (company_id) не учитываются —
    cross-branch reuse не происходит.
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


@dataclass
class CardResolution:
    """Результат resolve_or_issue_loyalty_card.

    outcome:
      'issued_new'      — новая карта выпущена через API (cards_issued +1).
      'reused_existing' — найдена существующая карта в БД (без API-запроса).
      'failed_conflict' — найдено 2+ карт для одного номера; безопасный отказ.
    """

    outcome: str
    loyalty_card_id: str
    loyalty_card_number: str
    loyalty_card_type_id: str
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
        logger.debug("cleanup client_id=%s: no pending campaign cards", client_id)
        return CleanupResult(ok=True, deleted_ids=[])

    deleted: list[str] = []
    for card_id in card_ids:
        try:
            await loyalty.delete_card(location_id, int(card_id))
            deleted.append(card_id)
            logger.info("cleanup deleted card_id=%s client_id=%s", card_id, client_id)
        except Exception as exc:
            logger.error(
                "cleanup FAILED card_id=%s client_id=%s: %s",
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


async def find_existing_campaign_card_for_phone(
    session: AsyncSession,
    *,
    phone_e164: str,
    campaign_code: str,
    company_id: int,
) -> list[tuple[str, str, str]]:
    """Найти loyalty-карты, выпущенные кампанией для phone_e164 в компании.

    Возвращает список кортежей (card_id, card_number, card_type_id) для
    всех CampaignRecipient, у которых phone_e164, campaign_code и
    company_id совпадают, и loyalty_card_id заполнен.

    Фильтрация по company_id обязательна: одинаковый campaign_code
    используется в нескольких филиалах, и без этого фильтра
    возможен ложный cross-branch reuse или ложный failed_conflict.

    Используется для CRM-only клиентов (client_id=None): у них нет
    локальной записи Client, поэтому поиск выполняется по номеру телефона.

    # TODO: рассмотреть исключение карт, которые уже были помечены
    # как удалённые (cleanup_card_ids). В текущем потоке CRM-only
    # клиент не проходит через cleanup_campaign_cards, поэтому
    # cleanup_card_ids всегда пустой для этих записей — риск низкий.
    # Если в будущем добавится cleanup для CRM-only, нужно добавить
    # фильтр аналогично find_campaign_card_ids().
    """
    stmt = (
        select(
            CampaignRecipient.loyalty_card_id,
            CampaignRecipient.loyalty_card_number,
            CampaignRecipient.loyalty_card_type_id,
        )
        .join(CampaignRun, CampaignRun.id == CampaignRecipient.campaign_run_id)
        .where(CampaignRecipient.phone_e164 == phone_e164)
        .where(CampaignRun.campaign_code == campaign_code)
        .where(CampaignRecipient.company_id == company_id)
        .where(CampaignRecipient.loyalty_card_id.is_not(None))
        .where(CampaignRecipient.loyalty_card_id != "")
    )
    rows = (await session.execute(stmt)).all()
    return [
        (
            row.loyalty_card_id,
            row.loyalty_card_number or "",
            row.loyalty_card_type_id or "",
        )
        for row in rows
    ]


async def resolve_or_issue_loyalty_card(
    session: AsyncSession,
    loyalty: AltegioLoyaltyClient,
    *,
    phone_e164: str,
    location_id: int,
    card_type_id: str,
    campaign_code: str,
    company_id: int,
) -> CardResolution:
    """Переиспользовать существующую карту или выпустить новую.

    Матрица решений по числу найденных записей CampaignRecipient для
    phone_e164 + campaign_code + company_id с непустым loyalty_card_id:

      0 → issue_new: вызывает loyalty.issue_card().
          Поднимает исключение при ошибке API — вызывающий код должен
          трактовать это как card_issue_failed.
      1 → reused_existing: возвращает данные существующей карты,
          API не вызывается.
      2+ → failed_conflict: возвращает
           CardResolution(outcome='failed_conflict') без API-запроса.
           Вызывающий код должен безопасно завершить обработку
           получателя с excluded_reason='card_conflict'.

    Карты другого филиала (иной company_id) игнорируются полностью —
    cross-branch reuse не происходит.

    Почему безопаснее прямого issue_card:
      - Повторная попытка send-real для телефона, у которого уже была
        выпущена карта (например, queue_failed в предыдущем запуске),
        переиспользует существующую карту вместо попытки создать дубликат.
      - Неоднозначное состояние (2+ карт) становится явным вместо
        молчаливого создания ещё одной карты.
    """
    existing = await find_existing_campaign_card_for_phone(
        session,
        phone_e164=phone_e164,
        campaign_code=campaign_code,
        company_id=company_id,
    )

    if len(existing) > 1:
        card_ids = [c[0] for c in existing]
        reason = f"found {len(existing)} existing cards for phone {phone_e164}: " + ", ".join(card_ids)
        logger.warning(
            "card_conflict phone=%s campaign=%s ids=%s",
            phone_e164,
            campaign_code,
            card_ids,
        )
        return CardResolution(
            outcome="failed_conflict",
            loyalty_card_id="",
            loyalty_card_number="",
            loyalty_card_type_id="",
            reason=reason,
        )

    if len(existing) == 1:
        card_id, card_number, existing_type_id = existing[0]
        logger.info(
            "card_reused card_id=%s phone=%s campaign=%s",
            card_id,
            phone_e164,
            campaign_code,
        )
        return CardResolution(
            outcome="reused_existing",
            loyalty_card_id=card_id,
            loyalty_card_number=card_number,
            loyalty_card_type_id=existing_type_id or card_type_id,
        )

    # 0 существующих карт — выпустить новую.
    card_number = make_card_number(phone_e164)
    phone_num = int(phone_e164.lstrip("+"))
    card = await loyalty.issue_card(
        location_id,
        loyalty_card_number=card_number,
        loyalty_card_type_id=card_type_id,
        phone=phone_num,
    )
    issued_number = str(card.get("loyalty_card_number") or card_number)
    issued_id = str(card.get("id") or card.get("loyalty_card_id") or "")
    logger.info(
        "card_issued card_id=%s phone=%s campaign=%s",
        issued_id,
        phone_e164,
        campaign_code,
    )
    return CardResolution(
        outcome="issued_new",
        loyalty_card_id=issued_id,
        loyalty_card_number=issued_number,
        loyalty_card_type_id=card_type_id,
    )
