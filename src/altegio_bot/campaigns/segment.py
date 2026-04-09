"""Сегментация: поиск новых клиентов для рассылки.

Вариант А — строгие бизнес-правила:

  Клиент eligible только если:
  1. В периоде у него РОВНО ОДНА lash-запись (любого статуса, не удалённая).
  2. Эта единственная lash-запись подтверждена (confirmed == 1).
  3. ДО начала периода у него нет НИ ОДНОЙ записи в Altegio CRM
     (любой услуги, любого статуса).
  4. Есть phone_e164.
  5. Не в wa_opted_out.

Источники истины:
  - Всё из CRM Altegio API (GET /records/{company_id}?client_id=X):
      записи в периоде, услуги, статус подтверждения, история до периода.
  - Исключение: wa_opted_out берётся из локальной БД (clients.wa_opted_out).
  - Исключение: phone_e164 берётся из локальной БД (clients.phone_e164).

Локальная БД используется ТОЛЬКО для:
  - Обнаружения candidate client_id (performance: не опрашивать CRM по всем клиентам).
  - wa_opted_out и phone_e164.
  - Технических связей (CampaignRecipient и т.п.).

Множественность:
  - Если lash-записей в периоде >= 2 → multiple_lash_records_in_period (даже если
    подтверждена только одна).
  - Если lash-записей == 0 → no_lash_record_in_period.
  - Если lash-записей == 1 и она не подтверждена → no_confirmed_lash_record_in_period.

CRM недоступность:
  - Если Altegio CRM API вернул ошибку → клиент получает excluded_reason =
    'crm_history_unavailable'. Возвращать 0 и считать клиента новым НЕЛЬЗЯ.

Константа CONFIRMED_FLAG = 1 — единственная точка правды для «подтверждена».
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.altegio_crm import (
    CrmRecord,
    CrmUnavailableError,
    classify_crm_records,
    get_client_crm_records,
)
from altegio_bot.models.models import Client, Record
from altegio_bot.service_filter import LASH_CATEGORY_IDS_BY_COMPANY, is_lash_service

logger = logging.getLogger(__name__)

# Единственная точка правды: что считается «подтверждённой» записью.
CONFIRMED_FLAG = 1


@dataclass
class ClientCandidate:
    """Кандидат на рассылку с метаданными сегментации."""

    client: Client

    # --- Записи в периоде (из CRM) ---
    # Все не удалённые не-отменённые записи клиента в периоде
    total_records_in_period: int
    # Подтверждённые записи (confirmed == CONFIRMED_FLAG)
    confirmed_records_in_period: int

    # --- Ресничные записи в периоде (из CRM + category lookup) ---
    lash_records_in_period: int
    confirmed_lash_records_in_period: int

    # --- Услуги в периоде (для диагностики) ---
    service_titles_in_period: list[str]

    # --- История до периода (источник истины: Altegio CRM API) ---
    records_before_period: int

    # --- Диагностика ---
    # True если Client найден в локальной БД
    local_client_found: bool = field(default=True)

    # Причина исключения; None — клиент eligible
    excluded_reason: str | None = field(default=None)

    @property
    def is_eligible(self) -> bool:
        return self.excluded_reason is None


def _classify(candidate: ClientCandidate) -> None:
    """Проставить excluded_reason по бизнес-правилам «Вариант А».

    Порядок проверок важен: более приоритетные идут первыми.
    crm_history_unavailable устанавливается до вызова _classify() напрямую
    в find_candidates(), но здесь проверяем для полноты.
    """
    client = candidate.client

    if client.wa_opted_out:
        candidate.excluded_reason = "opted_out"
        return

    if not client.phone_e164:
        candidate.excluded_reason = "no_phone"
        return

    # crm_history_unavailable — установлено ранее, не перезаписываем
    if candidate.excluded_reason == "crm_history_unavailable":
        return

    # Клиент с историей до периода — не новый
    if candidate.records_before_period > 0:
        candidate.excluded_reason = "has_records_before_period"
        return

    # Нет ресничных записей в периоде вообще
    if candidate.lash_records_in_period == 0:
        candidate.excluded_reason = "no_lash_record_in_period"
        return

    # 2+ lash-записей в периоде (включая неподтверждённые) — не подходит
    # Правило строгое: даже если только 1 подтверждена, 2+ записей → excluded
    if candidate.lash_records_in_period >= 2:
        candidate.excluded_reason = "multiple_lash_records_in_period"
        return

    # Ровно 1 lash-запись в периоде, но она не подтверждена
    if candidate.confirmed_lash_records_in_period == 0:
        candidate.excluded_reason = "no_confirmed_lash_record_in_period"
        return

    # Клиент прошёл все проверки — eligible


async def _check_lash_services(
    http_client: httpx.AsyncClient,
    company_id: int,
    crm_records: list[CrmRecord],
) -> tuple[int, int, list[str]]:
    """Подсчитать ресничные записи и собрать названия услуг.

    Возвращает:
        (lash_records_in_period, confirmed_lash_records_in_period, service_titles)
    """
    has_lash_config = bool(LASH_CATEGORY_IDS_BY_COMPANY.get(company_id))

    # Уникальные service_id для батч-проверки (с кешем)
    unique_svc_ids: set[int] = set()
    for rec in crm_records:
        unique_svc_ids.update(rec.service_ids)

    # Определить lash service_id через category lookup с кешем
    lash_svc_ids: set[int] = set()
    if has_lash_config:
        for svc_id in unique_svc_ids:
            try:
                if await is_lash_service(company_id, svc_id, http_client):
                    lash_svc_ids.add(svc_id)
            except Exception as exc:
                logger.warning(
                    "is_lash_service failed company=%d service=%d: %s (treating as non-lash)",
                    company_id,
                    svc_id,
                    exc,
                )
    else:
        logger.warning(
            "No LASH_CATEGORY_IDS_BY_COMPANY for company_id=%d — lash filter disabled",
            company_id,
        )

    lash_count = 0
    confirmed_lash_count = 0
    titles: set[str] = set()

    for rec in crm_records:
        has_lash = any(sid in lash_svc_ids for sid in rec.service_ids)
        if has_lash:
            lash_count += 1
            if rec.is_confirmed:
                confirmed_lash_count += 1
        titles.update(t for t in rec.service_titles if t)

    return lash_count, confirmed_lash_count, sorted(titles)


async def find_candidates(
    session: AsyncSession,
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[ClientCandidate]:
    """Найти кандидатов на рассылку и классифицировать их.

    Источник истины для записей, услуг и истории: Altegio CRM API.
    Локальная БД используется только для обнаружения candidate client_id
    и для wa_opted_out / phone_e164.

    Шаги:
    1. SQL: candidate client_id (non-deleted records in period, local DB).
    2. SQL: Client объекты (wa_opted_out, phone_e164).
    3. CRM API (один переиспользуемый HTTP client): для каждого клиента
       получить все записи → разбить на in-period и before-period.
    4. Определить ресничность услуг (category lookup с LRU-кешем).
    5. Классификация по правилам Варианта А.
    6. Вернуть список ClientCandidate.

    Returns:
        Список ClientCandidate (eligible + excluded).
    """
    # ------------------------------------------------------------------
    # Шаг 1. Обнаружение: client_id с записями в периоде (local DB)
    # Используем только для построения списка кандидатов. Реальные данные
    # придут из CRM.
    # ------------------------------------------------------------------
    records_stmt = (
        select(Record.client_id)
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .where(Record.is_deleted.is_(False))
        .distinct()
    )
    candidate_client_ids_result = (await session.execute(records_stmt)).scalars().all()
    candidate_client_ids: list[int] = list(candidate_client_ids_result)

    if not candidate_client_ids:
        logger.info(
            "segment company_id=%d period=[%s, %s) → 0 candidate client_ids in local DB",
            company_id,
            period_start.date(),
            period_end.date(),
        )
        return []

    # ------------------------------------------------------------------
    # Шаг 2. Загрузить Client объекты (wa_opted_out, phone_e164)
    # ------------------------------------------------------------------
    clients_stmt = select(Client).where(
        Client.id.in_(candidate_client_ids),
        Client.company_id == company_id,
    )
    clients_map: dict[int, Client] = {c.id: c for c in (await session.execute(clients_stmt)).scalars()}

    # ------------------------------------------------------------------
    # Шаг 3-5. CRM lookup + классификация
    # Один переиспользуемый AsyncClient для всего батча (keep-alive)
    # ------------------------------------------------------------------
    candidates: list[ClientCandidate] = []

    async with httpx.AsyncClient(timeout=30.0) as http:
        for client_id in candidate_client_ids:
            client = clients_map.get(client_id)
            if client is None:
                logger.warning(
                    "segment: record in period for client_id=%d but Client not found (company=%d)",
                    client_id,
                    company_id,
                )
                continue

            if not client.altegio_client_id:
                # Нет altegio_client_id — не можем запросить CRM
                logger.warning(
                    "segment: client_id=%d has no altegio_client_id — cannot check CRM",
                    client_id,
                )
                c = ClientCandidate(
                    client=client,
                    total_records_in_period=0,
                    confirmed_records_in_period=0,
                    lash_records_in_period=0,
                    confirmed_lash_records_in_period=0,
                    service_titles_in_period=[],
                    records_before_period=0,
                    local_client_found=True,
                    excluded_reason="crm_history_unavailable",
                )
                candidates.append(c)
                continue

            # CRM: получить ВСЕ записи клиента
            try:
                crm_records = await get_client_crm_records(
                    http,
                    company_id=company_id,
                    altegio_client_id=int(client.altegio_client_id),
                )
            except CrmUnavailableError as exc:
                logger.warning(
                    "segment: CRM unavailable for client_id=%d altegio_id=%s: %s",
                    client_id,
                    client.altegio_client_id,
                    exc,
                )
                c = ClientCandidate(
                    client=client,
                    total_records_in_period=0,
                    confirmed_records_in_period=0,
                    lash_records_in_period=0,
                    confirmed_lash_records_in_period=0,
                    service_titles_in_period=[],
                    records_before_period=0,
                    local_client_found=True,
                    excluded_reason="crm_history_unavailable",
                )
                candidates.append(c)
                continue

            # Разбить на in-period и before-period
            in_period_records, count_before = classify_crm_records(crm_records, period_start, period_end)

            # Базовые счётчики по in-period записям
            total_in_period = len(in_period_records)
            confirmed_in_period = sum(1 for r in in_period_records if r.is_confirmed)

            # Ресничные записи и названия услуг
            lash_count, confirmed_lash_count, service_titles = await _check_lash_services(
                http, company_id, in_period_records
            )

            c = ClientCandidate(
                client=client,
                total_records_in_period=total_in_period,
                confirmed_records_in_period=confirmed_in_period,
                lash_records_in_period=lash_count,
                confirmed_lash_records_in_period=confirmed_lash_count,
                service_titles_in_period=service_titles,
                records_before_period=count_before,
                local_client_found=True,
            )
            _classify(c)
            candidates.append(c)

    logger.info(
        "segment company_id=%d period=[%s, %s) total=%d eligible=%d crm_unavailable=%d",
        company_id,
        period_start.date(),
        period_end.date(),
        len(candidates),
        sum(1 for c in candidates if c.is_eligible),
        sum(1 for c in candidates if c.excluded_reason == "crm_history_unavailable"),
    )
    return candidates
