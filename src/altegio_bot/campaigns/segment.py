"""Сегментация: поиск новых клиентов для рассылки.

Вариант А — строгие бизнес-правила:

  Клиент попадает в кампанию, только если:
  1. В периоде у него есть РОВНО ОДНА подтверждённая ресничная запись.
  2. ДО начала периода у него нет НИ ОДНОЙ записи в Altegio CRM
     (любой услуги, любого статуса).
  3. Есть phone_e164.
  4. Не в wa_opted_out.

Источники истины:
  - «Ресничные записи в периоде»:
      Локальная БД (Record + RecordService) — актуальна для текущего периода,
      т.к. bot синхронизирует вебхуки в реальном времени.
      Категории услуг: LASH_CATEGORY_IDS_BY_COMPANY из service_filter.py.

  - «Записи ДО периода»:
      Altegio CRM API (GET /records/{company_id}?client_id=X) —
      источник истины для истории.  Локальная БД неполна: bot работает
      меньше месяца и не имеет вебхуков до запуска.

  - «Opted-out»:
      Локальная БД (clients.wa_opted_out).

Константа CONFIRMED_FLAG = 1 — единственная точка правды для «подтверждена».
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.altegio_crm import count_client_records_before_period
from altegio_bot.models.models import Client, Record, RecordService
from altegio_bot.service_filter import LASH_CATEGORY_IDS_BY_COMPANY, is_lash_service

logger = logging.getLogger(__name__)

# Единственная точка правды: что считается «подтверждённой» записью.
CONFIRMED_FLAG = 1


def is_confirmed_record(record: Record) -> bool:
    """Вернуть True, если запись считается подтверждённой."""
    return record.confirmed == CONFIRMED_FLAG


@dataclass
class ClientCandidate:
    """Кандидат на рассылку с метаданными сегментации."""

    client: Client

    # --- Записи в периоде (из локальной БД) ---
    # Все не удалённые записи клиента в периоде
    total_records_in_period: int
    # Подтверждённые записи (confirmed == CONFIRMED_FLAG)
    confirmed_records_in_period: int

    # --- Ресничные записи в периоде (из локальной БД + category lookup) ---
    lash_records_in_period: int
    confirmed_lash_records_in_period: int

    # --- Услуги в периоде (для диагностики) ---
    service_titles_in_period: list[str]

    # --- История до периода (источник истины: Altegio CRM API) ---
    records_before_period: int  # == total_records_before_period_any

    # --- Локальная связка ---
    local_client_found: bool = field(default=True)

    # Причина исключения; None — клиент eligible
    excluded_reason: str | None = field(default=None)

    @property
    def is_eligible(self) -> bool:
        return self.excluded_reason is None


def _classify(candidate: ClientCandidate) -> None:
    """Проставить excluded_reason по бизнес-правилам «Вариант А».

    Порядок проверок важен: более приоритетные идут первыми.
    """
    client = candidate.client

    if client.wa_opted_out:
        candidate.excluded_reason = "opted_out"
        return

    if not client.phone_e164:
        candidate.excluded_reason = "no_phone"
        return

    # Клиент с историей до периода — не новый
    if candidate.records_before_period > 0:
        candidate.excluded_reason = "has_records_before_period"
        return

    # Нет ресничных записей в периоде вообще
    if candidate.lash_records_in_period == 0:
        candidate.excluded_reason = "no_lash_record_in_period"
        return

    # Нет ПОДТВЕРЖДЁННОЙ ресничной записи
    if candidate.confirmed_lash_records_in_period == 0:
        candidate.excluded_reason = "no_confirmed_lash_record_in_period"
        return

    # 2+ подтверждённых ресничных записей — не подходит
    if candidate.confirmed_lash_records_in_period >= 2:
        candidate.excluded_reason = "multiple_lash_records_in_period"
        return

    # Клиент прошёл все проверки — eligible


async def find_candidates(
    session: AsyncSession,
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[ClientCandidate]:
    """Найти кандидатов на рассылку и классифицировать их.

    Шаги:
    1. SQL: все не удалённые записи компании в периоде (local DB).
    2. SQL: услуги для этих записей (local DB).
    3. Async: определить, какие service_id являются ресничными (category API).
    4. Агрегация: total/lash/confirmed_lash на клиента.
    5. SQL: загрузить объекты Client.
    6. Async (Altegio CRM API): для каждого кандидата проверить историю ДО периода.
    7. Классификация по правилам Варианта А.

    Возвращает список ClientCandidate (eligible + excluded).
    """
    # ------------------------------------------------------------------
    # Шаг 1. Записи в периоде
    # ------------------------------------------------------------------
    records_stmt = (
        select(Record.id, Record.client_id, Record.confirmed)
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .where(Record.is_deleted.is_(False))
    )
    period_records = (await session.execute(records_stmt)).all()

    if not period_records:
        logger.info(
            "segment company_id=%d period=[%s, %s) → 0 records in local DB",
            company_id,
            period_start.date(),
            period_end.date(),
        )
        return []

    record_ids = [row.id for row in period_records]

    # ------------------------------------------------------------------
    # Шаг 2. Услуги этих записей
    # ------------------------------------------------------------------
    services_stmt = select(
        RecordService.record_id,
        RecordService.service_id,
        RecordService.title,
    ).where(RecordService.record_id.in_(record_ids))
    service_rows = (await session.execute(services_stmt)).all()

    # ------------------------------------------------------------------
    # Шаг 3. Определить ресничные service_id через category lookup
    # Уникальные service_id → батч API-запросов (с кешем)
    # ------------------------------------------------------------------
    unique_service_ids: set[int] = {row.service_id for row in service_rows}
    lash_service_ids: set[int] = set()

    if LASH_CATEGORY_IDS_BY_COMPANY.get(company_id):
        for svc_id in unique_service_ids:
            try:
                if await is_lash_service(company_id, svc_id):
                    lash_service_ids.add(svc_id)
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

    # ------------------------------------------------------------------
    # Шаг 4. Индексы: record → (has_lash, titles)
    # ------------------------------------------------------------------
    record_lash: dict[int, bool] = {}
    record_titles: dict[int, list[str]] = {}

    for row in service_rows:
        rid = row.record_id
        if rid not in record_lash:
            record_lash[rid] = False
            record_titles[rid] = []
        if row.service_id in lash_service_ids:
            record_lash[rid] = True
        if row.title:
            record_titles[rid].append(row.title)

    # ------------------------------------------------------------------
    # Шаг 5. Агрегация по client_id
    # ------------------------------------------------------------------
    agg: dict[int, dict] = defaultdict(
        lambda: {
            "total": 0,
            "confirmed": 0,
            "lash": 0,
            "confirmed_lash": 0,
            "titles": set(),
        }
    )

    for row in period_records:
        cid = row.client_id
        a = agg[cid]
        a["total"] += 1
        is_conf = row.confirmed == CONFIRMED_FLAG
        has_lash = record_lash.get(row.id, False)

        if is_conf:
            a["confirmed"] += 1
        if has_lash:
            a["lash"] += 1
            if is_conf:
                a["confirmed_lash"] += 1
        for t in record_titles.get(row.id, []):
            a["titles"].add(t)

    # ------------------------------------------------------------------
    # Шаг 6. Загрузить объекты Client
    # ------------------------------------------------------------------
    client_ids = list(agg.keys())
    clients_stmt = select(Client).where(
        Client.id.in_(client_ids),
        Client.company_id == company_id,
    )
    clients_map: dict[int, Client] = {c.id: c for c in (await session.execute(clients_stmt)).scalars()}

    # ------------------------------------------------------------------
    # Шаг 7. Altegio CRM: история до периода + классификация
    # ------------------------------------------------------------------
    candidates: list[ClientCandidate] = []

    for client_id, a in agg.items():
        client = clients_map.get(client_id)
        if client is None:
            # Запись есть, но Client не найден — пропускаем
            logger.warning(
                "segment: record in period for client_id=%d but Client not found (company=%d)",
                client_id,
                company_id,
            )
            continue

        # Проверка истории через Altegio CRM API
        records_before = 0
        if client.altegio_client_id:
            records_before = await count_client_records_before_period(
                company_id=company_id,
                altegio_client_id=int(client.altegio_client_id),
                period_start=period_start,
            )
        else:
            logger.warning(
                "segment: client_id=%d has no altegio_client_id — cannot check CRM history",
                client_id,
            )

        c = ClientCandidate(
            client=client,
            total_records_in_period=a["total"],
            confirmed_records_in_period=a["confirmed"],
            lash_records_in_period=a["lash"],
            confirmed_lash_records_in_period=a["confirmed_lash"],
            service_titles_in_period=sorted(a["titles"]),
            records_before_period=records_before,
            local_client_found=True,
        )
        _classify(c)
        candidates.append(c)

    logger.info(
        "segment company_id=%d period=[%s, %s) total=%d eligible=%d",
        company_id,
        period_start.date(),
        period_end.date(),
        len(candidates),
        sum(1 for c in candidates if c.is_eligible),
    )
    return candidates
