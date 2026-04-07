"""Сегментация: поиск новых клиентов для рассылки.

Новый клиент — это клиент, у которого:
  1. Ровно одна запись в периоде (не удалённая).
  2. Эта запись подтверждена: Record.confirmed == 1.
  3. Нет ни одной записи ДО начала периода (любой статус, включая удалённые).
  4. Есть phone_e164.
  5. Не в wa_opted_out.

Определение «подтверждённой записи»:
  Record.confirmed == 1 — запись подтверждена клиентом в Altegio.
  Единственная точка правды — константа CONFIRMED_FLAG.
  Если понадобится изменить критерий, менять только её.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, Record

logger = logging.getLogger(__name__)

# Единственная точка правды: что считается «подтверждённой» записью.
# Record.confirmed == 1 означает, что клиент подтвердил запись в Altegio.
CONFIRMED_FLAG = 1


def is_confirmed_record(record: Record) -> bool:
    """Вернуть True, если запись считается подтверждённой.

    Критерий: Record.confirmed == CONFIRMED_FLAG (== 1).
    """
    return record.confirmed == CONFIRMED_FLAG


@dataclass
class ClientCandidate:
    """Кандидат на рассылку с метаданными сегментации."""

    client: Client
    # Все не удалённые записи клиента в периоде
    total_records_in_period: int
    # Подтверждённые записи (confirmed == CONFIRMED_FLAG) в периоде
    confirmed_records_in_period: int
    # Все записи до начала периода (любой статус)
    records_before_period: int
    # Причина исключения; None — клиент eligible
    excluded_reason: str | None = field(default=None)

    @property
    def is_eligible(self) -> bool:
        return self.excluded_reason is None


def _classify(candidate: ClientCandidate) -> None:
    """Проставить excluded_reason по бизнес-правилам.

    Порядок проверок важен: более приоритетные правила идут первыми.
    """
    client = candidate.client

    if client.wa_opted_out:
        candidate.excluded_reason = "opted_out"
        return

    if not client.phone_e164:
        candidate.excluded_reason = "no_phone"
        return

    # Клиент с историей — не новый
    if candidate.records_before_period > 0:
        candidate.excluded_reason = "has_records_before_period"
        return

    # 2+ записей в периоде — не подходит
    if candidate.total_records_in_period >= 2:
        candidate.excluded_reason = "multiple_records_in_period"
        return

    # Нет ни одной подтверждённой записи
    if candidate.total_records_in_period == 0 or candidate.confirmed_records_in_period == 0:
        candidate.excluded_reason = "no_confirmed_record_in_period"
        return

    # Клиент прошёл все проверки — eligible


async def find_candidates(
    session: AsyncSession,
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[ClientCandidate]:
    """Найти клиентов с записями в периоде и классифицировать их.

    Возвращает список ClientCandidate (eligible + excluded).
    """
    # ------------------------------------------------------------------
    # Агрегаты по записям внутри периода.
    # Считаем только не удалённые записи.
    # ------------------------------------------------------------------
    confirmed_expr = func.sum(case((Record.confirmed == CONFIRMED_FLAG, 1), else_=0)).label("confirmed")

    in_period_subq = (
        select(
            Record.client_id,
            func.count(Record.id).label("total"),
            confirmed_expr,
        )
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .where(Record.is_deleted.is_(False))
        .group_by(Record.client_id)
        .subquery()
    )

    # ------------------------------------------------------------------
    # Записи ДО начала периода.
    # Считаем независимо от статуса (включая удалённые) —
    # любая прошлая запись означает «не новый клиент».
    # ------------------------------------------------------------------
    before_period_subq = (
        select(
            Record.client_id,
            func.count(Record.id).label("cnt"),
        )
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at < period_start)
        .group_by(Record.client_id)
        .subquery()
    )

    # ------------------------------------------------------------------
    # Основной запрос: клиенты + агрегаты
    # ------------------------------------------------------------------
    stmt = (
        select(
            Client,
            in_period_subq.c.total,
            in_period_subq.c.confirmed,
            func.coalesce(before_period_subq.c.cnt, 0).label("before"),
        )
        .where(Client.company_id == company_id)
        .join(in_period_subq, in_period_subq.c.client_id == Client.id)
        .outerjoin(before_period_subq, before_period_subq.c.client_id == Client.id)
        .order_by(Client.id.asc())
    )

    rows = (await session.execute(stmt)).all()

    candidates: list[ClientCandidate] = []
    for row in rows:
        client, total, confirmed, before = row
        c = ClientCandidate(
            client=client,
            total_records_in_period=int(total or 0),
            confirmed_records_in_period=int(confirmed or 0),
            records_before_period=int(before or 0),
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
