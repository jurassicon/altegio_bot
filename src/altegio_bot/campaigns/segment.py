"""Сегментация: поиск новых клиентов для рассылки.

  Клиент eligible только если:
  1. В периоде у него РОВНО ОДНА lash-запись (любого статуса, не удалённая).
  2. Эта единственная lash-запись имеет статус «Пришел» (attendance == 1).
     ВАЖНО: confirmed == 1 означает только «запись не отменена»; это НЕ то же
     самое, что «клиент пришёл». Клиенты с будущими или несостоявшимися
     записями имеют confirmed=1, но attendance=0 — они НЕ eligible.
  3. ДО начала периода у него нет НИ ОДНОЙ записи в Altegio CRM
     (любой услуги, любого статуса).
  4. Есть phone_e164.
  5. Не в wa_opted_out.

Источники истины:
  - Всё из CRM Altegio API (GET /records/{company_id}?client_id=X):
      записи в периоде, услуги, статус подтверждения, история до периода.
  - Исключение: wa_opted_out берётся из локальной БД (clients.wa_opted_out).
  - Исключение: phone_e164 берётся из локальной БД (clients.phone_e164).

Ограничение обнаружения (Discovery Limitation):
  Кандидаты обнаруживаются через локальную БД (таблица records).
  Это означает, что клиенты, которые посещали салон ДО запуска бота
  (до синхронизации записей), НЕ будут обнаружены как кандидаты.
  Это false negative (пропуск), а не false positive (ложная рассылка).
  Безопасно: мы никогда не отправляем тем, кто не должен получать рассылку.
  НЕ безопасно: мы можем пропустить часть новых клиентов.
  Если это неприемлемо — нужен другой способ обнаружения (например, через
  CRM API с перебором всех клиентов компании, если endpoint существует).

Локальная БД используется ТОЛЬКО для:
  - Обнаружения candidate client_id (performance: не опрашивать CRM по всем клиентам).
  - wa_opted_out и phone_e164.
  - Технических связей (CampaignRecipient и т.п.).

Множественность:
  - Если lash-записей в периоде >= 2 → multiple_lash_records_in_period (даже если
    посещена только одна).
  - Если lash-записей == 0 → no_lash_record_in_period.
  - Если lash-записей == 1 и attendance != 1 → no_confirmed_lash_record_in_period.

CRM недоступность:
  - Если Altegio CRM API вернул ошибку → клиент получает excluded_reason =
    'crm_history_unavailable'. Возвращать 0 и считать клиента новым НЕЛЬЗЯ.

Service lookup недоступность:
  - Если Altegio API для получения category_id услуги вернул ошибку →
    клиент получает excluded_reason = 'service_category_unavailable'.
    Считать услугу non-lash и пропускать проверку НЕЛЬЗЯ — это даст
    ложно-eligible клиентов.

Константа ATTENDED_FLAG = 1 — единственная точка правды для «Пришел» (attendance).
Константа CONFIRMED_FLAG = 1 — «запись не отменена» (confirmed); используется
только в диагностических счётчиках, НЕ для определения eligible.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime

import httpx
from sqlalchemy import select

from altegio_bot.campaigns.altegio_crm import (
    CrmRecord,
    CrmUnavailableError,
    classify_crm_records,
    get_client_crm_records,
)
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import Client, Record
from altegio_bot.service_filter import LASH_CATEGORY_IDS_BY_COMPANY, ServiceLookupError, is_lash_service
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

# «Пришел» (attendance=1) — единственный критерий eligible lash-записи.
ATTENDED_FLAG = 1
# «Подтверждена, не отменена» (confirmed=1) — диагностический счётчик; НЕ критерий eligible.
CONFIRMED_FLAG = 1


@dataclass(frozen=True)
class ClientSnapshot:
    """Immutable snapshot клиента для конкурентных CRM-задач.

    Заменяет прямую передачу ORM Client объектов в конкурентные asyncio задачи.
    ORM объекты привязаны к сессии и небезопасны для использования после её закрытия
    (expiry при expire_on_commit=True и детачинг). ClientSnapshot — plain dataclass
    без SQLAlchemy зависимостей, безопасен для передачи в любые задачи.
    """

    id: int | None
    company_id: int
    altegio_client_id: int | None
    display_name: str | None
    phone_e164: str | None
    wa_opted_out: bool


@dataclass
class ClientCandidate:
    """Кандидат на рассылку с метаданными сегментации."""

    client: ClientSnapshot

    # --- Записи в периоде (из CRM) ---
    # Все не удалённые не-отменённые записи клиента в периоде
    total_records_in_period: int
    # Подтверждённые записи (confirmed == CONFIRMED_FLAG) — диагностика
    confirmed_records_in_period: int

    # --- Ресничные записи в периоде (из CRM + category lookup) ---
    lash_records_in_period: int
    # Посещённые lash-записи (attendance == ATTENDED_FLAG) — критерий eligible
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


def compute_excluded_reason(
    *,
    wa_opted_out: bool,
    phone_e164: str | None,
    crm_unavailable: bool = False,
    service_unavailable: bool = False,
    count_before: int,
    lash_count: int,
    attended_lash_count: int,
) -> str | None:
    """Вычислить excluded_reason по бизнес-правилам «Вариант А».

    Единственная точка правды для логики классификации.
    Используется и в основном pipeline (_classify / _process_one_client),
    и в debug endpoint — что гарантирует их идентичное поведение.

    Порядок проверок важен: более приоритетные идут первыми.

    Args:
        wa_opted_out:       клиент отписался от WhatsApp рассылок.
        phone_e164:         нормализованный номер телефона (или None).
        crm_unavailable:    CRM API вернул ошибку.
        service_unavailable: category lookup для услуг вернул ошибку.
        count_before:       кол-во записей до начала периода.
        lash_count:         кол-во ресничных записей в периоде.
        attended_lash_count: кол-во ресничных записей со статусом «Пришел».

    Returns:
        excluded_reason строкой, или None если клиент eligible.
    """
    if wa_opted_out:
        return "opted_out"
    if not phone_e164:
        return "no_phone"
    if crm_unavailable:
        return "crm_history_unavailable"
    if service_unavailable:
        return "service_category_unavailable"
    if count_before > 0:
        return "has_records_before_period"
    if lash_count == 0:
        return "no_lash_record_in_period"
    # 2+ lash-записей — excluded даже если одна из них с attendance=1
    if lash_count >= 2:
        return "multiple_lash_records_in_period"
    if attended_lash_count == 0:
        return "no_confirmed_lash_record_in_period"
    return None


def _classify(candidate: ClientCandidate) -> None:
    """Проставить excluded_reason через compute_excluded_reason.

    Обёртка над compute_excluded_reason, сохраняющая результат в candidate.
    Вызывается только когда CRM и service lookup доступны (crm/service_unavailable=False):
    эти случаи обрабатываются раньше через _make_excluded_candidate().
    Проверка candidate.excluded_reason — защитная: при нормальном flow он None.
    """
    client = candidate.client
    candidate.excluded_reason = compute_excluded_reason(
        wa_opted_out=client.wa_opted_out,
        phone_e164=client.phone_e164,
        crm_unavailable=candidate.excluded_reason == "crm_history_unavailable",
        service_unavailable=candidate.excluded_reason == "service_category_unavailable",
        count_before=candidate.records_before_period,
        lash_count=candidate.lash_records_in_period,
        attended_lash_count=candidate.confirmed_lash_records_in_period,
    )


async def check_lash_services(
    http_client: httpx.AsyncClient,
    company_id: int,
    crm_records: list[CrmRecord],
) -> tuple[int, int, list[str], frozenset[int]]:
    """Подсчитать ресничные записи и собрать названия услуг.

    Публичная функция: используется как в основном pipeline (_process_one_client),
    так и в debug endpoint — гарантирует идентичную lash-логику в обоих местах.

    Возвращает:
        (lash_count, attended_lash_count, service_titles, lash_svc_ids)

        lash_count              — все не удалённые lash-записи в периоде.
        attended_lash_count     — lash-записи со статусом «Пришел»
                                  (attendance == ATTENDED_FLAG). Критерий eligible.
        service_titles          — уникальные названия услуг (для диагностики).
        lash_svc_ids            — frozenset ID ресничных услуг (для debug-вывода).

    Raises:
        ServiceLookupError: если Altegio service category API недоступен.
            Caller должен пометить клиента excluded_reason='service_category_unavailable'.
    """
    has_lash_config = bool(LASH_CATEGORY_IDS_BY_COMPANY.get(company_id))

    # Уникальные service_id для батч-проверки (с кешем)
    unique_svc_ids: set[int] = set()
    for rec in crm_records:
        unique_svc_ids.update(rec.service_ids)

    # Определить lash service_id через category lookup с кешем
    _lash_svc_ids: set[int] = set()
    if has_lash_config:
        for svc_id in unique_svc_ids:
            # ServiceLookupError propagates to caller — не глотаем!
            if await is_lash_service(company_id, svc_id, http_client):
                _lash_svc_ids.add(svc_id)
    else:
        logger.warning(
            "No LASH_CATEGORY_IDS_BY_COMPANY for company_id=%d — lash filter disabled",
            company_id,
        )

    lash_count = 0
    attended_lash_count = 0
    titles: set[str] = set()

    for rec in crm_records:
        has_lash = any(sid in _lash_svc_ids for sid in rec.service_ids)
        if has_lash:
            lash_count += 1
            # Критерий eligible: attendance == 1 («Пришел»), а не просто confirmed == 1.
            # confirmed=1 лишь означает «запись не отменена» — это НЕ эквивалент явки.
            if rec.is_attended:
                attended_lash_count += 1
        titles.update(t for t in rec.service_titles if t)

    return lash_count, attended_lash_count, sorted(titles), frozenset(_lash_svc_ids)


def _make_excluded_candidate(snapshot: ClientSnapshot, reason: str) -> ClientCandidate:
    """Создать ClientCandidate с указанной причиной исключения.

    Единый helper для всех случаев исключения (замена двух отдельных функций).
    Все счётчики — нули; excluded_reason задаётся явно.
    """
    return ClientCandidate(
        client=snapshot,
        total_records_in_period=0,
        confirmed_records_in_period=0,
        lash_records_in_period=0,
        confirmed_lash_records_in_period=0,
        service_titles_in_period=[],
        records_before_period=0,
        local_client_found=True,
        excluded_reason=reason,
    )


async def _process_one_client(
    http: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    snapshot: ClientSnapshot,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> ClientCandidate:
    """Получить CRM-данные для одного клиента и классифицировать его.

    Защищена семафором для ограничения конкурентности.
    Внешний try/except Exception — защитный пояс: если какое-то неожиданное
    исключение не было поймано внутренними блоками, клиент помечается
    crm_history_unavailable, а не роняет весь asyncio.gather.

    Только BaseException (KeyboardInterrupt и т.п.) может пробиться через этот
    пояс. В этом случае asyncio.gather с return_exceptions=True поймает его
    в виде объекта исключения, а find_candidates создаст excluded candidate
    для данного snapshot — клиент не теряется.
    """
    try:
        if not snapshot.altegio_client_id:
            logger.warning(
                "segment: client_id=%s has no altegio_client_id — cannot check CRM",
                snapshot.id,
            )
            return _make_excluded_candidate(snapshot, "crm_history_unavailable")

        async with sem:
            # CRM: получить ВСЕ записи клиента
            try:
                crm_records = await get_client_crm_records(
                    http,
                    company_id=company_id,
                    altegio_client_id=int(snapshot.altegio_client_id),
                )
            except CrmUnavailableError as exc:
                logger.warning(
                    "segment: CRM unavailable for client_id=%s altegio_id=%s: %s",
                    snapshot.id,
                    snapshot.altegio_client_id,
                    exc,
                )
                return _make_excluded_candidate(snapshot, "crm_history_unavailable")

            # Разбить на in-period и before-period
            in_period_records, count_before = classify_crm_records(crm_records, period_start, period_end)

            # Базовые счётчики по in-period записям
            total_in_period = len(in_period_records)
            confirmed_in_period = sum(1 for r in in_period_records if r.is_confirmed)

            # Ресничные записи и названия услуг
            try:
                lash_count, confirmed_lash_count, service_titles, _ = await check_lash_services(
                    http, company_id, in_period_records
                )
            except ServiceLookupError as exc:
                logger.warning(
                    "segment: service category lookup failed for client_id=%s: %s",
                    snapshot.id,
                    exc,
                )
                return _make_excluded_candidate(snapshot, "service_category_unavailable")

            c = ClientCandidate(
                client=snapshot,
                total_records_in_period=total_in_period,
                confirmed_records_in_period=confirmed_in_period,
                lash_records_in_period=lash_count,
                confirmed_lash_records_in_period=confirmed_lash_count,
                service_titles_in_period=service_titles,
                records_before_period=count_before,
                local_client_found=True,
            )
            _classify(c)
            logger.debug(
                "segment debug: client_id=%s altegio_client_id=%s "
                "records_before=%d total_in_period=%d "
                "lash_in_period=%d attended_lash_in_period=%d "
                "services=%r → %s",
                snapshot.id,
                snapshot.altegio_client_id,
                count_before,
                total_in_period,
                lash_count,
                confirmed_lash_count,
                service_titles,
                c.excluded_reason if c.excluded_reason else "ELIGIBLE",
            )
            return c

    except Exception:
        logger.exception(
            "segment: unexpected error for client_id=%s altegio_id=%s — marking crm_history_unavailable",
            snapshot.id,
            snapshot.altegio_client_id,
        )
        return _make_excluded_candidate(snapshot, "crm_history_unavailable")


async def find_candidates(
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[ClientCandidate]:
    """Найти кандидатов на рассылку и классифицировать их.

    Источник истины для записей, услуг и истории: Altegio CRM API.
    Локальная БД используется только для обнаружения candidate client_id
    и для wa_opted_out / phone_e164.

    ВАЖНО — Discovery Limitation:
        Кандидаты обнаруживаются через таблицу records локальной БД.
        Клиенты, которые посещали салон ДО запуска бота (до синхронизации
        записей в локальную БД), могут быть пропущены. Это false negative.
        Безопасно: ложных рассылок не будет. Небезопасно: часть новых клиентов
        может не получить рассылку. Принятое ограничение зафиксировано в meta
        CampaignRun через поле discovery_source='local_db'.

    Шаги:
    1. SQL: candidate client_id (non-deleted records in period, local DB).
    2. SQL: Client объекты (wa_opted_out, phone_e164) → конвертируем в
       ClientSnapshot до закрытия сессии. ORM объекты в задачи не передаются.
    3. CRM API (один переиспользуемый HTTP client): для каждого клиента
       получить все записи → разбить на in-period и before-period.
       Конкурентность ограничена семафором (settings.campaign_crm_max_concurrency).
    4. Определить ресничность услуг (category lookup с LRU-кешем).
       При ошибке lookup → service_category_unavailable (не silent non-lash).
    5. Классификация по правилам Варианта А.
    6. asyncio.gather с return_exceptions=True: единичный сбой задачи не
       прерывает остальные. Основная защита — outer except Exception в
       _process_one_client. task_snapshots параллельный список — если gather
       всё же вернёт exception-объект, snapshot известен и клиент превращается
       в excluded candidate (crm_history_unavailable), а не дропается молча.
    7. Вернуть список ClientCandidate.

    Returns:
        Список ClientCandidate (eligible + excluded).
    """
    # ------------------------------------------------------------------
    # Шаг 1-2. DB queries в короткой сессии (закрывается до HTTP-вызовов)
    # ------------------------------------------------------------------
    async with SessionLocal() as session:
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
                "segment company_id=%d period=[%s, %s) → 0 candidate client_ids in local DB"
                " (discovery limitation: may miss clients before bot start)",
                company_id,
                period_start.date(),
                period_end.date(),
            )
            return []

        clients_stmt = select(Client).where(
            Client.id.in_(candidate_client_ids),
            Client.company_id == company_id,
        )
        clients_map: dict[int, Client] = {c.id: c for c in (await session.execute(clients_stmt)).scalars()}

        # Конвертировать ORM Client → ClientSnapshot ДО закрытия сессии.
        # ORM объекты не должны покидать сессию — они привязаны к ней.
        # ClientSnapshot — immutable plain dataclass, безопасен для передачи
        # в конкурентные asyncio задачи.
        snapshots: dict[int, ClientSnapshot] = {
            cid: ClientSnapshot(
                id=client.id,
                company_id=client.company_id,
                altegio_client_id=client.altegio_client_id,
                display_name=client.display_name,
                phone_e164=client.phone_e164,
                wa_opted_out=bool(client.wa_opted_out),
            )
            for cid, client in clients_map.items()
        }
    # Session closed here — snapshots are safe, ORM objects are not used further

    # ------------------------------------------------------------------
    # Шаги 3-5. CRM HTTP calls + классификация с ограниченной конкурентностью
    # ------------------------------------------------------------------
    max_concurrency = settings.campaign_crm_max_concurrency
    sem = asyncio.Semaphore(max_concurrency)
    logger.debug(
        "segment: starting CRM processing company_id=%d candidates=%d concurrency=%d",
        company_id,
        len(candidate_client_ids),
        max_concurrency,
    )

    async with httpx.AsyncClient(timeout=30.0) as http:
        # task_snapshots параллелен tasks: tasks[i] соответствует task_snapshots[i].
        # Это нужно для резервной ветки gather: если result — exception-объект,
        # мы знаем, какой snapshot потерпел сбой, и создаём excluded candidate,
        # а не дропаем задачу молча.
        task_snapshots: list[ClientSnapshot] = []
        tasks = []
        for client_id in candidate_client_ids:
            snapshot = snapshots.get(client_id)
            if snapshot is None:
                logger.warning(
                    "segment: record in period for client_id=%d but Client not found (company=%d)",
                    client_id,
                    company_id,
                )
                continue
            task_snapshots.append(snapshot)
            tasks.append(_process_one_client(http, sem, snapshot, company_id, period_start, period_end))

        # return_exceptions=True: резервный пояс безопасности.
        # Основная защита — outer except Exception в _process_one_client.
        # Если BaseException всё же пробьётся — gather вернёт его в results,
        # и мы создадим excluded candidate для конкретного snapshot.
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    candidates: list[ClientCandidate] = []
    for snapshot, result in zip(task_snapshots, raw_results):
        if isinstance(result, ClientCandidate):
            candidates.append(result)
        else:
            # BaseException пробилось через outer except Exception.
            # Не дропаем задачу молча — превращаем snapshot в excluded candidate.
            logger.error(
                "segment: BaseException escaped _process_one_client for client_id=%s "
                "— marking crm_history_unavailable: %r",
                snapshot.id,
                result,
            )
            candidates.append(_make_excluded_candidate(snapshot, "crm_history_unavailable"))

    logger.info(
        "segment company_id=%d period=[%s, %s) total=%d eligible=%d"
        " crm_unavailable=%d service_unavailable=%d"
        " (discovery via local_db — may miss pre-bot clients)",
        company_id,
        period_start.date(),
        period_end.date(),
        len(candidates),
        sum(1 for c in candidates if c.is_eligible),
        sum(1 for c in candidates if c.excluded_reason == "crm_history_unavailable"),
        sum(1 for c in candidates if c.excluded_reason == "service_category_unavailable"),
    )
    return candidates
