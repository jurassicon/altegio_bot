"""Altegio CRM API: чтение истории клиентов для сегментации.

Используется в campaigns/segment.py для двух целей:
  1. Получить ВСЕ записи клиента (in-period + history) из CRM.
  2. Через classify_crm_records() разбить их на «в периоде» и «до периода».

Источник истины: Altegio CRM API (endpoint /records/{company_id}).
Локальная БД records НЕ является источником истины для сегментации:
  - bot работает меньше месяца → история до его запуска в локальной БД отсутствует;
  - даже для текущего периода CRM является каноническим источником записей.

Важно:
  - При сетевой ошибке / недоступности API функции выбрасывают CrmUnavailableError.
  - Caller (segment.py) обязан обработать эту ошибку и исключить клиента
    с причиной 'crm_history_unavailable'. Возвращать 0 и считать клиента
    новым НЕЛЬЗЯ — это даст ложно-новых клиентов.

Authorization: Bearer {partner_token},{user_token}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

_PAGE_SIZE = 200
_ALTEGIO_LOCAL_TZ = ZoneInfo("Europe/Belgrade")


# ---------------------------------------------------------------------------
# Исключение: CRM недоступен
# ---------------------------------------------------------------------------


class CrmUnavailableError(Exception):
    """Altegio CRM API недоступен или вернул ошибку.

    Вызывается при сетевых ошибках и HTTP 4xx/5xx ответах.
    Сегментатор обязан исключить такого клиента с причиной
    'crm_history_unavailable', а НЕ считать его новым.
    """


# ---------------------------------------------------------------------------
# Структура записи из CRM
# ---------------------------------------------------------------------------


@dataclass
class CrmRecord:
    """Одна запись клиента из Altegio CRM API."""

    crm_id: int | None
    starts_at: datetime | None  # UTC; None если не удалось распарсить дату
    confirmed: int  # 1 = не отменена (активна), 0 = отменена; None-like → 0
    deleted: bool
    service_ids: list[int] = field(default_factory=list)
    service_titles: list[str] = field(default_factory=list)
    # attendance == 1 означает статус «Пришел» в Altegio.
    # Отличается от confirmed: confirmed=1 — запись активна (не отменена),
    # attendance=1 — клиент фактически явился на приём.
    #
    # Источник истины: поле ``attendance`` из ответа CRM (Altegio API).
    # Fallback 1: если ``attendance`` отсутствует или None — используется
    #             ``visit_attendance`` (присутствует в локальной модели Record
    #             и может нести тот же семантический смысл «Пришел»).
    # Default: если оба поля отсутствуют — 0 («не явился»).
    attendance: int = 0
    # Какое поле стало источником значения attendance:
    #   "attendance"       — основное поле из ответа CRM
    #   "visit_attendance" — fallback, если attendance отсутствует
    #   "default"          — оба поля отсутствовали, возвращено 0 по умолчанию
    attendance_source: str = "default"
    # Сырые attendance-related поля из ответа CRM (для диагностики debug endpoint).
    # Заполняется в get_client_crm_records(), до парсинга.
    raw_debug: dict = field(default_factory=dict)

    @property
    def is_confirmed(self) -> bool:
        """True если запись не отменена (confirmed == 1).

        Не означает, что клиент пришёл — только что запись не была отменена.
        Для проверки явки используй is_attended.
        """
        return self.confirmed == 1

    @property
    def is_attended(self) -> bool:
        """True если клиент фактически явился («Пришел», attendance == 1)."""
        return self.attendance == 1

    @property
    def is_active(self) -> bool:
        """True если запись не удалена и не отменена."""
        return not self.deleted and self.confirmed != 0


# ---------------------------------------------------------------------------
# Ссылка на клиента из ответа компании
# ---------------------------------------------------------------------------


@dataclass
class CrmClientRef:
    """Ссылка на клиента из ответа CRM при обходе записей компании за период.

    Используется в get_company_period_client_refs() для построения списка
    уникальных клиентов, обнаруженных в CRM за период кампании.
    """

    altegio_client_id: int
    name: str | None = None
    phone_raw: str | None = None  # Сырой телефон из CRM (не нормализован)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


def _parse_record_starts_at(record_data: dict[str, Any]) -> datetime | None:
    """Распарсить дату начала записи из ответа Altegio API → UTC.

    Приоритет: поле date (наивное локальное время Europe/Belgrade).
    Fallback: первые 19 символов поля datetime.
    None при любой ошибке парсинга.
    """
    raw_date = record_data.get("date")
    if isinstance(raw_date, str) and raw_date.strip():
        try:
            naive_dt = datetime.fromisoformat(raw_date.strip().replace(" ", "T"))
            return naive_dt.replace(tzinfo=_ALTEGIO_LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    raw_datetime = record_data.get("datetime")
    if isinstance(raw_datetime, str) and len(raw_datetime) >= 19:
        try:
            naive_dt = datetime.fromisoformat(raw_datetime[:19])
            return naive_dt.replace(tzinfo=_ALTEGIO_LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    if raw_date or raw_datetime:
        logger.warning(
            "crm record id=%s: cannot parse date=%r / datetime=%r",
            record_data.get("id"),
            raw_date,
            raw_datetime,
        )
    return None


def _parse_confirmed(record_data: dict[str, Any]) -> int:
    """Вернуть значение поля confirmed (0 или 1; по умолчанию 0)."""
    raw = record_data.get("confirmed")
    if raw is not None:
        try:
            return int(raw)
        except (ValueError, TypeError):
            pass
    return 0


def _parse_attendance_with_source(record_data: dict[str, Any]) -> tuple[int, str]:
    """Вернуть (attendance_value, source) из сырого ответа CRM.

    Источник истины: поле ``attendance`` из ответа Altegio CRM API.
    Fallback 1:     если ``attendance`` отсутствует или None — ``visit_attendance``.
                    Это поле присутствует в локальной модели Record и может
                    нести тот же семантический смысл «Пришел» для части записей.
    Default:        если оба поля отсутствуют — (0, "default").

    Если в будущем появится третье attendance-like поле из CRM, его следует
    добавить сюда как Fallback 2 перед возвратом дефолта.

    Возвращает:
        (value, source):
          value  — 0 или 1 (int);
          source — "attendance" | "visit_attendance" | "default" (str)
    """
    raw_att = record_data.get("attendance")
    if raw_att is not None:
        try:
            return int(raw_att), "attendance"
        except (ValueError, TypeError):
            pass

    raw_visit = record_data.get("visit_attendance")
    if raw_visit is not None:
        try:
            return int(raw_visit), "visit_attendance"
        except (ValueError, TypeError):
            pass

    return 0, "default"


def _parse_services(record_data: dict[str, Any]) -> tuple[list[int], list[str]]:
    """Извлечь service_ids и service_titles из записи CRM.

    Altegio API включает список услуг прямо в каждую запись.
    Формат: [{"id": 99001, "title": "Wimpernverlängerung", ...}, ...]
    """
    service_ids: list[int] = []
    service_titles: list[str] = []

    services = record_data.get("services")
    if not isinstance(services, list):
        return service_ids, service_titles

    for svc in services:
        if not isinstance(svc, dict):
            continue
        svc_id = svc.get("id")
        if isinstance(svc_id, int) and svc_id > 0:
            service_ids.append(svc_id)
        title = svc.get("title") or svc.get("name") or ""
        if title and isinstance(title, str):
            service_titles.append(title.strip())

    return service_ids, service_titles


# ---------------------------------------------------------------------------
# Основная функция: получить все записи клиента из CRM
# ---------------------------------------------------------------------------


async def get_client_crm_records(
    http_client: httpx.AsyncClient,
    *,
    company_id: int,
    altegio_client_id: int,
) -> list[CrmRecord]:
    """Получить ВСЕ записи клиента из Altegio CRM API.

    Выполняет постраничный обход endpoint /records/{company_id}?client_id=X.
    Возвращает все записи независимо от статуса и даты.

    Args:
        http_client: переиспользуемый AsyncClient (передаётся снаружи).
        company_id: ID компании в Altegio.
        altegio_client_id: ID клиента в Altegio CRM.

    Returns:
        Список CrmRecord со всеми записями клиента.

    Raises:
        CrmUnavailableError: при сетевой ошибке или HTTP-ошибке.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/records/{company_id}"
    page = 1
    all_records: list[CrmRecord] = []

    try:
        while True:
            params: dict[str, Any] = {
                "client_id": altegio_client_id,
                "count": _PAGE_SIZE,
                "page": page,
            }
            resp = await http_client.get(url, headers=_headers(), params=params)
            resp.raise_for_status()

            try:
                payload = resp.json()
            except Exception as exc:
                raise CrmUnavailableError(
                    f"CRM API returned invalid JSON: company={company_id} client={altegio_client_id}: {exc}"
                ) from exc

            if not isinstance(payload, dict):
                raise CrmUnavailableError(
                    f"CRM API returned unexpected payload type {type(payload).__name__}: "
                    f"company={company_id} client={altegio_client_id}"
                )

            data = payload.get("data")
            raw_records: list[dict[str, Any]] = []
            if data is None:
                # Empty response — treat as no records on this page
                pass
            elif isinstance(data, list):
                raw_records = data
            else:
                raise CrmUnavailableError(
                    f"CRM API returned unexpected 'data' type {type(data).__name__}: "
                    f"company={company_id} client={altegio_client_id}"
                )

            for rec in raw_records:
                starts_at = _parse_record_starts_at(rec)
                service_ids, service_titles = _parse_services(rec)
                att_val, att_source = _parse_attendance_with_source(rec)
                # Сохраняем сырые attendance-related поля для диагностики.
                # Берём ДО парсинга — именно то, что пришло из CRM.
                raw_debug: dict[str, Any] = {
                    "confirmed": rec.get("confirmed"),
                    "attendance": rec.get("attendance"),
                    "visit_attendance": rec.get("visit_attendance"),
                    "deleted": rec.get("deleted"),
                    "service_ids": [s.get("id") for s in rec.get("services", []) if isinstance(s, dict)],
                    "service_titles": [
                        s.get("title") or s.get("name") for s in rec.get("services", []) if isinstance(s, dict)
                    ],
                    "starts_at": rec.get("date") or rec.get("datetime"),
                }
                all_records.append(
                    CrmRecord(
                        crm_id=rec.get("id"),
                        starts_at=starts_at,
                        confirmed=_parse_confirmed(rec),
                        deleted=bool(rec.get("deleted")),
                        service_ids=service_ids,
                        service_titles=service_titles,
                        attendance=att_val,
                        attendance_source=att_source,
                        raw_debug=raw_debug,
                    )
                )

            if len(raw_records) < _PAGE_SIZE:
                break

            logger.debug(
                "crm records page=%d company=%d client=%d total_so_far=%d",
                page,
                company_id,
                altegio_client_id,
                len(all_records),
            )
            page += 1

    except httpx.HTTPError as exc:
        raise CrmUnavailableError(
            f"CRM API недоступен: company={company_id} client={altegio_client_id}: {exc}"
        ) from exc

    return all_records


# ---------------------------------------------------------------------------
# Компания-уровень: получить уникальных клиентов за период
# ---------------------------------------------------------------------------


async def get_company_period_client_refs(
    http_client: httpx.AsyncClient,
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[CrmClientRef]:
    """Получить уникальных клиентов компании за период из Altegio CRM.

    Пагинирует GET /records/{company_id}?start_date=...&end_date=...
    Используется для CRM-based discovery в find_candidates() вместо
    локальной БД. Позволяет обнаружить клиентов, которые посещали салон
    до запуска бота (нет записей в локальной records таблице).

    Даты конвертируются из UTC в Europe/Belgrade (локальное время Altegio).
    start_date — inclusive, end_date — inclusive в формате YYYY-MM-DD.
    period_end (exclusive UTC) → end_date = (period_end - 1 day).local.date().

    Args:
        http_client: переиспользуемый AsyncClient (передаётся снаружи).
        company_id: ID компании в Altegio.
        period_start: начало периода (inclusive), UTC.
        period_end: конец периода (exclusive), UTC.

    Returns:
        Список уникальных CrmClientRef, по одному на altegio_client_id.

    Raises:
        CrmUnavailableError: при сетевой ошибке или HTTP-ошибке.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/records/{company_id}"
    page = 1
    seen_client_ids: set[int] = set()
    refs: list[CrmClientRef] = []

    # period_end exclusive → subtract 1 day for inclusive end_date param
    local_start = period_start.astimezone(_ALTEGIO_LOCAL_TZ)
    local_end = (period_end - timedelta(days=1)).astimezone(_ALTEGIO_LOCAL_TZ)
    start_date_str = local_start.strftime("%Y-%m-%d")
    end_date_str = local_end.strftime("%Y-%m-%d")

    try:
        while True:
            params: dict[str, Any] = {
                "start_date": start_date_str,
                "end_date": end_date_str,
                "count": _PAGE_SIZE,
                "page": page,
            }
            resp = await http_client.get(url, headers=_headers(), params=params)
            resp.raise_for_status()

            try:
                payload = resp.json()
            except Exception as exc:
                raise CrmUnavailableError(f"CRM API returned invalid JSON: company={company_id}: {exc}") from exc

            if not isinstance(payload, dict):
                raise CrmUnavailableError(
                    f"CRM API returned unexpected payload type {type(payload).__name__}: company={company_id}"
                )

            data = payload.get("data")
            raw_records: list[dict[str, Any]] = []
            if data is None:
                pass
            elif isinstance(data, list):
                raw_records = data
            else:
                raise CrmUnavailableError(
                    f"CRM API returned unexpected 'data' type {type(data).__name__}: company={company_id}"
                )

            for rec in raw_records:
                # Try nested "client" dict first, then flat "client_id"
                client_data = rec.get("client")
                if isinstance(client_data, dict):
                    client_id = client_data.get("id")
                    name = client_data.get("name") or client_data.get("display_name")
                    phone_raw = client_data.get("phone")
                else:
                    client_id = rec.get("client_id")
                    name = None
                    phone_raw = None

                if not isinstance(client_id, int) or client_id <= 0:
                    continue

                if client_id not in seen_client_ids:
                    seen_client_ids.add(client_id)
                    refs.append(
                        CrmClientRef(
                            altegio_client_id=client_id,
                            name=name,
                            phone_raw=phone_raw,
                        )
                    )

            if len(raw_records) < _PAGE_SIZE:
                break

            logger.debug(
                "crm company period page=%d company=%d start=%s end=%s refs_so_far=%d",
                page,
                company_id,
                start_date_str,
                end_date_str,
                len(refs),
            )
            page += 1

    except httpx.HTTPError as exc:
        raise CrmUnavailableError(f"CRM API недоступен (company period): company={company_id}: {exc}") from exc

    logger.info(
        "crm company period discovery: company=%d period=[%s, %s) unique_clients=%d",
        company_id,
        start_date_str,
        end_date_str,
        len(refs),
    )
    return refs


# ---------------------------------------------------------------------------
# Хелпер: классификация записей по периоду
# ---------------------------------------------------------------------------


def classify_crm_records(
    records: list[CrmRecord],
    period_start: datetime,
    period_end: datetime,
) -> tuple[list[CrmRecord], int]:
    """Разбить записи CRM на «в периоде» и «до периода».

    Args:
        records: все записи клиента из CRM.
        period_start: начало периода (inclusive), UTC.
        period_end: конец периода (exclusive), UTC.

    Returns:
        (in_period_records, count_before_period):
          - in_period_records: не удалённые записи с starts_at в [period_start, period_end)
          - count_before_period: кол-во записей (любого статуса, включая удалённые)
            с starts_at < period_start.

    Записи с starts_at == None попадают в «неизвестные» и не учитываются ни там, ни там.
    Это консервативно: если дату распарсить нельзя, в неизвестное не засчитываем.
    """
    in_period: list[CrmRecord] = []
    count_before = 0

    for rec in records:
        if rec.starts_at is None:
            continue
        if rec.starts_at < period_start:
            count_before += 1
        elif period_start <= rec.starts_at < period_end:
            if not rec.deleted:
                in_period.append(rec)

    return in_period, count_before
