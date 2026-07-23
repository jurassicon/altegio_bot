"""Обработчик capture вебхуков EasyWeek (research-grade).

Первый шаг интеграции EasyWeek: сохраняем КАЖДУЮ аутентифицированную доставку
дословно, чтобы по живым данным разобрать (недокументированную) структуру
payload и семантику доставок. Модуль сознательно НЕ делает обработки — никакой
нормализации, воркеров, джобов — только запись строк ``EasyWeekEvent``.

Дизайн (зафиксирован):
  - URL вебхука полностью под нашим контролем в кабинете EasyWeek, поэтому тип
    события и токен живут в query:
      https://<host>/webhooks/easyweek?event=booking-created&token=<secret>
    Секрет в query означает, что access-логи НЕ должны писать query string:
    uvicorn запускается с --no-access-log, а собственный access-лог в main.py
    пишет только путь (см. docs/easyweek/capture_runbook.md).
  - Аутентификация — только общий секрет через ``hmac.compare_digest``. Пустой
    ``easyweek_webhook_secret`` держит эндпоинт закрытым (403) даже при
    включённом флаге. Выключенная интеграция отвечает 404 (неотличимо от
    несуществующего маршрута — поверхность не раскрываем до go-live).
  - Идемпотентности нет намеренно: каждая доставка (включая ретраи и Resend)
    становится своей строкой. Повторы анализируются по НЕуникальному индексу
    ``payload_hash``.
  - Источник истины по содержимому — ``body_raw``: до 128 КиБ ИСХОДНЫХ байт.
    ``payload`` (JSONB) — лишь их разбор, теряющий порядок ключей, пробелы,
    формат чисел, дубли ключей и невалидный UTF-8.

Контракт кодов ответа:
  Authenticated deliveries receive 200 after successful durable persistence.
  Infrastructure persistence failures return 503 so EasyWeek can retry.

То есть проблемы СОДЕРЖИМОГО (не-JSON, NUL, невалидный UTF-8, NaN, слишком
большое тело) не являются ошибкой: такая доставка приводится к безопасному виду,
СОХРАНЯЕТСЯ и получает 200. А вот недоступная БД, отсутствующая таблица или
исчерпание пула — это 503 без записи: durable spool в PR-1 нет, поэтому ответить
200 по незаписанной строке значило бы потерять доставку безвозвратно.

PII-безопасность: payload, тело, заголовки и значения query никогда не
логируются — включая ``event_hint``, который приходит из query и не валидируется.
Чувствительные query-параметры маскируются, а чувствительные заголовки
выбрасываются до записи строки.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy.exc import SQLAlchemyError

from ..db import SessionLocal
from ..models.models import EasyWeekEvent
from ..perf import perf_log
from ..settings import settings
from .common import (
    canonical_json_hash,
    mask_query,
    postgres_safe_text,
    read_bounded_body,
    safe_headers,
    token_matches,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Порог сырых байт тела. Данные capture — исследовательские, а не безлимитное
# хранилище; тело читается потоком, поэтому лимит ограничивает и пиковую память,
# а не только объём записи. Факт превышения фиксируется в body_truncated,
# фактический размер — в body_size_bytes.
_MAX_BODY_BYTES = 128 * 1024

# Заголовок-фолбэк для токена, если кастомные параметры EasyWeek окажутся в
# заголовках, а не в query.
_TOKEN_HEADER = "X-Altegio-Token"


def _text_projection(raw: bytes) -> str:
    """Postgres-безопасная текстовая проекция сырых байт.

    ``errors="replace"`` чинит невалидный UTF-8, но НЕ трогает NUL (0x00 —
    валидный UTF-8), который Postgres в TEXT не примет. Исходные байты не
    теряются: они целиком лежат в ``body_raw``.
    """
    return postgres_safe_text(raw.decode("utf-8", errors="replace"))


@router.post("/webhooks/easyweek")
async def easyweek_webhook(request: Request) -> dict[str, bool]:
    # 1) Выключенная интеграция неотличима от отсутствующего маршрута.
    if not settings.easyweek_enabled:
        raise HTTPException(status_code=404, detail="Not Found")

    # 2) Аутентификация. Пустой секрет закрывает эндпоинт безусловно — даже если
    #    прислали пустой токен, который формально «совпал» бы с ним.
    secret = settings.easyweek_webhook_secret
    if not secret:
        raise HTTPException(status_code=403, detail="Forbidden")

    query = dict(request.query_params)
    query_token = query.get("token")
    header_token = request.headers.get(_TOKEN_HEADER)

    # Query — авторитетный источник аутентификации, когда токен там есть;
    # заголовок только фолбэк. Присутствующий, но неверный query-токен НЕ должен
    # молча откатываться на заголовок — иначе ротация/опечатка секрета
    # маскируется. Проверка `header_token is not None` в elif обязательна: без
    # неё token_matches(None, ...) упал бы с AttributeError на обычном запросе
    # без этого заголовка.
    auth_via: str | None = None
    if query_token is not None:
        if token_matches(query_token, secret):
            auth_via = "query"
    elif header_token is not None and token_matches(header_token, secret):
        auth_via = "header"

    if auth_via is None:
        # Неверный/отсутствующий токен: отказываем и ничего не сохраняем.
        raise HTTPException(status_code=403, detail="Forbidden")

    # 3) Чтение тела, парсинг и запись под одним perf-спаном. Спан покрывает
    #    чтение+парсинг+хэш+вставку (на больших телах парсинг/хэш доминируют в
    #    латентности) и не логирует значений payload/query/тела.
    with perf_log("webhook", "easyweek_webhook"):
        # Потоковое чтение: в памяти держим максимум лимит + текущий chunk,
        # независимо от того, сколько прислали.
        raw, body_size_bytes, body_truncated = await read_bounded_body(request, limit=_MAX_BODY_BYTES)

        payload: dict = {}
        payload_hash: str | None = None
        body_text: str | None = None

        if body_truncated:
            # Тело не поместилось в лимит: парсить нечего (обрезанный JSON всё
            # равно невалиден), а CPU/RAM на произвольно больших и
            # злонамеренных телах ограничены. Сохраняем префикс и полный размер.
            body_text = _text_projection(raw)
        else:
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError, RecursionError):
                # Не JSON. UnicodeDecodeError (невалидный UTF-8) — подкласс
                # ValueError и ловится здесь же. RecursionError (подкласс
                # RuntimeError, который json.loads бросает на глубоко вложенном
                # входе) ловим тоже, чтобы глубокое тело сохранилось, а не стало
                # 500, который EasyWeek будет ретраить бесконечно.
                body_text = _text_projection(raw)
            else:
                # Колонка JSONB типизирована как dict: не-dict корень
                # оборачиваем, чтобы колонка и будущие читатели всегда видели
                # объект.
                candidate = parsed if isinstance(parsed, dict) else {"_non_dict_payload": parsed}
                try:
                    # strict=True отвергает всё, что Postgres JSONB не примет:
                    # NaN/Infinity, переполнение экспоненты, NUL внутри строк и
                    # ключей, непарные суррогаты. Без этой проверки запись
                    # упала бы на commit, и доставка терялась бы на каждом
                    # ретрае. При отказе — откат в текстовую проекцию.
                    payload_hash = canonical_json_hash(candidate, strict=True)
                except (ValueError, TypeError, RecursionError):
                    body_text = _text_projection(raw)
                else:
                    payload = candidate

        # 4) Запись. Маскируем чувствительные query-значения, выбрасываем
        #    чувствительные заголовки; и то и другое приводится к
        #    Postgres-безопасному виду внутри хелперов.
        safe_query = mask_query(query)

        event_hint = query.get("event")
        if event_hint is not None:
            event_hint = postgres_safe_text(event_hint)[:32]

        content_type = request.headers.get("content-type")
        if content_type is not None:
            content_type = postgres_safe_text(content_type)[:128]

        # status получает значение "captured" из умолчания колонки модели.
        event = EasyWeekEvent(
            event_hint=event_hint,
            auth_via=auth_via,
            payload_hash=payload_hash,
            content_type=content_type,
            body_raw=raw,
            body_size_bytes=body_size_bytes,
            body_text=body_text,
            body_truncated=body_truncated,
            query=safe_query,
            headers=safe_headers(request, extra_deny={_TOKEN_HEADER.lower()}),
            payload=payload,
        )

        try:
            async with SessionLocal() as session:
                session.add(event)
                try:
                    await session.commit()
                except SQLAlchemyError:
                    await session.rollback()
                    raise
                event_id = event.id
        except SQLAlchemyError as exc:
            # Инфраструктурный отказ, а не дефект содержимого: все контентные
            # проблемы уже приведены к безопасному виду выше. Логируем только
            # класс исключения — его текст может содержать SQL-параметры, то
            # есть payload и PII.
            logger.error(
                "easyweek capture persistence failed operation=%s error_type=%s "
                "auth_via=%s body_truncated=%s body_size_bytes=%s",
                "easyweek_webhook",
                type(exc).__name__,
                auth_via,
                body_truncated,
                body_size_bytes,
            )
            # 503 (а не 200) — строка не записана, а durable spool в PR-1 нет:
            # пусть EasyWeek повторит доставку.
            raise HTTPException(status_code=503, detail="Service Unavailable") from None

    # Только безопасные метаданные строки. event_hint сюда НЕ попадает: это
    # неотвалидированное значение из query.
    logger.info(
        "easyweek capture stored id=%s auth_via=%s body_truncated=%s body_size_bytes=%s",
        event_id,
        auth_via,
        body_truncated,
        body_size_bytes,
    )

    # 5) Доставка надёжно сохранена — подтверждаем, чтобы EasyWeek не отключил
    #    вебхук за серию неуспешных ответов.
    return {"ok": True}
