"""Обработчик capture вебхуков EasyWeek (research-grade).

Первый шаг интеграции EasyWeek: сохраняем КАЖДУЮ аутентифицированную доставку
дословно, чтобы по живым данным разобрать (недокументированную) структуру
payload и семантику доставок. Модуль сознательно НЕ делает обработки — никакой
нормализации, воркеров, джобов — только запись строк ``EasyWeekEvent``.

Дизайн (зафиксирован):
  - URL вебхука полностью под нашим контролем в кабинете EasyWeek, поэтому тип
    события и токен живут в query:
      https://<host>/webhooks/easyweek?event=booking-created&token=<secret>
  - Аутентификация — только общий секрет через ``hmac.compare_digest``. Пустой
    ``easyweek_webhook_secret`` держит эндпоинт закрытым (403) даже при
    включённом флаге. Выключенная интеграция отвечает 404 (неотличимо от
    несуществующего маршрута — поверхность не раскрываем до go-live).
  - Идемпотентности нет намеренно: каждая доставка (включая ретраи и Resend)
    становится своей строкой. Повторы анализируются по НЕуникальному индексу
    ``payload_hash``.
  - Любая аутентифицированная доставка — даже с битым/не-JSON телом — получает
    200 {"ok": true}, иначе EasyWeek засчитает доставку неуспешной и
    автоматически отключит вебхук.

PII-безопасность: payload, текст тела, заголовки и значения query никогда не
логируются; чувствительные query-параметры маскируются, а чувствительные
заголовки выбрасываются до записи строки.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request

from ..db import SessionLocal
from ..models.models import EasyWeekEvent
from ..perf import perf_log
from ..settings import settings
from .common import canonical_json_hash, mask_query, safe_headers, token_matches

logger = logging.getLogger(__name__)

router = APIRouter()

# Порог сырых байт тела до декодирования/парсинга. Данные capture —
# исследовательские, а не безлимитное хранилище; факт обрезки фиксируется.
_MAX_BODY_BYTES = 128 * 1024

# Заголовок-фолбэк для токена, если кастомные параметры EasyWeek окажутся в
# заголовках, а не в query.
_TOKEN_HEADER = "X-Altegio-Token"


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

    # 3) Чтение тела, парсинг и запись под одним perf-спаном. Capture никогда не
    #    отвечает ошибкой: не-JSON/огромное/глубоко вложенное тело сохраняется
    #    как текст, а не превращается в 400/500. Спан покрывает
    #    чтение+парсинг+хэш+вставку (на больших телах парсинг/хэш доминируют в
    #    латентности) и не логирует значений payload/query/тела.
    with perf_log("webhook", "easyweek_webhook"):
        raw = await request.body()
        payload: dict = {}
        payload_hash: str | None = None
        body_text: str | None = None
        body_truncated = False

        if len(raw) > _MAX_BODY_BYTES:
            # Слишком большое тело (JSON или нет): обрезаем и НЕ парсим. Это
            # ограничивает CPU/RAM на произвольно больших или злонамеренных
            # JSON, но факт крупной доставки всё равно фиксируется.
            body_truncated = True
            body_text = raw[:_MAX_BODY_BYTES].decode("utf-8", errors="replace")
        else:
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError, RecursionError):
                # Не JSON. RecursionError (подкласс RuntimeError, который
                # json.loads бросает на глубоко вложенном входе) ловим тоже,
                # чтобы глубокое тело сохранилось, а не стало 500, который
                # EasyWeek будет ретраить бесконечно.
                body_text = raw.decode("utf-8", errors="replace")
            else:
                # Колонка JSONB типизирована как dict: не-dict корень
                # оборачиваем, чтобы колонка и будущие читатели всегда видели
                # объект.
                candidate = parsed if isinstance(parsed, dict) else {"_non_dict_payload": parsed}
                try:
                    # strict=True отвергает NaN/Infinity и переполнение
                    # экспоненты (например 1e1000000 -> inf): Postgres JSONB эти
                    # токены не принимает, поэтому запись упала бы на commit и
                    # доставка терялась бы на каждом ретрае. При отказе —
                    # откат в сырой текст.
                    payload_hash = canonical_json_hash(candidate, strict=True)
                except ValueError:
                    body_text = raw.decode("utf-8", errors="replace")
                else:
                    payload = candidate

        # 4) Запись. Сначала маскируем чувствительные query-значения и
        #    выбрасываем чувствительные заголовки.
        safe_query = mask_query(query)

        event_hint = query.get("event")
        if event_hint is not None:
            event_hint = event_hint[:32]

        content_type = request.headers.get("content-type")
        if content_type is not None:
            content_type = content_type[:128]

        # status получает значение "captured" из умолчания колонки модели.
        event = EasyWeekEvent(
            event_hint=event_hint,
            auth_via=auth_via,
            payload_hash=payload_hash,
            content_type=content_type,
            body_text=body_text,
            body_truncated=body_truncated,
            query=safe_query,
            headers=safe_headers(request, extra_deny={_TOKEN_HEADER.lower()}),
            payload=payload,
        )
        async with SessionLocal() as session:
            session.add(event)
            await session.commit()
            event_id = event.id

    # Только метаданные строки: ни payload, ни тела, ни заголовков, ни query.
    logger.info(
        "easyweek capture stored id=%s event_hint=%s auth_via=%s body_truncated=%s",
        event_id,
        event_hint,
        auth_via,
        body_truncated,
    )

    # 5) Аутентифицированную доставку всегда подтверждаем, чтобы EasyWeek не
    #    отключил вебхук.
    return {"ok": True}
