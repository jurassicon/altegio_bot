from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from sqlalchemy.exc import IntegrityError
from starlette.middleware.base import BaseHTTPMiddleware

from .db import SessionLocal
from .models import AltegioEvent
from .ops.campaigns_api import router as campaigns_router
from .ops.router import login_router as ops_login_router
from .ops.router import router as ops_router
from .settings import settings
from .webhooks.chatwoot import router as chatwoot_router
from .webhooks.common import mask_query, safe_headers, token_matches
from .webhooks.easyweek import router as easyweek_router
from .webhooks.whatsapp import router as whatsapp_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

access_logger = logging.getLogger("altegio_bot.access")

app = FastAPI(title=settings.app_name)
app.include_router(whatsapp_router)
app.include_router(chatwoot_router)
app.include_router(easyweek_router)  # public: POST /webhooks/easyweek (token in query)
app.include_router(ops_login_router)  # public: /ops/login, /ops/logout
app.include_router(campaigns_router)  # protected: /ops/campaigns/ (JSON) — регистрируем до ops_router,
# чтобы точные маршруты JSON API (/runs, /runs/{id}, /dashboard/monthly) имели приоритет
# перед wildcard HTML-маршрутами (/campaigns/{run_id: int})
app.include_router(ops_router)  # protected: /ops/ (HTML dashboard)


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Access-лог БЕЗ query string.

    Стандартный access-лог uvicorn пишет полный request target вместе с query, а
    вебхуки носят секрет именно там (``/webhooks/easyweek?token=...``,
    ``/webhooks/altegio?secret=...``). Поэтому uvicorn в проде запускается с
    ``--no-access-log`` (Dockerfile / docker-compose.yml), а наблюдаемость даёт
    этот middleware: метод, ПУТЬ, статус и длительность — и ничего больше.
    Reverse proxy обязан быть настроен так же (см.
    docs/easyweek/capture_runbook.md): без ``$request``, ``$request_uri``, ``$args``.
    """

    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        status_code = 500  # если обработчик упадёт, в лог попадёт именно это
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            access_logger.info(
                "%s %s %s %.1fms",
                request.method,
                request.url.path,
                status_code,
                (time.perf_counter() - start) * 1000,
            )


app.add_middleware(AccessLogMiddleware)


def _make_dedupe_key(payload: dict[str, Any], query: dict[str, Any]) -> str:
    """
    Стабильный ключ, чтобы одинаковый вебхук не обработался дважды.
    Берём главные поля + last_change_date (если есть), иначе хэш всего payload.
    """
    company_id = payload.get("company_id")
    resource = payload.get("resource") or payload.get("type")
    resource_id = payload.get("resource_id")
    event_status = payload.get("status")
    last_change = (payload.get("data") or {}).get("last_change_date")
    secret = query.get("secret") or query.get("userGuid")

    main_fields = [company_id, resource, resource_id, event_status]
    if any(x is None for x in main_fields):
        canon = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canon.encode("utf-8")).hexdigest()
        base = f"fallback:{digest}"
    else:
        base = f"{company_id}:{resource}:{resource_id}:{event_status}:{last_change}:{secret}"

    return hashlib.sha256(base.encode("utf-8")).hexdigest()


@app.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/webhooks/altegio")
async def altegio_webhook(request: Request) -> dict[str, bool]:
    # 1) проверяем секрет (query param 'secret'; в access-лог query не попадает)
    query = dict(request.query_params)
    provided = query.get("secret")
    # constant-time сравнение вместо `!=`; token_matches кодирует в utf-8, чтобы
    # не-ASCII секрет не бросал TypeError (это стало бы 500 вместо 403).
    if provided is None or not token_matches(provided, settings.altegio_webhook_secret):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    # 2) читаем payload
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # 3) сохраняем в inbox
    dedupe_key = _make_dedupe_key(payload, query)

    event = AltegioEvent(
        dedupe_key=dedupe_key,
        company_id=payload.get("company_id"),
        resource=payload.get("resource"),
        resource_id=payload.get("resource_id"),
        event_status=payload.get("status"),
        # Маскируем только СОХРАНЯЕМУЮ копию: авторизация выше и _make_dedupe_key
        # ниже работают с оригинальным query, поэтому дедупликация не меняется.
        # Исторические строки этой правкой не чинятся — их чистка отдельная
        # операция с бэкапом.
        query=mask_query(query),
        headers=safe_headers(request),
        payload=payload,
    )

    async with SessionLocal() as session:
        session.add(event)
        try:
            await session.commit()
        except IntegrityError:
            # Уже получили такое событие — отвечаем ok (идемпотентность)
            await session.rollback()

    return {"ok": True}
