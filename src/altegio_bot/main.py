from __future__ import annotations

import hashlib
import json
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from sqlalchemy.exc import IntegrityError

from .db import SessionLocal
from .models import AltegioEvent
from .settings import settings
from .webhooks.whatsapp import router as whatsapp_router

app = FastAPI(title=settings.app_name)
app.include_router(whatsapp_router)


def _safe_headers(request: Request) -> dict[str, str]:
    # Не сохраняем потенциально чувствительные заголовки
    deny = {'authorization', 'cookie'}
    out: dict[str, str] = {}
    for key, value in request.headers.items():
        low_key = key.lower()
        if low_key in deny:
            continue
        out[key] = value
    return out


def _make_dedupe_key(payload: dict[str, Any], query: dict[str, Any]) -> str:
    '''
    Стабильный ключ, чтобы одинаковый вебхук не обработался дважды.
    Берём главные поля + last_change_date (если есть), иначе хэш всего payload.
    '''
    company_id = payload.get('company_id')
    resource = payload.get('resource') or payload.get('type')
    resource_id = payload.get('resource_id')
    event_status = payload.get('status')
    last_change = (payload.get('data') or {}).get('last_change_date')
    secret = query.get('secret') or query.get('userGuid')

    main_fields = [company_id, resource, resource_id, event_status]
    if any(x is None for x in main_fields):
        canon = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        )
        digest = hashlib.sha256(canon.encode('utf-8')).hexdigest()
        base = f'fallback:{digest}'
    else:
        base = (
            f'{company_id}:{resource}:{resource_id}:{event_status}:'
            f'{last_change}:{secret}'
        )

    return hashlib.sha256(base.encode('utf-8')).hexdigest()


@app.get('/health')
async def health() -> dict[str, bool]:
    return {'ok': True}


@app.post('/webhooks/altegio')
async def altegio_webhook(request: Request) -> dict[str, bool]:
    # 1) проверяем секрет (в логах это query param 'secret')
    query = dict(request.query_params)
    provided = query.get('secret')
    if provided != settings.altegio_webhook_secret:
        raise HTTPException(status_code=403, detail='Invalid webhook secret')

    # 2) читаем payload
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid JSON')

    # 3) сохраняем в inbox
    dedupe_key = _make_dedupe_key(payload, query)

    event = AltegioEvent(
        dedupe_key=dedupe_key,
        company_id=payload.get('company_id'),
        resource=payload.get('resource'),
        resource_id=payload.get('resource_id'),
        event_status=payload.get('status'),
        query=query,
        headers=_safe_headers(request),
        payload=payload,
    )

    async with SessionLocal() as session:
        session.add(event)
        try:
            await session.commit()
        except IntegrityError:
            # Уже получили такое событие — отвечаем ok (идемпотентность)
            await session.rollback()

    return {'ok': True}
