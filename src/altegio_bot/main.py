from __future__ import annotations

import hashlib
import json
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from .db import SessionLocal
from .models import AltegioEvent
from .settings import settings

app = FastAPI(title=settings.app_name)


def _safe_headers(request: Request) -> dict[str, str]:
    # Не сохраняем потенциально чувствительные заголовки
    deny = {"authorization", "cookie"}
    out: dict[str, str] = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk in deny:
            continue
        out[k] = v
    return out


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

    base = (f"{company_id}:{resource}:{resource_id}:{event_status}:"
            f"{last_change}:{query.get('secret') or query.get('userGuid')}")
    if any(x is None for x in
           [company_id, resource, resource_id, event_status]):
        # fallback: хэш канонического json
        canon = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
        base = f"fallback:{hashlib.sha256(canon.encode('utf-8')).hexdigest()}"

    return hashlib.sha256(base.encode("utf-8")).hexdigest()


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/webhooks/altegio")
async def altegio_webhook(request: Request):
    # 1) проверяем секрет (в твоих логах это query param "secret")
    q = dict(request.query_params)
    provided = q.get("secret")
    if provided != settings.altegio_webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    # 2) читаем payload
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # 3) сохраняем в inbox
    dedupe_key = _make_dedupe_key(payload, q)

    company_id = payload.get("company_id")
    resource = payload.get("resource")
    resource_id = payload.get("resource_id")
    event_status = payload.get("status")

    event = AltegioEvent(
        dedupe_key=dedupe_key,
        company_id=company_id,
        resource=resource,
        resource_id=resource_id,
        event_status=event_status,
        query=q,
        headers=_safe_headers(request),
        payload=payload,
    )

    async with SessionLocal() as session:
        session.add(event)
        try:
            await session.commit()
        except IntegrityError:
            # уже получили такое событие — просто отвечаем ok (идемпотентность)
            await session.rollback()
        return {"ok": True}
