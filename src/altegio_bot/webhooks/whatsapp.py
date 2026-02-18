from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy.exc import IntegrityError

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings

logger = logging.getLogger('whatsapp_webhook')

router = APIRouter()


def _payload_dedupe_key(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    return f'wa:{digest}'


@router.get('/webhook/whatsapp')
async def whatsapp_verify(request: Request) -> Response:
    qp = request.query_params
    mode = qp.get('hub.mode')
    token = qp.get('hub.verify_token')
    challenge = qp.get('hub.challenge')

    if mode != 'subscribe' or not challenge:
        raise HTTPException(status_code=400, detail='Invalid verify request')

    if token != settings.whatsapp_webhook_verify_token:
        raise HTTPException(status_code=403, detail='Verify token mismatch')

    return Response(content=challenge, media_type='text/plain')


@router.post('/webhook/whatsapp')
async def whatsapp_ingest(request: Request) -> Response:
    payload = await request.json()

    dedupe_key = _payload_dedupe_key(payload)
    query = dict(request.query_params)
    headers = dict(request.headers)

    async with SessionLocal() as session:
        async with session.begin():
            evt = WhatsAppEvent(
                dedupe_key=dedupe_key,
                status='received',
                error=None,
                query=query,
                headers=headers,
                payload=payload,
            )
            session.add(evt)
            try:
                await session.flush()
            except IntegrityError:
                logger.info('Duplicate whatsapp event: %s', dedupe_key)

    return Response(status_code=200)
