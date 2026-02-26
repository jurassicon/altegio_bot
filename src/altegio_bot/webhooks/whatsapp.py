from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import select
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


def _extract_phone_number_id(payload: dict[str, Any]) -> str | None:
    try:
        entry0 = (payload.get('entry') or [])[0]
        changes0 = (entry0.get('changes') or [])[0]
        value = changes0.get('value') or {}
        metadata = value.get('metadata') or {}
        pni = metadata.get('phone_number_id')
        if pni is None:
            return None
        return str(pni)
    except Exception:
        return None


def _parse_allowed_phone_number_ids() -> set[str]:
    raw = getattr(settings, 'whatsapp_allowed_phone_number_ids', '')
    ids: list[str] = []

    if raw:
        ids = [x.strip() for x in str(raw).split(',')]
        ids = [x for x in ids if x]

    if not ids and settings.meta_wa_phone_number_id:
        ids = [str(settings.meta_wa_phone_number_id).strip()]

    return {x for x in ids if x}


def _verify_signature(
    *,
    body: bytes,
    signature_header: str | None,
    app_secret: str,
) -> bool:
    if not signature_header:
        return False

    if not signature_header.startswith('sha256='):
        return False

    received = signature_header.split('=', 1)[1].strip()
    mac = hmac.new(app_secret.encode('utf-8'), body, hashlib.sha256)
    expected = mac.hexdigest()

    return hmac.compare_digest(received, expected)


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
    body = await request.body()

    try:
        payload: dict[str, Any] = json.loads(body.decode('utf-8'))
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid JSON')

    query = dict(request.query_params)
    headers = dict(request.headers)

    app_secret = getattr(settings, 'meta_app_secret', '')
    if app_secret:
        sig = (
            request.headers.get('x-hub-signature-256')
            or request.headers.get('X-Hub-Signature-256')
        )
        if not _verify_signature(
            body=body,
            signature_header=sig,
            app_secret=app_secret,
        ):
            logger.warning('Webhook signature mismatch')
            raise HTTPException(status_code=403, detail='Bad signature')

    pni = _extract_phone_number_id(payload)
    allowed = _parse_allowed_phone_number_ids()

    status = 'received'
    error: str | None = None
    ignored = False

    if pni is not None and allowed and pni not in allowed:
        status = 'ignored'
        ignored = True
        error = f'Ignored: phone_number_id not allowed ({pni})'
        logger.info('Ignored webhook event phone_number_id=%s', pni)

    dedupe_key = _payload_dedupe_key(payload)

    async with SessionLocal() as session:
        try:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key=dedupe_key,
                    status=status,
                    error=error,
                    query=query,
                    headers=headers,
                    payload=payload,
                )
                session.add(evt)
                await session.flush()

            return JSONResponse(
                {
                    'ok': True,
                    'duplicate': False,
                    'ignored': ignored,
                    'id': evt.id,
                    'dedupe_key': dedupe_key,
                    'phone_number_id': pni,
                }
            )

        except IntegrityError:
            await session.rollback()
            logger.info('Duplicate whatsapp event: %s', dedupe_key)

            res = await session.execute(
                select(WhatsAppEvent.id).where(
                    WhatsAppEvent.dedupe_key == dedupe_key
                )
            )
            existing_id = res.scalar_one_or_none()

            return JSONResponse(
                {
                    'ok': True,
                    'duplicate': True,
                    'ignored': ignored,
                    'id': existing_id,
                    'dedupe_key': dedupe_key,
                    'phone_number_id': pni,
                }
            )