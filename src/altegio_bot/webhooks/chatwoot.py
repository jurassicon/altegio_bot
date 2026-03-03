"""Chatwoot webhook handler."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings

logger = logging.getLogger('chatwoot_webhook')

router = APIRouter()


def _safe_headers(request: Request) -> dict[str, str]:
    deny = {'authorization', 'cookie', 'x-chatwoot-signature'}
    return {k: v for k, v in request.headers.items() if k.lower() not in deny}


@router.post('/webhook/chatwoot')
async def chatwoot_ingest(request: Request) -> JSONResponse:
    """Receive webhooks from Chatwoot on message_created events."""

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid JSON')

    # Log for debugging
    logger.info(
        'Chatwoot webhook received: event=%s', payload.get('event')
    )

    event_type = payload.get('event')
    if event_type != 'message_created':
        return JSONResponse({'ok': True, 'skipped': f'event={event_type}'})

    message = payload.get('message', {})
    message_type = message.get('message_type')

    # Only process incoming messages (from customers, not agents)
    if message_type != 'incoming':
        logger.info('Skipping non-incoming message: message_type=%s',
                    message_type)
        return JSONResponse(
            {'ok': True, 'skipped': f'message_type={message_type}'})

    conversation = payload.get('conversation', {})
    sender = payload.get('sender', {})

    phone_e164 = sender.get('phone_number')
    text = message.get('content', '')
    chatwoot_message_id = message.get('id')
    chatwoot_conversation_id = conversation.get('id')

    if not phone_e164:
        logger.warning('Missing phone_number in webhook payload')
        raise HTTPException(status_code=400, detail='Missing phone_number')

    if not chatwoot_message_id:
        logger.warning('Missing message id in webhook payload')
        raise HTTPException(status_code=400, detail='Missing message_id')

    # Normalize to Meta webhook format (compatible with existing worker)
    normalized_payload = {
        'entry': [{
            'changes': [{
                'value': {
                    'messages': [{
                        'from': phone_e164,
                        'type': 'text',
                        'text': {'body': text},
                        'id': str(chatwoot_message_id),
                        'timestamp': str(int(message.get('created_at', 0))),
                    }],
                    'metadata': {
                        'phone_number_id': settings.meta_wa_phone_number_id
                    }
                }
            }]
        }],
        '_chatwoot': {
            'conversation_id': chatwoot_conversation_id,
            'message_id': chatwoot_message_id,
            'account_id': payload.get('account', {}).get('id'),
        }
    }

    dedupe_key = f"chatwoot:{chatwoot_conversation_id}:{chatwoot_message_id}"

    async with SessionLocal() as session:
        try:
            async with session.begin():
                event = WhatsAppEvent(
                    dedupe_key=dedupe_key,
                    status='received',
                    error=None,
                    query=dict(request.query_params),
                    headers=_safe_headers(request),
                    payload=normalized_payload,
                )
                session.add(event)
                await session.flush()

                logger.info(
                    'Chatwoot webhook saved: conv_id=%s msg_id=%s phone=%s text="%s"',
                    chatwoot_conversation_id, chatwoot_message_id, phone_e164,
                    text[:50]
                )

                return JSONResponse({
                    'ok': True,
                    'duplicate': False,
                    'id': event.id,
                    'dedupe_key': dedupe_key,
                })

        except IntegrityError:
            await session.rollback()
            logger.info('Duplicate chatwoot event: %s', dedupe_key)
            return JSONResponse({
                'ok': True,
                'duplicate': True,
                'dedupe_key': dedupe_key,
            })
