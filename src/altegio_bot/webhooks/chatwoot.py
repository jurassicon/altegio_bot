"""Chatwoot webhook handler.

Chatwoot sends webhooks when:
- A new message arrives from a customer (message_created, message_type: incoming)
- An agent replies (message_created, message_type: outgoing)

We only care about *incoming* messages so the inbox worker can process
START/STOP commands.  All other traffic is ignored (admins reply manually
in the Chatwoot UI).

The payload from Chatwoot looks like::

    {
        "event": "message_created",
        "message_type": "incoming",          # or "outgoing"
        "content": "STOP",
        "conversation": {
            "id": 42,
            "meta": {
                "sender": {
                    "phone_number": "+49123456789"
                }
            }
        },
        "inbox": {"id": 1}
    }
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings

logger = logging.getLogger('chatwoot_webhook')

router = APIRouter()


def _verify_signature(body: bytes, signature_header: str | None) -> bool:
    """Verify HMAC-SHA256 signature when chatwoot_webhook_secret is set."""
    secret = settings.chatwoot_webhook_secret
    if not secret:
        return True  # no secret configured – accept all

    if not signature_header:
        return False

    mac = hmac.new(secret.encode('utf-8'), body, hashlib.sha256)
    expected = mac.hexdigest()
    return hmac.compare_digest(signature_header.strip(), expected)


def _dedupe_key(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    return f'cw:{digest}'


def _to_meta_payload(
    phone_e164: str,
    text: str,
    conversation_id: int,
    phone_number_id: str | None,
) -> dict[str, Any]:
    """Wrap the Chatwoot message into the same Meta Cloud API shape that the
    inbox worker already knows how to parse.

    This lets the *existing* ``whatsapp_inbox_worker`` handle START/STOP
    commands without any changes to the command-parsing logic.
    """
    metadata: dict[str, Any] = {}
    if phone_number_id:
        metadata['phone_number_id'] = phone_number_id

    return {
        'object': 'whatsapp_business_account',
        'entry': [
            {
                'id': 'chatwoot',
                'changes': [
                    {
                        'field': 'messages',
                        'value': {
                            'metadata': metadata,
                            'messages': [
                                {
                                    'from': phone_e164.lstrip('+'),
                                    'id': f'cw:{conversation_id}',
                                    'timestamp': '0',
                                    'type': 'text',
                                    'text': {'body': text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
        '_chatwoot': {
            'conversation_id': conversation_id,
        },
    }


@router.post('/webhook/chatwoot')
async def chatwoot_ingest(request: Request) -> Response:
    body = await request.body()

    sig = request.headers.get('x-chatwoot-signature') or request.headers.get('X-Chatwoot-Signature')
    if not _verify_signature(body, sig):
        logger.warning('Chatwoot webhook signature mismatch')
        raise HTTPException(status_code=403, detail='Bad signature')

    try:
        raw_payload: dict[str, Any] = json.loads(body.decode('utf-8'))
    except Exception:
        raise HTTPException(status_code=400, detail='Invalid JSON')

    event_type = raw_payload.get('event')
    message_type = raw_payload.get('message_type')

    # Only process incoming customer messages
    if event_type != 'message_created' or message_type != 'incoming':
        return JSONResponse({'ok': True, 'skipped': True})

    content: str = str(raw_payload.get('content') or '')
    conversation: dict[str, Any] = raw_payload.get('conversation') or {}
    conversation_id: int | None = conversation.get('id')
    meta_block: dict[str, Any] = conversation.get('meta') or {}
    sender: dict[str, Any] = meta_block.get('sender') or {}
    phone_e164: str = str(sender.get('phone_number') or '').strip()

    if not phone_e164 or conversation_id is None:
        logger.info(
            'Chatwoot webhook missing phone or conversation_id – skipping'
        )
        return JSONResponse({'ok': True, 'skipped': True})

    # phone_number_id is not available from Chatwoot payloads.
    # The inbox worker will attempt to look up the sender by phone_number_id,
    # but when it is absent (None/empty) _pick_sender returns (None, None).
    # The worker still processes START/STOP opt-out changes via the customer
    # phone number; it only skips sending the ack reply when no sender is found.
    meta_payload = _to_meta_payload(
        phone_e164=phone_e164,
        text=content,
        conversation_id=conversation_id,
        phone_number_id=None,
    )

    dedupe_key = _dedupe_key(raw_payload)

    async with SessionLocal() as session:
        try:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key=dedupe_key,
                    status='received',
                    error=None,
                    query={},
                    headers=dict(request.headers),
                    payload=meta_payload,
                    chatwoot_conversation_id=conversation_id,
                )
                session.add(evt)
                await session.flush()

            return JSONResponse(
                {
                    'ok': True,
                    'duplicate': False,
                    'id': evt.id,
                    'dedupe_key': dedupe_key,
                }
            )

        except IntegrityError:
            await session.rollback()
            logger.info('Duplicate chatwoot event: %s', dedupe_key)
            return JSONResponse(
                {
                    'ok': True,
                    'duplicate': True,
                    'dedupe_key': dedupe_key,
                }
            )
