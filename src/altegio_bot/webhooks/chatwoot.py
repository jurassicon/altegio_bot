"""Chatwoot webhook handler."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings

logger = logging.getLogger('chatwoot_webhook')

router = APIRouter()


def _safe_headers(request: Request) -> dict[str, str]:
    deny = {'authorization', 'cookie'}
    return {k: v for k, v in request.headers.items() if k.lower() not in deny}


def _verify_signature(body: bytes, signature: str | None) -> bool:
    """Verify HMAC signature from Chatwoot (optional)."""
    if not settings.chatwoot_webhook_secret:
        return True  # Signature verification disabled

    if not signature:
        return False  # Secret is set but signature is missing

    # Try to decode secret as base64 first (Chatwoot's default)
    try:
        secret_bytes = base64.b64decode(settings.chatwoot_webhook_secret)
    except binascii.Error:
        # Fallback to raw string if not base64
        secret_bytes = settings.chatwoot_webhook_secret.encode()

    expected = hmac.new(
        secret_bytes,
        body,
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(signature, expected)


@router.post('/webhook/chatwoot')
async def chatwoot_ingest(request: Request) -> JSONResponse:
    """Receive webhooks from Chatwoot on message_created events."""

    # Read body for signature verification
    body = await request.body()

    # === ENHANCED DEBUGGING ===
    signature = request.headers.get('x-chatwoot-signature')

    # Log all headers (to check exact casing)
    logger.debug(f"Webhook headers: {dict(request.headers)}")

    # Log raw body details
    logger.debug(f"Raw body length: {len(body)}")
    logger.debug(f"Body (first 200 chars): {body[:200]!r}")

    # Log received signature
    logger.debug(f"Received signature: {signature!r}")

    # Compute and log expected signatures for debugging
    if settings.chatwoot_webhook_secret and signature:
        # Method 1: Raw string
        expected_raw = hmac.new(
            settings.chatwoot_webhook_secret.encode(),
            body,
            hashlib.sha256
        ).hexdigest()
        logger.debug(f"Expected (raw secret): {expected_raw}")

        # Method 2: Base64-decoded secret
        try:
            decoded_secret = base64.b64decode(settings.chatwoot_webhook_secret)
            expected_b64 = hmac.new(
                decoded_secret,
                body,
                hashlib.sha256
            ).hexdigest()
            logger.debug(f"Expected (base64-decoded secret): {expected_b64}")

            # Check which one matches
            if hmac.compare_digest(signature, expected_raw):
                logger.info("Signature matches: RAW secret method")
            elif hmac.compare_digest(signature, expected_b64):
                logger.info("Signature matches: BASE64-decoded secret method")
            else:
                logger.warning("Signature DOES NOT match either method")
        except binascii.Error as e:
            logger.debug(f"Could not decode secret as base64: {e}")

    # === END DEBUGGING ===

    # Verify signature (if enabled)
    if not _verify_signature(body, signature):
        logger.warning('Invalid Chatwoot webhook signature')
        logger.error("Signature verification failed - check debug logs above for details")
        raise HTTPException(status_code=403, detail='Invalid signature')

    try:
        import json
        payload = json.loads(body)
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
        logger.info(
            'Skipping non-incoming message: message_type=%s', message_type
        )
        return JSONResponse(
            {'ok': True, 'skipped': f'message_type={message_type}'}
        )

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
                    chatwoot_conversation_id, chatwoot_message_id, phone_e164, text[:50]
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
