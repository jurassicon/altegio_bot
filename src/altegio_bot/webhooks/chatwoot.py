"""Chatwoot webhook handler."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.settings import settings

logger = logging.getLogger("chatwoot_webhook")

router = APIRouter()

def _safe_headers(request: Request) -> dict[str, str]:
    deny = {"authorization", "cookie"}
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

    expected = hmac.new(secret_bytes, body, hashlib.sha256).hexdigest()

    return hmac.compare_digest(signature, expected)

@router.post("/webhook/chatwoot")
async def chatwoot_ingest(request: Request) -> JSONResponse:
    """Receive webhooks from Chatwoot on message_created events."""

    # Read body for signature verification
    body = await request.body()

    signature = request.headers.get("x-chatwoot-signature")

    # Verify signature (if enabled)
    if not _verify_signature(body, signature):
        logger.warning("Invalid Chatwoot webhook signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Log for debugging
    logger.info("Chatwoot webhook received: event=%s", payload.get("event"))

    event_type = payload.get("event")
    if event_type != "message_created":
        return JSONResponse({"ok": True, "skipped": f"event={event_type}"})

    message_type = payload.get("message_type")

    # Handle outgoing messages with source_id as delivery status events
    if message_type in (1, "outgoing"):
        source_id = payload.get("source_id")
        if not source_id:
            # System message without a WhatsApp message ID — skip
            logger.debug(
                "Skipping outgoing message without source_id: message_id=%s",
                payload.get("id"),
            )
            return JSONResponse({"ok": True, "skipped": "outgoing_no_source_id"})

        chatwoot_message_id = payload.get("id")
        conversation = payload.get("conversation", {})
        chatwoot_conversation_id = conversation.get("id")
        account_id = payload.get("account", {}).get("id")

        cw_status = payload.get("status")
        if not cw_status:
            logger.debug(
                "Outgoing message has no status field, defaulting to 'sent': message_id=%s",
                chatwoot_message_id,
            )
            cw_status = "sent"

        # Recipient phone: Chatwoot puts the customer in conversation.meta.sender for outgoing messages
        conv_sender = conversation.get("meta", {}).get("sender", {})
        phone_e164 = conv_sender.get("phone_number") or payload.get("sender", {}).get("phone_number", "")
        phone_digits = phone_e164.lstrip("+") if phone_e164 else ""

        created_at_raw = payload.get("created_at")
        try:
            if isinstance(created_at_raw, str):
                timestamp_sec = int(datetime.fromisoformat(created_at_raw.replace("Z", "+00:00")).timestamp())
            else:
                timestamp_sec = int(created_at_raw or datetime.now(timezone.utc).timestamp())
        except (ValueError, TypeError):
            timestamp_sec = int(datetime.now(timezone.utc).timestamp())

        # Normalize to Meta webhook format for delivery statuses
        # (compatible with existing SQL queries: payload #>> '{entry,0,changes,0,value,statuses,0,id}')
        delivery_payload = {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "statuses": [
                                    {
                                        "id": source_id,
                                        "status": cw_status,
                                        "timestamp": str(timestamp_sec),
                                        "recipient_id": phone_digits,
                                        "errors": [],
                                    }
                                ],
                                "metadata": {"phone_number_id": settings.meta_wa_phone_number_id},
                            }
                        }
                    ]
                }
            ],
            "_chatwoot": {
                "conversation_id": chatwoot_conversation_id,
                "message_id": chatwoot_message_id,
                "account_id": account_id,
            },
        }

        dedupe_key = f"chatwoot_delivery:{chatwoot_message_id}"

        async with SessionLocal() as session:
            try:
                async with session.begin():
                    event = WhatsAppEvent(
                        dedupe_key=dedupe_key,
                        status="received",
                        error=None,
                        query=dict(request.query_params),
                        headers=_safe_headers(request),
                        payload=delivery_payload,
                    )
                    session.add(event)
                    await session.flush()

                    logger.info(
                        "Chatwoot delivery status saved: conv_id=%s msg_id=%s source_id=%s status=%s",
                        chatwoot_conversation_id,
                        chatwoot_message_id,
                        source_id,
                        cw_status,
                    )

                    return JSONResponse(
                        {
                            "ok": True,
                            "duplicate": False,
                            "id": event.id,
                            "dedupe_key": dedupe_key,
                        }
                    )

            except IntegrityError:
                await session.rollback()
                logger.info("Duplicate chatwoot delivery event: %s", dedupe_key)
                return JSONResponse(
                    {
                        "ok": True,
                        "duplicate": True,
                        "dedupe_key": dedupe_key,
                    }
                )

    # Only process incoming messages (from customers, not agents)
    if message_type not in (0, "incoming"):
        logger.info("Skipping non-incoming message: message_type=%s", message_type)
        return JSONResponse({"ok": True, "skipped": f"message_type={message_type}"})

    conversation = payload.get("conversation", {})
    sender = payload.get("sender", {})

    phone_e164 = sender.get("phone_number")
    text = payload.get("content", "")
    chatwoot_message_id = payload.get("id")
    chatwoot_conversation_id = conversation.get("id")

    if not phone_e164:
        logger.warning("Missing phone_number in webhook payload")
        raise HTTPException(status_code=400, detail="Missing phone_number")

    if not chatwoot_message_id:
        logger.warning("Missing message id in webhook payload")
        raise HTTPException(status_code=400, detail="Missing message_id")

    # Parse created_at timestamp
    created_at_raw = payload.get("created_at")
    try:
        if isinstance(created_at_raw, str):
            timestamp_sec = int(datetime.fromisoformat(created_at_raw.replace("Z", "+00:00")).timestamp())
        else:
            timestamp_sec = int(created_at_raw or datetime.now(timezone.utc).timestamp())
    except (ValueError, TypeError):
        timestamp_sec = int(datetime.now(timezone.utc).timestamp())

    # Normalize to Meta webhook format (compatible with existing worker)
    normalized_payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "messages": [
                                {
                                    "from": phone_e164,
                                    "type": "text",
                                    "text": {"body": text},
                                    "id": str(chatwoot_message_id),
                                    "timestamp": str(timestamp_sec),
                                }
                            ],
                            "metadata": {"phone_number_id": settings.meta_wa_phone_number_id},
                        }
                    }
                ]
            }
        ],
        "_chatwoot": {
            "conversation_id": chatwoot_conversation_id,
            "message_id": chatwoot_message_id,
            "account_id": payload.get("account", {}).get("id"),
        },
    }

    dedupe_key = f"chatwoot:{chatwoot_conversation_id}:{chatwoot_message_id}"

    async with SessionLocal() as session:
        try:
            async with session.begin():
                event = WhatsAppEvent(
                    dedupe_key=dedupe_key,
                    status="received",
                    error=None,
                    query=dict(request.query_params),
                    headers=_safe_headers(request),
                    payload=normalized_payload,
                )
                session.add(event)
                await session.flush()

                logger.info(
                    'Chatwoot webhook saved: conv_id=%s msg_id=%s phone=%s text="%s"',
                    chatwoot_conversation_id,
                    chatwoot_message_id,
                    phone_e164,
                    text[:50],
                )

                return JSONResponse(
                    {
                        "ok": True,
                        "duplicate": False,
                        "id": event.id,
                        "dedupe_key": dedupe_key,
                    }
                )

        except IntegrityError:
            await session.rollback()
            logger.info("Duplicate chatwoot event: %s", dedupe_key)
            return JSONResponse(
                {
                    "ok": True,
                    "duplicate": True,
                    "dedupe_key": dedupe_key,
                }
            )