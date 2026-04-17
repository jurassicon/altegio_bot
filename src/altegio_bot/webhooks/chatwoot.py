"""Chatwoot webhook handler.

Handles two event paths:

1. Incoming customer messages (message_type=0 / "incoming"):
   Normalized to Meta-like payload and stored as WhatsAppEvent for the
   whatsapp_inbox_worker to forward to Chatwoot (loop prevention is handled
   in the worker via _is_chatwoot_origin).

2. Outgoing operator messages (message_type=1 / "outgoing"), when
   chatwoot_operator_relay_enabled=True:
   Human-operator replies are stored with the _chatwoot_operator_relay marker
   so the whatsapp_inbox_worker can relay them through Meta API and create an
   OutboxMessage for canonical lifecycle tracking.
"""

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

# Chatwoot sender types that represent human operators.
# 'agent_bot' means the message was sent by an automated bot — never relay.
# 'user' is used by API-type inboxes for agent messages.
_HUMAN_SENDER_TYPES = frozenset({"agent", "supervisor", "user"})

# content_type values that are purely internal — never relay to Meta.
_SKIP_CONTENT_TYPES = frozenset({"activity", "input_select", "input_email"})


def _safe_headers(request: Request) -> dict[str, str]:
    deny = {"authorization", "cookie"}
    return {k: v for k, v in request.headers.items() if k.lower() not in deny}


def _verify_signature(body: bytes, signature: str | None) -> bool:
    """Verify HMAC signature from Chatwoot (optional).

    Chatwoot signs the body with SHA-256.  The secret may be stored as
    plain text or as a base64-encoded string — we try both.
    """
    if not settings.chatwoot_webhook_secret:
        return True

    if not signature:
        return False

    secret_raw = settings.chatwoot_webhook_secret.encode()
    expected_raw = hmac.new(secret_raw, body, hashlib.sha256).hexdigest()
    if hmac.compare_digest(signature, expected_raw):
        return True

    try:
        secret_b64 = base64.b64decode(settings.chatwoot_webhook_secret)
        expected_b64 = hmac.new(secret_b64, body, hashlib.sha256).hexdigest()
        if hmac.compare_digest(signature, expected_b64):
            return True
    except binascii.Error:
        pass

    return False


def _parse_timestamp(raw: object) -> int:
    """Return Unix timestamp (seconds) from a Chatwoot created_at value."""
    try:
        if isinstance(raw, str):
            return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())
        return int(raw or datetime.now(timezone.utc).timestamp())
    except (ValueError, TypeError):
        return int(datetime.now(timezone.utc).timestamp())


@router.post("/webhook/chatwoot")
async def chatwoot_ingest(request: Request) -> JSONResponse:
    """Receive webhooks from Chatwoot."""
    body = await request.body()
    signature = request.headers.get("x-chatwoot-signature")

    if not _verify_signature(body, signature):
        logger.warning("chatwoot_webhook: invalid signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = payload.get("event")
    logger.info("chatwoot_webhook: event=%s", event_type)

    if event_type != "message_created":
        return JSONResponse({"ok": True, "skipped": f"event={event_type}"})

    message_type = payload.get("message_type")

    # ------------------------------------------------------------------ #
    # Path 1: incoming customer message                                    #
    # ------------------------------------------------------------------ #
    if message_type in (0, "incoming"):
        return await _ingest_incoming(request, payload)

    # ------------------------------------------------------------------ #
    # Path 2: outgoing operator message (Meta-first relay)                 #
    # ------------------------------------------------------------------ #
    if message_type in (1, "outgoing"):
        private = payload.get("private", False)
        content_type = payload.get("content_type", "text")
        sender = payload.get("sender") or {}
        sender_type = sender.get("type", "")

        # Skip private notes and internal activity events — always.
        if private:
            logger.debug(
                "chatwoot_webhook: skipping private note conv_id=%s",
                (payload.get("conversation") or {}).get("id"),
            )
            return JSONResponse({"ok": True, "skipped": "private_note"})

        if content_type in _SKIP_CONTENT_TYPES:
            return JSONResponse({"ok": True, "skipped": f"content_type={content_type}"})

        # Only relay if feature flag is on and sender is a human operator.
        if settings.chatwoot_operator_relay_enabled and sender_type in _HUMAN_SENDER_TYPES:
            return await _ingest_operator_outgoing(request, payload)

        conv_id = (payload.get("conversation") or {}).get("id")
        msg_id = payload.get("id")
        logger.info(
            "chatwoot_webhook: skipping outgoing relay_enabled=%s"
            " message_type=%s sender_type=%s private=%s"
            " content_type=%s conv_id=%s msg_id=%s",
            settings.chatwoot_operator_relay_enabled,
            message_type,
            sender_type,
            private,
            content_type,
            conv_id,
            msg_id,
        )
        return JSONResponse({"ok": True, "skipped": f"message_type={message_type}"})

    return JSONResponse({"ok": True, "skipped": f"message_type={message_type}"})


async def _ingest_incoming(
    request: Request,
    payload: dict,
) -> JSONResponse:
    """Store an incoming customer message as a WhatsAppEvent.

    The whatsapp_inbox_worker will forward it to Chatwoot while loop
    prevention (_is_chatwoot_origin) ensures it is not re-sent to Meta.
    """
    conversation = payload.get("conversation") or {}
    sender = payload.get("sender") or {}

    phone_e164 = sender.get("phone_number")
    text = payload.get("content", "")
    chatwoot_message_id = payload.get("id")
    chatwoot_conversation_id = conversation.get("id")
    timestamp_sec = _parse_timestamp(payload.get("created_at"))

    if not phone_e164:
        logger.warning("chatwoot_webhook: missing phone_number")
        raise HTTPException(status_code=400, detail="Missing phone_number")

    if not chatwoot_message_id:
        logger.warning("chatwoot_webhook: missing message id")
        raise HTTPException(status_code=400, detail="Missing message_id")

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
                            "metadata": {
                                "phone_number_id": (settings.meta_wa_phone_number_id),
                            },
                        }
                    }
                ]
            }
        ],
        "_chatwoot": {
            "conversation_id": chatwoot_conversation_id,
            "message_id": chatwoot_message_id,
            "account_id": (payload.get("account") or {}).get("id"),
        },
    }

    dedupe_key = f"chatwoot:{chatwoot_conversation_id}:{chatwoot_message_id}"

    return await _store_event(
        request=request,
        dedupe_key=dedupe_key,
        normalized_payload=normalized_payload,
        chatwoot_conversation_id=chatwoot_conversation_id,
        log_ctx={
            "conv_id": chatwoot_conversation_id,
            "msg_id": chatwoot_message_id,
            "phone": phone_e164,
            "text": text[:50],
        },
    )


async def _ingest_operator_outgoing(
    request: Request,
    payload: dict,
) -> JSONResponse:
    """Store an operator-originated outgoing message for Meta relay.

    The whatsapp_inbox_worker will pick this up, send it through Meta API,
    and create an OutboxMessage for canonical delivery lifecycle tracking.
    """
    conversation = payload.get("conversation") or {}
    sender = payload.get("sender") or {}
    conv_meta = conversation.get("meta") or {}
    contact = conv_meta.get("sender") or {}

    chatwoot_message_id = payload.get("id")
    chatwoot_conversation_id = conversation.get("id")
    chatwoot_inbox_id = conversation.get("inbox_id")
    text = payload.get("content", "")

    # Recipient phone is the contact (customer) in the conversation.
    recipient_phone = contact.get("phone_number")

    if not recipient_phone:
        logger.warning(
            "chatwoot_webhook: operator_outgoing missing recipient phone conv_id=%s msg_id=%s",
            chatwoot_conversation_id,
            chatwoot_message_id,
        )
        # Accept but skip — we cannot route without a phone number.
        return JSONResponse({"ok": True, "skipped": "no_recipient_phone"})

    if not text:
        return JSONResponse({"ok": True, "skipped": "empty_content"})

    normalized_payload = {
        "_chatwoot_operator_relay": {
            "recipient_phone": recipient_phone,
            "text": text,
            "conversation_id": chatwoot_conversation_id,
            "message_id": chatwoot_message_id,
            "phone_number_id": settings.meta_wa_phone_number_id,
            "chatwoot_inbox_id": chatwoot_inbox_id,
            "agent_name": sender.get("name", ""),
            "agent_id": sender.get("id"),
        },
    }

    dedupe_key = f"chatwoot_out:{chatwoot_conversation_id}:{chatwoot_message_id}"

    logger.info(
        "chatwoot_webhook: operator_outgoing accepted conv_id=%s msg_id=%s phone=%s agent=%s",
        chatwoot_conversation_id,
        chatwoot_message_id,
        recipient_phone,
        sender.get("name", ""),
    )

    return await _store_event(
        request=request,
        dedupe_key=dedupe_key,
        normalized_payload=normalized_payload,
        chatwoot_conversation_id=chatwoot_conversation_id,
        log_ctx={
            "conv_id": chatwoot_conversation_id,
            "msg_id": chatwoot_message_id,
            "phone": recipient_phone,
        },
    )


async def _store_event(
    *,
    request: Request,
    dedupe_key: str,
    normalized_payload: dict,
    chatwoot_conversation_id: int | None,
    log_ctx: dict,
) -> JSONResponse:
    """Persist a WhatsAppEvent row, handling duplicate dedupe_key gracefully."""
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
                    chatwoot_conversation_id=chatwoot_conversation_id,
                )
                session.add(event)
                await session.flush()

                logger.info(
                    "chatwoot_webhook: saved dedupe_key=%s ctx=%s",
                    dedupe_key,
                    log_ctx,
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
            logger.info("chatwoot_webhook: duplicate dedupe_key=%s", dedupe_key)
            return JSONResponse(
                {
                    "ok": True,
                    "duplicate": True,
                    "dedupe_key": dedupe_key,
                }
            )
