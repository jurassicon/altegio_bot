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
from altegio_bot.webhooks.common import (
    bounded_dedupe_key,
    mapping_or_empty,
    mask_query,
    optional_chatwoot_id,
    postgres_safe_json_value,
    safe_headers,
    safe_log_value,
)

logger = logging.getLogger("chatwoot_webhook")

router = APIRouter()

# Заголовок с HMAC-подписью Chatwoot. Он читается напрямую из живого запроса в
# chatwoot_ingest (не из сохранённой копии), поэтому его можно и нужно
# выбрасывать перед записью — в БД подпись не нужна.
_CHATWOOT_SIGNATURE_HEADER = "x-chatwoot-signature"

# Chatwoot sender types that represent human operators.
# 'agent_bot' means the message was sent by an automated bot — never relay.
# 'user' is used by API-type inboxes for agent messages.
_HUMAN_SENDER_TYPES = frozenset({"agent", "supervisor", "user"})

# content_type values that are purely internal — never relay to Meta.
_SKIP_CONTENT_TYPES = frozenset({"activity", "input_select", "input_email"})


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

    # Structural validation runs AFTER the HMAC check above, so a malformed body
    # still gets 403 (not 400) when the signature is wrong. `[]`/`"text"`/`123`/
    # `null` parse fine but every branch below treats payload as a mapping.
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON payload must be an object")

    event_type = payload.get("event")
    # event приходит из тела и не валидируется до этой строки — экранируем,
    # иначе значение вида "message_created\n2026-.. INFO forged" подделало бы
    # отдельную запись в application-логе.
    logger.info("chatwoot_webhook: event=%s", safe_log_value(event_type, limit=64))

    if event_type != "message_created":
        # Стабильный reason code: отражать sender-controlled значение обратно
        # клиенту нельзя — оно может быть некодируемым и уронить JSONResponse.
        return JSONResponse({"ok": True, "skipped": "unsupported_event"})

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
        # content_type is sender-controlled and used in a set membership test
        # below; an unhashable value (list/dict) would raise TypeError there.
        raw_content_type = payload.get("content_type", "text")
        content_type = raw_content_type if isinstance(raw_content_type, str) else ""
        sender = mapping_or_empty(payload.get("sender"))
        # Same reason as content_type: sender_type is used in a set membership
        # test, so an unhashable value must not reach it.
        raw_sender_type = sender.get("type", "")
        sender_type = raw_sender_type if isinstance(raw_sender_type, str) else ""

        # Skip private notes and internal activity events — always.
        if private:
            logger.debug(
                "chatwoot_webhook: skipping private note conv_id=%s",
                safe_log_value(mapping_or_empty(payload.get("conversation")).get("id"), limit=32),
            )
            return JSONResponse({"ok": True, "skipped": "private_note"})

        if content_type in _SKIP_CONTENT_TYPES:
            return JSONResponse({"ok": True, "skipped": "unsupported_content_type"})

        # Only relay if feature flag is on and sender is a human operator.
        if settings.chatwoot_operator_relay_enabled and sender_type in _HUMAN_SENDER_TYPES:
            return await _ingest_operator_outgoing(request, payload)

        conv_id = mapping_or_empty(payload.get("conversation")).get("id")
        msg_id = payload.get("id")
        logger.info(
            "chatwoot_webhook: skipping outgoing relay_enabled=%s"
            " message_type=%s sender_type=%s private=%s"
            " content_type=%s conv_id=%s msg_id=%s",
            settings.chatwoot_operator_relay_enabled,
            safe_log_value(message_type, limit=32),
            safe_log_value(sender_type, limit=32),
            bool(private),
            safe_log_value(content_type, limit=32),
            safe_log_value(conv_id, limit=32),
            safe_log_value(msg_id, limit=32),
        )
        # Covers both "relay flag is off" and "sender is not a human operator"
        # (e.g. agent_bot). Stable code — never the sender-controlled value.
        return JSONResponse({"ok": True, "skipped": "outgoing_not_relayed"})

    return JSONResponse({"ok": True, "skipped": "unsupported_message_type"})


async def _ingest_incoming(
    request: Request,
    payload: dict,
) -> JSONResponse:
    """Store an incoming customer message as a WhatsAppEvent.

    The whatsapp_inbox_worker will forward it to Chatwoot while loop
    prevention (_is_chatwoot_origin) ensures it is not re-sent to Meta.
    """
    conversation = mapping_or_empty(payload.get("conversation"))
    sender = mapping_or_empty(payload.get("sender"))

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
            "account_id": mapping_or_empty(payload.get("account")).get("id"),
        },
    }

    # Составляющие приходят из тела: они могут быть нечисловыми, содержать NUL
    # или быть длиннее VARCHAR(128). bounded_dedupe_key даёт побайтово тот же
    # ключ для корректных коротких id и безопасный хэш-хвост для остальных.
    dedupe_key = bounded_dedupe_key("chatwoot", chatwoot_conversation_id, chatwoot_message_id)

    return await _store_event(
        request=request,
        dedupe_key=dedupe_key,
        normalized_payload=normalized_payload,
        # BIGINT-колонки: невалидный id даёт NULL вместо падения INSERT.
        chatwoot_conversation_id=optional_chatwoot_id(chatwoot_conversation_id),
        chatwoot_message_id=optional_chatwoot_id(chatwoot_message_id),
        # Только технические идентификаторы: телефон и текст сообщения — PII и в
        # логи не попадают (сами данные сохраняются в БД, где доступ ограничен).
        log_ctx={
            "conv_id": chatwoot_conversation_id,
            "msg_id": chatwoot_message_id,
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
    conversation = mapping_or_empty(payload.get("conversation"))
    sender = mapping_or_empty(payload.get("sender"))
    conv_meta = mapping_or_empty(conversation.get("meta"))
    contact = mapping_or_empty(conv_meta.get("sender"))

    chatwoot_message_id = payload.get("id")
    chatwoot_conversation_id = conversation.get("id")
    chatwoot_inbox_id = conversation.get("inbox_id")
    text = payload.get("content", "")
    content_attributes = mapping_or_empty(payload.get("content_attributes"))
    reply_to_chatwoot_message_id = optional_chatwoot_id(content_attributes.get("in_reply_to"))

    # Recipient phone is the contact (customer) in the conversation.
    recipient_phone = contact.get("phone_number")

    if not recipient_phone:
        logger.warning(
            "chatwoot_webhook: operator_outgoing missing recipient phone conv_id=%s msg_id=%s",
            safe_log_value(chatwoot_conversation_id, limit=32),
            safe_log_value(chatwoot_message_id, limit=32),
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
            "contact_name": contact.get("name"),
            "content_attributes": content_attributes,
            "reply_to_chatwoot_message_id": reply_to_chatwoot_message_id,
        },
    }

    dedupe_key = bounded_dedupe_key("chatwoot_out", chatwoot_conversation_id, chatwoot_message_id)

    # Ни телефона получателя, ни имени агента — это PII.
    logger.info(
        "chatwoot_webhook: operator_outgoing accepted conv_id=%s msg_id=%s",
        safe_log_value(chatwoot_conversation_id, limit=32),
        safe_log_value(chatwoot_message_id, limit=32),
    )

    return await _store_event(
        request=request,
        dedupe_key=dedupe_key,
        normalized_payload=normalized_payload,
        chatwoot_conversation_id=optional_chatwoot_id(chatwoot_conversation_id),
        chatwoot_message_id=optional_chatwoot_id(chatwoot_message_id),
        log_ctx={
            "conv_id": chatwoot_conversation_id,
            "msg_id": chatwoot_message_id,
        },
    )


async def _store_event(
    *,
    request: Request,
    dedupe_key: str,
    normalized_payload: dict,
    chatwoot_conversation_id: int | None,
    chatwoot_message_id: int | None = None,
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
                    # mask_query/safe_headers — общие хелперы: маскируют
                    # чувствительные query-значения, выбрасывают authorization,
                    # cookie и подпись Chatwoot, и приводят метаданные к
                    # Postgres-безопасному виду (NUL/суррогаты). Санитайзится
                    # ТОЛЬКО сохраняемая копия — проверка HMAC выше уже прошла по
                    # живому заголовку.
                    query=mask_query(dict(request.query_params)),
                    headers=safe_headers(request, extra_deny={_CHATWOOT_SIGNATURE_HEADER}),
                    # Сохраняемая проекция. Содержимое сообщения, имена агента и
                    # контакта контролируются отправителем: NUL или непарный
                    # суррогат внутри них прошёл бы json.loads и проверку HMAC,
                    # но уронил бы INSERT в JSONB. Подпись уже проверена по
                    # исходным байтам body, так что менять проекцию безопасно.
                    payload=postgres_safe_json_value(normalized_payload),
                    chatwoot_conversation_id=chatwoot_conversation_id,
                    chatwoot_message_id=chatwoot_message_id,
                )
                session.add(event)
                await session.flush()

                logger.info(
                    "chatwoot_webhook: saved dedupe_key=%s ctx=%s",
                    safe_log_value(dedupe_key, limit=128),
                    safe_log_value(log_ctx, limit=128),
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
            logger.info("chatwoot_webhook: duplicate dedupe_key=%s", safe_log_value(dedupe_key, limit=128))
            return JSONResponse(
                {
                    "ok": True,
                    "duplicate": True,
                    "dedupe_key": dedupe_key,
                }
            )
