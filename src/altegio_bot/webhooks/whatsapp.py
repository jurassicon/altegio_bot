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
from altegio_bot.models.models import (
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    WhatsAppEvent,
)
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow
from altegio_bot.webhooks.common import (
    mask_query,
    postgres_safe_json_value,
    safe_headers,
    safe_log_value,
)
from altegio_bot.whatsapp_consent import record_inbound_receipts
from altegio_bot.whatsapp_inbound_commands import (
    allowed_sender_routes,
    payload_sender_routes,
    route_allowed,
)

logger = logging.getLogger("whatsapp_webhook")

router = APIRouter()

# Заголовок с HMAC-подписью Meta. Проверяется по живому запросу до записи,
# поэтому из сохраняемой копии заголовков его нужно выбросить.
_META_SIGNATURE_HEADERS = frozenset({"x-hub-signature-256", "x-hub-signature"})


def _payload_dedupe_key(payload: dict[str, Any]) -> str:
    # ensure_ascii=True is intentional and load-bearing (it is also the json
    # default, but stated explicitly so it cannot be "cleaned up" away): it
    # escapes lone surrogates into ASCII, so .encode("utf-8") below cannot raise
    # UnicodeEncodeError on hostile webhook content. Do NOT switch this to
    # canonical_json_hash — that helper uses ensure_ascii=False and a different
    # historical canonical form, so it would change every existing dedupe key.
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"wa:{digest}"


def _route_decision(payload: dict[str, Any], allowed: set[str]) -> tuple[bool, str | None]:
    """``(ignored, first_refused_route)`` for one delivery.

    The rule per change is the historical one, unchanged. What changed is how a
    payload of several changes is judged: it is ignored only when NONE of its
    routes is one this deployment accepts.

    Both halves of that were defects. A delivery whose first change named a
    refused route was ignored whole, and a STOP from an allowed sender further
    down it was never acted on — a lost refusal. The mirror case was worse: a
    delivery whose first change named an allowed route was accepted whole, and a
    START from a refused sender behind it got a free attempt at lifting
    somebody's refusal. Each message now carries its own verdict on its receipt,
    and the inbox worker reads that rather than re-deciding.
    """
    routes = payload_sender_routes(payload)
    if not routes:
        return False, None
    refused = [route for route in routes if not route_allowed(route, allowed)]
    if len(refused) < len(routes):
        return False, None
    return True, next((route for route in refused if route is not None), None)


def _parse_allowed_phone_number_ids() -> set[str]:
    """Historical name, one definition. See ``allowed_sender_routes``."""
    return allowed_sender_routes()


def _verify_signature(
    *,
    body: bytes,
    signature_header: str | None,
    app_secret: str,
) -> bool:
    if not signature_header:
        return False

    if not signature_header.startswith("sha256="):
        return False

    received = signature_header.split("=", 1)[1].strip()
    mac = hmac.new(app_secret.encode("utf-8"), body, hashlib.sha256)
    expected = mac.hexdigest()

    return hmac.compare_digest(received, expected)


@router.get("/webhook/whatsapp")
async def whatsapp_verify(request: Request) -> Response:
    qp = request.query_params
    mode = qp.get("hub.mode")
    token = qp.get("hub.verify_token")
    challenge = qp.get("hub.challenge")

    if mode != "subscribe" or not challenge:
        raise HTTPException(status_code=400, detail="Invalid verify request")

    if token != settings.whatsapp_webhook_verify_token:
        raise HTTPException(status_code=403, detail="Verify token mismatch")

    return Response(content=challenge, media_type="text/plain")


@router.post("/webhook/whatsapp")
async def whatsapp_ingest(request: Request) -> Response:
    body = await request.body()

    try:
        payload: dict[str, Any] = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    app_secret = getattr(settings, "meta_app_secret", "")

    # Whether Meta's own HMAC was CHECKED for this very request, over these very
    # bytes. It is recorded with the inbound receipts below and never derived
    # again: the stored copy of the headers has the signature deliberately
    # stripped out of it, so after this point the proof no longer exists
    # anywhere and no later process may reconstruct one.
    #
    # An unconfigured ``meta_app_secret`` therefore means ``unverified`` rather
    # than "fine" — §46.4.7: a check that is not configured is not a check, and
    # a refusal arriving unverified still refuses while a START arriving
    # unverified grants nothing.
    signature_verified = False

    if app_secret:
        sig = request.headers.get("x-hub-signature-256") or request.headers.get("X-Hub-Signature-256")
        if not _verify_signature(
            body=body,
            signature_header=sig,
            app_secret=app_secret,
        ):
            logger.warning("Webhook signature mismatch")
            raise HTTPException(status_code=403, detail="Bad signature")
        signature_verified = True

    # Structural validation deliberately runs AFTER the signature check, so an
    # unsigned malformed body is still a 403 rather than a 400.
    #
    # A non-object root (`[]`, `"text"`, `123`, `null`) parses fine but no worker
    # can process it. Persisting it and answering 200 would be the worst outcome:
    # Meta treats the delivery as accepted and never retries, while the row later
    # ends up failed/processed without anything actually happening — the event is
    # lost for good. Reject it instead so Meta can retry.
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON payload must be an object")

    allowed = _parse_allowed_phone_number_ids()
    ignored, refused_route = _route_decision(payload, allowed)

    status = "received"
    error: str | None = None

    if ignored:
        status = "ignored"
        # pni приходит из payload: и в TEXT-колонку error, и в лог он попадает
        # только приведённым к безопасному виду.
        shown = safe_log_value(refused_route, limit=64) if refused_route is not None else "unknown"
        error = f"Ignored: phone_number_id not allowed ({shown})"
        logger.info("Ignored webhook event phone_number_id=%s", shown)

    # Дедуп-ключ считается по ОРИГИНАЛЬНОМУ payload — санитизация ниже касается
    # только сохраняемой проекции, поэтому семантика дедупликации не меняется.
    dedupe_key = _payload_dedupe_key(payload)

    # Подпись уже проверена выше по исходным байтам body; теперь можно готовить
    # безопасную копию metadata: маскируем секреты в query, выбрасываем
    # authorization/cookie и подпись Meta, приводим строки к Postgres-безопасному
    # виду (NUL/суррогаты иначе роняют commit).
    async with SessionLocal() as session:
        try:
            async with session.begin():
                evt = WhatsAppEvent(
                    dedupe_key=dedupe_key,
                    status=status,
                    error=error,
                    query=mask_query(dict(request.query_params)),
                    headers=safe_headers(request, extra_deny=_META_SIGNATURE_HEADERS),
                    payload=postgres_safe_json_value(payload),
                    inbound_authenticity=(
                        INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED
                    ),
                )
                session.add(evt)
                await session.flush()

                # §46.4.6 — the durable boundary of inbound receipt.
                #
                # Written in THIS transaction, so it commits together with the
                # event and before the 200 below. Once Meta has been told the
                # delivery was accepted, the fact that these addresses said
                # something is on disk: a stopped, slow or crashed inbox worker
                # then leaves a HOLD for those people rather than a hole, and a
                # STOP cannot be lost in the gap between accepting a webhook and
                # processing it.
                #
                # It records, never decides. Consent itself is applied by the
                # worker, through one code path, so there is no second mechanism
                # here that could answer differently. The allow list is passed
                # rather than a single verdict: each message is judged against
                # its OWN sender route and a refused one is resolved on the spot,
                # because nothing is ever going to act on it and a permanent hold
                # would be a bug, not caution — while an allowed message in the
                # same delivery keeps its hold.
                await record_inbound_receipts(
                    session,
                    dedupe_key=dedupe_key,
                    event_id=int(evt.id),
                    payload=payload,
                    signature_verified=signature_verified,
                    observed_at=utcnow(),
                    allowed_phone_number_ids=allowed,
                )

            # phone_number_id сознательно НЕ возвращается: значение приходит из
            # тела и может быть некодируемым (непарный суррогат) — тогда
            # JSONResponse упал бы уже ПОСЛЕ успешного commit, отправитель
            # получил бы 500, а ретрай попадал бы в ту же ошибку на duplicate-
            # ветке. Технического ответа ниже достаточно; сам pni сохранён в
            # payload и в error.
            return JSONResponse(
                {
                    "ok": True,
                    "duplicate": False,
                    "ignored": ignored,
                    "id": evt.id,
                    "dedupe_key": dedupe_key,
                }
            )

        except IntegrityError:
            await session.rollback()
            logger.info("Duplicate whatsapp event: %s", dedupe_key)

            res = await session.execute(select(WhatsAppEvent.id).where(WhatsAppEvent.dedupe_key == dedupe_key))
            existing_id = res.scalar_one_or_none()

            return JSONResponse(
                {
                    "ok": True,
                    "duplicate": True,
                    "ignored": ignored,
                    "id": existing_id,
                    "dedupe_key": dedupe_key,
                }
            )
