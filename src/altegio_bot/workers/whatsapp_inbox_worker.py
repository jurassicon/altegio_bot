from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.runner import recompute_campaign_run_stats
from altegio_bot.chatwoot_client import ChatwootClient
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import (
    CampaignRecipient,
    Client,
    MessageJob,
    OutboxMessage,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.perf import perf_log
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send
from altegio_bot.settings import settings

logger = logging.getLogger("whatsapp_inbox_worker")

MARKETING_JOB_TYPES = (
    "review_3d",
    "repeat_10d",
    "comeback_3d",
)

STOP_KEYWORDS = {
    "stop",
    "unsubscribe",
    "unsub",
    "abmelden",
    "отписка",
    "отпиши",
    "odjava",
}

START_KEYWORDS = {
    "start",
    "subscribe",
    "anmelden",
    "подписка",
    "подпиши",
    "prijava",
}

# Rank used to ensure OutboxMessage.status never regresses.
# A new status is applied only when its rank exceeds the current rank.
# 'failed' has rank 0 so it only applies when outbox is still in
# queued/sending state — never downgrades a delivered/read message.
_WA_STATUS_RANK: dict[str, int] = {
    "failed": 0,
    "queued": 1,
    "sending": 2,
    "sent": 3,
    "delivered": 4,
    "read": 5,
}

# Meta status values we will apply to OutboxMessage.
_WA_HANDLED_STATUSES = frozenset({"sent", "delivered", "read", "failed"})


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _norm_phone(raw: str | None) -> str | None:
    if not raw:
        return None

    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None

    return f"+{digits}"


def _norm_text(raw: str | None) -> str:
    if not raw:
        return ""

    text = raw.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" \t\n\r.,!?:;\"'()[]{}")
    return text


def _extract_message_text(msg: dict[str, Any]) -> str:
    msg_type = msg.get("type")

    if msg_type == "text":
        text = msg.get("text") or {}
        return str(text.get("body") or "")

    if msg_type == "button":
        btn = msg.get("button") or {}
        return str(btn.get("text") or btn.get("payload") or "")

    if msg_type == "interactive":
        inter = msg.get("interactive") or {}
        btn_reply = inter.get("button_reply") or {}
        list_reply = inter.get("list_reply") or {}
        return str(
            btn_reply.get("title") or btn_reply.get("id") or list_reply.get("title") or list_reply.get("id") or ""
        )

    return ""


def _parse_command(text: str) -> str | None:
    norm = _norm_text(text)
    if not norm:
        return None

    first = norm.split(" ", 1)[0]

    if first in STOP_KEYWORDS:
        return "stop"

    if first in START_KEYWORDS:
        return "start"

    return None


def _is_operator_relay(payload: dict[str, Any]) -> bool:
    """Return True if this event is an operator relay from Chatwoot.

    Operator relay events carry _chatwoot_operator_relay in the payload,
    written by the Chatwoot webhook handler.  They must be sent to Meta,
    NOT forwarded back to Chatwoot (that would duplicate the message the
    operator already sees in their own Chatwoot UI).
    """
    return "_chatwoot_operator_relay" in payload


def _is_chatwoot_origin(event: WhatsAppEvent, payload: dict[str, Any]) -> bool:
    """Return True if this event originated from Chatwoot (not from Meta directly).

    Prevents an infinite loop:
    Chatwoot webhook -> WhatsAppEvent -> worker -> log_incoming_message -> Chatwoot webhook -> ...
    """
    if "_chatwoot" in payload:
        return True
    if isinstance(event.dedupe_key, str) and event.dedupe_key.startswith("chatwoot:"):
        return True
    if event.chatwoot_conversation_id is not None:
        return True
    return False


async def _pick_sender(
    session: AsyncSession,
    phone_number_id: str | None,
) -> tuple[int | None, int | None]:
    if not phone_number_id:
        return None, None

    stmt = (
        select(WhatsAppSender)
        .where(WhatsAppSender.phone_number_id == phone_number_id)
        .where(WhatsAppSender.is_active.is_(True))
        .limit(1)
    )
    res = await session.execute(stmt)
    sender = res.scalar_one_or_none()
    if sender is None:
        return None, None

    return int(sender.id), int(sender.company_id)


async def _resolve_relay_sender(
    session: AsyncSession,
    phone_number_id: str | None,
) -> tuple[int | None, int | None, str | None]:
    """Strict, fail-closed sender resolution for operator relay.

    Returns (sender_id, company_id, error).
    error is None on success; non-None means the relay must be blocked.

    Unlike _pick_sender(), this never silently picks an arbitrary match:
    - 0 active senders → error
    - 1 active sender  → ok
    - >1 active senders → error (ambiguous routing — different company_ids
      share the same phone_number_id; picking one would be wrong)
    """
    if not phone_number_id:
        return None, None, "operator_relay: missing phone_number_id"

    stmt = (
        select(WhatsAppSender)
        .where(WhatsAppSender.phone_number_id == phone_number_id)
        .where(WhatsAppSender.is_active.is_(True))
    )
    res = await session.execute(stmt)
    senders = list(res.scalars().all())

    if not senders:
        return (
            None,
            None,
            f"operator_relay: no active sender for phone_number_id={phone_number_id}",
        )

    if len(senders) > 1:
        company_ids = [str(s.company_id) for s in senders]
        logger.warning(
            "operator_relay: ambiguous sender routing "
            "phone_number_id=%s matched %d senders "
            "company_ids=%s — blocking send",
            phone_number_id,
            len(senders),
            ",".join(company_ids),
        )
        return (
            None,
            None,
            f"operator_relay: ambiguous sender routing for "
            f"phone_number_id={phone_number_id} "
            f"(matched {len(senders)} senders across "
            f"company_ids={','.join(company_ids)})",
        )

    s = senders[0]
    return int(s.id), int(s.company_id), None


def _phone_variants(phone_e164: str) -> list[str]:
    digits = re.sub(r"\D+", "", phone_e164)
    variants = {
        phone_e164,
        digits,
        f"+{digits}",
    }
    return [v for v in variants if v]


async def _set_opt_out(
    session: AsyncSession,
    *,
    phone_e164: str,
    opted_out: bool,
    reason: str,
) -> int:
    variants = _phone_variants(phone_e164)

    if opted_out:
        values: dict[str, Any] = {
            "wa_opted_out": True,
            "wa_opted_out_at": utcnow(),
            "wa_opt_out_reason": reason,
        }
    else:
        values = {
            "wa_opted_out": False,
            "wa_opted_out_at": None,
            "wa_opt_out_reason": None,
        }

    stmt = update(Client).where(Client.phone_e164.in_(variants)).values(**values)

    res = await session.execute(stmt)
    return int(getattr(res, "rowcount", 0) or 0)


async def _cancel_marketing_jobs(
    session: AsyncSession,
    *,
    phone_e164: str,
) -> int:
    variants = _phone_variants(phone_e164)

    stmt = select(Client.id).where(Client.phone_e164.in_(variants))
    res = await session.execute(stmt)
    client_ids = [int(x) for x in res.scalars().all()]
    if not client_ids:
        return 0

    upd = (
        update(MessageJob)
        .where(MessageJob.client_id.in_(client_ids))
        .where(MessageJob.status == "queued")
        .where(MessageJob.job_type.in_(MARKETING_JOB_TYPES))
        .values(
            status="canceled",
            updated_at=utcnow(),
            last_error="Canceled: client unsubscribed",
        )
    )
    res2 = await session.execute(upd)
    return int(getattr(res2, "rowcount", 0) or 0)


def _ack_text(cmd: str) -> str:
    if cmd == "stop":
        return "Sie haben sich von Marketing-Nachrichten abgemeldet. Um wieder zu abonnieren, senden Sie START."

    return "Sie sind wieder angemeldet und erhalten Marketing-Nachrichten. Um sich abzumelden, senden Sie STOP."


async def lock_next_batch(
    session: AsyncSession,
    batch_size: int,
) -> Sequence[WhatsAppEvent]:
    stmt = (
        select(WhatsAppEvent)
        .where(WhatsAppEvent.status == "received")
        .order_by(WhatsAppEvent.received_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    events = list(res.scalars().all())

    for event in events:
        event.status = "processing"

    return events


def _extract_actions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []

    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue

        for change in entry.get("changes") or []:
            if not isinstance(change, dict):
                continue

            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue

            metadata = value.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}

            phone_number_id = metadata.get("phone_number_id")

            for msg in value.get("messages") or []:
                if not isinstance(msg, dict):
                    continue

                text = _extract_message_text(msg)
                cmd = _parse_command(text)

                phone = _norm_phone(msg.get("from"))
                if phone is None:
                    continue

                actions.append(
                    {
                        "cmd": cmd,
                        "phone_e164": phone,
                        "phone_number_id": phone_number_id,
                        "text": text,
                    }
                )

    return actions


def _extract_status_updates(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract delivery status events from a Meta WhatsApp payload.

    Returns a list of dicts with keys: wamid, status, timestamp, raw.
    Only includes entries whose status is in _WA_HANDLED_STATUSES.
    """
    updates: list[dict[str, Any]] = []

    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue

        for change in entry.get("changes") or []:
            if not isinstance(change, dict):
                continue

            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue

            for st in value.get("statuses") or []:
                if not isinstance(st, dict):
                    continue

                wamid = st.get("id")
                status = st.get("status")
                timestamp = st.get("timestamp")

                if not wamid or not status:
                    continue

                if status not in _WA_HANDLED_STATUSES:
                    continue

                updates.append(
                    {
                        "wamid": str(wamid),
                        "status": str(status),
                        "timestamp": timestamp,
                        "raw": st,
                    }
                )

    return updates


async def _apply_status_updates(
    session: AsyncSession,
    status_updates: list[dict[str, Any]],
) -> list[int]:
    """Apply Meta delivery status webhooks to OutboxMessage rows.

    Looks up each wamid via provider_message_id, advances status
    monotonically (no regression), and stores the raw payload in meta.

    Returns distinct campaign_run_id values for affected OutboxMessage
    rows that are linked to a CampaignRecipient — so the caller can
    trigger recompute_campaign_run_stats for each run.
    """
    if not status_updates:
        return []

    wamids = [u["wamid"] for u in status_updates]

    stmt = select(OutboxMessage).where(OutboxMessage.provider_message_id.in_(wamids))
    res = await session.execute(stmt)
    outbox_by_wamid: dict[str, OutboxMessage] = {}
    for ob in res.scalars().all():
        if ob.provider_message_id:
            outbox_by_wamid[ob.provider_message_id] = ob

    updated_outbox_ids: list[int] = []

    for upd in status_updates:
        wamid = upd["wamid"]
        new_status = upd["status"]
        ob = outbox_by_wamid.get(wamid)
        if ob is None:
            logger.debug(
                "status_webhook: no OutboxMessage wamid=%s status=%s",
                wamid,
                new_status,
            )
            continue

        current_rank = _WA_STATUS_RANK.get(ob.status, 0)
        new_rank = _WA_STATUS_RANK.get(new_status, 0)

        if new_rank <= current_rank:
            logger.debug(
                "status_webhook: no-op (no-regression) outbox_id=%s wamid=%s current=%s new=%s",
                ob.id,
                wamid,
                ob.status,
                new_status,
            )
            continue

        logger.info(
            "status_webhook: advancing outbox_id=%s wamid=%s %s -> %s",
            ob.id,
            wamid,
            ob.status,
            new_status,
        )

        ob.status = new_status

        # Persist timestamp and raw payload in meta for audit.
        meta = dict(ob.meta or {})
        meta[f"wa_status_{new_status}"] = {
            "timestamp": upd.get("timestamp"),
            "raw": upd.get("raw"),
        }
        ob.meta = meta

        updated_outbox_ids.append(int(ob.id))

    if not updated_outbox_ids:
        return []

    # Resolve campaign_run_ids linked to the updated outbox messages.
    cr_stmt = (
        select(CampaignRecipient.campaign_run_id)
        .where(CampaignRecipient.outbox_message_id.in_(updated_outbox_ids))
        .distinct()
    )
    cr_res = await session.execute(cr_stmt)
    return [int(r) for r in cr_res.scalars().all()]


async def _handle_operator_relay(
    session: AsyncSession,
    event: WhatsAppEvent,
    payload: dict[str, Any],
    provider: WhatsAppProvider,
) -> None:
    """Send operator reply from Chatwoot through Meta API.

    Creates an OutboxMessage with message_source='operator' so subsequent
    Meta delivery/read webhooks can be matched to this canonical record.

    Guard: this function is only called when chatwoot_operator_relay_enabled
    is True (checked in handle_event).
    """
    relay = payload.get("_chatwoot_operator_relay") or {}
    phone_e164 = relay.get("recipient_phone")
    text = relay.get("text", "")
    conversation_id = relay.get("conversation_id")
    chatwoot_message_id = relay.get("message_id")
    phone_number_id = relay.get("phone_number_id")
    agent_name = relay.get("agent_name", "")

    if not phone_e164 or not text:
        logger.warning(
            "operator_relay: missing recipient_phone or text conv_id=%s msg_id=%s — skipping",
            conversation_id,
            chatwoot_message_id,
        )
        event.error = "operator_relay: missing recipient_phone or text"
        return

    sender_id, company_id, routing_err = await _resolve_relay_sender(session, phone_number_id)

    if routing_err is not None:
        logger.warning(
            "operator_relay: routing blocked conv_id=%s msg_id=%s phone_number_id=%s err=%s",
            conversation_id,
            chatwoot_message_id,
            phone_number_id,
            routing_err,
        )
        event.error = routing_err
        return

    logger.info(
        "operator_relay: accepted conv_id=%s msg_id=%s phone=%s phone_number_id=%s sender_id=%s company_id=%s agent=%s",
        conversation_id,
        chatwoot_message_id,
        phone_e164,
        phone_number_id,
        sender_id,
        company_id,
        agent_name,
    )

    wamid, err = await safe_send(
        provider=provider,
        sender_id=sender_id,
        phone=phone_e164,
        text=text,
        company_id=company_id,
    )

    if err is not None:
        logger.warning(
            "operator_relay: send failed phone=%s sender_id=%s err=%s",
            phone_e164,
            sender_id,
            err,
        )
        event.error = f"operator_relay: send failed: {err}"
        return

    logger.info(
        "operator_relay: sent phone=%s wamid=%s sender_id=%s company_id=%s",
        phone_e164,
        wamid,
        sender_id,
        company_id,
    )

    now = utcnow()
    outbox = OutboxMessage(
        company_id=company_id,
        client_id=None,
        record_id=None,
        job_id=None,
        sender_id=sender_id,
        phone_e164=phone_e164,
        template_code="operator_relay",
        language="de",
        body=text,
        status="sent",
        provider_message_id=wamid,
        scheduled_at=now,
        sent_at=now,
        message_source="operator",
        meta={
            "chatwoot_conversation_id": conversation_id,
            "chatwoot_message_id": chatwoot_message_id,
            "agent_name": agent_name,
        },
    )
    session.add(outbox)
    await session.flush()

    logger.info(
        "operator_relay: outbox created outbox_id=%s wamid=%s phone=%s company_id=%s",
        outbox.id,
        wamid,
        phone_e164,
        company_id,
    )


async def handle_event(
    session: AsyncSession,
    event: WhatsAppEvent,
    provider: WhatsAppProvider,
) -> None:
    payload = event.payload or {}

    # ------------------------------------------------------------------ #
    # 0. Operator relay: Chatwoot outgoing → Meta (Meta-first path)       #
    # ------------------------------------------------------------------ #
    if _is_operator_relay(payload):
        if settings.chatwoot_operator_relay_enabled:
            await _handle_operator_relay(session, event, payload, provider)
        else:
            logger.warning(
                "operator_relay: event received but chatwoot_operator_relay_enabled=False, skipping event_id=%s",
                event.id,
            )
            event.error = "operator_relay: disabled by chatwoot_operator_relay_enabled"
        return

    # ------------------------------------------------------------------ #
    # 1. Delivery status webhooks (value.statuses)                        #
    # ------------------------------------------------------------------ #
    status_updates = _extract_status_updates(payload)
    if status_updates:
        run_ids = await _apply_status_updates(session, status_updates)
        for run_id in run_ids:
            try:
                await recompute_campaign_run_stats(session, run_id)
            except Exception as exc:
                logger.warning(
                    "status_webhook: recompute failed run_id=%s err=%s",
                    run_id,
                    exc,
                )

    # ------------------------------------------------------------------ #
    # 2. Inbound messages (value.messages) — STOP/START, Chatwoot        #
    # ------------------------------------------------------------------ #
    actions = _extract_actions(payload)
    if not actions:
        return

    action = actions[0]
    cmd = action.get("cmd")
    phone_e164 = str(action["phone_e164"])
    phone_number_id = action.get("phone_number_id")
    text = action.get("text", "")

    sender_id, company_id = await _pick_sender(session, phone_number_id)
    if text:
        if _is_chatwoot_origin(event, payload):
            logger.debug(
                "Skipping Chatwoot log for chatwoot-origin event dedupe_key=%s phone=%s",
                event.dedupe_key,
                phone_e164,
            )
        else:
            # 1. Ищем имя клиента в нашей базе данных.
            variants = _phone_variants(phone_e164)
            stmt = (
                select(Client.display_name)
                .where(Client.phone_e164.in_(variants))
                .where(Client.display_name.is_not(None))
                .limit(1)
            )
            res = await session.execute(stmt)
            client_name = res.scalar_one_or_none()

            # 2. Передаем найденное имя в Chatwoot.
            cw = ChatwootClient()
            try:
                await cw.log_incoming_message(phone_e164, text, contact_name=client_name)
                logger.info("Forwarded incoming message to Chatwoot phone=%s name=%s", phone_e164, client_name)
            except Exception as exc:
                logger.warning("Failed to forward to Chatwoot phone=%s err=%s", phone_e164, exc)
            finally:
                await cw.aclose()

    if cmd is None:
        event.error = None
        return

    reason = f"wa:{cmd}"
    opted_out = cmd == "stop"

    affected = await _set_opt_out(
        session,
        phone_e164=phone_e164,
        opted_out=opted_out,
        reason=reason,
    )

    canceled = 0
    if opted_out:
        canceled = await _cancel_marketing_jobs(session, phone_e164=phone_e164)

    logger.info(
        "wa_cmd=%s phone=%s sender_phone_number_id=%s sender_id=%s clients_updated=%s jobs_canceled=%s event_id=%s",
        cmd,
        phone_e164,
        phone_number_id,
        sender_id,
        affected,
        canceled,
        event.id,
    )

    if sender_id is None:
        event.error = "No sender found for incoming phone_number_id"
        return

    ack = _ack_text(cmd)
    msg_id, err = await safe_send(
        provider=provider,
        sender_id=sender_id,
        phone=phone_e164,
        text=ack,
    )

    if err is not None:
        logger.warning(
            "Ack send failed phone=%s sender_id=%s err=%s",
            phone_e164,
            sender_id,
            err,
        )
        event.error = f"Ack send failed: {err}"
        return

    event.error = None
    logger.info(
        "Ack sent phone=%s sender_id=%s msg_id=%s",
        phone_e164,
        sender_id,
        msg_id,
    )


async def process_one_event(
    event_id: int,
    provider: WhatsAppProvider,
) -> None:
    with perf_log("whatsapp_inbox_worker", "process_event", event_id=event_id) as ctx:
        async with SessionLocal() as session:
            async with session.begin():
                stmt = select(WhatsAppEvent).where(WhatsAppEvent.id == event_id).with_for_update()
                res = await session.execute(stmt)
                event = res.scalar_one_or_none()
                if event is None:
                    return

                ctx.update(
                    company_id=event.company_id,
                    dedupe_key=event.dedupe_key,
                    chatwoot_conversation_id=event.chatwoot_conversation_id,
                    origin="chatwoot" if _is_chatwoot_origin(event, event.payload or {}) else "meta",
                )

                try:
                    await handle_event(session, event, provider)
                    event.status = "processed"
                    event.processed_at = utcnow()
                except Exception as exc:
                    event.status = "failed"
                    event.processed_at = utcnow()
                    event.error = str(exc)
                    logger.exception("WhatsApp event failed id=%s", event_id)

                ctx.update(outcome=event.status)


async def run_loop(
    provider: WhatsAppProvider,
    batch_size: int = 50,
    poll_sec: float = 1.0,
) -> None:
    logger.info(
        "WhatsApp inbox worker started. batch_size=%s poll=%ss",
        batch_size,
        poll_sec,
    )

    while True:
        event_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                events = await lock_next_batch(session, batch_size)
                event_ids = [int(e.id) for e in events]

        if not event_ids:
            await asyncio.sleep(poll_sec)
            continue

        for eid in event_ids:
            await process_one_event(eid, provider)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    raise SystemExit("Run as a script: python -m altegio_bot.scripts.run_whatsapp_inbox_worker")


if __name__ == "__main__":
    main()
