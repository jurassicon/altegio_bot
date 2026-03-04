from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import Client, MessageJob, WhatsAppEvent, WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send

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
                if cmd is None:
                    continue

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


async def handle_event(
    session: AsyncSession,
    event: WhatsAppEvent,
    provider: WhatsAppProvider,
) -> None:
    payload = event.payload or {}

    actions = _extract_actions(payload)
    if not actions:
        return

    action = actions[0]
    cmd = str(action["cmd"])
    phone_e164 = str(action["phone_e164"])
    phone_number_id = action.get("phone_number_id")

    sender_id, company_id = await _pick_sender(session, phone_number_id)
    if company_id is not None:
        event.company_id = company_id

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
    async with SessionLocal() as session:
        async with session.begin():
            stmt = select(WhatsAppEvent).where(WhatsAppEvent.id == event_id).with_for_update()
            res = await session.execute(stmt)
            event = res.scalar_one_or_none()
            if event is None:
                return

            try:
                await handle_event(session, event, provider)
                event.status = "processed"
                event.processed_at = utcnow()
            except Exception as exc:
                event.status = "failed"
                event.processed_at = utcnow()
                event.error = str(exc)
                logger.exception("WhatsApp event failed id=%s", event_id)


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
