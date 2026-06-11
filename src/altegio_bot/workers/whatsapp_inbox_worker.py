from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import or_, select, update
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
from altegio_bot.providers.dummy import safe_send, safe_send_template
from altegio_bot.settings import settings
from altegio_bot.whatsapp_window import is_whatsapp_customer_window_open, normalize_phone
from altegio_bot.workers.promo_lead_handler import (
    handle_promo_command,
    handle_promo_info_command,
)

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


def _promo_keywords() -> frozenset[str]:
    return frozenset(word.strip().lower() for word in settings.promo_secret_words.split(",") if word.strip())


# Derived from settings so the word list is configurable without code changes.
PROMO_KEYWORDS: frozenset[str] = _promo_keywords()

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


def _normalize_reply_context_id(value: Any) -> str | None:
    """Normalize a WhatsApp ``context.id`` into a non-empty string or ``None``.

    Meta sends the replied-to wamid as a string, but a malformed/spoofed
    webhook could carry a non-string (int, dict, …) or whitespace.  Anything
    that is not a non-empty string becomes ``None`` so it never reaches a
    String-column lookup.
    """
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _coerce_chatwoot_id(value: Any) -> int | None:
    """Coerce a Chatwoot numeric id (int or digit string) to int, else None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


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

    if first in _promo_keywords():
        return "promo"

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
    Chatwoot webhook -> WhatsAppEvent -> worker -> forward to Chatwoot -> Chatwoot webhook -> ...

    Origin is decided ONLY by markers the Chatwoot webhook itself stamps: the
    "_chatwoot" payload key and the "chatwoot:" dedupe_key prefix.
    ``chatwoot_conversation_id`` is deliberately NOT an origin signal: it is a
    source-only marker, while forwarding a real Meta inbound records its
    destination separately (``forwarded_chatwoot_conversation_id``), so a Meta
    event must never flip into "chatwoot-origin" after being forwarded.
    """
    if "_chatwoot" in payload:
        return True
    if isinstance(event.dedupe_key, str) and event.dedupe_key.startswith("chatwoot:"):
        return True
    return False


def _event_origin_for_metrics(event: WhatsAppEvent, payload: dict[str, Any]) -> str:
    """Classify an event's origin for observability (metrics/log context only).

    This is intentionally SEPARATE from :func:`_is_chatwoot_origin`, which
    governs inbound loop prevention and must stay True only for the
    "_chatwoot" payload marker and the "chatwoot:" dedupe_key prefix.

    Operator relay events ("_chatwoot_operator_relay" payload, "chatwoot_out:"
    dedupe_key) are Chatwoot-authored but are NOT inbound-loop chatwoot-origin,
    so without a dedicated bucket they would be mislabeled "meta".  They get
    their own label here to remove that observability noise; classification
    has no effect on delivery or loop-prevention behavior.
    """
    if _is_operator_relay(payload):
        return "chatwoot_operator_relay"
    if _is_chatwoot_origin(event, payload):
        return "chatwoot"
    return "meta"


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


def _company_hint_from_inbox(
    chatwoot_inbox_id: int | None,
) -> tuple[int | None, str | None]:
    """Resolve company_id hint from Chatwoot inbox_id via settings mapping.

    Returns (company_id, error):
    - (None, None)  — mapping not configured; fall through to default logic.
    - (cid,  None)  — inbox found in mapping; use cid as routing hint.
    - (None, msg)   — mapping configured but inbox absent; fail-closed.
    """
    if chatwoot_inbox_id is None:
        return None, None

    raw = settings.chatwoot_inbox_company_map.strip()
    if not raw or raw == "{}":
        return None, None  # not configured

    try:
        mapping: dict = json.loads(raw)
    except Exception as exc:
        return (
            None,
            f"operator_relay: invalid CHATWOOT_INBOX_COMPANY_MAP: {exc}",
        )

    key = str(chatwoot_inbox_id)
    if key not in mapping:
        return (
            None,
            f"operator_relay: inbox_id={chatwoot_inbox_id} not found in CHATWOOT_INBOX_COMPANY_MAP — fail-closed",
        )

    return int(mapping[key]), None


async def _resolve_relay_sender(
    session: AsyncSession,
    phone_number_id: str | None,
    *,
    company_id_hint: int | None = None,
) -> tuple[int | None, int | None, str | None]:
    """Strict, fail-closed sender resolution for operator relay.

    Returns (sender_id, company_id, error).
    error is None on success; non-None means the relay must be blocked.

    If company_id_hint is provided (from inbox mapping), senders are
    filtered to that company first — routing is unambiguous within one
    company, safety-guard remains intact.

    Resolution rules (in order):
    - 0 active senders → error.
    - company_id_hint given → filter to that company; pick deterministically.
    - Active senders span >1 distinct company_ids → ambiguous error.
      Picking one would silently route through the wrong company context.
    - Multiple active senders but all in the same company → pick
      deterministically:
        1. prefer sender_code == 'default';
        2. fallback: sender with the lowest id.
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

    # ── Hint path: inbox mapping resolved a specific company ───────────
    if company_id_hint is not None:
        hinted = [s for s in senders if s.company_id == company_id_hint]
        if not hinted:
            return (
                None,
                None,
                f"operator_relay: no active sender for "
                f"phone_number_id={phone_number_id} "
                f"company_id={company_id_hint} (from inbox mapping)",
            )
        default_s = [s for s in hinted if s.sender_code == "default"]
        chosen = (default_s or sorted(hinted, key=lambda s: s.id))[0]
        logger.info(
            "operator_relay: resolved via inbox_company_map sender_id=%s company_id=%s hint=%s",
            chosen.id,
            chosen.company_id,
            company_id_hint,
        )
        return int(chosen.id), int(chosen.company_id), None

    # ── Default path: no hint, existing safety-guard ───────────────────
    distinct_companies = {s.company_id for s in senders}

    if len(distinct_companies) > 1:
        cids = sorted(str(c) for c in distinct_companies)
        logger.warning(
            "operator_relay: ambiguous sender routing "
            "phone_number_id=%s matched %d senders "
            "across %d company_ids=%s — blocking send",
            phone_number_id,
            len(senders),
            len(distinct_companies),
            ",".join(cids),
        )
        return (
            None,
            None,
            f"operator_relay: ambiguous sender routing for "
            f"phone_number_id={phone_number_id} "
            f"(matched {len(senders)} senders across "
            f"company_ids={','.join(cids)})",
        )

    default_senders = [s for s in senders if s.sender_code == "default"]
    chosen = (default_senders or sorted(senders, key=lambda s: s.id))[0]
    return int(chosen.id), int(chosen.company_id), None


def _phone_variants(phone_e164: str) -> list[str]:
    digits = re.sub(r"\D+", "", phone_e164)
    variants = {
        phone_e164,
        digits,
        f"+{digits}",
    }
    return [v for v in variants if v]


def _payload_message_from_matches_phone(payload: dict[str, Any], phone_e164: str) -> bool:
    expected_digits = re.sub(r"\D+", "", phone_e164)
    if not expected_digits:
        return False

    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        for change in entry.get("changes") or []:
            if not isinstance(change, dict):
                continue
            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue
            for msg in value.get("messages") or []:
                if not isinstance(msg, dict):
                    continue
                from_digits = re.sub(r"\D+", "", str(msg.get("from") or ""))
                if from_digits == expected_digits:
                    return True
    return False


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

                # Native WhatsApp reply: context.id is the wamid of the
                # message the client replied to.
                context = msg.get("context")
                if not isinstance(context, dict):
                    context = {}

                actions.append(
                    {
                        "cmd": cmd,
                        "phone_e164": phone,
                        "phone_number_id": phone_number_id,
                        "text": text,
                        "reply_to_provider_message_id": _normalize_reply_context_id(context.get("id")),
                        "whatsapp_message_id": _normalize_reply_context_id(msg.get("id")),
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

    logger.info(
        "status_webhook: processing %d update(s) wamids=%s",
        len(status_updates),
        wamids,
    )

    stmt = select(OutboxMessage).where(OutboxMessage.provider_message_id.in_(wamids))
    res = await session.execute(stmt)
    outbox_by_wamid: dict[str, OutboxMessage] = {}
    for ob in res.scalars().all():
        if ob.provider_message_id:
            outbox_by_wamid[ob.provider_message_id] = ob

    updated_outbox_ids: list[int] = []
    outbox_id_to_new_status: dict[int, str] = {}

    for upd in status_updates:
        wamid = upd["wamid"]
        new_status = upd["status"]
        ob = outbox_by_wamid.get(wamid)
        if ob is None:
            logger.info(
                "status_webhook: no OutboxMessage matched wamid=%s status=%s",
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
        outbox_id_to_new_status[int(ob.id)] = new_status

    if not updated_outbox_ids:
        return []

    # Advance followup_status on CampaignRecipient rows linked via followup_outbox_id.
    # Uses the same no-downgrade rule: read > delivered > sent.
    fu_status_ids = [oid for oid, st in outbox_id_to_new_status.items() if st in {"delivered", "read"}]
    if fu_status_ids:
        fu_stmt = select(CampaignRecipient).where(CampaignRecipient.followup_outbox_id.in_(fu_status_ids))
        fu_res = await session.execute(fu_stmt)
        for recipient in fu_res.scalars().all():
            fu_oid = recipient.followup_outbox_id
            if fu_oid is None:
                continue
            new_followup = outbox_id_to_new_status.get(int(fu_oid))
            if new_followup == "read":
                recipient.followup_status = "read"
            elif new_followup == "delivered" and recipient.followup_status != "read":
                recipient.followup_status = "delivered"

    # Resolve campaign_run_ids linked to the updated outbox messages (primary or follow-up).
    cr_stmt = (
        select(CampaignRecipient.campaign_run_id)
        .where(
            or_(
                CampaignRecipient.outbox_message_id.in_(updated_outbox_ids),
                CampaignRecipient.followup_outbox_id.in_(updated_outbox_ids),
            )
        )
        .distinct()
    )
    cr_res = await session.execute(cr_stmt)
    return [int(r) for r in cr_res.scalars().all()]


@dataclass(frozen=True)
class ReplyContextTarget:
    """Resolved native-reply mapping for an inbound WhatsApp reply.

    ``chatwoot_message_id`` / ``chatwoot_conversation_id`` point at the
    operator message the client replied to; ``body`` is its display text,
    used only for the visible fallback quote when no native id is usable.
    """

    chatwoot_message_id: int | None
    chatwoot_conversation_id: int | None
    body: str | None


@dataclass(frozen=True)
class WhatsAppReplyContextTarget:
    provider_message_id: str
    source: str


async def _get_reply_context_target(
    session: AsyncSession,
    provider_message_id: str | None,
    *,
    phone_e164: str | None,
) -> ReplyContextTarget | None:
    """Resolve a replied-to wamid to a prior operator-relay OutboxMessage.

    Scoped to ``phone_e164`` as defense-in-depth so a malformed/spoofed
    ``context.id`` can never resolve to another client's message.  PR1 covers
    replies to operator messages only (``message_source='operator'``);
    bot/campaign messages are not reply targets.  Returns ``None`` on a miss.
    """
    if not provider_message_id or not phone_e164:
        return None

    stmt = (
        select(
            OutboxMessage.chatwoot_message_id,
            OutboxMessage.chatwoot_conversation_id,
            OutboxMessage.body,
        )
        .where(OutboxMessage.provider_message_id == provider_message_id)
        .where(OutboxMessage.phone_e164 == phone_e164)
        .where(OutboxMessage.message_source == "operator")
        .order_by(OutboxMessage.created_at.desc(), OutboxMessage.id.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    row = res.first()
    if row is None:
        return None
    return ReplyContextTarget(
        chatwoot_message_id=row[0],
        chatwoot_conversation_id=row[1],
        body=row[2],
    )


async def _get_whatsapp_reply_context_target(
    session: AsyncSession,
    chatwoot_message_id: int | None,
    *,
    chatwoot_conversation_id: int | None,
    phone_e164: str | None,
) -> WhatsAppReplyContextTarget | None:
    """Resolve a Chatwoot Reply target to a WhatsApp wamid for Meta context.

    Source A is a real inbound WhatsAppEvent that PR1 forwarded into the same
    Chatwoot conversation.  Source B is a previous human operator OutboxMessage
    in the same conversation.  Both are phone-scoped defense-in-depth checks.
    """
    if not chatwoot_message_id or not chatwoot_conversation_id or not phone_e164:
        return None

    event_stmt = (
        select(WhatsAppEvent)
        .where(WhatsAppEvent.chatwoot_message_id == chatwoot_message_id)
        .where(WhatsAppEvent.forwarded_chatwoot_conversation_id == chatwoot_conversation_id)
        .where(WhatsAppEvent.whatsapp_message_id.is_not(None))
        .where(WhatsAppEvent.whatsapp_message_id != "")
        .where(WhatsAppEvent.dedupe_key.like("wa:%"))
        .order_by(WhatsAppEvent.received_at.desc(), WhatsAppEvent.id.desc())
        .limit(20)
    )
    event_res = await session.execute(event_stmt)
    for candidate in event_res.scalars().all():
        candidate_payload = candidate.payload or {}
        if not isinstance(candidate_payload, dict):
            continue
        if _payload_message_from_matches_phone(candidate_payload, phone_e164):
            provider_message_id = _normalize_reply_context_id(candidate.whatsapp_message_id)
            if provider_message_id:
                return WhatsAppReplyContextTarget(
                    provider_message_id=provider_message_id,
                    source="whatsapp_event",
                )

    outbox_stmt = (
        select(OutboxMessage.provider_message_id)
        .where(OutboxMessage.chatwoot_message_id == chatwoot_message_id)
        .where(OutboxMessage.chatwoot_conversation_id == chatwoot_conversation_id)
        .where(OutboxMessage.phone_e164 == phone_e164)
        .where(OutboxMessage.message_source == "operator")
        .where(OutboxMessage.provider_message_id.is_not(None))
        .where(OutboxMessage.provider_message_id != "")
        .order_by(OutboxMessage.created_at.desc(), OutboxMessage.id.desc())
        .limit(1)
    )
    outbox_res = await session.execute(outbox_stmt)
    provider_message_id = _normalize_reply_context_id(outbox_res.scalar_one_or_none())
    if provider_message_id:
        return WhatsAppReplyContextTarget(
            provider_message_id=provider_message_id,
            source="outbox_operator",
        )

    return None


_QUOTE_MAX_CHARS = 300


def _format_reply_context_prefix(quoted_body: str | None) -> str:
    """Build the visible quote prefix shown above a WhatsApp reply in Chatwoot.

    Used only when no safe native ``in_reply_to`` mapping is available
    (missing target, no chatwoot_message_id, or a cross-conversation target),
    so the operator still sees that the client used a reply.
    """
    if not quoted_body:
        return "↩️ Ответ на сообщение в WhatsApp"
    if quoted_body == "[image]":
        return "↩️ Ответ на изображение"
    quoted = quoted_body
    if len(quoted) > _QUOTE_MAX_CHARS:
        quoted = quoted[:_QUOTE_MAX_CHARS].rstrip() + "…"
    return f"↩️ Ответ на сообщение:\n«{quoted}»"


async def _forward_text_to_chatwoot(
    session: AsyncSession,
    event: WhatsAppEvent,
    *,
    phone_e164: str,
    text: str,
    reply_to_provider_message_id: str | None = None,
) -> None:
    """Forward an inbound Meta-origin text to Chatwoot, native-reply first.

    Resolves the destination conversation BEFORE posting so a native
    ``in_reply_to`` is attached only when the replied-to operator message
    lives in that same conversation; otherwise a visible quote prefix is
    used.  Records the destination in ``forwarded_chatwoot_conversation_id``
    — never in ``chatwoot_conversation_id``, which stays a Chatwoot-origin
    source marker.
    """
    variants = _phone_variants(phone_e164)
    stmt = (
        select(Client.display_name)
        .where(Client.phone_e164.in_(variants))
        .where(Client.display_name.is_not(None))
        .limit(1)
    )
    res = await session.execute(stmt)
    client_name = res.scalar_one_or_none()

    cw = ChatwootClient()
    try:
        conversation_id = await cw.get_or_create_incoming_conversation(
            phone_e164,
            contact_name=client_name,
        )

        content = text
        content_attributes: dict[str, Any] | None = None
        if reply_to_provider_message_id:
            target = await _get_reply_context_target(
                session,
                reply_to_provider_message_id,
                phone_e164=phone_e164,
            )
            native_ok = (
                target is not None
                and target.chatwoot_message_id is not None
                and target.chatwoot_conversation_id == conversation_id
            )
            if native_ok:
                content_attributes = {
                    "in_reply_to": target.chatwoot_message_id,
                    "in_reply_to_external_id": reply_to_provider_message_id,
                }
            else:
                if (
                    target is not None
                    and target.chatwoot_message_id is not None
                    and target.chatwoot_conversation_id != conversation_id
                ):
                    logger.info(
                        "reply_context: skipping native mapping, conversation differs "
                        "target_conversation_id=%s destination_conversation_id=%s",
                        target.chatwoot_conversation_id,
                        conversation_id,
                    )
                quoted_body = target.body if target is not None else None
                prefix = _format_reply_context_prefix(quoted_body)
                content = f"{prefix}\n\n{text}"

        message_id = await cw.send_message(
            conversation_id,
            content,
            message_type="incoming",
            content_attributes=content_attributes,
        )
    except Exception as exc:
        # Keep the persisted error free of URLs/tokens/response bodies.
        safe_error = f"chatwoot forward failed: {type(exc).__name__}"
        event.error = safe_error
        logger.warning(
            "chatwoot: forward failed phone=%s %s",
            phone_e164,
            type(exc).__name__,
        )
        raise RuntimeError(safe_error) from None
    finally:
        await cw.aclose()

    event.forwarded_chatwoot_conversation_id = conversation_id
    event.chatwoot_message_id = message_id
    event.error = None
    logger.info(
        "Forwarded incoming message to Chatwoot phone=%s name=%s conversation_id=%s message_id=%s native_reply=%s",
        phone_e164,
        client_name,
        conversation_id,
        message_id,
        content_attributes is not None,
    )


async def _handle_operator_relay(
    session: AsyncSession,
    event: WhatsAppEvent,
    payload: dict[str, Any],
    provider: WhatsAppProvider,
) -> None:
    """Send operator reply from Chatwoot through Meta API.

    Always checks the 24h Meta customer service window before sending:
    - Window open → sends free-form text.
    - Window closed + private_note_only (default) → blocks the Meta send,
      creates a canceled OutboxMessage, and adds a Chatwoot private note to
      alert the operator that the message was not delivered.
    - Window closed + reopen_template → sends an approved Meta template and
      adds a Chatwoot private note with the original text.

    Creates an OutboxMessage with message_source='operator' so subsequent
    Meta delivery/read webhooks can be matched to this canonical record.

    Guard: this function is only called when chatwoot_operator_relay_enabled
    is True (checked in handle_event).
    """
    relay = payload.get("_chatwoot_operator_relay") or {}
    raw_phone = relay.get("recipient_phone")
    phone_e164 = normalize_phone(raw_phone)
    text = relay.get("text", "")
    conversation_id = relay.get("conversation_id")
    chatwoot_message_id = relay.get("message_id")
    phone_number_id = relay.get("phone_number_id")
    chatwoot_inbox_id = relay.get("chatwoot_inbox_id")
    agent_name = relay.get("agent_name", "")
    content_attributes = relay.get("content_attributes") or {}
    if not isinstance(content_attributes, dict):
        content_attributes = {}

    # Indexed copies for the native-reply lookup (kept in meta as-is for
    # backward compatibility).  Non-numeric values degrade to None.
    cw_conversation_id = _coerce_chatwoot_id(conversation_id)
    cw_message_id = _coerce_chatwoot_id(chatwoot_message_id)
    reply_to_chatwoot_message_id = _coerce_chatwoot_id(relay.get("reply_to_chatwoot_message_id"))
    reply_context_audit: dict[str, Any] = {
        "reply_to_chatwoot_message_id": reply_to_chatwoot_message_id,
        "reply_to_provider_message_id": None,
        "reply_context_source": None,
        "reply_context_native": False,
    }
    if content_attributes:
        reply_context_audit["content_attributes"] = content_attributes

    if phone_e164 is None:
        logger.warning(
            "operator_relay: invalid recipient_phone=%r conv_id=%s msg_id=%s — skipping",
            raw_phone,
            conversation_id,
            chatwoot_message_id,
        )
        event.error = "operator_relay: invalid recipient_phone"
        return

    if not text:
        logger.warning(
            "operator_relay: missing text conv_id=%s msg_id=%s — skipping",
            conversation_id,
            chatwoot_message_id,
        )
        event.error = "operator_relay: missing text"
        return

    company_id_hint, hint_err = _company_hint_from_inbox(chatwoot_inbox_id)
    if hint_err is not None:
        logger.warning(
            "operator_relay: inbox routing error conv_id=%s msg_id=%s inbox_id=%s: %s",
            conversation_id,
            chatwoot_message_id,
            chatwoot_inbox_id,
            hint_err,
        )
        event.error = hint_err
        return

    sender_id, company_id, routing_err = await _resolve_relay_sender(
        session,
        phone_number_id,
        company_id_hint=company_id_hint,
    )

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

    # Use the primary (Meta) transport directly — the operator's message is
    # already visible in Chatwoot, so mirroring it back would create a duplicate.
    meta_provider = getattr(provider, "_primary", provider)
    now = utcnow()
    mode = settings.chatwoot_operator_closed_window_mode

    # ── Always check the 24h customer service window ──────────────────────
    window_open, last_inbound_at = await is_whatsapp_customer_window_open(session, phone_e164, now)
    hours_since: float = (now - last_inbound_at).total_seconds() / 3600 if last_inbound_at else -1.0
    logger.info(
        "operator_relay: window_check phone=%s conv_id=%s msg_id=%s "
        "window_open=%s last_inbound_at=%s hours_since=%.1f mode=%s",
        phone_e164,
        conversation_id,
        chatwoot_message_id,
        window_open,
        last_inbound_at.isoformat() if last_inbound_at else None,
        hours_since,
        mode,
    )

    last_inbound_iso: str | None = last_inbound_at.isoformat() if last_inbound_at else None

    # ── Branch: window open → send as free-form text ──────────────────────
    if window_open:
        reply_to_provider_message_id: str | None = None
        if reply_to_chatwoot_message_id is not None:
            target = await _get_whatsapp_reply_context_target(
                session,
                reply_to_chatwoot_message_id,
                chatwoot_conversation_id=cw_conversation_id,
                phone_e164=phone_e164,
            )
            if target is not None:
                reply_to_provider_message_id = target.provider_message_id
                reply_context_audit.update(
                    {
                        "reply_to_provider_message_id": target.provider_message_id,
                        "reply_context_source": target.source,
                        "reply_context_native": True,
                    }
                )
                logger.info(
                    "operator_relay: native reply context resolved conv_id=%s msg_id=%s source=%s",
                    conversation_id,
                    chatwoot_message_id,
                    target.source,
                )
            else:
                logger.info(
                    "operator_relay: native reply context target not found conv_id=%s msg_id=%s reply_to=%s",
                    conversation_id,
                    chatwoot_message_id,
                    reply_to_chatwoot_message_id,
                )

        logger.info(
            "operator_relay: direct text sent (window open) phone=%s conv_id=%s mode=%s",
            phone_e164,
            conversation_id,
            mode,
        )

        wamid, err = await safe_send(
            provider=meta_provider,
            sender_id=sender_id,
            phone=phone_e164,
            text=text,
            company_id=company_id,
            reply_to_provider_message_id=reply_to_provider_message_id,
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
            chatwoot_conversation_id=cw_conversation_id,
            chatwoot_message_id=cw_message_id,
            meta={
                "chatwoot_conversation_id": conversation_id,
                "chatwoot_message_id": chatwoot_message_id,
                "agent_name": agent_name,
                "send_type": "text",
                "wa_window_open": True,
                "last_meta_inbound_at": last_inbound_iso,
                "closed_window_mode": mode,
                **reply_context_audit,
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
        return

    # ── Branch: window closed + mode=private_note_only ────────────────────
    if mode == "private_note_only":
        logger.info(
            "operator_relay: window closed, mode=private_note_only → blocking send phone=%s conv_id=%s",
            phone_e164,
            conversation_id,
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
            status="canceled",
            provider_message_id=None,
            scheduled_at=now,
            sent_at=None,
            message_source="operator",
            chatwoot_conversation_id=cw_conversation_id,
            chatwoot_message_id=cw_message_id,
            meta={
                "send_type": "none",
                "attempted_send_type": "text",
                "wa_window_open": False,
                "last_meta_inbound_at": last_inbound_iso,
                "closed_window_mode": mode,
                "cancel_reason": "customer_service_window_closed",
                "chatwoot_conversation_id": conversation_id,
                "chatwoot_message_id": chatwoot_message_id,
                "agent_name": agent_name,
                **reply_context_audit,
            },
        )
        session.add(outbox)
        await session.flush()

        logger.info(
            "operator_relay: canceled outbox created outbox_id=%s phone=%s company_id=%s",
            outbox.id,
            phone_e164,
            company_id,
        )

        if settings.chatwoot_operator_reopen_private_note_enabled and conversation_id:
            private_note = (
                "⚠️ Das 24h-WhatsApp-Fenster ist geschlossen."
                " Die Nachricht wurde nicht an WhatsApp zugestellt.\n"
                "Bitte warte, bis der Kunde erneut schreibt, oder wende dich direkt an ihn.\n\n"
                f'Originalnachricht:\n"{text}"'
            )
            cw = ChatwootClient()
            try:
                await cw.send_message(
                    conversation_id,
                    private_note,
                    message_type="outgoing",
                    private=True,
                )
                logger.info(
                    "operator_relay: closed-window note sent conv_id=%s",
                    conversation_id,
                )
                outbox.meta = {**outbox.meta, "private_note_status": "sent"}
            except Exception as exc:
                logger.warning(
                    "operator_relay: closed-window note failed conv_id=%s err=%s",
                    conversation_id,
                    exc,
                )
                outbox.meta = {
                    **outbox.meta,
                    "private_note_status": "failed",
                    "private_note_error": str(exc),
                }
                outbox.error = f"private note failed: {exc}"
                event.error = f"operator_relay: private note failed: {exc}"
            finally:
                await cw.aclose()
        else:
            outbox.meta = {**outbox.meta, "private_note_status": "disabled"}
        return

    # ── Branch: window closed + mode=reopen_template ──────────────────────
    logger.info(
        "operator_relay: direct text skipped (window closed) phone=%s conv_id=%s — sending reopen template",
        phone_e164,
        conversation_id,
    )

    template_name = settings.chatwoot_operator_reopen_template_name
    language = settings.chatwoot_operator_reopen_template_language
    param_mode = settings.chatwoot_operator_reopen_template_param_mode

    contact_name = relay.get("contact_name") or phone_e164 or "Kunde"
    params: list[str] = []
    if param_mode == "contact_name":
        params = [contact_name]

    wamid, err = await safe_send_template(
        provider=meta_provider,
        sender_id=sender_id,
        phone=phone_e164,
        template_name=template_name,
        language=language,
        params=params,
        company_id=company_id,
    )

    if err is not None:
        logger.warning(
            "operator_relay: reopen template failed phone=%s sender_id=%s template=%s err=%s",
            phone_e164,
            sender_id,
            template_name,
            err,
        )
        event.error = f"operator_relay: reopen template failed: {err}"

        if settings.chatwoot_operator_reopen_private_note_enabled and conversation_id:
            failure_note = (
                "❌ Die Vorlage zum Wiederöffnen des WhatsApp-Dialogs konnte nicht gesendet"
                " werden. Die ursprüngliche Nachricht wurde nicht an WhatsApp zugestellt."
            )
            cw = ChatwootClient()
            try:
                await cw.send_message(
                    conversation_id,
                    failure_note,
                    message_type="outgoing",
                    private=True,
                )
                logger.info(
                    "operator_relay: failure note sent conv_id=%s",
                    conversation_id,
                )
            except Exception as exc:
                logger.warning(
                    "operator_relay: failure note failed conv_id=%s err=%s",
                    conversation_id,
                    exc,
                )
            finally:
                await cw.aclose()
        return

    logger.info(
        "operator_relay: reopen template sent phone=%s wamid=%s template=%s lang=%s conv_id=%s",
        phone_e164,
        wamid,
        template_name,
        language,
        conversation_id,
    )

    now = utcnow()
    outbox = OutboxMessage(
        company_id=company_id,
        client_id=None,
        record_id=None,
        job_id=None,
        sender_id=sender_id,
        phone_e164=phone_e164,
        template_code="operator_reopen_template",
        language=language,
        body=text,
        status="sent",
        provider_message_id=wamid,
        scheduled_at=now,
        sent_at=now,
        message_source="operator",
        chatwoot_conversation_id=cw_conversation_id,
        chatwoot_message_id=cw_message_id,
        meta={
            "send_type": "template",
            "template": template_name,
            "template_language": language,
            "original_operator_text": text,
            "chatwoot_conversation_id": conversation_id,
            "chatwoot_message_id": chatwoot_message_id,
            "agent_name": agent_name,
            "wa_window_open": False,
            "last_meta_inbound_at": last_inbound_iso,
            "reopen_reason": "customer_service_window_closed",
            "closed_window_mode": mode,
            **reply_context_audit,
        },
    )
    session.add(outbox)
    await session.flush()

    logger.info(
        "operator_relay: reopen outbox created outbox_id=%s wamid=%s phone=%s company_id=%s",
        outbox.id,
        wamid,
        phone_e164,
        company_id,
    )

    if settings.chatwoot_operator_reopen_private_note_enabled and conversation_id:
        private_note = (
            "⚠️ Das 24h-WhatsApp-Fenster war geschlossen. Die ursprüngliche Nachricht"
            " wurde nicht direkt gesendet. Stattdessen wurde eine Vorlage gesendet, damit"
            " der Kunde den Dialog wieder öffnen kann.\n\n"
            f'Originalnachricht:\n"{text}"'
        )
        cw = ChatwootClient()
        try:
            await cw.send_message(
                conversation_id,
                private_note,
                message_type="outgoing",
                private=True,
            )
            logger.info(
                "operator_relay: private note sent conv_id=%s",
                conversation_id,
            )
        except Exception as exc:
            logger.warning(
                "operator_relay: private note failed conv_id=%s err=%s",
                conversation_id,
                exc,
            )
        finally:
            await cw.aclose()


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
        logger.info(
            "status_webhook: received %d status update(s) event_id=%s",
            len(status_updates),
            event.id,
        )
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
    reply_to_provider_message_id = action.get("reply_to_provider_message_id")

    sender_id, company_id = await _pick_sender(session, phone_number_id)

    chatwoot_origin = _is_chatwoot_origin(event, payload)

    # Audit: keep the inbound wamid of real Meta-origin messages.  Chatwoot
    # mirrors carry a synthetic Chatwoot message id there, so they are skipped.
    if not chatwoot_origin and action.get("whatsapp_message_id"):
        event.whatsapp_message_id = action["whatsapp_message_id"]

    if text and cmd is None:
        if chatwoot_origin:
            logger.debug(
                "Skipping Chatwoot log for chatwoot-origin event dedupe_key=%s phone=%s",
                event.dedupe_key,
                phone_e164,
            )
        else:
            await _forward_text_to_chatwoot(
                session,
                event,
                phone_e164=phone_e164,
                text=text,
                reply_to_provider_message_id=reply_to_provider_message_id,
            )

    if cmd is None:
        event.error = None
        return

    # Guard: Chatwoot-origin mirror events must not execute inbound commands.
    # The original Meta-origin event already handled the command; processing
    # its Chatwoot mirror causes duplicate acks and double opt-outs.
    if chatwoot_origin:
        logger.info(
            "Skipping inbound command handling for Chatwoot-origin event id=%s dedupe_key=%s",
            event.id,
            event.dedupe_key,
        )
        return

    # ── Promo lead funnel ────────────────────────────────────────────────────
    if cmd == "promo":
        if sender_id is None:
            event.error = "No sender found for incoming phone_number_id"
            return
        if settings.promo_lead_funnel_enabled:
            await handle_promo_command(
                session=session,
                event=event,
                phone_e164=phone_e164,
                text=text,
                sender_id=sender_id,
                company_id=company_id,
                provider=provider,
            )
        else:
            await handle_promo_info_command(
                session=session,
                event=event,
                phone_e164=phone_e164,
                text=text,
                sender_id=sender_id,
                company_id=company_id,
                provider=provider,
            )
        return

    if cmd in ("stop", "start"):
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
    else:
        logger.info(
            "wa_cmd=%s phone=%s sender_phone_number_id=%s sender_id=%s event_id=%s",
            cmd,
            phone_e164,
            phone_number_id,
            sender_id,
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

    now = utcnow()
    session.add(
        OutboxMessage(
            company_id=company_id,
            client_id=None,
            record_id=None,
            job_id=None,
            sender_id=sender_id,
            phone_e164=phone_e164,
            template_code=f"wa_cmd_{cmd}",
            language="de",
            body=ack,
            status="sent",
            provider_message_id=msg_id,
            scheduled_at=now,
            sent_at=now,
            message_source="bot",
            meta={
                "source": "inbound_command",
                "command": cmd,
                "inbound_text": text,
                "whatsapp_event_id": event.id,
            },
        )
    )

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
                    origin=_event_origin_for_metrics(event, event.payload or {}),
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


def _resolve_poll_sec(
    explicit: float | None,
    settings_value: float,
) -> float:
    return settings_value if explicit is None else explicit


async def run_loop(
    provider: WhatsAppProvider,
    batch_size: int = 50,
    poll_sec: float | None = None,
) -> None:
    effective_poll_sec = _resolve_poll_sec(poll_sec, settings.whatsapp_inbox_worker_poll_sec)
    logger.info(
        "WhatsApp inbox worker started. batch_size=%s poll=%ss",
        batch_size,
        effective_poll_sec,
    )

    while True:
        event_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                events = await lock_next_batch(session, batch_size)
                event_ids = [int(e.id) for e in events]

        if not event_ids:
            await asyncio.sleep(effective_poll_sec)
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
