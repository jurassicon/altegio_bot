from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Sequence

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.runner import recompute_campaign_run_stats
from altegio_bot.chatwoot_client import ChatwootClient
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import (
    CampaignRecipient,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.perf import perf_log
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send, safe_send_template
from altegio_bot.services import meta_circuit
from altegio_bot.services.meta_error_classifier import (
    is_transient_provider_error,
    transient_error_reason,
)
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

DELIVERY_RETRY_JOB_TYPES = (
    "record_created",
    "record_updated",
    "record_canceled",
    "reminder_24h",
    "reminder_2h",
)

_DELIVERY_RETRY_DEDUPE_PREFIX = "delivery_retry:"
_DELIVERY_RETRY_DELAYS_SECONDS = (
    10 * 60,
    30 * 60,
    2 * 60 * 60,
    6 * 60 * 60,
)
_DELIVERY_RETRY_MAX_ATTEMPTS = 4
_DELIVERY_STATUS_ORDER = {"sent": 0, "delivered": 1, "read": 2}
_SUCCESSFUL_DELIVERY_STATUSES = ("delivered", "read")

_PERMANENT_DELIVERY_ERROR_CODES = {
    131026,
    131051,
    131008,
    131009,
    100,
    33,
    132000,
    132001,
    132005,
    132007,
    132012,
    132015,
    132016,
}
_TRANSIENT_DELIVERY_ERROR_CODES = {
    0,
    131000,
    131016,
    131056,
}
_PERMANENT_DELIVERY_FAILURE_KEYWORDS = (
    "not a whatsapp user",
    "not registered on whatsapp",
    "invalid recipient",
    "invalid phone",
    "recipient has blocked",
    "opted out",
    "unsubscribed",
)
_PERMANENT_DELIVERY_CODE_10_KEYWORDS = (
    "permission",
    "auth",
    "access",
    "config",
    "credential",
    "token",
    "oauth",
    "unauthorized",
    "forbidden",
    "permanent",
)
_TRANSIENT_DELIVERY_FAILURE_KEYWORDS = (
    "transient",
    "temporary",
    "temporarily",
    "rate",
    "throttle",
    "throttled",
    "timeout",
    "timed out",
    "try again",
    "unavailable",
    "overloaded",
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

                # Inbound reaction: a first-class action, not free text.  Meta
                # sends type="reaction" with reaction.emoji (empty when the
                # client removes the reaction) and reaction.message_id (the
                # wamid of the reacted-to message).  Handled before any text /
                # command logic so an emoji never flows into STOP/START/promo.
                if msg.get("type") == "reaction":
                    reaction = msg.get("reaction")
                    if not isinstance(reaction, dict):
                        reaction = {}
                    emoji = reaction.get("emoji")
                    actions.append(
                        {
                            "kind": "reaction",
                            "cmd": None,
                            "phone_e164": phone,
                            "phone_number_id": phone_number_id,
                            "text": emoji or "",
                            "reaction_emoji": emoji,
                            "reaction_target_provider_message_id": _normalize_reply_context_id(
                                reaction.get("message_id")
                            ),
                            "whatsapp_message_id": _normalize_reply_context_id(msg.get("id")),
                        }
                    )
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


def _delivery_retry_delay_seconds(attempt: int) -> int:
    if attempt < 1:
        attempt = 1
    if attempt > len(_DELIVERY_RETRY_DELAYS_SECONDS):
        return _DELIVERY_RETRY_DELAYS_SECONDS[-1]
    return _DELIVERY_RETRY_DELAYS_SECONDS[attempt - 1]


def _extract_statuses(payload: dict[str, Any]) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []

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

            for st in value.get("statuses") or []:
                if not isinstance(st, dict):
                    continue
                wamid = st.get("id")
                status = str(st.get("status") or "").strip().lower()
                if not isinstance(wamid, str) or not wamid.strip() or status not in _WA_HANDLED_STATUSES:
                    continue

                errors = st.get("errors")
                first_error: dict[str, Any] = {}
                if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                    first_error = errors[0]
                error_data = first_error.get("error_data")
                if not isinstance(error_data, dict):
                    error_data = {}

                statuses.append(
                    {
                        "provider_message_id": wamid.strip(),
                        "status": status,
                        "timestamp": st.get("timestamp"),
                        "recipient_id": st.get("recipient_id"),
                        "phone_number_id": phone_number_id,
                        "error_code": first_error.get("code"),
                        "error_title": first_error.get("title"),
                        "error_message": first_error.get("message"),
                        "error_details": error_data.get("details"),
                    }
                )

    return statuses


def _classify_delivery_failure(status: dict[str, Any]) -> str:
    if not status.get("provider_message_id"):
        return "permanent"

    code = status.get("error_code")
    try:
        code_int: int | None = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_int = None

    title = str(status.get("error_title") or "").lower()
    message = str(status.get("error_message") or "").lower()
    details = str(status.get("error_details") or "").lower()
    combined = " ".join(part for part in (title, message, details) if part)

    if code_int == 10:
        if any(kw in combined for kw in _PERMANENT_DELIVERY_CODE_10_KEYWORDS):
            return "permanent"
        if any(kw in combined for kw in _TRANSIENT_DELIVERY_FAILURE_KEYWORDS):
            return "retryable"
        return "permanent"

    if code_int in _TRANSIENT_DELIVERY_ERROR_CODES:
        return "retryable"
    if code_int in _PERMANENT_DELIVERY_ERROR_CODES:
        return "permanent"

    if any(kw in combined for kw in _PERMANENT_DELIVERY_FAILURE_KEYWORDS):
        return "permanent"
    if any(kw in combined for kw in _TRANSIENT_DELIVERY_FAILURE_KEYWORDS):
        return "retryable"

    return "unknown_retryable_bounded"


def _is_retryable_delivery_failure(status: dict[str, Any]) -> bool:
    return _classify_delivery_failure(status) != "permanent"


def _sanitize_delivery_detail(value: Any, limit: int = 300) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def _mark_outbox_delivery_failed(outbox: OutboxMessage, status: dict[str, Any], reason: str) -> None:
    code = status.get("error_code")
    title = status.get("error_title")
    details = _sanitize_delivery_detail(status.get("error_details"))
    provider_message_id = status.get("provider_message_id")

    outbox.status = "failed"
    bits = " ".join(
        bit for bit in (f"code={code}" if code is not None else "", f"title={title}" if title else "") if bit
    )
    outbox.error = f"WA delivery failed {bits}".strip()

    meta = dict(outbox.meta or {})
    meta["delivery_failed"] = True
    meta["delivery_failed_at"] = utcnow().isoformat()
    meta["delivery_failed_code"] = code
    meta["delivery_failed_title"] = title
    meta["delivery_failed_details"] = details
    meta["delivery_failed_provider_message_id"] = provider_message_id
    meta["delivery_failed_reason"] = reason
    outbox.meta = meta


def _mark_stale_failed_after_success(outbox: OutboxMessage, status: dict[str, Any]) -> None:
    meta = dict(outbox.meta or {})
    meta["stale_failed_after_success"] = True
    meta["stale_failed_code"] = status.get("error_code")
    meta["stale_failed_title"] = status.get("error_title")
    meta["stale_failed_at"] = utcnow().isoformat()
    outbox.meta = meta


def _delivery_retry_chain_original_outbox_id(outbox: OutboxMessage) -> int:
    meta = outbox.meta or {}
    raw = meta.get("delivery_retry_of_outbox_id")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    return int(outbox.id)


async def _find_outbox_by_provider_message_id(
    session: AsyncSession,
    provider_message_id: str,
) -> OutboxMessage | None:
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.provider_message_id == provider_message_id)
        .order_by(OutboxMessage.id.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().first()


async def _should_ignore_failed_after_success(session: AsyncSession, outbox: OutboxMessage) -> bool:
    if outbox.status in _SUCCESSFUL_DELIVERY_STATUSES:
        return True

    from altegio_bot.workers.outbox_worker import _delivery_retry_chain_has_success

    return await _delivery_retry_chain_has_success(session, _delivery_retry_chain_original_outbox_id(outbox))


async def _cancel_queued_delivery_retry_jobs_for_chain(
    session: AsyncSession,
    original_outbox_id: int,
    reason: str,
) -> int:
    prefix = f"{_DELIVERY_RETRY_DEDUPE_PREFIX}{original_outbox_id}:"
    stmt = (
        update(MessageJob)
        .where(MessageJob.status == "queued")
        .where(MessageJob.dedupe_key.like(prefix + "%"))
        .values(status="canceled", locked_at=None, updated_at=utcnow(), last_error=reason)
    )
    res = await session.execute(stmt)
    return int(getattr(res, "rowcount", 0) or 0)


async def _select_message_job_by_dedupe(session: AsyncSession, dedupe_key: str) -> MessageJob | None:
    stmt = select(MessageJob).where(MessageJob.dedupe_key == dedupe_key).limit(1)
    return (await session.execute(stmt)).scalars().first()


async def _create_delivery_retry_job_idempotent(
    session: AsyncSession,
    *,
    dedupe_key: str,
    **fields: Any,
) -> MessageJob | None:
    existing = await _select_message_job_by_dedupe(session, dedupe_key)
    if existing is not None:
        return existing

    job = MessageJob(dedupe_key=dedupe_key, **fields)
    try:
        async with session.begin_nested():
            session.add(job)
            await session.flush()
    except IntegrityError:
        return await _select_message_job_by_dedupe(session, dedupe_key)
    return job


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


async def _campaign_run_ids_for_outbox_ids(
    session: AsyncSession,
    updated_outbox_ids: list[int],
) -> list[int]:
    if not updated_outbox_ids:
        return []

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


async def _advance_followup_statuses(
    session: AsyncSession,
    outbox_id_to_new_status: dict[int, str],
) -> None:
    fu_status_ids = [oid for oid, st in outbox_id_to_new_status.items() if st in {"delivered", "read"}]
    if not fu_status_ids:
        return

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


async def _handle_delivery_statuses(
    session: AsyncSession,
    event: WhatsAppEvent | None,
    statuses: list[dict[str, Any]],
) -> list[int]:
    updated_outbox_ids: list[int] = []
    outbox_id_to_new_status: dict[int, str] = {}

    for status in statuses:
        kind = status["status"]
        provider_message_id = status["provider_message_id"]
        outbox = await _find_outbox_by_provider_message_id(session, provider_message_id)
        if outbox is None:
            logger.info(
                "status_webhook: no OutboxMessage matched provider_message_id=%s status=%s",
                provider_message_id,
                kind,
            )
            continue

        if kind == "failed":
            if await _should_ignore_failed_after_success(session, outbox):
                _mark_stale_failed_after_success(outbox, status)
                continue
            _mark_outbox_delivery_failed(outbox, status, "whatsapp_delivery_failed")
            updated_outbox_ids.append(int(outbox.id))
            outbox_id_to_new_status[int(outbox.id)] = "failed"
            await _handle_failed_delivery_status(session, event, status, outbox=outbox)
            continue

        if kind not in {"sent", "delivered", "read"}:
            continue

        current_rank = _WA_STATUS_RANK.get(outbox.status, 0)
        if outbox.status == "failed" and kind in _SUCCESSFUL_DELIVERY_STATUSES:
            current_rank = -1
        new_rank = _WA_STATUS_RANK.get(kind, 0)
        if new_rank <= current_rank:
            continue

        if outbox.status == "failed":
            outbox.error = None
            recovered_meta = dict(outbox.meta or {})
            recovered_meta["delivery_failed"] = False
            recovered_meta["delivery_recovered_to"] = kind
            recovered_meta["delivery_recovered_at"] = utcnow().isoformat()
            outbox.meta = recovered_meta

        outbox.status = kind
        meta = dict(outbox.meta or {})
        meta[f"wa_status_{kind}"] = {"timestamp": status.get("timestamp")}
        outbox.meta = meta
        updated_outbox_ids.append(int(outbox.id))
        outbox_id_to_new_status[int(outbox.id)] = kind

        if kind in _SUCCESSFUL_DELIVERY_STATUSES:
            original_outbox_id = _delivery_retry_chain_original_outbox_id(outbox)
            canceled = await _cancel_queued_delivery_retry_jobs_for_chain(
                session,
                original_outbox_id,
                "Canceled: original delivery later succeeded",
            )
            if canceled:
                logger.info(
                    "Canceled queued delivery retries original_outbox_id=%s canceled_count=%s status=%s",
                    original_outbox_id,
                    canceled,
                    kind,
                )

    await _advance_followup_statuses(session, outbox_id_to_new_status)
    return await _campaign_run_ids_for_outbox_ids(session, updated_outbox_ids)


async def _apply_status_updates(
    session: AsyncSession,
    status_updates: list[dict[str, Any]],
) -> list[int]:
    if not status_updates:
        return []

    statuses = [
        {
            "provider_message_id": str(update["wamid"]),
            "status": str(update["status"]).strip().lower(),
            "timestamp": update.get("timestamp"),
            "recipient_id": None,
            "phone_number_id": None,
            "error_code": None,
            "error_title": None,
            "error_details": None,
        }
        for update in status_updates
        if update.get("wamid") and update.get("status")
    ]
    return await _handle_delivery_statuses(session, None, statuses)


async def _handle_failed_delivery_status(
    session: AsyncSession,
    event: WhatsAppEvent | None,
    status: dict[str, Any],
    *,
    outbox: OutboxMessage | None,
) -> None:
    provider_message_id = str(status["provider_message_id"])
    if outbox is None:
        return
    if not settings.outbox_delivery_retry_enabled:
        return

    job_type = outbox.template_code
    if job_type not in DELIVERY_RETRY_JOB_TYPES:
        return
    if not _is_retryable_delivery_failure(status):
        return

    meta = outbox.meta or {}
    original_outbox_id = int(outbox.id)
    attempt_number = 1
    if meta.get("delivery_retry_of_outbox_id") is not None:
        try:
            original_outbox_id = int(meta["delivery_retry_of_outbox_id"])
            attempt_number = int(meta.get("delivery_retry_attempt") or 0) + 1
        except (TypeError, ValueError):
            original_outbox_id = int(outbox.id)
            attempt_number = 1

    dedupe_prefix = f"{_DELIVERY_RETRY_DEDUPE_PREFIX}{original_outbox_id}:"
    rows = (
        await session.execute(
            select(MessageJob.id, MessageJob.dedupe_key).where(MessageJob.dedupe_key.like(dedupe_prefix + "%"))
        )
    ).all()
    existing_job_ids: list[int] = []
    existing_attempts: set[int] = set()
    for job_id, dedupe_key in rows:
        existing_job_ids.append(int(job_id))
        suffix = str(dedupe_key).rsplit(":", 1)[-1]
        try:
            existing_attempts.add(int(suffix))
        except ValueError:
            continue

    if attempt_number in existing_attempts:
        return
    if attempt_number > _DELIVERY_RETRY_MAX_ATTEMPTS or len(existing_attempts) >= _DELIVERY_RETRY_MAX_ATTEMPTS:
        if event is not None:
            event.error = f"Delivery retry limit reached for outbox_id={original_outbox_id}"
        return

    if existing_job_ids:
        delivered = (
            await session.execute(
                select(OutboxMessage.id)
                .where(OutboxMessage.job_id.in_(existing_job_ids))
                .where(OutboxMessage.status.in_(_SUCCESSFUL_DELIVERY_STATUSES))
                .limit(1)
            )
        ).scalar_one_or_none()
        if delivered is not None:
            return

    anchor_outbox = outbox
    if original_outbox_id != int(outbox.id):
        original = await session.get(OutboxMessage, original_outbox_id)
        if original is None:
            skip_meta = dict(outbox.meta or {})
            skip_meta["delivery_retry_skipped"] = True
            skip_meta["delivery_retry_skip_reason"] = "original_outbox_missing"
            skip_meta["delivery_retry_original_outbox_id"] = original_outbox_id
            outbox.meta = skip_meta
            return
        anchor_outbox = original

    record: Record | None = None
    if anchor_outbox.record_id is not None:
        record = await session.get(Record, anchor_outbox.record_id)

    delay = _delivery_retry_delay_seconds(attempt_number)
    next_run_at = utcnow() + timedelta(seconds=delay)

    from altegio_bot.workers.outbox_worker import _ORIGINAL_RUN_AT_KEY, _retry_deadline_at

    job_like = SimpleNamespace(job_type=job_type, run_at=anchor_outbox.scheduled_at, payload={})
    deadline = _retry_deadline_at(job_like, record, original_outbox=anchor_outbox)
    if deadline is not None and next_run_at > deadline:
        return

    payload: dict[str, Any] = {
        "kind": "delivery_failed_retry",
        "delivery_retry_of_outbox_id": original_outbox_id,
        "delivery_retry_of_provider_message_id": provider_message_id,
        "delivery_retry_attempt": attempt_number,
        "delivery_retry_error_code": status.get("error_code"),
        "delivery_retry_error_title": status.get("error_title"),
        "delivery_retry_error_details": _sanitize_delivery_detail(status.get("error_details")),
        "delivery_retry_original_outbox_id": original_outbox_id,
    }
    anchor_scheduled = anchor_outbox.scheduled_at
    if anchor_scheduled is not None:
        if anchor_scheduled.tzinfo is None:
            anchor_scheduled = anchor_scheduled.replace(tzinfo=timezone.utc)
        payload[_ORIGINAL_RUN_AT_KEY] = anchor_scheduled.isoformat()
        payload["delivery_retry_original_scheduled_at"] = anchor_scheduled.isoformat()

    record_starts_at = getattr(record, "starts_at", None) if record is not None else None
    if job_type in ("reminder_24h", "reminder_2h") and record_starts_at is not None:
        if record_starts_at.tzinfo is None:
            record_starts_at = record_starts_at.replace(tzinfo=timezone.utc)
        payload["record_starts_at"] = record_starts_at.isoformat()

    original_job = None
    if anchor_outbox.job_id is not None:
        original_job = await session.get(MessageJob, anchor_outbox.job_id)
    max_attempts = int(getattr(original_job, "max_attempts", 5) or 5)

    dedupe_key = f"{_DELIVERY_RETRY_DEDUPE_PREFIX}{original_outbox_id}:{attempt_number}"
    created = await _create_delivery_retry_job_idempotent(
        session,
        dedupe_key=dedupe_key,
        company_id=anchor_outbox.company_id,
        record_id=anchor_outbox.record_id,
        client_id=anchor_outbox.client_id,
        job_type=job_type,
        status="queued",
        run_at=next_run_at,
        attempts=0,
        max_attempts=max_attempts,
        payload=payload,
    )
    logger.warning(
        "Scheduled delivery retry original_outbox_id=%s attempt=%s delay_seconds=%s dedupe_key=%s created=%s",
        original_outbox_id,
        attempt_number,
        delay,
        dedupe_key,
        created is not None,
    )


@dataclass(frozen=True)
class ReplyContextTarget:
    """Resolved reply target for an inbound WhatsApp reply.

    ``chatwoot_message_id`` / ``chatwoot_conversation_id`` point at the prior
    message the client replied to; ``body`` is its display text, used for the
    visible fallback quote when no native id is usable.  ``kind`` records which
    prior message matched:

    - ``"operator"`` — a relayed human-operator message; may carry a native
      ``chatwoot_message_id`` (native ``in_reply_to`` candidate).
    - ``"bot_outbox_message"`` — an automation/campaign send.  In practice these
      rows have no ``chatwoot_message_id`` (it is only populated for operator
      relays), so they render as a visible fallback quote of ``body``.
    """

    chatwoot_message_id: int | None
    chatwoot_conversation_id: int | None
    body: str | None
    kind: str = "operator"


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
    """Resolve a replied-to wamid to the prior OutboxMessage the client answered.

    Scoped to ``phone_e164`` as defense-in-depth so a malformed/spoofed
    ``context.id`` can never resolve to another client's message.  Two-step
    lookup, operator first (operator always wins when both match the same wamid):

    1. operator-relay row (``message_source='operator'``) — may carry a native
       ``chatwoot_message_id``; returned with ``kind='operator'``.
    2. bot/automation row (``message_source != 'operator'``) — typically has no
       native id, so it drives the visible fallback quote from ``body``;
       returned with ``kind='bot_outbox_message'``.

    Returns ``None`` on a miss.
    """
    if not provider_message_id or not phone_e164:
        return None

    operator_stmt = (
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
    row = (await session.execute(operator_stmt)).first()
    if row is not None:
        return ReplyContextTarget(
            chatwoot_message_id=row[0],
            chatwoot_conversation_id=row[1],
            body=row[2],
            kind="operator",
        )

    # PR2: a reply to a bot/automation message. These rows have no native
    # chatwoot_message_id, so the caller renders a visible fallback quote of body.
    bot_stmt = (
        select(
            OutboxMessage.chatwoot_message_id,
            OutboxMessage.chatwoot_conversation_id,
            OutboxMessage.body,
        )
        .where(OutboxMessage.provider_message_id == provider_message_id)
        .where(OutboxMessage.phone_e164 == phone_e164)
        .where(OutboxMessage.message_source != "operator")
        .order_by(OutboxMessage.created_at.desc(), OutboxMessage.id.desc())
        .limit(1)
    )
    row = (await session.execute(bot_stmt)).first()
    if row is not None:
        return ReplyContextTarget(
            chatwoot_message_id=row[0],
            chatwoot_conversation_id=row[1],
            body=row[2],
            kind="bot_outbox_message",
        )

    return None


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
    ``in_reply_to`` is attached only when the replied-to prior message has a
    Chatwoot message id in that same conversation. Prior bot/automation
    OutboxMessage rows usually do not have a native Chatwoot id, so they fall
    back to a visible quote prefix. Records the destination in
    ``forwarded_chatwoot_conversation_id`` — never in
    ``chatwoot_conversation_id``, which stays a Chatwoot-origin source marker.
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

            # Low-noise observability for reply-context resolution. Safe technical
            # fields only — never body/content/tokens/URLs/payload.
            if target is not None:
                logger.debug(
                    "reply_context: resolved target_found=True target_kind=%s has_native_id=%s "
                    "conversation_matches=%s native_reply=%s destination_conversation_id=%s "
                    "target_conversation_id=%s",
                    target.kind,
                    target.chatwoot_message_id is not None,
                    target.chatwoot_conversation_id == conversation_id,
                    native_ok,
                    conversation_id,
                    target.chatwoot_conversation_id,
                )
            else:
                logger.debug(
                    "reply_context: target not found native_reply=False destination_conversation_id=%s",
                    conversation_id,
                )

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


@dataclass(frozen=True)
class ReactionTarget:
    """The message an inbound WhatsApp reaction points at.

    ``kind`` decides how the reaction is rendered in Chatwoot:
    ``chatwoot_agent_message`` and ``inbound_whatsapp_event`` carry a real
    Chatwoot message id (native reply candidate); ``outbox_message`` is an
    automatic bot/outbox send without a Chatwoot message id (visible fallback);
    ``unknown`` is a safe fallback when nothing matched.
    """

    kind: str
    provider_message_id: str | None = None
    chatwoot_conversation_id: int | None = None
    chatwoot_message_id: int | None = None
    outbox_id: int | None = None
    outbox_template_code: str | None = None
    outbox_record_id: int | None = None
    body_preview: str | None = None


async def _resolve_reaction_target(
    session: AsyncSession,
    reaction_target_provider_message_id: str | None,
    *,
    phone_e164: str | None,
) -> ReactionTarget:
    """Resolve the reacted-to wamid to its stored message, phone-scoped.

    Resolution order (altegio_bot has no separate agent-message table — operator
    replies are OutboxMessage rows carrying a Chatwoot message id):

    1. Operator OutboxMessage (``message_source='operator'``) with a real
       ``chatwoot_message_id`` (agent message, native reply candidate) matched by
       ``provider_message_id`` AND phone.  Bot/automatic rows are excluded here
       so they can never become a native ``chatwoot_agent_message`` target.
    2. Prior Meta-origin inbound WhatsAppEvent forwarded to Chatwoot, matched by
       ``whatsapp_message_id`` AND payload sender phone.
    3. Automatic OutboxMessage (any source) matched by ``provider_message_id``
       AND phone — visible fallback only, no native reply.
    4. Unknown fallback.

    OutboxMessage.provider_message_id is indexed but not unique, so the lookup is
    always phone-scoped; without a phone we never fall back to an unsafe
    provider_message_id-only OutboxMessage match.
    """
    if not reaction_target_provider_message_id:
        return ReactionTarget(kind="unknown")

    variants = _phone_variants(phone_e164) if phone_e164 else None

    # 1. Operator/agent OutboxMessage that carries a real Chatwoot message id.
    if variants:
        agent_stmt = (
            select(
                OutboxMessage.id,
                OutboxMessage.chatwoot_message_id,
                OutboxMessage.chatwoot_conversation_id,
                OutboxMessage.template_code,
                OutboxMessage.record_id,
                OutboxMessage.body,
            )
            .where(OutboxMessage.provider_message_id == reaction_target_provider_message_id)
            .where(OutboxMessage.phone_e164.in_(variants))
            .where(OutboxMessage.message_source == "operator")
            .where(OutboxMessage.chatwoot_message_id.is_not(None))
            .where(OutboxMessage.chatwoot_conversation_id.is_not(None))
            .order_by(OutboxMessage.created_at.desc(), OutboxMessage.id.desc())
            .limit(1)
        )
        row = (await session.execute(agent_stmt)).first()
        if row is not None:
            return ReactionTarget(
                kind="chatwoot_agent_message",
                provider_message_id=reaction_target_provider_message_id,
                chatwoot_message_id=row[1],
                chatwoot_conversation_id=row[2],
                outbox_id=row[0],
                outbox_template_code=row[3],
                outbox_record_id=row[4],
                body_preview=row[5],
            )

    # 2. Prior Meta-origin inbound WhatsAppEvent that was forwarded to Chatwoot.
    #    The dedupe_key filter keeps this to real Meta inbound events (consistent
    #    with the reply-context lookup) and never matches Chatwoot-origin events.
    event_stmt = (
        select(WhatsAppEvent)
        .where(WhatsAppEvent.whatsapp_message_id == reaction_target_provider_message_id)
        .where(WhatsAppEvent.chatwoot_message_id.is_not(None))
        .where(WhatsAppEvent.forwarded_chatwoot_conversation_id.is_not(None))
        .where(WhatsAppEvent.dedupe_key.like("wa:%"))
        .order_by(WhatsAppEvent.received_at.desc(), WhatsAppEvent.id.desc())
        .limit(20)
    )
    for candidate in (await session.execute(event_stmt)).scalars().all():
        cand_payload = candidate.payload or {}
        if not isinstance(cand_payload, dict):
            continue
        if not phone_e164:
            continue
        if not _payload_message_from_matches_phone(cand_payload, phone_e164):
            continue
        return ReactionTarget(
            kind="inbound_whatsapp_event",
            provider_message_id=reaction_target_provider_message_id,
            chatwoot_message_id=candidate.chatwoot_message_id,
            chatwoot_conversation_id=candidate.forwarded_chatwoot_conversation_id,
        )

    # 3. Automatic OutboxMessage without a Chatwoot message id (fallback only).
    if variants:
        outbox_stmt = (
            select(
                OutboxMessage.id,
                OutboxMessage.template_code,
                OutboxMessage.record_id,
                OutboxMessage.body,
            )
            .where(OutboxMessage.provider_message_id == reaction_target_provider_message_id)
            .where(OutboxMessage.phone_e164.in_(variants))
            .order_by(OutboxMessage.created_at.desc(), OutboxMessage.id.desc())
            .limit(1)
        )
        row = (await session.execute(outbox_stmt)).first()
        if row is not None:
            return ReactionTarget(
                kind="outbox_message",
                provider_message_id=reaction_target_provider_message_id,
                outbox_id=row[0],
                outbox_template_code=row[1],
                outbox_record_id=row[2],
                body_preview=row[3],
            )

    # 4. Unknown fallback.
    return ReactionTarget(
        kind="unknown",
        provider_message_id=reaction_target_provider_message_id,
    )


def _reaction_display_text(emoji: str | None, target: ReactionTarget, *, native_ok: bool) -> str:
    """Visible Chatwoot text for an inbound reaction.

    Native reply targets show the bare emoji (Chatwoot attaches it to the
    original message); other targets show a descriptive line so the operator
    sees the reaction even without native rendering.
    """
    if not emoji:
        return "Реакция удалена в WhatsApp"
    if native_ok:
        return emoji
    if target.kind == "outbox_message":
        if target.outbox_template_code:
            return f"{emoji} Реакция на отправленное сообщение WhatsApp ({target.outbox_template_code})"
        return f"{emoji} Реакция на отправленное сообщение WhatsApp"
    return f"{emoji} Реакция на сообщение в WhatsApp"


def _reaction_content_attributes(
    *,
    target: ReactionTarget,
    reaction_emoji: str | None,
    reaction_target_provider_message_id: str | None,
    whatsapp_message_id: str | None,
    destination_conversation_id: int,
) -> dict[str, Any]:
    """Build safe Chatwoot content_attributes for an inbound reaction.

    Native ``in_reply_to`` is set only when the target carries a real Chatwoot
    message id that lives in the destination conversation; a cross-conversation
    target is flagged instead.  No PII / tokens / raw webhook are stored.
    """
    attrs: dict[str, Any] = {
        "whatsapp_event_type": "reaction",
        "whatsapp_reaction_emoji": reaction_emoji,
        "whatsapp_reaction_target_provider_message_id": reaction_target_provider_message_id,
        "whatsapp_reaction_message_id": whatsapp_message_id,
        "whatsapp_reaction_target_kind": target.kind,
    }

    if target.chatwoot_message_id is not None:
        if target.chatwoot_conversation_id == destination_conversation_id:
            attrs["in_reply_to"] = target.chatwoot_message_id
            attrs["in_reply_to_external_id"] = reaction_target_provider_message_id
        else:
            attrs["whatsapp_reaction_target_conversation_mismatch"] = True

    if target.kind == "outbox_message":
        attrs["whatsapp_reaction_target_outbox_id"] = target.outbox_id
        attrs["whatsapp_reaction_target_template_code"] = target.outbox_template_code
        attrs["whatsapp_reaction_target_record_id"] = target.outbox_record_id

    return attrs


async def _forward_reaction_to_chatwoot(
    session: AsyncSession,
    event: WhatsAppEvent,
    *,
    phone_e164: str,
    reaction_emoji: str | None,
    reaction_target_provider_message_id: str | None,
    whatsapp_message_id: str | None,
) -> int | None:
    """Mirror an inbound WhatsApp reaction into Chatwoot as an incoming message.

    Inbound-only: nothing is ever sent back to WhatsApp.  Native reply is used
    only when the reacted-to message has a Chatwoot message id in the same
    conversation; otherwise a visible fallback message is posted.
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

        target = await _resolve_reaction_target(
            session,
            reaction_target_provider_message_id,
            phone_e164=phone_e164,
        )
        native_ok = target.chatwoot_message_id is not None and target.chatwoot_conversation_id == conversation_id
        content = _reaction_display_text(reaction_emoji, target, native_ok=native_ok)
        content_attributes = _reaction_content_attributes(
            target=target,
            reaction_emoji=reaction_emoji,
            reaction_target_provider_message_id=reaction_target_provider_message_id,
            whatsapp_message_id=whatsapp_message_id,
            destination_conversation_id=conversation_id,
        )

        message_id = await cw.send_message(
            conversation_id,
            content,
            message_type="incoming",
            content_attributes=content_attributes,
        )
    except Exception as exc:
        # Keep the persisted error free of URLs/tokens/response bodies.
        safe_error = f"Incoming reaction forwarding failed: {type(exc).__name__}"
        event.error = safe_error
        logger.warning(
            "chatwoot: reaction forward failed phone=%s %s",
            phone_e164,
            type(exc).__name__,
        )
        raise RuntimeError(safe_error) from None
    finally:
        await cw.aclose()

    event.forwarded_chatwoot_conversation_id = conversation_id
    event.chatwoot_message_id = message_id
    # event.whatsapp_message_id is already stamped by the inbound action audit
    # path in handle_event (the reaction wamid), so it is not re-set here.
    event.error = None
    logger.info(
        "Forwarded WhatsApp reaction to Chatwoot phone=%s conversation_id=%s message_id=%s "
        "target_kind=%s native_reply=%s",
        phone_e164,
        conversation_id,
        message_id,
        target.kind,
        native_ok,
    )
    return message_id


# Safe placeholder bodies for canceled operator-relay audit rows. The operator's
# original message text is intentionally never stored on these audit rows.
_OPERATOR_RELAY_CIRCUIT_CLOSED_BODY = "[operator relay canceled: Meta circuit closed]"
_OPERATOR_RELAY_TRANSIENT_BODY = "[operator relay canceled: Meta transient send error]"


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

    async def _add_canceled_operator_relay_outbox(
        *,
        attempted_send_type: str,
        cancel_reason: str,
        body: str,
        error: str,
        circuit_action: str,
        error_kind: str | None = None,
        error_code: str | None = None,
        extra_meta: dict[str, Any] | None = None,
    ) -> OutboxMessage:
        """Build, persist and return a canceled operator-relay audit row.

        Single constructor for both Meta-circuit cancel paths so the safe
        placeholder body and the row skeleton can never diverge. The
        helper-controlled ``body``, ``error`` and base circuit ``meta`` do not
        include the operator message text, raw Meta response, tokens, or
        template params. ``extra_meta`` is caller-controlled and may include
        legacy relay audit fields (e.g. ``agent_name``, reply-context audit) for
        compatibility, so callers must keep it intentionally scoped and must not
        put raw Meta body, tokens, operator message text, or template params
        into it.
        """
        meta: dict[str, Any] = {
            "send_type": "none",
            "attempted_send_type": attempted_send_type,
            "cancel_reason": cancel_reason,
            "circuit_action": circuit_action,
            "circuit_state": "closed",
            "event_id": getattr(event, "id", None),
            "provider": type(meta_provider).__name__,
            "phone_number_id": phone_number_id,
        }
        if error_kind is not None:
            meta["error_kind"] = error_kind
        if error_code is not None:
            meta["error_code"] = error_code
        if extra_meta:
            meta.update(extra_meta)

        outbox = OutboxMessage(
            company_id=company_id,
            client_id=None,
            record_id=None,
            job_id=None,
            sender_id=sender_id,
            phone_e164=phone_e164,
            template_code="operator_relay",
            language="de",
            body=body,
            status="canceled",
            provider_message_id=None,
            scheduled_at=utcnow(),
            sent_at=None,
            message_source="operator",
            chatwoot_conversation_id=cw_conversation_id,
            chatwoot_message_id=cw_message_id,
            error=error,
            meta=meta,
        )
        session.add(outbox)
        await session.flush()
        return outbox

    async def _pause_for_meta_circuit(attempted_send_type: str, template_name: str | None = None) -> bool:
        if not settings.meta_circuit_breaker_enabled:
            return False
        if not await meta_circuit.should_pause_meta_sends(session=session):
            return False

        extra_meta: dict[str, Any] = {
            "wa_window_open": window_open,
            "last_meta_inbound_at": last_inbound_iso,
            "closed_window_mode": mode,
            "chatwoot_conversation_id": conversation_id,
            "chatwoot_message_id": chatwoot_message_id,
            "agent_name": agent_name,
            **reply_context_audit,
        }
        if template_name:
            extra_meta["template"] = template_name

        outbox = await _add_canceled_operator_relay_outbox(
            attempted_send_type=attempted_send_type,
            cancel_reason="meta_circuit_closed",
            body=_OPERATOR_RELAY_CIRCUIT_CLOSED_BODY,
            error="Meta circuit closed: operator relay paused",
            circuit_action="already_closed",
            extra_meta=extra_meta,
        )
        event.error = "operator_relay: Meta circuit closed"
        logger.warning(
            "operator_relay: Meta circuit closed; paused conv_id=%s msg_id=%s outbox_id=%s company_id=%s",
            conversation_id,
            chatwoot_message_id,
            outbox.id,
            company_id,
        )

        await _send_circuit_pause_note(outbox)
        return True

    async def _send_circuit_pause_note(outbox: OutboxMessage) -> None:
        """Add the operator-facing 'Meta unavailable' Chatwoot private note.

        Static, PII-free text; records private_note_status on the outbox meta.
        Shared by the pre-send pause path and the transient-error close path.
        """
        if settings.chatwoot_operator_reopen_private_note_enabled and conversation_id:
            private_note = (
                "Meta/WhatsApp ist voruebergehend nicht erreichbar. "
                "Die Operator-Nachricht wurde nicht an WhatsApp gesendet."
            )
            cw = ChatwootClient()
            try:
                await cw.send_message(
                    conversation_id,
                    private_note,
                    message_type="outgoing",
                    private=True,
                )
                outbox.meta = {**outbox.meta, "private_note_status": "sent"}
            except Exception as exc:
                logger.warning(
                    "operator_relay: circuit pause note failed conv_id=%s err=%s",
                    conversation_id,
                    exc,
                )
                outbox.meta = {**outbox.meta, "private_note_status": "failed"}
            finally:
                await cw.aclose()
        else:
            outbox.meta = {**outbox.meta, "private_note_status": "disabled"}

    async def _close_circuit_on_transient_send_error(attempted_send_type: str, err: str) -> bool:
        """Close the Meta circuit when an operator-relay send fails transiently.

        Returns True when the error was transient and fully handled here: the
        global circuit is closed, a canceled audit OutboxMessage is written, and
        the operator is notified. Returns False for permanent/token-expired
        errors so the caller keeps its existing permanent-failure handling.

        Makes no additional Meta calls and never triggers template fallback.
        Audit fields and logs are restricted to non-PII data (event/outbox ids,
        company, provider class, phone_number_id, circuit state, error kind/code).
        """
        if not settings.meta_circuit_breaker_enabled:
            return False
        if not is_transient_provider_error(err):
            return False

        error_kind, error_code = transient_error_reason(err)
        await meta_circuit.close_meta_circuit(
            reason="operator_relay_transient_send_error",
            error_kind=error_kind,
            error_code=error_code,
            next_probe_at=utcnow() + timedelta(seconds=settings.meta_circuit_probe_initial_delay_seconds),
        )

        outbox = await _add_canceled_operator_relay_outbox(
            attempted_send_type=attempted_send_type,
            cancel_reason="meta_transient_send_error",
            body=_OPERATOR_RELAY_TRANSIENT_BODY,
            error="Meta transient error: operator relay paused and circuit closed",
            circuit_action="closed",
            error_kind=error_kind,
            error_code=error_code,
        )
        event.error = "operator_relay: Meta transient error, circuit closed"
        logger.warning(
            "operator_relay: transient Meta error closed circuit; canceled "
            "conv_id=%s msg_id=%s outbox_id=%s company_id=%s provider=%s "
            "phone_number_id=%s error_kind=%s error_code=%s",
            conversation_id,
            chatwoot_message_id,
            outbox.id,
            company_id,
            type(meta_provider).__name__,
            phone_number_id,
            error_kind,
            error_code,
        )
        await _send_circuit_pause_note(outbox)
        return True

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

        if await _pause_for_meta_circuit("text"):
            return

        wamid, err = await safe_send(
            provider=meta_provider,
            sender_id=sender_id,
            phone=phone_e164,
            text=text,
            company_id=company_id,
            reply_to_provider_message_id=reply_to_provider_message_id,
        )

        if err is not None:
            if await _close_circuit_on_transient_send_error("text", err):
                return
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

    if await _pause_for_meta_circuit("template", template_name=template_name):
        return

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
        if await _close_circuit_on_transient_send_error("template", err):
            return
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

    chatwoot_origin = _is_chatwoot_origin(event, payload)

    # ------------------------------------------------------------------ #
    # 1. Delivery status webhooks (value.statuses)                        #
    # ------------------------------------------------------------------ #
    statuses = [] if chatwoot_origin else _extract_statuses(payload)
    if statuses:
        logger.info(
            "status_webhook: received %d status update(s) event_id=%s",
            len(statuses),
            event.id,
        )
        run_ids = await _handle_delivery_statuses(session, event, statuses)
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

    # Audit: keep the inbound wamid of real Meta-origin messages.  Chatwoot
    # mirrors carry a synthetic Chatwoot message id there, so they are skipped.
    if not chatwoot_origin and action.get("whatsapp_message_id"):
        event.whatsapp_message_id = action["whatsapp_message_id"]

    # Inbound reaction: mirror to Chatwoot and stop.  Must run before any
    # text/command/opt-out/promo/LLM logic so an emoji never triggers them, and
    # never sends anything back to WhatsApp.  Status webhooks above already ran.
    if action.get("kind") == "reaction":
        if chatwoot_origin:
            logger.info(
                "Skipping Chatwoot mirror for chatwoot-origin reaction event id=%s dedupe_key=%s",
                event.id,
                event.dedupe_key,
            )
            event.error = None
            return
        await _forward_reaction_to_chatwoot(
            session,
            event,
            phone_e164=phone_e164,
            reaction_emoji=action.get("reaction_emoji"),
            reaction_target_provider_message_id=action.get("reaction_target_provider_message_id"),
            whatsapp_message_id=action.get("whatsapp_message_id"),
        )
        return

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
