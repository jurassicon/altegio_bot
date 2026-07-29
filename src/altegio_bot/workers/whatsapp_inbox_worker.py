from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Sequence

from sqlalchemy import exists, func, or_, select, update
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
    is_deterministic_meta_rejection,
    is_transient_provider_error,
    transient_error_reason,
)
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import (
    list_or_empty,
    mapping_or_empty,
    nonempty_str,
    normalize_phone_candidate,
    optional_chatwoot_id,
    parse_chatwoot_inbox_company_map,
    positive_int,
    safe_log_value,
)
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


def _norm_phone(raw: object) -> str | None:
    """Type-safe phone normalisation shared with ingress and window logic.

    ``raw`` is a sender-controlled leaf (``messages[].from``) and may be any JSON
    type; a non-string degrades to ``None`` instead of reaching ``re.sub``.
    """
    return normalize_phone_candidate(raw)


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


def _norm_text(raw: str | None) -> str:
    if not raw:
        return ""

    text = raw.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" \t\n\r.,!?:;\"'()[]{}")
    return text


def _extract_message_text(msg: dict[str, Any]) -> str:
    # Every nested field is sender-controlled: `msg.get("text") or {}` does not
    # protect against `"text": []`/`"text": "bad"` (truthy non-dict → .get()
    # raises). mapping_or_empty degrades any non-dict to "" instead of a crash.
    msg_type = msg.get("type")

    if msg_type == "text":
        text = mapping_or_empty(msg.get("text"))
        return str(text.get("body") or "")

    if msg_type == "button":
        btn = mapping_or_empty(msg.get("button"))
        return str(btn.get("text") or btn.get("payload") or "")

    if msg_type == "interactive":
        inter = mapping_or_empty(msg.get("interactive"))
        btn_reply = mapping_or_empty(inter.get("button_reply"))
        list_reply = mapping_or_empty(inter.get("list_reply"))
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


_OPERATOR_RELAY_MARKER_KEY = "_chatwoot_operator_relay"


def _is_operator_relay(payload: dict[str, Any]) -> bool:
    """Return True if this event is an operator relay from Chatwoot.

    Operator relay events carry _chatwoot_operator_relay in the payload,
    written by the Chatwoot webhook handler.  They must be sent to Meta,
    NOT forwarded back to Chatwoot (that would duplicate the message the
    operator already sees in their own Chatwoot UI).

    Membership alone is not enough: a spoofed ``{"_chatwoot_operator_relay": []}``
    would route here and then crash on ``relay.get(...)``. Require the marker to
    be a dict so a malformed one is treated as a non-relay event, not relayed.
    """
    return isinstance(payload.get(_OPERATOR_RELAY_MARKER_KEY), dict)


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
    phone_number_id: object,
) -> tuple[int | None, int | None]:
    # Only a non-empty string may reach the String-column lookup; a dict/list/
    # bool/number would make the driver raise on parameter binding. Malformed →
    # "no sender", so the action is safely ignored (no send).
    phone_number_id = nonempty_str(phone_number_id)
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
    chatwoot_inbox_id: object,
) -> tuple[int | None, str | None]:
    """Resolve company_id hint from Chatwoot inbox_id via the validated map.

    Returns (company_id, error) with STABLE, non-sensitive error codes. The
    inbox-company map is parsed/validated once by
    :func:`parse_chatwoot_inbox_company_map`; this function never touches a raw
    ``json.loads`` result.

    Contract:
    - map not configured ("" / "{}") → (None, None): fall through to the legacy
      phone_number_id fallback.
    - map invalid → (None, "invalid_inbox_company_map").
    - map configured (valid): the inbox_id is MANDATORY and there is NO
      phone_number_id fallback (that would route to the wrong tenant):
        * inbox_id absent          → (None, "missing_inbox_id");
        * inbox_id not a positive int → (None, "invalid_inbox_id");
        * inbox_id not in the map  → (None, "inbox_mapping_missing");
        * inbox_id in the map      → (company_id, None).
    """
    parsed = parse_chatwoot_inbox_company_map(settings.chatwoot_inbox_company_map)

    if not parsed.configured:
        return None, None  # not configured → legacy fallback allowed

    if not parsed.valid:
        # Never persist/return raw config or exception text.
        logger.warning("operator_relay: invalid CHATWOOT_INBOX_COMPANY_MAP")
        return None, "operator_relay: invalid_inbox_company_map"

    # Configured map: inbox_id is required and must be a valid positive int. No
    # fallback to phone_number_id — that could route to the wrong company.
    if chatwoot_inbox_id is None:
        return None, "operator_relay: missing_inbox_id"

    inbox_int = positive_int(chatwoot_inbox_id)
    if inbox_int is None:
        # inbox_id is sender-controlled: keep it out of the returned error.
        logger.warning(
            "operator_relay: invalid inbox_id inbox_id=%s",
            safe_log_value(chatwoot_inbox_id, limit=32),
        )
        return None, "operator_relay: invalid_inbox_id"

    if inbox_int not in parsed.mapping:
        logger.warning(
            "operator_relay: inbox not in company map inbox_id=%s — fail-closed",
            safe_log_value(chatwoot_inbox_id, limit=32),
        )
        return None, "operator_relay: inbox_mapping_missing"

    return parsed.mapping[inbox_int], None


async def _resolve_relay_sender(
    session: AsyncSession,
    phone_number_id: object,
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
    # A non-string id (dict/list/…) is treated as missing, not bound into SQL.
    # Returned errors are STABLE reason codes: the id is sender-controlled and
    # must never end up in event.error or a raw log line (injection/PII). The raw
    # id is only ever logged separately, escaped, via safe_log_value.
    safe_pnid = nonempty_str(phone_number_id)
    if not safe_pnid:
        return None, None, "operator_relay: missing_phone_number_id"

    stmt = (
        select(WhatsAppSender)
        .where(WhatsAppSender.phone_number_id == safe_pnid)
        .where(WhatsAppSender.is_active.is_(True))
    )
    res = await session.execute(stmt)
    senders = list(res.scalars().all())

    if not senders:
        logger.warning(
            "operator_relay: no active sender phone_number_id=%s",
            safe_log_value(safe_pnid, limit=32),
        )
        return None, None, "operator_relay: sender_not_found"

    # ── Hint path: inbox mapping resolved a specific company ───────────
    if company_id_hint is not None:
        hinted = [s for s in senders if s.company_id == company_id_hint]
        if not hinted:
            logger.warning(
                "operator_relay: no active sender in hinted company phone_number_id=%s company_id=%s",
                safe_log_value(safe_pnid, limit=32),
                company_id_hint,
            )
            return None, None, "operator_relay: sender_not_found_for_company"
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
        # company_ids come from the DB (not sender-controlled) → safe to log.
        cids = ",".join(sorted(str(c) for c in distinct_companies))
        logger.warning(
            "operator_relay: ambiguous sender routing phone_number_id=%s matched %d senders company_ids=%s — blocking",
            safe_log_value(safe_pnid, limit=32),
            len(senders),
            cids,
        )
        return None, None, "operator_relay: ambiguous_sender"

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
    expected = normalize_phone_candidate(phone_e164)
    if expected is None:
        return False

    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue
            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue
            # list_or_empty (not `or []`): a historical/replay payload with a
            # truthy non-list messages (e.g. 123) must not crash this secondary
            # scan, and a valid entry later in the list must still be reached.
            for msg in list_or_empty(value.get("messages")):
                if not isinstance(msg, dict):
                    continue
                if normalize_phone_candidate(msg.get("from")) == expected:
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

    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue

        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue

            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue

            metadata = value.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}

            # Normalise to str|None at the boundary: the action dict flows into
            # SQL lookups, logs and stored meta, none of which may see a
            # dict/list/number here.
            phone_number_id = nonempty_str(metadata.get("phone_number_id"))

            for msg in list_or_empty(value.get("messages")):
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

    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue
            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue
            metadata = value.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}
            # Normalise to str|None at the boundary: the action dict flows into
            # SQL lookups, logs and stored meta, none of which may see a
            # dict/list/number here.
            phone_number_id = nonempty_str(metadata.get("phone_number_id"))

            for st in list_or_empty(value.get("statuses")):
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

    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue

        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue

            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue

            for st in list_or_empty(value.get("statuses")):
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


# Visible fallback quotes show a short, single-line preview of the replied-to
# message (like native messengers), so a long bot/automation body does not flood
# the Chatwoot bubble.
_REPLY_CONTEXT_QUOTE_PREVIEW_MAX_CHARS = 100


def _shorten_reply_context_quote(body: str, max_chars: int = _REPLY_CONTEXT_QUOTE_PREVIEW_MAX_CHARS) -> str:
    """Collapse whitespace and truncate a quoted body to a short preview.

    Trims, collapses any internal whitespace/newlines to single spaces, and
    truncates to ``max_chars`` with a trailing ``…`` only when truncation happens.
    """
    preview = re.sub(r"\s+", " ", body.strip())
    if len(preview) <= max_chars:
        return preview
    return preview[:max_chars].rstrip() + "…"


def _format_reply_context_prefix(quoted_body: str | None) -> str:
    """Build the visible quote prefix shown above a WhatsApp reply in Chatwoot.

    Used for every inbound WhatsApp reply that carries context, regardless of
    whether native ``content_attributes`` are also sent through the Chatwoot API,
    so the operator always sees the replied-to message in the body. When
    ``quoted_body`` is missing it returns a generic reply marker; otherwise the
    body is rendered as a short single-line preview. This helper only formats
    visible body text; it does not decide native-vs-API metadata.
    """
    if not quoted_body:
        return "↩️ Ответ на сообщение в WhatsApp"
    if quoted_body == "[image]":
        return "↩️ Ответ на изображение"
    return f"↩️ Ответ на сообщение:\n«{_shorten_reply_context_quote(quoted_body)}»"


async def _forward_text_to_chatwoot(
    session: AsyncSession,
    event: WhatsAppEvent,
    *,
    phone_e164: str,
    text: str,
    reply_to_provider_message_id: str | None = None,
) -> None:
    """Forward an inbound Meta-origin text to Chatwoot with reply context.

    When a WhatsApp reply carries context, the worker tries to resolve the
    replied-to message. For same-conversation targets with a Chatwoot message id,
    native ``in_reply_to`` metadata is sent through the Chatwoot REST API. In the
    default ``fallback_only`` mode the message body stays clean and Chatwoot
    renders the native reply preview.

    Visible body quotes are added only when native metadata is unavailable/unsafe
    (bot or automation target without a native Chatwoot id, cross-conversation
    target, missing target), or when ``CHATWOOT_REPLY_CONTEXT_VISIBLE_QUOTE_MODE``
    is set to ``always``. Visible quotes use a shortened single-line preview.

    ``altegio_bot`` never writes to Chatwoot's database. Records the destination
    in ``forwarded_chatwoot_conversation_id`` — never in
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
                # Best-effort native metadata through the API only; never relied
                # upon for visibility and never written to Chatwoot's database.
                content_attributes = {
                    "in_reply_to": target.chatwoot_message_id,
                    "in_reply_to_external_id": reply_to_provider_message_id,
                }
            elif (
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

            # Add a visible body quote for every reply with context EXCEPT a native
            # same-conversation reply in the default fallback_only mode: there
            # Chatwoot renders its own native reply preview, so a body quote would
            # duplicate it. Mode "always" keeps the quote unconditionally. Uses the
            # target body when known, otherwise a generic reply marker.
            add_visible_quote = not native_ok or settings.chatwoot_reply_context_visible_quote_mode == "always"
            if add_visible_quote:
                quoted_body = target.body if target is not None else None
                content = f"{_format_reply_context_prefix(quoted_body)}\n\n{text}"

            # Low-noise observability for reply-context resolution. Safe technical
            # fields only — never body/content/tokens/URLs/payload.
            if target is not None:
                logger.debug(
                    "reply_context: resolved target_found=True target_kind=%s has_native_id=%s "
                    "conversation_matches=%s native_reply=%s visible_quote=%s "
                    "destination_conversation_id=%s target_conversation_id=%s",
                    target.kind,
                    target.chatwoot_message_id is not None,
                    target.chatwoot_conversation_id == conversation_id,
                    native_ok,
                    add_visible_quote,
                    conversation_id,
                    target.chatwoot_conversation_id,
                )
            else:
                logger.debug(
                    "reply_context: target not found native_reply=False visible_quote=%s "
                    "destination_conversation_id=%s",
                    add_visible_quote,
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
            "chatwoot: forward failed event_id=%s %s",
            event.id,
            type(exc).__name__,
        )
        raise RuntimeError(safe_error) from None
    finally:
        await cw.aclose()

    event.forwarded_chatwoot_conversation_id = conversation_id
    event.chatwoot_message_id = message_id
    event.error = None
    # No phone / client name / message body: see docs/easyweek/capture_runbook.md
    # §4.1 — tracing goes through these technical ids, details come from the DB.
    logger.info(
        "Forwarded incoming message to Chatwoot event_id=%s conversation_id=%s message_id=%s native_reply=%s",
        event.id,
        safe_log_value(conversation_id, limit=32),
        safe_log_value(message_id, limit=32),
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
            "chatwoot: reaction forward failed event_id=%s %s",
            event.id,
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
        "Forwarded WhatsApp reaction to Chatwoot event_id=%s conversation_id=%s message_id=%s "
        "target_kind=%s native_reply=%s",
        event.id,
        safe_log_value(conversation_id, limit=32),
        safe_log_value(message_id, limit=32),
        target.kind,
        native_ok,
    )
    return message_id


# Safe placeholder bodies for canceled operator-relay audit rows. The operator's
# original message text is intentionally never stored on these audit rows.
_OPERATOR_RELAY_CIRCUIT_CLOSED_BODY = "[operator relay canceled: Meta circuit closed]"
_OPERATOR_RELAY_TRANSIENT_BODY = "[operator relay canceled: Meta transient send error]"

# Canonical operator note for EVERY indeterminate outcome (immediate unknown,
# stale-sending recovery, crash recovery). It must never claim the message was
# not sent — an unknown outcome means Meta may have accepted it.
_OPERATOR_RELAY_UNKNOWN_NOTE = (
    "Die Zustellung dieser WhatsApp-Nachricht konnte technisch nicht eindeutig "
    "bestätigt werden. Bitte prüfe den Chatverlauf, bevor du die Nachricht erneut sendest."
)

# Deterministic rejection: here the app HAS a confirmed rejection from Meta, so —
# unlike the unknown note — it may state plainly that the customer did not get it.
_OPERATOR_RELAY_TEXT_FAILED_NOTE = (
    "❌ Die WhatsApp-Nachricht konnte nicht gesendet werden.\n"
    "Bitte prüfe die Kundendaten und kontaktiere den Kunden bei Bedarf auf einem anderen Weg."
)
_OPERATOR_RELAY_TEMPLATE_FAILED_NOTE = (
    "❌ Die Vorlage zum Wiederöffnen des WhatsApp-Dialogs konnte nicht gesendet"
    " werden. Die ursprüngliche Nachricht wurde nicht an WhatsApp zugestellt."
)
_OPERATOR_RELAY_CIRCUIT_PAUSED_NOTE = (
    "Meta/WhatsApp ist voruebergehend nicht erreichbar. Die Operator-Nachricht wurde nicht an WhatsApp gesendet."
)

# Note kinds persisted in Outbox.meta['private_note_kind']. The dispatcher builds
# the final text from the kind plus the row itself, so a note owed before a crash
# can still be reconstructed exactly after restart.
_NOTE_KIND_UNKNOWN = "unknown"
_NOTE_KIND_TEXT_FAILED = "text_failed"
_NOTE_KIND_TEMPLATE_FAILED = "template_failed"
_NOTE_KIND_TEMPLATE_SENT = "template_sent"
_NOTE_KIND_WINDOW_CLOSED = "window_closed"
_NOTE_KIND_CIRCUIT_PAUSED = "circuit_paused"

# Bounded retry for secondary actions. Chatwoot offers no idempotency key, so a
# retry after an unconfirmed send may duplicate a note; a duplicate note is far
# safer than silently losing the manual-review signal, but it must not loop.
_TERMINAL_ACTION_MAX_ATTEMPTS = 5

# Outbox states from which no further automatic send can happen. Only these carry
# pending secondary actions for the terminal-action dispatcher.
_TERMINAL_RELAY_STATUSES = ("sent", "failed", "canceled", "unknown", "delivered", "read")


def _relay_note_text(kind: str, row: OutboxMessage) -> str | None:
    """Rebuild the operator note text from the durable kind + the Outbox row."""
    if kind == _NOTE_KIND_UNKNOWN:
        return _OPERATOR_RELAY_UNKNOWN_NOTE
    if kind == _NOTE_KIND_TEXT_FAILED:
        return _OPERATOR_RELAY_TEXT_FAILED_NOTE
    if kind == _NOTE_KIND_TEMPLATE_FAILED:
        return _OPERATOR_RELAY_TEMPLATE_FAILED_NOTE
    if kind == _NOTE_KIND_CIRCUIT_PAUSED:
        return _OPERATOR_RELAY_CIRCUIT_PAUSED_NOTE
    if kind == _NOTE_KIND_WINDOW_CLOSED:
        return (
            "⚠️ Das 24h-WhatsApp-Fenster ist geschlossen."
            " Die Nachricht wurde nicht an WhatsApp zugestellt.\n"
            "Bitte warte, bis der Kunde erneut schreibt, oder wende dich direkt an ihn.\n\n"
            f'Originalnachricht:\n"{row.body}"'
        )
    if kind == _NOTE_KIND_TEMPLATE_SENT:
        return (
            "⚠️ Das 24h-WhatsApp-Fenster war geschlossen. Die ursprüngliche Nachricht"
            " wurde nicht direkt gesendet. Stattdessen wurde eine Vorlage gesendet, damit"
            " der Kunde den Dialog wieder öffnen kann.\n\n"
            f'Originalnachricht:\n"{row.body}"'
        )
    return None


def _pending_note_marker(kind: str, conversation_id: Any) -> dict[str, Any]:
    """Durable marker for a note owed AFTER the terminal state is committed.

    Written inside the same transaction as the terminal status, so a crash before
    the note is actually delivered still leaves a recoverable record of it.
    """
    if not (settings.chatwoot_operator_reopen_private_note_enabled and conversation_id):
        return {"private_note_kind": kind, "private_note_status": "disabled"}
    return {"private_note_kind": kind, "private_note_status": "pending", "private_note_attempts": 0}


def _pending_circuit_marker(outcome: RelayProviderOutcome) -> dict[str, Any]:
    """Durable marker for a Meta-circuit close owed after an indeterminate send."""
    if not (outcome.circuit_transient and settings.meta_circuit_breaker_enabled):
        return {"circuit_action_status": "not_required"}
    return {
        "circuit_action_status": "pending",
        "circuit_action_attempts": 0,
        "circuit_error_kind": outcome.error_kind,
        "circuit_error_code": outcome.error_code,
    }


# ===========================================================================
# Operator relay: durable, staged send lifecycle
# ---------------------------------------------------------------------------
# The relay is split into short, independently committed transactions so a
# durable send intent always exists on disk BEFORE the first Meta side effect,
# and no DB transaction / row lock is held across the network call:
#
#   Tx A  prepare  — lock event, idempotency check, validate/route, and either
#                    reach a terminal state (validation error / replay / a
#                    canceled audit row) or COMMIT an OutboxMessage(status=queued)
#                    linked to the source event; provider is NOT called yet.
#   Tx B  claim    — atomic conditional UPDATE queued → sending (+ attempt time),
#                    COMMIT. Only the worker whose UPDATE returned the row may
#                    call Meta.
#   --    execute  — safe_send / safe_send_template OUTSIDE any DB transaction,
#                    using only values read from the committed row.
#   Tx C  finalize — reload the row, confirm it is still 'sending', write the
#                    terminal outcome (sent / failed / unknown) and the source
#                    event state, COMMIT.
#
# The partial unique index on OutboxMessage.source_whatsapp_event_id is the last
# line of defence: one event can never spawn two send attempts, even if the
# event-status bookkeeping is imperfect or two workers race.
# ===========================================================================


@dataclass(frozen=True)
class RelayProviderOutcome:
    """Explicit classification of a single provider attempt.

    ``sent``    — Meta confirmed acceptance (a wamid is present).
    ``failed``  — deterministic rejection; the app is confident Meta did NOT
                  accept the message.
    ``unknown`` — the outcome cannot be proven (transient/indeterminate network
                  error, or crash around the call). Never auto-retried.
    """

    kind: str  # "sent" | "failed" | "unknown"
    provider_message_id: str | None = None
    error_kind: str | None = None
    error_code: str | None = None
    circuit_transient: bool = False


@dataclass(frozen=True)
class _PreparedRelay:
    """Result of Stage A.

    ``outbox_id`` set → a queued row to claim + execute (``send_type`` tells the
    execute stage which provider call to make). ``outbox_id`` None → terminal:
    nothing to send (validation error, replay, or a committed canceled audit row).
    ``terminal_outbox_id`` names a committed terminal row whose durable pending
    secondary actions still have to be dispatched.
    """

    outbox_id: int | None = None
    send_type: str | None = None
    terminal_outbox_id: int | None = None


@dataclass(frozen=True)
class _ClaimedRelay:
    """Immutable execution parameters read from the committed 'sending' row."""

    outbox_id: int
    event_id: int
    send_type: str  # "text" | "template"
    sender_id: int
    company_id: int
    phone_e164: str
    text: str
    reply_to_provider_message_id: str | None
    template_name: str | None
    language: str
    params: tuple[str, ...]
    conversation_id: Any


async def _run_pending_private_note(row: OutboxMessage) -> dict[str, Any]:
    """Deliver the note owed by ``row`` and return ONLY its meta updates.

    Never raises, never touches the primary lifecycle fields. A Chatwoot exception
    may carry a URL / token / response body, so only its class name is recorded.
    """
    meta = row.meta or {}
    kind = meta.get("private_note_kind")
    conversation_id = meta.get("chatwoot_conversation_id")
    text = _relay_note_text(kind, row) if kind else None
    attempts = int(meta.get("private_note_attempts") or 0) + 1
    stamp = {"private_note_attempts": attempts, "private_note_updated_at": utcnow().isoformat()}

    if text is None or not conversation_id or not settings.chatwoot_operator_reopen_private_note_enabled:
        return {**stamp, "private_note_status": "disabled"}

    cw = ChatwootClient()
    try:
        await cw.send_message(conversation_id, text, message_type="outgoing", private=True)
        logger.info(
            "operator_relay: private note sent outbox_id=%s kind=%s conv_id=%s",
            row.id,
            kind,
            safe_log_value(conversation_id, limit=32),
        )
        return {**stamp, "private_note_status": "sent"}
    except Exception as exc:
        err_type = type(exc).__name__
        exhausted = attempts >= _TERMINAL_ACTION_MAX_ATTEMPTS
        logger.warning(
            "operator_relay: private note failed outbox_id=%s kind=%s conv_id=%s error_type=%s attempts=%s",
            row.id,
            kind,
            safe_log_value(conversation_id, limit=32),
            err_type,
            attempts,
        )
        return {
            **stamp,
            "private_note_status": "failed" if exhausted else "pending",
            "private_note_error": err_type,
        }
    finally:
        await cw.aclose()


async def _run_pending_circuit_action(row: OutboxMessage) -> dict[str, Any]:
    """Close the Meta circuit owed by ``row`` and return ONLY its meta updates.

    Never raises and never calls a Meta *send* — it only records circuit state.
    """
    meta = row.meta or {}
    attempts = int(meta.get("circuit_action_attempts") or 0) + 1
    stamp = {"circuit_action_attempts": attempts, "circuit_action_updated_at": utcnow().isoformat()}
    try:
        await meta_circuit.close_meta_circuit(
            reason="operator_relay_transient_send_error",
            error_kind=meta.get("circuit_error_kind"),
            error_code=meta.get("circuit_error_code"),
            next_probe_at=utcnow() + timedelta(seconds=settings.meta_circuit_probe_initial_delay_seconds),
        )
        return {**stamp, "circuit_action_status": "completed"}
    except Exception as exc:
        exhausted = attempts >= _TERMINAL_ACTION_MAX_ATTEMPTS
        logger.error(
            "operator_relay: circuit action failed outbox_id=%s error_type=%s attempts=%s operation=circuit_close",
            row.id,
            type(exc).__name__,
            attempts,
        )
        return {
            **stamp,
            "circuit_action_status": "failed" if exhausted else "pending",
            "circuit_action_error": type(exc).__name__,
        }


async def _dispatch_terminal_actions_for(outbox_id: int) -> None:
    """Execute the durable pending secondary actions of ONE terminal Outbox row.

    The circuit action and the Chatwoot note are fully independent: each runs in
    its own guarded step, so a failure of one can never suppress the other. Only
    ``meta`` markers are updated — the primary ``status`` / ``error`` /
    ``provider_message_id`` describing the WhatsApp lifecycle are never touched,
    and no Meta *send* is ever issued from here.
    """
    async with SessionLocal() as session:
        row = await session.get(OutboxMessage, outbox_id)
        if row is None:
            return
        meta = row.meta or {}
        updates: dict[str, Any] = {}

        if meta.get("circuit_action_status") == "pending":
            updates.update(await _run_pending_circuit_action(row))

        if meta.get("private_note_status") == "pending":
            updates.update(await _run_pending_private_note(row))

    if not updates:
        return

    # Re-read under a short write transaction so the marker update never races
    # with (or clobbers) a concurrent primary-lifecycle write.
    async with SessionLocal() as session:
        async with session.begin():
            fresh = await session.get(OutboxMessage, outbox_id)
            if fresh is None:
                return
            fresh.meta = {**(fresh.meta or {}), **updates}

            # private_note_only is the one branch where the note IS the primary
            # user-facing result (no Meta send happens at all), so an established
            # product contract surfaces its failure on the source event. Even
            # here Outbox.error keeps the original cancel reason.
            if (
                updates.get("private_note_error")
                and (fresh.meta or {}).get("private_note_kind") == _NOTE_KIND_WINDOW_CLOSED
                and fresh.source_whatsapp_event_id is not None
            ):
                event = await session.get(WhatsAppEvent, fresh.source_whatsapp_event_id)
                if event is not None:
                    event.error = f"operator_relay: private note failed: {updates['private_note_error']}"


async def dispatch_pending_terminal_actions(*, limit: int | None = None) -> int:
    """Recover terminal operator rows whose secondary actions never ran.

    A crash between the finalize COMMIT and the side effects would otherwise lose
    the manual-review signal forever, because terminal rows are not revisited by
    any other recovery path. Bounded batch; never calls a Meta send.
    """
    batch = limit or settings.chatwoot_operator_relay_recovery_batch_size
    async with SessionLocal() as session:
        outbox_ids = list(
            (
                await session.execute(
                    select(OutboxMessage.id)
                    .where(
                        OutboxMessage.message_source == "operator",
                        OutboxMessage.status.in_(_TERMINAL_RELAY_STATUSES),
                        or_(
                            OutboxMessage.meta["private_note_status"].astext == "pending",
                            OutboxMessage.meta["circuit_action_status"].astext == "pending",
                        ),
                    )
                    .order_by(OutboxMessage.id.asc())
                    .limit(batch)
                )
            ).scalars()
        )

    dispatched = 0
    for outbox_id in outbox_ids:
        try:
            await _dispatch_terminal_actions_for(int(outbox_id))
            dispatched += 1
        except Exception as exc:
            logger.error(
                "operator_relay: terminal action dispatch failed outbox_id=%s error_type=%s operation=dispatch",
                outbox_id,
                type(exc).__name__,
            )
    return dispatched


def _mark_event_processed(event: WhatsAppEvent, error: str | None) -> None:
    event.status = "processed"
    event.processed_at = utcnow()
    event.error = error


def _existing_relay_replay(event: WhatsAppEvent, existing: OutboxMessage) -> _PreparedRelay:
    """Decide what to do when an Outbox already exists for this event (§9).

    Never calls the provider and never creates a second row. A still-``queued``
    row is returned for claim (crash-before-claim recovery); every terminal
    status maps the event to a stable marker; a ``sending`` row is left for the
    owning worker or stale recovery.
    """
    status = existing.status
    if status == "queued":
        return _PreparedRelay(
            outbox_id=existing.id,
            send_type=(existing.meta or {}).get("send_type"),
        )
    if status in ("sent", "delivered", "read"):
        _mark_event_processed(event, None)
    elif status == "failed":
        _mark_event_processed(event, "operator_relay: send failed (permanent)")
    elif status == "canceled":
        _mark_event_processed(event, "operator_relay: send canceled")
    elif status == "unknown":
        _mark_event_processed(event, "operator_relay: delivery outcome unknown")
    else:  # "sending": another worker owns the attempt, or stale recovery will.
        logger.info(
            "operator_relay: attempt already in progress event_id=%s outbox_id=%s — no re-send",
            event.id,
            existing.id,
        )
    return _PreparedRelay(outbox_id=None)


async def _prepare_operator_relay(event_id: int, provider: WhatsAppProvider) -> _PreparedRelay:
    """Stage A: validate, route, and commit a durable send intent (or terminal).

    Runs in a single short transaction holding a ``FOR UPDATE`` lock on the
    source event, so concurrent processing of the same event is serialized and
    the idempotency check is authoritative. The provider is never called here.
    """
    async with SessionLocal() as session:
        async with session.begin():
            event = (
                await session.execute(select(WhatsAppEvent).where(WhatsAppEvent.id == event_id).with_for_update())
            ).scalar_one_or_none()
            if event is None:
                return _PreparedRelay(outbox_id=None)

            # Idempotency: a prior attempt (any state) wins — never a 2nd send.
            existing = (
                await session.execute(
                    select(OutboxMessage)
                    .where(OutboxMessage.source_whatsapp_event_id == event_id)
                    .order_by(OutboxMessage.id.asc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if existing is not None:
                return _existing_relay_replay(event, existing)

            if not settings.chatwoot_operator_relay_enabled:
                logger.warning(
                    "operator_relay: event received but chatwoot_operator_relay_enabled=False event_id=%s",
                    event.id,
                )
                _mark_event_processed(event, "operator_relay: disabled by chatwoot_operator_relay_enabled")
                return _PreparedRelay(outbox_id=None)

            payload = event.payload or {}
            relay = mapping_or_empty(payload.get("_chatwoot_operator_relay"))
            phone_e164 = normalize_phone(relay.get("recipient_phone"))
            text = relay.get("text", "")
            conversation_id = relay.get("conversation_id")
            chatwoot_message_id = relay.get("message_id")
            phone_number_id = relay.get("phone_number_id")
            chatwoot_inbox_id = relay.get("chatwoot_inbox_id")
            agent_name = relay.get("agent_name", "")
            content_attributes = mapping_or_empty(relay.get("content_attributes"))

            cw_conversation_id = optional_chatwoot_id(conversation_id)
            cw_message_id = optional_chatwoot_id(chatwoot_message_id)
            reply_to_chatwoot_message_id = optional_chatwoot_id(relay.get("reply_to_chatwoot_message_id"))
            reply_context_audit: dict[str, Any] = {
                "reply_to_chatwoot_message_id": reply_to_chatwoot_message_id,
                "reply_to_provider_message_id": None,
                "reply_context_source": None,
                "reply_context_native": False,
            }
            if content_attributes:
                reply_context_audit["content_attributes"] = content_attributes

            # ── Validation (fail closed, stable non-PII markers) ──────────────
            if phone_e164 is None:
                logger.warning(
                    "operator_relay: invalid recipient_phone event_id=%s conv_id=%s msg_id=%s — skipping",
                    event.id,
                    safe_log_value(conversation_id, limit=32),
                    safe_log_value(chatwoot_message_id, limit=32),
                )
                _mark_event_processed(event, "operator_relay: invalid recipient_phone")
                return _PreparedRelay(outbox_id=None)

            if not (isinstance(text, str) and text.strip()):
                logger.warning(
                    "operator_relay: missing text conv_id=%s msg_id=%s — skipping",
                    safe_log_value(conversation_id, limit=32),
                    safe_log_value(chatwoot_message_id, limit=32),
                )
                _mark_event_processed(event, "operator_relay: missing text")
                return _PreparedRelay(outbox_id=None)

            company_id_hint, hint_err = _company_hint_from_inbox(chatwoot_inbox_id)
            if hint_err is not None:
                logger.warning(
                    "operator_relay: inbox routing error conv_id=%s msg_id=%s inbox_id=%s: %s",
                    safe_log_value(conversation_id, limit=32),
                    safe_log_value(chatwoot_message_id, limit=32),
                    safe_log_value(chatwoot_inbox_id, limit=32),
                    hint_err,
                )
                _mark_event_processed(event, hint_err)
                return _PreparedRelay(outbox_id=None)

            sender_id, company_id, routing_err = await _resolve_relay_sender(
                session,
                phone_number_id,
                company_id_hint=company_id_hint,
            )
            if routing_err is not None:
                logger.warning(
                    "operator_relay: routing blocked conv_id=%s msg_id=%s phone_number_id=%s err=%s",
                    safe_log_value(conversation_id, limit=32),
                    safe_log_value(chatwoot_message_id, limit=32),
                    safe_log_value(phone_number_id, limit=32),
                    routing_err,
                )
                _mark_event_processed(event, routing_err)
                return _PreparedRelay(outbox_id=None)

            logger.info(
                "operator_relay: accepted event_id=%s conv_id=%s msg_id=%s pnid=%s sender_id=%s company_id=%s",
                event.id,
                safe_log_value(conversation_id, limit=32),
                safe_log_value(chatwoot_message_id, limit=32),
                safe_log_value(phone_number_id, limit=32),
                sender_id,
                company_id,
            )

            meta_provider = getattr(provider, "_primary", provider)
            now = utcnow()
            mode = settings.chatwoot_operator_closed_window_mode

            window_open, last_inbound_at = await is_whatsapp_customer_window_open(session, phone_e164, now)
            hours_since: float = (now - last_inbound_at).total_seconds() / 3600 if last_inbound_at else -1.0
            last_inbound_iso: str | None = last_inbound_at.isoformat() if last_inbound_at else None
            logger.info(
                "operator_relay: window_check conv_id=%s msg_id=%s window_open=%s hours_since=%.1f mode=%s",
                safe_log_value(conversation_id, limit=32),
                safe_log_value(chatwoot_message_id, limit=32),
                window_open,
                hours_since,
                mode,
            )

            def _new_outbox(**overrides: Any) -> OutboxMessage:
                base: dict[str, Any] = dict(
                    company_id=company_id,
                    client_id=None,
                    record_id=None,
                    job_id=None,
                    sender_id=sender_id,
                    phone_e164=phone_e164,
                    language="de",
                    provider_message_id=None,
                    scheduled_at=now,
                    sent_at=None,
                    message_source="operator",
                    chatwoot_conversation_id=cw_conversation_id,
                    chatwoot_message_id=cw_message_id,
                    source_whatsapp_event_id=event.id,
                )
                base.update(overrides)
                return OutboxMessage(**base)

            # ── Pre-send Meta circuit guard: never call Meta while paused ──────
            if settings.meta_circuit_breaker_enabled and await meta_circuit.should_pause_meta_sends(session=session):
                attempted = "text" if window_open else ("template" if mode == "reopen_template" else "text")
                cancel_meta: dict[str, Any] = {
                    "send_type": "none",
                    "attempted_send_type": attempted,
                    "cancel_reason": "meta_circuit_closed",
                    "circuit_action": "already_closed",
                    "circuit_state": "closed",
                    "event_id": event.id,
                    "provider": type(meta_provider).__name__,
                    "phone_number_id": phone_number_id,
                    "wa_window_open": window_open,
                    "last_meta_inbound_at": last_inbound_iso,
                    "closed_window_mode": mode,
                    "chatwoot_conversation_id": conversation_id,
                    "chatwoot_message_id": chatwoot_message_id,
                    "agent_name": agent_name,
                    **reply_context_audit,
                }
                if attempted == "template":
                    cancel_meta["template"] = settings.chatwoot_operator_reopen_template_name
                # The owed operator note is recorded durably in the SAME
                # transaction as the canceled row, so a crash before delivery
                # still leaves it recoverable.
                cancel_meta.update(_pending_note_marker(_NOTE_KIND_CIRCUIT_PAUSED, conversation_id))
                outbox = _new_outbox(
                    template_code="operator_relay",
                    body=_OPERATOR_RELAY_CIRCUIT_CLOSED_BODY,
                    status="canceled",
                    meta=cancel_meta,
                    error="Meta circuit closed: operator relay paused",
                )
                session.add(outbox)
                await session.flush()
                _mark_event_processed(event, "operator_relay: Meta circuit closed")
                logger.warning(
                    "operator_relay: Meta circuit closed; paused conv_id=%s outbox_id=%s company_id=%s",
                    safe_log_value(conversation_id, limit=32),
                    outbox.id,
                    company_id,
                )
                return _PreparedRelay(outbox_id=None, terminal_outbox_id=outbox.id)

            # ── Window open → durable queued free-form text intent ────────────
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
                            safe_log_value(conversation_id, limit=32),
                            safe_log_value(chatwoot_message_id, limit=32),
                            target.source,
                        )
                    else:
                        logger.info(
                            "operator_relay: native reply context target not found conv_id=%s msg_id=%s reply_to=%s",
                            safe_log_value(conversation_id, limit=32),
                            safe_log_value(chatwoot_message_id, limit=32),
                            safe_log_value(reply_to_chatwoot_message_id, limit=32),
                        )

                outbox = _new_outbox(
                    template_code="operator_relay",
                    body=text,
                    status="queued",
                    meta={
                        "chatwoot_conversation_id": conversation_id,
                        "chatwoot_message_id": chatwoot_message_id,
                        "agent_name": agent_name,
                        "send_type": "text",
                        "wa_window_open": True,
                        "last_meta_inbound_at": last_inbound_iso,
                        "closed_window_mode": mode,
                        "reply_to_provider_message_id": reply_to_provider_message_id,
                        **reply_context_audit,
                    },
                )
                session.add(outbox)
                await session.flush()
                logger.info(
                    "operator_relay: direct text send attempt prepared event_id=%s outbox_id=%s company_id=%s",
                    event.id,
                    outbox.id,
                    company_id,
                )
                return _PreparedRelay(outbox_id=outbox.id, send_type="text")

            # ── Window closed + private_note_only → canceled audit, no Meta ────
            if mode == "private_note_only":
                outbox = _new_outbox(
                    template_code="operator_relay",
                    body=text,
                    status="canceled",
                    # Primary lifecycle error records the cancel reason; a later
                    # private-note failure must never overwrite it.
                    error="operator_relay: canceled (customer service window closed)",
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
                        **_pending_note_marker(_NOTE_KIND_WINDOW_CLOSED, conversation_id),
                    },
                )
                session.add(outbox)
                await session.flush()
                _mark_event_processed(event, None)
                logger.info(
                    "operator_relay: window closed, mode=private_note_only → canceled outbox_id=%s company_id=%s",
                    outbox.id,
                    company_id,
                )
                return _PreparedRelay(outbox_id=None, terminal_outbox_id=outbox.id)

            # ── Window closed + reopen_template → durable queued template ──────
            template_name = settings.chatwoot_operator_reopen_template_name
            language = settings.chatwoot_operator_reopen_template_language
            param_mode = settings.chatwoot_operator_reopen_template_param_mode
            contact_name = relay.get("contact_name") or phone_e164 or "Kunde"
            params: list[str] = [contact_name] if param_mode == "contact_name" else []

            outbox = _new_outbox(
                template_code="operator_reopen_template",
                language=language,
                body=text,
                status="queued",
                meta={
                    "send_type": "template",
                    "template": template_name,
                    "template_language": language,
                    "template_params": params,
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
                "operator_relay: reopen template send attempt prepared event_id=%s outbox_id=%s template=%s",
                event.id,
                outbox.id,
                safe_log_value(template_name, limit=64),
            )
            return _PreparedRelay(outbox_id=outbox.id, send_type="template")


@dataclass(frozen=True)
class _ClaimResult:
    """Outcome of Stage B.

    ``claimed`` set → this worker owns the single provider attempt.
    ``canceled_outbox_id`` set → the intent was no longer safe to send and was
    canceled instead; its durable pending note still has to be dispatched.
    Both None → another worker owns the attempt (or the row is not claimable).
    """

    claimed: _ClaimedRelay | None = None
    canceled_outbox_id: int | None = None


# Stable, non-sensitive cancel reasons for an intent that became unsafe between
# prepare and claim (crash/restart may put hours between the two).
_CLAIM_BLOCK_REASONS: dict[str, tuple[str, str, str | None]] = {
    # reason -> (Outbox.error, WhatsAppEvent.error, note kind)
    "operator_relay_disabled_before_claim": (
        "operator_relay: canceled (relay disabled before claim)",
        "operator_relay: canceled (relay disabled before claim)",
        None,  # the operator switched the feature off deliberately — no note spam
    ),
    "meta_circuit_closed_before_claim": (
        "operator_relay: canceled (Meta circuit closed before claim)",
        "operator_relay: Meta circuit closed",
        _NOTE_KIND_CIRCUIT_PAUSED,
    ),
    "customer_service_window_closed_before_claim": (
        "operator_relay: canceled (customer service window closed before claim)",
        "operator_relay: canceled (customer service window closed before claim)",
        _NOTE_KIND_WINDOW_CLOSED,
    ),
}


async def _relay_claim_block_reason(session: AsyncSession, row: OutboxMessage) -> str | None:
    """Re-check the live send gates for an intent that is about to be claimed.

    A queued intent may have been committed hours ago (crash/restart), so the
    world it was prepared in can be gone: the relay may have been switched off,
    the Meta circuit may have closed, and — decisively for a free-form text —
    the 24h customer service window may have expired, which would make sending
    it now a Meta policy violation. A closed window is expected and harmless for
    a committed reopen-template intent, so it is not re-checked there.
    """
    if not settings.chatwoot_operator_relay_enabled:
        return "operator_relay_disabled_before_claim"
    if settings.meta_circuit_breaker_enabled and await meta_circuit.should_pause_meta_sends(session=session):
        return "meta_circuit_closed_before_claim"
    if (row.meta or {}).get("send_type") != "template":
        window_open, _ = await is_whatsapp_customer_window_open(session, row.phone_e164, utcnow())
        if not window_open:
            return "customer_service_window_closed_before_claim"
    return None


async def _claim_operator_relay(outbox_id: int, event_id: int) -> _ClaimResult:
    """Stage B: revalidate the gates, then atomically claim queued → sending.

    The row is locked FOR UPDATE first, so the gate re-check and the state
    transition are decided under the same lock — a concurrent worker cannot slip
    a send in between them. The transition itself keeps its conditional
    ``WHERE status='queued'`` predicate, so exactly one worker can ever win it.
    """
    async with SessionLocal() as session:
        async with session.begin():
            locked = (
                await session.execute(select(OutboxMessage).where(OutboxMessage.id == outbox_id).with_for_update())
            ).scalar_one_or_none()
            if locked is None or locked.status != "queued":
                return _ClaimResult()

            block_reason = await _relay_claim_block_reason(session, locked)
            if block_reason is not None:
                outbox_error, event_error, note_kind = _CLAIM_BLOCK_REASONS[block_reason]
                conversation_id = (locked.meta or {}).get("chatwoot_conversation_id")
                locked.status = "canceled"
                locked.error = outbox_error
                new_meta = {**(locked.meta or {}), "cancel_reason": block_reason}
                if note_kind is not None:
                    new_meta.update(_pending_note_marker(note_kind, conversation_id))
                locked.meta = new_meta
                event = await session.get(WhatsAppEvent, event_id)
                if event is not None:
                    _mark_event_processed(event, event_error)
                logger.warning(
                    "operator_relay: queued intent canceled before claim outbox_id=%s event_id=%s reason=%s",
                    outbox_id,
                    event_id,
                    block_reason,
                )
                return _ClaimResult(canceled_outbox_id=outbox_id)

            now = utcnow()
            claimed_id = (
                await session.execute(
                    update(OutboxMessage)
                    .where(OutboxMessage.id == outbox_id, OutboxMessage.status == "queued")
                    .values(status="sending", attempt_started_at=now)
                    .returning(OutboxMessage.id)
                )
            ).scalar_one_or_none()
            if claimed_id is None:
                return _ClaimResult()

            row = await session.get(OutboxMessage, outbox_id)
            meta = row.meta or {}
            send_type = meta.get("send_type") or "text"
            claimed = _ClaimedRelay(
                outbox_id=row.id,
                event_id=event_id,
                send_type=send_type,
                sender_id=int(row.sender_id) if row.sender_id is not None else 0,
                company_id=int(row.company_id),
                phone_e164=row.phone_e164,
                text=row.body,
                reply_to_provider_message_id=meta.get("reply_to_provider_message_id"),
                template_name=meta.get("template"),
                language=row.language,
                params=tuple(meta.get("template_params") or []),
                conversation_id=meta.get("chatwoot_conversation_id"),
            )
    logger.info(
        "operator_relay: %s attempt claimed event_id=%s outbox_id=%s sender_id=%s",
        claimed.send_type,
        event_id,
        outbox_id,
        claimed.sender_id,
    )
    return _ClaimResult(claimed=claimed)


async def _execute_operator_relay(
    claimed: _ClaimedRelay,
    provider: WhatsAppProvider,
) -> RelayProviderOutcome:
    """Stage C: call Meta OUTSIDE any DB transaction and classify the outcome."""
    meta_provider = getattr(provider, "_primary", provider)
    if claimed.send_type == "template":
        wamid, err = await safe_send_template(
            provider=meta_provider,
            sender_id=claimed.sender_id,
            phone=claimed.phone_e164,
            template_name=claimed.template_name or "",
            language=claimed.language,
            params=list(claimed.params),
            company_id=claimed.company_id,
        )
    else:
        wamid, err = await safe_send(
            provider=meta_provider,
            sender_id=claimed.sender_id,
            phone=claimed.phone_e164,
            text=claimed.text,
            company_id=claimed.company_id,
            reply_to_provider_message_id=claimed.reply_to_provider_message_id,
        )

    if err is None:
        # A 2xx response with no usable wamid does NOT confirm acceptance — Meta
        # may have taken the message but we cannot prove it. Treat as unknown.
        if isinstance(wamid, str) and wamid.strip():
            return RelayProviderOutcome(kind="sent", provider_message_id=wamid)
        return RelayProviderOutcome(kind="unknown", error_kind="missing_wamid")

    # Pre-request deterministic guard: safe_send refused before any HTTP call, so
    # Meta definitely did NOT receive the message — this is a real failure, never
    # an indeterminate post-request outcome.
    if err == "Real send disabled":
        return RelayProviderOutcome(kind="failed", error_kind="real_send_disabled")

    # Conservative classification (raw error is never logged or persisted):
    #   transient network/5xx/rate-limit → unknown (+ circuit close);
    #   documented deterministic rejection → failed;
    #   everything else after a possible request → unknown (default).
    # The absence of a transient marker is NOT proof that Meta rejected the send.
    if is_transient_provider_error(err):
        error_kind, error_code = transient_error_reason(err)
        return RelayProviderOutcome(
            kind="unknown",
            error_kind=error_kind,
            error_code=error_code,
            circuit_transient=True,
        )
    if is_deterministic_meta_rejection(err):
        return RelayProviderOutcome(kind="failed", error_kind="permanent")
    return RelayProviderOutcome(kind="unknown", error_kind="indeterminate")


async def _finalize_operator_relay(claimed: _ClaimedRelay, outcome: RelayProviderOutcome) -> bool:
    """Stage D: persist the terminal outcome and source-event state in one tx.

    The durable markers for the secondary actions still owed (Chatwoot note,
    Meta-circuit close) are written in THIS SAME transaction, so a crash between
    this COMMIT and the side effects cannot lose the manual-review signal: no
    other recovery path revisits a terminal row, but the terminal-action
    dispatcher does.

    Returns True when this call actually finalized the row (it was still
    'sending'); False when another path already moved it (finalize must not
    overwrite a recovered 'unknown' with a late 'sent').
    """
    async with SessionLocal() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(OutboxMessage).where(OutboxMessage.id == claimed.outbox_id).with_for_update()
                )
            ).scalar_one_or_none()
            if row is None or row.status != "sending":
                return False
            event = (
                await session.execute(
                    select(WhatsAppEvent).where(WhatsAppEvent.id == claimed.event_id).with_for_update()
                )
            ).scalar_one_or_none()
            now = utcnow()

            conversation_id = (row.meta or {}).get("chatwoot_conversation_id")
            is_template = claimed.send_type == "template"

            if outcome.kind == "sent":
                row.status = "sent"
                row.provider_message_id = outcome.provider_message_id
                row.sent_at = now
                # Only the reopen-template success owes the operator a note; a
                # plain text send that succeeded needs no explanation.
                if is_template:
                    row.meta = {
                        **(row.meta or {}),
                        **_pending_note_marker(_NOTE_KIND_TEMPLATE_SENT, conversation_id),
                    }
                if event is not None:
                    _mark_event_processed(event, None)
            elif outcome.kind == "failed":
                # Deterministic rejection: Meta confirmed the customer did NOT get
                # it. Both paths owe the operator a note — Chatwoot already shows
                # the agent's message as sent, so without one the operator would
                # believe it was delivered.
                note_kind = _NOTE_KIND_TEMPLATE_FAILED if is_template else _NOTE_KIND_TEXT_FAILED
                row.status = "failed"
                row.error = "operator_relay: send failed (permanent)"
                row.meta = {
                    **(row.meta or {}),
                    "error_kind": outcome.error_kind,
                    **_pending_note_marker(note_kind, conversation_id),
                }
                if event is not None:
                    _mark_event_processed(event, "operator_relay: send failed (permanent)")
            else:  # unknown
                row.status = "unknown"
                row.error = "operator_relay: delivery outcome unknown"
                row.meta = {
                    **(row.meta or {}),
                    "manual_review_required": True,
                    "recovery_reason": "indeterminate_send_outcome",
                    "error_kind": outcome.error_kind,
                    "error_code": outcome.error_code,
                    **_pending_note_marker(_NOTE_KIND_UNKNOWN, conversation_id),
                    **_pending_circuit_marker(outcome),
                }
                if event is not None:
                    _mark_event_processed(event, "operator_relay: delivery outcome unknown")
    return True


async def _process_operator_relay_event(event_id: int, provider: WhatsAppProvider) -> None:
    """Drive the full staged relay lifecycle for one event.

    prepare (commit) -> claim (commit) -> execute (no tx) -> finalize (commit),
    then dispatch the secondary actions that the terminal commit recorded as
    pending. Those markers are durable, so if this process dies before (or
    during) the dispatch, production recovery still delivers them.
    """
    # prepare is retried once: the only way the FOR UPDATE serialization can be
    # defeated is a raced insert caught by the unique index, after which a retry
    # observes the existing row and takes the replay path.
    for _attempt in range(2):
        try:
            prepared = await _prepare_operator_relay(event_id, provider)
            break
        except IntegrityError:
            logger.info("operator_relay: concurrent prepare raced on unique index event_id=%s — retrying", event_id)
    else:
        return

    if prepared.terminal_outbox_id is not None:
        await _dispatch_terminal_actions_for(prepared.terminal_outbox_id)

    if prepared.outbox_id is None:
        return

    claim = await _claim_operator_relay(prepared.outbox_id, event_id)
    if claim.canceled_outbox_id is not None:
        # The intent was no longer safe to send (flag/circuit/window) and was
        # canceled instead; its owed operator note is durable and dispatched here.
        await _dispatch_terminal_actions_for(claim.canceled_outbox_id)
        return
    claimed = claim.claimed
    if claimed is None:
        return

    outcome = await _execute_operator_relay(claimed, provider)
    finalized = await _finalize_operator_relay(claimed, outcome)
    if not finalized:
        return

    _log_relay_outcome(claimed, outcome)
    await _dispatch_terminal_actions_for(claimed.outbox_id)


def _log_relay_outcome(claimed: _ClaimedRelay, outcome: RelayProviderOutcome) -> None:
    """Operational log for a committed terminal outcome (no PII, no raw error)."""
    if outcome.kind == "sent":
        logger.info(
            "operator_relay: %s sent outbox_id=%s sender_id=%s",
            claimed.send_type,
            claimed.outbox_id,
            claimed.sender_id,
        )
    elif outcome.kind == "failed":
        logger.warning(
            "operator_relay: %s send failed outbox_id=%s error_kind=permanent",
            claimed.send_type,
            claimed.outbox_id,
        )
    else:
        logger.warning(
            "operator_relay: %s send outcome unknown outbox_id=%s error_kind=%s error_code=%s — manual review",
            claimed.send_type,
            claimed.outbox_id,
            outcome.error_kind,
            outcome.error_code,
        )


async def recover_stale_operator_relay_sending(
    provider: WhatsAppProvider | None = None,
    *,
    now: datetime | None = None,
) -> int:
    """Move operator Outbox rows stuck in 'sending' past the threshold to 'unknown'.

    Never calls the provider and never returns a row to 'queued'. Each stale row
    becomes an explicit manual-review marker; the source event is closed with a
    stable, non-sensitive error. Returns the number of rows recovered.
    """
    now = now or utcnow()
    threshold = now - timedelta(seconds=settings.chatwoot_operator_relay_stale_sending_seconds)
    recovered = 0

    async with SessionLocal() as session:
        async with session.begin():
            rows = list(
                (
                    await session.execute(
                        select(OutboxMessage)
                        .where(
                            OutboxMessage.message_source == "operator",
                            OutboxMessage.status == "sending",
                            OutboxMessage.attempt_started_at.is_not(None),
                            OutboxMessage.attempt_started_at < threshold,
                        )
                        .with_for_update(skip_locked=True)
                    )
                ).scalars()
            )
            note_targets: list[int] = []
            for row in rows:
                conversation_id = (row.meta or {}).get("chatwoot_conversation_id")
                row.status = "unknown"
                row.error = "operator_relay: delivery outcome unknown"
                # The owed manual-review note is recorded durably in the SAME
                # transaction as the 'unknown' status.
                row.meta = {
                    **(row.meta or {}),
                    "manual_review_required": True,
                    "recovery_reason": "stale_sending_attempt",
                    **_pending_note_marker(_NOTE_KIND_UNKNOWN, conversation_id),
                }
                if row.source_whatsapp_event_id is not None:
                    event = await session.get(WhatsAppEvent, row.source_whatsapp_event_id)
                    if event is not None:
                        _mark_event_processed(event, "operator_relay: delivery outcome unknown")
                logger.warning(
                    "operator_relay: recovered stale sending attempt outbox_id=%s event_id=%s — marked unknown",
                    row.id,
                    row.source_whatsapp_event_id,
                )
                note_targets.append(int(row.id))
                recovered += 1

    for outbox_id in note_targets:
        await _dispatch_terminal_actions_for(outbox_id)

    return recovered


@dataclass
class RecoveryStats:
    """Counts from one operator-relay recovery cycle (for logs/metrics/tests)."""

    recovered_sending: int = 0
    recovered_processing: int = 0
    resumed_queued: int = 0
    dispatched_actions: int = 0


async def recover_stale_processing_events(*, now: datetime | None = None) -> int:
    """Reset operator events stuck in 'processing' with NO Outbox back to 'received'.

    A 'processing' operator event whose durable prepare never produced an Outbox
    was interrupted BEFORE any Meta side effect, so it is safe to re-pick. Events
    that already have ANY Outbox (queued/sending/terminal) are never touched here
    — those are resumed or recovered by the Outbox-driven paths. Bounded batch,
    ``FOR UPDATE SKIP LOCKED`` so two workers never reset the same event.

    The operator-relay predicate is evaluated IN SQL, before ORDER BY / LIMIT /
    FOR UPDATE, so older unrelated 'processing' rows can never fill the bounded
    batch and starve relay recovery (a relay event would then stay 'processing'
    forever). ``jsonb_typeof(payload->'_chatwoot_operator_relay') = 'object'`` is
    the exact SQL equivalent of :func:`_is_operator_relay` — the same marker
    ``process_one_event`` routes on — so the SQL filter and the Python guard can
    never disagree. The dedupe_key prefix is deliberately NOT used: it is a
    formatting convention, and ANDing it in would silently skip (and re-starve) a
    relay event whose key format ever changes.
    """
    now = now or utcnow()
    threshold = now - timedelta(seconds=settings.chatwoot_operator_relay_stale_processing_seconds)
    batch = settings.chatwoot_operator_relay_recovery_batch_size
    recovered = 0

    async with SessionLocal() as session:
        async with session.begin():
            has_outbox = exists(
                select(OutboxMessage.id).where(OutboxMessage.source_whatsapp_event_id == WhatsAppEvent.id)
            )
            is_operator_relay_sql = func.jsonb_typeof(WhatsAppEvent.payload[_OPERATOR_RELAY_MARKER_KEY]) == "object"
            rows = list(
                (
                    await session.execute(
                        select(WhatsAppEvent)
                        .where(
                            WhatsAppEvent.status == "processing",
                            WhatsAppEvent.processed_at.is_(None),
                            WhatsAppEvent.received_at < threshold,
                            is_operator_relay_sql,
                            ~has_outbox,
                        )
                        .order_by(WhatsAppEvent.received_at.asc())
                        .limit(batch)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars()
            )
            for event in rows:
                # Defense in depth only — the authoritative filter ran in SQL above.
                if not _is_operator_relay(event.payload or {}):
                    continue
                event.status = "received"
                recovered += 1
                logger.warning(
                    "operator_relay: recovered stale processing event without outbox event_id=%s — re-queued",
                    event.id,
                )
    return recovered


async def resume_queued_operator_relay(provider: WhatsAppProvider, *, batch_size: int | None = None) -> int:
    """Resume committed 'queued' operator Outbox rows through the shared pipeline.

    Re-drives ``process_one_event`` for each queued row's source event: prepare
    sees the existing queued row and returns it for the atomic claim, so no second
    Outbox is created and only one worker's claim wins the single provider call.
    Per-row exceptions are isolated (class name only) so one bad row cannot stop
    the scan or crash the worker.
    """
    batch = batch_size or settings.chatwoot_operator_relay_recovery_batch_size
    async with SessionLocal() as session:
        event_ids = list(
            (
                await session.execute(
                    select(OutboxMessage.source_whatsapp_event_id)
                    .where(
                        OutboxMessage.message_source == "operator",
                        OutboxMessage.status == "queued",
                        OutboxMessage.source_whatsapp_event_id.is_not(None),
                    )
                    .order_by(OutboxMessage.id.asc())
                    .limit(batch)
                )
            ).scalars()
        )

    resumed = 0
    for event_id in event_ids:
        try:
            await process_one_event(int(event_id), provider)
            resumed += 1
        except Exception as exc:
            logger.error(
                "operator_relay: queued resume failed event_id=%s error_type=%s operation=resume_queued",
                event_id,
                type(exc).__name__,
            )
    return resumed


async def recover_operator_relay_lifecycle(
    provider: WhatsAppProvider,
    *,
    now: datetime | None = None,
) -> RecoveryStats:
    """Run the four independent operator-relay recovery actions (order matters):

      1. stale 'sending' → 'unknown' (never retried);
      2. stale 'processing' without Outbox → 'received' (re-preparable);
      3. committed 'queued' → revalidate gates, then claim/send once;
      4. terminal rows with pending secondary actions → dispatch them.

    Each action is isolated: a failure in one is logged by class name only and
    never prevents the others or crashes the worker.
    """
    stats = RecoveryStats()
    try:
        stats.recovered_sending = await recover_stale_operator_relay_sending(provider, now=now)
    except Exception as exc:
        logger.error("operator_relay: recovery step failed operation=stale_sending error_type=%s", type(exc).__name__)
    try:
        stats.recovered_processing = await recover_stale_processing_events(now=now)
    except Exception as exc:
        logger.error(
            "operator_relay: recovery step failed operation=stale_processing error_type=%s", type(exc).__name__
        )
    try:
        stats.resumed_queued = await resume_queued_operator_relay(provider)
    except Exception as exc:
        logger.error("operator_relay: recovery step failed operation=queued_resume error_type=%s", type(exc).__name__)
    try:
        # Terminal rows are revisited by no other path, so a crash between the
        # finalize COMMIT and its side effects would strand the manual-review
        # signal forever without this step.
        stats.dispatched_actions = await dispatch_pending_terminal_actions()
    except Exception as exc:
        logger.error(
            "operator_relay: recovery step failed operation=terminal_actions error_type=%s", type(exc).__name__
        )

    if stats.recovered_sending or stats.recovered_processing or stats.resumed_queued or stats.dispatched_actions:
        logger.info(
            "operator_relay: recovery cycle recovered_sending=%s recovered_processing=%s "
            "resumed_queued=%s dispatched_actions=%s",
            stats.recovered_sending,
            stats.recovered_processing,
            stats.resumed_queued,
            stats.dispatched_actions,
        )
    return stats


async def handle_event(
    session: AsyncSession,
    event: WhatsAppEvent,
    provider: WhatsAppProvider,
) -> None:
    payload = event.payload or {}

    # ------------------------------------------------------------------ #
    # 0. Operator relay: Chatwoot outgoing → Meta (Meta-first path)       #
    # ------------------------------------------------------------------ #
    # Operator relay is NOT handled inside this single-transaction path: it runs
    # a durable, multi-transaction pipeline (prepare/claim/execute/finalize) that
    # commits a send intent before the first Meta side effect. process_one_event
    # routes relay events there. Reaching here means a relay payload was handed to
    # the single-transaction handler directly — refuse to send (it cannot offer
    # durability) rather than fall through and risk a send-before-Outbox.
    if _is_operator_relay(payload):
        logger.warning(
            "operator_relay: reached single-transaction handle_event event_id=%s — "
            "must go through the durable pipeline; skipping",
            event.id,
        )
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
                "Skipping Chatwoot log for chatwoot-origin event dedupe_key=%s event_id=%s",
                safe_log_value(event.dedupe_key, limit=128),
                event.id,
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
            "wa_cmd=%s sender_phone_number_id=%s sender_id=%s clients_updated=%s jobs_canceled=%s event_id=%s",
            cmd,
            safe_log_value(phone_number_id, limit=32),
            sender_id,
            affected,
            canceled,
            event.id,
        )
    else:
        logger.info(
            "wa_cmd=%s sender_phone_number_id=%s sender_id=%s event_id=%s",
            cmd,
            safe_log_value(phone_number_id, limit=32),
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
            "Ack send failed sender_id=%s err=%s",
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
        "Ack sent sender_id=%s msg_id=%s",
        sender_id,
        safe_log_value(msg_id, limit=64),
    )


async def process_one_event(
    event_id: int,
    provider: WhatsAppProvider,
) -> None:
    # Classify with a short read that holds no long lock. Operator relay must NOT
    # run inside the single event transaction below: it drives its own staged,
    # separately-committed pipeline so a durable send intent is on disk before
    # the first Meta side effect and no lock is held across the network call.
    async with SessionLocal() as peek:
        peeked = (await peek.execute(select(WhatsAppEvent).where(WhatsAppEvent.id == event_id))).scalar_one_or_none()
        if peeked is None:
            return
        is_relay = _is_operator_relay(peeked.payload or {})

    if is_relay:
        with perf_log("whatsapp_inbox_worker", "process_event", event_id=event_id, origin="operator_relay"):
            await _process_operator_relay_event(event_id, provider)
        return

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


@dataclass
class PollCycleStats:
    """Outcome of a single production poll iteration (for tests/observability)."""

    recovery: RecoveryStats | None = None
    processed: int = 0
    failed: int = 0


async def run_poll_cycle(
    provider: WhatsAppProvider,
    *,
    batch_size: int,
    run_recovery: bool,
) -> PollCycleStats:
    """One production poll iteration: optional recovery, then a normal batch.

    Recovery runs first so stale ``sending`` becomes ``unknown`` before anything
    is re-examined, stale ``processing`` events are re-queued, and committed
    ``queued`` intents are resumed. Every per-event failure is isolated: one
    operator relay can never terminate the loop or block later events, and the
    log carries only the event id, the exception class, and the operation — never
    the raw exception text.
    """
    stats = PollCycleStats()
    if run_recovery:
        stats.recovery = await recover_operator_relay_lifecycle(provider)

    async with SessionLocal() as session:
        async with session.begin():
            events = await lock_next_batch(session, batch_size)
            event_ids = [int(e.id) for e in events]

    for eid in event_ids:
        try:
            await process_one_event(eid, provider)
            stats.processed += 1
        except Exception as exc:
            stats.failed += 1
            logger.error(
                "operator_relay: event processing failed event_id=%s error_type=%s operation=process_one_event",
                eid,
                type(exc).__name__,
            )
    return stats


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

    # Startup recovery: reclaim anything a previous crash left stranded before
    # the first normal batch. Never fatal to the worker.
    try:
        await recover_operator_relay_lifecycle(provider)
    except Exception as exc:
        logger.error("operator_relay: startup recovery failed error_type=%s", type(exc).__name__)

    recovery_interval = settings.chatwoot_operator_relay_recovery_interval_seconds
    last_recovery = time.monotonic()

    while True:
        run_recovery = (time.monotonic() - last_recovery) >= recovery_interval
        if run_recovery:
            last_recovery = time.monotonic()

        stats = await run_poll_cycle(provider, batch_size=batch_size, run_recovery=run_recovery)

        if stats.processed == 0 and stats.failed == 0:
            await asyncio.sleep(effective_poll_sec)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    raise SystemExit("Run as a script: python -m altegio_bot.scripts.run_whatsapp_inbox_worker")


if __name__ == "__main__":
    main()
