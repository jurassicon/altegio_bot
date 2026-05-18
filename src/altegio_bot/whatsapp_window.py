"""24-hour WhatsApp customer service window detection.

A reusable helper that tells callers whether the 24h Meta customer service
window is still open for a given phone number.  The window is open if the
customer sent a real Meta-origin inbound message within the last 24 hours.

The effective inbound time is the Meta message timestamp (msg['timestamp'],
Unix seconds from Meta), not event.received_at.  event.received_at is used
only as a fallback when the message timestamp is missing or unparseable.
This prevents a delayed or redelivered webhook with a fresh received_at but
an old msg.timestamp from incorrectly opening the window.

Chatwoot-origin events (mirrored incoming, operator relay, etc.) are
explicitly excluded so they do not inadvertently reset the window.

Public exports:
    normalize_phone(raw) -> str | None
    get_last_meta_inbound_at(session, phone_e164, before) -> datetime | None
    is_whatsapp_customer_window_open(session, phone_e164, now) -> (bool, datetime | None)

Usage::

    window_open, last_inbound_at = await is_whatsapp_customer_window_open(
        session, phone_e164, now
    )
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import not_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import WhatsAppEvent

logger = logging.getLogger("whatsapp_window")

_WINDOW_HOURS = 24
_QUERY_LOOKBACK_HOURS = 26


def normalize_phone(raw: str | None) -> str | None:
    """Normalize a phone string to E.164 (+digits only). Returns None if invalid."""
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    return f"+{digits}"


def _extract_meta_inbound_times(
    payload: dict[str, Any],
    target_phone: str,
    fallback_received_at: datetime,
    before: datetime,
) -> list[datetime]:
    """Return effective inbound times from payload for target_phone.

    For each matching message (from == target_phone after normalization):
    - Parse msg['timestamp'] (Unix seconds string from Meta).
    - If valid and not in the future (> before), use it as the candidate time.
    - If the timestamp is missing or unparseable, fall back to fallback_received_at.
    - Future timestamps are skipped entirely — not fallen back to.

    Returns timezone-aware UTC datetimes.
    """
    candidates: list[datetime] = []
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
                if normalize_phone(msg.get("from")) != target_phone:
                    continue
                ts_raw = msg.get("timestamp")
                candidate: datetime
                if ts_raw:
                    try:
                        ts_sec = int(ts_raw)
                        parsed = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                        if parsed > before:
                            continue  # future timestamp — skip, do not fall back
                        candidate = parsed
                    except (ValueError, TypeError, OSError):
                        candidate = fallback_received_at
                else:
                    candidate = fallback_received_at
                if candidate.tzinfo is None:
                    candidate = candidate.replace(tzinfo=timezone.utc)
                candidates.append(candidate)
    return candidates


async def get_last_meta_inbound_at(
    session: AsyncSession,
    phone_e164: str,
    before: datetime,
) -> datetime | None:
    """Return the most recent effective inbound time for phone_e164.

    The effective time is the Meta message timestamp (msg['timestamp']),
    with event.received_at as fallback when timestamp is missing or invalid.

    Only considers events whose received_at is within the last 26 hours
    before `before` (performance guard).  Excludes all Chatwoot-origin events
    (dedupe_key prefix, payload markers, or chatwoot_conversation_id set).

    If several candidate times exist across multiple events or messages, returns
    the maximum (most recent) that is <= before.
    """
    target_phone = normalize_phone(phone_e164)
    if target_phone is None:
        return None

    cutoff = before - timedelta(hours=_QUERY_LOOKBACK_HOURS)

    stmt = (
        select(WhatsAppEvent)
        .where(WhatsAppEvent.received_at >= cutoff)
        .where(WhatsAppEvent.received_at <= before)
        .where(WhatsAppEvent.chatwoot_conversation_id.is_(None))
        .where(not_(WhatsAppEvent.payload.has_key("_chatwoot")))
        .where(not_(WhatsAppEvent.payload.has_key("_chatwoot_operator_relay")))
        .where(not_(WhatsAppEvent.dedupe_key.like("chatwoot:%")))
    )
    res = await session.execute(stmt)
    events = list(res.scalars().all())

    all_candidates: list[datetime] = []
    for event in events:
        fallback = event.received_at
        if fallback is None:
            continue
        if fallback.tzinfo is None:
            fallback = fallback.replace(tzinfo=timezone.utc)
        candidates = _extract_meta_inbound_times(
            event.payload or {},
            target_phone,
            fallback_received_at=fallback,
            before=before,
        )
        all_candidates.extend(candidates)

    if not all_candidates:
        return None

    return max(all_candidates)


async def is_whatsapp_customer_window_open(
    session: AsyncSession,
    phone_e164: str,
    now: datetime,
) -> tuple[bool, datetime | None]:
    """Return (window_open, last_meta_inbound_at).

    window_open is True iff the customer sent a real Meta-origin message
    within the last 24 hours (inclusive boundary: exactly 24 h counts as open).
    The inbound time is based on the Meta message timestamp, not server
    received_at — see get_last_meta_inbound_at for details.
    """
    last_inbound = await get_last_meta_inbound_at(session, phone_e164, before=now)
    if last_inbound is None:
        return False, None

    if last_inbound.tzinfo is None:
        last_inbound = last_inbound.replace(tzinfo=timezone.utc)

    window_open = (now - last_inbound) <= timedelta(hours=_WINDOW_HOURS)
    return window_open, last_inbound
