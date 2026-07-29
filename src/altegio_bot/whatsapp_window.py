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
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import not_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import WhatsAppEvent
from altegio_bot.webhooks.common import list_or_empty, mapping_or_empty, normalize_phone_candidate

logger = logging.getLogger("whatsapp_window")

_WINDOW_HOURS = 24
_QUERY_LOOKBACK_HOURS = 26


def normalize_phone(raw: object) -> str | None:
    """Normalize a phone value to E.164 (+digits only). Returns None if invalid.

    Type-safe: delegates to the shared ``normalize_phone_candidate`` so Chatwoot
    ingress, the worker and this module share one contract. A non-string value
    (list/dict/int/bool/float/None) degrades to None instead of raising.
    """
    return normalize_phone_candidate(raw)


def _extract_meta_inbound_times(
    payload: dict[str, Any],
    target_phone: str,
    fallback_received_at: datetime,
    before: datetime,
    phone_number_id: str | None = None,
) -> list[datetime]:
    """Return effective inbound times from payload for target_phone.

    For each matching message (from == target_phone after normalization):
    - Parse msg['timestamp'] (Unix seconds string from Meta).
    - If valid and not in the future (> before), use it as the candidate time.
    - If the timestamp is missing or unparseable, fall back to fallback_received_at.
    - Future timestamps are skipped entirely — not fallen back to.

    When phone_number_id is given, only messages whose enclosing
    value.metadata.phone_number_id matches are considered.  This prevents a
    customer inbound on one WhatsApp sender number from incorrectly opening
    the window for a job sent from a different sender number.

    Returns timezone-aware UTC datetimes.
    """
    candidates: list[datetime] = []
    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue
            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue
            if phone_number_id is not None:
                meta_pnid = mapping_or_empty(value.get("metadata")).get("phone_number_id")
                if meta_pnid != phone_number_id:
                    continue
            for msg in list_or_empty(value.get("messages")):
                if not isinstance(msg, dict):
                    continue
                raw_from = msg.get("from")
                if not isinstance(raw_from, str) or normalize_phone(raw_from) != target_phone:
                    continue
                ts_raw = msg.get("timestamp")
                candidate: datetime
                if ts_raw:
                    try:
                        # OverflowError guards int(float("inf")); the finite check
                        # keeps a NaN/inf timestamp from reaching int() at all.
                        if isinstance(ts_raw, float) and not math.isfinite(ts_raw):
                            candidate = fallback_received_at
                        else:
                            ts_sec = int(ts_raw)
                            parsed = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                            if parsed > before:
                                continue  # future timestamp — skip, do not fall back
                            candidate = parsed
                    except (ValueError, TypeError, OSError, OverflowError):
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
    phone_number_id: str | None = None,
) -> datetime | None:
    """Return the most recent effective inbound time for phone_e164.

    The effective time is the Meta message timestamp (msg['timestamp']),
    with event.received_at as fallback when timestamp is missing or invalid.

    Only considers events whose received_at is within the last 26 hours
    before `before` (performance guard).  Excludes all Chatwoot-origin events
    (dedupe_key prefix, payload markers, or chatwoot_conversation_id set).

    When phone_number_id is given, only messages from that WhatsApp sender
    number are counted.  Pass None to match any sender (backward-compatible).

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
            phone_number_id=phone_number_id,
        )
        all_candidates.extend(candidates)

    if not all_candidates:
        return None

    return max(all_candidates)


async def is_whatsapp_customer_window_open(
    session: AsyncSession,
    phone_e164: str,
    now: datetime,
    phone_number_id: str | None = None,
) -> tuple[bool, datetime | None]:
    """Return (window_open, last_meta_inbound_at).

    window_open is True iff the customer sent a real Meta-origin message
    within the last 24 hours (inclusive boundary: exactly 24 h counts as open).
    The inbound time is based on the Meta message timestamp, not server
    received_at — see get_last_meta_inbound_at for details.

    When phone_number_id is given, only inbound messages addressed to that
    WhatsApp sender number are counted.  Pass None for phone-only matching
    (backward-compatible behaviour).
    """
    last_inbound = await get_last_meta_inbound_at(session, phone_e164, before=now, phone_number_id=phone_number_id)
    if last_inbound is None:
        return False, None

    if last_inbound.tzinfo is None:
        last_inbound = last_inbound.replace(tzinfo=timezone.utc)

    window_open = (now - last_inbound) <= timedelta(hours=_WINDOW_HOURS)
    return window_open, last_inbound
