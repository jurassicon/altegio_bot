"""24-hour WhatsApp customer service window detection.

A reusable helper that tells callers whether the 24h Meta customer service
window is still open for a given phone number.  The window is open if the
customer sent a real Meta-origin inbound message within the last 24 hours.

Chatwoot-origin events (mirrored incoming, operator relay, etc.) are
explicitly excluded so they do not inadvertently reset the window.

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


def _norm_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    return f"+{digits}"


def _payload_has_inbound_from(payload: dict[str, Any], phone_e164: str) -> bool:
    """Return True if payload contains a Meta messages entry with from==phone_e164."""
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
                from_phone = _norm_phone(msg.get("from"))
                if from_phone == phone_e164:
                    return True
    return False


async def get_last_meta_inbound_at(
    session: AsyncSession,
    phone_e164: str,
    before: datetime,
) -> datetime | None:
    """Return received_at of the most recent Meta-origin inbound from phone_e164.

    Only considers events within the last 26 hours before `before` to avoid
    a full table scan.  Excludes Chatwoot-origin events (dedupe_key starts with
    'chatwoot:', payload contains '_chatwoot' or '_chatwoot_operator_relay',
    or chatwoot_conversation_id is not None).
    """
    cutoff = before - timedelta(hours=_QUERY_LOOKBACK_HOURS)

    stmt = (
        select(WhatsAppEvent)
        .where(WhatsAppEvent.received_at >= cutoff)
        .where(WhatsAppEvent.received_at <= before)
        .where(WhatsAppEvent.chatwoot_conversation_id.is_(None))
        .where(not_(WhatsAppEvent.payload.has_key("_chatwoot")))
        .where(not_(WhatsAppEvent.payload.has_key("_chatwoot_operator_relay")))
        .where(not_(WhatsAppEvent.dedupe_key.like("chatwoot:%")))
        .order_by(WhatsAppEvent.received_at.desc())
    )
    res = await session.execute(stmt)
    events = list(res.scalars().all())

    for event in events:
        if _payload_has_inbound_from(event.payload or {}, phone_e164):
            return event.received_at

    return None


async def is_whatsapp_customer_window_open(
    session: AsyncSession,
    phone_e164: str,
    now: datetime,
) -> tuple[bool, datetime | None]:
    """Return (window_open, last_meta_inbound_at).

    window_open is True iff the customer sent a real Meta-origin message within
    the last 24 hours (inclusive boundary: exactly 24 h counts as open).
    """
    last_inbound = await get_last_meta_inbound_at(session, phone_e164, before=now)
    if last_inbound is None:
        return False, None

    if last_inbound.tzinfo is None:
        last_inbound = last_inbound.replace(tzinfo=timezone.utc)

    window_open = (now - last_inbound) <= timedelta(hours=_WINDOW_HOURS)
    return window_open, last_inbound
