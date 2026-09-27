"""Durable WAMID → Chatwoot Message.id registry for outbound mirror notes.

An automatic (bot) WhatsApp send has no Chatwoot message of its own: it exists
there only as the private mirror note :meth:`ChatwootClient.mirror_outbound_as_note`
posts. When the client later reacts to that WhatsApp message, the worker has to
name the Chatwoot message the reaction replies to.

Trust model
-----------
The only thing trusted here is a record of an action we took ourselves: the side
that created the note writes the link after Chatwoot answered with a valid
message id. Nothing in this module searches Chatwoot, compares bodies, template
codes or timestamps, or picks a plausible candidate — a missing, conflicting or
not-yet-committed link is simply a miss, and the caller falls back to the visible
quote.

Two consequences worth stating:

* the write is idempotent on the wamid (``ON CONFLICT DO NOTHING``), because the
  wamid is globally unique per Meta message. The first successful Chatwoot
  response wins; a second note for the same wamid cannot overwrite it;
* the lookup is version-pinned. A row whose ``marker_version`` is not the one the
  caller expects is ignored, so bumping the marker contract retires old rows
  instead of silently trusting a different shape.

Isolation
---------
The lookup matches on ``(provider_message_id, chatwoot_conversation_id)``. The
conversation IS the isolation boundary — a Chatwoot conversation belongs to
exactly one inbox, and the inbox map binds an inbox to exactly one
provider/company pair — and it is the same boundary the legacy scan enforces, so
a link recorded for another conversation can never be used. The stored route /
inbox / tenant columns are descriptive provenance for ops, never the gate.

This module talks to ``altegio_bot``'s own PostgreSQL. It never touches
Chatwoot's database.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import ChatwootOutboundMirror

logger = logging.getLogger(__name__)


def _positive_int(value: Any) -> int | None:
    """A usable Chatwoot id: a positive integer and not a bool."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


async def record_outbound_mirror(
    session: AsyncSession,
    *,
    provider_message_id: str | None,
    chatwoot_message_id: Any,
    chatwoot_conversation_id: Any,
    marker_version: str,
    chatwoot_route: str,
    chatwoot_inbox_id: Any = None,
    tenant_provider: str | None = None,
    company_id: Any = None,
) -> bool:
    """Record the mirror note of one outbound wamid. Returns True when stored.

    Idempotent: a row for this wamid already existing is a success, not an error,
    and its stored ids are left untouched. Returns ``False`` when the inputs are
    not a complete, usable proof — there is nothing to guess at, so nothing is
    written.

    The caller is expected to treat a ``False`` (or an exception it catches) as a
    non-event: the mirror note itself was still posted, and the reaction path
    degrades to the visible quote.
    """
    wamid = (provider_message_id or "").strip()
    message_id = _positive_int(chatwoot_message_id)
    conversation_id = _positive_int(chatwoot_conversation_id)
    if not wamid or message_id is None or conversation_id is None:
        return False
    if chatwoot_route not in {"tenant", "general"}:
        return False

    stmt = (
        pg_insert(ChatwootOutboundMirror)
        .values(
            provider_message_id=wamid,
            chatwoot_message_id=message_id,
            chatwoot_conversation_id=conversation_id,
            marker_version=marker_version,
            chatwoot_route=chatwoot_route,
            chatwoot_inbox_id=_positive_int(chatwoot_inbox_id),
            tenant_provider=tenant_provider,
            company_id=company_id if isinstance(company_id, int) and not isinstance(company_id, bool) else None,
        )
        # The wamid is unique, so a concurrent or replayed write is a no-op
        # rather than a constraint error the caller would have to interpret.
        .on_conflict_do_nothing(constraint="uq_chatwoot_outbound_mirror_provider_message")
    )
    await session.execute(stmt)
    return True


async def find_recorded_mirror_message_id(
    session: AsyncSession,
    *,
    provider_message_id: str | None,
    chatwoot_conversation_id: Any,
    marker_version: str,
) -> int | None:
    """The recorded Chatwoot message id for this wamid in THIS conversation.

    One indexed read, no history walk. Returns ``None`` when no link was recorded
    (a note created before this registry existed, or a mirror whose Chatwoot post
    failed), when the link belongs to another conversation, or when it was written
    under a different marker contract version.
    """
    wamid = (provider_message_id or "").strip()
    conversation_id = _positive_int(chatwoot_conversation_id)
    if not wamid or conversation_id is None:
        return None

    stmt = select(ChatwootOutboundMirror.chatwoot_message_id).where(
        ChatwootOutboundMirror.provider_message_id == wamid,
        ChatwootOutboundMirror.chatwoot_conversation_id == conversation_id,
        ChatwootOutboundMirror.marker_version == marker_version,
    )
    return _positive_int((await session.execute(stmt)).scalar_one_or_none())
