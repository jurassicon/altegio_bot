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

Three consequences worth stating:

* every row is namespaced by ``chatwoot_scope_id`` — the installation, account and
  generation the ids belong to. Chatwoot message and conversation ids only mean
  something inside one installation and one account, so a row from another
  installation (or from the previous generation of this one) is refused even when
  its wamid and its numeric ids match exactly. A row with no scope at all is a
  legacy row and is never evidence;
* the write is idempotent within a scope (``ON CONFLICT DO NOTHING`` on
  ``(chatwoot_scope_id, provider_message_id)``). The first successful Chatwoot
  response for that installation wins. Because the key includes the scope, the
  same wamid can legitimately hold a DIFFERENT Chatwoot message id in a different
  installation, and neither hides the other;
* the lookup is version-pinned. A row whose ``marker_version`` is not the one the
  caller expects is ignored, so bumping the marker contract retires old rows
  instead of silently trusting a different shape.

Isolation
---------
The lookup requires an exact match on all four of ``chatwoot_scope_id``,
``provider_message_id``, ``chatwoot_conversation_id`` and ``marker_version``.

Within one installation the conversation IS the tenant boundary — a Chatwoot
conversation belongs to exactly one inbox, and the inbox map binds an inbox to
exactly one provider/company pair — and it is the same boundary the legacy scan
enforces. The stored route / inbox / tenant columns are descriptive provenance for
ops, never the gate.

The scope itself is composed once by the ``ChatwootClient`` that actually talks to
Chatwoot (:meth:`ChatwootClient.scope_id`), and both sides read it from that
object, so the writer and the reader cannot normalize it differently. A ``None``
scope means the registry is unavailable: nothing is written, every read is a miss.
It is never a secret — the API token is not part of it.

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
    chatwoot_scope_id: str | None,
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

    Idempotent within the scope: a row for this ``(scope, wamid)`` already existing
    is a success, not an error, and its stored ids are left untouched. Returns
    ``False`` when the inputs are not a complete, usable proof — including when no
    valid ``chatwoot_scope_id`` is configured, because an unscoped row could later
    be mistaken for one belonging to a different installation. There is nothing to
    guess at, so nothing is written.

    The caller is expected to treat a ``False`` (or an exception it catches) as a
    non-event: the mirror note itself was still posted, and the reaction path
    degrades to the visible quote.
    """
    scope = (chatwoot_scope_id or "").strip()
    wamid = (provider_message_id or "").strip()
    message_id = _positive_int(chatwoot_message_id)
    conversation_id = _positive_int(chatwoot_conversation_id)
    if not scope or not wamid or message_id is None or conversation_id is None:
        return False
    if chatwoot_route not in {"tenant", "general"}:
        return False

    stmt = (
        pg_insert(ChatwootOutboundMirror)
        .values(
            chatwoot_scope_id=scope,
            provider_message_id=wamid,
            chatwoot_message_id=message_id,
            chatwoot_conversation_id=conversation_id,
            marker_version=marker_version,
            chatwoot_route=chatwoot_route,
            chatwoot_inbox_id=_positive_int(chatwoot_inbox_id),
            tenant_provider=tenant_provider,
            company_id=company_id if isinstance(company_id, int) and not isinstance(company_id, bool) else None,
        )
        # Keyed by (scope, wamid), so a concurrent or replayed write inside one
        # installation is a no-op rather than a constraint error the caller would
        # have to interpret — while the SAME wamid in another installation is a
        # separate row, not a hidden conflict.
        .on_conflict_do_nothing(constraint="uq_chatwoot_outbound_mirror_scope_provider_message")
    )
    await session.execute(stmt)
    return True


async def find_recorded_mirror_message_id(
    session: AsyncSession,
    *,
    chatwoot_scope_id: str | None,
    provider_message_id: str | None,
    chatwoot_conversation_id: Any,
    marker_version: str,
) -> int | None:
    """The recorded Chatwoot message id for this wamid, in THIS scope and conversation.

    One indexed read, no history walk. All four of scope, wamid, conversation and
    marker version must match exactly.

    Returns ``None`` — a miss the caller must answer with the legacy scan and then
    the visible quote — when no valid scope is configured, when no link was
    recorded (a note created before this registry existed, or a mirror whose
    Chatwoot post failed), when the row carries no scope at all (a legacy row,
    which is never assumed to belong to the current installation), when it belongs
    to another installation, account or generation, when it belongs to another
    conversation, or when it was written under a different marker contract version.
    """
    scope = (chatwoot_scope_id or "").strip()
    wamid = (provider_message_id or "").strip()
    conversation_id = _positive_int(chatwoot_conversation_id)
    if not scope or not wamid or conversation_id is None:
        return None

    stmt = select(ChatwootOutboundMirror.chatwoot_message_id).where(
        # An unscoped legacy row can never satisfy this equality, so it fails
        # closed without a special case.
        ChatwootOutboundMirror.chatwoot_scope_id == scope,
        ChatwootOutboundMirror.provider_message_id == wamid,
        ChatwootOutboundMirror.chatwoot_conversation_id == conversation_id,
        ChatwootOutboundMirror.marker_version == marker_version,
    )
    return _positive_int((await session.execute(stmt)).scalar_one_or_none())
