"""The one protocol for projecting a consent refusal onto a legacy card (§46.4.3).

Why this module exists
----------------------
``Client.wa_opted_out`` is what ``message_planner``, the outbox worker and every
campaign path actually read. The durable ledger is the authority, but a
consumer that reads the flag is only safe while the two agree — and a card can
start matching a refused address at any moment, long after any consent decision
was made. Importing a client whose phone changed, adopting a proven EasyWeek
UUID, creating a first card for somebody: each of those can put a brand new row
in front of a refusal that is already on disk.

Locking the addresses was not enough and could not be. A consent lock only
orders the transactions that take it, and the client importer does not; it has
no reason to know this phase exists. A projection that locked the card rows it
could find, took the consent locks and then wrote only those rows therefore left
exactly one hole: a card moved onto the address in between was excluded from the
write, the ledger refused, the flag stayed ``false``, and
``compute_excluded_reason`` went on allowing marketing to somebody who had said
stop.

The protocol
------------
So the obligation is turned around. **Whoever writes a card's phone converges
that card itself**, in the same transaction, before it commits:

1. the writer already holds the row it is writing;
2. it calls :func:`converge_cards_with_ledger`, which takes the consent locks
   for the addresses involved — always the LAST locks a card writer takes;
3. under those locks it reads the authoritative ledger;
4. where the ledger refuses, it sets the flag on the rows it was given.

A card that appears after a projection has read its set is then not the
projection's problem: its own writer is holding it, and that writer cannot
commit until it has asked the ledger. If the projection is mid-flight the writer
queues on the consent lock and reads the settled answer afterwards.

Lock order, and why the old deadlock cannot come back
----------------------------------------------------
Every participant takes **all of its card row locks before its first consent
lock**: the importer writes its row and then calls this; the EasyWeek UUID
bridge locks its rows, does its business write and then binds and converges; the
two consent projections lock the rows they will write, take the consent locks,
and write only those rows. No transaction anywhere asks for a card row while
holding a consent lock, so the cycle that once made the projection a deadlock
victim has no second edge.

What this never does
--------------------
It only ever moves a card TO refusing. It never clears a refusal — lifting one
needs the proven, strictly newer opt-in the ledger alone can establish, and that
projection belongs to the inbox worker. It never restamps a card that already
refuses, because ``wa_opted_out_at`` and ``wa_opt_out_reason`` describe when and
why somebody refused rather than when a writer noticed. It makes no HTTP
request, so the consent locks it takes are never held across one.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Collection, Final, Iterable

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import (
    address_variants,
    ledger_state,
    lock_consent_addresses,
    normalize_wa_address,
)

logger = logging.getLogger("whatsapp_consent_cards")

# Why a card flag was set from here rather than from an inbound command. Kept
# apart from ``wa:stop`` on purpose: this is a projection of a refusal that was
# recorded elsewhere, and an operator reading the card should be able to tell.
REASON_LEDGER: Final = "wa:consent_ledger"


async def converge_cards_with_ledger(
    session: AsyncSession,
    *,
    addresses: Iterable[object],
    card_ids: Collection[int] | None = None,
    reason: str = REASON_LEDGER,
    now: datetime | None = None,
) -> int:
    """Set ``wa_opted_out`` where the ledger refuses, under the consent locks.

    ``addresses`` are the numbers whose refusal applies; ``card_ids`` the rows
    the caller is holding and is allowed to write. ``None`` means "every card
    matching those addresses", which is only safe for a caller that holds no
    consent lock yet and takes its row locks here — the two consent projections
    pass an explicit set for exactly that reason.

    Returns how many card flags were changed, for the caller's own reporting.
    """
    normalised = sorted({value for value in (normalize_wa_address(raw) for raw in addresses) if value})
    if not normalised:
        return 0
    if card_ids is not None and not card_ids:
        return 0

    # The consent locks, last of all. Sorted, so two callers never cross.
    locked = await lock_consent_addresses(session, normalised)
    moment = now or utcnow()
    changed = 0
    for address in locked:
        state = await ledger_state(session, phone=address)
        if not state.refuses:
            # No proven refusal for this number. Nothing is written in the other
            # direction: clearing a flag needs the ledger's own proven opt-in,
            # and that projection is the inbox worker's.
            continue
        statement = (
            update(Client)
            .where(Client.phone_e164.in_(address_variants(address)))
            .where(Client.wa_opted_out.is_(False))
            .values(wa_opted_out=True, wa_opted_out_at=moment, wa_opt_out_reason=reason)
        )
        if card_ids is not None:
            statement = statement.where(Client.id.in_(sorted(card_ids)))
        result = await session.execute(statement)
        changed += int(getattr(result, "rowcount", 0) or 0)
    if changed:
        logger.info("consent_cards: %s card flag(s) brought into line with the ledger", changed)
    return changed


# Where a transaction accumulates the cards it has written, until its consent
# section runs. A plain dict on ``Session.info``, which lives exactly as long as
# the session does and costs nothing to read.
_PENDING_KEY: Final = "whatsapp_consent_cards_pending"


def note_card_write(session: AsyncSession, *, card_id: int, phone: object) -> None:
    """Record that this transaction wrote a card's phone. No lock, no query.

    The counterpart of :func:`settle_card_writes`, and the reason the pair
    exists rather than one call at the write site: both providers' event
    handlers make external HTTP requests AFTER writing a card and before they
    commit — Altegio looks up the service category, EasyWeek reads the
    multi-service snapshot — and a consent lock taken at the write would be held
    across those calls. A STOP arriving for the same number then waits for a CRM
    request to finish before its receipt can even be stored, which is the one
    thing the durable inbound boundary exists to prevent.

    So the write site only remembers, locally, and the transaction's single
    consent section runs once everything external is done.
    """
    address = normalize_wa_address(phone)
    if address is None:
        return
    pending: dict[str, set[int]] = session.info.setdefault(_PENDING_KEY, {})
    pending.setdefault(address, set()).add(int(card_id))


def noted_addresses(session: AsyncSession) -> list[str]:
    """The addresses this transaction has written a card for, so far."""
    return sorted(session.info.get(_PENDING_KEY) or {})


async def settle_card_writes(
    session: AsyncSession,
    *,
    extra_addresses: Iterable[object] = (),
    reason: str = REASON_LEDGER,
    now: datetime | None = None,
) -> int:
    """The transaction's ONE consent section. Call it after every external call.

    Takes every consent address lock this transaction needs in a **single
    sorted acquisition** — the cards it wrote plus ``extra_addresses``, which is
    how a caller that also binds a refusal to a UUID declares those addresses —
    and only then converges the cards.

    One acquisition is the point. Sorting inside each separate helper call was
    not enough: the UUID path's nested client upsert took the proven number's
    lock while the resolver later asked for the old number's, and an ingress
    holding the old one and waiting for the proven one closed the cycle. A
    single sorted set per transaction has no such pair.

    Consent locks are therefore the LAST locks any write transaction takes, and
    nothing external happens after this call — the transaction commits.
    """
    pending: dict[str, set[int]] = session.info.pop(_PENDING_KEY, {}) or {}
    extras = {value for value in (normalize_wa_address(raw) for raw in extra_addresses) if value}
    addresses = sorted(set(pending) | extras)
    if not addresses:
        return 0

    # The single acquisition, before anything is read or written below. Every
    # ``converge_cards_with_ledger`` call that follows re-takes a lock this
    # transaction already holds, which PostgreSQL counts rather than queues.
    await lock_consent_addresses(session, addresses)
    changed = 0
    for address in sorted(pending):
        changed += await converge_cards_with_ledger(
            session,
            addresses=[address],
            card_ids=pending[address],
            reason=reason,
            now=now,
        )
    return changed


__all__ = [
    "REASON_LEDGER",
    "converge_cards_with_ledger",
    "note_card_write",
    "noted_addresses",
    "settle_card_writes",
]
