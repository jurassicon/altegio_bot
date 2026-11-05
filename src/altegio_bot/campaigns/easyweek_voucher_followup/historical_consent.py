"""Was this person's historical refusal ever accounted for? (§46.4.4)

The gap this closes
-------------------
PR-22.1 made a refusal durable from the moment the ingress accepts it: a
receipt is written in the same transaction as the event, and the consent
aggregate reads the ledger, the cards and the historical recipient snapshot.
None of that reaches backwards. A WhatsApp event stored before that machinery
existed — one carrying a «стоп» the old keyword set could not even see — has no
receipt, no suppression row and no card flag. Every source the follow-up
consults is therefore silent about it, and the person reads as sendable.

§46.4.4 requires that backlog to be reconciled **before** a follow-up audience
is opened, and §46.4.3 is explicit that for an affected addressee an unproven
state is not permission. So this module answers one question, per preview:

    for the people this preview is about, is the stored-event backlog
    established — and does it hold a refusal nothing has accounted for?

What it does, and deliberately does not do
------------------------------------------
It READS. It writes nothing, it contacts nothing, it acknowledges nothing, it
projects nothing and it runs no reconciliation: §46.4.4 keeps the projection a
separate, explicit, idempotent operation with its own preview and audit, and
turning a page load into one would be exactly the hidden replay that section
rules out. A refusal it finds is reported as a blocking reason, not written to
the ledger — lifting or recording consent stays with the paths that own it, and
nothing here can clear a refusal, which still needs the proven newer START the
existing contract defines.

It is also bounded. The scan reads at most :data:`MAX_SCANNED_EVENTS` stored
events and only those that have **no receipts at all**, because an event with
receipts was accounted for by the live path the moment it arrived — pending
inbound, which is the half that is accounted for but not yet applied, is
already a hold in the consent aggregate. If the bound is reached the scan
reports itself incomplete and every candidate becomes ``unknown``: a partial
answer about a backlog is not an answer.

When a stored STOP is already answered
--------------------------------------
"The ledger does not refuse this number" is not the same as "this refusal was
never accounted for". A refusal can have been accounted for and then LIFTED, by
exactly the one thing §46.4.7 allows to lift it: a proven, strictly newer
explicit opt-in under the existing START contract. Treating that as an
unaccounted gap would make the backlog scan re-impose, on every preview, a
refusal the customer themself withdrew.

So the question asked of each found STOP is the writer's own:
:func:`...whatsapp_consent.stop_superseded_by_opt_in`, the predicate
:func:`...whatsapp_consent.apply_consent_decision` applies when a STOP arrives
against a row that is already opted in. One rule, one place, no second order
invented here and no comparison of arrival times — a delivery can arrive days
after the message it carries, so ``received_at`` proves nothing about what the
customer sent first. An unproven order, an equal time, an unknown time on
either side and a START that never became the decision in force all leave the
refusal standing, and the opt-in has to be on the SAME address: a START from
one number is not a subscription on another.

Why the addresses are passed in
-------------------------------
Narrowness is the point. §46.4.4 allows either a proven completed
reconciliation or a narrow completeness proof for the affected recipients, and
this is the second: the scan is interested only in the numbers this preview's
recipients are KNOWN BY, so one bounded pass serves the whole preview and
nobody else's backlog is examined.

Known by, not written to. §46.4.2–3 keep the refusal veto global and bound to
the person: a stored STOP sent from another card of the same proven EasyWeek
uuid — another branch's, or one whose number the card has since dropped — is
this person's refusal, and the live aggregate already spans exactly that set.
:meth:`HistoricalConsentScan.blocks_any` is therefore asked about the whole
aggregate rather than the outbound number alone. The expansion runs in one
direction only: a matching number can widen a ban and can never prove a
positive identity or a branch membership.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Collection, Final

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import (
    CONSENT_COMMAND_STOP,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
)
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import (
    address_variants,
    is_chatwoot_sourced,
    iter_payload_consent_commands,
    ledger_state,
    normalize_wa_address,
)

logger = logging.getLogger("easyweek_voucher_followup_historical_consent")

__all__ = ["MAX_SCANNED_EVENTS", "HistoricalConsentScan", "scan_historical_refusals"]

# The hard bound on one pass. Generous enough to cover the whole stored backlog
# of this deployment at the time of writing and small enough that a database
# which has grown past it says so instead of taking an unbounded amount of time
# on an operator's click.
MAX_SCANNED_EVENTS: Final = 50_000


@dataclass(frozen=True)
class HistoricalConsentScan:
    """What one bounded pass over the stored-event backlog established."""

    # The only licence to draw a negative conclusion. While this is False the
    # backlog is simply not established, for anybody.
    complete: bool
    scanned_events: int = 0
    truncated: bool = False
    # Normalised addresses whose stored STOP nothing has accounted for. One
    # entry means: this person really sent a refusal, and the ledger does not
    # refuse them.
    unaccounted: frozenset[str] = field(default_factory=frozenset)
    # Shape facts only, for the operator's screen and the stored snapshot.
    gaps: tuple[str, ...] = ()

    def blocks(self, address: object) -> bool:
        """Does this scan hold an unaccounted refusal for this number?"""
        normalised = normalize_wa_address(address)
        if normalised is None:
            return False
        return any(variant in self.unaccounted for variant in address_variants(normalised))

    def blocks_any(self, addresses: Collection[object]) -> bool:
        """Does it hold one for ANY number this person is known by?

        The question the preview asks. §46.4.2–3 make a refusal a fact about the
        person rather than about the number it was sent from, so the veto is
        taken against the whole proven aggregate — the outbound number, the
        numbers this uuid's other cards hold, another branch's card, an Altegio
        one. A match widens the ban and nothing else: it never reads back as
        proof that an address belongs to this customer.
        """
        return any(self.blocks(address) for address in addresses)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "scanned_events": int(self.scanned_events),
            "truncated": bool(self.truncated),
            "unaccounted_count": len(self.unaccounted),
            "gaps": list(self.gaps),
        }


async def scan_historical_refusals(
    session: AsyncSession,
    *,
    addresses: Collection[object],
    limit: int = MAX_SCANNED_EVENTS,
    now: datetime | None = None,
) -> HistoricalConsentScan:
    """One bounded, read-only pass over the unaccounted stored events.

    Read with the SAME extractor the live ingress uses, so a historical «стоп»
    is found by exactly the rule that will see the next live one rather than by
    a second, looser scan written for the backlog — and every message of every
    payload is read, not only the first.

    The exclusions mirror the reconciliation's own, for the same reasons:

    * a Chatwoot mirror or an operator relay is the same message Meta already
      delivered, so counting it would double one person's refusal;
    * an event the ingress marked ``ignored`` named no route this deployment
      accepts and was never acted on; projecting it here would turn a
      deliberate refusal into a block;
    * a historical START is never read as permission. Its authenticity cannot
      be recovered from a sanitised stored event, and §46.4.7 lets only a
      proven newer opt-in lift a refusal. So a START in the backlog neither
      blocks nor clears: it is simply not evidence.

    ``complete=False`` is returned for a truncated pass, and the caller must
    treat that as unknown for every candidate.
    """
    wanted: set[str] = set()
    for raw in addresses:
        normalised = normalize_wa_address(raw)
        if normalised is not None:
            wanted.update(address_variants(normalised))
    if not wanted:
        # Nothing to look for. Honest and complete: a pass with no question
        # has no unanswered part.
        return HistoricalConsentScan(complete=True)

    moment = now or utcnow()
    accounted = exists().where(WhatsAppInboundReceipt.whatsapp_event_id == WhatsAppEvent.id)
    rows = list(
        (
            await session.execute(
                select(
                    WhatsAppEvent.id,
                    WhatsAppEvent.dedupe_key,
                    WhatsAppEvent.payload,
                    WhatsAppEvent.received_at,
                    WhatsAppEvent.status,
                )
                .where(WhatsAppEvent.payload.is_not(None))
                # Events the live path already accounted for are not the
                # backlog. One receipt means the ingress wrote receipts for
                # every extractable message of that delivery.
                .where(~accounted)
                .order_by(WhatsAppEvent.id.asc())
                .limit(int(limit) + 1)
            )
        ).all()
    )

    truncated = len(rows) > int(limit)
    rows = rows[: int(limit)]
    gaps: list[str] = []
    if truncated:
        gaps.append(f"stored_events_truncated_at_{int(limit)}")

    # address → the PROVEN event times of the stored refusals sent from it.
    # ``None`` is a refusal whose own time the payload did not state, which no
    # opt-in can be ordered against and which therefore always stands.
    found: dict[str, list[datetime | None]] = {}
    for event_id, dedupe_key, payload, received_at, status in rows:
        if not isinstance(payload, dict) or not payload:
            gaps.append("stored_event_payload_not_object")
            continue
        if status == "ignored":
            continue
        event = WhatsAppEvent(dedupe_key=dedupe_key, payload=payload)
        if is_chatwoot_sourced(event, payload):
            continue
        observed = received_at or moment
        for draft in iter_payload_consent_commands(payload, observed_at=observed):
            if draft.command != CONSENT_COMMAND_STOP:
                continue
            if draft.wa_address not in wanted:
                continue
            # The event's OWN stated time, never its arrival time: a delivery
            # can arrive days after the message it carries.
            found.setdefault(draft.wa_address, []).append(draft.event_at)
            logger.info(
                "followup_historical_consent: stored stop found in event %s",
                int(event_id),
            )

    # A stored STOP is accounted for in exactly two ways, and only the second
    # one is new here:
    #
    # * the ledger REFUSES this address. The refusal is in force, the consent
    #   aggregate excludes that person on its own, and this module has nothing
    #   to add;
    # * the ledger holds a PROVEN, strictly newer opt-in on this same address
    #   that already answered this refusal. §46.4.7 lets exactly that lift a
    #   refusal, and the test is the writer's own predicate rather than a
    #   second order invented here.
    #
    # Anything else — no row at all, a row that is not a proven opt-in, an
    # opt-in that cannot be ordered against the refusal — is the §46.4.4 gap.
    unaccounted: set[str] = set()
    for address in sorted(found):
        state = await ledger_state(session, phone=address)
        if state.refuses:
            continue
        if all(state.supersedes_stop(moment) for moment in found[address]):
            continue
        unaccounted.update(address_variants(address))

    return HistoricalConsentScan(
        complete=not truncated,
        scanned_events=len(rows),
        truncated=truncated,
        unaccounted=frozenset(unaccounted),
        gaps=tuple(dict.fromkeys(gaps)),
    )
