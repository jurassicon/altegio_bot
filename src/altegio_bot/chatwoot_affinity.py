"""PR-7.2: which branch does this phone number actually belong to?

Chatwoot keeps one inbox per branch (Karlsruhe, Durlach, Rastatt) plus a
separate validated General inbox. Two flows need an answer to the same
question and must not answer it differently:

  * a customer's inbound text/reaction that carries no authoritative reply
    context — which branch inbox should it appear in?
  * an operator reply typed in the General inbox — which provider/company
    sender may carry it back to Meta?

Before this module the second flow had no answer at all: the operator relay
accepted only inboxes listed in ``CHATWOOT_INBOX_COMPANY_MAP``, so a reply
written in General terminated as ``operator_relay: inbox_mapping_missing``
and never reached the customer (production event 20794, 2026-08-13).

Evidence, in order
------------------
1. the last PROVEN tenant communication with this phone;
2. the client's own bookings — nearest future, else latest past;
3. a single unambiguous Client identity.

Identity is ALWAYS the pair ``(provider, company_id)``. EasyWeek and Altegio
share one integer space for company ids, so a numeric id on its own proves
nothing and is never compared across providers.

Fail-closed by construction. The result distinguishes "nothing known"
(NO_EVIDENCE — General is correct) from "several answers" (AMBIGUOUS) and
"the data contradicts itself" (INVALID). Only the first may fall back to
General; the other two block, because guessing a branch here means sending a
customer message from the wrong salon.

Nothing in this module returns or logs a phone number, a name, an e-mail, a
message body, a service title or a payload — only ids, providers, company ids
and stable reason codes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Final

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, MessageJob, OutboxMessage, Record, WhatsAppSender
from altegio_bot.webhooks.common import ChatwootTenantIdentity, chatwoot_tenant_identity

# `sent` means Meta accepted the message; a `failed` callback can still follow
# it. Only these two prove the customer actually got something from this
# branch — the same contract the delivery audit uses.
PROVEN_DELIVERY_STATUSES: Final[frozenset[str]] = frozenset({"delivered", "read"})

# An explicit General ACK (STOP/START, promo answers) is deliberately routed to
# General and says nothing about a branch. Neither does a bot row with no job
# behind it: without a MessageJob there is no provider-scoped identity to read.
GENERAL_ROUTE_MARKER: Final[str] = "general"


def _utcnow() -> datetime:
    """Local, like every worker here — importing one would risk a cycle."""
    return datetime.now(timezone.utc)


class AffinityOutcome(str, Enum):
    """Four distinct answers — never collapsed into "no branch"."""

    PROVEN = "proven"
    NO_EVIDENCE = "no_evidence"
    AMBIGUOUS = "ambiguous"
    INVALID = "invalid"


@dataclass(frozen=True)
class AffinityResult:
    """A tenant decision plus the stable technical reason behind it."""

    outcome: AffinityOutcome
    identity: ChatwootTenantIdentity | None = None
    source: str = ""
    reason: str = ""

    @property
    def is_proven(self) -> bool:
        return self.outcome is AffinityOutcome.PROVEN and self.identity is not None

    def as_safe_dict(self) -> dict[str, object]:
        """Only ids, providers, company ids and reason codes leave here."""
        return {
            "outcome": self.outcome.value,
            "provider": self.identity.provider if self.identity else None,
            "company_id": self.identity.company_id if self.identity else None,
            "source": self.source,
            "reason": self.reason,
        }


def _proven(identity: ChatwootTenantIdentity, source: str) -> AffinityResult:
    return AffinityResult(outcome=AffinityOutcome.PROVEN, identity=identity, source=source, reason="proven")


def _no_evidence(reason: str) -> AffinityResult:
    return AffinityResult(outcome=AffinityOutcome.NO_EVIDENCE, reason=reason)


def _ambiguous(source: str, reason: str) -> AffinityResult:
    return AffinityResult(outcome=AffinityOutcome.AMBIGUOUS, source=source, reason=reason)


def _invalid(source: str, reason: str) -> AffinityResult:
    return AffinityResult(outcome=AffinityOutcome.INVALID, source=source, reason=reason)


async def _identity_from_communication(
    session: AsyncSession,
    phones: list[str],
) -> AffinityResult | None:
    """The last delivery this phone provably received from a branch.

    Bot rows take their identity from ``MessageJob``, operator rows from
    ``WhatsAppSender`` — never from ``OutboxMessage.company_id`` alone, which
    carries no provider and so cannot separate an EasyWeek company from an
    Altegio one with the same number.
    """
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.phone_e164.in_(phones))
        .where(OutboxMessage.status.in_(PROVEN_DELIVERY_STATUSES))
        # Deterministic: real delivery time first, then the row id as the
        # tie-break. Never an unordered `.first()`.
        .order_by(OutboxMessage.sent_at.desc().nullslast(), OutboxMessage.id.desc())
        .limit(50)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    if not rows:
        return None

    # Walk newest-first and stop at the first row whose identity is PROVABLE.
    # Rows that prove nothing (General ACK, jobless bot row) are skipped rather
    # than treated as evidence or as a conflict.
    ranked: list[tuple[tuple[datetime | None, int], ChatwootTenantIdentity]] = []
    for row in rows:
        identity, verdict = await _identity_of_outbox(session, row)
        if verdict is not None:
            return verdict
        if identity is None:
            continue
        ranked.append(((row.sent_at, int(row.id)), identity))

    if not ranked:
        return None

    top_key = ranked[0][0]
    top_identities = {identity for key, identity in ranked if key == top_key}
    if len(top_identities) > 1:
        # Two rows at the very same instant AND id cannot both be "the last".
        return _ambiguous("communication", "conflicting_latest_communication")

    return _proven(ranked[0][1], "communication")


async def _identity_of_outbox(
    session: AsyncSession,
    row: OutboxMessage,
) -> tuple[ChatwootTenantIdentity | None, AffinityResult | None]:
    """(identity, blocking_result). Both None means "proves nothing"."""
    meta = row.meta if isinstance(row.meta, dict) else {}
    if meta.get("chatwoot_route") == GENERAL_ROUTE_MARKER:
        return None, None  # explicit General: routed there on purpose

    if row.message_source == "operator":
        if row.sender_id is None:
            return None, None
        sender = await session.get(WhatsAppSender, row.sender_id)
        if sender is None:
            return None, None
        identity = chatwoot_tenant_identity(sender.provider, sender.company_id)
        if identity is None:
            return None, _invalid("communication", "sender_identity_invalid")
        if row.company_id is not None and int(row.company_id) != int(sender.company_id):
            return None, _invalid("communication", "operator_outbox_sender_company_mismatch")
        return identity, None

    # Bot / lifecycle row: the job is the only provider-scoped source.
    if row.job_id is None:
        return None, None
    job = await session.get(MessageJob, row.job_id)
    if job is None:
        return None, None
    identity = chatwoot_tenant_identity(job.provider, job.company_id)
    if identity is None:
        return None, _invalid("communication", "job_identity_invalid")
    if row.company_id is not None and int(row.company_id) != int(job.company_id):
        return None, _invalid("communication", "outbox_job_company_mismatch")
    return identity, None


async def _identity_from_bookings(
    session: AsyncSession,
    clients: list[Client],
    *,
    now: datetime,
) -> AffinityResult | None:
    """Nearest future booking, else the latest past one.

    Records are read only through the clients that own them, so a phone number
    can never inherit a branch from somebody else's booking. Service category
    is deliberately not consulted: a customer whose booking is not eligible for
    notifications may still message the salon.
    """
    client_ids = [int(client.id) for client in clients]
    if not client_ids:
        return None

    stmt = (
        select(Record)
        .where(Record.client_id.in_(client_ids))
        .where(or_(Record.is_deleted.is_(False), Record.is_deleted.is_(None)))
        .where(Record.starts_at.is_not(None))
    )
    records = list((await session.execute(stmt)).scalars().all())
    if not records:
        return None

    by_client = {int(client.id): client for client in clients}
    future = sorted((r for r in records if r.starts_at >= now), key=lambda r: (r.starts_at, int(r.id)))
    past = sorted((r for r in records if r.starts_at < now), key=lambda r: (r.starts_at, int(r.id)), reverse=True)
    candidates = future or past
    if not candidates:
        return None

    top = candidates[0]
    tied = [r for r in candidates if r.starts_at == top.starts_at]

    identities: set[ChatwootTenantIdentity] = set()
    for record in tied:
        identity = chatwoot_tenant_identity(record.provider, record.company_id)
        if identity is None:
            return _invalid("booking", "record_identity_invalid")
        owner = by_client.get(int(record.client_id)) if record.client_id is not None else None
        if owner is None:
            return _invalid("booking", "record_client_missing")
        owner_identity = chatwoot_tenant_identity(owner.provider, owner.company_id)
        if owner_identity is None or owner_identity != identity:
            # A record must live in the same tenant as the client holding it.
            return _invalid("booking", "record_client_scope_mismatch")
        identities.add(identity)

    if len(identities) > 1:
        return _ambiguous("booking", "conflicting_top_booking")
    return _proven(next(iter(identities)), "booking")


def _identity_from_clients(clients: list[Client]) -> AffinityResult:
    identities: set[ChatwootTenantIdentity] = set()
    for client in clients:
        identity = chatwoot_tenant_identity(client.provider, client.company_id)
        if identity is None:
            return _invalid("client", "client_identity_invalid")
        identities.add(identity)

    if not identities:
        return _no_evidence("no_client")
    if len(identities) > 1:
        return _ambiguous("client", "multiple_client_tenants")
    return _proven(next(iter(identities)), "client")


async def resolve_tenant_affinity(
    session: AsyncSession,
    phones: list[str],
    *,
    now: datetime | None = None,
) -> AffinityResult:
    """The one entry point. Text, reactions and General relay all call this.

    *phones* is the caller's already-normalised set of E.164 variants; this
    module never normalises or logs them.
    """
    if not phones:
        return _no_evidence("no_phone")

    moment = now or _utcnow()

    from_communication = await _identity_from_communication(session, phones)
    if from_communication is not None:
        return from_communication

    clients = list((await session.execute(select(Client).where(Client.phone_e164.in_(phones)))).scalars().all())
    if not clients:
        return _no_evidence("no_client")

    from_bookings = await _identity_from_bookings(session, clients, now=moment)
    if from_bookings is not None:
        return from_bookings

    return _identity_from_clients(clients)
