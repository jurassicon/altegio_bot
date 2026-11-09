"""Delivery evidence about the FOLLOW-UP's own messages (§46.3, §46.5, PR-22.3).

Narrow routing, and only narrow routing
---------------------------------------
A callback reaches a follow-up row through three things at once:

* the EXACT ``provider_message_id`` this row recorded when Meta accepted its one
  request. Never a phone number, never a batch-wide timestamp, never proximity;
* the sender and recipient digests the row FROZE with its claim. A callback
  naming another route is about another message and is kept as ``conflicting``;
* the signature the ingress verified over the raw body. An unsigned callback is
  an unattributed claim — anybody who can reach the webhook can make one — so it
  proves nothing and changes nothing.

All three, or the fact is stored and marked rather than acted on.

What it never touches
---------------------
The ORIGINAL message. Its status, its counters, its receipts, its failures, its
booking report and that report's anchor are another phase's record of another
message, and §46.5 keeps the two apart: the follow-up has its own statuses and
its own row. Nothing in this module writes to any production table.

Precedence, and contradictions
------------------------------
``read`` outranks ``delivered`` and both outrank a late ``failed``. A failure
arriving after either is kept — an operator should be able to see the
provider contradicting itself — and the row's success is not rolled back; only
``contradiction_count`` moves. Two deliveries of the same callback are one fact,
because the key is computed from the callback's proven contents and not from
when it was processed.

The early callback
------------------
Meta can deliver a ``read`` while the transaction recording the message id is
still in flight. "Nothing owns this id" is therefore an ordinary momentary state
and not evidence that the callback is foreign, so a SIGNED early callback is
kept as ``unmatched`` and bound by the very commit that makes it bindable — on
the exact identifier, under the identifier's advisory lock. An unsigned one is
not kept at all: authenticity cannot be established after the fact and no later
binding may promote one.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_production.delivery_evidence import lock_message_identity
from altegio_bot.models.models import (
    FOLLOWUP_CALLBACK_CONFIRMED,
    FOLLOWUP_CALLBACK_CONFLICTING,
    FOLLOWUP_CALLBACK_DELIVERED,
    FOLLOWUP_CALLBACK_FAILED,
    FOLLOWUP_CALLBACK_KINDS,
    FOLLOWUP_CALLBACK_READ,
    FOLLOWUP_CALLBACK_UNMATCHED,
    FOLLOWUP_SEND_ACCEPTED,
    FOLLOWUP_SEND_CLAIMED,
    FOLLOWUP_SEND_DELIVERED,
    FOLLOWUP_SEND_FAILED,
    FOLLOWUP_SEND_READ,
    FOLLOWUP_SEND_UNKNOWN,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    EasyWeekVoucherFollowupCallback,
    EasyWeekVoucherFollowupSend,
)
from altegio_bot.services.meta_error_classifier import parse_meta_error_code
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import recipient_route_digest, sender_route_digest

logger = logging.getLogger("easyweek_voucher_followup_callbacks")

__all__ = [
    "CALLBACK_FAILED_REASON",
    "CONFLICT_AFTER_SUCCESS",
    "CONFLICT_FOREIGN_ROUTE",
    "CONFLICT_ROUTE_UNVERIFIABLE",
    "CONFLICT_SIGNATURE_UNPROVEN",
    "SKIPPED_NOT_OURS",
    "SKIPPED_NO_MESSAGE_ID",
    "CallbackOutcome",
    "bind_unmatched_locked",
    "lock_followup_message",
    "record_followup_callback",
    "record_unmatched_followup_callback",
]

_FACT_DOMAIN: Final = "altegio_bot/easyweek_voucher_followup/callback/v1"

# Why a stored callback is not evidence about the row it names.
CONFLICT_FOREIGN_ROUTE: Final = "foreign_sender_or_recipient"
CONFLICT_ROUTE_UNVERIFIABLE: Final = "route_not_named_by_the_callback"
CONFLICT_SIGNATURE_UNPROVEN: Final = "callback_signature_unproven"
CONFLICT_AFTER_SUCCESS: Final = "failed_after_delivered_or_read"

# Why nothing was written.
SKIPPED_NOT_OURS: Final = "not_a_followup_message"
SKIPPED_NO_MESSAGE_ID: Final = "callback_without_message_id"

# What a row says about itself once a proven ``failed`` callback has moved it.
CALLBACK_FAILED_REASON: Final = "followup_meta_callback_failed"

# Rank, so a callback can only ever move a row forwards. ``accepted`` is where a
# row sits after its POST; ``unknown`` is deliberately rankable too, because a
# status can arrive for a message whose answer was lost — and that status is
# better news than the unknown, not worse.
_RANK: Final[dict[str, int]] = {
    FOLLOWUP_SEND_CLAIMED: 0,
    FOLLOWUP_SEND_UNKNOWN: 1,
    FOLLOWUP_SEND_FAILED: 2,
    FOLLOWUP_SEND_ACCEPTED: 3,
    FOLLOWUP_SEND_DELIVERED: 4,
    FOLLOWUP_SEND_READ: 5,
}

_KIND_TO_STATUS: Final[dict[str, str]] = {
    FOLLOWUP_CALLBACK_DELIVERED: FOLLOWUP_SEND_DELIVERED,
    FOLLOWUP_CALLBACK_READ: FOLLOWUP_SEND_READ,
    FOLLOWUP_CALLBACK_FAILED: FOLLOWUP_SEND_FAILED,
}


async def lock_followup_message(session: AsyncSession, provider_message_id: str) -> None:
    """Serialize everything that happens about ONE Meta message identifier.

    Deliberately the SAME advisory lock the production evidence takes, in the
    same domain. The WhatsApp status worker already holds it for every
    identifier a delivery names — ``lock_callback_scope`` takes them all up
    front, before any header — so a follow-up callback inherits the lock rather
    than introducing a second one that could be taken in the other order.

    What it closes is the window between "no row owns this identifier" and
    keeping the callback for later: the acceptance that records the identifier
    and the callback that names it both look for something the other has not
    committed, and the lock makes whichever commits first visible to the second.
    """
    await lock_message_identity(session, provider_message_id)


def _fact_key(parts: tuple[str, ...]) -> str:
    """A stable identity for one FACT, not for one delivery of it.

    Built from the callback's proven contents — kind, identifier, route, the
    event's own time, the numeric code — and never from when it was processed.
    So the same callback delivered three times is one row, an early one and the
    late duplicate of it are one row, and two genuinely different callbacks
    about one message are two rows and both are kept.
    """
    material = "|".join(parts)
    return hashlib.sha256(f"{_FACT_DOMAIN}:{material}".encode("utf-8")).hexdigest()


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else "none"


async def _insert_fact(session: AsyncSession, values: dict[str, Any]) -> int | None:
    """Insert one callback fact, idempotently on ``fact_key``.

    ``ON CONFLICT DO NOTHING`` rather than read-then-write: two deliveries can
    be processed concurrently, and losing the race on a unique index must be a
    no-op rather than an exception that fails a whole batch of statuses.
    """
    stmt = (
        pg_insert(EasyWeekVoucherFollowupCallback)
        .values(**values)
        .on_conflict_do_nothing(index_elements=["fact_key"])
        .returning(EasyWeekVoucherFollowupCallback.id)
    )
    return (await session.execute(stmt)).scalars().first()


@dataclass(frozen=True)
class CallbackOutcome:
    """What one follow-up callback turned into.

    ``claimed`` is the one the WhatsApp status worker acts on: it means this
    callback has been accounted for here and must NOT fall through to the
    ordinary ``OutboxMessage`` path, whose matching is looser and which knows
    nothing about this phase's frozen route.
    """

    claimed: bool
    stored: bool = False
    match_state: str | None = None
    reason: str | None = None
    advanced: bool = False
    contradiction: bool = False


def _route_verdict(
    row: EasyWeekVoucherFollowupSend,
    *,
    observed_sender_digest: str | None,
    observed_recipient_digest: str | None,
) -> tuple[bool, str | None]:
    """``(route_proven, conflict)`` for one observed route.

    A follow-up row that reached ``accepted`` always froze both halves of its
    route — the CHECK constraint requires it of every spent attempt — so unlike
    the historical production attempts there is no "froze nothing" case here. A
    callback that names no route therefore cannot be checked against one we did
    freeze, and the stricter check must not be bypassable by omitting fields.
    """
    frozen_sender = row.sender_digest
    frozen_recipient = row.recipient_digest
    if frozen_sender is None or frozen_recipient is None:  # pragma: no cover - guarded by a CHECK
        return False, CONFLICT_ROUTE_UNVERIFIABLE
    if observed_sender_digest is None or observed_recipient_digest is None:
        return False, CONFLICT_ROUTE_UNVERIFIABLE
    if frozen_sender != observed_sender_digest or frozen_recipient != observed_recipient_digest:
        return False, CONFLICT_FOREIGN_ROUTE
    return True, None


async def _locate_send(
    session: AsyncSession,
    provider_message_id: str,
    *,
    for_update: bool = False,
) -> EasyWeekVoucherFollowupSend | None:
    """The one follow-up row that owns this identifier, by exact equality.

    ``provider_message_id`` is UNIQUE table-wide across every follow-up run, so
    this resolves to exactly one row of exactly one run or to nothing.
    """
    stmt = select(EasyWeekVoucherFollowupSend).where(
        EasyWeekVoucherFollowupSend.provider_message_id == provider_message_id
    )
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


def _advance(row: EasyWeekVoucherFollowupSend, *, kind: str, event_at: datetime | None, now: datetime) -> bool:
    """Apply one PROVEN callback to the row, forwards only.

    The timestamps are written whenever the callback proves them, even when the
    status does not move: a ``delivered`` arriving after a ``read`` is a real
    fact about the same message and the report should be able to show both
    moments. Only the ``status`` field obeys the rank.
    """
    stamp = event_at or now
    target = _KIND_TO_STATUS[kind]
    if kind == FOLLOWUP_CALLBACK_DELIVERED and row.delivered_at is None:
        row.delivered_at = stamp
    if kind == FOLLOWUP_CALLBACK_READ and row.read_at is None:
        row.read_at = stamp
    if kind == FOLLOWUP_CALLBACK_FAILED and row.failed_at is None:
        row.failed_at = stamp
    current = _RANK.get(str(row.status))
    if current is None or _RANK[target] <= current:
        # A duplicate, a callback that arrived out of order, or a late failure
        # after a success. Nothing about the status moves — §46.3's precedence
        # is that a proven success is never unsaid.
        return False
    row.status = target
    if target == FOLLOWUP_SEND_FAILED:
        row.reason_code = CALLBACK_FAILED_REASON
    row.updated_at = now
    return True


async def record_followup_callback(
    session: AsyncSession,
    *,
    status: dict[str, Any],
    kind: str,
    whatsapp_event_id: int | None = None,
    signature_verified: bool = False,
    observed_at: datetime | None = None,
) -> CallbackOutcome:
    """Record one ``delivered`` / ``read`` / ``failed`` about a follow-up message.

    Runs inside the CALLER's transaction — the WhatsApp status worker already
    holds a session for the whole callback batch — and takes the identifier's
    advisory lock before its own lookup, because the lookup is one half of the
    race the lock exists for.

    Returns ``claimed=True`` whenever the identifier belongs to this phase,
    whatever the fact turned out to be. A conflicting or unproven fact must not
    fall back to the generic handler.

    Deliberately NOT behind the follow-up send fence. These are facts about a
    message that has already gone out; dropping them because an operator has
    since closed the fence would corrupt the record of a send that really
    happened, and the fence exists to stop NEW effects.
    """
    if kind not in FOLLOWUP_CALLBACK_KINDS:
        raise ValueError("unsupported follow-up callback kind")
    now = observed_at or utcnow()
    provider_message_id = str(status.get("provider_message_id") or "").strip()
    if not provider_message_id:
        return CallbackOutcome(False, reason=SKIPPED_NO_MESSAGE_ID)

    await lock_followup_message(session, provider_message_id)
    with session.no_autoflush:
        row = await _locate_send(session, provider_message_id, for_update=True)
    if row is None:
        return CallbackOutcome(False, reason=SKIPPED_NOT_OURS)

    event_at = status.get("event_at") if isinstance(status.get("event_at"), datetime) else None
    observed_sender = sender_route_digest(status.get("phone_number_id"))
    observed_recipient = recipient_route_digest(status.get("recipient_id"))
    route_proven, conflict = _route_verdict(
        row,
        observed_sender_digest=observed_sender,
        observed_recipient_digest=observed_recipient,
    )
    error_code = parse_meta_error_code(status.get("error_code")) if kind == FOLLOWUP_CALLBACK_FAILED else None

    # A late failure after a proven success is a contradiction in the provider's
    # own account of one message. Kept, and it changes nothing.
    after_success = kind == FOLLOWUP_CALLBACK_FAILED and str(row.status) in {
        FOLLOWUP_SEND_DELIVERED,
        FOLLOWUP_SEND_READ,
    }
    if conflict is None and after_success:
        conflict = CONFLICT_AFTER_SUCCESS
    if conflict is None and not signature_verified:
        conflict = CONFLICT_SIGNATURE_UNPROVEN

    match_state = FOLLOWUP_CALLBACK_CONFIRMED if conflict is None else FOLLOWUP_CALLBACK_CONFLICTING
    inserted = await _insert_fact(
        session,
        {
            "fact_key": _fact_key(
                (
                    kind,
                    provider_message_id,
                    observed_sender or "none",
                    observed_recipient or "none",
                    _iso(event_at),
                    str(error_code if error_code is not None else "none"),
                )
            ),
            "kind": kind,
            "authenticity": INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED,
            "match_state": match_state,
            "send_id": int(row.id),
            "run_id": int(row.run_id),
            "source_batch_id": int(row.source_batch_id),
            "source_slot": int(row.source_slot),
            "provider_message_id": provider_message_id,
            "observed_sender_digest": observed_sender,
            "observed_recipient_digest": observed_recipient,
            "route_proven": bool(route_proven and conflict is None),
            "meta_error_code": error_code,
            "reason_code": conflict,
            "whatsapp_event_id": whatsapp_event_id,
            "observed_at": now,
            "event_at": event_at,
            "evidence": {"source": "status_callback"},
        },
    )
    if conflict is not None:
        if after_success:
            row.contradiction_count = int(row.contradiction_count or 0) + 1
            row.updated_at = now
            await session.flush()
        return CallbackOutcome(
            True,
            stored=inserted is not None,
            match_state=match_state,
            reason=conflict,
            contradiction=after_success,
        )

    advanced = _advance(row, kind=kind, event_at=event_at, now=now)
    await session.flush()
    return CallbackOutcome(
        True,
        stored=inserted is not None,
        match_state=match_state,
        advanced=advanced,
    )


async def record_unmatched_followup_callback(
    session: AsyncSession,
    *,
    status: dict[str, Any],
    kind: str,
    whatsapp_event_id: int | None = None,
    signature_verified: bool = False,
    observed_at: datetime | None = None,
) -> CallbackOutcome:
    """Keep a SIGNED callback that no follow-up row owns yet.

    The early-callback contract, exactly as PR-22.1 wrote it for the original
    mailing. A row is kept only while some follow-up row could still record a
    NEW message id — one that is ``claimed`` and has no identifier yet — because
    that is the only window in which a callback can name an identifier this
    phase is about to own. That gate decides whether to KEEP the callback and
    never which row it belongs to, so it matches nothing and proves nothing.

    The signature is required even here: it is the one part that cannot be
    established after the fact, and no later binding may promote an unsigned
    callback.
    """
    if kind not in FOLLOWUP_CALLBACK_KINDS:
        raise ValueError("unsupported follow-up callback kind")
    now = observed_at or utcnow()
    provider_message_id = str(status.get("provider_message_id") or "").strip()
    if not provider_message_id:
        return CallbackOutcome(False, reason=SKIPPED_NO_MESSAGE_ID)
    if not signature_verified:
        return CallbackOutcome(False, reason=CONFLICT_SIGNATURE_UNPROVEN)

    await lock_followup_message(session, provider_message_id)
    with session.no_autoflush:
        if await _locate_send(session, provider_message_id) is not None:
            # Owned after all, so the matched path is the right one and this
            # function must not write a row that would never be bound.
            return CallbackOutcome(False, reason=CONFLICT_ROUTE_UNVERIFIABLE)
        pending = await session.scalar(
            select(EasyWeekVoucherFollowupSend.id)
            .where(EasyWeekVoucherFollowupSend.status == FOLLOWUP_SEND_CLAIMED)
            .where(EasyWeekVoucherFollowupSend.provider_message_id.is_(None))
            .limit(1)
        )
    if pending is None:
        return CallbackOutcome(False, reason=SKIPPED_NOT_OURS)

    event_at = status.get("event_at") if isinstance(status.get("event_at"), datetime) else None
    observed_sender = sender_route_digest(status.get("phone_number_id"))
    observed_recipient = recipient_route_digest(status.get("recipient_id"))
    error_code = parse_meta_error_code(status.get("error_code")) if kind == FOLLOWUP_CALLBACK_FAILED else None
    inserted = await _insert_fact(
        session,
        {
            "fact_key": _fact_key(
                (
                    kind,
                    provider_message_id,
                    observed_sender or "none",
                    observed_recipient or "none",
                    _iso(event_at),
                    str(error_code if error_code is not None else "none"),
                )
            ),
            "kind": kind,
            "authenticity": INBOUND_AUTHENTICITY_SIGNED,
            "match_state": FOLLOWUP_CALLBACK_UNMATCHED,
            "send_id": None,
            "provider_message_id": provider_message_id,
            "observed_sender_digest": observed_sender,
            "observed_recipient_digest": observed_recipient,
            "route_proven": False,
            "meta_error_code": error_code,
            "whatsapp_event_id": whatsapp_event_id,
            "observed_at": now,
            "event_at": event_at,
            "evidence": {"source": "status_callback", "awaiting_binding": True},
        },
    )
    return CallbackOutcome(
        True,
        stored=inserted is not None,
        match_state=FOLLOWUP_CALLBACK_UNMATCHED,
    )


async def bind_unmatched_locked(
    session: AsyncSession,
    *,
    send_row: EasyWeekVoucherFollowupSend,
    provider_message_id: str,
    now: datetime,
) -> int:
    """Bind every kept callback for this identifier to the row that now owns it.

    Called by the commit that records the message id, inside that transaction
    and under the identifier's advisory lock — which the caller already holds,
    and which is what makes the binding race-free rather than merely likely.

    Each kept callback is judged exactly as it would have been had it arrived
    now: the frozen route is compared, the signature is already proven, the rank
    decides whether the row moves. A foreign route becomes ``conflicting`` and
    changes nothing; a late failure after a success becomes ``conflicting`` and
    only counts a contradiction. Order does not matter, because the rank is
    applied per callback and never walks the row backwards.
    """
    rows = list(
        (
            await session.execute(
                select(EasyWeekVoucherFollowupCallback)
                .where(EasyWeekVoucherFollowupCallback.provider_message_id == provider_message_id)
                .where(EasyWeekVoucherFollowupCallback.match_state == FOLLOWUP_CALLBACK_UNMATCHED)
                # Oldest first, so ``delivered`` before ``read`` in the ordinary
                # case; the rank makes the result the same either way.
                .order_by(EasyWeekVoucherFollowupCallback.event_at.asc().nullslast())
                .order_by(EasyWeekVoucherFollowupCallback.id.asc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    bound = 0
    for fact in rows:
        route_proven, conflict = _route_verdict(
            send_row,
            observed_sender_digest=fact.observed_sender_digest,
            observed_recipient_digest=fact.observed_recipient_digest,
        )
        after_success = str(fact.kind) == FOLLOWUP_CALLBACK_FAILED and str(send_row.status) in {
            FOLLOWUP_SEND_DELIVERED,
            FOLLOWUP_SEND_READ,
        }
        if conflict is None and after_success:
            conflict = CONFLICT_AFTER_SUCCESS
        fact.send_id = int(send_row.id)
        fact.run_id = int(send_row.run_id)
        fact.source_batch_id = int(send_row.source_batch_id)
        fact.source_slot = int(send_row.source_slot)
        fact.updated_at = now
        if conflict is not None:
            fact.match_state = FOLLOWUP_CALLBACK_CONFLICTING
            fact.reason_code = conflict
            fact.route_proven = False
            if after_success:
                send_row.contradiction_count = int(send_row.contradiction_count or 0) + 1
            continue
        fact.match_state = FOLLOWUP_CALLBACK_CONFIRMED
        fact.route_proven = bool(route_proven)
        fact.reason_code = None
        _advance(send_row, kind=str(fact.kind), event_at=fact.event_at, now=now)
        bound += 1
    if rows:
        await session.flush()
    return bound
