"""Durable evidence that one original voucher send really failed (§46.3).

What this phase could already say, and what it could not
--------------------------------------------------------
The production ledger has always recorded ``delivered`` and ``read``: a webhook
naming a slot's exact message id moves that slot forward and the batch report
counts it. A ``failed`` callback had nowhere to land. It reached the WhatsApp
status worker, matched no ``OutboxMessage`` — production slots deliberately have
none — and was logged and dropped. And ``send_rejected`` could not be used as a
substitute, because the same state is written for a local parameter refusal that
never left the process.

So §46.3's third audience group had no evidence behind it. This module is that
evidence, and nothing else.

It is a fact, not a permission
------------------------------
Nothing here re-opens a slot. ``send_attempt_count <= 1`` is untouched, no
retry, no new POST, no job and no follow-up send exists in this file. A
confirmed failure says what happened to the one attempt that was made; whether
any later phase may act on it is a separate policy that does not live here.

Two things count as proof, and only two
---------------------------------------
* Meta refused the request itself, with its own error envelope, on a status it
  decides before acting on the message. This process watched that happen on its
  own request, so there is no signature to check and the link to the slot is the
  intent committed before the request left;
* a ``failed`` delivery status naming the exact message id of this slot, WITH
  the signature the ingress verified over the raw body AND a route matching the
  one that slot's single attempt froze.

The second one needs all three parts. An unsigned callback is an unattributed
claim — anybody who can reach the webhook can make it — and a slot whose attempt
froze no route gives nothing to compare a claimed route against. Either way the
fact is kept, as ``unproven`` rather than ``confirmed``, and marks nothing: "the
right message id and nothing contradicting it" is a weaker statement than proof,
and §46.3 asks for proof.

A timeout, a transport failure, a 429, a 409, a redirect, an unreadable 2xx, a
2xx without a message id, a missing callback, ``accepted``-only, a slot with no
attempt, a CREATE or PAY error, and a local parameter refusal are all UNKNOWN.
None of them writes a row here, and none of them is upgraded later.

Success is never rolled back
----------------------------
``read`` outranks ``delivered`` and both outrank a late ``failed``. A failure
arriving after either is stored as ``conflicting`` — kept, because an operator
should be able to see the contradiction — and changes nothing about the slot. A
callback naming a sender or a recipient that is not the frozen route of the
original attempt is stored the same way and likewise changes nothing.
"""

from __future__ import annotations

import hashlib
import logging
import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Iterable

from sqlalchemy import select
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import (
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_FAILURE_MATCH_CONFLICTING,
    DELIVERY_FAILURE_MATCH_UNMATCHED,
    DELIVERY_FAILURE_MATCH_UNPROVEN,
    DELIVERY_FAILURE_REASON_API_REJECTED,
    DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
    DELIVERY_FAILURE_SOURCE_CALLBACK,
    DELIVERY_FAILURE_SOURCE_META_API,
    DELIVERY_RECEIPT_KIND_DELIVERED,
    DELIVERY_RECEIPT_KIND_READ,
    DELIVERY_RECEIPT_KINDS,
    DELIVERY_RECEIPT_MATCH_CONFIRMED,
    DELIVERY_RECEIPT_MATCH_CONFLICTING,
    DELIVERY_RECEIPT_MATCH_UNMATCHED,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    VOUCHER_PRODUCTION_ITEM_DELIVERED,
    VOUCHER_PRODUCTION_ITEM_READ,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    EasyWeekVoucherProductionDeliveryReceipt,
)
from altegio_bot.services.meta_error_classifier import (
    DELIVERY_CLASS_UNKNOWN,
    classify_delivery_error_code,
    parse_meta_error_code,
)
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import recipient_route_digest, sender_route_digest

logger = logging.getLogger("easyweek_voucher_production_delivery_evidence")

_FACT_DOMAIN: Final = "altegio_bot/easyweek_voucher_production/delivery_failure/v1"

# Why a stored fact is not bound to the slot it names.
CONFLICT_FOREIGN_ROUTE: Final = "foreign_sender_or_recipient"
CONFLICT_AFTER_SUCCESS: Final = "failed_after_delivered_or_read"
# The attempt froze a route and this fact names none, so the two cannot be
# compared. Not the same as a historical attempt that froze nothing: there the
# absence is the honest state of an old row, while here it is a fact that
# declines to say which message it is about.
CONFLICT_ROUTE_UNVERIFIABLE: Final = "route_not_named_by_the_fact"

# Why a fact about the right slot is nevertheless not proof. Not conflicts —
# nothing contradicts them — but §46.3 asks a callback for two things, and a
# fact missing either of them is evidence of nothing.
UNPROVEN_CALLBACK_UNVERIFIED: Final = "callback_signature_unproven"
UNPROVEN_ROUTE_UNKNOWN: Final = "attempt_route_unknown"

# Why nothing was written.
SKIPPED_NOT_OURS: Final = "not_a_production_message"
SKIPPED_NO_MESSAGE_ID: Final = "callback_without_message_id"

# Domain-separated handle for the per-identifier lock below.
_MESSAGE_LOCK_DOMAIN: Final = "altegio_bot/easyweek_voucher_production/message_identity/v1"


async def lock_message_identity(session: AsyncSession, provider_message_id: str) -> None:
    """Serialize everything that happens about ONE Meta message identifier.

    This is the synchronisation protocol §46.3 needs, and the window it closes
    cannot be closed by re-reading. Two transactions are involved: the one that
    records a slot's message id and scans for facts that arrived early (A), and
    the one that handles a ``failed`` callback and inserts an unmatched fact
    when nobody owns the id yet (C). Both of them look for something the other
    has not committed, so without a shared lock this interleaving loses the
    fact for good::

        C: look for the slot  → not committed yet, so none
        A: look for unmatched facts → not committed yet, so none
        A: commit (message id now on disk)
        C: insert unmatched, commit (fact now on disk, bound to nothing)

    ``SELECT ... FOR UPDATE`` does not help: there is no row to lock in either
    direction. A transaction-scoped advisory lock on the identifier itself does,
    because it exists before the rows do. Whichever transaction takes it first
    commits first, and the second one then SEES what the first wrote: A finds
    the unmatched fact and binds it, or C finds the slot and judges the callback
    against it directly.

    Lock order is ``message → header → item`` on both sides, the same relative
    order the stage writers already use among themselves, so this adds no cycle.
    """
    # ``surrogatepass`` for the same reason the consent digests use it: a message
    # id arrives in a webhook body and may not be encodable, and a raise here
    # would be a 500 on a payload that cannot succeed on retry either.
    key = int.from_bytes(
        hashlib.sha256(f"{_MESSAGE_LOCK_DOMAIN}:{provider_message_id}".encode("utf-8", "surrogatepass")).digest()[:8],
        "big",
        signed=True,
    )
    await session.execute(sql_text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})


async def lock_callback_scope(
    session: AsyncSession,
    provider_message_ids: Iterable[object],
) -> list[str]:
    """Take every lock ONE callback transaction needs, in the one global order.

    Per-status locking was not enough, and the counterexample is an ordinary
    Meta delivery. One callback carries ``delivered`` for message A and
    ``failed`` for message B of the same batch. The handler took the header to
    record A's delivery, then asked for B's message lock — while the transaction
    that was accepting B held B's message lock and was waiting for that same
    header. A cycle, resolved by PostgreSQL aborting one of them, and the one it
    aborted was the acceptance: Meta had the message, the slot kept
    ``send_claimed`` with no identifier, and B's failure stayed unmatched.
    Sorting the failures after the header was already held could not help,
    because the header was the half taken first.

    So the whole scope is declared up front: every message identifier the
    payload names, in sorted order, and only then the production headers those
    identifiers belong to, in ascending id order. After this function no message
    lock is ever requested behind a header lock, so the acceptance either has
    its identifier — and this transaction waits for it holding no header — or it
    does not, and waits for this one. The global order is
    ``message → header → item`` on both sides, unchanged; what is new is that a
    transaction handling several messages takes ALL of its message locks at the
    start rather than one per message as it goes.

    The header pre-read is safe without a lock precisely because the identifiers
    are already held: nothing can bind one of them to a different slot while
    this transaction owns it. Identifiers belonging to no production slot lock
    nothing further, which is every ordinary campaign callback.
    """
    ordered = sorted({str(value).strip() for value in provider_message_ids if str(value or "").strip()})
    for provider_message_id in ordered:
        await lock_message_identity(session, provider_message_id)
    if not ordered:
        return ordered
    with session.no_autoflush:
        batch_ids = sorted(
            {
                int(value)
                for value in (
                    await session.scalars(
                        select(EasyWeekVoucherProductionBatchItem.batch_id).where(
                            EasyWeekVoucherProductionBatchItem.provider_message_id.in_(ordered)
                        )
                    )
                ).all()
            }
        )
    for batch_id in batch_ids:
        await session.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
    return ordered


@dataclass(frozen=True)
class FailureOutcome:
    """What one failure fact turned into.

    ``claimed`` is the one the WhatsApp status worker acts on: it means this
    callback has been accounted for here and must NOT fall through to the
    ordinary ``OutboxMessage`` path. That path matches on a looser rule and has
    no notion of this phase's frozen route, so letting a refused or conflicting
    fact fall back into it would be a way around the stricter check — which
    §46.3 forbids by name.
    """

    claimed: bool
    stored: bool
    match_state: str | None = None
    failure_class: str | None = None
    reason: str | None = None
    fact_id: int | None = None


def _fact_key(parts: tuple[str, ...]) -> str:
    """A stable identity for one failure FACT, not for one delivery of it.

    Built from the proven contents — source, identifier, class of event, the
    event's own time, the numeric code and the route it names — and deliberately
    NOT from when we happened to see it. So the same callback delivered three
    times is one row, while two genuinely different callbacks about one message
    are two rows and both are kept.
    """
    material = "|".join(parts)
    return hashlib.sha256(f"{_FACT_DOMAIN}:{material}".encode("utf-8")).hexdigest()


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else "none"


async def _insert_fact(
    session: AsyncSession,
    values: dict[str, Any],
    *,
    table: Any = EasyWeekVoucherProductionDeliveryFailure,
) -> int | None:
    """Insert one fact, idempotently on ``fact_key``.

    ``ON CONFLICT DO NOTHING`` rather than a read-then-write: two callbacks can
    be processed concurrently by two workers, and losing the race on a unique
    index must be a no-op rather than an exception that fails a whole batch of
    status updates.

    ``table`` defaults to the failure ledger, which every existing caller
    means; the positive-status receipts of §46.3 share the same key column and
    the same conflict rule, so they share this one insert rather than growing a
    second copy of it that could drift.
    """
    stmt = pg_insert(table).values(**values).on_conflict_do_nothing(index_elements=["fact_key"]).returning(table.id)
    return (await session.execute(stmt)).scalars().first()


async def _attempt_for(
    session: AsyncSession,
    item: EasyWeekVoucherProductionBatchItem,
) -> EasyWeekVoucherProductionBatchAttempt | None:
    """The one committed attempt row of this slot, by its intent.

    By ``outbound_intent_uuid``, which is unique table-wide and written in the
    same transaction that spent the slot's single attempt. Never by batch and
    slot alone: that would find a row for a slot whose attempt is not the one
    this callback is about.
    """
    if item.outbound_intent_uuid is None:
        return None
    return (
        await session.execute(
            select(EasyWeekVoucherProductionBatchAttempt).where(
                EasyWeekVoucherProductionBatchAttempt.intent_uuid == item.outbound_intent_uuid
            )
        )
    ).scalar_one_or_none()


def _route_verdict(
    attempt: EasyWeekVoucherProductionBatchAttempt | None,
    *,
    observed_sender_digest: str | None,
    observed_recipient_digest: str | None,
) -> tuple[bool, str | None]:
    """``(route_proven, conflict)`` for one observed route.

    Four outcomes, and the distinctions between them are the point:

    * both sides frozen, both observed, both equal → PROVEN;
    * both sides frozen and observed, either differs → FOREIGN. This fact is
      about somebody else's message and may not touch this slot;
    * both sides frozen, the fact names no route → UNVERIFIABLE. A real Meta
      status always carries its recipient and its sender metadata, so a fact
      without them cannot be checked against a route we did freeze — and the
      stricter check must not be bypassable by simply omitting the fields;
    * the attempt froze no route → neither proven nor in conflict. A historical
      attempt predates the frozen route, so its facts stay honestly unproven
      rather than being matched against whichever sender the configuration names
      today or whichever number the card now carries. The fact is still kept.
    """
    if attempt is None:
        return False, None
    frozen_sender = attempt.sender_phone_number_id_digest
    frozen_recipient = attempt.recipient_digest
    if frozen_sender is None or frozen_recipient is None:
        return False, None
    if observed_sender_digest is None or observed_recipient_digest is None:
        return False, CONFLICT_ROUTE_UNVERIFIABLE
    if frozen_sender != observed_sender_digest or frozen_recipient != observed_recipient_digest:
        return False, CONFLICT_FOREIGN_ROUTE
    return True, None


def _not_proven(
    *,
    source: str,
    signature_verified: bool,
    route_proven: bool,
) -> str | None:
    """Why this fact is not proof, or ``None`` when it is.

    The two sources have different proof contracts, and collapsing them would
    break one of them:

    * a CALLBACK is somebody else's claim arriving over the network. §46.3 asks
      it for both of the things that make it checkable — the HMAC the ingress
      verified over the raw body, and a route that matches the one this slot's
      one attempt froze. "Nothing contradicts it" is not either of those: an
      unsigned callback is unattributed, and a route nobody froze cannot be
      compared, so neither may mark a slot as a confirmed failure;
    * an API REJECTION was observed by this process on its OWN request. There is
      no webhook and therefore no signature to require, and the link to the
      attempt is the intent committed before the request left. Requiring a
      signature here would mean no real Meta rejection could ever be confirmed,
      and requiring a frozen route would silently drop every historical one.
    """
    if source != DELIVERY_FAILURE_SOURCE_CALLBACK:
        return None
    if not signature_verified:
        return UNPROVEN_CALLBACK_UNVERIFIED
    if not route_proven:
        return UNPROVEN_ROUTE_UNKNOWN
    return None


def _mark_item(
    item: EasyWeekVoucherProductionBatchItem,
    *,
    reason_code: str,
    failure_class: str,
    observed_at: datetime,
) -> None:
    """Record the confirmed failure on the slot, as safe evidence only.

    Deliberately does NOT move ``status``. The slot is wherever its own ladder
    left it — ``provider_accepted`` for an accepted message, ``send_rejected``
    for a refused request — and a confirmed failure is a fact BESIDE that, not a
    step backwards along it. Writing it as a status change would both regress the
    rank the ledger protects and lose the distinction between "Meta took the
    message and it then failed" and "Meta never took it".

    ``reconciliation_required`` is left alone too: this is a known, recorded
    outcome of a finished attempt, not an unresolved claim a human has to chase.
    """
    merged = dict(item.evidence or {})
    merged["confirmed_delivery_failure"] = True
    merged["confirmed_delivery_failure_reason"] = reason_code
    merged["confirmed_delivery_failure_class"] = failure_class
    merged["confirmed_delivery_failure_observed_at"] = observed_at.isoformat()
    item.evidence = merged


def _unmark_item(
    item: EasyWeekVoucherProductionBatchItem,
    *,
    kind: str,
    observed_at: datetime,
) -> None:
    """Withdraw a confirmed-failure mark a proven success has contradicted.

    The mark says "this slot's one attempt provably failed", and a proven
    ``delivered``/``read`` for the same message id says it provably did not —
    §46.3 is explicit that a success keeps its force against a `failed`,
    whichever of them this process happened to read first. So the mark goes.

    What does NOT go is the evidence. The failure row keeps its reason code, its
    class, its route and its times, re-stated as a conflict; and the withdrawal
    itself is written down here, so the slot says that a confirmation was made
    and then contradicted rather than quietly losing both halves.
    """
    merged = dict(item.evidence or {})
    withdrawn = merged.pop("confirmed_delivery_failure_reason", None)
    merged.pop("confirmed_delivery_failure", None)
    merged.pop("confirmed_delivery_failure_class", None)
    merged.pop("confirmed_delivery_failure_observed_at", None)
    merged["withdrawn_delivery_failure_reason"] = withdrawn
    merged["withdrawn_delivery_failure_by"] = kind
    merged["withdrawn_delivery_failure_at"] = observed_at.isoformat()
    item.evidence = merged


async def demote_confirmed_failures_after_success(
    session: AsyncSession,
    *,
    item: EasyWeekVoucherProductionBatchItem,
    provider_message_id: str,
    kind: str,
    observed_at: datetime | None = None,
) -> int:
    """A PROVEN success re-states every confirmed failure of the same message.

    One rule for both arrival orders, so the ledger ends in the same state
    whichever way round this process read the pair:

    * a `failed` read while the slot already carried a success was always
      recorded as a conflict, by the route check in :func:`_store`;
    * a `failed` read FIRST became ``confirmed`` and stamped the slot, and the
      success that followed advanced the slot and left that stamp standing. The
      same two facts, two different answers, and the wrong one asserted a
      delivery failure about a message the customer had demonstrably read.

    Called only where a success is PROVEN — a verified signature and a route
    equal to the one the slot's single attempt froze — which is what keeps an
    unsigned or foreign-route callback from clearing a real confirmation. The
    decision is precedence, not chronology: no event time is required and the
    row ids of two tables are never compared as clocks. Meta's own times are
    recorded for the audit when it states them.

    Runs in the CALLER's transaction, after the item lock is held, and takes no
    lock of its own beyond the rows it re-states. Returns how many it moved.
    """
    now = observed_at or utcnow()
    rows = list(
        (
            await session.scalars(
                select(EasyWeekVoucherProductionDeliveryFailure)
                .where(EasyWeekVoucherProductionDeliveryFailure.provider_message_id == provider_message_id)
                .where(EasyWeekVoucherProductionDeliveryFailure.item_id == int(item.id))
                .where(EasyWeekVoucherProductionDeliveryFailure.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED)
                .order_by(EasyWeekVoucherProductionDeliveryFailure.id.asc())
                .with_for_update()
            )
        ).all()
    )
    if not rows:
        return 0
    for row in rows:
        row.match_state = DELIVERY_FAILURE_MATCH_CONFLICTING
        merged = dict(row.evidence or {})
        merged["conflict"] = CONFLICT_AFTER_SUCCESS
        merged["superseded_by_status"] = kind
        merged["superseded_at"] = now.isoformat()
        if row.event_at is not None:
            merged["superseded_failure_event_at"] = row.event_at.isoformat()
        row.evidence = merged
        row.updated_at = now
    _unmark_item(item, kind=kind, observed_at=now)
    logger.info(
        "voucher_production_failure: %s superseded %s confirmed fact(s) batch=%s slot=%s",
        kind,
        len(rows),
        item.batch_id,
        item.slot,
    )
    return len(rows)


async def _locate_item(
    session: AsyncSession,
    provider_message_id: str,
) -> tuple[int, int] | None:
    """``(batch_id, slot)`` for a message id, read WITHOUT a lock.

    Same contract and same reason as the ledger's own locator: its only job is
    to learn which header to lock, because taking the item before the header
    would be the reverse of the order every stage writer uses. Nothing it
    returns is trusted — the row is found again under the locks.
    """
    found = (
        await session.execute(
            select(
                EasyWeekVoucherProductionBatchItem.batch_id,
                EasyWeekVoucherProductionBatchItem.slot,
            ).where(EasyWeekVoucherProductionBatchItem.provider_message_id == provider_message_id)
        )
    ).first()
    return (int(found[0]), int(found[1])) if found is not None else None


async def record_callback_failure(
    session: AsyncSession,
    *,
    status: dict[str, Any],
    whatsapp_event_id: int | None = None,
    signature_verified: bool = False,
    observed_at: datetime | None = None,
) -> FailureOutcome:
    """Record a ``failed`` delivery status for a production slot, if it is ours.

    Runs inside the CALLER's transaction — the WhatsApp status worker already
    holds a session for the whole callback batch, and opening a second one would
    put two connections on the same rows inside one logical unit of work.

    Returns ``claimed=True`` whenever the message id belongs to this phase,
    whatever the fact turned out to be. That is the point: a conflicting or
    unproven fact must not fall back to the ordinary ``OutboxMessage`` handler,
    whose matching is looser and which knows nothing about the frozen route.

    Not behind the §42 fence, for the same reason ``delivered``/``read`` are not:
    a failure is a fact about a message that was already sent, and dropping it
    because an operator has since closed the fence would silently corrupt the
    record of a mailing that really happened.
    """
    now = observed_at or utcnow()
    provider_message_id = str(status.get("provider_message_id") or "").strip()
    if not provider_message_id:
        return FailureOutcome(False, False, reason=SKIPPED_NO_MESSAGE_ID)

    # Before the lookup, not after: the lookup is one half of the race.
    await lock_message_identity(session, provider_message_id)

    with session.no_autoflush:
        located = await _locate_item(session, provider_message_id)
    if located is None:
        return FailureOutcome(False, False, reason=SKIPPED_NOT_OURS)

    batch_id, _slot = located
    header = await session.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
    if header is None:
        return FailureOutcome(False, False, reason=SKIPPED_NOT_OURS)
    item = (
        await session.execute(
            select(EasyWeekVoucherProductionBatchItem)
            .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
            .where(EasyWeekVoucherProductionBatchItem.provider_message_id == provider_message_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if item is None:
        # The unlocked lookup saw something this transaction does not.
        return FailureOutcome(False, False, reason=SKIPPED_NOT_OURS)

    attempt = await _attempt_for(session, item)
    return await _store(
        session,
        source=DELIVERY_FAILURE_SOURCE_CALLBACK,
        reason_code=DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
        item=item,
        attempt=attempt,
        provider_message_id=provider_message_id,
        intent_uuid=item.outbound_intent_uuid,
        error_code_raw=status.get("error_code"),
        http_status=None,
        event_at=status.get("event_at") if isinstance(status.get("event_at"), datetime) else None,
        observed_sender_digest=sender_route_digest(status.get("phone_number_id")),
        observed_recipient_digest=recipient_route_digest(status.get("recipient_id")),
        signature_verified=signature_verified,
        whatsapp_event_id=whatsapp_event_id,
        observed_at=now,
    )


async def record_unmatched_callback_failure(
    session: AsyncSession,
    *,
    status: dict[str, Any],
    whatsapp_event_id: int | None = None,
    signature_verified: bool = False,
    observed_at: datetime | None = None,
) -> FailureOutcome:
    """Keep a ``failed`` callback that NOBODY owns yet, on its identifier alone.

    Meta can deliver a status for a message id while the transaction recording
    that id is still in flight, so "no slot and no outbox row has this id" is an
    ordinary, momentary state rather than evidence that the callback is foreign.
    Dropping it would lose a real failure, which is why §46.3 requires an early
    callback to be matchable later instead.

    The caller reaches this only after both the production lookup and the
    ``OutboxMessage`` lookup have come back empty, so ordinary campaign traffic
    never lands here. Nothing about the slot is guessed: the row is ``unmatched``,
    names no item, and is bound only when an acceptance commits the exact same
    identifier. No phone similarity and no timestamp proximity is ever used.
    """
    now = observed_at or utcnow()
    provider_message_id = str(status.get("provider_message_id") or "").strip()
    if not provider_message_id:
        return FailureOutcome(False, False, reason=SKIPPED_NO_MESSAGE_ID)

    # Re-entrant: the caller reached here through ``record_callback_failure``,
    # which already holds this lock for this identifier, and the acceptance that
    # will bind this row cannot commit while it is held.
    await lock_message_identity(session, provider_message_id)

    error_code = parse_meta_error_code(status.get("error_code"))
    failure_class = classify_delivery_error_code(status.get("error_code"))
    event_at = status.get("event_at") if isinstance(status.get("event_at"), datetime) else None
    sender_digest = sender_route_digest(status.get("phone_number_id"))
    recipient_digest = recipient_route_digest(status.get("recipient_id"))
    fact_key = _fact_key(
        (
            DELIVERY_FAILURE_SOURCE_CALLBACK,
            provider_message_id,
            DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
            _iso(event_at),
            str(error_code) if error_code is not None else "none",
            sender_digest or "none",
            recipient_digest or "none",
        )
    )
    fact_id = await _insert_fact(
        session,
        {
            "fact_key": fact_key,
            "source": DELIVERY_FAILURE_SOURCE_CALLBACK,
            "reason_code": DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
            "match_state": DELIVERY_FAILURE_MATCH_UNMATCHED,
            "failure_class": failure_class,
            "authenticity": (INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED),
            "provider_message_id": provider_message_id,
            "observed_sender_digest": sender_digest,
            "observed_recipient_digest": recipient_digest,
            "route_proven": False,
            "meta_error_code": error_code,
            "observed_at": now,
            "event_at": event_at,
            "whatsapp_event_id": whatsapp_event_id,
            "evidence": {"awaiting_binding": True},
        },
    )
    return FailureOutcome(
        claimed=False,
        stored=fact_id is not None,
        match_state=DELIVERY_FAILURE_MATCH_UNMATCHED,
        failure_class=failure_class,
        fact_id=fact_id,
    )


async def _store(
    session: AsyncSession,
    *,
    source: str,
    reason_code: str,
    item: EasyWeekVoucherProductionBatchItem,
    attempt: EasyWeekVoucherProductionBatchAttempt | None,
    provider_message_id: str | None,
    intent_uuid: uuid_module.UUID | None,
    error_code_raw: object,
    http_status: int | None,
    event_at: datetime | None,
    observed_sender_digest: str | None,
    observed_recipient_digest: str | None,
    signature_verified: bool,
    whatsapp_event_id: int | None,
    observed_at: datetime,
) -> FailureOutcome:
    """Write one fact about one known slot, and decide what it is worth."""
    error_code = parse_meta_error_code(error_code_raw)
    failure_class = classify_delivery_error_code(error_code_raw)
    route_proven, conflict = _route_verdict(
        attempt,
        observed_sender_digest=observed_sender_digest,
        observed_recipient_digest=observed_recipient_digest,
    )

    if conflict is None and (item.delivered_at is not None or item.read_at is not None):
        conflict = CONFLICT_AFTER_SUCCESS

    unproven = (
        None
        if conflict
        else _not_proven(source=source, signature_verified=signature_verified, route_proven=route_proven)
    )
    if conflict:
        match_state = DELIVERY_FAILURE_MATCH_CONFLICTING
    elif unproven:
        match_state = DELIVERY_FAILURE_MATCH_UNPROVEN
    else:
        match_state = DELIVERY_FAILURE_MATCH_CONFIRMED
    evidence: dict[str, Any] = {"route_proven": route_proven}
    if conflict:
        evidence["conflict"] = conflict
    if unproven:
        evidence["unproven"] = unproven
    if attempt is not None and (attempt.sender_phone_number_id_digest is None or attempt.recipient_digest is None):
        # Named rather than inferred: a historical attempt has no frozen route,
        # so this fact is about the right slot and an unknown route — and no
        # later reader should read the absence as a match.
        evidence["historical_route_unknown"] = True

    fact_key = _fact_key(
        (
            source,
            provider_message_id or f"intent:{intent_uuid}",
            reason_code,
            _iso(event_at),
            str(error_code) if error_code is not None else "none",
            observed_sender_digest or "none",
            observed_recipient_digest or "none",
        )
    )
    fact_id = await _insert_fact(
        session,
        {
            "fact_key": fact_key,
            "source": source,
            "reason_code": reason_code,
            "match_state": match_state,
            "failure_class": failure_class,
            "authenticity": (INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED),
            "item_id": int(item.id),
            "attempt_id": int(attempt.id) if attempt is not None else None,
            "batch_id": int(item.batch_id),
            "slot": int(item.slot),
            "intent_uuid": intent_uuid,
            "provider_message_id": provider_message_id,
            "observed_sender_digest": observed_sender_digest,
            "observed_recipient_digest": observed_recipient_digest,
            "route_proven": route_proven,
            "meta_error_code": error_code,
            "http_status": http_status,
            "observed_at": observed_at,
            "event_at": event_at,
            "whatsapp_event_id": whatsapp_event_id,
            "evidence": evidence,
        },
    )
    if match_state == DELIVERY_FAILURE_MATCH_CONFIRMED and fact_id is not None:
        _mark_item(item, reason_code=reason_code, failure_class=failure_class, observed_at=observed_at)
    logger.info(
        "voucher_production_failure: source=%s match=%s class=%s batch=%s slot=%s stored=%s",
        source,
        match_state,
        failure_class,
        item.batch_id,
        item.slot,
        fact_id is not None,
    )
    return FailureOutcome(
        claimed=True,
        stored=fact_id is not None,
        match_state=match_state,
        failure_class=failure_class,
        reason=conflict or unproven,
        fact_id=fact_id,
    )


# ---------------------------------------------------------------------------
# §46.3 — the proof behind a POSITIVE status
# ---------------------------------------------------------------------------
_RECEIPT_DOMAIN: Final = "altegio_bot/easyweek_voucher_production/delivery_receipt/v1"


@dataclass(frozen=True)
class ReceiptOutcome:
    """Whether a positive status left proof behind, and why not when it did not."""

    stored: bool
    reason: str | None = None


def _receipt_fact_key(
    *,
    kind: str,
    provider_message_id: str,
    sender_digest: str | None,
    recipient_digest: str | None,
    event_at: datetime | None,
) -> str:
    """ONE key for both paths, so an early callback and a late one are one fact.

    Computed from the proven contents of the callback and from nothing about
    when or how it was processed. That is what makes the unmatched row the
    SAME row the matched path would have written: a Resend, a duplicate
    delivery, and the identical callback arriving again after the slot owns its
    message id all land on this key and add nothing.
    """
    return _fact_key(
        (
            _RECEIPT_DOMAIN,
            kind,
            provider_message_id,
            sender_digest or "none",
            recipient_digest or "none",
            _iso(event_at),
        )
    )


async def record_unmatched_status_receipt(
    session: AsyncSession,
    *,
    status: dict[str, Any],
    kind: str,
    whatsapp_event_id: int | None = None,
    signature_verified: bool = False,
    observed_at: datetime | None = None,
) -> ReceiptOutcome:
    """Keep a signed ``delivered``/``read`` that NOBODY owns yet (§46.3).

    The positive half of the early-callback contract, and the exact counterpart
    of :func:`record_unmatched_callback_failure`. Meta can deliver a ``read``
    for a message id while the transaction that records that id is still in
    flight: the slot is ``send_claimed``, its POST has returned, and the commit
    that stores the identifier has not landed. "No slot owns this id" is
    therefore an ordinary momentary state, not evidence that the callback is
    foreign — and dropping it loses a status that may never be sent again,
    leaving the slot's audience ``unknown`` forever.

    So the proof is stored now and bound later, by the very commit that makes it
    bindable, on the exact identifier. Never on phone similarity and never on
    timestamp proximity.

    Two things are required even here. The SIGNATURE, because it is the one part
    that cannot be established after the fact — an unsigned callback is not kept
    at all, and no later binding can promote it. And a message id, which is what
    the binding will match on.

    One narrow gate keeps this table about this phase: a row is kept only while
    some slot of this mailing could still claim a NEW message id — one that is
    claimed for sending and has not recorded an identifier yet. That is the
    window this function exists for, exactly; it decides whether to KEEP the
    callback and never which slot it belongs to, so it matches nothing and
    proves nothing.

    Runs in the CALLER's transaction. The caller is expected to hold
    :func:`lock_message_identity` for this identifier already — the status hook
    takes it before its own lookup — which is what makes the gap between
    "nothing owns this id" and this insert unreachable rather than merely
    narrow; the lock is re-taken here so the function is safe on its own too.
    """
    if kind not in DELIVERY_RECEIPT_KINDS:
        raise ValueError("unsupported status receipt kind")
    now = observed_at or utcnow()
    provider_message_id = str(status.get("provider_message_id") or "").strip()
    if not provider_message_id:
        return ReceiptOutcome(False, SKIPPED_NO_MESSAGE_ID)
    if not signature_verified:
        return ReceiptOutcome(False, UNPROVEN_CALLBACK_UNVERIFIED)

    await lock_message_identity(session, provider_message_id)
    with session.no_autoflush:
        located = await _locate_item(session, provider_message_id)
    if located is not None:
        # It is owned after all, so the matched path is the right one and this
        # function must not write a row that would never be bound.
        return ReceiptOutcome(False, CONFLICT_ROUTE_UNVERIFIABLE)
    if not await _send_could_claim_a_new_message_id(session):
        return ReceiptOutcome(False, SKIPPED_NOT_OURS)

    event_at = status.get("event_at") if isinstance(status.get("event_at"), datetime) else None
    sender_digest = sender_route_digest(status.get("phone_number_id"))
    recipient_digest = recipient_route_digest(status.get("recipient_id"))
    inserted = await _insert_fact(
        session,
        {
            "fact_key": _receipt_fact_key(
                kind=kind,
                provider_message_id=provider_message_id,
                sender_digest=sender_digest,
                recipient_digest=recipient_digest,
                event_at=event_at,
            ),
            "kind": kind,
            "authenticity": INBOUND_AUTHENTICITY_SIGNED,
            "match_state": DELIVERY_RECEIPT_MATCH_UNMATCHED,
            "provider_message_id": provider_message_id,
            "observed_sender_digest": sender_digest,
            "observed_recipient_digest": recipient_digest,
            "route_proven": False,
            "whatsapp_event_id": whatsapp_event_id,
            "observed_at": now,
            "event_at": event_at,
            "evidence": {"source": DELIVERY_FAILURE_SOURCE_CALLBACK, "awaiting_binding": True},
        },
        table=EasyWeekVoucherProductionDeliveryReceipt,
    )
    return ReceiptOutcome(inserted is not None)


async def _send_could_claim_a_new_message_id(session: AsyncSession) -> bool:
    """Is a send of this phase in the window where a callback can overtake it?

    True while some slot is claimed for sending and has recorded no provider
    message id. That is the only state in which a positive callback can name an
    identifier this phase is about to own, so it is the only state in which one
    is worth keeping. A process that died mid-send leaves its slot in exactly
    this state, which is when the keeping matters most.

    This is NOT a match: it says nothing about which slot, and no row written
    under it names one.
    """
    found = await session.scalar(
        select(EasyWeekVoucherProductionBatchItem.id)
        .where(EasyWeekVoucherProductionBatchItem.status == VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED)
        .where(EasyWeekVoucherProductionBatchItem.provider_message_id.is_(None))
        .limit(1)
    )
    return found is not None


async def bind_unmatched_receipts(
    session: AsyncSession,
    *,
    item: EasyWeekVoucherProductionBatchItem,
    provider_message_id: str,
    observed_at: datetime | None = None,
) -> frozenset[str]:
    """Claim the positive callbacks that arrived before this id was committed.

    Called from the acceptance write, inside the same transaction and under the
    same header and item locks, so an early callback is bound by the very commit
    that made it bindable. Matching is on the exact identifier and nothing else.

    A bound receipt is re-judged from scratch against the frozen route: it is
    not promoted just because it waited, and an unsigned one was never stored to
    begin with. A foreign or unverifiable route makes it ``conflicting``, which
    is kept for audit and is never read as proof.

    Returns the kinds that became CONFIRMED, for the caller to apply to the slot
    in its own monotonic order. This function writes no slot status itself: the
    ledger owns that, and ``read`` outranking ``delivered`` is its rule.
    """
    now = observed_at or utcnow()
    rows = list(
        (
            await session.scalars(
                select(EasyWeekVoucherProductionDeliveryReceipt)
                .where(EasyWeekVoucherProductionDeliveryReceipt.provider_message_id == provider_message_id)
                .where(EasyWeekVoucherProductionDeliveryReceipt.match_state == DELIVERY_RECEIPT_MATCH_UNMATCHED)
                .order_by(EasyWeekVoucherProductionDeliveryReceipt.id.asc())
                .with_for_update()
            )
        ).all()
    )
    if not rows:
        return frozenset()

    attempt = await _attempt_for(session, item)
    confirmed: set[str] = set()
    for row in rows:
        route_proven, conflict = _route_verdict(
            attempt,
            observed_sender_digest=row.observed_sender_digest,
            observed_recipient_digest=row.observed_recipient_digest,
        )
        row.item_id = int(item.id)
        row.attempt_id = int(attempt.id) if attempt is not None else None
        row.route_proven = bool(route_proven)
        merged = dict(row.evidence or {})
        merged.pop("awaiting_binding", None)
        merged["bound_at"] = now.isoformat()
        merged["route_proven"] = bool(route_proven)
        if route_proven:
            row.match_state = DELIVERY_RECEIPT_MATCH_CONFIRMED
            row.batch_id = int(item.batch_id)
            row.slot = int(item.slot)
            confirmed.add(str(row.kind))
            # Same rule, same transaction: a proven success withdraws any
            # confirmed failure already bound to this message id. Ordinarily
            # there is none at this point — the acceptance binds the positive
            # callbacks before the failures — and saying it once here keeps the
            # two paths on one model instead of two.
            await demote_confirmed_failures_after_success(
                session,
                item=item,
                provider_message_id=provider_message_id,
                kind=str(row.kind),
                observed_at=now,
            )
        else:
            row.match_state = DELIVERY_RECEIPT_MATCH_CONFLICTING
            merged["conflict"] = conflict or UNPROVEN_ROUTE_UNKNOWN
        row.evidence = merged
        row.updated_at = now

    logger.info(
        "voucher_production_receipt: bound=%s confirmed=%s batch=%s slot=%s",
        len(rows),
        len(confirmed),
        item.batch_id,
        item.slot,
    )
    return frozenset(confirmed)


# The two slot statuses a confirmed receipt may advance a slot to, in rank
# order. Named here so the ledger and this module cannot disagree about which
# receipt kind means which status.
RECEIPT_KIND_STATUS: Final[tuple[tuple[str, str], ...]] = (
    (DELIVERY_RECEIPT_KIND_DELIVERED, VOUCHER_PRODUCTION_ITEM_DELIVERED),
    (DELIVERY_RECEIPT_KIND_READ, VOUCHER_PRODUCTION_ITEM_READ),
)


async def record_status_receipt(
    session: AsyncSession,
    *,
    status: dict[str, Any],
    kind: str,
    whatsapp_event_id: int | None = None,
    signature_verified: bool = False,
    observed_at: datetime | None = None,
) -> ReceiptOutcome:
    """Record the PROOF behind one ``delivered``/``read`` callback (§46.3).

    Written beside the slot's own transition, never instead of it. The ledger
    keeps applying the status exactly as before — it is the record of a mailing
    that really happened — and this adds, for the cases where it can be proven,
    the two things that record does not keep: that the delivery's signature was
    verified, and that the route the callback named is the one this slot's single
    attempt froze.

    Nothing is written unless all three parts hold. A callback with no verified
    signature is an unattributed claim; one naming a foreign sender or recipient
    is about somebody else's message; one that names no route at all cannot be
    compared with a route we did freeze, and the stricter check must not be
    bypassable by omitting fields. In each case the slot keeps its status and
    gains no proof, which is what makes a later audience fail closed.

    Runs inside the CALLER's transaction, re-taking locks it already holds, and
    is idempotent: the key is computed from the proven contents of the fact, so
    a Resend or a duplicate delivery adds nothing.
    """
    if kind not in DELIVERY_RECEIPT_KINDS:
        raise ValueError("unsupported status receipt kind")
    now = observed_at or utcnow()
    provider_message_id = str(status.get("provider_message_id") or "").strip()
    if not provider_message_id:
        return ReceiptOutcome(False, SKIPPED_NO_MESSAGE_ID)
    if not signature_verified:
        return ReceiptOutcome(False, UNPROVEN_CALLBACK_UNVERIFIED)

    await lock_message_identity(session, provider_message_id)
    with session.no_autoflush:
        located = await _locate_item(session, provider_message_id)
    if located is None:
        return ReceiptOutcome(False, SKIPPED_NOT_OURS)

    batch_id, _slot = located
    item = (
        await session.execute(
            select(EasyWeekVoucherProductionBatchItem)
            .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
            .where(EasyWeekVoucherProductionBatchItem.provider_message_id == provider_message_id)
        )
    ).scalar_one_or_none()
    if item is None:
        return ReceiptOutcome(False, SKIPPED_NOT_OURS)

    attempt = await _attempt_for(session, item)
    observed_sender = sender_route_digest(status.get("phone_number_id"))
    observed_recipient = recipient_route_digest(status.get("recipient_id"))
    route_proven, conflict = _route_verdict(
        attempt,
        observed_sender_digest=observed_sender,
        observed_recipient_digest=observed_recipient,
    )
    if not route_proven:
        # A foreign route, an unverifiable one, or an attempt that froze none.
        # All three leave the status without proof, honestly.
        return ReceiptOutcome(False, conflict or UNPROVEN_ROUTE_UNKNOWN)
    assert observed_sender is not None and observed_recipient is not None

    # The success is proven here, so §46.3's precedence applies now rather than
    # depending on which of the pair this process read first. Done before the
    # idempotent insert and not conditionally on it, so a duplicate delivery of
    # the same callback reaches the same conclusion.
    await demote_confirmed_failures_after_success(
        session,
        item=item,
        provider_message_id=provider_message_id,
        kind=kind,
        observed_at=now,
    )

    event_at = status.get("event_at") if isinstance(status.get("event_at"), datetime) else None
    fact_key = _receipt_fact_key(
        kind=kind,
        provider_message_id=provider_message_id,
        sender_digest=observed_sender,
        recipient_digest=observed_recipient,
        event_at=event_at,
    )
    inserted = await _insert_fact(
        session,
        {
            "fact_key": fact_key,
            "kind": kind,
            "authenticity": INBOUND_AUTHENTICITY_SIGNED,
            "match_state": DELIVERY_RECEIPT_MATCH_CONFIRMED,
            "item_id": int(item.id),
            "attempt_id": int(attempt.id) if attempt is not None else None,
            "batch_id": int(item.batch_id),
            "slot": int(item.slot),
            "provider_message_id": provider_message_id,
            "observed_sender_digest": observed_sender,
            "observed_recipient_digest": observed_recipient,
            "route_proven": True,
            "whatsapp_event_id": whatsapp_event_id,
            "observed_at": now,
            "event_at": event_at,
            "evidence": {"source": DELIVERY_FAILURE_SOURCE_CALLBACK},
        },
        table=EasyWeekVoucherProductionDeliveryReceipt,
    )
    return ReceiptOutcome(inserted is not None)


async def record_api_rejection(
    session: AsyncSession,
    *,
    item: EasyWeekVoucherProductionBatchItem,
    http_status: int | None,
    observed_at: datetime | None = None,
) -> FailureOutcome:
    """Record Meta's own refusal of the POST, in the CALLER's transaction.

    The one thing this is NOT is ``send_rejected``. That state is also written
    when this process refuses its own parameters before anything leaves it, so
    it cannot distinguish a proven external refusal from a local one — which is
    exactly the ambiguity §46.3 names. The caller reaches this function only on
    the proven-rejection branch, which needs Meta's own error envelope on a
    status Meta decides before acting.

    There is no message id yet, because Meta never issued one; the link to the
    attempt is the intent committed before the request left.
    """
    now = observed_at or utcnow()
    attempt = await _attempt_for(session, item)
    return await _store(
        session,
        source=DELIVERY_FAILURE_SOURCE_META_API,
        reason_code=DELIVERY_FAILURE_REASON_API_REJECTED,
        item=item,
        attempt=attempt,
        provider_message_id=None,
        intent_uuid=item.outbound_intent_uuid,
        # Meta's refusal body is deliberately never parsed for this phase: the
        # delivery client inspects its SHAPE and reports nothing from it, so
        # there is no code to record and the class is honestly unknown.
        error_code_raw=None,
        http_status=http_status,
        event_at=None,
        observed_sender_digest=(attempt.sender_phone_number_id_digest if attempt is not None else None),
        observed_recipient_digest=(attempt.recipient_digest if attempt is not None else None),
        # The refusal was observed by this process on its own request, not
        # received over a webhook, so webhook authenticity does not apply.
        signature_verified=False,
        whatsapp_event_id=None,
        observed_at=now,
    )


async def bind_unmatched_failures(
    session: AsyncSession,
    *,
    item: EasyWeekVoucherProductionBatchItem,
    provider_message_id: str,
    observed_at: datetime | None = None,
) -> int:
    """Claim the facts that arrived before this slot's message id was committed.

    Called from the acceptance write, inside the same transaction and under the
    same header and item locks, so an early callback is bound by the very commit
    that made it bindable. Matching is on the exact identifier and nothing else.

    A bound fact is re-judged from scratch against the frozen route and the
    slot's success timestamps; it is not promoted just because it waited.
    """
    now = observed_at or utcnow()
    rows = list(
        (
            await session.scalars(
                select(EasyWeekVoucherProductionDeliveryFailure)
                .where(EasyWeekVoucherProductionDeliveryFailure.provider_message_id == provider_message_id)
                .where(EasyWeekVoucherProductionDeliveryFailure.match_state == DELIVERY_FAILURE_MATCH_UNMATCHED)
                .order_by(EasyWeekVoucherProductionDeliveryFailure.id.asc())
                .with_for_update()
            )
        ).all()
    )
    if not rows:
        return 0

    attempt = await _attempt_for(session, item)
    bound = 0
    for row in rows:
        route_proven, conflict = _route_verdict(
            attempt,
            observed_sender_digest=row.observed_sender_digest,
            observed_recipient_digest=row.observed_recipient_digest,
        )
        if conflict is None and (item.delivered_at is not None or item.read_at is not None):
            conflict = CONFLICT_AFTER_SUCCESS
        # The same rule the live path uses, from the authenticity stored on the
        # row when it arrived. A fact is not promoted for having waited, and an
        # unsigned callback does not become proof once a slot owns its id.
        unproven = (
            None
            if conflict
            else _not_proven(
                source=row.source,
                signature_verified=row.authenticity == INBOUND_AUTHENTICITY_SIGNED,
                route_proven=route_proven,
            )
        )

        if conflict:
            row.match_state = DELIVERY_FAILURE_MATCH_CONFLICTING
        elif unproven:
            row.match_state = DELIVERY_FAILURE_MATCH_UNPROVEN
        else:
            row.match_state = DELIVERY_FAILURE_MATCH_CONFIRMED
        row.route_proven = route_proven
        row.attempt_id = int(attempt.id) if attempt is not None else None
        row.batch_id = int(item.batch_id)
        row.slot = int(item.slot)
        row.intent_uuid = item.outbound_intent_uuid
        # Every bound fact names which slot it was tested against, so an
        # operator can find it. Only a CONFIRMED one is that slot's evidence.
        row.item_id = int(item.id)
        if row.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED:
            _mark_item(
                item,
                reason_code=row.reason_code,
                failure_class=row.failure_class or DELIVERY_CLASS_UNKNOWN,
                observed_at=now,
            )
        merged = dict(row.evidence or {})
        merged.pop("awaiting_binding", None)
        merged["bound_at"] = now.isoformat()
        merged["route_proven"] = route_proven
        if conflict:
            merged["conflict"] = conflict
        if unproven:
            merged["unproven"] = unproven
        row.evidence = merged
        row.updated_at = now
        bound += 1

    logger.info(
        "voucher_production_failure: bound=%s batch=%s slot=%s",
        bound,
        item.batch_id,
        item.slot,
    )
    return bound


__all__ = [
    "CONFLICT_AFTER_SUCCESS",
    "CONFLICT_FOREIGN_ROUTE",
    "CONFLICT_ROUTE_UNVERIFIABLE",
    "SKIPPED_NOT_OURS",
    "SKIPPED_NO_MESSAGE_ID",
    "UNPROVEN_CALLBACK_UNVERIFIED",
    "UNPROVEN_ROUTE_UNKNOWN",
    "FailureOutcome",
    "bind_unmatched_failures",
    "lock_callback_scope",
    "lock_message_identity",
    "RECEIPT_KIND_STATUS",
    "ReceiptOutcome",
    "bind_unmatched_receipts",
    "demote_confirmed_failures_after_success",
    "record_api_rejection",
    "record_status_receipt",
    "record_callback_failure",
    "record_unmatched_callback_failure",
    "record_unmatched_status_receipt",
]
