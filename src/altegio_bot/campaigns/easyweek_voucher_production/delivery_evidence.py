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
  decides before acting on the message;
* a ``failed`` delivery status naming the exact message id of this slot.

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
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import (
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_FAILURE_MATCH_CONFLICTING,
    DELIVERY_FAILURE_MATCH_UNMATCHED,
    DELIVERY_FAILURE_REASON_API_REJECTED,
    DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
    DELIVERY_FAILURE_SOURCE_CALLBACK,
    DELIVERY_FAILURE_SOURCE_META_API,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
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

# Why nothing was written.
SKIPPED_NOT_OURS: Final = "not_a_production_message"
SKIPPED_NO_MESSAGE_ID: Final = "callback_without_message_id"


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


async def _insert_fact(session: AsyncSession, values: dict[str, Any]) -> int | None:
    """Insert one fact, idempotently on ``fact_key``.

    ``ON CONFLICT DO NOTHING`` rather than a read-then-write: two callbacks can
    be processed concurrently by two workers, and losing the race on a unique
    index must be a no-op rather than an exception that fails a whole batch of
    status updates.
    """
    stmt = (
        pg_insert(EasyWeekVoucherProductionDeliveryFailure)
        .values(**values)
        .on_conflict_do_nothing(index_elements=["fact_key"])
        .returning(EasyWeekVoucherProductionDeliveryFailure.id)
    )
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

    match_state = DELIVERY_FAILURE_MATCH_CONFLICTING if conflict else DELIVERY_FAILURE_MATCH_CONFIRMED
    evidence: dict[str, Any] = {"route_proven": route_proven}
    if conflict:
        evidence["conflict"] = conflict
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
        reason=conflict,
        fact_id=fact_id,
    )


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

        row.match_state = DELIVERY_FAILURE_MATCH_CONFLICTING if conflict else DELIVERY_FAILURE_MATCH_CONFIRMED
        row.route_proven = route_proven
        row.attempt_id = int(attempt.id) if attempt is not None else None
        row.batch_id = int(item.batch_id)
        row.slot = int(item.slot)
        row.intent_uuid = item.outbound_intent_uuid
        if row.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED:
            row.item_id = int(item.id)
            _mark_item(
                item,
                reason_code=row.reason_code,
                failure_class=row.failure_class or DELIVERY_CLASS_UNKNOWN,
                observed_at=now,
            )
        else:
            # A conflicting fact still names which slot it was tested against,
            # so an operator can find it — but it is NOT this slot's evidence.
            row.item_id = int(item.id)
        merged = dict(row.evidence or {})
        merged.pop("awaiting_binding", None)
        merged["bound_at"] = now.isoformat()
        merged["route_proven"] = route_proven
        if conflict:
            merged["conflict"] = conflict
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
    "FailureOutcome",
    "bind_unmatched_failures",
    "record_api_rejection",
    "record_callback_failure",
    "record_unmatched_callback_failure",
]
