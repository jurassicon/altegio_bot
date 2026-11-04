"""Durable WhatsApp marketing consent, independent of any client card (§46.4).

The one mechanism
-----------------
There is exactly one place that decides whether marketing may reach a WhatsApp
address, and this is it. The legacy ``Client.wa_opted_out`` flag and the
historical ``CampaignRecipient.is_opted_out`` snapshot are not replaced and not
bypassed — they are READ here, as additional refusals, so that a ban recorded
anywhere in this database keeps vetoing. What is new is that a refusal no longer
needs a card to live on.

Why the asymmetry is deliberate
-------------------------------
Moving to ``opted_out`` is cheap and always allowed: the cost of a ban nobody
asked for is one marketing message that was not sent. Moving back is expensive
and almost never allowed: the cost of a wrong opt-in is a message to somebody
who told us to stop. So:

* a STOP applies whatever its authenticity and whatever its timestamp, unless
  it can PROVE it is older than the opt-in currently in force — a replay of
  something already answered, which is kept and reported rather than acted on;
* a START lifts a refusal only when Meta's signature was configured AND
  verified for that very request, its own event time is proven, the refusal's
  event time is proven, no accepted STOP for that address has an unprovable
  event time, and the START's time is strictly later.

Everything else — an unsigned request, a deployment with no app secret, a
replayed payload, an equal timestamp, a timestamp nobody can parse, a historical
ban of unknown time, a refusal whose order cannot be established at all, a
reused message identifier — refuses, with a reason code. That is not a gap to be
closed later; it is §46.4.7 and §46.4.4 written out.

The absence of a refusal here is NOT evidence of marketing consent. It means
only that this database holds no proven refusal for that address.

What does not happen in this module
-----------------------------------
No HTTP, ever. No acknowledgement, no Chatwoot note, no Meta or EasyWeek
request, no job creation, no message. The consent write is a short transaction
of its own precisely so it can commit BEFORE anything else is attempted and
survive the failure of everything that follows.
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Collection, Final, Iterable, Sequence

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.models.models import (
    CONSENT_COMMAND_NONE,
    CONSENT_COMMAND_START,
    CONSENT_COMMAND_STOP,
    CONSENT_SOURCE_INBOUND,
    CONSENT_SOURCE_PROJECTION,
    CONSENT_STATE_OPTED_IN,
    CONSENT_STATE_OPTED_OUT,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    INBOUND_RESOLUTION_APPLIED,
    INBOUND_RESOLUTION_IGNORED_ROUTE,
    INBOUND_RESOLUTION_NOOP,
    INBOUND_RESOLUTION_REFUSED,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    Client,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
)
from altegio_bot.utils import utcnow
from altegio_bot.webhooks.common import list_or_empty, normalize_phone_candidate
from altegio_bot.whatsapp_inbound_commands import (
    consent_command,
    extract_message_text,
    first_command_token,
    parse_event_timestamp,
    route_allowed,
    sender_route_of_change,
)

logger = logging.getLogger("whatsapp_consent")

# Domain-separated digests. Nothing derived here is reversible, and the labels
# keep one value's handle from being correlated with another call site's.
_ADDRESS_LOCK_DOMAIN: Final = "altegio_bot/whatsapp_consent/address_lock/v1"
_RECEIPT_KEY_DOMAIN: Final = "altegio_bot/whatsapp_consent/receipt_key/v1"
_SENDER_ROUTE_DOMAIN: Final = "altegio_bot/whatsapp/sender_route/v1"
_RECIPIENT_ROUTE_DOMAIN: Final = "altegio_bot/whatsapp/recipient_route/v1"

# Why a START was not allowed to lift a refusal. Stable, non-personal labels:
# they reach receipts, suppression evidence and operator reports.
REFUSED_START_UNVERIFIED: Final = "start_authenticity_unproven"
REFUSED_START_EVENT_TIME_UNKNOWN: Final = "start_event_time_unproven"
REFUSED_START_BAN_TIME_UNKNOWN: Final = "stop_event_time_unproven"
REFUSED_START_NOT_NEWER: Final = "start_not_newer_than_stop"
REFUSED_START_IDENTIFIER_CONFLICT: Final = "start_event_identifier_conflict"
# Nothing to lift. Recorded rather than silently ignored, because "there was no
# ban" is a different fact from "the ban was lifted", and no row is created: an
# absent refusal must never be written down as a granted permission.
NOOP_START_NO_ACTIVE_BAN: Final = "start_without_active_refusal"
NOOP_STOP_ALREADY_ACTIVE: Final = "stop_already_active"
# A STOP that is PROVABLY older than the opt-in currently in force. It is kept
# and reported, and it does not take the state back: the only way to get here is
# a replay or a historical projection of an event the ledger has already moved
# past, and letting it win would undo a verified, strictly-newer opt-in every
# time the reconciliation ran.
NOOP_STOP_SUPERSEDED: Final = "stop_older_than_proven_opt_in"
# A projected historical refusal with no provable event time, whose own OBSERVED
# moment is older than the moment a proven opt-in for that same address was
# observed. It is a stale snapshot of a state this ledger has already moved
# past, and writing it would turn a resubscribe nobody disputes into a refusal
# of unknown order — one that, by §46.4.4, no later START could ever lift.
NOOP_PROJECTION_SUPERSEDED: Final = "projection_older_than_proven_opt_in"
# A projected refusal whose evidence was already durable and whose CARD half was
# all that remained. No decision is made and no evidence is written: the state
# the ledger already holds is reported back so the caller can say what it did.
NOOP_CARD_RECOVERY: Final = "card_projection_recovered"

# Where an aggregate refusal came from. The caller shows these to an operator.
SOURCE_SUPPRESSION: Final = "consent_suppression"
SOURCE_SUPPRESSION_UUID: Final = "consent_suppression_uuid_bound"
SOURCE_CLIENT_FLAG: Final = "client_wa_opted_out"
SOURCE_RECIPIENT_FLAG: Final = "campaign_recipient_opted_out"

# Why the aggregate could not reach a confident answer. Shown, never hidden.
GAP_ADDRESS_UNNORMALISABLE: Final = "address_not_normalisable"
GAP_UUID_BINDING_AMBIGUOUS: Final = "easyweek_uuid_binding_ambiguous"
GAP_PENDING_INBOUND: Final = "pending_inbound_unresolved"

# Why a final send guard refused.
GUARD_SUPPRESSED: Final = "consent_refusal_active"
GUARD_PENDING_INBOUND: Final = "pending_inbound_unresolved"
GUARD_ADDRESS_UNPROVEN: Final = "address_not_normalisable"
# The consent state is not the one the guard was issued against: a decision, a
# newly linked address or an unresolved inbound message landed in between.
GUARD_CONSENT_MOVED: Final = "consent_state_changed"
# The set of addresses this person's cards name changed between taking the
# locks and reading under them, so the read is not covered by the locks. Held
# rather than allowed, and the next attempt sees a settled set.
GUARD_ADDRESS_SET_MOVED: Final = "address_set_changed_under_the_lock"
GUARD_ALLOWED: Final = "no_local_refusal_and_nothing_pending"


# ---------------------------------------------------------------------------
# Addresses
# ---------------------------------------------------------------------------


def normalize_wa_address(raw: object) -> str | None:
    """The canonical form of one WhatsApp address, or ``None``.

    The same closed grammar the ingress, the worker and the 24h-window logic
    already share. A value that does not normalise is not quietly cleaned up:
    guessing at a phone number is how a message reaches a different person.
    """
    return normalize_phone_candidate(raw)


def address_variants(address: str) -> list[str]:
    """Every spelling of *address* that may be stored in a phone column.

    Historical rows were written by several code paths, so the same number
    appears as ``+49…``, ``49…`` and occasionally with separators already
    removed. A refusal must match all of them — under-matching here is a message
    sent to somebody who unsubscribed.
    """
    digits = re.sub(r"\D+", "", address or "")
    variants = {address, digits, f"+{digits}"} if digits else {address}
    return sorted(value for value in variants if value)


def _digest(domain: str, value: str) -> str:
    """A domain-separated one-way handle, total over hostile input.

    ``surrogatepass`` is load-bearing, not tidiness. Some of the values digested
    here come straight out of a webhook body — a sender route id, a recipient —
    and a lone surrogate in one of them would otherwise raise
    ``UnicodeEncodeError`` inside the ingress transaction, which is a 500 and a
    Meta retry loop on a payload that will fail the same way every time. It
    changes nothing for valid input: a well-formed string has no surrogates, so
    every digest this project has already computed stays the same.
    """
    return hashlib.sha256(f"{domain}:{value}".encode("utf-8", "surrogatepass")).hexdigest()


def sender_route_digest(phone_number_id: object) -> str | None:
    """A one-way handle for one Meta sender route, or ``None`` if there is none."""
    value = str(phone_number_id or "").strip()
    return _digest(_SENDER_ROUTE_DOMAIN, value) if value else None


def recipient_route_digest(phone: object) -> str | None:
    """A one-way handle for one recipient number, or ``None``.

    Normalised first, so the digest of ``4917…`` and of ``+4917…`` is the same
    handle — otherwise a callback's ``recipient_id`` (no ``+``) would never
    match a stored destination that has one, and every genuine callback would
    read as a foreign recipient.
    """
    normalised = normalize_wa_address(phone)
    if normalised is None:
        # Not normalisable, so not comparable. A digest of the raw bytes would
        # be a handle that can only ever mismatch, which is worse than absent.
        return None
    return _digest(_RECIPIENT_ROUTE_DOMAIN, normalised)


def receipt_key(dedupe_key: str, message_index: int) -> str:
    """A durable identity for one inbound message inside one stored event.

    Derived from the event's dedupe key rather than from its row id, so it
    survives the event row being removed and stays the same if the same payload
    is ingested again.
    """
    return _digest(_RECEIPT_KEY_DOMAIN, f"{dedupe_key}|{int(message_index)}")


async def lock_consent_address(session: AsyncSession, address: str) -> None:
    """Serialize consent writes for one address, including the absent-row race.

    Two STOPs for an address nobody has a row for would otherwise both insert
    and one would lose on the unique index. A transaction-scoped advisory lock
    on the address makes the read-decide-write sequence atomic without creating
    a row just to have something to lock, and it is released by the commit
    whether or not anything was written.

    No table row is locked here on purpose: this lock is taken by the consent
    writer and by the final send guard, and by nothing that holds a batch or
    item lock, so it cannot take part in a cycle with the voucher stages.
    """
    key = int.from_bytes(
        hashlib.sha256(f"{_ADDRESS_LOCK_DOMAIN}:{address}".encode("utf-8", "surrogatepass")).digest()[:8],
        "big",
        signed=True,
    )
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})


async def lock_consent_addresses(session: AsyncSession, addresses: Iterable[str]) -> list[str]:
    """Lock several addresses, always in sorted order. Returns what was locked.

    Sorted is the whole point. A send guard holds a person's own number and
    every number an EasyWeek card of their proven UUID carries; the ingress
    holds every address in one webhook delivery. Two such sets can overlap in
    any way, so a fixed global order over the addresses themselves is what keeps
    two writers from each holding what the other is waiting for.
    """
    ordered = sorted({address for address in addresses if address})
    for address in ordered:
        await lock_consent_address(session, address)
    return ordered


# ---------------------------------------------------------------------------
# Ingress: what arrived, written before the 200
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InboundReceiptDraft:
    """One inbound message as the ingress read it, before anything acts on it."""

    message_index: int
    wa_address: str
    command: str
    whatsapp_message_id: str | None
    event_at: datetime | None
    # The Meta sender route of the CHANGE this message arrived in, exactly as
    # the payload spelled it. Per message, because a delivery may carry several
    # changes with different routes and only some of them allowed.
    sender_phone_number_id: str | None = None
    # The exact token that matched, for a consent command only. In memory and
    # for reporting; never stored. Left ``None`` for an ordinary message, whose
    # first word is arbitrary customer text and has no business being carried
    # around by the consent layer.
    command_token: str | None = None


def extract_inbound_receipts(
    payload: dict[str, Any],
    *,
    observed_at: datetime,
) -> list[InboundReceiptDraft]:
    """Every inbound message of *payload* that names a usable sender address.

    EVERY message, not the first one. A payload may carry several messages from
    several people, and §46.4.1 requires the refusals in all of them to count —
    which means the sender is read per message here, never once per payload.

    Total by construction: every nested field is sender-controlled, so each one
    is narrowed before use and a malformed branch is skipped rather than raised.
    This runs inside the transaction that stores the event and answers Meta; an
    exception here would turn a hostile payload into a 500 and a retry loop.
    """
    drafts: list[InboundReceiptDraft] = []
    index = 0

    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue
            value = change.get("value")
            if not isinstance(value, dict):
                continue
            change_route = sender_route_of_change(change)
            for msg in list_or_empty(value.get("messages")):
                if not isinstance(msg, dict):
                    continue
                address = normalize_wa_address(msg.get("from"))
                if address is None:
                    # No provable sender, so nothing this row could be about.
                    # Not counted in the index either: the index must address
                    # the receipts that exist, stably across re-reads.
                    continue
                # A reaction is not text and must never become a command — the
                # worker has always treated it as its own kind of action, and an
                # emoji flowing into STOP/START would be a new behaviour.
                raw_text = "" if msg.get("type") == "reaction" else extract_message_text(msg)
                matched = consent_command(raw_text)
                command = matched or CONSENT_COMMAND_NONE
                identifier = msg.get("id")
                drafts.append(
                    InboundReceiptDraft(
                        message_index=index,
                        wa_address=address,
                        command=command,
                        whatsapp_message_id=(
                            identifier.strip() if isinstance(identifier, str) and identifier.strip() else None
                        ),
                        event_at=parse_event_timestamp(msg.get("timestamp"), observed_at=observed_at),
                        command_token=first_command_token(raw_text) if matched else None,
                        sender_phone_number_id=change_route,
                    )
                )
                index += 1

    return drafts


async def record_inbound_receipts(
    session: AsyncSession,
    *,
    dedupe_key: str,
    event_id: int | None,
    payload: dict[str, Any],
    signature_verified: bool,
    observed_at: datetime,
    allowed_phone_number_ids: Collection[str] | None = None,
) -> int:
    """Write the receipts for one event, in the CALLER's transaction.

    The caller is ``whatsapp_ingest``, which commits this together with the
    ``WhatsAppEvent`` and only then answers 200. That ordering is the durable
    boundary §46.4.6 asks for: once the sender has been told the delivery was
    accepted, the fact that this address said something is on disk, and a slow
    or stopped inbox worker leaves a HOLD rather than a hole.

    ``signature_verified`` is passed rather than re-derived: the proof is the
    live request's HMAC over its raw body, and the stored copy of the headers
    has the signature stripped out of it by design.

    The route is decided PER MESSAGE, against the allow list as it reads right
    now, and the verdict is stored on the row. A message whose own route this
    deployment refuses is written down and resolved in the same breath: it still
    happened, an operator should be able to see that it did, and leaving it
    pending would hold sends forever waiting for a worker that is never going to
    act on it. A message from an allowed route inside the same delivery keeps its
    hold, which is the half the payload-level verdict used to lose.

    The address lock is taken here, before the insert, and that is what makes the
    pending set a real boundary rather than a best effort: a final send guard
    holds the same lock while it reads, so a receipt either lands before the
    guard looks — and holds the send — or waits for the guard's transaction to
    end and is then caught by the claim-time re-check. There is no interleaving
    in which an unresolved refusal is invisible to both.
    """
    drafts = extract_inbound_receipts(payload, observed_at=observed_at)
    if not drafts:
        return 0

    await lock_consent_addresses(session, (draft.wa_address for draft in drafts))

    authenticity = INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED
    for draft in drafts:
        allowed = route_allowed(draft.sender_phone_number_id, allowed_phone_number_ids)
        session.add(
            WhatsAppInboundReceipt(
                receipt_key=receipt_key(dedupe_key, draft.message_index),
                whatsapp_event_id=event_id,
                message_index=draft.message_index,
                wa_address=draft.wa_address,
                command=draft.command,
                authenticity=authenticity,
                sender_route_digest=sender_route_digest(draft.sender_phone_number_id),
                route_allowed=allowed,
                whatsapp_message_id=draft.whatsapp_message_id,
                observed_at=observed_at,
                event_at=draft.event_at,
                resolved_at=None if allowed else observed_at,
                resolution=None if allowed else INBOUND_RESOLUTION_IGNORED_ROUTE,
                resolution_reason=None if allowed else "ingress_sender_route_not_allowed",
            )
        )
    return len(drafts)


# ---------------------------------------------------------------------------
# The decision, per address
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DecisionOutcome:
    """What one STOP/START did to one address's refusal."""

    applied: bool
    resolution: str
    reason: str | None
    state: str | None
    consent_version: int | None


async def _suppression_for_update(session: AsyncSession, address: str) -> WhatsAppConsentSuppression | None:
    return (
        await session.execute(
            select(WhatsAppConsentSuppression).where(WhatsAppConsentSuppression.wa_address == address).with_for_update()
        )
    ).scalar_one_or_none()


async def _unambiguous_banned_uuid(session: AsyncSession, address: str) -> tuple[uuid_module.UUID | None, bool]:
    """The one EasyWeek customer UUID this refusal should also follow, if any.

    A BAN EXTENSION and nothing else. The phone is being used to decide who must
    NOT be messaged, which is exactly what §46.4.2 permits; it is never read back
    as proof that this address belongs to that customer, proof of a branch, or
    proof that the person is an EasyWeek customer at all.

    Written only when the match is unambiguous. Several distinct UUIDs on one
    number is reported as a gap rather than resolved by picking one: the phone
    refusal already stands, and binding the wrong UUID would carry the ban onto
    somebody else's identity.
    """
    variants = address_variants(address)
    values = set(
        (
            await session.scalars(
                select(Client.easyweek_customer_uuid).where(
                    Client.provider == PROVIDER_EASYWEEK,
                    Client.easyweek_customer_uuid.is_not(None),
                    Client.phone_e164.in_(variants),
                )
            )
        ).all()
    )
    if len(values) == 1:
        return next(iter(values)), False
    return None, len(values) > 1


async def _start_identifier_conflict(
    session: AsyncSession,
    *,
    address: str,
    whatsapp_message_id: str | None,
) -> bool:
    """Has this Meta message identifier already been seen naming someone else?

    One wamid belongs to one message. Two receipts carrying the same identifier
    for different addresses means at least one of them is not what it claims to
    be, and a START is the one decision that must not be made on a claim that
    cannot be trusted. Both rows stay for audit; the lift is refused.
    """
    if not whatsapp_message_id:
        return False
    found = await session.scalar(
        select(func.count())
        .select_from(WhatsAppInboundReceipt)
        .where(
            WhatsAppInboundReceipt.whatsapp_message_id == whatsapp_message_id,
            WhatsAppInboundReceipt.wa_address != address,
        )
    )
    return bool(found)


async def apply_consent_decision(
    session: AsyncSession,
    *,
    wa_address: str,
    command: str,
    authenticity: str,
    source: str,
    observed_at: datetime,
    event_at: datetime | None,
    receipt_id: int | None = None,
    whatsapp_message_id: str | None = None,
    linked_customer_uuid: uuid_module.UUID | None = None,
    evidence: dict[str, Any] | None = None,
) -> DecisionOutcome:
    """Apply one STOP or START to one address, in the CALLER's transaction.

    Idempotent and monotonic. A duplicate STOP changes nothing and says so; a
    replayed or late STOP cannot undo a newer one; a START that cannot prove it
    is strictly newer than the refusal it would lift does not lift it.

    The caller is expected to hold :func:`lock_consent_address` for this address,
    which is what closes the absent-row race between two first-ever writers.
    """
    if command not in (CONSENT_COMMAND_STOP, CONSENT_COMMAND_START):
        raise ValueError("consent decision must be stop or start")

    row = await _suppression_for_update(session, wa_address)
    base_evidence = dict(evidence or {})

    if command == CONSENT_COMMAND_STOP:
        banned_uuid, ambiguous = await _unambiguous_banned_uuid(session, wa_address)
        if ambiguous:
            base_evidence["uuid_binding"] = "ambiguous"
        elif banned_uuid is not None:
            # Named for what it is: the basis is a phone match, which bans and
            # proves nothing else.
            base_evidence["uuid_binding"] = "phone_matched_easyweek_card"
        elif linked_customer_uuid is not None:
            # No card carries this number any more — but one did at the moment an
            # EasyWeek booking PROVED this UUID for this person, and that proof
            # was written onto the receipt this refusal is being applied from.
            #
            # Without it the refusal would be recorded against the old number
            # alone while the card had already moved to the new one, and nothing
            # would connect the two: the person who said stop before the worker
            # got to their message would come out sendable. Still a ban
            # extension and nothing more — it is never read back as proof that
            # this address belongs to that customer or to that branch.
            banned_uuid = linked_customer_uuid
            base_evidence["uuid_binding"] = "proven_uuid_link_on_receipt"

        if row is not None and row.state == CONSENT_STATE_OPTED_IN:
            # A proven opt-in is in force. A STOP that can PROVE it happened
            # before that opt-in is a replay of something already answered — a
            # reconciliation pass re-reading an old stored event, or Meta
            # delivering the same payload again — and must not take the state
            # back, or every run of the projection would undo the same person's
            # resubscribe again.
            #
            # Only a provable order refuses it. An equal time, an unknown time
            # on either side, or a newer STOP all APPLY: a tie and an unproven
            # order both fall to the refusal, which is the same asymmetry the
            # START rules use in the other direction.
            #
            # The bar is the PROVEN event time of the opt-in actually IN FORCE,
            # read from the decision itself. Never from ``last_start_event_at``,
            # which is an audit high-water mark over every START this address
            # ever sent — and that was the defect: the mark moved before any of
            # the START proofs below ran, so an unsigned START at 11:00 raised
            # the bar to 11:00 although it changed nothing, the genuine signed
            # START at 10:30 then took the state, and the next real STOP at
            # 10:45 was discarded as a replay of something that never happened.
            # An audit trail of rejected attempts must not be able to license
            # ignoring a refusal.
            opt_in_event_at = row.decided_event_at if row.decision_command == CONSENT_COMMAND_START else None
            if event_at is not None and opt_in_event_at is not None and event_at < opt_in_event_at:
                if row.last_stop_observed_at is None or observed_at > row.last_stop_observed_at:
                    row.last_stop_observed_at = observed_at
                merged = dict(row.evidence or {})
                merged.update(base_evidence)
                merged["superseded_stop_observed_at"] = observed_at.isoformat()
                merged["superseded_stop_event_at"] = event_at.isoformat()
                row.evidence = merged
                await session.flush()
                return DecisionOutcome(
                    False,
                    INBOUND_RESOLUTION_NOOP,
                    NOOP_STOP_SUPERSEDED,
                    row.state,
                    int(row.consent_version),
                )

        if row is None:
            row = WhatsAppConsentSuppression(
                wa_address=wa_address,
                state=CONSENT_STATE_OPTED_OUT,
                consent_version=1,
                decision_command=CONSENT_COMMAND_STOP,
                decision_source=source,
                decision_authenticity=authenticity,
                decision_receipt_id=receipt_id,
                decided_observed_at=observed_at,
                decided_event_at=event_at,
                opted_out_at=observed_at,
                last_stop_observed_at=observed_at,
                last_stop_event_at=event_at,
                stop_event_time_unknown_at=None if event_at is not None else observed_at,
                banned_easyweek_customer_uuid=banned_uuid,
                banned_uuid_recorded_at=observed_at if banned_uuid is not None else None,
                evidence=base_evidence,
            )
            session.add(row)
            await session.flush()
            return DecisionOutcome(True, INBOUND_RESOLUTION_APPLIED, None, row.state, int(row.consent_version))

        already_banned = row.state == CONSENT_STATE_OPTED_OUT
        # High-water marks move forward only. A late duplicate must not rewrite
        # the refusal's own time backwards, because that time is what a future
        # START has to beat.
        if row.last_stop_observed_at is None or observed_at > row.last_stop_observed_at:
            row.last_stop_observed_at = observed_at
        if event_at is not None and (row.last_stop_event_at is None or event_at > row.last_stop_event_at):
            row.last_stop_event_at = event_at
        if event_at is None and (
            row.stop_event_time_unknown_at is None or observed_at > row.stop_event_time_unknown_at
        ):
            # This refusal cannot be ordered against anything. Recorded as its
            # own fact rather than folded into the high-water mark above, which
            # only ever moves to a KNOWN time and would otherwise leave an older,
            # provable STOP as the bar a future START has to clear.
            row.stop_event_time_unknown_at = observed_at
        if row.banned_easyweek_customer_uuid is None and banned_uuid is not None:
            row.banned_easyweek_customer_uuid = banned_uuid
            row.banned_uuid_recorded_at = observed_at

        if already_banned:
            merged = dict(row.evidence or {})
            merged.update(base_evidence)
            merged["repeat_stop_observed_at"] = observed_at.isoformat()
            row.evidence = merged
            await session.flush()
            return DecisionOutcome(
                False,
                INBOUND_RESOLUTION_NOOP,
                NOOP_STOP_ALREADY_ACTIVE,
                row.state,
                int(row.consent_version),
            )

        # Opted in until now. The refusal takes over as the current decision.
        row.state = CONSENT_STATE_OPTED_OUT
        row.consent_version = int(row.consent_version) + 1
        row.decision_command = CONSENT_COMMAND_STOP
        row.decision_source = source
        row.decision_authenticity = authenticity
        row.decision_receipt_id = receipt_id
        row.decided_observed_at = observed_at
        row.decided_event_at = event_at
        row.opted_out_at = observed_at
        merged = dict(row.evidence or {})
        merged.update(base_evidence)
        row.evidence = merged
        await session.flush()
        return DecisionOutcome(True, INBOUND_RESOLUTION_APPLIED, None, row.state, int(row.consent_version))

    # ---- START ---------------------------------------------------------- #
    # Four separate proofs, each refusing on its own. None of them is a
    # formality: between them they are what stops an unsigned webhook, a
    # replayed one, a reordered one and a forged identifier from lifting a real
    # person's refusal.
    if row is None or row.state != CONSENT_STATE_OPTED_OUT:
        # No refusal to lift. Deliberately NO row is created: an address with no
        # recorded refusal must not be written down as having granted consent.
        return DecisionOutcome(
            False,
            INBOUND_RESOLUTION_NOOP,
            NOOP_START_NO_ACTIVE_BAN,
            row.state if row is not None else None,
            int(row.consent_version) if row is not None else None,
        )

    def _refuse(reason: str) -> DecisionOutcome:
        # A rejected START is AUDIT, and audit may not move a proof. Everything
        # written here lives in ``evidence``; the ``last_start_*`` marks below
        # are touched only once every proof has passed, so nothing a rejected
        # attempt claims can raise the time a later STOP has to beat.
        merged = dict(row.evidence or {})
        merged.update(base_evidence)
        merged["last_refused_start_reason"] = reason
        merged["last_refused_start_at"] = observed_at.isoformat()
        if event_at is not None:
            merged["last_refused_start_event_at"] = event_at.isoformat()
        row.evidence = merged
        return DecisionOutcome(
            False,
            INBOUND_RESOLUTION_REFUSED,
            reason,
            row.state,
            int(row.consent_version),
        )

    if authenticity != INBOUND_AUTHENTICITY_SIGNED:
        outcome = _refuse(REFUSED_START_UNVERIFIED)
        await session.flush()
        return outcome
    if event_at is None:
        outcome = _refuse(REFUSED_START_EVENT_TIME_UNKNOWN)
        await session.flush()
        return outcome
    ban_event_at = row.last_stop_event_at if row.last_stop_event_at is not None else row.decided_event_at
    if ban_event_at is None or row.stop_event_time_unknown_at is not None:
        # The refusal's own time is unknown, so "strictly newer" is not a
        # question that has an answer. §46.4.4: an unproven state is not a
        # permission, so the refusal stands.
        #
        # The second clause is the one a provable timestamp cannot talk its way
        # past: a STOP whose event time nobody could parse has been accepted for
        # this address, and beating the LAST provable STOP says nothing about
        # beating that one. Once such a refusal exists it stays unorderable and
        # no START lifts it — a real limitation, written down in the contract
        # document rather than papered over by comparing our own reception
        # clock, which Meta promises nothing about.
        outcome = _refuse(REFUSED_START_BAN_TIME_UNKNOWN)
        await session.flush()
        return outcome
    if event_at <= ban_event_at:
        # Equal counts as not newer, deliberately: two events one second apart
        # in Meta's resolution are not ordered, and a tie must not grant.
        outcome = _refuse(REFUSED_START_NOT_NEWER)
        await session.flush()
        return outcome
    if await _start_identifier_conflict(session, address=wa_address, whatsapp_message_id=whatsapp_message_id):
        outcome = _refuse(REFUSED_START_IDENTIFIER_CONFLICT)
        await session.flush()
        return outcome

    # Honoured, and only now do the marks move: every one of them describes a
    # START this ledger actually acted on.
    if row.last_start_observed_at is None or observed_at > row.last_start_observed_at:
        row.last_start_observed_at = observed_at
    if row.last_start_event_at is None or event_at > row.last_start_event_at:
        row.last_start_event_at = event_at

    row.state = CONSENT_STATE_OPTED_IN
    row.consent_version = int(row.consent_version) + 1
    row.decision_command = CONSENT_COMMAND_START
    row.decision_source = source
    row.decision_authenticity = authenticity
    row.decision_receipt_id = receipt_id
    row.decided_observed_at = observed_at
    row.decided_event_at = event_at
    row.opted_out_at = None
    merged = dict(row.evidence or {})
    merged.update(base_evidence)
    merged["lifted_refusal_event_at"] = ban_event_at.isoformat()
    row.evidence = merged
    await session.flush()
    return DecisionOutcome(True, INBOUND_RESOLUTION_APPLIED, None, row.state, int(row.consent_version))


# ---------------------------------------------------------------------------
# Worker side: apply one stored event's consent, durably, before anything else
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsentApplication:
    """What applying one event's consent commands turned out to be."""

    event_id: int
    receipts: int = 0
    applied: int = 0
    refused: int = 0
    noop: int = 0
    skipped_reason: str | None = None
    reasons: tuple[str, ...] = ()


async def _receipts_for_event(
    session: AsyncSession,
    event_id: int,
    *,
    refresh: bool = False,
) -> list[WhatsAppInboundReceipt]:
    """The receipts of one event, in message order.

    ``refresh`` re-reads the mutable state of rows this session has already
    loaded, and it is not a nicety. A second ``SELECT`` on its own returns the
    EXISTING instances from the identity map with the attributes they had when
    they were first read, so a row another transaction has since changed still
    reads as it did before — and taking a lock does not refresh anything either.

    That cost a refusal. The consent worker read the receipts, then took the
    address locks; in between, the booking bridge proved a new number for the
    person, moved the card and stamped the proven UUID onto the still-pending
    receipt. The worker went on with its stale ``None``, wrote a suppression
    bound to no UUID and resolved the receipt — after which the only record of
    the link was a resolved row the send guard no longer looks at, and the new
    number passed the guard.
    """
    statement = (
        select(WhatsAppInboundReceipt)
        .where(WhatsAppInboundReceipt.whatsapp_event_id == event_id)
        .order_by(WhatsAppInboundReceipt.message_index.asc())
    )
    if refresh:
        statement = statement.execution_options(populate_existing=True)
    return list((await session.scalars(statement)).all())


async def _backfill_receipts(
    session: AsyncSession,
    event: WhatsAppEvent,
    *,
    observed_at: datetime,
) -> list[WhatsAppInboundReceipt]:
    """Receipts for an event that was stored before this phase existed.

    The backlog at rollout, and every event re-examined by a recovery pass, has
    no receipt: the ingress that wrote it had nowhere to put one. Its STOPs must
    still be honoured, so they are read from the stored payload and written down
    as ``unverified`` — which is the truth. The signature header was already
    stripped from the stored copy, so authenticity CANNOT be recovered, and
    pretending otherwise would be manufacturing proof.

    Keyed by ``receipt_key``, so a second pass over the same event writes
    nothing new.
    """
    drafts = extract_inbound_receipts(event.payload or {}, observed_at=observed_at)
    if not drafts:
        return []
    # What the ingress of the day decided about this delivery's route, read from
    # the stored event and from nothing else. NOT recomputed against today's
    # allow list: that list is configuration, and re-deciding an old message by
    # it would turn a route that was refused then into an executable command now,
    # or silently refuse one that was accepted. An accepted event therefore
    # leaves ``route_allowed`` NULL — "accepted, but never recorded per
    # message", which is the honest state of a row written before that column
    # existed.
    route_allowed_then = False if event.status == "ignored" else None
    await lock_consent_addresses(session, (draft.wa_address for draft in drafts))
    for draft in drafts:
        session.add(
            WhatsAppInboundReceipt(
                receipt_key=receipt_key(str(event.dedupe_key or f"event:{event.id}"), draft.message_index),
                whatsapp_event_id=int(event.id),
                message_index=draft.message_index,
                wa_address=draft.wa_address,
                command=draft.command,
                authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                sender_route_digest=sender_route_digest(draft.sender_phone_number_id),
                route_allowed=route_allowed_then,
                whatsapp_message_id=draft.whatsapp_message_id,
                observed_at=event.received_at or observed_at,
                event_at=draft.event_at,
                resolved_at=None if route_allowed_then is not False else observed_at,
                resolution=None if route_allowed_then is not False else INBOUND_RESOLUTION_IGNORED_ROUTE,
                resolution_reason=(None if route_allowed_then is not False else "sender_route_not_allowed"),
            )
        )
    # A concurrent pass writing the same receipts loses on ``receipt_key`` and
    # the error is deliberately NOT caught: this transaction has nothing to
    # commit that the other one did not, the event stays unprocessed, and the
    # retry reads the rows that are now on disk. Swallowing it would leave this
    # transaction poisoned and the receipts unresolved.
    await session.flush()
    return await _receipts_for_event(session, int(event.id))


def is_chatwoot_sourced(event: WhatsAppEvent, payload: dict[str, Any]) -> bool:
    """A Chatwoot mirror or operator relay, which must not execute commands.

    Deliberately the same two markers the inbox worker uses for inbound loop
    prevention, plus the relay marker: a mirror of a message Meta already
    delivered is the SAME message, and acting on it twice would double-apply a
    refusal and, worse, give a mirrored START a second chance at lifting one.
    """
    if "_chatwoot" in payload:
        return True
    if isinstance(payload.get("_chatwoot_operator_relay"), dict):
        return True
    dedupe_key = event.dedupe_key
    if isinstance(dedupe_key, str) and (dedupe_key.startswith("chatwoot:") or dedupe_key.startswith("chatwoot_out:")):
        return True
    return False


async def extend_refusal_to_uuid(
    session: AsyncSession,
    *,
    addresses: Iterable[object],
    customer_uuid: uuid_module.UUID,
    observed_at: datetime | None = None,
) -> int:
    """Bind existing refusals for these numbers to a NEWLY PROVEN customer UUID.

    The gap this closes, in order: a STOP arrives before any card exists, so the
    refusal is recorded against the number alone; an ordinary booking later
    proves a customer UUID for that person and adopts a card; the card's number
    then changes. From that moment the aggregate read looks for refusals on the
    numbers the cards carry NOW, the old refusal is not bound to the UUID, and
    nothing connects the two — the person who said stop becomes sendable again.

    So the binding is written at the one moment it can be justified: when the
    UUID has just been proven for this person by ``easyweek_uuid_identity``,
    against the numbers that proof was about.

    An ACCEPTED BUT UNRESOLVED message is bound too, and that half is not an
    optimisation. The refusal does not have to be in the ledger yet: the ingress
    may have accepted a STOP seconds ago and the inbox worker may not have
    reached it, and a protection that only covered existing suppression rows
    would skip exactly that case. The number then changes, the worker finally
    applies the STOP, and it writes a refusal against a number no card names any
    more — provably the same person, and provably unreachable from the new one.
    So the proof is stamped onto the pending receipt as well, and the decision
    the worker eventually makes carries it onto the suppression row.

    It stays a BAN EXTENSION. The UUID is recorded on a refusal so the refusal
    travels; it is never written anywhere that reads it back as proof of who
    this address belongs to, which branch they are in, or that they are an
    EasyWeek customer. A row already bound to a DIFFERENT UUID is left exactly
    as it is — one number can belong to two people over time, and overwriting
    would move an existing ban onto the wrong identity. The phone-keyed refusal
    keeps vetoing either way, and the disagreement is recorded.

    Returns how many durable links were written — suppression rows plus pending
    receipts — so a caller can log that something was carried forward.
    """
    now = observed_at or utcnow()
    normalised = sorted({value for value in (normalize_wa_address(raw) for raw in addresses) if value})
    if not normalised:
        return 0
    await lock_consent_addresses(session, normalised)
    bound = await _link_pending_receipts_to_uuid(session, addresses=normalised, customer_uuid=customer_uuid, now=now)
    for address in normalised:
        row = await _suppression_for_update(session, address)
        if row is None:
            continue
        if row.banned_easyweek_customer_uuid is None:
            row.banned_easyweek_customer_uuid = customer_uuid
            row.banned_uuid_recorded_at = now
            merged = dict(row.evidence or {})
            merged["uuid_binding"] = "proven_uuid_adoption"
            merged["uuid_bound_at"] = now.isoformat()
            row.evidence = merged
            bound += 1
        elif row.banned_easyweek_customer_uuid != customer_uuid:
            merged = dict(row.evidence or {})
            merged["uuid_binding_conflict_at"] = now.isoformat()
            row.evidence = merged
    await session.flush()
    return bound


async def _link_pending_receipts_to_uuid(
    session: AsyncSession,
    *,
    addresses: Sequence[str],
    customer_uuid: uuid_module.UUID,
    now: datetime,
) -> int:
    """Carry the same proof onto every unresolved receipt for these addresses.

    The caller holds the address locks, so a receipt is either already on disk
    and stamped here, or its insert is waiting for those locks and will be read
    by the guard afterwards. Rows already linked to a different UUID are left
    alone for the same reason the suppression rows are: one number can belong to
    two people over time.

    Scoped to UNRESOLVED rows deliberately. A resolved receipt has already had
    its say — whatever it decided is on the suppression row, which this function
    binds separately — and re-stamping history would claim a link for a moment
    at which nothing had been proven.
    """
    rows = list(
        (
            await session.scalars(
                select(WhatsAppInboundReceipt)
                .where(WhatsAppInboundReceipt.resolved_at.is_(None))
                .where(WhatsAppInboundReceipt.wa_address.in_(list(addresses)))
                .with_for_update()
            )
        ).all()
    )
    linked = 0
    for row in rows:
        if row.linked_easyweek_customer_uuid is None:
            row.linked_easyweek_customer_uuid = customer_uuid
            row.linked_uuid_recorded_at = now
            linked += 1
    return linked


async def apply_inbound_consent(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    event_id: int,
) -> ConsentApplication:
    """Make one event's refusals durable, in a transaction of its own.

    Called by ``process_one_event`` BEFORE the event's own transaction opens.
    That ordering is the whole contract of §46.4.6 and §46.4.7:

    * the refusal is committed before any acknowledgement is attempted, so an
      ack that fails, a Chatwoot outage, a crash or an unhandled exception
      further down cannot roll it back;
    * no lock is held across HTTP, because no HTTP happens inside it;
    * it is idempotent, so a recovery pass re-processing the same event, a
      restart, or a duplicate delivery all converge on the same state.

    Every message of the payload is applied, not only the first — and each one
    against its OWN sender address.
    """
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            event = (
                await session.execute(select(WhatsAppEvent).where(WhatsAppEvent.id == event_id))
            ).scalar_one_or_none()
            if event is None:
                return ConsentApplication(event_id=event_id, skipped_reason="event_missing")
            payload = event.payload or {}
            if not isinstance(payload, dict):
                return ConsentApplication(event_id=event_id, skipped_reason="payload_not_object")
            if is_chatwoot_sourced(event, payload):
                return ConsentApplication(event_id=event_id, skipped_reason="chatwoot_sourced")

            receipts = await _receipts_for_event(session, event_id)
            if not receipts:
                receipts = await _backfill_receipts(session, event, observed_at=now)

            # Every address of the delivery, locked at once and in sorted order,
            # BEFORE the first decision is applied.
            #
            # This was a mutual block with a durable loss behind it. The ingress
            # locks a payload's addresses sorted; this loop used to take them one
            # at a time in MESSAGE order, so a delivery of [STOP B, STOP A] left
            # the worker holding B and waiting for A while a concurrent ingress
            # for the same two addresses held A and waited for B. PostgreSQL
            # resolves that by aborting one of them, and the one it aborts here
            # is not a retryable delay: ordinary inbound has no recovery pass of
            # its own, so the event stayed `processing`, the suppression was
            # never written, and the receipts stayed unresolved for good.
            #
            # The ORDER OF APPLICATION below is untouched — commands are still
            # applied in message order, with STOP taking precedence by the
            # ledger's own rules. Only the order in which the locks are acquired
            # is fixed, and it is now the same one on both sides.
            # Every address of the delivery, locked at once and in sorted order,
            # BEFORE the first decision is applied.
            #
            # This was a mutual block with a durable loss behind it. The ingress
            # locks a payload's addresses sorted; this loop used to take them one
            # at a time in MESSAGE order, so a delivery of [STOP B, STOP A] left
            # the worker holding B and waiting for A while a concurrent ingress
            # for the same two addresses held A and waited for B. PostgreSQL
            # resolves that by aborting one of them, and the one it aborts here
            # is not a retryable delay: ordinary inbound has no recovery pass of
            # its own, so the event stayed `processing`, the suppression was
            # never written, and the receipts stayed unresolved for good.
            #
            # The ORDER OF APPLICATION below is untouched — commands are still
            # applied in message order, with STOP taking precedence by the
            # ledger's own rules. Only the order in which the locks are acquired
            # is fixed, and it is now the same one on both sides.
            locked = set(await lock_consent_addresses(session, (receipt.wa_address for receipt in receipts)))

            # And only now is the receipt state read for real. Everything above
            # could be stale: the rows were loaded before the locks existed, and
            # neither a lock nor a second SELECT refreshes an instance this
            # session already holds. ``linked_easyweek_customer_uuid`` and
            # ``resolved_at`` are exactly the two attributes another transaction
            # may have changed in that window.
            receipts = await _receipts_for_event(session, event_id, refresh=True)

            applied = refused = noop = 0
            reasons: list[str] = []
            for receipt in receipts:
                if receipt.resolved_at is not None:
                    continue
                if receipt.wa_address not in locked:
                    # An address the locks do not cover, which can only come
                    # from a concurrent backfill of this same event. Left
                    # unresolved on purpose: an unresolved receipt HOLDS sends,
                    # so the fail-closed answer is to leave it for a pass that
                    # locks it rather than to decide it unserialised.
                    logger.warning("inbound receipt arrived outside the locked address set event_id=%s", event_id)
                    continue
                if receipt.route_allowed is False:
                    # Belt to the ingress's braces. A refused route is already
                    # resolved when it is written, so reaching this would mean a
                    # row got here another way — and a command from a sender
                    # route this deployment refuses must not be executed because
                    # some other message in the same delivery was allowed.
                    receipt.resolved_at = now
                    receipt.resolution = INBOUND_RESOLUTION_IGNORED_ROUTE
                    receipt.resolution_reason = "sender_route_not_allowed"
                    noop += 1
                    continue
                if receipt.command == CONSENT_COMMAND_NONE:
                    # An ordinary message is not consent in either direction.
                    # It is still resolved: the hold it placed on sending exists
                    # because nobody had looked at it, and now somebody has.
                    receipt.resolved_at = now
                    receipt.resolution = INBOUND_RESOLUTION_NOOP
                    receipt.resolution_reason = None
                    noop += 1
                    continue

                # No lock is taken here: this address is already held, with
                # every other address of the delivery, in the one sorted order
                # above.
                outcome = await apply_consent_decision(
                    session,
                    wa_address=receipt.wa_address,
                    command=receipt.command,
                    authenticity=receipt.authenticity,
                    source=CONSENT_SOURCE_INBOUND,
                    observed_at=receipt.observed_at or now,
                    event_at=receipt.event_at,
                    receipt_id=int(receipt.id),
                    whatsapp_message_id=receipt.whatsapp_message_id,
                    linked_customer_uuid=receipt.linked_easyweek_customer_uuid,
                )
                receipt.resolved_at = now
                receipt.resolution = outcome.resolution
                receipt.resolution_reason = outcome.reason
                if outcome.resolution == INBOUND_RESOLUTION_APPLIED:
                    applied += 1
                elif outcome.resolution == INBOUND_RESOLUTION_REFUSED:
                    refused += 1
                else:
                    noop += 1
                if outcome.reason:
                    reasons.append(outcome.reason)

            return ConsentApplication(
                event_id=event_id,
                receipts=len(receipts),
                applied=applied,
                refused=refused,
                noop=noop,
                reasons=tuple(reasons),
            )


# ---------------------------------------------------------------------------
# The aggregate read: every local refusal that matches this person
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsentDecision:
    """Every local refusal that matches one person, and what is still unknown.

    ``sources`` is what refused and ``gaps`` is what could not be established.
    Both are reported rather than collapsed into the boolean, because an
    operator reading a preview has to be able to tell "this person said stop"
    from "we could not tell, so we did not send".
    """

    address: str | None
    suppressed: bool
    sources: tuple[str, ...] = ()
    gaps: tuple[str, ...] = ()
    pending_inbound: int = 0
    fingerprint: str = ""
    primary_version: int | None = None
    matched_addresses: tuple[str, ...] = ()
    # The exact unresolved receipts this answer was taken over, not just how
    # many: the identities are what a claim-time re-check compares, so one
    # message being resolved while another arrives cannot cancel out.
    pending_receipt_ids: tuple[int, ...] = ()
    easyweek_customer_uuid: uuid_module.UUID | None = None

    @property
    def allowed_by_local_state(self) -> bool:
        """No PROVEN local refusal, and nothing unresolved.

        Named at length on purpose. It does NOT mean marketing consent was
        obtained; it means this database holds no proven refusal and nothing
        pending for this person. Consent itself is established elsewhere, and
        the absence of a ban is never evidence of it.
        """
        return not self.suppressed and self.pending_inbound == 0


def _fingerprint(
    pairs: Sequence[tuple[str, int]],
    *,
    suppressed: bool,
    addresses: Sequence[str] = (),
    pending_receipt_ids: Sequence[int] = (),
) -> str:
    """A handle over everything this decision was taken from.

    One number cannot describe several matched addresses, and a later claim has
    to be able to notice a row that did not exist when the plan was made — so
    the guard hands out a digest over every (address, version) pair plus the
    verdict, and the claim recomputes it. A new row, a bumped version or a
    flipped verdict all change it.

    Two more things are inside it, and both are defects this closed. The ADDRESS
    SET is here because a card gaining or losing a number changes which
    refusals apply to this person without touching any suppression row. The
    PENDING RECEIPT IDENTITIES are here because an unresolved inbound message
    may BE the refusal: a fingerprint that ignored them matched happily while a
    STOP sat in the pending set, and a sender re-checking only the fingerprint
    would have gone ahead with a send a fresh guard was already refusing.
    """
    material = "|".join(f"{address}:{version}" for address, version in sorted(pairs))
    scope = ",".join(sorted(addresses))
    pending = ",".join(str(int(value)) for value in sorted(pending_receipt_ids))
    return _digest(
        "altegio_bot/whatsapp_consent/fingerprint/v2",
        f"{material}#{scope}#{pending}#{int(suppressed)}",
    )


async def _uuid_linked_addresses(
    session: AsyncSession,
    customer_uuid: uuid_module.UUID | None,
) -> list[str]:
    """Every phone an EasyWeek card for *customer_uuid* currently carries.

    The second half of §46.4.2. A refusal recorded against an old number has to
    keep applying after the number changes, and a refusal recorded against the
    number of a card that only later gained its UUID has to keep applying too —
    so the ban travels in both directions: stored UUID → any address, and
    UUID → the addresses its cards hold now.
    """
    if customer_uuid is None:
        return []
    rows = (
        await session.scalars(
            select(Client.phone_e164).where(
                Client.provider == PROVIDER_EASYWEEK,
                Client.easyweek_customer_uuid == customer_uuid,
                Client.phone_e164.is_not(None),
            )
        )
    ).all()
    addresses: set[str] = set()
    for value in rows:
        normalised = normalize_wa_address(value)
        if normalised is not None:
            addresses.update(address_variants(normalised))
    return sorted(addresses)


async def _historically_linked_addresses(
    session: AsyncSession,
    customer_uuid: uuid_module.UUID | None,
) -> list[str]:
    """Addresses this UUID is PROVABLY linked to that no card carries any more.

    The cards hold today's numbers. They are not the history of the negative
    links, and §46.4.2 needs that history: a refusal — or an accepted message
    that has not been decided yet — recorded against a number the person used to
    have is still about this person after the card moves on, and the old number
    is then reachable from nowhere else.

    Two sources, both of them facts this database already wrote down for the ban
    to travel: suppression rows bound to this UUID, and unresolved receipts
    stamped with it when the booking bridge proved it. Used ONLY to widen the
    search for refusals and the set of addresses to serialise against — never
    read back as proof that one of them belongs to this customer.
    """
    if customer_uuid is None:
        return []
    addresses: set[str] = set()
    suppressed = (
        await session.scalars(
            select(WhatsAppConsentSuppression.wa_address).where(
                WhatsAppConsentSuppression.banned_easyweek_customer_uuid == customer_uuid
            )
        )
    ).all()
    pending = (
        await session.scalars(
            select(WhatsAppInboundReceipt.wa_address)
            .where(WhatsAppInboundReceipt.linked_easyweek_customer_uuid == customer_uuid)
            .where(WhatsAppInboundReceipt.resolved_at.is_(None))
        )
    ).all()
    for value in (*suppressed, *pending):
        normalised = normalize_wa_address(value)
        if normalised is not None:
            addresses.update(address_variants(normalised))
    return sorted(addresses)


@dataclass(frozen=True)
class _Identity:
    """Who the refusal search is about, before anything is read or locked."""

    address: str | None
    customer_uuid: uuid_module.UUID | None
    # Every address a refusal for this person could be recorded against: what to
    # search, and what to lock.
    addresses: tuple[str, ...]
    # The subset of those that no card of this person carries today, reached
    # only through a stored negative link. Kept apart because the operator
    # report needs to be able to say so — "this refusal is on a number they used
    # to have" is a different sentence from "this refusal is on their number".
    linked_addresses: tuple[str, ...] = ()
    gaps: tuple[str, ...] = ()


async def _resolve_identity(
    session: AsyncSession,
    *,
    phone: object,
    easyweek_customer_uuid: uuid_module.UUID | str | None,
) -> _Identity:
    """Every address a refusal for this person could be recorded against.

    Three sources, and the third one is the fix for a real hole: the address
    itself in every spelling a phone column may hold; every number an EasyWeek
    card of this UUID carries now; and — when the caller did not name a UUID —
    the one a card for this number names, so a refusal recorded against an older
    number of the same proven person is still found by a caller that only has
    the new number.

    That derivation is a BAN EXTENSION, exactly as §46.4.2 permits, and nothing
    else: it is used to widen the search for refusals, never read back as proof
    that this address belongs to that customer or to that branch. An ambiguous
    phone is reported as a gap and derives nothing.
    """
    address = normalize_wa_address(phone)
    customer_uuid: uuid_module.UUID | None
    if isinstance(easyweek_customer_uuid, str):
        try:
            customer_uuid = uuid_module.UUID(easyweek_customer_uuid)
        except ValueError:
            customer_uuid = None
    else:
        customer_uuid = easyweek_customer_uuid

    if address is None:
        return _Identity(None, customer_uuid, (), gaps=(GAP_ADDRESS_UNNORMALISABLE,))

    gaps: list[str] = []
    if customer_uuid is None:
        derived, ambiguous = await _unambiguous_banned_uuid(session, address)
        if ambiguous:
            gaps.append(GAP_UUID_BINDING_AMBIGUOUS)
        customer_uuid = derived

    addresses = set(address_variants(address))
    addresses.update(await _uuid_linked_addresses(session, customer_uuid))
    linked = {value for value in await _historically_linked_addresses(session, customer_uuid) if value not in addresses}
    addresses.update(linked)
    return _Identity(
        address,
        customer_uuid,
        tuple(sorted(addresses)),
        tuple(sorted(linked)),
        tuple(gaps),
    )


async def consent_decision(
    session: AsyncSession,
    *,
    phone: object,
    easyweek_customer_uuid: uuid_module.UUID | str | None = None,
    include_pending: bool = True,
) -> ConsentDecision:
    """Read every local refusal that matches this person. No locks, no writes.

    Lock-free on purpose. This is called from inside transactions that already
    hold a batch header or a client row — the voucher freeze and the per-item
    claim among them — so taking anything here would introduce a second lock
    order into paths that have exactly one. The durable serialisation that
    matters happens where decisions are WRITTEN, and the final send guard takes
    the address locks itself.

    The four sources of §46.4.3, in one answer:

    1. the suppression ledger for this address and for every address an EasyWeek
       card of this UUID holds;
    2. a suppression bound to this UUID by an earlier refusal;
    3. ``Client.wa_opted_out`` on ANY matching card — another branch's or an
       Altegio one, which is the point: a refusal is not scoped to a provider;
    4. the historical ``CampaignRecipient.is_opted_out`` snapshot, unless a
       proven, verified opt-in landed after that snapshot was taken.
    """
    identity = await _resolve_identity(session, phone=phone, easyweek_customer_uuid=easyweek_customer_uuid)
    address = identity.address
    customer_uuid = identity.customer_uuid

    if address is None:
        # Nothing to match. Reported as a gap, never as permission: an address
        # we cannot normalise is an address we cannot clear.
        return ConsentDecision(
            address=None,
            suppressed=False,
            gaps=identity.gaps,
            fingerprint=_fingerprint((), suppressed=False),
        )

    ordered_addresses = list(identity.addresses)

    conditions = [WhatsAppConsentSuppression.wa_address.in_(ordered_addresses)]
    if customer_uuid is not None:
        conditions.append(WhatsAppConsentSuppression.banned_easyweek_customer_uuid == customer_uuid)
    rows = list(
        (
            await session.scalars(
                select(WhatsAppConsentSuppression).where(or_(*conditions)).order_by(WhatsAppConsentSuppression.id.asc())
            )
        ).all()
    )

    sources: list[str] = []
    gaps: list[str] = list(identity.gaps)
    suppressed = False
    newest_opt_in: datetime | None = None
    for row in rows:
        if row.state == CONSENT_STATE_OPTED_OUT:
            suppressed = True
            sources.append(
                SOURCE_SUPPRESSION_UUID
                if row.wa_address in identity.linked_addresses
                or (row.wa_address not in ordered_addresses and row.banned_easyweek_customer_uuid is not None)
                else SOURCE_SUPPRESSION
            )
        elif row.decided_observed_at is not None and (newest_opt_in is None or row.decided_observed_at > newest_opt_in):
            newest_opt_in = row.decided_observed_at
        if row.banned_easyweek_customer_uuid is None and (row.evidence or {}).get("uuid_binding") == "ambiguous":
            gaps.append(GAP_UUID_BINDING_AMBIGUOUS)

    client_conditions = [Client.phone_e164.in_(ordered_addresses)]
    if customer_uuid is not None:
        client_conditions.append(
            (Client.provider == PROVIDER_EASYWEEK) & (Client.easyweek_customer_uuid == customer_uuid)
        )
    opted_out_card = await session.scalar(
        select(Client.id).where(Client.wa_opted_out.is_(True)).where(or_(*client_conditions)).limit(1)
    )
    if opted_out_card is not None:
        suppressed = True
        sources.append(SOURCE_CLIENT_FLAG)

    recipient_rows = (
        await session.scalars(
            select(CampaignRecipient.created_at)
            .where(CampaignRecipient.is_opted_out.is_(True))
            .where(CampaignRecipient.phone_e164.in_(ordered_addresses))
            .order_by(CampaignRecipient.created_at.desc().nullslast())
            .limit(50)
        )
    ).all()
    for created_at in recipient_rows:
        # A snapshot taken BEFORE a proven, verified opt-in has been answered by
        # that opt-in and is not a live refusal. One taken after it, or one whose
        # own time is unknown, still refuses — an unproven order never grants.
        if newest_opt_in is not None and created_at is not None and created_at < newest_opt_in:
            continue
        suppressed = True
        sources.append(SOURCE_RECIPIENT_FLAG)
        break

    pending_ids: list[int] = []
    if include_pending:
        pending_conditions = [WhatsAppInboundReceipt.wa_address.in_(ordered_addresses)]
        if customer_uuid is not None:
            # A message accepted from a number this UUID was PROVEN to use is
            # still unaccounted for, whatever the cards say today. Without this
            # clause a STOP that arrived moments before the card's number
            # changed was invisible to the guard for the new number, and the
            # send went ahead while the refusal sat unread.
            pending_conditions.append(WhatsAppInboundReceipt.linked_easyweek_customer_uuid == customer_uuid)
        pending_ids = [
            int(value)
            for value in (
                await session.scalars(
                    select(WhatsAppInboundReceipt.id)
                    .where(WhatsAppInboundReceipt.resolved_at.is_(None))
                    .where(or_(*pending_conditions))
                    .order_by(WhatsAppInboundReceipt.id.asc())
                    .limit(200)
                )
            ).all()
        ]
        if pending_ids:
            gaps.append(GAP_PENDING_INBOUND)

    primary = next((int(row.consent_version) for row in rows if row.wa_address == address), None)
    return ConsentDecision(
        address=address,
        suppressed=suppressed,
        sources=tuple(dict.fromkeys(sources)),
        gaps=tuple(dict.fromkeys(gaps)),
        pending_inbound=len(pending_ids),
        fingerprint=_fingerprint(
            [(str(row.wa_address), int(row.consent_version)) for row in rows],
            suppressed=suppressed,
            addresses=ordered_addresses,
            pending_receipt_ids=pending_ids,
        ),
        primary_version=primary,
        matched_addresses=tuple(ordered_addresses),
        pending_receipt_ids=tuple(pending_ids),
        easyweek_customer_uuid=customer_uuid,
    )


@dataclass(frozen=True)
class LedgerState:
    """What the ledger says about ONE address, for projecting onto a card.

    Deliberately narrow — this one address's row, not the aggregate. The card
    flag of this number must follow the decision made about this number; a
    refusal recorded against another number of the same identity bans the send
    through :func:`consent_decision`, which is where that question belongs.
    """

    state: str | None
    consent_version: int | None = None
    last_refused_start_reason: str | None = None
    # When the decision in force was OBSERVED by this process. For an
    # ``opted_in`` row that is the moment a proven opt-in landed, which is what
    # a historical source with no event time of its own has to be ordered
    # against — our own clock on both sides, never a timestamp invented for the
    # snapshot.
    decided_observed_at: datetime | None = None

    @property
    def refuses(self) -> bool:
        return self.state == CONSENT_STATE_OPTED_OUT

    @property
    def permits(self) -> bool:
        """A refusal was PROVEN lifted. Not "there is no refusal"."""
        return self.state == CONSENT_STATE_OPTED_IN


async def ledger_state(session: AsyncSession, *, phone: object) -> LedgerState:
    """The authoritative consent state of one address, or ``None`` if unrecorded.

    This is what the inbox worker projects onto ``Client.wa_opted_out``. It
    exists because the worker used to derive that flag from the COMMAND it had
    just read — so a refused START, one the ledger would not honour, still
    cleared the card flag that message_planner, the outbox worker and every
    marketing path actually read. The ledger decides; the card follows.
    """
    address = normalize_wa_address(phone)
    if address is None:
        return LedgerState(None)
    # Values, not the mapped entity. An entity already loaded in this session
    # comes back from the identity map with the attributes it had when it was
    # first read, so a second call inside one transaction would hand back a
    # stale answer — and the projection below re-reads deliberately, to find out
    # whether the ledger moved under it.
    row = (
        await session.execute(
            select(
                WhatsAppConsentSuppression.state,
                WhatsAppConsentSuppression.consent_version,
                WhatsAppConsentSuppression.evidence,
                WhatsAppConsentSuppression.decided_observed_at,
            ).where(WhatsAppConsentSuppression.wa_address == address)
        )
    ).first()
    if row is None:
        return LedgerState(None)
    state, version, evidence, decided_observed_at = row
    reason = (evidence or {}).get("last_refused_start_reason")
    return LedgerState(
        str(state),
        int(version),
        str(reason) if isinstance(reason, str) else None,
        decided_observed_at,
    )


def ledger_matches_clause(phone: object, observed: LedgerState) -> Any:
    """A SQL condition that holds only while the ledger still reads as *observed*.

    The compare-and-set half of §46.4.7's projection contract. The inbox worker
    derives the legacy card flag from the ledger and then writes it, and those
    are two statements: between them a newer STOP can be decided and projected
    by another event's handler, and the older handler would then write its stale
    answer over the newer one — the ledger refusing while ``Client.wa_opted_out``
    said sendable, which is the flag every marketing path reads.

    Holding a lock across that gap is not available: the write happens in a
    transaction that goes on to acknowledge the customer over HTTP, and no lock
    may be held across a network call. So the write carries its premise instead.
    An UPDATE guarded by this clause lands only while the ledger still says
    exactly what the value was derived from, and the writer re-reads afterwards
    and derives again if it moved. A stale projection cannot land, and the last
    projection to commit is the one that read the final state.

    ``observed.state is None`` means "no row for this address", and the clause is
    then the negative one: a suppression appearing in between must invalidate the
    legacy fallback just as surely as a changed one.
    """
    address = normalize_wa_address(phone)
    if address is None:
        # Nothing to compare, and nothing this clause could make safe.
        return text("false")
    matching = select(WhatsAppConsentSuppression.id).where(WhatsAppConsentSuppression.wa_address == address)
    if observed.state is None:
        return ~matching.exists()
    return matching.where(
        WhatsAppConsentSuppression.state == observed.state,
        WhatsAppConsentSuppression.consent_version == int(observed.consent_version or 0),
    ).exists()


async def is_suppressed(
    session: AsyncSession,
    *,
    phone: object,
    easyweek_customer_uuid: uuid_module.UUID | str | None = None,
) -> bool:
    """Is there a PROVEN local refusal for this person?

    The narrow question, for call sites that already have their own eligibility
    story and only need the veto. Pending inbound is NOT folded in here: a hold
    is a send-time concern and belongs to :func:`final_send_guard`, while a
    composition read that treated "something unread arrived" as a refusal would
    flip rows in and out of a preview for reasons an operator cannot see.
    """
    decision = await consent_decision(
        session,
        phone=phone,
        easyweek_customer_uuid=easyweek_customer_uuid,
        include_pending=False,
    )
    return decision.suppressed


# ---------------------------------------------------------------------------
# The final send boundary — the contract PR-22.3's executor must honour
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FinalSendGuard:
    """The verdict at the last moment before an external request may be made.

    What it does and does not promise
    ---------------------------------
    It promises that at the instant it committed, this database held no proven
    refusal for this person and nothing unresolved from them — and it hands back
    a ``fingerprint`` the caller must re-check inside the same transaction as
    its send claim.

    It does NOT promise to cancel a request that has already left the process.
    Nothing can. A STOP that arrives while a POST is in flight is recorded with
    its own time, the POST's answer is recorded with its own, and the report
    says both. The guarantee is one-sided and bounded on purpose: zero requests
    once a refusal is durable BEFORE this boundary, and an honest record of the
    ordering when it is not.
    """

    allowed: bool
    reason: str
    decision: ConsentDecision

    @property
    def fingerprint(self) -> str:
        return self.decision.fingerprint


async def final_send_guard(
    session: AsyncSession,
    *,
    phone: object,
    easyweek_customer_uuid: uuid_module.UUID | str | None = None,
) -> FinalSendGuard:
    """Take the address locks, then answer. In the CALLER's transaction.

    The locks are what make the ordering single and checkable (§46.4.6): a
    concurrent consent write for any of these addresses — or a receipt insert at
    the ingress, which takes the same locks before it writes — either commits
    before this guard and is seen by it, or waits until this transaction ends and
    is then caught by the claim-time re-check. There is no third interleaving,
    and no row has to exist for a lock to work, which is what closes the
    absent-row race for an address that has never been heard from.

    Every address is locked, not only the one passed in: a refusal recorded
    against another number of the same proven EasyWeek identity bans this send
    just as hard, so leaving those out would have left a real refusal able to
    land unserialised.

    No HTTP happens under these locks, and none may: the caller is expected to
    commit its claim and only then make its external request.
    """
    identity = await _resolve_identity(session, phone=phone, easyweek_customer_uuid=easyweek_customer_uuid)
    if identity.address is None:
        return FinalSendGuard(
            False,
            GUARD_ADDRESS_UNPROVEN,
            ConsentDecision(
                address=None,
                suppressed=False,
                gaps=(GAP_ADDRESS_UNNORMALISABLE,),
                fingerprint=_fingerprint((), suppressed=False),
            ),
        )
    locked = await lock_consent_addresses(session, identity.addresses or (identity.address,))
    decision = await consent_decision(
        session,
        phone=identity.address,
        easyweek_customer_uuid=identity.customer_uuid,
        include_pending=True,
    )
    return _verdict(decision, locked=locked)


def _verdict(decision: ConsentDecision, *, locked: Sequence[str]) -> FinalSendGuard:
    """One place where an answer becomes an allow or a hold.

    Shared by the guard and by the claim-time re-check deliberately: two
    separate readings of what counts as allowed is how a boundary ends up
    permitting something at the claim that the guard itself refused.
    """
    if decision.suppressed:
        return FinalSendGuard(False, GUARD_SUPPRESSED, decision)
    if decision.pending_inbound:
        # §46.4.6. A durable inbound message this process has not accounted for
        # may be carrying the refusal, so the send is HELD rather than allowed:
        # a queue that is behind must not become a permission.
        return FinalSendGuard(False, GUARD_PENDING_INBOUND, decision)
    if not set(decision.matched_addresses) <= set(locked):
        # The read reached an address the locks do not cover, because a card
        # gained a number between the two. Held rather than allowed: this answer
        # is not one the locks make safe, and the next attempt sees a settled
        # set.
        return FinalSendGuard(False, GUARD_ADDRESS_SET_MOVED, decision)
    return FinalSendGuard(True, GUARD_ALLOWED, decision)


async def recheck_consent_boundary(
    session: AsyncSession,
    *,
    phone: object,
    expected_fingerprint: str,
    easyweek_customer_uuid: uuid_module.UUID | str | None = None,
) -> FinalSendGuard:
    """Re-prove the boundary inside the transaction that claims the send.

    This replaced a bare fingerprint comparison, and the difference is the whole
    point of the function. The old check answered "is the state the one I was
    issued against?" and nothing else, so a STOP sitting unresolved in the
    pending set — which a fresh guard refuses on sight — compared equal and the
    claim went ahead. It also held no lock, so a receipt could commit between
    the comparison and the claim without either side noticing.

    So this takes the same locks the guard takes, re-asks the same question
    through the same verdict, AND requires the fingerprint to be the one the
    guard handed out. All three, because each catches something the others do
    not: the locks order it against concurrent writers, the verdict catches a
    refusal or a hold that appeared since, and the fingerprint catches a
    decision that landed and was answered in between — a STOP then a proven
    START, say, which leaves the verdict allowed and the version moved.
    """
    guard = await final_send_guard(session, phone=phone, easyweek_customer_uuid=easyweek_customer_uuid)
    if not guard.allowed:
        return guard
    if not expected_fingerprint or guard.decision.fingerprint != expected_fingerprint:
        return FinalSendGuard(False, GUARD_CONSENT_MOVED, guard.decision)
    return guard


# ---------------------------------------------------------------------------
# Historical projection support
# ---------------------------------------------------------------------------


@dataclass
class ProjectedRefusal:
    """One refusal a historical source says exists, before anything is written."""

    wa_address: str
    origin: str
    observed_at: datetime
    event_at: datetime | None = None
    authenticity: str = INBOUND_AUTHENTICITY_UNVERIFIED
    whatsapp_event_id: int | None = None
    receipt_id: int | None = None
    # Is this a SNAPSHOT of a state — a card flag, a preview row — rather than a
    # message somebody sent? Load-bearing. A snapshot has no event of its own,
    # so one taken before a proven opt-in is a copy of a state the ledger has
    # already moved past and must not refuse again. A stored STOP MESSAGE is not
    # a snapshot: its order may be unknown, but it is a refusal somebody really
    # sent, and treating the moment we happened to RECEIVE it as proof that it
    # is stale made this projection disagree with live processing about the same
    # message.
    snapshot: bool = False
    notes: dict[str, Any] = field(default_factory=dict)


async def apply_projected_refusal(
    session: AsyncSession,
    refusal: ProjectedRefusal,
) -> DecisionOutcome:
    """Write one projected historical refusal through the ordinary ledger path.

    Through the same function the live worker uses, deliberately. The projection
    gets no private write path, so it cannot be the thing that overwrites a
    newer live decision: every monotonicity rule, the address lock and the
    START proof requirements apply to it exactly as they do to an inbound
    message. A projection replayed from a stale snapshot therefore converges
    instead of regressing.

    STOP only. A historical START is never projected: its authenticity cannot be
    recovered from a sanitised stored event, so it could not lift anything even
    if it were, and writing it down would create an opt-in record out of a
    payload nobody can verify.

    And the decision is re-made HERE, under the lock, not only when the
    candidate was collected. That was a defect with two halves. A
    ``CampaignRecipient`` snapshot taken at 10:10 was still written as a fresh
    refusal after a proven START at 10:30 — and because a snapshot has no event
    time of its own, the refusal it created was one of UNKNOWN order, which
    §46.4.4 then makes permanent: no later START can lift it. The other half was
    the collection-time shortcut, which skipped a candidate whenever any refusal
    was already active, so a stored STOP at 11:00 never advanced the boundary
    past the 10:00 one and a START at 10:30 lifted a refusal the database could
    prove was newer than it.

    A SNAPSHOT with no provable event time is therefore ordered against the
    moment a proven opt-in for THIS address was observed. Both sides of that
    comparison are this process's own clock, so nothing external is invented;
    and it is strictly per address, because a proven opt-in on another number of
    the same person is not an opt-in for this one.

    A stored STOP MESSAGE is never ordered that way. That was the second mistake
    in the same rule: a refusal somebody SENT is not stale because of when this
    process happened to receive it. A delivery can sit in a queue — a signed
    START sent at 10:10 can arrive at 10:30, behind a STOP whose own timestamp
    is unreadable and which was received at 10:20 — and dropping the STOP on
    that basis made this module disagree with :func:`apply_inbound_consent`
    about one and the same message: live processing applies it and refuses,
    while the reconciliation skipped it and the send went ahead. A real message
    goes through the ordinary ledger rules, where an unknown order refuses.
    """
    await lock_consent_address(session, refusal.wa_address)
    if refusal.snapshot and refusal.event_at is None:
        current = await _suppression_for_update(session, refusal.wa_address)
        if (
            current is not None
            and current.state == CONSENT_STATE_OPTED_IN
            and current.decided_observed_at is not None
            and refusal.observed_at < current.decided_observed_at
        ):
            return DecisionOutcome(
                False,
                INBOUND_RESOLUTION_NOOP,
                NOOP_PROJECTION_SUPERSEDED,
                current.state,
                int(current.consent_version),
            )
    evidence: dict[str, Any] = {"projected_from": refusal.origin}
    evidence.update(refusal.notes)
    return await apply_consent_decision(
        session,
        wa_address=refusal.wa_address,
        command=CONSENT_COMMAND_STOP,
        authenticity=refusal.authenticity,
        source=CONSENT_SOURCE_PROJECTION,
        observed_at=refusal.observed_at,
        event_at=refusal.event_at,
        receipt_id=refusal.receipt_id,
        evidence=evidence,
    )


def iter_payload_consent_commands(
    payload: dict[str, Any],
    *,
    observed_at: datetime,
) -> Iterable[InboundReceiptDraft]:
    """The STOP/START drafts of a stored payload, for the projection's preview.

    The same extractor the ingress uses, so a historical «стоп» that the old
    keyword set could not see is now found by exactly the rule that will see the
    next live one — rather than by a second, looser scan written for the
    backlog.
    """
    for draft in extract_inbound_receipts(payload, observed_at=observed_at):
        if draft.command != CONSENT_COMMAND_NONE:
            yield draft


__all__ = [
    "GAP_ADDRESS_UNNORMALISABLE",
    "NOOP_CARD_RECOVERY",
    "NOOP_PROJECTION_SUPERSEDED",
    "GAP_PENDING_INBOUND",
    "GAP_UUID_BINDING_AMBIGUOUS",
    "GUARD_ADDRESS_SET_MOVED",
    "GUARD_ADDRESS_UNPROVEN",
    "GUARD_ALLOWED",
    "GUARD_CONSENT_MOVED",
    "GUARD_PENDING_INBOUND",
    "GUARD_SUPPRESSED",
    "NOOP_START_NO_ACTIVE_BAN",
    "NOOP_STOP_ALREADY_ACTIVE",
    "NOOP_STOP_SUPERSEDED",
    "REFUSED_START_BAN_TIME_UNKNOWN",
    "REFUSED_START_EVENT_TIME_UNKNOWN",
    "REFUSED_START_IDENTIFIER_CONFLICT",
    "REFUSED_START_NOT_NEWER",
    "REFUSED_START_UNVERIFIED",
    "SOURCE_CLIENT_FLAG",
    "SOURCE_RECIPIENT_FLAG",
    "SOURCE_SUPPRESSION",
    "SOURCE_SUPPRESSION_UUID",
    "ConsentApplication",
    "ConsentDecision",
    "DecisionOutcome",
    "FinalSendGuard",
    "InboundReceiptDraft",
    "LedgerState",
    "ProjectedRefusal",
    "address_variants",
    "apply_consent_decision",
    "apply_inbound_consent",
    "apply_projected_refusal",
    "consent_decision",
    "extend_refusal_to_uuid",
    "extract_inbound_receipts",
    "final_send_guard",
    "is_suppressed",
    "is_chatwoot_sourced",
    "iter_payload_consent_commands",
    "ledger_matches_clause",
    "ledger_state",
    "lock_consent_address",
    "lock_consent_addresses",
    "normalize_wa_address",
    "receipt_key",
    "recheck_consent_boundary",
    "recipient_route_digest",
    "record_inbound_receipts",
    "sender_route_digest",
]
