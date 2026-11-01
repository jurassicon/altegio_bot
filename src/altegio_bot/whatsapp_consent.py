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

* a STOP applies whatever its authenticity and whatever its timestamp;
* a START lifts a refusal only when Meta's signature was configured AND
  verified for that very request, its own event time is proven, the refusal's
  event time is proven, and the START's is strictly later.

Everything else — an unsigned request, a deployment with no app secret, a
replayed payload, an equal timestamp, a timestamp nobody can parse, a historical
ban of unknown time, a reused message identifier — refuses, with a reason code.
That is not a gap to be closed later; it is §46.4.7 and §46.4.4 written out.

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
from typing import Any, Final, Iterable, Sequence

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
    return hashlib.sha256(f"{domain}:{value}".encode("utf-8")).hexdigest()


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
        hashlib.sha256(f"{_ADDRESS_LOCK_DOMAIN}:{address}".encode("utf-8")).digest()[:8],
        "big",
        signed=True,
    )
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})


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
    ignored_route: bool = False,
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

    An event this deployment will never process — a sender route that is not
    allowed — is written down and resolved in the same breath. It still happened,
    an operator should be able to see that it did, and leaving it pending would
    hold sends forever waiting for a worker that is never going to look at it.
    """
    drafts = extract_inbound_receipts(payload, observed_at=observed_at)
    if not drafts:
        return 0

    authenticity = INBOUND_AUTHENTICITY_SIGNED if signature_verified else INBOUND_AUTHENTICITY_UNVERIFIED
    for draft in drafts:
        session.add(
            WhatsAppInboundReceipt(
                receipt_key=receipt_key(dedupe_key, draft.message_index),
                whatsapp_event_id=event_id,
                message_index=draft.message_index,
                wa_address=draft.wa_address,
                command=draft.command,
                authenticity=authenticity,
                whatsapp_message_id=draft.whatsapp_message_id,
                observed_at=observed_at,
                event_at=draft.event_at,
                resolved_at=observed_at if ignored_route else None,
                resolution=INBOUND_RESOLUTION_IGNORED_ROUTE if ignored_route else None,
                resolution_reason="ingress_sender_route_not_allowed" if ignored_route else None,
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

    if row.last_start_observed_at is None or observed_at > row.last_start_observed_at:
        row.last_start_observed_at = observed_at
    if event_at is not None and (row.last_start_event_at is None or event_at > row.last_start_event_at):
        row.last_start_event_at = event_at

    def _refuse(reason: str) -> DecisionOutcome:
        merged = dict(row.evidence or {})
        merged.update(base_evidence)
        merged["last_refused_start_reason"] = reason
        merged["last_refused_start_at"] = observed_at.isoformat()
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
    if ban_event_at is None:
        # The refusal's own time is unknown, so "strictly newer" is not a
        # question that has an answer. §46.4.4: an unproven state is not a
        # permission, so the refusal stands.
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


async def _receipts_for_event(session: AsyncSession, event_id: int) -> list[WhatsAppInboundReceipt]:
    return list(
        (
            await session.scalars(
                select(WhatsAppInboundReceipt)
                .where(WhatsAppInboundReceipt.whatsapp_event_id == event_id)
                .order_by(WhatsAppInboundReceipt.message_index.asc())
            )
        ).all()
    )


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
    for draft in drafts:
        session.add(
            WhatsAppInboundReceipt(
                receipt_key=receipt_key(str(event.dedupe_key or f"event:{event.id}"), draft.message_index),
                whatsapp_event_id=int(event.id),
                message_index=draft.message_index,
                wa_address=draft.wa_address,
                command=draft.command,
                authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                whatsapp_message_id=draft.whatsapp_message_id,
                observed_at=event.received_at or observed_at,
                event_at=draft.event_at,
            )
        )
    # A concurrent pass writing the same receipts loses on ``receipt_key`` and
    # the error is deliberately NOT caught: this transaction has nothing to
    # commit that the other one did not, the event stays unprocessed, and the
    # retry reads the rows that are now on disk. Swallowing it would leave this
    # transaction poisoned and the receipts unresolved.
    await session.flush()
    return await _receipts_for_event(session, int(event.id))


def _is_chatwoot_sourced(event: WhatsAppEvent, payload: dict[str, Any]) -> bool:
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
            if _is_chatwoot_sourced(event, payload):
                return ConsentApplication(event_id=event_id, skipped_reason="chatwoot_sourced")

            receipts = await _receipts_for_event(session, event_id)
            if not receipts:
                receipts = await _backfill_receipts(session, event, observed_at=now)

            applied = refused = noop = 0
            reasons: list[str] = []
            for receipt in receipts:
                if receipt.resolved_at is not None:
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

                await lock_consent_address(session, receipt.wa_address)
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

    @property
    def allowed_by_local_state(self) -> bool:
        """No PROVEN local refusal, and nothing unresolved.

        Named at length on purpose. It does NOT mean marketing consent was
        obtained; it means this database holds no proven refusal and nothing
        pending for this person. Consent itself is established elsewhere, and
        the absence of a ban is never evidence of it.
        """
        return not self.suppressed and self.pending_inbound == 0


def _fingerprint(pairs: Sequence[tuple[str, int]], *, suppressed: bool) -> str:
    """A handle over the exact consent rows this decision was taken from.

    One number cannot describe several matched addresses, and a later claim has
    to be able to notice a row that did not exist when the plan was made — so
    the guard hands out a digest over every (address, version) pair plus the
    verdict, and the claim recomputes it. A new row, a bumped version or a
    flipped verdict all change it.
    """
    material = "|".join(f"{address}:{version}" for address, version in sorted(pairs))
    return _digest("altegio_bot/whatsapp_consent/fingerprint/v1", f"{material}#{int(suppressed)}")


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
    the address lock itself.

    The four sources of §46.4.3, in one answer:

    1. the suppression ledger for this address and for every address an EasyWeek
       card of this UUID holds;
    2. a suppression bound to this UUID by an earlier refusal;
    3. ``Client.wa_opted_out`` on ANY matching card — another branch's or an
       Altegio one, which is the point: a refusal is not scoped to a provider;
    4. the historical ``CampaignRecipient.is_opted_out`` snapshot, unless a
       proven, verified opt-in landed after that snapshot was taken.
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
        # Nothing to match. Reported as a gap, never as permission: an address
        # we cannot normalise is an address we cannot clear.
        return ConsentDecision(
            address=None,
            suppressed=False,
            gaps=(GAP_ADDRESS_UNNORMALISABLE,),
            fingerprint=_fingerprint((), suppressed=False),
        )

    addresses = set(address_variants(address))
    addresses.update(await _uuid_linked_addresses(session, customer_uuid))
    ordered_addresses = sorted(addresses)

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
    gaps: list[str] = []
    suppressed = False
    newest_opt_in: datetime | None = None
    for row in rows:
        if row.state == CONSENT_STATE_OPTED_OUT:
            suppressed = True
            sources.append(
                SOURCE_SUPPRESSION_UUID
                if row.wa_address not in addresses and row.banned_easyweek_customer_uuid is not None
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

    pending = 0
    if include_pending:
        pending = int(
            await session.scalar(
                select(func.count())
                .select_from(WhatsAppInboundReceipt)
                .where(WhatsAppInboundReceipt.resolved_at.is_(None))
                .where(WhatsAppInboundReceipt.wa_address.in_(ordered_addresses))
            )
            or 0
        )
        if pending:
            gaps.append(GAP_PENDING_INBOUND)

    primary = next((int(row.consent_version) for row in rows if row.wa_address == address), None)
    return ConsentDecision(
        address=address,
        suppressed=suppressed,
        sources=tuple(dict.fromkeys(sources)),
        gaps=tuple(dict.fromkeys(gaps)),
        pending_inbound=pending,
        fingerprint=_fingerprint(
            [(str(row.wa_address), int(row.consent_version)) for row in rows],
            suppressed=suppressed,
        ),
        primary_version=primary,
        matched_addresses=tuple(ordered_addresses),
    )


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
    """Take the address lock, then answer. In the CALLER's transaction.

    The lock is what makes the ordering single and checkable (§46.4.6): a
    concurrent consent write for this address either commits before this guard
    and is seen by it, or waits until this transaction ends and is then caught by
    the claim-time re-check. There is no third interleaving, and no row has to
    exist for the lock to work — which is what closes the absent-row race for an
    address that has never been heard from.

    No HTTP happens under this lock, and none may: the caller is expected to
    commit its claim and only then make its external request.
    """
    address = normalize_wa_address(phone)
    if address is None:
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
    await lock_consent_address(session, address)
    decision = await consent_decision(
        session,
        phone=address,
        easyweek_customer_uuid=easyweek_customer_uuid,
        include_pending=True,
    )
    if decision.suppressed:
        return FinalSendGuard(False, GUARD_SUPPRESSED, decision)
    if decision.pending_inbound:
        # §46.4.6. A durable inbound message this process has not accounted for
        # may be carrying the refusal, so the send is HELD rather than allowed:
        # a queue that is behind must not become a permission.
        return FinalSendGuard(False, GUARD_PENDING_INBOUND, decision)
    return FinalSendGuard(True, GUARD_ALLOWED, decision)


async def consent_unchanged(
    session: AsyncSession,
    *,
    phone: object,
    expected_fingerprint: str,
    easyweek_customer_uuid: uuid_module.UUID | str | None = None,
) -> bool:
    """Is the consent state exactly the one a guard was issued against?

    Called by a sender inside the same transaction as its claim. A refusal that
    landed in between, a version that moved, or a suppression row that did not
    exist before all change the fingerprint and refuse the claim.
    """
    decision = await consent_decision(
        session,
        phone=phone,
        easyweek_customer_uuid=easyweek_customer_uuid,
        include_pending=True,
    )
    return bool(expected_fingerprint) and decision.fingerprint == expected_fingerprint


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
    """
    await lock_consent_address(session, refusal.wa_address)
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
    "GAP_PENDING_INBOUND",
    "GAP_UUID_BINDING_AMBIGUOUS",
    "GUARD_ADDRESS_UNPROVEN",
    "GUARD_ALLOWED",
    "GUARD_PENDING_INBOUND",
    "GUARD_SUPPRESSED",
    "NOOP_START_NO_ACTIVE_BAN",
    "NOOP_STOP_ALREADY_ACTIVE",
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
    "ProjectedRefusal",
    "address_variants",
    "apply_consent_decision",
    "apply_inbound_consent",
    "apply_projected_refusal",
    "consent_decision",
    "consent_unchanged",
    "extract_inbound_receipts",
    "final_send_guard",
    "is_suppressed",
    "iter_payload_consent_commands",
    "lock_consent_address",
    "normalize_wa_address",
    "receipt_key",
    "recipient_route_digest",
    "record_inbound_receipts",
    "sender_route_digest",
]
