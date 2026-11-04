"""Whether one original recipient may appear in a follow-up audience (§46.1–46.5).

The rule the whole module follows
---------------------------------
Inclusion needs PROOF of every condition. Exclusion needs proof of one. And
everything in between — a read that failed, a page that did not come back, a
number that changed hands, an identity that does not line up — is ``unknown``,
which is a third answer and never a quiet "yes". That asymmetry is the whole
design: a wrong exclusion costs one person one message, a wrong inclusion writes
to somebody who said stop, or who has already booked, or who was never the
addressee in the first place.

The order of the checks is deliberate
-------------------------------------
Cheap and local first, external last, and the external part holds no lock:

1. identity — is this row still about the person the original message went to?
2. the group — what does PR-22.1 actually prove about the original attempt?
3. the interval — has the operator's minimum time passed since that attempt?
4. the entitlement — is the ORIGINAL voucher still a thing we may refer to?
5. consent — the §46.4 aggregate, including pending inbound;
6. replies — did this person already answer the first message?
7. bookings — a live, read-only, fully paginated check, and the only network
   step in the whole preview;
8. the failure policy, for the third group only.

Steps 1–6 cost one local query set each and can refuse most rows before any
request is made. Step 7 is therefore spent only on rows that would otherwise be
included, which is both cheaper and the reason the live read can be strict.

Nothing here sends, claims or reserves
--------------------------------------
No POST, no job, no counter, no voucher mutation, no attempt. This module reads
and judges. Even the booking step is ``get_customer`` plus ``list_customer_
bookings``, both read-only, and no database row lock or advisory lock may be
held across it.
"""

from __future__ import annotations

import hashlib
import hmac
import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select

from altegio_bot.campaigns.easyweek_customer_history import (
    CustomerBookingSet,
    LiveProofDisposition,
)
from altegio_bot.campaigns.easyweek_voucher_followup.booking_evidence import SlotCreations
from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import FailurePolicy
from altegio_bot.campaigns.easyweek_voucher_followup.groups import GroupVerdict, SourceEvidence, resolve_group
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    BOOKING_CAPTURE_UNPROVEN,
    BOOKING_CREATION_UNPROVEN,
    CONSENT_PENDING_INBOUND,
    CONSENT_REFUSED,
    CONSENT_UNKNOWN,
    ENTITLEMENT_INVALID,
    ENTITLEMENT_UNPROVEN,
    FOLLOWUP_GROUP_FAILED,
    FOLLOWUP_ROW_EXCLUDED,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
    FUTURE_BOOKING_ACTIVE,
    HISTORY_UNPROVEN,
    IDENTITY_CONFLICT,
    NEW_BOOKING_AFTER_ATTEMPT,
)
from altegio_bot.models.models import (
    INBOUND_AUTHENTICITY_SIGNED,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
    VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUNDED,
    CampaignRecipient,
    Client,
    WhatsAppInboundReceipt,
)
from altegio_bot.whatsapp_consent import (
    address_variants,
    consent_decision,
    normalize_wa_address,
    recipient_route_digest,
)

__all__ = [
    "ENTITLEMENT_INVALID_STATUSES",
    "SUPPORTED_ITEM_BASES",
    "BookingVerdict",
    "ConsentSnapshot",
    "EntitlementVerdict",
    "RecipientIdentity",
    "RowFacts",
    "booking_verdict",
    "consent_snapshot",
    "entitlement_verdict",
    "has_replied_since",
    "interval_elapsed",
    "resolve_identity",
    "source_entitlement_digest",
]


# A voucher that was refunded, cleaned up by hand, or whose state nobody could
# settle is not something a follow-up may point at. ``refund_claimed`` and
# ``refund_unknown`` are in the list for the same reason as the settled ones:
# while a refund is in flight the entitlement's fate is undecided, and a message
# promising a voucher that is being taken back is worse than no message.
ENTITLEMENT_INVALID_STATUSES = frozenset(
    {
        VOUCHER_PRODUCTION_ITEM_REFUNDED,
        VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
        VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
        VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
        VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    }
)


# Which per-slot bases this phase supports. §46.2 keeps the first version to
# the fixed €10 production batches with proven own bindings; those batches can
# be composed from earned visits, from an operator's manual selection, or from
# both, and nothing in §46 narrows the audience to one of them. A canary, a
# snapshot ledger, an owner test and an Altegio campaign stay out — they are
# other kinds of mailing, refused at the batch gate by name.
SUPPORTED_ITEM_BASES = frozenset({RECIPIENT_BASIS_EARNED, RECIPIENT_BASIS_MANUAL})


def _as_uuid(value: object) -> uuid_module.UUID | None:
    """One canonical uuid, or ``None``. Never a guess from a partial value."""
    if isinstance(value, uuid_module.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid_module.UUID(value)
        except ValueError:
            return None
    return None


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def source_entitlement_digest(
    *,
    batch_id: int,
    slot: int,
    voucher_code_hmac: str | None,
    hmac_key_id: str | None,
    target_order_uuid: object,
) -> str:
    """A stable handle for "the one voucher this slot issued".

    Why a digest rather than the stored MAC: the MAC is the ledger's own binding
    material, and copying it into a second table would spread a security
    artifact across the schema for no gain. This is derived from it instead, so
    the preview (and PR-22.3 after it) can recognise the same entitlement twice
    without carrying the MAC — and two slots that genuinely issued the same
    voucher collide here, which is exactly what the dedupe index needs.

    Derived from the batch and the slot as well, because that pair is inside the
    MAC's own bound material: a handle that ignored them would let one batch's
    entitlement look like another's.
    """
    material = "\x1f".join(
        (
            "altegio_bot/easyweek_voucher_followup/entitlement/v1",
            str(int(batch_id)),
            str(int(slot)),
            (voucher_code_hmac or "").strip(),
            (hmac_key_id or "").strip(),
            str(target_order_uuid or "").strip(),
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def recipient_digest_for(phone: object) -> str | None:
    """The same one-way recipient digest the original attempt froze."""
    return recipient_route_digest(phone)


# ---------------------------------------------------------------------------
# 1. identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecipientIdentity:
    """Is this still the person the original message was addressed to?"""

    proven: bool
    reason: str | None = None
    address: str | None = None
    customer_uuid: uuid_module.UUID | None = None


async def resolve_identity(
    session: Any,
    *,
    campaign_recipient_id: int,
    company_id: int,
    campaign_run_id: int,
    recipient_basis: str,
    expected_customer_uuid: uuid_module.UUID,
    expected_source_booking_uuid: object = None,
    attempt_recipient_digest: str | None,
) -> RecipientIdentity:
    """Re-prove the addressee from the ledger's own links, UUID first.

    The UUID is the identity and the phone number is an attribute of it — that
    asymmetry is the whole check, and it holds for both bases a production batch
    can carry.

    What differs between them is only which link the campaign row offers, and
    the shared schema decides that rather than this module: an EARNED row carries
    the booking its voucher was earned on and no customer uuid of its own, while
    a MANUAL row carries the uuid an operator proved and no booking. So each
    basis is checked against the link it actually has, and a row whose basis this
    phase does not support is refused rather than guessed at.

    Six ways this says no, and each one is a case §46.1 and §46.5 name:

    * the campaign row behind the slot is gone, or belongs to another workspace,
      company or run. Nothing here reaches across workspaces;
    * its basis is not the one the slot was composed under, or is not a basis
      this phase supports;
    * an EARNED row no longer names the booking the slot was composed from, or a
      MANUAL row no longer names the proven customer. The proof the slot rests on
      has moved, so this row is about something else now;
    * the recorded uuid resolves to no EasyWeek card, or to more than one. An
      ambiguous identity is unknown, never "probably the first one";
    * that card's number is not the number on the campaign row. Somebody's phone
      has changed hands or been reassigned, and §46.1 is explicit that a
      follow-up may not be handed to whoever owns a matching number now;
    * the number no longer digests to the route the original attempt froze. The
      same substitution seen from the other side, and the reason the digest is
      compared rather than the number: the attempt deliberately stored no phone,
      so this re-proves the route without either side holding one.
    """
    recipient = await session.get(CampaignRecipient, int(campaign_recipient_id))
    if recipient is None:
        return RecipientIdentity(False, IDENTITY_CONFLICT)
    if (
        recipient.provider != PROVIDER_EASYWEEK
        or int(recipient.company_id or 0) != int(company_id)
        or int(recipient.campaign_run_id or 0) != int(campaign_run_id)
    ):
        return RecipientIdentity(False, IDENTITY_CONFLICT)
    if recipient_basis not in SUPPORTED_ITEM_BASES or str(recipient.recipient_basis or "") != recipient_basis:
        return RecipientIdentity(False, IDENTITY_CONFLICT)

    if recipient_basis == RECIPIENT_BASIS_EARNED:
        expected_booking = str(expected_source_booking_uuid) if expected_source_booking_uuid else None
        observed_booking = str(recipient.source_booking_uuid) if recipient.source_booking_uuid else None
        if expected_booking is None or observed_booking != expected_booking:
            return RecipientIdentity(False, IDENTITY_CONFLICT)
    else:
        # The manual basis. Its campaign row names the customer uuid an
        # operator proved, and the slot must still agree with it; a phone that
        # merely matches proves nothing positive here (§46.4.2).
        if _as_uuid(recipient.easyweek_customer_uuid) != expected_customer_uuid:
            return RecipientIdentity(False, IDENTITY_CONFLICT)

    address = normalize_wa_address(recipient.phone_e164)
    if address is None:
        return RecipientIdentity(False, IDENTITY_CONFLICT)

    cards = list(
        (
            await session.execute(
                select(Client)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.company_id == int(company_id))
                .where(Client.easyweek_customer_uuid == expected_customer_uuid)
                .order_by(Client.id.asc())
                .limit(2)
            )
        ).scalars()
    )
    if len(cards) != 1:
        return RecipientIdentity(False, IDENTITY_CONFLICT)
    card_address = normalize_wa_address(cards[0].phone_e164)
    if card_address is None or card_address != address:
        return RecipientIdentity(False, IDENTITY_CONFLICT)

    observed_digest = recipient_digest_for(address)
    if (
        attempt_recipient_digest is None
        or observed_digest is None
        or not hmac.compare_digest(observed_digest, attempt_recipient_digest)
    ):
        return RecipientIdentity(False, IDENTITY_CONFLICT)

    return RecipientIdentity(True, None, address=address, customer_uuid=expected_customer_uuid)


# ---------------------------------------------------------------------------
# 3. the interval
# ---------------------------------------------------------------------------


def interval_elapsed(
    *,
    attempted_at: datetime | None,
    min_interval_seconds: int,
    now: datetime,
) -> bool:
    """Has the operator's minimum time passed since the PROVEN attempt?

    The anchor is ``send_attempted_at`` and nothing else. §46.3 rules out every
    substitute that looks convenient: a callback's arrival (which says when a
    network delivered a status, not when we wrote to somebody), the row's
    ``updated_at`` (which moves whenever anything touches it) and the moment the
    page was opened (which is not a fact about the recipient at all).

    For the failure group the same anchor is used, and it is deliberately NOT
    evidence that anything was received: it is the moment this system last tried
    this person's number, which is the thing a minimum interval is about.

    The boundary is inclusive — exactly the interval is enough — because the
    operator chose the number and a strict comparison would make the choice mean
    one second more than it says.
    """
    anchor = _aware(attempted_at)
    if anchor is None or min_interval_seconds <= 0:
        return False
    return _aware(now) - anchor >= timedelta(seconds=int(min_interval_seconds))


# ---------------------------------------------------------------------------
# 4. the original entitlement
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntitlementVerdict:
    proven: bool
    reason: str | None = None
    digest: str = ""


def entitlement_verdict(
    *,
    batch_id: int,
    slot: int,
    status: str,
    voucher_code_hmac: str | None,
    hmac_key_id: str | None,
    target_order_uuid: object,
    configured_key_id: str | None,
) -> EntitlementVerdict:
    """Is the ORIGINAL voucher still something a repeat message may refer to?

    What this checks, and in which direction each answer falls:

    * an explicitly refunded, cleaned or unsettled slot is EXCLUDED. The
      provider or an operator has written the voucher off, and §46.4 forbids
      pointing at it;
    * a slot with no stored MAC, no key id or no order uuid is UNPROVEN. There
      is no artifact to speak about, so neither inclusion nor a claim that it is
      invalid would be honest;
    * a MAC written under a key this deployment no longer holds is UNPROVEN
      too. Nothing is wrong with the voucher; what is gone is our ability to
      re-prove the binding, and §46.4 asks for proof rather than for optimism.

    What it deliberately does NOT do is ask the provider for a term. §45.4
    settled that: EasyWeek owns validity, the approved product states it, and an
    artifact that simply carries no activation or expiry date is the ORDINARY
    shape of a correct voucher under this contract. Turning that silence into a
    new prohibition here would contradict the section outright. An explicit
    provider refusal remains a refusal, and the place that reads it is
    :mod:`...easyweek_voucher_production.validity`, against a live order — which
    the preview does not fetch, and must not pretend it did.
    """
    digest = source_entitlement_digest(
        batch_id=batch_id,
        slot=slot,
        voucher_code_hmac=voucher_code_hmac,
        hmac_key_id=hmac_key_id,
        target_order_uuid=target_order_uuid,
    )
    if status in ENTITLEMENT_INVALID_STATUSES:
        return EntitlementVerdict(False, ENTITLEMENT_INVALID, digest)
    if not (voucher_code_hmac or "").strip() or not (hmac_key_id or "").strip():
        return EntitlementVerdict(False, ENTITLEMENT_UNPROVEN, digest)
    if not str(target_order_uuid or "").strip():
        return EntitlementVerdict(False, ENTITLEMENT_UNPROVEN, digest)
    expected = (configured_key_id or "").strip()
    if not expected or not hmac.compare_digest((hmac_key_id or "").strip(), expected):
        return EntitlementVerdict(False, ENTITLEMENT_UNPROVEN, digest)
    return EntitlementVerdict(True, None, digest)


# ---------------------------------------------------------------------------
# 5. consent
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsentSnapshot:
    """The §46.4 aggregate's answer, kept with the fingerprint it was taken at."""

    allowed: bool
    reason: str | None
    fingerprint: str = ""
    sources: tuple[str, ...] = ()
    gaps: tuple[str, ...] = ()
    pending_inbound: int = 0


async def consent_snapshot(
    session: Any,
    *,
    phone: object,
    easyweek_customer_uuid: uuid_module.UUID | None,
) -> ConsentSnapshot:
    """Read the whole consent aggregate for one person. No locks, no writes.

    Everything §46.4.3 put in it applies unchanged, and this adds nothing of its
    own: the suppression ledger for this address and for every address a card of
    this uuid holds, a refusal bound to the uuid itself, ``wa_opted_out`` on ANY
    matching card — another branch's or an Altegio one, because a refusal is not
    scoped to a provider — and the historical recipient snapshot unless a proven
    opt-in landed after it.

    Three different answers, never collapsed into two:

    * a proven refusal excludes. A new booking, an ordinary reply or a delivery
      status does not undo it, and the proven-START rules are untouched here;
    * accepted but unprocessed inbound HOLDS. A STOP sitting in the pending set
      is a refusal this deployment has been told about and has not finished
      writing down, and a busy worker queue is not a reason to write to somebody
      who may have just said stop;
    * a gap — an address that cannot be normalised, an ambiguous uuid binding —
      is unknown. Unknown is not permission.

    Lock-free on purpose, and this is the one place the preview differs from a
    send: :func:`final_send_guard` takes the consent address locks because a
    claim must be serialised against concurrent writers, while a preview must
    not delay an arriving STOP by so much as one transaction. The fingerprint is
    carried into the snapshot instead, so PR-22.3's locked guard can tell that
    the state moved since the preview was taken.
    """
    decision = await consent_decision(
        session,
        phone=phone,
        easyweek_customer_uuid=easyweek_customer_uuid,
        include_pending=True,
    )
    if decision.suppressed:
        return ConsentSnapshot(
            False,
            CONSENT_REFUSED,
            decision.fingerprint,
            tuple(decision.sources),
            tuple(decision.gaps),
            int(decision.pending_inbound or 0),
        )
    if int(decision.pending_inbound or 0) > 0:
        return ConsentSnapshot(
            False,
            CONSENT_PENDING_INBOUND,
            decision.fingerprint,
            tuple(decision.sources),
            tuple(decision.gaps),
            int(decision.pending_inbound or 0),
        )
    if decision.gaps:
        return ConsentSnapshot(
            False,
            CONSENT_UNKNOWN,
            decision.fingerprint,
            tuple(decision.sources),
            tuple(decision.gaps),
            0,
        )
    return ConsentSnapshot(True, None, decision.fingerprint, (), (), 0)


# ---------------------------------------------------------------------------
# 6. replies
# ---------------------------------------------------------------------------


async def has_replied_since(
    session: Any,
    *,
    address: str,
    easyweek_customer_uuid: uuid_module.UUID | None,
    since: datetime,
) -> bool:
    """Did the ADDRESSEE write to us after the original attempt?

    §46.5 asks for a proven reply and names what does not count. So:

    * the receipt's signature must have been verified by the ingress. An
      unsigned delivery is an unattributed claim, and acting on it would let
      anybody who can reach the webhook suppress a follow-up — or, worse, be
      trusted in the other direction one day;
    * the sender route must not be one this deployment refused. A message that
      arrived on a foreign line is not this conversation;
    * a Chatwoot mirror or an operator relay is not an independent client reply.
      Those never produce receipts — only the Meta ingress writes them — which
      is why this can be a property of the data rather than a filter;
    * the reply need NOT mention the first message. §46.5 is explicit: a person
      who answers at all has answered, and requiring a quoted message id would
      silently narrow the rule to the few clients whose app sends context.

    Matched on the person, not the number alone: the addressee's own addresses
    and any receipt the consent layer has bound to their uuid, which is how a
    reply from a number a card has since dropped still counts.
    """
    boundary = _aware(since)
    if boundary is None:
        return False
    conditions = [WhatsAppInboundReceipt.wa_address.in_(address_variants(address))]
    if easyweek_customer_uuid is not None:
        conditions.append(WhatsAppInboundReceipt.linked_easyweek_customer_uuid == easyweek_customer_uuid)
    found = (
        await session.execute(
            select(WhatsAppInboundReceipt.id)
            .where(or_(*conditions))
            .where(WhatsAppInboundReceipt.authenticity == INBOUND_AUTHENTICITY_SIGNED)
            .where(WhatsAppInboundReceipt.route_allowed.is_not(False))
            .where(
                or_(
                    WhatsAppInboundReceipt.observed_at > boundary,
                    WhatsAppInboundReceipt.event_at > boundary,
                )
            )
            .limit(1)
        )
    ).first()
    return found is not None


# ---------------------------------------------------------------------------
# 7. bookings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BookingVerdict:
    """What the two booking reads together say about this person."""

    proven: bool
    reason: str | None = None
    unknown: bool = False
    # The underlying proof's own reason, when there is one worth keeping. Never
    # the row's reason code: that stays in this phase's safe vocabulary.
    detail: str | None = None
    booking_count: int = 0
    pages_read: int = 0


def booking_verdict(
    bookings: CustomerBookingSet,
    *,
    creations: SlotCreations,
    attempted_at: datetime,
    now: datetime,
) -> BookingVerdict:
    """Apply §46.5's two booking rules to a complete booking set.

    Separated from both reads on purpose — the live read is shared with PR-15
    and the creation capture with the §PR-17 report — so every case below is
    testable without a transport and without a webhook.

    The rules, and which DATE each one rests on:

    * an incomplete or untrusted LIVE set is UNKNOWN. A timeout, a page that did
      not come back, pagination that does not add up, a card whose uuid resolves
      to somebody else: none of them is "this person has no bookings". Turning
      an API failure into an empty list is the most dangerous mistake available
      here, and it is why ``complete`` exists;
    * an active booking whose APPOINTMENT is still ahead excludes, whenever it
      was made. They already have a slot booked;
    * a booking this person CREATED after the original attempt excludes,
      cancelled ones included and whatever the appointment time is. A booking
      made the day after the mailing to record a visit that already happened is
      exactly the engagement the rule is about;
    * a booking whose appointment falls after the attempt but whose CREATION
      cannot be proven is UNKNOWN. Whether it was made before or after the
      attempt decides the answer, so neither verdict is available;
    * everything else is proven clear. A visit that happened BEFORE the attempt
      excludes nothing by itself — that is the visit which earned the voucher,
      and §46.5 is explicit that it must not read as a reason to stay silent.

    Branch scope comes from the two reads: the live one is keyed on the customer
    uuid and is therefore workspace-wide, and the capture covers every
    configured branch of the same workspace.
    """
    if not bookings.complete:
        # This phase's OWN code, with the reader's reason kept beside it. A
        # stored snapshot and an operator's screen speak the follow-up's safe
        # vocabulary; the live proof's internal codes belong to another phase
        # and would arrive on the page with nothing to say them in words.
        return BookingVerdict(
            False,
            HISTORY_UNPROVEN,
            unknown=True,
            detail=bookings.reason,
            pages_read=bookings.pages_read,
        )
    anchor = _aware(attempted_at)
    moment = _aware(now)
    assert anchor is not None and moment is not None

    # The appointment-time rule first: it needs no creation evidence at all, so
    # a person who plainly has a slot booked ahead is excluded for that reason
    # rather than reported as unknown because a webhook was never captured.
    for row in bookings.bookings:
        start = _aware(row.start_time)
        if start is None:  # pragma: no cover - the reader proves tz-aware starts
            return BookingVerdict(False, HISTORY_UNPROVEN, unknown=True, pages_read=bookings.pages_read)
        if not row.is_canceled and start > moment:
            return BookingVerdict(False, FUTURE_BOOKING_ACTIVE, pages_read=bookings.pages_read)

    # Then the creation rule, which does.
    if not creations.proven:
        return BookingVerdict(
            False,
            creations.reason or BOOKING_CAPTURE_UNPROVEN,
            unknown=True,
            pages_read=bookings.pages_read,
        )
    if creations.created_after(anchor):
        return BookingVerdict(False, NEW_BOOKING_AFTER_ATTEMPT, pages_read=bookings.pages_read)

    # A booking the live read can see, appointed after the attempt, whose own
    # creation nothing proves. It may be the booking that earned nothing and was
    # made yesterday, or one made months ago; the rule cannot be applied to it.
    for row in bookings.bookings:
        start = _aware(row.start_time)
        if start is None or start <= anchor:
            continue
        if creations.creation_of(row.booking_uuid) is None:
            return BookingVerdict(
                False,
                BOOKING_CREATION_UNPROVEN,
                unknown=True,
                pages_read=bookings.pages_read,
            )

    return BookingVerdict(
        True,
        None,
        booking_count=len(bookings.bookings),
        pages_read=bookings.pages_read,
    )


def booking_disposition_is_retryable(bookings: CustomerBookingSet) -> bool:
    """Was the booking read a transport problem rather than a verdict?

    Reported to the operator as such: "try again" and "this person is excluded"
    are different sentences, and §46.6 asks the page to say which one it means.
    """
    return bookings.disposition is LiveProofDisposition.RETRYABLE_UNCERTAINTY


# ---------------------------------------------------------------------------
# The per-row assembly
# ---------------------------------------------------------------------------


@dataclass
class RowFacts:
    """One candidate row, with everything proven about it so far.

    Mutable and deliberately plain: the orchestrator fills it in stages, and
    the stage that writes the snapshot reads it. Nothing in it is a phone
    number, a name, a voucher code or a rendered body.
    """

    item_id: int
    slot: int
    campaign_recipient_id: int
    easyweek_customer_uuid: uuid_module.UUID
    source_attempted_at: datetime | None
    evidence: SourceEvidence
    group: GroupVerdict
    entitlement_digest: str = ""
    recipient_digest: str | None = None
    # The local half of the booking question, resolved before the external read
    # and carried across it: which bookings this person created, and when.
    creations: SlotCreations = field(default_factory=lambda: SlotCreations(False, BOOKING_CAPTURE_UNPROVEN))
    decision: str = FOLLOWUP_ROW_UNKNOWN
    reason: str | None = None
    notes: dict[str, Any] = field(default_factory=dict)

    def exclude(self, reason: str, **notes: Any) -> None:
        self.decision = FOLLOWUP_ROW_EXCLUDED
        self.reason = reason
        self.notes.update(notes)

    def unknown(self, reason: str, **notes: Any) -> None:
        self.decision = FOLLOWUP_ROW_UNKNOWN
        self.reason = reason
        self.notes.update(notes)

    def include(self, **notes: Any) -> None:
        self.decision = FOLLOWUP_ROW_INCLUDED
        self.reason = None
        self.notes.update(notes)

    @property
    def included(self) -> bool:
        return self.decision == FOLLOWUP_ROW_INCLUDED


def failure_policy_reason(facts: RowFacts, *, policy: FailurePolicy) -> str | None:
    """The versioned failure policy's verdict, for the third group only.

    Fail-closed in two ways. A row in the failure group with no confirmed fact
    at all is refused, and a row with several is refused unless EVERY one of
    them is admissible: one inadmissible reason among several is a veto, not an
    average. A permanent block hiding behind a transient retry must not become
    eligibility because the transient one was listed first.
    """
    if facts.group.group != FOLLOWUP_GROUP_FAILED:
        return None
    confirmed = facts.evidence.confirmed_failures
    if not confirmed:
        return policy.refusal_for(None)
    for fact in confirmed:
        refusal = policy.refusal_for(fact)
        if refusal is not None:
            return refusal
    return None


def resolve_source_group(evidence: SourceEvidence) -> GroupVerdict:
    """Re-exported so the orchestrator has one import for the row rules."""
    return resolve_group(evidence)
