"""A safe, local reading of what this database already knows (§46.4.4, §46.3).

What it is for
--------------
Three kinds of refusal and one kind of delivery fact exist in this database
today, recorded by code that had nowhere durable to put them:

* ``Client.wa_opted_out`` — a live card flag, on any branch and any provider;
* ``CampaignRecipient.is_opted_out`` — a snapshot taken at preview time;
* stored ``whatsapp_events`` payloads carrying a STOP that the old keyword set
  could not see, the Cyrillic «стоп» among them;
* production slots whose ONE attempt ended in a refusal Meta itself made.

This module reads all four, says what it would write, and — only when asked
explicitly — writes it. Nothing else.

What it must never do, and does not
-----------------------------------
No acknowledgement, no Chatwoot note, no Meta or EasyWeek request, no job, no
message, and no call into the inbox worker's event handler. It does not touch
``WhatsAppEvent.status`` and therefore cannot "replay" anything: an event it
reads stays exactly as processed or unprocessed as it was.

Dry run is the default. ``apply=True`` is a separate, explicit decision, and
even then every write goes through the ordinary ledger functions — the same
ones the live worker uses, with the same address lock and the same monotonicity
rules. That is what makes it safe to run beside live ingress: a projection built
from a stale snapshot converges on the newer state instead of overwriting it,
and running it twice changes nothing the first run did not.

Honesty about completeness
--------------------------
The report counts what it could not establish as loudly as what it could.
A stored payload's signature cannot be recovered — the ingress strips it by
design — so every historical refusal is recorded as ``unverified`` and every
historical START is skipped rather than honoured: it could not lift anything
even if it were applied, and writing it down would create an opt-in record out
of a payload nobody can verify. An unprovable delivery event stays unknown and
never becomes a confirmed failure.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence
from altegio_bot.models.models import (
    CONSENT_COMMAND_STOP,
    CONSENT_STATE_OPTED_OUT,
    DELIVERY_FAILURE_REASON_API_REJECTED,
    DELIVERY_FAILURE_SOURCE_META_API,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    CampaignRecipient,
    Client,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
)
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import (
    NOOP_PROJECTION_SUPERSEDED,
    SOURCE_SUPPRESSION,
    SOURCE_SUPPRESSION_UUID,
    DecisionOutcome,
    ProjectedRefusal,
    address_variants,
    apply_projected_refusal,
    consent_decision,
    iter_payload_consent_commands,
    ledger_state,
    normalize_wa_address,
)

logger = logging.getLogger("whatsapp_consent_projection")

ORIGIN_CLIENT_FLAG = "client_wa_opted_out"
ORIGIN_RECIPIENT_FLAG = "campaign_recipient_is_opted_out"
ORIGIN_STORED_EVENT = "stored_whatsapp_event"

# Why a candidate was listed but not projected.
SKIP_ALREADY_SUPPRESSED = "already_suppressed"
SKIP_HISTORICAL_START = "historical_start_not_provable"
SKIP_ADDRESS_UNNORMALISABLE = "address_not_normalisable"
# The ingress refused this sender route when the message arrived. The command
# was never executed then and must not be executed now: re-deciding an old
# message against today's allow list would make the reconciliation a way to
# apply exactly the commands the deployment was configured to refuse.
SKIP_ROUTE_NOT_ALLOWED = "sender_route_not_allowed"
# The source has no provable event time and its own observed moment predates the
# moment a proven opt-in for that address was observed. A stale snapshot of a
# state the ledger has already moved past: projecting it would undo a
# resubscribe nobody disputes AND make the resulting refusal unorderable, which
# by §46.4.4 means permanent.
SKIP_SUPERSEDED_BY_OPT_IN = "superseded_by_proven_opt_in"

# Why a historical delivery outcome stays unknown.
UNKNOWN_NOT_PROVEN_EXTERNAL = "rejection_not_proven_external"
UNKNOWN_NO_ATTEMPT = "no_proven_attempt"
UNKNOWN_ALREADY_SUCCEEDED = "delivered_or_read_present"

# A reason code the delivery client only ever produces for Meta's own refusal of
# the request, on a status Meta decides before acting on the message. Every
# other `send_rejected` reason is either a local refusal or an unknown, and
# §46.3 says in as many words that a general `meta_rejected` is not enough.
_PROVEN_API_REJECTION_REASON = "meta_rejected_request"


@dataclass
class RefusalCandidate:
    """One refusal a historical source says exists."""

    wa_address: str
    origin: str
    observed_at: datetime
    event_at: datetime | None = None
    whatsapp_event_id: int | None = None
    notes: dict[str, Any] = field(default_factory=dict)
    already_suppressed: bool = False
    newly_suppressed: bool = False
    projectable: bool = False

    @property
    def carries_event_evidence(self) -> bool:
        """Is this a stored MESSAGE rather than a flag or a snapshot?

        A stored event is a refusal somebody actually sent, with its own event
        time or a provable absence of one, so it has something to say about WHEN
        the refusal happened and must still be applied when a refusal is already
        active — that is how the boundary a future START has to beat moves to
        the newest known STOP. A card flag or a preview snapshot carries no time
        of its own, so once the ledger already refuses there is nothing left for
        it to add.
        """
        return self.origin == ORIGIN_STORED_EVENT


@dataclass
class FailureCandidate:
    """One historical delivery outcome, and whether it can be proven."""

    batch_id: int
    slot: int
    item_id: int
    provable: bool
    reason: str
    route_proven: bool = False
    already_recorded: bool = False


@dataclass
class ProjectionReport:
    """Everything the projection saw, would do, or did. Safe to print.

    Carries no phone number, no name, no voucher code and no message body:
    addresses are counted, never listed, and the per-candidate detail an
    operator needs is the ORIGIN and the reason.
    """

    applied: bool = False
    started_at: datetime | None = None
    refusals_seen: int = 0
    refusals_already_suppressed: int = 0
    refusals_to_write: int = 0
    refusals_written: int = 0
    refusals_noop: int = 0
    refusals_superseded: int = 0
    refusals_unsettled: int = 0
    cards_converged: int = 0
    newly_suppressed_addresses: int = 0
    skipped: dict[str, int] = field(default_factory=dict)
    by_origin: dict[str, int] = field(default_factory=dict)
    cyrillic_stop_events: int = 0
    historical_starts_seen: int = 0
    events_route_not_allowed: int = 0
    messages_route_not_allowed: int = 0
    failures_seen: int = 0
    failures_provable: int = 0
    failures_written: int = 0
    failures_unknown: dict[str, int] = field(default_factory=dict)
    failures_route_unknown: int = 0
    source_gaps: list[str] = field(default_factory=list)
    truncated: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "refusals": {
                "seen": self.refusals_seen,
                "already_suppressed": self.refusals_already_suppressed,
                "to_write": self.refusals_to_write,
                "written": self.refusals_written,
                "noop": self.refusals_noop,
                "superseded": self.refusals_superseded,
                "unsettled": self.refusals_unsettled,
                "cards_converged": self.cards_converged,
                "newly_suppressed_addresses": self.newly_suppressed_addresses,
                "by_origin": dict(sorted(self.by_origin.items())),
                "skipped": dict(sorted(self.skipped.items())),
                "cyrillic_stop_events": self.cyrillic_stop_events,
                "historical_starts_seen": self.historical_starts_seen,
                "events_route_not_allowed": self.events_route_not_allowed,
                "messages_route_not_allowed": self.messages_route_not_allowed,
            },
            "delivery_failures": {
                "seen": self.failures_seen,
                "provable": self.failures_provable,
                "written": self.failures_written,
                "unknown": dict(sorted(self.failures_unknown.items())),
                "route_unknown": self.failures_route_unknown,
            },
            "source_gaps": sorted(set(self.source_gaps)),
            "truncated": self.truncated,
        }


def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


class CardSetMoved(Exception):
    """A card started matching this address after its rows were locked.

    Raised so the candidate's transaction rolls back — releasing every lock it
    holds — and is attempted again from the top. The new row cannot be locked
    where it was noticed, because that lock would come AFTER the consent lock
    and invert the one order this project keeps; and it cannot be left alone
    either, because a card the ledger refuses must not read as sendable.
    """


async def _card_ids_matching(session: AsyncSession, address: str, *, lock: bool) -> set[int]:
    """The card rows this address matches right now, optionally locking them.

    ``lock=True`` is the first step of the order this project keeps: card rows
    by ascending id, then the consent address. ``resolve_webhook_identity`` and
    the inbox worker's projection take the same two in the same order, so a run
    of this reconciliation queues behind them instead of crossing them.

    ``lock=False`` is the re-check under the consent lock. ``FOR UPDATE`` only
    protects the rows that matched when it ran, and a concurrent contact proof
    can move a card onto this address afterwards; the re-check is what turns the
    locked set from an assumption into a checked fact.
    """
    variants = address_variants(address)
    if not variants:
        return set()
    statement = select(Client.id).where(Client.phone_e164.in_(variants)).order_by(Client.id.asc())
    if lock:
        statement = statement.with_for_update()
    return {int(value) for value in (await session.scalars(statement)).all()}


async def _converge_cards(session: AsyncSession, address: str, *, card_ids: set[int], now: datetime) -> int:
    """Bring the legacy card flag into line with a refusal that is now active.

    The refusing direction only, and the reason it has to happen HERE is that
    this module writes the ledger. Without it a run left the ledger refusing and
    ``Client.wa_opted_out`` saying sendable — and that flag is what
    ``message_planner``, the outbox worker and every campaign path read, so the
    disagreement was the refusal being ignored by everything except the new
    consent guard. It is the same projection the inbox worker performs, taken
    under the same two locks, so the two cannot end up disagreeing.

    A card that already refuses is left untouched: ``wa_opted_out_at`` and
    ``wa_opt_out_reason`` say when and why somebody refused, and restamping them
    from a reconciliation would overwrite that history with the time of the run.

    Queued marketing jobs are deliberately NOT cancelled. That is one of the
    inbox worker's business actions and §46.4.4 keeps this module out of them;
    the outbox re-reads the flag before it sends, so the refusal still vetoes.

    Restricted to ``card_ids`` — the rows this transaction locked before it took
    the consent lock. Matching by phone alone could reach a card that started
    matching afterwards, and taking that row's lock here would invert the order
    and deadlock against the very contact proof that moved it.
    """
    variants = address_variants(address)
    if not variants or not card_ids:
        return 0
    result = await session.execute(
        update(Client)
        .where(Client.phone_e164.in_(variants))
        .where(Client.id.in_(sorted(card_ids)))
        .where(Client.wa_opted_out.is_(False))
        .values(
            wa_opted_out=True,
            wa_opted_out_at=now,
            wa_opt_out_reason="wa:consent_projection",
        )
    )
    return int(getattr(result, "rowcount", 0) or 0)


async def _collect_client_flags(session: AsyncSession, report: ProjectionReport) -> list[RefusalCandidate]:
    """Live card flags, from any branch and any provider.

    Altegio rows included on purpose: §46.4.3 says a refusal recorded on an
    Altegio card must keep vetoing EasyWeek work, and the ledger is keyed by the
    address precisely so provider has nothing to do with it.
    """
    rows = list(
        (
            await session.execute(
                select(Client.id, Client.phone_e164, Client.wa_opted_out_at, Client.wa_opt_out_reason)
                .where(Client.wa_opted_out.is_(True))
                .order_by(Client.id.asc())
            )
        ).all()
    )
    candidates: list[RefusalCandidate] = []
    for _client_id, phone, opted_out_at, reason in rows:
        address = normalize_wa_address(phone)
        if address is None:
            # A flagged card with no usable number cannot be projected onto an
            # address. Counted, never guessed at, and the card flag itself keeps
            # vetoing wherever it is already read.
            _bump(report.skipped, SKIP_ADDRESS_UNNORMALISABLE)
            continue
        candidates.append(
            RefusalCandidate(
                wa_address=address,
                origin=ORIGIN_CLIENT_FLAG,
                # The flag's own moment when it has one. It is an OBSERVED time
                # — this process set it — and it is passed as such; the proven
                # external event time stays unknown, which is why a later START
                # cannot beat it.
                observed_at=opted_out_at or utcnow(),
                notes={"legacy_reason": str(reason)[:64] if reason else None},
            )
        )
    return candidates


async def _collect_recipient_flags(session: AsyncSession, report: ProjectionReport) -> list[RefusalCandidate]:
    """Preview-time snapshots of a refusal.

    A snapshot, not a live state: it says this person was opted out when that
    preview was taken. Projected anyway, because §46.4.3 requires it to keep
    vetoing, and the aggregate read already answers the one case where it should
    not — a snapshot older than a proven, verified opt-in.
    """
    rows = list(
        (
            await session.execute(
                select(CampaignRecipient.id, CampaignRecipient.phone_e164, CampaignRecipient.created_at)
                .where(CampaignRecipient.is_opted_out.is_(True))
                .order_by(CampaignRecipient.id.asc())
            )
        ).all()
    )
    candidates: list[RefusalCandidate] = []
    for _recipient_id, phone, created_at in rows:
        address = normalize_wa_address(phone)
        if address is None:
            _bump(report.skipped, SKIP_ADDRESS_UNNORMALISABLE)
            continue
        candidates.append(
            RefusalCandidate(
                wa_address=address,
                origin=ORIGIN_RECIPIENT_FLAG,
                observed_at=created_at or utcnow(),
            )
        )
    return candidates


async def _collect_stored_events(
    session: AsyncSession,
    report: ProjectionReport,
    *,
    limit: int,
) -> list[RefusalCandidate]:
    """STOP commands sitting in stored payloads, including ones never recognised.

    Read with the SAME extractor the live ingress uses, so a historical «стоп»
    is found by exactly the rule that will see the next live one — not by a
    second, looser scan written for the backlog. Every message of every payload
    is read, not only the first, which is the other half of what the old path
    missed.

    Chatwoot-sourced rows are excluded here for the same reason the worker
    excludes them: a mirror of a message Meta already delivered is the same
    message, and counting it again would double one person's refusal.

    And the SENDER ROUTE decision of the day is honoured rather than re-made. An
    event the ingress marked ``ignored`` named no route this deployment accepts,
    and a message whose receipt records ``route_allowed = false`` was refused
    individually; neither was ever acted on, and projecting either would turn a
    deliberate refusal into a global suppression the moment somebody ran the
    reconciliation with ``--apply``. Where the per-message verdict was never
    recorded — every event older than that column — the event-level status is
    what is known, and the report says so rather than inventing a verdict.
    """
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
                .order_by(WhatsAppEvent.id.asc())
                .limit(limit + 1)
            )
        ).all()
    )
    if len(rows) > limit:
        report.truncated = True
        report.source_gaps.append(f"stored_events_truncated_at_{limit}")
        rows = rows[:limit]

    # The per-message refusals, read from the receipts the ingress wrote. A tiny
    # set by nature: it holds one row per message this deployment declined.
    refused_messages = {
        (int(event_id), int(index))
        for event_id, index in (
            await session.execute(
                select(WhatsAppInboundReceipt.whatsapp_event_id, WhatsAppInboundReceipt.message_index)
                .where(WhatsAppInboundReceipt.route_allowed.is_(False))
                .where(WhatsAppInboundReceipt.whatsapp_event_id.is_not(None))
            )
        ).all()
    }

    candidates: list[RefusalCandidate] = []
    for event_id, dedupe_key, payload, received_at, event_status in rows:
        if not isinstance(payload, dict):
            report.source_gaps.append("stored_event_payload_not_object")
            continue
        if event_status == "ignored":
            report.events_route_not_allowed += 1
            _bump(report.skipped, SKIP_ROUTE_NOT_ALLOWED)
            continue
        if "_chatwoot" in payload or isinstance(payload.get("_chatwoot_operator_relay"), dict):
            continue
        if isinstance(dedupe_key, str) and (
            dedupe_key.startswith("chatwoot:") or dedupe_key.startswith("chatwoot_out:")
        ):
            continue
        observed = received_at or utcnow()
        for draft in iter_payload_consent_commands(payload, observed_at=observed):
            if (int(event_id), int(draft.message_index)) in refused_messages:
                report.messages_route_not_allowed += 1
                _bump(report.skipped, SKIP_ROUTE_NOT_ALLOWED)
                continue
            if draft.command != CONSENT_COMMAND_STOP:
                # A historical START is never projected. Its authenticity cannot
                # be recovered from a sanitised stored event, so it could not
                # lift a refusal even if it were written — and writing it would
                # manufacture an opt-in record out of an unverifiable payload.
                report.historical_starts_seen += 1
                _bump(report.skipped, SKIP_HISTORICAL_START)
                continue
            # The token that actually matched, so the count means "a refusal
            # the OLD keyword set could not see" rather than "a payload that
            # happens to contain Cyrillic somewhere".
            token_was_cyrillic = any("\u0400" <= ch <= "\u04ff" for ch in (draft.command_token or ""))
            candidates.append(
                RefusalCandidate(
                    wa_address=draft.wa_address,
                    origin=ORIGIN_STORED_EVENT,
                    observed_at=observed,
                    event_at=draft.event_at,
                    whatsapp_event_id=int(event_id),
                    notes={"message_index": draft.message_index},
                )
            )
            if token_was_cyrillic:
                report.cyrillic_stop_events += 1
    return candidates


async def _collect_failure_candidates(
    session: AsyncSession,
    report: ProjectionReport,
) -> list[FailureCandidate]:
    """Production slots whose single attempt ended in a PROVEN Meta refusal.

    The filter is deliberately narrow, and each clause removes a different way
    of being wrong:

    * ``status = send_rejected`` AND ``reason_code`` exactly the delivery
      client's proven-rejection reason — so a local parameter refusal, which
      writes the same status, is excluded;
    * an attempt row with ``outcome='rejected'`` — so a slot with no attempt at
      all cannot become a failure;
    * no ``provider_accepted_at``, no ``delivered_at``, no ``read_at`` — Meta
      never issued an identifier for a refused request, and a success is never
      rolled back into a failure.

    Everything excluded is counted as unknown with its reason. Nothing is
    upgraded, and a slot whose attempt froze no route is recorded with
    ``route_proven=False`` rather than matched against today's configuration.
    """
    rows = list(
        (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem,
                    EasyWeekVoucherProductionBatchAttempt,
                )
                .outerjoin(
                    EasyWeekVoucherProductionBatchAttempt,
                    EasyWeekVoucherProductionBatchAttempt.intent_uuid
                    == EasyWeekVoucherProductionBatchItem.outbound_intent_uuid,
                )
                .where(EasyWeekVoucherProductionBatchItem.status == VOUCHER_PRODUCTION_ITEM_SEND_REJECTED)
                .order_by(EasyWeekVoucherProductionBatchItem.id.asc())
            )
        ).all()
    )
    candidates: list[FailureCandidate] = []
    for item, attempt in rows:
        report.failures_seen += 1
        if item.delivered_at is not None or item.read_at is not None:
            candidates.append(
                FailureCandidate(
                    batch_id=int(item.batch_id),
                    slot=int(item.slot),
                    item_id=int(item.id),
                    provable=False,
                    reason=UNKNOWN_ALREADY_SUCCEEDED,
                )
            )
            _bump(report.failures_unknown, UNKNOWN_ALREADY_SUCCEEDED)
            continue
        if attempt is None or attempt.outcome != "rejected" or item.send_attempted_at is None:
            candidates.append(
                FailureCandidate(
                    batch_id=int(item.batch_id),
                    slot=int(item.slot),
                    item_id=int(item.id),
                    provable=False,
                    reason=UNKNOWN_NO_ATTEMPT,
                )
            )
            _bump(report.failures_unknown, UNKNOWN_NO_ATTEMPT)
            continue
        if item.reason_code != _PROVEN_API_REJECTION_REASON:
            candidates.append(
                FailureCandidate(
                    batch_id=int(item.batch_id),
                    slot=int(item.slot),
                    item_id=int(item.id),
                    provable=False,
                    reason=UNKNOWN_NOT_PROVEN_EXTERNAL,
                )
            )
            _bump(report.failures_unknown, UNKNOWN_NOT_PROVEN_EXTERNAL)
            continue

        route_proven = attempt.sender_phone_number_id_digest is not None and attempt.recipient_digest is not None
        if not route_proven:
            report.failures_route_unknown += 1
        already = await session.scalar(
            select(EasyWeekVoucherProductionDeliveryFailure.id)
            .where(EasyWeekVoucherProductionDeliveryFailure.item_id == int(item.id))
            .where(EasyWeekVoucherProductionDeliveryFailure.source == DELIVERY_FAILURE_SOURCE_META_API)
            .limit(1)
        )
        candidates.append(
            FailureCandidate(
                batch_id=int(item.batch_id),
                slot=int(item.slot),
                item_id=int(item.id),
                provable=True,
                reason=DELIVERY_FAILURE_REASON_API_REJECTED,
                route_proven=route_proven,
                already_recorded=already is not None,
            )
        )
        report.failures_provable += 1
    return candidates


async def project(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    apply: bool = False,
    event_limit: int = 20000,
) -> ProjectionReport:
    """Read every historical source, and write only when ``apply`` is true.

    Reads in one short read-only session, then writes — if asked — one candidate
    per transaction through :func:`apply_projected_refusal`. Per candidate on
    purpose: a projection over a long backlog must not hold one transaction open
    across all of it, because that is exactly the shape that would collide with
    live ingress instead of interleaving with it.

    Re-running is safe and re-running against a stale read is safe: each write
    re-reads the ledger under the address lock and DECIDES AGAIN there, a
    refusal that is already active is a no-op, and nothing here can lower a
    state. A live STOP that lands between the read and the write wins and stays
    won; a live proven START that lands there turns the stale snapshot behind a
    time-unknown candidate into a no-op instead of a permanent refusal.

    A stored STOP is applied even when a refusal is already active, which is how
    the boundary a future START has to beat moves to the newest known one. A
    card flag or a preview snapshot is not: it carries no time of its own, so
    once the ledger refuses there is nothing left for it to say.
    """
    report = ProjectionReport(applied=apply, started_at=utcnow())

    async with session_maker() as session:
        candidates = await _collect_client_flags(session, report)
        candidates.extend(await _collect_recipient_flags(session, report))
        candidates.extend(await _collect_stored_events(session, report, limit=event_limit))
        failures = await _collect_failure_candidates(session, report)
        # Stated every run, because it never stops being true: the ingress
        # strips Meta's signature header before storing an event, so no
        # historical refusal can be proven authentic and no historical START can
        # ever lift one. This is a boundary of the data, not of the projection.
        report.source_gaps.append("historical_signature_unrecoverable")
        # Also stated every run: events stored before the per-message route
        # verdict existed carry only the event-level one. Their accepted
        # messages are projected on that basis, which is what the ingress of the
        # day actually decided — not on today's allow list.
        report.source_gaps.append("historical_per_message_route_unrecorded")

        # Oldest first, so the earliest provable refusal is the one that becomes
        # the ledger's recorded decision rather than whichever source happened
        # to be read last.
        candidates.sort(key=lambda c: (c.observed_at, c.origin, c.wa_address))

        for candidate in candidates:
            report.refusals_seen += 1
            _bump(report.by_origin, candidate.origin)
            decision = await consent_decision(
                session,
                phone=candidate.wa_address,
                include_pending=False,
            )
            # "Already suppressed" means by the LEDGER, not by a legacy flag:
            # the whole point of the projection is to give a flag-only refusal a
            # durable row that survives the card.
            ledger_already = any(source in (SOURCE_SUPPRESSION, SOURCE_SUPPRESSION_UUID) for source in decision.sources)
            candidate.already_suppressed = ledger_already
            if ledger_already and not candidate.carries_event_evidence:
                report.refusals_already_suppressed += 1
                _bump(report.skipped, SKIP_ALREADY_SUPPRESSED)
                continue
            # A SNAPSHOT with no event time of its own cannot override a proven
            # opt-in for the SAME address that this process observed later. Per
            # address deliberately: an opt-in proven for another number of the
            # same person says nothing about this one, and the asymmetry only
            # ever runs in the refusing direction.
            #
            # Scoped to snapshots, which is the fix. A stored STOP MESSAGE whose
            # timestamp cannot be read is NOT stale because of when we received
            # it: a signed START sent at 10:10 can arrive at 10:30, behind a
            # STOP received at 10:20, and dropping that STOP here made this
            # command's answer depend on whether the live worker or this
            # reconciliation got to it. It goes through the ordinary ledger
            # rules instead, where an unknown order refuses.
            #
            # This is the preview of the rule, and it is the same predicate the
            # write uses. The decision that counts is made again inside the
            # write transaction, under the address lock, so a START landing
            # between the two is not missed.
            if not candidate.carries_event_evidence and candidate.event_at is None:
                state = await ledger_state(session, phone=candidate.wa_address)
                if (
                    state.permits
                    and state.decided_observed_at is not None
                    and candidate.observed_at < state.decided_observed_at
                ):
                    report.refusals_superseded += 1
                    _bump(report.skipped, SKIP_SUPERSEDED_BY_OPT_IN)
                    continue
            candidate.projectable = True
            candidate.newly_suppressed = not decision.suppressed
            if candidate.newly_suppressed:
                report.newly_suppressed_addresses += 1
            report.refusals_to_write += 1

    if not apply:
        logger.info("consent_projection: dry run %s", report.as_safe_dict())
        return report

    # Counted from what the WRITES actually did, not from the preview: the
    # ledger is re-read under the lock in every one of those transactions, and
    # its answer there is the only one that describes a change to this database.
    report.newly_suppressed_addresses = 0
    for candidate in candidates:
        if not candidate.projectable:
            continue
        written = None
        for attempt in range(_CARD_SET_ATTEMPTS):
            try:
                written = await _write_one_refusal(session_maker, candidate)
            except CardSetMoved as moved:
                logger.warning(
                    "consent_projection: restarting a candidate, %s (attempt %s)",
                    moved,
                    attempt + 1,
                )
                continue
            break
        if written is None:
            # The matching card set kept moving. The ledger write rolled back
            # with it, so nothing half-applied is left behind, and the candidate
            # is reported as unsettled rather than as a no-op.
            logger.error("consent_projection: gave up on a candidate whose card set kept moving")
            report.refusals_unsettled += 1
            continue
        outcome, converged, was_suppressed = written
        report.cards_converged += converged
        if outcome.applied:
            report.refusals_written += 1
            if not was_suppressed:
                # The same question the preview asks, so the number means one
                # thing in both: this person was sendable and now is not.
                report.newly_suppressed_addresses += 1
        elif outcome.reason == NOOP_PROJECTION_SUPERSEDED:
            report.refusals_superseded += 1
            _bump(report.skipped, SKIP_SUPERSEDED_BY_OPT_IN)
        else:
            report.refusals_noop += 1

    for failure in failures:
        if not failure.provable or failure.already_recorded:
            continue
        if await _write_historical_failure(session_maker, failure):
            report.failures_written += 1

    logger.info("consent_projection: applied %s", report.as_safe_dict())
    return report


# How many times one candidate's transaction is started again when the set of
# matching card rows moved under its locks. Each restart needs another committed
# contact change onto that address inside the span of two statements.
_CARD_SET_ATTEMPTS = 3


async def _write_one_refusal(
    session_maker: async_sessionmaker[AsyncSession],
    candidate: RefusalCandidate,
) -> tuple[DecisionOutcome, int, bool]:
    """One candidate, one transaction, in the one lock order.

    Card rows first, then the consent address inside
    :func:`apply_projected_refusal`, then the re-check that proves the locked
    set still is the set being written, and only then the card write — scoped to
    the rows that were locked. A restart releases every lock and takes the full
    set again from the top, which a retry that kept its locks could not do.

    Returns the ledger outcome, how many card flags were converged, and whether
    this person was already unsendable before the write.
    """
    async with session_maker() as session:
        async with session.begin():
            locked_ids = await _card_ids_matching(session, candidate.wa_address, lock=True)
            before = await consent_decision(
                session,
                phone=candidate.wa_address,
                include_pending=False,
            )
            outcome = await apply_projected_refusal(
                session,
                ProjectedRefusal(
                    wa_address=candidate.wa_address,
                    origin=candidate.origin,
                    observed_at=candidate.observed_at,
                    event_at=candidate.event_at,
                    authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                    whatsapp_event_id=candidate.whatsapp_event_id,
                    snapshot=not candidate.carries_event_evidence,
                    notes={key: value for key, value in candidate.notes.items() if value is not None},
                ),
            )
            if await _card_ids_matching(session, candidate.wa_address, lock=False) != locked_ids:
                raise CardSetMoved(f"{len(locked_ids)} card rows were locked, the match set has changed")
            converged = 0
            if outcome.state == CONSENT_STATE_OPTED_OUT:
                converged = await _converge_cards(
                    session,
                    candidate.wa_address,
                    card_ids=locked_ids,
                    now=utcnow(),
                )
    return outcome, converged, bool(before.suppressed)


async def _write_historical_failure(
    session_maker: async_sessionmaker[AsyncSession],
    candidate: FailureCandidate,
) -> bool:
    """Record one provable historical rejection, through the live write path.

    Through :func:`delivery_evidence.record_api_rejection`, so the row is judged
    by the same rules a live one is: the route comes from the attempt and is
    ``NULL`` for a historical one, the numeric code is absent because the
    delivery client never read one, and the class is therefore ``unknown``. That
    is the honest shape of an old fact — not a fabricated route and not a
    guessed cause — and the fact key makes a second run a no-op.

    Locks the header and then the item, in the one order this phase uses, and
    re-checks under those locks that the slot still looks the way the read said
    it did. A slot that moved in between is left alone.
    """
    async with session_maker() as session:
        async with session.begin():
            header = await session.get(EasyWeekVoucherProductionBatch, candidate.batch_id, with_for_update=True)
            if header is None:
                return False
            item = (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.id == candidate.item_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if item is None:
                return False
            if (
                item.status != VOUCHER_PRODUCTION_ITEM_SEND_REJECTED
                or item.reason_code != _PROVEN_API_REJECTION_REASON
                or item.delivered_at is not None
                or item.read_at is not None
            ):
                # It moved since the read. Whatever it is now, it is not the
                # fact this candidate described.
                return False
            outcome = await delivery_evidence.record_api_rejection(
                session,
                item=item,
                http_status=None,
            )
            return outcome.stored


async def describe_failures(
    session_maker: async_sessionmaker[AsyncSession],
) -> list[FailureCandidate]:
    """The historical delivery outcomes and whether each can be proven.

    Separate from :func:`project` so a reviewer can read the list without the
    refusal pass running at all. Nothing here writes, and the conflict labels it
    reuses are the same ones the live path records, so one vocabulary describes
    both.
    """
    report = ProjectionReport(started_at=utcnow())
    async with session_maker() as session:
        return await _collect_failure_candidates(session, report)


__all__ = [
    "ORIGIN_CLIENT_FLAG",
    "ORIGIN_RECIPIENT_FLAG",
    "ORIGIN_STORED_EVENT",
    "SKIP_ADDRESS_UNNORMALISABLE",
    "SKIP_ALREADY_SUPPRESSED",
    "SKIP_HISTORICAL_START",
    "SKIP_ROUTE_NOT_ALLOWED",
    "SKIP_SUPERSEDED_BY_OPT_IN",
    "UNKNOWN_ALREADY_SUCCEEDED",
    "UNKNOWN_NOT_PROVEN_EXTERNAL",
    "UNKNOWN_NO_ATTEMPT",
    "FailureCandidate",
    "ProjectionReport",
    "CardSetMoved",
    "RefusalCandidate",
    "describe_failures",
    "project",
]
