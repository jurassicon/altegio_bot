"""Re-prove ONE row's eligibility immediately before its attempt (§46.5).

Why this exists beside the preview
----------------------------------
A preview is evidence of what was true at ``checked_at``. §46.5 requires every
condition to be proven AGAIN at confirmation and again before the send, because
consent, a reply, a booking, an identity, a template or a stop can all move in
between — and a row that loses eligibility must be excluded with a reason while
nobody takes its place.

So this module re-runs the SAME checks the preview ran, through the same
primitives in :mod:`...easyweek_voucher_followup.eligibility`, for one row. It
is deliberately not a second, friendlier derivation: a disagreement between what
an operator approved and what the executor acts on is exactly the class of bug
the section is written against.

The shape of a re-check, and why it is two phases
-------------------------------------------------
**Local.** One short read transaction: the source slot, its single attempt, the
confirmed failures, the proven receipts, the identity, the entitlement, the
consent aggregate, the replies, and the bounded stored-event backlog for this
person's own addresses. No lock outlives it.

**External.** No transaction at all. The original artifact and the live booking
history are the only network steps, and §46.5 forbids holding a row lock or an
advisory lock across either. The simplest way to obey that is to hold no session
while they run.

What this module never does
---------------------------
It never claims, sends, writes or reserves anything. It reads and judges. The
claim — and with it the consumption of the one extra attempt — belongs to
:mod:`...easyweek_voucher_followup.send_ledger`, and the consent boundary is
re-asked there, under the consent address locks, inside the claiming
transaction.
"""

from __future__ import annotations

import uuid as uuid_module
from collections.abc import Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Final, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_customer_history import CustomerBookingSet
from altegio_bot.campaigns.easyweek_customer_history import (
    read_customer_bookings as _read_customer_bookings,
)
from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message
from altegio_bot.campaigns.easyweek_voucher_followup import preview as preview_module
from altegio_bot.campaigns.easyweek_voucher_followup.artifact_proof import (
    prove_source_artifact as _prove_source_artifact,
)
from altegio_bot.campaigns.easyweek_voucher_followup.booking_evidence import load_creation_evidence
from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import (
    RowFacts,
    booking_verdict,
    consent_snapshot,
    entitlement_verdict,
    failure_policy_reason,
    has_replied_since,
    interval_elapsed,
    resolve_identity,
    source_entitlement_digest,
)
from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import (
    CURRENT_FAILURE_POLICY,
    FailureFact,
    FailurePolicy,
)
from altegio_bot.campaigns.easyweek_voucher_followup.groups import SourceEvidence, resolve_group
from altegio_bot.campaigns.easyweek_voucher_followup.historical_consent import (
    MAX_SCANNED_EVENTS,
    scan_historical_refusals,
)
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    BOOKING_CAPTURE_UNPROVEN,
    CUSTOMER_REPLIED,
    GROUP_MOVED_OUT_OF_SELECTION,
    HISTORICAL_CONSENT_UNPROVEN,
    HISTORICAL_STOP_UNACCOUNTED,
    HISTORY_UNPROVEN,
    IDENTITY_CONFLICT,
    INTERVAL_NOT_ELAPSED,
    SENDER_UNPROVEN,
    SOURCE_ATTEMPT_UNPROVEN,
    STATUS_EVIDENCE_UNPROVEN,
    TEMPLATE_UNPROVEN,
)
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import (
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_RECEIPT_KIND_DELIVERED,
    DELIVERY_RECEIPT_KIND_READ,
    DELIVERY_RECEIPT_MATCH_CONFIRMED,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    EasyWeekVoucherProductionDeliveryReceipt,
)

__all__ = [
    "BOOKING_LINK_UNPROVEN",
    "claim_time_guard",
    "TEMPLATE_PARAMETERS_UNPROVEN",
    "LocalRecheck",
    "RowRecheck",
    "SendContract",
    "local_recheck",
    "prepare_row",
]

# The branch registry has no usable booking page for this company. Its own code,
# because an empty link is a link to nowhere in a real person's WhatsApp and
# §46.6 forbids sending a message that is not the one Meta approved.
BOOKING_LINK_UNPROVEN: Final = "followup_booking_link_unproven"
# One of the two approved BODY parameters could not be built, or came out empty.
TEMPLATE_PARAMETERS_UNPROVEN: Final = "followup_template_parameters_unproven"


@dataclass(frozen=True)
class SendContract:
    """The approved message contract an attempt must match, exactly.

    Taken from the stored approval rather than recomputed, so a template that
    has changed since the operator confirmed is a refusal instead of a different
    message going out under an old confirmation.
    """

    template_code: str
    meta_template_name: str
    template_language: str
    template_category: str
    template_parameter_order: tuple[str, ...]
    template_body_digest: str
    sender_digest: str
    selected_groups: tuple[str, ...]
    min_interval_seconds: int
    failure_policy_version: str

    def matches_today(self, *, policy: FailurePolicy = CURRENT_FAILURE_POLICY) -> bool:
        """Is this still the contract this deployment would compose now?

        Compared field by field rather than by one digest: the body digest
        already covers the text, and the rest has to be checked because an
        approval may not outlive a change to the code, the name, the language,
        the category, the parameter order or the failure policy version.
        """
        return (
            self.template_code == message.FOLLOWUP_TEMPLATE_CODE
            and self.meta_template_name == message.FOLLOWUP_META_TEMPLATE_NAME
            and self.template_language == message.FOLLOWUP_TEMPLATE_LANGUAGE
            and self.template_category == message.FOLLOWUP_TEMPLATE_CATEGORY
            and tuple(self.template_parameter_order) == message.FOLLOWUP_TEMPLATE_FIELDS
            and self.template_body_digest == message.body_digest()
            and self.failure_policy_version == policy.version
        )


@dataclass
class LocalRecheck:
    """What one short read transaction established about one row.

    ``address`` and ``display_name`` are working values of the send, not
    evidence to keep: one is personal data and the other is a customer's name.
    They are carried in memory to the POST and never written to this phase's
    tables (§46.8).
    """

    eligible: bool
    reason: str | None = None
    unknown: bool = False
    address: str | None = None
    customer_uuid: uuid_module.UUID | None = None
    display_name: str = ""
    consent_fingerprint: str | None = None
    sender_digest: str | None = None
    recipient_digest: str | None = None
    group_code: str | None = None
    entitlement_digest: str = ""
    source_attempted_at: datetime | None = None
    # Carried across the external phase so that phase holds no session.
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RowRecheck:
    """The complete verdict for one row: local and external together."""

    eligible: bool
    reason: str | None = None
    unknown: bool = False
    address: str | None = None
    customer_uuid: uuid_module.UUID | None = None
    display_name: str = ""
    booking_link: str = ""
    consent_fingerprint: str | None = None
    sender_digest: str | None = None
    recipient_digest: str | None = None
    group_code: str | None = None
    booking_reads: int = 0
    evidence: dict[str, Any] = field(default_factory=dict)

    def params(self) -> list[str]:
        """The approved BODY parameters, in the approved order.

        Built here so there is one place that knows the order, and deliberately
        returned as a fresh list the caller is expected to delete after the
        POST: it carries a real person's name and a link.
        """
        values = {"client_name": self.display_name, "booking_link": self.booking_link}
        return [values[name] for name in message.FOLLOWUP_TEMPLATE_FIELDS]


async def _confirmed_failures(
    session: AsyncSession, *, item_id: int
) -> tuple[EasyWeekVoucherProductionDeliveryFailure, ...]:
    rows = (
        await session.execute(
            select(EasyWeekVoucherProductionDeliveryFailure)
            .where(EasyWeekVoucherProductionDeliveryFailure.item_id == int(item_id))
            .where(EasyWeekVoucherProductionDeliveryFailure.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED)
            .order_by(EasyWeekVoucherProductionDeliveryFailure.id.asc())
        )
    ).scalars()
    return tuple(rows)


async def _proven_receipt_kinds(session: AsyncSession, *, item_id: int) -> set[str]:
    rows = (
        await session.execute(
            select(EasyWeekVoucherProductionDeliveryReceipt.kind)
            .where(EasyWeekVoucherProductionDeliveryReceipt.item_id == int(item_id))
            .where(EasyWeekVoucherProductionDeliveryReceipt.match_state == DELIVERY_RECEIPT_MATCH_CONFIRMED)
        )
    ).all()
    return {str(kind) for (kind,) in rows}


async def _sender_still_approved(
    session: AsyncSession,
    *,
    company_id: int,
    expected_digest: str,
) -> bool:
    """Is this branch's WhatsApp sender still active AND still the frozen route?

    Both, and the digest comparison is the load-bearing half: a deployment
    repointed at another number must not inherit this audience, and a sender row
    switched off must not be sent from. ``None`` of the two is a reason to pick a
    different sender — this phase has exactly one route.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.dispatch import SENDER_CODE
    from altegio_bot.models.models import WhatsAppSender
    from altegio_bot.whatsapp_consent import sender_route_digest

    row = (
        await session.execute(
            select(WhatsAppSender)
            .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
            .where(WhatsAppSender.company_id == int(company_id))
            .where(WhatsAppSender.sender_code == SENDER_CODE)
        )
    ).scalar_one_or_none()
    if row is None or not row.is_active:
        return False
    identifier = (row.phone_number_id or "").strip()
    if not identifier:
        return False
    return bool(expected_digest) and sender_route_digest(identifier) == expected_digest


def booking_link_for(company_id: int) -> str:
    """This branch's approved booking page, from the server-side registry.

    Never from a request, never a constant here, and never an empty string
    standing in for a link that is not configured: the caller refuses instead.
    """
    registry = configured_easyweek_locations()
    if not registry.ready:
        return ""
    location = registry.locations.get(int(company_id))
    link = (location.booking_page_url or "").strip() if location is not None else ""
    return link if link.startswith("https://") else ""


async def local_recheck(
    session: AsyncSession,
    *,
    gate: preview_module.BatchGate,
    source_item_id: int,
    contract: SendContract,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime,
    configured_key_id: str | None,
    historical_event_limit: int = MAX_SCANNED_EVENTS,
) -> LocalRecheck:
    """Everything about one row that can be decided without a network request.

    The preview's own order, kept deliberately: the source attempt and its
    group, the sender geography, the interval, the failure policy, the
    entitlement, the identity, consent, the stored-event backlog, the replies.
    Cheap and local first, so the external phase is spent only on a row that
    would otherwise be sent to.

    Every refusal is one §46.5 names, and an unproven fact is reported as
    ``unknown`` rather than as a pass.
    """
    item = await session.get(EasyWeekVoucherProductionBatchItem, int(source_item_id))
    if item is None or int(item.batch_id) != int(gate.batch_id or 0):
        return LocalRecheck(False, IDENTITY_CONFLICT, unknown=True)

    # §46.6, before anything else about this person: the approved message
    # contract and the route it would go out on, re-proven at this instant.
    #
    # Both halves used to be checked once per PLAN and never again, so a
    # template deactivated after the confirmation still sent, and a sender
    # deactivated between two recipients still sent to the second. They are
    # local reads, so they belong in this pass and are re-run for every attempt.
    if not contract.matches_today(policy=policy):
        return LocalRecheck(False, TEMPLATE_UNPROVEN)
    if not await preview_module.local_template_row_proven(session, company_id=int(gate.company_id or 0)):
        return LocalRecheck(False, TEMPLATE_UNPROVEN)
    if not await _sender_still_approved(
        session,
        company_id=int(gate.company_id or 0),
        expected_digest=contract.sender_digest,
    ):
        # §46.1 keeps the repeat message on the original send geography, and
        # nothing here falls back to whichever sender the settings now name.
        return LocalRecheck(False, SENDER_UNPROVEN)

    attempt = (
        await session.execute(
            select(EasyWeekVoucherProductionBatchAttempt)
            .where(EasyWeekVoucherProductionBatchAttempt.item_id == int(item.id))
            .order_by(EasyWeekVoucherProductionBatchAttempt.id.asc())
            .limit(1)
        )
    ).scalar_one_or_none()
    receipts = await _proven_receipt_kinds(session, item_id=int(item.id))
    failures = await _confirmed_failures(session, item_id=int(item.id))

    evidence = SourceEvidence(
        item_id=int(item.id),
        slot=int(item.slot),
        status=str(item.status or ""),
        send_attempt_count=int(item.send_attempt_count or 0),
        send_attempted_at=item.send_attempted_at,
        delivered_at=item.delivered_at,
        read_at=item.read_at,
        item_message_id=item.provider_message_id,
        attempt_message_id=(attempt.provider_message_id if attempt is not None else None),
        attempt_sender_digest=(attempt.sender_phone_number_id_digest if attempt is not None else None),
        attempt_recipient_digest=(attempt.recipient_digest if attempt is not None else None),
        confirmed_failures=tuple(
            FailureFact(
                match_state=str(row.match_state),
                failure_class=str(row.failure_class),
                meta_error_code=(int(row.meta_error_code) if row.meta_error_code is not None else None),
                reason_code=str(row.reason_code),
            )
            for row in failures
        ),
        delivered_receipt_proven=DELIVERY_RECEIPT_KIND_DELIVERED in receipts,
        read_receipt_proven=DELIVERY_RECEIPT_KIND_READ in receipts,
    )

    group = resolve_group(evidence, expected_sender_digest=contract.sender_digest)
    if not group.proven:
        reason = group.reason or STATUS_EVIDENCE_UNPROVEN
        return LocalRecheck(False, reason, unknown=reason != SENDER_UNPROVEN)
    if group.group not in contract.selected_groups:
        # §46.5 allows a move between two groups the operator already selected,
        # under one approved message contract, and requires a new preview
        # otherwise. The contract is one neutral text for all three, so the only
        # refusal left is a move OUT of the selection.
        return LocalRecheck(False, GROUP_MOVED_OUT_OF_SELECTION)
    if message.template_code_for_groups(sorted({group.group, *contract.selected_groups})) != contract.template_code:
        return LocalRecheck(False, TEMPLATE_UNPROVEN)

    if evidence.send_attempted_at is None or int(evidence.send_attempt_count or 0) != 1:
        return LocalRecheck(False, SOURCE_ATTEMPT_UNPROVEN, unknown=True)
    if not interval_elapsed(
        attempted_at=item.send_attempted_at,
        min_interval_seconds=int(contract.min_interval_seconds),
        now=now,
    ):
        return LocalRecheck(False, INTERVAL_NOT_ELAPSED)

    digest = source_entitlement_digest(
        batch_id=int(item.batch_id),
        slot=int(item.slot),
        voucher_code_hmac=item.voucher_code_hmac,
        hmac_key_id=item.hmac_key_id,
        target_order_uuid=item.target_order_uuid,
    )
    facts = RowFacts(
        item_id=int(item.id),
        slot=int(item.slot),
        campaign_recipient_id=int(item.campaign_recipient_id),
        easyweek_customer_uuid=item.easyweek_customer_uuid,
        source_attempted_at=item.send_attempted_at,
        evidence=evidence,
        group=group,
        entitlement_digest=digest,
        recipient_digest=evidence.attempt_recipient_digest,
    )
    policy_refusal = failure_policy_reason(facts, policy=policy)
    if policy_refusal is not None:
        return LocalRecheck(False, policy_refusal)

    entitlement = entitlement_verdict(
        batch_id=int(item.batch_id),
        slot=int(item.slot),
        status=str(item.status or ""),
        voucher_code_hmac=item.voucher_code_hmac,
        hmac_key_id=item.hmac_key_id,
        target_order_uuid=item.target_order_uuid,
        configured_key_id=configured_key_id,
    )
    if not entitlement.proven:
        assert entitlement.reason is not None
        return LocalRecheck(False, entitlement.reason)

    identity = await resolve_identity(
        session,
        campaign_recipient_id=int(item.campaign_recipient_id),
        company_id=int(gate.company_id or 0),
        campaign_run_id=int(gate.campaign_run_id or 0),
        recipient_basis=str(item.recipient_basis or ""),
        expected_customer_uuid=item.easyweek_customer_uuid,
        expected_source_booking_uuid=item.source_booking_uuid,
        attempt_recipient_digest=evidence.attempt_recipient_digest,
    )
    if not identity.proven or identity.address is None:
        return LocalRecheck(False, identity.reason or IDENTITY_CONFLICT, unknown=True)

    consent = await consent_snapshot(
        session,
        phone=identity.address,
        easyweek_customer_uuid=identity.customer_uuid,
    )
    if not consent.allowed:
        assert consent.reason is not None
        return LocalRecheck(False, consent.reason)

    # §46.4.4, for this person's own addresses. The live aggregate reads the
    # ledger, the cards and the recipient snapshot, and every one of those is
    # silent about a refusal still sitting unaccounted in a stored event.
    historical = await scan_historical_refusals(
        session,
        addresses=set(consent.addresses) | {identity.address},
        limit=int(historical_event_limit),
        now=now,
    )
    if not historical.complete:
        return LocalRecheck(False, HISTORICAL_CONSENT_UNPROVEN, unknown=True)
    if historical.blocks_any((identity.address, *consent.addresses)):
        return LocalRecheck(False, HISTORICAL_STOP_UNACCOUNTED)

    assert item.send_attempted_at is not None
    if await has_replied_since(
        session,
        address=str(identity.address),
        easyweek_customer_uuid=identity.customer_uuid,
        since=item.send_attempted_at,
    ):
        return LocalRecheck(False, CUSTOMER_REPLIED)

    card = (
        await session.execute(
            select(Client.display_name)
            .where(Client.provider == PROVIDER_EASYWEEK)
            .where(Client.company_id == int(gate.company_id or 0))
            .where(Client.easyweek_customer_uuid == item.easyweek_customer_uuid)
            .limit(2)
        )
    ).all()
    display_name = (card[0][0] or "").strip() if len(card) == 1 else ""

    return LocalRecheck(
        True,
        None,
        address=identity.address,
        customer_uuid=identity.customer_uuid,
        display_name=display_name,
        consent_fingerprint=consent.fingerprint,
        sender_digest=evidence.attempt_sender_digest,
        recipient_digest=evidence.attempt_recipient_digest,
        group_code=group.group,
        entitlement_digest=digest,
        source_attempted_at=item.send_attempted_at,
        notes={
            "batch_id": int(item.batch_id),
            "slot": int(item.slot),
            "schema_version": gate.schema_version,
            "target_order_uuid": str(item.target_order_uuid) if item.target_order_uuid else None,
            "reconciliation_marker": str(item.reconciliation_marker or ""),
        },
    )


async def prepare_row(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    gate: preview_module.BatchGate,
    source_item_id: int,
    contract: SendContract,
    booking_reader: Any | None,
    order_reader: Any | None = None,
    template_reader: Any | None = None,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime,
    read_bookings: Callable[..., Any] = _read_customer_bookings,
    prove_artifact: Callable[..., Any] = _prove_source_artifact,
    prove_meta_template: Callable[..., Any] | None = None,
) -> RowRecheck:
    """The whole re-check for one row: local, external, then local AGAIN.

    Four phases, and the fourth is the fix for a real hole. The old shape was
    local-then-external and returned the FIRST pass's values, so anything local
    that moved while the artifact and the history were being read was simply not
    seen: a proven ``read`` arriving for a row chosen as "delivered, read not
    confirmed", or the customer's own reply being processed, both left the row
    eligible and the executor then sent to them. A consent fingerprint does not
    cover any of that — it covers consent.

    So:

    1. **local** — identity, group, sender geography, interval, failure policy,
       entitlement, the approved contract, the local template row, the active
       sender, consent, the stored-event backlog, replies;
    2. **the live Meta proof** of the exact APPROVED template, in THIS
       evaluation, because an approved template can be paused between two rows;
    3. **the external proofs** — the original artifact and the fully paginated
       booking history;
    4. **local again**, in a fresh snapshot. It must still be eligible, and it
       must still agree with the first pass about the things the claim will act
       on: the group, the addressee, the route and the consent version. The
       values returned are the SECOND pass's, so the claim acts on what was true
       after the network, not before it.

    No transaction and no lock is held across any request, which is §46.5's
    rule and the reason each local phase closes its session first.

    A missing reader is never an empty answer: without a configured EasyWeek
    read the booking history is UNKNOWN, and unknown excludes rather than
    permits.
    """
    configured_key_id = preview_module.configured_binding_key_id()
    async with session_maker() as session:
        await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        local = await local_recheck(
            session,
            gate=gate,
            source_item_id=int(source_item_id),
            contract=contract,
            policy=policy,
            now=now,
            configured_key_id=configured_key_id,
        )
    if not local.eligible:
        return RowRecheck(
            False,
            local.reason,
            unknown=local.unknown,
            group_code=local.group_code,
            evidence={"phase": "local"},
        )

    assert local.customer_uuid is not None and local.address is not None

    # §46.6: the live Meta half, with no session open. Asked per attempt rather
    # than per plan, because a template paused between two recipients must stop
    # the second one.
    # Resolved at call time rather than bound at import: the module attribute is
    # the seam the Ops suites replace, and a reference captured at import would
    # quietly keep the real one.
    prove_template = prove_meta_template or preview_module.prove_meta_followup_template
    template = await prove_template(template_reader)
    if not template.proven or not template.meta_verified:
        return RowRecheck(
            False,
            TEMPLATE_UNPROVEN,
            group_code=local.group_code,
            evidence={"phase": "meta_template"},
        )
    if (
        template.template_code != contract.template_code
        or template.meta_template_name != contract.meta_template_name
        or template.language != contract.template_language
        or template.category != contract.template_category
    ):
        # Proven, but not the contract this run was confirmed under.
        return RowRecheck(
            False,
            TEMPLATE_UNPROVEN,
            group_code=local.group_code,
            evidence={"phase": "meta_template"},
        )

    # §46.6: the ORIGINAL artifact, read and proven rather than inferred from
    # the presence of its binding columns. First, because a slot whose voucher
    # is gone needs no booking read at all.
    artifact = await prove_artifact(
        order_reader if order_reader is not None else booking_reader,
        session_maker=session_maker,
        batch_id=int(local.notes["batch_id"]),
        slot=int(local.notes["slot"]),
        schema_version=str(local.notes["schema_version"]),
        target_order_uuid=local.notes.get("target_order_uuid"),
        reconciliation_marker=str(local.notes.get("reconciliation_marker") or ""),
        easyweek_customer_uuid=local.customer_uuid,
        now=now,
    )
    if not artifact.proven:
        assert artifact.reason is not None
        return RowRecheck(
            False,
            artifact.reason,
            unknown=bool(artifact.unknown),
            group_code=local.group_code,
            evidence={"phase": "artifact", "artifact": dict(artifact.observations)},
        )

    if booking_reader is None:
        return RowRecheck(
            False,
            HISTORY_UNPROVEN,
            unknown=True,
            group_code=local.group_code,
            evidence={"phase": "bookings", "booking_proof": "reader_unavailable"},
        )
    bookings: CustomerBookingSet = await read_bookings(booking_reader, customer_uuid=local.customer_uuid)

    # The stored creation deliveries, in a session of its own with nothing
    # external in flight. It needs the live read's answer, which is why it is a
    # second sub-phase rather than part of the local one.
    async with session_maker() as session:
        items = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.batch_id == int(local.notes["batch_id"]))
                    .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
                )
            ).scalars()
        )
        creations = await load_creation_evidence(
            session,
            items=items,
            now=now,
            unproven_reason=BOOKING_CAPTURE_UNPROVEN,
            conflict_reason=IDENTITY_CONFLICT,
            slot_bookings=(
                {int(local.notes["slot"]): tuple(row.booking_uuid for row in bookings.bookings)}
                if bookings.complete
                else {}
            ),
        )

    assert local.source_attempted_at is not None
    verdict = booking_verdict(
        bookings,
        creations=creations.for_slot(int(local.notes["slot"]), reason=BOOKING_CAPTURE_UNPROVEN),
        attempted_at=local.source_attempted_at,
        now=now,
    )
    if not verdict.proven:
        assert verdict.reason is not None
        return RowRecheck(
            False,
            verdict.reason,
            unknown=bool(verdict.unknown),
            group_code=local.group_code,
            booking_reads=1,
            evidence={"phase": "bookings", "booking_reason": verdict.detail or verdict.reason},
        )

    link = booking_link_for(int(gate.company_id or 0))
    if not link:
        return RowRecheck(
            False,
            BOOKING_LINK_UNPROVEN,
            group_code=local.group_code,
            booking_reads=1,
            evidence={"phase": "contract"},
        )
    # Phase 4: local AGAIN, in a fresh snapshot, after everything external.
    async with session_maker() as session:
        await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        again = await local_recheck(
            session,
            gate=gate,
            source_item_id=int(source_item_id),
            contract=contract,
            policy=policy,
            now=now,
            configured_key_id=configured_key_id,
        )
    if not again.eligible:
        # Something local moved while the network was busy. Excluded with the
        # reason it moved for, and nobody takes its place.
        return RowRecheck(
            False,
            again.reason,
            unknown=again.unknown,
            group_code=again.group_code or local.group_code,
            booking_reads=1,
            evidence={"phase": "local_after_external"},
        )
    if (
        again.address != local.address
        or again.customer_uuid != local.customer_uuid
        or again.recipient_digest != local.recipient_digest
        or again.sender_digest != local.sender_digest
        or again.consent_fingerprint != local.consent_fingerprint
    ):
        # Still eligible, but not about the same PERSON or the same route the
        # external proofs were spent on. §46.5 wants a new preview for that, not
        # a send.
        #
        # The GROUP is deliberately not compared here. §46.5 allows a move
        # between two groups the operator already selected under one approved
        # message contract, and that is exactly what a read receipt arriving
        # mid-run is; a move OUT of the selection is refused by the local pass
        # itself, with its own reason.
        return RowRecheck(
            False,
            IDENTITY_CONFLICT,
            unknown=True,
            group_code=again.group_code,
            booking_reads=1,
            evidence={"phase": "local_after_external", "recheck": "facts_changed"},
        )

    if not again.display_name:
        return RowRecheck(
            False,
            TEMPLATE_PARAMETERS_UNPROVEN,
            group_code=again.group_code,
            booking_reads=1,
            evidence={"phase": "contract"},
        )

    return RowRecheck(
        True,
        None,
        address=again.address,
        customer_uuid=again.customer_uuid,
        display_name=again.display_name,
        booking_link=link,
        consent_fingerprint=again.consent_fingerprint,
        sender_digest=again.sender_digest,
        recipient_digest=again.recipient_digest,
        group_code=again.group_code,
        booking_reads=1,
        evidence={"booking_count": verdict.booking_count, "booking_pages_read": verdict.pages_read},
    )


def selected_groups_cover(groups: Sequence[str], group_code: str | None) -> bool:
    """Is this row's proven group one the operator actually selected?"""
    return group_code is not None and group_code in tuple(groups)


def claim_time_guard(
    *,
    gate: preview_module.BatchGate,
    source_item_id: int,
    contract: SendContract,
    expected: RowRecheck,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime,
) -> Callable[[AsyncSession], Awaitable[Any]]:
    """The LAST local word, to be run inside the claiming transaction.

    Why this exists on top of the two passes in :func:`prepare_row`: those both
    finish before the claim opens its transaction, and that transaction can then
    WAIT — for the original batch header, for a consent address — while another
    writer holds the lock. Whatever moved locally during that wait was never
    looked at again, and the consent fingerprint the claim does compare is a
    version of CONSENT and of nothing else.

    What is re-proven, with every lock already granted: the whole local
    per-attempt check (identity and bindings, the proven group and the operator's
    selection, the interval, the failure policy, the entitlement, the approved
    contract, the local template row, the active sender, the consent aggregate
    including unresolved inbound, the stored-event backlog and the replies). It
    then has to still be about the same PERSON and the same ROUTE the external
    proofs were spent on.

    The GROUP is deliberately not compared with the earlier pass: §46.5 allows a
    move between two groups the operator already selected under one approved
    contract, and ``local_recheck`` refuses a move out of the selection itself.

    Local reads only, so nothing external runs under the locks, and bounded: it
    is the same per-row check, once more.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup.send_ledger import GuardVerdict

    configured_key_id = preview_module.configured_binding_key_id()

    async def guard(session: AsyncSession) -> Any:
        verdict = await local_recheck(
            session,
            gate=gate,
            source_item_id=int(source_item_id),
            contract=contract,
            policy=policy,
            now=now,
            configured_key_id=configured_key_id,
        )
        if not verdict.eligible:
            return GuardVerdict(False, verdict.reason)
        if (
            verdict.address != expected.address
            or verdict.customer_uuid != expected.customer_uuid
            or verdict.recipient_digest != expected.recipient_digest
            or verdict.sender_digest != expected.sender_digest
        ):
            return GuardVerdict(False, IDENTITY_CONFLICT)
        if not verdict.display_name:
            return GuardVerdict(False, TEMPLATE_PARAMETERS_UNPROVEN)
        return GuardVerdict(True, None)

    return guard
