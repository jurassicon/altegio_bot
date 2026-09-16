"""The durable claim ledger of the manual-basis voucher delivery canary (§37.2).

Every external step here is irreversible by repetition, and each can end with
the request sent and the answer lost. EasyWeek publishes no write idempotency
key, so a second attempt is a coin flip on whether a second order, a second
payment or a second customer message happens. The rule that makes that
survivable is the same one §35 and §36 earned:

    the claim is committed BEFORE the request leaves.

A crash anywhere after that commit — before the socket, mid-flight, after the
response — reads identically afterwards: *claimed, outcome unknown*. That is the
only reading that cannot charge a card twice or message a person twice, and it
is why these functions open and commit their own transactions rather than
joining a caller's.

Its own table, not §36's
------------------------
The machinery is §36's, proven; the identity is not. A row here says "an
operator chose this person by hand", and the entitlement it protects is not a
booking but a person within a campaign period. Sharing §36's table would mean
one of two lies: either a manual row claiming an earned basis, or a nullable
booking column standing open for something to be invented into.

Monotonic, and defended against a stale writer
----------------------------------------------
Every write names the states it is valid FROM; the row is locked with
``SELECT ... FOR UPDATE`` and compared before it is touched. Every status also
carries a rank, and a write that would lower it is refused. Together they mean a
reconciliation that read the world a moment too early cannot walk a stage back
into a state the same request could be claimed from again.

The send is different from the mutations
----------------------------------------
A create or a pay may be re-claimed after a rejection the transport proved did
not act. A send may not, ever. ``send_attempt_count`` is capped at one by a
database constraint, so "try again" is not a policy this code implements and
then guards — it is a state the schema cannot represent.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    MANUAL_VOUCHER_SCHEMA_VERSION,
    MANUAL_VOUCHER_SCOPE,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
    MANUAL_VOUCHER_DOMAIN,
    voucher_code_matches,
)
from altegio_bot.models.models import (
    MANUAL_VOUCHER_AMBIGUOUS,
    MANUAL_VOUCHER_CREATE_CLAIMED,
    MANUAL_VOUCHER_CREATE_REJECTED,
    MANUAL_VOUCHER_CREATE_UNKNOWN,
    MANUAL_VOUCHER_CREATED,
    MANUAL_VOUCHER_DELIVERED,
    MANUAL_VOUCHER_MANUALLY_CLEANED,
    MANUAL_VOUCHER_PAID,
    MANUAL_VOUCHER_PAY_CLAIMED,
    MANUAL_VOUCHER_PAY_REJECTED,
    MANUAL_VOUCHER_PAY_UNKNOWN,
    MANUAL_VOUCHER_PLANNED,
    MANUAL_VOUCHER_PROVIDER_ACCEPTED,
    MANUAL_VOUCHER_READ,
    MANUAL_VOUCHER_REFUND_CLAIMED,
    MANUAL_VOUCHER_REFUND_REJECTED,
    MANUAL_VOUCHER_REFUND_UNKNOWN,
    MANUAL_VOUCHER_REFUNDED,
    MANUAL_VOUCHER_SEND_CLAIMED,
    MANUAL_VOUCHER_SEND_REJECTED,
    MANUAL_VOUCHER_SEND_UNKNOWN,
    MANUAL_VOUCHER_STATUSES,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    EasyWeekManualVoucherDeliveryAttempt,
    EasyWeekManualVoucherDeliveryLedger,
)
from altegio_bot.utils import utcnow

# How far along the canary a status sits. A write that would lower this rank is
# a regression and is refused, whoever sent it and however late it arrives.
# `ambiguous` sits at the top because it is a full stop.
STATUS_RANK: Final[dict[str, int]] = {
    MANUAL_VOUCHER_PLANNED: 5,
    MANUAL_VOUCHER_CREATE_CLAIMED: 10,
    MANUAL_VOUCHER_CREATE_UNKNOWN: 11,
    MANUAL_VOUCHER_CREATE_REJECTED: 12,
    MANUAL_VOUCHER_CREATED: 20,
    MANUAL_VOUCHER_PAY_CLAIMED: 30,
    MANUAL_VOUCHER_PAY_UNKNOWN: 31,
    MANUAL_VOUCHER_PAY_REJECTED: 32,
    MANUAL_VOUCHER_PAID: 40,
    MANUAL_VOUCHER_REFUND_CLAIMED: 41,
    MANUAL_VOUCHER_REFUND_UNKNOWN: 42,
    MANUAL_VOUCHER_REFUND_REJECTED: 43,
    MANUAL_VOUCHER_REFUNDED: 45,
    MANUAL_VOUCHER_SEND_CLAIMED: 50,
    MANUAL_VOUCHER_SEND_REJECTED: 51,
    MANUAL_VOUCHER_SEND_UNKNOWN: 52,
    MANUAL_VOUCHER_PROVIDER_ACCEPTED: 60,
    MANUAL_VOUCHER_DELIVERED: 70,
    MANUAL_VOUCHER_READ: 80,
    MANUAL_VOUCHER_MANUALLY_CLEANED: 90,
    MANUAL_VOUCHER_AMBIGUOUS: 99,
}

# A stage may be claimed only from these states. `*_rejected` is re-claimable
# for the two EasyWeek mutations, because a proven validation refusal did not
# act. The SEND has no such entry: one attempt, ever.
CREATE_CLAIMABLE_FROM: Final = frozenset({MANUAL_VOUCHER_PLANNED, MANUAL_VOUCHER_CREATE_REJECTED})
PAY_CLAIMABLE_FROM: Final = frozenset({MANUAL_VOUCHER_CREATED, MANUAL_VOUCHER_PAY_REJECTED})
SEND_CLAIMABLE_FROM: Final = frozenset({MANUAL_VOUCHER_PAID})
# A refund is claimable from `pay_unknown` too. The money has probably moved and
# the artifact could not be proven — which is precisely when getting it back
# matters most. It is still pre-send only, and the CHECK constraint says so
# independently.
REFUND_CLAIMABLE_FROM: Final = frozenset(
    {MANUAL_VOUCHER_PAID, MANUAL_VOUCHER_PAY_UNKNOWN, MANUAL_VOUCHER_REFUND_REJECTED}
)

# States in which something may still have reached EasyWeek or Meta.
UNRESOLVED_STATUSES: Final = frozenset(
    {
        MANUAL_VOUCHER_CREATE_CLAIMED,
        MANUAL_VOUCHER_CREATE_UNKNOWN,
        MANUAL_VOUCHER_PAY_CLAIMED,
        MANUAL_VOUCHER_PAY_UNKNOWN,
        MANUAL_VOUCHER_SEND_CLAIMED,
        MANUAL_VOUCHER_SEND_UNKNOWN,
        MANUAL_VOUCHER_REFUND_CLAIMED,
        MANUAL_VOUCHER_REFUND_UNKNOWN,
        MANUAL_VOUCHER_AMBIGUOUS,
    }
)

# States in which a message may already be in a customer's hands. A refund is
# forbidden from every one of them, by plan, by claim and by CHECK constraint.
SENT_STATUSES: Final = frozenset(
    {
        MANUAL_VOUCHER_SEND_CLAIMED,
        MANUAL_VOUCHER_SEND_UNKNOWN,
        MANUAL_VOUCHER_SEND_REJECTED,
        MANUAL_VOUCHER_PROVIDER_ACCEPTED,
        MANUAL_VOUCHER_DELIVERED,
        MANUAL_VOUCHER_READ,
    }
)

CLAIM_GRANTED: Final = "claim_granted"
CLAIM_REFUSED_MISSING_ROW: Final = "claim_refused_missing_row"
CLAIM_REFUSED_STATE: Final = "claim_refused_state"
CLAIM_REFUSED_IDENTITY: Final = "claim_refused_identity"

RECORD_APPLIED: Final = "record_applied"
RECORD_MISSING_ROW: Final = "record_missing_row"
RECORD_STALE_STATE: Final = "record_stale_state"
RECORD_WOULD_REGRESS: Final = "record_would_regress"


@dataclass(frozen=True)
class ClaimOutcome:
    granted: bool
    reason: str
    status: str | None
    intent_uuid: str | None = None


@dataclass(frozen=True)
class RecordOutcome:
    applied: bool
    reason: str
    snapshot: LedgerSnapshot


@dataclass(frozen=True)
class LedgerSnapshot:
    """A PII-free view of the one row, safe to print and to sign into a digest."""

    exists: bool
    status: str | None = None
    reason_code: str | None = None
    baseline_version: str | None = None
    company_id: int | None = None
    campaign_code: str | None = None
    campaign_run_id: int | None = None
    campaign_recipient_id: int | None = None
    recipient_basis: str | None = None
    easyweek_customer_uuid: str | None = None
    campaign_period_start: str | None = None
    campaign_period_end: str | None = None
    location_uuid: str | None = None
    staffer_uuid: str | None = None
    payment_account_uuid: str | None = None
    voucher_template_uuid: str | None = None
    reconciliation_marker: str | None = None
    target_order_uuid: str | None = None
    outbound_intent_uuid: str | None = None
    provider_message_id_recorded: bool = False
    voucher_binding_recorded: bool = False
    hmac_key_id: str | None = None
    manual_cleanup_required: bool = False
    reconciliation_required: bool = False
    send_attempt_count: int = 0
    create_window_start: str | None = None
    create_window_end: str | None = None
    create_verified_at: str | None = None
    pay_verified_at: str | None = None
    live_guard_reproven_at: str | None = None
    send_attempted_at: str | None = None
    provider_accepted_at: str | None = None
    delivered_at: str | None = None
    read_at: str | None = None
    refund_verified_at: str | None = None
    evidence: dict[str, Any] | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        """Everything a report or a digest may see.

        The customer UUID, the order UUID and the intent UUID are deliberately
        absent: they are operational identifiers with real-world reach, and a
        stage digest does not need them to be unambiguous — the row ids and the
        marker already are.
        """
        return {
            "exists": self.exists,
            "status": self.status,
            "reason_code": self.reason_code,
            "baseline_version": self.baseline_version,
            "company_id": self.company_id,
            "campaign_code": self.campaign_code,
            "campaign_run_id": self.campaign_run_id,
            "campaign_recipient_id": self.campaign_recipient_id,
            "recipient_basis": self.recipient_basis,
            # A manual selection has no first visit to prove, and says so.
            "first_visit_proof": "not_applicable" if self.exists else None,
            "campaign_period_start": self.campaign_period_start,
            "campaign_period_end": self.campaign_period_end,
            "reconciliation_marker": self.reconciliation_marker,
            "target_order_recorded": self.target_order_uuid is not None,
            "provider_message_id_recorded": self.provider_message_id_recorded,
            "voucher_binding_recorded": self.voucher_binding_recorded,
            "hmac_key_id": self.hmac_key_id,
            "manual_cleanup_required": self.manual_cleanup_required,
            "reconciliation_required": self.reconciliation_required,
            "send_attempt_count": self.send_attempt_count,
            "create_verified_at": self.create_verified_at,
            "pay_verified_at": self.pay_verified_at,
            "live_guard_reproven_at": self.live_guard_reproven_at,
            "send_attempted_at": self.send_attempted_at,
            "provider_accepted_at": self.provider_accepted_at,
            "delivered_at": self.delivered_at,
            "read_at": self.read_at,
            "refund_verified_at": self.refund_verified_at,
            "evidence": dict(self.evidence or {}),
        }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _text(value: object) -> str | None:
    return str(value) if value is not None else None


def _snapshot(row: EasyWeekManualVoucherDeliveryLedger | None) -> LedgerSnapshot:
    if row is None:
        return LedgerSnapshot(exists=False)
    return LedgerSnapshot(
        exists=True,
        status=row.status,
        reason_code=row.reason_code,
        baseline_version=row.baseline_version,
        company_id=row.company_id,
        campaign_code=row.campaign_code,
        campaign_run_id=row.campaign_run_id,
        campaign_recipient_id=row.campaign_recipient_id,
        recipient_basis=row.recipient_basis,
        easyweek_customer_uuid=_text(row.easyweek_customer_uuid),
        campaign_period_start=_iso(row.campaign_period_start),
        campaign_period_end=_iso(row.campaign_period_end),
        location_uuid=_text(row.location_uuid),
        staffer_uuid=_text(row.staffer_uuid),
        payment_account_uuid=_text(row.payment_account_uuid),
        voucher_template_uuid=_text(row.voucher_template_uuid),
        reconciliation_marker=row.reconciliation_marker,
        target_order_uuid=_text(row.target_order_uuid),
        outbound_intent_uuid=_text(row.outbound_intent_uuid),
        provider_message_id_recorded=row.provider_message_id is not None,
        voucher_binding_recorded=row.voucher_code_hmac is not None,
        hmac_key_id=row.hmac_key_id,
        manual_cleanup_required=bool(row.manual_cleanup_required),
        reconciliation_required=bool(row.reconciliation_required),
        send_attempt_count=int(row.send_attempt_count or 0),
        create_window_start=_iso(row.create_window_start),
        create_window_end=_iso(row.create_window_end),
        create_verified_at=_iso(row.create_verified_at),
        pay_verified_at=_iso(row.pay_verified_at),
        live_guard_reproven_at=_iso(row.live_guard_reproven_at),
        send_attempted_at=_iso(row.send_attempted_at),
        provider_accepted_at=_iso(row.provider_accepted_at),
        delivered_at=_iso(row.delivered_at),
        read_at=_iso(row.read_at),
        refund_verified_at=_iso(row.refund_verified_at),
        evidence=dict(row.evidence or {}),
    )


async def _row(session: AsyncSession, *, for_update: bool = False) -> EasyWeekManualVoucherDeliveryLedger | None:
    statement = select(EasyWeekManualVoucherDeliveryLedger).where(
        EasyWeekManualVoucherDeliveryLedger.canary_scope == MANUAL_VOUCHER_SCOPE
    )
    if for_update:
        statement = statement.with_for_update()
    return (await session.execute(statement)).scalar_one_or_none()


async def load(session_maker: async_sessionmaker[AsyncSession]) -> LedgerSnapshot:
    """Read the one row without locking it. For status and plan only."""
    async with session_maker() as session:
        return _snapshot(await _row(session))


@dataclass(frozen=True)
class ManualCanaryIdentity:
    """The immutable identity one canary row is opened with."""

    company_id: int
    campaign_code: str
    campaign_run_id: int
    campaign_recipient_id: int
    easyweek_customer_uuid: str
    campaign_period_start: datetime
    campaign_period_end: datetime
    location_uuid: str
    staffer_uuid: str
    payment_account_uuid: str
    voucher_template_uuid: str
    reconciliation_marker: str
    baseline_version: str

    def matches(self, snapshot: LedgerSnapshot) -> bool:
        """Is the row this process is about to act on the row it planned for?

        Compared field by field rather than by a digest, so a mismatch cannot be
        mistaken for a drifted plan. Reported as a boolean; no value is echoed.

        The baseline version is part of it: a row planned against 42/42 may not
        be acted on by a process that has since been taught a different number.
        """
        if not snapshot.exists:
            return True
        return (
            snapshot.company_id == self.company_id
            and snapshot.campaign_code == self.campaign_code
            and snapshot.recipient_basis == RECIPIENT_BASIS_MANUAL
            and snapshot.campaign_run_id == self.campaign_run_id
            and snapshot.campaign_recipient_id == self.campaign_recipient_id
            and snapshot.easyweek_customer_uuid == self.easyweek_customer_uuid
            and snapshot.campaign_period_start == _iso(self.campaign_period_start)
            and snapshot.campaign_period_end == _iso(self.campaign_period_end)
            and snapshot.location_uuid == self.location_uuid
            and snapshot.staffer_uuid == self.staffer_uuid
            and snapshot.payment_account_uuid == self.payment_account_uuid
            and snapshot.voucher_template_uuid == self.voucher_template_uuid
            and snapshot.reconciliation_marker == self.reconciliation_marker
            and snapshot.baseline_version == self.baseline_version
        )


async def preview_is_locked_by_manual_canary(session: AsyncSession, *, campaign_run_id: int) -> bool:
    """Has a manual voucher canary attached itself to this preview run?

    Once it has, the snapshot stops being editable. Not tidiness: the canary
    addresses its recipient by run id and recipient id and re-proves that pair
    live before every external step. An operator who edits or discards the
    preview after CREATE or PAY does not undo anything — they make the DELIVER
    and the REFUND unprovable, leaving a real €15 order with no way to finish it
    and no way to take it back.

    Takes the caller's session so the check happens under the same lock as the
    edit it guards.
    """
    found = await session.scalar(
        select(EasyWeekManualVoucherDeliveryLedger.id)
        .where(EasyWeekManualVoucherDeliveryLedger.campaign_run_id == campaign_run_id)
        .where(EasyWeekManualVoucherDeliveryLedger.provider == PROVIDER_EASYWEEK)
        .limit(1)
    )
    return found is not None


async def open_canary(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: ManualCanaryIdentity,
) -> LedgerSnapshot:
    """Create the one ``planned`` row, or return the one that already exists.

    Not a claim and not a mutation: it records which recipient this canary is
    for, so the entitlement uniqueness rule starts protecting the person before
    any money moves. A second recipient — or the same person through a fresh
    preview of the same campaign period — collides with a database constraint
    rather than with a check somebody could forget to write.

    Atomic with the freeze it causes
    --------------------------------
    Writing this row is what stops the preview being edited, so the run and the
    recipient are re-proven HERE, under the very row lock the editor takes, in
    the same transaction that inserts. Without that, an operator's Remove and
    this insert can both pass their checks against a world that no longer
    exists, and the canary opens on a recipient somebody just removed.

    The lock order is the editor's, deliberately: ``CampaignRun`` FOR UPDATE
    first, then everything else. Two paths that take the same locks in the same
    order queue; two that disagree deadlock.

    Returns a snapshot with ``exists=False`` when the run or the recipient no
    longer qualifies — the caller must treat that as a refusal, not as an empty
    ledger.
    """
    async with session_maker() as session:
        async with session.begin():
            # 1. The editor's lock, taken first and held for the transaction.
            run = await session.get(CampaignRun, identity.campaign_run_id, with_for_update=True)
            if (
                run is None
                or run.provider != PROVIDER_EASYWEEK
                or run.mode != "preview"
                or run.status != "completed"
                or run.campaign_code != identity.campaign_code
                or identity.company_id not in (run.company_ids or [])
            ):
                return _snapshot(None)

            # 2. Under that lock, the recipient as it is NOW. A Remove that won
            # the race has already set `status='skipped'`, and this is where
            # that becomes visible rather than in a check taken seconds ago.
            existing = await _row(session, for_update=True)
            if existing is None:
                recipient = await session.get(CampaignRecipient, identity.campaign_recipient_id)
                if (
                    recipient is None
                    or recipient.provider != PROVIDER_EASYWEEK
                    or recipient.campaign_run_id != run.id
                    or recipient.company_id != identity.company_id
                    or recipient.status != "candidate"
                    or recipient.is_opted_out
                    or (recipient.recipient_basis or "") != RECIPIENT_BASIS_MANUAL
                    or _text(recipient.easyweek_customer_uuid) != identity.easyweek_customer_uuid
                ):
                    return _snapshot(None)

            if existing is not None:
                return _snapshot(existing)
            now = utcnow()
            row = EasyWeekManualVoucherDeliveryLedger(
                canary_scope=MANUAL_VOUCHER_SCOPE,
                request_schema_version=MANUAL_VOUCHER_SCHEMA_VERSION,
                baseline_version=identity.baseline_version,
                provider=PROVIDER_EASYWEEK,
                company_id=identity.company_id,
                campaign_code=identity.campaign_code,
                recipient_basis=RECIPIENT_BASIS_MANUAL,
                campaign_run_id=identity.campaign_run_id,
                campaign_recipient_id=identity.campaign_recipient_id,
                easyweek_customer_uuid=uuid_module.UUID(identity.easyweek_customer_uuid),
                campaign_period_start=identity.campaign_period_start,
                campaign_period_end=identity.campaign_period_end,
                location_uuid=uuid_module.UUID(identity.location_uuid),
                staffer_uuid=uuid_module.UUID(identity.staffer_uuid),
                payment_account_uuid=uuid_module.UUID(identity.payment_account_uuid),
                voucher_template_uuid=uuid_module.UUID(identity.voucher_template_uuid),
                reconciliation_marker=identity.reconciliation_marker,
                status=MANUAL_VOUCHER_PLANNED,
                evidence={},
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            await session.flush()
            return _snapshot(row)


async def _claim(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: ManualCanaryIdentity,
    claimable_from: frozenset[str],
    next_status: str,
    plan_digest: str,
    digest_field: str,
    claimed_field: str,
    attempted_field: str,
    extra: dict[str, Any] | None = None,
) -> ClaimOutcome:
    """Lock the row, check the transition and the identity, stamp, commit.

    In that order, and all of it before any caller may touch the network. The
    identity is re-checked here and not only in the plan, because the plan was
    built before the lock existed.
    """
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row(session, for_update=True)
            if row is None:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_MISSING_ROW, status=None)
            if not identity.matches(_snapshot(row)):
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_IDENTITY, status=row.status)
            if row.status not in claimable_from:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_STATE, status=row.status)

            setattr(row, digest_field, plan_digest)
            setattr(row, claimed_field, now)
            # Claimed AND attempted together, in one committed transaction:
            # after this commit a crash before the socket and a crash after the
            # response are indistinguishable, so both must read as "it may have
            # gone out".
            setattr(row, attempted_field, now)
            row.status = next_status
            row.reason_code = None
            row.reconciliation_required = True
            for name, value in (extra or {}).items():
                setattr(row, name, value)
            row.updated_at = now
            await session.flush()
            return ClaimOutcome(granted=True, reason=CLAIM_GRANTED, status=next_status)


async def claim_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: ManualCanaryIdentity,
    plan_digest: str,
    create_window_start: datetime,
    create_window_end: datetime,
) -> ClaimOutcome:
    """Reserve the right to send the ONE create POST."""
    return await _claim(
        session_maker,
        identity=identity,
        claimable_from=CREATE_CLAIMABLE_FROM,
        next_status=MANUAL_VOUCHER_CREATE_CLAIMED,
        plan_digest=plan_digest,
        digest_field="create_plan_digest",
        claimed_field="create_claimed_at",
        attempted_field="create_attempted_at",
        extra={
            "create_window_start": create_window_start,
            "create_window_end": create_window_end,
            # From this committed claim onwards an open draft may exist in the
            # POS. Cleared only by a proven payment, a proven refund, or a
            # proven manual closure — never by a search that found nothing.
            "manual_cleanup_required": True,
        },
    )


async def claim_pay(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: ManualCanaryIdentity,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the right to send the ONE payment POST."""
    return await _claim(
        session_maker,
        identity=identity,
        claimable_from=PAY_CLAIMABLE_FROM,
        next_status=MANUAL_VOUCHER_PAY_CLAIMED,
        plan_digest=plan_digest,
        digest_field="pay_plan_digest",
        claimed_field="pay_claimed_at",
        attempted_field="pay_attempted_at",
    )


async def claim_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: ManualCanaryIdentity,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the right to send the ONE refund POST.

    Pre-send only. The claimable states exclude every state in which a message
    may have reached the customer, and a CHECK constraint says the same thing
    again: returning the money for a code somebody is already holding is worse
    than losing the €15.
    """
    return await _claim(
        session_maker,
        identity=identity,
        claimable_from=REFUND_CLAIMABLE_FROM,
        next_status=MANUAL_VOUCHER_REFUND_CLAIMED,
        plan_digest=plan_digest,
        digest_field="refund_plan_digest",
        claimed_field="refund_claimed_at",
        attempted_field="refund_attempted_at",
    )


async def claim_send(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: ManualCanaryIdentity,
    plan_digest: str,
    live_guard_reproven_at: datetime,
    template_code: str,
    meta_template_name: str,
    template_language: str,
    sender_id: int | None,
) -> ClaimOutcome:
    """Reserve the ONE delivery, and write its redacted intent, before sending.

    Three things happen in one committed transaction: the row moves to
    ``send_claimed``, the attempt counter goes to one, and an audit row is
    written naming which approved template was used. None of them contains the
    message, the parameters or the code — there is deliberately nothing here
    from which a later process could re-render and re-send anything.
    """
    now = utcnow()
    intent = uuid_module.uuid4()
    async with session_maker() as session:
        async with session.begin():
            row = await _row(session, for_update=True)
            if row is None:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_MISSING_ROW, status=None)
            if not identity.matches(_snapshot(row)):
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_IDENTITY, status=row.status)
            if row.status not in SEND_CLAIMABLE_FROM or int(row.send_attempt_count or 0) != 0:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_STATE, status=row.status)

            row.deliver_plan_digest = plan_digest
            row.live_guard_reproven_at = live_guard_reproven_at
            row.outbound_intent_uuid = intent
            row.send_claimed_at = now
            row.send_attempted_at = now
            row.send_attempt_count = 1
            row.status = MANUAL_VOUCHER_SEND_CLAIMED
            row.reason_code = None
            row.reconciliation_required = True
            row.updated_at = now
            session.add(
                EasyWeekManualVoucherDeliveryAttempt(
                    ledger_id=row.id,
                    intent_uuid=intent,
                    template_code=template_code,
                    meta_template_name=meta_template_name,
                    template_language=template_language,
                    sender_id=sender_id,
                    campaign_recipient_id=row.campaign_recipient_id,
                    outcome="claimed",
                    claimed_at=now,
                )
            )
            await session.flush()
            return ClaimOutcome(
                granted=True,
                reason=CLAIM_GRANTED,
                status=MANUAL_VOUCHER_SEND_CLAIMED,
                intent_uuid=str(intent),
            )


async def record_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    status: str,
    expected_statuses: frozenset[str],
    reason_code: str | None = None,
    target_order_uuid: str | None = None,
    provider_message_id: str | None = None,
    voucher_code_hmac: str | None = None,
    hmac_key_id: str | None = None,
    verified_field: str | None = None,
    evidence: dict[str, Any] | None = None,
    manual_cleanup_required: bool | None = None,
    reconciliation_required: bool | None = None,
    manual_cleanup_observed: bool = False,
    attempt_outcome: str | None = None,
) -> RecordOutcome:
    """Write what a stage turned out to be — as a compare-and-set.

    ``expected_statuses`` names every state this write is valid FROM. The row is
    locked, compared, and only then touched, so a caller whose view of the world
    is stale is told so and gets the current snapshot back instead of
    overwriting somebody else's newer state.

    On top of that a write that would LOWER the rank is refused outright. That
    is what stops a slow original response, or a reconciliation that read a
    moment too early, from walking a claimed stage back to somewhere the same
    request could be claimed again.

    Timestamps are only ever set here, never cleared: an ``attempted_at`` is a
    fact about something that left this process.
    """
    if status not in MANUAL_VOUCHER_STATUSES:
        raise ValueError("unknown manual voucher status")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row(session, for_update=True)
            if row is None:
                return RecordOutcome(applied=False, reason=RECORD_MISSING_ROW, snapshot=_snapshot(None))
            if row.status not in expected_statuses:
                return RecordOutcome(applied=False, reason=RECORD_STALE_STATE, snapshot=_snapshot(row))
            if STATUS_RANK[status] < STATUS_RANK[row.status]:
                return RecordOutcome(applied=False, reason=RECORD_WOULD_REGRESS, snapshot=_snapshot(row))

            row.status = status
            row.reason_code = reason_code
            if target_order_uuid is not None:
                row.target_order_uuid = uuid_module.UUID(target_order_uuid)
            if provider_message_id is not None:
                row.provider_message_id = provider_message_id
            if voucher_code_hmac is not None and hmac_key_id is not None:
                row.voucher_code_hmac = voucher_code_hmac
                row.hmac_key_id = hmac_key_id
            if verified_field is not None:
                setattr(row, verified_field, now)
            if manual_cleanup_required is not None:
                row.manual_cleanup_required = manual_cleanup_required
            if reconciliation_required is not None:
                row.reconciliation_required = reconciliation_required
            if manual_cleanup_observed:
                row.manual_cleanup_observed_at = now
            if evidence:
                merged = dict(row.evidence or {})
                merged.update(evidence)
                row.evidence = merged
            if attempt_outcome is not None and row.outbound_intent_uuid is not None:
                attempt = (
                    await session.execute(
                        select(EasyWeekManualVoucherDeliveryAttempt).where(
                            EasyWeekManualVoucherDeliveryAttempt.intent_uuid == row.outbound_intent_uuid
                        )
                    )
                ).scalar_one_or_none()
                if attempt is not None:
                    attempt.outcome = attempt_outcome
                    attempt.reason_code = reason_code
                    attempt.provider_message_id = provider_message_id
                    attempt.completed_at = now
            row.updated_at = now
            await session.flush()
            return RecordOutcome(applied=True, reason=RECORD_APPLIED, snapshot=_snapshot(row))


async def binding_matches(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    voucher_code: str,
    target_order_uuid: str,
) -> bool:
    """Is this code the one the stored binding was made from?

    Lives here rather than in the runner because the MAC must not travel: the
    snapshot a report is built from carries only "a binding exists", and moving
    the digest itself into a caller would put verification material into
    something printable.

    False — never an exception — for a missing binding, a rotated key or a
    mismatch, so every caller fails closed on the same branch.
    """
    async with session_maker() as session:
        row = await _row(session)
        if row is None or row.voucher_code_hmac is None or row.hmac_key_id is None:
            return False
        if _text(row.target_order_uuid) != target_order_uuid:
            return False
        return voucher_code_matches(
            voucher_code=voucher_code,
            expected_mac=row.voucher_code_hmac,
            expected_key_id=row.hmac_key_id,
            ledger_uuid=MANUAL_VOUCHER_SCOPE,
            target_order_uuid=target_order_uuid,
            voucher_template_uuid=_text(row.voucher_template_uuid) or "",
            domain=MANUAL_VOUCHER_DOMAIN,
        )


async def apply_webhook_transition(
    session: AsyncSession,
    *,
    provider_message_id: str,
    status: str,
) -> RecordOutcome:
    """The webhook transition, inside a transaction the CALLER owns.

    Used by the WhatsApp status worker, which already holds a session for the
    whole callback batch. Opening a second one there would mean two connections
    racing over the same rows in one logical unit of work — and the row this
    canary owns has no ``OutboxMessage`` behind it, so this is the only place
    its delivered and read can ever be observed.
    """
    if status not in (MANUAL_VOUCHER_DELIVERED, MANUAL_VOUCHER_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    # `no_autoflush` matters rather than being defensive: this runs inside
    # somebody else's transaction, and an ordinary query would flush whatever
    # they have pending at a moment they did not choose.
    with session.no_autoflush:
        row = await _row_by_message(session, provider_message_id)
        if row is None:
            # Not ours, and nothing to write. Leave the caller's unit of work
            # exactly as it was found.
            return RecordOutcome(applied=False, reason=RECORD_MISSING_ROW, snapshot=_snapshot(None))
    return await _apply_webhook_row(session, row, provider_message_id=provider_message_id, status=status, now=now)


async def _row_by_message(
    session: AsyncSession, provider_message_id: str
) -> EasyWeekManualVoucherDeliveryLedger | None:
    """The one row Meta's identifier names, locked. Matched on nothing else."""
    if not provider_message_id:
        return None
    return (
        await session.execute(
            select(EasyWeekManualVoucherDeliveryLedger)
            .where(EasyWeekManualVoucherDeliveryLedger.provider_message_id == provider_message_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _apply_webhook_row(
    session: AsyncSession,
    row: EasyWeekManualVoucherDeliveryLedger | None,
    *,
    provider_message_id: str,
    status: str,
    now: datetime,
) -> RecordOutcome:
    if row is None or not row.provider_message_id:
        return RecordOutcome(applied=False, reason=RECORD_MISSING_ROW, snapshot=_snapshot(row))
    if row.provider_message_id != provider_message_id:
        return RecordOutcome(applied=False, reason=RECORD_STALE_STATE, snapshot=_snapshot(row))
    if STATUS_RANK[status] < STATUS_RANK[row.status]:
        # A duplicate, or a callback that arrived out of order. Acceptance is
        # what Meta said and a later webhook cannot unsay it.
        return RecordOutcome(applied=False, reason=RECORD_WOULD_REGRESS, snapshot=_snapshot(row))
    if row.provider_accepted_at is None:
        # A callback can land beside the commit that recorded acceptance. The
        # identifier only exists because Meta answered with it, so the callback
        # itself is the proof — and refusing here would silently lose a status.
        row.provider_accepted_at = now
    if row.delivered_at is None:
        # Read implies delivered. Recording read without it would leave a row
        # the database itself refuses, so this same callback stamps both.
        row.delivered_at = now
    if status == MANUAL_VOUCHER_READ and row.read_at is None:
        row.read_at = now
    row.status = status
    row.reconciliation_required = False
    # A delivered or read message means the voucher reached its person: there is
    # no open draft left for anybody to close by hand. Forced rather than left
    # alone, because the flag was set back when the order was still a draft.
    row.manual_cleanup_required = False
    row.updated_at = now
    await session.flush()
    return RecordOutcome(applied=True, reason=RECORD_APPLIED, snapshot=_snapshot(row))


async def record_webhook_transition(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    provider_message_id: str,
    status: str,
) -> RecordOutcome:
    """Apply a delivered/read webhook to the one message it names.

    The session-owning variant, for callers that hold no transaction of their
    own. Same rules: matched on the exact provider message id, monotonic by
    rank, and idempotent for a duplicate.
    """
    if status not in (MANUAL_VOUCHER_DELIVERED, MANUAL_VOUCHER_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row_by_message(session, provider_message_id)
            return await _apply_webhook_row(
                session, row, provider_message_id=provider_message_id, status=status, now=now
            )


__all__ = [
    "CREATE_CLAIMABLE_FROM",
    "apply_webhook_transition",
    "PAY_CLAIMABLE_FROM",
    "REFUND_CLAIMABLE_FROM",
    "SEND_CLAIMABLE_FROM",
    "SENT_STATUSES",
    "STATUS_RANK",
    "UNRESOLVED_STATUSES",
    "ClaimOutcome",
    "LedgerSnapshot",
    "ManualCanaryIdentity",
    "RecordOutcome",
    "claim_create",
    "claim_pay",
    "claim_refund",
    "binding_matches",
    "claim_send",
    "load",
    "open_canary",
    "preview_is_locked_by_manual_canary",
    "record_outcome",
    "record_webhook_transition",
]
