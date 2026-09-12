"""The durable claim ledger of the voucher delivery canary (§36).

Every external step here is irreversible by repetition, and each can end with
the request sent and the answer lost. EasyWeek publishes no write idempotency
key, so a second attempt is a coin flip on whether a second order, a second
payment or a second customer message happens. This module owns the rule that
makes that survivable:

    the claim is committed BEFORE the request leaves.

A crash anywhere after that commit — before the socket, mid-flight, after the
response — reads identically afterwards: *claimed, outcome unknown*. That is the
only reading that cannot charge a card twice or message a person twice, and it
is why these functions open and commit their own transactions rather than
joining a caller's.

Monotonic, and defended against a stale writer
----------------------------------------------
Every write names the states it is valid FROM; the row is locked with
``SELECT ... FOR UPDATE`` and compared before it is touched. Every status also
carries a rank, and a write that would lower it is refused. Together they mean a
reconciliation that read the world a moment too early cannot walk a stage back
into a state the same request could be claimed from again.

The send is different from the mutations
----------------------------------------
A create or a pay may be re-claimed after a rejection this transport proved did
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

from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    VOUCHER_DELIVERY_SCHEMA_VERSION,
    VOUCHER_DELIVERY_SCOPE,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    VOUCHER_DELIVERY_AMBIGUOUS,
    VOUCHER_DELIVERY_CREATE_CLAIMED,
    VOUCHER_DELIVERY_CREATE_REJECTED,
    VOUCHER_DELIVERY_CREATE_UNKNOWN,
    VOUCHER_DELIVERY_CREATED,
    VOUCHER_DELIVERY_DELIVERED,
    VOUCHER_DELIVERY_MANUALLY_CLEANED,
    VOUCHER_DELIVERY_PAID,
    VOUCHER_DELIVERY_PAY_CLAIMED,
    VOUCHER_DELIVERY_PAY_REJECTED,
    VOUCHER_DELIVERY_PAY_UNKNOWN,
    VOUCHER_DELIVERY_PLANNED,
    VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
    VOUCHER_DELIVERY_READ,
    VOUCHER_DELIVERY_REFUND_CLAIMED,
    VOUCHER_DELIVERY_REFUND_REJECTED,
    VOUCHER_DELIVERY_REFUND_UNKNOWN,
    VOUCHER_DELIVERY_REFUNDED,
    VOUCHER_DELIVERY_SEND_CLAIMED,
    VOUCHER_DELIVERY_SEND_REJECTED,
    VOUCHER_DELIVERY_SEND_UNKNOWN,
    VOUCHER_DELIVERY_STATUSES,
    EasyWeekCampaignVoucherDeliveryAttempt,
    EasyWeekCampaignVoucherDeliveryLedger,
)
from altegio_bot.utils import utcnow

# How far along the canary a status sits. A write that would lower this rank is
# a regression and is refused, whoever sent it and however late it arrives.
# `ambiguous` sits at the top because it is a full stop.
STATUS_RANK: Final[dict[str, int]] = {
    VOUCHER_DELIVERY_PLANNED: 5,
    VOUCHER_DELIVERY_CREATE_CLAIMED: 10,
    VOUCHER_DELIVERY_CREATE_UNKNOWN: 11,
    VOUCHER_DELIVERY_CREATE_REJECTED: 12,
    VOUCHER_DELIVERY_CREATED: 20,
    VOUCHER_DELIVERY_PAY_CLAIMED: 30,
    VOUCHER_DELIVERY_PAY_UNKNOWN: 31,
    VOUCHER_DELIVERY_PAY_REJECTED: 32,
    VOUCHER_DELIVERY_PAID: 40,
    VOUCHER_DELIVERY_SEND_CLAIMED: 50,
    VOUCHER_DELIVERY_SEND_REJECTED: 51,
    VOUCHER_DELIVERY_SEND_UNKNOWN: 52,
    VOUCHER_DELIVERY_PROVIDER_ACCEPTED: 60,
    VOUCHER_DELIVERY_DELIVERED: 70,
    VOUCHER_DELIVERY_READ: 80,
    VOUCHER_DELIVERY_REFUND_CLAIMED: 41,
    VOUCHER_DELIVERY_REFUND_UNKNOWN: 42,
    VOUCHER_DELIVERY_REFUND_REJECTED: 43,
    VOUCHER_DELIVERY_REFUNDED: 45,
    VOUCHER_DELIVERY_MANUALLY_CLEANED: 90,
    VOUCHER_DELIVERY_AMBIGUOUS: 99,
}

# A stage may be claimed only from these states. `*_rejected` is re-claimable
# for the two EasyWeek mutations, because a proven validation refusal did not
# act. The SEND has no such entry: one attempt, ever.
CREATE_CLAIMABLE_FROM: Final = frozenset({VOUCHER_DELIVERY_PLANNED, VOUCHER_DELIVERY_CREATE_REJECTED})
PAY_CLAIMABLE_FROM: Final = frozenset({VOUCHER_DELIVERY_CREATED, VOUCHER_DELIVERY_PAY_REJECTED})
SEND_CLAIMABLE_FROM: Final = frozenset({VOUCHER_DELIVERY_PAID})
REFUND_CLAIMABLE_FROM: Final = frozenset({VOUCHER_DELIVERY_PAID, VOUCHER_DELIVERY_REFUND_REJECTED})

# States in which something may still have reached EasyWeek or Meta.
UNRESOLVED_STATUSES: Final = frozenset(
    {
        VOUCHER_DELIVERY_CREATE_CLAIMED,
        VOUCHER_DELIVERY_CREATE_UNKNOWN,
        VOUCHER_DELIVERY_PAY_CLAIMED,
        VOUCHER_DELIVERY_PAY_UNKNOWN,
        VOUCHER_DELIVERY_SEND_CLAIMED,
        VOUCHER_DELIVERY_SEND_UNKNOWN,
        VOUCHER_DELIVERY_REFUND_CLAIMED,
        VOUCHER_DELIVERY_REFUND_UNKNOWN,
    }
)

# Once any of these is true a customer may be holding the code, so the money
# stays where it is and the decision becomes a human one.
SEND_TOUCHED_STATUSES: Final = frozenset(
    {
        VOUCHER_DELIVERY_SEND_CLAIMED,
        VOUCHER_DELIVERY_SEND_UNKNOWN,
        VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
        VOUCHER_DELIVERY_DELIVERED,
        VOUCHER_DELIVERY_READ,
    }
)

CLAIM_GRANTED: Final = "granted"
CLAIM_REFUSED_STATE: Final = "refused_state"
CLAIM_REFUSED_IDENTITY: Final = "refused_identity_drift"
CLAIM_REFUSED_MISSING_ROW: Final = "refused_missing_row"

RECORD_APPLIED: Final = "applied"
RECORD_STALE_STATE: Final = "stale_state"
RECORD_WOULD_REGRESS: Final = "would_regress"
RECORD_MISSING_ROW: Final = "missing_row"


@dataclass(frozen=True)
class ClaimOutcome:
    """Whether this process now owns the right to make one external request."""

    granted: bool
    reason: str
    status: str | None
    intent_uuid: str | None = None


@dataclass(frozen=True)
class RecordOutcome:
    applied: bool
    reason: str
    snapshot: "LedgerSnapshot"


@dataclass(frozen=True)
class LedgerSnapshot:
    """A PII-free view of the row, safe to print in an operator report."""

    exists: bool
    status: str | None
    reason_code: str | None
    campaign_run_id: int | None
    campaign_recipient_id: int | None
    company_id: int | None
    target_order_uuid: str | None
    reconciliation_marker: str | None
    create_window_start: datetime | None
    create_window_end: datetime | None
    # Held for comparison, reported only as booleans.
    source_booking_uuid: str | None
    easyweek_customer_uuid: str | None
    location_uuid: str | None
    staffer_uuid: str | None
    payment_account_uuid: str | None
    voucher_template_uuid: str | None
    voucher_code_hmac: str | None
    hmac_key_id: str | None
    outbound_intent_uuid: str | None
    provider_message_id: str | None
    send_attempt_count: int
    stage_plan_digests: dict[str, str | None]
    stage_timestamps: dict[str, str | None]
    manual_cleanup_required: bool
    reconciliation_required: bool
    evidence: dict[str, Any]
    row_id: int | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ledger_row_exists": self.exists,
            "status": self.status,
            "reason_code": self.reason_code,
            "campaign_run_id": self.campaign_run_id,
            "campaign_recipient_id": self.campaign_recipient_id,
            "company_id": self.company_id,
            # Operationally necessary but never printed: a payment, a refund and
            # a manual dashboard cleanup are impossible without it.
            "target_order_uuid_known": self.target_order_uuid is not None,
            "reconciliation_marker": self.reconciliation_marker,
            # The MAC is verification material in its own right, so the report
            # says only that a binding exists and which key made it.
            "voucher_binding_recorded": self.voucher_code_hmac is not None,
            "hmac_key_id": self.hmac_key_id,
            "outbound_intent_recorded": self.outbound_intent_uuid is not None,
            "provider_message_recorded": self.provider_message_id is not None,
            "send_attempt_count": self.send_attempt_count,
            "stage_plan_digests": dict(self.stage_plan_digests),
            "stage_timestamps": dict(self.stage_timestamps),
            "manual_cleanup_required": self.manual_cleanup_required,
            "reconciliation_required": self.reconciliation_required,
            "evidence": dict(self.evidence),
        }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _text(value: object) -> str | None:
    return str(value) if value is not None else None


def _snapshot(row: EasyWeekCampaignVoucherDeliveryLedger | None) -> LedgerSnapshot:
    if row is None:
        return LedgerSnapshot(
            exists=False,
            status=None,
            reason_code=None,
            campaign_run_id=None,
            campaign_recipient_id=None,
            company_id=None,
            target_order_uuid=None,
            reconciliation_marker=None,
            create_window_start=None,
            create_window_end=None,
            source_booking_uuid=None,
            easyweek_customer_uuid=None,
            location_uuid=None,
            staffer_uuid=None,
            payment_account_uuid=None,
            voucher_template_uuid=None,
            voucher_code_hmac=None,
            hmac_key_id=None,
            outbound_intent_uuid=None,
            provider_message_id=None,
            send_attempt_count=0,
            stage_plan_digests={},
            stage_timestamps={},
            manual_cleanup_required=False,
            reconciliation_required=False,
            evidence={},
        )
    return LedgerSnapshot(
        exists=True,
        status=row.status,
        reason_code=row.reason_code,
        campaign_run_id=row.campaign_run_id,
        campaign_recipient_id=row.campaign_recipient_id,
        company_id=row.company_id,
        target_order_uuid=_text(row.target_order_uuid),
        reconciliation_marker=row.reconciliation_marker,
        create_window_start=row.create_window_start,
        create_window_end=row.create_window_end,
        source_booking_uuid=_text(row.source_booking_uuid),
        easyweek_customer_uuid=_text(row.easyweek_customer_uuid),
        location_uuid=_text(row.location_uuid),
        staffer_uuid=_text(row.staffer_uuid),
        payment_account_uuid=_text(row.payment_account_uuid),
        voucher_template_uuid=_text(row.voucher_template_uuid),
        voucher_code_hmac=row.voucher_code_hmac,
        hmac_key_id=row.hmac_key_id,
        outbound_intent_uuid=_text(row.outbound_intent_uuid),
        provider_message_id=row.provider_message_id,
        send_attempt_count=int(row.send_attempt_count or 0),
        stage_plan_digests={
            "create": row.create_plan_digest,
            "pay": row.pay_plan_digest,
            "deliver": row.deliver_plan_digest,
            "refund": row.refund_plan_digest,
        },
        stage_timestamps={
            "create_claimed_at": _iso(row.create_claimed_at),
            "create_attempted_at": _iso(row.create_attempted_at),
            "create_verified_at": _iso(row.create_verified_at),
            "pay_claimed_at": _iso(row.pay_claimed_at),
            "pay_attempted_at": _iso(row.pay_attempted_at),
            "pay_verified_at": _iso(row.pay_verified_at),
            "live_guard_reproven_at": _iso(row.live_guard_reproven_at),
            "send_claimed_at": _iso(row.send_claimed_at),
            "send_attempted_at": _iso(row.send_attempted_at),
            "provider_accepted_at": _iso(row.provider_accepted_at),
            "delivered_at": _iso(row.delivered_at),
            "read_at": _iso(row.read_at),
            "refund_claimed_at": _iso(row.refund_claimed_at),
            "refund_attempted_at": _iso(row.refund_attempted_at),
            "refund_verified_at": _iso(row.refund_verified_at),
            "manual_cleanup_observed_at": _iso(row.manual_cleanup_observed_at),
        },
        manual_cleanup_required=bool(row.manual_cleanup_required),
        reconciliation_required=bool(row.reconciliation_required),
        evidence=dict(row.evidence or {}),
        row_id=row.id,
    )


async def _row(session: AsyncSession, *, for_update: bool = False) -> EasyWeekCampaignVoucherDeliveryLedger | None:
    stmt = select(EasyWeekCampaignVoucherDeliveryLedger).where(
        EasyWeekCampaignVoucherDeliveryLedger.canary_scope == VOUCHER_DELIVERY_SCOPE
    )
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def load(session_maker: async_sessionmaker[AsyncSession]) -> LedgerSnapshot:
    """Read-only. Safe at any moment, including while a stage is unresolved."""
    async with session_maker() as session:
        return _snapshot(await _row(session))


@dataclass(frozen=True)
class CanaryIdentity:
    """The immutable identity one canary row is opened with."""

    company_id: int
    campaign_code: str
    campaign_run_id: int
    campaign_recipient_id: int
    source_booking_uuid: str
    easyweek_customer_uuid: str
    location_uuid: str
    staffer_uuid: str
    payment_account_uuid: str
    voucher_template_uuid: str
    reconciliation_marker: str

    def matches(self, snapshot: LedgerSnapshot) -> bool:
        """Is the row this process is about to act on the row it planned for?

        Compared field by field rather than by a digest so a mismatch cannot be
        mistaken for a drifted plan. Reported as a boolean; no value is echoed.
        """
        if not snapshot.exists:
            return True
        return (
            snapshot.company_id == self.company_id
            and snapshot.campaign_run_id == self.campaign_run_id
            and snapshot.campaign_recipient_id == self.campaign_recipient_id
            and snapshot.source_booking_uuid == self.source_booking_uuid
            and snapshot.easyweek_customer_uuid == self.easyweek_customer_uuid
            and snapshot.location_uuid == self.location_uuid
            and snapshot.staffer_uuid == self.staffer_uuid
            and snapshot.payment_account_uuid == self.payment_account_uuid
            and snapshot.voucher_template_uuid == self.voucher_template_uuid
            and snapshot.reconciliation_marker == self.reconciliation_marker
        )


async def open_canary(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: CanaryIdentity,
) -> LedgerSnapshot:
    """Create the one ``planned`` row, or return the one that already exists.

    Not a claim and not a mutation: it only records which recipient this canary
    is for, so that the entitlement uniqueness rule starts protecting the person
    before any money moves. A second recipient — or the same person through a
    fresh preview run — collides with a database constraint rather than with a
    check somebody could forget to write.
    """
    async with session_maker() as session:
        async with session.begin():
            existing = await _row(session, for_update=True)
            if existing is not None:
                return _snapshot(existing)
            now = utcnow()
            row = EasyWeekCampaignVoucherDeliveryLedger(
                canary_scope=VOUCHER_DELIVERY_SCOPE,
                request_schema_version=VOUCHER_DELIVERY_SCHEMA_VERSION,
                provider=PROVIDER_EASYWEEK,
                company_id=identity.company_id,
                campaign_code=identity.campaign_code,
                campaign_run_id=identity.campaign_run_id,
                campaign_recipient_id=identity.campaign_recipient_id,
                source_booking_uuid=uuid_module.UUID(identity.source_booking_uuid),
                easyweek_customer_uuid=uuid_module.UUID(identity.easyweek_customer_uuid),
                location_uuid=uuid_module.UUID(identity.location_uuid),
                staffer_uuid=uuid_module.UUID(identity.staffer_uuid),
                payment_account_uuid=uuid_module.UUID(identity.payment_account_uuid),
                voucher_template_uuid=uuid_module.UUID(identity.voucher_template_uuid),
                reconciliation_marker=identity.reconciliation_marker,
                status=VOUCHER_DELIVERY_PLANNED,
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
    identity: CanaryIdentity,
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
            # Claimed AND attempted together, in one committed transaction: after
            # this commit a crash before the socket and a crash after the
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
            return ClaimOutcome(
                granted=True,
                reason=CLAIM_GRANTED,
                status=next_status,
                intent_uuid=_text(row.outbound_intent_uuid),
            )


async def claim_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: CanaryIdentity,
    plan_digest: str,
    create_window_start: datetime,
    create_window_end: datetime,
) -> ClaimOutcome:
    """Reserve the right to send the ONE create POST."""
    return await _claim(
        session_maker,
        identity=identity,
        claimable_from=CREATE_CLAIMABLE_FROM,
        next_status=VOUCHER_DELIVERY_CREATE_CLAIMED,
        plan_digest=plan_digest,
        digest_field="create_plan_digest",
        claimed_field="create_claimed_at",
        attempted_field="create_attempted_at",
        extra={"create_window_start": create_window_start, "create_window_end": create_window_end},
    )


async def claim_pay(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: CanaryIdentity,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the ONE payment. Only a proven created order may be paid for."""
    return await _claim(
        session_maker,
        identity=identity,
        claimable_from=PAY_CLAIMABLE_FROM,
        next_status=VOUCHER_DELIVERY_PAY_CLAIMED,
        plan_digest=plan_digest,
        digest_field="pay_plan_digest",
        claimed_field="pay_claimed_at",
        attempted_field="pay_attempted_at",
    )


async def claim_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: CanaryIdentity,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the ONE refund — and only while nothing has been sent.

    The database enforces the same rule in a CHECK constraint, so a future
    caller that forgot it fails on the write rather than on a refund a customer
    has already been told about.
    """
    return await _claim(
        session_maker,
        identity=identity,
        claimable_from=REFUND_CLAIMABLE_FROM,
        next_status=VOUCHER_DELIVERY_REFUND_CLAIMED,
        plan_digest=plan_digest,
        digest_field="refund_plan_digest",
        claimed_field="refund_claimed_at",
        attempted_field="refund_attempted_at",
    )


async def claim_send(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: CanaryIdentity,
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
            row.status = VOUCHER_DELIVERY_SEND_CLAIMED
            row.reason_code = None
            row.reconciliation_required = True
            row.updated_at = now
            session.add(
                EasyWeekCampaignVoucherDeliveryAttempt(
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
                status=VOUCHER_DELIVERY_SEND_CLAIMED,
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
    if status not in VOUCHER_DELIVERY_STATUSES:
        raise ValueError("unknown voucher delivery status")
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
                        select(EasyWeekCampaignVoucherDeliveryAttempt).where(
                            EasyWeekCampaignVoucherDeliveryAttempt.intent_uuid == row.outbound_intent_uuid
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


async def apply_webhook_transition(
    session: AsyncSession,
    *,
    provider_message_id: str,
    status: str,
) -> RecordOutcome:
    """The webhook transition, inside a transaction the CALLER owns.

    Used by the WhatsApp status worker, which already holds a session for the
    whole callback batch. Opening a second one there would mean two connections
    racing over the same rows in one logical unit of work.
    """
    if status not in (VOUCHER_DELIVERY_DELIVERED, VOUCHER_DELIVERY_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    # `no_autoflush` matters here rather than being defensive: this runs inside
    # somebody else's transaction, and an ordinary query would flush whatever
    # they have pending at a moment they did not choose.
    with session.no_autoflush:
        row = await _row(session, for_update=True)
        if row is None:
            # Not ours, and nothing to write. Leave the caller's unit of work
            # exactly as it was found.
            return RecordOutcome(applied=False, reason=RECORD_MISSING_ROW, snapshot=_snapshot(None))
    return await _apply_webhook_row(session, row, provider_message_id=provider_message_id, status=status, now=now)


async def _apply_webhook_row(
    session: AsyncSession,
    row: EasyWeekCampaignVoucherDeliveryLedger | None,
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
        return RecordOutcome(applied=False, reason=RECORD_WOULD_REGRESS, snapshot=_snapshot(row))
    if row.delivered_at is None:
        # Read implies delivered. Recording read without it would leave a row
        # the database itself refuses, so this same callback stamps both.
        row.delivered_at = now
    if status == VOUCHER_DELIVERY_READ and row.read_at is None:
        row.read_at = now
    row.status = status
    row.reconciliation_required = False
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

    Matched on the exact provider message id and nothing else: a webhook for
    another message says nothing about ours. Monotonic by rank, so a duplicate
    or an out-of-order callback — read arriving before delivered, delivered
    arriving twice — cannot move the row backwards or re-stamp a timestamp.
    """
    if status not in (VOUCHER_DELIVERY_DELIVERED, VOUCHER_DELIVERY_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row(session, for_update=True)
            return await _apply_webhook_row(
                session, row, provider_message_id=provider_message_id, status=status, now=now
            )


__all__ = [
    "CanaryIdentity",
    "apply_webhook_transition",
    "ClaimOutcome",
    "LedgerSnapshot",
    "RecordOutcome",
    "STATUS_RANK",
    "claim_create",
    "claim_pay",
    "claim_refund",
    "claim_send",
    "load",
    "open_canary",
    "record_outcome",
    "record_webhook_transition",
]
