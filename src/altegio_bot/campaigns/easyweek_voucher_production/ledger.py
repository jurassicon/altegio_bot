"""The durable claim ledger of the production voucher mailing (§42).

Everything §41 earned, plus two new questions
---------------------------------------------
EasyWeek publishes no write idempotency key and Meta will happily deliver
twice, so every external step here is irreversible by repetition and each can
end with the request sent and the answer lost. The rule that makes that
survivable is unchanged and non-negotiable:

    the claim is committed BEFORE the request leaves.

A crash anywhere after that commit — before the socket, mid-flight, after the
response — reads identically afterwards: *claimed, outcome unknown*. That is the
only reading that cannot charge a card twice or message a person twice.

The first new question is **which batch**. §41 was a singleton, so "the batch"
was unambiguous and a command could address a slot by number alone. This phase
runs again next month, and possibly twice in one week, so there is no "current"
batch and no "latest" one: every function below that touches a slot takes a
``batch_id`` and an explicit slot, and there is deliberately no lookup by slot
alone anywhere in this module.

The second is **how many**, and the answer is the operator's, recorded. A header
names its size and its approved cost once, at freeze; items hang off that size
through a composite foreign key and compare their own slot against it. What §41
enforced with a ceiling of five, this phase enforces with an arithmetic identity
the database will not let drift.

Its own tables, not §36's, §37.2's or §41's
-------------------------------------------
The machinery is theirs, proven three times. The identity is not: those ledgers
are singletons by construction, and widening any of them to hold a monthly
mailing would mean weakening the very constraint that makes it a singleton.
Their rows, their constraints and their historical HMAC bindings are untouched.

Per-item work stays per item
----------------------------
This is the one place where a bigger list changes the design rather than just
the numbers. §41 could afford to re-materialise its whole five-row composition
and compare it field by field on every single claim; at fifty recipients the
same code would re-read and re-lock fifty rows fifty times, for two and a half
thousand row locks and a quadratic verification cost nobody would see until it
was live.

So a claim here touches exactly two rows: the header, and its own slot. What it
verifies is the header's immutable identity — which includes ``frozen_digest``,
a hash over the entire composition — plus that one slot's own identity. The
whole-composition comparison still happens, once per stage, in the plan, against
LIVE data, which is strictly stronger than comparing the frozen rows against
themselves. Nothing is checked less; it is checked once instead of N times.

The header is still derived, never asserted, and it is now derived by an
aggregate over one index rather than by loading every row into Python.

Monotonic, and defended against a stale writer
----------------------------------------------
Every write names the states it is valid FROM; the row is locked with
``SELECT ... FOR UPDATE`` and compared before it is touched. Every status also
carries a rank, and a write that would lower it is refused. Together they mean a
reconciliation that read the world a moment too early cannot walk a stage back
into a state the same request could be claimed from again.

The first unknown stops the suffix OF ITS OWN BATCH
---------------------------------------------------
A batch is walked in slot order, and the moment one slot's outcome cannot be
proven the header is halted and every later slot of that batch is left
untouched. A halted header refuses the next plan, so the remaining slots cannot
be claimed until a human has looked at the one that went wrong. Other batches
are unaffected, and deliberately: they are separate mailings with separate
approvals, and one August unknown is not a reason to freeze September.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_delivery.binding import voucher_code_matches
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPROVAL_ARITHMETIC,
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_SCOPE,
    UNIT_PRICE_MINOR,
    binding_material,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    VOUCHER_PRODUCTION_COMPLETED,
    VOUCHER_PRODUCTION_FROZEN,
    VOUCHER_PRODUCTION_HALTED,
    VOUCHER_PRODUCTION_IN_PROGRESS,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED,
    VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_CREATED,
    VOUCHER_PRODUCTION_ITEM_DELIVERED,
    VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
    VOUCHER_PRODUCTION_ITEM_PAID,
    VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_PAY_REJECTED,
    VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_READ,
    VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUNDED,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_STATUSES,
    CampaignRecipient,
    CampaignRun,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.utils import utcnow

# The MAC domain of this phase. Shares §36's secret and §37.2's primitive, with
# its own label, so a MAC written by any of the four phases can never verify
# another's code even under an identical key.
VOUCHER_PRODUCTION_DOMAIN: Final = b"altegio_bot/easyweek_voucher_production/voucher_code/v1"

# How far along its own life a slot sits. A write that would lower this rank is
# a regression and is refused, whoever sent it and however late it arrives.
# `ambiguous` sits at the top because it is a full stop.
ITEM_RANK: Final[dict[str, int]] = {
    VOUCHER_PRODUCTION_ITEM_PLANNED: 5,
    VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED: 10,
    VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN: 11,
    VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED: 12,
    VOUCHER_PRODUCTION_ITEM_CREATED: 20,
    VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED: 30,
    VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN: 31,
    VOUCHER_PRODUCTION_ITEM_PAY_REJECTED: 32,
    VOUCHER_PRODUCTION_ITEM_PAID: 40,
    VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED: 41,
    VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN: 42,
    VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED: 43,
    VOUCHER_PRODUCTION_ITEM_REFUNDED: 45,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED: 50,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED: 51,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN: 52,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED: 60,
    VOUCHER_PRODUCTION_ITEM_DELIVERED: 70,
    VOUCHER_PRODUCTION_ITEM_READ: 80,
    VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED: 90,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS: 99,
}

# A stage may be claimed only from these states. `*_rejected` is re-claimable
# for the two EasyWeek mutations, because a proven validation refusal did not
# act. The SEND has no such entry: one attempt, ever.
CREATE_CLAIMABLE_FROM: Final = frozenset({VOUCHER_PRODUCTION_ITEM_PLANNED, VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED})
PAY_CLAIMABLE_FROM: Final = frozenset({VOUCHER_PRODUCTION_ITEM_CREATED, VOUCHER_PRODUCTION_ITEM_PAY_REJECTED})
SEND_CLAIMABLE_FROM: Final = frozenset({VOUCHER_PRODUCTION_ITEM_PAID})
# A refund is claimable from `pay_unknown` too. The money has probably moved and
# the artifact could not be proven — which is precisely when getting it back
# matters most. It is still pre-send only, and a CHECK constraint says so
# independently.
REFUND_CLAIMABLE_FROM: Final = frozenset(
    {
        VOUCHER_PRODUCTION_ITEM_PAID,
        VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
    }
)

# States in which something may still have reached EasyWeek or Meta.
UNRESOLVED_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED,
        VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED,
        VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
        VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
        VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    }
)

# States in which a message may already be in a customer's hands. A refund is
# forbidden from every one of them, by plan, by claim and by CHECK constraint.
SENT_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
        VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
        VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
        VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
        VOUCHER_PRODUCTION_ITEM_DELIVERED,
        VOUCHER_PRODUCTION_ITEM_READ,
    }
)

# Where a slot's life legitimately ends. A batch whose every slot is here, with
# nothing outstanding, is finished — which is NOT the same as every message
# having been read. `provider_accepted` is terminal for the execution and says
# only that Meta took the request.
TERMINAL_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
        VOUCHER_PRODUCTION_ITEM_DELIVERED,
        VOUCHER_PRODUCTION_ITEM_READ,
        VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
        VOUCHER_PRODUCTION_ITEM_REFUNDED,
        VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
    }
)

CLAIM_GRANTED: Final = "claim_granted"
CLAIM_REFUSED_MISSING_ROW: Final = "claim_refused_missing_row"
CLAIM_REFUSED_STATE: Final = "claim_refused_state"
CLAIM_REFUSED_IDENTITY: Final = "claim_refused_identity"
CLAIM_REFUSED_HALTED: Final = "claim_refused_halted"

RECORD_APPLIED: Final = "record_applied"
RECORD_MISSING_ROW: Final = "record_missing_row"
RECORD_STALE_STATE: Final = "record_stale_state"
RECORD_WOULD_REGRESS: Final = "record_would_regress"

FREEZE_APPLIED: Final = "freeze_applied"
FREEZE_REFUSED_EXISTS: Final = "freeze_refused_exists"
FREEZE_REFUSED_SNAPSHOT: Final = "freeze_refused_snapshot"
FREEZE_REFUSED_APPROVAL: Final = "freeze_refused_approval"


@dataclass(frozen=True)
class ClaimOutcome:
    granted: bool
    reason: str
    status: str | None
    intent_uuid: str | None = None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _text(value: object) -> str | None:
    return str(value) if value is not None else None


@dataclass(frozen=True)
class ItemSnapshot:
    """A PII-free view of one slot, safe to print and to sign into a digest."""

    slot: int
    campaign_recipient_id: int
    campaign_run_id: int
    easyweek_customer_uuid: str | None
    reconciliation_marker: str
    status: str
    reason_code: str | None = None
    voucher_value_minor: int = UNIT_PRICE_MINOR
    voucher_quantity: int = 1
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
        stage digest does not need them to be unambiguous — the slot, the row id
        and the marker already are.

        The four delivery facts are reported separately and never merged. An
        operator has to be able to tell "Meta accepted it" from "a webhook said
        delivered" from "a webhook said read", because only the last one means
        the person has seen their voucher.
        """
        return {
            "slot": self.slot,
            "campaign_recipient_id": self.campaign_recipient_id,
            "status": self.status,
            "reason_code": self.reason_code,
            "voucher_value_minor": self.voucher_value_minor,
            "voucher_quantity": self.voucher_quantity,
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
            # The three stages of "did it arrive", kept apart on purpose.
            "provider_accepted": self.provider_accepted_at is not None,
            "provider_accepted_at": self.provider_accepted_at,
            "webhook_delivered": self.delivered_at is not None,
            "delivered_at": self.delivered_at,
            "webhook_read": self.read_at is not None,
            "read_at": self.read_at,
            "refund_verified_at": self.refund_verified_at,
            "evidence": dict(self.evidence or {}),
        }


@dataclass(frozen=True)
class BatchSnapshot:
    """One batch's header and every slot of it, as a report may print them."""

    exists: bool
    batch_id: int | None = None
    status: str | None = None
    halted_reason_code: str | None = None
    baseline_version: str | None = None
    company_id: int | None = None
    campaign_code: str | None = None
    campaign_run_id: int | None = None
    campaign_period_start: str | None = None
    campaign_period_end: str | None = None
    location_uuid: str | None = None
    staffer_uuid: str | None = None
    payment_account_uuid: str | None = None
    voucher_template_uuid: str | None = None
    frozen_digest: str | None = None
    recipient_count: int = 0
    voucher_unit_price_minor: int = UNIT_PRICE_MINOR
    total_exposure_minor: int = 0
    approved_recipient_count: int | None = None
    approved_exposure_minor: int | None = None
    reconciliation_required: bool = False
    frozen_at: str | None = None
    items: tuple[ItemSnapshot, ...] = ()
    evidence: dict[str, Any] | None = None

    @property
    def period_label(self) -> str | None:
        """``2026-08-01..2026-08-31``, or ``None`` when there is no batch."""
        if not self.campaign_period_start or not self.campaign_period_end:
            return None
        return f"{self.campaign_period_start[:10]}..{self.campaign_period_end[:10]}"

    def item(self, slot: int) -> ItemSnapshot | None:
        for entry in self.items:
            if entry.slot == slot:
                return entry
        return None

    @property
    def halted(self) -> bool:
        return self.status == VOUCHER_PRODUCTION_HALTED

    @property
    def provider_accepted_count(self) -> int:
        return sum(1 for entry in self.items if entry.provider_accepted_at is not None)

    @property
    def delivered_count(self) -> int:
        return sum(1 for entry in self.items if entry.delivered_at is not None)

    @property
    def read_count(self) -> int:
        return sum(1 for entry in self.items if entry.read_at is not None)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "exists": self.exists,
            "batch_scope": PRODUCTION_SCOPE if self.exists else None,
            # The durable identity every post-freeze command must name. There is
            # no "latest batch" in this phase and nothing resolves one.
            "batch_id": self.batch_id,
            "status": self.status,
            "halted": self.halted,
            "halted_reason_code": self.halted_reason_code,
            "baseline_version": self.baseline_version,
            "company_id": self.company_id,
            "campaign_code": self.campaign_code,
            "campaign_run_id": self.campaign_run_id,
            "recipient_basis": RECIPIENT_BASIS_MANUAL if self.exists else None,
            # A manual selection has no first visit to prove, and says so.
            "first_visit_proof": "not_applicable" if self.exists else None,
            # The wave this batch is bound to, in one glance and in full. It is
            # the entitlement key, so it belongs in every report an operator
            # reads, not only in the digest that signs it.
            "campaign_period": self.period_label,
            "campaign_period_start": self.campaign_period_start,
            "campaign_period_end": self.campaign_period_end,
            "frozen_digest": self.frozen_digest,
            "frozen_at": self.frozen_at,
            "recipient_count": self.recipient_count,
            "voucher_unit_price_minor": self.voucher_unit_price_minor,
            "total_exposure_minor": self.total_exposure_minor,
            # What a human actually agreed to, kept beside what was frozen. The
            # CHECK constraints mean these cannot disagree; printing both is how
            # an operator sees that for themselves.
            "approved_recipient_count": self.approved_recipient_count,
            "approved_exposure_minor": self.approved_exposure_minor,
            "approval_arithmetic": APPROVAL_ARITHMETIC,
            "reconciliation_required": self.reconciliation_required,
            # Execution, acceptance, delivery and reading, counted separately.
            # `completed` below refers to the EXECUTION of the stages; it never
            # means that every message was delivered, let alone read.
            "execution_completed": self.status == VOUCHER_PRODUCTION_COMPLETED,
            "provider_accepted_count": self.provider_accepted_count,
            "webhook_delivered_count": self.delivered_count,
            "webhook_read_count": self.read_count,
            "items": [entry.as_safe_dict() for entry in self.items],
            "evidence": dict(self.evidence or {}),
        }


def _item_snapshot(row: EasyWeekVoucherProductionBatchItem) -> ItemSnapshot:
    return ItemSnapshot(
        slot=int(row.slot),
        campaign_recipient_id=int(row.campaign_recipient_id),
        campaign_run_id=int(row.campaign_run_id),
        easyweek_customer_uuid=_text(row.easyweek_customer_uuid),
        reconciliation_marker=row.reconciliation_marker,
        status=row.status,
        reason_code=row.reason_code,
        voucher_value_minor=int(row.voucher_value_minor),
        voucher_quantity=int(row.voucher_quantity),
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


async def _header_by_id(
    session: AsyncSession, batch_id: int, *, for_update: bool = False
) -> EasyWeekVoucherProductionBatch | None:
    """One batch by its durable id. The only way a stage finds its header."""
    if for_update:
        return await session.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
    return await session.get(EasyWeekVoucherProductionBatch, batch_id)


async def _header_by_preview(
    session: AsyncSession, campaign_run_id: int, *, for_update: bool = False
) -> EasyWeekVoucherProductionBatch | None:
    """The one batch frozen from this preview, if any. Unique by constraint."""
    statement = select(EasyWeekVoucherProductionBatch).where(
        EasyWeekVoucherProductionBatch.campaign_run_id == campaign_run_id
    )
    if for_update:
        statement = statement.with_for_update()
    return (await session.execute(statement)).scalar_one_or_none()


async def _items(session: AsyncSession, *, batch_id: int) -> list[EasyWeekVoucherProductionBatchItem]:
    """Every slot of ONE batch, in slot order, WITHOUT locking any of them.

    Used only by :func:`_snapshot`, which serves the full-snapshot readers:
    status, the plan, and the report at the end of a stage. It deliberately
    offers no ``for_update``, because taking N row locks is the one thing this
    phase's per-item path exists to avoid — §41 locked every slot on every
    single write, which is fine at five and is thousands of locks at fifty.

    A claim and a per-item write use :func:`_one_item` instead, and lock one row.
    Anybody reaching for a locking variant here should ask why a whole
    composition needs holding, because the answer is normally that it does not.
    """
    return list(
        (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem)
                .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
            )
        )
        .scalars()
        .all()
    )


async def _one_item(
    session: AsyncSession, *, batch_id: int, slot: int, for_update: bool = False
) -> EasyWeekVoucherProductionBatchItem | None:
    """Exactly one slot of exactly one batch.

    Always both keys. A slot number is a position inside one composition, and
    slot 1 exists in every batch — so there is no lookup by slot alone in this
    module, and adding one would be the bug.
    """
    statement = (
        select(EasyWeekVoucherProductionBatchItem)
        .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
        .where(EasyWeekVoucherProductionBatchItem.slot == slot)
    )
    if for_update:
        statement = statement.with_for_update()
    return (await session.execute(statement)).scalar_one_or_none()


async def _snapshot(session: AsyncSession, row: EasyWeekVoucherProductionBatch | None) -> BatchSnapshot:
    if row is None:
        return BatchSnapshot(exists=False)
    items = await _items(session, batch_id=row.id)
    return BatchSnapshot(
        exists=True,
        batch_id=int(row.id),
        status=row.status,
        halted_reason_code=row.halted_reason_code,
        baseline_version=row.baseline_version,
        company_id=int(row.company_id),
        campaign_code=row.campaign_code,
        campaign_run_id=int(row.campaign_run_id),
        campaign_period_start=_iso(row.campaign_period_start),
        campaign_period_end=_iso(row.campaign_period_end),
        location_uuid=_text(row.location_uuid),
        staffer_uuid=_text(row.staffer_uuid),
        payment_account_uuid=_text(row.payment_account_uuid),
        voucher_template_uuid=_text(row.voucher_template_uuid),
        frozen_digest=row.frozen_digest,
        recipient_count=int(row.recipient_count),
        voucher_unit_price_minor=int(row.voucher_unit_price_minor),
        total_exposure_minor=int(row.total_exposure_minor),
        approved_recipient_count=int(row.approved_recipient_count),
        approved_exposure_minor=int(row.approved_exposure_minor),
        reconciliation_required=bool(row.reconciliation_required),
        frozen_at=_iso(row.frozen_at),
        items=tuple(_item_snapshot(entry) for entry in items),
        evidence=dict(row.evidence or {}),
    )


async def load(session_maker: async_sessionmaker[AsyncSession], *, batch_id: int) -> BatchSnapshot:
    """Read ONE batch by id without locking it. For status and plan only."""
    async with session_maker() as session:
        return await _snapshot(session, await _header_by_id(session, batch_id))


async def load_for_preview(session_maker: async_sessionmaker[AsyncSession], *, campaign_run_id: int) -> BatchSnapshot:
    """Read the batch this preview was frozen into, if any.

    The one place a batch is resolved from something other than its id, and it
    is not a convenience: before the freeze the preview is all an operator has,
    and this is how a freeze finds out it has already happened. It resolves
    through a UNIQUE constraint, so it can never pick "one of" several.
    """
    async with session_maker() as session:
        return await _snapshot(session, await _header_by_preview(session, campaign_run_id))


@dataclass(frozen=True)
class BatchHeadline:
    """One batch as a listing shows it. No slots, no identities."""

    batch_id: int
    campaign_run_id: int
    status: str
    campaign_period_start: str | None
    campaign_period_end: str | None
    recipient_count: int
    total_exposure_minor: int
    reconciliation_required: bool
    frozen_at: str | None

    def as_safe_dict(self) -> dict[str, Any]:
        period = None
        if self.campaign_period_start and self.campaign_period_end:
            period = f"{self.campaign_period_start[:10]}..{self.campaign_period_end[:10]}"
        return {
            "batch_id": self.batch_id,
            "campaign_run_id": self.campaign_run_id,
            "status": self.status,
            "halted": self.status == VOUCHER_PRODUCTION_HALTED,
            "campaign_period": period,
            "recipient_count": self.recipient_count,
            "total_exposure_minor": self.total_exposure_minor,
            "reconciliation_required": self.reconciliation_required,
            "frozen_at": self.frozen_at,
        }


async def list_batches(
    session_maker: async_sessionmaker[AsyncSession], *, limit: int = 50
) -> tuple[BatchHeadline, ...]:
    """Every production batch, newest first. Headers only.

    Exists so an operator can find the id they need to name, and nothing more.
    It is deliberately not a way to act on a batch: the id it prints has to be
    typed into the next command, which is the point at which a human confirms
    which mailing they mean.
    """
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatch)
                    .order_by(EasyWeekVoucherProductionBatch.id.desc())
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
    return tuple(
        BatchHeadline(
            batch_id=int(row.id),
            campaign_run_id=int(row.campaign_run_id),
            status=row.status,
            campaign_period_start=_iso(row.campaign_period_start),
            campaign_period_end=_iso(row.campaign_period_end),
            recipient_count=int(row.recipient_count),
            total_exposure_minor=int(row.total_exposure_minor),
            reconciliation_required=bool(row.reconciliation_required),
            frozen_at=_iso(row.frozen_at),
        )
        for row in rows
    )


@dataclass(frozen=True)
class BatchItemIdentity:
    """The immutable identity of one slot, as the freeze wrote it."""

    slot: int
    campaign_recipient_id: int
    easyweek_customer_uuid: str
    reconciliation_marker: str


@dataclass(frozen=True)
class BatchIdentity:
    """The immutable identity one batch is frozen with.

    ``batch_id`` is ``None`` for the identity a freeze is about to WRITE and is
    set for every identity read back afterwards. A stage that acts on a slot
    always has the id, because it read the header to get here.
    """

    company_id: int
    campaign_code: str
    campaign_run_id: int
    campaign_period_start: datetime
    campaign_period_end: datetime
    location_uuid: str
    staffer_uuid: str
    payment_account_uuid: str
    voucher_template_uuid: str
    baseline_version: str
    frozen_digest: str
    items: tuple[BatchItemIdentity, ...]
    batch_id: int | None = None

    @property
    def recipient_count(self) -> int:
        return len(self.items)

    def item(self, slot: int) -> BatchItemIdentity | None:
        for entry in self.items:
            if entry.slot == slot:
                return entry
        return None

    def matches_header(self, row: EasyWeekVoucherProductionBatch) -> bool:
        """Is this header the one this process planned for? O(1), by design.

        Compared field by field rather than by a digest alone, so a mismatch
        cannot be mistaken for a drifted plan. Reported as a boolean; no value
        is echoed.

        ``frozen_digest`` is part of it, and it is what makes this check as
        strong as walking every row: the digest is a hash over the entire
        composition, so a header carrying the expected digest is a header whose
        slots are the slots that were approved. The per-slot comparison that
        remains is :meth:`matches_item`, on the one row being claimed.

        This is where §41's per-claim ``matches(snapshot)`` used to live, and
        moving it here is the whole reason a fifty-recipient mailing does not
        cost fifty full-composition comparisons per stage.
        """
        if self.batch_id is not None and int(row.id) != self.batch_id:
            return False
        return (
            int(row.company_id) == self.company_id
            and row.campaign_code == self.campaign_code
            and int(row.campaign_run_id) == self.campaign_run_id
            and row.campaign_period_start == self.campaign_period_start
            and row.campaign_period_end == self.campaign_period_end
            and _text(row.location_uuid) == self.location_uuid
            and _text(row.staffer_uuid) == self.staffer_uuid
            and _text(row.payment_account_uuid) == self.payment_account_uuid
            and _text(row.voucher_template_uuid) == self.voucher_template_uuid
            and row.baseline_version == self.baseline_version
            and row.frozen_digest == self.frozen_digest
            and int(row.recipient_count) == self.recipient_count
            and int(row.voucher_unit_price_minor) == UNIT_PRICE_MINOR
            and int(row.total_exposure_minor) == UNIT_PRICE_MINOR * self.recipient_count
        )

    def matches_item(self, row: EasyWeekVoucherProductionBatchItem) -> bool:
        """Is this row the slot this process planned to act on? O(1)."""
        expected = self.item(int(row.slot))
        if expected is None:
            return False
        return (
            int(row.campaign_recipient_id) == expected.campaign_recipient_id
            and _text(row.easyweek_customer_uuid) == expected.easyweek_customer_uuid
            and row.reconciliation_marker == expected.reconciliation_marker
            and int(row.voucher_value_minor) == UNIT_PRICE_MINOR
            and int(row.voucher_quantity) == 1
        )

    def matches(self, snapshot: BatchSnapshot) -> bool:
        """The full comparison, against an already-loaded snapshot.

        Used by the plan and by the freeze's idempotency check, where the whole
        composition is in hand anyway. Never by a per-item claim.
        """
        if not snapshot.exists:
            return False
        if self.batch_id is not None and snapshot.batch_id != self.batch_id:
            return False
        if (
            snapshot.company_id != self.company_id
            or snapshot.campaign_code != self.campaign_code
            or snapshot.campaign_run_id != self.campaign_run_id
            or snapshot.campaign_period_start != _iso(self.campaign_period_start)
            or snapshot.campaign_period_end != _iso(self.campaign_period_end)
            or snapshot.location_uuid != self.location_uuid
            or snapshot.staffer_uuid != self.staffer_uuid
            or snapshot.payment_account_uuid != self.payment_account_uuid
            or snapshot.voucher_template_uuid != self.voucher_template_uuid
            or snapshot.baseline_version != self.baseline_version
            or snapshot.frozen_digest != self.frozen_digest
            or snapshot.recipient_count != self.recipient_count
            or snapshot.voucher_unit_price_minor != UNIT_PRICE_MINOR
            or snapshot.total_exposure_minor != UNIT_PRICE_MINOR * self.recipient_count
        ):
            return False
        stored = {entry.slot: entry for entry in snapshot.items}
        if set(stored) != {entry.slot for entry in self.items}:
            return False
        for entry in self.items:
            row = stored[entry.slot]
            if (
                row.campaign_recipient_id != entry.campaign_recipient_id
                or row.easyweek_customer_uuid != entry.easyweek_customer_uuid
                or row.reconciliation_marker != entry.reconciliation_marker
                or row.voucher_value_minor != UNIT_PRICE_MINOR
                or row.voucher_quantity != 1
            ):
                return False
        return True


async def production_batch_id_for_preview(session: AsyncSession, *, campaign_run_id: int) -> int | None:
    """The id of the batch this preview is frozen into, or ``None``.

    Takes the caller's session so the Ops page and the editor can ask under
    whatever transaction they already hold.
    """
    found = await session.scalar(
        select(EasyWeekVoucherProductionBatch.id)
        .where(EasyWeekVoucherProductionBatch.campaign_run_id == campaign_run_id)
        .where(EasyWeekVoucherProductionBatch.provider == PROVIDER_EASYWEEK)
        .limit(1)
    )
    return int(found) if found is not None else None


async def preview_is_locked_by_voucher_production(session: AsyncSession, *, campaign_run_id: int) -> bool:
    """Has a §42 batch frozen itself onto this preview run?

    Once it has, the snapshot stops being editable. Not tidiness: the batch
    addresses each recipient by run id and recipient id and re-proves the whole
    composition live before every external step. An operator who edits or
    discards the preview after CREATE or PAY does not undo anything — they make
    the DELIVER and the REFUND unprovable, leaving real €15 orders with no way
    to finish them and no way to take them back.

    Takes the caller's session so the check happens under the same lock as the
    edit it guards.
    """
    return await production_batch_id_for_preview(session, campaign_run_id=campaign_run_id) is not None


@dataclass(frozen=True)
class FreezeOutcome:
    applied: bool
    reason: str
    snapshot: BatchSnapshot


async def freeze_batch(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    approved_recipient_count: int,
    approved_exposure_minor: int,
    freeze_plan_digest: str,
) -> FreezeOutcome:
    """Write one batch header and its slots, atomically, or refuse.

    Atomic with the freeze it causes
    --------------------------------
    Writing these rows is what stops the preview being edited, so the run and
    every recipient are re-proven HERE, under the very row lock the editor
    takes, in the same transaction that inserts. Without that, an operator's
    Remove and this insert can both pass their checks against a world that no
    longer exists, and the batch opens on somebody who was just removed.

    The lock order is the editor's, deliberately: ``CampaignRun`` FOR UPDATE
    first, then everything else. Two paths that take the same locks in the same
    order queue; two that disagree deadlock.

    This is the one function in the module that is O(N) per call by design: it
    verifies and writes N rows once. Every later stage touches one row at a
    time.

    A second batch on the same preview is refused here and, independently, by a
    unique constraint on ``campaign_run_id``. This function is the polite
    refusal; the schema is the one that cannot be talked round — which is also
    what decides a race between two operators freezing two different previews
    for the same person: the entitlement index lets exactly one of them commit.

    The approved numbers are re-checked against the rows this transaction can
    see, not against the ones the plan saw. A recipient added in between would
    otherwise be frozen into a batch whose approved count no longer describes
    it, and the CHECK constraints would then reject the write with a constraint
    name instead of a reason an operator can read.
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
                or run.period_start != identity.campaign_period_start
                or run.period_end != identity.campaign_period_end
            ):
                return FreezeOutcome(False, FREEZE_REFUSED_SNAPSHOT, BatchSnapshot(exists=False))

            existing = await _header_by_preview(session, identity.campaign_run_id, for_update=True)
            if existing is not None:
                # Idempotent for the exact same composition, refused for any
                # other: a resumed crash must find its own batch, and a second
                # freeze of a preview must find a wall.
                snapshot = await _snapshot(session, existing)
                if identity.matches(snapshot):
                    return FreezeOutcome(False, FREEZE_APPLIED, snapshot)
                return FreezeOutcome(False, FREEZE_REFUSED_EXISTS, snapshot)

            # 2. Under that lock, every recipient as it is NOW. A Remove that
            # won the race has already set `status='skipped'`, and this is where
            # that becomes visible rather than in a check taken seconds ago.
            for entry in identity.items:
                recipient = await session.get(CampaignRecipient, entry.campaign_recipient_id)
                if (
                    recipient is None
                    or recipient.provider != PROVIDER_EASYWEEK
                    or recipient.campaign_run_id != run.id
                    or recipient.company_id != identity.company_id
                    or recipient.status != "candidate"
                    or recipient.is_opted_out
                    or (recipient.recipient_basis or "") != RECIPIENT_BASIS_MANUAL
                    or _text(recipient.easyweek_customer_uuid) != entry.easyweek_customer_uuid
                ):
                    return FreezeOutcome(False, FREEZE_REFUSED_SNAPSHOT, BatchSnapshot(exists=False))

            # 3. The snapshot must still hold EXACTLY these rows. A recipient
            # added between the plan and this transaction would otherwise be
            # silently left out of a batch an operator believes is complete.
            active = list(
                (
                    await session.execute(
                        select(CampaignRecipient.id)
                        .where(CampaignRecipient.campaign_run_id == run.id)
                        .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                        .where(CampaignRecipient.status == "candidate")
                        .order_by(CampaignRecipient.id.asc())
                    )
                )
                .scalars()
                .all()
            )
            if [int(value) for value in active] != [entry.campaign_recipient_id for entry in identity.items]:
                return FreezeOutcome(False, FREEZE_REFUSED_SNAPSHOT, BatchSnapshot(exists=False))

            # 4. And the two numbers the operator approved must still describe
            # it. Re-checked against THIS transaction's rows, so an approval for
            # four people cannot freeze a five-person snapshot.
            count = identity.recipient_count
            if approved_recipient_count != count or approved_exposure_minor != UNIT_PRICE_MINOR * count:
                return FreezeOutcome(False, FREEZE_REFUSED_APPROVAL, BatchSnapshot(exists=False))

            now = utcnow()
            header = EasyWeekVoucherProductionBatch(
                batch_scope=PRODUCTION_SCOPE,
                request_schema_version=PRODUCTION_SCHEMA_VERSION,
                baseline_version=identity.baseline_version,
                provider=PROVIDER_EASYWEEK,
                company_id=identity.company_id,
                campaign_code=identity.campaign_code,
                recipient_basis=RECIPIENT_BASIS_MANUAL,
                campaign_run_id=identity.campaign_run_id,
                campaign_period_start=identity.campaign_period_start,
                campaign_period_end=identity.campaign_period_end,
                location_uuid=uuid_module.UUID(identity.location_uuid),
                staffer_uuid=uuid_module.UUID(identity.staffer_uuid),
                payment_account_uuid=uuid_module.UUID(identity.payment_account_uuid),
                voucher_template_uuid=uuid_module.UUID(identity.voucher_template_uuid),
                frozen_digest=identity.frozen_digest,
                recipient_count=count,
                voucher_unit_price_minor=UNIT_PRICE_MINOR,
                total_exposure_minor=UNIT_PRICE_MINOR * count,
                approved_recipient_count=approved_recipient_count,
                approved_exposure_minor=approved_exposure_minor,
                status=VOUCHER_PRODUCTION_FROZEN,
                freeze_plan_digest=freeze_plan_digest,
                frozen_at=now,
                evidence={},
                created_at=now,
                updated_at=now,
            )
            session.add(header)
            await session.flush()

            for entry in identity.items:
                session.add(
                    EasyWeekVoucherProductionBatchItem(
                        batch_id=header.id,
                        batch_recipient_count=count,
                        slot=entry.slot,
                        provider=PROVIDER_EASYWEEK,
                        company_id=identity.company_id,
                        campaign_code=identity.campaign_code,
                        recipient_basis=RECIPIENT_BASIS_MANUAL,
                        campaign_run_id=identity.campaign_run_id,
                        campaign_recipient_id=entry.campaign_recipient_id,
                        easyweek_customer_uuid=uuid_module.UUID(entry.easyweek_customer_uuid),
                        campaign_period_start=identity.campaign_period_start,
                        campaign_period_end=identity.campaign_period_end,
                        voucher_value_minor=UNIT_PRICE_MINOR,
                        voucher_quantity=1,
                        reconciliation_marker=entry.reconciliation_marker,
                        status=VOUCHER_PRODUCTION_ITEM_PLANNED,
                        evidence={},
                        created_at=now,
                        updated_at=now,
                    )
                )
            await session.flush()
            return FreezeOutcome(True, FREEZE_APPLIED, await _snapshot(session, header))


async def _settle_header(session: AsyncSession, header: EasyWeekVoucherProductionBatch, *, now: datetime) -> None:
    """Recompute the header from what its slots actually say — by aggregate.

    The header is never told what to be; it is derived. Deriving it is what
    keeps "may the next stage start?" from drifting away from the rows that
    answer it.

    Derived from ONE indexed aggregate rather than from every row materialised
    in Python. That is the difference between a mailing whose per-item cost is
    flat and one that quietly squares: §41 loaded and locked all N rows on every
    single write, which is fine at five and is two and a half thousand row locks
    at fifty.

    Reading those rows without locking them is safe because of an invariant this
    module maintains everywhere: every writer takes the header lock FIRST. So
    inside this transaction no other transaction can be part-way through
    changing any slot of this batch.

    Two different facts, deliberately not merged
    -------------------------------------------
    A batch is HALTED while any slot sits in a state where something may have
    reached EasyWeek or Meta and nobody can say what — an unknown, an
    outstanding claim, an ambiguity. That is the condition the spec calls "the
    first unknown stops the remaining suffix", and it is what refuses the next
    claim.

    ``reconciliation_required`` is weaker and separate: a slot whose send Meta
    refused outright is proven, not unknown, so it does not stop the slots after
    it — and it still needs a human to look, so the flag stays up and the exit
    code says so.
    """
    item = EasyWeekVoucherProductionBatchItem
    unresolved = item.status.in_(UNRESOLVED_ITEM_STATUSES)

    # Every fact this function needs is read BEFORE the header is touched, and
    # both reads run under `no_autoflush`.
    #
    # Not defensive tidiness — a CHECK constraint makes the order load bearing.
    # `ck_..._halt_has_reason` requires a halted header to name a reason, so a
    # query issued between `status = 'halted'` and `halted_reason_code = ...`
    # would autoflush exactly the row the database refuses, and the caller would
    # see an IntegrityError from a transition that is perfectly legal.
    with session.no_autoflush:
        aggregate = (
            await session.execute(
                select(
                    func.count().label("total"),
                    func.coalesce(func.sum(case((unresolved, 1), else_=0)), 0).label("unresolved"),
                    func.min(case((unresolved, item.slot), else_=None)).label("first_unresolved_slot"),
                    func.coalesce(func.sum(case((item.reconciliation_required.is_(True), 1), else_=0)), 0).label(
                        "needs_reconcile"
                    ),
                    func.coalesce(func.sum(case((item.status.in_(TERMINAL_ITEM_STATUSES), 1), else_=0)), 0).label(
                        "terminal"
                    ),
                ).where(item.batch_id == header.id)
            )
        ).one()

        # The CHECK constraint requires a halt to say why; the first unresolved
        # slot in slot order is the one that stopped the batch. Read as one
        # extra single-row lookup rather than by walking everything.
        halt_reason: str | None = None
        first_slot = aggregate.first_unresolved_slot
        if int(aggregate.unresolved or 0) > 0 and first_slot is not None:
            row = (
                await session.execute(
                    select(item.reason_code, item.status)
                    .where(item.batch_id == header.id)
                    .where(item.slot == int(first_slot))
                )
            ).first()
            if row is not None:
                halt_reason = row[0] or row[1]

    total = int(aggregate.total or 0)
    header.reconciliation_required = int(aggregate.needs_reconcile or 0) > 0

    if int(aggregate.unresolved or 0) > 0:
        # Status and reason set together, with nothing in between that could
        # flush half of the pair.
        header.status = VOUCHER_PRODUCTION_HALTED
        header.halted_reason_code = halt_reason or VOUCHER_PRODUCTION_HALTED
        if header.halted_at is None:
            header.halted_at = now
        return

    header.halted_reason_code = None
    header.halted_at = None
    if total > 0 and int(aggregate.terminal or 0) == total:
        # Every slot's EXECUTION has ended. Not "every message was read": a
        # slot sitting at `provider_accepted` is terminal here and its customer
        # may never have opened WhatsApp.
        header.status = VOUCHER_PRODUCTION_COMPLETED
    else:
        header.status = VOUCHER_PRODUCTION_IN_PROGRESS


async def _claim(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    batch_id: int,
    slot: int,
    claimable_from: frozenset[str],
    next_status: str,
    plan_digest: str,
    digest_field: str,
    claimed_field: str,
    attempted_field: str,
    extra: dict[str, Any] | None = None,
    allow_halted: bool = False,
) -> ClaimOutcome:
    """Lock the header and the one slot, check everything, stamp, commit.

    In that order, and all of it before any caller may touch the network. The
    identity is re-checked here and not only in the plan, because the plan was
    built before the lock existed — and a halted batch is refused here too, so
    the suffix of a batch that already went wrong cannot be claimed by a caller
    that read the header a moment too early.

    ``allow_halted`` exists for exactly one caller: the refund. A halt is
    precisely when an untouched paid slot most needs its money back, so the
    cleanup path must not be shut by the condition that makes it necessary.

    Two rows, not N
    ---------------
    The header, then this slot. The header carries ``frozen_digest``, a hash
    over the whole composition, so verifying it verifies that the batch's people
    are the approved people; the slot is then verified on its own fields. What
    is NOT done here is re-reading and re-locking every other slot, which is
    what would turn a fifty-person mailing into two and a half thousand row
    locks per stage.

    The header is halted BY THIS TRANSACTION
    ----------------------------------------
    The claimed slot is an unresolved one the instant it is written, so the
    header is re-derived here, in the same transaction, before the commit. It
    matters because of what a crash looks like: if the header were left reading
    ``in_progress`` until some later write settled it, a second command — or a
    fresh plan a minute later — would see a batch that looks healthy and would
    happily claim the slots BEHIND a request that may already be in flight.
    Halting at claim time is what makes "the first unknown stops the suffix"
    true for a process that died rather than only for one that ran to the end.

    Locks are taken header first, then the item. The same order as the webhook
    path and the settle, deliberately: two orders is how two sessions deadlock.
    """
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            header = await _header_by_id(session, batch_id, for_update=True)
            if header is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            if not identity.matches_header(header):
                return ClaimOutcome(False, CLAIM_REFUSED_IDENTITY, header.status)
            if header.status == VOUCHER_PRODUCTION_HALTED and not allow_halted:
                return ClaimOutcome(False, CLAIM_REFUSED_HALTED, header.status)

            row = await _one_item(session, batch_id=batch_id, slot=slot, for_update=True)
            if row is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            if not identity.matches_item(row):
                return ClaimOutcome(False, CLAIM_REFUSED_IDENTITY, row.status)
            if row.status not in claimable_from:
                return ClaimOutcome(False, CLAIM_REFUSED_STATE, row.status)

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

            # Derived, not asserted. The slot this transaction just claimed is
            # unresolved, so the header it belongs to is halted before anybody
            # else can read it.
            await session.flush()
            await _settle_header(session, header, now=now)
            header.updated_at = now
            await session.flush()
            return ClaimOutcome(True, CLAIM_GRANTED, next_status)


async def claim_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    batch_id: int,
    slot: int,
    plan_digest: str,
    create_window_start: datetime,
    create_window_end: datetime,
) -> ClaimOutcome:
    """Reserve the right to send the ONE create POST for this slot."""
    return await _claim(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=slot,
        claimable_from=CREATE_CLAIMABLE_FROM,
        next_status=VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED,
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
    identity: BatchIdentity,
    batch_id: int,
    slot: int,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the right to send the ONE payment POST for this slot."""
    return await _claim(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=slot,
        claimable_from=PAY_CLAIMABLE_FROM,
        next_status=VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED,
        plan_digest=plan_digest,
        digest_field="pay_plan_digest",
        claimed_field="pay_claimed_at",
        attempted_field="pay_attempted_at",
    )


async def claim_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    batch_id: int,
    slot: int,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the right to send the ONE refund POST for this slot.

    Pre-send only. The claimable states exclude every state in which a message
    may have reached the customer, and a CHECK constraint says the same thing
    again: returning the money for a code somebody is already holding is worse
    than losing the €15.

    Reachable while the batch is halted, and deliberately so: a halt is exactly
    when an untouched paid slot most needs its money back.
    """
    return await _claim(
        session_maker,
        identity=identity,
        batch_id=batch_id,
        slot=slot,
        claimable_from=REFUND_CLAIMABLE_FROM,
        next_status=VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
        plan_digest=plan_digest,
        digest_field="refund_plan_digest",
        claimed_field="refund_claimed_at",
        attempted_field="refund_attempted_at",
        allow_halted=True,
    )


async def claim_send(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    batch_id: int,
    slot: int,
    plan_digest: str,
    live_guard_reproven_at: datetime,
    template_code: str,
    meta_template_name: str,
    template_language: str,
    sender_id: int | None,
) -> ClaimOutcome:
    """Reserve the ONE delivery of this slot, and write its intent, before sending.

    Three things happen in one committed transaction: the slot moves to
    ``send_claimed``, its attempt counter goes to one, and a redacted audit row
    is written naming which approved template was used. None of them contains
    the message, the parameters or the code — there is deliberately nothing here
    from which a later process could re-render and re-send anything.
    """
    now = utcnow()
    intent = uuid_module.uuid4()
    async with session_maker() as session:
        async with session.begin():
            header = await _header_by_id(session, batch_id, for_update=True)
            if header is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            if not identity.matches_header(header):
                return ClaimOutcome(False, CLAIM_REFUSED_IDENTITY, header.status)
            if header.status == VOUCHER_PRODUCTION_HALTED:
                return ClaimOutcome(False, CLAIM_REFUSED_HALTED, header.status)

            row = await _one_item(session, batch_id=batch_id, slot=slot, for_update=True)
            if row is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            if not identity.matches_item(row):
                return ClaimOutcome(False, CLAIM_REFUSED_IDENTITY, row.status)
            if row.status not in SEND_CLAIMABLE_FROM or int(row.send_attempt_count or 0) != 0:
                return ClaimOutcome(False, CLAIM_REFUSED_STATE, row.status)

            row.deliver_plan_digest = plan_digest
            row.live_guard_reproven_at = live_guard_reproven_at
            row.outbound_intent_uuid = intent
            row.send_claimed_at = now
            row.send_attempted_at = now
            row.send_attempt_count = 1
            row.status = VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED
            row.reason_code = None
            row.reconciliation_required = True
            row.updated_at = now
            session.add(
                EasyWeekVoucherProductionBatchAttempt(
                    item_id=row.id,
                    intent_uuid=intent,
                    template_code=template_code,
                    meta_template_name=meta_template_name,
                    template_language=template_language,
                    sender_id=sender_id,
                    campaign_recipient_id=row.campaign_recipient_id,
                    batch_id=batch_id,
                    slot=row.slot,
                    outcome="claimed",
                    claimed_at=now,
                )
            )
            # As in `_claim`: the claimed slot is unresolved, so the header is
            # halted by this transaction rather than by some later write.
            await session.flush()
            await _settle_header(session, header, now=now)
            header.updated_at = now
            await session.flush()
            return ClaimOutcome(True, CLAIM_GRANTED, VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED, str(intent))


@dataclass(frozen=True)
class RecordOutcome:
    """What a per-item write turned out to be, and where the header now sits.

    Deliberately NOT a full ``BatchSnapshot``. §41 returned one from every
    record, which meant re-loading the whole composition after every single
    slot; at fifty recipients that is fifty full loads per stage for a value the
    caller almost always discards. The runner loads the snapshot once, at the
    end of the stage, when it actually prints one.
    """

    applied: bool
    reason: str
    header_status: str | None = None
    halted: bool = False
    reconciliation_required: bool = False


async def record_item_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
    slot: int,
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
    """Write what one slot's stage turned out to be — as a compare-and-set.

    ``expected_statuses`` names every state this write is valid FROM. The row is
    locked, compared, and only then touched, so a caller whose view of the world
    is stale is told so and gets the current header state back instead of
    overwriting somebody else's newer state.

    On top of that a write that would LOWER the rank is refused outright. That
    is what stops a slow original response, or a reconciliation that read a
    moment too early, from walking a claimed stage back to somewhere the same
    request could be claimed again.

    Timestamps are only ever set here, never cleared: an ``attempted_at`` is a
    fact about something that left this process.
    """
    if status not in VOUCHER_PRODUCTION_ITEM_STATUSES:
        raise ValueError("unknown voucher production item status")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            header = await _header_by_id(session, batch_id, for_update=True)
            if header is None:
                return RecordOutcome(False, RECORD_MISSING_ROW)
            row = await _one_item(session, batch_id=batch_id, slot=slot, for_update=True)
            if row is None:
                return RecordOutcome(
                    False,
                    RECORD_MISSING_ROW,
                    header.status,
                    header.status == VOUCHER_PRODUCTION_HALTED,
                    bool(header.reconciliation_required),
                )
            if row.status not in expected_statuses:
                return RecordOutcome(
                    False,
                    RECORD_STALE_STATE,
                    header.status,
                    header.status == VOUCHER_PRODUCTION_HALTED,
                    bool(header.reconciliation_required),
                )
            if ITEM_RANK[status] < ITEM_RANK[row.status]:
                return RecordOutcome(
                    False,
                    RECORD_WOULD_REGRESS,
                    header.status,
                    header.status == VOUCHER_PRODUCTION_HALTED,
                    bool(header.reconciliation_required),
                )

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
                        select(EasyWeekVoucherProductionBatchAttempt).where(
                            EasyWeekVoucherProductionBatchAttempt.intent_uuid == row.outbound_intent_uuid
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
            await _settle_header(session, header, now=now)
            header.updated_at = now
            await session.flush()
            return RecordOutcome(
                True,
                RECORD_APPLIED,
                header.status,
                header.status == VOUCHER_PRODUCTION_HALTED,
                bool(header.reconciliation_required),
            )


async def binding_matches(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
    slot: int,
    voucher_code: str,
    target_order_uuid: str,
) -> bool:
    """Is this code the one this slot's stored binding was made from?

    Lives here rather than in the runner because the MAC must not travel: the
    snapshot a report is built from carries only "a binding exists", and moving
    the digest itself into a caller would put verification material into
    something printable.

    Reads two rows, never the composition: the one slot and its header, for the
    template UUID the MAC is bound to.

    False — never an exception — for a missing binding, a rotated key or a
    mismatch, so every caller fails closed on the same branch.
    """
    async with session_maker() as session:
        header = await _header_by_id(session, batch_id)
        if header is None:
            return False
        row = await _one_item(session, batch_id=batch_id, slot=slot)
        if row is None or row.voucher_code_hmac is None or row.hmac_key_id is None:
            return False
        if _text(row.target_order_uuid) != target_order_uuid:
            return False
        return voucher_code_matches(
            voucher_code=voucher_code,
            expected_mac=row.voucher_code_hmac,
            expected_key_id=row.hmac_key_id,
            # The batch AND the slot are inside the bound material, so a MAC
            # written for one slot cannot verify another slot's code — not
            # within one batch, and not across two batches whose slot numbers
            # legitimately coincide.
            ledger_uuid=binding_material(batch_id=int(header.id), slot=int(row.slot)),
            target_order_uuid=target_order_uuid,
            voucher_template_uuid=_text(header.voucher_template_uuid) or "",
            domain=VOUCHER_PRODUCTION_DOMAIN,
        )


async def _locate_message(session: AsyncSession, provider_message_id: str) -> tuple[int, int] | None:
    """``(batch_id, slot)`` for Meta's identifier, read WITHOUT a lock.

    Unlocked on purpose. Its only job is to learn which header to lock, and
    taking the item's row lock here would be taking an item BEFORE a header —
    the reverse of the order every stage writer uses, and the reason a webhook
    and a stage could deadlock against each other. Nothing this returns is
    trusted: the row is found again, on the exact identifier, under the locks.

    The identifier is unique table-wide, so this resolves to exactly one slot of
    exactly one batch, across every mailing this phase has ever run.
    """
    if not provider_message_id:
        return None
    found = (
        await session.execute(
            select(
                EasyWeekVoucherProductionBatchItem.batch_id,
                EasyWeekVoucherProductionBatchItem.slot,
            ).where(EasyWeekVoucherProductionBatchItem.provider_message_id == provider_message_id)
        )
    ).first()
    return (int(found[0]), int(found[1])) if found is not None else None


async def _apply_webhook_locked(
    session: AsyncSession,
    *,
    batch_id: int,
    provider_message_id: str,
    status: str,
    now: datetime,
) -> RecordOutcome:
    """Header first, then the one item, then the transition.

    One lock order for the whole phase. A webhook that locked its item and then
    waited for the header, while a stage held the header and waited for that
    item, is a textbook deadlock — and both orders are individually reasonable,
    which is exactly why having two of them is the bug rather than either one.

    Nothing about the transition itself is relaxed by this: the row is matched
    on the exact provider message id and nothing else, the rank comparison
    still refuses a callback that would walk the slot backwards, and the header
    is re-derived from the rows this transaction holds.
    """
    header = await session.get(EasyWeekVoucherProductionBatch, batch_id, with_for_update=True)
    if header is None:
        return RecordOutcome(False, RECORD_MISSING_ROW)
    row = (
        await session.execute(
            select(EasyWeekVoucherProductionBatchItem)
            .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
            .where(EasyWeekVoucherProductionBatchItem.provider_message_id == provider_message_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        # The unlocked lookup saw something this transaction does not. Not ours
        # as far as this unit of work is concerned, and nothing is written.
        return RecordOutcome(False, RECORD_MISSING_ROW)
    if ITEM_RANK[status] < ITEM_RANK[row.status]:
        # A duplicate, or a callback that arrived out of order. Acceptance is
        # what Meta said and a later webhook cannot unsay it.
        return RecordOutcome(
            False,
            RECORD_WOULD_REGRESS,
            header.status,
            header.status == VOUCHER_PRODUCTION_HALTED,
            bool(header.reconciliation_required),
        )
    if row.provider_accepted_at is None:
        # A callback can land beside the commit that recorded acceptance. The
        # identifier only exists because Meta answered with it, so the callback
        # itself is the proof — and refusing here would silently lose a status.
        row.provider_accepted_at = now
    if row.delivered_at is None:
        # Read implies delivered. Recording read without it would leave a row
        # the database itself refuses, so this same callback stamps both.
        row.delivered_at = now
    if status == VOUCHER_PRODUCTION_ITEM_READ and row.read_at is None:
        row.read_at = now
    row.status = status
    row.reconciliation_required = False
    # A delivered or read message means the voucher reached its person: there is
    # no open draft left for anybody to close by hand. Forced rather than left
    # alone, because the flag was set back when the order was still a draft.
    row.manual_cleanup_required = False
    row.updated_at = now

    await session.flush()
    await _settle_header(session, header, now=now)
    header.updated_at = now
    await session.flush()
    return RecordOutcome(
        True,
        RECORD_APPLIED,
        header.status,
        header.status == VOUCHER_PRODUCTION_HALTED,
        bool(header.reconciliation_required),
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
    racing over the same rows in one logical unit of work — and the slots this
    phase owns have no ``OutboxMessage`` behind them, so this is the only place
    their delivered and read can ever be observed.

    Deliberately NOT behind the §42 fence. ``delivered`` and ``read`` are facts
    about messages that have already been sent; dropping them because an
    operator has since closed the fence would corrupt the record of a mailing
    that really happened, and would do it silently.
    """
    if status not in (VOUCHER_PRODUCTION_ITEM_DELIVERED, VOUCHER_PRODUCTION_ITEM_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    # `no_autoflush` matters rather than being defensive: this runs inside
    # somebody else's transaction, and an ordinary query would flush whatever
    # they have pending at a moment they did not choose.
    with session.no_autoflush:
        located = await _locate_message(session, provider_message_id)
        if located is None:
            # Not ours, and nothing to write — and, just as important, no lock
            # taken. Leave the caller's unit of work exactly as it was found.
            return RecordOutcome(False, RECORD_MISSING_ROW)
    batch_id, _slot = located
    return await _apply_webhook_locked(
        session,
        batch_id=batch_id,
        provider_message_id=provider_message_id,
        status=status,
        now=now,
    )


async def record_webhook_transition(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    provider_message_id: str,
    status: str,
) -> RecordOutcome:
    """Apply a delivered/read webhook to the one slot it names.

    The session-owning variant, for callers that hold no transaction of their
    own. Same rules: matched on the exact provider message id, monotonic by
    rank, and idempotent for a duplicate.
    """
    if status not in (VOUCHER_PRODUCTION_ITEM_DELIVERED, VOUCHER_PRODUCTION_ITEM_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            located = await _locate_message(session, provider_message_id)
            if located is None:
                return RecordOutcome(False, RECORD_MISSING_ROW)
            batch_id, _slot = located
            return await _apply_webhook_locked(
                session,
                batch_id=batch_id,
                provider_message_id=provider_message_id,
                status=status,
                now=now,
            )


async def resettle(session_maker: async_sessionmaker[AsyncSession], *, batch_id: int) -> BatchSnapshot:
    """Re-derive one batch's header from its slots and return the result.

    Used by ``reconcile`` after it has written whatever a readback proved: the
    halt is not something a human clears by hand, it is what the slots currently
    say, so lifting it is a consequence of resolving them.
    """
    async with session_maker() as session:
        async with session.begin():
            header = await _header_by_id(session, batch_id, for_update=True)
            if header is None:
                return BatchSnapshot(exists=False)
            await _settle_header(session, header, now=utcnow())
            header.updated_at = utcnow()
            await session.flush()
            return await _snapshot(session, header)


__all__ = [
    "CREATE_CLAIMABLE_FROM",
    "ITEM_RANK",
    "PAY_CLAIMABLE_FROM",
    "REFUND_CLAIMABLE_FROM",
    "SEND_CLAIMABLE_FROM",
    "SENT_ITEM_STATUSES",
    "TERMINAL_ITEM_STATUSES",
    "UNRESOLVED_ITEM_STATUSES",
    "VOUCHER_PRODUCTION_DOMAIN",
    "BatchHeadline",
    "BatchIdentity",
    "BatchItemIdentity",
    "BatchSnapshot",
    "ClaimOutcome",
    "FreezeOutcome",
    "ItemSnapshot",
    "RecordOutcome",
    "apply_webhook_transition",
    "binding_matches",
    "claim_create",
    "claim_pay",
    "claim_refund",
    "claim_send",
    "freeze_batch",
    "list_batches",
    "load",
    "load_for_preview",
    "preview_is_locked_by_voucher_production",
    "production_batch_id_for_preview",
    "record_item_outcome",
    "record_webhook_transition",
    "resettle",
]
