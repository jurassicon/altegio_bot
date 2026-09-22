"""The durable claim ledger of the controlled voucher snapshot batch (§41).

Everything §37.2 earned, plus one new question
----------------------------------------------
EasyWeek publishes no write idempotency key and Meta will happily deliver
twice, so every external step here is irreversible by repetition and each can
end with the request sent and the answer lost. The rule that makes that
survivable is unchanged and non-negotiable:

    the claim is committed BEFORE the request leaves.

A crash anywhere after that commit — before the socket, mid-flight, after the
response — reads identically afterwards: *claimed, outcome unknown*. That is the
only reading that cannot charge a card twice or message a person twice, and it
is why these functions open and commit their own transactions rather than
joining a caller's.

The new question a batch asks is *how many*, and the answer is the schema's.
A header names its size once, at freeze; items hang off that number through a
composite foreign key and compare their own slot against it. A sixth recipient
is not a case this module rejects — it is a row PostgreSQL will not store.

The first unknown stops the suffix
----------------------------------
A batch is walked in slot order, and the moment one slot's outcome cannot be
proven the header is halted and every later slot is left untouched. Not a
policy that could be forgotten: a halted header refuses the next plan, so the
remaining slots cannot be claimed until a human has looked at the one that
went wrong.

Its own tables, not §36's or §37.2's
------------------------------------
The machinery is theirs, proven twice. The identity is not: those ledgers are
singletons by construction, and widening either of them to hold five rows would
mean weakening the very constraint that makes it a singleton.

Monotonic, and defended against a stale writer
----------------------------------------------
Every write names the states it is valid FROM; the row is locked with
``SELECT ... FOR UPDATE`` and compared before it is touched. Every status also
carries a rank, and a write that would lower it is refused. Together they mean a
reconciliation that read the world a moment too early cannot walk a stage back
into a state the same request could be claimed from again.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    BATCH_SCHEMA_VERSION,
    BATCH_SCOPE,
    UNIT_PRICE_MINOR,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import voucher_code_matches
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    VOUCHER_BATCH_COMPLETED,
    VOUCHER_BATCH_FROZEN,
    VOUCHER_BATCH_HALTED,
    VOUCHER_BATCH_IN_PROGRESS,
    VOUCHER_BATCH_ITEM_AMBIGUOUS,
    VOUCHER_BATCH_ITEM_CREATE_CLAIMED,
    VOUCHER_BATCH_ITEM_CREATE_REJECTED,
    VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
    VOUCHER_BATCH_ITEM_CREATED,
    VOUCHER_BATCH_ITEM_DELIVERED,
    VOUCHER_BATCH_ITEM_MANUALLY_CLEANED,
    VOUCHER_BATCH_ITEM_PAID,
    VOUCHER_BATCH_ITEM_PAY_CLAIMED,
    VOUCHER_BATCH_ITEM_PAY_REJECTED,
    VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
    VOUCHER_BATCH_ITEM_PLANNED,
    VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_BATCH_ITEM_READ,
    VOUCHER_BATCH_ITEM_REFUND_CLAIMED,
    VOUCHER_BATCH_ITEM_REFUND_REJECTED,
    VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
    VOUCHER_BATCH_ITEM_REFUNDED,
    VOUCHER_BATCH_ITEM_SEND_CLAIMED,
    VOUCHER_BATCH_ITEM_SEND_REJECTED,
    VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
    VOUCHER_BATCH_ITEM_STATUSES,
    CampaignRecipient,
    CampaignRun,
    EasyWeekVoucherSnapshotBatch,
    EasyWeekVoucherSnapshotBatchAttempt,
    EasyWeekVoucherSnapshotBatchItem,
)
from altegio_bot.utils import utcnow

# The MAC domain of this phase. Shares §36's secret and §37.2's primitive, with
# its own label, so a MAC written by any of the three can never verify another's
# code even under an identical key.
VOUCHER_BATCH_DOMAIN: Final = b"altegio_bot/easyweek_voucher_batch/voucher_code/v1"

# How far along its own life a slot sits. A write that would lower this rank is
# a regression and is refused, whoever sent it and however late it arrives.
# `ambiguous` sits at the top because it is a full stop.
ITEM_RANK: Final[dict[str, int]] = {
    VOUCHER_BATCH_ITEM_PLANNED: 5,
    VOUCHER_BATCH_ITEM_CREATE_CLAIMED: 10,
    VOUCHER_BATCH_ITEM_CREATE_UNKNOWN: 11,
    VOUCHER_BATCH_ITEM_CREATE_REJECTED: 12,
    VOUCHER_BATCH_ITEM_CREATED: 20,
    VOUCHER_BATCH_ITEM_PAY_CLAIMED: 30,
    VOUCHER_BATCH_ITEM_PAY_UNKNOWN: 31,
    VOUCHER_BATCH_ITEM_PAY_REJECTED: 32,
    VOUCHER_BATCH_ITEM_PAID: 40,
    VOUCHER_BATCH_ITEM_REFUND_CLAIMED: 41,
    VOUCHER_BATCH_ITEM_REFUND_UNKNOWN: 42,
    VOUCHER_BATCH_ITEM_REFUND_REJECTED: 43,
    VOUCHER_BATCH_ITEM_REFUNDED: 45,
    VOUCHER_BATCH_ITEM_SEND_CLAIMED: 50,
    VOUCHER_BATCH_ITEM_SEND_REJECTED: 51,
    VOUCHER_BATCH_ITEM_SEND_UNKNOWN: 52,
    VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED: 60,
    VOUCHER_BATCH_ITEM_DELIVERED: 70,
    VOUCHER_BATCH_ITEM_READ: 80,
    VOUCHER_BATCH_ITEM_MANUALLY_CLEANED: 90,
    VOUCHER_BATCH_ITEM_AMBIGUOUS: 99,
}

# A stage may be claimed only from these states. `*_rejected` is re-claimable
# for the two EasyWeek mutations, because a proven validation refusal did not
# act. The SEND has no such entry: one attempt, ever.
CREATE_CLAIMABLE_FROM: Final = frozenset({VOUCHER_BATCH_ITEM_PLANNED, VOUCHER_BATCH_ITEM_CREATE_REJECTED})
PAY_CLAIMABLE_FROM: Final = frozenset({VOUCHER_BATCH_ITEM_CREATED, VOUCHER_BATCH_ITEM_PAY_REJECTED})
SEND_CLAIMABLE_FROM: Final = frozenset({VOUCHER_BATCH_ITEM_PAID})
# A refund is claimable from `pay_unknown` too. The money has probably moved and
# the artifact could not be proven — which is precisely when getting it back
# matters most. It is still pre-send only, and a CHECK constraint says so
# independently.
REFUND_CLAIMABLE_FROM: Final = frozenset(
    {VOUCHER_BATCH_ITEM_PAID, VOUCHER_BATCH_ITEM_PAY_UNKNOWN, VOUCHER_BATCH_ITEM_REFUND_REJECTED}
)

# States in which something may still have reached EasyWeek or Meta.
UNRESOLVED_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_BATCH_ITEM_CREATE_CLAIMED,
        VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
        VOUCHER_BATCH_ITEM_PAY_CLAIMED,
        VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
        VOUCHER_BATCH_ITEM_SEND_CLAIMED,
        VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
        VOUCHER_BATCH_ITEM_REFUND_CLAIMED,
        VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
        VOUCHER_BATCH_ITEM_AMBIGUOUS,
    }
)

# States in which a message may already be in a customer's hands. A refund is
# forbidden from every one of them, by plan, by claim and by CHECK constraint.
SENT_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_BATCH_ITEM_SEND_CLAIMED,
        VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
        VOUCHER_BATCH_ITEM_SEND_REJECTED,
        VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
        VOUCHER_BATCH_ITEM_DELIVERED,
        VOUCHER_BATCH_ITEM_READ,
    }
)

# Where a slot's life legitimately ends. A batch whose every slot is here, with
# nothing outstanding, is finished.
TERMINAL_ITEM_STATUSES: Final = frozenset(
    {
        VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
        VOUCHER_BATCH_ITEM_DELIVERED,
        VOUCHER_BATCH_ITEM_READ,
        VOUCHER_BATCH_ITEM_SEND_REJECTED,
        VOUCHER_BATCH_ITEM_REFUNDED,
        VOUCHER_BATCH_ITEM_MANUALLY_CLEANED,
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
            "provider_accepted_at": self.provider_accepted_at,
            "delivered_at": self.delivered_at,
            "read_at": self.read_at,
            "refund_verified_at": self.refund_verified_at,
            "evidence": dict(self.evidence or {}),
        }


@dataclass(frozen=True)
class BatchSnapshot:
    """The header and every slot of it, as a report may print them."""

    exists: bool
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
    reconciliation_required: bool = False
    frozen_at: str | None = None
    items: tuple[ItemSnapshot, ...] = ()
    evidence: dict[str, Any] | None = None

    def item(self, slot: int) -> ItemSnapshot | None:
        for entry in self.items:
            if entry.slot == slot:
                return entry
        return None

    @property
    def halted(self) -> bool:
        return self.status == VOUCHER_BATCH_HALTED

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "exists": self.exists,
            "batch_scope": BATCH_SCOPE if self.exists else None,
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
            "campaign_period_start": self.campaign_period_start,
            "campaign_period_end": self.campaign_period_end,
            "frozen_digest": self.frozen_digest,
            "frozen_at": self.frozen_at,
            "recipient_count": self.recipient_count,
            "voucher_unit_price_minor": self.voucher_unit_price_minor,
            "total_exposure_minor": self.total_exposure_minor,
            "reconciliation_required": self.reconciliation_required,
            "items": [entry.as_safe_dict() for entry in self.items],
            "evidence": dict(self.evidence or {}),
        }


def _item_snapshot(row: EasyWeekVoucherSnapshotBatchItem) -> ItemSnapshot:
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


async def _header(session: AsyncSession, *, for_update: bool = False) -> EasyWeekVoucherSnapshotBatch | None:
    statement = select(EasyWeekVoucherSnapshotBatch).where(EasyWeekVoucherSnapshotBatch.batch_scope == BATCH_SCOPE)
    if for_update:
        statement = statement.with_for_update()
    return (await session.execute(statement)).scalar_one_or_none()


async def _items(
    session: AsyncSession, *, batch_id: int, for_update: bool = False
) -> list[EasyWeekVoucherSnapshotBatchItem]:
    statement = (
        select(EasyWeekVoucherSnapshotBatchItem)
        .where(EasyWeekVoucherSnapshotBatchItem.batch_id == batch_id)
        .order_by(EasyWeekVoucherSnapshotBatchItem.slot.asc())
    )
    if for_update:
        statement = statement.with_for_update()
    return list((await session.execute(statement)).scalars().all())


async def _snapshot(session: AsyncSession, row: EasyWeekVoucherSnapshotBatch | None) -> BatchSnapshot:
    if row is None:
        return BatchSnapshot(exists=False)
    items = await _items(session, batch_id=row.id)
    return BatchSnapshot(
        exists=True,
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
        reconciliation_required=bool(row.reconciliation_required),
        frozen_at=_iso(row.frozen_at),
        items=tuple(_item_snapshot(entry) for entry in items),
        evidence=dict(row.evidence or {}),
    )


async def load(session_maker: async_sessionmaker[AsyncSession]) -> BatchSnapshot:
    """Read the batch without locking it. For status and plan only."""
    async with session_maker() as session:
        return await _snapshot(session, await _header(session))


async def batch_id_of(session_maker: async_sessionmaker[AsyncSession]) -> int | None:
    """The one batch's primary key, or ``None`` before the freeze.

    Deliberately not on the snapshot: the snapshot is a PII-free report that
    gets signed into digests, and a row id there would be one more thing a
    reader might be tempted to address something by. The one caller that needs
    it is the entitlement check, which has to be able to say "some OTHER batch".
    """
    async with session_maker() as session:
        header = await _header(session)
        return int(header.id) if header is not None else None


@dataclass(frozen=True)
class BatchItemIdentity:
    """The immutable identity of one slot, as the freeze wrote it."""

    slot: int
    campaign_recipient_id: int
    easyweek_customer_uuid: str
    reconciliation_marker: str


@dataclass(frozen=True)
class BatchIdentity:
    """The immutable identity one batch is frozen with."""

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

    @property
    def recipient_count(self) -> int:
        return len(self.items)

    def matches(self, snapshot: BatchSnapshot) -> bool:
        """Is the batch this process is about to act on the batch it planned for?

        Compared field by field rather than by a digest alone, so a mismatch
        cannot be mistaken for a drifted plan. Reported as a boolean; no value
        is echoed.

        The frozen digest is part of it: it covers the composition, so an
        operator who edited the preview between the plan and the apply changes
        it and the stage refuses before anything leaves the process.
        """
        if not snapshot.exists:
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


async def preview_is_locked_by_voucher_batch(session: AsyncSession, *, campaign_run_id: int) -> bool:
    """Has the §41 batch frozen itself onto this preview run?

    Once it has, the snapshot stops being editable. Not tidiness: the batch
    addresses each recipient by run id and recipient id and re-proves the whole
    composition live before every external step. An operator who edits or
    discards the preview after CREATE or PAY does not undo anything — they make
    the DELIVER and the REFUND unprovable, leaving real €15 orders with no way
    to finish them and no way to take them back.

    Takes the caller's session so the check happens under the same lock as the
    edit it guards.
    """
    found = await session.scalar(
        select(EasyWeekVoucherSnapshotBatch.id)
        .where(EasyWeekVoucherSnapshotBatch.campaign_run_id == campaign_run_id)
        .where(EasyWeekVoucherSnapshotBatch.provider == PROVIDER_EASYWEEK)
        .limit(1)
    )
    return found is not None


@dataclass(frozen=True)
class FreezeOutcome:
    applied: bool
    reason: str
    snapshot: BatchSnapshot


async def freeze_batch(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    freeze_plan_digest: str,
) -> FreezeOutcome:
    """Write the ONE batch header and its slots, atomically, or refuse.

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

    A second batch is refused here and, independently, by a unique constraint
    and a CHECK that pins the scope to a single literal. This function is the
    polite refusal; the schema is the one that cannot be talked round.
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

            existing = await _header(session, for_update=True)
            if existing is not None:
                # Idempotent for the exact same composition, refused for any
                # other: a resumed crash must find its own batch, and a second
                # batch must find a wall.
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

            now = utcnow()
            count = identity.recipient_count
            header = EasyWeekVoucherSnapshotBatch(
                batch_scope=BATCH_SCOPE,
                request_schema_version=BATCH_SCHEMA_VERSION,
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
                status=VOUCHER_BATCH_FROZEN,
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
                    EasyWeekVoucherSnapshotBatchItem(
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
                        status=VOUCHER_BATCH_ITEM_PLANNED,
                        evidence={},
                        created_at=now,
                        updated_at=now,
                    )
                )
            await session.flush()
            return FreezeOutcome(True, FREEZE_APPLIED, await _snapshot(session, header))


async def _claim(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
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
    """Lock the header and the slot, check everything, stamp, commit.

    In that order, and all of it before any caller may touch the network. The
    identity is re-checked here and not only in the plan, because the plan was
    built before the lock existed — and a halted batch is refused here too, so
    the suffix of a batch that already went wrong cannot be claimed by a caller
    that read the header a moment too early.

    ``allow_halted`` exists for exactly one caller: the refund. A halt is
    precisely when an untouched paid slot most needs its money back, so the
    cleanup path must not be shut by the condition that makes it necessary.
    """
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            header = await _header(session, for_update=True)
            if header is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            snapshot = await _snapshot(session, header)
            if not identity.matches(snapshot):
                return ClaimOutcome(False, CLAIM_REFUSED_IDENTITY, header.status)
            if header.status == VOUCHER_BATCH_HALTED and not allow_halted:
                return ClaimOutcome(False, CLAIM_REFUSED_HALTED, header.status)

            row = (
                await session.execute(
                    select(EasyWeekVoucherSnapshotBatchItem)
                    .where(EasyWeekVoucherSnapshotBatchItem.batch_id == header.id)
                    .where(EasyWeekVoucherSnapshotBatchItem.slot == slot)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
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

            if header.status == VOUCHER_BATCH_FROZEN:
                header.status = VOUCHER_BATCH_IN_PROGRESS
            header.reconciliation_required = True
            header.updated_at = now
            await session.flush()
            return ClaimOutcome(True, CLAIM_GRANTED, next_status)


async def claim_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
    slot: int,
    plan_digest: str,
    create_window_start: datetime,
    create_window_end: datetime,
) -> ClaimOutcome:
    """Reserve the right to send the ONE create POST for this slot."""
    return await _claim(
        session_maker,
        identity=identity,
        slot=slot,
        claimable_from=CREATE_CLAIMABLE_FROM,
        next_status=VOUCHER_BATCH_ITEM_CREATE_CLAIMED,
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
    slot: int,
    plan_digest: str,
) -> ClaimOutcome:
    """Reserve the right to send the ONE payment POST for this slot."""
    return await _claim(
        session_maker,
        identity=identity,
        slot=slot,
        claimable_from=PAY_CLAIMABLE_FROM,
        next_status=VOUCHER_BATCH_ITEM_PAY_CLAIMED,
        plan_digest=plan_digest,
        digest_field="pay_plan_digest",
        claimed_field="pay_claimed_at",
        attempted_field="pay_attempted_at",
    )


async def claim_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    identity: BatchIdentity,
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
        slot=slot,
        claimable_from=REFUND_CLAIMABLE_FROM,
        next_status=VOUCHER_BATCH_ITEM_REFUND_CLAIMED,
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
            header = await _header(session, for_update=True)
            if header is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            snapshot = await _snapshot(session, header)
            if not identity.matches(snapshot):
                return ClaimOutcome(False, CLAIM_REFUSED_IDENTITY, header.status)
            if header.status == VOUCHER_BATCH_HALTED:
                return ClaimOutcome(False, CLAIM_REFUSED_HALTED, header.status)

            row = (
                await session.execute(
                    select(EasyWeekVoucherSnapshotBatchItem)
                    .where(EasyWeekVoucherSnapshotBatchItem.batch_id == header.id)
                    .where(EasyWeekVoucherSnapshotBatchItem.slot == slot)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return ClaimOutcome(False, CLAIM_REFUSED_MISSING_ROW, None)
            if row.status not in SEND_CLAIMABLE_FROM or int(row.send_attempt_count or 0) != 0:
                return ClaimOutcome(False, CLAIM_REFUSED_STATE, row.status)

            row.deliver_plan_digest = plan_digest
            row.live_guard_reproven_at = live_guard_reproven_at
            row.outbound_intent_uuid = intent
            row.send_claimed_at = now
            row.send_attempted_at = now
            row.send_attempt_count = 1
            row.status = VOUCHER_BATCH_ITEM_SEND_CLAIMED
            row.reason_code = None
            row.reconciliation_required = True
            row.updated_at = now
            session.add(
                EasyWeekVoucherSnapshotBatchAttempt(
                    item_id=row.id,
                    intent_uuid=intent,
                    template_code=template_code,
                    meta_template_name=meta_template_name,
                    template_language=template_language,
                    sender_id=sender_id,
                    campaign_recipient_id=row.campaign_recipient_id,
                    slot=row.slot,
                    outcome="claimed",
                    claimed_at=now,
                )
            )
            if header.status == VOUCHER_BATCH_FROZEN:
                header.status = VOUCHER_BATCH_IN_PROGRESS
            header.reconciliation_required = True
            header.updated_at = now
            await session.flush()
            return ClaimOutcome(True, CLAIM_GRANTED, VOUCHER_BATCH_ITEM_SEND_CLAIMED, str(intent))


@dataclass(frozen=True)
class RecordOutcome:
    applied: bool
    reason: str
    snapshot: BatchSnapshot


def _settle_header(header: EasyWeekVoucherSnapshotBatch, items: list[EasyWeekVoucherSnapshotBatchItem]) -> None:
    """Recompute the header from what its slots actually say.

    The header is never told what to be; it is derived. Deriving it is what
    keeps "may the next stage start?" from drifting away from the rows that
    answer it.

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
    unresolved = [row for row in items if row.status in UNRESOLVED_ITEM_STATUSES]
    header.reconciliation_required = any(row.reconciliation_required for row in items)
    if unresolved:
        header.status = VOUCHER_BATCH_HALTED
        # The CHECK constraint requires a halt to say why; the first unresolved
        # slot in slot order is the one that stopped the batch.
        header.halted_reason_code = unresolved[0].reason_code or unresolved[0].status
        if header.halted_at is None:
            header.halted_at = utcnow()
        return
    header.halted_reason_code = None
    header.halted_at = None
    if items and all(row.status in TERMINAL_ITEM_STATUSES for row in items):
        header.status = VOUCHER_BATCH_COMPLETED
    else:
        header.status = VOUCHER_BATCH_IN_PROGRESS


async def record_item_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    *,
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
    is stale is told so and gets the current snapshot back instead of
    overwriting somebody else's newer state.

    On top of that a write that would LOWER the rank is refused outright. That
    is what stops a slow original response, or a reconciliation that read a
    moment too early, from walking a claimed stage back to somewhere the same
    request could be claimed again.

    Timestamps are only ever set here, never cleared: an ``attempted_at`` is a
    fact about something that left this process.
    """
    if status not in VOUCHER_BATCH_ITEM_STATUSES:
        raise ValueError("unknown voucher batch item status")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            header = await _header(session, for_update=True)
            if header is None:
                return RecordOutcome(False, RECORD_MISSING_ROW, BatchSnapshot(exists=False))
            rows = await _items(session, batch_id=header.id, for_update=True)
            row = next((entry for entry in rows if entry.slot == slot), None)
            if row is None:
                return RecordOutcome(False, RECORD_MISSING_ROW, await _snapshot(session, header))
            if row.status not in expected_statuses:
                return RecordOutcome(False, RECORD_STALE_STATE, await _snapshot(session, header))
            if ITEM_RANK[status] < ITEM_RANK[row.status]:
                return RecordOutcome(False, RECORD_WOULD_REGRESS, await _snapshot(session, header))

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
                        select(EasyWeekVoucherSnapshotBatchAttempt).where(
                            EasyWeekVoucherSnapshotBatchAttempt.intent_uuid == row.outbound_intent_uuid
                        )
                    )
                ).scalar_one_or_none()
                if attempt is not None:
                    attempt.outcome = attempt_outcome
                    attempt.reason_code = reason_code
                    attempt.provider_message_id = provider_message_id
                    attempt.completed_at = now
            row.updated_at = now
            _settle_header(header, rows)
            header.updated_at = now
            await session.flush()
            return RecordOutcome(True, RECORD_APPLIED, await _snapshot(session, header))


async def binding_matches(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    slot: int,
    voucher_code: str,
    target_order_uuid: str,
) -> bool:
    """Is this code the one this slot's stored binding was made from?

    Lives here rather than in the runner because the MAC must not travel: the
    snapshot a report is built from carries only "a binding exists", and moving
    the digest itself into a caller would put verification material into
    something printable.

    False — never an exception — for a missing binding, a rotated key or a
    mismatch, so every caller fails closed on the same branch.
    """
    async with session_maker() as session:
        header = await _header(session)
        if header is None:
            return False
        rows = await _items(session, batch_id=header.id)
        row = next((entry for entry in rows if entry.slot == slot), None)
        if row is None or row.voucher_code_hmac is None or row.hmac_key_id is None:
            return False
        if _text(row.target_order_uuid) != target_order_uuid:
            return False
        return voucher_code_matches(
            voucher_code=voucher_code,
            expected_mac=row.voucher_code_hmac,
            expected_key_id=row.hmac_key_id,
            # The slot is inside the bound material, so a MAC written for one
            # slot cannot verify another slot's code even within one batch.
            ledger_uuid=f"{BATCH_SCOPE}:{row.slot}",
            target_order_uuid=target_order_uuid,
            voucher_template_uuid=_text(header.voucher_template_uuid) or "",
            domain=VOUCHER_BATCH_DOMAIN,
        )


async def _row_by_message(session: AsyncSession, provider_message_id: str) -> EasyWeekVoucherSnapshotBatchItem | None:
    """The one slot Meta's identifier names, locked. Matched on nothing else."""
    if not provider_message_id:
        return None
    return (
        await session.execute(
            select(EasyWeekVoucherSnapshotBatchItem)
            .where(EasyWeekVoucherSnapshotBatchItem.provider_message_id == provider_message_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _apply_webhook_row(
    session: AsyncSession,
    row: EasyWeekVoucherSnapshotBatchItem | None,
    *,
    provider_message_id: str,
    status: str,
    now: datetime,
) -> RecordOutcome:
    if row is None or not row.provider_message_id:
        return RecordOutcome(False, RECORD_MISSING_ROW, BatchSnapshot(exists=False))
    if row.provider_message_id != provider_message_id:
        return RecordOutcome(False, RECORD_STALE_STATE, BatchSnapshot(exists=False))
    if ITEM_RANK[status] < ITEM_RANK[row.status]:
        # A duplicate, or a callback that arrived out of order. Acceptance is
        # what Meta said and a later webhook cannot unsay it.
        return RecordOutcome(False, RECORD_WOULD_REGRESS, BatchSnapshot(exists=False))
    if row.provider_accepted_at is None:
        # A callback can land beside the commit that recorded acceptance. The
        # identifier only exists because Meta answered with it, so the callback
        # itself is the proof — and refusing here would silently lose a status.
        row.provider_accepted_at = now
    if row.delivered_at is None:
        # Read implies delivered. Recording read without it would leave a row
        # the database itself refuses, so this same callback stamps both.
        row.delivered_at = now
    if status == VOUCHER_BATCH_ITEM_READ and row.read_at is None:
        row.read_at = now
    row.status = status
    row.reconciliation_required = False
    # A delivered or read message means the voucher reached its person: there is
    # no open draft left for anybody to close by hand. Forced rather than left
    # alone, because the flag was set back when the order was still a draft.
    row.manual_cleanup_required = False
    row.updated_at = now

    header = await session.get(EasyWeekVoucherSnapshotBatch, row.batch_id, with_for_update=True)
    if header is not None:
        rows = await _items(session, batch_id=header.id, for_update=True)
        _settle_header(header, rows)
        header.updated_at = now
    await session.flush()
    snapshot = await _snapshot(session, header) if header is not None else BatchSnapshot(exists=False)
    return RecordOutcome(True, RECORD_APPLIED, snapshot)


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
    """
    if status not in (VOUCHER_BATCH_ITEM_DELIVERED, VOUCHER_BATCH_ITEM_READ):
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
            return RecordOutcome(False, RECORD_MISSING_ROW, BatchSnapshot(exists=False))
    return await _apply_webhook_row(session, row, provider_message_id=provider_message_id, status=status, now=now)


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
    if status not in (VOUCHER_BATCH_ITEM_DELIVERED, VOUCHER_BATCH_ITEM_READ):
        raise ValueError("unsupported webhook transition")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row_by_message(session, provider_message_id)
            return await _apply_webhook_row(
                session, row, provider_message_id=provider_message_id, status=status, now=now
            )


async def resettle(session_maker: async_sessionmaker[AsyncSession]) -> BatchSnapshot:
    """Re-derive the header from its slots and return the result.

    Used by ``reconcile`` after it has written whatever a readback proved: the
    halt is not something a human clears by hand, it is what the slots currently
    say, so lifting it is a consequence of resolving them.
    """
    async with session_maker() as session:
        async with session.begin():
            header = await _header(session, for_update=True)
            if header is None:
                return BatchSnapshot(exists=False)
            rows = await _items(session, batch_id=header.id, for_update=True)
            _settle_header(header, rows)
            header.updated_at = utcnow()
            await session.flush()
            return await _snapshot(session, header)


__all__ = [
    "CREATE_CLAIMABLE_FROM",
    "ITEM_RANK",
    "PAY_CLAIMABLE_FROM",
    "REFUND_CLAIMABLE_FROM",
    "SENT_ITEM_STATUSES",
    "SEND_CLAIMABLE_FROM",
    "TERMINAL_ITEM_STATUSES",
    "UNRESOLVED_ITEM_STATUSES",
    "VOUCHER_BATCH_DOMAIN",
    "BatchIdentity",
    "BatchItemIdentity",
    "BatchSnapshot",
    "ClaimOutcome",
    "FreezeOutcome",
    "ItemSnapshot",
    "RecordOutcome",
    "apply_webhook_transition",
    "batch_id_of",
    "binding_matches",
    "claim_create",
    "claim_pay",
    "claim_refund",
    "claim_send",
    "freeze_batch",
    "load",
    "preview_is_locked_by_voucher_batch",
    "record_item_outcome",
    "record_webhook_transition",
    "resettle",
]
