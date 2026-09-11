"""The durable claim ledger of the voucher mutation canary (§35).

Every stage of the canary is a real, irreversible-by-repetition mutation, and
each of them can end with the request sent and the answer lost. This module owns
the one rule that makes that survivable:

    the claim is committed BEFORE the request leaves.

So a crash anywhere after the commit — before the socket, mid-flight, after the
response — reads identically afterwards: *claimed, outcome unknown*. That is the
only reading that cannot double-charge a real card, and it is why these
functions open and commit their own transactions rather than joining a caller's.

Re-claiming
-----------
A stage may be re-claimed from exactly one state: ``*_rejected``. A permanent
4xx means the server declined before acting, so nothing happened and a fresh
attempt cannot duplicate anything. Everything else — claimed, unknown, done —
refuses, and the way forward is reconciliation by reading, never another POST.

Concurrency
-----------
Two operators running the same stage at the same time meet in PostgreSQL: the
create claim is an ``INSERT ... ON CONFLICT DO NOTHING``, and every other claim
takes ``SELECT ... FOR UPDATE`` before it checks the transition. The loser is
told it lost; it never gets to send.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_voucher_identity import (
    VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
    VOUCHER_CANARY_SCOPE,
)
from altegio_bot.models.models import EasyWeekVoucherCanaryLedger
from altegio_bot.utils import utcnow

# The closed state vocabulary, mirrored by a CHECK constraint in the database.
STATUS_CREATE_CLAIMED: Final = "create_claimed"
STATUS_CREATE_UNKNOWN: Final = "create_unknown"
STATUS_CREATE_REJECTED: Final = "create_rejected"
STATUS_CREATED: Final = "created"
STATUS_PAY_CLAIMED: Final = "pay_claimed"
STATUS_PAY_UNKNOWN: Final = "pay_unknown"
STATUS_PAY_REJECTED: Final = "pay_rejected"
STATUS_PAID: Final = "paid"
STATUS_REFUND_CLAIMED: Final = "refund_claimed"
STATUS_REFUND_UNKNOWN: Final = "refund_unknown"
STATUS_REFUND_REJECTED: Final = "refund_rejected"
STATUS_REFUNDED: Final = "refunded"
STATUS_AMBIGUOUS: Final = "ambiguous"
STATUS_MANUALLY_CLEANED: Final = "manually_cleaned"

ALL_STATUSES: Final = (
    STATUS_CREATE_CLAIMED,
    STATUS_CREATE_UNKNOWN,
    STATUS_CREATE_REJECTED,
    STATUS_CREATED,
    STATUS_PAY_CLAIMED,
    STATUS_PAY_UNKNOWN,
    STATUS_PAY_REJECTED,
    STATUS_PAID,
    STATUS_REFUND_CLAIMED,
    STATUS_REFUND_UNKNOWN,
    STATUS_REFUND_REJECTED,
    STATUS_REFUNDED,
    STATUS_AMBIGUOUS,
    STATUS_MANUALLY_CLEANED,
)

# A stage may be claimed only from these states. `*_rejected` is the single
# re-claimable one: a permanent 4xx provably did not act.
CREATE_CLAIMABLE_FROM: Final = frozenset({STATUS_CREATE_REJECTED})
PAY_CLAIMABLE_FROM: Final = frozenset({STATUS_CREATED, STATUS_PAY_REJECTED})
REFUND_CLAIMABLE_FROM: Final = frozenset({STATUS_PAID, STATUS_REFUND_REJECTED})

# States in which a mutation may still have reached EasyWeek and been lost.
UNRESOLVED_STATUSES: Final = frozenset(
    {
        STATUS_CREATE_CLAIMED,
        STATUS_CREATE_UNKNOWN,
        STATUS_PAY_CLAIMED,
        STATUS_PAY_UNKNOWN,
        STATUS_REFUND_CLAIMED,
        STATUS_REFUND_UNKNOWN,
    }
)

CLAIM_GRANTED: Final = "granted"
CLAIM_REFUSED_STATE: Final = "refused_state"
CLAIM_REFUSED_PLAN_DRIFT: Final = "refused_plan_drift"


@dataclass(frozen=True)
class ClaimOutcome:
    """Whether this process now owns the right to send one request."""

    granted: bool
    reason: str
    status: str | None

    @property
    def may_send(self) -> bool:
        return self.granted


@dataclass(frozen=True)
class LedgerSnapshot:
    """A PII-free view of the ledger row, safe to print."""

    exists: bool
    status: str | None
    reason_code: str | None
    target_order_uuid: str | None
    plan_digest: str | None
    template_snapshot_digest: str | None
    reconciliation_marker: str | None
    create_window_start: datetime | None
    create_window_end: datetime | None
    stage_timestamps: dict[str, str | None]
    manual_cleanup_observed_at: str | None
    evidence: dict[str, Any]

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ledger_row_exists": self.exists,
            "status": self.status,
            "reason_code": self.reason_code,
            # Operationally necessary: a refund and a manual dashboard cleanup
            # are impossible without it. It is the only identifier here.
            "target_order_uuid_known": self.target_order_uuid is not None,
            "plan_digest": self.plan_digest,
            "template_snapshot_digest": self.template_snapshot_digest,
            "reconciliation_marker": self.reconciliation_marker,
            "create_window_start": self.create_window_start.isoformat() if self.create_window_start else None,
            "create_window_end": self.create_window_end.isoformat() if self.create_window_end else None,
            "stage_timestamps": dict(self.stage_timestamps),
            "manual_cleanup_observed_at": self.manual_cleanup_observed_at,
            "evidence": dict(self.evidence),
        }


def _snapshot(row: EasyWeekVoucherCanaryLedger | None) -> LedgerSnapshot:
    if row is None:
        return LedgerSnapshot(
            exists=False,
            status=None,
            reason_code=None,
            target_order_uuid=None,
            plan_digest=None,
            template_snapshot_digest=None,
            reconciliation_marker=None,
            create_window_start=None,
            create_window_end=None,
            stage_timestamps={},
            manual_cleanup_observed_at=None,
            evidence={},
        )

    def _iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    return LedgerSnapshot(
        exists=True,
        status=row.status,
        reason_code=row.reason_code,
        target_order_uuid=str(row.target_order_uuid) if row.target_order_uuid is not None else None,
        plan_digest=row.plan_digest,
        template_snapshot_digest=row.template_snapshot_digest,
        reconciliation_marker=row.reconciliation_marker,
        create_window_start=row.create_window_start,
        create_window_end=row.create_window_end,
        stage_timestamps={
            "create_claimed_at": _iso(row.create_claimed_at),
            "create_attempted_at": _iso(row.create_attempted_at),
            "create_verified_at": _iso(row.create_verified_at),
            "pay_claimed_at": _iso(row.pay_claimed_at),
            "pay_attempted_at": _iso(row.pay_attempted_at),
            "pay_verified_at": _iso(row.pay_verified_at),
            "refund_claimed_at": _iso(row.refund_claimed_at),
            "refund_attempted_at": _iso(row.refund_attempted_at),
            "refund_verified_at": _iso(row.refund_verified_at),
        },
        manual_cleanup_observed_at=_iso(row.manual_cleanup_observed_at),
        evidence=dict(row.evidence or {}),
    )


async def _row(session: AsyncSession, *, for_update: bool = False) -> EasyWeekVoucherCanaryLedger | None:
    stmt = select(EasyWeekVoucherCanaryLedger).where(EasyWeekVoucherCanaryLedger.canary_scope == VOUCHER_CANARY_SCOPE)
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def load(session_maker: async_sessionmaker[AsyncSession]) -> LedgerSnapshot:
    """Read-only. Safe to run at any time, including while a stage is unresolved."""
    async with session_maker() as session:
        return _snapshot(await _row(session))


async def claim_create(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    plan_digest: str,
    template_snapshot_digest: str,
    customer_fingerprint: str,
    staffer_fingerprint: str,
    account_fingerprint: str,
    reconciliation_marker: str,
    create_window_start: datetime,
    create_window_end: datetime,
) -> ClaimOutcome:
    """Reserve the right to send the ONE create POST, and commit before sending.

    The insert is ``ON CONFLICT DO NOTHING``, so the database — not a
    read-then-write race in this process — decides which of two simultaneous
    operators owns the canary. Only the winner may call EasyWeek.

    ``create_attempted_at`` is stamped together with the claim, in the same
    committed transaction, on purpose. After this commit a crash before the
    socket and a crash after the response are indistinguishable, so both have to
    read as "the request may have gone out".
    """
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            stmt = (
                pg_insert(EasyWeekVoucherCanaryLedger)
                .values(
                    canary_scope=VOUCHER_CANARY_SCOPE,
                    request_schema_version=VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
                    plan_digest=plan_digest,
                    template_snapshot_digest=template_snapshot_digest,
                    customer_fingerprint=customer_fingerprint,
                    staffer_fingerprint=staffer_fingerprint,
                    account_fingerprint=account_fingerprint,
                    reconciliation_marker=reconciliation_marker,
                    status=STATUS_CREATE_CLAIMED,
                    reason_code=None,
                    target_order_uuid=None,
                    create_window_start=create_window_start,
                    create_window_end=create_window_end,
                    create_claimed_at=now,
                    create_attempted_at=now,
                    evidence={},
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_nothing(constraint="uq_easyweek_voucher_canary_scope")
                .returning(EasyWeekVoucherCanaryLedger.id)
            )
            if (await session.execute(stmt)).scalar_one_or_none() is not None:
                return ClaimOutcome(granted=True, reason=CLAIM_GRANTED, status=STATUS_CREATE_CLAIMED)

            row = await _row(session, for_update=True)
            if row is None:  # pragma: no cover - the conflict proves it exists
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_STATE, status=None)
            if row.status not in CREATE_CLAIMABLE_FROM:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_STATE, status=row.status)
            # A re-claim must be authorised by the SAME plan. A rejected create
            # under a stale plan is a new decision, not a resumed one.
            if row.plan_digest != plan_digest or row.template_snapshot_digest != template_snapshot_digest:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_PLAN_DRIFT, status=row.status)
            row.status = STATUS_CREATE_CLAIMED
            row.reason_code = None
            row.create_claimed_at = now
            row.create_attempted_at = now
            row.create_window_start = create_window_start
            row.create_window_end = create_window_end
            row.updated_at = now
            return ClaimOutcome(granted=True, reason=CLAIM_GRANTED, status=STATUS_CREATE_CLAIMED)


async def _claim_stage(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    claimable_from: frozenset[str],
    next_status: str,
    plan_digest: str,
    template_snapshot_digest: str,
    claimed_field: str,
    attempted_field: str,
) -> ClaimOutcome:
    """Lock the row, check the transition, stamp the claim, commit. In that order."""
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row(session, for_update=True)
            if row is None:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_STATE, status=None)
            if row.status not in claimable_from:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_STATE, status=row.status)
            if row.plan_digest != plan_digest or row.template_snapshot_digest != template_snapshot_digest:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_PLAN_DRIFT, status=row.status)
            setattr(row, claimed_field, now)
            setattr(row, attempted_field, now)
            row.status = next_status
            row.reason_code = None
            row.updated_at = now
            return ClaimOutcome(granted=True, reason=CLAIM_GRANTED, status=next_status)


async def claim_pay(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    plan_digest: str,
    template_snapshot_digest: str,
) -> ClaimOutcome:
    """Reserve the ONE payment. Only a proven created order may be paid for."""
    return await _claim_stage(
        session_maker,
        claimable_from=PAY_CLAIMABLE_FROM,
        next_status=STATUS_PAY_CLAIMED,
        plan_digest=plan_digest,
        template_snapshot_digest=template_snapshot_digest,
        claimed_field="pay_claimed_at",
        attempted_field="pay_attempted_at",
    )


async def claim_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    plan_digest: str,
    template_snapshot_digest: str,
) -> ClaimOutcome:
    """Reserve the ONE refund. Only a proven paid order may be refunded."""
    return await _claim_stage(
        session_maker,
        claimable_from=REFUND_CLAIMABLE_FROM,
        next_status=STATUS_REFUND_CLAIMED,
        plan_digest=plan_digest,
        template_snapshot_digest=template_snapshot_digest,
        claimed_field="refund_claimed_at",
        attempted_field="refund_attempted_at",
    )


async def record_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    status: str,
    reason_code: str | None = None,
    target_order_uuid: str | None = None,
    verified_field: str | None = None,
    evidence: dict[str, Any] | None = None,
    manual_cleanup_observed: bool = False,
) -> LedgerSnapshot:
    """Write what a stage turned out to be, after the request is over.

    Deliberately the only writer of ``target_order_uuid`` and of the evidence
    blob. ``evidence`` is merged rather than replaced so a later stage's
    observation never erases an earlier one — the whole research value of the
    canary is the sequence.
    """
    if status not in ALL_STATUSES:
        raise ValueError("unknown canary status")
    now = utcnow()
    async with session_maker() as session:
        async with session.begin():
            row = await _row(session, for_update=True)
            if row is None:
                raise RuntimeError("voucher canary ledger row is missing")
            row.status = status
            row.reason_code = reason_code
            if target_order_uuid is not None:
                row.target_order_uuid = uuid_module.UUID(target_order_uuid)
            if verified_field is not None:
                setattr(row, verified_field, now)
            if manual_cleanup_observed:
                row.manual_cleanup_observed_at = now
            if evidence:
                merged = dict(row.evidence or {})
                merged.update(evidence)
                row.evidence = merged
            row.updated_at = now
            await session.flush()
            return _snapshot(row)
