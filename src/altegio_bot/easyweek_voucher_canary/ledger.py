"""The durable claim ledger of the voucher mutation canary (§35).

Every stage of the canary is a real, irreversible-by-repetition mutation, and
each of them can end with the request sent and the answer lost. This module owns
the rule that makes that survivable:

    the claim is committed BEFORE the request leaves.

So a crash anywhere after the commit — before the socket, mid-flight, after the
response — reads identically afterwards: *claimed, outcome unknown*. That is the
only reading that cannot double-charge a real card, and it is why these
functions open and commit their own transactions rather than joining a caller's.

Monotonic, and defended against a stale writer
----------------------------------------------
A reconciliation running while a POST is still in flight, or a slow original
response landing after a reconciliation already moved on, must never walk the
state backwards into somewhere the same POST could be claimed again. Two
independent guards enforce that:

* every write names the states it is valid FROM, and the row is locked and
  compared before it is touched — a compare-and-set, not a blind update;
* every status carries a rank, and a write that would lower it is refused.

Together they mean a stale `created` cannot overwrite `pay_claimed`, a stale
`paid` cannot overwrite `refund_claimed`, and no timestamp already recorded is
ever cleared.

Re-claiming
-----------
A stage may be re-claimed from exactly one state: ``*_rejected``. That state is
reserved for the responses this API's own transport proved did not act — its
validation refusals — and a fresh attempt after one cannot duplicate anything.
Everything else — claimed, unknown, done — refuses, and the way forward is
reconciliation by reading, never another POST. A re-claim is never automatic: it
needs a new plan, a new digest, a new confirmation and another ``--apply``.

Bound to one identity
---------------------
The row records the salted fingerprints of the customer, staffer and account it
was opened with, and every claim re-checks them UNDER THE ROW LOCK. The plan
checks them too, but the plan was built before the lock existed. Pointing the
environment at a different customer, staffer or account of the same branch
passes every branch, template and workspace proof there is, and would still be
a real payment on somebody else.
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

# How far along the canary a status sits. A write that would lower this rank is
# a regression and is refused, whoever sent it and however late it arrives.
# `ambiguous` sits at the top because it is a full stop: nothing overwrites it.
STATUS_RANK: Final = {
    STATUS_CREATE_CLAIMED: 10,
    STATUS_CREATE_UNKNOWN: 11,
    STATUS_CREATE_REJECTED: 12,
    STATUS_CREATED: 20,
    STATUS_PAY_CLAIMED: 30,
    STATUS_PAY_UNKNOWN: 31,
    STATUS_PAY_REJECTED: 32,
    STATUS_PAID: 40,
    STATUS_REFUND_CLAIMED: 50,
    STATUS_REFUND_UNKNOWN: 51,
    STATUS_REFUND_REJECTED: 52,
    STATUS_REFUNDED: 60,
    STATUS_MANUALLY_CLEANED: 61,
    STATUS_AMBIGUOUS: 99,
}

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
# The environment's identity is not the one this row was opened with. Says only
# that; never which role, never a fingerprint, never a UUID.
CLAIM_REFUSED_IDENTITY_DRIFT: Final = "refused_identity_drift"

IDENTITY_ROLES: Final = ("customer", "staffer", "account")

RECORD_APPLIED: Final = "applied"
RECORD_STALE_STATE: Final = "stale_state"
RECORD_WOULD_REGRESS: Final = "would_regress"
RECORD_MISSING_ROW: Final = "missing_row"


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
    template_config_digest: str | None
    stage_plan_digests: dict[str, str | None]
    # The salted fingerprints this row was opened with, for comparison against
    # the runtime identity. Held so every later stage can prove it is still
    # acting on the same customer, staffer and account — and reported only as
    # the boolean below, because a comparison result is all an operator needs.
    identity_fingerprints: dict[str, str | None]
    reconciliation_marker: str | None
    create_window_start: datetime | None
    create_window_end: datetime | None
    stage_timestamps: dict[str, str | None]
    stage_counters: dict[str, Any]
    manual_cleanup_observed_at: str | None
    evidence: dict[str, Any]

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ledger_row_exists": self.exists,
            "status": self.status,
            "reason_code": self.reason_code,
            # Operationally necessary but never printed: a refund and a manual
            # dashboard cleanup are impossible without it.
            "target_order_uuid_known": self.target_order_uuid is not None,
            "template_config_digest": self.template_config_digest,
            "stage_plan_digests": dict(self.stage_plan_digests),
            "identity_fingerprints_recorded": all(self.identity_fingerprints.get(role) for role in IDENTITY_ROLES),
            "reconciliation_marker": self.reconciliation_marker,
            "create_window_start": self.create_window_start.isoformat() if self.create_window_start else None,
            "create_window_end": self.create_window_end.isoformat() if self.create_window_end else None,
            "stage_timestamps": dict(self.stage_timestamps),
            "stage_counters": dict(self.stage_counters),
            "manual_cleanup_observed_at": self.manual_cleanup_observed_at,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class RecordOutcome:
    """Whether one result write landed, and what the row says now."""

    applied: bool
    reason: str
    snapshot: LedgerSnapshot


def _snapshot(row: EasyWeekVoucherCanaryLedger | None) -> LedgerSnapshot:
    if row is None:
        return LedgerSnapshot(
            exists=False,
            status=None,
            reason_code=None,
            target_order_uuid=None,
            template_config_digest=None,
            stage_plan_digests={},
            identity_fingerprints={},
            reconciliation_marker=None,
            create_window_start=None,
            create_window_end=None,
            stage_timestamps={},
            stage_counters={},
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
        template_config_digest=row.template_config_digest,
        stage_plan_digests={
            "create": row.create_plan_digest,
            "pay": row.pay_plan_digest,
            "refund": row.refund_plan_digest,
        },
        identity_fingerprints={
            "customer": row.customer_fingerprint,
            "staffer": row.staffer_fingerprint,
            "account": row.account_fingerprint,
        },
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
        stage_counters=dict(row.stage_counters or {}),
        manual_cleanup_observed_at=_iso(row.manual_cleanup_observed_at),
        evidence=dict(row.evidence or {}),
    )


def _identity_matches(row: EasyWeekVoucherCanaryLedger, fingerprints: dict[str, str]) -> bool:
    """Is this row's recorded identity the one the caller is acting with?

    Re-checked here, under the row lock, and not only in the plan: the plan was
    built before the lock existed, and a claim is the last moment at which the
    process can still refuse. The comparison is over salted fingerprints, and
    its only output is this boolean.
    """
    stored = {
        "customer": row.customer_fingerprint,
        "staffer": row.staffer_fingerprint,
        "account": row.account_fingerprint,
    }
    return all(stored[role] == fingerprints.get(role) for role in IDENTITY_ROLES)


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
    create_plan_digest: str,
    template_config_digest: str,
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
                    template_config_digest=template_config_digest,
                    create_plan_digest=create_plan_digest,
                    pay_plan_digest=None,
                    refund_plan_digest=None,
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
                    stage_counters={},
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
            # A re-claim must be authorised by the SAME frozen configuration. A
            # rejected create under a drifted template is a new decision.
            if row.template_config_digest != template_config_digest:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_PLAN_DRIFT, status=row.status)
            if not _identity_matches(
                row,
                {
                    "customer": customer_fingerprint,
                    "staffer": staffer_fingerprint,
                    "account": account_fingerprint,
                },
            ):
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_IDENTITY_DRIFT, status=row.status)
            row.status = STATUS_CREATE_CLAIMED
            row.reason_code = None
            row.create_plan_digest = create_plan_digest
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
    template_config_digest: str,
    identity_fingerprints: dict[str, str],
    stage_plan_digest: str,
    digest_field: str,
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
            if row.template_config_digest != template_config_digest:
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_PLAN_DRIFT, status=row.status)
            if not _identity_matches(row, identity_fingerprints):
                return ClaimOutcome(granted=False, reason=CLAIM_REFUSED_IDENTITY_DRIFT, status=row.status)
            setattr(row, digest_field, stage_plan_digest)
            setattr(row, claimed_field, now)
            setattr(row, attempted_field, now)
            row.status = next_status
            row.reason_code = None
            row.updated_at = now
            return ClaimOutcome(granted=True, reason=CLAIM_GRANTED, status=next_status)


async def claim_pay(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    pay_plan_digest: str,
    template_config_digest: str,
    identity_fingerprints: dict[str, str],
) -> ClaimOutcome:
    """Reserve the ONE payment. Only a proven created order may be paid for."""
    return await _claim_stage(
        session_maker,
        claimable_from=PAY_CLAIMABLE_FROM,
        next_status=STATUS_PAY_CLAIMED,
        template_config_digest=template_config_digest,
        identity_fingerprints=identity_fingerprints,
        stage_plan_digest=pay_plan_digest,
        digest_field="pay_plan_digest",
        claimed_field="pay_claimed_at",
        attempted_field="pay_attempted_at",
    )


async def claim_refund(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    refund_plan_digest: str,
    template_config_digest: str,
    identity_fingerprints: dict[str, str],
) -> ClaimOutcome:
    """Reserve the ONE refund. Only a proven paid order may be refunded."""
    return await _claim_stage(
        session_maker,
        claimable_from=REFUND_CLAIMABLE_FROM,
        next_status=STATUS_REFUND_CLAIMED,
        template_config_digest=template_config_digest,
        identity_fingerprints=identity_fingerprints,
        stage_plan_digest=refund_plan_digest,
        digest_field="refund_plan_digest",
        claimed_field="refund_claimed_at",
        attempted_field="refund_attempted_at",
    )


async def record_outcome(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    status: str,
    expected_statuses: frozenset[str],
    reason_code: str | None = None,
    target_order_uuid: str | None = None,
    verified_field: str | None = None,
    evidence: dict[str, Any] | None = None,
    stage_counters: dict[str, Any] | None = None,
    manual_cleanup_observed: bool = False,
) -> RecordOutcome:
    """Write what a stage turned out to be — as a compare-and-set, not a blind update.

    ``expected_statuses`` names every state this write is valid FROM. The row is
    locked, compared, and only then touched; a caller whose view of the world is
    stale is told so and gets the current snapshot back instead of overwriting
    somebody else's newer state.

    On top of that, a write that would LOWER the status rank is refused outright.
    That is what stops a slow original response, or a reconciliation that read
    the remote order a moment too early, from walking `pay_claimed` back to
    `created` — which would be a state the same POST could be claimed from again.

    Timestamps are only ever set here, never cleared: an ``attempted_at`` is a
    fact about something that left this process, and no later observation can
    make it untrue.
    """
    if status not in ALL_STATUSES:
        raise ValueError("unknown canary status")
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
            if verified_field is not None:
                setattr(row, verified_field, now)
            if manual_cleanup_observed:
                row.manual_cleanup_observed_at = now
            if evidence:
                merged = dict(row.evidence or {})
                merged.update(evidence)
                row.evidence = merged
            if stage_counters:
                merged_counters = dict(row.stage_counters or {})
                merged_counters.update(stage_counters)
                row.stage_counters = merged_counters
            row.updated_at = now
            await session.flush()
            return RecordOutcome(applied=True, reason=RECORD_APPLIED, snapshot=_snapshot(row))
