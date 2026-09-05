"""Durable idempotency for the cutover (PR-11.1).

The ledger answers one question, and it has to answer it after a crash, from
another machine, a week later: **has this Altegio record already been created in
EasyWeek?**

The answer is a row under a unique constraint on
``(source_provider, source_company_id, source_record_id)``. Not a set in memory,
not a line in a report — those do not survive the failure modes that matter.

Status vocabulary
-----------------
``pending``    claimed, and the mutation's outcome was never recorded. A process
               that died anywhere between the claim and its bookkeeping leaves
               this — and the POST may well have been *sent*. So ``pending`` is
               an UNKNOWN outcome, exactly like ``uncertain``, and is treated as
               one everywhere.
``created``    proven: EasyWeek answered 2xx and named a booking UUID.
``uncertain``  a mutation was sent and its outcome is unknown. This status is
               the whole reason the ledger exists. It never becomes ``created``
               by assumption and never gets retried automatically; ``reconcile``
               resolves it by *reading* EasyWeek.
``failed``     a definite, proven failure — the request was rejected before
               anything was created. Safe to try again once the cause is fixed.
``rolled_back`` a confirmed rollback cancelled the target booking.

The row's insert happens **before** the mutation, not after. That ordering is the
crash guarantee: a process killed around the POST leaves a row an operator can
find, rather than no row at all and a booking nobody knows about.

Only ``failed`` is ever re-claimable, because it is the only status where the
request provably never reached EasyWeek. Everything else could correspond to a
booking that exists, and re-claiming it would be the duplicate this whole
design exists to prevent.

A *blocked* booking has no status here at all, deliberately. Blocking is a
conclusion about the current source and mapping, recomputed from scratch on
every run; storing it would only create rows that go stale the moment an
operator fixes the manifest. It lives in the report, not in the ledger.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_migration.classify import LedgerView
from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK, EasyWeekMigrationLedger

logger = logging.getLogger("easyweek_migration.ledger")

STATUS_PENDING: Final = "pending"
STATUS_CREATED: Final = "created"
STATUS_UNCERTAIN: Final = "uncertain"
STATUS_FAILED: Final = "failed"
STATUS_ROLLED_BACK: Final = "rolled_back"

# Prefix of the marker written into the EasyWeek booking's comment field. Stable
# across runs and derived only from source identity, so it is greppable in the
# EasyWeek UI and carries nothing about the customer.
MARKER_PREFIX: Final = "altegio-migration"


def migration_marker(*, source_company_id: int, source_record_id: int) -> str:
    """A stable technical reference for one migrated booking.

    ``altegio-migration:758285:1234567``. Deliberately not the run id: a booking
    is identified by what it came *from*, and that does not change when a second
    run touches the same row. Deliberately not a phone, a name or a comment the
    customer wrote — this string is visible to salon staff in the EasyWeek UI.
    """
    return f"{MARKER_PREFIX}:{source_company_id}:{source_record_id}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def load_ledger_views(
    session: AsyncSession,
    *,
    company_ids: tuple[int, ...],
) -> dict[tuple[int, int], LedgerView]:
    """Load every known source row for *company_ids*, keyed by (company, record).

    Read in one query rather than per booking: a bulk run classifies hundreds of
    rows, and a round trip each would dominate a run that is already paced by the
    EasyWeek rate limit.
    """
    if not company_ids:
        return {}

    stmt = select(EasyWeekMigrationLedger).where(
        EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
        EasyWeekMigrationLedger.source_company_id.in_(company_ids),
    )
    rows = (await session.execute(stmt)).scalars().all()
    return {
        (row.source_company_id, row.source_record_id): LedgerView(
            status=row.status,
            target_booking_uuid=row.target_booking_uuid,
            source_fingerprint=row.source_fingerprint,
        )
        for row in rows
    }


async def claim_for_apply(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    source_fingerprint: str,
) -> bool:
    """Reserve this source booking BEFORE its mutation is sent.

    Returns ``True`` when the caller now owns the row and may issue the POST, and
    ``False`` when somebody already did — a rerun, a second operator, a resumed
    crash.

    The reservation is an ``INSERT ... ON CONFLICT DO NOTHING``, so the database
    arbitrates rather than a read-then-write race in the tool. This is the reason
    a concurrent second apply cannot double-book: only one INSERT wins, and only
    the winner is allowed to call EasyWeek.
    """
    now = _utcnow()
    stmt = (
        pg_insert(EasyWeekMigrationLedger)
        .values(
            source_provider=PROVIDER_ALTEGIO,
            source_company_id=source_company_id,
            source_record_id=source_record_id,
            source_fingerprint=source_fingerprint,
            target_provider=PROVIDER_EASYWEEK,
            target_booking_uuid=None,
            run_id=run_id,
            status=STATUS_PENDING,
            attempts=0,
            reason_code=None,
            created_at=now,
            updated_at=now,
        )
        .on_conflict_do_nothing(constraint="uq_easyweek_migration_ledger_source_identity")
        .returning(EasyWeekMigrationLedger.id)
    )
    claimed = (await session.execute(stmt)).scalar_one_or_none()
    if claimed is not None:
        return True

    # The row exists. Only `failed` may be re-claimed: there the request provably
    # never reached EasyWeek, so a fresh attempt cannot duplicate anything.
    #
    # `pending` is deliberately NOT in that list, and the distinction is the
    # whole crash story. A `pending` row means some process claimed the booking
    # and never came back to say what happened — and it may have died *after*
    # sending the POST. Re-claiming it would create a second appointment for a
    # real person. It goes to reconciliation instead, like `uncertain`.
    reclaimed = (
        await session.execute(
            update(EasyWeekMigrationLedger)
            .where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
                EasyWeekMigrationLedger.status == STATUS_FAILED,
            )
            .values(
                run_id=run_id,
                status=STATUS_PENDING,
                source_fingerprint=source_fingerprint,
                reason_code=None,
                updated_at=_utcnow(),
            )
            .returning(EasyWeekMigrationLedger.id)
        )
    ).scalar_one_or_none()
    return reclaimed is not None


async def _finalize(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    status: str,
    target_booking_uuid: str | None,
    reason_code: str | None,
    count_attempt: bool,
    target_snapshot_fingerprint: str | None = None,
) -> None:
    row = (
        await session.execute(
            select(EasyWeekMigrationLedger).where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise RuntimeError(f"migration ledger row vanished company_id={source_company_id} record_id={source_record_id}")
    # `run_id` is the ORIGIN run and is never rewritten here. A reconciliation or
    # a resolution that stamped its own id over it would remove the booking from
    # the rollback set of the apply that actually created it — the one run an
    # operator would reach for. Bookkeeping about a row must not rewrite the
    # row's origin.
    row.last_resolution_run_id = run_id
    row.status = status
    if target_booking_uuid is not None:
        row.target_booking_uuid = target_booking_uuid
    if target_snapshot_fingerprint is not None:
        row.target_snapshot_fingerprint = target_snapshot_fingerprint
    row.reason_code = reason_code
    if count_attempt:
        row.attempts = (row.attempts or 0) + 1
    row.updated_at = _utcnow()
    await session.flush()


async def record_created(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    target_booking_uuid: str,
    target_snapshot_fingerprint: str | None = None,
) -> None:
    """A proven creation. This is the only place a target UUID is ever stored.

    ``target_snapshot_fingerprint`` is the digest of the booking as written. It is
    what rollback later compares a live GET against, so a booking somebody moved
    or reassigned by hand is refused instead of cancelled.
    """
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_CREATED,
        target_booking_uuid=target_booking_uuid,
        reason_code=None,
        count_attempt=True,
        target_snapshot_fingerprint=target_snapshot_fingerprint,
    )


async def record_uncertain(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    reason_code: str,
) -> None:
    """The outcome is unknown. No target is claimed, and no retry is scheduled."""
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_UNCERTAIN,
        target_booking_uuid=None,
        reason_code=reason_code,
        count_attempt=True,
    )


async def record_failed(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    reason_code: str,
) -> None:
    """A proven failure: nothing was created, so a later run may try again."""
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_FAILED,
        target_booking_uuid=None,
        reason_code=reason_code,
        count_attempt=True,
    )


async def record_rolled_back(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    expected_attempt_run_id: str,
) -> bool:
    """The target booking was cancelled, by the attempt this call names.

    The target UUID is deliberately KEPT. An operator asking "what did the
    rollback touch?" six months later needs the identifier, and clearing it would
    make the answer unrecoverable.

    Conditional on the attempt it is finishing. `expected_attempt_run_id` is the
    run whose marker licensed this cancellation — the current run when it sent
    the PUT itself, or the earlier run whose unknown result this one just proved
    by reading the booking. If that marker is gone or now belongs to somebody
    else, the row is NOT finalised: the evidence this conclusion rested on moved
    while the conclusion was being drawn.

    Returns whether the row was recorded, so a caller that lost the race reports
    an unresolved row instead of a rollback it cannot stand behind.
    """
    recorded = (
        await session.execute(
            update(EasyWeekMigrationLedger)
            .where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
                EasyWeekMigrationLedger.status == STATUS_CREATED,
                EasyWeekMigrationLedger.rollback_attempt_run_id == expected_attempt_run_id,
            )
            .values(
                status=STATUS_ROLLED_BACK,
                last_resolution_run_id=run_id,
                reason_code=None,
                updated_at=_utcnow(),
            )
            .returning(EasyWeekMigrationLedger.id)
        )
    ).scalar_one_or_none()
    return recorded is not None


@dataclass(frozen=True)
class RollbackClaim:
    """Whether THIS run holds the right to send one cancel for one row."""

    won: bool
    # Why not, when it lost. A stable code, never a provider message.
    reason: str | None = None
    # The run that holds the attempt, when somebody else does. Reported so an
    # operator can find the run that has to be resolved first.
    owner_run_id: str | None = None
    status: str | None = None


# Stable, PII-free reasons a claim can fail.
CLAIM_ROW_MISSING: Final = "rollback_claim_row_missing"
CLAIM_HELD_BY_ANOTHER_RUN: Final = "rollback_claim_held_by_another_run"
CLAIM_ROW_CHANGED: Final = "rollback_claim_row_changed"


async def claim_rollback_attempt(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    origin_run_id: str,
    target_booking_uuid: str,
) -> RollbackClaim:
    """Take the exclusive right to send ONE cancel for this row, atomically.

    The mutation right is the row itself: a single conditional UPDATE both tests
    every precondition and takes ownership, so two runs walking the same wave
    cannot both conclude they may send a PUT. The previous version read the row
    and then wrote it, and two readers of a NULL marker both proceeded — two
    cancels for one appointment, with no idempotency key to make the second one
    safe.

    The WHERE clause carries every fact the decision rested on, not just the
    marker: the source identity, the ORIGIN run whose rollback this is, the
    status that made the row a candidate, and the exact target uuid that was
    proven. A row that moved between the candidate list and this statement
    therefore loses the claim instead of being mutated on stale evidence.

    Returns rather than raises: losing is an ordinary outcome with a report line
    of its own, and the caller must be able to tell "somebody else owns this"
    from "it vanished". Whatever the reason, a loser sends nothing.
    """
    claimed = (
        await session.execute(
            update(EasyWeekMigrationLedger)
            .where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
                EasyWeekMigrationLedger.run_id == origin_run_id,
                EasyWeekMigrationLedger.status == STATUS_CREATED,
                EasyWeekMigrationLedger.target_booking_uuid == target_booking_uuid,
                EasyWeekMigrationLedger.rollback_attempted_at.is_(None),
            )
            .values(
                rollback_attempted_at=_utcnow(),
                rollback_attempt_run_id=run_id,
                updated_at=_utcnow(),
            )
            .returning(EasyWeekMigrationLedger.id)
        )
    ).scalar_one_or_none()
    if claimed is not None:
        return RollbackClaim(won=True)

    # Lost. Say WHY from the row as it stands now — the caller re-reads the
    # target before deciding what to do, and an operator needs to know whether
    # to look for another run or for a changed booking.
    row = (
        await session.execute(
            select(EasyWeekMigrationLedger).where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return RollbackClaim(won=False, reason=CLAIM_ROW_MISSING)
    if row.rollback_attempted_at is not None:
        return RollbackClaim(
            won=False,
            reason=CLAIM_HELD_BY_ANOTHER_RUN,
            owner_run_id=row.rollback_attempt_run_id,
            status=row.status,
        )
    return RollbackClaim(won=False, reason=CLAIM_ROW_CHANGED, status=row.status)


async def release_rollback_attempt(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
) -> bool:
    """Give the mutation right back, and ONLY if this run still holds it.

    Called when it is proven that no unknown mutation exists: the read before
    the PUT failed, the booking turned out to be already cancelled, or the PUT
    itself came back with a deterministic refusal. Leaving the marker there
    would state that a cancel may be in flight, and a later cancellation by a
    person would then be credited to this tool.

    Conditional on ownership, so one run can never clear another run's marker —
    that marker is the only thing standing between an unresolved cancel and a
    second PUT.

    Returns whether the marker was actually released. `False` means somebody
    else owns it or it was already gone, and the caller must fail closed rather
    than assume a retry is safe.
    """
    released = (
        await session.execute(
            update(EasyWeekMigrationLedger)
            .where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
                EasyWeekMigrationLedger.rollback_attempt_run_id == run_id,
                EasyWeekMigrationLedger.rollback_attempted_at.is_not(None),
            )
            .values(rollback_attempted_at=None, rollback_attempt_run_id=None, updated_at=_utcnow())
            .returning(EasyWeekMigrationLedger.id)
        )
    ).scalar_one_or_none()
    return released is not None


async def rows_for_run(
    session: AsyncSession,
    *,
    run_id: str,
    statuses: tuple[str, ...] | None = None,
) -> list[EasyWeekMigrationLedger]:
    """Every ledger row a given run ORIGINATED, optionally filtered by status.

    Matches on ``run_id``, the origin, not on the last run to touch the row. That
    is what makes a rollback of one apply still find a booking whose uncertain
    outcome a later reconciliation resolved.
    """
    stmt = select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.run_id == run_id)
    if statuses:
        stmt = stmt.where(EasyWeekMigrationLedger.status.in_(statuses))
    stmt = stmt.order_by(EasyWeekMigrationLedger.source_company_id, EasyWeekMigrationLedger.source_record_id)
    return list((await session.execute(stmt)).scalars().all())


# Both statuses mean "a booking may exist and we cannot say". Reconciliation has
# to see both; an apply must refuse both.
UNRESOLVED_STATUSES: Final = (STATUS_UNCERTAIN, STATUS_PENDING)


async def uncertain_rows(session: AsyncSession) -> list[EasyWeekMigrationLedger]:
    """Every row with an unknown outcome, across all runs.

    Includes ``pending``: a claim whose result was never recorded is exactly as
    unresolved as an explicit ``uncertain``, and a crashed run leaves the former.

    Not scoped to one run on purpose: an unresolved row from last week is still
    an unresolved possible booking, and reconciliation must see it.
    """
    stmt = (
        select(EasyWeekMigrationLedger)
        .where(EasyWeekMigrationLedger.status.in_(UNRESOLVED_STATUSES))
        .order_by(EasyWeekMigrationLedger.source_company_id, EasyWeekMigrationLedger.source_record_id)
    )
    return list((await session.execute(stmt)).scalars().all())


def row_as_safe_dict(row: EasyWeekMigrationLedger) -> dict[str, Any]:
    """Ledger row → report entry. Every field here is already PII-free."""
    return {
        "source_provider": row.source_provider,
        "source_company_id": row.source_company_id,
        "source_record_id": row.source_record_id,
        "target_provider": row.target_provider,
        "target_booking_uuid": row.target_booking_uuid,
        "run_id": row.run_id,
        "last_resolution_run_id": row.last_resolution_run_id,
        "target_snapshot_fingerprint": row.target_snapshot_fingerprint,
        "status": row.status,
        "attempts": row.attempts,
        "reason_code": row.reason_code,
        "rollback_attempted_at": row.rollback_attempted_at.isoformat() if row.rollback_attempted_at else None,
        "rollback_attempt_run_id": row.rollback_attempt_run_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Operator resolution of an unknown outcome
# ---------------------------------------------------------------------------
# A timeout leaves `uncertain` with NO target uuid — the POST never returned one.
# Reconciliation cannot fetch a booking it cannot name, so without an operator
# path those rows stay unresolved forever, and every later apply refuses them.
#
# Both paths below are deliberately narrow, and neither ever issues a POST.


async def resolve_uncertain_as_created(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    target_booking_uuid: str,
    target_snapshot_fingerprint: str,
) -> None:
    """Record an operator-supplied target that the tool has already PROVEN.

    The caller must have fetched the booking and matched its marker, branch and
    write-critical fields first; this function only writes the verdict down. The
    origin ``run_id`` is preserved, so the booking stays inside the rollback set
    of the apply that created it.
    """
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_CREATED,
        target_booking_uuid=target_booking_uuid,
        reason_code=None,
        count_attempt=False,
        target_snapshot_fingerprint=target_snapshot_fingerprint,
    )


async def resolve_uncertain_as_absent(
    session: AsyncSession,
    *,
    run_id: str,
    source_company_id: int,
    source_record_id: int,
    reason_code: str,
) -> None:
    """Record that an operator checked EasyWeek and the booking is NOT there.

    This is the dangerous direction, and the danger is worth naming: it makes the
    row re-claimable, so the next apply will POST again. If the operator was
    wrong, the customer gets two appointments.

    Nothing here decides that on its own — the CLI requires a separate, explicit
    multi-step confirmation, and no automatic path ever reaches this function.
    """
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_FAILED,
        target_booking_uuid=None,
        reason_code=reason_code,
        count_attempt=False,
    )


async def get_row(
    session: AsyncSession,
    *,
    source_company_id: int,
    source_record_id: int,
) -> EasyWeekMigrationLedger | None:
    """One ledger row by its source identity, or ``None``."""
    return (
        await session.execute(
            select(EasyWeekMigrationLedger).where(
                EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
                EasyWeekMigrationLedger.source_company_id == source_company_id,
                EasyWeekMigrationLedger.source_record_id == source_record_id,
            )
        )
    ).scalar_one_or_none()


async def all_rows(
    session: AsyncSession,
    *,
    company_ids: tuple[int, ...],
) -> list[EasyWeekMigrationLedger]:
    """Every ledger row for the given source companies, ordered by identity."""
    if not company_ids:
        return []
    stmt = (
        select(EasyWeekMigrationLedger)
        .where(
            EasyWeekMigrationLedger.source_provider == PROVIDER_ALTEGIO,
            EasyWeekMigrationLedger.source_company_id.in_(company_ids),
        )
        .order_by(EasyWeekMigrationLedger.source_company_id, EasyWeekMigrationLedger.source_record_id)
    )
    return list((await session.execute(stmt)).scalars().all())
