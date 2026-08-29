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
    row.run_id = run_id
    row.status = status
    if target_booking_uuid is not None:
        row.target_booking_uuid = target_booking_uuid
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
) -> None:
    """A proven creation. This is the only place a target UUID is ever stored."""
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_CREATED,
        target_booking_uuid=target_booking_uuid,
        reason_code=None,
        count_attempt=True,
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
) -> None:
    """The target booking was cancelled by a confirmed rollback.

    The target UUID is deliberately KEPT. An operator asking "what did the
    rollback touch?" six months later needs the identifier, and clearing it would
    make the answer unrecoverable.
    """
    await _finalize(
        session,
        run_id=run_id,
        source_company_id=source_company_id,
        source_record_id=source_record_id,
        status=STATUS_ROLLED_BACK,
        target_booking_uuid=None,
        reason_code=None,
        count_attempt=False,
    )


async def rows_for_run(
    session: AsyncSession,
    *,
    run_id: str,
    statuses: tuple[str, ...] | None = None,
) -> list[EasyWeekMigrationLedger]:
    """Every ledger row a given run last touched, optionally filtered by status."""
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
        "status": row.status,
        "attempts": row.attempts,
        "reason_code": row.reason_code,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
