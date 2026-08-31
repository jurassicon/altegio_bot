"""Durable storage for the service expectation a wave was reviewed against.

One table, one row per ``(location, service)``, and two operations: establish it
once, read it back forever. There is deliberately no update.

Why it has to survive a restart
-------------------------------
The migration proves a booking's service by its attributes, because EasyWeek
gives back an order-line uuid rather than a catalogue one (plan §28). An
attribute check is only worth anything if the attributes it compares against are
the ones an operator reviewed — and the first version kept them nowhere. Every
command re-derived them from the current catalogue, so:

* renaming a service between the canary and the bulk produced a *new*
  expectation that the *new* catalogue satisfied by construction, and the old
  canary went on licensing the wave;
* a restart lost the expectation entirely, and the next run quietly invented a
  fresh one.

Writing it down turns the chain ``reviewed dry-run → canary → apply →
reconcile / resolve-created / rollback`` into one expectation instead of four
independently re-derived ones.

What this is not
----------------
Not a catalogue history: no versions, no audit trail, no superseded rows. Not a
manifest snapshot. Not somewhere a lost baseline can be regenerated from — a
missing row is a named refusal, because rebuilding it from today's catalogue is
precisely the circularity the table exists to break.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_migration.service_catalog import ServiceBaseline
from altegio_bot.models.models import EasyWeekMigrationServiceBaseline

logger = logging.getLogger("easyweek_migration.baseline")

BASELINE_ESTABLISHED: Final = "service_baseline_established"
BASELINE_ALREADY_SET: Final = "service_baseline_already_established"


def _as_baseline(row: EasyWeekMigrationServiceBaseline) -> ServiceBaseline:
    return ServiceBaseline(
        easyweek_location_uuid=row.easyweek_location_uuid,
        easyweek_service_uuid=row.easyweek_service_uuid,
        normalized_name=row.canonical_name,
        currency=row.currency,
        price_minor=row.price_minor,
        duration_minutes=row.duration_minutes,
        method=row.proof_method,
        version=row.proof_version,
    )


async def load_baselines(
    session: AsyncSession, *, location_uuids: tuple[str, ...]
) -> dict[tuple[str, str], ServiceBaseline]:
    """Every stored expectation for the given locations, keyed by identity."""
    if not location_uuids:
        return {}
    stmt = select(EasyWeekMigrationServiceBaseline).where(
        EasyWeekMigrationServiceBaseline.easyweek_location_uuid.in_(location_uuids)
    )
    rows = (await session.execute(stmt)).scalars().all()
    return {(row.easyweek_location_uuid, row.easyweek_service_uuid): _as_baseline(row) for row in rows}


async def get_baseline(session: AsyncSession, *, location_uuid: str, service_uuid: str) -> ServiceBaseline | None:
    """One stored expectation, or ``None`` if this service has never had one."""
    stmt = select(EasyWeekMigrationServiceBaseline).where(
        EasyWeekMigrationServiceBaseline.easyweek_location_uuid == location_uuid,
        EasyWeekMigrationServiceBaseline.easyweek_service_uuid == service_uuid,
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    return _as_baseline(row) if row is not None else None


async def establish(
    session: AsyncSession,
    baseline: ServiceBaseline,
    *,
    run_id: str,
    wave_identity: str | None,
) -> tuple[ServiceBaseline, str]:
    """Write an expectation down if the service has none, and return what stands.

    ``ON CONFLICT DO NOTHING`` and then a read, rather than an upsert. The
    difference matters: if two runs race, or if this run is simply wrong about
    the service having no baseline, the row that was already there **wins**. An
    upsert would let the newcomer overwrite a reviewed expectation with whatever
    today's catalogue says, which is the one thing this table exists to prevent.

    Returns the expectation now in force together with a stable code saying
    whether it was written here — the caller must verify the returned baseline,
    not the one it offered, because those may differ.
    """
    stmt = (
        pg_insert(EasyWeekMigrationServiceBaseline)
        .values(
            easyweek_location_uuid=baseline.easyweek_location_uuid,
            easyweek_service_uuid=baseline.easyweek_service_uuid,
            canonical_name=baseline.normalized_name,
            currency=baseline.currency,
            price_minor=baseline.price_minor,
            duration_minutes=baseline.duration_minutes,
            proof_method=baseline.method,
            proof_version=baseline.version,
            wave_identity=wave_identity,
            established_run_id=run_id,
        )
        .on_conflict_do_nothing(constraint="uq_easyweek_service_baseline_identity")
        .returning(EasyWeekMigrationServiceBaseline.id)
    )
    inserted = (await session.execute(stmt)).scalar_one_or_none()

    stored = await get_baseline(
        session,
        location_uuid=baseline.easyweek_location_uuid,
        service_uuid=baseline.easyweek_service_uuid,
    )
    assert stored is not None, "baseline row vanished between insert and read"

    if inserted is not None:
        # The digest, never the identifiers. A log line is not a report, and the
        # report is where location, service and run ids belong — the suite pins
        # that rule, and a new line is not a reason to bend it.
        logger.info("easyweek_migration: service baseline established digest=%s", stored.digest[:16])
        return stored, BASELINE_ESTABLISHED
    return stored, BASELINE_ALREADY_SET


def as_safe_dict(baselines: dict[tuple[str, str], ServiceBaseline]) -> list[dict[str, Any]]:
    """Report form: identifiers, numbers and digests. Never a service name."""
    return [baseline.as_safe_dict() for _key, baseline in sorted(baselines.items())]
