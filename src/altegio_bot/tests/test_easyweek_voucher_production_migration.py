"""Migration contract for the §42 production mailing tables (PR-19).

Runs upgrade → downgrade → upgrade on a THROWAWAY database, never production.
Skips when no disposable PostgreSQL is reachable, and FAILS instead of skipping
when the environment declares the check mandatory (``ALTEGIO_REQUIRE_MIGTEST=1``),
so a green build can never hide a migration that was silently never exercised.

What it proves, beyond "the DDL runs"
-------------------------------------
Almost everything §42 promises about identity and money is a database
constraint, and a constraint that exists only in the ORM is a constraint
production does not have. So this module applies the migration and then tries,
in SQL, to write the rows the phase says are impossible: a batch whose approved
count does not match its composition, an approved exposure that is not the
product, a second batch on one preview, a zero-recipient batch, a slot outside
its own batch's declared size, a €14 voucher, a second delivery attempt, a
refund after a send, and one customer twice in one campaign period.

It also proves the two things a reviewer is most likely to want checked about a
phase that follows a singleton one:

* the migration creates exactly the three NEW tables and leaves the §35, §36,
  §37.2 and §41 tables — including rows already in them — untouched, in both
  directions;
* there is deliberately **no** upper bound on ``recipient_count`` here, while
  §41's cap of five is still in force on §41's own table.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from altegio_bot.models.models import (
    VOUCHER_PRODUCTION_SCOPE,
    VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
    Base,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = _REPO_ROOT / "alembic.ini"

PR19_REVISION = "e2c7b4f16a83"
PR19_PARENT_REVISION = "d7b2f6a4c318"
# The §41 migration. It must stay in the chain and it is no longer the head.
PR18_REVISION = "b3f7c2a90d14"

BATCHES = "easyweek_voucher_production_batches"
ITEMS = "easyweek_voucher_production_batch_items"
ATTEMPTS = "easyweek_voucher_production_batch_attempts"
PR19_TABLES = (BATCHES, ITEMS, ATTEMPTS)

# The §§35–41 tables this revision must not touch, in either direction.
UNTOUCHED_TABLES = (
    "easyweek_voucher_canary_ledger",
    "easyweek_campaign_voucher_delivery_ledger",
    "easyweek_campaign_voucher_delivery_attempts",
    "easyweek_manual_voucher_delivery_ledger",
    "easyweek_manual_voucher_delivery_attempts",
    "easyweek_voucher_snapshot_batches",
    "easyweek_voucher_snapshot_batch_items",
    "easyweek_voucher_snapshot_batch_attempts",
)

_TEMP_DB_PREFIX = "altegio_pr19_migtest_"
_REMEDY = "Grant CREATEDB to the DATABASE_URL role, or point ALTEGIO_MIGTEST_DATABASE_URL at a disposable PostgreSQL."


def _unavailable(reason: str) -> None:
    if os.environ.get("ALTEGIO_REQUIRE_MIGTEST") == "1":
        pytest.fail(f"PR-19 migration test is required (ALTEGIO_REQUIRE_MIGTEST=1) but {reason}. {_REMEDY}")
    pytest.skip(reason)


def _server_url() -> str:
    raw = os.environ.get("ALTEGIO_MIGTEST_DATABASE_URL") or os.environ.get("DATABASE_URL") or ""
    if not raw:
        _unavailable("no DATABASE_URL configured")
    return raw


def _db_url(name: str) -> str:
    return _server_url().rsplit("/", 1)[0] + "/" + name


def _run_alembic(*args: str, db_url: str) -> subprocess.CompletedProcess:
    """Run Alembic in a subprocess pinned to *db_url*.

    A subprocess is required: ``alembic/env.py`` reads the settings singleton at
    import time and overrides ``sqlalchemy.url`` from it, so the URL has to be in
    the environment before Python starts.
    """
    env = dict(os.environ)
    env["DATABASE_URL"] = db_url
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ALEMBIC_INI), *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(_REPO_ROOT),
    )


def _alembic_ok(*args: str, db_url: str) -> str:
    result = _run_alembic(*args, db_url=db_url)
    assert result.returncode == 0, f"alembic {args} failed:\n{result.stdout}\n{result.stderr}"
    return result.stdout


@pytest_asyncio.fixture
async def temp_db_url():
    """Create and drop a disposable database. Never touches production."""
    name = _TEMP_DB_PREFIX + uuid.uuid4().hex[:12]
    assert name.startswith(_TEMP_DB_PREFIX)

    try:
        admin = create_async_engine(_server_url(), isolation_level="AUTOCOMMIT")
    except Exception as exc:  # pragma: no cover - configuration problem
        _unavailable(f"PostgreSQL not configured: {type(exc).__name__}")

    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as exc:
        await admin.dispose()
        _unavailable(f"cannot create a throwaway database: {type(exc).__name__}")

    try:
        yield _db_url(name)
    finally:
        async with admin.connect() as conn:
            await conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :name AND pid <> pg_backend_pid()"
                ),
                {"name": name},
            )
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        await admin.dispose()


async def _fetch(db_url: str, sql: str, params: dict | None = None) -> list[tuple]:
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql), params or {})
            return [tuple(row) for row in result]
    finally:
        await engine.dispose()


async def _execute(db_url: str, sql: str, params: dict | None = None) -> None:
    engine = create_async_engine(db_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text(sql), params or {})
    finally:
        await engine.dispose()


async def _refused(db_url: str, sql: str, params: dict | None = None) -> str:
    """Assert PostgreSQL refuses this write, and return the message it gave."""
    with pytest.raises(Exception) as excinfo:  # noqa: PT011 - the driver's own error type
        await _execute(db_url, sql, params)
    return str(excinfo.value)


async def _tables(db_url: str, like: str) -> set[str]:
    rows = await _fetch(
        db_url,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE :like",
        {"like": like},
    )
    return {row[0] for row in rows}


# ===========================================================================
# Graph shape
# ===========================================================================


def test_exactly_one_alembic_head() -> None:
    """One head — two would mean two lineages and a deploy that cannot upgrade.

    Deliberately NOT an assertion that PR-19 IS the head. It is the tip today
    and the next phase will add a child, at which point "this revision is the
    tip" would go red while saying nothing about this migration. Pinning it was
    the mistake this PR had to fix in the Chatwoot mirror test, and repeating it
    here would just move the same trap one PR down the line.

    What this phase actually needs is the single-head invariant plus its own
    revision being present and correctly parented, which the next two tests
    check directly.
    """
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    heads = script.get_heads()

    assert len(heads) == 1, f"expected exactly one Alembic head, got {heads}"
    assert PR19_REVISION in {revision.revision for revision in script.walk_revisions()}


def test_pr19_builds_on_the_deployed_head() -> None:
    """A direct child of the single head, never a fork from somewhere older."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert script.get_revision(PR19_REVISION).down_revision == PR19_PARENT_REVISION


def test_the_pr18_revision_is_still_in_the_chain() -> None:
    """§41's migration is history, not head. It must not have been removed."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    revisions = {revision.revision for revision in script.walk_revisions()}
    assert PR18_REVISION in revisions
    assert PR19_REVISION in revisions


def test_the_orm_and_the_migration_describe_the_same_tables() -> None:
    """Every §42 table the application expects is one this revision creates."""
    metadata_tables = {table.name for table in Base.metadata.sorted_tables}
    for table in PR19_TABLES:
        assert table in metadata_tables


# ===========================================================================
# Upgrade, downgrade, upgrade
# ===========================================================================


@pytest.mark.asyncio
async def test_upgrading_to_the_pr19_revision_creates_the_three_tables(temp_db_url: str) -> None:
    """§42's own migration, addressed by its own revision rather than by "head".

    It used to upgrade to ``head`` and assert that §42's three tables were the only
    ``easyweek_voucher_production%`` ones. That was true while §42 WAS the head and
    stopped being true the moment a later phase added a revision — which is exactly
    the trap §42's runbook warns about for operators. Naming the revision keeps this
    test about the migration it is about, for every phase after it.

    The chain still has to have exactly one head; that is asserted separately.
    """
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert len(script.get_heads()) == 1

    _alembic_ok("upgrade", PR19_REVISION, db_url=temp_db_url)
    current = _alembic_ok("current", db_url=temp_db_url)

    assert PR19_REVISION in current
    assert await _tables(temp_db_url, "easyweek_voucher_production%") == set(PR19_TABLES)


@pytest.mark.asyncio
async def test_the_migration_matches_the_orm_exactly(temp_db_url: str) -> None:
    """Column for column, constraint for constraint, index for index.

    A hand-written migration that drifts from the ORM is the failure mode this
    catches: the application would then rely on a CHECK production does not
    have, which is precisely the situation every constraint here exists to
    prevent.
    """
    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    query_columns = (
        "SELECT t.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull "
        "FROM pg_attribute a JOIN pg_class t ON t.oid = a.attrelid "
        "WHERE t.relname LIKE 'easyweek_voucher_production%' AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY 1, 2"
    )
    query_constraints = (
        "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
        "WHERE t.relname LIKE 'easyweek_voucher_production%' ORDER BY 1"
    )
    query_indexes = "SELECT indexname FROM pg_indexes WHERE tablename LIKE 'easyweek_voucher_production%' ORDER BY 1"
    migrated = (
        await _fetch(temp_db_url, query_columns),
        await _fetch(temp_db_url, query_constraints),
        await _fetch(temp_db_url, query_indexes),
    )

    # The same schema, built from the ORM metadata in its own database.
    orm_name = _TEMP_DB_PREFIX + "orm_" + uuid.uuid4().hex[:8]
    admin = create_async_engine(_server_url(), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{orm_name}"'))
    except Exception as exc:  # pragma: no cover - permissions
        await admin.dispose()
        _unavailable(f"cannot create a second throwaway database: {type(exc).__name__}")
    orm_url = _db_url(orm_name)
    try:
        engine = create_async_engine(orm_url)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        finally:
            await engine.dispose()
        from_orm = (
            await _fetch(orm_url, query_columns),
            await _fetch(orm_url, query_constraints),
            await _fetch(orm_url, query_indexes),
        )
    finally:
        async with admin.connect() as conn:
            await conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :name AND pid <> pg_backend_pid()"
                ),
                {"name": orm_name},
            )
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{orm_name}"'))
        await admin.dispose()

    assert migrated[0] == from_orm[0], "columns differ between the migration and the ORM"
    assert migrated[1] == from_orm[1], "constraints differ between the migration and the ORM"
    assert migrated[2] == from_orm[2], "indexes differ between the migration and the ORM"


@pytest.mark.asyncio
async def test_downgrade_removes_only_the_new_objects(temp_db_url: str) -> None:
    """Exactly the three new tables go, and every historical ledger stays.

    Bounded to §42's revision rather than to ``head``: a later phase's tables are
    not this migration's to remove, and upgrading past it would make "before minus
    after" describe two migrations at once.
    """
    _alembic_ok("upgrade", PR19_REVISION, db_url=temp_db_url)
    before = await _tables(temp_db_url, "easyweek_%voucher%")

    _alembic_ok("downgrade", PR19_PARENT_REVISION, db_url=temp_db_url)
    after = await _tables(temp_db_url, "easyweek_%voucher%")

    assert await _tables(temp_db_url, "easyweek_voucher_production%") == set()
    assert before - after == set(PR19_TABLES)
    for table in UNTOUCHED_TABLES:
        assert table in after

    # And it goes back up cleanly, so a rollback is not a one-way door.
    _alembic_ok("upgrade", PR19_REVISION, db_url=temp_db_url)
    assert await _tables(temp_db_url, "easyweek_voucher_production%") == set(PR19_TABLES)
    assert _alembic_ok("current", db_url=temp_db_url).count(PR19_REVISION) == 1


async def _seed_run_and_recipients(db_url: str, *, count: int, phone_offset: int = 0) -> tuple[int, list[int]]:
    """One completed preview and *count* manually selected candidates."""
    await _execute(
        db_url,
        "INSERT INTO campaign_runs (provider, campaign_code, mode, company_ids, status, "
        " period_start, period_end, meta) "
        "VALUES ('easyweek', 'new_clients_monthly', 'preview', '[322579]'::jsonb, 'completed', "
        " '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', '{}'::jsonb)",
    )
    run_id = (await _fetch(db_url, "SELECT id FROM campaign_runs ORDER BY id DESC LIMIT 1"))[0][0]
    recipient_ids: list[int] = []
    for index in range(count):
        await _execute(
            db_url,
            "INSERT INTO campaign_recipients (provider, campaign_run_id, company_id, phone_e164, status, "
            " recipient_basis, easyweek_customer_uuid, meta, service_titles_in_period, cleanup_card_ids) "
            "VALUES ('easyweek', :run, 322579, :phone, 'candidate', 'operator_manual_selection', "
            " :customer, '{}'::jsonb, '[]'::jsonb, '[]'::jsonb)",
            {
                "run": run_id,
                "phone": f"+4900000{phone_offset + index:05d}",
                "customer": str(uuid.UUID(int=(0xAB << 96) | (phone_offset + index))),
            },
        )
        recipient_ids.append(
            (await _fetch(db_url, "SELECT id FROM campaign_recipients ORDER BY id DESC LIMIT 1"))[0][0]
        )
    return run_id, recipient_ids


_BATCH_INSERT = (
    "INSERT INTO easyweek_voucher_production_batches "
    "(batch_scope, request_schema_version, baseline_version, provider, company_id, campaign_code, "
    " recipient_basis, campaign_run_id, campaign_period_start, campaign_period_end, location_uuid, "
    " staffer_uuid, payment_account_uuid, voucher_template_uuid, frozen_digest, recipient_count, "
    " voucher_unit_price_minor, total_exposure_minor, approved_recipient_count, approved_exposure_minor, "
    " status, frozen_at, evidence) "
    "VALUES (:scope, '1', '2026-09-27-43', 'easyweek', 322579, 'new_clients_monthly', "
    " 'operator_manual_selection', :run, '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', "
    " '22222222-2222-4222-8222-222222222222', '33333333-3333-4333-8333-333333333333', "
    " '44444444-4444-4444-8444-444444444444', '55555555-5555-4555-8555-555555555555', "
    " 'digest', :count, :price, :total, :approved_count, :approved_total, 'frozen', now(), '{}'::jsonb)"
)


async def _batch_insert_sql(db_url: str) -> str:
    """Seed schema 1 on either its original revision or the current head.

    At head the separately stored issue price is mandatory. Supplying the old
    paid amounts lets the tests reach their intended financial CHECK rather
    than fail earlier on a missing new column. Old-revision seeds stay exact.
    """
    has_issue_price = await _fetch(
        db_url,
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
        "WHERE table_name='easyweek_voucher_production_batches' "
        "AND column_name='voucher_issue_price_minor')",
    )
    if not has_issue_price[0][0]:
        return _BATCH_INSERT
    return _BATCH_INSERT.replace(
        " status, frozen_at, evidence)",
        " voucher_issue_price_minor, total_issue_price_minor, status, frozen_at, evidence)",
    ).replace(":approved_total, 'frozen'", ":approved_total, :price, :total, 'frozen'")


_ITEM_INSERT = (
    "INSERT INTO easyweek_voucher_production_batch_items "
    "(batch_id, batch_recipient_count, slot, provider, company_id, campaign_code, recipient_basis, "
    " campaign_run_id, campaign_recipient_id, easyweek_customer_uuid, campaign_period_start, "
    " campaign_period_end, voucher_value_minor, voucher_quantity, reconciliation_marker, status, evidence) "
    "VALUES (:batch, :batch_count, :slot, 'easyweek', 322579, 'new_clients_monthly', "
    " 'operator_manual_selection', :run, :recipient, :customer, '2026-08-01T00:00:00+00', "
    " '2026-08-31T23:59:59+00', :value, :quantity, :marker, 'planned', '{}'::jsonb)"
)


async def _seed_batch(
    db_url: str,
    *,
    recipient_count: int,
    approved_count: int | None = None,
    approved_total: int | None = None,
    phone_offset: int = 0,
) -> tuple[int, list[int], int]:
    """One preview, its recipients and one batch header. Returns their ids."""
    run_id, recipient_ids = await _seed_run_and_recipients(db_url, count=recipient_count, phone_offset=phone_offset)
    total = VOUCHER_PRODUCTION_UNIT_PRICE_MINOR * recipient_count
    await _execute(
        db_url,
        await _batch_insert_sql(db_url),
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": recipient_count,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": total,
            "approved_count": recipient_count if approved_count is None else approved_count,
            "approved_total": total if approved_total is None else approved_total,
        },
    )
    batch_id = (await _fetch(db_url, "SELECT id FROM easyweek_voucher_production_batches ORDER BY id DESC LIMIT 1"))[0][
        0
    ]
    return run_id, recipient_ids, batch_id


async def _add_item(
    db_url: str,
    *,
    batch_id: int,
    batch_count: int,
    slot: int,
    run_id: int,
    recipient_id: int,
    customer: str,
    marker: str,
    value: int = VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
    quantity: int = 1,
) -> None:
    await _execute(
        db_url,
        _ITEM_INSERT,
        {
            "batch": batch_id,
            "batch_count": batch_count,
            "slot": slot,
            "run": run_id,
            "recipient": recipient_id,
            "customer": customer,
            "marker": marker,
            "value": value,
            "quantity": quantity,
        },
    )


# ===========================================================================
# The money contract, as PostgreSQL enforces it
# ===========================================================================


@pytest.mark.asyncio
async def test_an_approved_count_that_does_not_match_is_refused(temp_db_url: str) -> None:
    """§42.5's first half, as a CHECK rather than a branch in the composition."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _ = await _seed_run_and_recipients(temp_db_url, count=3)

    message = await _refused(
        temp_db_url,
        await _batch_insert_sql(temp_db_url),
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": 3,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": 3 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            # Three people, approved as two.
            "approved_count": 2,
            "approved_total": 3 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
        },
    )
    assert "ck_ew_voucher_production_batch_count_approved" in message


@pytest.mark.asyncio
async def test_an_approved_exposure_that_does_not_match_is_refused(temp_db_url: str) -> None:
    """§42.5's second half. The money an operator agreed to is pinned too."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _ = await _seed_run_and_recipients(temp_db_url, count=2)

    message = await _refused(
        temp_db_url,
        await _batch_insert_sql(temp_db_url),
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": 2,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": 2 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "approved_count": 2,
            "approved_total": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
        },
    )
    assert "ck_ew_voucher_production_batch_exposure_approved" in message


@pytest.mark.asyncio
async def test_a_total_that_is_not_the_product_is_refused(temp_db_url: str) -> None:
    """The exposure is derived arithmetic, never a number somebody types."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _ = await _seed_run_and_recipients(temp_db_url, count=2)

    message = await _refused(
        temp_db_url,
        await _batch_insert_sql(temp_db_url),
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": 2,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": 9999,
            "approved_count": 2,
            "approved_total": 9999,
        },
    )
    assert "ck_ew_voucher_production_batch_exposure_matches" in message


@pytest.mark.asyncio
async def test_a_zero_recipient_batch_is_refused(temp_db_url: str) -> None:
    """An empty mailing is not a small one: ``recipient_count >= 1``."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _ = await _seed_run_and_recipients(temp_db_url, count=1)

    message = await _refused(
        temp_db_url,
        await _batch_insert_sql(temp_db_url),
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": 0,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": 0,
            "approved_count": 0,
            "approved_total": 0,
        },
    )
    assert "ck_ew_voucher_production_batch_recipient_count" in message


@pytest.mark.asyncio
async def test_there_is_no_upper_bound_on_the_recipient_count(temp_db_url: str) -> None:
    """The absence of §41's ceiling, asserted rather than assumed.

    Forty recipients store fine. This is the one place the difference between
    the two phases is a fact about the schema rather than a paragraph, so it is
    worth a test that would fail loudly if somebody "helpfully" added a cap.
    """
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _run_id, _recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=40)

    rows = await _fetch(
        temp_db_url,
        "SELECT recipient_count, total_exposure_minor, approved_exposure_minor "
        "FROM easyweek_voucher_production_batches WHERE id = :b",
        {"b": batch_id},
    )
    assert rows == [(40, 40 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR, 40 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR)]


@pytest.mark.asyncio
async def test_the_pr18_five_recipient_cap_is_still_in_force(temp_db_url: str) -> None:
    """And §41's own table still refuses a sixth. Nothing was widened."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _ = await _seed_run_and_recipients(temp_db_url, count=1)
    await _execute(
        temp_db_url,
        "INSERT INTO easyweek_voucher_snapshot_batches "
        "(batch_scope, request_schema_version, baseline_version, provider, company_id, campaign_code, "
        " recipient_basis, campaign_run_id, campaign_period_start, campaign_period_end, location_uuid, "
        " staffer_uuid, payment_account_uuid, voucher_template_uuid, frozen_digest, recipient_count, "
        " voucher_unit_price_minor, total_exposure_minor, status, frozen_at, evidence) "
        "VALUES ('easyweek_voucher_snapshot_batch_v1', '1', '2026-09-27-43', 'easyweek', 322579, "
        " 'new_clients_monthly', 'operator_manual_selection', :run, '2026-08-01T00:00:00+00', "
        " '2026-08-31T23:59:59+00', '22222222-2222-4222-8222-222222222222', "
        " '33333333-3333-4333-8333-333333333333', '44444444-4444-4444-8444-444444444444', "
        " '55555555-5555-4555-8555-555555555555', 'digest', 1, :price, :price, 'frozen', now(), '{}'::jsonb)",
        {"run": run_id, "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR},
    )

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_snapshot_batches SET recipient_count = 6, total_exposure_minor = :total",
        {"total": 6 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR},
    )
    assert "ck_ew_voucher_batch_recipient_count" in message


# ===========================================================================
# Identity, as PostgreSQL enforces it
# ===========================================================================


@pytest.mark.asyncio
async def test_a_second_batch_on_one_preview_is_refused(temp_db_url: str) -> None:
    """One preview is frozen once. This is what replaces §41's singleton scope."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _recipients, _batch_id = await _seed_batch(temp_db_url, recipient_count=2)

    message = await _refused(
        temp_db_url,
        await _batch_insert_sql(temp_db_url),
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": 2,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": 2 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "approved_count": 2,
            "approved_total": 2 * VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
        },
    )
    assert "uq_ew_voucher_production_batch_run" in message


@pytest.mark.asyncio
async def test_a_foreign_scope_literal_is_refused(temp_db_url: str) -> None:
    """The phase label is still pinned, even though the uniqueness moved."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, _ = await _seed_run_and_recipients(temp_db_url, count=1)

    message = await _refused(
        temp_db_url,
        await _batch_insert_sql(temp_db_url),
        {
            "scope": "easyweek_voucher_production_mailing_v2",
            "run": run_id,
            "count": 1,
            "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "total": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "approved_count": 1,
            "approved_total": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
        },
    )
    assert "ck_ew_voucher_production_batch_scope" in message


@pytest.mark.asyncio
async def test_a_backwards_campaign_period_is_refused(temp_db_url: str) -> None:
    """A period is an interval, and the entitlement key is built on both bounds.

    An inverted one would make "one voucher per person per wave" meaningless,
    so it is refused on the header and on every item independently.
    """
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=1)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batches SET campaign_period_start = '2026-09-01T00:00:00+00' WHERE id = :b",
        {"b": batch_id},
    )
    assert "ck_ew_voucher_production_batch_period_order" in message

    await _add_item(
        temp_db_url,
        batch_id=batch_id,
        batch_count=1,
        slot=1,
        run_id=run_id,
        recipient_id=recipients[0],
        customer=str(uuid.UUID(int=(0xBD << 96) | 3)),
        marker="marker-period",
    )
    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items "
        "SET campaign_period_start = '2026-09-01T00:00:00+00' WHERE batch_id = :b",
        {"b": batch_id},
    )
    assert "ck_ew_voucher_production_item_period_order" in message


@pytest.mark.asyncio
async def test_a_foreign_company_campaign_or_basis_is_refused(temp_db_url: str) -> None:
    """One branch, one campaign, one basis — as literals in the schema."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _run_id, _recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=1)

    for column, value, constraint in (
        ("company_id", "999999", "ck_ew_voucher_production_batch_company"),
        ("campaign_code", "'some_other_campaign'", "ck_ew_voucher_production_batch_campaign"),
        ("recipient_basis", "'earned_first_visit'", "ck_ew_voucher_production_batch_basis"),
        ("provider", "'altegio'", "ck_ew_voucher_production_batch_provider"),
    ):
        message = await _refused(
            temp_db_url,
            f"UPDATE easyweek_voucher_production_batches SET {column} = {value} WHERE id = :b",
            {"b": batch_id},
        )
        assert constraint in message, column


@pytest.mark.asyncio
async def test_a_slot_outside_its_own_batch_size_is_refused(temp_db_url: str) -> None:
    """``1 <= slot <= batch_recipient_count``, with no global ceiling involved."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=2)
    customer = str(uuid.UUID(int=(0xCD << 96) | 1))

    # Slot 3 in a two-recipient batch.
    message = await _refused(
        temp_db_url,
        _ITEM_INSERT,
        {
            "batch": batch_id,
            "batch_count": 2,
            "slot": 3,
            "run": run_id,
            "recipient": recipients[0],
            "customer": customer,
            "marker": "m-slot-3",
            "value": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "quantity": 1,
        },
    )
    assert "ck_ew_voucher_production_item_slot_range" in message

    # And slot 0.
    message = await _refused(
        temp_db_url,
        _ITEM_INSERT,
        {
            "batch": batch_id,
            "batch_count": 2,
            "slot": 0,
            "run": run_id,
            "recipient": recipients[0],
            "customer": customer,
            "marker": "m-slot-0",
            "value": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "quantity": 1,
        },
    )
    assert "ck_ew_voucher_production_item_slot_range" in message


@pytest.mark.asyncio
async def test_a_slot_claiming_the_wrong_batch_size_is_refused(temp_db_url: str) -> None:
    """The composite FK, which is what anchors a slot to its batch at all."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=2)

    message = await _refused(
        temp_db_url,
        _ITEM_INSERT,
        {
            "batch": batch_id,
            # The batch really holds two; this row claims it holds five.
            "batch_count": 5,
            "slot": 3,
            "run": run_id,
            "recipient": recipients[0],
            "customer": str(uuid.UUID(int=(0xCE << 96) | 1)),
            "marker": "m-wrong-size",
            "value": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "quantity": 1,
        },
    )
    assert "fk_ew_voucher_production_item_batch_size" in message


@pytest.mark.asyncio
async def test_slot_numbers_may_repeat_across_batches(temp_db_url: str) -> None:
    """Slot 1 exists in every mailing, and that is correct rather than tolerated."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_a, recipients_a, batch_a = await _seed_batch(temp_db_url, recipient_count=1, phone_offset=0)
    run_b, recipients_b, batch_b = await _seed_batch(temp_db_url, recipient_count=1, phone_offset=100)

    await _add_item(
        temp_db_url,
        batch_id=batch_a,
        batch_count=1,
        slot=1,
        run_id=run_a,
        recipient_id=recipients_a[0],
        customer=str(uuid.UUID(int=(0xAA << 96) | 1)),
        marker="marker-a-1",
    )
    await _add_item(
        temp_db_url,
        batch_id=batch_b,
        batch_count=1,
        slot=1,
        run_id=run_b,
        recipient_id=recipients_b[0],
        customer=str(uuid.UUID(int=(0xBB << 96) | 1)),
        marker="marker-b-1",
    )

    rows = await _fetch(
        temp_db_url,
        "SELECT batch_id, slot FROM easyweek_voucher_production_batch_items ORDER BY batch_id",
    )
    assert rows == [(batch_a, 1), (batch_b, 1)]


@pytest.mark.asyncio
async def test_one_customer_cannot_hold_two_entitlements_for_one_period(temp_db_url: str) -> None:
    """Table-wide, so a second preview and a second batch cannot get around it."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_a, recipients_a, batch_a = await _seed_batch(temp_db_url, recipient_count=1, phone_offset=0)
    run_b, recipients_b, batch_b = await _seed_batch(temp_db_url, recipient_count=1, phone_offset=100)
    shared_customer = str(uuid.UUID(int=(0xEE << 96) | 7))

    await _add_item(
        temp_db_url,
        batch_id=batch_a,
        batch_count=1,
        slot=1,
        run_id=run_a,
        recipient_id=recipients_a[0],
        customer=shared_customer,
        marker="marker-ent-a",
    )
    message = await _refused(
        temp_db_url,
        _ITEM_INSERT,
        {
            "batch": batch_b,
            "batch_count": 1,
            "slot": 1,
            "run": run_b,
            "recipient": recipients_b[0],
            "customer": shared_customer,
            "marker": "marker-ent-b",
            "value": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR,
            "quantity": 1,
        },
    )
    assert "uq_ew_voucher_production_item_entitlement" in message


@pytest.mark.asyncio
async def test_a_voucher_of_the_wrong_value_or_quantity_is_refused(temp_db_url: str) -> None:
    """Exactly one voucher of exactly €15. Not a default and not a maximum."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=1)

    for value, quantity, label in ((1400, 1, "€14"), (VOUCHER_PRODUCTION_UNIT_PRICE_MINOR, 2, "two vouchers")):
        message = await _refused(
            temp_db_url,
            _ITEM_INSERT,
            {
                "batch": batch_id,
                "batch_count": 1,
                "slot": 1,
                "run": run_id,
                "recipient": recipients[0],
                "customer": str(uuid.UUID(int=(0xDD << 96) | 1)),
                "marker": f"marker-{quantity}-{value}",
                "value": value,
                "quantity": quantity,
            },
        )
        assert "ck_ew_voucher_production_item_exact_voucher" in message, label


# ===========================================================================
# The state machine, as PostgreSQL enforces it
# ===========================================================================


async def _one_item(temp_db_url: str) -> tuple[int, int]:
    """A batch with a single planned slot. Returns ``(batch_id, item_id)``."""
    run_id, recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=1)
    await _add_item(
        temp_db_url,
        batch_id=batch_id,
        batch_count=1,
        slot=1,
        run_id=run_id,
        recipient_id=recipients[0],
        customer=str(uuid.UUID(int=(0xFA << 96) | 1)),
        marker="marker-state-1",
    )
    item_id = (await _fetch(temp_db_url, "SELECT id FROM easyweek_voucher_production_batch_items LIMIT 1"))[0][0]
    return batch_id, item_id


async def _make_sendable(temp_db_url: str, item_id: int) -> None:
    """Bring one slot to the state a legal DELIVER acts from.

    Every precondition satisfied — a proven create, a proven payment, a stored
    MAC with its key id, and a live guard taken after the payment — so that a
    later write's only defect is the one the test is about. Without this,
    PostgreSQL reports whichever CHECK it happens to evaluate first and the
    assertion would be about constraint ordering rather than about the rule.
    """
    await _execute(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET "
        " target_order_uuid = '66666666-6666-4666-8666-666666666666', "
        " create_claimed_at = now(), create_attempted_at = now(), create_verified_at = now(), "
        " pay_claimed_at = now(), pay_attempted_at = now(), pay_verified_at = now(), "
        " voucher_code_hmac = 'a', hmac_key_id = 'k', live_guard_reproven_at = now(), "
        " status = 'paid' "
        "WHERE id = :i",
        {"i": item_id},
    )


@pytest.mark.asyncio
async def test_a_second_delivery_attempt_is_refused(temp_db_url: str) -> None:
    """A counter that can only be zero or one, for the lifetime of the row."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)
    await _make_sendable(temp_db_url, item_id)

    # One attempt is legal...
    await _execute(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items "
        "SET send_claimed_at = now(), send_attempted_at = now(), send_attempt_count = 1, "
        " status = 'send_claimed' WHERE id = :i",
        {"i": item_id},
    )
    # ...and a second one is not a retry budget, it is a row PostgreSQL refuses.
    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET send_attempt_count = 2 WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_single_attempt" in message


@pytest.mark.asyncio
async def test_an_attempt_without_a_timestamp_is_refused(temp_db_url: str) -> None:
    """The counter and the timestamp are one fact, and must agree."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)
    await _make_sendable(temp_db_url, item_id)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items "
        "SET send_claimed_at = now(), send_attempt_count = 1 WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_attempt_count_matches" in message


@pytest.mark.asyncio
async def test_paying_without_a_proven_create_is_refused(temp_db_url: str) -> None:
    """Money cannot move before the order it pays for was proven to exist."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET pay_claimed_at = now() WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_pay_needs_created" in message


@pytest.mark.asyncio
async def test_sending_without_a_proven_payment_is_refused(temp_db_url: str) -> None:
    """A send needs a proven payment, a stored MAC and a guard taken after it."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items "
        "SET send_claimed_at = now(), send_attempted_at = now(), send_attempt_count = 1 WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_send_needs_paid" in message


@pytest.mark.asyncio
async def test_the_webhook_ladder_cannot_be_climbed_out_of_order(temp_db_url: str) -> None:
    """delivered needs accepted, read needs delivered, accepted needs an attempt.

    This is what makes "completed is not read" a property of the schema rather
    than a caveat in a report.
    """
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET delivered_at = now() WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_delivered_needs_accepted" in message

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET read_at = now() WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_read_needs_delivered" in message

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET provider_accepted_at = now() WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_accepted_needs_attempt" in message


@pytest.mark.asyncio
async def test_a_refund_claim_after_a_send_claim_is_refused(temp_db_url: str) -> None:
    """The refund is the pre-send escape hatch, and only that.

    Taking the money back for a code somebody may already be holding is worse
    than losing the €15, so this is a CHECK and not only a branch in the plan.
    """
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)
    await _make_sendable(temp_db_url, item_id)
    await _execute(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items "
        "SET send_claimed_at = now(), send_attempted_at = now(), send_attempt_count = 1, "
        " status = 'send_claimed' WHERE id = :i",
        {"i": item_id},
    )

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET refund_claimed_at = now() WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_refund_is_pre_send" in message


@pytest.mark.asyncio
async def test_a_refunded_status_after_acceptance_is_refused(temp_db_url: str) -> None:
    """And the terminal status carries the same rule independently."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)
    await _make_sendable(temp_db_url, item_id)
    await _execute(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET "
        " send_claimed_at = now(), send_attempted_at = now(), send_attempt_count = 1, "
        " provider_message_id = 'wamid.MIGTEST', provider_accepted_at = now(), "
        " status = 'provider_accepted' WHERE id = :i",
        {"i": item_id},
    )

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET status = 'refunded' WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_refunded_never_sent" in message


@pytest.mark.asyncio
async def test_a_halt_must_say_why(temp_db_url: str) -> None:
    """A halted header with no reason is a state an operator cannot act on."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _run_id, _recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=1)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batches SET status = 'halted' WHERE id = :b",
        {"b": batch_id},
    )
    assert "ck_ew_voucher_production_batch_halt_has_reason" in message


@pytest.mark.asyncio
async def test_a_mac_without_its_key_id_is_refused(temp_db_url: str) -> None:
    """A MAC nobody can say which key produced cannot be verified later."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _batch_id, item_id = await _one_item(temp_db_url)

    message = await _refused(
        temp_db_url,
        "UPDATE easyweek_voucher_production_batch_items SET voucher_code_hmac = 'abc' WHERE id = :i",
        {"i": item_id},
    )
    assert "ck_ew_voucher_production_item_hmac_pair" in message


# ===========================================================================
# Historical rows survive the round trip
# ===========================================================================


@pytest.mark.asyncio
async def test_historical_canary_and_batch_rows_survive_the_round_trip(temp_db_url: str) -> None:
    """The §37.2 ledger and the §41 batch are not touched, rows included."""
    _alembic_ok("upgrade", PR19_PARENT_REVISION, db_url=temp_db_url)
    run_id, recipients = await _seed_run_and_recipients(temp_db_url, count=1)
    customer = str(uuid.UUID(int=(0x11 << 96) | 1))

    await _execute(
        temp_db_url,
        "INSERT INTO easyweek_manual_voucher_delivery_ledger "
        "(canary_scope, request_schema_version, baseline_version, provider, company_id, campaign_code, "
        " recipient_basis, campaign_run_id, campaign_recipient_id, easyweek_customer_uuid, "
        " campaign_period_start, campaign_period_end, location_uuid, staffer_uuid, payment_account_uuid, "
        " voucher_template_uuid, reconciliation_marker, status, evidence) "
        "VALUES ('easyweek_manual_voucher_canary_v1', '1', '2026-09-15-42', 'easyweek', 322579, "
        " 'new_clients_monthly', 'operator_manual_selection', :run, :recipient, :customer, "
        " '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', "
        " '22222222-2222-4222-8222-222222222222', '33333333-3333-4333-8333-333333333333', "
        " '44444444-4444-4444-8444-444444444444', '55555555-5555-4555-8555-555555555555', "
        " 'ewmv1-historical', 'read', '{}'::jsonb)",
        {"run": run_id, "recipient": recipients[0], "customer": customer},
    )
    await _execute(
        temp_db_url,
        "INSERT INTO easyweek_voucher_snapshot_batches "
        "(batch_scope, request_schema_version, baseline_version, provider, company_id, campaign_code, "
        " recipient_basis, campaign_run_id, campaign_period_start, campaign_period_end, location_uuid, "
        " staffer_uuid, payment_account_uuid, voucher_template_uuid, frozen_digest, recipient_count, "
        " voucher_unit_price_minor, total_exposure_minor, status, frozen_at, evidence) "
        "VALUES ('easyweek_voucher_snapshot_batch_v1', '1', '2026-09-27-43', 'easyweek', 322579, "
        " 'new_clients_monthly', 'operator_manual_selection', :run, '2026-08-01T00:00:00+00', "
        " '2026-08-31T23:59:59+00', '22222222-2222-4222-8222-222222222222', "
        " '33333333-3333-4333-8333-333333333333', '44444444-4444-4444-8444-444444444444', "
        " '55555555-5555-4555-8555-555555555555', 'pr18digest', 1, :price, :price, 'completed', "
        " now(), '{}'::jsonb)",
        {"run": run_id, "price": VOUCHER_PRODUCTION_UNIT_PRICE_MINOR},
    )

    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _alembic_ok("downgrade", PR19_PARENT_REVISION, db_url=temp_db_url)
    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    manual = await _fetch(
        temp_db_url,
        "SELECT canary_scope, status, reconciliation_marker FROM easyweek_manual_voucher_delivery_ledger",
    )
    assert manual == [("easyweek_manual_voucher_canary_v1", "read", "ewmv1-historical")]

    batch = await _fetch(
        temp_db_url,
        "SELECT batch_scope, status, frozen_digest, recipient_count FROM easyweek_voucher_snapshot_batches",
    )
    assert batch == [("easyweek_voucher_snapshot_batch_v1", "completed", "pr18digest", 1)]


@pytest.mark.asyncio
async def test_an_upgrade_over_a_populated_production_table_is_a_no_op(temp_db_url: str) -> None:
    """Re-running the upgrade on an already-migrated database changes nothing.

    Pinned to §42's revision, so what is proven idempotent is §42's migration and
    not whatever the chain happens to end with.
    """
    _alembic_ok("upgrade", PR19_REVISION, db_url=temp_db_url)
    _run_id, _recipients, batch_id = await _seed_batch(temp_db_url, recipient_count=3)

    _alembic_ok("upgrade", PR19_REVISION, db_url=temp_db_url)

    rows = await _fetch(
        temp_db_url,
        "SELECT id, recipient_count, approved_recipient_count FROM easyweek_voucher_production_batches",
    )
    assert rows == [(batch_id, 3, 3)]
    assert _alembic_ok("current", db_url=temp_db_url).count(PR19_REVISION) == 1
    # One head overall, whichever revision that is now.
    assert len(ScriptDirectory.from_config(Config(str(ALEMBIC_INI))).get_heads()) == 1
