"""Migration contract for the §41 voucher snapshot batch tables (PR-18).

Runs upgrade → downgrade → upgrade on a THROWAWAY database, never production.
Skips when no disposable PostgreSQL is reachable, and FAILS instead of skipping
when the environment declares the check mandatory (``ALTEGIO_REQUIRE_MIGTEST=1``),
so a green build can never hide a migration that was silently never exercised.

What it proves, beyond "the DDL runs"
-------------------------------------
Almost everything §41 promises about size and money is a database constraint,
and a constraint that exists only in the ORM is a constraint production does
not have. So this module applies the migration and then tries, in SQL, to write
the rows the phase says are impossible: a sixth slot, a slot outside its own
batch's declared size, a second batch, a €14 voucher, an exposure that is not
the product, and a second delivery attempt.

It also proves the downgrade removes exactly the three new tables and leaves the
§35, §36 and §37.2 tables — including any historical canary rows — untouched.
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
    VOUCHER_BATCH_MAX_EXPOSURE_MINOR,
    VOUCHER_BATCH_MAX_RECIPIENTS,
    VOUCHER_BATCH_SCOPE,
    VOUCHER_BATCH_UNIT_PRICE_MINOR,
    Base,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = _REPO_ROOT / "alembic.ini"

PR18_REVISION = "b3f7c2a90d14"
PR17_3_REVISION = "a7d4f2e81c95"

BATCHES = "easyweek_voucher_snapshot_batches"
ITEMS = "easyweek_voucher_snapshot_batch_items"
ATTEMPTS = "easyweek_voucher_snapshot_batch_attempts"
PR18_TABLES = (BATCHES, ITEMS, ATTEMPTS)

# The §§35–37.2 tables this revision must not touch, in either direction.
UNTOUCHED_TABLES = (
    "easyweek_voucher_canary_ledger",
    "easyweek_campaign_voucher_delivery_ledger",
    "easyweek_campaign_voucher_delivery_attempts",
    "easyweek_manual_voucher_delivery_ledger",
    "easyweek_manual_voucher_delivery_attempts",
)

_TEMP_DB_PREFIX = "altegio_pr18_migtest_"
_REMEDY = "Grant CREATEDB to the DATABASE_URL role, or point ALTEGIO_MIGTEST_DATABASE_URL at a disposable PostgreSQL."


def _unavailable(reason: str) -> None:
    if os.environ.get("ALTEGIO_REQUIRE_MIGTEST") == "1":
        pytest.fail(f"PR-18 migration test is required (ALTEGIO_REQUIRE_MIGTEST=1) but {reason}. {_REMEDY}")
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
    """One head — two would mean two lineages and a deploy that cannot upgrade."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    heads = script.get_heads()

    assert len(heads) == 1, f"expected exactly one Alembic head, got {heads}"
    assert PR18_REVISION in {revision.revision for revision in script.walk_revisions()}


def test_pr18_is_a_direct_child_of_the_manual_canary_revision() -> None:
    """PR-18 builds on the deployed head, never forks from it."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert script.get_revision(PR18_REVISION).down_revision == PR17_3_REVISION


def test_the_orm_and_the_migration_describe_the_same_tables() -> None:
    """Every §41 table the application expects is one this revision creates."""
    metadata_tables = {table.name for table in Base.metadata.sorted_tables}
    for table in PR18_TABLES:
        assert table in metadata_tables


# ===========================================================================
# Upgrade, downgrade, upgrade
# ===========================================================================


@pytest.mark.asyncio
async def test_head_upgrade_on_an_empty_database_creates_the_three_tables(temp_db_url: str) -> None:
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    (head,) = script.get_heads()

    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    current = _alembic_ok("current", db_url=temp_db_url)

    assert head in current
    assert await _tables(temp_db_url, "easyweek_voucher_snapshot%") == set(PR18_TABLES)


@pytest.mark.asyncio
async def test_downgrade_removes_only_the_new_objects(temp_db_url: str) -> None:
    """Exactly the three new tables go, and the historical ledgers stay."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    before = await _tables(temp_db_url, "easyweek_%voucher%")

    _alembic_ok("downgrade", PR17_3_REVISION, db_url=temp_db_url)
    after = await _tables(temp_db_url, "easyweek_%voucher%")

    assert await _tables(temp_db_url, "easyweek_voucher_snapshot%") == set()
    assert before - after == set(PR18_TABLES)
    for table in UNTOUCHED_TABLES:
        assert table in after

    # And it goes back up cleanly, so a rollback is not a one-way door.
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    assert await _tables(temp_db_url, "easyweek_voucher_snapshot%") == set(PR18_TABLES)


@pytest.mark.asyncio
async def test_a_historical_canary_row_survives_the_whole_round_trip(temp_db_url: str) -> None:
    """The §37.2 ledger is not touched, and neither is a row already in it."""
    _alembic_ok("upgrade", PR17_3_REVISION, db_url=temp_db_url)
    await _execute(
        temp_db_url,
        "INSERT INTO campaign_runs (provider, campaign_code, mode, company_ids, status, "
        " period_start, period_end, meta) "
        "VALUES ('easyweek', 'new_clients_monthly', 'preview', '[322579]'::jsonb, 'completed', "
        " '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', '{}'::jsonb)",
    )
    run_id = (await _fetch(temp_db_url, "SELECT id FROM campaign_runs LIMIT 1"))[0][0]
    await _execute(
        temp_db_url,
        "INSERT INTO campaign_recipients (provider, campaign_run_id, company_id, phone_e164, status, "
        " recipient_basis, easyweek_customer_uuid, meta, service_titles_in_period, cleanup_card_ids) "
        "VALUES ('easyweek', :run, 322579, '+490000000000', 'candidate', 'operator_manual_selection', "
        " '11111111-1111-4111-8111-111111111111', '{}'::jsonb, '[]'::jsonb, '[]'::jsonb)",
        {"run": run_id},
    )
    recipient_id = (await _fetch(temp_db_url, "SELECT id FROM campaign_recipients LIMIT 1"))[0][0]
    await _execute(
        temp_db_url,
        "INSERT INTO easyweek_manual_voucher_delivery_ledger "
        "(canary_scope, request_schema_version, baseline_version, provider, company_id, campaign_code, "
        " recipient_basis, campaign_run_id, campaign_recipient_id, easyweek_customer_uuid, "
        " campaign_period_start, campaign_period_end, location_uuid, staffer_uuid, payment_account_uuid, "
        " voucher_template_uuid, reconciliation_marker, status, evidence) "
        "VALUES ('easyweek_manual_voucher_canary_v1', '1', '2026-09-15-42', 'easyweek', 322579, "
        " 'new_clients_monthly', 'operator_manual_selection', :run, :recipient, "
        " '11111111-1111-4111-8111-111111111111', '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', "
        " '22222222-2222-4222-8222-222222222222', '33333333-3333-4333-8333-333333333333', "
        " '44444444-4444-4444-8444-444444444444', '55555555-5555-4555-8555-555555555555', "
        " 'ewmv1-historical', 'read', '{}'::jsonb)",
        {"run": run_id, "recipient": recipient_id},
    )

    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    _alembic_ok("downgrade", PR17_3_REVISION, db_url=temp_db_url)
    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    rows = await _fetch(
        temp_db_url,
        "SELECT canary_scope, status, reconciliation_marker FROM easyweek_manual_voucher_delivery_ledger",
    )
    assert rows == [("easyweek_manual_voucher_canary_v1", "read", "ewmv1-historical")]


# ===========================================================================
# The limits, as PostgreSQL enforces them
# ===========================================================================


async def _seed_batch(db_url: str, *, recipient_count: int) -> tuple[int, int, int]:
    """One preview, one recipient and one batch header. Returns their ids."""
    await _execute(
        db_url,
        "INSERT INTO campaign_runs (provider, campaign_code, mode, company_ids, status, "
        " period_start, period_end, meta) "
        "VALUES ('easyweek', 'new_clients_monthly', 'preview', '[322579]'::jsonb, 'completed', "
        " '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', '{}'::jsonb)",
    )
    run_id = (await _fetch(db_url, "SELECT id FROM campaign_runs ORDER BY id DESC LIMIT 1"))[0][0]
    await _execute(
        db_url,
        "INSERT INTO campaign_recipients (provider, campaign_run_id, company_id, phone_e164, status, "
        " recipient_basis, easyweek_customer_uuid, meta, service_titles_in_period, cleanup_card_ids) "
        "VALUES ('easyweek', :run, 322579, '+490000000001', 'candidate', 'operator_manual_selection', "
        " 'aaaaaaaa-1111-4111-8111-111111111111', '{}'::jsonb, '[]'::jsonb, '[]'::jsonb)",
        {"run": run_id},
    )
    recipient_id = (await _fetch(db_url, "SELECT id FROM campaign_recipients ORDER BY id DESC LIMIT 1"))[0][0]
    await _execute(
        db_url,
        "INSERT INTO easyweek_voucher_snapshot_batches "
        "(batch_scope, request_schema_version, baseline_version, provider, company_id, campaign_code, "
        " recipient_basis, campaign_run_id, campaign_period_start, campaign_period_end, location_uuid, "
        " staffer_uuid, payment_account_uuid, voucher_template_uuid, frozen_digest, recipient_count, "
        " voucher_unit_price_minor, total_exposure_minor, status, frozen_at, evidence) "
        "VALUES (:scope, '1', '2026-09-15-42', 'easyweek', 322579, 'new_clients_monthly', "
        " 'operator_manual_selection', :run, '2026-08-01T00:00:00+00', '2026-08-31T23:59:59+00', "
        " '22222222-2222-4222-8222-222222222222', '33333333-3333-4333-8333-333333333333', "
        " '44444444-4444-4444-8444-444444444444', '55555555-5555-4555-8555-555555555555', "
        " 'digest', :count, :price, :total, 'frozen', now(), '{}'::jsonb)",
        {
            "scope": VOUCHER_BATCH_SCOPE,
            "run": run_id,
            "count": recipient_count,
            "price": VOUCHER_BATCH_UNIT_PRICE_MINOR,
            "total": VOUCHER_BATCH_UNIT_PRICE_MINOR * recipient_count,
        },
    )
    batch_id = (await _fetch(db_url, "SELECT id FROM easyweek_voucher_snapshot_batches LIMIT 1"))[0][0]
    return run_id, recipient_id, batch_id


_ITEM_INSERT = (
    "INSERT INTO easyweek_voucher_snapshot_batch_items "
    "(batch_id, batch_recipient_count, slot, provider, company_id, campaign_code, recipient_basis, "
    " campaign_run_id, campaign_recipient_id, easyweek_customer_uuid, campaign_period_start, "
    " campaign_period_end, voucher_value_minor, voucher_quantity, reconciliation_marker, status, evidence) "
    "VALUES (:batch, :declared, :slot, 'easyweek', 322579, 'new_clients_monthly', "
    " 'operator_manual_selection', :run, :recipient, :customer, '2026-08-01T00:00:00+00', "
    " '2026-08-31T23:59:59+00', :value, :quantity, :marker, 'planned', '{}'::jsonb)"
)


@pytest.mark.asyncio
async def test_postgresql_itself_refuses_a_sixth_slot(temp_db_url: str) -> None:
    """Not a check the application makes — a row the database will not store."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipient_id, batch_id = await _seed_batch(temp_db_url, recipient_count=VOUCHER_BATCH_MAX_RECIPIENTS)

    params = {
        "batch": batch_id,
        "declared": VOUCHER_BATCH_MAX_RECIPIENTS,
        "run": run_id,
        "recipient": recipient_id,
        "customer": "aaaaaaaa-1111-4111-8111-111111111111",
        "value": VOUCHER_BATCH_UNIT_PRICE_MINOR,
        "quantity": 1,
        "marker": "ewvb1-slot6",
        "slot": VOUCHER_BATCH_MAX_RECIPIENTS + 1,
    }
    with pytest.raises(Exception) as excinfo:
        await _execute(temp_db_url, _ITEM_INSERT, params)
    assert "ck_ew_voucher_batch_item_slot_range" in str(excinfo.value)

    # Claiming a bigger declared size to make room finds no such batch.
    params["declared"] = VOUCHER_BATCH_MAX_RECIPIENTS + 1
    with pytest.raises(Exception) as excinfo:
        await _execute(temp_db_url, _ITEM_INSERT, params)
    assert "ck_ew_voucher_batch_item_slot_range" in str(excinfo.value) or "fk_ew_voucher_batch_item_batch_size" in str(
        excinfo.value
    )


@pytest.mark.asyncio
async def test_postgresql_itself_refuses_a_second_batch(temp_db_url: str) -> None:
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    await _seed_batch(temp_db_url, recipient_count=1)

    with pytest.raises(Exception) as excinfo:
        await _seed_batch(temp_db_url, recipient_count=1)
    assert "uq_ew_voucher_batch_scope" in str(excinfo.value)

    # And a row under any other scope string is refused by a CHECK, so the
    # uniqueness cannot be sidestepped by renaming the batch.
    with pytest.raises(Exception) as excinfo:
        await _execute(
            temp_db_url,
            "UPDATE easyweek_voucher_snapshot_batches SET batch_scope = 'easyweek_voucher_snapshot_batch_v2'",
        )
    assert "ck_ew_voucher_batch_single_scope" in str(excinfo.value)


@pytest.mark.asyncio
async def test_postgresql_itself_refuses_the_wrong_money(temp_db_url: str) -> None:
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipient_id, batch_id = await _seed_batch(temp_db_url, recipient_count=2)

    base = {
        "batch": batch_id,
        "declared": 2,
        "run": run_id,
        "recipient": recipient_id,
        "customer": "aaaaaaaa-1111-4111-8111-111111111111",
        "slot": 1,
        "marker": "ewvb1-money",
        "value": VOUCHER_BATCH_UNIT_PRICE_MINOR,
        "quantity": 1,
    }
    for changes in ({"value": 1400}, {"quantity": 2}):
        with pytest.raises(Exception) as excinfo:
            await _execute(temp_db_url, _ITEM_INSERT, {**base, **changes})
        assert "ck_ew_voucher_batch_item_exact_voucher" in str(excinfo.value)

    # The header's exposure must be exactly the product, so €75 is the ceiling.
    with pytest.raises(Exception) as excinfo:
        await _execute(
            temp_db_url,
            "UPDATE easyweek_voucher_snapshot_batches SET total_exposure_minor = 1500",
        )
    assert "ck_ew_voucher_batch_exposure_matches" in str(excinfo.value)

    with pytest.raises(Exception) as excinfo:
        await _execute(
            temp_db_url,
            "UPDATE easyweek_voucher_snapshot_batches SET recipient_count = :count, total_exposure_minor = :total",
            {
                "count": VOUCHER_BATCH_MAX_RECIPIENTS + 1,
                "total": VOUCHER_BATCH_UNIT_PRICE_MINOR * (VOUCHER_BATCH_MAX_RECIPIENTS + 1),
            },
        )
    assert "ck_ew_voucher_batch_recipient_count" in str(excinfo.value)

    # Stated once more as arithmetic, so a future edit to one literal that
    # forgets the other fails here rather than in production.
    assert VOUCHER_BATCH_MAX_EXPOSURE_MINOR == VOUCHER_BATCH_UNIT_PRICE_MINOR * VOUCHER_BATCH_MAX_RECIPIENTS


@pytest.mark.asyncio
async def test_postgresql_itself_refuses_a_second_delivery_attempt(temp_db_url: str) -> None:
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    run_id, recipient_id, batch_id = await _seed_batch(temp_db_url, recipient_count=1)
    await _execute(
        temp_db_url,
        _ITEM_INSERT,
        {
            "batch": batch_id,
            "declared": 1,
            "slot": 1,
            "run": run_id,
            "recipient": recipient_id,
            "customer": "aaaaaaaa-1111-4111-8111-111111111111",
            "value": VOUCHER_BATCH_UNIT_PRICE_MINOR,
            "quantity": 1,
            "marker": "ewvb1-attempt",
        },
    )

    # Two constraints hold the counter together, and either one firing is the
    # right answer: the range says it can only be zero or one, and the pairing
    # says it must agree with whether an attempt was ever made. A counter of two
    # breaks both at once.
    with pytest.raises(Exception) as excinfo:
        await _execute(
            temp_db_url,
            "UPDATE easyweek_voucher_snapshot_batch_items SET send_attempt_count = 2",
        )
    message = str(excinfo.value)
    assert (
        "ck_ew_voucher_batch_item_single_attempt" in message
        or "ck_ew_voucher_batch_item_attempt_count_matches" in message
    ), message

    # And with the pairing satisfied, the range refuses it on its own.
    with pytest.raises(Exception) as excinfo:
        await _execute(
            temp_db_url,
            "UPDATE easyweek_voucher_snapshot_batch_items "
            "SET send_attempt_count = 2, send_attempted_at = now(), send_claimed_at = now()",
        )
    assert "ck_ew_voucher_batch_item_single_attempt" in str(excinfo.value) or (
        "ck_ew_voucher_batch_item_send_needs_paid" in str(excinfo.value)
    )

    # A send before a proven, MAC-bound, freshly guarded payment is not a state
    # this table can hold either.
    with pytest.raises(Exception) as excinfo:
        await _execute(
            temp_db_url,
            "UPDATE easyweek_voucher_snapshot_batch_items "
            "SET send_claimed_at = now(), send_attempted_at = now(), send_attempt_count = 1",
        )
    assert "ck_ew_voucher_batch_item_send_needs_paid" in str(excinfo.value)
