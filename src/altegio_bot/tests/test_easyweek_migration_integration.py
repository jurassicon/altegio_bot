"""Executable Alembic compatibility coverage on a real PostgreSQL database.

The structural guards in ``test_easyweek_migration.py`` only read the migration
files. They cannot prove that an environment which applied the EARLY base
revision (before ``body_raw``/``body_size_bytes`` existed) actually gets the
columns — and that is the exact regression this suite must prevent.

Isolation: every run creates its OWN throwaway database named
``altegio_migtest_<uuid>`` and drops it in ``finally``. The shared pytest schema
owned by ``conftest.py`` is never touched, and Alembic is never pointed at the
development or production database.

Where that database is created:
  * ``ALTEGIO_MIGTEST_DATABASE_URL`` if set — point it at a disposable server;
  * otherwise the server behind ``DATABASE_URL``.

Creating a database requires a role with ``CREATEDB``. CI's PostgreSQL service
user has it, so the test runs there. A local developer role often does not — in
that case the test skips with the exact remedy instead of silently passing. A
temporary *schema* is not a workable substitute here: ``alembic/env.py`` builds
its own asyncpg engine, and asyncpg honours neither an ``options`` URL parameter
nor ``PGOPTIONS``, so ``search_path`` cannot be injected into the migration run.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from altegio_bot.settings import Settings

_ROOT = Path(__file__).resolve().parents[3]
_BASE_REVISION = "8923be993170"
_HEAD_REVISION = "8705ec49cc73"
# Guard against ever pointing the DROP at something real.
_TEMP_DB_PREFIX = "altegio_migtest_"


_REMEDY = (
    "Grant CREATEDB to the DATABASE_URL role, or point ALTEGIO_MIGTEST_DATABASE_URL at a "
    "disposable PostgreSQL, e.g.: docker run -d --name ew-mig -e POSTGRES_PASSWORD=postgres "
    "-p 55433:5432 postgres:16-alpine && "
    "ALTEGIO_MIGTEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:55433/postgres "
    "uv run pytest src/altegio_bot/tests/test_easyweek_migration_integration.py"
)


def _unavailable(reason: str) -> None:
    """Skip locally, but FAIL where the test is declared mandatory.

    Migration compatibility is a CI responsibility: if the disposable database
    cannot be created there, a green skip would silently retire the only check
    that proves an early-revision environment gets the raw-body columns. CI sets
    ALTEGIO_REQUIRE_MIGTEST=1 (a project-specific flag rather than the generic
    CI=true, so the intent is explicit and greppable).
    """
    if os.environ.get("ALTEGIO_REQUIRE_MIGTEST") == "1":
        pytest.fail(f"Migration integration test is required (ALTEGIO_REQUIRE_MIGTEST=1) but {reason}. {_REMEDY}")
    pytest.skip(f"{reason}. {_REMEDY}")


def _server_url() -> str:
    """URL of the maintenance database on the server that will host the temp DB."""
    base = os.environ.get("ALTEGIO_MIGTEST_DATABASE_URL") or Settings().database_url
    return base.rsplit("/", 1)[0] + "/postgres"


def _temp_db_url(name: str) -> str:
    return _server_url().rsplit("/", 1)[0] + "/" + name


def _run_alembic(*args: str, db_url: str) -> subprocess.CompletedProcess:
    """Run Alembic in a subprocess pinned to ``db_url``.

    A subprocess is required: ``alembic/env.py`` reads the settings singleton at
    import time, so the URL has to be in the environment before Python starts.
    """
    env = dict(os.environ)
    env["DATABASE_URL"] = db_url
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(_ROOT / "alembic.ini"), *args],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def _alembic_ok(*args: str, db_url: str) -> str:
    result = _run_alembic(*args, db_url=db_url)
    assert result.returncode == 0, f"alembic {args} failed:\n{result.stdout}\n{result.stderr}"
    return result.stdout + result.stderr


@pytest_asyncio.fixture
async def temp_db_url():
    """Create a uniquely named throwaway database; always drop it afterwards."""
    name = _TEMP_DB_PREFIX + uuid.uuid4().hex[:12]
    assert name.startswith(_TEMP_DB_PREFIX)

    try:
        admin = create_async_engine(_server_url(), isolation_level="AUTOCOMMIT")
    except Exception as exc:  # pragma: no cover - configuration problem
        _unavailable(f"PostgreSQL not configured for integration tests: {type(exc).__name__}: {exc}")

    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as exc:
        await admin.dispose()
        _unavailable(
            f"Cannot create a throwaway database for the Alembic compatibility test: {type(exc).__name__}: {exc}"
        )

    try:
        yield _temp_db_url(name)
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


async def _raw_body_columns(db_url: str) -> dict[str, tuple]:
    rows = await _fetch(
        db_url,
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_name = 'easyweek_events' "
        "AND column_name IN ('body_raw', 'body_size_bytes')",
    )
    return {row[0]: row for row in rows}


@pytest.mark.asyncio
async def test_migration_compatibility_scenarios(temp_db_url) -> None:
    """All four scenarios, sequentially, on one throwaway database.

    They are one test on purpose: each scenario is the setup for the next, and
    re-creating a database per scenario would quadruple the runtime.
    """
    # ---------------------------------------------------------------- 1. fresh
    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    columns = await _raw_body_columns(temp_db_url)
    assert columns["body_raw"][1] == "bytea"
    assert columns["body_size_bytes"][1] == "bigint"
    assert columns["body_size_bytes"][2] == "NO"  # NOT NULL
    assert "0" in (columns["body_size_bytes"][3] or "")

    indexes = await _fetch(
        temp_db_url,
        "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'easyweek_events'",
    )
    payload_hash_index = [d for n, d in indexes if n == "ix_easyweek_events_payload_hash"]
    assert payload_hash_index, "payload_hash index is missing"
    assert "UNIQUE" not in payload_hash_index[0].upper()

    current = _alembic_ok("current", db_url=temp_db_url)
    assert _HEAD_REVISION in current

    # ------------------------------------------------------------ 4. downgrade
    # (Run here because it also produces the "early schema" state for step 2.)
    before = await _fetch(
        temp_db_url,
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'easyweek_events'",
    )
    _alembic_ok("downgrade", _BASE_REVISION, db_url=temp_db_url)

    assert await _raw_body_columns(temp_db_url) == {}
    table_exists = await _fetch(temp_db_url, "SELECT to_regclass('public.easyweek_events') IS NOT NULL")
    assert table_exists[0][0] is True

    after = await _fetch(
        temp_db_url,
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'easyweek_events'",
    )
    removed = {c[0] for c in before} - {c[0] for c in after}
    assert removed == {"body_raw", "body_size_bytes"}, f"downgrade removed too much: {removed}"

    remaining_indexes = await _fetch(
        temp_db_url,
        "SELECT indexname FROM pg_indexes WHERE tablename = 'easyweek_events'",
    )
    assert {"ix_easyweek_events_payload_hash", "ix_easyweek_events_status"} <= {i[0] for i in remaining_indexes}

    # -------------------------------------------------- 2. early applied schema
    # Alembic is at the base revision and the columns do not exist — exactly the
    # state of an environment that ran the early version of the base migration.
    assert _BASE_REVISION in _alembic_ok("current", db_url=temp_db_url)
    assert await _raw_body_columns(temp_db_url) == {}

    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    columns = await _raw_body_columns(temp_db_url)
    assert set(columns) == {"body_raw", "body_size_bytes"}
    assert columns["body_raw"][1] == "bytea"
    assert columns["body_size_bytes"][1] == "bigint"

    # ------------------------------------------- 3. columns already present
    # Simulates an environment that applied the EDITED base revision, which had
    # both columns in its CREATE TABLE: ADD COLUMN IF NOT EXISTS must be a no-op.
    _alembic_ok("stamp", _BASE_REVISION, db_url=temp_db_url)
    assert set(await _raw_body_columns(temp_db_url)) == {"body_raw", "body_size_bytes"}

    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    assert set(await _raw_body_columns(temp_db_url)) == {"body_raw", "body_size_bytes"}
    assert _HEAD_REVISION in _alembic_ok("current", db_url=temp_db_url)
