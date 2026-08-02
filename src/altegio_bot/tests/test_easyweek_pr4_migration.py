"""Migration contract for the PR-4 easyweek_events columns.

Runs upgrade → downgrade → upgrade on a THROWAWAY database, never production.
Skips when no disposable PostgreSQL is reachable, and FAILS instead of skipping
when the environment declares the check mandatory (``ALTEGIO_REQUIRE_MIGTEST=1``),
so a green build can never hide a migration that was silently never exercised.
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

_REPO_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = _REPO_ROOT / "alembic.ini"

PR4_REVISION = "d4e8a1c39f57"
PR3_REVISION = "c1a7d3f905b2"

PR4_COLUMNS = ("processed_at", "error_code")
PR4_INDEX = "ix_easyweek_events_status_received_at"

_TEMP_DB_PREFIX = "altegio_pr4_migtest_"
_REMEDY = "Grant CREATEDB to the DATABASE_URL role, or point ALTEGIO_MIGTEST_DATABASE_URL at a disposable PostgreSQL."


def _unavailable(reason: str) -> None:
    if os.environ.get("ALTEGIO_REQUIRE_MIGTEST") == "1":
        pytest.fail(f"PR-4 migration test is required (ALTEGIO_REQUIRE_MIGTEST=1) but {reason}. {_REMEDY}")
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
        cwd=_REPO_ROOT,
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


async def _columns_of(db_url: str, table: str) -> set[str]:
    rows = await _fetch(
        db_url,
        "SELECT column_name FROM information_schema.columns WHERE table_name = :t",
        {"t": table},
    )
    return {row[0] for row in rows}


async def _indexes_of(db_url: str, table: str) -> set[str]:
    rows = await _fetch(db_url, "SELECT indexname FROM pg_indexes WHERE tablename = :t", {"t": table})
    return {row[0] for row in rows}


# ===========================================================================
# Graph shape
# ===========================================================================


def test_exactly_one_alembic_head() -> None:
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert script.get_heads() == [PR4_REVISION]


def test_pr4_is_a_direct_child_of_the_pr3_revision() -> None:
    """PR-4 must build on the deployed production head, not fork from it."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert script.get_revision(PR4_REVISION).down_revision == PR3_REVISION


def _migration_statements(revision: str) -> str:
    """The executable SQL of a revision, with comments and docstrings removed.

    Prose explaining which columns are deliberately NOT touched would otherwise
    satisfy a substring check that is meant to inspect the statements.
    """
    import ast

    source = Path(ScriptDirectory.from_config(Config(str(ALEMBIC_INI))).get_revision(revision).path).read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
    return ast.unparse(tree)


def test_pr4_migration_leaves_the_raw_capture_columns_alone() -> None:
    """PR-1/PR-3 files stay untouched; PR-4 adds its own additive revision."""
    statements = _migration_statements(PR4_REVISION)
    assert "easyweek_events" in statements
    for untouched in ("body_raw", "body_text", "payload_hash", "DROP TABLE"):
        assert untouched not in statements, f"PR-4 must not touch {untouched}"


def test_pr4_migration_is_additive_and_idempotent_by_construction() -> None:
    statements = _migration_statements(PR4_REVISION)
    assert "ADD COLUMN IF NOT EXISTS" in statements
    assert statements.count("DROP COLUMN IF EXISTS") == 2, "downgrade must drop exactly the two PR-4 columns"


def test_applied_revisions_are_not_edited() -> None:
    """The PR-1 and PR-3 revisions must not gain new DDL."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    pr3 = _migration_statements(PR3_REVISION)
    assert "processed_at" not in pr3, "PR-4 columns must not be back-edited into PR-3"
    assert "error_code" not in pr3
    assert script.get_revision(PR3_REVISION).down_revision == "9a1f4c7b2e3d"


# ===========================================================================
# Model / migration parity
# ===========================================================================


def test_model_declares_the_pr4_columns() -> None:
    from altegio_bot.models.models import EasyWeekEvent

    columns = set(EasyWeekEvent.__table__.columns.keys())
    for column in PR4_COLUMNS:
        assert column in columns, f"{column} is missing from the EasyWeekEvent model"


def test_error_code_is_bounded_and_nullable() -> None:
    from altegio_bot.models.models import EasyWeekEvent

    error_code = EasyWeekEvent.__table__.columns["error_code"]
    assert error_code.nullable is True
    assert error_code.type.length == 64, "error_code must stay bounded"

    processed_at = EasyWeekEvent.__table__.columns["processed_at"]
    assert processed_at.nullable is True
    assert processed_at.type.timezone is True, "processed_at must be timezone-aware"


# ===========================================================================
# Real upgrade / downgrade / upgrade
# ===========================================================================


@pytest.mark.asyncio
async def test_upgrade_downgrade_upgrade_on_a_throwaway_database(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    columns = await _columns_of(temp_db_url, "easyweek_events")
    for column in PR4_COLUMNS:
        assert column in columns, f"upgrade did not add {column}"
    assert PR4_INDEX in await _indexes_of(temp_db_url, "easyweek_events")

    _alembic_ok("downgrade", PR3_REVISION, db_url=temp_db_url)
    after_downgrade = await _columns_of(temp_db_url, "easyweek_events")
    for column in PR4_COLUMNS:
        assert column not in after_downgrade, f"downgrade left {column} behind"
    assert PR4_INDEX not in await _indexes_of(temp_db_url, "easyweek_events")

    # Re-upgrading must work: a rolled-back deploy has to be retryable.
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    assert PR4_COLUMNS[0] in await _columns_of(temp_db_url, "easyweek_events")


@pytest.mark.asyncio
async def test_downgrade_removes_only_pr4_fields(temp_db_url: str) -> None:
    """The research-grade capture columns must survive a PR-4 rollback."""
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    _alembic_ok("downgrade", PR3_REVISION, db_url=temp_db_url)

    columns = await _columns_of(temp_db_url, "easyweek_events")
    for preserved in (
        "id",
        "received_at",
        "status",
        "event_hint",
        "auth_via",
        "payload_hash",
        "content_type",
        "body_raw",
        "body_size_bytes",
        "body_text",
        "body_truncated",
        "query",
        "headers",
        "payload",
    ):
        assert preserved in columns, f"downgrade destroyed the capture column {preserved}"


@pytest.mark.asyncio
async def test_captured_rows_survive_the_pr4_round_trip(temp_db_url: str) -> None:
    """A rollback must not lose captured deliveries."""
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)

    engine = create_async_engine(temp_db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO easyweek_events (status, event_hint, payload, query, headers) "
                    "VALUES ('captured', 'booking-created', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
                )
            )
    finally:
        await engine.dispose()

    _alembic_ok("downgrade", PR3_REVISION, db_url=temp_db_url)
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)

    rows = await _fetch(temp_db_url, "SELECT status, event_hint FROM easyweek_events")
    assert rows == [("captured", "booking-created")]


@pytest.mark.asyncio
async def test_head_upgrade_on_an_empty_database_reaches_pr4(temp_db_url: str) -> None:
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    current = _alembic_ok("current", db_url=temp_db_url)
    assert PR4_REVISION in current
