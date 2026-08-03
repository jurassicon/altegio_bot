"""Migration contract for the PR-4 easyweek_events columns.

Runs upgrade → downgrade → upgrade on a THROWAWAY database, never production.
Skips when no disposable PostgreSQL is reachable, and FAILS instead of skipping
when the environment declares the check mandatory (``ALTEGIO_REQUIRE_MIGTEST=1``),
so a green build can never hide a migration that was silently never exercised.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine

from altegio_bot.models.models import EasyWeekEvent

_REPO_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = _REPO_ROOT / "alembic.ini"

PR4_REVISION = "d4e8a1c39f57"
PR3_REVISION = "c1a7d3f905b2"

PR4_COLUMNS = ("booking_uuid", "processed_at", "error_code", "processing_attempts", "next_retry_at")
PR4_INDEX = "ix_easyweek_events_claim"
PR4_PENDING_INDEX = "ix_easyweek_events_pending_booking"

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


def test_pr4_migration_uses_strict_operations_not_if_not_exists() -> None:
    """Defensive DDL would accept a same-named object of the WRONG shape.

    `ADD COLUMN IF NOT EXISTS` silently succeeds against a pre-existing column
    of a different type and still stamps the revision as applied — the drift
    then surfaces at runtime instead of at deploy time.
    """
    statements = _migration_statements(PR4_REVISION)
    for defensive in ("IF NOT EXISTS", "IF EXISTS"):
        assert defensive not in statements, f"migration must fail closed on drift, found {defensive!r}"
    assert "op.add_column" in statements
    assert "op.create_index" in statements
    assert statements.count("op.drop_column") == len(PR4_COLUMNS), "downgrade must drop exactly the PR-4 columns"
    assert "op.drop_index" in statements


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
    indexes = await _indexes_of(temp_db_url, "easyweek_events")
    assert PR4_INDEX in indexes
    assert PR4_PENDING_INDEX in indexes

    _alembic_ok("downgrade", PR3_REVISION, db_url=temp_db_url)
    after_downgrade = await _columns_of(temp_db_url, "easyweek_events")
    for column in PR4_COLUMNS:
        assert column not in after_downgrade, f"downgrade left {column} behind"
    after = await _indexes_of(temp_db_url, "easyweek_events")
    assert PR4_INDEX not in after
    assert PR4_PENDING_INDEX not in after

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


# ===========================================================================
# Exact structure, and fail-closed on drift (review fix 10)
# ===========================================================================


@pytest.mark.asyncio
async def test_columns_have_the_exact_expected_types_and_nullability(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    rows = await _fetch(
        temp_db_url,
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'easyweek_events' AND column_name IN "
        "('processed_at', 'error_code', 'processing_attempts', 'next_retry_at')",
    )
    actual = {row[0]: (row[1], row[2]) for row in rows}
    assert actual == {
        "processed_at": ("timestamp with time zone", "YES"),
        "error_code": ("character varying", "YES"),
        "processing_attempts": ("integer", "NO"),
        "next_retry_at": ("timestamp with time zone", "YES"),
    }

    length = await _fetch(
        temp_db_url,
        "SELECT character_maximum_length FROM information_schema.columns "
        "WHERE table_name = 'easyweek_events' AND column_name = 'error_code'",
    )
    assert length[0][0] == 64, "error_code must stay bounded at 64"


@pytest.mark.asyncio
async def test_claim_index_matches_the_claim_query(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    rows = await _fetch(
        temp_db_url,
        "SELECT indexdef FROM pg_indexes WHERE tablename = 'easyweek_events' AND indexname = :n",
        {"n": PR4_INDEX},
    )
    assert rows, "the PR-4 index is missing"
    definition = rows[0][0]
    assert "(status, next_retry_at, received_at, id)" in definition, f"wrong index columns/order: {definition}"
    assert "UNIQUE" not in definition.upper()


@pytest.mark.asyncio
async def test_upgrade_fails_closed_on_a_same_named_column_of_the_wrong_type(temp_db_url: str) -> None:
    """Defensive DDL would ACCEPT this drift and still stamp the revision.

    A pre-existing `error_code` of the wrong type must stop the deploy, not be
    silently adopted and then blow up at runtime.
    """
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)

    engine = create_async_engine(temp_db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("ALTER TABLE easyweek_events ADD COLUMN error_code INTEGER"))
    finally:
        await engine.dispose()

    failed = _run_alembic("upgrade", PR4_REVISION, db_url=temp_db_url)
    assert failed.returncode != 0, "upgrade accepted a same-named column of the wrong type"

    current = _alembic_ok("current", db_url=temp_db_url)
    assert PR4_REVISION not in current, "the revision was stamped despite the drift"


@pytest.mark.asyncio
async def test_upgrade_fails_closed_on_a_same_named_index_with_a_different_definition(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)

    engine = create_async_engine(temp_db_url)
    try:
        async with engine.begin() as conn:
            # Same NAME, wrong columns.
            await conn.execute(text(f"CREATE INDEX {PR4_INDEX} ON easyweek_events (event_hint)"))
    finally:
        await engine.dispose()

    failed = _run_alembic("upgrade", PR4_REVISION, db_url=temp_db_url)
    assert failed.returncode != 0, "upgrade accepted a same-named index with the wrong definition"

    current = _alembic_ok("current", db_url=temp_db_url)
    assert PR4_REVISION not in current, "the revision was stamped despite the drift"


@pytest.mark.asyncio
async def test_processing_attempts_defaults_to_zero_and_is_not_null(temp_db_url: str) -> None:
    """Existing captured rows must become retryable without a backfill step."""
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

    rows = await _fetch(temp_db_url, "SELECT processing_attempts, next_retry_at FROM easyweek_events")
    assert rows == [(0, None)]


@pytest.mark.asyncio
async def test_upgrade_fails_closed_on_a_wrong_typed_retry_column(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)

    engine = create_async_engine(temp_db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("ALTER TABLE easyweek_events ADD COLUMN processing_attempts TEXT"))
    finally:
        await engine.dispose()

    failed = _run_alembic("upgrade", PR4_REVISION, db_url=temp_db_url)
    assert failed.returncode != 0, "upgrade accepted a same-named column of the wrong type"
    assert PR4_REVISION not in _alembic_ok("current", db_url=temp_db_url)


@pytest.mark.asyncio
async def test_pending_booking_index_matches_the_ordering_query(temp_db_url: str) -> None:
    """The correlated NOT EXISTS needs uid + capture order, on non-terminal rows."""
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    rows = await _fetch(
        temp_db_url,
        "SELECT indexdef FROM pg_indexes WHERE tablename = 'easyweek_events' AND indexname = :n",
        {"n": PR4_PENDING_INDEX},
    )
    assert rows, "the per-booking ordering index is missing"
    definition = rows[0][0]
    assert "uid" in definition, f"index does not key on the booking uid: {definition}"
    assert "received_at" in definition and "id" in definition, f"missing capture order: {definition}"
    assert "WHERE" in definition.upper(), "the index should be partial on non-terminal statuses"
    for terminal in ("captured", "processing"):
        assert terminal in definition, f"partial predicate misses {terminal}: {definition}"


@pytest.mark.asyncio
async def test_the_claim_query_runs_against_malformed_payloads(temp_db_url: str) -> None:
    """`payload ->> 'uid'` must be NULL-safe for every JSONB shape."""
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)

    engine = create_async_engine(temp_db_url)
    try:
        async with engine.begin() as conn:
            for payload in ("'{}'::jsonb", "'[1,2,3]'::jsonb", "'\"text\"'::jsonb", "'42'::jsonb", "'null'::jsonb"):
                await conn.execute(
                    text(
                        "INSERT INTO easyweek_events (status, event_hint, payload, query, headers) "
                        f"VALUES ('captured', 'booking-created', {payload}, '{{}}'::jsonb, '{{}}'::jsonb)"
                    )
                )
    finally:
        await engine.dispose()

    # The real eligibility predicate, exercised against those rows.
    rows = await _fetch(
        temp_db_url,
        """
        SELECT e.id FROM easyweek_events e
        WHERE e.status = 'captured'
          AND (e.next_retry_at IS NULL OR e.next_retry_at <= now())
          AND (
            (e.payload ->> 'uid') IS NULL
            OR NOT EXISTS (
              SELECT 1 FROM easyweek_events p
              WHERE p.id <> e.id
                AND p.status IN ('captured', 'processing')
                AND (p.payload ->> 'uid') = (e.payload ->> 'uid')
                AND (p.received_at, p.id) < (e.received_at, e.id)
            )
          )
        ORDER BY e.received_at, e.id
        """,
    )
    assert len(rows) == 5, "malformed payloads must stay claimable, not error or stall"


# ===========================================================================
# booking_uuid: the canonical causal-ordering key and its backfill
# ===========================================================================


def _backfill_batch_size() -> int:
    """The migration's own batch constant, so tests size themselves from it."""
    source = Path(ScriptDirectory.from_config(Config(str(ALEMBIC_INI))).get_revision(PR4_REVISION).path).read_text()
    match = re.search(r"_BACKFILL_BATCH\s*=\s*(\d+)", source)
    assert match, "the migration no longer defines _BACKFILL_BATCH"
    return int(match.group(1))


async def _exec(db_url: str, sql: str, params: dict | list | None = None) -> None:
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(sql), params or {})
    finally:
        await engine.dispose()


def test_booking_uuid_is_declared_on_the_model() -> None:
    column = EasyWeekEvent.__table__.c["booking_uuid"]
    assert column.nullable is True, "a malformed or missing uid must stay NULL"
    assert isinstance(column.type, postgresql.UUID), "the ordering key must be a real UUID column"


def test_the_pending_index_keys_on_the_canonical_column_not_raw_json() -> None:
    """Raw `payload ->> 'uid'` would split one booking across causal chains."""
    statements = _migration_statements(PR4_REVISION)
    assert '"booking_uuid", "received_at", "id"' in statements or (
        "'booking_uuid', 'received_at', 'id'" in statements
    ), "the pending-booking index must be keyed on booking_uuid"
    # The backfill legitimately READS `payload ->> 'uid'` to canonicalise it;
    # what must never happen is keying the INDEX on that raw text.
    index_call = statements[statements.index("_PENDING_BOOKING_INDEX,") :]
    assert "payload" not in index_call.split("postgresql_where")[0], "the index must not key on raw payload text"


def test_the_backfill_does_not_use_an_unguarded_sql_cast() -> None:
    """`(payload->>'uid')::uuid` aborts the whole migration on one bad row."""
    statements = _migration_statements(PR4_REVISION)
    assert "->> 'uid')::uuid" not in statements
    assert "::uuid" not in statements.replace("CAST(:value AS uuid)", "")


_BACKFILL_ROWS = (
    # (payload uid, expected canonical value or None)
    ("ac15372d-7422-4fc6-8fcb-b520bbffa669", "ac15372d-7422-4fc6-8fcb-b520bbffa669"),
    ("AC15372D-7422-4FC6-8FCB-B520BBFFA669", "ac15372d-7422-4fc6-8fcb-b520bbffa669"),
    ("  ac15372d-7422-4fc6-8fcb-b520bbffa669  ", "ac15372d-7422-4fc6-8fcb-b520bbffa669"),
    ("{ac15372d-7422-4fc6-8fcb-b520bbffa669}", "ac15372d-7422-4fc6-8fcb-b520bbffa669"),
    ("ac15372d74224fc68fcbb520bbffa669", "ac15372d-7422-4fc6-8fcb-b520bbffa669"),
    ("not-a-uuid", None),
    ("", None),
)


@pytest.mark.asyncio
async def test_backfill_canonicalises_the_existing_backlog(temp_db_url: str) -> None:
    """Rows captured BEFORE this revision must get the ordering key too.

    Otherwise the whole pre-PR-4 backlog would have a NULL key and none of it
    would be serialised per booking when processing is finally switched on.
    """
    # Bring the schema to the revision BEFORE PR-4 and seed a research backlog.
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)

    insert = (
        "INSERT INTO easyweek_events (id, status, payload, query, headers, body_size_bytes, payload_hash) "
        "VALUES (:id, 'captured', CAST(:payload AS jsonb), '{}', '{}', 0, :payload_hash)"
    )
    for index, (raw_uid, _expected) in enumerate(_BACKFILL_ROWS, start=1):
        await _exec(
            temp_db_url,
            insert,
            {
                "id": index,
                "payload": json.dumps({"uid": raw_uid, "id": 1000 + index}),
                "payload_hash": f"hash-{index}",
            },
        )
    # Shapes that carry no string uid at all must survive untouched.
    await _exec(
        temp_db_url,
        insert,
        {"id": 90, "payload": json.dumps({"id": 1}), "payload_hash": "hash-missing"},
    )
    await _exec(
        temp_db_url,
        insert,
        {"id": 91, "payload": json.dumps({"uid": 12345}), "payload_hash": "hash-number"},
    )
    await _exec(
        temp_db_url,
        insert,
        {"id": 92, "payload": json.dumps({"uid": ["x"]}), "payload_hash": "hash-list"},
    )

    before = await _fetch(
        temp_db_url,
        "SELECT id, status, payload_hash, payload FROM easyweek_events ORDER BY id",
    )

    # The migration must not abort on the malformed rows.
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)

    rows = dict(
        (row[0], row[1])
        for row in await _fetch(temp_db_url, "SELECT id, booking_uuid FROM easyweek_events ORDER BY id")
    )
    for index, (_raw_uid, expected) in enumerate(_BACKFILL_ROWS, start=1):
        actual = rows[index]
        assert (str(actual) if actual is not None else None) == expected, f"row {index} backfilled incorrectly"
    for null_row in (90, 91, 92):
        assert rows[null_row] is None

    # Capture data is untouched by the backfill.
    after = await _fetch(
        temp_db_url,
        "SELECT id, status, payload_hash, payload FROM easyweek_events ORDER BY id",
    )
    assert after == before, "the backfill changed status, payload_hash or payload"


@pytest.mark.asyncio
async def test_backfill_survives_a_downgrade_and_a_second_upgrade(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)
    await _exec(
        temp_db_url,
        "INSERT INTO easyweek_events (id, status, payload, query, headers, body_size_bytes) "
        "VALUES (1, 'captured', CAST(:payload AS jsonb), '{}', '{}', 0)",
        {"payload": json.dumps({"uid": "AC15372D-7422-4FC6-8FCB-B520BBFFA669"})},
    )

    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    assert "booking_uuid" in await _columns_of(temp_db_url, "easyweek_events")

    _alembic_ok("downgrade", PR3_REVISION, db_url=temp_db_url)
    assert "booking_uuid" not in await _columns_of(temp_db_url, "easyweek_events")
    # The row itself is never deleted by a downgrade.
    assert (await _fetch(temp_db_url, "SELECT count(*) FROM easyweek_events"))[0][0] == 1

    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    value = (await _fetch(temp_db_url, "SELECT booking_uuid FROM easyweek_events WHERE id = 1"))[0][0]
    assert str(value) == "ac15372d-7422-4fc6-8fcb-b520bbffa669", "the re-upgrade must backfill again"


@pytest.mark.asyncio
async def test_pending_index_is_partial_and_keyed_on_the_canonical_column(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)
    definition = (
        await _fetch(
            temp_db_url,
            "SELECT indexdef FROM pg_indexes WHERE indexname = :name",
            {"name": PR4_PENDING_INDEX},
        )
    )[0][0]
    assert "booking_uuid" in definition
    assert "payload" not in definition, "the index must not key on raw payload text"
    assert "received_at" in definition and "id" in definition
    normalised = definition.replace("(", "").replace(")", "").replace("::text", "")
    assert "status = ANY" in normalised or "status IN" in normalised or "captured" in normalised


# ===========================================================================
# The backfill must be bounded-memory and must never stall on a bad row
# ===========================================================================


def test_backfill_uses_keyset_pagination_not_a_full_table_load() -> None:
    """A research-capture backlog can be large; peak memory must be one batch."""
    statements = _migration_statements(PR4_REVISION)
    assert "id > :last_id" in statements, "the scan must be keyset-paginated on id"
    assert "ORDER BY id LIMIT" in statements
    assert "OFFSET" not in statements.upper(), "OFFSET re-scans the prefix on every page"
    # The cursor must advance on every row, valid or not, or one malformed value
    # would be re-read forever.
    body = statements[statements.index("def _backfill_booking_uuid") :]
    assert "last_id = row.id" in body
    advance = body.index("last_id = row.id")
    parse = body.index("uuid.UUID(raw.strip())")
    assert advance < parse, "the cursor must advance before parsing can fail"


@pytest.mark.asyncio
async def test_backfill_spans_multiple_batches_with_malformed_rows_throughout(temp_db_url: str) -> None:
    """More rows than one batch, with bad values at the start, middle and edge.

    Sized from the migration's own constant so the loop is genuinely exercised:
    a single-batch dataset would pass even if the keyset scan were broken.
    """
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)

    batch = _backfill_batch_size()
    total = batch * 2 + 1  # three batches, the last one partial

    valid = "ac15372d-7422-4fc6-8fcb-b520bbffa669"
    spellings = [
        valid,
        valid.upper(),
        "{" + valid + "}",
        valid.replace("-", ""),
        f"  {valid}  ",
        f"urn:uuid:{valid}",
    ]
    # First row, a middle row, exactly the first batch boundary, and the last row.
    malformed_ids = {1, batch, batch + 1, total // 2, total}

    rows = []
    for row_id in range(1, total + 1):
        if row_id in malformed_ids:
            payload = {"uid": "not-a-uuid", "id": row_id}
        else:
            payload = {"uid": spellings[row_id % len(spellings)], "id": row_id}
        rows.append({"id": row_id, "payload": json.dumps(payload), "payload_hash": f"h{row_id}"})
    # Shapes with no string uid at all.
    rows.append({"id": total + 1, "payload": json.dumps({"id": 1}), "payload_hash": "h-missing"})
    rows.append({"id": total + 2, "payload": json.dumps({"uid": 999}), "payload_hash": "h-number"})

    await _exec(
        temp_db_url,
        "INSERT INTO easyweek_events (id, status, payload, query, headers, body_size_bytes, payload_hash) "
        "VALUES (:id, 'captured', CAST(:payload AS jsonb), '{}', '{}', 0, :payload_hash)",
        rows,
    )

    before = await _fetch(temp_db_url, "SELECT id, status, payload_hash, payload FROM easyweek_events ORDER BY id")

    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)

    keys = dict(
        (row[0], row[1])
        for row in await _fetch(temp_db_url, "SELECT id, booking_uuid FROM easyweek_events ORDER BY id")
    )
    for row_id in range(1, total + 1):
        if row_id in malformed_ids:
            assert keys[row_id] is None, f"row {row_id} should have stayed NULL"
        else:
            assert str(keys[row_id]) == valid, f"row {row_id} was not canonicalised"
    assert keys[total + 1] is None and keys[total + 2] is None

    after = await _fetch(temp_db_url, "SELECT id, status, payload_hash, payload FROM easyweek_events ORDER BY id")
    assert after == before, "the backfill changed capture data"


@pytest.mark.asyncio
async def test_backfill_terminates_when_every_row_is_malformed(temp_db_url: str) -> None:
    """The loop must end even when nothing is ever written."""
    _alembic_ok("upgrade", PR3_REVISION, db_url=temp_db_url)
    for row_id in range(1, 21):
        await _exec(
            temp_db_url,
            "INSERT INTO easyweek_events (id, status, payload, query, headers, body_size_bytes) "
            "VALUES (:id, 'captured', CAST(:payload AS jsonb), '{}', '{}', 0)",
            {"id": row_id, "payload": json.dumps({"uid": f"bad-{row_id}"})},
        )

    _alembic_ok("upgrade", PR4_REVISION, db_url=temp_db_url)

    nulls = (await _fetch(temp_db_url, "SELECT count(*) FROM easyweek_events WHERE booking_uuid IS NULL"))[0][0]
    assert nulls == 20
    assert (await _fetch(temp_db_url, "SELECT count(*) FROM easyweek_events"))[0][0] == 20
