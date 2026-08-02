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
import re
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


def _current_head_revision() -> str:
    """The single head, read from the revision graph rather than hardcoded.

    These assertions mean "``upgrade head`` actually arrived at the head". A
    literal would silently start naming an OLD revision the moment a later PR
    adds one on top — exactly the staleness the comment below warns about for
    relative ``-1`` targets — and the check would then pass for the wrong state.
    """
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    heads = ScriptDirectory.from_config(Config(str(_ROOT / "alembic.ini"))).get_heads()
    assert len(heads) == 1, f"expected exactly one Alembic head, got {heads}"
    return heads[0]


_HEAD_REVISION = _current_head_revision()
# Named explicitly rather than reached with a relative "-1": once a revision is
# added on top, "-1" silently stops meaning "undo THIS migration" and the test
# would assert against the wrong schema.
_OPERATOR_RELAY_REVISION = "9a1f4c7b2e3d"
_OPERATOR_RELAY_PARENT = "8705ec49cc73"
_PRE_PROVIDER_SCOPE_REVISION = _OPERATOR_RELAY_REVISION

_PROVIDER_TABLES = (
    "clients",
    "records",
    "message_templates",
    "message_jobs",
    "whatsapp_senders",
)
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
    # Everything added to easyweek_events ABOVE _BASE_REVISION, and nothing else:
    #   8705ec49cc73 (PR-1 follow-up) -> body_raw, body_size_bytes
    #   d4e8a1c39f57 (PR-4)           -> processed_at, error_code
    # The point of the assertion is that the downgrade is scoped — it must not
    # take the base capture columns with it.
    assert removed == {
        "body_raw",
        "body_size_bytes",
        "processed_at",
        "error_code",
    }, f"downgrade removed too much: {removed}"

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
    #
    # The replay stops at the compatibility revision, which is the ONLY thing
    # this scenario is about. Going all the way to head would also replay PR-3
    # against a schema that already carries its objects, and PR-3's DDL is
    # deliberately fail-closed — it must refuse that, and does (see
    # test_upgrade_fails_closed_on_unexpected_schema_objects).
    _alembic_ok("stamp", _BASE_REVISION, db_url=temp_db_url)
    assert set(await _raw_body_columns(temp_db_url)) == {"body_raw", "body_size_bytes"}

    _alembic_ok("upgrade", _OPERATOR_RELAY_PARENT, db_url=temp_db_url)

    assert set(await _raw_body_columns(temp_db_url)) == {"body_raw", "body_size_bytes"}
    assert _OPERATOR_RELAY_PARENT in _alembic_ok("current", db_url=temp_db_url)

    # The physical schema is still the full head schema from step 1, so bring
    # the version table back in line with it.
    _alembic_ok("stamp", "head", db_url=temp_db_url)
    assert _HEAD_REVISION in _alembic_ok("current", db_url=temp_db_url)


# ---------------------------------------------------------------------------
# Operator-relay durable-Outbox schema: the DB-level last line of defence
# ---------------------------------------------------------------------------


async def _exec(db_url: str, sql: str, params: dict | None = None) -> None:
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:  # begin() commits on success
            await conn.execute(text(sql), params or {})
    finally:
        await engine.dispose()


_EVT = (
    "INSERT INTO whatsapp_events (id, dedupe_key, status, query, headers, payload) "
    "VALUES (:id, :dk, 'processed', '{}', '{}', '{}')"
)
_OB = (
    "INSERT INTO outbox_messages "
    "(company_id, phone_e164, template_code, language, body, status, scheduled_at, "
    " message_source, meta, source_whatsapp_event_id) "
    "VALUES (1, :phone, 'operator_relay', 'de', 'b', :status, now(), :src, '{}', :sev)"
)


@pytest.mark.asyncio
async def test_operator_relay_outbox_schema_and_constraints(temp_db_url) -> None:
    """Real-PostgreSQL proof of the durable-Outbox idempotency schema (§23–24)."""
    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    # ── columns: types + nullability ──────────────────────────────────────
    cols = {
        r[0]: r
        for r in await _fetch(
            temp_db_url,
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_name='outbox_messages' "
            "AND column_name IN ('source_whatsapp_event_id','attempt_started_at')",
        )
    }
    assert cols["source_whatsapp_event_id"][1] == "bigint"
    assert cols["source_whatsapp_event_id"][2] == "YES"  # nullable for historical/bot rows
    assert cols["attempt_started_at"][1] == "timestamp with time zone"
    assert cols["attempt_started_at"][2] == "YES"

    # ── foreign key → whatsapp_events(id), ON DELETE SET NULL ─────────────
    fk = await _fetch(
        temp_db_url,
        "SELECT c.confdeltype, tgt.relname, att.attname "
        "FROM pg_constraint c "
        "JOIN pg_class src ON src.oid = c.conrelid "
        "JOIN pg_class tgt ON tgt.oid = c.confrelid "
        "JOIN pg_attribute att ON att.attrelid = c.conrelid AND att.attnum = c.conkey[1] "
        "WHERE c.contype='f' AND src.relname='outbox_messages' AND att.attname='source_whatsapp_event_id'",
    )
    assert fk, "FK on source_whatsapp_event_id is missing"
    # confdeltype is a PostgreSQL "char" → asyncpg returns it as bytes.
    confdeltype = fk[0][0]
    if isinstance(confdeltype, bytes):
        confdeltype = confdeltype.decode()
    assert confdeltype == "n"  # 'n' == ON DELETE SET NULL
    assert fk[0][1] == "whatsapp_events"

    # ── partial unique index with the NOT NULL predicate ──────────────────
    idx = await _fetch(
        temp_db_url,
        "SELECT indexdef FROM pg_indexes WHERE indexname='uq_outbox_source_whatsapp_event_id'",
    )
    assert idx, "partial unique index is missing"
    indexdef = idx[0][0]
    assert "UNIQUE" in indexdef.upper()
    assert "source_whatsapp_event_id IS NOT NULL" in indexdef

    # ── behavioral: one operator Outbox per source event ──────────────────
    await _exec(temp_db_url, _EVT, {"id": 90001, "dk": "mig:A"})
    await _exec(temp_db_url, _EVT, {"id": 90002, "dk": "mig:B"})
    await _exec(temp_db_url, _OB, {"phone": "+49a", "status": "sent", "src": "operator", "sev": 90001})

    # a second operator Outbox for the SAME source event is rejected
    with pytest.raises(Exception) as exc_info:
        await _exec(temp_db_url, _OB, {"phone": "+49a2", "status": "queued", "src": "operator", "sev": 90001})
    assert "uq_outbox_source_whatsapp_event_id" in str(exc_info.value) or "unique" in str(exc_info.value).lower()

    # several historical/non-operator rows with NULL source are all allowed
    for i in range(3):
        await _exec(temp_db_url, _OB, {"phone": f"+49n{i}", "status": "sent", "src": "bot", "sev": None})
    null_rows = await _fetch(
        temp_db_url,
        "SELECT count(*) FROM outbox_messages WHERE source_whatsapp_event_id IS NULL",
    )
    assert null_rows[0][0] == 3

    # ON DELETE SET NULL: deleting the event keeps the audit row, nulls the link
    await _exec(temp_db_url, _OB, {"phone": "+49b", "status": "sent", "src": "operator", "sev": 90002})
    await _exec(temp_db_url, "DELETE FROM whatsapp_events WHERE id = 90002")
    kept = await _fetch(
        temp_db_url,
        "SELECT source_whatsapp_event_id FROM outbox_messages WHERE phone_e164 = '+49b'",
    )
    assert kept and kept[0][0] is None  # row kept, link nulled

    # ── downgrade removes the whole schema addition ───────────────────────
    # Target the operator-relay revision's PARENT by name. A relative "-1" only
    # undid this migration while it happened to be head; every later revision
    # would quietly shift what it means.
    _alembic_ok("downgrade", _OPERATOR_RELAY_PARENT, db_url=temp_db_url)
    gone = await _fetch(
        temp_db_url,
        "SELECT count(*) FROM information_schema.columns WHERE table_name='outbox_messages' "
        "AND column_name IN ('source_whatsapp_event_id','attempt_started_at')",
    )
    assert gone[0][0] == 0
    assert (
        await _fetch(
            temp_db_url, "SELECT count(*) FROM pg_indexes WHERE indexname='uq_outbox_source_whatsapp_event_id'"
        )
    )[0][0] == 0
    assert (
        await _fetch(
            temp_db_url,
            "SELECT count(*) FROM pg_constraint WHERE conname='fk_outbox_source_whatsapp_event_id'",
        )
    )[0][0] == 0

    # ── re-upgrade restores everything; still exactly one head ────────────
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    restored = await _fetch(
        temp_db_url,
        "SELECT count(*) FROM information_schema.columns WHERE table_name='outbox_messages' "
        "AND column_name IN ('source_whatsapp_event_id','attempt_started_at')",
    )
    assert restored[0][0] == 2
    heads = _alembic_ok("heads", db_url=temp_db_url)
    assert heads.count("(head)") == 1


# ---------------------------------------------------------------------------
# PR-3: provider scope + EasyWeek identity, proven on real PostgreSQL
# ---------------------------------------------------------------------------
#
# Structural guards read the migration as text; only a real database can prove
# that existing rows survive the backfill, that the database default fires for
# an INSERT that never names `provider`, and that the partial unique index
# really constrains EasyWeek rows and only those.

# Altegio seed rows written while the schema is still at the pre-PR-3 revision,
# i.e. before a `provider` column exists at all.
_SEED_SQL = (
    "INSERT INTO clients (company_id, altegio_client_id, phone_e164, raw) VALUES (100, 11, '+4900000001', '{}')",
    "INSERT INTO records (company_id, altegio_record_id, is_deleted, raw) VALUES (100, 21, false, '{}')",
    "INSERT INTO message_templates (company_id, code, language, body, is_active) "
    "VALUES (100, 'record_created', 'de', 'hello', true)",
    "INSERT INTO message_jobs (company_id, job_type, run_at, dedupe_key, payload) "
    "VALUES (100, 'record_created', now(), 'pr3:seed:1', '{}')",
    "INSERT INTO whatsapp_senders (company_id, sender_code, phone_number_id, is_active) "
    "VALUES (100, 'default', 'PNID-1', true)",
)


async def _counts(db_url: str) -> dict[str, int]:
    counts = {}
    for table in _PROVIDER_TABLES:
        rows = await _fetch(db_url, f"SELECT count(*) FROM {table}")
        counts[table] = rows[0][0]
    return counts


async def _providers(db_url: str, table: str) -> list[str]:
    return [row[0] for row in await _fetch(db_url, f"SELECT provider FROM {table}")]


async def _column(db_url: str, table: str, column: str) -> tuple | None:
    rows = await _fetch(
        db_url,
        "SELECT data_type, is_nullable, column_default, character_maximum_length "
        "FROM information_schema.columns WHERE table_name = :t AND column_name = :c",
        {"t": table, "c": column},
    )
    return rows[0] if rows else None


async def _constraint_names(db_url: str, table: str) -> set[str]:
    rows = await _fetch(
        db_url,
        "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid WHERE t.relname = :t",
        {"t": table},
    )
    return {row[0] for row in rows}


async def _index_defs(db_url: str, table: str) -> dict[str, str]:
    rows = await _fetch(
        db_url,
        "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = :t",
        {"t": table},
    )
    return {row[0]: row[1] for row in rows}


@pytest.mark.asyncio
async def test_provider_scope_migration_scenarios(temp_db_url) -> None:
    """PR-3 upgrade/downgrade behaviour, sequentially on one throwaway database.

    One test on purpose: each stage is the setup for the next, and a database
    per scenario would multiply the runtime for no extra coverage.
    """
    # ═══════════════════════════════════════════════ 1. fresh database → head
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    assert _HEAD_REVISION in _alembic_ok("current", db_url=temp_db_url)

    for table in _PROVIDER_TABLES:
        data_type, is_nullable, default, max_length = await _column(temp_db_url, table, "provider")
        assert data_type == "character varying", table
        assert max_length == 32, table
        assert is_nullable == "NO", f"{table}.provider must be NOT NULL"
        assert "altegio" in (default or ""), f"{table}.provider lost its server default"

    # EasyWeek identity columns, with the types the plan requires.
    booking_uuid = await _column(temp_db_url, "records", "easyweek_booking_uuid")
    assert booking_uuid[0] == "uuid" and booking_uuid[1] == "YES"
    hash_id = await _column(temp_db_url, "records", "easyweek_booking_hash_id")
    # A bounded string, never an integer: the hash can carry leading zeros.
    assert hash_id[0] == "character varying" and hash_id[3] == 64
    meta_name = await _column(temp_db_url, "message_templates", "meta_template_name")
    assert meta_name[0] == "character varying" and meta_name[3] == 128

    # Provider-scoped uniques replaced the old ones outright.
    assert "uq_clients_provider_company_altegio_id" in await _constraint_names(temp_db_url, "clients")
    assert "uq_clients_company_altegio_id" not in await _constraint_names(temp_db_url, "clients")
    assert "uq_records_provider_company_altegio_id" in await _constraint_names(temp_db_url, "records")
    assert "uq_records_company_altegio_id" not in await _constraint_names(temp_db_url, "records")
    senders = await _constraint_names(temp_db_url, "whatsapp_senders")
    assert "uq_whatsapp_senders_provider_company_code" in senders
    assert "uq_whatsapp_senders_company_code" not in senders

    # The partial unique index and its predicate.
    record_indexes = await _index_defs(temp_db_url, "records")
    partial = record_indexes["uq_records_easyweek_booking_uuid"]
    assert "UNIQUE" in partial.upper()
    # PostgreSQL echoes the predicate with its own parentheses and ::text casts,
    # e.g. "WHERE (((provider)::text = 'easyweek'::text) AND ...)".
    predicate = partial.replace("(", "").replace(")", "").replace("::text", "")
    assert "provider = 'easyweek'" in predicate
    assert "easyweek_booking_uuid IS NOT NULL" in predicate

    # Scoped composite indexes swapped; single-column ones preserved.
    client_indexes = await _index_defs(temp_db_url, "clients")
    assert "ix_clients_provider_company_phone" in client_indexes
    assert "ix_clients_company_phone" not in client_indexes
    assert "ix_clients_company_id" in client_indexes
    job_indexes = await _index_defs(temp_db_url, "message_jobs")
    assert "ix_message_jobs_provider_company_type_status" in job_indexes
    assert "ix_message_jobs_company_type_status" not in job_indexes
    assert "ix_message_jobs_status_run_at" in job_indexes
    assert "ix_message_templates_provider_company_code_lang" in await _index_defs(temp_db_url, "message_templates")

    # ═════════════════════════════ 2. previous revision + existing Altegio rows
    _alembic_ok("downgrade", _PRE_PROVIDER_SCOPE_REVISION, db_url=temp_db_url)
    assert await _column(temp_db_url, "clients", "provider") is None
    assert await _column(temp_db_url, "records", "easyweek_booking_uuid") is None
    assert await _column(temp_db_url, "message_templates", "meta_template_name") is None
    # The pre-PR-3 objects are genuinely restored, not just dropped.
    assert "uq_clients_company_altegio_id" in await _constraint_names(temp_db_url, "clients")
    assert "ix_clients_company_phone" in await _index_defs(temp_db_url, "clients")
    assert "ix_message_jobs_company_type_status" in await _index_defs(temp_db_url, "message_jobs")

    for statement in _SEED_SQL:
        await _exec(temp_db_url, statement)
    seeded = await _counts(temp_db_url)
    assert all(count == 1 for count in seeded.values()), seeded

    _alembic_ok("upgrade", "head", db_url=temp_db_url)

    # ═════════════════════ 3. every pre-existing row is backfilled as 'altegio'
    for table in _PROVIDER_TABLES:
        assert await _providers(temp_db_url, table) == ["altegio"], table
    assert await _counts(temp_db_url) == seeded, "upgrade must not lose or duplicate rows"

    # ═══════════════ 9. upgrade → downgrade → upgrade keeps Altegio-only rows
    before = await _counts(temp_db_url)
    _alembic_ok("downgrade", _PRE_PROVIDER_SCOPE_REVISION, db_url=temp_db_url)
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    assert await _counts(temp_db_url) == before, "a downgrade/upgrade round trip lost rows"
    for table in _PROVIDER_TABLES:
        assert await _providers(temp_db_url, table) == ["altegio"], table

    # ═══════════════════════ 4. an INSERT that never names provider gets it anyway
    await _exec(
        temp_db_url,
        "INSERT INTO clients (company_id, altegio_client_id, raw) VALUES (200, 31, '{}')",
    )
    default_applied = await _fetch(
        temp_db_url, "SELECT provider FROM clients WHERE company_id = 200 AND altegio_client_id = 31"
    )
    assert default_applied[0][0] == "altegio"

    # ══════════ 5. the same (company_id, external id) is allowed per provider
    await _exec(
        temp_db_url,
        "INSERT INTO clients (provider, company_id, altegio_client_id, raw) VALUES ('easyweek', 200, 31, '{}')",
    )
    both = await _fetch(
        temp_db_url,
        "SELECT provider FROM clients WHERE company_id = 200 AND altegio_client_id = 31 ORDER BY provider",
    )
    assert [row[0] for row in both] == ["altegio", "easyweek"]

    # ═══════════════════ 6. the same key WITHIN one provider is still rejected
    with pytest.raises(Exception) as exc_info:
        await _exec(
            temp_db_url,
            "INSERT INTO clients (provider, company_id, altegio_client_id, raw) VALUES ('easyweek', 200, 31, '{}')",
        )
    assert "uq_clients_provider_company_altegio_id" in str(exc_info.value)

    # ══════════════════ 7. a repeated non-null EasyWeek booking UUID is rejected
    duplicate_uuid = "11111111-2222-3333-4444-555555555555"
    await _exec(
        temp_db_url,
        "INSERT INTO records (provider, company_id, altegio_record_id, easyweek_booking_uuid, is_deleted, raw) "
        f"VALUES ('easyweek', 300, 41, '{duplicate_uuid}', false, '{{}}')",
    )
    with pytest.raises(Exception) as exc_info:
        await _exec(
            temp_db_url,
            "INSERT INTO records (provider, company_id, altegio_record_id, easyweek_booking_uuid, is_deleted, raw) "
            f"VALUES ('easyweek', 300, 42, '{duplicate_uuid}', false, '{{}}')",
        )
    assert "uq_records_easyweek_booking_uuid" in str(exc_info.value)

    # The index is genuinely provider-scoped: the same UUID on Altegio rows is
    # outside the predicate, so it is not constrained.
    for record_id in (43, 44):
        await _exec(
            temp_db_url,
            "INSERT INTO records (provider, company_id, altegio_record_id, easyweek_booking_uuid, is_deleted, raw) "
            f"VALUES ('altegio', 300, {record_id}, '{duplicate_uuid}', false, '{{}}')",
        )

    # ══════════════════ 8. several EasyWeek rows with a NULL UUID are allowed
    for record_id in (51, 52, 53):
        await _exec(
            temp_db_url,
            "INSERT INTO records (provider, company_id, altegio_record_id, is_deleted, raw) "
            f"VALUES ('easyweek', 300, {record_id}, false, '{{}}')",
        )
    null_uuid_rows = await _fetch(
        temp_db_url,
        "SELECT count(*) FROM records WHERE provider = 'easyweek' AND easyweek_booking_uuid IS NULL",
    )
    assert null_uuid_rows[0][0] == 3

    # ═══════ downgrade now FAILS CLOSED: cross-provider duplicates exist above
    refused = _run_alembic("downgrade", _PRE_PROVIDER_SCOPE_REVISION, db_url=temp_db_url)
    assert refused.returncode != 0, "downgrade must refuse to restore an impossible unique constraint"
    assert "Cannot downgrade" in refused.stdout + refused.stderr
    # ... and the refusal changed nothing.
    assert _HEAD_REVISION in _alembic_ok("current", db_url=temp_db_url)
    assert "uq_clients_provider_company_altegio_id" in await _constraint_names(temp_db_url, "clients")

    # ════════════════════ 10. exactly one head, and no PR-3 model/schema drift
    assert _alembic_ok("heads", db_url=temp_db_url).count("(head)") == 1

    # `alembic check` autogenerates against the live schema. The repository has
    # two PRE-EXISTING drift items, both on tables PR-3 never touches:
    #   * ix_outbox_messages_reply_context_lookup — an expression index created
    #     only by migration d0e1f2a3b4c5 and never declared in models.py, so
    #     autogenerate has always reported it as "removed";
    #   * promo_leads.location_id — Integer in the model, BigInteger in
    #     migration a3b4c5d6e7f8.
    # Fixing either would be unrelated refactoring outside this PR, so the
    # assertion is scoped: PR-3's own objects must produce NO drift.
    drift = _run_alembic("check", db_url=temp_db_url)
    report = drift.stdout + drift.stderr
    operations = [line for line in report.splitlines() if "upgrade operations detected" in line]
    pr3_objects = (
        *_PROVIDER_TABLES,
        "provider",
        "easyweek_booking_uuid",
        "easyweek_booking_hash_id",
        "meta_template_name",
        "uq_clients_provider_company_altegio_id",
        "uq_records_provider_company_altegio_id",
        "uq_whatsapp_senders_provider_company_code",
        "uq_records_easyweek_booking_uuid",
        "ix_clients_provider_company_phone",
        "ix_message_jobs_provider_company_type_status",
        "ix_message_templates_provider_company_code_lang",
    )
    # Whole-identifier matching: a bare "provider" substring would otherwise hit
    # the unrelated `provider_message_id` column of outbox_messages.
    for line in operations:
        for name in pr3_objects:
            assert not re.search(rf"\b{re.escape(name)}\b", line), (
                f"PR-3 introduced model/schema drift on {name!r}:\n{line}"
            )
    if drift.returncode != 0:
        assert "outbox_messages" in report or "promo_leads" in report, (
            f"unexpected model/schema drift after the PR-3 upgrade:\n{report}"
        )


@pytest.mark.asyncio
async def test_upgrade_fails_closed_on_unexpected_schema_objects(temp_db_url) -> None:
    """A same-named object that is NOT what PR-3 builds must abort the upgrade.

    The migration deliberately dropped its ``IF NOT EXISTS`` guards and its
    name-only ``_add_constraint_if_absent`` helper. Those would have accepted a
    drifted production object — a constraint over the wrong columns, an index
    with the wrong predicate — and silently declared the schema correct. Here
    the drift is planted on purpose and the upgrade has to refuse it, leaving
    the previous schema and every row untouched.
    """
    _alembic_ok("upgrade", _PRE_PROVIDER_SCOPE_REVISION, db_url=temp_db_url)
    for statement in _SEED_SQL:
        await _exec(temp_db_url, statement)
    before = await _counts(temp_db_url)

    async def assert_untouched(case: str) -> None:
        """Nothing may have changed: revision, old constraints, or rows."""
        current = _alembic_ok("current", db_url=temp_db_url)
        assert _HEAD_REVISION not in current, f"{case}: revision advanced despite the failure"
        assert _PRE_PROVIDER_SCOPE_REVISION in current, f"{case}: unexpected revision"
        # The pre-PR-3 constraints survive the rolled-back transaction.
        assert "uq_clients_company_altegio_id" in await _constraint_names(temp_db_url, "clients")
        assert "uq_records_company_altegio_id" in await _constraint_names(temp_db_url, "records")
        # And the provider column was never committed.
        assert await _column(temp_db_url, "clients", "provider") is None, f"{case}: provider column leaked"
        assert await _counts(temp_db_url) == before, f"{case}: rows changed"

    # ── A. a constraint with the right NAME but the wrong columns ────────────
    await _exec(
        temp_db_url,
        "ALTER TABLE clients ADD CONSTRAINT uq_clients_provider_company_altegio_id UNIQUE (company_id)",
    )
    failed = _run_alembic("upgrade", "head", db_url=temp_db_url)
    assert failed.returncode != 0, "upgrade accepted a same-named constraint over the wrong columns"
    await assert_untouched("wrong-column constraint")
    await _exec(temp_db_url, "ALTER TABLE clients DROP CONSTRAINT uq_clients_provider_company_altegio_id")

    # ── B. an index with the right NAME but the wrong definition ─────────────
    await _exec(
        temp_db_url,
        "CREATE UNIQUE INDEX uq_records_easyweek_booking_uuid ON records (altegio_record_id)",
    )
    failed = _run_alembic("upgrade", "head", db_url=temp_db_url)
    assert failed.returncode != 0, "upgrade accepted a same-named index with the wrong definition"
    await assert_untouched("wrong-predicate index")
    await _exec(temp_db_url, "DROP INDEX uq_records_easyweek_booking_uuid")

    # ── C. with the drift removed, the same upgrade succeeds ─────────────────
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    assert _HEAD_REVISION in _alembic_ok("current", db_url=temp_db_url)
    assert await _counts(temp_db_url) == before
    for table in _PROVIDER_TABLES:
        assert await _providers(temp_db_url, table) == ["altegio"], table
    assert "uq_clients_provider_company_altegio_id" in await _constraint_names(temp_db_url, "clients")
    assert "uq_clients_company_altegio_id" not in await _constraint_names(temp_db_url, "clients")

    # ── D. and the normal round trip still works afterwards ──────────────────
    _alembic_ok("downgrade", _PRE_PROVIDER_SCOPE_REVISION, db_url=temp_db_url)
    _alembic_ok("upgrade", "head", db_url=temp_db_url)
    assert await _counts(temp_db_url) == before
