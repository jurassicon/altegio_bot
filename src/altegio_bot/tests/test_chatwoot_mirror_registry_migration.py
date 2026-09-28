"""Migration contract for the durable Chatwoot outbound mirror registry.

Runs upgrade → downgrade → upgrade on a THROWAWAY database, never production.
Skips when no disposable PostgreSQL is reachable, and FAILS instead of skipping
when the environment declares the check mandatory (``ALTEGIO_REQUIRE_MIGTEST=1``),
so a green build can never hide a migration that was silently never exercised.

What it proves beyond "the DDL runs"
------------------------------------
The whole value of this table is that a wamid resolves to exactly one Chatwoot
message id. That is a database constraint, and a constraint that lives only in
the ORM is a constraint production does not have. So this module applies the
migration and then tries, in SQL, to write the rows the trust model says are
impossible: a second link for the same wamid, a non-positive Chatwoot id, an
empty wamid and an unknown route.

It also proves the downgrade removes exactly this one table and leaves the
neighbouring Chatwoot/Outbox tables untouched, because dropping it must simply
return the reaction path to the bounded legacy scan.
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

from altegio_bot.chatwoot_client import OUTBOUND_MIRROR_MESSAGE_KIND
from altegio_bot.models.models import Base

_REPO_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = _REPO_ROOT / "alembic.ini"

MIRROR_REVISION = "c4e9a1b78d52"
PARENT_REVISION = "b3f7c2a90d14"
SCOPE_REVISION = "d7b2f6a4c318"

_SCOPE_A = "https://chatwoot.a.test|1|generation-a"
_SCOPE_B = "https://chatwoot.b.test|1|generation-a"

OLD_UNIQUE = "uq_chatwoot_outbound_mirror_provider_message"
NEW_UNIQUE = "uq_chatwoot_outbound_mirror_scope_provider_message"

MIRRORS = "chatwoot_outbound_mirrors"

# Tables this revision must not touch, in either direction.
UNTOUCHED_TABLES = ("outbox_messages", "whatsapp_events", "whatsapp_senders")

_TEMP_DB_PREFIX = "altegio_mirrorreg_migtest_"
_REMEDY = "Grant CREATEDB to the DATABASE_URL role, or point ALTEGIO_MIGTEST_DATABASE_URL at a disposable PostgreSQL."

_WAMID = "wamid.MIGRATION_TEST"


def _unavailable(reason: str) -> None:
    if os.environ.get("ALTEGIO_REQUIRE_MIGTEST") == "1":
        pytest.fail(f"mirror registry migration test is required (ALTEGIO_REQUIRE_MIGTEST=1) but {reason}. {_REMEDY}")
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


async def _columns(db_url: str, table: str) -> set[str]:
    rows = await _fetch(
        db_url,
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = :table",
        {"table": table},
    )
    return {row[0] for row in rows}


async def _unique_constraints(db_url: str, table: str) -> set[str]:
    rows = await _fetch(
        db_url,
        "SELECT constraint_name FROM information_schema.table_constraints "
        "WHERE table_schema = 'public' AND table_name = :table AND constraint_type = 'UNIQUE'",
        {"table": table},
    )
    return {row[0] for row in rows}


async def _tables(db_url: str) -> set[str]:
    rows = await _fetch(
        db_url,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    )
    return {row[0] for row in rows}


def _insert_sql(**overrides: object) -> tuple[str, dict]:
    params: dict = {
        "chatwoot_scope_id": _SCOPE_A,
        "provider_message_id": _WAMID,
        "chatwoot_message_id": 4242,
        "chatwoot_conversation_id": 77,
        "marker_version": OUTBOUND_MIRROR_MESSAGE_KIND,
        "chatwoot_route": "tenant",
        "chatwoot_inbox_id": 101,
        "tenant_provider": "easyweek",
        "company_id": 900001,
    }
    params.update(overrides)
    columns = ", ".join(params)
    values = ", ".join(f":{name}" for name in params)
    return f"INSERT INTO {MIRRORS} ({columns}) VALUES ({values})", params


# ===========================================================================
# Graph shape
# ===========================================================================


def test_exactly_one_alembic_head() -> None:
    """One head — two would mean two lineages and a deploy that cannot upgrade.

    The single-head property is the invariant worth pinning. WHICH revision is
    the head is not: every later phase adds a child, so an assertion that this
    revision is still the tip would go red on the next PR and say nothing about
    this one. What matters here is that these two revisions are still in the
    chain and still in the right order, which the tests below check directly.
    """
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    heads = script.get_heads()

    assert len(heads) == 1, f"expected exactly one Alembic head, got {heads}"
    revisions = {revision.revision for revision in script.walk_revisions()}
    assert MIRROR_REVISION in revisions
    assert SCOPE_REVISION in revisions


def test_the_registry_revision_is_a_direct_child_of_the_previous_head() -> None:
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert script.get_revision(MIRROR_REVISION).down_revision == PARENT_REVISION


def test_the_scope_revision_is_a_separate_child_of_the_registry_revision() -> None:
    """A new child, not an edit: c4e9a1b78d52 may already be applied somewhere."""
    script = ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))
    assert script.get_revision(SCOPE_REVISION).down_revision == MIRROR_REVISION


def test_the_orm_and_the_migration_describe_the_same_table() -> None:
    """The table the application expects is the one this revision creates."""
    assert MIRRORS in {table.name for table in Base.metadata.sorted_tables}


def test_the_orm_columns_match_the_migration_columns() -> None:
    """Model/migration drift here would break production, not the test suite.

    The test database is built with ``create_all`` from the ORM, so a column that
    exists only in the model would pass every other test and be missing in
    production.
    """
    versions = _REPO_ROOT / "alembic" / "versions"
    migration = (versions / f"{MIRROR_REVISION}_add_chatwoot_outbound_mirror_registry.py").read_text(
        encoding="utf-8"
    ) + (versions / f"{SCOPE_REVISION}_scope_chatwoot_outbound_mirrors.py").read_text(encoding="utf-8")
    for column in Base.metadata.tables[MIRRORS].columns:
        assert f'"{column.name}"' in migration, f"{column.name} is in the ORM but not in the migration"


def test_the_orm_unique_key_is_the_scoped_one() -> None:
    """The ORM must not still be describing the pre-scope global unique key."""
    constraints = {constraint.name for constraint in Base.metadata.tables[MIRRORS].constraints}
    assert NEW_UNIQUE in constraints
    assert OLD_UNIQUE not in constraints


# ===========================================================================
# Upgrade, downgrade, upgrade
# ===========================================================================


@pytest.mark.asyncio
async def test_upgrade_creates_the_table_and_downgrade_removes_only_it(temp_db_url: str) -> None:
    _alembic_ok("upgrade", PARENT_REVISION, db_url=temp_db_url)
    before = await _tables(temp_db_url)
    assert MIRRORS not in before

    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    after_upgrade = await _tables(temp_db_url)
    assert MIRRORS in after_upgrade
    for table in UNTOUCHED_TABLES:
        assert table in after_upgrade

    _alembic_ok("downgrade", PARENT_REVISION, db_url=temp_db_url)
    after_downgrade = await _tables(temp_db_url)
    assert MIRRORS not in after_downgrade
    # Exactly one table went away, and nothing else changed.
    assert after_downgrade == before

    # Re-upgrade must work: a downgrade is not a one-way door.
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    assert MIRRORS in await _tables(temp_db_url)


@pytest.mark.asyncio
async def test_the_scope_revision_round_trips_on_its_own(temp_db_url: str) -> None:
    """upgrade → downgrade → upgrade across the scope step alone."""
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    assert await _columns(temp_db_url, MIRRORS) >= {"chatwoot_scope_id"}
    assert await _unique_constraints(temp_db_url, MIRRORS) >= {NEW_UNIQUE}

    _alembic_ok("downgrade", MIRROR_REVISION, db_url=temp_db_url)
    assert "chatwoot_scope_id" not in await _columns(temp_db_url, MIRRORS)
    uniques = await _unique_constraints(temp_db_url, MIRRORS)
    assert OLD_UNIQUE in uniques
    assert NEW_UNIQUE not in uniques

    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    assert "chatwoot_scope_id" in await _columns(temp_db_url, MIRRORS)
    assert NEW_UNIQUE in await _unique_constraints(temp_db_url, MIRRORS)


@pytest.mark.asyncio
async def test_downgrade_deduplicates_one_wamid_shared_by_two_scopes(temp_db_url: str) -> None:
    """The documented deterministic dedupe: the lowest id survives, no failure.

    After the scope revision two installations may legitimately hold the same
    wamid, which the old global unique key cannot express. A rollback must not trip
    over live data — and losing a derived mapping row only costs a safe fallback.
    """
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    first_sql, first = _insert_sql(chatwoot_scope_id=_SCOPE_A, chatwoot_message_id=4242)
    await _execute(temp_db_url, first_sql, first)
    second_sql, second = _insert_sql(chatwoot_scope_id=_SCOPE_B, chatwoot_message_id=9999)
    await _execute(temp_db_url, second_sql, second)
    assert len(await _fetch(temp_db_url, f"SELECT id FROM {MIRRORS}")) == 2

    _alembic_ok("downgrade", MIRROR_REVISION, db_url=temp_db_url)

    rows = await _fetch(temp_db_url, f"SELECT chatwoot_message_id FROM {MIRRORS}")
    assert rows == [(4242,)]  # the lowest id, deterministically
    assert OLD_UNIQUE in await _unique_constraints(temp_db_url, MIRRORS)


# ===========================================================================
# The scope namespace
# ===========================================================================


@pytest.mark.asyncio
async def test_one_wamid_may_exist_once_per_scope(temp_db_url: str) -> None:
    """Same wamid, same numeric conversation id, two installations — both stored."""
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    for scope, message_id in ((_SCOPE_A, 4242), (_SCOPE_B, 9999)):
        sql, params = _insert_sql(chatwoot_scope_id=scope, chatwoot_message_id=message_id)
        await _execute(temp_db_url, sql, params)

    rows = await _fetch(
        temp_db_url,
        f"SELECT chatwoot_scope_id, chatwoot_message_id, chatwoot_conversation_id FROM {MIRRORS} ORDER BY id",
    )
    assert rows == [(_SCOPE_A, 4242, 77), (_SCOPE_B, 9999, 77)]


@pytest.mark.asyncio
async def test_one_wamid_inside_one_scope_is_still_unique(temp_db_url: str) -> None:
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    sql, params = _insert_sql()
    await _execute(temp_db_url, sql, params)

    conflicting_sql, conflicting = _insert_sql(chatwoot_message_id=9999)
    with pytest.raises(Exception, match=NEW_UNIQUE):
        await _execute(temp_db_url, conflicting_sql, conflicting)

    assert await _fetch(temp_db_url, f"SELECT chatwoot_message_id FROM {MIRRORS}") == [(4242,)]


@pytest.mark.asyncio
async def test_on_conflict_do_nothing_is_scoped(temp_db_url: str) -> None:
    """The application's idempotent write names the scoped constraint."""
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    sql, params = _insert_sql()
    await _execute(temp_db_url, sql, params)

    replay_sql, replay = _insert_sql(chatwoot_message_id=9999)
    await _execute(
        temp_db_url,
        replay_sql + f" ON CONFLICT ON CONSTRAINT {NEW_UNIQUE} DO NOTHING",
        replay,
    )

    assert await _fetch(temp_db_url, f"SELECT chatwoot_message_id FROM {MIRRORS}") == [(4242,)]


@pytest.mark.asyncio
async def test_a_legacy_unscoped_row_can_still_be_stored_but_is_not_deduplicated(temp_db_url: str) -> None:
    """NULL scope models rows written before this revision: kept, never trusted.

    PostgreSQL treats NULLs as distinct in a unique key, so these rows are neither
    constrained nor merged — which is right for rows the application refuses to
    read (the lookup requires an exact scope match).
    """
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    for message_id in (4242, 9999):
        sql, params = _insert_sql(chatwoot_scope_id=None, chatwoot_message_id=message_id)
        await _execute(temp_db_url, sql, params)

    rows = await _fetch(temp_db_url, f"SELECT chatwoot_scope_id, chatwoot_message_id FROM {MIRRORS} ORDER BY id")
    assert rows == [(None, 4242), (None, 9999)]


# ===========================================================================
# The constraints the trust model depends on
# ===========================================================================


@pytest.mark.asyncio
async def test_one_wamid_can_hold_only_one_link_per_scope(temp_db_url: str) -> None:
    """The uniqueness that lets the reaction path trust a single row.

    At head the key is ``(chatwoot_scope_id, provider_message_id)``, so the clash
    is reported against the scoped constraint.
    """
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    sql, params = _insert_sql()
    await _execute(temp_db_url, sql, params)

    conflicting_sql, conflicting = _insert_sql(chatwoot_message_id=9999, chatwoot_conversation_id=88)
    with pytest.raises(Exception, match=NEW_UNIQUE):
        await _execute(temp_db_url, conflicting_sql, conflicting)

    rows = await _fetch(
        temp_db_url,
        f"SELECT chatwoot_message_id, chatwoot_conversation_id FROM {MIRRORS}",
    )
    assert rows == [(4242, 77)]


@pytest.mark.asyncio
async def test_the_pre_scope_global_unique_key_is_really_gone(temp_db_url: str) -> None:
    """The migration must DROP the old key, not just add the new one beside it.

    A leftover global key on ``provider_message_id`` would make the whole scope
    namespace pointless: the second installation could never store its own row.
    """
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)

    assert OLD_UNIQUE not in await _unique_constraints(temp_db_url, MIRRORS)
    # And a write naming it is rejected by PostgreSQL rather than silently
    # resolving against some other constraint.
    sql, params = _insert_sql()
    with pytest.raises(Exception, match=OLD_UNIQUE):
        await _execute(
            temp_db_url,
            sql + f" ON CONFLICT ON CONSTRAINT {OLD_UNIQUE} DO NOTHING",
            params,
        )


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"chatwoot_message_id": 0}, "ck_chatwoot_outbound_mirror_message_id"),
        ({"chatwoot_message_id": -1}, "ck_chatwoot_outbound_mirror_message_id"),
        ({"chatwoot_conversation_id": 0}, "ck_chatwoot_outbound_mirror_conversation_id"),
        ({"chatwoot_conversation_id": -5}, "ck_chatwoot_outbound_mirror_conversation_id"),
        ({"provider_message_id": ""}, "ck_chatwoot_outbound_mirror_wamid_present"),
        ({"chatwoot_route": "somewhere_else"}, "ck_chatwoot_outbound_mirror_route"),
    ],
    ids=[
        "zero_message_id",
        "negative_message_id",
        "zero_conversation",
        "negative_conversation",
        "empty_wamid",
        "bad_route",
    ],
)
@pytest.mark.asyncio
async def test_an_unusable_link_cannot_be_stored(
    temp_db_url: str,
    overrides: dict,
    constraint: str,
) -> None:
    """A row that could not serve as proof is one PostgreSQL refuses to keep."""
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    sql, params = _insert_sql(**overrides)

    with pytest.raises(Exception, match=constraint):
        await _execute(temp_db_url, sql, params)

    assert await _fetch(temp_db_url, f"SELECT 1 FROM {MIRRORS}") == []


@pytest.mark.asyncio
async def test_the_general_route_may_omit_its_tenant_provenance(temp_db_url: str) -> None:
    """Provenance is descriptive, so the legacy single-inbox client can omit it."""
    _alembic_ok("upgrade", SCOPE_REVISION, db_url=temp_db_url)
    sql, params = _insert_sql(
        chatwoot_route="general",
        chatwoot_inbox_id=None,
        tenant_provider=None,
        company_id=None,
    )

    await _execute(temp_db_url, sql, params)

    rows = await _fetch(temp_db_url, f"SELECT chatwoot_route, tenant_provider FROM {MIRRORS}")
    assert rows == [("general", None)]
