"""Structural guards for the EasyWeek migration compatibility fix.

These are deliberately DB-free (the shared test DB is owned by the conftest
schema fixture and must never be driven through Alembic here). The real
two-scenario upgrade — fresh DB and a DB that only applied the early base
revision without the raw-body columns — is run against a throwaway Postgres in
the migration checklist, not in this process.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from altegio_bot.models.models import Base

_ROOT = Path(__file__).resolve().parents[3]
_VERSIONS = _ROOT / "alembic" / "versions"
_BASE = "8923be993170"
_COMPAT = "8705ec49cc73"


def _script() -> ScriptDirectory:
    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "alembic"))
    return ScriptDirectory.from_config(cfg)


def test_single_head() -> None:
    assert len(_script().get_heads()) == 1


def test_compat_revision_follows_base_revision() -> None:
    script = _script()
    compat = script.get_revision(_COMPAT)
    assert compat.down_revision == _BASE


def test_base_revision_no_longer_creates_raw_body_columns() -> None:
    """The raw-body columns must NOT live in the already-applied base revision.

    Editing an applied revision in place is exactly the bug being fixed: an
    environment that ran the early version would never see the columns.
    """
    src = next(_VERSIONS.glob(f"{_BASE}_*.py")).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS easyweek_events" in src
    # Scope to the upgrade() body only — the module docstring legitimately
    # explains that the columns were moved to the compatibility revision.
    upgrade_body = src.split("def upgrade")[1].split("def downgrade")[0]
    assert "body_raw" not in upgrade_body
    assert "body_size_bytes" not in upgrade_body


def test_compat_revision_adds_columns_idempotently() -> None:
    src = next(_VERSIONS.glob(f"{_COMPAT}_*.py")).read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS body_raw BYTEA" in src
    assert "ADD COLUMN IF NOT EXISTS body_size_bytes BIGINT NOT NULL DEFAULT 0" in src
    # Downgrade only rolls back its own columns, nothing else.
    down = src.split("def downgrade")[1]
    assert "DROP COLUMN IF EXISTS body_size_bytes" in down
    assert "DROP COLUMN IF EXISTS body_raw" in down
    assert "DROP TABLE" not in down


# ===========================================================================
# PR-3 — provider scope + EasyWeek identity fields
# ===========================================================================

_PROVIDER_SCOPE = "c1a7d3f905b2"
_OPERATOR_RELAY = "9a1f4c7b2e3d"

# The canonical five (INTEGRATION_PLAN §3.1). Anything else getting a provider
# column would be scope creep with a real migration cost.
_PROVIDER_TABLES = (
    "clients",
    "records",
    "message_templates",
    "message_jobs",
    "whatsapp_senders",
)

# (table, pre-PR-3 constraint, provider-scoped replacement)
_UNIQUE_SWAPS = (
    ("clients", "uq_clients_company_altegio_id", "uq_clients_provider_company_altegio_id"),
    ("records", "uq_records_company_altegio_id", "uq_records_provider_company_altegio_id"),
    ("whatsapp_senders", "uq_whatsapp_senders_company_code", "uq_whatsapp_senders_provider_company_code"),
)


def _provider_scope_path() -> Path:
    return next(_VERSIONS.glob(f"{_PROVIDER_SCOPE}_*.py"))


def _provider_scope_source() -> str:
    return _provider_scope_path().read_text(encoding="utf-8")


def _provider_scope_module():
    """Import the revision module so its tables/constraints can be asserted.

    The migration drives every table and constraint name from shared tuples, so
    the names do not appear literally inside ``upgrade()``. Reading the real
    constants is both stronger and less brittle than a text search. Importing is
    side-effect free: nothing calls ``op`` at module level.
    """
    import importlib.util

    path = _provider_scope_path()
    spec = importlib.util.spec_from_file_location(f"_migration_{_PROVIDER_SCOPE}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _upgrade_body() -> str:
    return _provider_scope_source().split("def upgrade")[1].split("\ndef ")[0]


def _downgrade_body() -> str:
    return _provider_scope_source().split("def downgrade")[1]


def test_provider_scope_revision_is_the_single_head() -> None:
    assert _script().get_heads() == [_PROVIDER_SCOPE]


def test_provider_scope_follows_the_operator_relay_revision() -> None:
    """A new revision must extend the real head, never edit an applied one."""
    assert _script().get_revision(_PROVIDER_SCOPE).down_revision == _OPERATOR_RELAY


def test_provider_column_is_added_to_exactly_the_canonical_five() -> None:
    module = _provider_scope_module()
    assert tuple(module._PROVIDER_TABLES) == _PROVIDER_TABLES
    body = _upgrade_body()
    assert "op.add_column(" in body
    assert 'sa.Column("provider", _PROVIDER_TYPE' in body


def test_provider_column_is_backfilled_before_being_made_not_null() -> None:
    """Order matters: SET NOT NULL on unbackfilled rows would abort on prod."""
    body = _upgrade_body()
    backfill = body.index("UPDATE {table} SET provider = 'altegio' WHERE provider IS NULL")
    not_null = body.index("nullable=False")
    assert backfill < not_null


def test_provider_column_keeps_its_database_default() -> None:
    """Existing Altegio INSERTs never name `provider`; the DB must fill it."""
    source = _provider_scope_source()
    body = _upgrade_body()
    assert "_PROVIDER_DEFAULT = sa.text(\"'altegio'\")" in source
    assert "server_default=_PROVIDER_DEFAULT" in body
    # SET NOT NULL must not drop the default on the way.
    assert "existing_server_default=_PROVIDER_DEFAULT" in body
    assert "DROP DEFAULT" not in source


def test_provider_column_is_not_a_restrictive_enum_or_check() -> None:
    """A third CRM must be a code change, not another migration."""
    source = _provider_scope_source()
    assert "CREATE TYPE" not in source
    assert "CHECK (provider" not in source
    assert "sa.Enum" not in source


def test_unique_constraints_become_provider_scoped() -> None:
    """Every old constraint is dropped and replaced by its scoped twin."""
    swaps = {(table, old, new) for table, old, new, _columns in _provider_scope_module()._UNIQUE_SWAPS}
    assert swaps == set(_UNIQUE_SWAPS)

    body = _upgrade_body()
    assert "op.create_unique_constraint(new_name, table, list(columns))" in body
    assert 'op.drop_constraint(old_name, table, type_="unique")' in body


def test_provider_scoped_constraint_columns_lead_with_provider() -> None:
    """`provider` must come first so a lookup can be prefix-scoped."""
    expected = {
        "clients": ("provider", "company_id", "altegio_client_id"),
        "records": ("provider", "company_id", "altegio_record_id"),
        "whatsapp_senders": ("provider", "company_id", "sender_code"),
    }
    for table, _old, _new, columns in _provider_scope_module()._UNIQUE_SWAPS:
        assert tuple(columns) == expected[table]


def test_message_jobs_dedupe_key_uniqueness_is_untouched() -> None:
    """Out of scope for PR-3: the global dedupe_key unique stays as it is."""
    assert "dedupe_key" not in _provider_scope_source()


def test_message_templates_gets_no_invented_unique_constraint() -> None:
    """message_templates has no unique constraint today; do not add one."""
    body = _upgrade_body()
    assert "message_templates ADD CONSTRAINT" not in body
    assert "uq_message_templates" not in body


def test_easyweek_partial_unique_index_predicate() -> None:
    body = _upgrade_body()
    source = _provider_scope_source()
    assert '"uq_records_easyweek_booking_uuid"' in body
    assert "unique=True" in body
    assert "postgresql_where=_EASYWEEK_UUID_PREDICATE" in body
    assert "provider = 'easyweek' AND easyweek_booking_uuid IS NOT NULL" in source


def test_easyweek_identity_columns_are_added_with_safe_types() -> None:
    body = _upgrade_body()
    assert 'sa.Column("easyweek_booking_uuid", postgresql.UUID(as_uuid=True)' in body
    # A bounded string, never an integer: the hash may carry leading zeros.
    assert 'sa.Column("easyweek_booking_hash_id", sa.String(length=64)' in body
    assert "easyweek_booking_hash_id BIGINT" not in body
    assert "sa.BigInteger" not in body


def test_meta_template_name_column_is_added() -> None:
    assert 'sa.Column("meta_template_name", sa.String(length=128)' in _upgrade_body()


def test_downgrade_is_symmetric_and_non_destructive() -> None:
    down = _downgrade_body()
    assert 'op.drop_column(table, "provider")' in down
    assert 'op.drop_column("records", "easyweek_booking_uuid")' in down
    assert 'op.drop_column("records", "easyweek_booking_hash_id")' in down
    assert 'op.drop_column("message_templates", "meta_template_name")' in down
    assert 'op.drop_index("uq_records_easyweek_booking_uuid", table_name="records")' in down
    # Restores what it replaced ...
    assert '"ix_clients_company_phone", "clients", ["company_id", "phone_e164"]' in down
    assert '"ix_message_jobs_company_type_status"' in down
    assert "op.create_unique_constraint(old_name, table, legacy_columns)" in down
    # ... and never touches rows or tables.
    assert "DROP TABLE" not in down
    assert "DELETE FROM" not in down
    assert "TRUNCATE" not in down


def test_downgrade_fails_closed_on_cross_provider_duplicates() -> None:
    """Restoring the narrower unique must abort, never merge or delete rows."""
    source = _provider_scope_source()
    down = _downgrade_body()
    assert "_fail_closed_if_cross_provider_duplicates()" in down
    assert "raise RuntimeError" in source
    # The guard runs before anything is dropped, so a refusal changes nothing.
    guard = down.index("_fail_closed_if_cross_provider_duplicates()")
    first_drop = down.index("op.drop_index(")
    assert guard < first_drop


# ---------------------------------------------------------------------------
# The ORM metadata must describe the same schema the migration builds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", _PROVIDER_TABLES)
def test_model_metadata_declares_the_provider_column(table: str) -> None:
    column = Base.metadata.tables[table].c["provider"]
    assert column.nullable is False
    assert column.type.length == 32
    assert "altegio" in str(column.server_default.arg)
    assert column.default.arg == "altegio"


@pytest.mark.parametrize("table", _PROVIDER_TABLES)
def test_no_provider_column_leaks_outside_the_canonical_five(table: str) -> None:
    """Guard the boundary from the other side: nothing else may gain one."""
    del table  # the parametrization only pins the expected set below
    with_provider = {name for name, t in Base.metadata.tables.items() if "provider" in t.c}
    assert with_provider == set(_PROVIDER_TABLES)


@pytest.mark.parametrize(("table", "old_name", "new_name"), _UNIQUE_SWAPS)
def test_model_metadata_uses_the_provider_scoped_constraint(table: str, old_name: str, new_name: str) -> None:
    constraints = {c.name for c in Base.metadata.tables[table].constraints}
    assert new_name in constraints
    assert old_name not in constraints


def test_model_metadata_declares_the_easyweek_partial_unique_index() -> None:
    index = next(i for i in Base.metadata.tables["records"].indexes if i.name == "uq_records_easyweek_booking_uuid")
    assert index.unique is True
    assert [c.name for c in index.columns] == ["easyweek_booking_uuid"]
    predicate = str(index.dialect_options["postgresql"]["where"])
    assert "provider = 'easyweek'" in predicate
    assert "easyweek_booking_uuid IS NOT NULL" in predicate


def test_model_metadata_scoped_indexes_lead_with_provider() -> None:
    expected = {
        "clients": ("ix_clients_provider_company_phone", ["provider", "company_id", "phone_e164"]),
        "message_jobs": (
            "ix_message_jobs_provider_company_type_status",
            ["provider", "company_id", "job_type", "status"],
        ),
        "message_templates": (
            "ix_message_templates_provider_company_code_lang",
            ["provider", "company_id", "code", "language"],
        ),
    }
    for table, (name, columns) in expected.items():
        index = next(i for i in Base.metadata.tables[table].indexes if i.name == name)
        assert [c.name for c in index.columns] == columns

    # The replaced ones are gone from metadata.
    assert "ix_clients_company_phone" not in {i.name for i in Base.metadata.tables["clients"].indexes}
    assert "ix_message_jobs_company_type_status" not in {i.name for i in Base.metadata.tables["message_jobs"].indexes}


def test_single_column_company_indexes_are_preserved() -> None:
    """The audit was narrow on purpose: no mass index cleanup."""
    for table in _PROVIDER_TABLES:
        assert Base.metadata.tables[table].c["company_id"].index is True


def test_upsert_constraint_names_match_the_model_metadata() -> None:
    """The Altegio upsert pins constraint names; a rename must not orphan them."""
    source = (_ROOT / "src" / "altegio_bot" / "workers" / "inbox_worker.py").read_text(encoding="utf-8")
    for table, old_name, new_name in _UNIQUE_SWAPS[:2]:
        del table
        assert f'constraint="{new_name}"' in source
        assert f'constraint="{old_name}"' not in source


# ---------------------------------------------------------------------------
# Fail-closed DDL: no name-only existence checks on PR-3 objects
# ---------------------------------------------------------------------------


def test_name_only_existence_helper_is_gone() -> None:
    """`_add_constraint_if_absent` compared names only.

    It accepted a same-named constraint with different columns, order or type
    and called drifted production schema "already correct". Removed on purpose.
    """
    source = _provider_scope_source()
    assert "_add_constraint_if_absent" not in source


def test_pr3_ddl_uses_plain_alembic_operations() -> None:
    """Unexpected objects must abort the migration, not be tolerated."""
    source = _provider_scope_source()
    for guard in (
        "ADD COLUMN IF NOT EXISTS",
        "CREATE INDEX IF NOT EXISTS",
        "CREATE UNIQUE INDEX IF NOT EXISTS",
        "DROP CONSTRAINT IF EXISTS",
        "DROP COLUMN IF EXISTS",
        "DROP INDEX IF EXISTS",
        "DO $$",
    ):
        assert guard not in source, f"PR-3 DDL still uses the tolerant {guard!r}"

    body = _upgrade_body()
    for operation in (
        "op.add_column(",
        "op.alter_column(",
        "op.create_unique_constraint(",
        "op.drop_constraint(",
        "op.create_index(",
        "op.drop_index(",
    ):
        assert operation in body, f"upgrade does not use {operation}"


def test_downgrade_also_uses_plain_alembic_operations() -> None:
    down = _downgrade_body()
    for operation in ("op.drop_index(", "op.create_index(", "op.create_unique_constraint(", "op.drop_column("):
        assert operation in down


def test_offline_sql_does_not_pretend_the_duplicate_check_ran() -> None:
    """`--sql` cannot read the database; the script must say so loudly."""
    source = _provider_scope_source()
    assert "as_sql" in source
    assert "WARNING: NOT VERIFIED" in source
