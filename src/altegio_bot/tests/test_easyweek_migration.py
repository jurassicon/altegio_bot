"""Structural guards for the EasyWeek migration compatibility fix.

These are deliberately DB-free (the shared test DB is owned by the conftest
schema fixture and must never be driven through Alembic here). The real
two-scenario upgrade — fresh DB and a DB that only applied the early base
revision without the raw-body columns — is run against a throwaway Postgres in
the migration checklist, not in this process.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

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
