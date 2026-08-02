"""The deploy must agree with the database about which revision it is on.

Production was read directly with ``SELECT version_num FROM alembic_version``
and is on ``9a1f4c7b2e3d`` — exactly the pre-PR-3 revision. An earlier deploy
attempt nevertheless classified the database as being on ``57cd7c3a7a27``, a
revision that does not exist anywhere in this repository. The two readers
disagreed, and the deploy trusted the wrong one.

Two independent defects made that possible, and both are pinned here.

1. The revision was recovered by grepping the first 12-hex token out of the
   human-readable ``alembic current`` output. That output also carries prose,
   warnings and failure text — ``FAILED: Can't locate revision identified by
   '<id>'`` — and any hex token in it is indistinguishable from a revision. It
   also silently collapses a multi-head database to whichever line came first.
   The replacement is a structured reader that never parses prose.

2. Which database each side talks to is decided by two unrelated ``.env``
   values — ``POSTGRES_DB`` for the postgres container, ``DATABASE_URL`` for
   Alembic — with nothing in Compose tying them together. They can point at
   different physical databases, in which case a revision read from one says
   nothing about the schema the other is about to migrate. The deploy now
   proves both sides are on one database before it changes anything.

What is NOT claimed: this module does not assert how ``57cd7c3a7a27`` was
produced on the production host. That was not reproduced. It asserts only that
each defect class above is now impossible.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEPLOY_SCRIPT_FILE = _REPO_ROOT / "scripts" / "deploy_pr3.sh"

PRE_PR3_REVISION = "9a1f4c7b2e3d"
PR3_REVISION = "c1a7d3f905b2"
PRODUCTION_REVISION = PRE_PR3_REVISION
PHANTOM_REVISION = "57cd7c3a7a27"


def _deploy_script() -> str:
    return DEPLOY_SCRIPT_FILE.read_text()


def _index(marker: str) -> int:
    position = _deploy_script().find(marker)
    assert position != -1, f"deploy script is missing the required marker {marker!r}"
    return position


def _main_flow() -> str:
    script = _deploy_script()
    return script[script.index("trap 'recover $?' EXIT") :]


def _main_index(marker: str) -> int:
    main = _main_flow()
    position = main.find(marker)
    assert position != -1, f"main deploy flow is missing the marker {marker!r}"
    return position


def _uncommented(block: str) -> str:
    """Drop ``#`` comment lines so prose cannot satisfy a code assertion."""
    return "\n".join(line for line in block.splitlines() if not line.lstrip().startswith("#"))


def _extract_shell_function(name: str) -> str:
    script = _deploy_script()
    start = _index(f"{name}() {{")
    body = script[start:]
    end = body.index("\n}\n") + len("\n}\n")
    return body[:end]


# ===========================================================================
# is_revision_id(): executed against the real shell
# ===========================================================================


def _run_is_revision_id(value: str) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        program = Path(tmp) / "check.sh"
        program.write_text(_extract_shell_function("is_revision_id") + f'\nis_revision_id "{value}"\n')
        return subprocess.run(["sh", str(program)], capture_output=True, text=True).returncode


@pytest.mark.parametrize("value", [PRE_PR3_REVISION, PR3_REVISION, PHANTOM_REVISION, "000000000000"])
def test_well_formed_revision_ids_are_accepted(value: str) -> None:
    """Shape only — `57cd…` is well-formed, it is the lineage check that rejects it."""
    assert _run_is_revision_id(value) == 0


@pytest.mark.parametrize(
    ("value", "why"),
    [
        ("", "empty"),
        ("9a1f4c7b2e3", "eleven characters"),
        ("9a1f4c7b2e3dd", "thirteen characters"),
        ("9A1F4C7B2E3D", "uppercase"),
        ("9a1f4c7b2e3d unexpected text", "revision followed by prose"),
        ("9a1f4c7b2e3d c1a7d3f905b2", "two revision ids"),
        ("FAILED: 57cd7c3a7a27", "id embedded in failure text"),
        ("(head)", "prose only"),
        ("9a1f-c7b2e3d", "non-hex character"),
    ],
)
def test_malformed_values_are_rejected_whole(value: str, why: str) -> None:
    """A prefix of a longer string must never be accepted as the value."""
    assert _run_is_revision_id(value) != 0, f"accepted {why}: {value!r}"


# ===========================================================================
# The reader is structured, not a parse of `alembic current`
# ===========================================================================


def test_the_deploy_no_longer_parses_alembic_current() -> None:
    script = _uncommented(_deploy_script())
    assert "alembic current" not in script, "the human-readable output must not be a revision source"


def test_no_hex_token_is_ever_scraped_out_of_output() -> None:
    """The exact shape of the original defect."""
    script = _uncommented(_deploy_script())
    assert "[0-9a-f]{12}" not in script, "revisions must not be recovered by grepping for hex tokens"


def test_the_reader_uses_the_migration_runners_own_connection() -> None:
    """Same container, same engine, same configuration as the migration."""
    body = _extract_shell_function("alembic_revision_facts")
    assert "--profile ops run --rm --no-deps -T migrate" in body
    assert "from altegio_bot.db import engine" in body
    assert "MigrationContext" in body
    assert "get_current_heads" in body


def test_the_reader_emits_a_machine_readable_protocol() -> None:
    body = _extract_shell_function("alembic_revision_facts")
    for key in ("REVISION_STATUS", "REVISION", "DB_HEAD_COUNT", "DB_NAME", "DB_SYSTEM_ID", "DB_OID"):
        assert f'"{key}"' in body or f"{key}=" in body, f"the reader must report {key}"


def _revision_status_case() -> str:
    """The `case "$REVISION_STATUS" in … esac` block from the main flow."""
    main = _main_flow()
    start = main.index('case "$REVISION_STATUS" in')
    return main[start : main.index("esac", start)]


@pytest.mark.parametrize("status", ["none", "multiple", "unknown"])
def test_every_non_ok_reader_status_fails_closed(status: str) -> None:
    block = _revision_status_case()
    assert f"{status})" in block, f"status {status!r} is not handled"


def test_only_the_ok_status_continues_the_deploy() -> None:
    block = _revision_status_case()
    # ok) falls through with no exit; every other arm, plus the catch-all, exits.
    assert "  ok)\n    ;;" in block, "the ok arm must be the only one that continues"
    assert block.count("exit 1") == 4, "none, multiple, unknown and the catch-all must each exit"


def test_reader_never_leaks_credentials() -> None:
    body = _extract_shell_function("alembic_revision_facts")
    for forbidden in ("password", "render_as_string", "str(url)", "settings.database_url", "os.environ"):
        assert forbidden not in body, f"the reader must not expose {forbidden!r}"


def test_only_safe_connection_components_are_logged() -> None:
    """Driver, host, port and database name — never the DSN."""
    script = _deploy_script()
    assert "Migration runner connects as:" in script
    assert "DATABASE_URL=" not in script


# ===========================================================================
# Database identity cross-check, before any change
# ===========================================================================


def test_both_sides_report_the_same_identity_triple() -> None:
    """Name plus cluster system identifier plus OID pins one physical database."""
    script = _deploy_script()
    for component in ("current_database()", "system_identifier", "pg_control_system()"):
        assert script.count(component) >= 2, f"{component} must be read on both sides"


def test_identity_mismatch_fails_closed_before_anything_is_touched() -> None:
    guard = _main_index("are NOT on the same database")
    assert guard < _main_index("$COMPOSE --profile ops run --rm --no-deps migrate\n")
    assert guard < _main_index("$COMPOSE stop -t 300 altegio-inbox-worker")
    assert guard < _main_index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar')


def test_identity_mismatch_diagnostic_names_both_sides() -> None:
    script = _deploy_script()
    assert "migrate  sees:" in script
    assert "postgres sees:" in script
    assert "DATABASE_URL and POSTGRES_DB disagree" in script


def test_the_two_revision_sources_are_compared() -> None:
    """migrate's MigrationContext vs a direct read of alembic_version."""
    script = _deploy_script()
    assert 'POSTGRES_REVISION="$(postgres_alembic_version)"' in script
    assert '[ "$POSTGRES_REVISION" != "$REVISION_BEFORE" ]' in script
    assert "The two revision sources disagree" in script


def test_revision_disagreement_fails_closed_before_anything_is_touched() -> None:
    guard = _main_index("The two revision sources disagree")
    assert guard < _main_index("$COMPOSE --profile ops run --rm --no-deps migrate\n")
    assert guard < _main_index("$COMPOSE stop -t 300 altegio-inbox-worker")
    assert guard < _main_index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar')


def test_direct_read_distinguishes_missing_table_from_empty_table() -> None:
    body = _extract_shell_function("postgres_alembic_version")
    assert "NOTABLE" in body
    assert "EMPTY" in body
    assert "string_agg" in body, "every row must be returned, not just the first"


def test_cross_check_runs_after_the_backup() -> None:
    assert _main_index("pg_dump") < _main_index('POSTGRES_DB_IDENTITY="$(postgres_db_identity)"')


# ===========================================================================
# Production reality: the database is on 9a1f4c7b2e3d
# ===========================================================================


def test_production_revision_takes_the_exact_transition_branch() -> None:
    """9a1f4c7b2e3d == PRE_PR3_REVISION, so this is a plain Phase B deploy."""
    script = _deploy_script()
    assert f'PRE_PR3_REVISION="{PRODUCTION_REVISION}"' in script
    assert 'if [ "$REVISION_BEFORE" = "$PRE_PR3_REVISION" ] && [ "$TARGET_HEAD" = "$PR3_REVISION" ]; then' in script


def test_no_catch_up_machinery_remains_in_this_hotfix() -> None:
    """Phase A was removed: production needs none of it, so it is not carried."""
    script = _deploy_script()
    for removed in ("PHASE_A", "AUDITED_CATCHUP_PATH", "expected_catchup_from", "CATCHUP_PATH"):
        assert removed not in script, f"{removed} should have been removed from the hotfix"


def test_a_lagging_database_still_refuses_instead_of_guessing() -> None:
    script = _deploy_script()
    assert "would apply PR-3" in script
    assert "Bring the database to $PRE_PR3_REVISION first" in script
    assert "No schema change was made" in script


def test_the_transition_is_exactly_one_step() -> None:
    script = _deploy_script()
    assert '[ "$PR3_PARENT" != "$PRE_PR3_REVISION" ]' in script
    assert "is no longer a direct child of" in script


def test_worker_stop_happens_only_inside_the_armed_transition() -> None:
    """Nothing is drained until the classification proved DB == PRE_PR3."""
    main = _main_flow()
    guard = main.index('if [ "$PR3_TRANSITION" -eq 1 ]; then')
    assert guard < main.index("$COMPOSE stop -t 300 altegio-inbox-worker")
    assert guard < main.index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar')


def test_boundary_is_recorded_only_after_classification() -> None:
    assert _main_index('SCRIPT_FACTS="$(alembic_script_facts') < _main_index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar')


def test_rollback_remains_bounded_to_the_pr3_step() -> None:
    script = _deploy_script()
    assert script.count("alembic downgrade") == 1
    assert 'alembic downgrade "$PRE_PR3_REVISION"' in script
    assert "downgrade -1" not in script
    assert "downgrade base" not in script


def test_a_verified_regular_worker_still_blocks_rollback() -> None:
    script = _deploy_script()
    assert 'if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]; then' in script
    assert "NO schema rollback" in script


# ===========================================================================
# Nothing is softened, nothing is stamped
# ===========================================================================


def test_the_deploy_never_stamps_or_rewrites_alembic_version() -> None:
    script = "\n".join(line for line in _deploy_script().splitlines() if not line.lstrip().startswith("#"))
    assert "alembic stamp" not in script
    for mutation in (
        "UPDATE alembic_version",
        "INSERT INTO alembic_version",
        "DELETE FROM alembic_version",
        "TRUNCATE alembic_version",
        "DROP TABLE alembic_version",
    ):
        assert mutation not in script, f"the deploy must never run {mutation!r}"


def test_no_mandatory_guard_is_softened() -> None:
    script = _deploy_script()
    assert "continue-on-error" not in script
    assert "set -Eeuo pipefail" in script


def test_the_classification_never_sources_or_prints_the_environment() -> None:
    script = _deploy_script()
    for forbidden in ("source .env", "cat .env", "printenv", "set -x"):
        assert forbidden not in script, f"the deploy must not use {forbidden!r}"


def test_embedded_python_blocks_are_syntactically_valid() -> None:
    """The readers are Python inside shell inside YAML; compile them for real."""
    blocks = re.findall(r"/app/\.venv/bin/python -c '\n(.*?)\n' 2>/dev/null", _deploy_script(), re.DOTALL)
    assert len(blocks) == 2, f"expected two embedded readers, found {len(blocks)}"
    for index, block in enumerate(blocks):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / f"block_{index}.py"
            source.write_text(block)
            completed = subprocess.run(
                ["python3", "-m", "py_compile", str(source)],
                capture_output=True,
                text=True,
                env=dict(os.environ),
            )
            assert completed.returncode == 0, f"embedded block {index} does not compile: {completed.stderr}"
