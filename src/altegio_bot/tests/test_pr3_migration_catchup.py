"""The audited pre-PR-3 catch-up (Phase A) must stay exactly what was audited.

Production can sit a few ORDINARY revisions behind the pre-PR-3 revision. Those
have to be applied on their own, while the legacy runtime keeps working, BEFORE
the worker-drained constraint swap of PR-3 (Phase B). Mixing them into the PR-3
step would make the bounded rollback ambiguous about how far back to go.

Two independent things are pinned here:

* the SHAPE of the shell program — Phase A runs after the backup, before the
  deploy boundary, the worker stop and every queue mutation, targets the exact
  pre-PR-3 revision rather than ``head``, and re-reads the revision afterwards;
* the CONTENT of the audited path — the revision ids hardcoded in the deploy
  script must still be a real, contiguous, linear chain in the Alembic graph,
  and must still be the exact path Alembic itself would take. A new revision
  slipped into the middle changes that and fails here instead of being applied
  to production unaudited.

The audited property of every revision on the path is that it is additive,
idempotent, and does not rename or drop a constraint the old worker pins by
name in ``ON CONFLICT ON CONSTRAINT`` — which is precisely what PR-3 itself
does, and why PR-3 alone needs the drained window.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEPLOY_SCRIPT_FILE = _REPO_ROOT / "scripts" / "deploy_pr3.sh"
ALEMBIC_INI = _REPO_ROOT / "alembic.ini"

PRE_PR3_REVISION = "9a1f4c7b2e3d"
PR3_REVISION = "c1a7d3f905b2"

# The exact audited chain, oldest → newest, ending at the pre-PR-3 revision.
# Mirrors AUDITED_CATCHUP_PATH in the deploy script; the test below proves the
# two agree, so this list cannot silently drift from the shell.
AUDITED_CATCHUP_PATH = (
    "b7c8d9e0f1a2",
    "c9d0e1f2a3b4",
    "d0e1f2a3b4c5",
    "d8f6e4c2b1a0",
    "e9a7c6b5d4f3",
    "8923be993170",
    "8705ec49cc73",
    "9a1f4c7b2e3d",
)


def _deploy_script() -> str:
    return DEPLOY_SCRIPT_FILE.read_text()


def _index(marker: str) -> int:
    position = _deploy_script().find(marker)
    assert position != -1, f"deploy script is missing the required marker {marker!r}"
    return position


def _main_flow() -> str:
    """The rollout only. ``recover()`` is defined first and repeats statements."""
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


def _script_directory() -> ScriptDirectory:
    return ScriptDirectory.from_config(Config(str(ALEMBIC_INI)))


def _phase_a_block() -> str:
    """The Phase A body only, so assertions cannot match the rest of the file."""
    start = _index('if [ "$PHASE_A_REQUIRED" -eq 1 ]; then')
    script = _deploy_script()
    end = script.index('if [ "$PR3_TRANSITION" -eq 1 ]; then', start)
    return script[start:end]


def _extract_shell_function(name: str) -> str:
    script = _deploy_script()
    start = _index(f"{name}() {{")
    body = script[start:]
    end = body.index("\n}\n") + len("\n}\n")
    return body[:end]


# ===========================================================================
# The audited path is real, and is what Alembic actually does
# ===========================================================================


def test_script_and_test_agree_on_the_audited_path() -> None:
    """The shell list and this module's list must be the same chain."""
    match = re.search(r'AUDITED_CATCHUP_PATH="([^"]+)"', _deploy_script())
    assert match is not None, "deploy script does not define AUDITED_CATCHUP_PATH"
    assert tuple(match.group(1).split()) == AUDITED_CATCHUP_PATH


def test_audited_path_ends_at_the_pre_pr3_revision() -> None:
    assert AUDITED_CATCHUP_PATH[-1] == PRE_PR3_REVISION


def test_audited_path_is_a_contiguous_linear_chain() -> None:
    """Each entry must be the direct, single parent of the next one."""
    script = _script_directory()
    for parent, child in zip(AUDITED_CATCHUP_PATH, AUDITED_CATCHUP_PATH[1:]):
        down = script.get_revision(child).down_revision
        assert down == parent, f"{child}.down_revision is {down!r}, expected {parent!r}"


@pytest.mark.parametrize("start", AUDITED_CATCHUP_PATH[:-1])
def test_alembic_computes_exactly_the_audited_path(start: str) -> None:
    """The real graph walk must equal the audited tail, for every entry point."""
    script = _script_directory()
    revisions = list(script.iterate_revisions(PRE_PR3_REVISION, start))
    revisions.reverse()
    computed = [revision.revision for revision in revisions if revision.revision != start]
    expected = list(AUDITED_CATCHUP_PATH[AUDITED_CATCHUP_PATH.index(start) + 1 :])
    assert computed == expected


def test_pr3_is_still_a_direct_child_of_the_pre_pr3_revision() -> None:
    """Phase B, and therefore the bounded rollback, must stay a single step."""
    assert _script_directory().get_revision(PR3_REVISION).down_revision == PRE_PR3_REVISION


def test_the_graph_still_has_exactly_one_head_and_it_is_pr3() -> None:
    assert _script_directory().get_heads() == [PR3_REVISION]


def test_the_full_documented_path_is_reachable_end_to_end() -> None:
    """57cd…-style claims aside, this is the path the deploy actually walks."""
    script = _script_directory()
    revisions = list(script.iterate_revisions(PR3_REVISION, AUDITED_CATCHUP_PATH[0]))
    revisions.reverse()
    walked = [revision.revision for revision in revisions if revision.revision != AUDITED_CATCHUP_PATH[0]]
    assert walked == [*AUDITED_CATCHUP_PATH[1:], PR3_REVISION]


def test_no_audited_revision_touches_a_pinned_on_conflict_constraint() -> None:
    """The old worker pins these by name; only PR-3 may swap them."""
    pinned = (
        "uq_clients_company_altegio_id",
        "uq_records_company_altegio_id",
    )
    script = _script_directory()
    for revision_id in AUDITED_CATCHUP_PATH:
        source = Path(script.get_revision(revision_id).path).read_text()
        for constraint in pinned:
            assert constraint not in source, (
                f"{revision_id} references {constraint}, which the legacy worker pins by name; "
                "it cannot be applied while that worker is running"
            )


# ===========================================================================
# expected_catchup_from(): executed, not pattern-matched
# ===========================================================================


def _run_expected_catchup(revision: str) -> tuple[int, str]:
    """Execute the real shell helper against the real audited path."""
    with tempfile.TemporaryDirectory() as tmp:
        program = Path(tmp) / "check.sh"
        program.write_text(
            f'AUDITED_CATCHUP_PATH="{" ".join(AUDITED_CATCHUP_PATH)}"\n'
            + _extract_shell_function("expected_catchup_from")
            + f'\nexpected_catchup_from "{revision}"\n'
        )
        completed = subprocess.run(
            ["sh", str(program)],
            env=dict(os.environ),
            capture_output=True,
            text=True,
        )
        return completed.returncode, completed.stdout.strip()


@pytest.mark.parametrize("start", AUDITED_CATCHUP_PATH[:-1])
def test_helper_returns_the_audited_tail_for_every_entry_point(start: str) -> None:
    """Partial catch-up retry: resuming from any audited revision works."""
    returncode, output = _run_expected_catchup(start)
    expected = " ".join(AUDITED_CATCHUP_PATH[AUDITED_CATCHUP_PATH.index(start) + 1 :])
    assert returncode == 0
    assert output == expected


def test_helper_refuses_the_pre_pr3_revision_itself() -> None:
    """Already there — there is no catch-up to run, Phase B applies instead."""
    returncode, output = _run_expected_catchup(PRE_PR3_REVISION)
    assert returncode == 1
    assert output == ""


@pytest.mark.parametrize(
    "revision",
    [
        "57cd7c3a7a27",  # not a revision in this repository at all
        "c1a7d3f905b2",  # PR-3 itself is not a catch-up entry point
        "aef7d5e35640",  # a real revision, but older than the audited window
        "deadbeef1234",
        "",
    ],
)
def test_helper_fails_closed_for_anything_off_the_audited_path(revision: str) -> None:
    returncode, output = _run_expected_catchup(revision)
    assert returncode == 1
    assert output == ""


# ===========================================================================
# Phase A: placement inside the deploy program
# ===========================================================================


def test_phase_a_runs_after_the_backup() -> None:
    assert _index("pg_dump") < _index('if [ "$PHASE_A_REQUIRED" -eq 1 ]; then')


def test_phase_a_runs_before_the_deploy_boundary() -> None:
    """The boundary scopes the constraint-failure requeue to Phase B only."""
    assert _index('if [ "$PHASE_A_REQUIRED" -eq 1 ]; then') < _index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar')


def test_phase_a_runs_before_any_worker_is_stopped() -> None:
    assert _main_index('if [ "$PHASE_A_REQUIRED" -eq 1 ]; then') < _main_index(
        "$COMPOSE stop -t 300 altegio-inbox-worker"
    )


def test_phase_a_upgrades_to_the_exact_pre_pr3_revision() -> None:
    assert 'alembic upgrade "$PRE_PR3_REVISION"' in _uncommented(_phase_a_block())


def test_phase_a_never_upgrades_to_head() -> None:
    """`upgrade head` here would swap constraints with the old worker live."""
    block = _uncommented(_phase_a_block())
    assert "upgrade head" not in block
    assert "$TARGET_HEAD" not in block


def test_phase_a_stops_no_worker_and_starts_no_canary() -> None:
    block = _uncommented(_phase_a_block())
    for forbidden in ("$COMPOSE stop", "docker stop", "COMPOSE run -d", "CANARY"):
        assert forbidden not in block, f"Phase A must not contain {forbidden!r}"


def test_phase_a_changes_no_event_status() -> None:
    block = _uncommented(_phase_a_block())
    for forbidden in ("UPDATE altegio_events", "recover_orphaned_processing_rows", "processing_count"):
        assert forbidden not in block, f"Phase A must not contain {forbidden!r}"


def test_phase_a_does_not_record_the_deploy_boundary() -> None:
    assert "DEPLOY_BOUNDARY_EPOCH_US=" not in _uncommented(_phase_a_block())


def test_phase_a_does_not_set_the_transition_started_flag() -> None:
    """Nothing was stopped, so recovery must not think a transition began."""
    assert "PR3_TRANSITION_STARTED=1" not in _uncommented(_phase_a_block())


def test_phase_a_rereads_the_revision_from_the_database() -> None:
    block = _uncommented(_phase_a_block())
    assert 'REVISION_AFTER_CATCHUP="$(alembic_revision)"' in block
    assert '[ "$REVISION_AFTER_CATCHUP" != "$PRE_PR3_REVISION" ]' in block


def test_phase_b_is_armed_only_after_an_exact_revision_match() -> None:
    """PR3_TRANSITION=1 must come after the re-read check, not before it."""
    block = _uncommented(_phase_a_block())
    check = block.index('[ "$REVISION_AFTER_CATCHUP" != "$PRE_PR3_REVISION" ]')
    armed = block.index("PR3_TRANSITION=1")
    assert check < armed


def test_phase_a_failure_is_fail_closed_and_not_softened() -> None:
    block = _uncommented(_phase_a_block())
    assert "exit 1" in block
    assert "continue-on-error" not in block
    assert "|| true" not in block


def test_phase_a_failure_says_nothing_was_stopped_or_changed() -> None:
    block = _phase_a_block()
    assert "No worker was stopped and no event status was changed." in block


def test_phase_a_is_not_rolled_back_automatically() -> None:
    assert "NOT rolled back automatically" in _phase_a_block()


# ===========================================================================
# Rollback stays bounded to PR-3
# ===========================================================================


def test_rollback_targets_the_pre_pr3_revision_not_the_starting_revision() -> None:
    """After a successful Phase A, recovery rests on the pre-PR-3 revision."""
    script = _deploy_script()
    assert 'alembic downgrade "$PRE_PR3_REVISION"' in script
    assert 'alembic downgrade "$REVISION_BEFORE"' not in script


def test_there_is_exactly_one_downgrade_and_it_is_bounded() -> None:
    script = _deploy_script()
    assert script.count("alembic downgrade") == 1
    assert "downgrade -1" not in script
    assert "downgrade base" not in script


@pytest.mark.parametrize("revision", AUDITED_CATCHUP_PATH[:-1])
def test_no_catchup_revision_is_ever_a_downgrade_target(revision: str) -> None:
    """Nothing may roll production back past the audited catch-up."""
    assert f"downgrade {revision}" not in _deploy_script()
    assert f'downgrade "{revision}"' not in _deploy_script()


def test_rollback_comment_states_that_catchup_is_preserved() -> None:
    assert "catch-up is preserved" in _deploy_script()


# ===========================================================================
# The unresolvable-revision guard (fail closed, before anything is touched)
# ===========================================================================


def test_revision_reader_checks_the_exit_status_before_scraping() -> None:
    """`alembic current` prints its FAILURE text, containing a hex id, to stdout.

    Scraping stdout without checking the status echoes a revision id back out of
    an error message and reports it as the database revision.
    """
    body = _extract_shell_function("alembic_revision")
    assert "|| return 1" in body, "the exit status of `alembic current` must be checked"
    assert "'^[0-9a-f]{12}'" in body, "only a bare id at the start of a line may be accepted"


def test_unresolvable_revision_fails_before_any_schema_or_queue_change() -> None:
    guard = _main_index("could not resolve the revision recorded in this database")
    assert guard < _main_index("$COMPOSE --profile ops run --rm --no-deps migrate\n")
    assert guard < _main_index("$COMPOSE stop -t 300 altegio-inbox-worker")
    assert guard < _main_index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar')


def test_the_deploy_never_stamps_or_rewrites_alembic_version() -> None:
    """A phantom revision must be investigated by hand, never stamped away."""
    script = _uncommented(_deploy_script())
    assert "alembic stamp" not in script
    # Mentioning the table in an operator-facing message is fine; writing to it
    # from the deploy is not.
    for mutation in (
        "UPDATE alembic_version",
        "INSERT INTO alembic_version",
        "DELETE FROM alembic_version",
        "TRUNCATE alembic_version",
        "DROP TABLE alembic_version",
    ):
        assert mutation not in script, f"the deploy must never run {mutation!r}"


def _run_revision_reader(stdout: str, returncode: int) -> tuple[int, str]:
    """Execute the real reader against a stubbed `docker` that fakes Alembic."""
    with tempfile.TemporaryDirectory() as tmp:
        stub = Path(tmp) / "docker"
        stub.write_text('#!/bin/sh\nprintf \'%s\\n\' "$FAKE_OUT"\nexit "$FAKE_RC"\n')
        stub.chmod(0o755)

        program = Path(tmp) / "check.sh"
        program.write_text(
            'COMPOSE="docker compose"\n' + _extract_shell_function("alembic_revision") + "\nalembic_revision\n"
        )

        env = dict(os.environ)
        env["PATH"] = f"{tmp}:{env['PATH']}"
        env["FAKE_OUT"] = stdout
        env["FAKE_RC"] = str(returncode)
        completed = subprocess.run(["sh", str(program)], env=env, capture_output=True, text=True)
        return completed.returncode, completed.stdout.strip()


def test_reader_returns_a_valid_revision() -> None:
    assert _run_revision_reader("9a1f4c7b2e3d", 0) == (0, "9a1f4c7b2e3d")


def test_reader_reports_an_empty_database_as_empty_not_as_a_failure() -> None:
    """A fresh database has no revision yet; that is not an error."""
    assert _run_revision_reader("", 0) == (0, "")


def test_reader_never_scrapes_a_revision_out_of_the_failure_text() -> None:
    """The regression this guard exists for."""
    returncode, output = _run_revision_reader(
        "FAILED: Can't locate revision identified by '57cd7c3a7a27'",
        255,
    )
    assert returncode == 1, "an unresolvable revision must be reported as a failure"
    assert "57cd7c3a7a27" not in output, "the id was scraped out of an error message"
    assert output == ""
