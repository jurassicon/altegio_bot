"""The production deploy must make the PR-3 constraint swap a ONE-TIME, recoverable step.

The migration renames the unique constraints that the inbox worker pins by name
in ``ON CONFLICT ON CONSTRAINT``. Two things follow, and both are asserted here
as structure rather than as greps:

1. **Ordering.** The legacy worker must be stopped, its orphaned rows recovered
   and ``processing`` proven empty *before* the migration; a new-image worker
   must be proven *after* it and before the general rollout.

2. **One-shot-ness.** The special flow is armed only for the exact
   ``9a1f4c7b2e3d -> c1a7d3f905b2`` step. A repeat deploy, or any later
   revision, must take the ordinary path and must not be able to reach the
   downgrade branch — otherwise a second deploy could roll a live PR-3 schema
   back under a worker that requires it.

Also pinned: altegio-api is never stopped, the canary is never force-removed and
never confused with the regular Compose container, and constraint failures are
compared against a baseline instead of a rolling time window.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_FILE = _REPO_ROOT / ".github" / "workflows" / "ci_deploy.yml"
COMPOSE_FILE = _REPO_ROOT / "docker-compose.yml"
DEPLOY_SCRIPT_FILE = _REPO_ROOT / "scripts" / "deploy_pr3.sh"
DEPLOY_SCRIPT_PATH = "scripts/deploy_pr3.sh"

# GitHub evaluates a `script:` input as ONE template expression and caps it at
# this many characters. The full rollout program is several times larger, which
# is why it lives in its own file: inlining it made the workflow itself invalid,
# so not even lint or tests could start.
GITHUB_EXPRESSION_LIMIT = 21000

INBOX_SERVICE = "altegio-inbox-worker"
PR3_REVISION = "c1a7d3f905b2"
PRE_PR3_REVISION = "9a1f4c7b2e3d"

CONSTRAINT_NAMES = (
    "uq_clients_company_altegio_id",
    "uq_records_company_altegio_id",
    "uq_clients_provider_company_altegio_id",
    "uq_records_provider_company_altegio_id",
)


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_FILE.read_text())


def _deploy_steps() -> list[dict[str, Any]]:
    return [step for step in _workflow()["jobs"]["deploy"]["steps"] if isinstance(step, dict)]


def _deploy_script() -> str:
    """The rollout program itself, now a versioned, directly testable file."""
    assert DEPLOY_SCRIPT_FILE.is_file(), f"missing deploy script: {DEPLOY_SCRIPT_FILE}"
    return DEPLOY_SCRIPT_FILE.read_text()


def _bootstrap_step() -> dict[str, Any]:
    """The SSH step that fetches the commit and hands over to the script."""
    for step in _deploy_steps():
        if DEPLOY_SCRIPT_PATH in str(step.get("with", {}).get("script", "")):
            return step
    raise AssertionError("no deploy step invokes " + DEPLOY_SCRIPT_PATH)


def _bootstrap_script() -> str:
    return str(_bootstrap_step().get("with", {}).get("script", ""))


def _index(marker: str) -> int:
    script = _deploy_script()
    position = script.find(marker)
    assert position != -1, f"deploy script is missing the required marker {marker!r}"
    return position


def _between(start: str, end: str) -> str:
    return _deploy_script()[_index(start) : _index(end)]


def _recovery_body() -> str:
    return _between("recover() {", "trap 'recover $?' EXIT")


def _main_flow() -> str:
    """Everything after the helper/recovery definitions."""
    return _deploy_script()[_index("trap 'recover $?' EXIT") :]


def _main_index(marker: str) -> int:
    """Index inside the main flow only.

    Several statements legitimately appear in both ``recover()`` and the main
    rollout; ordering assertions must look at the rollout copy.
    """
    main = _main_flow()
    position = main.find(marker)
    assert position != -1, f"main deploy flow is missing the marker {marker!r}"
    return position


# Ordered phases of the rollout, each identified by a statement that can only
# belong to that phase and only appears in the main flow.
_PHASES = (
    ("build", "$COMPOSE build"),
    ("backup", "pg_dump"),
    ("structured revision read", 'REVISION_BEFORE="$(fact "$REVISION_FACTS" REVISION)"'),
    ("database identity cross-check", 'POSTGRES_DB_IDENTITY="$(postgres_db_identity)"'),
    ("transition classification", 'SCRIPT_FACTS="$(alembic_script_facts "$REVISION_BEFORE")"'),
    ("deploy boundary", 'DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar'),
    ("legacy worker stop", 'echo "🛑 Stopping the legacy altegio-inbox-worker'),
    ("constraint-failure baseline", 'CONSTRAINT_FAILURES_BEFORE="$(constraint_failure_count)"'),
    ("migration", "$COMPOSE --profile ops run --rm --no-deps migrate\n"),
    ("canary verification", "CANARY_STATE="),
    ("regular worker verification", 'REGULAR_WORKER_IDS="$($COMPOSE ps -q'),
    ("general compose up", "$COMPOSE up -d --remove-orphans"),
)


def test_deploy_phases_are_in_the_required_order() -> None:
    positions = [(name, _index(marker)) for name, marker in _PHASES]
    ordered = sorted(positions, key=lambda item: item[1])
    assert [name for name, _ in positions] == [name for name, _ in ordered], (
        f"deploy phases are out of order: {[name for name, _ in ordered]}"
    )


@pytest.mark.parametrize(("earlier", "later"), [(a[0], b[0]) for a, b in zip(_PHASES, _PHASES[1:])])
def test_each_phase_precedes_the_next(earlier: str, later: str) -> None:
    markers = dict(_PHASES)
    assert _index(markers[earlier]) < _index(markers[later])


# ===========================================================================
# The one-time transition gate
# ===========================================================================


def test_target_head_comes_from_the_code_not_a_hardcoded_revision() -> None:
    """A hardcoded head would fail every future migration deploy."""
    script = _deploy_script()
    assert 'TARGET_HEAD="$(fact "$SCRIPT_FACTS" TARGET_HEAD)"' in script
    assert 'if [ "$REVISION_AFTER" != "$TARGET_HEAD" ]' in script
    # The post-migration check must NOT compare against the literal PR-3 head.
    assert 'REVISION_AFTER" != "$PR3_REVISION"' not in script


def test_exactly_one_alembic_head_is_required() -> None:
    script = _deploy_script()
    assert 'HEAD_COUNT="$(fact "$SCRIPT_FACTS" HEAD_COUNT)"' in script
    assert 'if [ "$HEAD_COUNT" != "1" ]' in script


def test_ancestry_is_computed_with_the_alembic_script_directory() -> None:
    """String comparison cannot answer "is PR-3 an ancestor of this head?"."""
    script = _deploy_script()
    assert "ScriptDirectory.from_config" in script
    assert "iterate_revisions" in script
    assert "PR3_IN_HEAD_LINEAGE" in script
    assert "PR3_IN_DB_LINEAGE" in script


def test_transition_matrix_branches_on_lineage_not_only_strings() -> None:
    """The four documented cases must each have their own branch."""
    script = _deploy_script()

    # 1. PR-3 already in the DB lineage (repeat deploy, or a later revision).
    assert 'if [ "$PR3_IN_DB_LINEAGE" = "1" ]' in script
    repeat_branch = _between('if [ "$PR3_IN_DB_LINEAGE" = "1" ]', 'elif [ "$PR3_IN_HEAD_LINEAGE" = "1" ]')
    assert "PR3_TRANSITION=0" in repeat_branch
    assert "PR3_TRANSITION=1" not in repeat_branch

    # 2. The exact one-time step arms the special flow.
    assert 'if [ "$REVISION_BEFORE" = "$PRE_PR3_REVISION" ] && [ "$TARGET_HEAD" = "$PR3_REVISION" ]; then' in script

    # 3. PR-3 reached as part of a multi-revision upgrade is blocked outright.
    blocked = _between("would apply PR-3", "One-time constraint-swap window")
    assert "exit 1" in blocked
    assert "No schema change was made" in blocked

    # 4. PR-3 not in the graph at all → ordinary flow.
    assert script.count("PR3_TRANSITION=0") >= 2


@pytest.mark.parametrize(
    "refusal",
    [
        "Bring the database to $PRE_PR3_REVISION first",
        "is no longer a direct child of",
        "is not a well-formed Alembic revision id",
        "are NOT on the same database",
        "The two revision sources disagree",
        "not one this code knows",
        "Alembic heads; exactly one is required",
        "has no Alembic revision",
    ],
)
def test_every_classification_refusal_precedes_any_schema_change(refusal: str) -> None:
    """Each fail-closed branch must stop before the migrate step runs."""
    assert _index(refusal) < _index("$COMPOSE --profile ops run --rm --no-deps migrate\n")


def test_special_flow_is_entirely_inside_the_transition_guard() -> None:
    """Stop, orphan reset and canary must not run on an ordinary deploy."""
    main = _main_flow()
    guard = main.index('if [ "$PR3_TRANSITION" -eq 1 ]; then')
    stop = main.index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
    canary = main.index('CANARY_ID="$($COMPOSE run -d')
    assert guard < stop, "the legacy worker stop is not gated by PR3_TRANSITION"
    assert guard < canary, "the canary is not gated by PR3_TRANSITION"


# ===========================================================================
# Rollback safety
# ===========================================================================


def test_transition_applied_flag_requires_a_real_revision_change() -> None:
    """A no-op migration must never arm the rollback branch."""
    script = _deploy_script()
    start = _index('if [ "$PR3_TRANSITION" -eq 1 ] \\')
    arming = script[start : script.index("PR3_TRANSITION_APPLIED=1", start)]
    assert '[ "$REVISION_BEFORE" = "$PRE_PR3_REVISION" ]' in arming
    assert '[ "$REVISION_AFTER" = "$PR3_REVISION" ]' in arming
    assert '[ "$REVISION_AFTER" != "$REVISION_BEFORE" ]' in arming


def test_downgrade_exists_only_in_the_recovery_path() -> None:
    script = _deploy_script()
    assert script.count("alembic downgrade") == 1, "downgrade appears outside recovery"
    assert "alembic downgrade" in _recovery_body()


def test_downgrade_is_guarded_by_the_transition_flags() -> None:
    recovery = _recovery_body()
    assert '[ "$PR3_TRANSITION" -ne 1 ]' in recovery, "recovery does not check PR3_TRANSITION"
    downgrade_guard = recovery.index('if [ "$PR3_TRANSITION_APPLIED" -eq 1 ]')
    downgrade = recovery.index('alembic downgrade "$PRE_PR3_REVISION"')
    assert downgrade_guard < downgrade
    # And the actual DB revision has to be PR-3 right before the downgrade.
    revision_check = recovery.index('if [ "$CURRENT_REVISION" != "$PR3_REVISION" ]')
    assert revision_check < downgrade


def test_downgrade_targets_an_exact_revision_not_a_relative_step() -> None:
    script = _deploy_script()
    assert f'PRE_PR3_REVISION="{PRE_PR3_REVISION}"' in script
    assert 'alembic downgrade "$PRE_PR3_REVISION"' in script
    assert "downgrade -1" not in script
    assert 'downgrade "-1"' not in script


def test_a_verified_regular_worker_blocks_any_rollback() -> None:
    recovery = _recovery_body()
    verified_branch = recovery[recovery.index('if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]') :]
    assert "NO schema rollback" in verified_branch[:600]
    # The rollback branch is reached only after both verified-flags fall through.
    assert recovery.index('if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]') < recovery.index("alembic downgrade")
    assert recovery.index('if [ "$CANARY_VERIFIED" -eq 1 ]') < recovery.index("alembic downgrade")


def test_old_worker_is_only_started_after_the_revision_is_confirmed() -> None:
    recovery = _recovery_body()
    start = recovery.index("start_preserved_old_worker")
    assert recovery.index('if [ "$CURRENT_REVISION" != "$PRE_PR3_REVISION" ]') < start
    assert 'elif [ "$CURRENT_REVISION" != "$PRE_PR3_REVISION" ]' in recovery


def test_migration_failure_is_not_masked() -> None:
    script = _deploy_script()
    assert "set -Eeuo pipefail" in script
    assert "migrate || " not in script


# ===========================================================================
# Legacy first rollout: bounded orphan recovery
# ===========================================================================


def test_legacy_stop_is_not_described_as_graceful() -> None:
    """The parent image has no SIGTERM handler; saying otherwise would be a lie."""
    script = _deploy_script()
    assert "NOT a graceful drain" in script
    assert "PARENT image has no SIGTERM" in script


def test_orphan_reset_order_stop_then_confirm_then_reset_then_migrate() -> None:
    main = _main_flow()
    stop = main.index('echo "🛑 Stopping the legacy altegio-inbox-worker')
    confirmed = main.index("if ! require_no_inbox_worker_running; then")
    reset = main.index("if ! recover_orphaned_processing_rows; then")
    migration = main.index("$COMPOSE --profile ops run --rm --no-deps migrate\n")
    assert stop < confirmed < reset < migration


def test_orphan_reset_is_bounded_and_guarded() -> None:
    script = _deploy_script()
    body = _between("recover_orphaned_processing_rows() {", "# THE single definition")
    # Refuses while any worker still runs.
    assert "if ! require_no_inbox_worker_running; then" in body
    assert "Refusing to touch event statuses" in body
    # Only claimed-but-unfinished rows.
    assert "WHERE status = 'processing' AND processed_at IS NULL" in body
    # And it proves the queue is empty afterwards.
    assert "SELECT count(*) FROM altegio_events WHERE status = 'processing';" in script
    assert 'if [ "$REMAINING" != "0" ]' in body


def test_orphan_reset_is_never_an_unconditional_deploy_step() -> None:
    """A blanket reset on every deploy would destroy in-flight work."""
    script = _deploy_script()
    assert "UPDATE altegio_events SET status = 'received' WHERE status = 'processing';" not in script
    # Exactly two UPDATEs, both bounded: orphaned processing rows and this
    # deploy's constraint failures.
    assert script.count("UPDATE altegio_events") == 2
    main = _main_flow()
    guard = main.index('if [ "$PR3_TRANSITION" -eq 1 ]; then')
    assert guard < main.index("if ! recover_orphaned_processing_rows; then")


def test_orphan_reset_logs_only_a_count() -> None:
    body = _between("recover_orphaned_processing_rows() {", "# THE single definition")
    for forbidden in ("payload", "phone", "customer", "error"):
        assert forbidden not in body, f"the orphan reset mentions {forbidden!r}"


# ===========================================================================
# Canary lifecycle
# ===========================================================================


def test_canary_is_never_force_removed() -> None:
    script = _deploy_script()
    assert "docker rm -f" not in script, "the canary (a real worker) must never be force-removed"
    assert "docker kill" not in script
    assert "$COMPOSE kill" not in script


def test_canary_is_verified_by_exact_container_id() -> None:
    script = _deploy_script()
    assert 'CANARY_ID="$($COMPOSE run -d --no-deps --name "$CANARY_NAME" altegio-inbox-worker)"' in script
    for field in ("{{.State.Status}}", "{{.RestartCount}}", "{{.Image}}"):
        assert f"container_field \"$CANARY_ID\" '{field}'" in script
    assert 'com.docker.compose.oneoff"}}' in script


def test_regular_worker_is_verified_by_exact_compose_id() -> None:
    script = _deploy_script()
    assert 'REGULAR_WORKER_IDS="$($COMPOSE ps -q altegio-inbox-worker)"' in script
    assert 'if [ "$REGULAR_WORKER_ID" = "$CANARY_ID" ]' in script
    assert 'if [ "$REGULAR_ONEOFF" = "True" ]' in script
    assert 'if [ "$REGULAR_STATE" != "running" ]' in script
    assert 'if [ "$REGULAR_RESTARTS" != "0" ]' in script
    assert 'if [ "$REGULAR_IMAGE" != "$CANARY_IMAGE" ]' in script


def test_regular_identity_and_all_worker_scan_are_separate_helpers() -> None:
    """Two different questions, two different mechanisms.

    "Is ANY worker running?" must be a Compose label scan so an operator's
    ad-hoc one-off is caught. "Which container is THE regular worker?" must be
    the exact Compose service id, because a label scan would also match the
    canary and could leave the system with no worker at all.
    """
    scan = _between("running_inbox_worker_ids() {", "any_inbox_worker_running() {")
    assert "docker ps -q" in scan
    assert "label=com.docker.compose.project=altegio_bot" in scan
    assert "label=com.docker.compose.service=altegio-inbox-worker" in scan

    identity = _between("regular_worker_id() {", "container_field() {")
    assert "$COMPOSE ps -q altegio-inbox-worker" in identity
    assert "docker ps" not in identity, "regular identity must not use a label scan"

    # The all-worker check must not be built from known ids/names only.
    presence = _between("any_inbox_worker_running() {", "require_no_inbox_worker_running() {")
    assert "running_inbox_worker_ids" in presence
    assert "CANARY_NAME" not in presence
    assert "CANARY_ID" not in presence


def test_canary_and_regular_have_separate_verification_flags() -> None:
    script = _deploy_script()
    assert "CANARY_VERIFIED=0" in script and "CANARY_VERIFIED=1" in script
    assert "REGULAR_WORKER_VERIFIED=0" in script and "REGULAR_WORKER_VERIFIED=1" in script
    assert "NEW_WORKER_VERIFIED" not in script, "the merged flag must be gone"
    main = _main_flow()
    assert main.index("CANARY_VERIFIED=1") < main.index("REGULAR_WORKER_VERIFIED=1")


def test_canary_is_removed_only_after_the_regular_worker_is_verified() -> None:
    main = _main_flow()
    assert main.index("REGULAR_WORKER_VERIFIED=1") < main.index("remove_stopped_canary")


def test_canary_is_drained_and_verified_before_removal() -> None:
    main = _main_flow()
    drain = main.index("if ! stop_canary_and_verify_exit; then")
    removal = main.index("remove_stopped_canary")
    assert drain < removal
    body = _between("stop_canary_and_verify_exit() {", "# Only ever called once the canary")
    assert 'docker stop -t 300 "$CANARY_ID"' in body
    assert "verify_container_drained" in body


def test_regular_worker_failure_leaves_the_canary_running() -> None:
    script = _deploy_script()
    assert "The canary is left RUNNING on every failure below" in script
    recovery = _recovery_body()
    canary_branch = recovery[recovery.index('if [ "$CANARY_VERIFIED" -eq 1 ]') :]
    assert "NOT removing the canary" in canary_branch or "NOT rolling back the schema and NOT removing" in canary_branch
    assert "FAILED deploy" in canary_branch


def test_a_stale_canary_blocks_the_deploy_before_anything_is_stopped() -> None:
    main = _main_flow()
    stale = main.index("A stale container named $CANARY_NAME already exists")
    stop = main.index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
    assert stale < stop
    assert "Nothing was stopped" in _deploy_script()


# ===========================================================================
# Constraint-failure baseline
# ===========================================================================


def test_constraint_failures_use_a_baseline_not_a_time_window() -> None:
    script = _deploy_script()
    assert "now() - interval" not in script, "a rolling window would catch pre-existing failures"
    assert 'CONSTRAINT_FAILURES_BEFORE="$(constraint_failure_count)"' in script
    assert 'CONSTRAINT_FAILURES_AFTER="$(constraint_failure_count)"' in script
    assert '[ "$CONSTRAINT_FAILURES_AFTER" -gt "$CONSTRAINT_FAILURES_BEFORE" ]' in script
    assert _index('CONSTRAINT_FAILURES_BEFORE="$(constraint_failure_count)"') < _index(
        'CONSTRAINT_FAILURES_AFTER="$(constraint_failure_count)"'
    )


def test_constraint_probe_prints_no_event_content() -> None:
    script = _deploy_script()
    assert "SELECT payload" not in script
    assert 'echo "$CONSTRAINT_FAILURES_AFTER_DETAIL' not in script
    assert "phone_e164" not in script


# ===========================================================================
# Preserved guarantees
# ===========================================================================


def test_api_is_never_stopped_during_the_migration_window() -> None:
    script = _deploy_script()
    for forbidden in (
        "$COMPOSE stop altegio-api",
        "$COMPOSE stop -t 300 altegio-api",
        "$COMPOSE down",
        "docker stop altegio-api",
    ):
        assert forbidden not in script, f"the deploy stops the API: {forbidden!r}"


def test_inbox_worker_stop_has_a_non_zero_timeout() -> None:
    match = re.search(rf"\$COMPOSE stop -t (\d+) {re.escape(INBOX_SERVICE)}", _deploy_script())
    assert match is not None, "the inbox worker is not stopped with an explicit timeout"
    assert int(match.group(1)) > 0
    assert "stop -t 0" not in _deploy_script()


def test_compose_gives_the_worker_time_to_drain() -> None:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    grace = compose["services"][INBOX_SERVICE].get("stop_grace_period")
    assert grace, f"{INBOX_SERVICE} has no stop_grace_period"
    assert str(grace) not in {"0", "0s"}


def test_recovery_is_declared_before_anything_is_stopped() -> None:
    """The trap must exist before the rollout stops the legacy worker."""
    assert _index("trap 'recover $?' EXIT") < _index('echo "🛑 Stopping the legacy altegio-inbox-worker')
    assert "recover() {" in _deploy_script()


def test_recovery_tracks_deploy_phases() -> None:
    script = _deploy_script()
    for flag in (
        "PR3_TRANSITION",
        "PR3_TRANSITION_STARTED",
        "PR3_TRANSITION_APPLIED",
        "CANARY_VERIFIED",
        "REGULAR_WORKER_VERIFIED",
        "OLD_WORKER_STOPPED",
    ):
        assert f"{flag}=0" in script, f"{flag} is never initialised"
        assert f"{flag}=1" in script, f"{flag} is never set"


def test_the_old_container_survives_until_a_new_worker_is_proven() -> None:
    main = _main_flow()
    stop = main.index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
    canary_verified = main.index("CANARY_VERIFIED=1")
    recreate = main.index(f"$COMPOSE up -d --force-recreate {INBOX_SERVICE}")
    assert stop < canary_verified < recreate
    assert f"$COMPOSE rm {INBOX_SERVICE}" not in _deploy_script()


def test_no_mandatory_operation_is_softened() -> None:
    """Scoped to the PR-3 window.

    The pre-existing Chatwoot check uses `|| true` on a purely diagnostic
    `docker inspect` inside a branch that then exits 1; that is out of scope.
    """
    window = _between("recover() {", "$COMPOSE up -d --remove-orphans")
    assert "|| true" not in window, "a mandatory PR-3 deploy operation is allowed to fail silently"
    for step in _deploy_steps():
        assert "continue-on-error" not in step, f"{step.get('name')!r} has continue-on-error"
    assert "continue-on-error" not in _workflow()["jobs"]["deploy"]


def test_existing_deploy_guarantees_are_preserved() -> None:
    script = _deploy_script()
    # The exact-commit guarantee now spans the bootstrap (which resets to it)
    # and the script (which re-verifies it before touching anything).
    assert 'git reset --hard "$DEPLOY_SHA"' in _bootstrap_script()
    assert 'if [ "$DEPLOYED_SHA" != "$DEPLOY_SHA" ]' in script
    assert "pg_dump" in script
    assert "Pre-deploy dump is empty" in script
    names = [step.get("name") for step in _deploy_steps()]
    assert "Wait for service stabilization" in names
    assert "Verify deployment on server" in names
    assert "Verify public HTTPS health endpoint" in names
    assert "Notify via Telegram" in names


# ===========================================================================
# Drain verification: `docker stop` exit code proves nothing
# ===========================================================================


def _extract_shell_function(name: str) -> str:
    """Pull one helper out of the deploy script, dedented to column 0."""
    script = _deploy_script()
    start = _index(f"{name}() {{")
    body = script[start:]
    end = body.index("\n}\n") + len("\n}\n")
    return body[:end]


def _run_drain_check(**state: str) -> int:
    """Execute the real ``verify_container_drained`` against a stubbed docker.

    The helper is shell, so the only honest way to pin its semantics is to run
    it. A fake ``docker`` on PATH answers ``inspect -f`` from *state*.
    """
    import os
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        stub = Path(tmp) / "docker"
        stub.write_text(
            "#!/bin/sh\n"
            'case "$3" in\n'
            '  *State.Status*) printf "%s" "$FAKE_STATUS" ;;\n'
            '  *State.ExitCode*) printf "%s" "$FAKE_EXIT" ;;\n'
            '  *State.OOMKilled*) printf "%s" "$FAKE_OOM" ;;\n'
            '  *State.Error*) printf "%s" "$FAKE_ERROR" ;;\n'
            '  *State.FinishedAt*) printf "%s" "$FAKE_FINISHED" ;;\n'
            "esac\n"
            'exit "${FAKE_INSPECT_RC:-0}"\n'
        )
        stub.chmod(0o755)

        program = Path(tmp) / "check.sh"
        program.write_text(
            _extract_shell_function("container_field")
            + "\n"
            + _extract_shell_function("verify_container_drained")
            + '\nverify_container_drained "cid" "canary"\n'
        )

        env = dict(os.environ)
        env["PATH"] = f"{tmp}:{env['PATH']}"
        env.update(
            {
                "FAKE_STATUS": state.get("status", "exited"),
                "FAKE_EXIT": state.get("exit_code", "0"),
                "FAKE_OOM": state.get("oom", "false"),
                "FAKE_ERROR": state.get("error", ""),
                "FAKE_FINISHED": state.get("finished", "2026-07-31T12:00:00Z"),
                "FAKE_INSPECT_RC": state.get("inspect_rc", "0"),
            }
        )
        return subprocess.run(["sh", str(program)], env=env, capture_output=True, text=True).returncode


@pytest.mark.parametrize(
    ("state", "accepted"),
    [
        ({}, True),  # exited, code 0, no OOM, no error
        ({"exit_code": "137"}, False),  # SIGKILL after the grace period
        ({"exit_code": "1"}, False),
        ({"oom": "true"}, False),
        ({"error": "oci runtime error"}, False),
        ({"status": "running"}, False),
        ({"status": "", "inspect_rc": "1"}, False),  # cannot read the state
    ],
    ids=["clean-exit", "sigkill-137", "exit-1", "oom-killed", "runtime-error", "still-running", "inspect-failed"],
)
def test_drain_is_judged_from_the_container_exit_state(state: dict[str, str], accepted: bool) -> None:
    """`docker stop` returns 0 even after SIGKILL, so only the state counts."""
    assert (_run_drain_check(**state) == 0) is accepted


def test_stop_helper_does_not_trust_the_docker_stop_exit_code() -> None:
    body = _extract_shell_function("stop_canary_and_verify_exit")
    assert 'docker stop -t 300 "$CANARY_ID"' in body
    # The state check runs regardless of how `docker stop` exited.
    assert "verify_container_drained" in body
    assert "CANARY_DRAIN_UNCERTAIN=1" in body


def test_drain_verification_reads_every_required_state_field() -> None:
    body = _extract_shell_function("verify_container_drained")
    for field in ("{{.State.Status}}", "{{.State.ExitCode}}", "{{.State.OOMKilled}}", "{{.State.Error}}"):
        assert field in body, f"drain verification ignores {field}"
    assert "{{.State.FinishedAt}}" in body


def test_uncertain_drain_never_removes_the_container_in_the_success_path() -> None:
    main = _main_flow()
    guard = main.index("if ! stop_canary_and_verify_exit; then")
    removal = main.index("remove_stopped_canary")
    assert guard < removal
    # The guard exits before reaching the removal.
    assert "exit 1" in main[guard:removal]
    body = _extract_shell_function("remove_stopped_canary")
    assert "Refusing to remove a running canary" in body


# ===========================================================================
# No global processing race once the regular worker is live
# ===========================================================================


def test_no_global_processing_gate_after_the_regular_worker_is_verified() -> None:
    """The regular worker legitimately holds rows in `processing`."""
    main = _main_flow()
    verified = main.index("REGULAR_WORKER_VERIFIED=1")
    removal = main.index("remove_stopped_canary")
    assert "processing_count" not in main[verified:removal]
    assert "REMAINING_PROCESSING" not in _deploy_script()


def test_global_processing_checks_only_happen_with_all_workers_stopped() -> None:
    """Every `processing_count` call sits behind a full-stop assertion."""
    script = _deploy_script()
    # Definition, the pre-migration echo, and the check inside the bounded
    # recovery helper (which itself refuses while any worker runs).
    assert script.count("processing_count") == 3
    helper = _between("recover_orphaned_processing_rows() {", "# THE single definition")
    assert "require_no_inbox_worker_running" in helper
    assert 'REMAINING="$(processing_count)"' in helper


# ===========================================================================
# Arbitrary one-off workers
# ===========================================================================


def test_status_resets_are_blocked_by_any_labelled_worker() -> None:
    """An operator's ad-hoc `compose run` worker must block every reset."""
    for helper in ("recover_orphaned_processing_rows", "recover_current_deploy_constraint_failures"):
        body = _extract_shell_function(helper)
        assert "require_no_inbox_worker_running" in body, f"{helper} can run while a worker is live"

    guard = _extract_shell_function("require_no_inbox_worker_running")
    assert "any_inbox_worker_running" in guard
    scan = _extract_shell_function("running_inbox_worker_ids")
    assert "docker ps -q" in scan
    assert "--filter" in scan


def test_downgrade_is_blocked_while_any_worker_runs() -> None:
    recovery = _recovery_body()
    downgrade = recovery.index('alembic downgrade "$PRE_PR3_REVISION"')
    assert recovery.index("require_no_inbox_worker_running") < downgrade
    assert recovery.index("recover_orphaned_processing_rows") < downgrade


# ===========================================================================
# Deploy boundary and bounded requeue
# ===========================================================================


def test_deploy_boundary_comes_from_postgresql_and_is_validated() -> None:
    script = _deploy_script()
    assert "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint;" in script
    assert 'DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar' in script
    # Non-empty, numeric, positive.
    assert "''|*[!0-9]*)" in script
    assert '[ "$DEPLOY_BOUNDARY_EPOCH_US" -le 0 ]' in script
    # Taken before the legacy worker is stopped.
    assert _main_index('DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar') < _main_index(
        'echo "🛑 Stopping the legacy altegio-inbox-worker'
    )


def test_constraint_predicate_is_bounded_by_the_deploy_boundary() -> None:
    predicate = _between("constraint_failure_predicate() {", "constraint_failure_count() {")
    assert "processed_at >= to_timestamp(${DEPLOY_BOUNDARY_EPOCH_US} / 1000000.0)" in predicate
    assert "now() - interval" not in _deploy_script()


def test_failed_requeue_touches_only_status_processed_at_and_error() -> None:
    body = _extract_shell_function("recover_current_deploy_constraint_failures")
    assert "UPDATE altegio_events SET status = 'received', processed_at = NULL, error = NULL" in body
    # Everything else stays exactly as it is.
    for untouched in ("payload", "received_at", "company_id", "resource", "dedupe_key"):
        assert f"{untouched} =" not in body, f"the requeue rewrites {untouched}"
    # Only `failed` rows, only after the boundary, only swap failures.
    assert "$(constraint_failure_predicate)" in body
    # And it proves there is nothing left.
    assert 'STILL_FAILED="$(constraint_failure_count)"' in body
    assert 'if [ "$STILL_FAILED" != "0" ]' in body


def test_failed_requeue_is_guarded_three_ways() -> None:
    body = _extract_shell_function("recover_current_deploy_constraint_failures")
    assert '[ "$PR3_TRANSITION" -ne 1 ]' in body
    assert '[ -z "$DEPLOY_BOUNDARY_EPOCH_US" ]' in body
    assert "require_no_inbox_worker_running" in body


def test_counts_and_requeue_share_one_predicate() -> None:
    """They must never drift: a count that matches more than the requeue fixes
    would fail the deploy over rows nothing recovers.
    """
    script = _deploy_script()
    assert script.count("$(constraint_failure_predicate)") == 2
    assert script.count("error LIKE '%does not exist%'") == 1


# ===========================================================================
# Recovery matrices
# ===========================================================================


def test_recovery_before_regular_verification_recovers_then_downgrades() -> None:
    # Scope to the tail of recover(): the two "already verified" branches above
    # legitimately mention the same helpers.
    full = _recovery_body()
    recovery = full[full.index('if [ "$OLD_WORKER_STOPPED" -eq 0 ]') :]
    order = [
        "stop_canary_and_verify_exit",
        "require_no_inbox_worker_running",
        "recover_orphaned_processing_rows",
        "recover_current_deploy_constraint_failures",
        'alembic downgrade "$PRE_PR3_REVISION"',
        'CURRENT_REVISION" != "$PRE_PR3_REVISION"',
        "remove_stopped_canary",
        "start_preserved_old_worker",
    ]
    positions = [recovery.index(marker) for marker in order]
    assert positions == sorted(positions), f"pre-verification recovery is out of order: {order}"


def test_recovery_after_regular_verification_repairs_without_downgrading() -> None:
    recovery = _recovery_body()
    branch = recovery[
        recovery.index('if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]') : recovery.index('if [ "$CANARY_VERIFIED" -eq 1 ]')
    ]
    assert "NO schema rollback" in branch
    assert "alembic downgrade" not in branch, "the after-verification branch must never roll back"
    order = [
        "$COMPOSE stop -t 300 altegio-inbox-worker",
        "verify_container_drained",
        "require_no_inbox_worker_running",
        "recover_orphaned_processing_rows",
        "recover_current_deploy_constraint_failures",
        "remove_stopped_canary",
        "$COMPOSE start altegio-inbox-worker",
    ]
    positions = [branch.index(marker) for marker in order]
    assert positions == sorted(positions), "after-verification recovery is out of order"
    # The restarted worker is re-verified on the same image.
    assert 'RESTARTED_IMAGE" = "$CANARY_IMAGE' in branch
    assert 'RESTARTED_ONEOFF" != "True' in branch
    assert "FAILED deploy" in branch


def test_after_verification_recovery_bails_out_if_the_regular_worker_wont_stop() -> None:
    recovery = _recovery_body()
    branch = recovery[recovery.index('if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]') :]
    bail = branch.index("Could not stop the regular worker")
    assert "No status reset, no canary removal, no rollback" in branch[bail : bail + 300]


def test_clean_canary_exit_short_circuits_the_after_verification_branch() -> None:
    """A deploy that failed later, with the canary already retired cleanly,
    must not stop the healthy regular worker for nothing.
    """
    recovery = _recovery_body()
    branch = recovery[recovery.index('if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]') :]
    assert '[ "$CANARY_DRAIN_UNCERTAIN" -eq 0 ] && [ -z "$CANARY_ID" ]' in branch
    assert "Canary already retired cleanly" in branch


# ===========================================================================
# The rollout program lives in a file, not in the workflow
# ===========================================================================


def test_deploy_script_exists_and_is_executable() -> None:
    import os
    import stat

    assert DEPLOY_SCRIPT_FILE.is_file(), f"missing {DEPLOY_SCRIPT_PATH}"
    mode = DEPLOY_SCRIPT_FILE.stat().st_mode
    assert mode & stat.S_IXUSR, f"{DEPLOY_SCRIPT_PATH} is not executable"
    assert os.access(DEPLOY_SCRIPT_FILE, os.X_OK)


def test_deploy_script_has_a_shebang_and_strict_mode() -> None:
    text = _deploy_script()
    assert text.startswith("#!/usr/bin/env bash\n")
    assert "set -Eeuo pipefail" in text.split("\n\n", 1)[0] or "set -Eeuo pipefail" in text[:2000]


def test_workflow_invokes_the_deploy_script() -> None:
    bootstrap = _bootstrap_script()
    assert f"exec bash {DEPLOY_SCRIPT_PATH}" in bootstrap


def test_inline_bootstrap_is_far_below_the_expression_limit() -> None:
    """The bug being fixed: an oversized `script:` invalidates the whole file.

    GitHub rejected the workflow outright, so lint, tests, Alembic checks and
    deploy all stopped running.
    """
    bootstrap = _bootstrap_script()
    assert len(bootstrap) < GITHUB_EXPRESSION_LIMIT // 10, (
        f"the inline bootstrap is {len(bootstrap)} characters; keep it small"
    )
    for step in _deploy_steps():
        script = str(step.get("with", {}).get("script", ""))
        assert len(script) < GITHUB_EXPRESSION_LIMIT, f"{step.get('name')!r} has a {len(script)}-character script input"


def test_the_rollout_program_is_not_inlined_anywhere_in_the_workflow() -> None:
    """No step may carry the deploy logic, and it must not be split into
    several large blocks either.
    """
    workflow_text = WORKFLOW_FILE.read_text()
    for marker in (
        "PR3_TRANSITION",
        "stop_canary_and_verify_exit",
        "recover_orphaned_processing_rows",
        "constraint_failure_predicate",
        "alembic downgrade",
        "verify_container_drained",
        "trap 'recover",
        "DEPLOY_BOUNDARY_EPOCH_US",
    ):
        assert marker not in workflow_text, f"deploy logic {marker!r} is still inlined in the workflow"

    # NOTE: the separate post-deploy verification step legitimately uses Alembic's
    # ScriptDirectory for its own smoke check. That step is small and is not the
    # rollout program, so it is deliberately not covered by the markers above.


def test_bootstrap_fetches_the_exact_commit_before_handing_over() -> None:
    """The script that runs must be the one this deploy is shipping."""
    bootstrap = _bootstrap_script()
    fetch = bootstrap.index("git fetch")
    reset = bootstrap.index('git reset --hard "$DEPLOY_SHA"')
    handover = bootstrap.index(f"exec bash {DEPLOY_SCRIPT_PATH}")
    assert fetch < reset < handover
    assert "set -Eeuo pipefail" in bootstrap


def test_deploy_sha_is_passed_as_an_environment_variable() -> None:
    """Keeping `${{ github.sha }}` out of the script block is what keeps the
    block from being treated as a template expression at all.
    """
    step = _bootstrap_step()
    assert step.get("env", {}).get("DEPLOY_SHA") == "${{ github.sha }}"
    assert "DEPLOY_SHA" in str(step.get("with", {}).get("envs", ""))
    assert "${{" not in _bootstrap_script(), "the bootstrap must contain no workflow expressions"
    assert "${{" not in _deploy_script(), "the script must contain no workflow expressions"


def test_deploy_script_verifies_the_sha_before_any_mutation() -> None:
    script = _deploy_script()
    assert 'if [ -z "${DEPLOY_SHA:-}" ]' in script
    assert 'DEPLOYED_SHA="$(git rev-parse HEAD)"' in script
    assert 'if [ "$DEPLOYED_SHA" != "$DEPLOY_SHA" ]' in script

    guard = script.index('if [ "$DEPLOYED_SHA" != "$DEPLOY_SHA" ]')
    for mutation in (
        "$COMPOSE --profile ops run --rm --no-deps migrate\n",
        "UPDATE altegio_events",
        f"$COMPOSE stop -t 300 {INBOX_SERVICE}",
        "$COMPOSE build",
    ):
        assert guard < script.index(mutation), f"{mutation!r} runs before the SHA is verified"


def test_deploy_script_is_syntactically_valid_bash() -> None:
    import subprocess

    result = subprocess.run(["bash", "-n", str(DEPLOY_SCRIPT_FILE)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed:\n{result.stderr}"


def test_safety_guarantees_survived_the_move() -> None:
    """A compact spot-check that the extraction dropped nothing.

    The detailed behaviour is asserted by the rest of this module; this is the
    one test that fails loudly if the script were ever replaced by a stub.
    """
    script = _deploy_script()
    for guarantee in (
        "PR3_TRANSITION=1",
        "PR3_TRANSITION_APPLIED=1",
        "CANARY_VERIFIED=1",
        "REGULAR_WORKER_VERIFIED=1",
        "verify_container_drained",
        "running_inbox_worker_ids",
        "require_no_inbox_worker_running",
        "recover_orphaned_processing_rows",
        "recover_current_deploy_constraint_failures",
        "constraint_failure_predicate",
        "DEPLOY_BOUNDARY_EPOCH_US",
        'alembic downgrade "$PRE_PR3_REVISION"',
        "trap 'recover $?' EXIT",
        "clock_timestamp()",
    ):
        assert guarantee in script, f"the extraction lost {guarantee!r}"
