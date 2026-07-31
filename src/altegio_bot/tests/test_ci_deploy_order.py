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
    """The SSH script that performs the actual rollout."""
    for step in _deploy_steps():
        script = str(step.get("with", {}).get("script", ""))
        if "$COMPOSE build" in script:
            return script
    raise AssertionError("the deploy job has no build/rollout SSH script")


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


# Ordered phases of the rollout, each identified by a statement that can only
# belong to that phase and only appears in the main flow.
_PHASES = (
    ("build", "$COMPOSE build"),
    ("backup", "pg_dump"),
    ("transition classification", 'SCRIPT_FACTS="$(alembic_script_facts "$REVISION_BEFORE")"'),
    ("legacy worker stop", f"$COMPOSE stop -t 300 {INBOX_SERVICE}"),
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


def test_the_block_happens_before_any_schema_change() -> None:
    """The multi-revision refusal must precede the migration."""
    assert _index("Deploy PR-3 on its own first") < _index("$COMPOSE --profile ops run --rm --no-deps migrate\n")


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
    assert "no schema rollback" in verified_branch[:600]
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
    assert "set -euo pipefail" in script
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
    stop = main.index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
    confirmed = main.index("if any_inbox_worker_running; then")
    reset = main.index("if ! recover_orphaned_processing_rows; then")
    migration = main.index("$COMPOSE --profile ops run --rm --no-deps migrate\n")
    assert stop < confirmed < reset < migration


def test_orphan_reset_is_bounded_and_guarded() -> None:
    script = _deploy_script()
    body = _between("recover_orphaned_processing_rows() {", "constraint_failure_count() {")
    # Refuses while any worker still runs.
    assert "if any_inbox_worker_running; then" in body
    assert "refusing to touch event statuses" in body
    # Only claimed-but-unfinished rows.
    assert "WHERE status = 'processing' AND processed_at IS NULL" in body
    # And it proves the queue is empty afterwards.
    assert "SELECT count(*) FROM altegio_events WHERE status = 'processing';" in script
    assert 'if [ "$REMAINING" != "0" ]' in body


def test_orphan_reset_is_never_an_unconditional_deploy_step() -> None:
    """A blanket reset on every deploy would destroy in-flight work."""
    script = _deploy_script()
    assert "UPDATE altegio_events SET status = 'received' WHERE status = 'processing';" not in script
    # The only UPDATE is the bounded one inside the helper.
    assert script.count("UPDATE altegio_events") == 1
    main = _main_flow()
    guard = main.index('if [ "$PR3_TRANSITION" -eq 1 ]; then')
    assert guard < main.index("if ! recover_orphaned_processing_rows; then")


def test_orphan_reset_logs_only_a_count() -> None:
    body = _between("recover_orphaned_processing_rows() {", "constraint_failure_count() {")
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


def test_regular_lookup_never_falls_back_to_a_service_label_search() -> None:
    """A label search matches the one-off canary too."""
    script = _deploy_script()
    assert "label=com.docker.compose.service=altegio-inbox-worker" not in script


def test_canary_and_regular_have_separate_verification_flags() -> None:
    script = _deploy_script()
    assert "CANARY_VERIFIED=0" in script and "CANARY_VERIFIED=1" in script
    assert "REGULAR_WORKER_VERIFIED=0" in script and "REGULAR_WORKER_VERIFIED=1" in script
    assert "NEW_WORKER_VERIFIED" not in script, "the merged flag must be gone"
    main = _main_flow()
    assert main.index("CANARY_VERIFIED=1") < main.index("REGULAR_WORKER_VERIFIED=1")


def test_canary_is_removed_only_after_the_regular_worker_is_verified() -> None:
    main = _main_flow()
    assert main.index("REGULAR_WORKER_VERIFIED=1") < main.index('docker rm "$CANARY_ID"')


def test_canary_is_drained_and_the_queue_checked_before_removal() -> None:
    main = _main_flow()
    drain = main.index("if ! stop_canary_gracefully; then")
    queue = main.index('REMAINING_PROCESSING="$(processing_count)"')
    removal = main.index('docker rm "$CANARY_ID"')
    assert drain < queue < removal
    body = _between("stop_canary_gracefully() {", "processing_count() {")
    assert 'docker stop -t 300 "$CANARY_ID"' in body


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


def test_constraint_probe_matches_only_the_pr3_constraint_names() -> None:
    script = _deploy_script()
    body = _between("constraint_failure_count() {", "start_preserved_old_worker() {")
    for name in CONSTRAINT_NAMES:
        assert name in body, f"the probe does not look for {name!r}"
    # A bare missing-object phrase would match unrelated errors, so the SQL must
    # never use one on its own.
    assert "does not exist" not in body
    assert "LIKE '%does not exist%'" not in script


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
    assert _index("trap 'recover $?' EXIT") < _index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
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
    assert 'git reset --hard "${{ github.sha }}"' in script
    assert "pg_dump" in script
    assert "Pre-deploy dump is empty" in script
    names = [step.get("name") for step in _deploy_steps()]
    assert "Wait for service stabilization" in names
    assert "Verify deployment on server" in names
    assert "Verify public HTTPS health endpoint" in names
    assert "Notify via Telegram" in names
