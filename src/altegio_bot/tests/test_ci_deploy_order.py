"""The production deploy must be ordered so the PR-3 constraint swap is safe.

The migration renames the unique constraints that the inbox worker pins by name
in ``ON CONFLICT ON CONSTRAINT``. A worker from the previous release that keeps
running across the swap turns every event it touches into a ``failed`` row that
nothing retries. The ordering that prevents this is not a style preference, it
is the fix — so it is asserted here as an ORDER, not as a set of greps:

    build < backup < graceful inbox stop < processing-count check < migration
          < new-worker verification < general `compose up`

Also pinned: altegio-api is never stopped (webhooks keep landing as
``received``), the stop timeout is non-zero, recovery is declared before
anything is stopped, the rollback targets an exact revision, and the old
container survives until a new-image worker has been proven.
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


# Ordered phases of the rollout, each identified by a statement that can only
# belong to that phase.
_PHASES = (
    ("build", "$COMPOSE build"),
    ("backup", "pg_dump"),
    # The assignment, not the `REVISION_BEFORE=""` initialiser that sits in the
    # phase-flag block above the trap.
    ("record revision", 'REVISION_BEFORE="$(alembic_revision)"'),
    ("graceful inbox stop", f"$COMPOSE stop -t 300 {INBOX_SERVICE}"),
    ("processing-count check", "PROCESSING_COUNT="),
    ("migration", "$COMPOSE --profile ops run --rm --no-deps migrate\n"),
    ("new worker verification", "CANARY_STATE="),
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
# The window itself
# ===========================================================================


def test_api_is_never_stopped_during_the_migration_window() -> None:
    """Webhooks must keep being accepted and pile up as `received`."""
    script = _deploy_script()
    for forbidden in (
        "$COMPOSE stop altegio-api",
        "$COMPOSE stop -t 300 altegio-api",
        "$COMPOSE down",
        "docker stop altegio-api",
    ):
        assert forbidden not in script, f"the deploy stops the API: {forbidden!r}"


def test_inbox_worker_stop_has_a_non_zero_timeout() -> None:
    """A zero timeout is an immediate kill and would strand the claimed batch."""
    match = re.search(rf"\$COMPOSE stop -t (\d+) {re.escape(INBOX_SERVICE)}", _deploy_script())
    assert match is not None, "the inbox worker is not stopped with an explicit timeout"
    assert int(match.group(1)) > 0


def test_no_forced_kill_of_the_inbox_worker() -> None:
    script = _deploy_script()
    for forbidden in ("docker kill", "$COMPOSE kill", "docker compose kill", "stop -t 0"):
        assert forbidden not in script, f"the deploy force-kills a container: {forbidden!r}"


def test_processing_count_gate_blocks_the_migration() -> None:
    """A non-empty drain must stop the deploy, not silently reset statuses."""
    script = _deploy_script()
    assert "FROM altegio_events WHERE status = 'processing'" in script
    gate = script[_index("PROCESSING_COUNT=") : _index("Applying DB Migrations")]
    assert 'if [ "$PROCESSING_COUNT" != "0" ]' in gate
    assert "exit 1" in gate
    # No automatic mass status reset.
    assert "UPDATE altegio_events SET status" not in script


def test_compose_gives_the_worker_time_to_drain() -> None:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    grace = compose["services"][INBOX_SERVICE].get("stop_grace_period")
    assert grace, f"{INBOX_SERVICE} has no stop_grace_period; Docker would kill the drain"
    assert str(grace) not in {"0", "0s"}


# ===========================================================================
# Recovery
# ===========================================================================


def test_recovery_is_declared_before_anything_is_stopped() -> None:
    script = _deploy_script()
    assert _index("trap 'recover $?' EXIT") < _index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
    assert "recover()" in script


def test_recovery_tracks_deploy_phases() -> None:
    script = _deploy_script()
    for flag in ("OLD_WORKER_STOPPED", "MIGRATION_APPLIED", "NEW_WORKER_VERIFIED"):
        assert f"{flag}=0" in script, f"{flag} is never initialised"
        assert f"{flag}=1" in script, f"{flag} is never set"


def test_rollback_targets_an_exact_revision_not_a_relative_step() -> None:
    """`downgrade -1` would silently shift meaning once another revision lands."""
    script = _deploy_script()
    assert f'PRE_PR3_REVISION="{PRE_PR3_REVISION}"' in script
    assert 'alembic downgrade "$PRE_PR3_REVISION"' in script
    assert "downgrade -1" not in script
    assert 'downgrade "-1"' not in script


def test_rollback_verifies_the_revision_before_restarting_the_old_worker() -> None:
    """Never assume the downgrade worked; never run the old image on new schema."""
    script = _deploy_script()
    recovery = script[_index("recover()") : _index("trap 'recover $?' EXIT")]
    assert 'if [ "$CURRENT_REVISION" != "$PRE_PR3_REVISION" ]' in recovery
    restart = recovery.index("$COMPOSE start altegio-inbox-worker")
    revision_guard = recovery.index('CURRENT_REVISION" != "$PRE_PR3_REVISION')
    assert revision_guard < restart, "the old worker is started before the revision is verified"


def test_migration_failure_is_not_masked() -> None:
    script = _deploy_script()
    assert "set -euo pipefail" in script
    # The migration is not "applied" until the revision is what PR-3 expects.
    assert 'REVISION_AFTER" != "$PR3_REVISION"' in script
    assert "migrate || " not in script
    assert "$COMPOSE --profile ops run --rm --no-deps migrate || true" not in script


def test_no_mandatory_operation_is_softened() -> None:
    """Scoped to the PR-3 window.

    The pre-existing Chatwoot check uses `|| true` on a purely diagnostic
    `docker inspect` inside a branch that then exits 1; that is not a mandatory
    operation and is out of scope here.
    """
    script = _deploy_script()
    window = script[_index("recover()") : _index("$COMPOSE up -d --remove-orphans")]
    assert "|| true" not in window, "a mandatory PR-3 deploy operation is allowed to fail silently"
    for step in _deploy_steps():
        assert "continue-on-error" not in step, f"{step.get('name')!r} has continue-on-error"
    assert "continue-on-error" not in _workflow()["jobs"]["deploy"]


# ===========================================================================
# New worker verification and the canary
# ===========================================================================


def test_the_old_container_survives_until_a_new_worker_is_proven() -> None:
    """`stop` keeps the old container; only after the canary is it recreated.

    This is what makes `$COMPOSE start altegio-inbox-worker` a real recovery
    rather than a no-op against a container that was already destroyed.
    """
    script = _deploy_script()
    stop_position = _index(f"$COMPOSE stop -t 300 {INBOX_SERVICE}")
    canary_verified = _index("NEW_WORKER_VERIFIED=1")
    recreate = _index(f"$COMPOSE up -d --force-recreate {INBOX_SERVICE}")
    assert stop_position < canary_verified < recreate, (
        "the old worker container is recreated before a new-image worker is verified"
    )
    # The drain must not use a command that destroys the container.
    assert f"$COMPOSE up -d --force-recreate {INBOX_SERVICE}" not in script[:canary_verified]
    assert f"$COMPOSE rm {INBOX_SERVICE}" not in script


def test_canary_runs_from_the_new_image_and_is_checked() -> None:
    script = _deploy_script()
    assert 'CANARY="altegio-inbox-worker-pr3-canary"' in script
    assert f'$COMPOSE run -d --no-deps --name "$CANARY" {INBOX_SERVICE}' in script
    assert "CANARY_STATE=" in script and "CANARY_RESTARTS=" in script
    assert 'if [ "$CANARY_STATE" != "running" ]' in script
    assert 'if [ "$CANARY_RESTARTS" != "0" ]' in script


def test_canary_is_removed_after_the_regular_worker_is_running() -> None:
    script = _deploy_script()
    regular_verified = _index('NEW_WORKER="$(inbox_worker_container)"')
    removal = _index('docker rm "$CANARY"')
    assert regular_verified < removal, "the canary is removed before the regular worker is verified"
    assert 'docker stop -t 300 "$CANARY"' in script, "the canary must be drained, not killed"


def test_new_worker_is_verified_before_the_general_rollout() -> None:
    assert _index('NEW_WORKER="$(inbox_worker_container)"') < _index("$COMPOSE up -d --remove-orphans")


def test_constraint_failures_are_detected_without_printing_payloads() -> None:
    script = _deploy_script()
    for marker in (
        "uq_clients_company_altegio_id",
        "uq_records_company_altegio_id",
        "uq_clients_provider_company_altegio_id",
        "uq_records_provider_company_altegio_id",
        "does not exist",
    ):
        assert marker in script, f"the constraint-failure probe does not look for {marker!r}"
    assert 'if [ "$CONSTRAINT_FAILURES" != "0" ]' in script
    # Only a count is selected; no payload/error/customer column is echoed.
    assert "SELECT payload" not in script
    assert 'echo "$CONSTRAINT_FAILURES_DETAIL' not in script


# ===========================================================================
# Nothing that already worked was dropped
# ===========================================================================


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
