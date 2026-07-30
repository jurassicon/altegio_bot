"""The Nginx webhook-log security suite must be a required CI gate.

``test_nginx_webhook_logging_integration.py`` needs Docker, and by default it
skips when Docker is unavailable. A skipped security test is indistinguishable
from a passing one in a green build, so documentation alone is not enough: the
required workflow has to run it in mandatory mode, in its own step, with no
``continue-on-error`` and no conditional guard.

The complementary half is that the general application suite must NOT run the
same Docker-dependent file again — it already ran under its own gate, and a
second container round is pure duplication.

These guards read the workflow, so a future edit that quietly drops the step or
the ``--ignore`` fails here instead of silently weakening the gate.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_FILE = _REPO_ROOT / ".github" / "workflows" / "ci_deploy.yml"

NGINX_GATE_ENV = "ALTEGIO_REQUIRE_NGINX_LOGTEST"
NGINX_SUITE = "src/altegio_bot/tests/test_nginx_webhook_logging_integration.py"
MIGRATION_SUITE = "src/altegio_bot/tests/test_easyweek_migration_integration.py"


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_FILE.read_text())


def _jobs() -> dict[str, Any]:
    return _workflow()["jobs"]


def _steps(job_name: str) -> list[dict[str, Any]]:
    return [step for step in _jobs()[job_name].get("steps", []) if isinstance(step, dict)]


def _runs_suite(step: dict[str, Any], suite: str) -> bool:
    """True when the step actually executes *suite* (an ``--ignore=`` is not a run)."""
    command = str(step.get("run", ""))
    remaining = command
    executed = False
    while suite in remaining:
        index = remaining.index(suite)
        if not remaining[:index].endswith("--ignore="):
            executed = True
        remaining = remaining[index + len(suite) :]
    return executed


def _ignores_suite(step: dict[str, Any], suite: str) -> bool:
    return f"--ignore={suite}" in str(step.get("run", ""))


def _nginx_gate_steps() -> list[dict[str, Any]]:
    return [step for step in _steps("tests") if NGINX_GATE_ENV in (step.get("env") or {})]


# ===========================================================================
# The workflow itself is the required one
# ===========================================================================


def test_workflow_file_exists() -> None:
    assert WORKFLOW_FILE.is_file(), f"missing required workflow: {WORKFLOW_FILE}"


def test_workflow_is_valid_yaml_with_a_tests_job() -> None:
    workflow = _workflow()
    assert isinstance(workflow, dict), "the workflow must parse into a mapping"
    assert "tests" in workflow["jobs"], "the required test job is gone"
    # `on:` is parsed by PyYAML 1.1 semantics as the boolean True.
    triggers = workflow.get("on", workflow.get(True))
    assert "push" in triggers, "the workflow must still run on push"


def test_deploy_depends_on_the_tests_job() -> None:
    """The gate is only meaningful if a red `tests` job blocks the deploy."""
    deploy = _jobs()["deploy"]
    assert "tests" in deploy["needs"]


# ===========================================================================
# The mandatory Nginx step
# ===========================================================================


def test_a_dedicated_mandatory_nginx_step_exists() -> None:
    gate_steps = _nginx_gate_steps()
    assert len(gate_steps) == 1, f"expected exactly one {NGINX_GATE_ENV} step, found {len(gate_steps)}"


def test_nginx_gate_env_is_exactly_one() -> None:
    step = _nginx_gate_steps()[0]
    assert step["env"][NGINX_GATE_ENV] == "1", "the gate is only active when the flag is the string '1'"


def test_nginx_gate_step_runs_the_integration_suite() -> None:
    step = _nginx_gate_steps()[0]
    assert _runs_suite(step, NGINX_SUITE), "the mandatory step does not execute the Nginx integration suite"


def test_nginx_gate_step_cannot_be_softened() -> None:
    """No continue-on-error, no conditional skip — a failure must be red."""
    step = _nginx_gate_steps()[0]
    assert "continue-on-error" not in step, "continue-on-error would make the security gate advisory"
    assert "if" not in step, "a conditional guard would let the security gate be skipped"


def test_nginx_gate_job_has_no_job_level_softening() -> None:
    job = _jobs()["tests"]
    assert "continue-on-error" not in job
    assert "if" not in job


# ===========================================================================
# The general suite excludes the Docker-dependent file
# ===========================================================================


def test_general_pytest_step_ignores_the_nginx_suite() -> None:
    ignoring = [step for step in _steps("tests") if _ignores_suite(step, NGINX_SUITE)]
    assert ignoring, f"the general pytest step must pass --ignore={NGINX_SUITE}"


def test_nginx_suite_is_executed_exactly_once() -> None:
    """Anything else means the container work runs twice in one job."""
    executing = [step for step in _steps("tests") if _runs_suite(step, NGINX_SUITE)]
    assert len(executing) == 1, f"the Nginx suite is executed by {len(executing)} steps"
    assert NGINX_GATE_ENV in (executing[0].get("env") or {}), "the only execution must be the mandatory one"


@pytest.mark.parametrize("suite", [NGINX_SUITE, MIGRATION_SUITE])
def test_both_docker_dependent_suites_keep_their_own_gate(suite: str) -> None:
    """The migration gate must not be lost while adding the Nginx one."""
    executing = [step for step in _steps("tests") if _runs_suite(step, suite)]
    assert len(executing) == 1, f"{suite} must be executed by exactly one dedicated step"
    assert executing[0].get("env"), f"{suite} must run under a mandatory env flag"
    ignoring = [step for step in _steps("tests") if _ignores_suite(step, suite)]
    assert ignoring, f"the general pytest step must --ignore {suite}"


def test_runs_suite_helper_does_not_confuse_ignore_with_execution() -> None:
    """Guard the guard: ``--ignore=<path>`` must not read as "runs the suite"."""
    assert not _runs_suite({"run": f"uv run pytest -q --ignore={NGINX_SUITE}"}, NGINX_SUITE)
    assert _runs_suite({"run": f"uv run pytest -q {NGINX_SUITE}"}, NGINX_SUITE)
    assert _runs_suite(
        {"run": f"uv run pytest -q --ignore={MIGRATION_SUITE} {NGINX_SUITE}"},
        NGINX_SUITE,
    )
    assert not _runs_suite({"run": "uv run pytest -q"}, NGINX_SUITE)
