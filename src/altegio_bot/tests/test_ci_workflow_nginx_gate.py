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

import re
import shlex
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_FILE = _REPO_ROOT / ".github" / "workflows" / "ci_deploy.yml"

NGINX_GATE_ENV = "ALTEGIO_REQUIRE_NGINX_LOGTEST"
MIGRATION_GATE_ENV = "ALTEGIO_REQUIRE_MIGTEST"
NGINX_SUITE = "src/altegio_bot/tests/test_nginx_webhook_logging_integration.py"
MIGRATION_SUITE = "src/altegio_bot/tests/test_easyweek_migration_integration.py"

_REQUIRED_PR_TYPES = {"opened", "synchronize", "reopened"}
_REQUIRED_DEPLOY_ATOMS = Counter(
    {
        ("github.event_name", "push"): 1,
        ("github.ref", "refs/heads/main"): 1,
        ("needs.tests.result", "success"): 1,
        ("needs.alembic.result", "success"): 1,
    }
)
_CONDITION_EQUALITY_RE = re.compile(
    r"""(?P<left>[A-Za-z_][A-Za-z0-9_.]*)\s*==\s*"""
    r"""'(?P<right>[^']+)'"""
)
_GITHUB_CONCURRENCY_CONTEXT_RE = re.compile(r"\$\{\{\s*github\.(?P<name>workflow|event_name|ref)\s*\}\}")


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_FILE.read_text())


def _workflow_triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    """Return triggers under either YAML 1.2 ``on`` or PyYAML 1.1 ``True``."""
    key: str | bool
    for key in ("on", True):
        if key in workflow:
            triggers = workflow[key]
            assert isinstance(triggers, dict), "workflow triggers must be a mapping"
            return triggers
    raise AssertionError("workflow has no trigger mapping")


def _triggers() -> dict[str, Any]:
    return _workflow_triggers(_workflow())


def _trigger_config(name: str) -> dict[str, Any]:
    config = _triggers()[name]
    if config is None:
        return {}
    assert isinstance(config, dict), f"{name} trigger must be a mapping or null"
    return config


def _string_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    assert isinstance(value, list), "expected a string or list"
    return {str(item) for item in value}


def _jobs() -> dict[str, Any]:
    return _workflow()["jobs"]


def _steps(job_name: str) -> list[dict[str, Any]]:
    return [step for step in _jobs()[job_name].get("steps", []) if isinstance(step, dict)]


def _normalized_run(step: dict[str, Any]) -> str:
    return " ".join(str(step.get("run", "")).split())


def _needs(job_name: str) -> set[str]:
    needs = _jobs()[job_name].get("needs", [])
    if isinstance(needs, str):
        return {needs}
    assert isinstance(needs, list), "job needs must be a string or list"
    return {str(item) for item in needs}


def _shell_commands(step: dict[str, Any]) -> list[list[str]]:
    """Tokenize simple workflow shell commands without trusting comments/echo."""
    script = str(step.get("run", "")).replace("\\\n", " ")
    commands: list[list[str]] = []
    for line in script.splitlines():
        lexer = shlex.shlex(line, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        lexer.commenters = "#"
        command: list[str] = []
        for token in lexer:
            if token in {";", "&&", "||", "|"}:
                if command:
                    commands.append(command)
                    command = []
            else:
                command.append(token)
        if command:
            commands.append(command)
    return commands


def _pytest_invocations(step: dict[str, Any]) -> list[list[str]]:
    return [command for command in _shell_commands(step) if command[:3] == ["uv", "run", "pytest"]]


def _ignored_suites(invocation: list[str]) -> set[str]:
    ignored: set[str] = set()
    arguments = invocation[3:]
    for index, argument in enumerate(arguments):
        if argument.startswith("--ignore="):
            ignored.add(argument.removeprefix("--ignore="))
        elif argument == "--ignore" and index + 1 < len(arguments):
            ignored.add(arguments[index + 1])
    return ignored


def _pytest_targets(invocation: list[str]) -> list[str]:
    """Return positional collection targets for the small CI pytest grammar."""
    targets: list[str] = []
    arguments = invocation[3:]
    options_with_values = {"--ignore", "-k", "-m", "--maxfail", "--rootdir", "-c"}
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "--":
            targets.extend(arguments[index + 1 :])
            break
        if argument in options_with_values:
            index += 2
            continue
        if argument.startswith("-"):
            index += 1
            continue
        targets.append(argument)
        index += 1
    return targets


def _target_covers_suite(target: str, suite: str) -> bool:
    target_path = target.split("::", 1)[0].rstrip("/")
    return target_path in {"", "."} or target_path == suite or suite.startswith(f"{target_path}/")


def _invocation_runs_suite(invocation: list[str], suite: str) -> bool:
    if suite in _ignored_suites(invocation):
        return False
    targets = _pytest_targets(invocation)
    return not targets or any(_target_covers_suite(target, suite) for target in targets)


def _runs_suite(step: dict[str, Any], suite: str) -> bool:
    """True when a real pytest invocation collects *suite*."""
    return any(_invocation_runs_suite(invocation, suite) for invocation in _pytest_invocations(step))


def _ignores_suite(step: dict[str, Any], suite: str) -> bool:
    return any(suite in _ignored_suites(invocation) for invocation in _pytest_invocations(step))


def _suite_executions(job_name: str, suite: str) -> list[tuple[dict[str, Any], list[str]]]:
    return [
        (step, invocation)
        for step in _steps(job_name)
        for invocation in _pytest_invocations(step)
        if _invocation_runs_suite(invocation, suite)
    ]


def _nginx_gate_steps() -> list[dict[str, Any]]:
    return [step for step in _steps("tests") if NGINX_GATE_ENV in (step.get("env") or {})]


def _migration_gate_steps() -> list[dict[str, Any]]:
    return [step for step in _steps("tests") if MIGRATION_GATE_ENV in (step.get("env") or {})]


def _condition_atoms(condition: Any) -> Counter[tuple[str, str]] | None:
    """Parse a strict conjunction of equality atoms used by the deploy guard."""
    if not isinstance(condition, str):
        return None
    source = condition.strip()
    if source.startswith("${{") and source.endswith("}}"):
        source = source[3:-2].strip()
    source = " ".join(source.split())
    if not source or any(forbidden in source for forbidden in ("||", "#", "(", ")")):
        return None

    atoms: Counter[tuple[str, str]] = Counter()
    for raw_atom in source.split("&&"):
        atom = raw_atom.strip()
        match = _CONDITION_EQUALITY_RE.fullmatch(atom)
        if match is None:
            return None
        atoms[(match.group("left"), match.group("right"))] += 1
    return atoms


def _deploy_condition_is_strict_main_push(condition: Any) -> bool:
    return _condition_atoms(condition) == _REQUIRED_DEPLOY_ATOMS


def _render_concurrency_group(
    template: Any,
    *,
    workflow: str,
    event_name: str,
    ref: str,
) -> str:
    values = {"workflow": workflow, "event_name": event_name, "ref": ref}
    rendered = _GITHUB_CONCURRENCY_CONTEXT_RE.sub(
        lambda match: values[match.group("name")],
        str(template),
    )
    assert "${{" not in rendered, "concurrency group contains an unsupported expression"
    return rendered


# ===========================================================================
# The workflow itself is the required one
# ===========================================================================


def test_workflow_file_exists() -> None:
    assert WORKFLOW_FILE.is_file(), f"missing required workflow: {WORKFLOW_FILE}"


def test_workflow_is_valid_yaml_with_a_tests_job() -> None:
    workflow = _workflow()
    assert isinstance(workflow, dict), "the workflow must parse into a mapping"
    assert "tests" in workflow["jobs"], "the required test job is gone"


def test_workflow_triggers_pull_requests_main_pushes_and_manual_runs() -> None:
    triggers = _triggers()
    assert {"pull_request", "push", "workflow_dispatch"} <= triggers.keys()
    assert _string_set(_trigger_config("push").get("branches")) == {"main"}


def test_pull_request_trigger_covers_open_update_and_reopen() -> None:
    pull_request = _trigger_config("pull_request")
    configured_types = _string_set(pull_request.get("types"))
    if configured_types:
        assert _REQUIRED_PR_TYPES <= configured_types
    configured_branches = _string_set(pull_request.get("branches"))
    if configured_branches:
        assert configured_branches == {"main"}
    assert "branches-ignore" not in pull_request


def test_security_workflow_has_no_path_filter_bypass() -> None:
    for event_name in ("pull_request", "push"):
        config = _trigger_config(event_name)
        assert "paths" not in config
        assert "paths-ignore" not in config


def test_trigger_helper_handles_pyyaml_boolean_on_key() -> None:
    string_triggers = {"push": {"branches": ["main"]}}
    boolean_triggers = {"pull_request": None}
    assert _workflow_triggers({"on": string_triggers}) is string_triggers
    assert _workflow_triggers({True: boolean_triggers}) is boolean_triggers
    assert "push" not in _workflow_triggers({True: boolean_triggers})


@pytest.mark.parametrize("job_name", ["lint", "tests", "alembic"])
def test_required_jobs_are_available_on_pull_requests(job_name: str) -> None:
    job = _jobs()[job_name]
    assert "if" not in job, f"{job_name} has a job guard that can exclude pull requests"
    assert "continue-on-error" not in job, f"{job_name} is advisory instead of required"


def test_pull_requests_cannot_cancel_main_or_other_pull_request_runs() -> None:
    concurrency = _workflow()["concurrency"]
    assert concurrency["cancel-in-progress"] is True
    template = concurrency["group"]
    rendered_groups = {
        _render_concurrency_group(
            template,
            workflow="CI / Deploy",
            event_name="push",
            ref="refs/heads/main",
        ),
        _render_concurrency_group(
            template,
            workflow="CI / Deploy",
            event_name="pull_request",
            ref="refs/pull/10/merge",
        ),
        _render_concurrency_group(
            template,
            workflow="CI / Deploy",
            event_name="pull_request",
            ref="refs/pull/11/merge",
        ),
        _render_concurrency_group(
            template,
            workflow="CI / Deploy",
            event_name="workflow_dispatch",
            ref="refs/heads/main",
        ),
        _render_concurrency_group(
            template,
            workflow="Other workflow",
            event_name="push",
            ref="refs/heads/main",
        ),
    }
    assert len(rendered_groups) == 5


def test_deploy_depends_on_tests_and_alembic() -> None:
    """Every security and schema gate must be green before deployment."""
    assert {"tests", "alembic"} <= _needs("deploy")


def test_deploy_is_only_a_successful_push_to_main() -> None:
    deploy = _jobs()["deploy"]
    assert _deploy_condition_is_strict_main_push(deploy.get("if"))


def test_deploy_job_and_step_cannot_be_softened() -> None:
    deploy = _jobs()["deploy"]
    assert "continue-on-error" not in deploy
    for step in _steps("deploy"):
        assert "continue-on-error" not in step


def test_notify_does_not_run_for_pull_requests() -> None:
    condition = " ".join(str(_jobs()["notify"].get("if", "")).split())
    assert condition == (
        "always() && ( "
        "(github.event_name == 'push' && github.ref == 'refs/heads/main') || "
        "github.event_name == 'workflow_dispatch' "
        ")"
    )


@pytest.mark.parametrize(
    "condition",
    [
        (
            "github.event_name == 'pull_request' && "
            "github.ref == 'refs/heads/main' && "
            "needs.tests.result == 'success' && "
            "needs.alembic.result == 'success'"
        ),
        (
            "github.event_name == 'push' && "
            "github.ref == 'refs/heads/main-feature' && "
            "needs.tests.result == 'success' && "
            "needs.alembic.result == 'success'"
        ),
        (
            "github.event_name == 'push' || "
            "github.event_name == 'pull_request' && "
            "github.ref == 'refs/heads/main' && "
            "needs.tests.result == 'success' && "
            "needs.alembic.result == 'success'"
        ),
        (
            "github.event_name == 'pull_request' && "
            "github.ref == 'refs/heads/feature'\n"
            "# github.event_name == 'push' && github.ref == 'refs/heads/main' && "
            "needs.tests.result == 'success' && needs.alembic.result == 'success'"
        ),
        (
            "github.event_name == 'push' && "
            "contains(github.ref, 'main') && "
            "needs.tests.result != 'failure' && "
            "needs.alembic.result == 'success'"
        ),
        (
            'github.event_name == "push" && '
            'github.ref == "refs/heads/main" && '
            'needs.tests.result == "success" && '
            'needs.alembic.result == "success"'
        ),
        (
            ")github.event_name == 'push'( && "
            "github.ref == 'refs/heads/main' && "
            "needs.tests.result == 'success' && "
            "needs.alembic.result == 'success'"
        ),
    ],
)
def test_deploy_condition_parser_rejects_unsafe_guards(condition: str) -> None:
    assert not _deploy_condition_is_strict_main_push(condition)


def test_deploy_condition_parser_accepts_wrapped_reordered_guard() -> None:
    condition = """${{
        needs.alembic.result == 'success' &&
        github.ref == 'refs/heads/main' &&
        github.event_name == 'push' &&
        needs.tests.result == 'success'
    }}"""
    assert _deploy_condition_is_strict_main_push(condition)


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
    assert _normalized_run(step) == f"uv run pytest -q {NGINX_SUITE}"
    invocations = _pytest_invocations(step)
    assert len(invocations) == 1
    assert _pytest_targets(invocations[0]) == [NGINX_SUITE]


def test_nginx_gate_step_cannot_be_softened() -> None:
    """No continue-on-error, no conditional skip — a failure must be red."""
    step = _nginx_gate_steps()[0]
    assert "continue-on-error" not in step, "continue-on-error would make the security gate advisory"
    assert "if" not in step, "a conditional guard would let the security gate be skipped"


def test_migration_gate_is_also_mandatory_and_dedicated() -> None:
    gate_steps = _migration_gate_steps()
    assert len(gate_steps) == 1
    step = gate_steps[0]
    assert step["env"][MIGRATION_GATE_ENV] == "1"
    assert "continue-on-error" not in step
    assert "if" not in step
    assert _normalized_run(step) == f"uv run pytest -q {MIGRATION_SUITE}"
    invocations = _pytest_invocations(step)
    assert len(invocations) == 1
    assert _pytest_targets(invocations[0]) == [MIGRATION_SUITE]


# ===========================================================================
# The general suite excludes the Docker-dependent file
# ===========================================================================


def test_general_pytest_step_ignores_both_docker_dependent_suites() -> None:
    general_invocations = [
        invocation
        for step in _steps("tests")
        for invocation in _pytest_invocations(step)
        if {NGINX_SUITE, MIGRATION_SUITE} <= _ignored_suites(invocation)
    ]
    assert len(general_invocations) == 1
    assert _pytest_targets(general_invocations[0]) == []


def test_nginx_suite_is_executed_exactly_once() -> None:
    """Anything else means the container work runs twice in one job."""
    executing = _suite_executions("tests", NGINX_SUITE)
    assert len(executing) == 1, f"the Nginx suite is executed {len(executing)} times"
    step, _ = executing[0]
    assert NGINX_GATE_ENV in (step.get("env") or {}), "the only execution must be the mandatory one"


@pytest.mark.parametrize("suite", [NGINX_SUITE, MIGRATION_SUITE])
def test_both_docker_dependent_suites_keep_their_own_gate(suite: str) -> None:
    """The migration gate must not be lost while adding the Nginx one."""
    executing = _suite_executions("tests", suite)
    assert len(executing) == 1, f"{suite} must be executed exactly once"
    step, _ = executing[0]
    assert step.get("env"), f"{suite} must run under a mandatory env flag"
    ignoring = [step for step in _steps("tests") if _ignores_suite(step, suite)]
    assert ignoring, f"the general pytest step must --ignore {suite}"


def test_runs_suite_helper_does_not_confuse_ignore_with_execution() -> None:
    """Guard the guard against ignored, echoed and commented commands."""
    assert not _runs_suite({"run": f"uv run pytest -q --ignore={NGINX_SUITE}"}, NGINX_SUITE)
    assert not _runs_suite({"run": f"uv run pytest -q --ignore {NGINX_SUITE}"}, NGINX_SUITE)
    assert _runs_suite({"run": f"uv run pytest -q {NGINX_SUITE}"}, NGINX_SUITE)
    assert _runs_suite(
        {"run": f"uv run pytest -q --ignore={MIGRATION_SUITE} {NGINX_SUITE}"},
        NGINX_SUITE,
    )
    assert _runs_suite({"run": "uv run pytest -q"}, NGINX_SUITE)
    assert not _runs_suite({"run": f'echo "uv run pytest -q {NGINX_SUITE}"'}, NGINX_SUITE)
    assert not _runs_suite({"run": f"# uv run pytest -q {NGINX_SUITE}"}, NGINX_SUITE)


def test_suite_execution_counter_counts_two_commands_in_one_step() -> None:
    step = {"run": (f"uv run pytest -q {NGINX_SUITE}\nuv run pytest -q {NGINX_SUITE}\n")}
    executions = [
        invocation for invocation in _pytest_invocations(step) if _invocation_runs_suite(invocation, NGINX_SUITE)
    ]
    assert len(executions) == 2
