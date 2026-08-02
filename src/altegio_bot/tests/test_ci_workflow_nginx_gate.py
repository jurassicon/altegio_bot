"""The Nginx webhook-log security suite must be a required CI gate.

``test_nginx_webhook_logging_integration.py`` needs Docker, and by default it
skips when Docker is unavailable. A skipped security test is indistinguishable
from a passing one in a green build, so documentation alone is not enough: the
required workflow has to run it in mandatory mode, in its own step, with no
``continue-on-error`` and no conditional guard.

The complementary half is that the general application suite must NOT run the
same Docker-dependent file again — it already ran under its own gate, and a
second container round is pure duplication.

This file also pins the push/deploy split: ``lint``/``alembic``/``tests`` must
run only for pull requests (branch protection already required them to pass
before merge), ``deploy`` must run straight off ``push``/``workflow_dispatch``
against ``refs/heads/main`` without re-depending on those jobs, production
deploys must serialize instead of being cancelled mid-flight, and the Telegram
notification must live inside ``deploy`` instead of a separate runner.

These guards read the workflow, so a future edit that quietly drops a step,
re-introduces a duplicate CI run, or softens a gate fails here instead of
silently weakening the pipeline.
"""

from __future__ import annotations

import re
import shlex
from collections import Counter
from dataclasses import dataclass
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


@dataclass(frozen=True)
class _DeployPolicy:
    events: frozenset[str]
    ref: str


_REQUIRED_PR_TYPES = {"opened", "synchronize", "reopened"}
_ALLOWED_DEPLOY_EVENTS = frozenset({"push", "workflow_dispatch"})
_MAIN_REF = "refs/heads/main"
_EXPECTED_DEPLOY_POLICY = _DeployPolicy(events=_ALLOWED_DEPLOY_EVENTS, ref=_MAIN_REF)
_REQUIRED_PR_JOB_IF = "github.event_name == 'pull_request'"
_REQUIRED_PR_JOB_IF_AFTER_LINT = f"{_REQUIRED_PR_JOB_IF} && needs.lint.result == 'success'"
_CONDITION_EQUALITY_RE = re.compile(
    r"""(?P<left>[A-Za-z_][A-Za-z0-9_.]*)\s*==\s*"""
    r"""'(?P<right>[^']+)'"""
)
_DEPLOY_ALLOWLIST_RE = re.compile(
    r"^\(\s*(?P<events>[^()]*)\s*\)\s*&&\s*(?P<ref>[A-Za-z_][A-Za-z0-9_.]*\s*==\s*'[^']+')$"
)
_GITHUB_CONCURRENCY_CONTEXT_RE = re.compile(r"\$\{\{\s*github\.(?P<name>workflow|event_name|ref)\s*\}\}")
_GROUP_EXPRESSION_RE = re.compile(
    r"""^github\.ref\s*==\s*'refs/heads/main'\s*&&\s*"""
    r"""format\('(?P<main_template>[^']*)',\s*github\.workflow\)\s*\|\|\s*"""
    r"""format\('(?P<other_template>[^']*)',\s*github\.workflow,\s*github\.event_name,\s*github\.ref\)$"""
)


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


def _ssh_script(step: dict[str, Any]) -> str:
    return str(step.get("with", {}).get("script", ""))


def _step_shell_text(step: dict[str, Any]) -> str:
    return f"{step.get('run', '')}\n{_ssh_script(step)}"


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


def _condition_source(condition: Any) -> str | None:
    """Unwrap and normalise the narrow GitHub condition syntax we accept."""
    if not isinstance(condition, str):
        return None
    source = condition.strip()
    if source.startswith("${{") and source.endswith("}}"):
        source = source[3:-2].strip()
    source = " ".join(source.split())
    if not source or "#" in source:
        return None
    return source


def _equality_atoms(source: str, *, separator: str) -> Counter[tuple[str, str]] | None:
    """Parse only ``identifier == 'literal'`` atoms separated by one operator."""
    atoms: Counter[tuple[str, str]] = Counter()
    for raw_atom in source.split(separator):
        atom = raw_atom.strip()
        match = _CONDITION_EQUALITY_RE.fullmatch(atom)
        if match is None:
            return None
        atoms[(match.group("left"), match.group("right"))] += 1
    return atoms


def _parse_deploy_policy(condition: Any) -> _DeployPolicy | None:
    """Parse only ``(event || event) && ref``."""
    source = _condition_source(condition)
    if source is None:
        return None
    match = _DEPLOY_ALLOWLIST_RE.fullmatch(source)
    if match is None:
        return None
    event_atoms = _equality_atoms(match.group("events"), separator="||")
    if event_atoms is None:
        return None
    if any(identifier != "github.event_name" or count != 1 for (identifier, _), count in event_atoms.items()):
        return None

    ref_match = _CONDITION_EQUALITY_RE.fullmatch(match.group("ref").strip())
    if ref_match is None or ref_match.group("left") != "github.ref":
        return None

    return _DeployPolicy(
        events=frozenset(value for (_, value) in event_atoms),
        ref=ref_match.group("right"),
    )


def _deploy_condition_matches_contract(condition: Any) -> bool:
    return _parse_deploy_policy(condition) == _EXPECTED_DEPLOY_POLICY


def deploy_allowed(*, policy: _DeployPolicy, event_name: str, ref: str) -> bool:
    """Evaluate an artificial GitHub context against the parsed YAML policy."""
    return event_name in policy.events and ref == policy.ref


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


def _render_deploy_group(template: Any, *, workflow: str, event_name: str, ref: str) -> str:
    """Evaluate the narrow ``ref == main ? main-template : other-template`` grammar we emit."""
    source = _condition_source(template)
    assert source is not None, "concurrency group must be a non-empty condition"
    match = _GROUP_EXPRESSION_RE.fullmatch(source)
    assert match is not None, f"unexpected concurrency group expression shape: {source!r}"
    if ref == "refs/heads/main":
        return match.group("main_template").format(workflow)
    return match.group("other_template").format(workflow, event_name, ref)


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


# ===========================================================================
# lint / alembic / tests only run for pull requests
# ===========================================================================


@pytest.mark.parametrize("job_name", ["lint", "tests", "alembic"])
def test_required_jobs_are_not_softened(job_name: str) -> None:
    job = _jobs()[job_name]
    assert "continue-on-error" not in job, f"{job_name} is advisory instead of required"


def test_lint_runs_only_for_pull_requests() -> None:
    assert _condition_source(_jobs()["lint"].get("if")) == _REQUIRED_PR_JOB_IF


@pytest.mark.parametrize("job_name", ["alembic", "tests"])
def test_alembic_and_tests_run_only_for_pull_requests_after_successful_lint(job_name: str) -> None:
    """Event-scoped, but still gated behind a green lint so runners aren't wasted."""
    assert _needs(job_name) == {"lint"}
    assert _condition_source(_jobs()[job_name].get("if")) == _REQUIRED_PR_JOB_IF_AFTER_LINT


def test_push_and_dispatch_do_not_allocate_runners_for_pr_only_jobs() -> None:
    """A job-level ``if`` skips the job before a runner is assigned — no checkout, no services."""
    for job_name in ("lint", "alembic", "tests"):
        job = _jobs()[job_name]
        condition = _condition_source(job.get("if"))
        assert condition is not None
        assert "github.event_name == 'pull_request'" in condition


# ===========================================================================
# Concurrency: PR runs can be cancelled, production deploys cannot
# ===========================================================================


def test_pull_request_runs_can_still_be_cancelled_by_a_newer_commit() -> None:
    concurrency = _workflow()["concurrency"]
    assert _condition_source(concurrency["cancel-in-progress"]) == "github.ref != 'refs/heads/main'"


def test_main_deploys_share_one_group_and_are_never_cancelled() -> None:
    group_template = _workflow()["concurrency"]["group"]

    push_main = _render_deploy_group(group_template, workflow="CI / Deploy", event_name="push", ref="refs/heads/main")
    dispatch_main = _render_deploy_group(
        group_template, workflow="CI / Deploy", event_name="workflow_dispatch", ref="refs/heads/main"
    )
    pr10 = _render_deploy_group(
        group_template, workflow="CI / Deploy", event_name="pull_request", ref="refs/pull/10/merge"
    )
    pr11 = _render_deploy_group(
        group_template, workflow="CI / Deploy", event_name="pull_request", ref="refs/pull/11/merge"
    )
    other_workflow_push_main = _render_deploy_group(
        group_template, workflow="Other workflow", event_name="push", ref="refs/heads/main"
    )

    # Both trigger types that can reach production must serialize in one group.
    assert push_main == dispatch_main
    # Different pull requests, and a different workflow, must stay independent.
    assert len({push_main, pr10, pr11, other_workflow_push_main}) == 4


# ===========================================================================
# deploy no longer re-runs CI, and only targets refs/heads/main
# ===========================================================================


def test_deploy_has_no_needs_on_repeated_ci_jobs() -> None:
    assert "needs" not in _jobs()["deploy"]


def test_deploy_condition_matches_context_contract() -> None:
    deploy = _jobs()["deploy"]
    assert _parse_deploy_policy(deploy.get("if")) == _EXPECTED_DEPLOY_POLICY


@pytest.mark.parametrize(
    ("event_name", "ref", "expected"),
    [
        ("push", "refs/heads/main", True),
        ("workflow_dispatch", "refs/heads/main", True),
        ("pull_request", "refs/pull/123/merge", False),
        ("pull_request", "refs/heads/main", False),
        ("push", "refs/heads/feature", False),
        ("workflow_dispatch", "refs/heads/feature", False),
        ("workflow_dispatch", "refs/tags/v1", False),
        ("schedule", "refs/heads/main", False),
    ],
)
def test_deploy_context_contract(event_name: str, ref: str, expected: bool) -> None:
    policy = _parse_deploy_policy(_jobs()["deploy"].get("if"))
    assert policy is not None
    assert deploy_allowed(policy=policy, event_name=event_name, ref=ref) is expected


@pytest.mark.parametrize(
    "condition",
    [
        ("(github.event_name == 'push' || github.event_name == 'pull_request') && github.ref == 'refs/heads/main'"),
        (
            "(github.event_name == 'push' || "
            "github.event_name == 'workflow_dispatch') && "
            "github.ref == 'refs/heads/main-feature'"
        ),
        ("(github.event_name != 'pull_request') && github.ref == 'refs/heads/main'"),
        ("(github.event_name == 'push' || github.event_name == 'workflow_dispatch') && github.ref contains 'main'"),
        (
            "(github.event_name == 'workflow_dispatch') && "
            "github.ref == 'refs/heads/main' && "
            "needs.tests.result == 'success'"
        ),
        (
            "(github.event_name == 'push' || "
            "github.event_name == 'workflow_dispatch') && "
            "github.ref == 'refs/heads/feature'\n"
            "# github.ref == 'refs/heads/main'"
        ),
        (
            "echo \"(github.event_name == 'push' || "
            "github.event_name == 'workflow_dispatch') && "
            "github.ref == 'refs/heads/main'\""
        ),
        ("(github.event_name == 'push') && github.ref == 'refs/heads/main'"),
        (
            "github.event_name == 'push' || "
            "(github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main')"
        ),
        (
            '(github.event_name == "push" || '
            'github.event_name == "workflow_dispatch") && '
            'github.ref == "refs/heads/main"'
        ),
        (
            ")github.event_name == 'push' || "
            "github.event_name == 'workflow_dispatch'( && "
            "github.ref == 'refs/heads/main'"
        ),
        (
            "(github.event_name == 'push' || "
            "github.event_name == 'workflow_dispatch' || "
            "github.event_name == 'schedule') && "
            "github.ref == 'refs/heads/main'"
        ),
        ("(github.event_name == 'push' || github.event_name == 'push') && github.ref == 'refs/heads/main'"),
    ],
)
def test_deploy_condition_parser_rejects_unsafe_guards(condition: str) -> None:
    assert not _deploy_condition_matches_contract(condition)


def test_deploy_condition_parser_accepts_safe_allowlist() -> None:
    condition = """${{
        (
            github.event_name == 'workflow_dispatch' ||
            github.event_name == 'push'
        ) &&
        github.ref == 'refs/heads/main'
    }}"""
    assert _deploy_condition_matches_contract(condition)


def test_deploy_job_and_step_cannot_be_softened() -> None:
    deploy = _jobs()["deploy"]
    assert "continue-on-error" not in deploy
    for step in _steps("deploy"):
        assert "continue-on-error" not in step


# ===========================================================================
# Deploy pins the exact workflow SHA, not whatever main has drifted to
# ===========================================================================


def test_deploy_resets_to_the_exact_workflow_sha() -> None:
    """The commit is still pinned exactly — now via an env var.

    The rollout program moved to scripts/deploy_pr3.sh because an inline
    `script:` is one template expression capped at 21000 characters. Keeping
    `${{ github.sha }}` out of that block is part of the same fix, so the SHA
    now arrives as DEPLOY_SHA.
    """
    step = _steps("deploy")[0]
    script = _ssh_script(step)
    assert step.get("env", {}).get("DEPLOY_SHA") == "${{ github.sha }}"
    assert "DEPLOY_SHA" in str(step.get("with", {}).get("envs", ""))
    assert 'git reset --hard "$DEPLOY_SHA"' in script
    assert "git reset --hard origin/main" not in script


def test_deploy_verifies_the_reset_landed_on_the_expected_sha() -> None:
    """Verification lives in the script, before anything is mutated."""
    deploy_script = (_REPO_ROOT / "scripts" / "deploy_pr3.sh").read_text()
    assert 'DEPLOYED_SHA="$(git rev-parse HEAD)"' in deploy_script
    assert 'if [ "$DEPLOYED_SHA" != "$DEPLOY_SHA" ]' in deploy_script
    assert 'if [ -z "${DEPLOY_SHA:-}" ]' in deploy_script


# ===========================================================================
# Stabilization wait and post-deploy verification
# ===========================================================================


def test_deploy_step_order() -> None:
    names = [step.get("name") for step in _steps("deploy")]
    assert names == [
        "Deploy to server via SSH",
        "Wait for service stabilization",
        "Verify deployment on server",
        "Verify public HTTPS health endpoint",
        "Notify via Telegram",
    ]


def test_stabilization_wait_is_exactly_sixty_seconds() -> None:
    wait_step = _steps("deploy")[1]
    assert _normalized_run(wait_step) == "sleep 60"
    assert "if" not in wait_step
    assert "continue-on-error" not in wait_step
    assert "uses" not in wait_step


_EXPECTED_CRITICAL_SERVICES = frozenset(
    {
        "postgres",
        "redis",
        "altegio-api",
        "altegio-inbox-worker",
        "altegio-outbox-worker",
        "altegio-whatsapp-inbox-worker",
        "altegio-meta-guard-worker",
        "altegio-campaign-worker",
        "altegio-followup-worker",
        # PR-4: the EasyWeek normalizer is a standing service and must be
        # verified after every deploy like any other worker.
        "altegio-easyweek-inbox-worker",
    }
)


def _critical_services(script: str) -> frozenset[str]:
    """Pull the ``CRITICAL_SERVICES="..."`` bash list out of the verification script."""
    match = re.search(r'CRITICAL_SERVICES="\n(?P<body>.*?)\n"', script, re.DOTALL)
    assert match is not None, "verification script must define CRITICAL_SERVICES"
    return frozenset(line.strip() for line in match.group("body").splitlines() if line.strip())


def test_server_verification_step_checks_revision_containers_db_and_cache() -> None:
    step = _steps("deploy")[2]
    assert step["name"] == "Verify deployment on server"
    script = _ssh_script(step)
    assert "set -euo pipefail" in script
    assert 'test "$DEPLOYED_SHA" = "${{ github.sha }}"' in script
    assert _critical_services(script) == _EXPECTED_CRITICAL_SERVICES, (
        "migrate has profile 'ops' and is not a standing service — it must not be in this list"
    )
    assert "redis-cli ping" in script
    assert 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1"' in script
    assert "get_heads" in script
    assert "get_current_heads" in script
    assert "http://127.0.0.1:8000/health" in script


def test_server_verification_step_never_leaks_secrets() -> None:
    script = _ssh_script(_steps("deploy")[2])
    for forbidden in ("cat .env", "source .env", "printenv", "eval ", " env\n", "set -x"):
        assert forbidden not in script


def test_public_health_check_runs_on_the_runner_not_over_ssh() -> None:
    step = _steps("deploy")[3]
    assert "uses" not in step, "the public check must run on the GitHub runner, not over SSH"
    assert step.get("env", {}).get("PUBLIC_HEALTH_URL") == "https://api.kitilash.com/health"
    run = str(step.get("run", ""))
    assert "curl -k" not in run
    assert "--insecure" not in run
    assert "PUBLIC_HEALTH_URL" in run
    assert "set -euo pipefail" in run


# ===========================================================================
# Notification moved into the deploy job; no separate runner
# ===========================================================================


def test_no_separate_notify_job() -> None:
    assert "notify" not in _jobs()


def test_telegram_notification_is_the_final_deploy_step_and_always_runs() -> None:
    telegram_step = _steps("deploy")[-1]
    assert telegram_step["name"] == "Notify via Telegram"
    assert str(telegram_step.get("uses", "")).startswith("appleboy/telegram-action")
    assert _condition_source(telegram_step.get("if")) == "always()"
    assert "continue-on-error" not in telegram_step


def test_telegram_message_reports_repo_branch_sha_status_and_run_url() -> None:
    telegram_step = _steps("deploy")[-1]
    message = str(telegram_step["with"]["message"])
    assert "${{ github.repository }}" in message
    assert "${{ github.ref_name }}" in message
    assert "${{ github.sha }}" in message
    assert "${{ job.status }}" in message
    assert "${{ github.run_id }}" in message


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
