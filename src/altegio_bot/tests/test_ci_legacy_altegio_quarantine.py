"""The legacy-Altegio quarantine must stay a quarantine, not a hole in CI.

Wave 1 withdrew 24 explicitly marked Altegio-only modules from the required
pull-request gate. That is only safe while four properties hold together, and
each of them is one careless edit away from silently disappearing:

1. a plain ``uv run pytest`` still collects everything, so the marker cannot
   quietly shrink what a developer runs locally;
2. exactly one CI invocation — the general one — filters the marker out, and
   the dedicated migration / Nginx / reminder-handover gates never do;
3. the required check keeps the job key and name that branch protection refers
   to, so the gate cannot be renamed out of existence;
4. the quarantined tests really do run somewhere, in a workflow that has no
   deploy power of its own.

These assertions read the real ``pyproject.toml`` and the real workflow YAML
structurally — parsed TOML, parsed YAML and tokenized shell commands — rather
than grepping for substrings that a comment or an ``echo`` could fake.

Deliberately absent: any assertion pinning the exact number of quarantined
tests, and any allowlist of the Wave 1 files. Later owner-approved waves must
be able to grow the marked set without editing this file; what they must not do
is break the four properties above. The one module named below is named
negatively — it must *not* carry the marker — which constrains nothing about
what a future wave may add.
"""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

from altegio_bot.tests.test_ci_workflow_nginx_gate import (
    MIGRATION_GATE_ENV,
    MIGRATION_SUITE,
    NGINX_GATE_ENV,
    NGINX_SUITE,
    _pytest_invocations,
    _steps,
    _workflow,
    _workflow_triggers,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
PYPROJECT_FILE = _REPO_ROOT / "pyproject.toml"
REQUIRED_WORKFLOW_FILE = _REPO_ROOT / ".github" / "workflows" / "ci_deploy.yml"
LEGACY_WORKFLOW_FILE = _REPO_ROOT / ".github" / "workflows" / "legacy_altegio_tests.yml"

# Wave 1 marked this module and review took it back out: its tests cover
# `_update_run_exclusion_counters`, which `run_preview` calls unconditionally
# after BOTH provider branches, EasyWeek included. Named here as a single
# negative guard, not as the first entry of an allowlist.
PROVIDER_NEUTRAL_MODULE = _REPO_ROOT / "src" / "altegio_bot" / "tests" / "campaigns" / "test_runner_counters.py"

MARKER = "legacy_altegio"
NEGATIVE_FILTER = f"not {MARKER}"

REQUIRED_WORKFLOW_NAME = "CI / Deploy"
REQUIRED_JOB_KEY = "tests"
REQUIRED_JOB_NAME = "Run Tests"

LEGACY_WORKFLOW_NAME = "Legacy Altegio Tests"
HANDOVER_SUITES = (
    "src/altegio_bot/tests/test_easyweek_reminder_handover.py",
    "src/altegio_bot/tests/test_easyweek_reminder_handover_db.py",
    "src/altegio_bot/tests/test_easyweek_reminder_handover_safety.py",
)
DEDICATED_GATE_SUITES = (MIGRATION_SUITE, NGINX_SUITE, *HANDOVER_SUITES)

# The five --ignore entries that keep the heavy mandatory suites from running a
# second time inside the general invocation.
EXPECTED_IGNORED_SUITES = frozenset({MIGRATION_SUITE, NGINX_SUITE, *HANDOVER_SUITES})


# ===========================================================================
# Parsing helpers
# ===========================================================================


def _pytest_config() -> dict[str, Any]:
    config = tomllib.loads(PYPROJECT_FILE.read_text())["tool"]["pytest"]["ini_options"]
    assert isinstance(config, dict), "[tool.pytest.ini_options] must be a table"
    return config


def _addopts_tokens() -> list[str]:
    addopts = _pytest_config().get("addopts", "")
    if isinstance(addopts, list):
        return [str(token) for token in addopts]
    return str(addopts).split()


def _marker_selections(invocation: list[str]) -> list[str]:
    """Return every ``-m`` expression in one tokenized pytest command."""
    selections: list[str] = []
    arguments = invocation[3:]
    for index, argument in enumerate(arguments):
        if argument == "-m" and index + 1 < len(arguments):
            selections.append(arguments[index + 1])
        elif argument.startswith("-m") and argument != "-m":
            selections.append(argument[2:])
    return selections


def _ignored_suites(invocation: list[str]) -> set[str]:
    ignored: set[str] = set()
    arguments = invocation[3:]
    for index, argument in enumerate(arguments):
        if argument.startswith("--ignore="):
            ignored.add(argument.removeprefix("--ignore="))
        elif argument == "--ignore" and index + 1 < len(arguments):
            ignored.add(arguments[index + 1])
    return ignored


def _declares_legacy_marker(module_path: Path) -> bool:
    """True when the module assigns a `pytestmark` carrying the legacy marker.

    Parsed, not grepped: a mention of the marker in a docstring, a comment or a
    string literal must not read as a classification, and `pytestmark = [a, b]`
    must read as one.
    """
    for node in ast.parse(module_path.read_text()).body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets):
            continue
        attributes = {child.attr for child in ast.walk(node.value) if isinstance(child, ast.Attribute)}
        if MARKER in attributes:
            return True
    return False


def _legacy_workflow() -> dict[str, Any]:
    workflow = yaml.safe_load(LEGACY_WORKFLOW_FILE.read_text())
    assert isinstance(workflow, dict), "the legacy workflow must parse into a mapping"
    return workflow


def _legacy_jobs() -> dict[str, Any]:
    jobs = _legacy_workflow()["jobs"]
    assert isinstance(jobs, dict) and jobs, "the legacy workflow must define jobs"
    return jobs


def _legacy_steps() -> list[dict[str, Any]]:
    return [step for job in _legacy_jobs().values() for step in job.get("steps", []) if isinstance(step, dict)]


def _required_general_invocations() -> list[list[str]]:
    """The general application run: the one that --ignore's the dedicated gates."""
    return [
        invocation
        for step in _steps(REQUIRED_JOB_KEY)
        for invocation in _pytest_invocations(step)
        if _ignored_suites(invocation)
    ]


def _required_dedicated_invocations() -> list[list[str]]:
    return [
        invocation
        for step in _steps(REQUIRED_JOB_KEY)
        for invocation in _pytest_invocations(step)
        if not _ignored_suites(invocation)
    ]


# ===========================================================================
# The marker is real, strict, and not applied behind anybody's back
# ===========================================================================


def test_marker_is_registered_with_a_description() -> None:
    markers = _pytest_config().get("markers", [])
    entries = [str(entry) for entry in markers if str(entry).split(":", 1)[0].strip() == MARKER]
    assert len(entries) == 1, f"{MARKER} must be registered exactly once, got {entries}"
    description = entries[0].split(":", 1)[1].strip()
    assert description, "an undocumented marker invites exactly the misuse this PR forbids"


def test_strict_markers_is_enabled() -> None:
    """Without it a typo'd marker is silently ignored and the test stays required."""
    tokens = _addopts_tokens()
    assert "--strict-markers" in tokens or _pytest_config().get("strict_markers") is True


def test_marker_is_known_to_the_running_pytest(pytestconfig: pytest.Config) -> None:
    """Registration has to reach the live config, not only the TOML file."""
    registered = {str(line).split(":", 1)[0].strip() for line in pytestconfig.getini("markers")}
    assert MARKER in registered


def test_global_addopts_does_not_exclude_legacy_tests() -> None:
    """`uv run pytest` must stay the full project suite."""
    tokens = _addopts_tokens()
    assert "-m" not in tokens, "a global -m would hide tests from every local run"
    assert not any(token.startswith("-m") and token != "-m" for token in tokens)
    assert MARKER not in " ".join(tokens)


def test_marker_is_applied_per_module_not_by_a_central_hook() -> None:
    """Classification is per module — and one module must stay unclassified.

    Two halves of the same property: nothing may attach the marker centrally,
    and a module whose helper is on the EasyWeek path may not carry it at all.
    """
    for conftest in _REPO_ROOT.joinpath("src").rglob("conftest.py"):
        source = conftest.read_text()
        assert "pytest_collection_modifyitems" not in source, f"{conftest} adds markers behind the author's back"
        assert MARKER not in source, f"{conftest} must not classify tests centrally"

    # A keyword scan called this module Altegio-only: it never says "easyweek"
    # and its fixtures name no provider. Tracing the call site says otherwise —
    # `run_preview` calls `_update_run_exclusion_counters` after the EasyWeek
    # branch and the Altegio branch alike, so these tests guard EasyWeek
    # preview too and belong in the required tier.
    assert PROVIDER_NEUTRAL_MODULE.is_file(), f"missing {PROVIDER_NEUTRAL_MODULE}"
    assert not _declares_legacy_marker(PROVIDER_NEUTRAL_MODULE), (
        f"{PROVIDER_NEUTRAL_MODULE.name} covers a provider-neutral helper reached by EasyWeek run_preview; "
        "it must stay in the required tier"
    )


# ===========================================================================
# The required gate: one negative filter, and only on the general run
# ===========================================================================


def test_required_workflow_keeps_its_branch_protection_identity() -> None:
    workflow = _workflow()
    assert workflow["name"] == REQUIRED_WORKFLOW_NAME, "renaming the workflow breaks the required check"
    job = workflow["jobs"][REQUIRED_JOB_KEY]
    assert job["name"] == REQUIRED_JOB_NAME, "renaming the job breaks the required check"


def test_required_job_has_no_matrix_or_path_filters() -> None:
    """A matrix multiplies the check name; a path filter lets a PR skip it."""
    job = _workflow()["jobs"][REQUIRED_JOB_KEY]
    assert "strategy" not in job, "a matrix would rename the required check"
    triggers = _workflow_triggers(_workflow())
    for event in ("pull_request", "push"):
        config = triggers.get(event) or {}
        if isinstance(config, dict):
            assert "paths" not in config and "paths-ignore" not in config


def test_exactly_one_general_invocation_filters_the_marker_out() -> None:
    general = _required_general_invocations()
    assert len(general) == 1, f"expected one general pytest run, found {len(general)}"
    selections = _marker_selections(general[0])
    assert selections == [NEGATIVE_FILTER], f"the general run must select exactly {NEGATIVE_FILTER!r}, got {selections}"


def test_general_invocation_keeps_all_five_ignores() -> None:
    """The marker filter must not become an excuse to re-run the heavy gates."""
    general = _required_general_invocations()
    assert len(general) == 1
    ignored = _ignored_suites(general[0])
    assert ignored == EXPECTED_IGNORED_SUITES, f"unexpected --ignore set: {sorted(ignored)}"
    assert len(ignored) == 5


@pytest.mark.parametrize("suite", DEDICATED_GATE_SUITES)
def test_dedicated_gates_are_never_marker_filtered(suite: str) -> None:
    """A marker filter on a dedicated gate could silence it without a diff to the gate."""
    running = [
        invocation
        for invocation in _required_dedicated_invocations()
        if any(target == suite for target in invocation[3:])
    ]
    assert running, f"no dedicated invocation runs {suite}"
    for invocation in running:
        assert _marker_selections(invocation) == [], f"{suite} must not be marker-filtered"


def test_mandatory_env_gates_survive_the_change() -> None:
    steps = _steps(REQUIRED_JOB_KEY)
    env_flags = {name for step in steps for name in (step.get("env") or {})}
    assert {MIGRATION_GATE_ENV, NGINX_GATE_ENV, "REQUIRE_PG_CONCURRENCY"} <= env_flags


def test_no_required_step_is_softened() -> None:
    for job_key, job in _workflow()["jobs"].items():
        assert "continue-on-error" not in job, f"job {job_key} is softened"
        for step in job.get("steps", []):
            if isinstance(step, dict):
                assert "continue-on-error" not in step, f"a step of {job_key} is softened"


# ===========================================================================
# The legacy workflow: runs the quarantined tests, deploys nothing
# ===========================================================================


def test_legacy_workflow_exists_and_is_named() -> None:
    assert LEGACY_WORKFLOW_FILE.is_file(), f"missing {LEGACY_WORKFLOW_FILE}"
    assert _legacy_workflow()["name"] == LEGACY_WORKFLOW_NAME


def test_legacy_workflow_triggers_are_only_schedule_and_manual() -> None:
    triggers = _workflow_triggers(_legacy_workflow())
    assert set(triggers) == {"schedule", "workflow_dispatch"}, f"unexpected triggers: {sorted(triggers)}"


def test_legacy_workflow_runs_daily_off_peak() -> None:
    schedule = _workflow_triggers(_legacy_workflow())["schedule"]
    assert isinstance(schedule, list) and len(schedule) == 1
    minute, hour, day_of_month, month, day_of_week = str(schedule[0]["cron"]).split()
    assert (day_of_month, month, day_of_week) == ("*", "*", "*"), "the legacy suite must run every day"
    assert minute.isdigit() and int(minute) not in {0, 30}, "avoid the congested top/half of the hour"
    assert hour.isdigit() and 0 <= int(hour) <= 5, "keep the legacy run off-peak"


def test_legacy_workflow_is_read_only() -> None:
    assert _legacy_workflow()["permissions"] == {"contents": "read"}


def test_legacy_workflow_selects_the_marker_positively() -> None:
    invocations = [invocation for step in _legacy_steps() for invocation in _pytest_invocations(step)]
    assert len(invocations) == 1, f"expected one pytest run, found {len(invocations)}"
    assert _marker_selections(invocations[0]) == [MARKER]
    assert _ignored_suites(invocations[0]) == set(), "the legacy run must not skip its own tests"


def test_legacy_workflow_has_a_postgres_service_with_a_healthcheck() -> None:
    for job in _legacy_jobs().values():
        postgres = job["services"]["postgres"]
        assert str(postgres["image"]).startswith("postgres:16"), "pin the same major version as the required gate"
        assert "pg_isready" in str(postgres["options"]), "an unchecked service turns into a flaky suite"


def test_legacy_workflow_uses_python_312_and_frozen_deps() -> None:
    steps = _legacy_steps()
    versions = [str(step["with"]["python-version"]) for step in steps if "setup-python" in str(step.get("uses", ""))]
    assert versions == ["3.12"]
    assert any("uv sync --frozen" in str(step.get("run", "")) for step in steps)


def test_legacy_workflow_uses_throwaway_test_credentials() -> None:
    for job in _legacy_jobs().values():
        env = job.get("env") or {}
        assert "localhost" in env["DATABASE_URL"] and "altegio_bot_test" in env["DATABASE_URL"]
        assert env["ALTEGIO_WEBHOOK_SECRET"] == "test-secret"


def test_legacy_workflow_has_no_deploy_power() -> None:
    """The whole point of a separate file: a nightly run must never ship code."""
    raw = LEGACY_WORKFLOW_FILE.read_text()
    assert "secrets." not in raw, "the legacy suite must not read repository secrets"
    for step in _legacy_steps():
        uses = str(step.get("uses", ""))
        assert "ssh-action" not in uses and "telegram" not in uses, f"deploy tooling leaked in: {uses}"
        shell = str(step.get("run", "")) + str(step.get("with", {}).get("script", ""))
        for forbidden in ("docker compose", "deploy_pr3.sh", "git reset --hard", "/opt/altegio_bot"):
            assert forbidden not in shell, f"production command leaked in: {forbidden}"


def test_legacy_workflow_is_not_softened_and_is_bounded() -> None:
    for job_key, job in _legacy_jobs().items():
        assert "continue-on-error" not in job, f"job {job_key} is softened"
        assert isinstance(job.get("timeout-minutes"), int), f"job {job_key} needs a timeout"
        for step in job.get("steps", []):
            if isinstance(step, dict):
                assert "continue-on-error" not in step, f"a step of {job_key} is softened"


def test_schedule_was_not_bolted_onto_the_deploying_workflow() -> None:
    """A schedule on ci_deploy.yml would be one condition away from deploying."""
    assert "schedule" not in _workflow_triggers(_workflow())


# ===========================================================================
# Guards for the guards
# ===========================================================================


def test_marker_selection_parser_understands_both_spellings() -> None:
    assert _marker_selections(["uv", "run", "pytest", "-m", NEGATIVE_FILTER]) == [NEGATIVE_FILTER]
    assert _marker_selections(["uv", "run", "pytest", f"-m{MARKER}"]) == [MARKER]
    assert _marker_selections(["uv", "run", "pytest", "-q"]) == []
    assert _marker_selections(["uv", "run", "pytest", "-m", "a", "-m", "b"]) == ["a", "b"]
