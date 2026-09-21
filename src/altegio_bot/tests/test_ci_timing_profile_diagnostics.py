"""The timing-profile diagnostics must stay diagnostics.

TEMPORARY, together with the workflow change it guards. Branch
`chore/pytest-timing-profile` exists only to read per-test duration off a real
GitHub runner; when the three profiles have been analysed, the PR is closed and
this file goes with it.

A profiling change is tempting to make sloppily — it "only adds flags" — and
that is exactly how a required gate quietly loses a test. So the same structure
that makes the gate trustworthy is pinned here while the diagnostics are in
place:

* the required check keeps the identity branch protection refers to;
* the general run still selects exactly what it selected before — same marker
  filter, same five ``--ignore``, same single invocation;
* the three diagnostic arguments appear **only** there, never on a dedicated
  gate, never in the legacy workflow, never in ``addopts``;
* the artifact carries the JUnit XML and nothing else, and it cannot silently
  become a way to ship logs or an environment dump off the runner;
* nothing in the job is softened, and the job gains no new capability.

Everything is read from the parsed YAML and from tokenized shell commands, so a
comment or an ``echo`` cannot satisfy any of it.

The five ``--ignore`` entries, the marker filter and the required job's names
are imported from ``test_ci_legacy_altegio_quarantine`` rather than restated.
Restating them would let this file keep passing against a gate that has already
drifted.
"""

from __future__ import annotations

import tomllib
from typing import Any

import pytest

from altegio_bot.tests.test_ci_legacy_altegio_quarantine import (
    DEDICATED_GATE_SUITES,
    EXPECTED_IGNORED_SUITES,
    LEGACY_WORKFLOW_FILE,
    NEGATIVE_FILTER,
    PYPROJECT_FILE,
    REQUIRED_JOB_KEY,
    REQUIRED_JOB_NAME,
    REQUIRED_WORKFLOW_NAME,
    _ignored_suites,
    _legacy_steps,
    _marker_selections,
    _required_dedicated_invocations,
    _required_general_invocations,
)
from altegio_bot.tests.test_ci_workflow_nginx_gate import (
    _condition_source,
    _pytest_invocations,
    _pytest_targets,
    _steps,
    _workflow,
    _workflow_triggers,
)

PYTEST_PREFIX = ["uv", "run", "pytest"]

DURATIONS_ARG = "--durations=100"
DURATIONS_MIN_ARG = "--durations-min=0.25"
JUNIT_XML_PATH = "pytest-results.xml"
JUNIT_ARG = f"--junitxml={JUNIT_XML_PATH}"
DIAGNOSTIC_ARGS = (DURATIONS_ARG, DURATIONS_MIN_ARG, JUNIT_ARG)

# Every family of diagnostic flag, in both spellings, so a future edit cannot
# reach a dedicated gate through `--durations 100` or `--junit-xml ...`.
JUNIT_PREFIXES = ("--junitxml", "--junit-xml", "--junit-prefix")
DIAGNOSTIC_PREFIXES = ("--durations", *JUNIT_PREFIXES)

UPLOAD_STEP_NAME = "Upload pytest timing profile"
UPLOAD_ACTION = "actions/upload-artifact@v4"
RETENTION_DAYS = 7

# The tests job may reach for these and nothing else. A new `uses:` here is how
# a diagnostics job would grow the power to reach outside the runner.
ALLOWED_TEST_JOB_ACTIONS = frozenset(
    {
        "actions/checkout@v4",
        "actions/setup-python@v5",
        UPLOAD_ACTION,
    }
)


# ===========================================================================
# Helpers
# ===========================================================================


def _required_job() -> dict[str, Any]:
    return _workflow()["jobs"][REQUIRED_JOB_KEY]


def _main_invocation() -> list[str]:
    """The one general pytest run — the invocation that --ignore's the gates."""
    general = _required_general_invocations()
    assert len(general) == 1, f"expected exactly one general pytest run, found {len(general)}"
    return general[0]


def _main_pytest_step_index() -> int:
    steps = _steps(REQUIRED_JOB_KEY)
    indexes = [
        index
        for index, step in enumerate(steps)
        for invocation in _pytest_invocations(step)
        if _ignored_suites(invocation)
    ]
    assert len(indexes) == 1, f"the general pytest run must live in exactly one step, found {len(indexes)}"
    return indexes[0]


def _upload_step() -> dict[str, Any]:
    steps = _steps(REQUIRED_JOB_KEY)
    uploads = [step for step in steps if str(step.get("uses", "")).startswith("actions/upload-artifact")]
    assert len(uploads) == 1, f"expected exactly one upload step, found {len(uploads)}"
    return uploads[0]


def _upload_inputs() -> dict[str, Any]:
    inputs = _upload_step().get("with")
    assert isinstance(inputs, dict), "the upload step must configure the action"
    return inputs


def _diagnostic_arguments(invocation: list[str]) -> list[str]:
    return [argument for argument in invocation[3:] if argument.startswith(DIAGNOSTIC_PREFIXES)]


def _addopts_tokens() -> list[str]:
    config = tomllib.loads(PYPROJECT_FILE.read_text())["tool"]["pytest"]["ini_options"]
    addopts = config.get("addopts", "")
    tokens = [str(token) for token in addopts] if isinstance(addopts, list) else str(addopts).split()
    return tokens


def _pytest_ini_options() -> dict[str, Any]:
    return tomllib.loads(PYPROJECT_FILE.read_text())["tool"]["pytest"]["ini_options"]


# ===========================================================================
# The required check still is the required check
# ===========================================================================


def test_workflow_and_job_identity_are_untouched() -> None:
    """Branch protection refers to these three strings by name."""
    workflow = _workflow()
    assert workflow["name"] == REQUIRED_WORKFLOW_NAME
    assert REQUIRED_JOB_KEY in workflow["jobs"], "the required test job is gone"
    assert workflow["jobs"][REQUIRED_JOB_KEY]["name"] == REQUIRED_JOB_NAME


def test_profiling_did_not_bring_a_matrix_or_path_filter() -> None:
    """A matrix renames the check; a path filter lets a PR skip it entirely."""
    assert "strategy" not in _required_job(), "a matrix would rename the required check"
    triggers = _workflow_triggers(_workflow())
    for event, config in triggers.items():
        if isinstance(config, dict):
            assert "paths" not in config, f"{event} gained a path filter"
            assert "paths-ignore" not in config, f"{event} gained a path-ignore filter"


def test_profiling_granted_no_new_capability() -> None:
    """Diagnostics read timings off the runner; they do not reach outside it."""
    job = _required_job()
    used = {str(step["uses"]) for step in _steps(REQUIRED_JOB_KEY) if "uses" in step}
    assert used <= ALLOWED_TEST_JOB_ACTIONS, f"unexpected action in the required job: {sorted(used)}"

    for scope, level in (job.get("permissions") or {}).items():
        assert level != "write", f"the required job asked for write on {scope}"
    for scope, level in (_workflow().get("permissions") or {}).items():
        assert level != "write", f"the workflow asked for write on {scope}"

    for step in _steps(REQUIRED_JOB_KEY):
        rendered = f"{step.get('run', '')}{step.get('with', {})}"
        assert "secrets." not in rendered, f"step {step.get('name')!r} reads repository secrets"


# ===========================================================================
# Selection is byte-for-byte what it was
# ===========================================================================


def test_main_invocation_still_starts_with_uv_run_pytest() -> None:
    """No `/usr/bin/time`, no `time`, no wrapper: the contract tests parse this prefix."""
    assert _main_invocation()[:3] == PYTEST_PREFIX


def test_main_invocation_keeps_exactly_one_negative_marker_filter() -> None:
    assert _marker_selections(_main_invocation()) == [NEGATIVE_FILTER]


def test_main_invocation_keeps_exactly_the_same_five_ignores() -> None:
    ignored = _ignored_suites(_main_invocation())
    assert ignored == EXPECTED_IGNORED_SUITES, f"the --ignore set changed: {sorted(ignored)}"
    assert len(ignored) == 5


def test_main_invocation_collects_no_positional_target() -> None:
    """A positional path would silently narrow the gate to part of the suite."""
    assert _pytest_targets(_main_invocation()) == []


# ===========================================================================
# The diagnostic arguments: present here, nowhere else
# ===========================================================================


@pytest.mark.parametrize("argument", DIAGNOSTIC_ARGS)
def test_main_invocation_carries_the_diagnostic_argument(argument: str) -> None:
    """The `=` form matters: a space-separated value would read as a target."""
    assert argument in _main_invocation(), f"{argument} is missing from the general run"


def test_diagnostic_arguments_appear_once_each() -> None:
    invocation = _main_invocation()
    for argument in DIAGNOSTIC_ARGS:
        assert invocation.count(argument) == 1, f"{argument} appears {invocation.count(argument)} times"


def test_diagnostic_arguments_are_the_only_ones_added() -> None:
    """Profiling must not smuggle in a second, unreviewed flag family."""
    assert sorted(_diagnostic_arguments(_main_invocation())) == sorted(DIAGNOSTIC_ARGS)


@pytest.mark.parametrize("suite", DEDICATED_GATE_SUITES)
def test_dedicated_gates_stay_free_of_diagnostics(suite: str) -> None:
    """A dedicated gate must keep running exactly as it did before profiling."""
    running = [
        invocation
        for invocation in _required_dedicated_invocations()
        if any(token == suite for token in invocation[3:])
    ]
    assert running, f"no dedicated invocation runs {suite}"
    for invocation in running:
        assert _diagnostic_arguments(invocation) == [], f"{suite} picked up a diagnostic argument"
        assert _marker_selections(invocation) == [], f"{suite} must not be marker-filtered"


def test_legacy_workflow_produces_no_profile_and_uploads_nothing() -> None:
    """The nightly run keeps its own pre-existing `--durations=50` console
    report — that predates this branch and is not touched. What it must not
    gain is this branch's machinery: a JUnit file on disk, or any artifact
    leaving a scheduled run.
    """
    assert LEGACY_WORKFLOW_FILE.is_file(), f"missing {LEGACY_WORKFLOW_FILE}"
    raw = LEGACY_WORKFLOW_FILE.read_text()

    for invocation in (invocation for step in _legacy_steps() for invocation in _pytest_invocations(step)):
        for argument in DIAGNOSTIC_ARGS:
            assert argument not in invocation, f"the legacy workflow picked up {argument}"
        assert not any(token.startswith(JUNIT_PREFIXES) for token in invocation), "the legacy run must write no XML"

    assert JUNIT_XML_PATH not in raw, "the legacy workflow references the profile file"
    assert "upload-artifact" not in raw, "a scheduled run must not upload artifacts"


def test_diagnostics_are_not_global() -> None:
    """In `addopts` they would change every local run and outlive this branch."""
    tokens = _addopts_tokens()
    assert not any(token.startswith(DIAGNOSTIC_PREFIXES) for token in tokens), f"addopts carries diagnostics: {tokens}"
    options = _pytest_ini_options()
    for key in ("junit_logging", "junit_family", "junit_duration_report"):
        assert key not in options, f"{key} must not be configured for the whole project"


def test_junit_logging_stays_off_so_the_artifact_carries_no_log_output() -> None:
    """Webhook secrets travel through these tests; an artifact is distribution."""
    assert "junit_logging" not in " ".join(_main_invocation())
    assert "-o" not in _main_invocation(), "an inline -o could switch junit_logging on"


# ===========================================================================
# The upload step
# ===========================================================================


def test_upload_step_comes_immediately_after_the_pytest_step() -> None:
    steps = _steps(REQUIRED_JOB_KEY)
    upload_index = steps.index(_upload_step())
    assert upload_index == _main_pytest_step_index() + 1, "the upload step must directly follow the general pytest run"


def test_upload_step_is_named_and_uses_the_pinned_action() -> None:
    step = _upload_step()
    assert step.get("name") == UPLOAD_STEP_NAME
    assert step["uses"] == UPLOAD_ACTION


def test_upload_step_runs_even_when_the_suite_fails() -> None:
    """A red run is the one whose timings are worth having."""
    condition = _condition_source(_upload_step().get("if"))
    assert condition == "always()", f"unexpected upload condition: {condition!r}"


def test_upload_step_ships_only_the_junit_xml() -> None:
    """No logs, no environment dump, no .env, no directory."""
    path = _upload_inputs()["path"]
    assert path == JUNIT_XML_PATH, f"unexpected artifact path: {path!r}"
    assert str(path).count("\n") == 0, "a multi-line path would upload more than the profile"


def test_upload_step_keeps_the_artifact_short_lived() -> None:
    assert int(_upload_inputs()["retention-days"]) == RETENTION_DAYS


def test_upload_step_fails_when_the_profile_is_missing() -> None:
    """A silently absent artifact would look like a green, useless run."""
    assert _upload_inputs()["if-no-files-found"] == "error"


def test_artifact_name_separates_sha_and_run_attempt() -> None:
    """Three re-runs of one SHA must land as three distinct artifacts."""
    name = str(_upload_inputs()["name"])
    assert "github.sha" in name, "the artifact name must carry the SHA"
    assert "github.run_attempt" in name, "re-runs would otherwise collide on one name"


# ===========================================================================
# Nothing is softened
# ===========================================================================


def test_neither_the_job_nor_the_profiling_steps_are_softened() -> None:
    job = _required_job()
    assert "continue-on-error" not in job, "the required job is softened"

    steps = _steps(REQUIRED_JOB_KEY)
    pytest_step = steps[_main_pytest_step_index()]
    assert "continue-on-error" not in pytest_step, "a failing suite must still fail the job"
    assert "if" not in pytest_step, "the general run must not become conditional"
    assert "continue-on-error" not in _upload_step(), "the upload step is softened"


def test_no_step_in_the_required_job_is_softened() -> None:
    for step in _steps(REQUIRED_JOB_KEY):
        assert "continue-on-error" not in step, f"step {step.get('name')!r} is softened"
