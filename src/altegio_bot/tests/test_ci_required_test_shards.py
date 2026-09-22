"""Sharding the required gate must not lose, duplicate or weaken a single test.

The general suite now runs on two runners and the mandatory gates on a third.
Three runners means three chances to drop coverage silently, and a green CI
would report nothing: a test that stops running simply stops failing.

So the partition is asserted here as structure, from parsed YAML and tokenized
shell, and it is deliberately **asymmetric**:

    heavy = exactly the sixteen listed modules
    rest  = the whole test root, MINUS the dedicated suites, MINUS heavy

Only ``heavy`` is a list. ``rest`` is a subtraction, which is what makes the
union total: a module nobody classified — including every test file added
tomorrow — is collected by ``rest`` and stays required by default. An allowlist
of "light" modules would invert that, and the first forgotten entry would
silently leave the gate. There is no such list, and
:func:`test_rest_shard_is_a_subtraction_not_an_allowlist` refuses one.

The reporting job keeps the key ``tests`` and the name ``Run Tests``, because
branch protection refers to those strings. It runs under ``always()`` so a
failed or cancelled dependency cannot skip it into a neutral, non-blocking
result, and it then refuses anything that is not ``success``.

Not asserted anywhere: a total test count. New tests must be addable without
editing a number here.
"""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml

from altegio_bot.tests.test_ci_legacy_altegio_quarantine import (
    DEDICATED_GATE_SUITES,
    EXPECTED_IGNORED_SUITES,
    LEGACY_WORKFLOW_FILE,
    MARKER,
    NEGATIVE_FILTER,
    REQUIRED_JOB_KEY,
    REQUIRED_JOB_NAME,
    REQUIRED_WORKFLOW_NAME,
    _addopts_tokens,
    _ignored_suites,
    _marker_selections,
)
from altegio_bot.tests.test_ci_workflow_nginx_gate import (
    AGGREGATOR_JOB,
    DEDICATED_JOB,
    EXECUTION_JOBS,
    HEAVY_JOB,
    REST_JOB,
    _condition_source,
    _jobs,
    _pytest_invocations,
    _pytest_targets,
    _steps,
    _workflow,
    _workflow_triggers,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
TEST_ROOT = "src/altegio_bot/tests"

# The heavy shard, written out in full so the split is reviewable in the diff.
# Chosen from median per-module duration over three profiling runs on this
# runner class — a measurement taken beforehand, never a rule the workflow
# evaluates at run time. Nothing here is derived from a glob, from the word
# "easyweek", or from timings observed during the run being sharded.
EXPECTED_HEAVY_MODULES = (
    f"{TEST_ROOT}/test_easyweek_inbox_worker_integration.py",
    f"{TEST_ROOT}/test_easyweek_outbox_pr5_integration.py",
    f"{TEST_ROOT}/test_easyweek_voucher_delivery_runner.py",
    f"{TEST_ROOT}/test_easyweek_manual_voucher_canary.py",
    f"{TEST_ROOT}/test_easyweek_migration_live_proof.py",
    f"{TEST_ROOT}/test_easyweek_migration_rollback_recovery.py",
    f"{TEST_ROOT}/test_chatwoot_branch_compose_contract.py",
    f"{TEST_ROOT}/test_easyweek_pr4_migration.py",
    f"{TEST_ROOT}/test_easyweek_migration_apply.py",
    f"{TEST_ROOT}/test_easyweek_multi_service_snapshot_recovery.py",
    f"{TEST_ROOT}/test_easyweek_voucher_canary_runner.py",
    f"{TEST_ROOT}/test_chatwoot_webhook_sanitization.py",
    f"{TEST_ROOT}/test_easyweek_manual_recipient.py",
    f"{TEST_ROOT}/test_easyweek_visit_counter.py",
    f"{TEST_ROOT}/test_easyweek_migration_cumulative_manifest.py",
    f"{TEST_ROOT}/test_easyweek_post_booking_handover.py",
)

POSTGRES_IMAGE_PREFIX = "postgres:16"
PYTHON_VERSION = "3.12"
FROZEN_SYNC = "uv sync --frozen"
TEST_DATABASE_NAME = "altegio_bot_test"

REQUIRED_PR_JOB_IF = "github.event_name == 'pull_request' && needs.lint.result == 'success'"

# Anything that would let a shard reach outside its own runner.
FORBIDDEN_STEP_ACTIONS = ("ssh-action", "telegram", "upload-artifact")
# Diagnostics from the closed profiling branch must not ride along.
FORBIDDEN_PYTEST_ARGS = ("--durations", "--junitxml", "--junit-xml", "-n", "--numprocesses", "--dist")


# ===========================================================================
# Helpers
# ===========================================================================


def _job(job_name: str) -> dict[str, Any]:
    return _jobs()[job_name]


def _invocations(job_name: str) -> list[list[str]]:
    return [invocation for step in _steps(job_name) for invocation in _pytest_invocations(step)]


def _single_invocation(job_name: str) -> list[str]:
    invocations = _invocations(job_name)
    assert len(invocations) == 1, f"{job_name} must run pytest exactly once, found {len(invocations)}"
    return invocations[0]


def _heavy_invocation() -> list[str]:
    return _single_invocation(HEAVY_JOB)


def _rest_invocation() -> list[str]:
    return _single_invocation(REST_JOB)


def _postgres_service(job_name: str) -> dict[str, Any]:
    services = _job(job_name).get("services")
    assert isinstance(services, dict), f"{job_name} has no services block"
    postgres = services.get("postgres")
    assert isinstance(postgres, dict), f"{job_name} has no postgres service"
    return postgres


def _step_shell(step: dict[str, Any]) -> str:
    return str(step.get("run", ""))


def _module_paths(invocation: list[str]) -> list[str]:
    """Positional collection targets, normalised and order-independent."""
    return sorted(_pytest_targets(invocation))


# ===========================================================================
# Guard the guards: the parsers must reject as well as accept
# ===========================================================================


def test_target_parser_separates_targets_from_options() -> None:
    """A target is a path; an option, with or without `=`, is not."""
    invocation = shlex.split(f'uv run pytest -q -m "not {MARKER}" --ignore=a/b.py c/d.py')
    assert _pytest_targets(invocation) == ["c/d.py"]
    assert _ignored_suites(invocation) == {"a/b.py"}
    assert _marker_selections(invocation) == [f"not {MARKER}"]


def test_ignore_parser_accepts_both_spellings_and_nothing_else() -> None:
    assert _ignored_suites(shlex.split("uv run pytest --ignore=x.py")) == {"x.py"}
    assert _ignored_suites(shlex.split("uv run pytest --ignore x.py")) == {"x.py"}
    assert _ignored_suites(shlex.split("uv run pytest x.py")) == set()
    # A path merely mentioned as a target is not ignored, and vice versa.
    both = shlex.split("uv run pytest --ignore=a.py b.py")
    assert _ignored_suites(both) == {"a.py"}
    assert _pytest_targets(both) == ["b.py"]


def test_marker_parser_counts_selections_and_rejects_lookalikes() -> None:
    assert _marker_selections(shlex.split(f'uv run pytest -m "not {MARKER}"')) == [f"not {MARKER}"]
    assert _marker_selections(shlex.split(f"uv run pytest -m{MARKER}")) == [MARKER]
    assert _marker_selections(shlex.split("uv run pytest -q --maxfail=1")) == []
    assert len(_marker_selections(shlex.split("uv run pytest -m a -m b"))) == 2


def test_invocation_parser_ignores_comments_and_echoes() -> None:
    """A commented or echoed command must not count as an execution."""
    assert _pytest_invocations({"run": "# uv run pytest -q foo.py"}) == []
    assert _pytest_invocations({"run": 'echo "uv run pytest -q foo.py"'}) == []
    assert _pytest_invocations({"run": "uv run pytest -q foo.py"}) == [["uv", "run", "pytest", "-q", "foo.py"]]


# ===========================================================================
# 1-2. The four jobs exist and are scoped to pull requests after lint
# ===========================================================================


def test_all_four_jobs_exist() -> None:
    jobs = _workflow()["jobs"]
    for job_name in (*EXECUTION_JOBS, AGGREGATOR_JOB):
        assert job_name in jobs, f"missing job {job_name}"
    assert (HEAVY_JOB, REST_JOB, DEDICATED_JOB) == EXECUTION_JOBS


@pytest.mark.parametrize("job_name", EXECUTION_JOBS)
def test_execution_jobs_run_only_for_pull_requests_after_lint(job_name: str) -> None:
    assert _condition_source(_job(job_name).get("if")) == REQUIRED_PR_JOB_IF
    needs = _job(job_name).get("needs")
    assert needs == "lint" or needs == ["lint"], f"{job_name} must depend on lint alone, got {needs!r}"


@pytest.mark.parametrize("job_name", EXECUTION_JOBS)
def test_execution_jobs_run_on_ubuntu(job_name: str) -> None:
    assert _job(job_name)["runs-on"] == "ubuntu-latest"


# ===========================================================================
# 3. Each execution job brings its own database — this is why not xdist
# ===========================================================================


@pytest.mark.parametrize("job_name", EXECUTION_JOBS)
def test_each_execution_job_has_its_own_checked_postgres_16(job_name: str) -> None:
    """Separate services, not xdist workers: two processes sharing one Postgres
    would race on migrations and on the truncated shared tables.
    """
    postgres = _postgres_service(job_name)
    assert str(postgres["image"]).startswith(POSTGRES_IMAGE_PREFIX), f"{job_name} is not on PostgreSQL 16"
    assert "pg_isready" in str(postgres["options"]), f"{job_name} has no healthcheck"


@pytest.mark.parametrize("job_name", EXECUTION_JOBS)
def test_each_execution_job_points_at_a_throwaway_database(job_name: str) -> None:
    env = _job(job_name).get("env") or {}
    database_url = str(env["DATABASE_URL"])
    assert "localhost" in database_url, f"{job_name} must use its own service, not a remote database"
    assert TEST_DATABASE_NAME in database_url, f"{job_name} must use the disposable test database"
    assert env["ALTEGIO_WEBHOOK_SECRET"] == "test-secret"


@pytest.mark.parametrize("job_name", EXECUTION_JOBS)
def test_each_execution_job_installs_python_312_and_frozen_deps(job_name: str) -> None:
    steps = _steps(job_name)
    versions = [str(step["with"]["python-version"]) for step in steps if "setup-python" in str(step.get("uses", ""))]
    assert versions == [PYTHON_VERSION], f"{job_name} must set up exactly Python {PYTHON_VERSION}"
    assert any(FROZEN_SYNC in _step_shell(step) for step in steps), f"{job_name} must sync frozen dependencies"
    assert any("Postgres is ready" in _step_shell(step) for step in steps), f"{job_name} must wait for Postgres"


# ===========================================================================
# 4. The heavy shard
# ===========================================================================


def test_heavy_job_runs_pytest_exactly_once() -> None:
    assert len(_invocations(HEAVY_JOB)) == 1


def test_heavy_invocation_has_exactly_one_negative_marker_filter() -> None:
    assert _marker_selections(_heavy_invocation()) == [NEGATIVE_FILTER]


def test_heavy_invocation_runs_exactly_the_sixteen_expected_modules() -> None:
    assert _module_paths(_heavy_invocation()) == sorted(EXPECTED_HEAVY_MODULES)


def test_heavy_modules_are_listed_once_each() -> None:
    assert len(set(EXPECTED_HEAVY_MODULES)) == len(EXPECTED_HEAVY_MODULES) == 16
    targets = _pytest_targets(_heavy_invocation())
    assert len(targets) == len(set(targets)), "a module listed twice would run twice"


def test_heavy_modules_all_exist_on_disk() -> None:
    for module in EXPECTED_HEAVY_MODULES:
        assert (_REPO_ROOT / module).is_file(), f"heavy shard names a missing module: {module}"


def test_heavy_shard_touches_no_dedicated_suite() -> None:
    assert not set(EXPECTED_HEAVY_MODULES) & set(DEDICATED_GATE_SUITES)
    assert not set(_pytest_targets(_heavy_invocation())) & set(DEDICATED_GATE_SUITES)


def test_heavy_invocation_ignores_nothing() -> None:
    """It names its modules; an --ignore here could only subtract from them."""
    assert _ignored_suites(_heavy_invocation()) == set()


# ===========================================================================
# 5. The rest shard
# ===========================================================================


def test_rest_job_runs_pytest_exactly_once() -> None:
    assert len(_invocations(REST_JOB)) == 1


def test_rest_invocation_has_exactly_one_negative_marker_filter() -> None:
    assert _marker_selections(_rest_invocation()) == [NEGATIVE_FILTER]


def test_rest_shard_is_a_subtraction_not_an_allowlist() -> None:
    """The invariant that keeps the union total.

    No positional target means the whole test root is collected and then
    narrowed. List the light modules instead and the first one forgotten would
    leave the required gate without a single failing check.
    """
    assert _pytest_targets(_rest_invocation()) == [], "the rest shard must not enumerate what it runs"


def test_rest_invocation_ignores_exactly_the_dedicated_and_heavy_modules() -> None:
    ignored = _ignored_suites(_rest_invocation())
    expected = EXPECTED_IGNORED_SUITES | set(EXPECTED_HEAVY_MODULES)
    assert ignored == expected, (
        f"unexpected --ignore set; missing={sorted(expected - ignored)} extra={sorted(ignored - expected)}"
    )
    assert len(ignored) == 5 + 16


def test_rest_invocation_ignores_each_path_once() -> None:
    arguments = _rest_invocation()[3:]
    ignores = [argument for argument in arguments if argument.startswith("--ignore")]
    assert len(ignores) == len(set(ignores)) == 21, "a duplicated --ignore hides a typo in one of them"


# ===========================================================================
# 6. The dedicated gates, unchanged in meaning
# ===========================================================================


def test_dedicated_job_runs_the_three_gates() -> None:
    invocations = _invocations(DEDICATED_JOB)
    assert len(invocations) == 3, f"expected three gate invocations, found {len(invocations)}"


@pytest.mark.parametrize("suite", DEDICATED_GATE_SUITES)
def test_each_dedicated_suite_runs_exactly_once_in_the_dedicated_job(suite: str) -> None:
    running = [invocation for invocation in _invocations(DEDICATED_JOB) if suite in _pytest_targets(invocation)]
    assert len(running) == 1, f"{suite} is executed {len(running)} times in {DEDICATED_JOB}"
    assert _marker_selections(running[0]) == [], f"{suite} must not be marker-filtered"


def test_dedicated_gates_keep_their_mandatory_env_flags() -> None:
    flags = {name: str(value) for step in _steps(DEDICATED_JOB) for name, value in (step.get("env") or {}).items()}
    assert flags.get("ALTEGIO_REQUIRE_MIGTEST") == "1"
    assert flags.get("ALTEGIO_REQUIRE_NGINX_LOGTEST") == "1"
    assert flags.get("REQUIRE_PG_CONCURRENCY") == "1"


def test_dedicated_gate_steps_cannot_be_skipped_or_softened() -> None:
    """Without the env flags these suites skip when Docker is missing, so a
    conditional guard here would turn a security gate advisory.
    """
    for step in _steps(DEDICATED_JOB):
        if _pytest_invocations(step):
            assert "if" not in step, f"step {step.get('name')!r} is conditional"
            assert "continue-on-error" not in step, f"step {step.get('name')!r} is softened"


# ===========================================================================
# 7-8. The partition: disjoint, and total by construction
# ===========================================================================


def test_heavy_and_rest_do_not_overlap() -> None:
    """Every heavy module is subtracted from rest, so nothing runs twice."""
    heavy = set(_pytest_targets(_heavy_invocation()))
    assert heavy <= _ignored_suites(_rest_invocation()), (
        f"these heavy modules would run twice: {sorted(heavy - _ignored_suites(_rest_invocation()))}"
    )


def test_the_union_is_total_by_construction() -> None:
    """Nothing can fall between the shards.

    rest collects the whole root and subtracts exactly two sets: the dedicated
    suites, which the dedicated job runs, and the heavy modules, which the
    heavy job runs. Both subtracted sets are executed elsewhere, so the union
    of the three jobs is the entire required tier — including files that do not
    exist yet.
    """
    subtracted = _ignored_suites(_rest_invocation())
    executed_elsewhere = set(_pytest_targets(_heavy_invocation())) | {
        suite for invocation in _invocations(DEDICATED_JOB) for suite in _pytest_targets(invocation)
    }
    orphaned = subtracted - executed_elsewhere
    assert not orphaned, f"these paths are ignored by rest and run by nobody: {sorted(orphaned)}"


def test_no_shard_narrows_the_gate_with_an_extra_selector() -> None:
    """`-k`, `--maxfail`, `--deselect` or xdist would all silently shrink a shard."""
    for job_name in EXECUTION_JOBS:
        for invocation in _invocations(job_name):
            arguments = invocation[3:]
            assert "-k" not in arguments, f"{job_name} narrows by keyword"
            assert not any(argument.startswith("--deselect") for argument in arguments), f"{job_name} deselects"
            assert not any(argument.startswith("--maxfail") for argument in arguments), f"{job_name} stops early"
            for forbidden in FORBIDDEN_PYTEST_ARGS:
                assert not any(argument.startswith(forbidden) for argument in arguments), (
                    f"{job_name} carries {forbidden}"
                )


# ===========================================================================
# 9-10. The aggregator keeps the required check and fails closed
# ===========================================================================


def test_aggregator_keeps_the_branch_protection_identity() -> None:
    workflow = _workflow()
    assert workflow["name"] == REQUIRED_WORKFLOW_NAME
    assert REQUIRED_JOB_KEY == AGGREGATOR_JOB
    assert workflow["jobs"][AGGREGATOR_JOB]["name"] == REQUIRED_JOB_NAME


def test_aggregator_depends_on_every_execution_job() -> None:
    needs = _job(AGGREGATOR_JOB)["needs"]
    assert set(needs) == {"lint", *EXECUTION_JOBS}, f"unexpected needs: {needs}"


def test_aggregator_always_runs_so_it_cannot_be_skipped_into_neutral() -> None:
    condition = _condition_source(_job(AGGREGATOR_JOB).get("if"))
    assert condition is not None
    assert "always()" in condition, "a failed dependency would skip the required check"
    assert "github.event_name == 'pull_request'" in condition


def test_aggregator_reads_every_dependency_result() -> None:
    steps = _steps(AGGREGATOR_JOB)
    assert len(steps) == 1, "the reporting job must stay a single check"
    env = steps[0].get("env") or {}
    referenced = " ".join(str(value) for value in env.values())
    for job_name in ("lint", *EXECUTION_JOBS):
        assert f"'{job_name}'" in referenced or f"needs.{job_name}.result" in referenced, (
            f"the aggregator never reads the result of {job_name}"
        )
    assert len(env) == 1 + len(EXECUTION_JOBS)


def test_aggregator_fails_on_anything_that_is_not_success() -> None:
    """Failure, cancelled and skipped must all be red, not just failure."""
    script = _step_shell(_steps(AGGREGATOR_JOB)[0])
    assert '!= "success"' in script, "the check must be an equality against success, not a blocklist"
    assert "exit 1" in script, "the aggregator must actually fail"


def test_aggregator_does_no_work_of_its_own() -> None:
    """No services, no checkout, no dependencies, no pytest, no secrets."""
    job = _job(AGGREGATOR_JOB)
    assert "services" not in job, "the reporting job must not start a database"
    steps = _steps(AGGREGATOR_JOB)
    assert not any("uses" in step for step in steps), "the reporting job must not run actions"
    assert not any(_pytest_invocations(step) for step in steps), "the reporting job must not run tests"
    for step in steps:
        assert "secrets." not in f"{step.get('run', '')}{step.get('env', {})}", "the reporting job reads secrets"


# ===========================================================================
# 11. No new capability anywhere in the required gate
# ===========================================================================


def test_no_path_filters_were_introduced() -> None:
    triggers = _workflow_triggers(_workflow())
    for event, config in triggers.items():
        if isinstance(config, dict):
            assert "paths" not in config, f"{event} gained a path filter"
            assert "paths-ignore" not in config, f"{event} gained a path-ignore filter"


@pytest.mark.parametrize("job_name", [*EXECUTION_JOBS, AGGREGATOR_JOB])
def test_no_required_job_gained_deploy_power_or_secrets(job_name: str) -> None:
    for step in _steps(job_name):
        uses = str(step.get("uses", ""))
        for forbidden in FORBIDDEN_STEP_ACTIONS:
            assert forbidden not in uses, f"{job_name} uses {uses}"
        rendered = f"{step.get('run', '')}{step.get('with', {})}{step.get('env', {})}"
        assert "secrets." not in rendered, f"{job_name} reads repository secrets"
        assert "continue-on-error" not in step, f"a step of {job_name} is softened"
    assert "continue-on-error" not in _job(job_name), f"{job_name} is softened"


def test_deploy_job_is_untouched_by_the_split() -> None:
    """The split adds runners to CI; it must add nothing to the deploy path."""
    deploy = _job("deploy")
    assert "needs" not in deploy, "deploy must keep its documented no-needs shortcut"
    condition = _condition_source(deploy.get("if"))
    assert condition is not None and "refs/heads/main" in condition


# ===========================================================================
# 12. Local pytest and the nightly legacy workflow are unchanged
# ===========================================================================


def test_plain_pytest_is_still_the_whole_suite() -> None:
    tokens = _addopts_tokens()
    assert "-m" not in tokens, "a global -m would hide tests from every local run"
    assert not any(token.startswith("-m") and token != "-m" for token in tokens)
    assert MARKER not in " ".join(tokens)
    assert not any(token.startswith("--ignore") for token in tokens), "addopts must carry no shard filter"


def test_legacy_workflow_is_untouched_and_still_schedule_only() -> None:
    workflow = yaml.safe_load(LEGACY_WORKFLOW_FILE.read_text())
    triggers = _workflow_triggers(workflow)
    assert set(triggers) == {"schedule", "workflow_dispatch"}, f"unexpected triggers: {sorted(triggers)}"
    raw = LEGACY_WORKFLOW_FILE.read_text()
    for module in EXPECTED_HEAVY_MODULES:
        assert module not in raw, "the legacy workflow must not learn about the shards"
