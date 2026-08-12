"""Rollout contract for the EasyWeek worker: flags, Compose wiring, shutdown.

The whole point of PR-4's flag design is that deploying it changes nothing on a
production box that already runs ``EASYWEEK_ENABLED=true`` for capture. These
tests pin that: the gates are separate, they all default to off, and the worker
container stays inert and quiet until an operator deliberately turns it on.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import math
from pathlib import Path

import pytest
import yaml

from altegio_bot.settings import Settings, settings
from altegio_bot.workers import easyweek_inbox_worker as worker
from altegio_bot.workers import outbox_worker

_REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = _REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE = _REPO_ROOT / "easyweek.env.example"
WORKER_SERVICE = "altegio-easyweek-inbox-worker"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text())


def _worker_service() -> dict:
    services = _compose()["services"]
    assert WORKER_SERVICE in services, f"{WORKER_SERVICE} is missing from docker-compose.yml"
    return services[WORKER_SERVICE]


# ===========================================================================
# Three independent gates, all fail-closed
# ===========================================================================


@pytest.mark.parametrize(
    "field",
    ["easyweek_enabled", "easyweek_processing_enabled", "easyweek_notifications_enabled"],
)
def test_every_easyweek_gate_defaults_to_off(field: str) -> None:
    assert Settings.model_fields[field].default is False


def test_location_registry_defaults_to_unset() -> None:
    assert Settings.model_fields["easyweek_location_map"].default == "{}"


def test_service_category_allowlist_defaults_to_deny_all() -> None:
    assert Settings.model_fields["easyweek_allowed_service_categories"].default == ""


def test_processing_is_a_separate_field_from_capture() -> None:
    """Capture and processing must not share a flag.

    Production already runs capture with EASYWEEK_ENABLED=true; a worker gated
    on that same flag would sweep the whole captured backlog on deploy.
    """
    assert "easyweek_processing_enabled" in Settings.model_fields
    assert "easyweek_enabled" in Settings.model_fields


def test_worker_reads_only_the_processing_gate_not_the_capture_gate() -> None:
    """The worker must never read `settings.easyweek_enabled`.

    That flag belongs to the capture endpoint alone; reading it here would
    re-couple processing to capture, which is exactly what PR-4 separates.
    """
    source = inspect.getsource(worker)
    assert "settings.easyweek_processing_enabled" in source
    assert "settings.easyweek_enabled" not in source, "the worker must not gate itself on the capture flag"


@pytest.mark.parametrize(
    ("processing", "location_map", "expected"),
    [
        (
            True,
            '{"test":{"location_id":999001,"location_uuid":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","meta_template_prefix":"tt","booking_page_url":"https://booking.example.invalid/test"}}',
            True,
        ),
        (True, "{}", False),
        (True, "{not json", False),
        (
            False,
            '{"test":{"location_id":999001,"location_uuid":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","meta_template_prefix":"tt","booking_page_url":"https://booking.example.invalid/test"}}',
            False,
        ),
        (False, "{}", False),
    ],
)
def test_processing_requires_both_the_flag_and_a_valid_registry(
    monkeypatch: pytest.MonkeyPatch, processing: bool, location_map: str, expected: bool
) -> None:
    """A configured location is as mandatory as the flag itself.

    Without it the worker could not tell its own location from a foreign one.
    """
    monkeypatch.setattr(settings, "easyweek_processing_enabled", processing, raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map, raising=False)
    assert worker.processing_is_configured() is expected


def test_disabling_processing_does_not_disable_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)
    assert settings.easyweek_enabled is True, "capture must survive processing being turned off"
    assert worker.processing_is_configured() is False


def test_production_location_identities_are_not_hardcoded_in_python() -> None:
    forbidden = ("305156", "308697", "315607", "b9d689f2", "cd91816d")
    for path in (_REPO_ROOT / "src" / "altegio_bot").rglob("*.py"):
        if "tests" in path.parts:
            continue
        source = path.read_text()
        for value in forbidden:
            assert value not in source, f"production location identity hardcoded in {path}"


# ===========================================================================
# easyweek.env.example
# ===========================================================================


def test_env_example_documents_every_new_flag() -> None:
    text = ENV_EXAMPLE.read_text()
    for key in (
        "EASYWEEK_PROCESSING_ENABLED",
        "EASYWEEK_NOTIFICATIONS_ENABLED",
        "EASYWEEK_ALLOWED_SERVICE_CATEGORIES",
        "EASYWEEK_LOCATION_MAP",
    ):
        assert key in text, f"{key} is undocumented in easyweek.env.example"


def test_env_example_ships_fail_closed_values() -> None:
    text = ENV_EXAMPLE.read_text()
    assert "EASYWEEK_PROCESSING_ENABLED=false" in text
    assert "EASYWEEK_NOTIFICATIONS_ENABLED=false" in text
    assert "EASYWEEK_ALLOWED_SERVICE_CATEGORIES=[]" in text
    assert "EASYWEEK_LOCATION_MAP={}" in text
    assert "EASYWEEK_LOCATION_ID" not in text
    assert "EASYWEEK_LOCATION_UUID" not in text
    assert "EASYWEEK_BOOKING_PAGE_URL" not in text


def test_env_example_carries_no_real_secrets_or_production_values() -> None:
    text = ENV_EXAMPLE.read_text()
    for value in ("305156", "308697", "315607", "a02a61bf", "b9d689f2", "cd91816d"):
        assert value not in text, "a real location identity must not be committed"

    assignments = {
        line.split("=", 1)[0]: line.split("=", 1)[1]
        for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    for key in ("EASYWEEK_WEBHOOK_SECRET", "EASYWEEK_API_KEY", "EASYWEEK_WORKSPACE_SLUG"):
        assert key in assignments, f"{key} is missing from easyweek.env.example"
        assert assignments[key] == "", f"{key} must ship empty, got {assignments[key]!r}"


# ===========================================================================
# Compose service
# ===========================================================================


def test_worker_has_its_own_compose_service() -> None:
    service = _worker_service()
    assert service["command"] == [
        "/app/.venv/bin/python",
        "-m",
        "altegio_bot.scripts.run_easyweek_inbox_worker",
    ]


def test_worker_reads_env_and_optional_easyweek_env() -> None:
    """easyweek.env must be optional, or a host without it fails to deploy."""
    env_file = _worker_service()["env_file"]
    assert ".env" in env_file
    optional = [entry for entry in env_file if isinstance(entry, dict)]
    assert len(optional) == 1
    assert optional[0]["path"] == "easyweek.env"
    assert optional[0]["required"] is False


def test_outbox_worker_reads_env_and_optional_easyweek_env() -> None:
    """The outbox worker renders EasyWeek jobs, so it needs easyweek.env too.

    ``settings.easyweek_location_map`` and ``settings.easyweek_default_language``
    are read inside ``outbox_worker``. Those live in ``easyweek.env``, which is
    deliberately NOT copied into the image, so without this entry the worker
    would silently fall back to ``""`` — an empty booking page — and fail every
    EasyWeek lifecycle job locally.

    ``required: false`` is not decoration: an Altegio-only host has no
    ``easyweek.env`` and must still deploy the shared outbox worker.
    """
    env_file = _compose()["services"]["altegio-outbox-worker"]["env_file"]
    assert env_file == [
        ".env",
        {"path": "easyweek.env", "required": False},
    ]


def test_both_category_guard_consumers_read_the_shared_setting() -> None:
    assert "settings.easyweek_allowed_service_categories" in inspect.getsource(worker)
    assert "settings.easyweek_allowed_service_categories" in inspect.getsource(outbox_worker)


@pytest.mark.parametrize(
    "service",
    [
        "altegio-inbox-worker",
        "altegio-whatsapp-inbox-worker",
        "altegio-meta-guard-worker",
        "altegio-campaign-worker",
        "altegio-followup-worker",
    ],
)
def test_unrelated_workers_do_not_gain_easyweek_env(service: str) -> None:
    """Only the two services that actually touch EasyWeek get its secrets."""
    assert _compose()["services"][service]["env_file"] == ".env"


def test_worker_waits_for_a_healthy_database() -> None:
    depends = _worker_service()["depends_on"]
    assert depends["postgres"]["condition"] == "service_healthy"


def test_worker_restarts_like_the_other_workers() -> None:
    assert _worker_service()["restart"] == "always"


def test_the_altegio_services_are_unchanged() -> None:
    """PR-4 adds a service; it must not touch the live Altegio ones."""
    services = _compose()["services"]
    altegio_inbox = services["altegio-inbox-worker"]
    assert altegio_inbox["command"] == [
        "/app/.venv/bin/python",
        "-m",
        "altegio_bot.scripts.run_inbox_worker",
    ]
    assert altegio_inbox["env_file"] == ".env", "the Altegio worker must not gain easyweek.env"


def test_migrate_service_still_has_the_ops_profile() -> None:
    assert _compose()["services"]["migrate"]["profiles"] == ["ops"]


# ===========================================================================
# Inert, quiet, and gracefully stoppable
# ===========================================================================


async def _run_briefly(stop_after: float = 0.15, **kwargs) -> None:
    stop_event = asyncio.Event()
    task = asyncio.create_task(worker.run_loop(poll_sec=0.01, stop_event=stop_event, **kwargs))
    await asyncio.sleep(stop_after)
    stop_event.set()
    await asyncio.wait_for(task, timeout=2.0)


def _set_ready_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test": {
                    "location_id": 999001,
                    "location_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                    "meta_template_prefix": "tt",
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )


@pytest.mark.asyncio
async def test_disabled_worker_never_claims_and_never_busy_loops(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)

    claims = 0

    async def _never_called() -> bool:
        nonlocal claims
        claims += 1
        return False

    monkeypatch.setattr(worker, "process_one", _never_called)

    with caplog.at_level("INFO", logger="easyweek_inbox_worker"):
        await _run_briefly()

    assert claims == 0, "a disabled worker must not claim anything"
    # The disabled state is announced once, not once per poll.
    disabled_lines = [r for r in caplog.records if "processing is disabled" in r.getMessage()]
    assert len(disabled_lines) == 1, f"log spam: {len(disabled_lines)} disabled notices"


@pytest.mark.asyncio
async def test_worker_stops_promptly_on_the_stop_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """SIGTERM must not cost a whole polling interval of dead time."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)
    stop_event = asyncio.Event()
    task = asyncio.create_task(worker.run_loop(poll_sec=30.0, stop_event=stop_event))
    await asyncio.sleep(0.05)
    stop_event.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert task.done()


@pytest.mark.asyncio
async def test_stop_is_checked_before_claiming_not_mid_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    """A claim already in flight is always finished."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    _set_ready_registry(monkeypatch)

    stop_event = asyncio.Event()
    started = asyncio.Event()
    finished = False

    async def _slow_process() -> bool:
        nonlocal finished
        started.set()
        await asyncio.sleep(0.05)
        finished = True
        return True

    monkeypatch.setattr(worker, "process_one", _slow_process)

    task = asyncio.create_task(worker.run_loop(poll_sec=0.01, stop_event=stop_event))
    await started.wait()
    stop_event.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert finished is True, "an in-flight cycle was abandoned"


def test_entrypoint_module_exists_and_calls_main() -> None:
    entrypoint = _REPO_ROOT / "src" / "altegio_bot" / "scripts" / "run_easyweek_inbox_worker.py"
    source = entrypoint.read_text()
    assert "from altegio_bot.workers.easyweek_inbox_worker import main" in source
    assert "main()" in source


def test_worker_installs_signal_handlers() -> None:
    source = inspect.getsource(worker)
    assert "SIGTERM" in source and "SIGINT" in source
    assert "add_signal_handler" in source


# ===========================================================================
# Unexpected errors must not leak PII or kill the worker (review fix 5)
# ===========================================================================

_PII_PHONE = "+4915112345678"
_PII_EMAIL = "real.customer@example.com"
_PII_NAME = "Erika Mustermann"


class _FakeDBError(Exception):
    """Shaped like a SQLAlchemy error: renders the statement WITH parameters."""

    def __str__(self) -> str:
        return (
            "(asyncpg.exceptions.DataError) invalid input\n"
            "[SQL: INSERT INTO clients (phone_e164, email, display_name) VALUES ($1, $2, $3)]\n"
            f"[parameters: ('{_PII_PHONE}', '{_PII_EMAIL}', '{_PII_NAME}')]"
        )


@pytest.mark.asyncio
async def test_unexpected_error_logs_no_pii_and_no_traceback(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    _set_ready_registry(monkeypatch)

    calls = {"n": 0}

    async def _explode() -> bool:
        calls["n"] += 1
        raise _FakeDBError()

    monkeypatch.setattr(worker, "process_one", _explode)

    stop_event = asyncio.Event()
    with caplog.at_level("DEBUG", logger="easyweek_inbox_worker"):
        task = asyncio.create_task(worker.run_loop(poll_sec=0.01, stop_event=stop_event))
        await asyncio.sleep(0.12)
        stop_event.set()
        await asyncio.wait_for(task, timeout=3.0)

    assert calls["n"] >= 1, "the loop never reached the failing cycle"

    text = "\n".join(record.getMessage() for record in caplog.records)
    for secret in (_PII_PHONE, _PII_EMAIL, _PII_NAME, "parameters:", "INSERT INTO clients"):
        assert secret not in text, f"PII/statement leaked into the log: {secret!r}"

    assert "processing_error" in text, "the failure must still be reported"
    assert "_FakeDBError" in text, "the exception class name is the safe detail"

    # No traceback was attached to any record.
    for record in caplog.records:
        assert record.exc_info is None, "a traceback was logged"


@pytest.mark.asyncio
async def test_worker_survives_a_failing_cycle_and_keeps_going(monkeypatch: pytest.MonkeyPatch) -> None:
    """One poisoned row must not kill the process or wedge the backlog."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    _set_ready_registry(monkeypatch)

    attempts = {"n": 0}

    async def _fail_then_succeed() -> bool:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise _FakeDBError()
        return False

    monkeypatch.setattr(worker, "process_one", _fail_then_succeed)
    monkeypatch.setattr(worker, "MAX_ERROR_BACKOFF_SEC", 0.02, raising=False)

    stop_event = asyncio.Event()
    task = asyncio.create_task(worker.run_loop(poll_sec=0.01, stop_event=stop_event))
    await asyncio.sleep(0.2)
    stop_event.set()
    await asyncio.wait_for(task, timeout=3.0)

    assert task.done() and task.exception() is None, "the worker died on a transient error"
    assert attempts["n"] >= 2, "the worker did not retry after the failure"


@pytest.mark.asyncio
async def test_transient_errors_back_off_instead_of_hot_looping(monkeypatch: pytest.MonkeyPatch) -> None:
    """A permanently failing row must not spin the loop at full speed."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    _set_ready_registry(monkeypatch)

    attempts = {"n": 0}

    async def _always_fail() -> bool:
        attempts["n"] += 1
        raise _FakeDBError()

    monkeypatch.setattr(worker, "process_one", _always_fail)

    stop_event = asyncio.Event()
    task = asyncio.create_task(worker.run_loop(poll_sec=0.01, stop_event=stop_event))
    await asyncio.sleep(0.25)
    stop_event.set()
    await asyncio.wait_for(task, timeout=5.0)

    # Without backoff a 0.01s poll over 0.25s would burn through far more.
    assert attempts["n"] <= 8, f"no backoff: {attempts['n']} attempts in 0.25s"


@pytest.mark.asyncio
async def test_shutdown_signals_are_never_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """`except Exception` must not catch CancelledError or SystemExit."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    _set_ready_registry(monkeypatch)

    async def _system_exit() -> bool:
        raise SystemExit(3)

    monkeypatch.setattr(worker, "process_one", _system_exit)

    with pytest.raises(SystemExit):
        await worker.run_loop(poll_sec=0.01, stop_event=asyncio.Event())


def _worker_statements() -> str:
    """The worker's executable code, with comments AND docstrings removed.

    Prose describing the ban ("never as text or a traceback") would otherwise
    satisfy a substring check meant to inspect the statements.
    """
    tree = ast.parse(inspect.getsource(worker))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
    return ast.unparse(tree)


def test_the_worker_never_uses_unsafe_logging_calls() -> None:
    code = _worker_statements()
    assert "logger.exception" not in code, "logger.exception attaches a traceback"
    assert "str(exc)" not in code, "the exception text can contain SQL parameters"
    assert "traceback" not in code
    assert "type(exc).__name__" in code, "only the class name is safe to log"


def test_the_worker_catches_exception_not_baseexception() -> None:
    """CancelledError / KeyboardInterrupt / SystemExit must keep propagating."""
    code = _worker_statements()
    assert "except Exception" in code
    assert "except BaseException" not in code


# ===========================================================================
# Production deploy verification (review fix 9)
# ===========================================================================

WORKFLOW_FILE = _REPO_ROOT / ".github" / "workflows" / "ci_deploy.yml"
DEPLOY_SCRIPT = _REPO_ROOT / "scripts" / "deploy_pr3.sh"


def _verification_script() -> str:
    workflow = yaml.safe_load(WORKFLOW_FILE.read_text())
    steps = workflow["jobs"]["deploy"]["steps"]
    step = next(s for s in steps if s.get("name") == "Verify deployment on server")
    return str(step["with"]["script"])


def test_the_easyweek_worker_is_a_critical_post_deploy_service() -> None:
    """A standing service that is never verified can die unnoticed."""
    import re

    script = _verification_script()
    match = re.search(r'CRITICAL_SERVICES="\n(?P<body>.*?)\n"', script, re.DOTALL)
    assert match is not None, "CRITICAL_SERVICES is missing from the verification step"
    services = {line.strip() for line in match.group("body").splitlines() if line.strip()}
    assert WORKER_SERVICE in services, f"{WORKER_SERVICE} is not verified after deploy"


def test_critical_service_check_fails_on_a_non_running_or_unhealthy_container() -> None:
    script = _verification_script()
    assert "state=${STATE}" in script or "state=" in script
    assert 'if [ "$STATE" != "running" ]; then' in script
    assert 'if [ "$HEALTH" = "unhealthy" ]; then' in script
    assert "RestartCount" in script, "a restart loop must be visible"


def test_every_compose_standing_service_is_verified_after_deploy() -> None:
    """Guards against the next added worker being forgotten the same way."""
    import re

    compose = _compose()["services"]
    standing = {
        name
        for name, service in compose.items()
        # `migrate` has profile `ops` and is a one-shot, not a standing service.
        if "ops" not in (service.get("profiles") or [])
    }
    script = _verification_script()
    match = re.search(r'CRITICAL_SERVICES="\n(?P<body>.*?)\n"', script, re.DOTALL)
    assert match is not None
    verified = {line.strip() for line in match.group("body").splitlines() if line.strip()}
    assert standing <= verified, f"unverified standing services: {sorted(standing - verified)}"


def test_deploy_script_takes_the_ordinary_path_once_pr3_is_applied() -> None:
    """Production is already on the PR-3 revision, so PR-4 is a normal deploy.

    The one-time constraint-swap window (worker drain, canary, bounded
    rollback) must NOT re-arm for a deploy that merely adds a later revision.
    """
    script = DEPLOY_SCRIPT.read_text()
    assert 'if [ "$PR3_IN_DB_LINEAGE" = "1" ]; then' in script
    branch = script.split('if [ "$PR3_IN_DB_LINEAGE" = "1" ]; then', 1)[1].split("elif", 1)[0]
    assert "PR3_TRANSITION=0" in branch
    assert "PR3_TRANSITION=1" not in branch


def test_deploy_script_keeps_backup_and_revision_verification() -> None:
    script = DEPLOY_SCRIPT.read_text()
    assert "pg_dump" in script
    assert "alembic_revision_facts" in script
    assert 'test "$DEPLOYED_SHA" = "${{ github.sha }}"' not in script  # that lives in the workflow
    assert "REVISION_AFTER" in script


def test_new_services_are_created_by_the_ordinary_compose_up() -> None:
    assert "$COMPOSE up -d --remove-orphans" in DEPLOY_SCRIPT.read_text()


# ===========================================================================
# Loop-level backoff must stay finite for any failure count
# ===========================================================================
#
# `min(base * 2**failures, cap)` evaluates the exponent BEFORE min sees it, so
# around 1024 the product stops fitting a float and raises OverflowError —
# inside the handler whose whole job is to keep the worker alive through a long
# outage.


@pytest.mark.parametrize(("failures", "expected"), [(0, 1.0), (1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0)])
def test_small_failure_counts_keep_the_previous_doubling(failures, expected) -> None:
    assert worker.loop_backoff_delay(failures, 1.0) == expected


def test_the_delay_reaches_and_holds_the_cap() -> None:
    assert worker.loop_backoff_delay(5, 1.0) == worker.MAX_ERROR_BACKOFF_SEC
    assert worker.loop_backoff_delay(50, 1.0) == worker.MAX_ERROR_BACKOFF_SEC


@pytest.mark.parametrize("failures", [1024, 10_000, 1_000_000])
def test_a_huge_failure_count_does_not_overflow(failures) -> None:
    """The exact regression: this used to raise OverflowError."""
    # Confirm the old expression really would have blown up.
    with pytest.raises(OverflowError):
        min(1.0 * (2**failures), worker.MAX_ERROR_BACKOFF_SEC)

    delay = worker.loop_backoff_delay(failures, 1.0)
    assert math.isfinite(delay)
    assert 0 < delay <= worker.MAX_ERROR_BACKOFF_SEC


@pytest.mark.parametrize("base", [0.01, 0.5, 1.0, 7.5, 29.9, 30.0, 120.0])
@pytest.mark.parametrize("failures", [0, 1, 7, 64, 4096])
def test_the_delay_is_always_finite_and_within_bounds(base, failures) -> None:
    delay = worker.loop_backoff_delay(failures, base)
    assert math.isfinite(delay)
    assert 0 < delay <= worker.MAX_ERROR_BACKOFF_SEC


def test_a_non_positive_base_falls_back_to_a_sane_one() -> None:
    for base in (0.0, -1.0):
        delay = worker.loop_backoff_delay(3, base)
        assert 0 < delay <= worker.MAX_ERROR_BACKOFF_SEC


def test_both_loop_error_paths_use_the_shared_helper() -> None:
    """Neither handler may keep its own unbounded exponent."""
    import inspect

    source = inspect.getsource(worker.run_loop)
    assert source.count("loop_backoff_delay(") == 2
    assert "2**" not in source, "an unbounded exponent is back in the loop"


# ===========================================================================
# Runbook contract: the candidate count is not a rollout gate
# ===========================================================================


def _runbook() -> str:
    return (Path(__file__).resolve().parents[3] / "docs" / "easyweek" / "pr4_normalizer_runbook.md").read_text()


def test_runbook_does_not_equate_candidate_count_with_repaired() -> None:
    """`repaired` counts only PARSEABLE ids, so equality was never true.

    A `public-deploy-smoke-<uuid>` row is a JSON string — counted as a candidate
    — but `uuid.UUID()` cannot parse it, so it is never repaired.
    """
    text = _runbook()
    assert "string-кандидатов" in text
    assert "repaired <= число string-кандидатов" in text
    assert "НЕ является gate" in text
    # The three real gate conditions.
    assert "easyweek reconcile complete" in text
    assert "reconcile_error` не повторяется" in text


def test_runbook_forbids_unsafe_uuid_casts_and_rival_regexes() -> None:
    """One source of truth for parsing: Python `uuid.UUID` via the helper."""
    text = _runbook()
    assert "(payload->>'uid')::uuid" not in text.replace(
        "Не пытаться уточнить счётчик небезопасным приведением `(payload->>'uid')::uuid`", ""
    )
    assert "canonical_booking_uuid()" in text


def test_runbook_documents_the_write_fence_and_rollback_order() -> None:
    text = _runbook()
    assert "write fence" in text
    assert "один контейнер `altegio-api`" in text
    assert "--force-recreate altegio-api" in text
    # Rollback: processing off and worker stopped BEFORE the API is rolled back.
    rollback = text[text.index("#### Rollback API на старый image") :]
    steps = rollback.index("EASYWEEK_PROCESSING_ENABLED=false")
    worker_stop = rollback.index("перестал claim")
    api_rollback = rollback.index("откатывать `altegio-api`")
    assert steps < worker_stop < api_rollback
    # The limitation is stated, not glossed over.
    assert "НЕ поддерживает одновременно" in text
    assert "rolling API replicas" in text
