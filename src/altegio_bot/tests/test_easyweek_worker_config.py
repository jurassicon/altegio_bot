"""Rollout contract for the EasyWeek worker: flags, Compose wiring, shutdown.

The whole point of PR-4's flag design is that deploying it changes nothing on a
production box that already runs ``EASYWEEK_ENABLED=true`` for capture. These
tests pin that: the gates are separate, they all default to off, and the worker
container stays inert and quiet until an operator deliberately turns it on.
"""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

import pytest
import yaml

from altegio_bot.settings import Settings, settings
from altegio_bot.workers import easyweek_inbox_worker as worker

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


def test_location_id_defaults_to_unset() -> None:
    assert Settings.model_fields["easyweek_location_id"].default == 0


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
    ("processing", "location", "expected"),
    [
        (True, 305156, True),
        (True, 0, False),
        (True, -1, False),
        (False, 305156, False),
        (False, 0, False),
    ],
)
def test_processing_requires_both_the_flag_and_a_location(
    monkeypatch: pytest.MonkeyPatch, processing: bool, location: int, expected: bool
) -> None:
    """A configured location is as mandatory as the flag itself.

    Without it the worker could not tell its own location from a foreign one.
    """
    monkeypatch.setattr(settings, "easyweek_processing_enabled", processing, raising=False)
    monkeypatch.setattr(settings, "easyweek_location_id", location, raising=False)
    assert worker.processing_is_configured() is expected


def test_disabling_processing_does_not_disable_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)
    assert settings.easyweek_enabled is True, "capture must survive processing being turned off"
    assert worker.processing_is_configured() is False


def test_the_production_location_id_is_not_hardcoded_in_python() -> None:
    """305156 belongs in easyweek.env, never in the package."""
    for path in (_REPO_ROOT / "src" / "altegio_bot").rglob("*.py"):
        if "tests" in path.parts:
            continue
        assert "305156" not in path.read_text(), f"production location id hardcoded in {path}"


# ===========================================================================
# easyweek.env.example
# ===========================================================================


def test_env_example_documents_every_new_flag() -> None:
    text = ENV_EXAMPLE.read_text()
    for key in (
        "EASYWEEK_PROCESSING_ENABLED",
        "EASYWEEK_NOTIFICATIONS_ENABLED",
        "EASYWEEK_LOCATION_ID",
    ):
        assert key in text, f"{key} is undocumented in easyweek.env.example"


def test_env_example_ships_fail_closed_values() -> None:
    text = ENV_EXAMPLE.read_text()
    assert "EASYWEEK_PROCESSING_ENABLED=false" in text
    assert "EASYWEEK_NOTIFICATIONS_ENABLED=false" in text
    assert "EASYWEEK_LOCATION_ID=0" in text


def test_env_example_carries_no_real_secrets_or_production_values() -> None:
    text = ENV_EXAMPLE.read_text()
    assert "305156" not in text, "the real location id must not be committed"
    assert "a02a61bf" not in text, "the real location uuid must not be committed"

    assignments = {
        line.split("=", 1)[0]: line.split("=", 1)[1]
        for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    for key in ("EASYWEEK_WEBHOOK_SECRET", "EASYWEEK_API_KEY", "EASYWEEK_WORKSPACE_SLUG", "EASYWEEK_LOCATION_UUID"):
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
    monkeypatch.setattr(settings, "easyweek_location_id", 999001, raising=False)

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
