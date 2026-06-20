"""Tests that the follow-up worker is wired into deployment.

Covers the Run #23 root cause: the worker code existed but had no compose
service, so it never ran in production.

  1. docker-compose.yml has an altegio-followup-worker service with the right
     command, restart policy, env_file and postgres health dependency.
  2. The worker is NOT attached to the Chatwoot internal network (it makes no
     Chatwoot API calls).
  3. The script entrypoint imports and calls the worker run_loop.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

# Repo root: .../altegio_bot (contains docker-compose.yml). This file lives at
# src/altegio_bot/tests/campaigns/, so go up 4 levels.
_REPO_ROOT = Path(__file__).resolve().parents[4]
_COMPOSE = _REPO_ROOT / "docker-compose.yml"
_COMPOSE_CHATWOOT = _REPO_ROOT / "docker-compose.chatwoot-internal.yml"

_SERVICE = "altegio-followup-worker"
_MODULE = "altegio_bot.scripts.run_followup_worker"

try:
    import yaml  # type: ignore

    _HAS_YAML = True
except ImportError:  # pragma: no cover - PyYAML is a dev dependency
    _HAS_YAML = False


def test_compose_file_exists() -> None:
    assert _COMPOSE.is_file(), f"docker-compose.yml not found at {_COMPOSE}"


@pytest.mark.skipif(not _HAS_YAML, reason="PyYAML not available")
def test_compose_has_followup_worker_service_yaml() -> None:
    config = yaml.safe_load(_COMPOSE.read_text())
    services = config.get("services", {})
    assert _SERVICE in services, f"{_SERVICE} missing from docker-compose.yml services"

    svc = services[_SERVICE]
    # Command runs the script entrypoint module.
    assert _MODULE in " ".join(svc["command"])
    # Same operational policy as the other workers.
    assert svc["restart"] == "always"
    assert svc["env_file"] == ".env"
    # Waits for a healthy database before starting.
    depends = svc["depends_on"]
    assert depends["postgres"]["condition"] == "service_healthy"


def test_compose_has_followup_worker_service_text() -> None:
    """Text-based fallback assertions (work even without PyYAML)."""
    text = _COMPOSE.read_text()
    assert _SERVICE in text
    assert _MODULE in text


@pytest.mark.skipif(not _HAS_YAML, reason="PyYAML not available")
def test_followup_worker_not_on_chatwoot_network() -> None:
    """The follow-up worker creates MessageJob rows only — no Chatwoot API."""
    if not _COMPOSE_CHATWOOT.is_file():
        pytest.skip("no chatwoot-internal override file")
    config = yaml.safe_load(_COMPOSE_CHATWOOT.read_text())
    services = config.get("services", {})
    assert _SERVICE not in services, f"{_SERVICE} must NOT be attached to the Chatwoot internal network"


def test_entrypoint_calls_worker_run_loop() -> None:
    import altegio_bot.scripts.run_followup_worker as entry
    from altegio_bot.workers import followup_worker

    # Entrypoint reuses the worker's run_loop (single source of truth).
    assert entry.run_loop is followup_worker.run_loop
    assert inspect.iscoroutinefunction(entry.main)

    # main() awaits run_loop — verify by source inspection (lightweight).
    src = inspect.getsource(entry.main)
    assert "run_loop()" in src
