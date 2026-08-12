"""The legacy-retirement command in the runbook must actually run.

The previous version told the operator to run the helper inside `altegio-api`.
That could never work: the API image is built from the *previous* commit, so it
does not contain the module; `python:3.12-slim` has no Docker CLI; and
`altegio-api` has no Docker socket. The helper needs the new code, a Docker
client and a database connection in one process.

This suite BUILDS the ops image and runs commands in it. It is slow and needs a
working Docker daemon — deliberately, because the failure it guards against is
precisely "the documented command does not execute".

Nothing here touches production: the containers are one-off, run with
`--entrypoint`, and only ever read.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
OPS_DOCKERFILE = REPO_ROOT / "Dockerfile.ops"
ACTIVATION_RUNBOOK = REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md"

OPS_SERVICE = "easyweek-legacy-retire"
WA_SERVICE = "altegio-whatsapp-inbox-worker"
HELPER_MODULE = "altegio_bot.scripts.retire_legacy_whatsapp_worker"
OPS_IMAGE_TAG = "altegio-bot-ops-test:pytest"

DOCKER_MISSING = shutil.which("docker") is None


def _docker_available() -> bool:
    if DOCKER_MISSING:
        return False
    return subprocess.run(["docker", "info"], capture_output=True, timeout=120).returncode == 0


requires_docker = pytest.mark.skipif(not _docker_available(), reason="Docker daemon is not reachable")


@pytest.fixture(scope="module")
def ops_image() -> str:
    """Build the real ops image from Dockerfile.ops."""
    build = subprocess.run(
        ["docker", "build", "-f", str(OPS_DOCKERFILE), "-t", OPS_IMAGE_TAG, str(REPO_ROOT)],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    assert build.returncode == 0, f"ops image build failed:\n{build.stderr[-3000:]}"
    return OPS_IMAGE_TAG


def _run_in_ops(image: str, *args: str, mount_socket: bool = False) -> subprocess.CompletedProcess[str]:
    command = ["docker", "run", "--rm", "--entrypoint", "/app/.venv/bin/python"]
    if mount_socket:
        command += ["-v", "/var/run/docker.sock:/var/run/docker.sock"]
    command += [image, *args]
    return subprocess.run(command, capture_output=True, text=True, timeout=600)


# ---------------------------------------------------------------------------
# Compose wiring — no Docker needed
# ---------------------------------------------------------------------------


def test_the_ops_service_exists_behind_the_ops_profile() -> None:
    services = yaml.safe_load(COMPOSE_FILE.read_text())["services"]

    assert OPS_SERVICE in services, "the runbook command needs a real service to run"
    assert services[OPS_SERVICE]["profiles"] == ["ops"], "a plain `up -d` must never start it"
    assert services[OPS_SERVICE]["build"]["dockerfile"] == "Dockerfile.ops"
    assert services[OPS_SERVICE]["restart"] == "no"


def test_only_the_ephemeral_ops_service_gets_the_docker_socket() -> None:
    """A Docker socket on a long-running service is a standing root escalation."""
    services = yaml.safe_load(COMPOSE_FILE.read_text())["services"]

    with_socket = {
        name
        for name, spec in services.items()
        if any("docker.sock" in str(volume) for volume in spec.get("volumes", []) or [])
    }
    assert with_socket == {OPS_SERVICE}, f"unexpected services mount the Docker socket: {with_socket}"


def test_the_api_image_carries_no_docker_client() -> None:
    assert "docker" not in (REPO_ROOT / "Dockerfile").read_text().lower().split("from")[0]
    api_dockerfile = (REPO_ROOT / "Dockerfile").read_text()
    assert "docker-cli" not in api_dockerfile
    assert "/usr/local/bin/docker" not in api_dockerfile


def test_the_runbook_documents_the_ops_command_not_the_api_one() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert OPS_SERVICE in text
    assert "--profile ops" in text
    assert "--build" in text, "the helper must be built from the deployed commit"
    # The old, unrunnable command must be gone.
    assert f"altegio-api \\\n  -m {HELPER_MODULE}" not in text
    assert "--container <id>" not in text, "no unverified container id may be pasted"


# ---------------------------------------------------------------------------
# The image itself — requires Docker
# ---------------------------------------------------------------------------


@requires_docker
def test_the_ops_image_contains_the_helper_module(ops_image: str) -> None:
    result = _run_in_ops(ops_image, "-c", f"import {HELPER_MODULE} as m; print(m.WA_SERVICE)")

    assert result.returncode == 0, result.stderr[-2000:]
    assert WA_SERVICE in result.stdout


@requires_docker
def test_the_ops_image_contains_a_working_docker_cli(ops_image: str) -> None:
    result = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "docker", ops_image, "--version"],
        capture_output=True,
        text=True,
        timeout=300,
    )

    assert result.returncode == 0, result.stderr[-2000:]
    assert "Docker version" in result.stdout


@requires_docker
def test_the_ops_container_reaches_the_daemon_only_with_the_socket(ops_image: str) -> None:
    """The socket is what makes the client useful — and it is opt-in."""
    without = _run_in_ops(ops_image, "-c", f"import {HELPER_MODULE} as m; print(m.DockerCli().service_container_ids())")
    assert "None" in without.stdout, "no socket must read as a discovery failure, not as an empty list"

    with_socket = _run_in_ops(
        ops_image,
        "-c",
        (
            f"import {HELPER_MODULE} as m;"
            "ids = m.DockerCli().service_container_ids();"
            "print('DISCOVERY_OK' if ids is not None else 'FAILED')"
        ),
        mount_socket=True,
    )
    assert "DISCOVERY_OK" in with_socket.stdout, with_socket.stderr[-2000:]


@requires_docker
def test_the_probe_is_read_only_against_a_real_daemon(ops_image: str) -> None:
    """It reports what it found and pauses/removes nothing."""
    result = _run_in_ops(
        ops_image,
        "-c",
        (f"import {HELPER_MODULE} as m;d = m.DockerCli();r = m.probe_ops_runtime(d);print(r)"),
        mount_socket=True,
    )

    assert result.returncode == 0, result.stderr[-2000:]
    assert "'docker_cli': True" in result.stdout
    assert "'docker_daemon': True" in result.stdout
    # No side effects: the probe only lists.
    assert "pause" not in result.stdout
    assert "removed" not in result.stdout


@requires_docker
def test_the_default_command_is_the_read_only_probe() -> None:
    """A bare `run` must not be able to retire anything."""
    dockerfile = OPS_DOCKERFILE.read_text()

    assert '"--probe"' in dockerfile, "the image default must be the probe"
    cmd_line = next(line for line in dockerfile.splitlines() if line.startswith("CMD"))
    assert "--probe" in cmd_line
