"""PR-11.1 revision 16: the runbook's commands have to actually be runnable.

The first version told an operator to run the CLI inside
``altegio-easyweek-inbox-worker`` with paths under ``/opt/altegio_bot/outputs``.
That container has no mounts, so every one of those paths pointed at a directory
that did not exist inside it, and any report it managed to write died with the
container. The whole runbook was unexecutable.

These tests read ``docker-compose.yml`` and the runbook as data. They cannot
start containers, so what they prove is the contract: the one-off service exists,
it is gated behind the ``ops`` profile, its inputs are mounted read-only, its
report directory is writable, and the runbook's commands go through it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE = _REPO_ROOT / "docker-compose.yml"
RUNBOOK = _REPO_ROOT / "docs" / "easyweek" / "pr11_1_cutover_runbook.md"

SERVICE = "easyweek-booking-migration"
CONTAINER_INPUT = "/migration/input"
CONTAINER_REPORTS = "/migration/reports"


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def service(compose: dict) -> dict:
    assert SERVICE in compose["services"], "the one-off cutover runner must exist"
    return compose["services"][SERVICE]


def test_the_migration_service_is_behind_the_ops_profile(service: dict):
    """A plain `docker compose up -d` must never start a migration runner."""
    assert service.get("profiles") == ["ops"]
    assert service.get("restart") == "no"


def test_no_default_service_would_start_it(compose: dict):
    for name, definition in compose["services"].items():
        if name == SERVICE:
            continue
        assert SERVICE not in str(definition.get("depends_on", {}))


def test_it_runs_the_same_image_and_code_as_the_workers(compose: dict, service: dict):
    """Same build context as the workers: the tool is the deployed commit."""
    assert service.get("build") == compose["services"]["altegio-easyweek-inbox-worker"].get("build")


def test_it_reads_both_env_files(service: dict):
    env_files = service.get("env_file", [])
    assert ".env" in env_files
    # easyweek.env stays optional so a host without it does not fail to start.
    assert any(isinstance(entry, dict) and entry.get("path") == "easyweek.env" for entry in env_files)


def test_it_waits_for_postgres(service: dict):
    assert service["depends_on"]["postgres"]["condition"] == "service_healthy"


def test_inputs_are_mounted_read_only(service: dict):
    """The customer export is PII; the container has no reason to be able to write it."""
    mounts = service["volumes"]
    input_mount = next(m for m in mounts if m.endswith(f":{CONTAINER_INPUT}:ro"))
    assert input_mount.endswith(":ro")


def test_the_report_directory_is_mounted_writable_on_the_host(service: dict):
    """A report that only exists inside an ephemeral container is not a report."""
    mounts = service["volumes"]
    report_mount = next(m for m in mounts if m.endswith(f":{CONTAINER_REPORTS}"))
    assert not report_mount.endswith(":ro")
    # And the host side is under outputs/, which is git-ignored.
    assert "outputs/easyweek_migration" in report_mount


def test_the_container_writes_its_report_to_the_mounted_directory(service: dict):
    assert service["environment"]["EASYWEEK_MIGRATION_REPORT_DIR"] == CONTAINER_REPORTS


def test_the_entrypoint_is_the_migration_cli(service: dict):
    assert service["entrypoint"][-1] == "altegio_bot.scripts.easyweek_migration"
    # A bare `run` with no arguments must not do anything.
    assert service["command"] == ["--help"]


def test_the_compose_file_carries_no_secrets_or_pii(service: dict):
    blob = str(service)
    assert "EASYWEEK_API_KEY" not in blob
    assert "Bearer" not in blob
    assert not re.search(r"\+\d{9,}", blob)


# ---------------------------------------------------------------------------
# The runbook has to match
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def runbook() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _command_blocks(runbook: str) -> list[str]:
    return [block.strip() for block in re.findall(r"```bash\n(.*?)```", runbook, flags=re.DOTALL)]


def test_every_migration_command_goes_through_the_one_off_service(runbook: str):
    """No command may point the CLI at a container that has no mounts."""
    for block in _command_blocks(runbook):
        if "altegio_bot.scripts.easyweek_migration" not in block:
            continue
        assert SERVICE in block, f"migration command does not use the one-off service:\n{block}"
        assert "altegio-easyweek-inbox-worker" not in block


def test_runbook_paths_are_the_container_paths(runbook: str):
    """The old paths did not exist inside the container they were run in."""
    for block in _command_blocks(runbook):
        if "altegio_bot.scripts.easyweek_migration" not in block:
            continue
        if "--manifest" in block:
            assert f"--manifest {CONTAINER_INPUT}/" in block, block
        if "--customer-directory" in block:
            assert f"--customer-directory {CONTAINER_INPUT}/" in block, block
        # A host path handed to the CLI is the original bug: it named a
        # directory that does not exist inside the container. Host paths are
        # fine on the MOUNT side, which is what `export ...INPUT_DIR` sets.
        assert "--manifest /opt/" not in block, block
        assert "--customer-directory /opt/" not in block, block
        assert "--report-dir /opt/" not in block, block


def test_every_command_block_is_paste_clean(runbook: str):
    """One command per block, no comments, no prompts — the block is copyable."""
    for block in _command_blocks(runbook):
        assert not block.startswith("$"), block
        for line in block.splitlines():
            assert not line.strip().startswith("#"), block


def test_the_runbook_puts_rollback_inside_the_notification_off_window(runbook: str):
    """Re-enabling notifications before a rollback would message every customer."""
    rollback = runbook.index("## 19.")
    re_enable = runbook.index("## 20.")
    assert rollback < re_enable, "rollback must come before the re-enable step"
