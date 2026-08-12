"""Executable contract of the WhatsApp worker retirement, run as real shell.

`scripts/lib/whatsapp_drain.sh` decides whether the delivery-retry producer may
be taken down, and every branch of that decision is a production outcome:
letting a legacy container be SIGTERM'd strands events forever, and accepting a
SIGKILL'd container as "drained" hides the same damage behind a green deploy.

So these tests do not read the script — they SOURCE it and call its functions
with a fake `docker` on PATH and a stubbed `psql_scalar`, asserting the exit
status and the exact commands that were and were not issued.

No Docker, no database, no network: the fake records its argv to a log file and
answers from environment variables.
"""

from __future__ import annotations

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DRAIN_LIB = REPO_ROOT / "scripts" / "lib" / "whatsapp_drain.sh"
DEPLOY_SCRIPT = REPO_ROOT / "scripts" / "deploy_pr3.sh"

FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG"

case "$1" in
  ps)
    [ "$FAKE_PS_FAILS" = "1" ] && exit 1
    printf '%s\n' "$FAKE_CONTAINER_IDS"
    ;;
  inspect)
    fmt="$3"
    current_state="$FAKE_STATE"
    if [ -s "$FAKE_STATE_FILE" ]; then current_state="$(cat "$FAKE_STATE_FILE")"; fi
    case "$fmt" in
      *State.Status*)   printf '%s\n' "$current_state" ;;
      *State.ExitCode*) printf '%s\n' "$FAKE_EXIT_CODE" ;;
      *State.OOMKilled*) printf '%s\n' "$FAKE_OOM" ;;
      *State.Error*)    printf '%s\n' "$FAKE_ERROR" ;;
      *State.Paused*)     printf '%s\n' "$FAKE_PAUSED" ;;
      *State.Restarting*) printf '%s\n' "$FAKE_RESTARTING" ;;
      *oneoff*)         printf '%s\n' "$FAKE_ONEOFF" ;;
      *)                printf '\n' ;;
    esac
    [ "$FAKE_INSPECT_FAILS" = "1" ] && exit 1
    ;;
  exec)
    printf '%s\n' "$FAKE_CAPABILITY"
    [ "$FAKE_EXEC_FAILS" = "1" ] && exit 1
    ;;
  stop)
    # A real `docker stop` returns 0 even when the timeout expired and the
    # daemon had to SIGKILL — which is exactly why the verdict comes from the
    # exit state instead of from this status.
    printf '%s\n' "${FAKE_STATE_AFTER_STOP:-$FAKE_STATE}" > "$FAKE_STATE_FILE"
    exit "${FAKE_STOP_STATUS:-0}"
    ;;
  rm)
    exit 0
    ;;
esac
exit 0
"""


@pytest.fixture
def shell(tmp_path):
    """Run drain-library functions with a fake docker and stubbed psql."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_docker = bin_dir / "docker"
    fake_docker.write_text(FAKE_DOCKER)
    fake_docker.chmod(0o755)

    log = tmp_path / "docker.log"
    log.write_text("")
    state_file = tmp_path / "state"
    state_file.write_text("")

    def run(body: str, **env_overrides: str) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "FAKE_LOG": str(log),
            "FAKE_STATE_FILE": str(state_file),
            "FAKE_CONTAINER_IDS": "cafe1234",
            "FAKE_STATE": "running",
            "FAKE_STATE_AFTER_STOP": "",
            "FAKE_EXIT_CODE": "0",
            "FAKE_OOM": "false",
            "FAKE_ERROR": "",
            "FAKE_ONEOFF": "false",
            "FAKE_PAUSED": "false",
            "FAKE_RESTARTING": "false",
            "FAKE_CAPABILITY": "graceful",
            "FAKE_PROCESSING": "0",
            "FAKE_INSPECT_FAILS": "0",
            "FAKE_PS_FAILS": "0",
            "FAKE_EXEC_FAILS": "0",
        }
        env.update(env_overrides)

        script = textwrap.dedent(f"""
            set -uo pipefail
            COMPOSE="docker compose"
            psql_scalar() {{ printf '%s\\n' "$FAKE_PROCESSING"; }}
            container_is_running() {{ [ "$(docker inspect -f '{{{{.State.Status}}}}' "$1")" = "running" ]; }}
            . "{DRAIN_LIB}"
            {body}
        """)
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, timeout=60)

    run.log = log  # type: ignore[attr-defined]
    return run


def _commands(shell) -> list[str]:
    return [line for line in shell.log.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Legacy bootstrap: the a82d449 image must never be SIGTERM'd by the deploy
# ---------------------------------------------------------------------------


def test_a_legacy_worker_aborts_the_deploy_before_anything_is_touched(shell) -> None:
    """No signal handler means `docker stop` strands the claimed batch."""
    result = shell("wa_require_drainable_worker", FAKE_CAPABILITY="legacy")

    assert result.returncode != 0, "a legacy worker must fail the gate"
    assert "predates the graceful shutdown" in result.stderr
    assert "durlach_activation_runbook" in result.stderr, "the operator needs the transitional procedure"

    # Match the command VERB, not a substring: the probe's own source mentions
    # `stop_event`, which would otherwise look like a stop command.
    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "stop" not in verbs, "the gate must not try to stop a legacy worker"
    assert "rm" not in verbs
    assert "kill" not in verbs
    assert "--scale" not in " ".join(_commands(shell))


def test_capability_is_read_from_the_running_container_not_the_checkout(shell) -> None:
    """The deployed source always has the new runner — it proves nothing here."""
    shell("wa_require_drainable_worker", FAKE_CAPABILITY="legacy")

    issued = shell.log.read_text()
    assert "exec cafe1234" in issued, "capability must be probed inside the running container"
    # The probe inspects the loop the live process was built with.
    assert "whatsapp_inbox_worker" in issued
    assert "stop_event" in issued


def test_a_graceful_worker_passes_the_gate(shell) -> None:
    result = shell("wa_require_drainable_worker", FAKE_CAPABILITY="graceful")

    assert result.returncode == 0
    assert "honours the graceful shutdown contract" in result.stdout


def test_an_unprobeable_worker_is_treated_as_legacy(shell) -> None:
    """Fail closed: an exec that cannot answer is not evidence of safety."""
    result = shell("wa_require_drainable_worker", FAKE_EXEC_FAILS="1", FAKE_CAPABILITY="")

    assert result.returncode != 0


# An absent or stopped container is NOT evidence that nothing was stranded: a
# crash, an OOM kill or a manual `docker rm` removes exactly the thing that
# would have shown it, while the committed `processing` rows stay behind.


@pytest.mark.parametrize("gate", ["wa_require_drainable_worker", "wa_retire_before_reconciliation"])
@pytest.mark.parametrize(
    ("label", "env"),
    [
        ("absent", {"FAKE_CONTAINER_IDS": ""}),
        ("stopped", {"FAKE_STATE": "exited"}),
    ],
)
def test_a_worker_that_is_gone_still_has_to_prove_the_queue_is_empty(
    shell, gate: str, label: str, env: dict[str, str]
) -> None:
    allowed = shell(gate, FAKE_PROCESSING="0", **env)
    assert allowed.returncode == 0, f"{label}: an empty queue is safe to continue on"
    assert "no stranded processing rows" in allowed.stdout or "not running" in allowed.stdout

    blocked = shell(gate, FAKE_PROCESSING="3", **env)
    assert blocked.returncode != 0, f"{label}: stranded rows must stop the deploy"
    assert "3 whatsapp_events row(s)" in blocked.stderr
    assert "Do NOT bulk-update" in blocked.stderr

    unreadable = shell(gate, FAKE_PROCESSING="", **env)
    assert unreadable.returncode != 0, f"{label}: an unreadable count is not a zero"

    nonnumeric = shell(gate, FAKE_PROCESSING="ERROR: connection refused", **env)
    assert nonnumeric.returncode != 0, f"{label}: a SQL error is not a zero"


def test_a_stopped_worker_is_never_called_a_proven_drain(shell) -> None:
    """Its exit state says nothing: this deploy did not stop it."""
    result = shell("wa_require_drainable_worker", FAKE_STATE="exited", FAKE_PROCESSING="0")

    assert result.returncode == 0
    assert "not a proven graceful drain" in result.stdout


# ---------------------------------------------------------------------------
# Container resolution
# ---------------------------------------------------------------------------


def test_several_containers_stop_the_deploy(shell) -> None:
    """A hand-started replica may hold its own claimed batch."""
    result = shell("wa_require_drainable_worker", FAKE_CONTAINER_IDS="cafe1234\ndead5678")

    assert result.returncode != 0
    assert "Expected exactly one" in result.stderr


def test_a_one_off_container_is_refused(shell) -> None:
    result = shell("wa_require_drainable_worker", FAKE_ONEOFF="True")

    assert result.returncode != 0
    assert "one-off" in result.stderr


# ---------------------------------------------------------------------------
# The drain itself, and every way it can fail
# ---------------------------------------------------------------------------


def test_a_clean_drain_is_accepted(shell) -> None:
    result = shell("wa_graceful_quiesce cafe1234 300 && echo QUIESCED", FAKE_STATE="exited")

    assert result.returncode == 0
    assert "QUIESCED" in result.stdout
    assert "drained cleanly" in result.stdout


def test_the_container_is_not_removed_by_the_drain(shell) -> None:
    """Removal would destroy ExitCode/OOMKilled — the evidence itself."""
    shell("wa_graceful_quiesce cafe1234 300", FAKE_STATE="exited")

    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "rm" not in verbs


@pytest.mark.parametrize(
    ("label", "env"),
    [
        ("sigkill-after-timeout", {"FAKE_STATE": "exited", "FAKE_EXIT_CODE": "137"}),
        ("nonzero-exit", {"FAKE_STATE": "exited", "FAKE_EXIT_CODE": "1"}),
        ("oom-killed", {"FAKE_STATE": "exited", "FAKE_OOM": "true"}),
        ("container-error", {"FAKE_STATE": "exited", "FAKE_ERROR": "runtime failure"}),
        ("still-running", {"FAKE_STATE": "running"}),
        ("restarting", {"FAKE_STATE": "restarting"}),
        ("stranded-rows", {"FAKE_STATE": "exited", "FAKE_PROCESSING": "3"}),
        ("unreadable-count", {"FAKE_STATE": "exited", "FAKE_PROCESSING": ""}),
        ("vanished", {"FAKE_STATE": ""}),
    ],
)
def test_every_unproven_drain_fails_and_rewrites_nothing(shell, label: str, env: dict[str, str]) -> None:
    result = shell("wa_graceful_quiesce cafe1234 300 && echo QUIESCED", **env)

    assert result.returncode != 0, f"{label} must not be accepted as a drain"
    assert "QUIESCED" not in result.stdout

    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "rm" not in verbs, f"{label}: the container must survive for analysis"
    combined = result.stdout + result.stderr
    assert "UPDATE" not in combined, f"{label}: no automatic rewrite of whatsapp_events"


def test_stranded_rows_forbid_the_bulk_update_explicitly(shell) -> None:
    result = shell("wa_graceful_quiesce cafe1234 300", FAKE_STATE="exited", FAKE_PROCESSING="7")

    assert "STOP" in result.stderr
    assert "Do NOT bulk-update" in result.stderr
    assert "7" in result.stderr, "the operator needs the count to size the analysis"


def test_a_docker_stop_that_returns_zero_is_not_itself_proof(shell) -> None:
    """The daemon returns 0 even when it had to SIGKILL after the timeout."""
    result = shell(
        "wa_graceful_quiesce cafe1234 300",
        FAKE_STOP_STATUS="0",
        FAKE_STATE="exited",
        FAKE_EXIT_CODE="137",
    )

    assert result.returncode != 0, "exit code 137 must be rejected despite a successful stop call"


# ---------------------------------------------------------------------------
# Diagnostics stay safe
# ---------------------------------------------------------------------------


def test_the_container_error_text_is_never_echoed(shell) -> None:
    """`State.Error` can carry arbitrary runtime text — report the fact only."""
    secret = "Bearer super-secret-token"
    result = shell("wa_graceful_quiesce cafe1234 300", FAKE_STATE="exited", FAKE_ERROR=secret)

    assert result.returncode != 0
    assert secret not in result.stdout + result.stderr
    assert "recorded a container-level error" in result.stderr


def test_the_drain_never_issues_a_write(shell) -> None:
    shell("wa_graceful_quiesce cafe1234 300", FAKE_STATE="exited")

    body = DRAIN_LIB.read_text()
    for write in ("UPDATE ", "DELETE ", "INSERT "):
        assert write not in body, f"the drain library must stay read-only: {write}"


# ---------------------------------------------------------------------------
# Wiring into the real deploy
# ---------------------------------------------------------------------------


def test_the_deploy_sources_the_library_and_gates_before_building() -> None:
    script = DEPLOY_SCRIPT.read_text()

    assert "lib/whatsapp_drain.sh" in script
    gate = script.index("wa_require_drainable_worker")
    build = script.index("$COMPOSE build")
    assert gate < build, "a legacy worker must abort before anything is built or migrated"


def test_the_deploy_drains_and_verifies_before_the_scale_to_zero() -> None:
    script = DEPLOY_SCRIPT.read_text()

    quiesce = script.index("wa_retire_before_reconciliation")
    scale_zero = script.index("--scale altegio-whatsapp-inbox-worker=0")
    outbox = script.index("up -d --force-recreate altegio-outbox-worker")
    producer = script.index("$COMPOSE up -d altegio-whatsapp-inbox-worker")

    assert quiesce < scale_zero < outbox < producer, (
        "order must be: drain+verify -> reconcile -> new outbox -> producer"
    )


def test_a_failed_drain_stops_the_deploy_before_the_producer() -> None:
    script = DEPLOY_SCRIPT.read_text()
    start = script.index("wa_retire_before_reconciliation")
    block = script[start : script.index("--scale altegio-whatsapp-inbox-worker=0", start)]

    assert "exit 1" in block
    assert "left in" in block and "analysis" in block


def test_the_recovery_trap_never_starts_the_producer() -> None:
    """Recovery must not race a producer against an unverified outbox."""
    script = DEPLOY_SCRIPT.read_text()
    recover = script[script.index("recover() {") : script.index("trap 'recover $?' EXIT")]

    assert "whatsapp" not in recover.lower()


# ---------------------------------------------------------------------------
# The final gate, immediately before the Compose reconciliation
# ---------------------------------------------------------------------------
#
# The preflight ran before the build, the backup and the migrations. The
# container set can change under the deploy in that window, so the gate repeats
# the full discovery — and a Docker failure here must never look like "there is
# no container".


def test_a_second_container_appearing_after_preflight_blocks_the_gate(shell) -> None:
    preflight = shell("wa_require_drainable_worker")
    assert preflight.returncode == 0

    gate = shell("wa_retire_before_reconciliation", FAKE_CONTAINER_IDS="cafe1234\ndead5678")
    assert gate.returncode != 0
    assert "Expected exactly one" in gate.stderr

    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "stop" not in verbs, "nothing may be stopped once the container set is untrusted"


@pytest.mark.parametrize(
    ("label", "env"),
    [
        ("docker-ps-failure", {"FAKE_PS_FAILS": "1"}),
        ("docker-inspect-failure", {"FAKE_INSPECT_FAILS": "1"}),
        ("one-off-container", {"FAKE_ONEOFF": "True"}),
        ("legacy-image", {"FAKE_CAPABILITY": "legacy"}),
        ("absent-with-stranded-rows", {"FAKE_CONTAINER_IDS": "", "FAKE_PROCESSING": "2"}),
        ("stopped-with-stranded-rows", {"FAKE_STATE": "exited", "FAKE_PROCESSING": "2"}),
        ("sigkill-after-timeout", {"FAKE_STATE_AFTER_STOP": "exited", "FAKE_EXIT_CODE": "137"}),
        ("oom-killed", {"FAKE_STATE_AFTER_STOP": "exited", "FAKE_OOM": "true"}),
        ("unreadable-oom", {"FAKE_STATE_AFTER_STOP": "exited", "FAKE_OOM": ""}),
        ("container-error", {"FAKE_STATE_AFTER_STOP": "exited", "FAKE_ERROR": "boom"}),
        ("stranded-after-drain", {"FAKE_STATE_AFTER_STOP": "exited", "FAKE_PROCESSING": "4"}),
        ("never-exits", {"FAKE_STATE_AFTER_STOP": "running"}),
    ],
)
def test_the_final_gate_blocks_every_unproven_state(shell, label: str, env: dict[str, str]) -> None:
    result = shell("wa_retire_before_reconciliation && echo GATE_PASSED", **env)

    assert result.returncode != 0, f"{label} must not pass the final gate"
    assert "GATE_PASSED" not in result.stdout

    combined = result.stdout + result.stderr
    assert "UPDATE" not in combined, f"{label}: no automatic rewrite of whatsapp_events"
    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "rm" not in verbs, f"{label}: the container must survive for analysis"


def test_a_docker_failure_is_not_read_as_an_absent_container(shell) -> None:
    """The bug this gate replaced: every error collapsed into an empty id."""
    failure = shell("wa_retire_before_reconciliation", FAKE_PS_FAILS="1")
    assert failure.returncode != 0
    assert "Docker discovery failed" in failure.stderr

    # Whereas a genuinely absent container with an empty queue is fine.
    absent = shell("wa_retire_before_reconciliation", FAKE_CONTAINER_IDS="", FAKE_PROCESSING="0")
    assert absent.returncode == 0


def test_an_unreadable_oneoff_label_does_not_count_as_a_service_container(shell) -> None:
    """`<no value>` is an answer; a failed inspect is not."""
    unknown = shell("wa_retire_before_reconciliation", FAKE_ONEOFF="<no value>", FAKE_STATE_AFTER_STOP="exited")
    assert unknown.returncode == 0, "an absent label is the normal Compose service case"

    unreadable = shell("wa_retire_before_reconciliation", FAKE_INSPECT_FAILS="1")
    assert unreadable.returncode != 0


def test_the_happy_path_stops_verifies_and_allows_the_reconciliation(shell) -> None:
    result = shell(
        "wa_retire_before_reconciliation && echo GATE_PASSED",
        FAKE_STATE="running",
        FAKE_STATE_AFTER_STOP="exited",
        FAKE_EXIT_CODE="0",
        FAKE_OOM="false",
        FAKE_PROCESSING="0",
    )

    assert result.returncode == 0
    assert "GATE_PASSED" in result.stdout
    assert "drained cleanly" in result.stdout

    verbs = [line.split()[0] for line in _commands(shell) if line.split()]
    assert "stop" in verbs, "the graceful stop must actually be issued"
    assert "rm" not in verbs, "removal is left to the reconciliation, after the evidence is read"


# ---------------------------------------------------------------------------
# The one-off label, as Compose actually writes it
# ---------------------------------------------------------------------------
#
# Compose v5.3.1 writes `com.docker.compose.oneoff=False` — capitalised. A
# resolver that only accepted lowercase `false` rejected the ordinary service
# container and blocked the whole rollout.


@pytest.mark.parametrize("label", ["False", "false", "<no value>", ""])
def test_a_service_container_is_accepted_whatever_the_label_casing(shell, label: str) -> None:
    result = shell("wa_retire_before_reconciliation", FAKE_ONEOFF=label, FAKE_STATE_AFTER_STOP="exited")

    assert result.returncode == 0, f"{label!r} identifies an ordinary service container"


@pytest.mark.parametrize("label", ["True", "true"])
def test_a_one_off_is_refused_whatever_the_label_casing(shell, label: str) -> None:
    result = shell("wa_retire_before_reconciliation", FAKE_ONEOFF=label)

    assert result.returncode != 0
    assert "one-off" in result.stderr


@pytest.mark.parametrize("label", ["maybe", "0", "1", "Yes"])
def test_an_unrecognisable_one_off_label_is_refused(shell, label: str) -> None:
    """Fail closed: an unknown value is not evidence of a service container."""
    result = shell("wa_retire_before_reconciliation", FAKE_ONEOFF=label)

    assert result.returncode != 0
    assert "Unrecognisable one-off label" in result.stderr


# ---------------------------------------------------------------------------
# Container states
# ---------------------------------------------------------------------------
#
# `paused` used to fall through the "not running" branch: with processing=0 the
# gate returned success, WA_RESOLVED_ID stayed empty, and the reconciliation
# scaled a frozen worker to zero — discarding the very batch the barrier
# procedure exists to protect.


@pytest.mark.parametrize(
    ("label", "env"),
    [
        ("paused-status", {"FAKE_STATE": "paused"}),
        ("paused-flag", {"FAKE_STATE": "running", "FAKE_PAUSED": "true"}),
        ("restarting-status", {"FAKE_STATE": "restarting"}),
        ("restarting-flag", {"FAKE_STATE": "running", "FAKE_RESTARTING": "true"}),
        ("removing", {"FAKE_STATE": "removing"}),
        ("created", {"FAKE_STATE": "created"}),
        ("dead", {"FAKE_STATE": "dead"}),
        ("unknown", {"FAKE_STATE": "wat"}),
    ],
)
def test_transitional_states_never_pass_the_gate(shell, label: str, env: dict[str, str]) -> None:
    result = shell("wa_retire_before_reconciliation && echo GATE_PASSED", FAKE_PROCESSING="0", **env)

    assert result.returncode != 0, f"{label} must stop the deploy even with an empty queue"
    assert "GATE_PASSED" not in result.stdout
    assert "STOP" in result.stderr

    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "rm" not in verbs, f"{label}: nothing may be removed"


def test_a_paused_worker_is_never_unpaused_by_the_deploy(shell) -> None:
    """Only the controlled legacy helper may touch a frozen worker."""
    shell("wa_retire_before_reconciliation", FAKE_STATE="paused", FAKE_PROCESSING="0")

    verbs = {line.split()[0] for line in _commands(shell) if line.split()}
    assert "unpause" not in verbs
    assert "stop" not in verbs
