"""The executor is part of the deployment stack (review R4).

A Python entrypoint is not a deployment. The reviewed state had one, and the runbook
told an administrator to start it with an interactive `docker compose exec` inside the
API container — which gives a minutes-long, money-moving stage none of the properties
it needs: it dies with the terminal, it is not restarted, and nobody can tell "still
running" from "died silently".

So the service is asserted here, as a contract, the same way the CI shards are. These
are file-level assertions and run anywhere: they start no container, touch no
production and read no secret.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

COMPOSE_FILE = Path(__file__).resolve().parents[3] / "docker-compose.yml"
EXECUTOR_SERVICE = "altegio-easyweek-voucher-executor"
EXECUTOR_MODULE = "altegio_bot.scripts.run_easyweek_voucher_production_worker"
API_SERVICE = "altegio-api"
RUNBOOK = Path(__file__).resolve().parents[3] / "docs" / "easyweek" / "VOUCHER_PRODUCTION_MAILING_RUNBOOK.md"


def _compose() -> dict[str, Any]:
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))


def _service() -> dict[str, Any]:
    services = _compose()["services"]
    assert EXECUTOR_SERVICE in services, (
        f"{EXECUTOR_SERVICE} is not in docker-compose.yml: a confirmed stage would have nothing supervised to run it"
    )
    return services[EXECUTOR_SERVICE]


def test_the_executor_is_a_supervised_service() -> None:
    """Built from this repository, and restarted when it dies."""
    service = _service()
    assert service.get("build") == ".", "the executor must be the code of this commit"
    assert service.get("restart") == "always", (
        "an executor that is not restarted turns one crash into a queue nobody drains"
    )


def test_the_executor_runs_the_dedicated_entrypoint() -> None:
    """Its own worker — never the generic campaign worker, which retries."""
    command = _service().get("command")
    assert isinstance(command, list), f"expected an exec-form command, found {command!r}"
    joined = " ".join(str(part) for part in command)
    assert EXECUTOR_MODULE in joined, joined
    # The generic campaign worker's whole purpose is to retry what it finds, which is
    # exactly wrong here: EasyWeek publishes no write idempotency key and Meta will
    # deliver twice.
    assert "run_campaign_worker" not in joined
    assert "run_outbox_worker" not in joined


def test_the_executor_reads_the_same_environment_as_the_api() -> None:
    """Both halves of the fence, and the EasyWeek secrets file, reach it.

    An executor on a different environment than the API is the §43 hazard the runbook
    calls out: the API could refuse new confirmations with the fence shut while the
    executor went on executing with a stale `true`.
    """
    compose = _compose()
    executor = compose["services"][EXECUTOR_SERVICE]
    api = compose["services"][API_SERVICE]

    def sources(service: dict[str, Any]) -> set[str]:
        entries = service.get("env_file") or []
        if isinstance(entries, str):
            entries = [entries]
        found = set()
        for entry in entries:
            found.add(entry if isinstance(entry, str) else str(entry.get("path")))
        return found

    assert sources(executor) == sources(api), (
        f"executor env_file {sorted(sources(executor))} != api {sorted(sources(api))}"
    )
    # The EasyWeek secrets file must stay optional, or a host without it fails to deploy.
    optional = [
        entry
        for entry in executor.get("env_file", [])
        if isinstance(entry, dict) and str(entry.get("path")).endswith("easyweek.env")
    ]
    assert optional and optional[0].get("required") is False


def test_the_executor_waits_for_a_healthy_database() -> None:
    """Its first act is to interrupt abandoned operations, which needs the database."""
    depends = _service().get("depends_on") or {}
    assert "postgres" in depends, depends
    condition = depends["postgres"]
    assert (condition.get("condition") if isinstance(condition, dict) else condition) == "service_healthy"


def test_exactly_one_executor_is_declared() -> None:
    """One API and ONE executor, with no rolling deploy.

    The executor interrupts every `running` operation it finds at start-up, which is
    correct for a single executor — such a row can only belong to a dead process — and
    wrong for two overlapping ones. The database enforces it regardless, through
    `FOR UPDATE SKIP LOCKED` on the claim; this states the intent where an
    orchestrator can read it.
    """
    service = _service()
    replicas = (service.get("deploy") or {}).get("replicas")
    assert replicas == 1, f"expected replicas: 1, found {replicas!r}"
    # And no second service runs the same entrypoint.
    running_it = [
        name
        for name, definition in _compose()["services"].items()
        if EXECUTOR_MODULE in " ".join(str(part) for part in (definition.get("command") or []))
    ]
    assert running_it == [EXECUTOR_SERVICE], running_it


def test_the_executor_is_given_time_to_finish_a_stage() -> None:
    """SIGTERM is checked between operations, so the grace period must allow one.

    Docker's default is ten seconds, which would kill a stage mid-flight and turn an
    ordinary redeploy into an interrupted operation needing a readback.
    """
    grace = str(_service().get("stop_grace_period") or "")
    assert grace, "no stop_grace_period: a redeploy would cut a stage in half"
    assert grace.endswith("s")
    assert int(grace[:-1]) >= 60, grace


def test_the_executor_is_started_by_default_not_behind_a_profile() -> None:
    """`docker compose up -d` must bring it up, like every other worker."""
    assert "profiles" not in _service(), (
        "a profile would leave the executor out of the ordinary deploy, and a confirmed "
        "stage would sit in `queued` with nothing to run it"
    )


def test_the_executor_gets_no_ports_and_no_docker_socket() -> None:
    """It needs neither, and both would be new exposure."""
    service = _service()
    assert "ports" not in service
    volumes = [str(entry) for entry in (service.get("volumes") or [])]
    assert not any("docker.sock" in entry for entry in volumes), volumes


# ===========================================================================
# The runbook has to describe the service that exists
# ===========================================================================


def test_the_runbook_manages_the_service_rather_than_an_interactive_exec() -> None:
    """The reviewed runbook told an operator to run the worker by hand."""
    text = RUNBOOK.read_text(encoding="utf-8")
    assert EXECUTOR_SERVICE in text, "the runbook never names the executor service"
    assert f"up -d {EXECUTOR_SERVICE}" in text, "the runbook does not say how to start it"
    assert f"logs --tail 50 {EXECUTOR_SERVICE}" in text or f"logs --tail 30 {EXECUTOR_SERVICE}" in text
    # The exact shape the review rejected: starting the long-running worker with
    # `exec` inside the API container.
    assert f"exec altegio-api uv run python -m {EXECUTOR_MODULE}" not in text


def test_the_runbook_recreates_both_containers_when_the_fence_changes() -> None:
    """Recreating only the API leaves the executor on the old settings."""
    text = RUNBOOK.read_text(encoding="utf-8")
    # Only the COMMANDS, not the prose that explains them.
    recreate = [
        line for line in text.splitlines() if "--force-recreate" in line and line.lstrip().startswith("docker compose")
    ]
    assert recreate, "the runbook never recreates anything"
    assert all(API_SERVICE in line and EXECUTOR_SERVICE in line for line in recreate), (
        f"a --force-recreate command omits one of the two services: {recreate}"
    )
    # And the value is verified inside each container, not in the file.
    assert text.count("printenv EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED") >= 2


def test_the_runbook_states_the_closed_fence_smoke_reason_the_cli_actually_gives() -> None:
    """Review R4: the documented reason must match the dispatch order.

    With the fence checked first the smoke answered `voucher_production_disabled`,
    which proves the fence works and says nothing about the CLI closure the
    administrator was checking.
    """
    text = RUNBOOK.read_text(encoding="utf-8")
    assert '"reasons": ["voucher_production_cli_mutation_closed"]' in text
    # And the runbook says why that is the answer either way.
    assert "whether the fence is open or shut" in text


def test_the_runbook_explains_the_drain_and_the_forced_stop() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    assert "stop_grace_period" in text
    assert "interrupted" in text
    assert "Сверить с EasyWeek" in text


@pytest.mark.parametrize(
    "variable",
    [
        "EASYWEEK_VOUCHER_PRODUCTION_EXECUTOR_ENABLED",
        "EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED",
        "EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID",
        "EASYWEEK_VOUCHER_PRODUCTION_MAILING_ACCOUNT_UUID",
        "EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY",
        "EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY_ID",
        "OPS_USER",
        "OPS_SECRET",
    ],
)
def test_the_runbook_documents_every_setting_this_phase_needs(variable: str) -> None:
    assert variable in RUNBOOK.read_text(encoding="utf-8"), variable


def test_no_secret_value_appears_in_the_compose_file_or_the_runbook() -> None:
    """Names and placeholders only — never a value.

    The approved issuer UUID in particular: it is deployment configuration and the
    owner's own record, and this repository must not carry it.
    """
    for path in (COMPOSE_FILE, RUNBOOK):
        text = path.read_text(encoding="utf-8")
        assert "b15ffc91" not in text, f"{path.name} carries the approved issuer UUID"
        for variable in (
            "EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY",
            "EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID",
            "OPS_SECRET",
        ):
            # A documented NAME is fine; `NAME=<something>` in these files would be a
            # value, and the placeholder form is the only exception.
            for line in text.splitlines():
                if f"{variable}=" not in line:
                    continue
                value = line.split(f"{variable}=", 1)[1].strip().strip("`")
                assert value == "" or value.startswith("<"), f"{path.name}: {line.strip()}"


def test_the_fence_default_stays_false() -> None:
    """Starting the executor authorises nothing."""
    from altegio_bot.settings import Settings

    fields = Settings.model_fields
    assert fields["easyweek_voucher_production_mailing_enabled"].default is False
    assert fields["easyweek_voucher_production_executor_enabled"].default is False
