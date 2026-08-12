"""One-time retirement of a pre-graceful altegio-whatsapp-inbox-worker.

The worker shipped before the SIGTERM contract (a82d449 and earlier) cannot be
asked to drain: it has no signal handler, so any stop kills it wherever it is
and a batch it already committed as ``received -> processing`` is stranded for
good — the normal claim reads ``received`` only, and
``recover_stale_processing_events`` covers Chatwoot operator-relay rows alone.

Why freezing the container is not enough
----------------------------------------
``docker pause`` freezes the *client* process, not its PostgreSQL backend. A
COMMIT already sent travels on:

    worker sends COMMIT -> container frozen before the reply -> backend finishes
    the COMMIT -> an operator's SELECT in another session may still have read 0
    -> container removed -> the rows become visible, stranded, owner-less.

A second SELECT, a longer pause or a post-removal audit only discovers that
damage later. The barrier has to live in the database.

The barrier
-----------
``LOCK TABLE whatsapp_events IN SHARE MODE`` inside the checking transaction.

SHARE is the weakest mode that conflicts with ROW EXCLUSIVE, which is what the
claim path takes: ``lock_next_batch`` runs ``SELECT ... FOR UPDATE`` and then
flushes ``UPDATE whatsapp_events SET status='processing'``. So either

  * the claim transaction still holds ROW EXCLUSIVE — our LOCK waits, and can
    only be granted once that transaction has fully committed or aborted, after
    which the count sees its rows; or
  * we take SHARE first — and then the claim's UPDATE blocks on us, so it cannot
    add rows behind our back, and killing the frozen container aborts it.

Either way the count taken under the lock is the truth, and it stays the truth
until we release. SHARE deliberately also blocks the API's inserts of new
webhook events for the few seconds the barrier is held; that pause is the cost
of the guarantee and is called out in the runbook.

An advisory lock would NOT work here: the legacy claim path never takes one, so
it would synchronise nothing.

Read-only with respect to the events themselves: this never rewrites a row.
``processing > 0`` is reported for an operator to analyse, never repaired.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import shutil
import subprocess
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

WA_SERVICE = "altegio-whatsapp-inbox-worker"
COMPOSE_PROJECT = "altegio_bot"


class LegacyRetirementError(RuntimeError):
    """Fail-closed: the retirement is not proven, so it did not happen."""


class Presence(str, Enum):
    """Three outcomes, never two: "Docker failed" is not "container gone"."""

    PRESENT = "present"
    ABSENT = "absent"
    UNREADABLE = "unreadable"


# Compose writes this label with a capitalised Python bool on some versions
# (`False` on Compose v5.3.1, `false` elsewhere) and omits it entirely on
# others. Only an explicit negative or a missing label means "service
# container"; anything else — including an unreadable value — is refused.
_ONEOFF_SERVICE_VALUES = frozenset({"false", "<no value>", ""})
_ONEOFF_ONESHOT_VALUES = frozenset({"true"})


def oneoff_label_is_service_container(raw: str | None) -> bool:
    """True only for a label that positively identifies a service container."""
    if raw is None:
        return False
    return raw.strip().lower() in _ONEOFF_SERVICE_VALUES


def oneoff_label_is_one_shot(raw: str | None) -> bool:
    return raw is not None and raw.strip().lower() in _ONEOFF_ONESHOT_VALUES


class ContainerControl(Protocol):
    """The container operations this procedure needs, injected for testing."""

    def pause(self, container: str) -> None: ...

    def unpause(self, container: str) -> None: ...

    def remove(self, container: str) -> None: ...

    def presence(self, container: str) -> Presence: ...


class DockerCli:
    """Real `docker` CLI. Never prints container output into our logs."""

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["docker", *args], capture_output=True, text=True, check=False, timeout=120)

    def pause(self, container: str) -> None:
        if self._run("pause", container).returncode != 0:
            raise LegacyRetirementError(f"cannot pause container {container}")

    def unpause(self, container: str) -> None:
        if self._run("unpause", container).returncode != 0:
            raise LegacyRetirementError(f"cannot unpause container {container}")

    def remove(self, container: str) -> None:
        if self._run("rm", "-f", container).returncode != 0:
            raise LegacyRetirementError(f"cannot remove container {container}")

    def presence(self, container: str) -> Presence:
        """Absent and unreadable are different facts and stay different.

        Treating an inspect failure as "gone" would let a cleanup path claim the
        worker is retired when it may still be sitting there, paused.
        """
        result = self._run("inspect", "-f", "{{.Id}}", container)
        if result.returncode == 0:
            return Presence.PRESENT
        if "No such object" in result.stderr or "no such container" in result.stderr.lower():
            return Presence.ABSENT
        return Presence.UNREADABLE

    def inspect_field(self, container: str, fmt: str) -> str | None:
        result = self._run("inspect", "-f", fmt, container)
        if result.returncode != 0:
            return None
        return result.stdout.strip()

    def service_container_ids(self) -> list[str] | None:
        """Ids of the WhatsApp worker service, or None if discovery failed."""
        result = self._run(
            "ps",
            "-a",
            "--filter",
            f"label=com.docker.compose.project={COMPOSE_PROJECT}",
            "--filter",
            f"label=com.docker.compose.service={WA_SERVICE}",
            "--format",
            "{{.ID}}",
        )
        if result.returncode != 0:
            return None
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def supports_graceful_shutdown(self, container: str) -> bool | None:
        """Probe the LIVE image; None when the answer cannot be read."""
        result = self._run(
            "exec",
            container,
            "/app/.venv/bin/python",
            "-c",
            "import inspect\n"
            "from altegio_bot.workers import whatsapp_inbox_worker as w\n"
            'print("graceful" if "stop_event" in inspect.signature(w.run_loop).parameters '
            'and hasattr(w, "run_with_graceful_shutdown") else "legacy")',
        )
        if result.returncode != 0:
            return None
        answer = result.stdout.strip()
        if answer == "graceful":
            return True
        if answer == "legacy":
            return False
        return None


@dataclass(frozen=True)
class RetirementOutcome:
    retired: bool
    processing_under_lock: int
    processing_after_removal: int


async def _count_processing(session: AsyncSession) -> int:
    result = await session.execute(text("SELECT count(*) FROM whatsapp_events WHERE status = 'processing'"))
    return int(result.scalar_one())


async def retire_with_barrier(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    container: str,
    docker: ContainerControl,
    lock_timeout_ms: int = 5_000,
    statement_timeout_ms: int = 15_000,
) -> RetirementOutcome:
    """Freeze, take the DB barrier, prove emptiness, retire, prove again.

    Raises :class:`LegacyRetirementError` on every unproven path, leaving the
    container in place (and unpaused where that is safe) so an operator can
    investigate. Never updates ``whatsapp_events``.
    """
    if lock_timeout_ms <= 0 or statement_timeout_ms <= 0:
        raise LegacyRetirementError("timeouts must be positive")

    await asyncio.to_thread(docker.pause, container)
    removed = False

    try:
        # Topology is re-proven AFTER the freeze: a replica started between the
        # resolve and the pause would keep claiming while we retire this one.
        ids_after_pause = await asyncio.to_thread(docker.service_container_ids)
        if ids_after_pause is None:
            raise LegacyRetirementError("Docker discovery failed after the freeze")
        if ids_after_pause != [container]:
            raise LegacyRetirementError(
                f"the container set changed after the freeze ({len(ids_after_pause)} found); STOP"
            )

        async with session_factory() as session:
            async with session.begin():
                # Bounded: a barrier that waits forever would just move the
                # outage rather than fail closed. Ints are validated above and
                # cannot be parameterised in SET LOCAL.
                await session.execute(text(f"SET LOCAL lock_timeout = {int(lock_timeout_ms)}"))
                await session.execute(text(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}"))

                try:
                    # THE barrier. Conflicts with the claim path's ROW
                    # EXCLUSIVE, so this returns only once no claim transaction
                    # is in flight — including one whose COMMIT was already on
                    # the wire when the container froze.
                    await session.execute(text("LOCK TABLE whatsapp_events IN SHARE MODE"))
                except DBAPIError as exc:
                    # Most likely lock_timeout: an old claim is still open.
                    raise LegacyRetirementError(
                        "could not acquire the whatsapp_events barrier within the timeout; "
                        "a claim transaction is still in flight"
                    ) from exc

                under_lock = await _count_processing(session)
                if under_lock:
                    raise LegacyRetirementError(
                        f"{under_lock} whatsapp_events row(s) are in 'processing' under the barrier; "
                        "the worker committed a batch it never finished"
                    )

                # Still holding the lock: no claim can slip in between this
                # check and the removal.
                await asyncio.to_thread(docker.remove, container)
                removed = True

                after = await _count_processing(session)
                if after:
                    raise LegacyRetirementError(f"{after} whatsapp_events row(s) appeared during retirement; STOP")

        logger.info("legacy worker retired under barrier container=%s", container)
        return RetirementOutcome(retired=True, processing_under_lock=0, processing_after_removal=0)

    except Exception:
        # The transaction/lock is released by the context managers above. Only
        # the container is ours to restore, and only if it still exists: a
        # container removed mid-failure must not be silently recreated.
        if not removed:
            try:
                presence = docker.presence(container)
                if presence is Presence.PRESENT:
                    docker.unpause(container)
                    logger.info("legacy worker left running for analysis container=%s", container)
                elif presence is Presence.UNREADABLE:
                    # Never silently read a Docker failure as "already gone".
                    logger.error(
                        "container %s state is UNREADABLE — it may still be PAUSED; "
                        "check and unpause it manually before investigating",
                        container,
                    )
            except Exception:
                logger.error(
                    "container %s may still be PAUSED — unpause it manually before investigating",
                    container,
                )
        else:
            # Removal was attempted. If it failed we cannot claim the worker is
            # gone, and it is frozen, so say so plainly.
            if docker.presence(container) is not Presence.ABSENT:
                logger.error(
                    "container %s was NOT confirmed removed and may still be PAUSED; "
                    "resolve its state manually before continuing the rollout",
                    container,
                )
        raise


def resolve_legacy_container(docker: ContainerControl) -> str:
    """The one container this helper is allowed to touch.

    Identity is PROVEN here rather than accepted from an argument: a mistyped id
    would otherwise pause and remove an unrelated production container. Every
    check is a refusal, not a warning.
    """
    ids = docker.service_container_ids()
    if ids is None:
        raise LegacyRetirementError("Docker discovery failed")
    if not ids:
        raise LegacyRetirementError(f"no {WA_SERVICE} container found")
    if len(ids) != 1:
        raise LegacyRetirementError(f"expected exactly one {WA_SERVICE} container, found {len(ids)}")

    container = ids[0]

    # The labels are re-read from the container itself: `docker ps --filter`
    # selected it, but the identity this helper acts on must be its own.
    project = docker.inspect_field(container, '{{index .Config.Labels "com.docker.compose.project"}}')
    service = docker.inspect_field(container, '{{index .Config.Labels "com.docker.compose.service"}}')
    if project is None or service is None:
        raise LegacyRetirementError("cannot inspect the resolved container")
    if project != COMPOSE_PROJECT:
        raise LegacyRetirementError(f"container belongs to project '{project}', not '{COMPOSE_PROJECT}'")
    if service != WA_SERVICE:
        raise LegacyRetirementError(f"container belongs to service '{service}', not '{WA_SERVICE}'")

    oneoff = docker.inspect_field(container, '{{index .Config.Labels "com.docker.compose.oneoff"}}')
    if oneoff is None:
        raise LegacyRetirementError("cannot read the one-off label of the resolved container")
    if oneoff_label_is_one_shot(oneoff):
        raise LegacyRetirementError("the resolved container is a one-off, not the service container")
    if not oneoff_label_is_service_container(oneoff):
        raise LegacyRetirementError("the one-off label of the resolved container is not recognisable")

    state = docker.inspect_field(container, "{{.State.Status}}")
    if not state:
        raise LegacyRetirementError("cannot read the state of the resolved container")
    if state != "running":
        raise LegacyRetirementError(
            f"the {WA_SERVICE} container is '{state}', not 'running'; this helper only retires a live legacy worker"
        )

    graceful = docker.supports_graceful_shutdown(container)
    if graceful is None:
        raise LegacyRetirementError("cannot read the shutdown capability of the running image")
    if graceful:
        raise LegacyRetirementError(
            "the running image already honours the graceful shutdown contract; "
            "use the ordinary deploy path, not this one-time retirement"
        )

    return container


def probe_ops_runtime(docker: ContainerControl) -> dict[str, object]:
    """Read-only: can this environment run the retirement at all?

    Pauses nothing, removes nothing, writes nothing. It exists so the operator
    can verify the ops image before the procedure that touches production.
    """
    ids = docker.service_container_ids()
    return {
        "helper_module": __name__,
        "docker_cli": shutil.which("docker") is not None,
        "docker_daemon": ids is not None,
        "whatsapp_worker_containers": len(ids) if ids is not None else None,
    }


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="One-time retirement of a legacy WhatsApp worker.")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Read-only: check module, Docker CLI, daemon and database, then exit.",
    )
    parser.add_argument(
        "--expect-container",
        default=None,
        help=(
            "Optional cross-check. The helper always resolves the container itself; "
            "if given, this id must match what it resolved, or the run is refused."
        ),
    )
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    parser.add_argument("--statement-timeout-ms", type=int, default=15_000)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    docker = DockerCli()

    # Imported here, not at module scope: `Settings` requires the full
    # production environment, and the module must stay importable for the probe
    # and for inspection without it.
    from altegio_bot.db import SessionLocal

    if args.probe:
        report = probe_ops_runtime(docker)
        try:
            async with SessionLocal() as session:
                await session.execute(text("SELECT 1"))
            report["database"] = True
        except Exception as exc:  # noqa: BLE001 - class name only, never the text
            report["database"] = False
            report["database_error_type"] = type(exc).__name__
        print(report)
        ok = bool(report["docker_cli"]) and bool(report["docker_daemon"]) and bool(report["database"])
        return 0 if ok else 1

    try:
        container = resolve_legacy_container(docker)
        # An operator-supplied id never SELECTS the target; it can only disagree
        # with the proven one, and then nothing happens.
        if args.expect_container and args.expect_container != container:
            raise LegacyRetirementError("the resolved container does not match --expect-container")

        outcome = await retire_with_barrier(
            SessionLocal,
            container=container,
            docker=docker,
            lock_timeout_ms=args.lock_timeout_ms,
            statement_timeout_ms=args.statement_timeout_ms,
        )
    except LegacyRetirementError as exc:
        # The message is ours: counts, ids and technical states only.
        print(f"STOP: {exc}")
        return 1

    print({"retired": outcome.retired, "processing_under_lock": outcome.processing_under_lock})
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
