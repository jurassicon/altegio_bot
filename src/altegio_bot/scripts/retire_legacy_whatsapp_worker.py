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
import subprocess
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.db import SessionLocal

logger = logging.getLogger(__name__)

WA_SERVICE = "altegio-whatsapp-inbox-worker"
COMPOSE_PROJECT = "altegio_bot"


class LegacyRetirementError(RuntimeError):
    """Fail-closed: the retirement is not proven, so it did not happen."""


class ContainerControl(Protocol):
    """The container operations this procedure needs, injected for testing."""

    def pause(self, container: str) -> None: ...

    def unpause(self, container: str) -> None: ...

    def remove(self, container: str) -> None: ...

    def exists(self, container: str) -> bool: ...


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

    def exists(self, container: str) -> bool:
        return self._run("inspect", "-f", "{{.Id}}", container).returncode == 0


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

    docker.pause(container)
    removed = False

    try:
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
                docker.remove(container)
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
                if docker.exists(container):
                    docker.unpause(container)
                    logger.info("legacy worker left running for analysis container=%s", container)
            except Exception:
                logger.error(
                    "container %s may still be PAUSED — unpause it manually before investigating",
                    container,
                )
        raise


def _resolve_container() -> str:
    """Exactly one non-one-off service container, or refuse."""
    listed = subprocess.run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=com.docker.compose.project={COMPOSE_PROJECT}",
            "--filter",
            f"label=com.docker.compose.service={WA_SERVICE}",
            "--format",
            "{{.ID}}",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if listed.returncode != 0:
        raise LegacyRetirementError("Docker discovery failed")

    ids = [line.strip() for line in listed.stdout.splitlines() if line.strip()]
    if not ids:
        raise LegacyRetirementError(f"no {WA_SERVICE} container found")
    if len(ids) != 1:
        raise LegacyRetirementError(f"expected exactly one {WA_SERVICE} container, found {len(ids)}")

    oneoff = subprocess.run(
        ["docker", "inspect", "-f", '{{index .Config.Labels "com.docker.compose.oneoff"}}', ids[0]],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if oneoff.returncode != 0:
        raise LegacyRetirementError("cannot inspect the resolved container")
    if oneoff.stdout.strip() not in {"false", "<no value>", ""}:
        raise LegacyRetirementError("the resolved container is a one-off, not the service container")

    return ids[0]


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="One-time retirement of a legacy WhatsApp worker.")
    parser.add_argument("--container", default=None, help="Container id; resolved automatically if omitted.")
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    parser.add_argument("--statement-timeout-ms", type=int, default=15_000)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        container = args.container or _resolve_container()
        outcome = await retire_with_barrier(
            SessionLocal,
            container=container,
            docker=DockerCli(),
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
