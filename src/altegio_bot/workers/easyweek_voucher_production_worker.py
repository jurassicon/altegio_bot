"""The dedicated executor of confirmed voucher-mailing stages (§43.5).

One worker, for one scenario. It does exactly one thing: take an operation an
operator confirmed in the browser, re-prove the plan it was authorised against,
run that one stage, and write down what it turned out to be.

Why it is not the generic campaign worker
-----------------------------------------
The generic worker's whole purpose is to retry what it finds. That is right for
an idempotent message job and catastrophically wrong here: EasyWeek publishes no
write idempotency key and Meta will deliver twice, so a retry after an ambiguous
outcome is how a customer is charged twice or messaged twice. This worker has no
retry path at all — not a backoff, not a maximum attempt count, not a requeue.
An operation it cannot finish becomes ``interrupted``, which a human resolves.

Why it is not ``BackgroundTasks``
---------------------------------
A stage over a real list takes minutes and must survive the request that started
it, the tab that was closed and the process that was restarted. A background task
inside the API survives none of those, and an operator who refreshed would have no
way to tell "still running" from "died silently". The durable operation row is
what makes the question answerable.

Startup is where a crash is caught
----------------------------------
The supported topology is one API and ONE executor, with no rolling deploy. So an
executor that is only now starting cannot be the owner of anything that claims to
be ``running``: every such row belongs to a process that died. It marks them
``interrupted`` before it takes any new work — never ``queued``, because the
per-item ledger may already hold a committed claim whose request went out.

What it never does
------------------
Approve anything. Extend a TTL. Pick a batch, a stage, a slot or an amount. Run
two stages. Retry an external call. Everything it acts on was decided by an
authenticated operator and stored before it ever saw the row.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
from datetime import timedelta

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_production import dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_log_redaction import redact_easyweek_url_logging
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

# How often the lease is pushed out while a stage runs. Comfortably inside
# :data:`operations.LEASE`, so an executor that is alive is never mistaken for one
# that died — and an executor that died stops renewing within one interval.
HEARTBEAT = timedelta(minutes=2)


def worker_identity() -> str:
    """Who owns a lease. Host and pid, which is enough to tell two apart."""
    return f"{socket.gethostname()}:{os.getpid()}"


async def _heartbeat(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    operation_id: int,
    owner: str,
    interval: float,
) -> None:
    """Renew the lease until cancelled. Never touches the operation's outcome."""
    while True:
        await asyncio.sleep(interval)
        try:
            await operations_module.renew_lease(session_maker, operation_id=operation_id, owner=owner)
        except SQLAlchemyError:
            # A renewal that cannot be written is not a reason to abandon a stage
            # that may be mid-flight. The lease will lapse and the sweep will call
            # the operation interrupted, which is the safe reading.
            logger.warning("voucher production executor could not renew its lease", exc_info=True)


async def run_once(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    owner: str,
    transports: dispatch_module.Transports | None = None,
    heartbeat_interval: float | None = None,
) -> operations_module.StoredOperation | None:
    """Claim at most one operation and run it to a terminal state.

    Returns the finished operation, or ``None`` when the queue was empty. Exactly
    one operation per call, deliberately: a worker that drained a queue in a loop
    without going back through the sweep would keep a stale view of the world.
    """
    claimed = await operations_module.claim_next_operation(session_maker, owner=owner)
    if claimed is None:
        return None

    logger.info(
        "voucher production executor claimed operation=%s stage=%s batch=%s",
        claimed.id,
        claimed.stage,
        claimed.batch_id,
    )
    interval = heartbeat_interval if heartbeat_interval is not None else HEARTBEAT.total_seconds()
    beat = asyncio.create_task(_heartbeat(session_maker, operation_id=claimed.id, owner=owner, interval=interval))
    try:
        finished = await dispatch_module.execute_operation(
            session_maker,
            operation=claimed,
            owner=owner,
            transports=transports,
        )
    finally:
        beat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beat

    if finished is not None:
        logger.info(
            "voucher production executor finished operation=%s status=%s outcome=%s",
            finished.id,
            finished.status,
            finished.outcome_code,
        )
    return finished


async def run_worker(
    session_maker: async_sessionmaker[AsyncSession] | None = None,
    *,
    owner: str | None = None,
    poll_sec: float | None = None,
    transports: dispatch_module.Transports | None = None,
    max_iterations: int | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Sweep once for abandoned work, then serve the queue until stopped.

    ``stop_event`` is checked BETWEEN operations and never during one. A SIGTERM
    that arrived mid-stage must not abandon a stage whose next line may be a
    payment: the stage is bounded, it writes each slot's outcome as it goes, and
    letting it finish is what keeps "stopped cleanly" distinct from "interrupted".
    An operator stop is the in-stage control, and it has its own durable row.

    ``max_iterations`` exists for tests and for a one-shot administrative run; the
    deployed worker leaves it ``None`` and runs until the container stops.
    """
    maker = session_maker or SessionLocal
    identity = owner or worker_identity()
    interval = poll_sec if poll_sec is not None else settings.easyweek_voucher_production_executor_poll_sec

    redact_easyweek_url_logging()

    # Before any new work. See the module docstring: with one executor in the
    # supported topology, a `running` row at start-up is a dead process's.
    abandoned = await operations_module.interrupt_abandoned(maker, include_all_running=True)
    for entry in abandoned:
        logger.warning(
            "voucher production operation=%s was running at start-up and is now interrupted; "
            "it will NOT be retried — reconcile and confirm a fresh plan",
            entry.id,
        )

    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        if stop_event is not None and stop_event.is_set():
            logger.info("voucher production executor stopping between operations")
            return
        iterations += 1
        try:
            # A second executor somebody deployed by mistake, or a worker killed
            # between its claim and its next renewal.
            await operations_module.interrupt_abandoned(maker)
            finished = await run_once(maker, owner=identity, transports=transports)
        except SQLAlchemyError:
            logger.exception("voucher production executor could not reach the database")
            finished = None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a worker loop must not die on one operation
            logger.exception("voucher production executor failed an iteration")
            finished = None
        if finished is None:
            if stop_event is not None:
                # Wake immediately on shutdown instead of sleeping out the poll
                # interval, while still never cutting a stage short.
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
            else:
                await asyncio.sleep(interval)


def _install_stop_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signal_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover - platform dependent
            logger.warning("Cannot install %s handler; graceful drain unavailable.", signal_name)


async def _run_with_graceful_shutdown() -> None:
    stop_event = asyncio.Event()
    _install_stop_handlers(stop_event)
    await run_worker(stop_event=stop_event)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(_run_with_graceful_shutdown())


__all__ = ["HEARTBEAT", "main", "run_once", "run_worker", "worker_identity"]
