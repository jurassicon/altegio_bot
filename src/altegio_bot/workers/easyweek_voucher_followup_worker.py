"""The dedicated executor of confirmed follow-up runs (§46.8, PR-22.3).

One worker, for one scenario. It takes a run an operator confirmed in the
browser, re-proves every recipient's eligibility, spends at most one extra
attempt each, and writes down what each turned out to be.

Why it is not the old follow-up worker
--------------------------------------
``workers/followup_worker.py`` drives the historical campaign follow-ups through
MessageJob and the Outbox, and §46 is explicit that it gains no new authority.
This process never touches those tables and that one never sees a row of this
ledger. The separation is the point: the generic path's whole purpose is to
retry what it finds, which is right for an idempotent job and catastrophically
wrong here — Meta will deliver twice, and §46.7 allows one extra message per
original recipient for the lifetime of the row.

Why it is not the production voucher executor either
----------------------------------------------------
That one owns CREATE, PAY, DELIVER and REFUND of real vouchers. This one creates
nothing, pays for nothing and refunds nothing; it repeats information about an
entitlement that already exists. Sharing a process would mean sharing a queue,
a lease and a fence, and §46.8 asks for the follow-up's own.

Why it is not ``BackgroundTasks``
---------------------------------
A run over a real audience must survive the request that started it, the tab
that was closed and the process that was restarted. The durable run row is what
makes "still running" answerable at all.

Startup is where a crash is caught
----------------------------------
The supported topology is one API and ONE executor, with no rolling deploy. So
an executor that is only now starting cannot own anything that claims to be
``running``: every such run belongs to a process that died. It interrupts them
before taking new work — and every row they left ``claimed`` becomes ``unknown``,
never ``planned``, because the attempt was spent before the POST and the message
may already be on its way to a real person.

What it never does
------------------
Authorise anything. Extend a TTL. Pick a batch, a preview version, a recipient
or a template. Retry an external call. Open the fence. Everything it acts on was
decided by an authenticated operator and committed before it saw the row.
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

from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as ledger_module
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_log_redaction import redact_easyweek_url_logging
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

# How often the lease is pushed out while a run executes. Comfortably inside
# :data:`...send_identity.LEASE`, so a live executor is never mistaken for a
# dead one — and a dead one stops renewing within one interval.
HEARTBEAT = timedelta(minutes=2)


def worker_identity() -> str:
    """Who owns a lease. Host and pid, which is enough to tell two apart."""
    return f"{socket.gethostname()}:{os.getpid()}"


async def _heartbeat(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    owner: str,
    interval: float,
) -> None:
    """Renew the lease until cancelled. Never touches the run's outcome."""
    while True:
        await asyncio.sleep(interval)
        try:
            await ledger_module.renew_lease(session_maker, run_id=run_id, owner=owner)
        except SQLAlchemyError:
            # A renewal that cannot be written is not a reason to abandon a run
            # that may be mid-flight. The lease lapses and the sweep calls the
            # run interrupted, which is the safe reading.
            logger.warning("followup executor could not renew its lease", exc_info=True)


async def run_once(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    owner: str,
    transports: dispatch_module.Transports | None = None,
    heartbeat_interval: float | None = None,
) -> ledger_module.StoredRun | None:
    """Claim at most one run and take it to a terminal state.

    Returns the finished run, or ``None`` when the queue was empty. Exactly one
    run per call, deliberately: a worker that drained a queue in a loop without
    going back through the sweep would keep a stale view of the world.
    """
    claimed = await ledger_module.claim_next_run(session_maker, owner=owner)
    if claimed is None:
        return None

    logger.info("followup executor claimed run=%s batch=%s", claimed.id, claimed.source_batch_id)
    interval = heartbeat_interval if heartbeat_interval is not None else HEARTBEAT.total_seconds()
    beat = asyncio.create_task(_heartbeat(session_maker, run_id=claimed.id, owner=owner, interval=interval))
    try:
        report = await dispatch_module.execute_run(
            session_maker,
            run=claimed,
            owner=owner,
            transports=transports,
        )
    finally:
        beat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beat

    logger.info(
        "followup executor finished run=%s outcome=%s attempted=%s requests=%s",
        report.run_id,
        report.outcome,
        report.attempted,
        report.meta_requests,
    )
    return await ledger_module.load_run(session_maker, run_id=claimed.id)


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

    ``stop_event`` is checked BETWEEN runs and never during one. A SIGTERM that
    arrived mid-run must not abandon a run whose next line is a message to a
    real person: the run is bounded, it writes each row's outcome as it goes,
    and letting it finish is what keeps "stopped cleanly" distinct from
    "interrupted". An operator stop is the in-run control, and it has its own
    durable column — the executor reads it before every claim.

    ``max_iterations`` exists for tests and for a one-shot administrative run;
    the deployed worker leaves it ``None``.
    """
    maker = session_maker or SessionLocal
    identity = owner or worker_identity()
    interval = poll_sec if poll_sec is not None else settings.easyweek_voucher_followup_executor_poll_sec

    redact_easyweek_url_logging()

    # Before any new work. See the module docstring: with one executor in the
    # supported topology, a `running` row at start-up is a dead process's, and
    # its claimed rows become honest unknowns.
    abandoned = await ledger_module.interrupt_abandoned_runs(maker, include_all_running=True)
    for entry in abandoned:
        logger.warning(
            "followup run=%s was running at start-up and is now interrupted; it will NOT be retried — "
            "every claimed row is recorded as unknown and reconciliation is a human decision",
            entry.id,
        )

    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        if stop_event is not None and stop_event.is_set():
            logger.info("followup executor stopping between runs")
            return
        iterations += 1
        try:
            # A second executor somebody deployed by mistake, or a worker killed
            # between its claim and its next renewal.
            await ledger_module.interrupt_abandoned_runs(maker)
            finished = await run_once(maker, owner=identity, transports=transports)
        except SQLAlchemyError:
            logger.exception("followup executor could not reach the database")
            finished = None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a worker loop must not die on one run
            logger.exception("followup executor failed an iteration")
            finished = None
        if finished is None:
            if stop_event is not None:
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
