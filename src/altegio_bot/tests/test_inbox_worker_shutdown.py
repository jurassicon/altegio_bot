"""Graceful-drain contract of the Altegio inbox worker.

PR-3 swaps the unique constraints that the worker pins by name in
``ON CONFLICT ON CONSTRAINT``. The deploy therefore has to drain the OLD worker
to a state where no Altegio event is left in ``processing`` before the migration
runs — otherwise an event handled across the swap hits a missing constraint and
is stored as ``failed``, which nothing retries.

That drain is only possible if the worker honours a precise shutdown contract:

  * the stop flag is checked ONLY before claiming a new batch;
  * a batch already moved ``received -> processing`` is always finished;
  * claimed events are never pushed back to ``received`` and never marked
    ``failed`` just because a signal arrived;
  * the idle wait ends immediately instead of burning the polling interval.

These tests drive ``run_loop`` with stubbed database helpers — no PostgreSQL, no
external API, no real sessions.

SCOPE, stated explicitly so nobody reads more into them than is there:

    These tests prove the behaviour of the NEW worker version. They say nothing
    about the FIRST PR-3 rollout, where the container being stopped still runs
    the PARENT image — which has no signal handler at all and can be killed
    between claiming a batch and finishing it.

    That case is not covered by a signal handler and must not be presented as
    if it were. The deploy handles it separately, with a bounded orphan
    recovery: after every inbox worker is confirmed stopped, rows left in
    ``processing`` with no ``processed_at`` are returned to ``received``, and
    the migration only runs once that count is zero. See
    ``test_ci_deploy_order.py`` and ``.github/workflows/ci_deploy.yml``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from altegio_bot.workers import inbox_worker


class _FakeSession:
    """Stands in for ``SessionLocal()`` — the loop only uses it as a scope."""

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    def begin(self) -> _FakeSession:
        return self


class _Event:
    def __init__(self, event_id: int) -> None:
        self.id = event_id
        self.status = "received"


@pytest.fixture
def worker(monkeypatch):
    """Install stubs and record what the loop did.

    ``batches`` is the queue of batches ``lock_next_batch`` will hand out, in
    order; an exhausted queue yields an empty batch (the idle path).
    """

    class Recorder:
        def __init__(self) -> None:
            self.batches: list[list[int]] = []
            self.claims = 0
            self.processed: list[int] = []
            self.sleeps: list[float] = []
            self.on_process = None

        async def lock_next_batch(self, session: Any, batch_size: int) -> list[_Event]:
            self.claims += 1
            ids = self.batches.pop(0) if self.batches else []
            return [_Event(i) for i in ids]

        async def process_one_event(self, event_id: int) -> None:
            self.processed.append(event_id)
            if self.on_process is not None:
                await self.on_process(event_id)

    recorder = Recorder()
    monkeypatch.setattr(inbox_worker, "SessionLocal", _FakeSession)
    monkeypatch.setattr(inbox_worker, "lock_next_batch", recorder.lock_next_batch)
    monkeypatch.setattr(inbox_worker, "process_one_event", recorder.process_one_event)
    return recorder


# ===========================================================================
# Stop before claiming
# ===========================================================================


@pytest.mark.asyncio
async def test_shutdown_before_first_claim_never_touches_the_queue(worker) -> None:
    """A flag set before the loop starts must prevent any claim at all."""
    stop_event = asyncio.Event()
    stop_event.set()
    worker.batches = [[1, 2, 3]]

    await asyncio.wait_for(inbox_worker.run_loop(stop_event=stop_event), timeout=5)

    assert worker.claims == 0, "lock_next_batch must not run after shutdown is requested"
    assert worker.processed == []


# ===========================================================================
# Stop while idle
# ===========================================================================


@pytest.mark.asyncio
async def test_shutdown_while_idle_wakes_up_immediately(worker) -> None:
    """The idle wait must end on the signal, not after the polling interval."""
    stop_event = asyncio.Event()
    worker.batches = []  # always idle

    async def request_stop() -> None:
        await asyncio.sleep(0.05)
        stop_event.set()

    started = time.monotonic()
    await asyncio.wait_for(
        asyncio.gather(
            inbox_worker.run_loop(poll_sec=30.0, stop_event=stop_event),
            request_stop(),
        ),
        timeout=5,
    )
    elapsed = time.monotonic() - started

    assert elapsed < 5, f"shutdown waited for the polling interval ({elapsed:.2f}s)"
    assert worker.processed == []


@pytest.mark.asyncio
async def test_idle_shutdown_does_not_claim_another_batch(worker) -> None:
    """Waking from the idle wait must exit, not grab one more batch."""
    stop_event = asyncio.Event()
    worker.batches = []

    async def request_stop() -> None:
        await asyncio.sleep(0.05)
        stop_event.set()
        # Work appearing at the same moment must NOT be claimed on the way out.
        worker.batches.append([99])

    await asyncio.wait_for(
        asyncio.gather(
            inbox_worker.run_loop(poll_sec=0.01, stop_event=stop_event),
            request_stop(),
        ),
        timeout=5,
    )

    assert 99 not in worker.processed


@pytest.mark.asyncio
async def test_without_shutdown_the_idle_path_uses_a_plain_sleep(monkeypatch, worker) -> None:
    """Normal mode keeps the previous polling contract."""

    class _StopLoop(Exception):
        pass

    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)
        raise _StopLoop

    monkeypatch.setattr(inbox_worker.asyncio, "sleep", fake_sleep)
    worker.batches = []

    with pytest.raises(_StopLoop):
        await inbox_worker.run_loop(poll_sec=7.5)

    assert slept == [7.5]
    await real_sleep(0)


# ===========================================================================
# Stop after claiming — the case the whole drain depends on
# ===========================================================================


@pytest.mark.asyncio
async def test_shutdown_after_claim_finishes_the_whole_claimed_batch(worker) -> None:
    """A signal mid-batch must not strand the rest of the claimed rows.

    Every id in the batch is already ``processing`` in the database; abandoning
    it would leave rows no other worker picks up, and the deploy's
    "zero events in processing" gate would never clear.
    """
    stop_event = asyncio.Event()
    worker.batches = [[11, 12, 13, 14], [21, 22]]

    async def stop_after_first(event_id: int) -> None:
        if event_id == 11:
            stop_event.set()

    worker.on_process = stop_after_first

    await asyncio.wait_for(inbox_worker.run_loop(stop_event=stop_event), timeout=5)

    assert worker.processed == [11, 12, 13, 14], "the claimed batch was not drained completely"
    assert worker.claims == 1, "a second batch was claimed after shutdown"


@pytest.mark.asyncio
async def test_claimed_events_are_not_reset_or_failed_on_shutdown(worker) -> None:
    """Shutdown must not touch event status itself — only stop claiming.

    Any requeue/fail decision belongs to ``process_one_event``; a shutdown path
    that rewrote statuses would corrupt exactly the events it is meant to save.
    """
    stop_event = asyncio.Event()
    worker.batches = [[31, 32]]
    statuses: dict[int, str] = {}

    async def record_status(event_id: int) -> None:
        statuses[event_id] = "processed"
        stop_event.set()

    worker.on_process = record_status

    await asyncio.wait_for(inbox_worker.run_loop(stop_event=stop_event), timeout=5)

    assert statuses == {31: "processed", 32: "processed"}


@pytest.mark.asyncio
async def test_shutdown_between_batches_stops_before_the_next_claim(worker) -> None:
    stop_event = asyncio.Event()
    worker.batches = [[41], [42], [43]]

    async def stop_after_batch(event_id: int) -> None:
        stop_event.set()

    worker.on_process = stop_after_batch

    await asyncio.wait_for(inbox_worker.run_loop(stop_event=stop_event), timeout=5)

    assert worker.processed == [41]
    assert worker.claims == 1


# ===========================================================================
# Normal mode is unchanged
# ===========================================================================


@pytest.mark.asyncio
async def test_normal_mode_keeps_processing_batch_after_batch(worker) -> None:
    """Without a stop event the loop behaves exactly as before."""
    worker.batches = [[51, 52], [53]]

    class _Enough(Exception):
        pass

    async def stop_when_done(event_id: int) -> None:
        if event_id == 53:
            raise _Enough

    worker.on_process = stop_when_done

    with pytest.raises(_Enough):
        await asyncio.wait_for(inbox_worker.run_loop(poll_sec=0.01), timeout=5)

    assert worker.processed == [51, 52, 53]
    assert worker.claims == 2


@pytest.mark.asyncio
async def test_run_loop_accepts_a_stop_event_argument() -> None:
    """The deploy relies on this signature; keep it explicit."""
    import inspect

    parameters = inspect.signature(inbox_worker.run_loop).parameters
    assert "stop_event" in parameters
    assert parameters["stop_event"].default is None


def test_entrypoint_installs_signal_handlers() -> None:
    """``docker compose stop`` sends SIGTERM; it must reach the stop event."""
    import inspect

    source = inspect.getsource(inbox_worker._install_stop_handlers)
    assert "SIGTERM" in source
    assert "SIGINT" in source
    assert "add_signal_handler" in source

    entrypoint = inspect.getsource(inbox_worker._run_with_graceful_shutdown)
    assert "_install_stop_handlers" in entrypoint
    assert "stop_event=stop_event" in entrypoint

    assert "_run_with_graceful_shutdown" in inspect.getsource(inbox_worker.main)


def test_module_documents_that_this_does_not_cover_the_legacy_rollout() -> None:
    """Guard the caveat itself.

    The new signal handler must never be cited as protection for the parent
    container, so the limitation stays written down next to the tests that
    could otherwise be mistaken for covering it.
    """
    import altegio_bot.tests.test_inbox_worker_shutdown as module

    docstring = module.__doc__ or ""
    assert "PARENT image" in docstring
    assert "bounded orphan" in docstring
    assert "processed_at" in docstring
    assert "ci_deploy.yml" in docstring
