"""Graceful-drain contract of the WhatsApp inbox worker.

The PR-7.1 rollout stops this worker to fence the delivery-retry producer —
`_handle_failed_delivery_status` is the second source of EasyWeek lifecycle
jobs. Stopping it is only safe if the process drains what it already claimed.

Why stranding is permanent here, not merely untidy:

  * `lock_next_batch` commits ``received -> processing`` for the whole batch
    and only then processes the ids one by one;
  * the normal claim selects ``received`` only, so a ``processing`` row is
    never picked up again;
  * `recover_stale_processing_events` is scoped to Chatwoot operator-relay
    rows — ordinary inbound messages and Meta status callbacks are not covered.

So a SIGTERM landing between the claim commit and the end of the batch would
lose those events for good. The contract mirrors the Altegio inbox worker: the
stop flag is read only before claiming, a claimed batch is always finished, and
the idle wait ends immediately.

Driven with stubbed database helpers — no PostgreSQL, no Meta, no sessions.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any

import pytest

from altegio_bot.workers import whatsapp_inbox_worker as wa


class _FakeSession:
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


class _Provider:
    """The loop only passes this through to `process_one_event`."""


@pytest.fixture
def worker(monkeypatch):
    class Recorder:
        def __init__(self) -> None:
            self.batches: list[list[int]] = []
            self.claims = 0
            self.max_claims = 50
            self.processed: list[int] = []
            self.on_process = None

        async def lock_next_batch(self, session: Any, batch_size: int) -> list[_Event]:
            self.claims += 1
            # A worker that ignores the stop flag spins here forever and
            # starves the event loop, so `wait_for` never fires and the suite
            # hangs instead of failing. Cap the claims: a broken shutdown
            # contract then surfaces as a fast, readable error.
            if self.claims > self.max_claims:
                raise AssertionError(f"run_loop kept claiming after shutdown ({self.claims} claims)")
            ids = self.batches.pop(0) if self.batches else []
            return [_Event(i) for i in ids]

        async def process_one_event(self, event_id: int, provider: Any) -> None:
            self.processed.append(event_id)
            if self.on_process is not None:
                await self.on_process(event_id)

    recorder = Recorder()
    monkeypatch.setattr(wa, "SessionLocal", _FakeSession)
    monkeypatch.setattr(wa, "lock_next_batch", recorder.lock_next_batch)
    monkeypatch.setattr(wa, "process_one_event", recorder.process_one_event)
    # Operator-relay recovery is a separate concern; keep it out of the way.
    monkeypatch.setattr(wa, "recover_operator_relay_lifecycle", _noop_recovery)
    return recorder


async def _noop_recovery(provider: Any) -> Any:
    return None


# ===========================================================================
# Stop before claiming
# ===========================================================================


async def test_shutdown_before_the_first_claim_never_touches_the_queue(worker) -> None:
    stop_event = asyncio.Event()
    stop_event.set()
    worker.batches = [[1, 2, 3]]

    await asyncio.wait_for(wa.run_loop(_Provider(), stop_event=stop_event), timeout=5)

    assert worker.claims == 0, "lock_next_batch must not run after shutdown is requested"
    assert worker.processed == []


async def test_shutdown_after_a_claim_finishes_the_whole_batch(worker) -> None:
    """The batch is already `processing` in the DB — it must be drained."""
    stop_event = asyncio.Event()
    worker.batches = [[11, 12, 13]]

    async def stop_midway(event_id: int) -> None:
        if event_id == 11:
            stop_event.set()

    worker.on_process = stop_midway

    await asyncio.wait_for(wa.run_loop(_Provider(), poll_sec=0.01, stop_event=stop_event), timeout=5)

    assert worker.processed == [11, 12, 13], "a claimed batch must never be abandoned mid-flight"


async def test_no_second_batch_is_claimed_after_the_signal(worker) -> None:
    stop_event = asyncio.Event()
    worker.batches = [[21], [22]]

    async def stop_midway(event_id: int) -> None:
        stop_event.set()

    worker.on_process = stop_midway

    await asyncio.wait_for(wa.run_loop(_Provider(), poll_sec=0.01, stop_event=stop_event), timeout=5)

    assert worker.processed == [21]
    assert 22 not in worker.processed
    assert worker.claims == 1


async def test_one_failing_event_does_not_abort_the_drain(worker) -> None:
    """Per-event isolation must survive the shutdown path unchanged."""
    stop_event = asyncio.Event()
    worker.batches = [[31, 32, 33]]

    async def fail_middle(event_id: int) -> None:
        stop_event.set()
        if event_id == 32:
            raise RuntimeError("boom")

    worker.on_process = fail_middle

    await asyncio.wait_for(wa.run_loop(_Provider(), poll_sec=0.01, stop_event=stop_event), timeout=5)

    assert worker.processed == [31, 32, 33], "a failing id must not strand the rest of the claimed batch"


async def test_the_idle_wait_ends_on_the_signal(worker) -> None:
    stop_event = asyncio.Event()
    worker.batches = []

    async def request_stop() -> None:
        await asyncio.sleep(0.05)
        stop_event.set()

    started = time.monotonic()
    await asyncio.wait_for(
        asyncio.gather(
            wa.run_loop(_Provider(), poll_sec=30.0, stop_event=stop_event),
            request_stop(),
        ),
        timeout=5,
    )

    assert time.monotonic() - started < 5, "shutdown waited out the polling interval"
    assert worker.processed == []


async def test_waking_from_idle_does_not_grab_one_more_batch(worker) -> None:
    stop_event = asyncio.Event()
    worker.batches = []

    async def request_stop() -> None:
        await asyncio.sleep(0.05)
        stop_event.set()
        worker.batches.append([99])

    await asyncio.wait_for(
        asyncio.gather(
            wa.run_loop(_Provider(), poll_sec=0.01, stop_event=stop_event),
            request_stop(),
        ),
        timeout=5,
    )

    assert 99 not in worker.processed


async def test_without_a_stop_event_the_previous_polling_contract_holds(monkeypatch, worker) -> None:
    class _StopLoop(Exception):
        pass

    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)
        raise _StopLoop

    monkeypatch.setattr(wa.asyncio, "sleep", fake_sleep)
    worker.batches = []

    with pytest.raises(_StopLoop):
        await wa.run_loop(_Provider(), poll_sec=7.0)

    assert slept == [7.0]


# ===========================================================================
# The entrypoint has to actually arm all this
# ===========================================================================


def test_run_loop_accepts_a_stop_event_argument() -> None:
    assert "stop_event" in inspect.signature(wa.run_loop).parameters


def test_the_entrypoint_installs_signal_handlers_and_passes_the_event() -> None:
    source = inspect.getsource(wa.run_with_graceful_shutdown)

    assert "_install_stop_handlers" in source
    assert "stop_event=stop_event" in source

    handlers = inspect.getsource(wa._install_stop_handlers)
    for signal_name in ("SIGTERM", "SIGINT"):
        assert signal_name in handlers
    assert "add_signal_handler" in handlers


def test_the_script_entrypoint_uses_the_graceful_runner() -> None:
    """A worker whose entrypoint still calls run_loop() has no handlers."""
    from altegio_bot.scripts import run_whatsapp_inbox_worker as entrypoint

    source = inspect.getsource(entrypoint)
    assert "run_with_graceful_shutdown" in source


async def test_operator_relay_recovery_still_runs_on_startup(monkeypatch) -> None:
    """The drain work must not have removed the existing recovery pass."""
    calls: list[str] = []

    async def record_recovery(provider: Any) -> Any:
        calls.append("recovery")
        return None

    async def empty_batch(session: Any, batch_size: int) -> list[_Event]:
        return []

    monkeypatch.setattr(wa, "SessionLocal", _FakeSession)
    monkeypatch.setattr(wa, "lock_next_batch", empty_batch)
    monkeypatch.setattr(wa, "recover_operator_relay_lifecycle", record_recovery)

    stop_event = asyncio.Event()

    async def request_stop() -> None:
        await asyncio.sleep(0.05)
        stop_event.set()

    await asyncio.wait_for(
        asyncio.gather(
            wa.run_loop(_Provider(), poll_sec=0.01, stop_event=stop_event),
            request_stop(),
        ),
        timeout=5,
    )

    assert calls, "startup operator-relay recovery must still run"
