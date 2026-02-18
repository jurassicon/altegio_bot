from __future__ import annotations

import asyncio
from typing import Any

from altegio_bot.workers import outbox_worker as ow


class _BeginCM:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _Session:
    def begin(self) -> _BeginCM:
        return _BeginCM()

    async def execute(self, *_: Any, **__: Any) -> Any:
        class _Result:
            rowcount = 0

        return _Result()


class _SessionLocalCM:
    async def __aenter__(self) -> _Session:
        return _Session()

    async def __aexit__(
            self,
            exc_type: Any,
            exc: Any,
            tb: Any,
    ) -> None:
        return None


def _session_local_factory() -> _SessionLocalCM:
    return _SessionLocalCM()


def test_run_loop_sleeps_when_no_jobs(monkeypatch: Any) -> None:
    monkeypatch.setattr(ow, "SessionLocal", _session_local_factory)

    async def fake_lock_next_jobs(session: Any, batch_size: int) -> list[Any]:
        _ = session
        _ = batch_size
        return []

    async def fake_process_job(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("process_job must not be called")

    sleep_calls: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleep_calls.append(sec)
        raise asyncio.CancelledError

    monkeypatch.setattr(ow, "_lock_next_jobs", fake_lock_next_jobs)
    monkeypatch.setattr(ow, "process_job", fake_process_job)
    monkeypatch.setattr(ow.asyncio, "sleep", fake_sleep)

    try:
        asyncio.run(
            ow.run_loop(provider=object(), batch_size=50, poll_sec=1.5)
        )
        raise AssertionError("Expected CancelledError")
    except asyncio.CancelledError:
        pass

    assert sleep_calls == [1.5]


def test_run_loop_processes_job_ids(monkeypatch: Any) -> None:
    monkeypatch.setattr(ow, "SessionLocal", _session_local_factory)

    class Job:
        def __init__(self, jid: int) -> None:
            self.id = jid
            self.status = "queued"

    jobs = [Job(10), Job(11), Job(12)]

    async def fake_lock_next_jobs(session: Any, batch_size: int) -> list[Any]:
        _ = session
        _ = batch_size
        return jobs

    calls: list[int] = []

    async def fake_process_job(job_id: int, provider: Any) -> None:
        _ = provider
        calls.append(job_id)
        if len(calls) == len(jobs):
            raise asyncio.CancelledError

    monkeypatch.setattr(ow, "_lock_next_jobs", fake_lock_next_jobs)
    monkeypatch.setattr(ow, "process_job", fake_process_job)

    async def fake_sleep(_: float) -> None:
        raise AssertionError("sleep should not be called")

    monkeypatch.setattr(ow.asyncio, "sleep", fake_sleep)

    try:
        asyncio.run(
            ow.run_loop(provider=object(), batch_size=50, poll_sec=1.0)
        )
        raise AssertionError("Expected CancelledError")
    except asyncio.CancelledError:
        pass

    assert calls == [10, 11, 12]
