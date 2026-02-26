from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from altegio_bot.workers import outbox_worker as ow


def run(coro: Any) -> Any:
    return asyncio.run(coro)


@dataclass
class FakeJob:
    id: int
    company_id: int
    job_type: str
    status: str
    run_at: datetime
    record_id: int | None = None
    client_id: int | None = None
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5
    locked_at: datetime | None = None


@dataclass
class FakeClient:
    id: int
    phone_e164: str | None = '+491234567890'
    wa_opted_out: bool = True


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)


def test_outbox_skips_marketing_when_opted_out(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    job = FakeJob(
        id=1,
        company_id=758285,
        job_type='review_3d',
        status='queued',
        run_at=fixed_now,
        record_id=None,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return None

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1)

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError('safe_send should not be called')

    monkeypatch.setattr(ow, '_load_job', fake_load_job)
    monkeypatch.setattr(ow, '_find_success_outbox', fake_find_success)
    monkeypatch.setattr(ow, '_load_record', fake_load_record)
    monkeypatch.setattr(ow, '_load_client', fake_load_client)
    monkeypatch.setattr(ow, 'safe_send', fake_safe_send)

    session = FakeSession()
    run(ow.process_job_in_session(session, 1, provider=object()))

    assert job.status == 'canceled'
    assert job.last_error == 'Skipped: client unsubscribed'
    assert session.added == []
