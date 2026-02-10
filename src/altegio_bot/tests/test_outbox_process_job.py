from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from altegio_bot.workers import outbox_worker as ow


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


@dataclass
class FakeClient:
    id: int
    display_name: str = "Anna"
    phone_e164: str | None = "+491234567890"


@dataclass
class FakeRecord:
    id: int
    company_id: int
    client_id: int | None = 1


@dataclass
class FakeOutbox:
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []
        self._pk = 0

    def add(self, obj: Any) -> None:
        if not hasattr(obj, "id"):
            self._pk += 1
            setattr(obj, "id", self._pk)
        self.added.append(obj)



def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_process_job_skips_if_outbox_exists(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    job = FakeJob(
        id=1,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return FakeOutbox(
            company_id=758285,
            job_id=job_id,
            status="sent",
            phone_e164="+491234",
            provider_message_id="x",
            error=None,
            id=99,
        )

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("safe_send should not be called")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession()
    run(ow.process_job_in_session(session, 1, provider=object()))  # type: ignore

    assert job.status == "done"
    assert job.last_error is None
    assert session.added == []


def test_process_job_fails_when_no_phone(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    job = FakeJob(
        id=2,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164=None)

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)

    session = FakeSession()
    run(ow.process_job_in_session(session, 2, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error == "No phone_e164"


def test_process_job_requeues_on_rate_limit(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    delay = datetime(2026, 2, 10, 12, 5, tzinfo=timezone.utc)
    job = FakeJob(
        id=3,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return delay

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)

    session = FakeSession()
    run(ow.process_job_in_session(session, 3, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.run_at == delay


def test_process_job_fails_on_template_render(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    job = FakeJob(
        id=4,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("boom")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)

    session = FakeSession()
    run(ow.process_job_in_session(session, 4, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error == "Template render error: boom"


def test_process_job_creates_outbox_on_send_ok(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    job = FakeJob(
        id=5,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de")

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        return ("msg-1", None)

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)

    session = FakeSession()
    run(ow.process_job_in_session(session, 5, provider=object()))  # type: ignore

    assert job.status == "done"
    assert job.last_error is None
    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "sent"
    assert out.provider_message_id == "msg-1"


def test_process_job_creates_outbox_on_send_fail(monkeypatch: Any) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    job = FakeJob(
        id=6,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de")

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        return ("msg-2", "provider error")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_find_existing_outbox", fake_find_existing)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)

    session = FakeSession()
    run(ow.process_job_in_session(session, 6, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error == "Send failed: provider error"
    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "failed"
    assert out.error == "provider error"
