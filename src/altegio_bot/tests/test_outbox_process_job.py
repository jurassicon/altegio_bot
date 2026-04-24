from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    staff_name: str = "Tanja"
    starts_at: datetime | None = None
    short_link: str = ""


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


# Complete render context for kitilash_ka_record_updated_v1 (7 params).
# Tests that exercise send/retry logic use this to pass preflight validation.
_RECORD_UPDATED_CTX: dict = {
    "client_name": "Anna",
    "staff_name": "Tanja",
    "date": "01.05.2026",
    "time": "10:00",
    "services": "Haarschnitt",
    "total_cost": "30.00",
    "short_link": "https://example.com",
}


def patch_outbox_checks(
    monkeypatch: Any,
    *,
    result: Any,
) -> None:
    async def _fake_find_success(session: Any, job_id: int) -> Any:
        return result

    async def _fake_find_existing(session: Any, job_id: int) -> Any:
        return result

    async def _fake_count_131026(session: Any, phone: str, window_days: int) -> int:
        return 0

    monkeypatch.setattr(ow, "_find_success_outbox", _fake_find_success)
    monkeypatch.setattr(ow, "_find_existing_outbox", _fake_find_existing)
    monkeypatch.setattr(ow, "_count_131026_failures", _fake_count_131026)


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

    existing = FakeOutbox(
        id=99,
        company_id=758285,
        job_id=1,
        status="sent",
        phone_e164="+491234",
        provider_message_id="x",
        error=None,
    )

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("safe_send should not be called")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=existing)
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

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164=None)

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
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

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return delay

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
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

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("boom")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
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

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de", _RECORD_UPDATED_CTX)

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        return ("msg-1", None)

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)

    session = FakeSession()
    run(ow.process_job_in_session(session, 5, provider=object()))  # type: ignore

    assert job.status == "done"
    assert job.last_error is None
    assert job.attempts == 1

    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "sent"
    assert out.provider_message_id == "msg-1"
    assert out.scheduled_at == fixed_now
    assert out.sent_at == fixed_now


def test_process_job_requeues_on_send_fail(monkeypatch: Any) -> None:
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

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de", _RECORD_UPDATED_CTX)

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        return ("msg-2", "provider error")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)

    session = FakeSession()
    run(ow.process_job_in_session(session, 6, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.last_error == "Send failed: provider error"
    assert job.attempts == 1
    assert job.run_at == fixed_now + timedelta(seconds=30)

    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "failed"
    assert out.error == "provider error"
    assert out.provider_message_id == "msg-2"
    assert out.scheduled_at == fixed_now
    assert out.sent_at == fixed_now


def test_process_job_fails_when_max_attempts_reached_before_send(
    monkeypatch: Any,
) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    job = FakeJob(
        id=7,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
        attempts=5,
        max_attempts=5,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("safe_send should not be called")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)

    session = FakeSession()
    run(ow.process_job_in_session(session, 7, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error == "Max attempts reached"
    assert session.added == []


def test_process_job_fails_on_send_fail_when_attempt_becomes_max(
    monkeypatch: Any,
) -> None:
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    job = FakeJob(
        id=8,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
        attempts=4,
        max_attempts=5,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de", _RECORD_UPDATED_CTX)

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        return ("msg-3", "provider error")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)

    session = FakeSession()
    run(ow.process_job_in_session(session, 8, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error == "Send failed: provider error"
    assert job.attempts == 5

    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "failed"
    assert out.provider_message_id == "msg-3"


def test_process_job_fails_when_no_template_in_auto_mode(
    monkeypatch: Any,
) -> None:
    """In auto/template mode: no Meta template → job must fail, no text fallback."""
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    job = FakeJob(
        id=9,
        company_id=999999,  # unknown company → no template
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=999999)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de", {})

    async def fake_safe_send(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("safe_send must not be called when template is missing")

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send)
    # Force auto mode (default, but be explicit)
    from altegio_bot.settings import Settings

    monkeypatch.setattr(
        ow,
        "settings",
        Settings.model_construct(whatsapp_send_mode="auto"),
    )

    session = FakeSession()
    run(ow.process_job_in_session(session, 9, provider=object()))  # type: ignore

    assert job.status == "failed"
    assert job.last_error is not None
    assert "No Meta template" in job.last_error
    assert session.added == []  # no outbox record when template lookup fails


def test_process_job_sends_text_when_mode_is_text(
    monkeypatch: Any,
) -> None:
    """In text mode free-form send is used even if template would exist."""
    fixed_now = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)

    job = FakeJob(
        id=10,
        company_id=758285,
        job_type="record_updated",
        status="queued",
        run_at=fixed_now,
        record_id=10,
        client_id=1,
    )

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=758285)

    async def fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164="+491234")

    async def fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("TEXT", 123, "de", {})

    safe_send_template_called: list[bool] = []

    async def fake_safe_send_text(*args: Any, **kwargs: Any) -> Any:
        return ("msg-text", None)

    async def fake_safe_send_tpl(*args: Any, **kwargs: Any) -> Any:
        safe_send_template_called.append(True)
        return ("msg-tpl", None)

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    patch_outbox_checks(monkeypatch, result=None)
    monkeypatch.setattr(ow, "_load_record", fake_load_record)
    monkeypatch.setattr(ow, "_load_client", fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", fake_render)
    monkeypatch.setattr(ow, "safe_send", fake_safe_send_text)
    monkeypatch.setattr(ow, "safe_send_template", fake_safe_send_tpl)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)
    from altegio_bot.settings import Settings

    monkeypatch.setattr(
        ow,
        "settings",
        Settings.model_construct(whatsapp_send_mode="text"),
    )

    session = FakeSession()
    run(ow.process_job_in_session(session, 10, provider=object()))  # type: ignore

    assert job.status == "done"
    assert safe_send_template_called == []  # template send must not be called
    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "sent"
    assert out.provider_message_id == "msg-text"
    assert out.meta == {"send_type": "text"}
