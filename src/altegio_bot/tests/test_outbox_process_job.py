from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
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
    payload: dict | None = None


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


# ---------------------------------------------------------------------------
# _is_permanent_meta_template_error
# ---------------------------------------------------------------------------


def test_permanent_error_132000_code() -> None:
    assert ow._is_permanent_meta_template_error(
        "Meta send_template failed status=400 body=(#132000) Number of parameters does not match"
    )


def test_permanent_error_number_of_parameters_phrase() -> None:
    assert ow._is_permanent_meta_template_error("number of parameters does not match the required count")


def test_permanent_error_does_not_match_expected() -> None:
    assert ow._is_permanent_meta_template_error("does not match the expected number of params for this template")


def test_permanent_error_required_parameter_missing() -> None:
    assert ow._is_permanent_meta_template_error("required parameter is missing in the request")


def test_permanent_error_template_does_not_exist() -> None:
    assert ow._is_permanent_meta_template_error("template does not exist in the system")


def test_permanent_error_template_name_does_not_exist() -> None:
    assert ow._is_permanent_meta_template_error("template name does not exist")


def test_permanent_error_does_not_exist_in_translation() -> None:
    assert ow._is_permanent_meta_template_error("does not exist in the translation for this language")


def test_permanent_error_status_400_alone_is_not_permanent() -> None:
    """Generic HTTP 400 (without specific Meta error) must NOT be treated as permanent."""
    assert not ow._is_permanent_meta_template_error("status=400 unknown error")


def test_permanent_error_500_is_not_permanent() -> None:
    assert not ow._is_permanent_meta_template_error("status=500 internal server error")


def test_permanent_error_empty_string_is_not_permanent() -> None:
    assert not ow._is_permanent_meta_template_error("")


# ---------------------------------------------------------------------------
# Campaign follow-up recipient backfill tests
# ---------------------------------------------------------------------------

KA_COMPANY = 758285
FOLLOWUP_JOB_TYPE = "newsletter_new_clients_followup"
MONTHLY_JOB_TYPE = "newsletter_new_clients_monthly"


@dataclass
class FakeCampaignRecipient:
    id: int = 0
    altegio_client_id: int | None = 9001
    followup_outbox_id: int | None = None
    followup_sent_at: datetime | None = None
    followup_status: str | None = "queued"
    # Primary campaign fields pre-populated to verify they are NOT overwritten.
    outbox_message_id: int | None = 555
    provider_message_id: str | None = "existing-msg-id"
    sent_at: datetime | None = datetime(2026, 1, 1, tzinfo=timezone.utc)
    booked_after_at: datetime | None = None


@dataclass
class FakeCampaignRecipientEmpty:
    """Recipient for monthly test — primary fields not yet set."""

    id: int = 0
    altegio_client_id: int | None = None
    followup_outbox_id: int | None = None
    followup_sent_at: datetime | None = None
    followup_status: str | None = None
    outbox_message_id: int | None = None
    provider_message_id: str | None = None
    sent_at: datetime | None = None
    booked_after_at: datetime | None = None


class _FollowupFakeSession:
    """FakeSession with async get/flush/execute for follow-up backfill tests."""

    def __init__(self, *, get_map: dict | None = None) -> None:
        self.added: list[Any] = []
        self._pk = 0
        self._get_map: dict = get_map or {}

    def add(self, obj: Any) -> None:
        if not hasattr(obj, "id") or getattr(obj, "id", None) is None:
            self._pk += 1
            obj.id = self._pk
        self.added.append(obj)

    async def flush(self) -> None:
        pass

    async def get(self, cls: Any, pk: Any) -> Any:
        return self._get_map.get((cls.__name__, pk))

    async def execute(self, _stmt: Any) -> Any:
        return SimpleNamespace(
            scalar_one_or_none=lambda: None,
            scalars=lambda: SimpleNamespace(first=lambda: None, all=lambda: []),
        )


def _patch_followup_common(
    monkeypatch: Any,
    *,
    job: Any,
    followup_eligible: bool = True,
    has_future_record: bool = False,
) -> None:
    """Patch all outbox_worker dependencies common to follow-up job tests."""

    async def _fake_load_job(_session: Any, _job_id: int) -> Any:
        return job

    monkeypatch.setattr(ow, "_load_job", _fake_load_job)
    monkeypatch.setattr(ow, "_find_success_outbox", lambda *a, **kw: _async_ret(None))
    monkeypatch.setattr(ow, "_find_existing_outbox", lambda *a, **kw: _async_ret(None))
    monkeypatch.setattr(ow, "_count_131026_failures", lambda *a, **kw: _async_ret(0))
    monkeypatch.setattr(ow, "_load_record", lambda *a, **kw: _async_ret(None))
    monkeypatch.setattr(ow, "_load_client", lambda *a, **kw: _async_ret(None))
    monkeypatch.setattr(ow, "_apply_rate_limit", lambda *a, **kw: _async_ret(None))
    monkeypatch.setattr(ow, "_render_message", lambda *a, **kw: _async_ret(("", 1, "de", {})))

    guard_result = SimpleNamespace(
        eligible=followup_eligible,
        booked_after_at=None,
        followup_status=None,
        skip_reason=None,
    )
    monkeypatch.setattr(ow, "check_followup_final_eligibility", lambda *a, **kw: _async_ret(guard_result))
    monkeypatch.setattr(
        ow,
        "client_has_any_future_record",
        lambda *a, **kw: _async_ret(has_future_record),
    )
    monkeypatch.setattr(ow, "safe_send_template", lambda *a, **kw: _async_ret(("msg-fu", None)))
    monkeypatch.setattr(ow, "safe_send", lambda *a, **kw: _async_ret(("msg-fu", None)))
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)
    monkeypatch.setattr(ow.settings, "meta_newsletter_followup_header_image_url", "https://cdn.example.com/fu.jpg")
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/m.jpg")


async def _async_ret(val: Any) -> Any:
    return val


def test_followup_job_backfills_followup_fields_on_success(monkeypatch: Any) -> None:
    """newsletter_new_clients_followup: followup_outbox_id/sent_at/status set; primary fields untouched."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)

    recipient = FakeCampaignRecipient(id=100, altegio_client_id=9001)
    run_obj = SimpleNamespace(completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc))

    session = _FollowupFakeSession(
        get_map={
            ("CampaignRecipient", 100): recipient,
            ("CampaignRun", 200): run_obj,
        }
    )

    job = FakeJob(
        id=3657,
        company_id=KA_COMPANY,
        job_type=FOLLOWUP_JOB_TYPE,
        status="queued",
        run_at=fixed_now,
        payload={
            "campaign_recipient_id": 100,
            "campaign_run_id": 200,
            "phone_e164": "+4917684571576",
            "contact_name": "Anna",
        },
    )

    _patch_followup_common(monkeypatch, job=job)

    run(ow.process_job_in_session(session, 3657, provider=object()))  # type: ignore

    assert job.status == "done", f"Expected done, got {job.status!r} last_error={job.last_error!r}"
    assert len(session.added) == 1
    out = session.added[0]

    # Follow-up fields must be set.
    assert recipient.followup_outbox_id == out.id, "followup_outbox_id must be set to out.id"
    assert recipient.followup_sent_at == fixed_now, "followup_sent_at must be set"
    assert recipient.followup_status == "sent", "followup_status must be 'sent'"

    # Primary campaign fields must NOT be overwritten.
    assert recipient.outbox_message_id == 555, "outbox_message_id must not be overwritten"
    assert recipient.provider_message_id == "existing-msg-id", "provider_message_id must not be overwritten"
    assert recipient.sent_at == datetime(2026, 1, 1, tzinfo=timezone.utc), "sent_at must not be overwritten"


def test_monthly_campaign_job_backfills_primary_fields_on_success(monkeypatch: Any) -> None:
    """newsletter_new_clients_monthly: primary fields set; followup fields not touched."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)

    recipient = FakeCampaignRecipientEmpty(id=101)

    session = _FollowupFakeSession(
        get_map={
            ("CampaignRecipient", 101): recipient,
        }
    )

    job = FakeJob(
        id=4000,
        company_id=KA_COMPANY,
        job_type=MONTHLY_JOB_TYPE,
        status="queued",
        run_at=fixed_now,
        payload={
            "campaign_recipient_id": 101,
            "campaign_run_id": 201,
            "phone_e164": "+4917684571576",
            "contact_name": "Anna",
            "loyalty_card_text": "Kundenkarte #001",
        },
    )

    _patch_followup_common(monkeypatch, job=job)
    # Monthly template needs booking_link to pass preflight validation (3 params).
    monkeypatch.setattr(
        ow,
        "_render_message",
        lambda *a, **kw: _async_ret(
            ("", 1, "de", {"client_name": "Anna", "booking_link": "https://n813709.alteg.io/"})
        ),
    )

    run(ow.process_job_in_session(session, 4000, provider=object()))  # type: ignore

    assert job.status == "done", f"Expected done, got {job.status!r} last_error={job.last_error!r}"
    assert len(session.added) == 1
    out = session.added[0]

    # Primary fields must be set.
    assert recipient.outbox_message_id == out.id, "outbox_message_id must be set"
    assert recipient.sent_at == fixed_now, "sent_at must be set"

    # Follow-up fields must NOT be touched.
    assert recipient.followup_outbox_id is None, "followup_outbox_id must not be set"
    assert recipient.followup_sent_at is None, "followup_sent_at must not be set"
    assert recipient.followup_status is None, "followup_status must not be set"


def test_followup_job_sends_normally_without_campaign_recipient_id(monkeypatch: Any) -> None:
    """Follow-up job with no campaign_recipient_id: job canceled (fail-closed), no send."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)

    session = _FollowupFakeSession()

    job = FakeJob(
        id=3700,
        company_id=KA_COMPANY,
        job_type=FOLLOWUP_JOB_TYPE,
        status="queued",
        run_at=fixed_now,
        # Deliberately missing campaign_recipient_id.
        payload={"phone_e164": "+4917684571576", "contact_name": "Anna"},
    )

    _patch_followup_common(monkeypatch, job=job)

    run(ow.process_job_in_session(session, 3700, provider=object()))  # type: ignore

    assert job.status == "canceled", f"Expected canceled, got {job.status!r}"
    assert job.last_error is not None
    assert "campaign_recipient_id" in job.last_error
    assert session.added == []  # no OutboxMessage


def test_followup_job_sends_normally_when_recipient_not_found(monkeypatch: Any) -> None:
    """Follow-up job where recipient_id is in payload but not in DB: job canceled (fail-closed)."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)

    # session.get(CampaignRecipient, 999) returns None → not found.
    session = _FollowupFakeSession(get_map={("CampaignRecipient", 999): None})

    job = FakeJob(
        id=3800,
        company_id=KA_COMPANY,
        job_type=FOLLOWUP_JOB_TYPE,
        status="queued",
        run_at=fixed_now,
        payload={
            "campaign_recipient_id": 999,
            "campaign_run_id": 200,
            "phone_e164": "+4917684571576",
            "contact_name": "Anna",
        },
    )

    _patch_followup_common(monkeypatch, job=job)

    run(ow.process_job_in_session(session, 3800, provider=object()))  # type: ignore

    assert job.status == "canceled", f"Expected canceled, got {job.status!r}"
    assert job.last_error is not None
    assert "campaign_recipient_id" in job.last_error
    assert session.added == []  # no OutboxMessage


# ---------------------------------------------------------------------------
# _backfill_campaign_recipient_after_send helper unit tests
# ---------------------------------------------------------------------------


def test_backfill_helper_sets_followup_fields(monkeypatch: Any) -> None:
    """Helper sets followup_* fields and never touches primary fields."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    recipient = FakeCampaignRecipient(id=100)
    session = _FollowupFakeSession(get_map={("CampaignRecipient", 100): recipient})

    run(
        ow._backfill_campaign_recipient_after_send(
            session=session,
            job_type=FOLLOWUP_JOB_TYPE,
            job_id=9999,
            payload={"campaign_recipient_id": 100},
            outbox_id=42,
            now_sent=fixed_now,
            provider_message_id="fu-msg-id",
        )
    )

    assert recipient.followup_outbox_id == 42
    assert recipient.followup_sent_at == fixed_now
    assert recipient.followup_status == "sent"
    # Primary fields must not be touched.
    assert recipient.outbox_message_id == 555
    assert recipient.provider_message_id == "existing-msg-id"
    assert recipient.sent_at == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_backfill_helper_does_not_downgrade_delivered_status(monkeypatch: Any) -> None:
    """Helper must not downgrade followup_status from 'delivered' to 'sent'."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    recipient = FakeCampaignRecipient(id=200)
    recipient.followup_status = "delivered"
    session = _FollowupFakeSession(get_map={("CampaignRecipient", 200): recipient})

    run(
        ow._backfill_campaign_recipient_after_send(
            session=session,
            job_type=FOLLOWUP_JOB_TYPE,
            job_id=9999,
            payload={"campaign_recipient_id": 200},
            outbox_id=43,
            now_sent=fixed_now,
        )
    )

    assert recipient.followup_status == "delivered"  # not downgraded


def test_backfill_helper_sets_primary_fields_for_monthly_job(monkeypatch: Any) -> None:
    """Helper sets primary fields for non-followup job type."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    recipient = FakeCampaignRecipientEmpty(id=300)
    session = _FollowupFakeSession(get_map={("CampaignRecipient", 300): recipient})

    run(
        ow._backfill_campaign_recipient_after_send(
            session=session,
            job_type=MONTHLY_JOB_TYPE,
            job_id=9999,
            payload={"campaign_recipient_id": 300},
            outbox_id=44,
            now_sent=fixed_now,
            provider_message_id="monthly-msg-id",
        )
    )

    assert recipient.outbox_message_id == 44
    assert recipient.provider_message_id == "monthly-msg-id"
    assert recipient.sent_at == fixed_now
    # Follow-up fields must not be touched.
    assert recipient.followup_outbox_id is None
    assert recipient.followup_sent_at is None


def test_backfill_helper_noop_when_no_campaign_recipient_id() -> None:
    """Helper returns early and does nothing when campaign_recipient_id is absent."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    session = _FollowupFakeSession()

    run(
        ow._backfill_campaign_recipient_after_send(
            session=session,
            job_type=MONTHLY_JOB_TYPE,
            job_id=9999,
            payload={},
            outbox_id=45,
            now_sent=fixed_now,
        )
    )

    assert session.added == []


# ---------------------------------------------------------------------------
# P3 — safe integer parsing: malformed campaign_recipient_id / campaign_run_id
# ---------------------------------------------------------------------------


def test_followup_job_invalid_campaign_recipient_id_cancels(monkeypatch: Any) -> None:
    """Malformed campaign_recipient_id='abc' → job canceled, no send."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)

    session = _FollowupFakeSession()

    job = FakeJob(
        id=3901,
        company_id=KA_COMPANY,
        job_type=FOLLOWUP_JOB_TYPE,
        status="queued",
        run_at=fixed_now,
        payload={"campaign_recipient_id": "abc", "campaign_run_id": 200, "phone_e164": "+49111"},
    )

    _patch_followup_common(monkeypatch, job=job)

    run(ow.process_job_in_session(session, 3901, provider=object()))  # type: ignore

    assert job.status == "canceled", f"Expected canceled, got {job.status!r}"
    assert job.last_error is not None
    assert "invalid campaign_recipient_id" in job.last_error
    assert session.added == []  # no OutboxMessage


def test_followup_job_invalid_campaign_run_id_cancels(monkeypatch: Any) -> None:
    """Malformed campaign_run_id='abc' with valid recipient → job canceled, no send."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ow, "utcnow", lambda: fixed_now)

    recipient = FakeCampaignRecipient(id=100)

    session = _FollowupFakeSession(get_map={("CampaignRecipient", 100): recipient})

    job = FakeJob(
        id=3902,
        company_id=KA_COMPANY,
        job_type=FOLLOWUP_JOB_TYPE,
        status="queued",
        run_at=fixed_now,
        payload={
            "campaign_recipient_id": 100,
            "campaign_run_id": "abc",
            "phone_e164": "+49111",
        },
    )

    _patch_followup_common(monkeypatch, job=job)

    run(ow.process_job_in_session(session, 3902, provider=object()))  # type: ignore

    assert job.status == "canceled", f"Expected canceled, got {job.status!r}"
    assert job.last_error is not None
    assert "invalid campaign_run_id" in job.last_error
    assert session.added == []  # no OutboxMessage


def test_backfill_helper_invalid_campaign_recipient_id_does_not_raise(monkeypatch: Any) -> None:
    """_backfill_campaign_recipient_after_send with 'abc' id must log and return, not crash."""
    fixed_now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    session = _FollowupFakeSession()

    run(
        ow._backfill_campaign_recipient_after_send(
            session=session,
            job_type=MONTHLY_JOB_TYPE,
            job_id=9999,
            payload={"campaign_recipient_id": "abc"},
            outbox_id=46,
            now_sent=fixed_now,
        )
    )

    assert session.added == []
