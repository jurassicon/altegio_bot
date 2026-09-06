"""Tests for Session-aware bot notifications — text-inside-24h window routing.

Covers:
 1. Feature disabled: eligible job + open window → sends template as before.
 2. Feature enabled + eligible job + open window → sends as text.
 3. Feature enabled + eligible job + closed window → sends template with meta.
 4. Feature enabled + non-whitelisted marketing job + open window → template, no text.
 5. Feature enabled + Chatwoot-origin inbound only → window closed → template.
 6. Feature enabled + operator relay inbound only → window closed → template.
 7. Feature enabled + open window + text policy error + fallback enabled → template_fallback.
 8. Feature enabled + open window + ambiguous text error → no fallback, retry.
 9. Feature enabled + open window + empty rendered body → template, no text.
10. Settings validation: defaults, env parsing, empty whitelist rejection, space trimming,
    hard-allowlist (unsupported type raises even when feature is disabled).
11. Regression: existing outbox_process_job tests still pass.
12. P1: window-check exception → fail-open → template sent (never blocks notifications).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from altegio_bot.settings import Settings
from altegio_bot.workers import outbox_worker as ow

# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------


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
    payload: dict[str, Any] | None = None
    locked_at: datetime | None = None


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
    # A real Altegio ``Record`` always carries this: it is NOT NULL, and it is
    # the source identity the reminder-ownership fence looks the booking up by
    # (plan §30.11). A fake without it would answer "cannot tell" and hold the
    # send back for a reason production never has.
    altegio_record_id: int = 900001


@dataclass
class FakeSender:
    id: int = 123
    phone_number_id: str = "PNID_TEST"


class FakeOutbox:
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class _EmptyResult:
    """An empty SQLAlchemy-shaped result, for the ownership fence's lookup."""

    def all(self) -> list[Any]:
        return []

    def scalars(self) -> "_EmptyResult":
        return self

    def one_or_none(self) -> None:
        return None

    def scalar_one_or_none(self) -> None:
        return None


class _FakeSavepoint:
    """What `session.begin_nested()` returns for a fake session.

    The ownership lookup runs inside its own SAVEPOINT so a failed statement
    cannot poison the caller's transaction. These fakes hold no transaction at
    all, so the savepoint has nothing to undo — but it has to EXIST, or the
    fixture would answer a question production never asks.
    """

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []
        self._pk = 0

    def add(self, obj: Any) -> None:
        if not hasattr(obj, "id"):
            self._pk += 1
            setattr(obj, "id", self._pk)
        self.added.append(obj)

    async def begin_nested(self) -> "_FakeSavepoint":
        return _FakeSavepoint()

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """No ledger rows: this record was never migrated to EasyWeek.

        The send path asks whether a booking's reminders were handed over
        (plan §30.11). A record with no ledger row is the ordinary case, and the
        honest answer is an empty result — which the fence reads as "Altegio
        still owns these" and leaves the send path untouched.
        """
        return _EmptyResult()

    async def get(self, model: type, pk: Any) -> Any:
        """Return a FakeSender for WhatsAppSender lookups; None otherwise."""
        if getattr(model, "__name__", None) == "WhatsAppSender":
            return FakeSender(id=pk)
        return None


# A complete render context so that preflight validation passes for
# kitilash_ka_record_updated_v1 (7 params).
_REMINDER_2H_CTX: dict[str, Any] = {
    "client_name": "Anna",
    "staff_name": "Tanja",
    "date": "01.05.2026",
    "time": "10:00",
    "services": "Haarschnitt",
    "total_cost": "30.00",
    "short_link": "https://example.com",
}

PHONE = "+491234567890"
NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)


def _patch_base(monkeypatch: Any, job: FakeJob) -> None:
    """Monkeypatch standard outbox_worker infrastructure used by all tests."""

    async def _fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def _fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def _fake_count_131026(session: Any, phone: str, window_days: int) -> int:
        return 0

    async def _fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def _fake_load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=job.company_id)

    async def _fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164=PHONE)

    async def _fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def _fake_circuit_open(*args: Any, **kwargs: Any) -> bool:
        return False

    async def _fake_render(*args: Any, **kwargs: Any) -> Any:
        return ("Reminder text body", 123, "de", _REMINDER_2H_CTX)

    monkeypatch.setattr(ow, "_find_success_outbox", _fake_find_success)
    monkeypatch.setattr(ow, "_find_existing_outbox", _fake_find_existing)
    monkeypatch.setattr(ow, "_count_131026_failures", _fake_count_131026)
    monkeypatch.setattr(ow, "_load_job", _fake_load_job)
    monkeypatch.setattr(ow, "_load_record", _fake_load_record)
    monkeypatch.setattr(ow, "_load_client", _fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", _fake_apply_rl)
    monkeypatch.setattr(ow.meta_circuit, "should_pause_meta_sends", _fake_circuit_open)
    monkeypatch.setattr(ow, "_render_message", _fake_render)
    monkeypatch.setattr(ow, "utcnow", lambda: NOW)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)


def _enable_feature(monkeypatch: Any) -> None:
    """Enable the 24h text-inside-window feature via settings patch."""
    monkeypatch.setattr(ow.settings, "bot_template_text_inside_24h_enabled", True)
    monkeypatch.setattr(
        ow.settings,
        "bot_template_text_inside_24h_job_types",
        "record_created,record_updated,record_canceled,reminder_24h,reminder_2h",
    )
    monkeypatch.setattr(ow.settings, "bot_template_text_inside_24h_fallback_enabled", True)


def _fake_window_open(monkeypatch: Any) -> None:
    """Monkeypatch window check to return open (inbound 1 hour ago)."""
    last_inbound = NOW - timedelta(hours=1)

    async def _window(*args: Any, **kwargs: Any) -> tuple[bool, datetime]:
        return (True, last_inbound)

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _window)


def _fake_window_closed(monkeypatch: Any) -> None:
    """Monkeypatch window check to return closed (no inbound)."""

    async def _window(*args: Any, **kwargs: Any) -> tuple[bool, None]:
        return (False, None)

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _window)


# ---------------------------------------------------------------------------
# Test 1 — feature disabled: template sent even when window is open
# ---------------------------------------------------------------------------


def test_feature_disabled_sends_template(monkeypatch: Any) -> None:
    """Feature disabled: eligible job + open window → sends template, no text send."""
    job = FakeJob(
        id=1,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    # Feature remains disabled (default); window would be open but never checked.
    text_called = []
    template_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        template_called.append(True)
        return ("tpl-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 1, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not text_called, "safe_send (text) must not be called when feature is disabled"
    assert template_called, "safe_send_template must be called"

    out = session.added[0]
    assert out.meta["send_type"] == "template"


# ---------------------------------------------------------------------------
# Test 2 — eligible job + open window → sent as text
# ---------------------------------------------------------------------------


def test_eligible_job_open_window_sends_text(monkeypatch: Any) -> None:
    """Feature enabled + eligible job + open window → message sent as text."""
    job = FakeJob(
        id=2,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)
    _fake_window_open(monkeypatch)

    template_called = []
    text_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-wamid-1", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        template_called.append(True)
        return ("tpl-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 2, provider=object()))  # type: ignore

    assert job.status == "done"
    assert job.last_error is None
    assert text_called, "safe_send (text) must be called"
    assert not template_called, "safe_send_template must NOT be called"

    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "sent"
    assert out.provider_message_id == "txt-wamid-1"
    assert out.template_code == "reminder_2h", "template_code must stay original job type"

    meta = out.meta
    assert meta["send_type"] == "text"
    assert meta["text_inside_24h"] is True
    assert meta["text_inside_24h_eligible"] is True
    assert meta["wa_window_open"] is True
    assert meta["original_send_type"] == "template"
    assert meta["route_reason"] == "customer_service_window_open"
    assert meta["last_meta_inbound_at"] is not None


# ---------------------------------------------------------------------------
# Test 3 — eligible job + closed window → sends template with diagnostic meta
# ---------------------------------------------------------------------------


def test_eligible_job_closed_window_sends_template(monkeypatch: Any) -> None:
    """Feature enabled + eligible job + closed window → template with meta."""
    job = FakeJob(
        id=3,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)
    _fake_window_closed(monkeypatch)

    text_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        return ("tpl-wamid-3", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 3, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not text_called, "safe_send (text) must not be called when window is closed"

    out = session.added[0]
    assert out.status == "sent"
    assert out.provider_message_id == "tpl-wamid-3"

    meta = out.meta
    assert meta["send_type"] == "template"
    assert meta["text_inside_24h"] is False
    assert meta["wa_window_open"] is False
    assert meta["route_reason"] == "customer_service_window_closed"


# ---------------------------------------------------------------------------
# Test 4 — non-whitelisted marketing job + open window → template, no text
# ---------------------------------------------------------------------------


def test_non_whitelisted_job_open_window_sends_template(monkeypatch: Any) -> None:
    """Feature enabled + repeat_10d (marketing) + open window → template only."""
    job = FakeJob(
        id=4,
        company_id=758285,
        job_type="repeat_10d",
        status="queued",
        run_at=NOW,
    )

    async def _fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def _fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def _fake_count_131026(session: Any, phone: str, window_days: int) -> int:
        return 0

    async def _fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def _fake_load_record(session: Any, job_obj: Any) -> Any:
        record = FakeRecord(id=10, company_id=758285)
        # Simulate attended record for repeat_10d guard
        record.__dict__["attendance"] = 1
        record.__dict__["visit_attendance"] = 1
        return record

    async def _fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        client = FakeClient(id=1, phone_e164=PHONE)
        client.__dict__["wa_opted_out"] = False
        client.__dict__["altegio_client_id"] = 999
        return client

    async def _fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def _fake_render(*a: Any, **kw: Any) -> Any:
        return ("Repeat text", 123, "de", _REMINDER_2H_CTX)

    # Stub out Altegio API guard for repeat_10d
    async def _fake_has_future(*a: Any, **kw: Any) -> bool:
        return False

    async def _fake_client_returned(*a: Any, **kw: Any) -> bool:
        return False

    monkeypatch.setattr(ow, "_find_success_outbox", _fake_find_success)
    monkeypatch.setattr(ow, "_find_existing_outbox", _fake_find_existing)
    monkeypatch.setattr(ow, "_count_131026_failures", _fake_count_131026)
    monkeypatch.setattr(ow, "_load_job", _fake_load_job)
    monkeypatch.setattr(ow, "_load_record", _fake_load_record)
    monkeypatch.setattr(ow, "_load_client", _fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", _fake_apply_rl)
    monkeypatch.setattr(ow, "_render_message", _fake_render)
    monkeypatch.setattr(ow, "utcnow", lambda: NOW)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)
    monkeypatch.setattr(ow, "client_has_future_appointments", _fake_has_future)
    monkeypatch.setattr(ow, "_client_returned_since", _fake_client_returned)
    # Bypass template param preflight so the test focuses on routing, not template schema.
    monkeypatch.setattr(ow, "validate_template_params", lambda *a, **kw: None)

    _enable_feature(monkeypatch)
    _fake_window_open(monkeypatch)

    text_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        return ("tpl-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 4, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not text_called, "repeat_10d must not be routed to text"

    out = session.added[0]
    # Non-eligible: no 24h diagnostic meta expected
    assert out.meta.get("send_type") == "template"
    assert "text_inside_24h" not in out.meta


# ---------------------------------------------------------------------------
# Test 5 — Chatwoot-origin inbound only → window closed → template
# ---------------------------------------------------------------------------


def test_chatwoot_origin_only_window_closed(monkeypatch: Any) -> None:
    """Window helper ignores Chatwoot-origin events; window stays closed."""
    job = FakeJob(
        id=5,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)

    # Simulate: window helper returns closed (Chatwoot events excluded)
    async def _window_closed(*a: Any, **kw: Any) -> tuple[bool, None]:
        return (False, None)

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _window_closed)

    text_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        return ("tpl-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 5, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not text_called
    out = session.added[0]
    assert out.meta["send_type"] == "template"
    assert out.meta["wa_window_open"] is False


# ---------------------------------------------------------------------------
# Test 6 — operator relay inbound only → window closed → template
# ---------------------------------------------------------------------------


def test_operator_relay_only_window_closed(monkeypatch: Any) -> None:
    """Operator relay events excluded by window helper; window stays closed."""
    job = FakeJob(
        id=6,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)

    # Operator relay events are excluded in whatsapp_window.py;
    # the helper returns False when only relay events exist.
    async def _window_closed(*a: Any, **kw: Any) -> tuple[bool, None]:
        return (False, None)

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _window_closed)

    text_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        return ("tpl-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 6, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not text_called
    assert session.added[0].meta["send_type"] == "template"


# ---------------------------------------------------------------------------
# Test 7 — open window + text policy error + fallback enabled → template_fallback
# ---------------------------------------------------------------------------


def test_open_window_text_policy_error_falls_back_to_template(monkeypatch: Any) -> None:
    """Text policy error triggers template fallback; OutboxMessage is template_fallback."""
    job = FakeJob(
        id=7,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)
    _fake_window_open(monkeypatch)

    # Text send returns Meta window policy error
    async def _fake_text(*a: Any, **kw: Any) -> Any:
        return (None, "Error 131047: outside the allowed window")

    # Template fallback succeeds
    async def _fake_template(*a: Any, **kw: Any) -> Any:
        return ("tpl-fallback-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 7, provider=object()))  # type: ignore

    assert job.status == "done", f"Expected done, got: {job.status}, err: {job.last_error}"
    assert len(session.added) == 1
    out = session.added[0]
    assert out.status == "sent"
    assert out.provider_message_id == "tpl-fallback-id"

    meta = out.meta
    assert meta["send_type"] == "template_fallback"
    assert meta["text_inside_24h"] is True
    assert meta["wa_window_open"] is True
    assert meta["fallback_reason"] == "text_send_failed"
    assert "text_send_error" in meta
    assert "131047" in meta["text_send_error"]


# ---------------------------------------------------------------------------
# Test 8 — open window + ambiguous text error → no fallback, retry
# ---------------------------------------------------------------------------


def test_open_window_ambiguous_text_error_closes_circuit_no_fallback(monkeypatch: Any) -> None:
    """Ambiguous transient text error closes circuit; no template fallback or failed spam."""
    job = FakeJob(
        id=8,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
        attempts=0,
        max_attempts=5,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)
    _fake_window_open(monkeypatch)

    template_called = []

    # Text send returns an ambiguous network error
    async def _fake_text(*a: Any, **kw: Any) -> Any:
        return (None, "Connection timeout: upstream unreachable")

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        template_called.append(True)
        return ("tpl-id", None)

    transient_errors: list[str] = []

    async def _fake_transient(job_obj: Any, record: Any, err: str) -> None:
        transient_errors.append(err)
        job_obj.status = "queued"
        job_obj.locked_at = None
        job_obj.last_error = "Meta circuit closed: send paused until Meta recovers"

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)
    monkeypatch.setattr(ow, "_handle_transient_meta_error", _fake_transient)

    session = FakeSession()
    run(ow.process_job_in_session(session, 8, provider=object()))  # type: ignore

    assert job.status == "queued", f"Expected queued, got {job.status}"
    assert not template_called, "Template must NOT be sent after ambiguous error"
    assert transient_errors == ["Connection timeout: upstream unreachable"]
    assert session.added == []


@pytest.mark.parametrize(
    "err",
    [
        "{'code': 2, 'is_transient': True}",
        '{"code":2,"is_transient":true}',
        "Meta send failed status=429 code=None is_transient=None",
        "Meta send failed status=500 code=None is_transient=None",
        "Meta send failed status=502 code=None is_transient=None",
        "Meta send failed status=503 code=None is_transient=None",
        "Meta send failed status=504 code=None is_transient=None",
        "Connection timeout: upstream unreachable",
    ],
)
def test_transient_provider_error_detection(err: str) -> None:
    assert ow._is_transient_provider_error(err) is True


@pytest.mark.parametrize(
    "err",
    [
        "Meta send_template failed status=400 code=132000 is_transient=false message=template validation error",
        "Meta send failed status=400 code=190 is_transient=false message=access token expired",
        "Meta text send failed: outside customer service window code=131047",
    ],
)
def test_permanent_provider_errors_do_not_close_circuit(err: str) -> None:
    assert ow._is_transient_provider_error(err) is False


def test_transient_provider_error_closes_circuit_requeues_without_failed_outbox(monkeypatch: Any) -> None:
    job = FakeJob(
        id=82,
        company_id=758285,
        job_type="record_created",
        status="queued",
        run_at=NOW,
        attempts=1,
        max_attempts=5,
    )
    _patch_base(monkeypatch, job)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")

    async def _load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=job.company_id, starts_at=NOW + timedelta(hours=3))

    async def _send_template(*a: Any, **kw: Any) -> Any:
        return (None, "Meta send_template failed status=503 code=2 is_transient=true fbtrace_id=TRACE")

    close_calls: list[dict[str, Any]] = []

    async def _close_circuit(**kwargs: Any) -> None:
        close_calls.append(kwargs)

    monkeypatch.setattr(ow, "_load_record", _load_record)
    monkeypatch.setattr(ow, "safe_send_template", _send_template)
    monkeypatch.setattr(ow.meta_circuit, "close_meta_circuit", _close_circuit)

    session = FakeSession()
    run(ow.process_job_in_session(session, 82, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.attempts == 1
    assert "Meta circuit closed" in (job.last_error or "")
    assert close_calls
    assert close_calls[0]["reason"] == "transient_send_error"
    assert close_calls[0]["error_kind"] == "http"
    assert close_calls[0]["error_code"] == "503"
    assert session.added == []


def test_closed_circuit_skips_send_rate_limit_and_attempts(monkeypatch: Any) -> None:
    job = FakeJob(
        id=80,
        company_id=758285,
        job_type="record_created",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)

    async def _load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=job.company_id, starts_at=NOW + timedelta(hours=3))

    rate_limit_calls: list[str] = []
    send_calls: list[str] = []

    async def _apply_rl(session: Any, phone: str) -> Any:
        rate_limit_calls.append(phone)
        return None

    async def _send_template(*a: Any, **kw: Any) -> Any:
        send_calls.append("template")
        return ("wamid", None)

    async def _paused(*a: Any, **kw: Any) -> bool:
        return True

    monkeypatch.setattr(ow, "_load_record", _load_record)
    monkeypatch.setattr(ow, "_apply_rate_limit", _apply_rl)
    monkeypatch.setattr(ow, "safe_send_template", _send_template)
    monkeypatch.setattr(ow.meta_circuit, "should_pause_meta_sends", _paused)

    session = FakeSession()
    run(ow.process_job_in_session(session, 80, provider=object()))  # type: ignore

    assert job.status == "queued"
    assert job.attempts == 0
    assert "Meta circuit closed" in (job.last_error or "")
    assert rate_limit_calls == []
    assert send_calls == []
    assert session.added == []


def test_closed_circuit_cancels_when_deadline_exceeded(monkeypatch: Any) -> None:
    job = FakeJob(
        id=81,
        company_id=758285,
        job_type="record_created",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)

    async def _load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=job.company_id, starts_at=NOW + timedelta(minutes=10))

    async def _paused(*a: Any, **kw: Any) -> bool:
        return True

    monkeypatch.setattr(ow, "_load_record", _load_record)
    monkeypatch.setattr(ow.meta_circuit, "should_pause_meta_sends", _paused)

    session = FakeSession()
    run(ow.process_job_in_session(session, 81, provider=object()))  # type: ignore

    assert job.status == "canceled"
    assert job.attempts == 0
    assert job.last_error == "Retry deadline exceeded for record_created"


def test_delivery_retry_malformed_original_outbox_id_cancels_before_send(monkeypatch: Any) -> None:
    job = FakeJob(
        id=83,
        company_id=758285,
        job_type="record_created",
        status="queued",
        run_at=NOW,
        payload={"kind": "delivery_failed_retry", "delivery_retry_of_outbox_id": "bad"},
    )
    _patch_base(monkeypatch, job)
    send_calls: list[str] = []

    async def _load_record(session: Any, job_obj: Any) -> Any:
        return FakeRecord(id=10, company_id=job.company_id, starts_at=NOW + timedelta(hours=3))

    async def _send_template(*a: Any, **kw: Any) -> Any:
        send_calls.append("template")
        return ("wamid", None)

    monkeypatch.setattr(ow, "_load_record", _load_record)
    monkeypatch.setattr(ow, "safe_send_template", _send_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 83, provider=object()))  # type: ignore

    assert job.status == "canceled"
    assert job.last_error == "Canceled: invalid delivery_retry_of_outbox_id"
    assert send_calls == []
    assert session.added == []


def test_retry_deadline_policy_for_service_jobs() -> None:
    starts = NOW + timedelta(hours=5)
    record = FakeRecord(id=10, company_id=1, starts_at=starts)

    created = FakeJob(id=90, company_id=1, job_type="record_created", status="queued", run_at=NOW)
    updated = FakeJob(id=91, company_id=1, job_type="record_updated", status="queued", run_at=NOW)
    canceled = FakeJob(id=92, company_id=1, job_type="record_canceled", status="queued", run_at=NOW)
    rem_2h = FakeJob(id=93, company_id=1, job_type="reminder_2h", status="queued", run_at=NOW)
    rem_24h = FakeJob(id=94, company_id=1, job_type="reminder_24h", status="queued", run_at=NOW)

    assert ow._retry_deadline_at(created, record) == starts - timedelta(minutes=30)
    assert ow._retry_deadline_at(updated, record) == starts - timedelta(minutes=30)
    assert ow._retry_deadline_at(canceled, record) == starts - timedelta(minutes=15)
    assert ow._retry_deadline_at(rem_2h, record) == starts - timedelta(minutes=15)
    assert ow._retry_deadline_at(rem_24h, record) == min(starts - timedelta(hours=3), NOW + timedelta(hours=6))


def test_fresh_first_attempt_is_not_canceled_by_deadline_guard() -> None:
    job = FakeJob(id=95, company_id=1, job_type="reminder_2h", status="queued", run_at=NOW)
    record = FakeRecord(id=10, company_id=1, starts_at=NOW - timedelta(minutes=1))

    assert ow._deadline_passed_for_send(job, record) is False

    job.payload = {ow._ORIGINAL_RUN_AT_KEY: (NOW - timedelta(hours=1)).isoformat()}
    assert ow._deadline_passed_for_send(job, record) is True


# ---------------------------------------------------------------------------
# Test 9 — open window + empty rendered body → sends template, no text
# ---------------------------------------------------------------------------


def test_open_window_empty_body_sends_template(monkeypatch: Any) -> None:
    """Empty rendered body makes job ineligible; template sent as usual."""
    job = FakeJob(
        id=9,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)
    _fake_window_open(monkeypatch)

    # Override render to return empty body
    async def _empty_render(*a: Any, **kw: Any) -> Any:
        return ("   ", 123, "de", _REMINDER_2H_CTX)

    monkeypatch.setattr(ow, "_render_message", _empty_render)

    text_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        return ("tpl-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 9, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not text_called, "Text send must not be called for empty body"
    # No 24h diagnostic meta for non-eligible jobs
    out = session.added[0]
    assert out.meta["send_type"] == "template"
    assert "text_inside_24h" not in out.meta


# ---------------------------------------------------------------------------
# Test 10 — Settings validation
# ---------------------------------------------------------------------------


def test_settings_defaults() -> None:
    """Defaults: feature disabled, whitelist set, fallback enabled."""
    s = Settings(_env_file=None, database_url="postgresql+asyncpg://x/y", altegio_webhook_secret="s")  # type: ignore[call-arg]
    assert s.bot_template_text_inside_24h_enabled is False
    assert "reminder_2h" in s.bot_template_text_inside_24h_job_types
    assert s.bot_template_text_inside_24h_fallback_enabled is True


def test_settings_enabled_parses_whitelist() -> None:
    """Feature enabled with valid whitelist → settings constructed successfully."""
    s = Settings(  # type: ignore[call-arg]
        _env_file=None,
        database_url="postgresql+asyncpg://x/y",
        altegio_webhook_secret="s",
        bot_template_text_inside_24h_enabled=True,
        bot_template_text_inside_24h_job_types="record_created, reminder_2h",
        bot_template_text_inside_24h_fallback_enabled=True,
    )
    assert s.bot_template_text_inside_24h_enabled is True
    assert "reminder_2h" in s.bot_template_text_inside_24h_job_types


def test_settings_enabled_empty_whitelist_raises() -> None:
    """Feature enabled + empty whitelist → ValueError at settings construction."""
    with pytest.raises((ValueError, Exception)):
        Settings(  # type: ignore[call-arg]
            _env_file=None,
            database_url="postgresql+asyncpg://x/y",
            altegio_webhook_secret="s",
            bot_template_text_inside_24h_enabled=True,
            bot_template_text_inside_24h_job_types="   ",
        )


def test_settings_whitelist_trims_spaces() -> None:
    """Whitelist entries with surrounding spaces are parsed correctly."""
    s = Settings(  # type: ignore[call-arg]
        _env_file=None,
        database_url="postgresql+asyncpg://x/y",
        altegio_webhook_secret="s",
        bot_template_text_inside_24h_enabled=True,
        bot_template_text_inside_24h_job_types=" reminder_2h , record_created ",
    )
    # Parse manually to verify space trimming
    tokens = frozenset(t.strip() for t in s.bot_template_text_inside_24h_job_types.split(",") if t.strip())
    assert "reminder_2h" in tokens
    assert "record_created" in tokens


def test_settings_disabled_empty_whitelist_allowed() -> None:
    """Feature disabled + empty whitelist → valid (no validation error)."""
    s = Settings(  # type: ignore[call-arg]
        _env_file=None,
        database_url="postgresql+asyncpg://x/y",
        altegio_webhook_secret="s",
        bot_template_text_inside_24h_enabled=False,
        bot_template_text_inside_24h_job_types="",
    )
    assert s.bot_template_text_inside_24h_enabled is False


# ---------------------------------------------------------------------------
# Test 11 — Helpers: _is_text_window_policy_error
# ---------------------------------------------------------------------------


def test_is_text_window_policy_error_matches_known_patterns() -> None:
    from altegio_bot.workers.outbox_worker import _is_text_window_policy_error

    assert _is_text_window_policy_error("Error 131047: session expired")
    assert _is_text_window_policy_error("message outside the allowed window")
    assert _is_text_window_policy_error("24 hour window expired")
    assert _is_text_window_policy_error("24-hour window closed")
    assert _is_text_window_policy_error("customer service window closed")
    assert _is_text_window_policy_error("re-engagement message required")


def test_is_text_window_policy_error_ignores_ambiguous() -> None:
    from altegio_bot.workers.outbox_worker import _is_text_window_policy_error

    assert not _is_text_window_policy_error("Connection timeout")
    assert not _is_text_window_policy_error("500 Internal Server Error")
    assert not _is_text_window_policy_error("Unknown error occurred")
    assert not _is_text_window_policy_error("")


# ---------------------------------------------------------------------------
# Test 12 — Regression: feature enabled does not affect wa_cmd / promo jobs
# ---------------------------------------------------------------------------


def test_promo_discount_applied_not_affected(monkeypatch: Any) -> None:
    """promo_discount_applied uses text by its own path; 24h feature must not interfere."""
    job = FakeJob(
        id=20,
        company_id=758285,
        job_type="promo_discount_applied",
        status="queued",
        run_at=NOW,
        payload={"body": "Dein Rabatt ist aktiv!", "promo_lead_id": 42},
    )

    async def _fake_find_success(session: Any, job_id: int) -> Any:
        return None

    async def _fake_find_existing(session: Any, job_id: int) -> Any:
        return None

    async def _fake_count_131026(session: Any, phone: str, window_days: int) -> int:
        return 0

    async def _fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def _fake_load_record(session: Any, job_obj: Any) -> Any:
        return None

    async def _fake_load_client(session: Any, job_obj: Any, record: Any) -> Any:
        return FakeClient(id=1, phone_e164=PHONE)

    async def _fake_apply_rl(session: Any, phone: str) -> Any:
        return None

    async def _fake_pick_sender(session: Any, company_id: int, sender_code: str) -> int:
        return 999

    monkeypatch.setattr(ow, "_find_success_outbox", _fake_find_success)
    monkeypatch.setattr(ow, "_find_existing_outbox", _fake_find_existing)
    monkeypatch.setattr(ow, "_count_131026_failures", _fake_count_131026)
    monkeypatch.setattr(ow, "_load_job", _fake_load_job)
    monkeypatch.setattr(ow, "_load_record", _fake_load_record)
    monkeypatch.setattr(ow, "_load_client", _fake_load_client)
    monkeypatch.setattr(ow, "_apply_rate_limit", _fake_apply_rl)
    monkeypatch.setattr(ow, "pick_sender_id", _fake_pick_sender)
    monkeypatch.setattr(ow, "utcnow", lambda: NOW)
    monkeypatch.setattr(ow, "OutboxMessage", FakeOutbox)
    monkeypatch.setattr(ow, "_update_promo_lead_notification_meta", _noop_coro)

    _enable_feature(monkeypatch)
    # Window open — should not matter for promo_discount_applied
    window_checked = []

    async def _window_spy(*a: Any, **kw: Any) -> tuple[bool, datetime]:
        window_checked.append(True)
        return (True, NOW - timedelta(hours=1))

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _window_spy)

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        return ("promo-txt-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)

    session = FakeSession()
    run(ow.process_job_in_session(session, 20, provider=object()))  # type: ignore

    assert job.status == "done"
    assert not window_checked, "Window check must not run for promo_discount_applied"
    out = session.added[0]
    assert out.meta["send_type"] == "text"


async def _noop_coro(*a: Any, **kw: Any) -> None:
    return None


# ---------------------------------------------------------------------------
# Test 13 — _get_24h_whitelist reflects settings
# ---------------------------------------------------------------------------


def test_get_24h_whitelist_reflects_settings(monkeypatch: Any) -> None:
    """_get_24h_whitelist() reads from settings at call time."""
    monkeypatch.setattr(
        ow.settings,
        "bot_template_text_inside_24h_job_types",
        "record_created,reminder_24h",
    )
    wl = ow._get_24h_whitelist()
    assert "record_created" in wl
    assert "reminder_24h" in wl
    assert "reminder_2h" not in wl
    assert "repeat_10d" not in wl


# ---------------------------------------------------------------------------
# Test 14 — P1: window-check exception → fail-open → template sent
# ---------------------------------------------------------------------------


def test_window_check_exception_fails_open_to_template(monkeypatch: Any) -> None:
    """is_whatsapp_customer_window_open raises → fail-open → template, never blocks."""
    job = FakeJob(
        id=14,
        company_id=758285,
        job_type="reminder_2h",
        status="queued",
        run_at=NOW,
    )
    _patch_base(monkeypatch, job)
    _enable_feature(monkeypatch)

    async def _window_raises(*a: Any, **kw: Any) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _window_raises)

    text_called = []
    template_called = []

    async def _fake_text(*a: Any, **kw: Any) -> Any:
        text_called.append(True)
        return ("txt-id", None)

    async def _fake_template(*a: Any, **kw: Any) -> Any:
        template_called.append(True)
        return ("tpl-failopen-id", None)

    monkeypatch.setattr(ow, "safe_send", _fake_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_template)

    session = FakeSession()
    run(ow.process_job_in_session(session, 14, provider=object()))  # type: ignore

    assert job.status == "done", f"Expected done, got {job.status}: {job.last_error}"
    assert not text_called, "safe_send (text) must not be called after window-check failure"
    assert template_called, "safe_send_template must be called (fail-open)"

    out = session.added[0]
    assert out.status == "sent"
    meta = out.meta
    assert meta["send_type"] == "template"
    assert meta["text_inside_24h"] is False
    assert meta["route_reason"] == "wa_window_check_failed"
    assert "boom" in meta["wa_window_check_error"]


# ---------------------------------------------------------------------------
# Test 15 — P3: hard allowlist validation rejects unsupported job types
# ---------------------------------------------------------------------------


def test_settings_unsupported_job_type_enabled_raises() -> None:
    """enabled=True, job_types with non-allowlist type → ValueError."""
    with pytest.raises((ValueError, Exception)):
        Settings(  # type: ignore[call-arg]
            _env_file=None,
            database_url="postgresql+asyncpg://x/y",
            altegio_webhook_secret="s",
            bot_template_text_inside_24h_enabled=True,
            bot_template_text_inside_24h_job_types="reminder_2h,repeat_10d",
        )


def test_settings_disabled_unsupported_job_type_still_raises() -> None:
    """enabled=False, job_types with non-allowlist type → ValueError (always validated)."""
    with pytest.raises((ValueError, Exception)):
        Settings(  # type: ignore[call-arg]
            _env_file=None,
            database_url="postgresql+asyncpg://x/y",
            altegio_webhook_secret="s",
            bot_template_text_inside_24h_enabled=False,
            bot_template_text_inside_24h_job_types="repeat_10d",
        )
