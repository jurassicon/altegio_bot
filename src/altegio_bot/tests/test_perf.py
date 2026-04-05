from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

import altegio_bot.perf as perf_mod
from altegio_bot.workers import outbox_worker as ow
from altegio_bot.workers import whatsapp_inbox_worker as wa_iw


def test_perf_disabled_by_default(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """When PERF_LOGGING_ENABLED is not set, no log records are emitted."""
    monkeypatch.delenv("PERF_LOGGING_ENABLED", raising=False)

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        with perf_mod.perf_log("worker", "op", event_id=42) as ctx:
            ctx.update(extra="data")

    assert caplog.records == []


def test_perf_disabled_does_not_break_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disabled perf_log is transparent — ctx is still a dict, updates are no-ops."""
    monkeypatch.delenv("PERF_LOGGING_ENABLED", raising=False)

    result = []
    with perf_mod.perf_log("worker", "op") as ctx:
        ctx.update(x=1)
        result.append(ctx.get("x"))

    # ctx.update worked, business code can call it safely
    assert result == [1]


def test_perf_enabled_emits_json(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """When enabled, emits one structured JSON log with required fields."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        with perf_mod.perf_log("outbox_worker", "process_job", job_id=99) as ctx:
            ctx.update(company_id=1, job_type="review_3d")

    assert len(caplog.records) == 1
    record = json.loads(caplog.records[0].message)

    assert record["kind"] == "perf"
    assert record["component"] == "outbox_worker"
    assert record["operation"] == "process_job"
    assert record["job_id"] == 99
    assert record["company_id"] == 1
    assert record["job_type"] == "review_3d"
    assert record["status"] == "ok"
    assert isinstance(record["duration_ms"], float)
    assert record["duration_ms"] >= 0


def test_perf_enabled_error_status(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """On exception, status='error' and exception propagates unchanged."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        with pytest.raises(ValueError, match="boom"):
            with perf_mod.perf_log("inbox_worker", "process_event", event_id=7):
                raise ValueError("boom")

    assert len(caplog.records) == 1
    record = json.loads(caplog.records[0].message)
    assert record["status"] == "error"
    assert record["event_id"] == 7


def test_perf_enabled_false_string(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """PERF_LOGGING_ENABLED=false does not emit logs."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "false")

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        with perf_mod.perf_log("worker", "op"):
            pass

    assert caplog.records == []


# ---------------------------------------------------------------------------
# outbox_worker: outcome captured in perf log
# ---------------------------------------------------------------------------


@dataclass
class _FakeJob:
    id: int = 1
    company_id: int = 758285
    job_type: str = "review_3d"
    status: str = "queued"
    run_at: datetime = field(default_factory=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc))
    record_id: int | None = None
    client_id: int | None = None
    locked_at: Any = None
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5


class _FakeSession:
    def begin(self) -> Any:
        class _CM:
            async def __aenter__(self_) -> None:
                return None

            async def __aexit__(self_, *_: Any) -> None:
                return None

        return _CM()

    async def execute(self, *_: Any, **__: Any) -> Any:
        class _R:
            rowcount = 0

        return _R()


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_outbox_perf_outcome_done(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """outbox perf log includes outcome=done when job completes successfully."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")

    job = _FakeJob()

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_run_logic(session: Any, job_obj: Any, provider: Any) -> None:
        job_obj.status = "done"

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_run_job_logic", fake_run_logic)

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        _run(ow.process_job_in_session(_FakeSession(), 1, provider=object()))

    assert len(caplog.records) == 1
    rec = json.loads(caplog.records[0].message)
    assert rec["outcome"] == "done"
    assert rec["job_id"] == 1
    assert rec["company_id"] == 758285
    assert rec["job_type"] == "review_3d"


def test_outbox_perf_outcome_canceled(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """outbox perf log includes outcome=canceled when job is skipped."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")

    job = _FakeJob()

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return job

    async def fake_run_logic(session: Any, job_obj: Any, provider: Any) -> None:
        job_obj.status = "canceled"

    monkeypatch.setattr(ow, "_load_job", fake_load_job)
    monkeypatch.setattr(ow, "_run_job_logic", fake_run_logic)

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        _run(ow.process_job_in_session(_FakeSession(), 1, provider=object()))

    assert len(caplog.records) == 1
    rec = json.loads(caplog.records[0].message)
    assert rec["outcome"] == "canceled"


def test_outbox_perf_no_outcome_when_job_missing(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When _load_job returns None, outcome is not set (job was locked elsewhere)."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")

    async def fake_load_job(session: Any, job_id: int) -> Any:
        return None

    monkeypatch.setattr(ow, "_load_job", fake_load_job)

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        _run(ow.process_job_in_session(_FakeSession(), 99, provider=object()))

    assert len(caplog.records) == 1
    rec = json.loads(caplog.records[0].message)
    assert "outcome" not in rec
    assert rec["job_id"] == 99


# ---------------------------------------------------------------------------
# whatsapp_inbox_worker: enriched perf context
# ---------------------------------------------------------------------------


@dataclass
class _FakeWAEvent:
    id: int = 5
    status: str = "received"
    company_id: int | None = 758285
    dedupe_key: str | None = "meta:abc123"
    chatwoot_conversation_id: int | None = None
    payload: dict = field(default_factory=dict)
    processed_at: Any = None
    error: str | None = None


class _BeginCM:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: Any) -> None:
        return None


class _WAFakeSessionResult:
    def scalar_one_or_none(self) -> _FakeWAEvent:
        return _FakeWAEvent()


class _WAFakeSession:
    def begin(self) -> _BeginCM:
        return _BeginCM()

    async def execute(self, *_: Any, **__: Any) -> _WAFakeSessionResult:
        return _WAFakeSessionResult()


class _WAFakeSessionLocalCM:
    async def __aenter__(self) -> _WAFakeSession:
        return _WAFakeSession()

    async def __aexit__(self, *_: Any) -> None:
        return None


def test_wa_inbox_perf_enrichment(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """whatsapp_inbox perf log includes company_id, dedupe_key, origin, outcome."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")
    monkeypatch.setattr(wa_iw, "SessionLocal", lambda: _WAFakeSessionLocalCM())

    async def fake_handle(session: Any, event: Any, provider: Any) -> None:
        pass

    monkeypatch.setattr(wa_iw, "handle_event", fake_handle)

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        _run(wa_iw.process_one_event(5, provider=object()))

    assert len(caplog.records) == 1
    rec = json.loads(caplog.records[0].message)
    assert rec["company_id"] == 758285
    assert rec["dedupe_key"] == "meta:abc123"
    assert rec["chatwoot_conversation_id"] is None
    assert rec["origin"] == "meta"
    assert rec["outcome"] == "processed"


def test_wa_inbox_perf_chatwoot_origin(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """origin='chatwoot' when event has chatwoot_conversation_id set."""
    monkeypatch.setenv("PERF_LOGGING_ENABLED", "true")

    @dataclass
    class _CWEvent:
        id: int = 6
        status: str = "received"
        company_id: int | None = 758285
        dedupe_key: str | None = "chatwoot:99"
        chatwoot_conversation_id: int | None = 99
        payload: dict = field(default_factory=dict)
        processed_at: Any = None
        error: str | None = None

    class _CWResult:
        def scalar_one_or_none(self) -> _CWEvent:
            return _CWEvent()

    class _CWSession:
        def begin(self) -> _BeginCM:
            return _BeginCM()

        async def execute(self, *_: Any, **__: Any) -> _CWResult:
            return _CWResult()

    class _CWCM:
        async def __aenter__(self) -> _CWSession:
            return _CWSession()

        async def __aexit__(self, *_: Any) -> None:
            return None

    monkeypatch.setattr(wa_iw, "SessionLocal", lambda: _CWCM())

    async def fake_handle(session: Any, event: Any, provider: Any) -> None:
        pass

    monkeypatch.setattr(wa_iw, "handle_event", fake_handle)

    with caplog.at_level(logging.INFO, logger="altegio_bot.perf"):
        _run(wa_iw.process_one_event(6, provider=object()))

    assert len(caplog.records) == 1
    rec = json.loads(caplog.records[0].message)
    assert rec["origin"] == "chatwoot"


def test_is_chatwoot_origin_none_value() -> None:
    """'_chatwoot' key with None value still counts as chatwoot origin."""
    from altegio_bot.workers.whatsapp_inbox_worker import _is_chatwoot_origin

    @dataclass
    class _MinEvent:
        dedupe_key: str | None = None
        chatwoot_conversation_id: int | None = None

    event = _MinEvent()
    assert _is_chatwoot_origin(event, {"_chatwoot": None}) is True  # type: ignore[arg-type]
