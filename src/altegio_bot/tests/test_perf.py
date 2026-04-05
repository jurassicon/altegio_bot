from __future__ import annotations

import json
import logging

import pytest

import altegio_bot.perf as perf_mod


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
