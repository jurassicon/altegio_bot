"""Tests for configurable worker polling intervals (poll_sec settings)."""

from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest
from pydantic import ValidationError

from altegio_bot.settings import Settings
from altegio_bot.workers.inbox_worker import _resolve_poll_sec as _inbox_resolve
from altegio_bot.workers.outbox_worker import _resolve_poll_sec as _outbox_resolve
from altegio_bot.workers.whatsapp_inbox_worker import (
    _resolve_poll_sec as _wa_resolve,
)

_REQUIRED = {
    "database_url": "postgresql+asyncpg://x/y",
    "altegio_webhook_secret": "x",
}

_POLL_ENV_NAMES = (
    "INBOX_WORKER_POLL_SEC",
    "OUTBOX_WORKER_POLL_SEC",
    "WHATSAPP_INBOX_WORKER_POLL_SEC",
)


def _clear_poll_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _POLL_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------
# Minimal session mock — reused by all run_loop sleep-path tests
# ---------------------------------------------------------------------------


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

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


def _session_local_factory() -> _SessionLocalCM:
    return _SessionLocalCM()


# ---------------------------------------------------------------------------
# Settings defaults
# Isolated from process env and .env file.
# Tests must pass even when INBOX_WORKER_POLL_SEC=0.2 etc. are set externally.
# ---------------------------------------------------------------------------


class TestSettingsDefaults:
    def test_inbox_worker_poll_sec_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        s = Settings(**_REQUIRED, _env_file=None)
        assert s.inbox_worker_poll_sec == 1.0

    def test_outbox_worker_poll_sec_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        s = Settings(**_REQUIRED, _env_file=None)
        assert s.outbox_worker_poll_sec == 1.0

    def test_whatsapp_inbox_worker_poll_sec_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        s = Settings(**_REQUIRED, _env_file=None)
        assert s.whatsapp_inbox_worker_poll_sec == 1.0


# ---------------------------------------------------------------------------
# Settings env parsing
# .env file skipped; each test sets exactly one env var via monkeypatch.
# ---------------------------------------------------------------------------


class TestSettingsEnvParsing:
    def test_inbox_worker_poll_sec_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        monkeypatch.setenv("INBOX_WORKER_POLL_SEC", "0.2")
        s = Settings(**_REQUIRED, _env_file=None)
        assert s.inbox_worker_poll_sec == pytest.approx(0.2)

    def test_outbox_worker_poll_sec_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        monkeypatch.setenv("OUTBOX_WORKER_POLL_SEC", "0.2")
        s = Settings(**_REQUIRED, _env_file=None)
        assert s.outbox_worker_poll_sec == pytest.approx(0.2)

    def test_whatsapp_inbox_worker_poll_sec_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        monkeypatch.setenv("WHATSAPP_INBOX_WORKER_POLL_SEC", "0.5")
        s = Settings(**_REQUIRED, _env_file=None)
        assert s.whatsapp_inbox_worker_poll_sec == pytest.approx(0.5)

    def test_values_are_float(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_poll_env(monkeypatch)
        monkeypatch.setenv("INBOX_WORKER_POLL_SEC", "0.2")
        s = Settings(**_REQUIRED, _env_file=None)
        assert isinstance(s.inbox_worker_poll_sec, float)


# ---------------------------------------------------------------------------
# Settings validation
# ---------------------------------------------------------------------------


class TestSettingsValidation:
    def _make(self, **kwargs: object) -> Settings:
        return Settings(**_REQUIRED, _env_file=None, **kwargs)

    def test_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._make(inbox_worker_poll_sec=0)

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._make(outbox_worker_poll_sec=-1.0)

    def test_too_small_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._make(whatsapp_inbox_worker_poll_sec=0.01)

    def test_too_large_rejected(self) -> None:
        with pytest.raises(ValidationError):
            self._make(inbox_worker_poll_sec=61)

    def test_minimum_accepted(self) -> None:
        s = self._make(inbox_worker_poll_sec=0.05)
        assert s.inbox_worker_poll_sec == pytest.approx(0.05)

    def test_maximum_accepted(self) -> None:
        s = self._make(outbox_worker_poll_sec=60.0)
        assert s.outbox_worker_poll_sec == pytest.approx(60.0)

    def test_error_message_contains_range(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            self._make(inbox_worker_poll_sec=0.0)
        assert "0.05" in str(exc_info.value)
        assert "60.0" in str(exc_info.value)


# ---------------------------------------------------------------------------
# run_loop -> asyncio.sleep path (cancellation-style, one per worker)
#
# Each test patches:
#   - worker.settings  (SimpleNamespace with just the poll field)
#   - worker.SessionLocal  (no-op async CM)
#   - worker.lock_next_batch / worker._lock_next_jobs  (returns empty list)
#   - worker.asyncio.sleep  (captures value, raises CancelledError to stop loop)
#
# run_loop is called with poll_sec=None so the code path
#   _resolve_poll_sec(None, settings.<worker>_poll_sec)
# is exercised, and the resolved value is passed to asyncio.sleep.
# ---------------------------------------------------------------------------


class TestInboxRunLoopSleepPath:
    def test_uses_settings_poll_sec(self, monkeypatch: Any) -> None:
        import altegio_bot.workers.inbox_worker as iw

        monkeypatch.setattr(
            iw,
            "settings",
            types.SimpleNamespace(inbox_worker_poll_sec=0.2),
        )
        monkeypatch.setattr(iw, "SessionLocal", _session_local_factory)

        async def fake_lock_next_batch(session: Any, batch_size: int) -> list:
            return []

        monkeypatch.setattr(iw, "lock_next_batch", fake_lock_next_batch)

        sleep_calls: list[float] = []

        async def fake_sleep(sec: float) -> None:
            sleep_calls.append(sec)
            raise asyncio.CancelledError

        monkeypatch.setattr(iw.asyncio, "sleep", fake_sleep)

        try:
            asyncio.run(iw.run_loop(poll_sec=None))
        except asyncio.CancelledError:
            pass

        assert sleep_calls == [pytest.approx(0.2)]


class TestOutboxRunLoopSleepPath:
    def test_uses_settings_poll_sec(self, monkeypatch: Any) -> None:
        import altegio_bot.workers.outbox_worker as ow

        monkeypatch.setattr(
            ow,
            "settings",
            types.SimpleNamespace(outbox_worker_poll_sec=0.2),
        )
        monkeypatch.setattr(ow, "SessionLocal", _session_local_factory)

        async def fake_lock_next_jobs(session: Any, batch_size: int) -> list:
            return []

        monkeypatch.setattr(ow, "_lock_next_jobs", fake_lock_next_jobs)

        sleep_calls: list[float] = []

        async def fake_sleep(sec: float) -> None:
            sleep_calls.append(sec)
            raise asyncio.CancelledError

        monkeypatch.setattr(ow.asyncio, "sleep", fake_sleep)

        try:
            asyncio.run(ow.run_loop(provider=object(), poll_sec=None))
        except asyncio.CancelledError:
            pass

        assert sleep_calls == [pytest.approx(0.2)]


class TestWhatsAppRunLoopSleepPath:
    def test_uses_settings_poll_sec(self, monkeypatch: Any) -> None:
        import altegio_bot.workers.whatsapp_inbox_worker as wiw

        monkeypatch.setattr(
            wiw,
            "settings",
            types.SimpleNamespace(whatsapp_inbox_worker_poll_sec=0.5),
        )
        monkeypatch.setattr(wiw, "SessionLocal", _session_local_factory)

        async def fake_lock_next_batch(session: Any, batch_size: int) -> list:
            return []

        monkeypatch.setattr(wiw, "lock_next_batch", fake_lock_next_batch)

        sleep_calls: list[float] = []

        async def fake_sleep(sec: float) -> None:
            sleep_calls.append(sec)
            raise asyncio.CancelledError

        monkeypatch.setattr(wiw.asyncio, "sleep", fake_sleep)

        try:
            asyncio.run(wiw.run_loop(provider=object(), poll_sec=None))
        except asyncio.CancelledError:
            pass

        assert sleep_calls == [pytest.approx(0.5)]


# ---------------------------------------------------------------------------
# _resolve_poll_sec helper — all three workers
# ---------------------------------------------------------------------------


class TestResolvePollSec:
    # inbox_worker
    def test_inbox_none_returns_settings_value(self) -> None:
        assert _inbox_resolve(None, 0.3) == pytest.approx(0.3)

    def test_inbox_explicit_takes_priority(self) -> None:
        assert _inbox_resolve(0.7, 0.2) == pytest.approx(0.7)

    # outbox_worker
    def test_outbox_none_returns_settings_value(self) -> None:
        assert _outbox_resolve(None, 0.3) == pytest.approx(0.3)

    def test_outbox_explicit_takes_priority(self) -> None:
        assert _outbox_resolve(0.7, 0.2) == pytest.approx(0.7)

    # whatsapp_inbox_worker
    def test_wa_none_returns_settings_value(self) -> None:
        assert _wa_resolve(None, 0.5) == pytest.approx(0.5)

    def test_wa_explicit_takes_priority(self) -> None:
        assert _wa_resolve(0.7, 0.5) == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# Smoke: workers import cleanly
# ---------------------------------------------------------------------------


class TestWorkerImports:
    def test_inbox_worker_imports(self) -> None:
        import altegio_bot.workers.inbox_worker  # noqa: F401

    def test_outbox_worker_imports(self) -> None:
        import altegio_bot.workers.outbox_worker  # noqa: F401

    def test_whatsapp_inbox_worker_imports(self) -> None:
        import altegio_bot.workers.whatsapp_inbox_worker  # noqa: F401
