"""Tests for configurable worker polling intervals (poll_sec settings)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from altegio_bot.settings import Settings
from altegio_bot.workers.inbox_worker import _resolve_poll_sec

_REQUIRED = {
    "database_url": "postgresql+asyncpg://x/y",
    "altegio_webhook_secret": "x",
}


# ---------------------------------------------------------------------------
# Settings defaults
# ---------------------------------------------------------------------------


class TestSettingsDefaults:
    def test_inbox_worker_poll_sec_default(self) -> None:
        s = Settings(**_REQUIRED)
        assert s.inbox_worker_poll_sec == 1.0

    def test_outbox_worker_poll_sec_default(self) -> None:
        s = Settings(**_REQUIRED)
        assert s.outbox_worker_poll_sec == 1.0

    def test_whatsapp_inbox_worker_poll_sec_default(self) -> None:
        s = Settings(**_REQUIRED)
        assert s.whatsapp_inbox_worker_poll_sec == 1.0


# ---------------------------------------------------------------------------
# Settings env parsing
# ---------------------------------------------------------------------------


class TestSettingsEnvParsing:
    def test_inbox_worker_poll_sec_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INBOX_WORKER_POLL_SEC", "0.2")
        s = Settings(**_REQUIRED)
        assert s.inbox_worker_poll_sec == pytest.approx(0.2)

    def test_outbox_worker_poll_sec_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OUTBOX_WORKER_POLL_SEC", "0.2")
        s = Settings(**_REQUIRED)
        assert s.outbox_worker_poll_sec == pytest.approx(0.2)

    def test_whatsapp_inbox_worker_poll_sec_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("WHATSAPP_INBOX_WORKER_POLL_SEC", "0.5")
        s = Settings(**_REQUIRED)
        assert s.whatsapp_inbox_worker_poll_sec == pytest.approx(0.5)

    def test_values_are_float(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INBOX_WORKER_POLL_SEC", "0.2")
        s = Settings(**_REQUIRED)
        assert isinstance(s.inbox_worker_poll_sec, float)


# ---------------------------------------------------------------------------
# Settings validation
# ---------------------------------------------------------------------------


class TestSettingsValidation:
    def _make(self, **kwargs: object) -> Settings:
        return Settings(**_REQUIRED, **kwargs)

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
# _resolve_poll_sec helper
# ---------------------------------------------------------------------------


class TestResolvePollSec:
    def test_none_returns_settings_value(self) -> None:
        assert _resolve_poll_sec(None, 0.3) == pytest.approx(0.3)

    def test_explicit_takes_priority_over_settings(self) -> None:
        assert _resolve_poll_sec(0.7, 1.0) == pytest.approx(0.7)

    def test_explicit_zero_point_two_takes_priority(self) -> None:
        assert _resolve_poll_sec(0.2, 1.0) == pytest.approx(0.2)

    def test_explicit_overrides_any_settings_value(self) -> None:
        assert _resolve_poll_sec(5.0, 0.1) == pytest.approx(5.0)


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
