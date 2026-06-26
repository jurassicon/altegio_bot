"""Focused unit tests for Settings validators."""

from __future__ import annotations

import pytest

from altegio_bot.settings import Settings


def _settings(**overrides: object) -> Settings:
    """Construct Settings with only the minimal required fields, plus overrides.

    Avoids relying on the production ``.env`` so the tests are deterministic.
    """
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://user:pass@localhost/test",
        "altegio_webhook_secret": "secret",
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_reply_context_visible_quote_mode_default_is_fallback_only(monkeypatch) -> None:
    monkeypatch.delenv("CHATWOOT_REPLY_CONTEXT_VISIBLE_QUOTE_MODE", raising=False)
    assert _settings().chatwoot_reply_context_visible_quote_mode == "fallback_only"


def test_reply_context_visible_quote_mode_accepts_always() -> None:
    assert _settings(chatwoot_reply_context_visible_quote_mode="always").chatwoot_reply_context_visible_quote_mode == (
        "always"
    )


def test_reply_context_visible_quote_mode_rejects_invalid_value() -> None:
    with pytest.raises(ValueError, match="CHATWOOT_REPLY_CONTEXT_VISIBLE_QUOTE_MODE"):
        _settings(chatwoot_reply_context_visible_quote_mode="bad")
