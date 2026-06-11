"""Tests: settings-free Chatwoot X-Forwarded-Proto helpers.

Covers:
1. normalize_forwarded_proto semantics (trim, lowercase, http/https only,
   invalid → None + warning).
2. forwarded_proto_header returns {} or the single header dict.
3. Clean-env regression (P2): importing the helpers module must not
   instantiate Settings(), so it works with no app env at all.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from altegio_bot.chatwoot_headers import forwarded_proto_header, normalize_forwarded_proto

# ---------------------------------------------------------------------------
# 1. normalize_forwarded_proto
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("https", "https"),
        ("http", "http"),
        (" https ", "https"),
        ("HTTPS", "https"),
        ("HTTP", "http"),
        ("ftp", None),
        ("https://chatwoot.example.com", None),
    ],
)
def test_normalize_forwarded_proto(value: str | None, expected: str | None) -> None:
    assert normalize_forwarded_proto(value) == expected


def test_normalize_invalid_value_warns(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING", logger="altegio_bot.chatwoot_headers"):
        assert normalize_forwarded_proto("ftp") is None
    assert "CHATWOOT_API_FORWARDED_PROTO" in caplog.text


# ---------------------------------------------------------------------------
# 2. forwarded_proto_header
# ---------------------------------------------------------------------------


def test_forwarded_proto_header_valid() -> None:
    assert forwarded_proto_header("https") == {"X-Forwarded-Proto": "https"}
    assert forwarded_proto_header(" HTTP ") == {"X-Forwarded-Proto": "http"}


@pytest.mark.parametrize("value", [None, "", "   ", "ftp"])
def test_forwarded_proto_header_empty_or_invalid(value: str | None) -> None:
    assert forwarded_proto_header(value) == {}


# ---------------------------------------------------------------------------
# 3. Clean-env regression: no Settings() on import
# ---------------------------------------------------------------------------


def _clean_chatwoot_env() -> dict[str, str]:
    """Minimal env: only Chatwoot variables, no DATABASE_URL etc."""
    return {
        "PATH": os.environ.get("PATH", ""),
        "CHATWOOT_BASE_URL": "https://chatwoot.example.com",
        "CHATWOOT_ACCOUNT_ID": "2",
        "CHATWOOT_API_TOKEN": "test-token",
        "CHATWOOT_API_FORWARDED_PROTO": "https",
    }


def test_chatwoot_headers_imports_without_full_app_env(tmp_path) -> None:
    """The helper module itself must stay free of any Settings dependency."""
    result = subprocess.run(
        [sys.executable, "-c", "import altegio_bot.chatwoot_headers"],
        env=_clean_chatwoot_env(),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
