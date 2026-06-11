"""Tests: probe_chatwoot_latency CLI script.

Covers (offline only, no real Chatwoot):
1. Parser defaults: 15 requests, 10s timeout, placeholder query, no base-url,
   no forwarded proto.
2. Parser overrides via CLI flags.
3. format_stats output: count/avg/median/min/max + status summary.
4. format_stats with no successful durations (errors only).
5. Missing env (base URL / account id / token) → exit 2, token never printed.
6. X-Forwarded-Proto: --forwarded-proto / env add the header, CLI wins over
   env, default sends no header, token never printed (respx, no real HTTP).
7. Clean-env regression: the probe runs with only CHATWOOT_* env set —
   importing it must not instantiate Settings() (no DATABASE_URL needed).
"""

from __future__ import annotations

import httpx
import pytest
import respx

from altegio_bot.scripts.probe_chatwoot_latency import _build_parser, _run, format_stats


def _parse(*argv: str) -> object:
    return _build_parser().parse_args(list(argv))


# ---------------------------------------------------------------------------
# 1–2. Argument parsing
# ---------------------------------------------------------------------------


def test_parser_defaults() -> None:
    args = _parse()
    assert args.base_url is None
    assert args.query == "+490000000000"
    assert args.requests == 15
    assert args.timeout == 10.0
    assert args.forwarded_proto is None


def test_parser_overrides() -> None:
    args = _parse(
        "--base-url",
        "http://chatwoot_rails_1:3000",
        "--query",
        "+491234567890",
        "--requests",
        "5",
        "--timeout",
        "2.5",
    )
    assert args.base_url == "http://chatwoot_rails_1:3000"
    assert args.query == "+491234567890"
    assert args.requests == 5
    assert args.timeout == 2.5


# ---------------------------------------------------------------------------
# 3–4. Stats formatting
# ---------------------------------------------------------------------------


def test_format_stats_basic() -> None:
    out = format_stats([0.100, 0.200, 0.300], ["200", "200", "200"])
    assert "count=3" in out
    assert "avg=0.200s" in out
    assert "median=0.200s" in out
    assert "min=0.100s" in out
    assert "max=0.300s" in out
    assert "statuses: 200x3" in out


def test_format_stats_errors_only() -> None:
    out = format_stats([], ["ConnectError", "ConnectError"])
    assert "count=2" in out
    assert "avg=" not in out
    assert "statuses: ConnectErrorx2" in out


# ---------------------------------------------------------------------------
# 5. Missing env → exit 2, no token leakage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_base_url_exits_2(capsys, monkeypatch) -> None:
    monkeypatch.delenv("CHATWOOT_BASE_URL", raising=False)
    monkeypatch.setenv("CHATWOOT_ACCOUNT_ID", "2")
    monkeypatch.setenv("CHATWOOT_API_TOKEN", "secret-token-value")
    exit_code = await _run(_parse())
    assert exit_code == 2
    out = capsys.readouterr().out
    assert "ERROR" in out
    assert "secret-token-value" not in out


@pytest.mark.asyncio
async def test_missing_account_id_exits_2(capsys, monkeypatch) -> None:
    monkeypatch.setenv("CHATWOOT_BASE_URL", "http://chatwoot_rails_1:3000")
    monkeypatch.delenv("CHATWOOT_ACCOUNT_ID", raising=False)
    monkeypatch.setenv("CHATWOOT_API_TOKEN", "secret-token-value")
    exit_code = await _run(_parse())
    assert exit_code == 2
    out = capsys.readouterr().out
    assert "CHATWOOT_ACCOUNT_ID" in out
    assert "secret-token-value" not in out


@pytest.mark.asyncio
async def test_missing_token_exits_2(capsys, monkeypatch) -> None:
    monkeypatch.setenv("CHATWOOT_BASE_URL", "http://chatwoot_rails_1:3000")
    monkeypatch.setenv("CHATWOOT_ACCOUNT_ID", "2")
    monkeypatch.delenv("CHATWOOT_API_TOKEN", raising=False)
    exit_code = await _run(_parse())
    assert exit_code == 2
    out = capsys.readouterr().out
    assert "CHATWOOT_API_TOKEN" in out


# ---------------------------------------------------------------------------
# 6. X-Forwarded-Proto (CLI flag / env var)
# ---------------------------------------------------------------------------

_SEARCH_URL = "http://rails:3000/api/v1/accounts/2/contacts/search"


def _set_probe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CHATWOOT_BASE_URL", raising=False)
    monkeypatch.delenv("CHATWOOT_API_FORWARDED_PROTO", raising=False)
    monkeypatch.setenv("CHATWOOT_ACCOUNT_ID", "2")
    monkeypatch.setenv("CHATWOOT_API_TOKEN", "secret-token-value")


def _mock_search() -> respx.Route:
    return respx.get(_SEARCH_URL).mock(return_value=httpx.Response(200, json={"payload": []}))


@respx.mock
@pytest.mark.asyncio
async def test_probe_cli_forwarded_proto_adds_header(capsys, monkeypatch) -> None:
    _set_probe_env(monkeypatch)
    route = _mock_search()

    exit_code = await _run(_parse("--base-url", "http://rails:3000", "--forwarded-proto", "https", "--requests", "2"))

    assert exit_code == 0
    assert len(route.calls) == 2
    assert route.calls[0].request.headers["X-Forwarded-Proto"] == "https"
    assert route.calls[0].request.headers["api_access_token"] == "secret-token-value"
    out = capsys.readouterr().out
    assert "forwarded_proto=https" in out
    assert "secret-token-value" not in out


@respx.mock
@pytest.mark.asyncio
async def test_probe_env_forwarded_proto_adds_header(capsys, monkeypatch) -> None:
    _set_probe_env(monkeypatch)
    monkeypatch.setenv("CHATWOOT_BASE_URL", "http://rails:3000")
    monkeypatch.setenv("CHATWOOT_API_FORWARDED_PROTO", "https")
    route = _mock_search()

    exit_code = await _run(_parse("--requests", "1"))

    assert exit_code == 0
    assert route.calls[0].request.headers["X-Forwarded-Proto"] == "https"
    out = capsys.readouterr().out
    assert "forwarded_proto=https" in out
    assert "secret-token-value" not in out


@respx.mock
@pytest.mark.asyncio
async def test_probe_cli_forwarded_proto_wins_over_env(monkeypatch, capsys) -> None:
    _set_probe_env(monkeypatch)
    monkeypatch.setenv("CHATWOOT_API_FORWARDED_PROTO", "http")
    route = _mock_search()

    exit_code = await _run(_parse("--base-url", "http://rails:3000", "--forwarded-proto", "https", "--requests", "1"))

    assert exit_code == 0
    assert route.calls[0].request.headers["X-Forwarded-Proto"] == "https"
    assert "forwarded_proto=https" in capsys.readouterr().out


@respx.mock
@pytest.mark.asyncio
async def test_probe_no_forwarded_proto_by_default(capsys, monkeypatch) -> None:
    _set_probe_env(monkeypatch)
    route = _mock_search()

    exit_code = await _run(_parse("--base-url", "http://rails:3000", "--requests", "1"))

    assert exit_code == 0
    assert "X-Forwarded-Proto" not in route.calls[0].request.headers
    out = capsys.readouterr().out
    assert "forwarded_proto=<none>" in out
    assert "secret-token-value" not in out


@respx.mock
@pytest.mark.asyncio
async def test_probe_invalid_forwarded_proto_sends_no_header(capsys, monkeypatch) -> None:
    _set_probe_env(monkeypatch)
    route = _mock_search()

    exit_code = await _run(_parse("--base-url", "http://rails:3000", "--forwarded-proto", "ftp", "--requests", "1"))

    assert exit_code == 0
    assert "X-Forwarded-Proto" not in route.calls[0].request.headers
    assert "forwarded_proto=<none>" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# 7. Clean-env regression (P2): probe must run without full app env
# ---------------------------------------------------------------------------


def test_probe_runs_in_clean_env_without_app_settings(tmp_path) -> None:
    """Reproduction of the review finding:

    env -i CHATWOOT_BASE_URL=... CHATWOOT_ACCOUNT_ID=... CHATWOOT_API_TOKEN=...
        CHATWOOT_API_FORWARDED_PROTO=https \\
        python -m altegio_bot.scripts.probe_chatwoot_latency --help

    used to crash on import because chatwoot_client instantiates Settings(),
    which requires unrelated app env (DATABASE_URL, ALTEGIO_WEBHOOK_SECRET).
    cwd is a tmp dir so pydantic cannot pick up the repo's local .env file.
    --help exits before any HTTP request is made.
    """
    import os
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "altegio_bot.scripts.probe_chatwoot_latency", "--help"],
        env={
            "PATH": os.environ.get("PATH", ""),
            "CHATWOOT_BASE_URL": "https://chatwoot.example.com",
            "CHATWOOT_ACCOUNT_ID": "2",
            "CHATWOOT_API_TOKEN": "test-token",
            "CHATWOOT_API_FORWARDED_PROTO": "https",
        },
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout.lower()
    assert "DATABASE_URL" not in result.stderr
    assert "ALTEGIO_WEBHOOK_SECRET" not in result.stderr
