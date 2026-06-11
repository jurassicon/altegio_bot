"""Tests: probe_chatwoot_latency CLI script.

Covers (offline only, no real Chatwoot):
1. Parser defaults: 15 requests, 10s timeout, placeholder query, no base-url.
2. Parser overrides via CLI flags.
3. format_stats output: count/avg/median/min/max + status summary.
4. format_stats with no successful durations (errors only).
5. Missing env (base URL / account id / token) → exit 2, token never printed.
"""

from __future__ import annotations

import pytest

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
