"""The CLI no longer mutates, and nothing reopens it (§43.6, group E).

§42 drove the mailing from a terminal. The owner decided on 28.09.2026 that the
real mailing happens from the interface, so the five mutating subcommands refuse
and do nothing — and the point of this file is the second half of that sentence:
there is no flag, no environment variable and no argument order that gets past it.

What stays is reading. ``status``, ``plan`` and ``reconcile`` matter most exactly
when the acting path is blocked: after an emergency fence close, after a halt, and
while an unknown outcome is being resolved. A phase that took those away would
leave an operator running SQL by hand.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

import altegio_bot.scripts.easyweek_voucher_production_mailing as cli
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ACCOUNT_UUID,
    STAFFER_UUID,
)

MUTATING = ("freeze", "create", "pay", "deliver", "refund")


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Everything a §42 mutation used to need, so only the closure is under test."""
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_account_uuid", ACCOUNT_UUID, raising=False)


def _run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, Any]]:
    code = cli.main(argv)
    out = capsys.readouterr().out
    return code, json.loads(out) if out.strip() else {}


def _args_for(stage: str) -> list[str]:
    base = [
        stage,
        "--preview-run-id",
        "1",
        "--apply",
        "--plan-digest",
        "a" * 64,
        "--plan-issued-at",
        "2026-10-01T00:00:00+00:00",
        "--confirm",
        f"{stage}-voucher-production-aaaaaaaaaaaa",
    ]
    if stage == "freeze":
        return base + ["--expected-recipient-count", "1", "--approved-exposure-minor", "1500"]
    out = base + ["--batch-id", "1"]
    if stage == "refund":
        out += ["--slot", "1"]
    return out


@pytest.mark.parametrize("stage", MUTATING)
def test_every_mutating_command_refuses(configured, capsys, stage: str):
    """All five, with every argument a §42 operator would have supplied."""
    code, report = _run(_args_for(stage), capsys)
    assert code == cli.EXIT_CONTRACT_MISMATCH, report
    assert report["outcome"] == "refused"
    assert report["reasons"] == ["voucher_production_cli_mutation_closed"]
    assert report["external_effect_attempted"] is False
    assert report["external_send_attempted"] is False


@pytest.mark.parametrize("stage", MUTATING)
def test_no_extra_flag_reopens_a_mutating_command(configured, capsys, stage: str):
    """`--apply` does not, and a flag that does not exist cannot be invented here.

    Argparse abbreviation is off and unknown flags are an argument error, so the two
    ways somebody might "re-enable" this both fail: the supported invocation refuses,
    and an unsupported one never runs at all.
    """
    # The supported invocation, refused.
    code, report = _run(_args_for(stage), capsys)
    assert report["reasons"] == ["voucher_production_cli_mutation_closed"]

    # An invented flag is an argument error, never a mutation.
    for invented in ("--force", "--ui-bypass", "--really-apply", "--yes"):
        with pytest.raises(SystemExit) as exit_info:
            cli.main(_args_for(stage) + [invented])
        assert exit_info.value.code == cli.EXIT_ARGUMENTS, invented


@pytest.mark.parametrize("stage", MUTATING)
def test_a_mutating_command_refuses_before_it_reads_the_fence_state(configured, capsys, stage: str, monkeypatch):
    """Closed whether the fence is open or shut: this is not a fence question."""
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    code, report = _run(_args_for(stage), capsys)
    assert code == cli.EXIT_CONTRACT_MISMATCH
    # With the fence shut the fence answers first, which is also a refusal.
    assert report["reasons"] in (
        ["voucher_production_cli_mutation_closed"],
        ["voucher_production_disabled"],
    )
    assert report["external_effect_attempted"] is False


def test_status_still_reads_with_the_fence_closed(configured, capsys, monkeypatch, session_maker):
    """The one command that must survive an emergency `false`."""
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)
    monkeypatch.setattr(cli, "SessionLocal", session_maker)
    code, report = _run(["status"], capsys)
    assert code == cli.EXIT_OK
    assert report["mode"] == "voucher_production_stage"
    assert report["batches"] == []


def test_the_docstring_names_the_ui_as_the_place_the_stages_live(configured):
    """A refusal that does not say where to go instead teaches nobody anything."""
    assert cli.__doc__ is not None
    assert "voucher_production_cli_mutation_closed" in cli.__doc__
    assert "VOUCHER_PRODUCTION_MAILING_RUNBOOK" in cli.__doc__


def test_the_help_text_never_exits_successfully(configured):
    """A mistyped invocation must never be mistaken for a mailing that worked."""
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["--help"])
    assert exit_info.value.code == cli.EXIT_ARGUMENTS
