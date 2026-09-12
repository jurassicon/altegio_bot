"""Operator CLI fences and exit codes for the voucher delivery canary (§36).

Everything a mistyped command could do wrong is asserted here: that it reaches
nothing, that it cannot be mistaken for a success, and that the one command that
messages a real person cannot be reached by accident.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    APPLY_FLAG_MISSING,
    CANARY_DISABLED,
    MUTATION_STAGES,
)
from altegio_bot.scripts import easyweek_voucher_delivery_canary as cli
from altegio_bot.scripts.easyweek_voucher_delivery_canary import (
    EXIT_AMBIGUOUS,
    EXIT_ARGUMENTS,
    EXIT_CONTRACT_MISMATCH,
    EXIT_MANUAL_CLEANUP,
    EXIT_OK,
    EXIT_UNKNOWN,
    main,
)
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow


@pytest.fixture
def forbid_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing in this module may construct a client or a sender."""

    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("this invocation must not construct a client")

    monkeypatch.setattr(cli, "EasyWeekClient", forbidden)
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", forbidden)
    monkeypatch.setattr(cli, "VoucherDeliveryClient", forbidden)


def _output(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def _stage_argv(stage: str, **changes: str) -> list[str]:
    argv = [
        stage,
        "--preview-run-id",
        "1",
        "--campaign-recipient-id",
        "2",
        "--apply",
        "--plan-digest",
        "a" * 64,
        "--plan-issued-at",
        utcnow().isoformat(),
        "--confirm",
        "deliver-voucher-delivery-abcdef012345",
    ]
    for flag, value in changes.items():
        index = argv.index(f"--{flag.replace('_', '-')}")
        argv[index + 1] = value
    return argv


# ---------------------------------------------------------------------------
# Nothing about a bad invocation looks like success
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--help"],
        ["-h"],
        ["nonsense"],
        ["deliver", "--help"],
        # Abbreviations are off: a half-typed flag authorises nothing.
        ["deliver", "--app"],
        ["deliver", "--apply", "--plan"],
        # The recipient is mandatory, and never optional.
        ["deliver", "--apply"],
        ["plan", "--stage", "deliver"],
        ["plan", "--stage", "nonsense", "--preview-run-id", "1", "--campaign-recipient-id", "2"],
    ],
)
def test_every_bad_invocation_exits_with_the_argument_code(
    monkeypatch, capsys, argv, configuration, forbid_clients
) -> None:
    try:
        code = main(argv)
    except SystemExit as exc:
        code = exc.code

    assert code == EXIT_ARGUMENTS
    assert code != EXIT_OK
    capsys.readouterr()


@pytest.mark.parametrize("stage", list(MUTATION_STAGES))
def test_a_stage_without_apply_reaches_nothing(monkeypatch, capsys, stage, configuration, forbid_clients) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    argv = [stage, "--preview-run-id", "1", "--campaign-recipient-id", "2"]

    assert main(argv) == EXIT_ARGUMENTS

    report = _output(capsys)
    assert report["outcome"] == "refused"
    assert APPLY_FLAG_MISSING in report["reasons"]
    assert report["external_send_attempted"] is False


@pytest.mark.parametrize("stage", list(MUTATION_STAGES))
def test_the_fence_blocks_every_stage_before_any_client_exists(
    monkeypatch, capsys, stage, configuration, forbid_clients
) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", False, raising=False)

    assert main(_stage_argv(stage)) == EXIT_ARGUMENTS

    report = _output(capsys)
    assert CANARY_DISABLED in report["reasons"]


@pytest.mark.parametrize("stage", list(MUTATION_STAGES))
def test_a_stage_without_a_full_authorisation_reaches_nothing(
    monkeypatch, capsys, stage, configuration, forbid_clients
) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    argv = _stage_argv(stage, plan_digest="")

    assert main(argv) == EXIT_ARGUMENTS
    capsys.readouterr()


def test_an_unusable_runtime_identity_refuses_before_any_client(
    monkeypatch, capsys, configuration, forbid_clients
) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", "", raising=False)

    assert main(_stage_argv("create")) == EXIT_ARGUMENTS

    assert _output(capsys)["outcome"] == "refused"


# ---------------------------------------------------------------------------
# status is database-only
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_answers_from_the_database_with_nothing_configured(
    monkeypatch, capsys, session_maker, forbid_clients
) -> None:
    """The moment an operator most needs `status` is when the env is broken."""
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", "", raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", "", raising=False)
    monkeypatch.setattr(cli, "SessionLocal", session_maker)

    assert await asyncio.to_thread(main, ["status"]) == EXIT_OK

    report = _output(capsys)
    assert report["stage"] == "status"
    assert report["ledger"]["ledger_row_exists"] is False
    assert report["global_ready_for_send"] is False


# ---------------------------------------------------------------------------
# Shape of the command surface
# ---------------------------------------------------------------------------


def test_there_is_no_command_that_runs_the_whole_canary() -> None:
    """create, pay and deliver are three decisions, never one."""
    parser = cli._build_parser()
    actions = [
        action
        for action in parser._subparsers._group_actions[0].choices  # type: ignore[union-attr]
    ]

    assert sorted(actions) == ["create", "deliver", "pay", "plan", "reconcile", "refund", "status"]
    for forbidden in ("run", "all", "full", "auto", "send-all"):
        assert forbidden not in actions


def test_every_exit_code_is_distinct() -> None:
    codes = {EXIT_OK, EXIT_ARGUMENTS, EXIT_UNKNOWN, EXIT_CONTRACT_MISMATCH, EXIT_AMBIGUOUS, EXIT_MANUAL_CLEANUP}

    assert len(codes) == 6


def test_the_recipient_can_only_be_named_by_run_and_recipient_id() -> None:
    """Never a phone number, a name or a bare customer UUID.

    The entitlement is earned by one visit recorded in one preview run, and an
    addressing mode without that link would be a different feature entirely.
    """
    parser = cli._build_parser()
    deliver = parser._subparsers._group_actions[0].choices["deliver"]  # type: ignore[union-attr]
    flags = {option for action in deliver._actions for option in action.option_strings}

    assert "--preview-run-id" in flags
    assert "--campaign-recipient-id" in flags
    for forbidden in ("--phone", "--customer-uuid", "--to", "--name"):
        assert forbidden not in flags
