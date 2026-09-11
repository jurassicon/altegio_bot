"""Operator CLI fences and exit codes for the voucher canary (§35).

Everything a mistyped command could do wrong is asserted here: that it sends
nothing, that it cannot be mistaken for a success, and that it never prints an
identity.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
import pytest

from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_DISABLED_BY_ENV,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
)
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationClient
from altegio_bot.scripts import easyweek_voucher_canary as cli
from altegio_bot.scripts.easyweek_voucher_canary import (
    EXIT_AMBIGUOUS,
    EXIT_ARGUMENTS,
    EXIT_CONTRACT_MISMATCH,
    EXIT_MANUAL_CLEANUP,
    EXIT_OK,
    EXIT_ROLLBACK_UNPROVEN,
    EXIT_UNKNOWN_MUTATION,
    main,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_canary_fixtures import (
    ACCOUNT_UUID,
    ACCOUNTS,
    CUSTOMER,
    CUSTOMER_UUID,
    LOCATIONS,
    ORDER_UUID,
    STAFFER_UUID,
    STAFFERS,
    TEMPLATE,
    WORKSPACE,
    code_without_docstrings,
    imported_modules,
    open_order,
)

KEY = "SENTINEL_CLIKEY_ggg111"
SLUG = "SENTINEL_CLISLUG_ggg222"
BASE = "https://my.easyweek.io/api/public/v2"


class Recorder:
    """One MockTransport handler serving both clients."""

    def __init__(self, *, order: dict[str, Any] | None = None, orders_page: dict[str, Any] | None = None) -> None:
        self.seen: list[tuple[str, str]] = []
        self.order = order
        self.orders_page = orders_page if orders_page is not None else {"data": [], "meta": {"last_page": 1}}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/public/v2")
        self.seen.append((request.method, path))

        if request.method == "POST":  # pragma: no cover - guarded by the fences
            raise AssertionError(f"no mutation expected: {path}")
        if path == "/workspace":
            return httpx.Response(200, json=WORKSPACE)
        if path == "/locations":
            return httpx.Response(200, json=LOCATIONS)
        if path == "/voucher-templates":
            return httpx.Response(200, json=[TEMPLATE])
        if path == f"/voucher-templates/{EASYWEEK_VOUCHER_TEMPLATE_UUID}":
            return httpx.Response(200, json=TEMPLATE)
        if path == f"/customers/{CUSTOMER_UUID}":
            return httpx.Response(200, json=CUSTOMER)
        # The documented nested paths, not a workspace-wide filter.
        if path == f"/locations/{KARLSRUHE_LOCATION_UUID}/staffers":
            page = int(request.url.params.get("page", "1"))
            return httpx.Response(200, json=STAFFERS if page == 1 else {"data": [], "meta": {"last_page": 1}})
        if path == f"/locations/{KARLSRUHE_LOCATION_UUID}/accounts":
            return httpx.Response(200, json=ACCOUNTS)
        if path == "/orders":
            assert request.url.params.get("staffer_uuid") == STAFFER_UUID
            return httpx.Response(200, json=self.orders_page)
        if path == f"/orders/{ORDER_UUID}":
            return httpx.Response(200, json=self.order or open_order(marker="x"))
        raise AssertionError(f"unexpected request: {request.method} {path}")


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fully configured run, with the env fence ON."""
    monkeypatch.setattr(settings, "easyweek_voucher_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_customer_uuid", CUSTOMER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_account_uuid", ACCOUNT_UUID, raising=False)


def _install(monkeypatch: pytest.MonkeyPatch, recorder: Recorder, session_maker: Any = None) -> None:
    transport = httpx.MockTransport(recorder)

    def read_client(*args: Any, **kwargs: Any) -> EasyWeekClient:
        return EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=BASE, transport=transport)

    def mutation_client(*args: Any, **kwargs: Any) -> EasyWeekVoucherMutationClient:
        return EasyWeekVoucherMutationClient(api_key=KEY, workspace_slug=SLUG, base_url=BASE, transport=transport)

    monkeypatch.setattr(cli, "EasyWeekClient", read_client)
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", mutation_client)
    if session_maker is not None:
        monkeypatch.setattr(cli, "SessionLocal", session_maker)


def _forbid_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("this invocation must not construct a client")

    monkeypatch.setattr(cli, "EasyWeekClient", forbidden)
    monkeypatch.setattr(cli, "EasyWeekVoucherMutationClient", forbidden)


def _output(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


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
        ["create", "--help"],
        # Abbreviations are off: a half-typed flag authorises nothing.
        ["create", "--app"],
        ["create", "--apply", "--plan"],
    ],
)
def test_every_bad_invocation_exits_with_the_argument_code(monkeypatch, capsys, argv, configured) -> None:
    _forbid_clients(monkeypatch)

    try:
        code = main(argv)
    except SystemExit as exc:
        code = exc.code

    assert code == EXIT_ARGUMENTS
    assert code != EXIT_OK
    capsys.readouterr()


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_REFUND])
def test_a_mutation_without_apply_sends_nothing(monkeypatch, capsys, stage, configured) -> None:
    _forbid_clients(monkeypatch)

    assert main([stage]) == EXIT_ARGUMENTS

    report = _output(capsys)
    assert report["outcome"] == "refused"
    assert report["external_mutation_attempted"] is False
    assert report["ready_for_send"] is False


@pytest.mark.parametrize("stage", [STAGE_CREATE, STAGE_PAY, STAGE_REFUND])
def test_the_env_fence_blocks_a_mutation_before_any_client_exists(monkeypatch, capsys, stage) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_canary_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_customer_uuid", CUSTOMER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_account_uuid", ACCOUNT_UUID, raising=False)
    _forbid_clients(monkeypatch)

    assert main([stage, "--apply", "--plan-digest", "x", "--confirm", "y"]) == EXIT_ARGUMENTS

    assert CANARY_DISABLED_BY_ENV in _output(capsys)["reasons"]


@pytest.mark.parametrize(
    "extra",
    [
        ["--apply"],
        ["--apply", "--plan-digest", "abc"],
        ["--apply", "--confirm", "create-voucher-canary-abc"],
        ["--apply", "--plan-digest", "abc", "--confirm", "p"],
        ["--apply", "--plan-digest", "abc", "--confirm", "p", "--plan-issued-at", "not-a-time"],
        # A naive timestamp carries no timezone, so it cannot be compared safely.
        ["--apply", "--plan-digest", "abc", "--confirm", "p", "--plan-issued-at", "2026-09-11T10:00:00"],
    ],
)
def test_an_incomplete_plan_authorisation_sends_nothing(monkeypatch, capsys, extra, configured) -> None:
    _forbid_clients(monkeypatch)

    assert main([STAGE_CREATE, *extra]) == EXIT_ARGUMENTS

    report = _output(capsys)
    assert report["external_mutation_attempted"] is False


def test_a_missing_runtime_identity_blocks_without_leaking_a_value(monkeypatch, capsys) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_customer_uuid", "", raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_account_uuid", ACCOUNT_UUID, raising=False)
    _forbid_clients(monkeypatch)

    assert main(["plan", "--stage", "create"]) == EXIT_ARGUMENTS

    out = capsys.readouterr().out
    assert "canary_runtime_identity_missing" in out
    assert STAFFER_UUID not in out
    assert ACCOUNT_UUID not in out


def test_there_is_no_command_that_chains_the_three_mutations() -> None:
    """The stop between stages is the control; it cannot be automated away."""
    parser = cli._build_parser()
    actions = [action for action in parser._actions if hasattr(action, "choices") and action.choices]
    commands = set()
    for action in actions:
        commands.update(action.choices or {})

    assert commands == {"plan", "status", "reconcile", "create", "pay", "refund"}
    code = code_without_docstrings(cli)
    for forbidden in ("run_all", "full_canary", "create_pay_refund", "autopilot"):
        assert forbidden not in code, forbidden


def test_no_default_enables_a_mutation() -> None:
    parser = cli._build_parser()
    for action in parser._actions:
        if action.dest == "apply":
            assert action.default is False


# ---------------------------------------------------------------------------
# Plan, status and reconcile are reads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_plan_issues_reads_only_and_prints_one_stage_phrase(
    monkeypatch, capsys, session_maker, configured
) -> None:
    recorder = Recorder()
    _install(monkeypatch, recorder, session_maker)

    assert await asyncio.to_thread(main, ["plan", "--stage", "create"]) == EXIT_OK

    assert all(method == "GET" for method, _ in recorder.seen)
    report = _output(capsys)
    assert report["ready"] is True
    assert report["stage"] == "create"
    assert report["confirmation_phrase"].startswith("create-voucher-canary-")
    # Configuration, counters, ledger state and the authorisation are separate.
    assert report["immutable_template_digest"] != report["plan_digest"]
    assert report["counters_observed"] == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert report["ledger_state"]["status"] is None
    assert report["ready_for_send"] is False


@pytest.mark.asyncio
async def test_an_unready_plan_is_a_contract_mismatch_not_a_success(monkeypatch, capsys, session_maker) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_canary_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_customer_uuid", CUSTOMER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_canary_account_uuid", ACCOUNT_UUID, raising=False)
    _install(monkeypatch, Recorder(), session_maker)

    assert await asyncio.to_thread(main, ["plan", "--stage", "create"]) == EXIT_CONTRACT_MISMATCH
    assert CANARY_DISABLED_BY_ENV in _output(capsys)["reasons"]


@pytest.mark.asyncio
async def test_the_plan_output_carries_no_identity(monkeypatch, capsys, session_maker, configured) -> None:
    _install(monkeypatch, Recorder(), session_maker)
    await asyncio.to_thread(main, ["plan", "--stage", "create"])

    out = capsys.readouterr().out
    for forbidden in (
        CUSTOMER_UUID,
        STAFFER_UUID,
        ACCOUNT_UUID,
        KEY,
        SLUG,
        "Synthetic",
        "fixture@example.invalid",
        "+49",
        "Bearer",
    ):
        assert forbidden not in out, forbidden
    # The branch and product identities ARE expected: they are committed
    # constants, not runtime secrets.
    assert KARLSRUHE_LOCATION_UUID in out


@pytest.mark.asyncio
async def test_status_is_database_only(monkeypatch, capsys, session_maker, configured) -> None:
    recorder = Recorder()
    _install(monkeypatch, recorder, session_maker)

    # `main` runs its own event loop, so it goes to a worker thread here.
    assert await asyncio.to_thread(main, ["status"]) == EXIT_OK

    assert recorder.seen == []
    report = _output(capsys)
    assert report["ledger"]["ledger_row_exists"] is False


@pytest.mark.asyncio
async def test_reconcile_with_no_row_refuses(monkeypatch, capsys, session_maker, configured) -> None:
    _install(monkeypatch, Recorder(), session_maker)

    assert await asyncio.to_thread(main, ["reconcile"]) == EXIT_CONTRACT_MISMATCH
    assert _output(capsys)["outcome"] == "refused"


@pytest.mark.asyncio
async def test_reconcile_reports_a_manual_cleanup_for_an_open_draft(
    monkeypatch, capsys, session_maker, configured
) -> None:
    # Put the ledger into `created` with an open draft, without any mutation.
    from datetime import timedelta

    from altegio_bot.easyweek_voucher_canary.plan import canary_marker
    from altegio_bot.utils import utcnow

    now = utcnow()
    await ledger_module.claim_create(
        session_maker,
        create_plan_digest="a" * 64,
        template_config_digest="b" * 64,
        customer_fingerprint="c" * 64,
        staffer_fingerprint="d" * 64,
        account_fingerprint="e" * 64,
        reconciliation_marker=canary_marker(),
        create_window_start=now - timedelta(minutes=10),
        create_window_end=now + timedelta(hours=6),
    )
    await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATED,
        expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
        target_order_uuid=ORDER_UUID,
        verified_field="create_verified_at",
    )

    _install(monkeypatch, Recorder(order=open_order(marker=canary_marker())), session_maker)

    assert await asyncio.to_thread(main, ["reconcile"]) == EXIT_MANUAL_CLEANUP

    report = _output(capsys)
    assert report["manual_cleanup_required"] is True
    assert report["order_state"] == "open"
    assert ORDER_UUID not in json.dumps(report)


# ---------------------------------------------------------------------------
# Exit code vocabulary
# ---------------------------------------------------------------------------


def test_every_outcome_has_its_own_stable_exit_code() -> None:
    codes = {
        EXIT_OK,
        EXIT_ARGUMENTS,
        EXIT_UNKNOWN_MUTATION,
        EXIT_CONTRACT_MISMATCH,
        EXIT_AMBIGUOUS,
        EXIT_MANUAL_CLEANUP,
        EXIT_ROLLBACK_UNPROVEN,
    }
    assert len(codes) == 7
    assert EXIT_OK == 0
    assert 0 not in codes - {EXIT_OK}
    # Every runner outcome is mapped; a new one cannot silently become success.
    from altegio_bot.easyweek_voucher_canary import runner as runner_module

    mapped = set(cli._OUTCOME_EXIT_CODES)
    declared = {
        value for name, value in vars(runner_module).items() if name.startswith("OUTCOME_") and isinstance(value, str)
    }
    assert mapped == declared


def test_the_unknown_code_is_not_named_retryable() -> None:
    """Exit 3 means a request went out and its effect is unknown."""
    names = [name for name in vars(cli) if name.startswith("EXIT_")]
    assert all("RETRY" not in name for name in names), names
    assert EXIT_UNKNOWN_MUTATION == 3


# ---------------------------------------------------------------------------
# The CLI is not a worker, an endpoint or a campaign
# ---------------------------------------------------------------------------


def test_the_cli_creates_no_campaign_job_or_outbox_row() -> None:
    code = code_without_docstrings(cli)
    for forbidden in ("CampaignRun", "CampaignRecipient", "MessageJob", "Outbox", "APIRouter", "FastAPI"):
        assert forbidden not in code, forbidden

    imported = imported_modules(cli)
    assert not any("campaigns" in name for name in imported), imported


def test_the_cli_writes_no_file() -> None:
    code = code_without_docstrings(cli)
    for forbidden in ("open(", "write_text", "Path(", "makedirs"):
        assert forbidden not in code, forbidden


@pytest.mark.asyncio
async def test_the_cli_silences_url_logging_before_any_client_exists(
    monkeypatch, caplog, session_maker, configured
) -> None:
    """The shared test conftest pins httpx to WARNING, which would hide this."""
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.INFO)
    caplog.set_level(logging.INFO)

    _install(monkeypatch, Recorder(), session_maker)
    assert await asyncio.to_thread(main, ["plan", "--stage", "create"]) == EXIT_OK

    recorded = "\n".join(record.getMessage() for record in caplog.records)
    for forbidden in ("my.easyweek.io", "/voucher-templates", "/customers", KEY, SLUG, "Bearer"):
        assert forbidden not in recorded, forbidden
    assert logging.getLogger("httpx").level == logging.WARNING
