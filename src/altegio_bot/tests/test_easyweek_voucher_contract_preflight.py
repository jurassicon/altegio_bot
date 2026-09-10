"""Operator preflight tests: confirmation gate, call order, output safety, exit codes.

The CLI is driven end-to-end through ``httpx.MockTransport``: no test opens a
socket, a database session or a file.
"""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_voucher_calculation import EasyWeekVoucherCalculationClient
from altegio_bot.scripts import easyweek_voucher_contract_preflight as preflight
from altegio_bot.scripts.easyweek_voucher_contract_preflight import (
    EXIT_ARGUMENTS,
    EXIT_CONTRACT_MISMATCH,
    EXIT_OK,
    EXIT_RETRYABLE_UNCERTAINTY,
    GIFT_CARD_CALCULATION_NOT_CONFIRMED,
    main,
)
from altegio_bot.tests.easyweek_voucher_evidence_fixtures import (
    LOCATIONS,
    TEMPLATE,
    TEMPLATE_UUID,
    WORKSPACE,
    canonical_response,
    code_without_docstrings,
    imported_modules,
)

KEY = "SENTINEL_PREKEY_ccc111"
SLUG = "SENTINEL_PRESLUG_ccc222"
BODY_MARKER = "SENTINEL_PREBODY_ccc333"
BASE = "https://my.easyweek.io/api/public/v2"
CONFIRM = "--confirm-nonpersistent-calculate"


class Recorder:
    """One MockTransport handler for both clients, recording method+path."""

    def __init__(
        self,
        *,
        calculate_response: httpx.Response | None = None,
        template_after: dict[str, Any] | None = None,
    ) -> None:
        self.seen: list[tuple[str, str]] = []
        self.calculate_response = calculate_response
        self.template_after = template_after
        self._template_reads = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/public/v2")
        self.seen.append((request.method, path))

        if request.method == "POST" and path == "/orders/calculate":
            return self.calculate_response or httpx.Response(200, json=canonical_response())
        if path == "/workspace":
            return httpx.Response(200, json=WORKSPACE)
        if path == "/locations":
            return httpx.Response(200, json=LOCATIONS)
        if path == "/voucher-templates":
            return httpx.Response(200, json=[TEMPLATE])
        if path == f"/voucher-templates/{TEMPLATE_UUID}":
            self._template_reads += 1
            if self._template_reads > 1 and self.template_after is not None:
                return httpx.Response(200, json=self.template_after)
            return httpx.Response(200, json=TEMPLATE)
        raise AssertionError(f"unexpected request: {request.method} {path}")


def _install(monkeypatch: pytest.MonkeyPatch, recorder: Recorder) -> None:
    transport = httpx.MockTransport(recorder)

    def read_client(*args: Any, **kwargs: Any) -> EasyWeekClient:
        return EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=BASE, transport=transport)

    def calculation_client(*args: Any, **kwargs: Any) -> EasyWeekVoucherCalculationClient:
        return EasyWeekVoucherCalculationClient(api_key=KEY, workspace_slug=SLUG, base_url=BASE, transport=transport)

    monkeypatch.setattr(preflight, "EasyWeekClient", read_client)
    monkeypatch.setattr(preflight, "EasyWeekVoucherCalculationClient", calculation_client)


def _output(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


# ---------------------------------------------------------------------------
# The confirmation gate
# ---------------------------------------------------------------------------


def test_without_the_confirmation_flag_no_client_is_even_constructed(monkeypatch, capsys) -> None:
    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("an unconfirmed run must not construct a client")

    monkeypatch.setattr(preflight, "EasyWeekClient", forbidden)
    monkeypatch.setattr(preflight, "EasyWeekVoucherCalculationClient", forbidden)

    assert main([]) == EXIT_ARGUMENTS

    report = _output(capsys)
    assert report["reasons"] == [GIFT_CARD_CALCULATION_NOT_CONFIRMED]
    assert report["calculation_contract_ready"] is False
    assert report["ready_for_send"] is False
    assert report["delivery_authorized"] is False


@pytest.mark.parametrize("argv", [["--confirm"], ["--confirm-nonpersistent"], ["--yes"], ["1"]])
def test_a_bad_invocation_exits_with_the_argument_code(monkeypatch, argv) -> None:
    # `--confirm` is deliberately NOT enough: argparse abbreviation is off, so a
    # half-typed flag cannot authorise the POST.
    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("a bad invocation must not construct a client")

    monkeypatch.setattr(preflight, "EasyWeekClient", forbidden)
    monkeypatch.setattr(preflight, "EasyWeekVoucherCalculationClient", forbidden)

    with pytest.raises(SystemExit) as excinfo:
        main(argv)
    assert excinfo.value.code == EXIT_ARGUMENTS


@pytest.mark.parametrize("forbidden_flag", ["--location-uuid", "--template-uuid", "--price", "--price-minor"])
def test_location_template_and_price_cannot_be_passed_in(forbidden_flag) -> None:
    parser = preflight._build_parser()
    actions = {option for action in parser._actions for option in action.option_strings}
    assert forbidden_flag not in actions
    assert actions == {"-h", "--help", CONFIRM}


# ---------------------------------------------------------------------------
# The exact call sequence
# ---------------------------------------------------------------------------


def test_the_confirmed_run_reads_then_posts_once_then_rereads(monkeypatch, capsys) -> None:
    recorder = Recorder()
    _install(monkeypatch, recorder)

    assert main([CONFIRM]) == EXIT_OK

    assert recorder.seen == [
        ("GET", "/workspace"),
        ("GET", "/locations"),
        ("GET", "/voucher-templates"),
        ("GET", f"/voucher-templates/{TEMPLATE_UUID}"),
        ("POST", "/orders/calculate"),
        ("GET", f"/voucher-templates/{TEMPLATE_UUID}"),
    ]
    assert sum(1 for method, _ in recorder.seen if method == "POST") == 1
    capsys.readouterr()


@pytest.mark.parametrize(
    "calculate_response",
    [
        httpx.Response(422, json={"errors": {"vouchers.0.price": ["required"]}}),
        httpx.Response(500, json={"message": BODY_MARKER}),
        httpx.Response(429, json={"message": BODY_MARKER}),
        httpx.Response(200, json={"invoice": {"total": BODY_MARKER}}),
    ],
)
def test_a_failing_calculate_is_still_posted_only_once(monkeypatch, capsys, calculate_response) -> None:
    recorder = Recorder(calculate_response=calculate_response)
    _install(monkeypatch, recorder)

    code = main([CONFIRM])

    assert code != EXIT_OK
    assert sum(1 for method, _ in recorder.seen if method == "POST") == 1
    report = _output(capsys)
    assert report["calculation_contract_ready"] is False


def test_a_blocked_prerequisite_never_reaches_the_post(monkeypatch, capsys) -> None:
    class WrongWorkspace(Recorder):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path.removeprefix("/api/public/v2")
            if path == "/workspace":
                self.seen.append((request.method, path))
                return httpx.Response(200, json={**WORKSPACE, "currency": "USD"})
            return super().__call__(request)

    recorder = WrongWorkspace()
    _install(monkeypatch, recorder)

    assert main([CONFIRM]) == EXIT_CONTRACT_MISMATCH
    assert not any(method == "POST" for method, _ in recorder.seen)
    report = _output(capsys)
    assert report["workspace_proven"] is False


# ---------------------------------------------------------------------------
# Report content and exit codes
# ---------------------------------------------------------------------------


def test_a_green_run_still_reports_that_nothing_may_be_sent(monkeypatch, capsys) -> None:
    _install(monkeypatch, Recorder())

    assert main([CONFIRM]) == EXIT_OK

    report = _output(capsys)
    assert report["calculation_contract_ready"] is True
    assert report["issue_contract_ready"] is False
    assert report["individual_voucher_artifact_proven"] is False
    assert report["customer_binding_proven"] is False
    assert report["write_idempotency_proven"] is False
    assert report["unknown_result_reconciliation_proven"] is False
    assert report["delivery_authorized"] is False
    assert report["ready_for_send"] is False
    assert report["send_authorization"] == "calculation_evidence_is_not_send_authorization"
    assert report["mode"] == "nonpersistent_calculation_evidence"


def test_the_report_compares_the_template_counters(monkeypatch, capsys) -> None:
    _install(monkeypatch, Recorder())
    main([CONFIRM])

    report = _output(capsys)
    assert report["template_counters_before"] == {"vouchers_count": 0, "activated_vouchers_count": 0}
    assert report["template_counters_after"] == report["template_counters_before"]
    assert report["template_counters_unchanged"] is True
    assert report["template_pristine"] is True


def test_counter_drift_fails_closed_with_the_mismatch_code(monkeypatch, capsys) -> None:
    _install(monkeypatch, Recorder(template_after={**TEMPLATE, "vouchers_count": 1}))

    assert main([CONFIRM]) == EXIT_CONTRACT_MISMATCH

    report = _output(capsys)
    assert report["template_counters_unchanged"] is False
    assert "gift_card_template_counter_drift" in report["reasons"]


@pytest.mark.parametrize(
    "calculate_response,expected_code",
    [
        (httpx.Response(200, json=canonical_response()), EXIT_OK),
        (httpx.Response(429, json={}), EXIT_RETRYABLE_UNCERTAINTY),
        (httpx.Response(503, json={}), EXIT_RETRYABLE_UNCERTAINTY),
        (httpx.Response(422, json={}), EXIT_CONTRACT_MISMATCH),
        (httpx.Response(403, json={}), EXIT_CONTRACT_MISMATCH),
        (httpx.Response(200, json=canonical_response(total=1499)), EXIT_CONTRACT_MISMATCH),
        (
            httpx.Response(200, json=canonical_response(order_uuid="11111111-2222-4333-8444-555555555555")),
            EXIT_CONTRACT_MISMATCH,
        ),
    ],
)
def test_each_outcome_has_its_own_stable_exit_code(monkeypatch, capsys, calculate_response, expected_code) -> None:
    _install(monkeypatch, Recorder(calculate_response=calculate_response))

    assert main([CONFIRM]) == expected_code
    capsys.readouterr()


def test_an_unreached_run_reports_the_same_shape_as_a_real_one(monkeypatch, capsys) -> None:
    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("an unconfirmed run must not construct a client")

    monkeypatch.setattr(preflight, "EasyWeekClient", forbidden)
    monkeypatch.setattr(preflight, "EasyWeekVoucherCalculationClient", forbidden)
    main([])
    unconfirmed = _output(capsys)

    _install(monkeypatch, Recorder())
    main([CONFIRM])
    green = _output(capsys)

    # `account_paid_amount_observed` is present only when the API sent one.
    assert set(green) - set(unconfirmed) == {"account_paid_amount_observed"}
    assert set(unconfirmed) - set(green) == set()


def test_the_four_exit_codes_are_distinct() -> None:
    codes = {EXIT_OK, EXIT_ARGUMENTS, EXIT_RETRYABLE_UNCERTAINTY, EXIT_CONTRACT_MISMATCH}
    assert len(codes) == 4
    assert EXIT_OK == 0
    assert 0 not in {EXIT_ARGUMENTS, EXIT_RETRYABLE_UNCERTAINTY, EXIT_CONTRACT_MISMATCH}


def test_the_safe_output_carries_no_uuid_secret_or_raw_body(monkeypatch, capsys) -> None:
    response = canonical_response()
    response["invoice"]["comment"] = BODY_MARKER
    response["customer"] = {"uuid": "11111111-2222-4333-8444-555555555555", "name": BODY_MARKER}
    _install(monkeypatch, Recorder(calculate_response=httpx.Response(200, json=response)))

    main([CONFIRM])

    out = capsys.readouterr().out
    for forbidden in (KEY, SLUG, BODY_MARKER, TEMPLATE_UUID, "my.easyweek.io", "Authorization", "Bearer"):
        assert forbidden not in out, forbidden


# ---------------------------------------------------------------------------
# What the CLI is not
# ---------------------------------------------------------------------------


def test_the_cli_opens_no_session_and_touches_no_campaign_row() -> None:
    imported = imported_modules(preflight)
    forbidden_modules = {
        "altegio_bot.db",
        "altegio_bot.models.models",
        "sqlalchemy",
        "sqlalchemy.ext.asyncio",
    }
    assert not (imported & forbidden_modules), imported & forbidden_modules

    code = code_without_docstrings(preflight)
    for forbidden in ("SessionLocal", "CampaignRun", "CampaignRecipient", "MessageJob", "Outbox", "commit("):
        assert forbidden not in code, forbidden


def test_the_cli_never_writes_a_file() -> None:
    code = code_without_docstrings(preflight)
    for forbidden in ("open(", "write_text", "Path(", "makedirs"):
        assert forbidden not in code, forbidden


def test_the_module_is_a_script_not_a_router() -> None:
    code = code_without_docstrings(preflight)
    for forbidden in ("APIRouter", "@router", "FastAPI", "Depends("):
        assert forbidden not in code, forbidden


def test_no_alembic_migration_was_added() -> None:
    versions = Path(__file__).resolve().parents[3] / "alembic" / "versions"
    assert versions.is_dir()
    added = [path.name for path in versions.glob("*.py") if "voucher_calculation" in path.name]
    assert added == []


def test_the_confirmation_flag_name_is_the_documented_one() -> None:
    # A renamed flag would silently invalidate the runbook procedure.
    assert preflight.CONFIRMATION_FLAG == CONFIRM
    tree = ast.parse(inspect.getsource(preflight))
    assert isinstance(tree, ast.Module)
