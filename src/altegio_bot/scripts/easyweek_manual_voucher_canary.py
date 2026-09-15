"""Operator CLI for the controlled manual-basis voucher delivery canary (§37.2).

Eight commands, each a deliberate stop::

    ... easyweek_manual_voucher_canary status
    ... easyweek_manual_voucher_canary plan --stage create  --preview-run-id N --campaign-recipient-id M
    ... easyweek_manual_voucher_canary create  --apply --plan-digest D --plan-issued-at T --confirm P ...
    ... easyweek_manual_voucher_canary plan --stage pay     ...
    ... easyweek_manual_voucher_canary pay     --apply ...
    ... easyweek_manual_voucher_canary plan --stage deliver ...
    ... easyweek_manual_voucher_canary deliver --apply ...
    ... easyweek_manual_voucher_canary plan --stage refund  ...
    ... easyweek_manual_voucher_canary refund  --apply ...
    ... easyweek_manual_voucher_canary reconcile ...

There is deliberately NO command that runs create, pay and deliver in sequence,
and there never may be: a human looking at what the last stage actually did, and
deciding to go on, is the control.

What it takes to reach a customer
---------------------------------
A mutation or a send runs only when ALL of these hold at once:

* ``EASYWEEK_MANUAL_VOUCHER_CANARY_ENABLED`` is true (false by default);
* the exact subcommand was typed — no default does anything;
* ``--apply`` was passed explicitly;
* ``--plan-digest`` matches THIS stage's plan rebuilt live, seconds earlier;
* ``--plan-issued-at`` is inside the plan's short maximum age;
* ``--confirm`` is the exact stage phrase that plan printed;
* the durable ledger state allows this stage;
* the live guard, the 42/42 baseline, the template, the sender and the key all
  still prove out.

Argparse abbreviation is off, so a half-typed flag authorises nothing, and every
argparse exit — ``--help`` included — returns the argument code rather than the
success code. A mistyped invocation must never be mistaken for a canary that
worked.

Nothing printed here carries a voucher code, a phone number, a name, a message
parameter, a request body, a response body, a header or a key.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from typing import Any, Final

from altegio_bot.campaigns.easyweek_manual_voucher import runner as runner_module
from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    ACCOUNT_UNCONFIGURED,
    CANARY_DISABLED,
    DATABASE_UNAVAILABLE,
    MUTATION_STAGES,
    RUNTIME_IDENTITY_UNUSABLE,
    STAFFER_UNCONFIGURED,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
)
from altegio_bot.campaigns.easyweek_manual_voucher.runner import ManualCanaryRequest
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import VoucherDeliveryClient
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError
from altegio_bot.easyweek_log_redaction import redact_easyweek_url_logging
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationClient
from altegio_bot.settings import settings

# Distinct, stable exit codes. A wrapper acts on the number, never on prose.
EXIT_OK: Final = 0
EXIT_ARGUMENTS: Final = 2
# Something left this process and its effect is UNKNOWN. Never wire this to a
# re-run: for a send it means a customer may already be holding the code.
EXIT_UNKNOWN: Final = 3
EXIT_CONTRACT_MISMATCH: Final = 4
EXIT_MANUAL_CLEANUP: Final = 6

COMMAND_PLAN: Final = "plan"
COMMAND_STATUS: Final = "status"
COMMAND_RECONCILE: Final = "reconcile"

# The EasyWeek sender line every EasyWeek send resolves through.
SENDER_CODE: Final = "default"


class _SafetyArgumentParser(argparse.ArgumentParser):
    """An ``ArgumentParser`` that can never hand back the success exit code."""

    def exit(self, status: int = 0, message: str | None = None) -> None:  # type: ignore[override]
        if message:
            self._print_message(message, sys.stderr)
        raise SystemExit(EXIT_ARGUMENTS if status == 0 else status)


def _add_recipient_args(parser: argparse.ArgumentParser) -> None:
    """The exact preview recipient, and nothing looser.

    Never a phone number, a name or a bare customer UUID. An operator naming a
    person directly would be a way to point a real voucher at anybody; the pair
    of ids points at one row an operator already chose in a preview, and the
    server re-reads everything about it.
    """
    parser.add_argument("--preview-run-id", type=int, required=True)
    parser.add_argument("--campaign-recipient-id", type=int, required=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafetyArgumentParser(
        prog="easyweek_manual_voucher_canary",
        description=(
            "Operator-only controlled EasyWeek voucher canary for a manually selected "
            "recipient. One person, one voucher, one message — never a campaign."
        ),
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser(COMMAND_PLAN, help="Read-only. Build and print ONE stage's plan.")
    plan_parser.add_argument("--stage", required=True, choices=list(MUTATION_STAGES))
    _add_recipient_args(plan_parser)

    subparsers.add_parser(COMMAND_STATUS, help="Database only. Print the durable canary state.")

    reconcile_parser = subparsers.add_parser(
        COMMAND_RECONCILE, help="Reads only. Resolve an unknown EasyWeek stage by looking."
    )
    _add_recipient_args(reconcile_parser)

    for stage in MUTATION_STAGES:
        stage_parser = subparsers.add_parser(stage, help=f"Perform the ONE {stage} step, after a committed claim.")
        _add_recipient_args(stage_parser)
        stage_parser.add_argument("--apply", action="store_true", help="Required. Without it nothing is sent.")
        stage_parser.add_argument("--plan-digest", default="")
        stage_parser.add_argument("--plan-issued-at", default="")
        stage_parser.add_argument("--confirm", default="")
    return parser


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _parse_issued_at(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    # A naive timestamp is not an approval about a moment: it is an approval
    # about a moment in an unstated timezone.
    return parsed if parsed.tzinfo is not None else None


def _refusal_report(stage: str, reasons: list[str]) -> dict[str, Any]:
    return runner_module.StageReport(stage=stage, outcome="refused", reasons=reasons).as_safe_dict()


def _build_request(args: argparse.Namespace) -> tuple[ManualCanaryRequest | None, dict[str, Any] | None]:
    """The frozen identity for this run, or a refusal naming what is unusable.

    The branch and the template are pinned literals. The staffer and the payment
    account come from this canary's own environment variables — separate from
    §35's and §36's, so that configuring one canary never configures another.
    """
    staffer = (settings.easyweek_manual_voucher_staffer_uuid or "").strip()
    account = (settings.easyweek_manual_voucher_account_uuid or "").strip()
    reasons: list[str] = []
    if not staffer:
        reasons.append(STAFFER_UNCONFIGURED)
    if not account:
        reasons.append(ACCOUNT_UNCONFIGURED)
    if reasons:
        return None, _refusal_report(args.command, [*reasons, RUNTIME_IDENTITY_UNUSABLE])
    return (
        ManualCanaryRequest(
            preview_run_id=args.preview_run_id,
            campaign_recipient_id=args.campaign_recipient_id,
            sender_code=SENDER_CODE,
            staffer_uuid=staffer,
            payment_account_uuid=account,
        ),
        None,
    )


async def _run_plan(request: ManualCanaryRequest, stage: str) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        async with SessionLocal() as session:
            plan, _proof, _prereq, _baseline = await runner_module.build_stage_plan(
                session,
                SessionLocal,
                stage=stage,
                request=request,
                reader=reader,
                order_reader=reader,
            )
    return plan.as_safe_dict(), (EXIT_OK if plan.ready else EXIT_CONTRACT_MISMATCH)


async def _run_status() -> tuple[dict[str, Any], int]:
    report = await runner_module.run_status(SessionLocal)
    return report.as_safe_dict(), EXIT_OK


async def _run_reconcile(request: ManualCanaryRequest) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        report = await runner_module.run_reconcile(SessionLocal, request=request, order_reader=reader)
    payload = report.as_safe_dict()
    return payload, _exit_for(report)


def _exit_for(report: runner_module.StageReport) -> int:
    if report.outcome == "unknown":
        return EXIT_UNKNOWN
    if report.outcome in ("refused", "rejected"):
        return EXIT_CONTRACT_MISMATCH
    if report.manual_cleanup_required:
        # Proven, and something is still open in the POS that a human must close.
        return EXIT_MANUAL_CLEANUP
    return EXIT_OK


async def _run_stage(request: ManualCanaryRequest, args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    """One stage, with its own transports opened for the length of one call."""
    stage = args.command
    issued_at = _parse_issued_at(args.plan_issued_at)

    async with EasyWeekClient() as reader:
        async with SessionLocal() as session:
            common = {
                "request": request,
                "reader": reader,
                "order_reader": reader,
                "apply": bool(args.apply),
                "supplied_digest": args.plan_digest,
                "supplied_issued_at": issued_at,
                "supplied_phrase": args.confirm,
            }
            if stage == STAGE_DELIVER:
                async with VoucherDeliveryClient() as sender:
                    report = await runner_module.run_deliver(session, SessionLocal, sender=sender, **common)
            else:
                async with EasyWeekVoucherMutationClient() as mutator:
                    if stage == STAGE_CREATE:
                        report = await runner_module.run_create(session, SessionLocal, mutator=mutator, **common)
                    elif stage == STAGE_PAY:
                        report = await runner_module.run_pay(session, SessionLocal, mutator=mutator, **common)
                    else:
                        report = await runner_module.run_refund(session, SessionLocal, mutator=mutator, **common)
    return report.as_safe_dict(), _exit_for(report)


async def _dispatch(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    # The fence is checked before anything opens a socket or a session — the
    # read-only plan included. A closed fence means this command does nothing at
    # all, not "nothing that writes".
    if not settings.easyweek_manual_voucher_canary_enabled:
        return _refusal_report(args.command, [CANARY_DISABLED]), EXIT_CONTRACT_MISMATCH

    if args.command == COMMAND_STATUS:
        return await _run_status()

    request, refusal = _build_request(args)
    if refusal is not None or request is None:
        return refusal or _refusal_report(args.command, [RUNTIME_IDENTITY_UNUSABLE]), EXIT_CONTRACT_MISMATCH

    if args.command == COMMAND_PLAN:
        return await _run_plan(request, args.stage)
    if args.command == COMMAND_RECONCILE:
        return await _run_reconcile(request)
    return await _run_stage(request, args)


def main(argv: list[str] | None = None) -> int:
    redact_easyweek_url_logging()
    args = _build_parser().parse_args(argv)
    try:
        payload, code = asyncio.run(_dispatch(args))
    except EasyWeekConfigError:
        # A deployment fault, not a verdict about the canary. Named, without the
        # configuration it is complaining about.
        payload, code = _refusal_report(args.command, [RUNTIME_IDENTITY_UNUSABLE]), EXIT_CONTRACT_MISMATCH
    except Exception:  # noqa: BLE001 - a stable, PII-free surface for operators
        # Deliberately no traceback, no exception text and no SQL: an operator
        # report is pasted into tickets, and an exception string here could
        # carry a URL with a customer UUID in it.
        payload, code = _refusal_report(args.command, [DATABASE_UNAVAILABLE]), EXIT_CONTRACT_MISMATCH
    _print_json(payload)
    return code


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(main())
