"""Operator CLI for the controlled EasyWeek voucher mutation canary (§35).

Six commands, each a deliberate stop::

    python -m altegio_bot.scripts.easyweek_voucher_canary plan
    python -m altegio_bot.scripts.easyweek_voucher_canary status
    python -m altegio_bot.scripts.easyweek_voucher_canary reconcile
    python -m altegio_bot.scripts.easyweek_voucher_canary create    --apply ...
    python -m altegio_bot.scripts.easyweek_voucher_canary pay       --apply ...
    python -m altegio_bot.scripts.easyweek_voucher_canary refund    --apply ...

There is deliberately NO command that runs create, pay and refund in sequence.
A human looking at what the previous stage actually did, and deciding to go on,
is the control — not an inconvenience to be automated away.

What it takes to mutate
-----------------------
A mutation command runs only when ALL of these hold at once:

* ``EASYWEEK_VOUCHER_CANARY_ENABLED`` is true (false by default, everywhere);
* the exact subcommand was typed — no default does anything;
* ``--apply`` was passed explicitly;
* ``--plan-digest`` matches a plan recomputed live, seconds earlier;
* ``--plan-issued-at`` is within the plan's short max age;
* ``--confirm`` is the exact stage phrase that plan prints;
* the durable ledger state allows this stage;
* the live GETs behind the plan still agree with it.

Argparse abbreviation is off, so a half-typed flag authorises nothing, and every
argparse exit — ``--help`` included — returns the argument code rather than the
success code. A mistyped invocation must never be mistaken for a canary that
worked.

``plan``, ``status`` and ``reconcile`` never mutate. ``reconcile`` and ``status``
are safe to repeat as often as an operator likes.

Nothing printed here carries a customer, a staffer, an account, a name, a phone
number, an e-mail, a voucher code, a customer-facing URL, a request body, a
response body, a header or a key.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Any, Final

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError
from altegio_bot.easyweek_voucher_canary import runner as runner_module
from altegio_bot.easyweek_voucher_canary.plan import (
    CANARY_DISABLED_BY_ENV,
    MUTATION_STAGES,
    STAGE_CREATE,
    STAGE_PAY,
    STAGE_REFUND,
    RuntimeIdentity,
    build_plan,
    resolve_runtime_identity,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationClient
from altegio_bot.settings import settings

# Distinct, stable exit codes. A wrapper acts on the number, never on prose.
EXIT_OK: Final = 0
EXIT_ARGUMENTS: Final = 2
# The request went out and its effect is UNKNOWN. Never wire this to a re-run.
EXIT_UNKNOWN_MUTATION: Final = 3
EXIT_CONTRACT_MISMATCH: Final = 4
EXIT_AMBIGUOUS: Final = 5
EXIT_MANUAL_CLEANUP: Final = 6
EXIT_ROLLBACK_UNPROVEN: Final = 7

_OUTCOME_EXIT_CODES: Final = {
    runner_module.OUTCOME_PROVEN: EXIT_OK,
    runner_module.OUTCOME_REFUSED: EXIT_CONTRACT_MISMATCH,
    runner_module.OUTCOME_UNKNOWN_MUTATION: EXIT_UNKNOWN_MUTATION,
    runner_module.OUTCOME_CONTRACT_MISMATCH: EXIT_CONTRACT_MISMATCH,
    runner_module.OUTCOME_AMBIGUOUS: EXIT_AMBIGUOUS,
    runner_module.OUTCOME_MANUAL_CLEANUP: EXIT_MANUAL_CLEANUP,
    runner_module.OUTCOME_ROLLBACK_UNPROVEN: EXIT_ROLLBACK_UNPROVEN,
}

COMMAND_PLAN: Final = "plan"
COMMAND_STATUS: Final = "status"
COMMAND_RECONCILE: Final = "reconcile"
MUTATION_COMMANDS: Final = (STAGE_CREATE, STAGE_PAY, STAGE_REFUND)

# httpx logs every request at INFO as a full URL, and httpcore can log connection
# targets. Neither belongs in an operator transcript for a command like this.
_URL_LOGGING_NAMESPACES: Final = ("httpx", "httpcore")


class _SafetyArgumentParser(argparse.ArgumentParser):
    """An ``ArgumentParser`` that can never hand back the success exit code.

    ``--help`` normally exits ``0``, which here is the code a proven canary
    stage returns. A wrapper that mistyped a flag would then read "success" off
    a help screen.
    """

    def exit(self, status: int = 0, message: str | None = None) -> None:  # type: ignore[override]
        if message:
            self._print_message(message, sys.stderr)
        raise SystemExit(EXIT_ARGUMENTS if status == 0 else status)


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafetyArgumentParser(
        prog="easyweek_voucher_canary",
        description=(
            "Operator-only controlled EasyWeek voucher mutation canary. "
            "This is research, not a campaign, and never a permission to send."
        ),
        # Abbreviations are off: a half-typed flag must not authorise a mutation.
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(COMMAND_PLAN, help="Read-only. Build and print the plan and its digest.")
    subparsers.add_parser(COMMAND_STATUS, help="Database only. Print the durable canary state.")
    subparsers.add_parser(COMMAND_RECONCILE, help="Reads only. Resolve an unknown stage by looking.")

    for stage in MUTATION_COMMANDS:
        stage_parser = subparsers.add_parser(
            stage,
            help=f"Send the ONE {stage} request, after a committed claim.",
        )
        stage_parser.add_argument(
            "--apply",
            action="store_true",
            help="Required. Without it this command sends nothing.",
        )
        stage_parser.add_argument("--plan-digest", default="", help="The digest printed by `plan`.")
        stage_parser.add_argument("--plan-issued-at", default="", help="The plan_issued_at printed by `plan`.")
        stage_parser.add_argument("--confirm", default="", help="The exact stage phrase printed by `plan`.")
    return parser


def _silence_url_logging() -> None:
    """Keep full request URLs out of this command's stderr."""
    for name in _URL_LOGGING_NAMESPACES:
        logging.getLogger(name).setLevel(logging.WARNING)


def _print_json(payload: dict[str, Any]) -> None:
    # stdout carries the safe report and nothing else; library logs go to stderr.
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _parse_issued_at(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _refusal_report(stage: str, reasons: list[str]) -> dict[str, Any]:
    """The same shape as a real stage report, with everything absent."""
    return runner_module.StageReport(
        stage=stage,
        outcome=runner_module.OUTCOME_REFUSED,
        reasons=reasons,
    ).as_safe_dict()


def _identity_or_refusal(stage: str) -> tuple[RuntimeIdentity | None, dict[str, Any] | None]:
    identity, reasons = resolve_runtime_identity(
        customer_uuid=settings.easyweek_voucher_canary_customer_uuid,
        staffer_uuid=settings.easyweek_voucher_canary_staffer_uuid,
        account_uuid=settings.easyweek_voucher_canary_account_uuid,
    )
    if identity is None:
        # The reason names the ROLE that was unusable; the value never appears.
        return None, _refusal_report(stage, list(reasons))
    return identity, None


async def _run_plan(identity: RuntimeIdentity) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        plan = await build_plan(reader, identity=identity, enabled=settings.easyweek_voucher_canary_enabled)
    return plan.as_safe_dict(), (EXIT_OK if plan.ready else EXIT_CONTRACT_MISMATCH)


async def _run_status() -> tuple[dict[str, Any], int]:
    report = await runner_module.run_status(SessionLocal)
    return report.as_safe_dict(), _OUTCOME_EXIT_CODES[report.outcome]


async def _run_reconcile(identity: RuntimeIdentity) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        report = await runner_module.run_reconcile(SessionLocal, reader, identity=identity)
    return report.as_safe_dict(), _OUTCOME_EXIT_CODES[report.outcome]


async def _run_mutation(
    stage: str,
    identity: RuntimeIdentity,
    *,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
) -> tuple[dict[str, Any], int]:
    handlers = {
        STAGE_CREATE: runner_module.run_create,
        STAGE_PAY: runner_module.run_pay,
        STAGE_REFUND: runner_module.run_refund,
    }
    # Two separate clients on purpose: the GET-only one does every read, and the
    # three-endpoint one is the only thing in this process that can mutate.
    async with EasyWeekClient() as reader, EasyWeekVoucherMutationClient() as mutator:
        report = await handlers[stage](
            SessionLocal,
            reader,
            mutator,
            identity=identity,
            enabled=settings.easyweek_voucher_canary_enabled,
            plan_digest=plan_digest,
            plan_issued_at=plan_issued_at,
            confirmation_phrase=confirmation_phrase,
        )
    return report.as_safe_dict(), _OUTCOME_EXIT_CODES[report.outcome]


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    _silence_url_logging()
    args = _build_parser().parse_args(argv)
    command = args.command

    if command in MUTATION_COMMANDS:
        # Checked before any client is constructed and before the env fence is
        # even consulted: an un-applied mutation command must not reach the
        # network, not even to read.
        if not args.apply:
            _print_json(_refusal_report(command, ["canary_apply_flag_missing"]))
            return EXIT_ARGUMENTS
        if not settings.easyweek_voucher_canary_enabled:
            _print_json(_refusal_report(command, [CANARY_DISABLED_BY_ENV]))
            return EXIT_ARGUMENTS
        issued_at = _parse_issued_at(args.plan_issued_at)
        if not args.plan_digest or not args.confirm or issued_at is None:
            _print_json(_refusal_report(command, ["canary_plan_authorisation_missing"]))
            return EXIT_ARGUMENTS

    identity, refusal = _identity_or_refusal(command)
    if identity is None:
        assert refusal is not None
        _print_json(refusal)
        return EXIT_ARGUMENTS

    try:
        if command == COMMAND_PLAN:
            report, code = asyncio.run(_run_plan(identity))
        elif command == COMMAND_STATUS:
            report, code = asyncio.run(_run_status())
        elif command == COMMAND_RECONCILE:
            report, code = asyncio.run(_run_reconcile(identity))
        else:
            report, code = asyncio.run(
                _run_mutation(
                    command,
                    identity,
                    plan_digest=args.plan_digest,
                    plan_issued_at=_parse_issued_at(args.plan_issued_at),
                    confirmation_phrase=args.confirm,
                )
            )
    except EasyWeekConfigError:
        # A missing key or slug is an operator configuration problem, reported
        # as a stable reason — never as the provider's own message.
        _print_json(_refusal_report(command, ["canary_api_unavailable"]))
        return EXIT_ARGUMENTS

    _print_json(report)
    return code


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EXIT_AMBIGUOUS",
    "EXIT_ARGUMENTS",
    "EXIT_CONTRACT_MISMATCH",
    "EXIT_MANUAL_CLEANUP",
    "EXIT_OK",
    "EXIT_ROLLBACK_UNPROVEN",
    "EXIT_UNKNOWN_MUTATION",
    "MUTATION_COMMANDS",
    "MUTATION_STAGES",
    "main",
]
