"""Operator CLI for the controlled EasyWeek voucher delivery canary (§36).

Ten commands, each a deliberate stop::

    ... easyweek_voucher_delivery_canary status
    ... easyweek_voucher_delivery_canary plan --stage create  --preview-run-id N --campaign-recipient-id M
    ... easyweek_voucher_delivery_canary create  --apply --plan-digest D --plan-issued-at T --confirm P ...
    ... easyweek_voucher_delivery_canary plan --stage pay     ...
    ... easyweek_voucher_delivery_canary pay     --apply ...
    ... easyweek_voucher_delivery_canary plan --stage deliver ...
    ... easyweek_voucher_delivery_canary deliver --apply ...
    ... easyweek_voucher_delivery_canary plan --stage refund  ...
    ... easyweek_voucher_delivery_canary refund  --apply ...
    ... easyweek_voucher_delivery_canary reconcile ...

There is deliberately NO command that runs create, pay and deliver in sequence,
and there never may be: a human looking at what the last stage actually did, and
deciding to go on, is the control.

What it takes to reach a customer
---------------------------------
A mutation or a send runs only when ALL of these hold at once:

* ``EASYWEEK_VOUCHER_DELIVERY_CANARY_ENABLED`` is true (false by default);
* the exact subcommand was typed — no default does anything;
* ``--apply`` was passed explicitly;
* ``--plan-digest`` matches THIS stage's plan rebuilt live, seconds earlier;
* ``--plan-issued-at`` is inside the plan's short maximum age;
* ``--confirm`` is the exact stage phrase that plan printed;
* the durable ledger state allows this stage;
* the live guard, the template, the sender and the key all still prove out.

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
import logging
import sys
from datetime import datetime
from typing import Any, Final

from altegio_bot.campaigns.easyweek_voucher_delivery import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import VoucherDeliveryClient
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    APPLY_FLAG_MISSING,
    CANARY_DISABLED,
    MUTATION_STAGES,
    RUNTIME_IDENTITY_UNUSABLE,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_PAY,
    STAGE_REFUND,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.runner import CanaryRequest
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError
from altegio_bot.easyweek_locations import configured_easyweek_locations
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
EXIT_AMBIGUOUS: Final = 5
EXIT_MANUAL_CLEANUP: Final = 6

_OUTCOME_EXIT_CODES: Final = {
    runner_module.OUTCOME_PROVEN: EXIT_OK,
    runner_module.OUTCOME_REFUSED: EXIT_CONTRACT_MISMATCH,
    runner_module.OUTCOME_UNKNOWN: EXIT_UNKNOWN,
    runner_module.OUTCOME_CONTRACT_MISMATCH: EXIT_CONTRACT_MISMATCH,
    runner_module.OUTCOME_AMBIGUOUS: EXIT_AMBIGUOUS,
    runner_module.OUTCOME_MANUAL_CLEANUP: EXIT_MANUAL_CLEANUP,
}

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

    Never a phone number, a name or a bare customer UUID: the entitlement is
    earned by a specific visit recorded in a specific preview run, and anything
    that could address a person without that link would be a different feature.
    """
    parser.add_argument("--preview-run-id", type=int, required=True)
    parser.add_argument("--campaign-recipient-id", type=int, required=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafetyArgumentParser(
        prog="easyweek_voucher_delivery_canary",
        description=(
            "Operator-only controlled EasyWeek voucher delivery canary. "
            "One recipient, one voucher, one message — never a campaign."
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


def _silence_url_logging() -> None:
    """The shared rule, not a second copy of it.

    This used to raise the transport loggers here and only here, which left the
    Ops endpoint — same client, same `/customers/{uuid}` URL, INFO logging —
    with no protection at all. One implementation now owns it, and it scrubs as
    well as silences.
    """
    redact_easyweek_url_logging()


def _print_json(payload: dict[str, Any]) -> None:
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
    return runner_module.StageReport(
        stage=stage,
        outcome=runner_module.OUTCOME_REFUSED,
        reasons=reasons,
    ).as_safe_dict()


def _build_request(args: argparse.Namespace) -> tuple[CanaryRequest | None, dict[str, Any] | None]:
    """The frozen identity for this run, or a refusal naming what is unusable.

    The branch comes from the reviewed location registry; the staffer and the
    payment account come from the same named environment variables §35 already
    uses, because they are the same two production identities and belong in a
    repository no more here than they did there.
    """
    registry = configured_easyweek_locations()
    locations = [location for location in registry.locations.values()] if registry.ready else []
    karlsruhe = next((entry for entry in locations if entry.name.lower().startswith("k")), None)
    staffer = (settings.easyweek_voucher_canary_staffer_uuid or "").strip()
    account = (settings.easyweek_voucher_canary_account_uuid or "").strip()
    if karlsruhe is None or not staffer or not account:
        return None, _refusal_report(args.command, [RUNTIME_IDENTITY_UNUSABLE])
    return (
        CanaryRequest(
            preview_run_id=args.preview_run_id,
            campaign_recipient_id=args.campaign_recipient_id,
            company_id=karlsruhe.company_id,
            sender_code=SENDER_CODE,
            staffer_uuid=staffer,
            payment_account_uuid=account,
        ),
        None,
    )


def _booking_link(request: CanaryRequest) -> str:
    registry = configured_easyweek_locations()
    location = registry.locations.get(request.company_id) if registry.ready else None
    return location.booking_page_url if location is not None else ""


async def _run_plan(request: CanaryRequest, stage: str) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        async with SessionLocal() as session:
            plan, _, _ = await runner_module.build_stage_plan(
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
    return report.as_safe_dict(), _OUTCOME_EXIT_CODES[report.outcome]


async def _run_reconcile(request: CanaryRequest) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        report = await runner_module.run_reconcile(SessionLocal, request=request, order_reader=reader)
    return report.as_safe_dict(), _OUTCOME_EXIT_CODES[report.outcome]


async def _run_stage(
    stage: str,
    request: CanaryRequest,
    *,
    plan_digest: str,
    plan_issued_at: datetime | None,
    confirmation_phrase: str,
) -> tuple[dict[str, Any], int]:
    """One stage, with exactly the clients that stage is allowed to use.

    The delivery client is constructed only for the delivery stage, and the
    mutation client only for the three EasyWeek stages: a stage cannot reach a
    surface it has no business touching, even by mistake.
    """
    async with EasyWeekClient() as reader:
        async with SessionLocal() as session:
            if stage == STAGE_DELIVER:
                async with VoucherDeliveryClient() as sender:
                    report = await runner_module.run_deliver(
                        session,
                        SessionLocal,
                        request=request,
                        reader=reader,
                        order_reader=reader,
                        sender=sender,
                        booking_link=_booking_link(request),
                        plan_digest=plan_digest,
                        plan_issued_at=plan_issued_at,
                        confirmation_phrase=confirmation_phrase,
                        apply=True,
                    )
            else:
                handlers = {
                    STAGE_CREATE: runner_module.run_create,
                    STAGE_PAY: runner_module.run_pay,
                    STAGE_REFUND: runner_module.run_refund,
                }
                async with EasyWeekVoucherMutationClient() as mutator:
                    report = await handlers[stage](
                        session,
                        SessionLocal,
                        request=request,
                        reader=reader,
                        order_reader=reader,
                        mutator=mutator,
                        plan_digest=plan_digest,
                        plan_issued_at=plan_issued_at,
                        confirmation_phrase=confirmation_phrase,
                        apply=True,
                    )
    return report.as_safe_dict(), _OUTCOME_EXIT_CODES[report.outcome]


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    _silence_url_logging()
    args = _build_parser().parse_args(argv)
    command = args.command

    if command == COMMAND_STATUS:
        # Answered from the durable ledger alone, before anything else is even
        # looked at. `status` is what an operator reaches for when something is
        # wrong, and a state report that refuses to print because the
        # environment is incomplete is useless exactly then.
        report, code = asyncio.run(_run_status())
        _print_json(report)
        return code

    if command in MUTATION_STAGES:
        # Checked before any client is constructed and before the fence is even
        # consulted: an un-applied stage must not reach the network to read.
        if not args.apply:
            _print_json(_refusal_report(command, [APPLY_FLAG_MISSING]))
            return EXIT_ARGUMENTS
        if not settings.easyweek_voucher_delivery_canary_enabled:
            _print_json(_refusal_report(command, [CANARY_DISABLED]))
            return EXIT_ARGUMENTS
        issued_at = _parse_issued_at(args.plan_issued_at)
        if not args.plan_digest or not args.confirm or issued_at is None:
            _print_json(_refusal_report(command, ["voucher_delivery_plan_authorisation_missing"]))
            return EXIT_ARGUMENTS

    request, refusal = _build_request(args)
    if request is None:
        assert refusal is not None
        _print_json(refusal)
        return EXIT_ARGUMENTS

    try:
        if command == COMMAND_PLAN:
            report, code = asyncio.run(_run_plan(request, args.stage))
        elif command == COMMAND_RECONCILE:
            report, code = asyncio.run(_run_reconcile(request))
        else:
            report, code = asyncio.run(
                _run_stage(
                    command,
                    request,
                    plan_digest=args.plan_digest,
                    plan_issued_at=_parse_issued_at(args.plan_issued_at),
                    confirmation_phrase=args.confirm,
                )
            )
    except EasyWeekConfigError:
        _print_json(_refusal_report(command, ["voucher_delivery_api_unavailable"]))
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
    "EXIT_UNKNOWN",
    "main",
]
