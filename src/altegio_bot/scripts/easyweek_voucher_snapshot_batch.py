"""Operator CLI for the controlled EasyWeek voucher snapshot batch (§41).

Every command is a deliberate stop::

    ... easyweek_voucher_snapshot_batch status
    ... easyweek_voucher_snapshot_batch plan --stage freeze  --preview-run-id N
    ... easyweek_voucher_snapshot_batch freeze  --apply --plan-digest D --plan-issued-at T --confirm P ...
    ... easyweek_voucher_snapshot_batch plan --stage create  ...
    ... easyweek_voucher_snapshot_batch create  --apply ...
    ... easyweek_voucher_snapshot_batch reconcile ...
    ... easyweek_voucher_snapshot_batch plan --stage pay     ...
    ... easyweek_voucher_snapshot_batch pay     --apply ...
    ... easyweek_voucher_snapshot_batch plan --stage deliver ...
    ... easyweek_voucher_snapshot_batch deliver --apply ...
    ... easyweek_voucher_snapshot_batch plan --stage refund  --slot K ...
    ... easyweek_voucher_snapshot_batch refund  --slot K --apply ...

There is deliberately NO command that runs two stages in sequence, and there
never may be: a human looking at what the last stage actually did, and deciding
to go on, is the control. That is true of a batch exactly as it was of one
recipient — more so, because a mistake here costs up to five times as much.

Bounds this command cannot exceed
---------------------------------
At most five recipients. At most €15 each. At most €75 in total. At most one
batch, ever. None of it is a flag: the limits are literals in the code and CHECK
constraints in the database, and raising one is a migration plus a review.

What it takes to reach a customer
---------------------------------
A mutation or a send runs only when ALL of these hold at once:

* ``EASYWEEK_VOUCHER_SNAPSHOT_BATCH_ENABLED`` is true (false by default);
* the exact subcommand was typed — no default does anything;
* ``--apply`` was passed explicitly;
* ``--plan-digest`` matches THIS stage's plan rebuilt live, seconds earlier;
* ``--plan-issued-at`` is inside the plan's short maximum age;
* ``--confirm`` is the exact stage phrase that plan printed;
* the durable batch state allows this stage, and the batch is not halted;
* the frozen composition still matches the live one, slot for slot;
* the live guard, the 42/42 baseline, the template, the sender and the key all
  still prove out.

Argparse abbreviation is off, so a half-typed flag authorises nothing, and every
argparse exit — ``--help`` included — returns the argument code rather than the
success code. A mistyped invocation must never be mistaken for a batch that
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

from altegio_bot.campaigns.easyweek_voucher_batch import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_batch import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    ACCOUNT_UNCONFIGURED,
    BATCH_DISABLED,
    BATCH_STAGES,
    DATABASE_UNAVAILABLE,
    MUTATION_UNKNOWN,
    RUNTIME_IDENTITY_UNUSABLE,
    STAFFER_UNCONFIGURED,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
)
from altegio_bot.campaigns.easyweek_voucher_batch.runner import BatchRequest
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


def _add_preview_arg(parser: argparse.ArgumentParser) -> None:
    """The exact preview, and nothing looser.

    Never a phone number, a name, a customer UUID or a list of people. An
    operator naming recipients directly would be a way to point five real
    vouchers at anybody; naming the preview points at rows an operator already
    curated in the editor, and the server re-reads every one of them.
    """
    parser.add_argument("--preview-run-id", type=int, required=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafetyArgumentParser(
        prog="easyweek_voucher_snapshot_batch",
        description=(
            "Operator-only controlled EasyWeek voucher snapshot batch. At most five "
            "manually selected recipients, at most €15 each, at most €75 — never a campaign."
        ),
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser(COMMAND_PLAN, help="Read-only. Build and print ONE stage's plan.")
    plan_parser.add_argument("--stage", required=True, choices=list(BATCH_STAGES))
    _add_preview_arg(plan_parser)
    plan_parser.add_argument("--slot", type=int, default=None, help="Required for --stage refund.")

    subparsers.add_parser(COMMAND_STATUS, help="Database only. Print the durable batch state.")

    reconcile_parser = subparsers.add_parser(COMMAND_RECONCILE, help="Reads only. Resolve an unknown stage by looking.")
    _add_preview_arg(reconcile_parser)

    for stage in BATCH_STAGES:
        stage_parser = subparsers.add_parser(stage, help=f"Perform the {stage} step, after a committed claim.")
        _add_preview_arg(stage_parser)
        if stage == STAGE_REFUND:
            stage_parser.add_argument(
                "--slot",
                type=int,
                required=True,
                help="Exactly one slot. There is no refund-everything.",
            )
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


def _build_request(args: argparse.Namespace) -> tuple[BatchRequest | None, dict[str, Any] | None]:
    """The frozen identity for this run, or a refusal naming what is unusable.

    The branch and the template are pinned literals. The staffer and the payment
    account come from this phase's own environment variables — separate from
    §35's, §36's and §37.2's, so that configuring one canary never configures
    this batch.
    """
    staffer = (settings.easyweek_voucher_snapshot_batch_staffer_uuid or "").strip()
    account = (settings.easyweek_voucher_snapshot_batch_account_uuid or "").strip()
    reasons: list[str] = []
    if not staffer:
        reasons.append(STAFFER_UNCONFIGURED)
    if not account:
        reasons.append(ACCOUNT_UNCONFIGURED)
    if reasons:
        return None, _refusal_report(args.command, [*reasons, RUNTIME_IDENTITY_UNUSABLE])
    return (
        BatchRequest(
            preview_run_id=args.preview_run_id,
            sender_code=SENDER_CODE,
            staffer_uuid=staffer,
            payment_account_uuid=account,
        ),
        None,
    )


async def _run_plan(request: BatchRequest, stage: str, slot: int | None) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        async with SessionLocal() as session:
            plan, _composition, _prereq, _baseline = await runner_module.build_stage_plan(
                session,
                SessionLocal,
                stage=stage,
                request=request,
                reader=reader,
                order_reader=reader,
                slot=slot,
            )
    return plan.as_safe_dict(), (EXIT_OK if plan.ready else EXIT_CONTRACT_MISMATCH)


async def _run_status() -> tuple[dict[str, Any], int]:
    report = await runner_module.run_status(SessionLocal)
    return report.as_safe_dict(), EXIT_OK


async def _run_reconcile(request: BatchRequest) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        report = await runner_module.run_reconcile(SessionLocal, request=request, order_reader=reader)
    return report.as_safe_dict(), _exit_for(report)


def _exit_for(report: runner_module.StageReport) -> int:
    """The number a wrapper acts on. Never optimistic.

    ``3`` means something may have happened and nobody can say what — including
    a halted batch and a reconcile that resolved nothing. ``6`` is reserved for
    the proven case: the state IS known, no reconciliation is outstanding, and
    an open draft is sitting in the POS waiting for a human. ``0`` means the
    requested stage did what it said, and never anything more than that.
    """
    if report.outcome == "unknown" or report.halted or report.reconciliation_required:
        return EXIT_UNKNOWN
    if report.outcome in ("refused", "rejected", "partial"):
        return EXIT_CONTRACT_MISMATCH
    if report.manual_cleanup_required:
        return EXIT_MANUAL_CLEANUP
    return EXIT_OK


async def _run_stage(request: BatchRequest, args: argparse.Namespace) -> tuple[dict[str, Any], int]:
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
            if stage == STAGE_FREEZE:
                # Local only. No mutation transport is opened at all, because
                # there is no external effect for one to carry.
                report = await runner_module.run_freeze(session, SessionLocal, **common)
            elif stage == STAGE_DELIVER:
                async with VoucherDeliveryClient() as sender:
                    report = await runner_module.run_deliver(session, SessionLocal, sender=sender, **common)
            else:
                async with EasyWeekVoucherMutationClient() as mutator:
                    if stage == STAGE_CREATE:
                        report = await runner_module.run_create(session, SessionLocal, mutator=mutator, **common)
                    elif stage == STAGE_PAY:
                        report = await runner_module.run_pay(session, SessionLocal, mutator=mutator, **common)
                    else:
                        report = await runner_module.run_refund(
                            session, SessionLocal, mutator=mutator, slot=int(args.slot), **common
                        )
    return report.as_safe_dict(), _exit_for(report)


async def _dispatch(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    # `status` is answered BEFORE the fence, and only `status`.
    #
    # It reads the durable ledger and nothing else: no HTTP, no approval, no
    # mutation, no transport constructed. Putting it behind the fence would
    # make the state unreadable at exactly the moment an operator needs it
    # most — after an emergency `false`, when the question is what a halted
    # batch left behind and whether a draft is still open in the POS. Refusing
    # to answer that would not be safety; it would be an operator running SQL
    # by hand instead.
    if args.command == COMMAND_STATUS:
        return await _run_status()

    # Everything else is behind the fence, before anything opens a socket or a
    # session — the read-only plan included. A closed fence means the command
    # does nothing at all, not "nothing that writes".
    if not settings.easyweek_voucher_snapshot_batch_enabled:
        return _refusal_report(args.command, [BATCH_DISABLED]), EXIT_CONTRACT_MISMATCH

    request, refusal = _build_request(args)
    if refusal is not None or request is None:
        return refusal or _refusal_report(args.command, [RUNTIME_IDENTITY_UNUSABLE]), EXIT_CONTRACT_MISMATCH

    if args.command == COMMAND_PLAN:
        return await _run_plan(request, args.stage, args.slot)
    if args.command == COMMAND_RECONCILE:
        return await _run_reconcile(request)
    return await _run_stage(request, args)


# The commands that can reach EasyWeek or Meta. A failure inside one of these
# cannot be reported as "nothing happened", because by the time anything can go
# wrong the durable claim is already committed and the request may already have
# left. Everything else — `status`, `plan`, `reconcile` and `freeze` — either
# reads or writes locally; none of them constructs a mutation transport at all,
# so their failure provably started no external effect.
_EFFECTFUL_COMMANDS: Final = frozenset({STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND})


def _durable_batch() -> dict[str, Any] | None:
    """The batch as the database currently reads it, or ``None`` if unreadable.

    Used only on the failure path, to answer an operator's real question — what
    does the ledger say NOW — rather than repeating what the crashed command
    believed a moment ago. Fails silently to ``None``: a second exception here
    must not replace the first report with a worse one.
    """
    try:
        report = asyncio.run(runner_module.run_status(SessionLocal))
    except Exception:  # noqa: BLE001 - an unreadable database is itself the answer
        return None
    return report.as_safe_dict().get("batch")


def _has_unresolved_slot(batch: dict[str, Any] | None) -> bool:
    """Does the ledger hold a slot whose request may have left this process?

    The claim is committed before the request goes out, so an unresolved slot is
    the durable trace of a possible external effect — and the absence of one,
    on a ledger that still answers, is the only thing that makes "nothing
    started" a fact rather than a hope.
    """
    if batch is None:
        return False
    return any(str(item.get("status")) in ledger_module.UNRESOLVED_ITEM_STATUSES for item in (batch.get("items") or []))


def _unexpected_failure(command: str) -> tuple[dict[str, Any], int]:
    """What to print when a mutating stage died somewhere nobody planned for.

    The old behaviour was the dangerous one: a blanket handler that answered
    `refused`, `external_effect_attempted=false` and exit 4 for ANY exception —
    including one raised after a voucher had been created, after €15 had been
    charged, or after Meta had accepted a message. Exit 4 is a promise that
    nothing started. That promise may only be made when it is provable.

    What makes "nothing started" PROVABLE is the invariant the whole phase is
    built on: a request only ever leaves after its claim is committed. So a
    ledger that still answers, and that holds no slot in an unresolved state,
    is proof that no CREATE, PAY, REFUND or Meta POST was made by the command
    that just died — and only then is exit 4 honest. A ledger that cannot be
    read proves nothing at all, and neither does one holding a claim.

    Nothing here prints a traceback, an exception message, SQL, a URL, a
    customer UUID, a phone number or a voucher code.
    """
    batch = _durable_batch()
    claimed = _has_unresolved_slot(batch)

    if command not in _EFFECTFUL_COMMANDS or (batch is not None and not claimed):
        payload = _refusal_report(command, [DATABASE_UNAVAILABLE])
        if batch is not None:
            # Still worth showing: a freeze that committed and then failed to
            # print must not read as though no batch exists.
            payload["batch"] = batch
        return payload, EXIT_CONTRACT_MISMATCH

    reasons = [MUTATION_UNKNOWN] if batch is not None else [MUTATION_UNKNOWN, DATABASE_UNAVAILABLE]
    report = runner_module.StageReport(
        stage=command,
        outcome="unknown",
        reasons=reasons,
        # Not proven to have happened — and, crucially, not proven not to.
        external_effect_attempted=True,
        external_send_attempted=command == STAGE_DELIVER,
        reconciliation_required=True,
        # The claim halts the batch in its own transaction, so a readable
        # ledger says so itself. When it is unreadable, the conservative
        # reading is the only safe one, and `DATABASE_UNAVAILABLE` above says
        # which case this is.
        halted=bool(batch.get("halted")) if batch is not None else True,
        batch=batch or {},
    )
    return report.as_safe_dict(), EXIT_UNKNOWN


def main(argv: list[str] | None = None) -> int:
    redact_easyweek_url_logging()
    args = _build_parser().parse_args(argv)
    try:
        payload, code = asyncio.run(_dispatch(args))
    except EasyWeekConfigError:
        # A deployment fault, and one raised while building the transport —
        # before any claim and before any request. Named, without the
        # configuration it is complaining about.
        payload, code = _refusal_report(args.command, [RUNTIME_IDENTITY_UNUSABLE]), EXIT_CONTRACT_MISMATCH
    except Exception:  # noqa: BLE001 - a stable, PII-free surface for operators
        # Deliberately no traceback, no exception text and no SQL: an operator
        # report is pasted into tickets, and an exception string here could
        # carry a URL with a customer UUID in it.
        payload, code = _unexpected_failure(args.command)
    _print_json(payload)
    return code


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(main())
