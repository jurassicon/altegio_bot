"""Operator CLI for the production EasyWeek voucher mailing (§42, read-only since §43).

**The mutating stages are closed here.** `freeze`, `create`, `pay`, `deliver` and
`refund` refuse with ``voucher_production_cli_mutation_closed`` and do nothing:
the owner decided on 28.09.2026 that the real mailing is driven from the
interface, and §43 moved the whole operator process into Ops. The subcommands
remain so that an operator who types one gets an explanation instead of an
argparse error — and there is no flag, `--apply` included, that reopens them.

What this command is still for is READING::

    ... easyweek_voucher_production_mailing status
    ... easyweek_voucher_production_mailing status --batch-id B
    ... easyweek_voucher_production_mailing plan --stage pay --preview-run-id N --batch-id B
    ... easyweek_voucher_production_mailing reconcile --preview-run-id N --batch-id B

`status` reads the durable ledger with no HTTP at all; `plan` performs GETs and
writes nothing; `reconcile` reads the outside world back and records what it read.
None of them buys anything or sends anything, and all three matter most exactly
when the acting path is blocked: after an emergency fence close, after a halt, and
while an unknown outcome is being resolved.

The historical contract below still describes how a stage is authorised, because
the UI did not replace those checks — it replaced who carries the approval. See
``docs/easyweek/VOUCHER_PRODUCTION_MAILING_RUNBOOK.md``.

Every command is a deliberate stop::

    ... easyweek_voucher_production_mailing status
    ... easyweek_voucher_production_mailing status --batch-id B
    ... easyweek_voucher_production_mailing plan --stage freeze --preview-run-id N \
            --expected-recipient-count K --approved-exposure-minor M
    ... easyweek_voucher_production_mailing freeze --preview-run-id N \
            --expected-recipient-count K --approved-exposure-minor M \
            --apply --plan-digest D --plan-issued-at T --confirm P
    ... easyweek_voucher_production_mailing plan --stage create --preview-run-id N --batch-id B
    ... easyweek_voucher_production_mailing create --preview-run-id N --batch-id B --apply ...
    ... easyweek_voucher_production_mailing reconcile --preview-run-id N --batch-id B
    ... easyweek_voucher_production_mailing plan --stage pay --preview-run-id N --batch-id B
    ... easyweek_voucher_production_mailing pay --preview-run-id N --batch-id B --apply ...
    ... easyweek_voucher_production_mailing plan --stage deliver --preview-run-id N --batch-id B
    ... easyweek_voucher_production_mailing deliver --preview-run-id N --batch-id B --apply ...
    ... easyweek_voucher_production_mailing plan --stage refund --preview-run-id N --batch-id B --slot K
    ... easyweek_voucher_production_mailing refund --preview-run-id N --batch-id B --slot K --apply ...

There is deliberately NO command that runs two stages in sequence, and there
never may be: a human looking at what the last stage actually did, and deciding
to go on, is the control. The larger the list, the more that matters.

Which batch, always named
-------------------------
Every command after the freeze requires BOTH ``--preview-run-id`` and
``--batch-id``, and refuses unless the batch it finds is the one bound to that
preview. This phase runs again next month and may run twice in a week, so there
is no "current" mailing and nothing here resolves one: a digit slip in either
argument is a refusal, not a stage against the wrong month's audience.

How many, and how much — stated, not inferred
---------------------------------------------
A freeze requires ``--expected-recipient-count`` and
``--approved-exposure-minor``. Both must describe the full active snapshot
exactly: the count must equal the number of active recipients found, and the
exposure must equal that count times €15. A missing or wrong number refuses the
whole freeze; nothing is truncated, nothing is dropped, and this command never
picks a number on an operator's behalf.

There is no ceiling and no ``--limit``. §41's five was a real limit; a flag that
could raise one would not be a limit at all.

What it takes to reach a customer
---------------------------------
A mutation or a send runs only when ALL of these hold at once:

* ``EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED`` is true (false by default);
* the exact subcommand was typed — no default does anything;
* ``--apply`` was passed explicitly;
* ``--batch-id`` names a batch bound to ``--preview-run-id``;
* ``--plan-digest`` matches THIS stage's plan of THIS batch rebuilt live,
  seconds earlier;
* ``--plan-issued-at`` is inside the plan's short maximum age;
* ``--confirm`` is the exact stage phrase that plan printed;
* the durable batch state allows this stage, and the batch is not halted;
* the frozen composition still matches the live one, slot for slot;
* the live guard, the 43/43 baseline, the template, the sender and the key all
  still prove out.

Argparse abbreviation is off, so a half-typed flag authorises nothing, and every
argparse exit — ``--help`` included — returns the argument code rather than the
success code. A mistyped invocation must never be mistaken for a mailing that
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

from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import VoucherDeliveryClient
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as runner_module
from altegio_bot.campaigns.easyweek_voucher_production.composition import BatchApproval
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    ACCOUNT_UNCONFIGURED,
    CLI_MUTATION_CLOSED,
    DATABASE_UNAVAILABLE,
    EXECUTION_INTERRUPTED,
    MUTATION_UNKNOWN,
    PRODUCTION_DISABLED,
    PRODUCTION_STAGES,
    RUNTIME_IDENTITY_UNUSABLE,
    STAFFER_UNCONFIGURED,
    STAGE_CREATE,
    STAGE_DELIVER,
    STAGE_FREEZE,
    STAGE_PAY,
    STAGE_REFUND,
)
from altegio_bot.campaigns.easyweek_voucher_production.runner import ProductionRequest
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
    operator naming recipients directly would be a way to point real vouchers
    at anybody; naming the preview points at rows an operator already curated
    in the editor, and the server re-reads every one of them.
    """
    parser.add_argument("--preview-run-id", type=int, required=True)


def _add_batch_arg(parser: argparse.ArgumentParser, *, required: bool) -> None:
    """Which mailing. Required everywhere except the freeze that creates it.

    There is no default and no "latest": two mailings can be in flight in the
    same week, and a command that guessed between them would be a command that
    could pay for the wrong month's audience.
    """
    parser.add_argument(
        "--batch-id",
        type=int,
        required=required,
        default=None,
        help="The durable id of the mailing. Printed by `status` and by `freeze`.",
    )


def _add_approval_args(parser: argparse.ArgumentParser) -> None:
    """The size and the cost an operator states before freezing.

    Both required, with no defaults. The count must equal the number of active
    recipients in the preview and the exposure must equal count x 1500; either
    being absent or wrong refuses the whole freeze.
    """
    parser.add_argument(
        "--expected-recipient-count",
        type=int,
        required=True,
        help="How many recipients you expect. Must equal the full active snapshot.",
    )
    parser.add_argument(
        "--approved-exposure-minor",
        type=int,
        required=True,
        help="Total exposure in minor units. Must equal --expected-recipient-count x 1500.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = _SafetyArgumentParser(
        prog="easyweek_voucher_production_mailing",
        description=(
            "Operator-only production EasyWeek voucher mailing. One €15 voucher per "
            "manually selected recipient, one confirmed stage at a time — never a campaign."
        ),
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser(COMMAND_PLAN, help="Read-only. Build and print ONE stage's plan.")
    plan_parser.add_argument("--stage", required=True, choices=list(PRODUCTION_STAGES))
    _add_preview_arg(plan_parser)
    _add_batch_arg(plan_parser, required=False)
    plan_parser.add_argument("--slot", type=int, default=None, help="Required for --stage refund.")
    plan_parser.add_argument("--expected-recipient-count", type=int, default=None)
    plan_parser.add_argument("--approved-exposure-minor", type=int, default=None)

    status_parser = subparsers.add_parser(
        COMMAND_STATUS, help="Database only. List the mailings, or print one in full."
    )
    _add_batch_arg(status_parser, required=False)
    status_parser.add_argument("--preview-run-id", type=int, default=None)

    reconcile_parser = subparsers.add_parser(COMMAND_RECONCILE, help="Reads only. Resolve an unknown stage by looking.")
    _add_preview_arg(reconcile_parser)
    _add_batch_arg(reconcile_parser, required=True)

    for stage in PRODUCTION_STAGES:
        stage_parser = subparsers.add_parser(stage, help=f"Perform the {stage} step, after a committed claim.")
        _add_preview_arg(stage_parser)
        # The freeze is the stage that CREATES the id, so it does not take one
        # at all — passing one is an argument error rather than a flag silently
        # ignored, because an operator who typed `--batch-id` on a freeze has
        # misunderstood something and should find that out here. It is instead
        # the one stage that must be told how many people and how much money it
        # is about to commit to.
        if stage == STAGE_FREEZE:
            _add_approval_args(stage_parser)
        else:
            _add_batch_arg(stage_parser, required=True)
        if stage == STAGE_REFUND:
            stage_parser.add_argument(
                "--slot",
                type=int,
                required=True,
                help="Exactly one slot of this batch. There is no refund-everything.",
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


def _approval_from(args: argparse.Namespace) -> BatchApproval:
    """What the operator stated, exactly as they stated it.

    ``None`` stays ``None``: an absent number is a refusal the composition
    reports by name, never something this function fills in.
    """
    return BatchApproval(
        expected_recipient_count=getattr(args, "expected_recipient_count", None),
        approved_exposure_minor=getattr(args, "approved_exposure_minor", None),
    )


def _build_request(args: argparse.Namespace) -> tuple[ProductionRequest | None, dict[str, Any] | None]:
    """The frozen identity for this run, or a refusal naming what is unusable.

    The branch and the template are pinned literals. The staffer and the payment
    account come from this phase's own environment variables — separate from
    §35's, §36's, §37.2's and §41's, so that configuring any canary or the
    controlled batch never configures this mailing.
    """
    staffer = (settings.easyweek_voucher_production_mailing_staffer_uuid or "").strip()
    account = (settings.easyweek_voucher_production_mailing_account_uuid or "").strip()
    reasons: list[str] = []
    if not staffer:
        reasons.append(STAFFER_UNCONFIGURED)
    if not account:
        reasons.append(ACCOUNT_UNCONFIGURED)
    if reasons:
        return None, _refusal_report(args.command, [*reasons, RUNTIME_IDENTITY_UNUSABLE])
    return (
        ProductionRequest(
            preview_run_id=args.preview_run_id,
            sender_code=SENDER_CODE,
            staffer_uuid=staffer,
            payment_account_uuid=account,
            batch_id=getattr(args, "batch_id", None),
        ),
        None,
    )


async def _run_plan(
    request: ProductionRequest, stage: str, slot: int | None, approval: BatchApproval
) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        async with SessionLocal() as session:
            plan, _composition, _prereq, _baseline, _snapshot = await runner_module.build_stage_plan(
                session,
                SessionLocal,
                stage=stage,
                request=request,
                reader=reader,
                order_reader=reader,
                approval=approval,
                slot=slot,
            )
    return plan.as_safe_dict(), (EXIT_OK if plan.ready else EXIT_CONTRACT_MISMATCH)


async def _run_status(batch_id: int | None, preview_run_id: int | None) -> tuple[dict[str, Any], int]:
    report = await runner_module.run_status(SessionLocal, batch_id=batch_id, preview_run_id=preview_run_id)
    return report.as_safe_dict(), EXIT_OK


async def _run_reconcile(request: ProductionRequest) -> tuple[dict[str, Any], int]:
    async with EasyWeekClient() as reader:
        report = await runner_module.run_reconcile(SessionLocal, request=request, order_reader=reader)
    return report.as_safe_dict(), _exit_for(report)


def _exit_for(report: runner_module.StageReport) -> int:
    """The number a wrapper acts on. Never optimistic.

    ``3`` means something may have happened and nobody can say what — including
    a halted batch and a reconcile that resolved nothing. ``6`` is reserved for
    the proven case: the state IS known, no reconciliation is outstanding, and
    an open draft is sitting in the POS waiting for a human. ``0`` means the
    requested stage did what it said, and never anything more than that — in
    particular it never means the messages were delivered or read.
    """
    if report.outcome == "unknown" or report.halted or report.reconciliation_required:
        return EXIT_UNKNOWN
    if report.outcome in ("refused", "rejected", "partial"):
        return EXIT_CONTRACT_MISMATCH
    if report.manual_cleanup_required:
        return EXIT_MANUAL_CLEANUP
    return EXIT_OK


async def _run_stage(request: ProductionRequest, args: argparse.Namespace) -> tuple[dict[str, Any], int]:
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
                report = await runner_module.run_freeze(session, SessionLocal, approval=_approval_from(args), **common)
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
    # mailing left behind and whether drafts are still open in the POS.
    # Refusing to answer that would not be safety; it would be an operator
    # running SQL by hand instead.
    if args.command == COMMAND_STATUS:
        return await _run_status(args.batch_id, args.preview_run_id)

    # §43.6: the mutating stages are not things this command does any more, and
    # that is answered BEFORE the fence.
    #
    # The order matters for one practical reason (review R4): a post-deploy smoke
    # runs with the fence CLOSED, and with the fence checked first the answer was
    # `voucher_production_disabled` — which proves the fence works and says nothing
    # about whether the CLI mutation path is shut. An administrator checking the
    # closure would have been reading the wrong refusal. Whether the CLI may mutate
    # is a property of the command, not of the deployment's fence, so it is the
    # answer regardless of either.
    #
    # Both are refusals with zero external effects, so nothing is weakened by
    # choosing which one speaks first.
    if args.command in _CLOSED_CLI_STAGES:
        # There is deliberately no flag that reopens this. `--apply` does not, and
        # adding a second one would simply recreate what §43 closed. The internal
        # executor is not a counter-example: it runs a stored, authorised UI action,
        # never a command somebody typed.
        return _refusal_report(args.command, [CLI_MUTATION_CLOSED]), EXIT_CONTRACT_MISMATCH

    # Everything else is behind the fence, before anything opens a socket or a
    # session — the read-only plan included. A closed fence means the command
    # does nothing at all, not "nothing that writes".
    if not settings.easyweek_voucher_production_mailing_enabled:
        return _refusal_report(args.command, [PRODUCTION_DISABLED]), EXIT_CONTRACT_MISMATCH

    request, refusal = _build_request(args)
    if refusal is not None or request is None:
        return refusal or _refusal_report(args.command, [RUNTIME_IDENTITY_UNUSABLE]), EXIT_CONTRACT_MISMATCH

    if args.command == COMMAND_PLAN:
        return await _run_plan(request, args.stage, args.slot, _approval_from(args))
    if args.command == COMMAND_RECONCILE:
        return await _run_reconcile(request)

    # Unreachable: every mutating command was answered above, and `status`, `plan`
    # and `reconcile` all returned. Kept as a fail-closed floor rather than an
    # assertion — a future subcommand that forgets to classify itself refuses
    # instead of falling through to something that acts.
    return (
        _refusal_report(args.command, [CLI_MUTATION_CLOSED]),
        EXIT_CONTRACT_MISMATCH,
    )


# The commands that can reach EasyWeek or Meta. A failure inside one of these
# cannot be reported as "nothing happened", because by the time anything can go
# wrong the durable claim is already committed and the request may already have
# left. Everything else — `status`, `plan`, `reconcile` and `freeze` — either
# reads or writes locally; none of them constructs a mutation transport at all,
# so their failure provably started no external effect.
_EFFECTFUL_COMMANDS: Final = frozenset({STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND})

# The five §42 stage commands §43 closed. Named as a set so the dispatch answers
# them in one place, before the fence — see `_dispatch`.
_CLOSED_CLI_STAGES: Final = frozenset({STAGE_FREEZE, STAGE_CREATE, STAGE_PAY, STAGE_DELIVER, STAGE_REFUND})

# Everything that writes durable state, which is the set worth taking a
# before-snapshot of. `freeze` reaches nobody, but it does create the batch,
# and a freeze that committed and then failed to print must not read as though
# no batch exists.
_DURABLE_COMMANDS: Final = _EFFECTFUL_COMMANDS | {STAGE_FREEZE}


async def _durable_batch(args: argparse.Namespace) -> dict[str, Any] | None:
    """This invocation's batch as the database currently reads it, or ``None``.

    Answers an operator's real question — what does the ledger say NOW — rather
    than repeating what the crashed command believed a moment ago. Fails
    silently to ``None``: a second exception here must not replace the first
    report with a worse one.

    Resolved by batch id when the command had one and by preview otherwise,
    which is what lets a crashed FREEZE be compared against the batch it may
    have just created.

    Awaited, never ``asyncio.run``. See :func:`_run_command`: every database
    read of one invocation has to happen in the SAME event loop, because the
    connections behind ``SessionLocal`` are pooled and a pooled asyncpg
    connection belongs to the loop that created it.
    """
    try:
        report = await runner_module.run_status(
            SessionLocal,
            batch_id=getattr(args, "batch_id", None),
            preview_run_id=getattr(args, "preview_run_id", None),
        )
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


def _fail_closed(
    command: str,
    *,
    outcome: str,
    reasons: list[str],
    batch: dict[str, Any] | None,
    external_effect_attempted: bool,
) -> tuple[dict[str, Any], int]:
    """One non-success report, built from the durable ledger, exit 3.

    ``halted`` and ``reconciliation_required`` come from what the ledger
    actually says rather than being forced true. When an outcome is already
    proven and written down, claiming the batch is halted would be inventing a
    state the database does not hold — and an operator who then found it
    running would trust the next report less.
    """
    report = runner_module.StageReport(
        stage=command,
        outcome=outcome,
        reasons=reasons,
        external_effect_attempted=external_effect_attempted,
        external_send_attempted=external_effect_attempted and command == STAGE_DELIVER,
        reconciliation_required=bool(batch.get("reconciliation_required")) if batch is not None else True,
        halted=bool(batch.get("halted")) if batch is not None else True,
        batch=batch or {},
    )
    return report.as_safe_dict(), EXIT_UNKNOWN


async def _unexpected_failure(
    args: argparse.Namespace,
    *,
    before: dict[str, Any] | None,
    reason: str = DATABASE_UNAVAILABLE,
) -> tuple[dict[str, Any], int]:
    """What to print when a command died somewhere nobody planned for.

    Exit 4 is a promise that nothing started, and it may only be made when it
    is provable FOR THIS INVOCATION.

    Why the ledger alone cannot prove it
    ------------------------------------
    Asking "does the ledger hold an unresolved slot?" and reading no as proof
    that nothing happened is simply false in a multi-slot stage: slot 1 can
    create a voucher, be recorded as ``created`` — a proven, resolved, terminal
    state — and the command can then die before it claims slot 2. The ledger
    holds nothing unresolved, and the operator would have been told `refused`,
    `external_effect_attempted=false`, exit 4, over a €15 order that exists.
    The more slots a mailing has, the more likely that shape becomes.

    The evidence has to belong to this invocation, so it is a before/after
    comparison of the batch taken around the command. Every external request in
    this phase is preceded by a committed claim, and a committed claim always
    moves a slot's status — so a ledger that is byte-identical afterwards is
    proof that THIS command claimed nothing and therefore sent nothing.

    Three answers, and only one of them is exit 4:

    * **unresolved slot** — a request may be in flight right now. Unknown.
    * **the ledger moved** — this invocation did something and then stopped
      part-way. Interrupted: what happened is on the record, what did not is a
      question for a human with a fresh plan.
    * **nothing moved, and the ledger answered** — provably pre-claim. Refused.

    Both readings are required. An unreadable ledger afterwards proves nothing,
    and a baseline that could not be taken before the command ran is just as
    disqualifying — "unchanged" is a comparison, and there is nothing to
    compare against. Either way a durable command falls back to unknown rather
    than to a refusal.

    A concurrent command that moved the ledger while this one failed early
    would be read as "interrupted" here. That is the conservative direction and
    deliberately so: the cost is one extra look by a human, and the cost of the
    other mistake is an operator who believes no voucher was issued.

    Nothing here prints a traceback, an exception message, SQL, a URL, a
    customer UUID, a phone number or a voucher code.
    """
    command = args.command
    after = await _durable_batch(args)
    effectful = command in _EFFECTFUL_COMMANDS

    # A slot is sitting claimed or unknown: something may be in flight.
    if _has_unresolved_slot(after):
        return _fail_closed(
            command,
            outcome="unknown",
            reasons=[MUTATION_UNKNOWN] if after is not None else [MUTATION_UNKNOWN, DATABASE_UNAVAILABLE],
            batch=after,
            external_effect_attempted=True,
        )

    # This invocation moved the ledger and then stopped. For an effectful
    # command that means a CREATE, a PAY, a REFUND or a Meta POST of its own
    # has already been proven and written down.
    if before is not None and after is not None and before != after:
        return _fail_closed(
            command,
            outcome="interrupted",
            reasons=[EXECUTION_INTERRUPTED],
            batch=after,
            # A freeze writes durably and reaches nobody: it never constructs a
            # mutation transport, so `false` here is provable rather than
            # hopeful.
            external_effect_attempted=effectful,
        )

    # Nothing is provable without both readings. A ledger that will not answer
    # afterwards says nothing; a baseline that could not be taken BEFORE the
    # command ran is just as disqualifying, because "unchanged" can only be
    # asserted against something. Both fail closed rather than quietly falling
    # through to a refusal.
    if command in _DURABLE_COMMANDS and (after is None or before is None):
        return _fail_closed(
            command,
            outcome="unknown",
            reasons=[MUTATION_UNKNOWN, DATABASE_UNAVAILABLE],
            batch=after,
            # A freeze reaches nobody whatever happened to the ledger.
            external_effect_attempted=effectful,
        )

    # Proven pre-claim: this command claimed nothing, so it sent nothing.
    report = runner_module.StageReport(
        stage=command,
        outcome="refused",
        reasons=[reason],
        external_effect_attempted=False,
        reconciliation_required=bool(after.get("reconciliation_required")) if after is not None else False,
        halted=bool(after.get("halted")) if after is not None else False,
        batch=after or {},
    )
    payload = report.as_safe_dict()
    # A batch that already needed a human before this command ran still does.
    code = EXIT_UNKNOWN if (report.halted or report.reconciliation_required) else EXIT_CONTRACT_MISMATCH
    return payload, code


async def _run_command(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    """The whole life of one invocation, inside ONE event loop.

    Why this function exists at all
    -------------------------------
    ``SessionLocal`` is the production engine, and it pools. A pooled asyncpg
    connection belongs to the event loop that opened it: hand it to a second
    loop and the checkout fails with *Event loop is closed* or *got Future
    attached to a different loop*. Running the baseline read, the command and
    the failure read in three separate ``asyncio.run`` calls would be three
    loops over one pool — the first read poisons it for everything after, and
    the broad catch below then turns a perfectly healthy database into one
    reported as unreadable.

    So the async lifecycle is not split. The baseline, the dispatch and both
    failure paths are awaited here, and the only ``asyncio.run`` in this module
    is the one line in :func:`main` that enters it.
    """
    # The ledger BEFORE this command touches anything, for the commands that
    # can write durably. It is the only way a failure handler can tell what
    # this invocation did from what earlier ones left behind.
    before = await _durable_batch(args) if args.command in _DURABLE_COMMANDS else None
    try:
        return await _dispatch(args)
    except EasyWeekConfigError:
        # A deployment fault, raised while building the transport — before any
        # claim and before any request. Named, without the configuration it is
        # complaining about, and still checked against the evidence so that a
        # misconfiguration discovered late cannot report a clean refusal over
        # work this command already did.
        return await _unexpected_failure(args, before=before, reason=RUNTIME_IDENTITY_UNUSABLE)
    except Exception:  # noqa: BLE001 - a stable, PII-free surface for operators
        # Deliberately no traceback, no exception text and no SQL: an operator
        # report is pasted into tickets, and an exception string here could
        # carry a URL with a customer UUID in it.
        return await _unexpected_failure(args, before=before)


def main(argv: list[str] | None = None) -> int:
    """The sync entry point: parse, run one loop, print, exit.

    Exactly one ``asyncio.run``. One CLI process is one invocation is one event
    loop, which is the boundary the pooled database engine requires.
    """
    redact_easyweek_url_logging()
    args = _build_parser().parse_args(argv)
    payload, code = asyncio.run(_run_command(args))
    _print_json(payload)
    return code


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(main())
