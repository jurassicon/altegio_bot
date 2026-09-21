"""Plan/apply/verify CLI for the operator-only failed-cancellation recovery.

    python -m altegio_bot.scripts.easyweek_failed_cancellation_recovery <mode> --event-id N

Modes::

    plan     prove the named events against the database and a real EasyWeek
             GET, and freeze a private 0600 snapshot. Writes nothing.
    apply    ONE PostgreSQL transaction against that exact snapshot: mark the
             historical cancellation, withdraw the record's queued EasyWeek
             reminders and terminalize the event. Re-proves the live state
             first; creates no job and no OutboxMessage.
    verify   read the end state back and prove it. Read-only.

**Explicit rows only.** There is no ``--all``, no scan and no automatic run:
``--event-id`` is required, repeatable and bounded. A general replay of failed
events is forbidden by the plan, and this tool is deliberately not one.

**Read-only by default.** ``plan`` is the default mode. ``apply`` needs the
mode, the ``--apply`` flag, the plan digest, the exact confirmation phrase AND
the host's environment authorisation. Any one of them alone is a refusal.

What this tool never does
-------------------------
It never calls Meta or Chatwoot, never calls an EasyWeek mutation endpoint,
never writes an ``OutboxMessage``, never creates a ``MessageJob`` of any type,
never re-opens a ``done``/``failed``/``canceled`` job, and never touches an
Altegio ``Record`` or an Altegio job — the reminder handover owns that half.

Exit codes::

    0  the mode completed and its report is clean
    1  the mode refused, or something still needs a person
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_failed_cancellation_recovery import (
    MAX_EVENT_IDS,
    RecoveryError,
    apply_recovery_plan,
    build_recovery_plan,
    check_apply_authorization,
    confirmation_phrase,
    read_apply_report,
    read_plan,
    validate_event_ids,
    verify_recovery,
    write_plan,
)
from altegio_bot.easyweek_multi_service_recovery import (
    DEFAULT_MAX_SNAPSHOT_AGE_SEC,
    DEFAULT_PAUSE_SEC,
    MODE_APPLY,
    MODE_PLAN,
    MODE_VERIFY,
    write_private_json,
)

logger = logging.getLogger("easyweek.failed_cancellation_recovery.cli")

MODES: Final = (MODE_PLAN, MODE_APPLY, MODE_VERIFY)
PROG: Final = "easyweek_failed_cancellation_recovery"

DEFAULT_PLAN: Final = (
    os.environ.get("EASYWEEK_FAILED_CANCELLATION_RECOVERY_PLAN") or "/recovery/easyweek-failed-cancellation-plan.json"
)
DEFAULT_APPLY_REPORT: Final = (
    os.environ.get("EASYWEEK_FAILED_CANCELLATION_RECOVERY_APPLY_REPORT")
    or "/recovery/easyweek-failed-cancellation-apply.json"
)

# The host-side half of the permission. Checked in addition to the typed flag,
# the digest and the phrase: the flag proves somebody meant it now, this proves
# the host is one where a historical write is allowed at all.
APPLY_ENV_FLAG: Final = "EASYWEEK_FAILED_CANCELLATION_RECOVERY_ALLOW_APPLY"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "Recover explicitly named EasyWeek booking-canceled events that failed with "
            "invalid_payload because the delivery carried service_id: null."
        ),
        allow_abbrev=False,
    )
    parser.add_argument("mode", nargs="?", default=MODE_PLAN, choices=MODES)
    parser.add_argument(
        "--event-id",
        action="append",
        type=int,
        default=[],
        required=True,
        metavar="EASYWEEK_EVENT_ID",
        help=(
            "exact easyweek_events.id to consider. Repeatable and required; nothing is in scope by "
            f"default and at most {MAX_EVENT_IDS} may be named. There is deliberately no --all."
        ),
    )
    parser.add_argument(
        "--plan",
        default=DEFAULT_PLAN,
        help="where the frozen plan lives. Holds booking UUIDs and technical ids; written 0600, never committed.",
    )
    parser.add_argument(
        "--apply-report",
        default=DEFAULT_APPLY_REPORT,
        help="durable PII-free apply evidence. Required by verify and written 0600 after a committed apply.",
    )
    parser.add_argument(
        "--pause-sec",
        type=float,
        default=DEFAULT_PAUSE_SEC,
        help=f"pause between sequential EasyWeek reads (default {DEFAULT_PAUSE_SEC}); the API allows 60/min.",
    )
    parser.add_argument(
        "--max-plan-age-sec",
        type=int,
        default=DEFAULT_MAX_SNAPSHOT_AGE_SEC,
        help=(
            f"how old a plan may be at apply time (default {DEFAULT_MAX_SNAPSHOT_AGE_SEC}s). "
            "A booking can be un-cancelled or moved, so an old plan describes a different world."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write. Without it, apply mode refuses. Never enough on its own.",
    )
    parser.add_argument("--plan-digest", help="the plan_digest printed by the plan run this authorises")
    parser.add_argument(
        "--confirm",
        help=(
            "the exact confirmation phrase for this plan: "
            f"'{confirmation_phrase('<PLAN_DIGEST>')}'. It carries the digest, so a phrase copied "
            "out of an earlier terminal cannot authorise today's plan."
        ),
    )
    return parser


def _fail(message: str) -> int:
    print(f"{PROG}: refused: {message}", file=sys.stderr)
    return 1


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _apply_permitted(args: argparse.Namespace) -> bool:
    """Both halves, or nothing. Neither one alone is authorisation."""
    env = (os.environ.get(APPLY_ENV_FLAG) or "").strip().lower()
    return bool(args.apply) and env == "true"


async def _run(args: argparse.Namespace) -> int:
    if not math.isfinite(args.pause_sec) or args.pause_sec < DEFAULT_PAUSE_SEC:
        return _fail("api_pacing_invalid")
    if not 1 <= args.max_plan_age_sec <= DEFAULT_MAX_SNAPSHOT_AGE_SEC:
        return _fail("plan_age_limit_invalid")
    try:
        event_ids = validate_event_ids(args.event_id)
    except RecoveryError as error:
        return _fail(str(error))

    if args.mode == MODE_PLAN:
        client = EasyWeekClient(max_attempts=1)
        try:
            async with SessionLocal() as session:
                # Belt to the module's own rollback: the database itself
                # refuses a write from this connection.
                await session.execute(text("SET TRANSACTION READ ONLY"))
                plan = await build_recovery_plan(
                    session,
                    event_ids=event_ids,
                    client=client,
                    pause_sec=args.pause_sec,
                )
        except RecoveryError as error:
            return _fail(str(error))
        finally:
            await client.aclose()

        report = plan.as_safe_dict()
        path = write_plan(plan, args.plan)
        _print(report)
        print(f"{PROG}: plan written to {path}", file=sys.stderr)
        if plan.apply_ready:
            print(
                "to apply this plan, pass\n"
                f"  --plan-digest {report['plan_digest']}\n"
                f"  --confirm '{confirmation_phrase(report['plan_digest'])}'\n"
                f"  --apply, with {APPLY_ENV_FLAG}=true in the one-off container",
                file=sys.stderr,
            )
            return 0
        blockers = ", ".join(plan.blockers) or "plan_not_apply_ready"
        print(f"{PROG}: this plan cannot be applied ({blockers}); resolve it and run plan again", file=sys.stderr)
        return 1

    try:
        frozen = read_plan(args.plan)
    except RecoveryError as error:
        return _fail(str(error))
    if frozen.event_ids != event_ids:
        return _fail("the plan was frozen for a different set of events; re-run plan")

    if args.mode == MODE_VERIFY:
        try:
            apply_report = read_apply_report(args.apply_report, frozen=frozen)
        except RecoveryError as error:
            return _fail(str(error))
        async with SessionLocal() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                report = await verify_recovery(session, frozen=frozen, apply_report=apply_report)
        _print(report)
        return 0 if report["passed"] else 1

    # -- apply ---------------------------------------------------------------
    if not _apply_permitted(args):
        return _fail(
            f"apply needs BOTH --apply and {APPLY_ENV_FLAG}=true. "
            "This permission does not allow sending a message or replaying any other event."
        )
    now = datetime.now(timezone.utc)
    try:
        check_apply_authorization(
            frozen,
            supplied_digest=args.plan_digest,
            supplied_confirmation=args.confirm,
            now=now,
            max_age_sec=args.max_plan_age_sec,
        )
    except RecoveryError as error:
        return _fail(str(error))

    client = EasyWeekClient(max_attempts=1)
    try:
        async with SessionLocal() as session:
            result = await apply_recovery_plan(
                session,
                frozen=frozen,
                client=client,
                pause_sec=args.pause_sec,
            )
    except RecoveryError as error:
        return _fail(f"{error}; nothing was changed")
    finally:
        await client.aclose()

    durable = result.report()
    _print(durable)
    path = write_private_json(durable, args.apply_report)
    print(f"{PROG}: apply report written to {path}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    print(
        f"{PROG}: plan path {args.plan} names real bookings; it is not committed and must not be shared",
        file=sys.stderr,
    )
    try:
        return asyncio.run(_run(args))
    except RecoveryError as error:
        return _fail(str(error))
    except SQLAlchemyError:
        return _fail("database_error")
    except OSError:
        return _fail("private_artifact_io_error")
    except Exception:
        logger.exception("unexpected failure")
        return _fail("recovery_unexpected_error")


if __name__ == "__main__":
    raise SystemExit(main())
