"""Align the §36 voucher template row with what Meta actually approved.

The runtime guard compares the DATABASE ROW against the source contract and
never reads Meta, so a row matching this repository could still differ from the
approved text a customer would receive. This command is the part that closes
that gap for ONE code, on ONE branch, and does nothing else.

What it proves before it writes anything
----------------------------------------
Exactly one Meta template with the expected name and language, APPROVED,
MARKETING, positional, BODY-only, a body equal to the source-owned contract
after the ``{{n}}`` conversion, and exactly three parameters. Anything else —
missing, pending, rejected, duplicated, a HEADER we do not render, a FOOTER
nobody reviewed here — blocks the write rather than relaxing a check.

What it refuses to do
---------------------
It creates no Meta template: approval is a human action in the Business Manager,
and a command that could request one would make this repository the author of
text nobody reviewed. It sends no message, touches no job, outbox row, sender,
feature flag or environment, and it never writes a row for another branch or
another code.

A green audit is not permission to send. Eligibility, the fence, the key and the
operator's per-stage approvals are all proven separately.

Dry-run is the default; ``--apply`` is the only way to reach a write.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate
from altegio_bot.scripts.clone_meta_templates_for_location import MetaTemplateClient, ScriptError
from altegio_bot.settings import settings

EXIT_OK: Final = 0
EXIT_BLOCKED: Final = 1
EXIT_ARGUMENTS: Final = 2

# The two blockers this command reasons about by name. Every other blocker is
# something a single UPDATE of one existing row fixes, or something a human
# fixes — never an INSERT.
BLOCKER_ROW_MISSING: Final = "row_missing"
BLOCKER_MULTIPLE_ROWS: Final = "multiple_rows_for_one_code"

# What --apply would do about a blocker.
REPAIR_INSERT: Final = "insert"
REPAIR_UPDATE: Final = "update"
REPAIR_NONE: Final = "none"

# What an operator sees when the database itself fails. The exception is not
# printed: a SQLAlchemy error carries the statement and its bound parameters,
# which here means the template body, and its connection context can carry the
# DSN. A stable reason code is what a runbook can act on anyway.
REASON_DATABASE_UNAVAILABLE: Final = "voucher_template_database_unavailable"


async def audit(
    session: AsyncSession,
    *,
    company_id: int,
    templates: list[dict[str, Any]],
) -> tuple[template_contract.TemplateProof, MessageTemplate | None, str | None]:
    """Prove Meta, then say what the stored row would have to become."""
    proof = template_contract.prove_meta_templates(templates)
    rows = list(
        (
            await session.execute(
                select(MessageTemplate)
                .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
                .where(MessageTemplate.company_id == company_id)
                .where(MessageTemplate.code == template_contract.VOUCHER_TEMPLATE_CODE)
                .where(MessageTemplate.language == template_contract.VOUCHER_TEMPLATE_LANGUAGE)
            )
        )
        .scalars()
        .all()
    )
    if len(rows) > 1:
        return proof, None, "multiple_rows_for_one_code"
    row = rows[0] if rows else None
    blocker = template_contract.db_row_blocker(row, company_id=company_id) if row is not None else "row_missing"
    return proof, row, blocker


def _repair_for(blocker: str, row: MessageTemplate | None) -> str:
    """What --apply may do about this blocker. Fail-closed by construction.

    An INSERT happens for exactly one blocker: ``row_missing``, which ``audit``
    reports only after proving the query returned nothing. An UPDATE happens
    only when there is exactly one row to update — ``audit`` hands back a row
    only in that case. Everything else is REPAIR_NONE.

    That last branch is the one that matters. Two rows carrying one code arrive
    here as ``multiple_rows_for_one_code`` with no row, and the earlier version
    of this command read "no row" as "create one" and inserted a third. A new
    blocker nobody has written yet would have done the same. Choosing between
    duplicates, or deleting one, is not a decision a script makes on its own.
    """
    if row is not None:
        return REPAIR_UPDATE
    if blocker == BLOCKER_ROW_MISSING:
        return REPAIR_INSERT
    return REPAIR_NONE


async def reconcile(*, company_id: int, apply: bool) -> tuple[dict[str, Any], int]:
    token = settings.whatsapp_access_token.strip()
    waba_id = (settings.meta_waba_id or "").strip()
    if not token or not waba_id:
        return {"ok": False, "reason": "meta_credentials_missing"}, EXIT_ARGUMENTS

    async with MetaTemplateClient(
        token=token,
        waba_id=waba_id,
        graph_url=settings.whatsapp_graph_url,
        api_version=settings.whatsapp_api_version,
        timeout_seconds=20.0,
    ) as client:
        templates = await client.list_templates()

    async with SessionLocal() as session:
        # Keep the audit SELECT and the optional write in one explicit
        # transaction.  AsyncSession autobegins on the first SELECT, so opening
        # ``session.begin()`` only after ``audit()`` raises
        # InvalidRequestError in production before any row can be written.
        async with session.begin():
            proof, row, blocker = await audit(session, company_id=company_id, templates=templates)
            report: dict[str, Any] = {
                "company_id": company_id,
                "meta": proof.as_safe_dict(),
                "db_row_blocker": blocker,
                "applied": False,
            }
            if not proof.proven:
                # Meta is the authority on what may be sent. Without its
                # approval there is nothing to align the row TO.
                return report, EXIT_BLOCKED
            if blocker is None:
                return report, EXIT_OK

            repair = _repair_for(blocker, row)
            if repair == REPAIR_NONE:
                # A blocker no automatic write may resolve. `would_apply` is
                # false rather than absent, because an operator reading this
                # needs to be told that --apply is not the answer — not left to
                # infer it from a missing field.
                report["would_apply"] = False
                return report, EXIT_BLOCKED
            if not apply:
                report["would_apply"] = True
                return report, EXIT_BLOCKED

            if repair == REPAIR_INSERT:
                session.add(
                    MessageTemplate(
                        provider=PROVIDER_EASYWEEK,
                        company_id=company_id,
                        code=template_contract.VOUCHER_TEMPLATE_CODE,
                        language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
                        body=template_contract.VOUCHER_TEMPLATE_BODY,
                        meta_template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
                        is_active=True,
                    )
                )
            else:
                assert row is not None  # REPAIR_UPDATE is returned only with a row
                row.body = template_contract.VOUCHER_TEMPLATE_BODY
                row.meta_template_name = template_contract.VOUCHER_META_TEMPLATE_NAME
                row.is_active = True
            report["applied"] = True
            # Returned from INSIDE the transaction on purpose: the commit
            # happens as this block unwinds, so a commit that fails discards
            # this report instead of printing `applied: true` over a rollback.
            return report, EXIT_OK


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Align the §36 voucher template row with the approved Meta content. Not a send permission.",
        allow_abbrev=False,
    )
    parser.add_argument("--company-id", type=int, required=True)
    parser.add_argument("--apply", action="store_true", help="Without it this command only reports.")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    registry = configured_easyweek_locations()
    if not registry.ready or args.company_id not in registry.locations:
        print(json.dumps({"ok": False, "reason": "unknown_company"}, ensure_ascii=False))
        return EXIT_ARGUMENTS
    try:
        report, code = await reconcile(company_id=args.company_id, apply=args.apply)
    except ScriptError as exc:
        print(json.dumps({"ok": False, "reason": "meta_unavailable", "detail": str(exc)}, ensure_ascii=False))
        return EXIT_BLOCKED
    except SQLAlchemyError:
        # Narrow on purpose: a database failure is an expected operational
        # outcome and gets a stable, safe answer. A NameError or a TypeError in
        # this file is a bug, and swallowing it here would hide it behind the
        # same reassuring reason code.
        #
        # The transaction has already rolled back — `session.begin()` unwound
        # with the exception — so there is nothing half-written to report.
        print(json.dumps({"ok": False, "reason": REASON_DATABASE_UNAVAILABLE}, ensure_ascii=False))
        return EXIT_BLOCKED
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


__all__ = ["audit", "main", "reconcile"]
