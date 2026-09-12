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
        proof, row, blocker = await audit(session, company_id=company_id, templates=templates)
        report: dict[str, Any] = {
            "company_id": company_id,
            "meta": proof.as_safe_dict(),
            "db_row_blocker": blocker,
            "applied": False,
        }
        if not proof.proven:
            # Meta is the authority on what may be sent. Without its approval
            # there is nothing to align the row TO.
            return report, EXIT_BLOCKED
        if blocker is None:
            return report, EXIT_OK
        if not apply:
            report["would_apply"] = True
            return report, EXIT_BLOCKED

        async with session.begin():
            if row is None:
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
                row.body = template_contract.VOUCHER_TEMPLATE_BODY
                row.meta_template_name = template_contract.VOUCHER_META_TEMPLATE_NAME
                row.is_active = True
        report["applied"] = True
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
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


__all__ = ["audit", "main", "reconcile"]
