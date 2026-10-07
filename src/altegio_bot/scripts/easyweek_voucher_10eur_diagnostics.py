"""Explicit read-only product/issued-artifact diagnostics; never a rollout bypass.

Prints fixed product facts, counters and allowlisted field types only. No
voucher codes, customer/order identities, dates, request payloads or secrets.
Run only when an operator authorizes these authenticated GETs.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production.baseline import prove_production_baseline
from altegio_bot.campaigns.easyweek_voucher_production.validity import issued_voucher_validity_reason
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_log_redaction import redact_easyweek_url_logging
from altegio_bot.easyweek_voucher_canary.orders import order_object
from altegio_bot.easyweek_voucher_production_contract import CURRENT_PRODUCTION_CONTRACT
from altegio_bot.models.models import EasyWeekVoucherProductionBatch, EasyWeekVoucherProductionBatchItem
from altegio_bot.utils import utcnow

DATE_CANDIDATES = ("activated_at", "expires_at", "valid_until", "is_activated", "is_expired", "status")


def issued_shape(payload: object) -> dict:
    order = order_object(payload) or {}
    nodes = order.get("vouchers")
    if not isinstance(nodes, list):
        nodes = [order.get("voucher")]
    return {
        "artifact_count": len(nodes),
        "date_candidate_fields": [
            {name: type(node[name]).__name__ for name in DATE_CANDIDATES if name in node}
            for node in nodes
            if isinstance(node, dict)
        ],
        "validity_reason": issued_voucher_validity_reason(payload, now=utcnow()),
        "positive_validity_contract_supported": False,
    }


async def diagnose(*, batch_id: int | None, slot: int | None) -> dict:
    order_uuid = None
    if batch_id is not None:
        async with SessionLocal() as session:
            batch = await session.get(EasyWeekVoucherProductionBatch, batch_id)
            if batch is None or batch.product_contract_version != CURRENT_PRODUCTION_CONTRACT.version:
                return {"reason": "new_contract_batch_required"}
            order_uuid = await session.scalar(
                select(EasyWeekVoucherProductionBatchItem.target_order_uuid).where(
                    EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                    EasyWeekVoucherProductionBatchItem.slot == slot,
                )
            )
            if order_uuid is None:
                return {"reason": "issued_order_not_recorded"}
    # The database read is closed before remote reads. No transaction writes.
    async with EasyWeekClient() as reader:
        payload = await reader.get_voucher_template(CURRENT_PRODUCTION_CONTRACT.template_uuid)
        proof = prove_production_baseline(payload, contract=CURRENT_PRODUCTION_CONTRACT)
        report = {"product_contract_version": CURRENT_PRODUCTION_CONTRACT.version, **proof.as_safe_dict()}
        if order_uuid is not None:
            report["issued_voucher"] = issued_shape(await reader.get_order(str(order_uuid)))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--batch-id", type=int)
    parser.add_argument("--slot", type=int)
    args = parser.parse_args(argv)
    if (args.batch_id is None) != (args.slot is None):
        parser.error("--batch-id and --slot must be supplied together")
    if any(value is not None and value < 1 for value in (args.batch_id, args.slot)):
        parser.error("batch and slot must be positive")
    redact_easyweek_url_logging()
    try:
        report = asyncio.run(diagnose(batch_id=args.batch_id, slot=args.slot))
    except Exception as exc:
        # The provider exception text can contain URLs and secrets.
        report = {"reason": "read_only_diagnostic_failed", "error_type": type(exc).__name__}
        print(json.dumps(report, sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("baseline_proven") else 1


if __name__ == "__main__":
    raise SystemExit(main())
