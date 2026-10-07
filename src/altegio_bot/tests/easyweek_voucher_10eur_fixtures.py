"""Explicit §45 synthetic product; historical 15 EUR fixtures remain unchanged."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production import template_contract
from altegio_bot.campaigns.easyweek_voucher_production.composition import BatchApproval
from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PRODUCTION_CONTRACT as CONTRACT,
)
from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PRODUCTION_META_BODY,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate
from altegio_bot.tests import easyweek_voucher_production_fixtures as old


def production_request(*, run_id: int, batch_id: int | None = None):
    return replace(
        old.production_request(run_id=run_id, batch_id=batch_id),
        schema_version="3",
        product_contract_version=CONTRACT.version,
        voucher_template_uuid=CONTRACT.template_uuid,
    )


def approval_for(count: int) -> BatchApproval:
    return BatchApproval(expected_recipient_count=count, approved_exposure_minor=count * CONTRACT.unit_price_minor)


def template_payload(**changes: Any) -> dict[str, Any]:
    return {
        "uuid": CONTRACT.template_uuid,
        **CONTRACT.template_facts(),
        "vouchers_count": 0,
        "activated_vouchers_count": 0,
        **changes,
    }


def meta_template(**changes: Any) -> dict[str, Any]:
    return {
        "name": CONTRACT.meta_template_name,
        "language": "de",
        "status": "APPROVED",
        "category": "MARKETING",
        "parameter_format": "POSITIONAL",
        "components": [{"type": "BODY", "text": CURRENT_PRODUCTION_META_BODY}],
        **changes,
    }


class FakeReader(old.FakeReader):
    def __init__(self, *, meta_templates=None, **kwargs):
        kwargs.setdefault("template", template_payload())
        super().__init__(**kwargs)
        self.meta_templates = [meta_template()] if meta_templates is None else meta_templates
        self.product_reads: list[str] = []
        self.meta_reads = 0
        contract = CONTRACT.digest_material()
        self.workspace = {
            "uuid": contract["workspace_uuid"],
            "slug": contract["workspace_slug"],
            "currency": contract["currency"],
        }
        self.locations = [
            {"uuid": contract["location_uuid"], "name": "Synthetic Karlsruhe", "timezone": "Europe/Berlin"}
        ]
        self.environment_reads: list[str] = []
        self.accounts = [{"uuid": old.ACCOUNT_UUID}]

    async def list_location_accounts(self, location_uuid):
        self.environment_reads.append("accounts")
        if isinstance(self.accounts, Exception):
            raise self.accounts
        return self.accounts

    async def get_workspace(self):
        self.environment_reads.append("workspace")
        if isinstance(self.workspace, Exception):
            raise self.workspace
        return self.workspace

    async def list_locations(self):
        self.environment_reads.append("locations")
        if isinstance(self.locations, Exception):
            raise self.locations
        return self.locations

    async def get_voucher_template(self, template_uuid):
        self.product_reads.append(template_uuid)
        return await super().get_voucher_template(template_uuid)

    async def list_meta_templates(self):
        self.meta_reads += 1
        return self.meta_templates


def voucher_order(index: int, *, marker: str, **changes: Any) -> dict[str, Any]:
    return old.voucher_order(
        index,
        marker=marker,
        total=CONTRACT.unit_price_minor,
        subtotal=CONTRACT.unit_price_minor,
        invoice={
            "total": CONTRACT.unit_price_minor,
            "subtotal": CONTRACT.unit_price_minor,
            "amount_paid": CONTRACT.unit_price_minor if changes.get("status") in ("paid", "refunded") else 0,
            "amount_due": 0 if changes.get("status") in ("paid", "refunded") else CONTRACT.unit_price_minor,
        },
        vouchers=[
            old.issued_voucher(
                index,
                voucher_template_uuid=CONTRACT.template_uuid,
                price=CONTRACT.unit_price_minor,
                value=CONTRACT.unit_price_minor,
            )
        ],
        **changes,
    )


async def marker_orders(session_maker, *, batch_id: int, count: int | None = None, offset: int = 0, **changes: Any):
    orders = await old.marker_orders(session_maker, batch_id=batch_id, count=count, offset=offset, **changes)
    for order in orders.values():
        order["total"] = order["subtotal"] = CONTRACT.unit_price_minor
        settled = order.get("status") in ("paid", "refunded")
        order["invoice"] = {
            "total": CONTRACT.unit_price_minor,
            "subtotal": CONTRACT.unit_price_minor,
            "amount_paid": CONTRACT.unit_price_minor if settled else 0,
            "amount_due": 0 if settled else CONTRACT.unit_price_minor,
        }
        for voucher in order["vouchers"]:
            voucher["voucher_template_uuid"] = CONTRACT.template_uuid
            voucher["price"] = voucher["value"] = CONTRACT.unit_price_minor
    return orders


async def seed_template_and_sender(session_maker):
    # Keep the legacy row intact and present alongside the new scoped row.
    await old.seed_template_and_sender(session_maker)
    async with session_maker() as session, session.begin():
        existing = await session.scalar(
            select(MessageTemplate).where(
                MessageTemplate.provider == PROVIDER_EASYWEEK,
                MessageTemplate.company_id == old.COMPANY_ID,
                MessageTemplate.code == CONTRACT.message_code,
            )
        )
        if existing is None:
            session.add(
                MessageTemplate(
                    provider=PROVIDER_EASYWEEK,
                    company_id=old.COMPANY_ID,
                    code=CONTRACT.message_code,
                    language="de",
                    meta_template_name=CONTRACT.meta_template_name,
                    body=template_contract.VOUCHER_TEMPLATE_BODY,
                    is_active=True,
                )
            )
