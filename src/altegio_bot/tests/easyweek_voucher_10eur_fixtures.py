"""Explicit §45 synthetic product; historical 15 EUR fixtures remain unchanged.

§45.4: there is deliberately no helper here that models an issued voucher's term as
proven, and none that replaces the validity boundary with a function returning
success. EasyWeek answers for the term, so the positive path needs neither — and a
test that wants a REFUSAL builds an artifact EasyWeek calls unusable instead.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_production import template_contract
from altegio_bot.campaigns.easyweek_voucher_production.composition import BatchApproval
from altegio_bot.easyweek_voucher_production_contract import (
    CURRENT_PRODUCTION_META_BODY,
    ProductionVoucherContract,
    new_mailing_contract,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate
from altegio_bot.tests import easyweek_voucher_production_fixtures as old

# What a NEW mailing is, which is what these fixtures describe.
#
# The page, the planner and the executor all prepare the current new-mailing
# product, so fixtures that feed them have to build orders for that product or the
# end-to-end walks compare a free issue against a paid one and fail for a reason
# that has nothing to do with what the test is about.
#
# PAID_CONTRACT stays exported and every helper takes ``contract=`` so the schema 3
# regressions keep asserting the paid product's own sums, tills and bytes.
CONTRACT: ProductionVoucherContract = new_mailing_contract()


# Which fixed product a helper describes. The current new-mailing product by
# default, with ``contract=`` for the schema 3 regressions. Parameterised rather
# than copied, because the order shapes below are exactly what the money rules are
# read from, and two divergent copies of them would be two different definitions
# of "a correct order".
def _contract(contract: ProductionVoucherContract | None = None) -> ProductionVoucherContract:
    return CONTRACT if contract is None else contract


def production_request(*, run_id: int, batch_id: int | None = None, contract: ProductionVoucherContract | None = None):
    selected = _contract(contract)
    return replace(
        old.production_request(run_id=run_id, batch_id=batch_id),
        schema_version=selected.request_schema_version,
        product_contract_version=selected.version,
        voucher_template_uuid=selected.template_uuid,
        # The till is part of the contract, so the request names the one this
        # product settles through rather than whatever the previous one used.
        payment_account_uuid=old.GIFT_ACCOUNT_UUID if selected.free_issue else old.ACCOUNT_UUID,
    )


def approval_for(count: int, *, contract: ProductionVoucherContract | None = None) -> BatchApproval:
    """What the operator states: the count and the NOMINAL, for either contract."""
    return BatchApproval(
        expected_recipient_count=count, approved_exposure_minor=count * _contract(contract).face_value_minor
    )


def template_payload(*, contract: ProductionVoucherContract | None = None, **changes: Any) -> dict[str, Any]:
    selected = _contract(contract)
    return {
        "uuid": selected.template_uuid,
        **selected.template_facts(),
        "vouchers_count": 0,
        "activated_vouchers_count": 0,
        **changes,
    }


def meta_template(*, contract: ProductionVoucherContract | None = None, **changes: Any) -> dict[str, Any]:
    return {
        "name": _contract(contract).meta_template_name,
        "language": "de",
        "status": "APPROVED",
        "category": "MARKETING",
        "parameter_format": "POSITIONAL",
        "components": [{"type": "BODY", "text": CURRENT_PRODUCTION_META_BODY}],
        **changes,
    }


class FakeReader(old.FakeReader):
    def __init__(self, *, meta_templates=None, contract: ProductionVoucherContract | None = None, **kwargs):
        selected = _contract(contract)
        self.contract = selected
        kwargs.setdefault("template", template_payload(contract=selected))
        super().__init__(**kwargs)
        self.meta_templates = [meta_template(contract=selected)] if meta_templates is None else meta_templates
        self.product_reads: list[str] = []
        self.meta_reads = 0
        contract = selected.digest_material()
        self.workspace = {
            "uuid": contract["workspace_uuid"],
            "slug": contract["workspace_slug"],
            "currency": contract["currency"],
        }
        self.locations = [
            {"uuid": contract["location_uuid"], "name": "Synthetic Karlsruhe", "timezone": "Europe/Berlin"}
        ]
        self.environment_reads: list[str] = []
        # The branch's till collection, containing the one THIS contract settles
        # through. A reader that lists the other product's till proves nothing here.
        self.accounts = [{"uuid": old.GIFT_ACCOUNT_UUID if selected.free_issue else old.ACCOUNT_UUID}]

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


def voucher_order(
    index: int, *, marker: str, contract: ProductionVoucherContract | None = None, **changes: Any
) -> dict[str, Any]:
    """One order for *contract*: money at the ISSUE price, the voucher at NOMINAL."""
    selected = _contract(contract)
    settled = changes.get("status") in ("paid", "refunded")
    return old.voucher_order(
        index,
        marker=marker,
        total=selected.issue_price_minor,
        subtotal=selected.issue_price_minor,
        invoice={
            "total": selected.issue_price_minor,
            "subtotal": selected.issue_price_minor,
            "amount_paid": selected.issue_price_minor if settled else 0,
            "amount_due": 0 if settled else selected.issue_price_minor,
        },
        vouchers=[
            old.issued_voucher(
                index,
                voucher_template_uuid=selected.template_uuid,
                price=selected.issue_price_minor,
                value=selected.face_value_minor,
            )
        ],
        **changes,
    )


async def marker_orders(
    session_maker,
    *,
    batch_id: int,
    count: int | None = None,
    offset: int = 0,
    contract: ProductionVoucherContract | None = None,
    **changes: Any,
):
    selected = _contract(contract)
    orders = await old.marker_orders(session_maker, batch_id=batch_id, count=count, offset=offset, **changes)
    for order in orders.values():
        order["total"] = order["subtotal"] = selected.issue_price_minor
        settled = order.get("status") in ("paid", "refunded")
        order["invoice"] = {
            "total": selected.issue_price_minor,
            "subtotal": selected.issue_price_minor,
            "amount_paid": selected.issue_price_minor if settled else 0,
            "amount_due": 0 if settled else selected.issue_price_minor,
        }
        for voucher in order["vouchers"]:
            voucher["voucher_template_uuid"] = selected.template_uuid
            voucher["price"] = selected.issue_price_minor
            voucher["value"] = selected.face_value_minor
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


# ---------------------------------------------------------------------------
# The browser round trip, for the suites that need the REAL authorisation path
# ---------------------------------------------------------------------------
# One copy, shared. A stage that is only ever planned in-process proves nothing
# about a stored approval: the plan endpoint writes an approval row, the confirm
# endpoint spends it into a durable operation, and the executor rebuilds the plan
# live before it acts. Several §45 regressions are specifically about what
# happens BETWEEN those steps, so they need the real round trip rather than a
# direct call into the runner.

PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"


async def ui_plan(client, **payload: Any) -> dict[str, Any]:
    """One stage offer, over HTTP, exactly as the page asks for it."""
    return (await client.post(PLAN_URL, json=payload)).json()


async def ui_confirm(client, offer: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Spend the offer by echoing the server's own numbers back, as the page does."""
    response = await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    return response.status_code, response.json()


async def ui_execute(session_maker):
    """One pass of the real executor over the real queue."""
    from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

    return await worker_module.run_once(session_maker, owner="test-executor")


async def ui_frozen(client, session_maker, transports, *, count: int):
    """A new-contract batch frozen through the browser and the executor."""
    from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module

    run_id, _ = await old.seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    transports.use(reader=reader)
    offer = await ui_plan(
        client,
        stage="freeze",
        preview_run_id=run_id,
        expected_recipient_count=count,
        approved_exposure_minor=count * CONTRACT.face_value_minor,
    )
    assert offer["ready"], offer["reasons"]
    assert (await ui_confirm(client, offer))[0] == 200
    assert await ui_execute(session_maker) is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    batch_id = int(snapshot.batch_id or 0)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    return run_id, batch_id, reader


def close_meta_read_seam(reader: Any) -> Any:
    """Take away the read-adapter seam, so the Meta proof uses the real client.

    ``prove_live_meta_template`` prefers ``reader.list_meta_templates`` when the
    reader has one, which is how most tests supply a Meta listing. A test about
    the production transport has to get PAST that preference: with the seam shut
    the proof builds the real ``MetaTemplateClient``, which is where the
    ``httpx`` errors actually come from.
    """
    reader.list_meta_templates = None
    return reader
