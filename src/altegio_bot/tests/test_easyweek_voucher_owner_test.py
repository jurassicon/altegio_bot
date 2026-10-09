"""Isolated single-grant UI test: no campaigns, delivery, retries or raw artifacts."""

from __future__ import annotations

import asyncio
import copy
import json
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select

from altegio_bot.campaigns import easyweek_voucher_owner_test as owner
from altegio_bot.campaigns.easyweek_voucher_production import account
from altegio_bot.campaigns.easyweek_voucher_production.operations import OpsPrincipal
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_WORKSPACE_CURRENCY,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    EasyWeekVoucherOwnerTest,
    EasyWeekVoucherProductionBatch,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    CUSTOMER_UUIDS,
    GIFT_ACCOUNT_UUID,
    ORDER_UUIDS,
    PHONES,
    STAFFER_UUID,
    pin_synthetic_issuer,
    staffers_page,
)
from altegio_bot.utils import utcnow

PRINCIPAL = OpsPrincipal(account="owner-test-operator", session_fingerprint="b" * 64)
CODE = "SYNTHETIC-OWNER-GIFT-CODE"


def configure_owner_test(monkeypatch):
    pin_synthetic_issuer(monkeypatch)
    pin = account.account_fingerprint(GIFT_ACCOUNT_UUID)
    monkeypatch.setattr(account, "expected_account_fingerprint", lambda contract=None: pin)
    monkeypatch.setattr(owner, "expected_account_fingerprint", lambda contract=None: pin)
    monkeypatch.setattr(settings, "ops_user", PRINCIPAL.account)
    monkeypatch.setattr(settings, "ops_secret", "synthetic-owner-test-signing-key")
    monkeypatch.setattr(settings, "easyweek_voucher_owner_test_enabled", True)
    monkeypatch.setattr(settings, "easyweek_voucher_owner_test_customer_uuid", CUSTOMER_UUIDS[0])
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_executor_enabled", True)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", STAFFER_UUID)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_account_uuid", GIFT_ACCOUNT_UUID)


class OwnerTestReader:
    def __init__(self):
        self.template = {
            "uuid": owner.CONTRACT.template_uuid,
            **owner.CONTRACT.template_facts(),
            "vouchers_count": 5,
            "activated_vouchers_count": 5,
        }
        self.order = None
        self.phone = PHONES[0]
        self.reads = 0
        self.fail_readback = False
        self.customer = CUSTOMER_UUIDS[0]

    async def get_workspace(self):
        self.reads += 1
        return {
            "uuid": EASYWEEK_WORKSPACE_UUID,
            "slug": EASYWEEK_WORKSPACE_SLUG,
            "currency": EASYWEEK_WORKSPACE_CURRENCY,
        }

    async def list_locations(self):
        return [{"uuid": KARLSRUHE_LOCATION_UUID}]

    async def get_voucher_template(self, template_uuid):
        assert template_uuid == owner.CONTRACT.template_uuid
        return copy.deepcopy(self.template)

    async def list_location_accounts(self, location_uuid):
        assert location_uuid == KARLSRUHE_LOCATION_UUID
        return [{"uuid": GIFT_ACCOUNT_UUID}]

    async def list_location_staffers(self, location_uuid, *, page):
        assert location_uuid == KARLSRUHE_LOCATION_UUID and page == 1
        return staffers_page([STAFFER_UUID])

    async def get_customer(self, customer_uuid):
        return {"uuid": self.customer, "phone": self.phone, "first_name": "Synthetic"}

    async def get_order(self, order_uuid):
        if self.fail_readback:
            raise RuntimeError("SENSITIVE PROVIDER RESPONSE " + CODE)
        assert self.order is not None and order_uuid == self.order["uuid"]
        return copy.deepcopy(self.order)

    async def list_location_customer_orders(self, *, location_uuid, customer_uuid, page):
        assert location_uuid == KARLSRUHE_LOCATION_UUID and customer_uuid == CUSTOMER_UUIDS[0] and page == 1
        return {
            "data": [copy.deepcopy(self.order)] if self.order else [],
            "meta": {"current_page": 1, "last_page": 1, "per_page": 100},
        }


class OwnerTestWriter:
    def __init__(self, reader):
        self.reader = reader
        self.creates = 0
        self.pays = 0
        self.create_unknown = False
        self.pay_unknown = False
        self.create_paid = False
        self.started = None
        self.release = None

    async def create_voucher_order(self, **kwargs):
        assert kwargs["price_minor"] == 0 and type(kwargs["price_minor"]) is int
        assert kwargs["customer_uuid"] == CUSTOMER_UUIDS[0]
        assert kwargs["staffer_uuid"] == STAFFER_UUID
        assert kwargs["product_contract_version"] == owner.CONTRACT.version
        self.creates += 1
        self.reader.order = {
            "uuid": ORDER_UUIDS[0],
            "status": "paid" if self.create_paid else "open",
            "customer_uuid": CUSTOMER_UUIDS[0],
            "location_uuid": KARLSRUHE_LOCATION_UUID,
            "comment": kwargs["marker"],
            "created_at": utcnow().isoformat(),
            "subtotal": 0,
            "is_reverted": False,
            "account_paid_amount": 0,
            "voucher_paid_amount": 0,
            "discount_amount": 0,
            "promocode_discount_amount": 0,
            "vouchers": [
                {"voucher_template_uuid": owner.CONTRACT.template_uuid, "price": 0, "value": 1000, "quantity": 1}
            ],
            "services": [],
            "goods": [],
        }
        if self.create_paid:
            self.reader.order["vouchers"][0]["code"] = CODE
            del self.reader.order["vouchers"][0]["quantity"]
        if self.started:
            self.started.set()
            await self.release.wait()
        if self.create_unknown:
            raise TimeoutError(CODE)
        return VoucherMutationResponse(http_status=201, envelope=copy.deepcopy(self.reader.order))

    async def pay_voucher_order(self, *, order_uuid, account_uuid):
        assert order_uuid == ORDER_UUIDS[0] and account_uuid == GIFT_ACCOUNT_UUID
        self.pays += 1
        self.reader.order["status"] = "paid"
        self.reader.order["vouchers"][0]["code"] = CODE
        del self.reader.order["vouchers"][0]["quantity"]
        if self.pay_unknown:
            raise TimeoutError(CODE)
        return VoucherMutationResponse(http_status=200, envelope=copy.deepcopy(self.reader.order))


@pytest.fixture
def owner_configuration(monkeypatch):
    configure_owner_test(monkeypatch)


async def queue(session_maker, reader, stage="create", principal=PRINCIPAL):
    plan = await owner.offer(session_maker, principal=principal, stage=stage, reader=reader)
    result = await owner.confirm(
        session_maker,
        principal=principal,
        approval_id=plan["approval_id"],
        confirmed_count=1,
        confirmed_nominal_minor=1000,
        confirmed_issue_minor=0,
    )
    assert result["accepted"]
    return plan


async def test_isolated_lifecycle_one_create_one_pay_no_campaign_rows(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    assert (await owner.get_status(session_maker))["available_actions"] == ["create"]
    plan = await queue(session_maker, reader)
    assert writer.creates == writer.pays == 0
    created = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert created["state"] == "open" and created["pay_attempted"] is False
    repeated = await owner.confirm(
        session_maker,
        principal=PRINCIPAL,
        approval_id=plan["approval_id"],
        confirmed_count=1,
        confirmed_nominal_minor=1000,
        confirmed_issue_minor=0,
    )
    assert repeated["state"] == "open"
    await queue(session_maker, reader, "pay")
    paid = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert paid["state"] == "paid" and paid["code_present"] and paid["delivery_authorized"] is False
    assert await owner.execute_next(session_maker, reader=reader, writer=writer) is None
    assert writer.creates == writer.pays == 1
    async with session_maker() as session:
        row = await session.get(EasyWeekVoucherOwnerTest, 1)
        serialized = json.dumps({"approval": row.approval, "audit": row.audit, "status": paid})
        assert CODE not in serialized and PHONES[0] not in serialized
        for model in (CampaignRun, CampaignRecipient, EasyWeekVoucherProductionBatch):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    with pytest.raises(owner.OwnerTestError, match="stage_unavailable"):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("easyweek_voucher_owner_test_enabled", False, "disabled"),
        ("easyweek_voucher_owner_test_customer_uuid", "", "customer_unconfigured"),
        ("easyweek_voucher_production_mailing_enabled", True, "closed_mailing_fence"),
        ("easyweek_voucher_production_executor_enabled", False, "executor_disabled"),
        ("easyweek_voucher_production_mailing_account_uuid", ORDER_UUIDS[0], "account_unproven"),
        ("easyweek_voucher_production_mailing_staffer_uuid", ORDER_UUIDS[0], "issuer_unproven"),
    ],
)
async def test_default_and_wrong_configuration_never_reads_or_queues(
    session_maker, owner_configuration, monkeypatch, field, value, reason
):
    monkeypatch.setattr(settings, field, value)
    reader = OwnerTestReader()
    with pytest.raises(owner.OwnerTestError, match=reason):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)
    assert reader.reads == 0
    assert (await owner.get_status(session_maker))["available_actions"] == []


@pytest.mark.parametrize("value", [True, None, "0", 1000, -1])
async def test_product_price_drift_blocks_offer(session_maker, owner_configuration, value):
    reader = OwnerTestReader()
    reader.template["cost"] = value
    with pytest.raises(owner.OwnerTestError, match="product_unproven"):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)


async def test_customer_readback_mismatch_refuses(session_maker, owner_configuration):
    reader = OwnerTestReader()
    reader.customer = CUSTOMER_UUIDS[1]
    with pytest.raises(owner.OwnerTestError, match="customer_unproven"):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)


async def test_double_confirmation_and_two_workers_claim_once(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    plan = await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)

    async def confirm():
        return await owner.confirm(
            session_maker,
            principal=PRINCIPAL,
            approval_id=plan["approval_id"],
            confirmed_count=1,
            confirmed_nominal_minor=1000,
            confirmed_issue_minor=0,
        )

    await asyncio.gather(confirm(), confirm())
    await asyncio.gather(
        owner.execute_next(session_maker, reader=reader, writer=writer),
        owner.execute_next(session_maker, reader=reader, writer=writer),
    )
    assert writer.creates == 1 and writer.pays == 0


async def test_approval_tamper_expiry_and_principal_refuse(session_maker, owner_configuration):
    reader = OwnerTestReader()
    plan = await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)
    args = {
        "approval_id": plan["approval_id"],
        "confirmed_count": 1,
        "confirmed_nominal_minor": 1000,
        "confirmed_issue_minor": 0,
    }
    with pytest.raises(owner.OwnerTestError, match="principal_mismatch"):
        await owner.confirm(session_maker, principal=OpsPrincipal(PRINCIPAL.account, "different-session"), **args)
    with pytest.raises(owner.OwnerTestError, match="amount_unconfirmed"):
        await owner.confirm(session_maker, principal=PRINCIPAL, **{**args, "confirmed_count": True})
    async with session_maker() as session, session.begin():
        row = await session.get(EasyWeekVoucherOwnerTest, 1)
        row.approval = {**row.approval, "issue_minor": 1000}
    with pytest.raises(owner.OwnerTestError, match="approval_invalid"):
        await owner.confirm(session_maker, principal=PRINCIPAL, **args)
    plan = await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)
    async with session_maker() as session, session.begin():
        row = await session.get(EasyWeekVoucherOwnerTest, 1)
        material = {k: v for k, v in row.approval.items() if k not in {"signature", "consumed"}}
        material["expires_at"] = (utcnow() - timedelta(seconds=1)).isoformat()
        row.approval = {**material, "signature": owner._digest(material), "consumed": False}
    with pytest.raises(owner.OwnerTestError, match="approval_expired"):
        await owner.confirm(session_maker, principal=PRINCIPAL, **{**args, "approval_id": plan["approval_id"]})


async def test_unknown_create_reconcile_only_never_recreates(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    writer.create_unknown = True
    await queue(session_maker, reader)
    unknown = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert unknown["state"] == "unknown" and not unknown["order_observed"]
    assert await owner.execute_next(session_maker, reader=reader, writer=writer) is None
    reconciled = await owner.reconcile(session_maker, principal=PRINCIPAL, reader=reader)
    assert reconciled["state"] == "open"
    with pytest.raises(owner.OwnerTestError):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)
    assert writer.creates == 1


async def test_unknown_pay_can_be_read_paid_but_never_repaid(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    await queue(session_maker, reader)
    await owner.execute_next(session_maker, reader=reader, writer=writer)
    await queue(session_maker, reader, "pay")
    writer.pay_unknown = True
    assert (await owner.execute_next(session_maker, reader=reader, writer=writer))["state"] == "unknown"
    assert (await owner.reconcile(session_maker, principal=PRINCIPAL, reader=reader))["state"] == "paid"
    assert await owner.execute_next(session_maker, reader=reader, writer=writer) is None
    assert writer.pays == 1


async def test_unexpected_paid_create_cannot_pay_or_pass_account_proof(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    writer.create_paid = True
    await queue(session_maker, reader)
    result = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert result["state"] == "blocked" and "account_unproven" in result["reason"]
    assert (await owner.reconcile(session_maker, principal=PRINCIPAL, reader=reader))["state"] == "blocked"
    with pytest.raises(owner.OwnerTestError):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="pay", reader=reader)
    assert writer.pays == 0


async def test_stop_queued_prevents_any_mutation(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    await queue(session_maker, reader)
    await owner.stop(session_maker, principal=PRINCIPAL)
    assert await owner.execute_next(session_maker, reader=reader, writer=writer) is None
    with pytest.raises(owner.OwnerTestError, match="stopped"):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)
    assert writer.creates == 0


async def test_stop_in_flight_keeps_real_result_and_never_allows_pay(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    writer.started, writer.release = asyncio.Event(), asyncio.Event()
    await queue(session_maker, reader)
    task = asyncio.create_task(owner.execute_next(session_maker, reader=reader, writer=writer))
    await writer.started.wait()
    await owner.stop(session_maker, principal=PRINCIPAL)
    writer.release.set()
    result = await task
    assert result["stopped"] and result["state"] == "open"
    assert result["available_actions"] == ["reconcile"]
    assert (await owner.reconcile(session_maker, principal=PRINCIPAL, reader=reader))["stopped"]
    assert writer.creates == 1 and writer.pays == 0


async def test_restart_marks_running_unknown_without_retry(session_maker, owner_configuration):
    reader = OwnerTestReader()
    await queue(session_maker, reader)
    async with session_maker() as session, session.begin():
        row = await session.get(EasyWeekVoucherOwnerTest, 1)
        row.state, row.create_attempted = "create_running", True
    await owner.interrupt_running(session_maker)
    assert (await owner.get_status(session_maker))["state"] == "unknown"
    writer = OwnerTestWriter(reader)
    assert await owner.execute_next(session_maker, reader=reader, writer=writer) is None
    assert writer.creates == 0


async def test_configuration_rotation_does_not_mint_another_grant(session_maker, owner_configuration, monkeypatch):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    await queue(session_maker, reader)
    monkeypatch.setattr(settings, "easyweek_voucher_owner_test_customer_uuid", CUSTOMER_UUIDS[1])
    result = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert result["state"] == "blocked" and result["reason"] == "owner_test_configuration_drift"
    reader.customer = CUSTOMER_UUIDS[1]
    with pytest.raises(owner.OwnerTestError):
        await owner.offer(session_maker, principal=PRINCIPAL, stage="create", reader=reader)
    assert writer.creates == 0


async def test_customer_phone_changes_invalidate_the_live_plan(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    await queue(session_maker, reader)
    reader.phone = PHONES[1]
    result = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert result["reason"] == "owner_test_configuration_drift" and writer.creates == 0


async def test_readback_failure_preserves_order_and_redacts_provider_exception(session_maker, owner_configuration):
    reader = OwnerTestReader()
    writer = OwnerTestWriter(reader)
    await queue(session_maker, reader)
    reader.fail_readback = True
    result = await owner.execute_next(session_maker, reader=reader, writer=writer)
    assert result["state"] == "unknown" and result["order_observed"]
    assert CODE not in json.dumps(result)
    async with session_maker() as session:
        row = await session.get(EasyWeekVoucherOwnerTest, 1)
        assert row.order_uuid == UUID(ORDER_UUIDS[0])
    reader.fail_readback = False
    assert (await owner.reconcile(session_maker, principal=PRINCIPAL, reader=reader))["state"] == "open"
