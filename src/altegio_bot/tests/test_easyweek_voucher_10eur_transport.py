"""Required fixed-product transport and monetary evidence regressions."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from altegio_bot.campaigns.easyweek_voucher_production.validity import (
    VALIDITY_UNPROVEN,
    VOUCHER_EXPIRED,
    issued_voucher_validity_reason,
)
from altegio_bot.easyweek_client import EasyWeekPermanentError
from altegio_bot.easyweek_voucher_canary.orders import (
    ORDER_PAID,
    PAYMENT_PROOF_AMOUNTS,
    classify_order,
    paid_order_amounts_proven,
    payable_order_reasons,
)
from altegio_bot.easyweek_voucher_canary.plan import frozen_template_mismatches
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationClient, EasyWeekVoucherMutationUnknown
from altegio_bot.easyweek_voucher_production_contract import CURRENT_PRODUCTION_CONTRACT as CONTRACT

CUSTOMER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
STAFFER = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
ORDER = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


def request_fields(**changes):
    fields = {
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "customer_uuid": CUSTOMER,
        "staffer_uuid": STAFFER,
        "voucher_template_uuid": CONTRACT.template_uuid,
        "price_minor": 1000,
        "marker": "synthetic-10eur-proof",
        "product_contract_version": CONTRACT.version,
    }
    return fields | changes


def client(handler, *, workspace="kitilash"):
    return EasyWeekVoucherMutationClient(
        api_key="SYNTHETIC-KEY",
        workspace_slug=workspace,
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.asyncio
async def test_explicit_new_contract_sends_exactly_one_fixed_voucher():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"uuid": ORDER})

    async with client(handler) as transport:
        await transport.create_voucher_order(**request_fields())
    assert len(calls) == 1
    assert json.loads(calls[0].content)["vouchers"] == [
        {
            "voucher_template_uuid": CONTRACT.template_uuid,
            "price": 1000,
            "quantity": 1,
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"product_contract_version": None},
        {"product_contract_version": "2"},
        {"product_contract_version": "unknown"},
        {"voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID},
        {"price_minor": 1500},
        {"price_minor": 1000.0},
        {"price_minor": "1000"},
        {"price_minor": True},
        {"voucher_template_uuid": "35231"},
    ],
)
async def test_no_implicit_new_product_or_mixed_identity_before_wire(changes):
    calls = []
    async with client(lambda request: calls.append(request)) as transport:
        with pytest.raises(EasyWeekPermanentError):
            await transport.create_voucher_order(**request_fields(**changes))
    assert calls == []


@pytest.mark.asyncio
async def test_new_product_refuses_different_workspace():
    async with client(lambda _: pytest.fail("request sent"), workspace="other") as transport:
        with pytest.raises(EasyWeekPermanentError):
            await transport.create_voucher_order(**request_fields())


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500, 307])
async def test_new_contract_never_retries_unknown(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={})

    async with client(handler) as transport:
        with pytest.raises(EasyWeekVoucherMutationUnknown):
            await transport.create_voucher_order(**request_fields())
    assert len(calls) == 1


def paid_order():
    return {
        "uuid": ORDER,
        "status": "paid",
        "total": 1000,
        "invoice": {"total": 1000, "amount_paid": 1000, "amount_due": 0},
        "vouchers": [{"voucher_template_uuid": CONTRACT.template_uuid, "price": 1000, "value": 1000, "quantity": 1}],
    }


def test_exact_new_invoice_and_artifact():
    payload = paid_order()
    assert paid_order_amounts_proven(payload, expected_price_minor=1000)
    # PAY admission checks the pre-payment debt; settled proof checks zero due.
    payload["invoice"]["amount_due"] = 1000
    assert (
        payable_order_reasons(payload, expected_template_uuid=CONTRACT.template_uuid, expected_price_minor=1000) == ()
    )
    payload["invoice"]["amount_due"] = 0
    payload.pop("status")
    assert classify_order(payload, expected_price_minor=1000) == (ORDER_PAID, PAYMENT_PROOF_AMOUNTS)
    assert classify_order(payload)[0] != ORDER_PAID


@pytest.mark.parametrize("value", [1500, 999, 1000.0, "1000", True, None])
@pytest.mark.parametrize("field", ["total", "amount_paid"])
def test_incorrect_payment_amount_rejected_even_with_paid_status(field, value):
    payload = paid_order()
    payload["invoice"][field] = value
    assert not paid_order_amounts_proven(payload, expected_price_minor=1000)


@pytest.mark.parametrize("field", ["amount_paid", "amount_due"])
def test_documented_paid_status_does_not_require_optional_invoice_fields(field):
    payload = paid_order()
    payload["invoice"].pop(field)
    assert paid_order_amounts_proven(payload, expected_price_minor=1000)
    payload.pop("status")
    assert not paid_order_amounts_proven(payload, expected_price_minor=1000)


def test_documented_pos_paid_shape_proves_exact_amount_without_calculation_fields():
    payload = {"uuid": ORDER, "status": "paid", "subtotal": 1000, "account_paid_amount": 1000}
    assert paid_order_amounts_proven(payload, expected_price_minor=1000)
    payload.pop("status")
    assert not paid_order_amounts_proven(payload, expected_price_minor=1000)


def test_new_template_facts_do_not_accidentally_keep_old_uuid():
    template = {"uuid": CONTRACT.template_uuid, **CONTRACT.template_facts()}
    assert frozen_template_mismatches(template, facts=CONTRACT.template_facts()) == ("uuid",)
    assert (
        frozen_template_mismatches(
            template, facts=CONTRACT.template_facts(), expected_template_uuid=CONTRACT.template_uuid
        )
        == ()
    )


@pytest.mark.parametrize(
    "facts",
    [
        {},
        {"validity": 1},
        {"is_activated": True},
        {"activated_at": "2026-10-01T12:00:00Z", "expires_at": "2026-11-01T12:00:00Z"},
    ],
)
def test_product_term_or_plausible_dates_never_prove_issued_validity(facts):
    now = datetime(2026, 10, 7, tzinfo=timezone.utc)
    assert issued_voucher_validity_reason({"vouchers": [facts]}, now=now) == VALIDITY_UNPROVEN


@pytest.mark.parametrize("field", ["expires_at", "valid_until"])
def test_expired_candidate_refuses_without_conferring_positive_semantics(field):
    now = datetime(2026, 10, 7, tzinfo=timezone.utc)
    assert (
        issued_voucher_validity_reason({"vouchers": [{field: (now - timedelta(seconds=1)).isoformat()}]}, now=now)
        == VOUCHER_EXPIRED
    )


def test_read_only_diagnostic_projects_only_known_field_types():
    from altegio_bot.scripts.easyweek_voucher_10eur_diagnostics import issued_shape

    report = issued_shape(
        {
            "customer": {"name": "SENTINEL_PRIVATE_NAME"},
            "vouchers": [
                {
                    "code": "SENTINEL_BEARER",
                    "activated_at": "SENTINEL_DATE",
                    "expires_at": None,
                    "hostile_key": "SENTINEL_PRIVATE_VALUE",
                }
            ],
        }
    )
    assert report["date_candidate_fields"] == [{"activated_at": "str", "expires_at": "NoneType"}]
    assert report["positive_validity_contract_supported"] is False
    assert "SENTINEL" not in json.dumps(report)


def test_read_only_diagnostic_has_no_apply_flag():
    from altegio_bot.scripts.easyweek_voucher_10eur_diagnostics import main

    with pytest.raises(SystemExit) as error:
        main(["--apply"])
    assert error.value.code == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,expected",
    [
        ([{"uuid": CUSTOMER}], True),
        ({"data": [{"uuid": CUSTOMER}]}, True),
        ([], False),
        ([{"uuid": STAFFER}], False),
        ([{"uuid": CUSTOMER}, {"uuid": CUSTOMER}], False),
        ({"data": [{"uuid": CUSTOMER}], "meta": {"last_page": 2}}, False),
        ([{"uuid": CUSTOMER}, "malformed"], False),
        ([{"uuid": CUSTOMER}, {}], False),
        ([{"uuid": CUSTOMER}, {"uuid": "not-a-uuid"}], False),
        (None, False),
    ],
)
async def test_new_account_requires_exact_pin_and_complete_unique_membership(monkeypatch, payload, expected):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from altegio_bot.campaigns.easyweek_voucher_production import account

    reader = SimpleNamespace(list_location_accounts=AsyncMock(return_value=payload))
    # Production's expected pin is fixed; synthetic identity is admitted only
    # by patching the same narrow pure boundary used by issuer tests.
    monkeypatch.setattr(account, "expected_account_fingerprint", lambda: account.account_fingerprint(CUSTOMER))
    assert (
        await account.prove_current_account(reader, account_uuid=CUSTOMER, location_uuid=KARLSRUHE_LOCATION_UUID)
        is expected
    )
    reader.list_location_accounts.assert_awaited_once_with(KARLSRUHE_LOCATION_UUID)


@pytest.mark.asyncio
async def test_unapproved_new_account_refuses_without_remote_read():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from altegio_bot.campaigns.easyweek_voucher_production.account import prove_current_account

    reader = SimpleNamespace(list_location_accounts=AsyncMock())
    assert not await prove_current_account(reader, account_uuid=CUSTOMER, location_uuid=KARLSRUHE_LOCATION_UUID)
    reader.list_location_accounts.assert_not_called()
