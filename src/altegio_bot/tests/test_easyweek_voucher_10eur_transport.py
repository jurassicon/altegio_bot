"""Required fixed-product transport and monetary evidence regressions."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production.identity import TEMPLATE_UNPROVEN
from altegio_bot.campaigns.easyweek_voucher_production.readiness import prove_live_meta_template
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
from altegio_bot.scripts import clone_meta_templates_for_location as meta_script
from altegio_bot.settings import settings
from altegio_bot.tests import easyweek_voucher_10eur_fixtures as new
from altegio_bot.tests import easyweek_voucher_production_fixtures as old
from altegio_bot.tests.test_easyweek_voucher_production_mailing import _ok_response

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


# ===========================================================================
# F2: a Meta read that fails in transport is a refusal, never an exception
# ===========================================================================
# The reviewed boundary caught only ``ScriptError``, which is what the client
# raises for a Meta ERROR BODY. A timeout, a refused connection and a truncated
# response are not error bodies: they are ``httpx`` exceptions out of the
# client's own ``get``, and they escaped stage preparation entirely.
#
# These run the REAL ``MetaTemplateClient`` over ``httpx.MockTransport``, not a
# reader stub that raises whatever the test chose. That matters because the
# exception types under test are the ones that class actually produces, and
# because the same seam proves the APPROVED/PENDING/REJECTED handling still
# works through the production code path.

# Both planted so a leak is visible, and neither may appear in any answer.
META_TOKEN_SENTINEL = "SENTINEL-WABA-TOKEN"
META_WABA_SENTINEL = "SENTINEL-WABA-ID"
TRANSPORT_DETAIL_SENTINEL = "SENTINEL_TRANSPORT_DETAIL"

META_TRANSPORT_ERRORS = [
    pytest.param(httpx.ReadTimeout(TRANSPORT_DETAIL_SENTINEL), id="read_timeout"),
    pytest.param(httpx.ConnectError(TRANSPORT_DETAIL_SENTINEL), id="connect_error"),
    pytest.param(httpx.RemoteProtocolError(TRANSPORT_DETAIL_SENTINEL), id="remote_protocol_error"),
]


@pytest.fixture
def meta_credentials(monkeypatch):
    """A configured WABA, so the proof actually opens a client instead of short-circuiting."""
    monkeypatch.setattr(settings, "whatsapp_access_token", META_TOKEN_SENTINEL, raising=False)
    monkeypatch.setattr(settings, "meta_waba_id", META_WABA_SENTINEL, raising=False)


# Captured once, at import. Subclassing whatever is currently installed would
# stack a second transport onto an already-patched class the moment one test
# installs two of them.
_REAL_META_CLIENT = meta_script.MetaTemplateClient


def _under_mock_transport(monkeypatch, handler):
    """Put an ``httpx.MockTransport`` under the real client the proof builds."""
    transport = httpx.MockTransport(handler)

    class Mocked(_REAL_META_CLIENT):
        def __init__(self, **kwargs):
            super().__init__(**kwargs, transport=transport)

    monkeypatch.setattr(meta_script, "MetaTemplateClient", Mocked)


def break_meta_transport(monkeypatch, error: BaseException):
    """The real client, against a transport that fails the way *error* says."""

    def handler(request):
        raise error

    _under_mock_transport(monkeypatch, handler)


def serve_meta_templates(monkeypatch, *templates):
    """The real client, against a transport that answers one page of templates."""
    _under_mock_transport(monkeypatch, lambda request: httpx.Response(200, json={"data": list(templates)}))


@pytest.mark.parametrize("error", META_TRANSPORT_ERRORS)
async def test_meta_transport_failure_is_a_negative_proof_and_never_escapes(monkeypatch, meta_credentials, error):
    """Level 1: the boundary itself. A stable negative answer, with nothing leaked."""
    break_meta_transport(monkeypatch, error)
    proof = await prove_live_meta_template()
    assert not proof.proven and not proof.meta_verified
    assert proof.reason == TEMPLATE_UNPROVEN
    assert proof.meta_template_name == CONTRACT.meta_template_name
    # No token, no WABA id, no exception text, no URL.
    assert "SENTINEL" not in json.dumps(proof.as_safe_dict())


async def test_the_meta_proof_boundary_catches_transport_errors_and_nothing_wider(monkeypatch, meta_credentials):
    """A cancellation and a bug are not transport failures and must still raise.

    The narrowness is the point. ``except Exception`` here would turn a typo in
    this module into a silent "the template is not approved", and would swallow
    the cancellation that shuts an executor down.
    """
    break_meta_transport(monkeypatch, asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await prove_live_meta_template()

    break_meta_transport(monkeypatch, AttributeError("SENTINEL_BUG"))
    with pytest.raises(AttributeError):
        await prove_live_meta_template()


@pytest.mark.parametrize(
    ("status", "proven"),
    [("APPROVED", True), ("PENDING", False), ("REJECTED", False)],
)
async def test_the_approval_states_still_decide_through_the_real_client(monkeypatch, meta_credentials, status, proven):
    """The transport fix did not change what an approval state means."""
    serve_meta_templates(monkeypatch, new.meta_template(status=status))
    proof = await prove_live_meta_template()
    assert proof.proven is proven and proof.meta_verified is proven
    assert proof.reason is (None if proven else TEMPLATE_UNPROVEN)


@pytest.mark.parametrize("error", META_TRANSPORT_ERRORS)
async def test_stage_preparation_answers_a_structured_refusal_not_a_500(
    session_maker,
    production_configuration,
    binding_key,
    meta_credentials,
    ui_client,
    transports,
    monkeypatch,
    error,
):
    """Level 2: through the real stage-preparation endpoint.

    The reviewed code let the ``httpx`` error out of ``build_stage_plan``, past
    ``offer_stage``'s ``EasyWeekError``/``SQLAlchemyError`` handlers, and into
    the ASGI stack — an HTTP 500 on a page an operator is standing in front of.
    """
    break_meta_transport(monkeypatch, error)
    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    transports.use(reader=new.close_meta_read_seam(new.FakeReader(count=1)))

    answer = await ui_client.post(
        new.PLAN_URL,
        json={
            "stage": "freeze",
            "preview_run_id": run_id,
            "expected_recipient_count": 1,
            "approved_exposure_minor": CONTRACT.unit_price_minor,
        },
    )
    assert answer.status_code == 409, answer.text
    body = answer.json()
    assert body["ready"] is False
    assert TEMPLATE_UNPROVEN in body["reasons"]
    assert "SENTINEL" not in answer.text
    # A refused offer is not a batch and not an approval to spend later.
    assert not (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists


@pytest.mark.parametrize("error", META_TRANSPORT_ERRORS)
async def test_a_meta_transport_failure_after_confirmation_finishes_the_operation(
    session_maker,
    production_configuration,
    binding_key,
    meta_credentials,
    executor_enabled,
    ui_client,
    transports,
    monkeypatch,
    error,
):
    """Level 3: between a successful plan/confirm and ``worker.run_once``.

    This is the shape the review found. The failure happens while the executor
    rebuilds the plan — before a claim, before a socket to EasyWeek — and the
    reviewed code left the operation ``running`` with no ``finished_at``, its
    heartbeat cancelled, until the ten-minute lease lapsed and the sweep called
    it ``interrupted``. ``interrupted`` is reserved for a stage that may have had
    an external effect; saying it about a pre-mutation refusal sends an operator
    to reconcile something that never happened.
    """
    new.model_issued_validity_capability(monkeypatch)
    run_id, batch_id, reader = await new.ui_frozen(ui_client, session_maker, transports, count=1)

    # A healthy Meta while the operator reads and confirms the plan.
    mutator = old.FakeMutator(create_sequence=[_ok_response(0)])
    transports.use(reader=reader, mutator=mutator)
    offer = await new.ui_plan(ui_client, stage="create", preview_run_id=run_id, batch_id=batch_id)
    assert offer["ready"], offer["reasons"]
    assert (await new.ui_confirm(ui_client, offer))[0] == 200

    # Meta breaks after the confirmation and before the executor claims the row.
    break_meta_transport(monkeypatch, error)
    transports.use(reader=new.close_meta_read_seam(reader), mutator=mutator)
    finished = await new.ui_execute(session_maker)

    assert finished is not None
    # Terminal, with a finishing time, and NOT `interrupted`.
    assert finished.status == "refused", finished.result
    assert finished.finished_at is not None
    assert finished.outcome_code == "refused"
    assert TEMPLATE_UNPROVEN in finished.reason_codes
    assert "SENTINEL" not in json.dumps(finished.as_safe_dict())
    # Nothing left the process, and nothing is waiting to be reconciled.
    assert mutator.create_calls == [] and mutator.pay_calls == []
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert {item.status for item in snapshot.items} == {"planned"}
    assert not snapshot.halted and not snapshot.reconciliation_required
