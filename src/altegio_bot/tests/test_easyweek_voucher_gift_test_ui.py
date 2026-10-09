"""The isolated test's browser door: real Ops auth, strict bodies, safe replies."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.campaigns import easyweek_voucher_owner_test as owner_test
from altegio_bot.ops import voucher_gift_test as ui
from altegio_bot.settings import settings

BASE = "/ops/voucher-gift-test"


def safe_state(**overrides):
    return {
        "status_known": True,
        "scope": owner_test.SCOPE,
        "state": "new",
        "available_actions": ["create", "stop"],
        "operation_status": None,
        "stopped": False,
        "order_observed": False,
        "code_present": False,
        "reasons": [],
        **overrides,
    }


@pytest.fixture
def backend(monkeypatch, session_maker):
    monkeypatch.setattr(ui, "SessionLocal", session_maker)
    actions = {}
    for name, result in {
        "get_status": safe_state(),
        "offer": {"ready": True, "approval_id": "synthetic-approval", "stage": "create"},
        "confirm": {"accepted": True},
        "stop": {"accepted": True},
        "reconcile": {"accepted": True},
    }.items():
        actions[name] = AsyncMock(return_value=result)
        monkeypatch.setattr(owner_test, name, actions[name])
    return actions


async def test_page_is_private_and_has_no_customer_selector(ui_client, backend):
    result = await ui_client.get(BASE)
    assert result.status_code == 200
    assert "no-store" in result.headers["cache-control"]
    assert "Aktionsgutscheine" in result.text
    assert "10,00 €" in result.text and "0,00 €" in result.text
    assert "gift-loading" in result.text and "spinner-border" in result.text
    assert 'name="customer_uuid"' not in result.text
    assert "gift-deliver" not in result.text
    assert "preview #44" not in result.text
    for action in backend.values():
        action.assert_not_called()


@pytest.mark.parametrize("path", [BASE, BASE + "/api/status"])
async def test_read_requires_a_real_session_even_when_dev_auth_is_open(anon_client, monkeypatch, backend, path):
    monkeypatch.setattr(settings, "ops_user", "")
    monkeypatch.setattr(settings, "ops_token", "")
    result = await anon_client.get(path)
    assert result.status_code == 403
    backend["get_status"].assert_not_called()


@pytest.mark.parametrize("action,body", [("plan", {"stage": "create"}), ("stop", {}), ("reconcile", {})])
async def test_mutating_door_requires_csrf(ui_client, backend, action, body):
    del ui_client.headers["X-Ops-CSRF"]
    result = await ui_client.post(BASE + "/api/" + action, json=body)
    assert result.status_code == 403
    for method in backend.values():
        method.assert_not_called()


async def test_cross_origin_plan_refuses_before_backend(ui_client, backend):
    result = await ui_client.post(
        BASE + "/api/plan", json={"stage": "create"}, headers={"Origin": "https://hostile.test"}
    )
    assert result.status_code == 403
    backend["offer"].assert_not_called()


@pytest.mark.parametrize(
    "field", ["customer_uuid", "account_uuid", "staffer_uuid", "preview_run_id", "principal", "price"]
)
async def test_plan_cannot_take_a_customer_product_or_actor_from_browser(ui_client, backend, field):
    result = await ui_client.post(BASE + "/api/plan", json={"stage": "create", field: "untrusted"})
    assert result.status_code == 422
    backend["offer"].assert_not_called()


@pytest.mark.parametrize("stage", ["deliver", "refund", "freeze", "CREATE"])
async def test_only_create_and_pay_may_be_planned(ui_client, backend, stage):
    result = await ui_client.post(BASE + "/api/plan", json={"stage": stage})
    assert result.status_code == 422
    backend["offer"].assert_not_called()


async def test_plan_and_confirm_delegate_only_to_the_queue_layer(ui_client, backend, session_maker):
    result = await ui_client.post(BASE + "/api/plan", json={"stage": "create"})
    assert result.status_code == 200
    assert "no-store" in result.headers["cache-control"]
    assert backend["offer"].call_args.args == (session_maker,)
    principal = backend["offer"].call_args.kwargs["principal"]
    assert principal.account == "ops-operator"
    payload = {
        "approval_id": result.json()["approval_id"],
        "confirmed_count": 1,
        "confirmed_nominal_minor": 1000,
        "confirmed_issue_minor": 0,
    }
    confirmed = await ui_client.post(BASE + "/api/confirm", json=payload)
    assert confirmed.status_code == 200 and confirmed.json()["accepted"]
    backend["confirm"].assert_awaited_once_with(session_maker, principal=principal, **payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("confirmed_count", 0),
        ("confirmed_count", 2),
        ("confirmed_count", True),
        ("confirmed_nominal_minor", 0),
        ("confirmed_nominal_minor", "1000"),
        ("confirmed_issue_minor", 1000),
        ("confirmed_issue_minor", False),
    ],
)
async def test_zero_price_does_not_remove_exact_confirmation(ui_client, backend, field, value):
    payload = {
        "approval_id": "synthetic-approval",
        "confirmed_count": 1,
        "confirmed_nominal_minor": 1000,
        "confirmed_issue_minor": 0,
        field: value,
    }
    result = await ui_client.post(BASE + "/api/confirm", json=payload)
    assert result.status_code == 422
    backend["confirm"].assert_not_called()


async def test_status_rejects_an_arbitrary_scope(ui_client, backend):
    result = await ui_client.get(BASE + "/api/status?customer_uuid=other")
    assert result.status_code == 400
    backend["get_status"].assert_not_called()


async def test_database_error_does_not_claim_empty_or_ready_state(ui_client, backend):
    backend["get_status"].side_effect = SQLAlchemyError("sensitive SQL must not be exposed")
    result = await ui_client.get(BASE + "/api/status")
    assert result.status_code == 503
    assert result.json()["status_known"] is False
    assert "sensitive SQL" not in result.text


async def test_named_backend_refusal_is_displayable_without_raw_detail(ui_client, backend):
    backend["offer"].side_effect = owner_test.OwnerTestError("owner_test_disabled")
    result = await ui_client.post(BASE + "/api/plan", json={"stage": "create"})
    assert result.status_code == 409
    assert result.json() == {"accepted": False, "ready": False, "reasons": ["owner_test_disabled"]}
