"""PR-21 strict bulk check/confirm HTTP boundaries, with only synthetic CRM reads."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select

import altegio_bot.ops.voucher_mailing as voucher_ops
from altegio_bot.models.models import CampaignRecipient, Client
from altegio_bot.tests.easyweek_voucher_mixed_ui_fixtures import MixedReader, seed_mixed_editor
from altegio_bot.tests.easyweek_voucher_production_fixtures import PHONES

CHECK = "/ops/voucher-mailings/api/recipients/check"
CONFIRM = "/ops/voucher-mailings/api/recipients/confirm"


def check_payload(run_id, phones=None):
    return {
        "preview_run_id": run_id,
        "phones": phones if phones is not None else PHONES[1],
        "prior_altegio_visit_confirmed": True,
        "assign_karlsruhe_confirmed": True,
    }


async def test_bulk_http_read_then_explicit_subset_confirm_is_atomic_and_replay_safe(
    session_maker, ui_client, production_configuration, monkeypatch
):
    run_id, earned_id = await seed_mixed_editor(session_maker)
    reader = MixedReader()
    monkeypatch.setattr(voucher_ops, "EasyWeekClient", lambda: reader)
    before = None
    async with session_maker() as session:
        before = await session.scalar(select(func.count()).select_from(Client))
    response = await ui_client.post(CHECK, json=check_payload(run_id, "\n".join([PHONES[0], PHONES[1], PHONES[1]])))
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    plan = response.json()
    assert plan["eligible_count"] == 1
    assert plan["already_present_count"] == 1
    assert plan["duplicate_count"] == 1
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(Client)) == before
        assert await session.scalar(select(func.count()).select_from(CampaignRecipient)) == 1
    confirm = {"preview_run_id": run_id, "plan_id": plan["plan_id"], "confirmed_count": 1}
    first = await ui_client.post(CONFIRM, json=confirm)
    assert first.status_code == 200, first.text
    second = await ui_client.post(CONFIRM, json=confirm)
    assert second.json() == first.json()
    async with session_maker() as session:
        earned = await session.get(CampaignRecipient, earned_id)
        assert earned.recipient_basis == "earned_first_visit"
        assert await session.scalar(select(func.count()).select_from(CampaignRecipient)) == 2


@pytest.mark.parametrize("endpoint", [CHECK, CONFIRM])
async def test_bulk_routes_require_csrf_and_origin(ui_client, endpoint, monkeypatch):
    monkeypatch.setattr(voucher_ops, "EasyWeekClient", lambda: pytest.fail("no provider on refused auth"))
    payload = check_payload(1) if endpoint == CHECK else {"preview_run_id": 1, "plan_id": "x", "confirmed_count": 1}
    response = await ui_client.post(endpoint, json=payload, headers={"X-Ops-CSRF": ""})
    assert response.status_code == 403
    response = await ui_client.post(endpoint, json=payload, headers={"Origin": "https://foreign.invalid"})
    assert response.status_code == 403


@pytest.mark.parametrize("endpoint", [CHECK, CONFIRM])
async def test_bulk_routes_refuse_unconfigured_auth(anon_client, monkeypatch, endpoint):
    from altegio_bot.settings import settings

    monkeypatch.setattr(settings, "ops_user", "")
    monkeypatch.setattr(voucher_ops, "EasyWeekClient", lambda: pytest.fail("no provider on refused auth"))
    response = await anon_client.post(endpoint, json=check_payload(1))
    assert response.status_code in (401, 403)


@pytest.mark.parametrize("extra", [{"customer_uuid": "browser-choice"}, {"phones": "x" * 25000}])
async def test_bulk_routes_bound_body_and_reject_extra_identity_fields(ui_client, monkeypatch, extra):
    monkeypatch.setattr(voucher_ops, "EasyWeekClient", lambda: pytest.fail("no provider on malformed input"))
    response = await ui_client.post(CHECK, json={**check_payload(1), **extra})
    assert response.status_code == 400
    assert response.json()["reason"] == "manual_batch_input_invalid"


async def test_bulk_route_rejects_overlap_before_live_reads(ui_client, monkeypatch):
    entered = asyncio.Event()
    released = asyncio.Event()
    reader = MixedReader()
    monkeypatch.setattr(voucher_ops, "EasyWeekClient", lambda: reader)

    async def delayed(*args, **kwargs):
        entered.set()
        await released.wait()
        return {"ok": True, "eligible_count": 0, "rows": []}

    monkeypatch.setattr(voucher_ops.easyweek_manual_batch, "check_manual_recipients", delayed)
    first = asyncio.create_task(ui_client.post(CHECK, json=check_payload(1)))
    await entered.wait()
    try:
        second = await ui_client.post(CHECK, json=check_payload(1))
        assert second.status_code == 429
    finally:
        released.set()
        await first
    assert not voucher_ops._MANUAL_BATCH_IN_FLIGHT


async def test_preview_shows_mixed_workflow_and_separate_fence(ui_client, session_maker, production_configuration):
    run_id, _ = await seed_mixed_editor(session_maker)
    response = await ui_client.get(f"/ops/campaigns/{run_id}")
    assert response.status_code == 200
    assert 'id="manual-batch-editor"' in response.text
    assert "Automatic: 1" in response.text
    assert "Administrative mailing fence:" in response.text
    assert "Executor:" in response.text
    assert 'id="preview-vouchers-link"' in response.text
    assert "только controlled voucher delivery canary" not in response.text
    index = await ui_client.get("/ops/voucher-mailings")
    assert f"/ops/voucher-mailings/prepare?preview_run_id={run_id}" in index.text


async def test_single_add_requires_csrf_for_new_identity_and_keeps_historical_manual_policy(
    session_maker, ui_client, production_configuration, monkeypatch
):
    import altegio_bot.ops.campaigns_api as campaign_ops

    run_id, _ = await seed_mixed_editor(session_maker)
    reader = MixedReader()
    monkeypatch.setattr(campaign_ops, "EasyWeekClient", lambda: reader)
    payload = {"phone": PHONES[1], "assign_karlsruhe": True}
    endpoint = f"/ops/campaigns/runs/{run_id}/recipients/add-manual"
    denied = await ui_client.post(endpoint, json=payload, headers={"X-Ops-CSRF": ""})
    assert denied.status_code == 403
    assert not reader.customer_calls
    response = await ui_client.post(endpoint, json=payload)
    assert response.status_code == 201, response.text
    assert not reader.history_calls  # This older manual mode does not assert zero bookings.
    async with session_maker() as session:
        row = await session.get(CampaignRecipient, response.json()["recipient_id"])
        assert row.recipient_basis == "operator_manual_selection"
        assert row.manual_policy is None
        client = await session.get(Client, row.client_id)
        assert client.altegio_client_id is None
        assert client.easyweek_visits_total is None
