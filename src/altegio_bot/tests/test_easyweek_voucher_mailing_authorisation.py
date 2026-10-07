"""What may authorise a voucher purchase or a WhatsApp send (§43.4, §43.7 group B).

Every test here asks the same question from a different angle: can anything other
than an authenticated operator, confirming a specific server-held plan, cause an
external effect? The answer has to be no, and it has to be no for zero external
calls — a refusal that happens after a POST has left is not a refusal.

The Ops auth dependency is deliberately NOT overridden anywhere in this file. A
suite that replaced it with ``lambda: None`` could not tell a configured
deployment from an unconfigured one, could not see a query token being refused,
and would pass even if somebody wired the permissive dev fallback into a money
endpoint. So the session is a genuine signed cookie, and the refusals are the
application's own.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.models.models import EasyWeekVoucherProductionApproval
from altegio_bot.ops.auth import SESSION_COOKIE, make_session_token
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import (
    FakeReader,
    marker_orders,
    model_issued_validity_capability,
    seed_template_and_sender,
)
from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import (
    OPS_SECRET,
    OPS_USER,
    OTHER_OPS_USER,
    csrf_for,
    session_cookie,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    FakeMutator,
    seed_production_preview,
)
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module


@pytest.fixture
def issued_validity_capability(monkeypatch):
    """§45.2 (a) modelled as answered, so a stage of the new contract may buy.

    This module's subject is how a browser click becomes permission for one stage.
    With the real default the fixed €10 contract refuses CREATE and PAY
    outright, before any order exists. That refusal is proven WITHOUT this
    fixture in ``test_easyweek_voucher_10eur_lifecycle.py``; nothing here
    weakens it. This models question (a) only — whether any issued term could
    be proven at all — and never question (b) about one particular voucher.
    """
    model_issued_validity_capability(monkeypatch)


PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"
STOP_URL = "/ops/voucher-mailings/api/stop"
RECONCILE_URL = "/ops/voucher-mailings/api/reconcile"
STATUS_URL = "/ops/voucher-mailings/api/status"

WRITE_URLS = (PLAN_URL, CONFIRM_URL, STOP_URL, RECONCILE_URL)


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


def _body_for(url: str) -> dict[str, Any]:
    """A well-formed body for each write endpoint, so only auth is under test."""
    if url == PLAN_URL:
        return {"stage": "create", "preview_run_id": 1, "batch_id": 1}
    if url == CONFIRM_URL:
        return {"approval_id": 1, "confirmed_count": 1, "confirmed_amount_minor": 1500}
    if url == STOP_URL:
        return {"batch_id": 1}
    return {"preview_run_id": 1, "batch_id": 1}


async def _frozen(client, session_maker, transports, *, count: int) -> tuple[int, int, FakeReader]:
    """A real frozen batch, reached the only way there is: through the UI."""
    run_id, _ = await seed_production_preview(session_maker, count=count)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(count=count)
    transports.use(reader=reader)
    offer = (
        await client.post(
            PLAN_URL,
            json={
                "stage": "freeze",
                "preview_run_id": run_id,
                "expected_recipient_count": count,
                "approved_exposure_minor": count * 1000,
            },
        )
    ).json()
    assert offer["ready"], offer["reasons"]
    confirmed = await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert confirmed.status_code == 200
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert snapshot.batch_id is not None
    reader.orders.update(await marker_orders(session_maker, batch_id=snapshot.batch_id))
    return run_id, int(snapshot.batch_id), reader


async def _create_offer(client, *, run_id: int, batch_id: int) -> dict[str, Any]:
    offer = (
        await client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"], offer["reasons"]
    return offer


# ===========================================================================
# Who is allowed through the door at all
# ===========================================================================


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_an_unauthenticated_request_cannot_write(anon_client, ops_credentials, session_maker, url: str):
    """No session, no action. On every write endpoint, not just the obvious one."""
    response = await anon_client.post(url, json=_body_for(url))
    assert response.status_code in (401, 403), response.text
    assert await operations_module.list_operations(session_maker) == []


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_an_unconfigured_deployment_refuses_instead_of_allowing(
    anon_client, session_maker, monkeypatch, url: str
):
    """The dev fallback must not reach anything that spends money.

    ``require_ops_auth`` lets an unconfigured deployment through, on purpose, for
    the read-only cabinet. These endpoints must do the opposite: a machine nobody
    finished setting up is the last place a €15 purchase should be possible.
    """
    from altegio_bot.settings import settings as live

    monkeypatch.setattr(live, "ops_user", "", raising=False)
    monkeypatch.setattr(live, "ops_pass", "", raising=False)
    monkeypatch.setattr(live, "ops_secret", "", raising=False)
    monkeypatch.setattr(live, "ops_token", "", raising=False)

    response = await anon_client.post(url, json=_body_for(url))
    assert response.status_code in (401, 403), response.text
    assert "voucher_production_ops_session_required" in response.text
    assert await operations_module.list_operations(session_maker) == []


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_an_invalid_or_expired_session_cannot_write(anon_client, ops_credentials, session_maker, url: str):
    """A tampered cookie and a cookie signed with the wrong key are both refused."""
    for cookie in (
        "ops-operator:9999999999:deadbeef",
        make_session_token(OPS_USER, "the-wrong-signing-key"),
        make_session_token("somebody-else", OPS_SECRET),
    ):
        anon_client.cookies.set(SESSION_COOKIE, cookie)
        anon_client.headers.update({"X-Ops-CSRF": csrf_for(cookie)})
        response = await anon_client.post(url, json=_body_for(url))
        assert response.status_code in (401, 403), (cookie, response.text)
    assert await operations_module.list_operations(session_maker) == []


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_a_query_token_does_not_open_the_new_endpoints(anon_client, session_maker, monkeypatch, url: str):
    """The historical token path must not reach a money action.

    ``require_ops_auth`` accepts ``?token=`` and ``X-Ops-Token`` and keeps doing
    so for the dashboard. Here they must not be enough: a secret in a URL is a
    secret in browser history, in a proxy log and in a referrer header.
    """
    from altegio_bot.settings import settings as live

    monkeypatch.setattr(live, "ops_token", "synthetic-ops-token", raising=False)
    monkeypatch.setattr(live, "ops_user", OPS_USER, raising=False)
    monkeypatch.setattr(live, "ops_secret", OPS_SECRET, raising=False)

    by_query = await anon_client.post(f"{url}?token=synthetic-ops-token", json=_body_for(url))
    by_header = await anon_client.post(url, json=_body_for(url), headers={"X-Ops-Token": "synthetic-ops-token"})
    assert by_query.status_code in (401, 403), by_query.text
    assert by_header.status_code in (401, 403), by_header.text
    assert await operations_module.list_operations(session_maker) == []


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_http_basic_does_not_open_the_new_endpoints(anon_client, ops_credentials, session_maker, url: str):
    """Basic auth is a credential that travels on every request — the CSRF problem."""
    from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import OPS_PASS

    response = await anon_client.post(url, json=_body_for(url), auth=(OPS_USER, OPS_PASS))
    assert response.status_code in (401, 403), response.text
    assert await operations_module.list_operations(session_maker) == []


# ===========================================================================
# CSRF and origin
# ===========================================================================


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_a_valid_session_without_the_csrf_token_cannot_write(
    anon_client, ops_credentials, session_maker, url: str
):
    """The cookie alone is exactly what a cross-site page can cause to be sent."""
    cookie = session_cookie()
    anon_client.cookies.set(SESSION_COOKIE, cookie)
    missing = await anon_client.post(url, json=_body_for(url))
    wrong = await anon_client.post(url, json=_body_for(url), headers={"X-Ops-CSRF": "not-the-token"})
    other_session = await anon_client.post(
        url,
        json=_body_for(url),
        headers={"X-Ops-CSRF": csrf_for(make_session_token(OPS_USER, OPS_SECRET) + "x")},
    )
    for response in (missing, wrong, other_session):
        assert response.status_code in (401, 403), response.text
        assert "voucher_production_ops_csrf_invalid" in response.text
    assert await operations_module.list_operations(session_maker) == []


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_a_cross_origin_write_is_refused(anon_client, ops_credentials, session_maker, url: str):
    """Another site's page, with the browser helpfully attaching the cookie."""
    cookie = session_cookie()
    anon_client.cookies.set(SESSION_COOKIE, cookie)
    anon_client.headers.update({"X-Ops-CSRF": csrf_for(cookie)})

    # A cross-site Origin, and the sandboxed form's "null".
    for origin in ("https://evil.example", "null"):
        response = await anon_client.post(url, json=_body_for(url), headers={"Origin": origin})
        assert response.status_code in (401, 403), (origin, response.text)
        assert "voucher_production_ops_origin_rejected" in response.text

    # The Referer fallback, for the clients that send only that. It applies ONLY
    # when there is no Origin: a present, same-origin Origin is the authoritative
    # signal and a Referer cannot override it, which is why this case has to drop
    # the header rather than add one beside it.
    anon_client.headers.pop("Origin", None)
    response = await anon_client.post(url, json=_body_for(url), headers={"Referer": "https://evil.example/page"})
    assert response.status_code in (401, 403), response.text
    assert "voucher_production_ops_origin_rejected" in response.text

    assert await operations_module.list_operations(session_maker) == []


@pytest.mark.parametrize("url", WRITE_URLS)
async def test_a_write_with_no_origin_and_no_referer_is_refused(anon_client, ops_credentials, session_maker, url: str):
    """Every browser sends one of them on a cross-origin POST, so neither is not a browser."""
    cookie = session_cookie()
    anon_client.cookies.set(SESSION_COOKIE, cookie)
    anon_client.headers.update({"X-Ops-CSRF": csrf_for(cookie)})
    anon_client.headers.pop("Origin", None)
    response = await anon_client.post(url, json=_body_for(url))
    assert response.status_code in (401, 403), response.text
    assert await operations_module.list_operations(session_maker) == []


async def test_the_csrf_token_is_not_in_a_url_and_the_page_carries_no_secret(ui_client, ops_credentials):
    """A page hands the browser a CSRF token; it never puts a credential in a link."""
    page = await ui_client.get("/ops/voucher-mailings")
    assert page.status_code == 200
    assert "?token=" not in page.text
    assert OPS_SECRET not in page.text
    from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import OPS_PASS

    assert OPS_PASS not in page.text


# ===========================================================================
# The payload cannot widen the permission
# ===========================================================================


async def test_a_spoofed_actor_in_the_payload_is_ignored(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """The audit records the session's account, never one the request named."""
    run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    response = await ui_client.post(
        PLAN_URL,
        json={
            "stage": "create",
            "preview_run_id": run_id,
            "batch_id": batch_id,
            # Fields the schema does not have. They must change nothing.
            "principal": "somebody-else",
            "actor": "somebody-else",
            "operator": "somebody-else",
        },
    )
    assert response.status_code == 200, response.text
    approval = await operations_module.load_approval(
        session_maker, approval_id=response.json()["approval"]["approval_id"]
    )
    assert approval is not None
    assert approval.principal == OPS_USER


async def test_a_browser_may_not_name_the_staffer(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """§43.9: the issuer is not a field, and a request that sends one is refused.

    Refused by name rather than silently ignored — silence would leave the next
    reader of this code unsure whether the value had been honoured.
    """
    run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    response = await ui_client.post(
        PLAN_URL,
        json={
            "stage": "create",
            "preview_run_id": run_id,
            "batch_id": batch_id,
            "staffer_uuid": "11111111-1111-4111-8111-111111111111",
        },
    )
    assert response.status_code == 409
    assert response.json()["reasons"] == ["voucher_production_issuer_supplied_by_client"]
    assert await operations_module.list_operations(session_maker, batch_id=batch_id) != []
    # Only the freeze. No create approval was written at all.
    assert all(entry.stage == "freeze" for entry in await operations_module.list_operations(session_maker))


@pytest.mark.parametrize(
    ("count_delta", "amount_delta", "expected"),
    [
        (-1, 0, "voucher_production_approval_count_unconfirmed"),
        (1, 0, "voucher_production_approval_count_unconfirmed"),
        (0, -1500, "voucher_production_approval_exposure_unconfirmed"),
        (0, 1500, "voucher_production_approval_exposure_unconfirmed"),
    ],
)
async def test_a_tampered_count_or_amount_gives_zero_external_calls(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    count_delta: int,
    amount_delta: int,
    expected: str,
):
    """The confirmation numbers are compared, never used. A changed one refuses."""
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)

    response = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"] + count_delta,
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"] + amount_delta,
        },
    )
    assert response.status_code == 409
    assert expected in response.json()["reasons"]
    # No operation, so nothing for the executor to run, so no POST.
    assert await worker_module.run_once(session_maker, owner="test-executor") is None
    assert mutator.calls == []
    # And the approval is still spendable by a correct confirmation.
    approval = await operations_module.load_approval(session_maker, approval_id=offer["approval"]["approval_id"])
    assert approval is not None and approval.pending


async def test_the_browser_cannot_name_the_slots(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """``target_slots`` comes from the server's plan and from nowhere else."""
    count = 3
    run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=count)
    response = await ui_client.post(
        PLAN_URL,
        json={
            "stage": "create",
            "preview_run_id": run_id,
            "batch_id": batch_id,
            "target_slots": [1],
            "slots": [1],
        },
    )
    assert response.status_code == 200
    approval = await operations_module.load_approval(
        session_maker, approval_id=response.json()["approval"]["approval_id"]
    )
    assert approval is not None
    # Every slot the plan found, not the one the request asked for.
    assert approval.target_slots == (1, 2, 3)


# ===========================================================================
# Replay, double-click, concurrency, expiry
# ===========================================================================


async def test_one_approval_cannot_become_two_operations_even_concurrently(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """Two confirmations racing on one approval. PostgreSQL decides, and it decides once."""
    import asyncio

    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)
    body = {
        "approval_id": offer["approval"]["approval_id"],
        "confirmed_count": offer["targets"]["stage_target_count"],
        "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
    }

    first, second = await asyncio.gather(
        ui_client.post(CONFIRM_URL, json=body),
        ui_client.post(CONFIRM_URL, json=body),
        return_exceptions=True,
    )
    accepted = [r for r in (first, second) if not isinstance(r, BaseException) and r.status_code == 200]
    assert accepted, (first, second)
    operations = await operations_module.list_operations(session_maker, batch_id=batch_id)
    creates = [entry for entry in operations if entry.stage == "create"]
    assert len(creates) == 1, creates


async def test_two_operators_cannot_confirm_the_same_offer(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
    monkeypatch,
):
    """An approval id is not a capability: it belongs to the session that got it."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)

    # A second, differently named account with its own valid session.
    from altegio_bot.settings import settings as live

    monkeypatch.setattr(live, "ops_user", OTHER_OPS_USER, raising=False)
    other_cookie = make_session_token(OTHER_OPS_USER, OPS_SECRET)
    ui_client.cookies.set(SESSION_COOKIE, other_cookie)
    ui_client.headers.update({"X-Ops-CSRF": csrf_for(other_cookie)})

    response = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert response.status_code == 409
    assert "voucher_production_approval_principal_mismatch" in response.json()["reasons"]
    assert await worker_module.run_once(session_maker, owner="test-executor") is None


async def test_an_expired_plan_refuses_at_confirmation(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """Thirty minutes of reading time, and not a minute of credit afterwards."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)

    await _age_approval(session_maker, offer["approval"]["approval_id"], by=timedelta(minutes=31))

    response = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert response.status_code == 409
    assert "voucher_production_plan_expired" in response.json()["reasons"]
    assert await worker_module.run_once(session_maker, owner="test-executor") is None


async def test_a_plan_that_expires_while_queued_is_not_executed(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """Waiting in the queue is not reading time, and no worker re-approves anything.

    This is the §43.4 rule that a naive implementation gets wrong: the confirmation
    was valid when it was made, the executor was busy, and by the time the
    operation came up the world had had half an hour to change.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)
    confirmed = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert confirmed.status_code == 200

    # Time passes while the operation sits in the queue.
    await _age_approval(session_maker, offer["approval"]["approval_id"], by=timedelta(minutes=31))

    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None
    assert finished.status == "expired"
    assert finished.outcome_code == "voucher_production_plan_expired"
    # Nothing left the process.
    assert mutator.calls == []
    items = await ledger_module.load(session_maker, batch_id=batch_id)
    assert all(entry.status == "planned" for entry in items.items)


async def _age_approval(session_maker, approval_id: int, *, by: timedelta) -> None:
    """Move one approval's issue and expiry back, as the clock passing would."""
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherProductionApproval, approval_id)
            assert row is not None
            row.plan_issued_at = row.plan_issued_at - by
            row.expires_at = row.expires_at - by


# ===========================================================================
# Cross stage, cross batch, cross provider
# ===========================================================================


async def test_an_approval_for_one_stage_cannot_run_another(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """The stage is inside the signed plan, so a create approval is useless for a pay."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    transports.use(reader=reader)
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)

    # Point the stored approval at another stage, exactly as a tampered server-side
    # record would look, and confirm it.
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherProductionApproval, offer["approval"]["approval_id"])
            assert row is not None
            row.stage = "pay"

    confirmed = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert confirmed.status_code == 200
    mutator = FakeMutator(pay_sequence=[_ok(0)], create=_ok(0))
    transports.use(reader=reader, mutator=mutator)
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None
    # The rebuilt pay plan does not hash to the digest the create plan signed.
    assert finished.status == "refused"
    assert mutator.calls == []


async def test_an_approval_for_one_batch_cannot_run_a_stage_of_another(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """Two mailings in one week is the normal case, and their approvals do not mix."""
    count = 1
    run_a, batch_a, reader_a = await _frozen(ui_client, session_maker, transports, count=count)
    # A second, independent mailing from its own preview.
    run_b, _recipients = await seed_production_preview(session_maker, count=count, offset=10)
    reader_b = FakeReader(indices=[10])
    transports.use(reader=reader_b)
    offer_b = (
        await ui_client.post(
            PLAN_URL,
            json={
                "stage": "freeze",
                "preview_run_id": run_b,
                "expected_recipient_count": count,
                "approved_exposure_minor": count * 1000,
            },
        )
    ).json()
    assert offer_b["ready"], offer_b["reasons"]
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer_b["approval"]["approval_id"],
            "confirmed_count": offer_b["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer_b["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    snapshot_b = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_b)
    batch_b = int(snapshot_b.batch_id or 0)
    assert batch_b and batch_b != batch_a

    # An approval built for batch A, repointed at batch B.
    transports.use(reader=reader_a)
    offer = await _create_offer(ui_client, run_id=run_a, batch_id=batch_a)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherProductionApproval, offer["approval"]["approval_id"])
            assert row is not None
            row.batch_id = batch_b
            row.campaign_run_id = run_b

    confirmed = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert confirmed.status_code == 200
    mutator = FakeMutator(create_sequence=[_ok(10)])
    reader_b.orders.update(await marker_orders(session_maker, batch_id=batch_b, offset=10))
    transports.use(reader=reader_b, mutator=mutator)
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None
    assert finished.status == "refused"
    assert mutator.calls == []


async def test_a_plan_for_an_unknown_batch_is_refused(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A digit slip is a refusal, not a stage against the wrong month."""
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=1)
    transports.use(reader=reader)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id + 999})
    ).json()
    assert offer["ready"] is False
    assert "voucher_production_batch_unknown" in offer["reasons"]


async def test_a_confirmation_for_an_unknown_approval_is_refused(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client
):
    """An id nobody issued authorises nothing."""
    response = await ui_client.post(
        CONFIRM_URL, json={"approval_id": 987654, "confirmed_count": 1, "confirmed_amount_minor": 1500}
    )
    assert response.status_code == 409
    assert "voucher_production_approval_unknown" in response.json()["reasons"]
    assert await operations_module.list_operations(session_maker) == []


# ===========================================================================
# The authorised slot set survives a ledger change (the PR-19 fix, through the UI)
# ===========================================================================


async def test_the_approval_records_exactly_the_slots_the_operator_was_shown(
    session_maker,
    production_configuration,
    binding_key,
    issued_validity_capability,
    executor_enabled,
    ui_client,
    transports,
):
    """§42.7 through the browser: the approved slot set is stored, not re-derived.

    The sequence is the one that actually happens on a bad day: slot 1 is created,
    slot 2's create is refused by EasyWeek's own validation, and the operator plans
    a payment for what is payable. The approval must record ONE slot and €15 — not
    "whatever is payable when the worker gets round to it" — because the number the
    operator agreed to was €15.

    The narrowing inside the per-slot loop is proven at runner level by
    ``test_a_create_landing_after_the_pay_plan_is_not_paid``, with a reader that
    lands the racing CREATE inside the live rebuild. What is proven here is the
    other half: that the browser path stores the authorised set immutably, and that
    a ledger that moved between the plan and the confirmation costs zero external
    calls rather than a wider payment.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    # Slot 1 creates; slot 2's create is refused — a proven pre-action refusal, so
    # the batch is not halted and slot 2 stays claimable.
    from altegio_bot.easyweek_client import EasyWeekPermanentError

    transports.use(
        reader=reader,
        mutator=FakeMutator(create_sequence=[_ok(0), EasyWeekPermanentError("422", status_code=422)]),
    )
    offer = await _create_offer(ui_client, run_id=run_id, batch_id=batch_id)
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    states = {entry.slot: entry.status for entry in (await ledger_module.load(session_maker, batch_id=batch_id)).items}
    assert states == {1: "created", 2: "create_rejected"}, states

    # The payment the operator reads: one slot, €15, and the whole batch beside it.
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    pay_mutator = FakeMutator(pay_sequence=[_ok(0), _ok(1)], reader=reader, settles=settles)
    transports.use(reader=reader, mutator=pay_mutator)
    pay_offer = (
        await ui_client.post(PLAN_URL, json={"stage": "pay", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert pay_offer["ready"], pay_offer["reasons"]
    assert pay_offer["targets"]["target_slots"] == [1]
    assert pay_offer["targets"]["stage_target_count"] == 1
    assert pay_offer["targets"]["stage_amount_minor"] == 1000
    # The batch total is shown separately and is NOT what is being approved.
    assert pay_offer["targets"]["batch_exposure_minor"] == count * 1000

    approval = await operations_module.load_approval(session_maker, approval_id=pay_offer["approval"]["approval_id"])
    assert approval is not None
    assert approval.target_slots == (1,)
    assert approval.stage_amount_minor == 1000

    # Now slot 2's voucher appears in the window between the plan and the payment.
    await _make_slot_created(session_maker, batch_id=batch_id, slot=2, order_index=1)

    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": pay_offer["approval"]["approval_id"],
            "confirmed_count": pay_offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": pay_offer["targets"]["stage_amount_minor"],
        },
    )
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None

    # Zero payments. The ledger is not the one the plan was signed over, so the
    # live rebuild hashes differently and the stage refuses BEFORE any claim —
    # which is strictly safer than paying the one approved slot would have been.
    assert pay_mutator.calls == [], pay_mutator.calls
    assert finished.status == "refused"
    final = {entry.slot: entry.status for entry in (await ledger_module.load(session_maker, batch_id=batch_id)).items}
    assert final == {1: "created", 2: "created"}, final


async def _make_slot_created(session_maker, *, batch_id: int, slot: int, order_index: int) -> None:
    """Move one slot to ``created``, the way a CREATE landing mid-window would.

    Written directly rather than by running another stage: the point is the LEDGER
    changing between the plan and the payment, and a second UI round trip would
    only add noise to that.
    """
    from sqlalchemy import select

    from altegio_bot.models.models import EasyWeekVoucherProductionBatchItem

    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem).where(
                        EasyWeekVoucherProductionBatchItem.batch_id == batch_id,
                        EasyWeekVoucherProductionBatchItem.slot == slot,
                    )
                )
            ).scalar_one()
            row.status = "created"
            row.target_order_uuid = ORDER_UUIDS[order_index]
            row.target_order_recorded_at = row.updated_at
            row.reconciliation_required = False
            row.manual_cleanup_required = False
