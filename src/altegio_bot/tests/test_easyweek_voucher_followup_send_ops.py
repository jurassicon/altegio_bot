"""§46.8 — the operator's send scenario over HTTP: plan, confirm, stop, status.

What is real here
-----------------
The FastAPI app, the routes, the rendered page, the session cookie, the CSRF
derivation, the origin check, the approval's signature and TTL, the ledger and
the executor. The Ops auth dependency is deliberately NOT overridden: a suite
that replaced it with ``lambda: None`` could not tell a configured deployment
from an unconfigured one, and could not fail if somebody later wired the
permissive fallback into the endpoint that sends messages.

Auth preconditions are stated, not assumed
------------------------------------------
``ops_credentials`` configures a synthetic cabinet for every client fixture
here, including the anonymous one, so a refusal is a CONFIGURED deployment
refusing a visitor rather than whatever this machine happens to have in its
environment. Each negative case has a positive control beside it, and the
anonymous client asserts that it really carries nothing.

The question every test asks
----------------------------
Can a request that is not an authenticated operator's deliberate confirmation
produce a message? The answer has to be no — and every refusal has to leave the
ledger byte-for-byte as it was.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any, Final

import pytest
import pytest_asyncio
import respx
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import func, select

import altegio_bot.ops.router as ops_router_module
import altegio_bot.ops.voucher_followup as followup_ops
import altegio_bot.ops.voucher_mailing as voucher_mailing_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as send_ids
from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as ledger_module
from altegio_bot.models.models import (
    EasyWeekVoucherFollowupApproval,
    EasyWeekVoucherFollowupRun,
    EasyWeekVoucherFollowupSend,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.ops.auth import SESSION_COOKIE
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    TemplateReader,
    location_map,
    person,
)
from altegio_bot.tests.easyweek_voucher_followup_send_fixtures import (
    CountingSender,
    StubTransports,
    build_preview,
    seed_ready_batch,
)
from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import (
    BASE_URL,
    ORIGIN,
    OTHER_OPS_USER,
    csrf_for,
    session_cookie,
)
from altegio_bot.tests.test_easyweek_voucher_followup_preview import FakeHistoryReader
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_voucher_followup_worker as worker_module

PAGE = "/ops/voucher-mailings/{batch_id}/followup"
PLAN = "/ops/voucher-mailings/{batch_id}/followup/plan"
CONFIRM = "/ops/voucher-mailings/{batch_id}/followup/confirm"
STOP = "/ops/voucher-mailings/{batch_id}/followup/stop"
STATUS = "/ops/voucher-mailings/{batch_id}/followup/status"

API_ACCEPT: Final = "application/json"
BROWSER_ACCEPT: Final = "text/html,application/xhtml+xml"


@pytest.fixture
def no_network() -> Any:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture(autouse=True)
def binding_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)


@pytest.fixture(autouse=True)
def meta_template(monkeypatch: pytest.MonkeyPatch) -> TemplateReader:
    """The live Meta listing seam, answered from a fixture.

    Patched where the routes reach it, so the HTTP requests get the fake. A
    per-call argument could not reach code a browser drives.
    """
    reader = TemplateReader()
    original = followup_ops.preview_module.prove_meta_followup_template

    async def with_reader(given: Any = None) -> Any:
        return await original(reader)

    monkeypatch.setattr(followup_ops.preview_module, "prove_meta_followup_template", with_reader)
    return reader


@pytest_asyncio.fixture
async def ops_client(session_maker, monkeypatch, ops_credentials) -> AsyncIterator[AsyncClient]:
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(voucher_mailing_module, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    cookie = session_cookie()
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, cookie)
        client.headers.update({"X-Ops-CSRF": csrf_for(cookie), "Origin": ORIGIN})
        yield client


@pytest_asyncio.fixture
async def anonymous_client(session_maker, monkeypatch, ops_credentials) -> AsyncIterator[AsyncClient]:
    """A visitor with no credentials at all, against a CONFIGURED cabinet."""
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL, follow_redirects=False) as client:
        client.headers.update({"Origin": ORIGIN})
        yield client


def _carries_no_credentials(client: AsyncClient) -> None:
    assert client.cookies.get(SESSION_COOKIE) is None
    sent = {name.lower() for name in client.headers}
    assert "authorization" not in sent
    assert "x-ops-token" not in sent


async def _state(session_maker) -> dict[str, int]:
    """Everything a refused request must leave exactly as it was."""
    async with session_maker() as session:
        return {
            "approvals": int(
                (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupApproval))).scalar_one()
            ),
            "runs": int(
                (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupRun))).scalar_one()
            ),
            "sends": int(
                (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupSend))).scalar_one()
            ),
            "jobs": int((await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()),
            "outbox": int((await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()),
        }


async def _ready(session_maker, *, slots: list[dict[str, Any]] | None = None) -> tuple[int, Any]:
    """A seeded mailing with a preview that is CURRENT by the served clock.

    The routes read the real clock and cannot be handed one, so the preview is
    built at ``utcnow()`` rather than at the fixtures' fixed instant: a snapshot
    from last month is correctly refused as stale, and a suite that ignored that
    would be testing the refusal instead of the scenario.
    """
    batch_id = await seed_ready_batch(session_maker, slots=slots)
    reader = FakeHistoryReader(batch_id=batch_id)
    await build_preview(session_maker, batch_id=batch_id, reader=reader, now=utcnow())
    return batch_id, reader


# ---------------------------------------------------------------------------
# Authorisation (§46.8)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("url", [PLAN, CONFIRM, STOP])
async def test_an_anonymous_visitor_cannot_reach_any_send_action(
    session_maker, anonymous_client, no_network, send_fence, url
) -> None:
    batch_id, _reader = await _ready(session_maker)
    _carries_no_credentials(anonymous_client)
    before = await _state(session_maker)
    response = await anonymous_client.post(
        url.format(batch_id=batch_id),
        json={"approval_id": 1, "plan_signature": "a" * 64, "confirmed_count": 1, "run_id": 1},
        headers={"Accept": API_ACCEPT},
    )
    assert response.status_code in (401, 403, 422)
    assert await _state(session_maker) == before


async def test_a_missing_csrf_header_cannot_prepare_a_send(
    session_maker, monkeypatch, ops_credentials, no_network, send_fence
) -> None:
    """A cookie alone is not enough: another origin can make a browser send it."""
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    cookie = session_cookie()
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, cookie)
        client.headers.update({"Origin": ORIGIN})
        response = await client.post(PLAN.format(batch_id=batch_id), json={})
    assert response.status_code == 403
    # The section's own PII-free vocabulary, shared with the §42 endpoints.
    assert response.json()["reasons"] == ["voucher_production_ops_csrf_invalid"]
    assert response.json()["detail"] == "ops_csrf_missing"
    assert await _state(session_maker) == before


async def test_a_cross_origin_request_cannot_prepare_a_send(
    session_maker, monkeypatch, ops_credentials, no_network, send_fence
) -> None:
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    cookie = session_cookie()
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, cookie)
        client.headers.update({"X-Ops-CSRF": csrf_for(cookie), "Origin": "http://evil.test"})
        response = await client.post(PLAN.format(batch_id=batch_id), json={})
    assert response.status_code == 403
    assert response.json()["reasons"] == ["voucher_production_ops_origin_rejected"]
    assert await _state(session_maker) == before


async def test_a_token_authenticated_script_cannot_prepare_a_send(
    session_maker, monkeypatch, ops_credentials, no_network, send_fence
) -> None:
    """§43.7's stricter door: a query or header token is not a browser session."""
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(settings, "ops_token", "synthetic-ops-token", raising=False)
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.headers.update({"X-Ops-Token": "synthetic-ops-token", "Origin": ORIGIN})
        response = await client.post(PLAN.format(batch_id=batch_id), json={})
    assert response.status_code in (401, 403)
    assert await _state(session_maker) == before


# ---------------------------------------------------------------------------
# The whole operator scenario, authorised (§46.8)
# ---------------------------------------------------------------------------


async def test_the_authorised_scenario_reaches_exactly_one_message_per_recipient(
    session_maker, ops_client, no_network, send_fence
) -> None:
    """The positive control, and the shape of the whole §46.8 path.

    Without this every refusal above would also hold for an endpoint that
    refuses everybody.
    """
    batch_id, reader = await _ready(session_maker, slots=[{"state": "delivered"}, {"state": "delivered"}])

    page = await ops_client.get(PAGE.format(batch_id=batch_id), headers={"Accept": BROWSER_ACCEPT})
    assert page.status_code == 200
    assert "Подтверждение отправки" in page.text

    plan = await ops_client.post(PLAN.format(batch_id=batch_id), json={})
    assert plan.status_code == 200, plan.text
    offer = plan.json()
    assert offer["accepted"] is True
    assert offer["approval"]["approved_recipient_count"] == 2

    confirmed = await ops_client.post(
        CONFIRM.format(batch_id=batch_id),
        json={
            "approval_id": offer["approval"]["approval_id"],
            "plan_signature": offer["plan_signature"],
            "confirmed_count": 2,
        },
    )
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["created"] is True
    assert confirmed.json()["planned"] == 2
    # Still nothing sent: the confirmation queues, the executor sends.
    sender = CountingSender()
    assert sender.count == 0

    finished = await worker_module.run_once(
        session_maker,
        owner="executor-http",
        transports=StubTransports(reader=reader, sender=sender),
        heartbeat_interval=60.0,
    )
    assert finished is not None and finished.finished
    assert sender.count == 2

    status = await ops_client.get(STATUS.format(batch_id=batch_id))
    assert status.status_code == 200
    body = status.json()
    assert body["counters"].get("accepted") == 2
    assert len(body["rows"]) == 2
    # The follow-up's own statuses, kept apart from the original's group.
    assert {row["group"] for row in body["rows"]} == {"delivered_unread"}
    assert {row["status"] for row in body["rows"]} == {"accepted"}


async def test_a_double_click_produces_one_run(session_maker, ops_client, no_network, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    payload = {
        "approval_id": offer["approval"]["approval_id"],
        "plan_signature": offer["plan_signature"],
        "confirmed_count": offer["approval"]["approved_recipient_count"],
    }
    first = await ops_client.post(CONFIRM.format(batch_id=batch_id), json=payload)
    second = await ops_client.post(CONFIRM.format(batch_id=batch_id), json=payload)
    assert first.status_code == 200 and first.json()["created"] is True
    assert second.status_code == 200 and second.json()["created"] is False
    assert second.json()["run_id"] == first.json()["run_id"]
    async with session_maker() as session:
        assert (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupRun))).scalar_one() == 1


async def test_two_tabs_holding_two_offers_produce_one_run(session_maker, ops_client, no_network, send_fence) -> None:
    """Two prepared offers, both confirmed: one run, and the second is refused.

    This is the two-tab case §46.8 names, and the refusal is the batch's own
    admission rule rather than a disabled button.
    """
    batch_id, _reader = await _ready(session_maker)
    first_tab = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    second_tab = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    assert first_tab["approval"]["approval_id"] != second_tab["approval"]["approval_id"]

    first = await ops_client.post(
        CONFIRM.format(batch_id=batch_id),
        json={
            "approval_id": first_tab["approval"]["approval_id"],
            "plan_signature": first_tab["plan_signature"],
            "confirmed_count": first_tab["approval"]["approved_recipient_count"],
        },
    )
    second = await ops_client.post(
        CONFIRM.format(batch_id=batch_id),
        json={
            "approval_id": second_tab["approval"]["approval_id"],
            "plan_signature": second_tab["plan_signature"],
            "confirmed_count": second_tab["approval"]["approved_recipient_count"],
        },
    )
    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["reasons"] == [send_ids.RUN_ALREADY_ACTIVE]
    async with session_maker() as session:
        assert (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupRun))).scalar_one() == 1


async def test_a_session_of_an_account_the_cabinet_does_not_configure_is_not_a_session(
    session_maker, monkeypatch, ops_credentials, no_network, send_fence
) -> None:
    """Who may confirm is the configured ACCOUNT, proven at the door.

    The cabinet configures one operator account, so "another operator" cannot
    present a valid session at all: a cookie signed for a different user is
    refused by ``resolve_ops_session`` before any approval is read. The
    complementary property — that an approval issued to one principal cannot be
    spent by a different one — is proven at the ledger, where the principal
    really can differ.

    What this also states, deliberately, is that a SECOND session of the same
    configured operator may confirm: the same human on another device is not a
    different actor, and the audit records which session acted.
    """
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    batch_id, _reader = await _ready(session_maker)

    foreign = session_cookie(OTHER_OPS_USER)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, foreign)
        client.headers.update({"X-Ops-CSRF": csrf_for(foreign), "Origin": ORIGIN})
        refused = await client.post(PLAN.format(batch_id=batch_id), json={})
    # Refused by the cabinet's own door, before this endpoint's body runs: the
    # cookie is not a session of the configured account at all.
    assert refused.status_code == 401
    assert "approval" not in refused.text
    async with session_maker() as session:
        assert (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupApproval))
        ).scalar_one() == 0

    # The positive control: the configured operator, on a second session.
    first = session_cookie()
    second = session_cookie()
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, first)
        client.headers.update({"X-Ops-CSRF": csrf_for(first), "Origin": ORIGIN})
        offer = (await client.post(PLAN.format(batch_id=batch_id), json={})).json()
    assert offer["accepted"] is True
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, second)
        client.headers.update({"X-Ops-CSRF": csrf_for(second), "Origin": ORIGIN})
        confirmed = await client.post(
            CONFIRM.format(batch_id=batch_id),
            json={
                "approval_id": offer["approval"]["approval_id"],
                "plan_signature": offer["plan_signature"],
                "confirmed_count": offer["approval"]["approved_recipient_count"],
            },
        )
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["created"] is True


async def test_an_offer_of_another_mailing_cannot_be_confirmed_on_this_page(
    session_maker, ops_client, no_network, send_fence
) -> None:
    """The path's batch is part of the authorisation, not decoration."""
    first_batch, _reader = await _ready(session_maker, slots=[{"state": "delivered", "index": 1}])
    second_batch, _second = await _ready(session_maker, slots=[{"state": "delivered", "index": 7}])
    offer = (await ops_client.post(PLAN.format(batch_id=first_batch), json={})).json()
    refused = await ops_client.post(
        CONFIRM.format(batch_id=second_batch),
        json={
            "approval_id": offer["approval"]["approval_id"],
            "plan_signature": offer["plan_signature"],
            "confirmed_count": offer["approval"]["approved_recipient_count"],
        },
    )
    assert refused.status_code == 409
    assert refused.json()["reasons"] == [send_ids.APPROVAL_UNKNOWN]


async def test_a_replayed_signature_from_an_expired_offer_is_refused(
    session_maker, ops_client, no_network, send_fence, monkeypatch
) -> None:
    batch_id, _reader = await _ready(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    # The offer ages out. Its signature still verifies — that is the point of
    # having a TTL as well as a signature.
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(EasyWeekVoucherFollowupApproval, int(offer["approval"]["approval_id"]))
            assert row is not None
            row.issued_at = row.issued_at - timedelta(hours=2)
            row.expires_at = row.expires_at - timedelta(hours=2)
            row.plan_signature = ledger_module.plan_signature(ledger_module._approval_material(row))
    refused = await ops_client.post(
        CONFIRM.format(batch_id=batch_id),
        json={
            "approval_id": offer["approval"]["approval_id"],
            "plan_signature": row.plan_signature,
            "confirmed_count": offer["approval"]["approved_recipient_count"],
        },
    )
    assert refused.status_code == 409
    assert refused.json()["reasons"] == [send_ids.APPROVAL_EXPIRED]


async def test_a_wrong_count_is_refused_and_starts_nothing(session_maker, ops_client, no_network, send_fence) -> None:
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    refused = await ops_client.post(
        CONFIRM.format(batch_id=batch_id),
        json={
            "approval_id": offer["approval"]["approval_id"],
            "plan_signature": offer["plan_signature"],
            "confirmed_count": offer["approval"]["approved_recipient_count"] + 1,
        },
    )
    assert refused.status_code == 409
    assert refused.json()["reasons"] == [send_ids.APPROVAL_COUNT_UNCONFIRMED]
    after = await _state(session_maker)
    assert after["runs"] == before["runs"] == 0
    assert after["sends"] == before["sends"] == 0


async def test_a_request_that_tries_to_name_a_recipient_list_is_refused_by_name(
    session_maker, ops_client, no_network, send_fence
) -> None:
    """``extra="forbid"``: a browser may choose a version, and nothing else."""
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    response = await ops_client.post(
        PLAN.format(batch_id=batch_id),
        json={"preview_version": 1, "recipients": [1, 2], "composition_digest": "d" * 64},
    )
    assert response.status_code == 422
    assert await _state(session_maker) == before


# ---------------------------------------------------------------------------
# The fence over HTTP (§46.8)
# ---------------------------------------------------------------------------


async def test_every_send_action_refuses_while_the_followup_fence_is_closed(
    session_maker, ops_client, no_network, primary_fence_only
) -> None:
    """The primary mailing permitted, the follow-up's own switch closed."""
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    plan = await ops_client.post(PLAN.format(batch_id=batch_id), json={})
    assert plan.status_code == 409
    assert plan.json()["reasons"] == [send_ids.SEND_DISABLED]
    assert "отдельным переключателем" in plan.json()["detail"]
    assert await _state(session_maker) == before


# ---------------------------------------------------------------------------
# The GET is a read (§46.8)
# ---------------------------------------------------------------------------


async def test_opening_the_page_and_the_status_starts_nothing(
    session_maker, ops_client, no_network, send_fence
) -> None:
    batch_id, _reader = await _ready(session_maker)
    before = await _state(session_maker)
    for _ in range(3):
        assert (await ops_client.get(PAGE.format(batch_id=batch_id))).status_code == 200
        assert (await ops_client.get(STATUS.format(batch_id=batch_id))).status_code == 200
    assert await _state(session_maker) == before


async def test_reloading_the_page_after_a_confirmation_does_not_duplicate_the_run(
    session_maker, ops_client, no_network, send_fence
) -> None:
    batch_id, _reader = await _ready(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    await ops_client.post(
        CONFIRM.format(batch_id=batch_id),
        json={
            "approval_id": offer["approval"]["approval_id"],
            "plan_signature": offer["plan_signature"],
            "confirmed_count": offer["approval"]["approved_recipient_count"],
        },
    )
    before = await _state(session_maker)
    for _ in range(3):
        assert (await ops_client.get(PAGE.format(batch_id=batch_id))).status_code == 200
    assert await _state(session_maker) == before


# ---------------------------------------------------------------------------
# STOP over HTTP (§46.4, §46.7)
# ---------------------------------------------------------------------------


async def test_the_operator_can_stop_a_run_for_good_and_the_page_says_so(
    session_maker, ops_client, no_network, send_fence
) -> None:
    batch_id, reader = await _ready(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    confirmed = (
        await ops_client.post(
            CONFIRM.format(batch_id=batch_id),
            json={
                "approval_id": offer["approval"]["approval_id"],
                "plan_signature": offer["plan_signature"],
                "confirmed_count": offer["approval"]["approved_recipient_count"],
            },
        )
    ).json()

    stopped = await ops_client.post(STOP.format(batch_id=batch_id), json={"run_id": confirmed["run_id"]})
    assert stopped.status_code == 200
    assert stopped.json()["tombstoned"] == 2
    assert stopped.json()["already_stopped"] is False

    # And the executor, run afterwards, sends nobody.
    sender = CountingSender()
    assert (
        await worker_module.run_once(
            session_maker,
            owner="executor-http",
            transports=StubTransports(reader=reader, sender=sender),
            heartbeat_interval=60.0,
        )
        is None
    )
    assert sender.count == 0

    page = await ops_client.get(PAGE.format(batch_id=batch_id))
    assert "Остановлена оператором" in page.text
    assert "остановка окончательная" in page.text.lower()


async def test_stopping_is_available_with_the_fence_closed(
    session_maker, ops_client, no_network, send_fence, monkeypatch
) -> None:
    """A stop is the one action that must keep working after everything else stops."""
    batch_id, _reader = await _ready(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    confirmed = (
        await ops_client.post(
            CONFIRM.format(batch_id=batch_id),
            json={
                "approval_id": offer["approval"]["approval_id"],
                "plan_signature": offer["plan_signature"],
                "confirmed_count": offer["approval"]["approved_recipient_count"],
            },
        )
    ).json()
    monkeypatch.setattr(settings, "easyweek_voucher_followup_send_enabled", False, raising=False)
    stopped = await ops_client.post(STOP.format(batch_id=batch_id), json={"run_id": confirmed["run_id"]})
    assert stopped.status_code == 200
    assert stopped.json()["applied"] is True


async def test_a_run_of_another_mailing_cannot_be_stopped_from_this_page(
    session_maker, ops_client, no_network, send_fence
) -> None:
    first_batch, _reader = await _ready(session_maker, slots=[{"state": "delivered", "index": 1}])
    second_batch, _second = await _ready(session_maker, slots=[{"state": "delivered", "index": 7}])
    offer = (await ops_client.post(PLAN.format(batch_id=first_batch), json={})).json()
    confirmed = (
        await ops_client.post(
            CONFIRM.format(batch_id=first_batch),
            json={
                "approval_id": offer["approval"]["approval_id"],
                "plan_signature": offer["plan_signature"],
                "confirmed_count": offer["approval"]["approved_recipient_count"],
            },
        )
    ).json()
    refused = await ops_client.post(STOP.format(batch_id=second_batch), json={"run_id": confirmed["run_id"]})
    assert refused.status_code == 409
    assert refused.json()["reasons"] == [send_ids.APPROVAL_UNKNOWN]
    run = await ledger_module.load_run(session_maker, run_id=int(confirmed["run_id"]))
    assert run is not None and run.stop_requested_at is None


# ---------------------------------------------------------------------------
# Redaction (§46.8)
# ---------------------------------------------------------------------------


async def test_no_personal_value_or_secret_appears_on_the_page_or_in_any_answer(
    session_maker, ops_client, no_network, send_fence
) -> None:
    """No voucher code, no phone, no name, no uuid, no rendered BODY, no secret."""
    batch_id, reader = await _ready(session_maker)
    offer = (await ops_client.post(PLAN.format(batch_id=batch_id), json={})).json()
    confirmed = (
        await ops_client.post(
            CONFIRM.format(batch_id=batch_id),
            json={
                "approval_id": offer["approval"]["approval_id"],
                "plan_signature": offer["plan_signature"],
                "confirmed_count": offer["approval"]["approved_recipient_count"],
            },
        )
    ).json()
    await worker_module.run_once(
        session_maker,
        owner="executor-http",
        transports=StubTransports(reader=reader, sender=CountingSender()),
        heartbeat_interval=60.0,
    )
    page = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    status = (await ops_client.get(STATUS.format(batch_id=batch_id))).text
    forbidden = [
        person(1)["phone"],
        person(2)["phone"],
        person(1)["phone"].lstrip("+"),
        person(1)["customer_uuid"],
        "FOLLOWUP-TEST-CODE",
        "Person 1",
        settings.easyweek_voucher_delivery_hmac_key.get_secret_value(),
        "synthetic-ops-signing-secret",
        "111111111111111",
    ]
    for value in forbidden:
        assert str(value) not in page, value
        assert str(value) not in status, value
    assert str(confirmed["run_id"]) in status
    # The message is shown as a shape, never rendered with anybody's data.
    assert "[Vorname]" in page


async def test_the_page_states_what_accepted_and_unknown_do_not_mean(
    session_maker, ops_client, no_network, send_fence
) -> None:
    batch_id, _reader = await _ready(session_maker)
    page = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "«Принято провайдером» — это не доставка" in page
    assert "Результат неизвестен" in page
    assert "не доказывает, что человек не прочитал" in page
