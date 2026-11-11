"""§46.6 — the follow-up preview screen, its one action, and its refusals.

What is real here
-----------------
The FastAPI app, the routes, the rendered page, the session cookie, the CSRF
derivation and the origin check. The Ops auth dependency is deliberately NOT
overridden: a suite that replaced it with ``lambda: None`` could not tell a
configured deployment from an unconfigured one, and could not fail if somebody
later wired a permissive fallback into this endpoint.

What is faked is the outside world, and only through this module's own seams.
``respx`` is armed with ``assert_all_mocked=True`` throughout, so a request that
escapes to Meta or EasyWeek fails the test instead of depending on credentials
being absent.

The question every test asks
----------------------------
Can anything reach a send? There is no send endpoint in this phase, so the
question becomes: can a request create an operation, a job, an Outbox row, a
voucher mutation or a spent attempt — and the answer has to be no for every
path, including an authenticated one.
"""

from __future__ import annotations

import contextlib
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
from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message
from altegio_bot.models.models import (
    EasyWeekVoucherFollowupPreview,
    EasyWeekVoucherFollowupPreviewRow,
    EasyWeekVoucherProductionBatchItem,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.ops.auth import SESSION_COOKIE
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    PHONE_NUMBER_ID,
    TemplateReader,
    location_map,
    person,
    seed_sender_and_template,
    seed_source_batch,
)
from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import (
    BASE_URL,
    ORIGIN,
    csrf_for,
    session_cookie,
)
from altegio_bot.tests.test_easyweek_voucher_followup_preview import FakeHistoryReader

PAGE = "/ops/voucher-mailings/{batch_id}/followup"
ACTION = "/ops/voucher-mailings/{batch_id}/followup/preview"
GOOD_BODY: dict[str, Any] = {"groups": ["delivered_unread"], "min_interval_days": 14}


@pytest.fixture
def no_network() -> Any:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture(autouse=True)
def binding_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """The voucher MAC key and the configured branch registry.

    Autouse, because every route here reaches the preview and the preview needs
    both: the MAC to re-prove a binding, and the registry to know which
    branches the booking question covers.
    """
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)


@pytest.fixture
def seams(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """The two read-only seams, replaced where the route builds them.

    Patched at the module the route imports, so the HTTP request gets the fake —
    a per-call argument could not reach code the browser drives.
    """
    reader = FakeHistoryReader()
    templates = TemplateReader()

    @contextlib.asynccontextmanager
    async def fake_client() -> AsyncIterator[Any]:
        yield reader

    monkeypatch.setattr(followup_ops, "EasyWeekClient", lambda *a, **k: fake_client())

    # The live Meta half, which the page and the action both reach after
    # closing their read session.
    meta_proof = followup_ops.preview_module.prove_meta_followup_template

    async def meta_with_reader(reader: Any = None) -> Any:
        return await meta_proof(templates)

    monkeypatch.setattr(followup_ops.preview_module, "prove_meta_followup_template", meta_with_reader)
    return {"reader": reader, "templates": templates}


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
    """A visitor with no credentials at all, against a CONFIGURED Ops cabinet.

    ``ops_credentials`` is the whole point of this fixture and used to be
    missing from it. Two different situations were being tested as one: a
    configured deployment refusing an anonymous visitor, and a deployment with
    no Ops authorisation configured at all — where ``require_ops_auth`` keeps a
    historical fallback that allows a GET. Which of the two ran depended on
    whatever the machine happened to have in its environment or its ``.env``,
    so the refusal was asserted on a developer's laptop and the page was served
    in CI. The settings this fixture needs are patched onto ``settings`` and
    restored when the test ends; no real file, secret or machine setting takes
    part.

    The client carries nothing that could authenticate it — no session cookie,
    no ``Authorization``, no ``X-Ops-Token`` — and every request below names
    its own ``Accept``, because that header is what decides between the two
    documented refusals. Redirects are not followed, so the refusal itself is
    what the assertions see.
    """
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url=BASE_URL,
        follow_redirects=False,
    ) as client:
        client.headers.update({"Origin": ORIGIN})
        yield client


# The two Accept headers §46.6's refusals are told apart by: a browser is sent
# to the login form, anything else is told it is unauthorised.
API_ACCEPT: Final = "application/json"
BROWSER_ACCEPT: Final = "text/html,application/xhtml+xml"

# One sentence the page cannot be read without. Asserted absent from every
# refusal: a 401 or a 302 that nevertheless carried the page would be the
# defect this section is about.
#
# PR-22.3 replaced the preview-only banner with the four-step one, because the
# page now carries a send — behind its own fence, its own offer and its own
# confirmation. The marker follows the contract rather than outliving it.
PAGE_MARKER: Final = "Четыре отдельных шага"


def _carries_no_credentials(client: AsyncClient) -> None:
    """The precondition, asserted rather than assumed.

    A fixture that quietly grew a cookie or a token would otherwise turn every
    refusal below into a test of nothing.
    """
    assert client.cookies.get(SESSION_COOKIE) is None
    sent = {name.lower() for name in client.headers}
    assert "authorization" not in sent
    assert "x-ops-token" not in sent


async def _state(session_maker) -> dict[str, Any]:
    """Everything a refused request must leave exactly as it was.

    Compared before and after as one value, which is stricter than asking
    whether some counter is still small: a preview version, a stored row, a
    job, an Outbox message or a moved ledger row all show up as a difference.
    """
    async with session_maker() as session:
        previews = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))
        ).scalar_one()
        rows = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
        ledger = sorted(
            (int(item.id), str(item.status), int(item.send_attempt_count or 0))
            for item in (await session.execute(select(EasyWeekVoucherProductionBatchItem))).scalars()
        )
    return {
        "previews": int(previews),
        "preview_rows": int(rows),
        "jobs": int(jobs),
        "outbox": int(outbox),
        "ledger": ledger,
    }


async def _batch(session_maker, *, with_template: bool = True) -> int:
    await seed_sender_and_template(session_maker, with_followup_template=with_template)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    return batch_id


async def _no_effects(session_maker) -> None:
    async with session_maker() as session:
        previews = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))
        ).scalar_one()
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
        spent = (
            await session.execute(
                select(func.coalesce(func.sum(EasyWeekVoucherProductionBatchItem.send_attempt_count), 0))
            )
        ).scalar_one()
    assert (previews, jobs, outbox) == (0, 0, 0)
    # One attempt per seeded slot, from the original mailing. Never two.
    assert int(spent) <= 1


# ---------------------------------------------------------------------------
# Authorisation (§46.6)
# ---------------------------------------------------------------------------


async def test_an_api_get_without_a_session_is_unauthorised(session_maker, anonymous_client, no_network) -> None:
    """Configured cabinet, no credentials, a non-browser Accept: 401.

    The exact status matters. A visitor to this page is told one of two things
    depending on what they can render, and a test that accepted any of several
    statuses could not tell either of them from the page being served.
    """
    batch_id = await _batch(session_maker)
    _carries_no_credentials(anonymous_client)
    before = await _state(session_maker)

    response = await anonymous_client.get(PAGE.format(batch_id=batch_id), headers={"Accept": API_ACCEPT})

    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate", "").startswith("Basic")
    assert PAGE_MARKER not in response.text
    assert await _state(session_maker) == before


async def test_a_browser_get_without_a_session_goes_to_the_login_form(
    session_maker, anonymous_client, no_network
) -> None:
    """The same request from a browser: 302 to the login form, and back here.

    The redirect is checked by destination and not merely by class: a 3xx
    pointing anywhere else would be a different behaviour entirely, and the
    page itself must not be in the response.
    """
    batch_id = await _batch(session_maker)
    page = PAGE.format(batch_id=batch_id)
    _carries_no_credentials(anonymous_client)
    before = await _state(session_maker)

    response = await anonymous_client.get(page, headers={"Accept": BROWSER_ACCEPT})

    assert response.status_code == 302
    assert response.headers["Location"] == f"/ops/login?next={page}"
    assert PAGE_MARKER not in response.text
    assert await _state(session_maker) == before


async def test_the_action_without_a_session_refuses_before_it_computes_anything(
    session_maker, anonymous_client, no_network
) -> None:
    """And the one POST is refused by the same configured cabinet.

    Refused before the preview is computed, which is what the unchanged state
    shows: no version, no stored rows, no job, no Outbox message, and the
    original mailing's own ledger — statuses and attempt counts — exactly as it
    was. ``no_network`` is armed throughout, so a refusal that nevertheless
    reached EasyWeek or Meta would fail here rather than pass quietly.
    """
    batch_id = await _batch(session_maker)
    _carries_no_credentials(anonymous_client)
    before = await _state(session_maker)

    response = await anonymous_client.post(
        ACTION.format(batch_id=batch_id), json=GOOD_BODY, headers={"Accept": API_ACCEPT}
    )

    assert response.status_code == 401
    assert await _state(session_maker) == before


async def test_a_real_session_reaches_the_page_the_refusals_withhold(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    """The positive control: the same two requests, authorised.

    Without this, every assertion above would also hold for an endpoint that
    refuses everybody. A genuine signed session — minted exactly as the login
    form mints one — reads the page and performs the action, and the preview
    the action creates is the one new version.
    """
    batch_id = await _batch(session_maker)
    before = await _state(session_maker)

    page = await ops_client.get(PAGE.format(batch_id=batch_id), headers={"Accept": BROWSER_ACCEPT})
    assert page.status_code == 200
    assert PAGE_MARKER in page.text

    accepted = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY, headers={"Accept": API_ACCEPT})
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["accepted"] is True

    after = await _state(session_maker)
    assert after["previews"] == before["previews"] + 1
    assert after["jobs"] == before["jobs"] == 0
    assert after["outbox"] == before["outbox"] == 0
    assert after["ledger"] == before["ledger"]


async def test_a_forged_session_cookie_is_refused(session_maker, ops_credentials, monkeypatch, no_network) -> None:
    """A cookie that was not signed by this cabinet is not a session."""
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    batch_id = await _batch(session_maker)
    before = await _state(session_maker)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL, follow_redirects=False) as client:
        client.cookies.set(SESSION_COOKIE, "operator:forged-not-signed")
        client.headers.update({"Origin": ORIGIN, "Accept": API_ACCEPT})
        response = await client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)

    assert response.status_code == 401
    assert await _state(session_maker) == before


async def test_a_token_authenticated_script_still_cannot_act(
    session_maker, ops_credentials, monkeypatch, no_network
) -> None:
    """The POST asks for a browser session of its own, not for any credential.

    ``X-Ops-Token`` satisfies the cabinet's own dependency — that is what the
    token is for, and read-only scripts use it — and this action still refuses
    it: the operator behind a protected write has to be an authenticated
    session this code can name. So the refusal here comes from the action's own
    check and names it.
    """
    from altegio_bot.main import app

    monkeypatch.setattr(settings, "ops_token", "synthetic-ops-token", raising=False)
    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    batch_id = await _batch(session_maker)
    before = await _state(session_maker)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL, follow_redirects=False) as client:
        client.headers.update({"Origin": ORIGIN, "Accept": API_ACCEPT, "X-Ops-Token": "synthetic-ops-token"})
        response = await client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)

    assert response.status_code == 401
    body = response.json()
    assert body["accepted"] is False
    assert body["detail"] == "ops_session_invalid"
    assert await _state(session_maker) == before


async def test_an_unconfigured_cabinet_refuses_the_action_and_stores_nothing(
    session_maker, monkeypatch, no_network
) -> None:
    """Ops not configured at all: the historical GET fallback, and its limit.

    ``require_ops_auth`` allows a GET when neither a token nor a user is
    configured, which is the shared Ops cabinet's own long-standing behaviour
    and not this section's to change. The one POST never relies on it: it
    resolves the operator itself, and an unconfigured cabinet has none — so the
    action refuses with ``ops_session_unconfigured`` and computes no preview.
    That is the boundary the two tests above are asserted inside.
    """
    from altegio_bot.main import app

    for name in ("ops_user", "ops_pass", "ops_secret", "ops_token"):
        monkeypatch.setattr(settings, name, "", raising=False)
    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    batch_id = await _batch(session_maker)
    before = await _state(session_maker)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL, follow_redirects=False) as client:
        client.headers.update({"Origin": ORIGIN, "Accept": API_ACCEPT})
        response = await client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)

    assert response.status_code == 403
    body = response.json()
    assert body["accepted"] is False
    assert body["detail"] == "ops_session_unconfigured"
    assert body["reasons"] == ["voucher_production_ops_session_required"]
    assert await _state(session_maker) == before


async def test_the_action_refuses_a_missing_csrf_token(session_maker, ops_client, no_network) -> None:
    batch_id = await _batch(session_maker)
    ops_client.headers.pop("X-Ops-CSRF", None)
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 403
    assert response.json()["accepted"] is False
    await _no_effects(session_maker)


async def test_the_action_refuses_a_wrong_csrf_token(session_maker, ops_client, no_network) -> None:
    batch_id = await _batch(session_maker)
    ops_client.headers["X-Ops-CSRF"] = "not-the-token"
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 403
    await _no_effects(session_maker)


async def test_the_action_refuses_a_foreign_origin(session_maker, ops_client, no_network) -> None:
    batch_id = await _batch(session_maker)
    ops_client.headers["Origin"] = "http://evil.example"
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 403
    await _no_effects(session_maker)


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"groups": [], "min_interval_days": 14},
        {"groups": ["delivered_unread"]},
        {"groups": ["delivered_unread"], "min_interval_days": 0},
        {"groups": ["delivered_unread"], "min_interval_days": -5},
        {"groups": ["delivered_unread"], "min_interval_days": 100000},
        {"groups": ["delivered_unread"], "min_interval_days": "soon"},
        {"groups": "delivered_unread", "min_interval_days": 14},
        # Fields the client may not name: a composition, a digest, an operator,
        # a template or another batch. ``extra="forbid"`` refuses each by name.
        {"groups": ["delivered_unread"], "min_interval_days": 14, "allowed_slots": [1]},
        {"groups": ["delivered_unread"], "min_interval_days": 14, "composition_digest": "a" * 64},
        {"groups": ["delivered_unread"], "min_interval_days": 14, "operator_principal": "root"},
        {"groups": ["delivered_unread"], "min_interval_days": 14, "template_code": "anything"},
        {"groups": ["delivered_unread"], "min_interval_days": 14, "batch_id": 999},
    ],
)
async def test_hostile_or_incomplete_bodies_are_refused(
    session_maker, ops_client, no_network, body: dict[str, Any]
) -> None:
    batch_id = await _batch(session_maker)
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=body)
    assert response.status_code == 422, response.text
    await _no_effects(session_maker)


async def test_an_unknown_group_name_is_refused_not_widened(session_maker, ops_client, no_network, seams) -> None:
    """A selection of one unreal group is empty, and empty is not "all three"."""
    batch_id = await _batch(session_maker)
    response = await ops_client.post(
        ACTION.format(batch_id=batch_id), json={"groups": ["everyone"], "min_interval_days": 14}
    )
    assert response.status_code == 409
    assert response.json()["reasons"] == ["followup_groups_required"]
    await _no_effects(session_maker)


async def test_the_section_exposes_exactly_the_five_followup_routes(session_maker) -> None:
    """§46.8: these five and nothing else, each with exactly its own method.

    An exact inventory rather than a list of forbidden words, which is what
    makes it useful in both directions: a sixth endpoint, a GET that gained a
    POST, or a path that quietly started executing something fails here. The
    preview-only inventory this replaced was the PR-22.2 contract; PR-22.3 adds
    the plan, the confirmation, the stop and the read-only status, and nothing
    more.
    """
    from altegio_bot.main import app

    followup_routes = {
        path: sorted(getattr(route, "methods", ()) or ())
        for route in app.routes
        if "followup" in (path := getattr(route, "path", "")) and path.startswith("/ops/voucher-mailings")
    }
    assert followup_routes == {
        "/ops/voucher-mailings/{batch_id}/followup": ["GET"],
        "/ops/voucher-mailings/{batch_id}/followup/preview": ["POST"],
        "/ops/voucher-mailings/{batch_id}/followup/plan": ["POST"],
        "/ops/voucher-mailings/{batch_id}/followup/confirm": ["POST"],
        "/ops/voucher-mailings/{batch_id}/followup/stop": ["POST"],
        "/ops/voucher-mailings/{batch_id}/followup/status": ["GET"],
    }
    # Everything that can start work is a POST, and the two reads are reads.
    for path, methods in followup_routes.items():
        if path.endswith(("/plan", "/confirm", "/stop", "/preview")):
            assert methods == ["POST"], path
        else:
            assert methods == ["GET"], path


# ---------------------------------------------------------------------------
# The GET is a read (§46.6)
# ---------------------------------------------------------------------------


async def test_the_page_creates_nothing(session_maker, ops_client, no_network, seams, binding_key) -> None:
    batch_id = await _batch(session_maker)
    for _ in range(3):
        response = await ops_client.get(PAGE.format(batch_id=batch_id))
        assert response.status_code == 200
    await _no_effects(session_maker)


async def test_the_page_keeps_the_four_steps_apart_and_names_the_closed_fence(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    """§46.8: four steps, and the default state of the send said in words.

    The fence is closed in this test's environment — no setting is patched —
    which is the deployed default, so the page has to say so rather than offer a
    button that would fail. And it must keep the three harmless steps separate
    from the one that is not: a preview spends nothing, a prepared send spends
    nothing, and only the confirmation creates a run.
    """
    batch_id = await _batch(session_maker)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "Четыре отдельных шага" in body
    assert "не расходует попытку" in body
    assert "Отправка закрыта отдельным переключателем" in body
    assert "Разрешение первичной рассылки этот переключатель не открывает" in body


async def test_the_page_explains_that_no_read_receipt_proves_nothing(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "не доказывает, что человек не прочитал" in body


async def test_the_page_says_an_empty_audience_is_correct(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "корректный результат" in body


async def test_the_page_shows_the_text_with_placeholders_only(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "[Vorname]" in body
    assert "STOP" in body and "СТОП" in body
    # And nothing personal: no number, no card name, no uuid, no digest, no id.
    for forbidden in ("+4915100", "Person 1", "00000001-1111", "wamid.", "test-key-1", "f" * 64):
        assert forbidden not in body, forbidden


async def test_the_page_never_prints_a_batch_digest_or_a_uuid(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored.composition_digest not in body
    assert str(stored.location_uuid) not in body


async def test_a_batch_that_may_not_have_a_preview_says_so_on_the_page(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger

    batch_id = await _batch(session_maker)
    await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="operator")
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "Предпросмотр недоступен" in body
    assert "followup_batch_terminal_stop" in body


# ---------------------------------------------------------------------------
# The one action, end to end through HTTP
# ---------------------------------------------------------------------------


async def test_an_authenticated_operator_can_store_a_preview(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["accepted"] is True
    assert payload["included"] == 1
    assert payload["sending_available"] is False
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored.version == 1
    assert stored.min_interval_seconds == 14 * 24 * 60 * 60
    assert stored.operator_principal
    assert stored.template_code == message.FOLLOWUP_TEMPLATE_CODE
    # Still no send, no job, no second attempt.
    async with session_maker() as session:
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
        spent = (
            await session.execute(select(func.sum(EasyWeekVoucherProductionBatchItem.send_attempt_count)))
        ).scalar_one()
    assert (jobs, outbox, int(spent)) == (0, 0, 1)


async def test_an_unproven_template_is_reported_with_a_sentence(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker, with_template=False)
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 409
    payload = response.json()
    assert payload["reasons"] == ["followup_template_unproven"]
    assert "APPROVED" in payload["detail"]


async def test_a_direct_api_call_cannot_name_its_own_composition(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    """The server reads the ledger; the client may choose groups and a time."""
    batch_id = await _batch(session_maker)
    accepted = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert accepted.status_code == 200
    async with session_maker() as session:
        first = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
        server_digest = first.composition_digest
    # A replay claiming a digest, a slot list and a count gets a 422, and the
    # stored proposal is untouched.
    replay = await ops_client.post(
        ACTION.format(batch_id=batch_id),
        json={
            **GOOD_BODY,
            "composition_digest": server_digest,
            "allowed_slots": [1, 2, 3],
            "included_count": 3,
        },
    )
    assert replay.status_code == 422
    async with session_maker() as session:
        rows = list((await session.execute(select(EasyWeekVoucherFollowupPreview))).scalars())
    assert len(rows) == 1
    assert rows[0].composition_digest == server_digest


async def test_a_recalculation_through_http_adds_a_version(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    first = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    second = await ops_client.post(ACTION.format(batch_id=batch_id), json={**GOOD_BODY, "min_interval_days": 21})
    assert (first.json()["version"], second.json()["version"]) == (1, 2)
    assert first.json()["composition_digest"] != second.json()["composition_digest"]


async def test_an_unconfigured_easyweek_is_unknown_not_a_500(
    session_maker, ops_client, no_network, monkeypatch, binding_key
) -> None:
    """A deployment with no EasyWeek credentials gets an answer, not a crash.

    §46.5 forbids turning an API failure into an empty booking list, and a 500
    would leave an operator with no reason to read. So the preview is computed
    with no reader and every candidate is reported as unestablished.
    """
    from altegio_bot.easyweek_client import EasyWeekConfigError

    templates = TemplateReader()
    original = followup_ops.preview_module.prove_meta_followup_template

    async def with_reader(reader: Any = None) -> Any:
        return await original(templates)

    monkeypatch.setattr(followup_ops.preview_module, "prove_meta_followup_template", with_reader)

    def unconfigured(*args: Any, **kwargs: Any) -> Any:
        raise EasyWeekConfigError("EASYWEEK_API_KEY is not configured")

    monkeypatch.setattr(followup_ops, "EasyWeekClient", unconfigured)

    batch_id = await _batch(session_maker)
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["accepted"] is True
    assert (payload["included"], payload["unknown"]) == (0, 1)
    assert payload["booking_reads"] == 0


# ---------------------------------------------------------------------------
# Honest standing of a stored result (§46.6)
# ---------------------------------------------------------------------------


async def _store_one(ops_client: Any, batch_id: int) -> dict[str, Any]:
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 200, response.text
    return response.json()


async def test_a_saved_result_reads_as_current_right_after_the_check(
    session_maker, ops_client, no_network, seams
) -> None:
    """Review round 3, finding 5: the green state says what the GET re-proved.

    A GET re-proves the batch, the snapshot's validity window and the message
    contract. It reads nobody's consent, replies, artifact or bookings — that is
    an external recalculation and §46.6 makes it an explicit operator action —
    so the page must not claim the recipients or their number are confirmed now.
    """
    batch_id = await _batch(session_maker)
    await _store_one(ops_client, batch_id)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert 'data-followup-standing="current"' in body
    # What was re-checked, named.
    assert "сама партия, срок действия снимка и контракт текста" in body
    assert "Состав при этом не пересчитывался" in body
    # And what was not.
    assert "Согласие, ответы и записи каждого получателя заново не проверялись" in body
    assert "текущее количество получателей этим не подтверждается" in body
    # The old claims are gone.
    assert "Эти проверки актуальны" not in body
    assert "подтверждены и сейчас" not in body


async def test_every_stored_number_is_printed_with_its_check_time(session_maker, ops_client, no_network, seams) -> None:
    """A stored figure never appears without the moment it was computed."""
    batch_id = await _batch(session_maker)
    await _store_one(ops_client, batch_id)
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
    stamp = stored.checked_at.strftime("%d.%m.%Y %H:%M UTC")
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert body.count(stamp) >= 4, stamp
    for caption in ("подходили для повторного сообщения", "исключены", "не удалось установить"):
        assert caption in body


async def test_a_paused_template_stops_the_result_reading_as_current(
    session_maker, ops_client, no_network, monkeypatch, seams
) -> None:
    """§46.6: a template that is no longer APPROVED cannot leave a green block."""
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import approved_meta_templates

    batch_id = await _batch(session_maker)
    await _store_one(ops_client, batch_id)

    paused = approved_meta_templates()
    paused[0]["status"] = "PAUSED"
    seams["templates"].templates = paused

    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "Эти проверки актуальны" not in body
    assert "сейчас не подтверждён" in body
    assert "исторический результат" in body
    assert "НЕ подтверждены заново" in body


async def test_a_changed_body_hides_the_old_text_rather_than_substituting(
    session_maker, ops_client, no_network, monkeypatch, seams
) -> None:
    """The stored digest no longer matches, so the old text cannot be shown."""
    batch_id = await _batch(session_maker)
    await _store_one(ops_client, batch_id)

    async with session_maker() as session:
        async with session.begin():
            stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
            # The snapshot is immutable; this stands in for the contract having
            # moved underneath it, which is what the page must notice.
            stored.template_body_digest = "0" * 64

    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "текст повторного сообщения изменился" in body
    assert "прежний текст показать нельзя" in body
    assert "Эти проверки актуальны" not in body


async def test_a_terminal_stop_after_the_check_stops_the_green_block(
    session_maker, ops_client, no_network, seams
) -> None:
    from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger

    batch_id = await _batch(session_maker)
    await _store_one(ops_client, batch_id)
    await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="operator")
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "Эти проверки актуальны" not in body
    assert "сейчас повторное сообщение по этой рассылке запрещено" in body.lower()
    assert "НЕ подтверждены заново" in body


async def test_a_refused_refresh_leaves_the_stored_result_untouched(
    session_maker, ops_client, no_network, seams
) -> None:
    """A 409 changes nothing, and the page does not pretend otherwise."""
    from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger

    batch_id = await _batch(session_maker)
    first = await _store_one(ops_client, batch_id)
    await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="operator")

    refused = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert refused.status_code == 409
    assert refused.json()["reasons"] == ["followup_batch_terminal_stop"]

    async with session_maker() as session:
        rows = list((await session.execute(select(EasyWeekVoucherFollowupPreview))).scalars())
    assert len(rows) == 1
    assert rows[0].composition_digest == first["composition_digest"]
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "исторический результат" in body


async def test_the_page_states_that_a_refused_check_keeps_the_old_result(
    session_maker, ops_client, no_network, seams
) -> None:
    """The sentence the browser shows on a failed check, present in the page."""
    batch_id = await _batch(session_maker)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "предыдущий результат не изменился" in body


async def test_an_expired_snapshot_is_shown_as_expired(session_maker, ops_client, no_network, seams) -> None:
    batch_id = await _batch(session_maker)
    await _store_one(ops_client, batch_id)
    async with session_maker() as session:
        async with session.begin():
            stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
            stored.checked_at = stored.checked_at - timedelta(days=5)
            stored.valid_until = stored.valid_until - timedelta(days=5)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "Срок действия этих проверок истёк" in body
    assert "Эти проверки актуальны" not in body


# ---------------------------------------------------------------------------
# The historical backlog is not bypassable through the API (§46.4.4)
# ---------------------------------------------------------------------------


async def test_a_direct_api_call_cannot_bypass_the_backlog_gate(session_maker, ops_client, no_network, seams) -> None:
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import person
    from altegio_bot.tests.test_easyweek_voucher_followup_preview import _stored_event

    batch_id = await _batch(session_maker)
    await _stored_event(session_maker, dedupe_key="legacy-api-1", address=person(1)["phone"], text="СТОП")
    payload = await _store_one(ops_client, batch_id)
    assert payload["included"] == 0
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    # The reason reaches the page in words, not only as a code.
    assert "отписка этого человека, которая ещё не учтена" in body
    assert "followup_historical_stop_unaccounted" in body


async def test_neither_the_page_nor_the_check_applies_a_reconciliation(
    session_maker, ops_client, no_network, seams
) -> None:
    """§46.4.4 keeps the projection a separate, explicit operation."""
    from altegio_bot.models.models import WhatsAppConsentSuppression, WhatsAppInboundReceipt
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import person
    from altegio_bot.tests.test_easyweek_voucher_followup_preview import _stored_event

    batch_id = await _batch(session_maker)
    await _stored_event(session_maker, dedupe_key="legacy-api-2", address=person(1)["phone"], text="стоп")
    for _ in range(2):
        await ops_client.get(PAGE.format(batch_id=batch_id))
        await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    async with session_maker() as session:
        suppressions = (
            await session.execute(select(func.count()).select_from(WhatsAppConsentSuppression))
        ).scalar_one()
        receipts = (await session.execute(select(func.count()).select_from(WhatsAppInboundReceipt))).scalar_one()
    assert (suppressions, receipts) == (0, 0)


async def test_the_page_can_demote_its_own_saved_result(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    """Review round 2, finding 6: the hooks the open page needs to stay honest.

    The server renders the standing it can prove at load time. A click that
    fails afterwards has to drop that standing on the page itself, so the block
    carries a state attribute, a named standing element and a named
    confirmation element, and the script carries the one function that changes
    all three.
    """
    batch_id = await _batch(session_maker)
    assert (await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)).status_code == 200
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert 'id="followup-stored"' in body
    assert 'data-followup-standing="current"' in body
    assert 'id="followup-standing"' in body
    assert 'id="followup-confirmation"' in body
    assert "followupDemoteStoredResult" in body
    # Called on every outcome that is not a new result: a refusal, an
    # unreadable answer and a transport failure.
    assert body.count("followupDemoteStoredResult()") >= 3


async def test_a_direct_api_call_does_not_bypass_the_historical_backlog(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    """§46.4.4 holds on the HTTP path too, not only inside the orchestrator."""
    from altegio_bot.models.models import WhatsAppEvent

    batch_id = await _batch(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppEvent(
                    dedupe_key="ops-legacy-stop",
                    status="processed",
                    query={},
                    headers={},
                    payload={
                        "entry": [
                            {
                                "changes": [
                                    {
                                        "value": {
                                            "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                                            "messages": [
                                                {
                                                    "from": person(1)["phone"].lstrip("+"),
                                                    "id": "wamid.ops-legacy",
                                                    "type": "text",
                                                    "text": {"body": "СТОП"},
                                                }
                                            ],
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                )
            )
    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code == 200, response.text
    assert response.json()["included"] == 0
    page = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "отписк" in page.lower()


async def test_a_wide_capture_still_answers_over_the_real_endpoint(
    session_maker, ops_client, no_network, seams, binding_key, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review round 7, finding 7: the answer must not depend on history's size.

    The reverse owner lookup asks about every (branch, number) pair the capture
    named. One branch with a long history names enough of them to cross the
    wire protocol's limit on bound parameters in a single statement, and the
    operator was told the database was unavailable.

    The same request is made twice over the real HTTP path — once with the
    statement budget as deployed, once with a budget so small that the lookup
    has to be split into many statements — and the two answers must be the
    same one. A split that lost a batch, or an error swallowed into an empty
    selection, would show up here as a different count.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import booking_evidence
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import ATTEMPTED_AT, seed_booking_created

    batch_id = await _batch(session_maker)
    who = person(1)
    for offset in range(12):
        await seed_booking_created(
            session_maker,
            booking_uuid=f"{200 + offset:08d}-7777-4777-8777-777777777777",
            numeric_customer_id=who["numeric_id"] + offset,
            created_at=ATTEMPTED_AT + timedelta(days=2),
        )

    whole = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert whole.status_code == 200, whole.text

    monkeypatch.setattr(booking_evidence, "MAX_STATEMENT_PARAMETERS", 4, raising=False)
    chunked = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert chunked.status_code == 200, chunked.text

    def counts(response: Any) -> tuple[int, int, int]:
        body = response.json()
        return body["included"], body["excluded"], body["unknown"]

    assert counts(chunked) == counts(whole)


@pytest.mark.parametrize(
    "stamp",
    [
        "0001-01-01T00:00:00+01:00",
        "9999-12-31T23:59:59-01:00",
        "whenever, really",
        "2026-09-05T10:00:00+02:00",
    ],
)
async def test_an_unrepresentable_creation_instant_is_one_rows_problem(
    session_maker, ops_client, no_network, seams, binding_key, stamp: str
) -> None:
    """Review round 8, finding 4: one bad instant, one affected row.

    The first two stamps are syntactically valid ISO-8601 and cannot be
    expressed in UTC at all: midnight on the first day of year one east of
    Greenwich, and the last second of year 9999 west of it. Converting either
    one overflows, and the exception used to leave the capture, cross the whole
    preview and reach the operator as HTTP 500 for the entire batch — no rows,
    no reasons, nothing to act on.

    Such an instant is an unusable creation time, exactly like an unreadable
    string: the delivery's branch, booking and customer stay proven, no arrival
    time is substituted for the missing instant, and only the row whose own
    booking it is becomes unknown. The fourth stamp is the ordinary case with
    an offset, which still excludes.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup.identity import BOOKING_CAPTURE_UNPROVEN
    from altegio_bot.models.models import EasyWeekVoucherFollowupPreviewRow
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
        seed_booking_created_without_a_date,
    )

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered"}, {"state": "delivered", "index": 2}],
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid="00000150-7777-4777-8777-777777777777",
        numeric_customer_id=person(1)["numeric_id"],
        created_at_value=stamp,
    )

    response = await ops_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)

    assert response.status_code == 200, response.text
    body = response.json()
    readable = stamp.startswith("2026")
    assert (body["included"], body["excluded"], body["unknown"]) == ((1, 1, 0) if readable else (1, 0, 1))
    async with session_maker() as session:
        rows = {
            row.source_slot: (row.decision, row.reason_code)
            for row in (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalars()
        }
    assert rows[2][0] == "included"
    if readable:
        assert rows[1] == ("excluded", "followup_new_booking_after_attempt")
    else:
        assert rows[1] == ("unknown", BOOKING_CAPTURE_UNPROVEN)
    await _no_send_effects(session_maker)


async def _no_send_effects(session_maker) -> None:
    """One preview version and nothing else: no job, no Outbox row, no attempt."""
    async with session_maker() as session:
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
        spent = (
            await session.execute(
                select(func.coalesce(func.sum(EasyWeekVoucherProductionBatchItem.send_attempt_count), 0))
            )
        ).scalar_one()
        previews = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))
        ).scalar_one()
    assert (jobs, outbox, previews) == (0, 0, 1)
    assert int(spent) <= 2
