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
from typing import Any

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
async def anonymous_client(session_maker, monkeypatch) -> AsyncIterator[AsyncClient]:
    from altegio_bot.main import app

    monkeypatch.setattr(followup_ops, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        client.headers.update({"Origin": ORIGIN})
        yield client


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


async def test_the_action_refuses_an_anonymous_caller(session_maker, anonymous_client, no_network) -> None:
    batch_id = await _batch(session_maker)
    response = await anonymous_client.post(ACTION.format(batch_id=batch_id), json=GOOD_BODY)
    assert response.status_code in {401, 403}
    await _no_effects(session_maker)


async def test_the_page_refuses_an_anonymous_caller(session_maker, anonymous_client, no_network) -> None:
    batch_id = await _batch(session_maker)
    response = await anonymous_client.get(PAGE.format(batch_id=batch_id))
    assert response.status_code in {401, 403, 307, 302}


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


async def test_there_is_no_send_endpoint_anywhere_in_this_section(session_maker) -> None:
    """§46.6: no send, no confirmation of a send, no path that becomes one."""
    from altegio_bot.main import app

    followup_routes = {
        path: sorted(getattr(route, "methods", ()) or ())
        for route in app.routes
        if "followup" in (path := getattr(route, "path", "")) and path.startswith("/ops/voucher-mailings")
    }
    assert followup_routes == {
        "/ops/voucher-mailings/{batch_id}/followup": ["GET"],
        "/ops/voucher-mailings/{batch_id}/followup/preview": ["POST"],
    }
    for forbidden in ("send", "confirm", "dispatch", "claim", "execute"):
        assert not any(forbidden in path for path in followup_routes)


# ---------------------------------------------------------------------------
# The GET is a read (§46.6)
# ---------------------------------------------------------------------------


async def test_the_page_creates_nothing(session_maker, ops_client, no_network, seams, binding_key) -> None:
    batch_id = await _batch(session_maker)
    for _ in range(3):
        response = await ops_client.get(PAGE.format(batch_id=batch_id))
        assert response.status_code == 200
    await _no_effects(session_maker)


async def test_the_page_says_only_a_preview_is_available(
    session_maker, ops_client, no_network, seams, binding_key
) -> None:
    batch_id = await _batch(session_maker)
    body = (await ops_client.get(PAGE.format(batch_id=batch_id))).text
    assert "Доступен только предпросмотр" in body
    assert "не расходует попытку отправки" in body


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
