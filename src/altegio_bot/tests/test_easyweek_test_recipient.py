"""Adding the one owner-approved test recipient to a preview (§36.11).

What these tests are guarding is not "does the row appear". It is that a screen
which can add a recipient to an EasyWeek preview cannot become a way to point a
real €15 voucher at an arbitrary person, and that a test identity can never be
mistaken later for an entitlement somebody earned.

So: the customer is named by the server and never by the request; both fences
must be open; the account is read live and compared in both directions; the row
that is written carries a typed test basis and no source proof at all; and the
edit and the counters it implies land in one transaction or not at all.

Every identity here is synthetic. The real test account's number and UUID belong
in the deployed environment and in neither this file nor any report it checks.
"""

from __future__ import annotations

import uuid as uuid_module
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.campaigns.easyweek_voucher_delivery import test_recipient as test_recipient_module
from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import prove_recipient
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    TEST_BINDING_MISMATCH,
    TEST_CUSTOMER_UNCONFIGURED,
    TEST_CUSTOMER_UNPROVEN,
    TEST_RECIPIENT_DISABLED,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.test_recipient import (
    ACTION_CREATED,
    ACTION_REACTIVATED,
    ACTION_UNCHANGED,
    add_test_recipient_to_preview,
)
from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.main import app
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    VOUCHER_DELIVERY_BASIS_EARNED,
    VOUCHER_DELIVERY_BASIS_TEST,
    CampaignRecipient,
    CampaignRun,
    Client,
)
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (  # noqa: F401 - fixtures
    COMPANY_ID,
    PHONE,
    seed_recipient,
)
from altegio_bot.utils import utcnow

# Synthetic throughout. The configured account is a UUID nobody owns, and the
# "real" number is a documentation-range German mobile.
TEST_CUSTOMER_UUID = "77777777-4444-4888-8aaa-777777777777"
OTHER_CUSTOMER_UUID = "12121212-4343-4565-8787-989898989898"
TEST_PHONE = "+4915100000001"


class _CustomerReader:
    """Read-only, and it records exactly what was asked of it."""

    def __init__(self, *, card: dict[str, Any] | None = None, error: Exception | None = None) -> None:
        self.card = card if card is not None else {"uuid": TEST_CUSTOMER_UUID, "phone": TEST_PHONE, "first_name": "T"}
        self.error = error
        self.calls: list[str] = []

    async def __aenter__(self) -> "_CustomerReader":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.calls.append(f"get_customer:{customer_uuid}")
        if self.error is not None:
            raise self.error
        return self.card

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - must not run
        raise AssertionError("the test recipient path must not read bookings")

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        # The history is exactly what this basis does not consult.
        raise AssertionError("the test recipient path must not walk history")


@pytest.fixture
def fences(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    """Both fences open, one account configured, and the branch registry ready."""
    # `configuration` also closes the canary fence, so it goes first. The MAC
    # key comes with it: a stage plan is not ready without one, and the tests
    # that drive a real CREATE need the whole prerequisite set, not just these
    # two flags.
    request.getfixturevalue("configuration")
    request.getfixturevalue("binding_key")
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_recipient_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_customer_uuid", TEST_CUSTOMER_UUID, raising=False)


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncClient:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def _empty_preview(session_maker, *, phone: str = TEST_PHONE) -> tuple[int, int]:
    """A completed EasyWeek preview and one local client, with no recipients.

    Seeded through the ordinary fixture and then emptied, so the run, the client
    and the branch are exactly the ones the real thing would have.
    """
    run_id, recipient_id = await seed_recipient(session_maker, phone=phone, client_phone=phone)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            await session.delete(row)
    return run_id, recipient_id


async def _rows(session_maker, run_id: int) -> list[CampaignRecipient]:
    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(CampaignRecipient)
                    .where(CampaignRecipient.campaign_run_id == run_id)
                    .order_by(CampaignRecipient.id)
                )
            )
            .scalars()
            .all()
        )


async def _add(session_maker, run_id: int, reader: _CustomerReader | None = None, **kwargs: Any):
    return await add_test_recipient_to_preview(
        session_maker,
        run_id=run_id,
        phone=kwargs.pop("phone", TEST_PHONE),
        client_reader=reader or _CustomerReader(),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# The fences are closed by default, and a closed fence touches nothing
# ---------------------------------------------------------------------------


def test_the_test_recipient_settings_are_closed_and_empty_by_default() -> None:
    """Not a default somebody could inherit: an unset variable is a refusal."""
    from altegio_bot.settings import Settings

    fresh = Settings.model_construct()

    assert fresh.easyweek_voucher_delivery_test_recipient_enabled is False
    assert fresh.easyweek_voucher_delivery_test_customer_uuid == ""


@pytest.mark.asyncio
async def test_a_closed_test_fence_reads_nothing_and_writes_nothing(session_maker, monkeypatch, configuration) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_recipient_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_customer_uuid", TEST_CUSTOMER_UUID, raising=False)
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is False
    assert outcome.reason == TEST_RECIPIENT_DISABLED
    assert reader.calls == []
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_the_canary_fence_alone_does_not_open_this(session_maker, monkeypatch, configuration) -> None:
    """Turning on the canary must never be what turns on the substitution."""
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_recipient_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_customer_uuid", TEST_CUSTOMER_UUID, raising=False)
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == TEST_RECIPIENT_DISABLED
    assert reader.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", ["", "   ", "not-a-uuid"])
async def test_an_unusable_configured_uuid_reads_nothing(
    session_maker, monkeypatch, configuration, configured: str
) -> None:
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_recipient_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_customer_uuid", configured, raising=False)
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == TEST_CUSTOMER_UNCONFIGURED
    assert reader.calls == []
    assert await _rows(session_maker, run_id) == []


# ---------------------------------------------------------------------------
# The happy path, and what the row is allowed to say
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_configured_account_is_added_as_one_test_candidate(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is True
    assert outcome.action == ACTION_CREATED
    # Exactly one read, of exactly the configured account.
    assert reader.calls == [f"get_customer:{TEST_CUSTOMER_UUID}"]
    rows = await _rows(session_maker, run_id)
    assert len(rows) == 1
    assert rows[0].status == "candidate"
    assert rows[0].provider == PROVIDER_EASYWEEK
    assert rows[0].easyweek_test_customer_uuid == uuid_module.UUID(TEST_CUSTOMER_UUID)


@pytest.mark.asyncio
async def test_a_test_recipient_carries_no_earned_proof_at_all(session_maker, fences) -> None:
    """The whole point: this row must never be able to look like a first visit."""
    run_id, _ = await _empty_preview(session_maker)

    await _add(session_maker, run_id)

    [row] = await _rows(session_maker, run_id)
    assert row.source_easyweek_event_id is None
    assert row.source_record_id is None
    assert row.source_booking_uuid is None
    assert row.source_visits_total is None
    assert row.source_visits_total_updated_at is None
    # And no Altegio identity was invented for an EasyWeek customer.
    assert row.altegio_client_id is None


@pytest.mark.asyncio
async def test_the_binding_is_typed_and_the_meta_stays_clean(session_maker, fences) -> None:
    """`meta` is untyped JSON that ends up in reports. Nothing personal goes in."""
    run_id, _ = await _empty_preview(session_maker)

    await _add(session_maker, run_id)

    [row] = await _rows(session_maker, run_id)
    assert row.easyweek_test_customer_uuid is not None
    serialised = repr(dict(row.meta or {}))
    for secret in (TEST_CUSTOMER_UUID, TEST_PHONE, "Synthetic Fixture"):
        assert secret not in serialised


# ---------------------------------------------------------------------------
# Doing it twice, and doing it over an existing row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adding_the_same_test_recipient_twice_creates_one_row(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)

    first = await _add(session_maker, run_id)
    second = await _add(session_maker, run_id)

    assert first.action == ACTION_CREATED
    assert second.ok is True
    assert second.action == ACTION_UNCHANGED
    assert second.recipient_id == first.recipient_id
    assert len(await _rows(session_maker, run_id)) == 1


@pytest.mark.asyncio
async def test_a_skipped_row_for_the_same_client_is_reactivated_in_place(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    created = await _add(session_maker, run_id)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, created.recipient_id)
            row.status = "skipped"
            row.excluded_reason = "manual_removed"

    again = await _add(session_maker, run_id)

    assert again.ok is True
    assert again.action == ACTION_REACTIVATED
    assert again.recipient_id == created.recipient_id
    rows = await _rows(session_maker, run_id)
    assert len(rows) == 1
    assert rows[0].status == "candidate"
    assert rows[0].excluded_reason is None


@pytest.mark.asyncio
async def test_an_earned_row_for_the_same_client_is_never_overwritten(session_maker, fences) -> None:
    """An earned recipient is somebody's proven entitlement. Not ours to edit."""
    run_id, recipient_id = await seed_recipient(session_maker, phone=TEST_PHONE, client_phone=TEST_PHONE)

    outcome = await _add(session_maker, run_id)

    assert outcome.ok is False
    assert outcome.reason == test_recipient_module.AMBIGUOUS_ROWS
    async with session_maker() as session:
        row = await session.get(CampaignRecipient, recipient_id)
        assert row.source_booking_uuid is not None
        assert row.easyweek_test_customer_uuid is None


@pytest.mark.asyncio
async def test_two_local_clients_for_one_number_are_an_ambiguity(session_maker, fences) -> None:
    """Which of two people would receive a real voucher is not a guess."""
    run_id, _ = await _empty_preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_client_id=999001,
                    phone_e164=TEST_PHONE,
                    display_name="Second Synthetic",
                    raw={},
                )
            )
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == test_recipient_module.CLIENT_UNRESOLVED
    assert reader.calls == []
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_an_opted_out_client_is_never_added(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == TEST_PHONE))).scalar_one()
            client.wa_opted_out = True
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == test_recipient_module.CLIENT_OPTED_OUT
    assert reader.calls == []
    assert await _rows(session_maker, run_id) == []


# ---------------------------------------------------------------------------
# The live read decides, and every way it can fail is the same refusal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "card, error",
    [
        pytest.param({"uuid": OTHER_CUSTOMER_UUID, "phone": TEST_PHONE}, None, id="uuid_mismatch"),
        pytest.param({"uuid": TEST_CUSTOMER_UUID, "phone": "+4915100009999"}, None, id="phone_mismatch"),
        pytest.param({"uuid": TEST_CUSTOMER_UUID}, None, id="no_phone"),
        pytest.param({"nonsense": True}, None, id="malformed"),
        pytest.param(None, EasyWeekError("not found"), id="not_found"),
        pytest.param(None, TimeoutError("slow"), id="timeout"),
        pytest.param(None, EasyWeekError("rate limited"), id="rate_limited"),
        pytest.param(None, EasyWeekError("server error"), id="server_error"),
    ],
)
async def test_a_live_read_that_does_not_prove_the_account_writes_nothing(session_maker, fences, card, error) -> None:
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader(card=card, error=error)

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is False
    assert outcome.reason == TEST_CUSTOMER_UNPROVEN
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_a_phone_that_cannot_be_normalised_reads_nothing(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader, phone="nonsense")

    assert outcome.reason == test_recipient_module.PHONE_UNUSABLE
    assert reader.calls == []


@pytest.mark.asyncio
async def test_an_altegio_client_id_is_refused_rather_than_ignored(session_maker, fences) -> None:
    """Accepting it would mean this path can be steered by the request."""
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()

    outcome = await _add(session_maker, run_id, reader, altegio_client_id=12345)

    assert outcome.reason == test_recipient_module.CLIENT_ID_NOT_ACCEPTED
    assert reader.calls == []
    assert await _rows(session_maker, run_id) == []


# ---------------------------------------------------------------------------
# The edit and its counters are one transaction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_counter_failure_rolls_the_whole_add_back(session_maker, fences, monkeypatch) -> None:
    """The production defect, in the other direction.

    The old code committed the INSERT and then recounted in a session of its
    own — where EasyWeek was refused outright, leaving a recipient in a snapshot
    whose totals did not know about it.
    """
    run_id, _ = await _empty_preview(session_maker)

    async def boom(session, run):
        raise RuntimeError("counter recompute failed")

    monkeypatch.setattr(runner_module, "recompute_snapshot_counters", boom)

    with pytest.raises(RuntimeError):
        await _add(session_maker, run_id)

    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_the_counters_see_the_added_test_recipient(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)

    await _add(session_maker, run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
    assert run.total_clients_seen == 1
    assert run.candidates_count == 1


@pytest.mark.asyncio
async def test_remove_and_its_counters_commit_together(session_maker, fences) -> None:
    """Remove had the same defect: commit the exclusion, then fail the recount."""
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)

    removed = await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)

    assert removed.status == "skipped"
    assert removed.excluded_reason == "manual_removed"
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        row = await session.get(CampaignRecipient, added.recipient_id)
    assert run.candidates_count == 0
    assert run.total_clients_seen == 1
    # The binding is kept: it is the audit trail of what this row was added as.
    assert row.easyweek_test_customer_uuid is not None


@pytest.mark.asyncio
async def test_a_counter_failure_rolls_the_whole_remove_back(session_maker, fences, monkeypatch) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)

    async def boom(session, run):
        raise RuntimeError("counter recompute failed")

    monkeypatch.setattr(runner_module, "recompute_snapshot_counters", boom)

    with pytest.raises(RuntimeError):
        await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)

    async with session_maker() as session:
        row = await session.get(CampaignRecipient, added.recipient_id)
    assert row.status == "candidate"
    assert row.excluded_reason is None


@pytest.mark.asyncio
async def test_removing_the_same_recipient_twice_is_idempotent(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)

    await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)
    again = await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)

    assert again.status == "skipped"
    assert len(await _rows(session_maker, run_id)) == 1


# ---------------------------------------------------------------------------
# Once the canary holds the preview, the snapshot is frozen
# ---------------------------------------------------------------------------


async def _attach_canary(session_maker, run_id: int, recipient_id: int) -> None:
    """Open the canary's ledger row against this preview, the way `create` does."""
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
    from altegio_bot.campaigns.easyweek_voucher_delivery.identity import NEW_CLIENT_CAMPAIGN_CODE, delivery_marker
    from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
    from altegio_bot.tests.easyweek_voucher_delivery_fixtures import ACCOUNT_UUID, STAFFER_UUID

    await ledger_module.open_canary(
        session_maker,
        identity=ledger_module.CanaryIdentity(
            company_id=COMPANY_ID,
            campaign_code=NEW_CLIENT_CAMPAIGN_CODE,
            campaign_run_id=run_id,
            campaign_recipient_id=recipient_id,
            recipient_basis=VOUCHER_DELIVERY_BASIS_TEST,
            source_booking_uuid=None,
            easyweek_customer_uuid=TEST_CUSTOMER_UUID,
            location_uuid=KARLSRUHE_LOCATION_UUID,
            staffer_uuid=STAFFER_UUID,
            payment_account_uuid=ACCOUNT_UUID,
            voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            reconciliation_marker=delivery_marker(preview_run_id=run_id, campaign_recipient_id=recipient_id),
        ),
    )


@pytest.mark.asyncio
async def test_a_canary_locked_preview_refuses_every_edit(session_maker, fences) -> None:
    """Editing after CREATE or PAY strands a real €15 rather than undoing it."""
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    await _attach_canary(session_maker, run_id, added.recipient_id)

    blocked = await _add(session_maker, run_id)
    assert blocked.ok is False
    assert blocked.reason == test_recipient_module.LOCKED_BY_CANARY

    for action in (
        runner_module.discard_preview_run(run_id),
        runner_module.delete_preview_run(run_id),
        runner_module.remove_recipient_from_preview(run_id, added.recipient_id),
    ):
        with pytest.raises(ValueError, match="canary"):
            await action

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        row = await session.get(CampaignRecipient, added.recipient_id)
    assert run.status == "completed"
    assert row.status == "candidate"


@pytest.mark.asyncio
async def test_the_api_reports_a_locked_preview_as_not_editable(session_maker, fences, http_client) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    await _attach_canary(session_maker, run_id, added.recipient_id)

    detail = (await http_client.get(f"/ops/campaigns/runs/{run_id}")).json()

    assert detail["canary_locked"] is True
    assert detail["is_snapshot_editable"] is False
    assert detail["is_discardable"] is False
    assert detail["is_deletable"] is False


@pytest.mark.asyncio
async def test_the_ui_stops_offering_edits_once_the_canary_holds_the_preview(
    session_maker, fences, http_client
) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    await _attach_canary(session_maker, run_id, added.recipient_id)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "voucher delivery canary" in page
    # The helper functions still exist in the page script; what must be gone is
    # every control wired to them.
    for gone in ('onclick="showAddRecipientForm()"', 'onclick="discardAndRefresh(', 'onclick="deleteAndRedirect('):
        assert gone not in page


# ---------------------------------------------------------------------------
# The Ops surface itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_easyweek_add_endpoint_no_longer_answers_segment_not_implemented(
    session_maker, fences, http_client, monkeypatch
) -> None:
    """The production symptom: the button existed and the endpoint refused."""
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader()
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: reader)

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add", json={"phone": TEST_PHONE})

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["recipient_basis"] == "owner_test_account"
    assert body["campaign_send_authorized"] is False
    assert body["bulk_delivery_authorized"] is False
    assert body["global_ready_for_send"] is False
    assert "easyweek_campaign_segment_not_implemented" not in resp.text


@pytest.mark.asyncio
async def test_the_easyweek_add_endpoint_never_calls_an_altegio_crm_helper(
    session_maker, fences, http_client, monkeypatch
) -> None:
    """That API has nothing to say about an EasyWeek customer."""
    run_id, _ = await _empty_preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _CustomerReader())

    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("the EasyWeek path must not reach the Altegio CRM")

    monkeypatch.setattr(campaigns_api_module, "get_client_crm_records", forbidden)
    monkeypatch.setattr(campaigns_api_module, "check_lash_services", forbidden)

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add", json={"phone": TEST_PHONE})

    assert resp.status_code == 201, resp.text


@pytest.mark.asyncio
async def test_a_refusal_carries_a_reason_code_and_no_personal_data(
    session_maker, fences, http_client, monkeypatch, caplog
) -> None:
    run_id, _ = await _empty_preview(session_maker)
    reader = _CustomerReader(card={"uuid": OTHER_CUSTOMER_UUID, "phone": TEST_PHONE})
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: reader)

    with caplog.at_level("INFO"):
        resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add", json={"phone": TEST_PHONE})

    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == TEST_CUSTOMER_UNPROVEN
    for surface in (resp.text, caplog.text):
        for secret in (TEST_PHONE, TEST_CUSTOMER_UUID, OTHER_CUSTOMER_UUID, "Synthetic Fixture"):
            assert secret not in surface


@pytest.mark.asyncio
async def test_the_easyweek_ui_offers_no_run_from_preview_and_no_altegio_field(
    session_maker, fences, http_client
) -> None:
    run_id, _ = await _empty_preview(session_maker)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "Run from preview" not in page
    assert "Altegio Client ID" not in page
    assert "Add test recipient" in page
    assert "обычный send-real" in page.lower()


@pytest.mark.asyncio
async def test_send_real_from_an_easyweek_preview_stays_closed(session_maker, fences, http_client) -> None:
    """The UI stops offering it; the backend still refuses it."""
    run_id, _ = await _empty_preview(session_maker)
    await _add(session_maker, run_id)
    async with session_maker() as session:
        before_runs = await session.scalar(select(func.count()).select_from(CampaignRun))

    resp = await http_client.post(
        "/ops/campaigns/new-clients/run",
        json={"provider": "easyweek", "company_id": COMPANY_ID, "from_preview_run_id": run_id},
    )

    assert resp.status_code in (409, 422)
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(CampaignRun)) == before_runs


@pytest.mark.asyncio
async def test_the_full_recompute_stays_closed_for_easyweek(session_maker, fences, http_client) -> None:
    """The lightweight snapshot recount is open; the Altegio/outbox one is not."""
    run_id, _ = await _empty_preview(session_maker)

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recompute")

    # Refused, whichever code the existing handler maps the refusal to. What
    # matters is that the Altegio/outbox recompute did not become reachable.
    assert resp.status_code >= 400
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
    assert run.status == "completed"


# ---------------------------------------------------------------------------
# Proving the recipient, stage after stage
# ---------------------------------------------------------------------------


async def _prove(session_maker, run_id: int, recipient_id: int, reader: _CustomerReader | None = None):
    async with session_maker() as session:
        return await prove_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=recipient_id,
            expected_company_id=COMPANY_ID,
            client_reader=reader or _CustomerReader(),
            now=utcnow(),
        )


@pytest.mark.asyncio
async def test_a_test_recipient_is_proven_without_any_history(session_maker, fences) -> None:
    """No first-visit proof — and the report says `not_applicable`, not `true`."""
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)

    proof = await _prove(session_maker, run_id, added.recipient_id)

    assert proof.proven is True
    assert proof.recipient_basis == VOUCHER_DELIVERY_BASIS_TEST
    assert proof.source_booking_uuid is None
    assert proof.checks["first_visit_proof_applicable"] is False
    assert "first_visit_current" not in proof.checks
    # And the printable form names the basis without naming the person.
    safe = proof.as_safe_dict()
    assert safe["recipient_basis"] == VOUCHER_DELIVERY_BASIS_TEST
    for secret in (TEST_CUSTOMER_UUID, TEST_PHONE):
        assert secret not in repr(safe)


@pytest.mark.asyncio
async def test_a_test_recipient_is_refused_after_an_opt_out(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == TEST_PHONE))).scalar_one()
            client.wa_opted_out = True

    proof = await _prove(session_maker, run_id, added.recipient_id)

    assert proof.proven is False


@pytest.mark.asyncio
async def test_a_test_recipient_is_refused_after_the_number_changes(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == TEST_PHONE))).scalar_one()
            client.phone_e164 = "+4915100007777"

    proof = await _prove(session_maker, run_id, added.recipient_id)

    assert proof.proven is False


@pytest.mark.asyncio
async def test_a_rotated_configured_uuid_stops_the_canary(session_maker, fences, monkeypatch) -> None:
    """A rotation must be a mismatch, never a quiet switch of account."""
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_customer_uuid", OTHER_CUSTOMER_UUID, raising=False)
    reader = _CustomerReader(card={"uuid": OTHER_CUSTOMER_UUID, "phone": TEST_PHONE})

    proof = await _prove(session_maker, run_id, added.recipient_id, reader)

    assert proof.proven is False
    assert TEST_BINDING_MISMATCH in proof.reasons


@pytest.mark.asyncio
async def test_a_test_recipient_is_refused_when_the_live_read_is_uncertain(session_maker, fences) -> None:
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    reader = _CustomerReader(error=TimeoutError("slow"))

    proof = await _prove(session_maker, run_id, added.recipient_id, reader)

    assert proof.proven is False
    assert TEST_CUSTOMER_UNPROVEN in proof.reasons


@pytest.mark.asyncio
async def test_an_earned_recipient_still_walks_the_whole_history(session_maker, fences) -> None:
    """The earned contract is untouched: it still walks the whole history.

    The reader here refuses to answer either history call, so an earned
    recipient must come back unproven — and must come back having gone down the
    earned path, with the first-visit check present rather than skipped.
    """
    run_id, recipient_id = await seed_recipient(session_maker)

    proof = await _prove(session_maker, run_id, recipient_id)

    assert proof.proven is False
    assert proof.recipient_basis == VOUCHER_DELIVERY_BASIS_EARNED
    assert "first_visit_current" in proof.checks
    assert "first_visit_proof_applicable" not in proof.checks


# ---------------------------------------------------------------------------
# What the database refuses to store
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_test_binding_and_earned_proof_cannot_share_a_row(session_maker, fences) -> None:
    from sqlalchemy.exc import IntegrityError

    run_id, recipient_id = await seed_recipient(session_maker)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                row = await session.get(CampaignRecipient, recipient_id)
                row.easyweek_test_customer_uuid = uuid_module.UUID(TEST_CUSTOMER_UUID)


@pytest.mark.asyncio
async def test_an_altegio_recipient_cannot_carry_a_test_binding(session_maker, configuration) -> None:
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import PROVIDER_ALTEGIO

    run_id, recipient_id = await seed_recipient(session_maker, provider=PROVIDER_ALTEGIO)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                row = await session.get(CampaignRecipient, recipient_id)
                row.easyweek_test_customer_uuid = uuid_module.UUID(TEST_CUSTOMER_UUID)


@pytest.mark.asyncio
async def test_one_test_recipient_per_customer_per_preview(session_maker, fences) -> None:
    from sqlalchemy.exc import IntegrityError

    run_id, _ = await _empty_preview(session_maker)
    await _add(session_maker, run_id)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    CampaignRecipient(
                        provider=PROVIDER_EASYWEEK,
                        campaign_run_id=run_id,
                        company_id=COMPANY_ID,
                        phone_e164=TEST_PHONE,
                        status="candidate",
                        easyweek_test_customer_uuid=uuid_module.UUID(TEST_CUSTOMER_UUID),
                    )
                )


# ---------------------------------------------------------------------------
# Rebuilding the identity from the durable ledger
# ---------------------------------------------------------------------------


def _snapshot_with(**changes: Any):
    """A complete, self-consistent earned snapshot, then whatever is overridden."""
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
    from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, KARLSRUHE_LOCATION_UUID
    from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (
        ACCOUNT_UUID,
        BOOKING_UUID,
        EW_CUSTOMER_UUID,
        STAFFER_UUID,
    )

    fields: dict[str, Any] = {
        "exists": True,
        "status": "create_unknown",
        "reason_code": None,
        "recipient_basis": VOUCHER_DELIVERY_BASIS_EARNED,
        "campaign_run_id": 1,
        "campaign_recipient_id": 2,
        "company_id": COMPANY_ID,
        "target_order_uuid": None,
        "reconciliation_marker": "ewvd1-abcdef012345",
        "create_window_start": None,
        "create_window_end": None,
        "source_booking_uuid": str(BOOKING_UUID),
        "easyweek_customer_uuid": str(EW_CUSTOMER_UUID),
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "staffer_uuid": STAFFER_UUID,
        "payment_account_uuid": ACCOUNT_UUID,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "voucher_code_hmac": None,
        "hmac_key_id": None,
        "outbound_intent_uuid": None,
        "provider_message_id": None,
        "send_attempt_count": 0,
        "stage_plan_digests": {},
        "stage_timestamps": {},
        "manual_cleanup_required": False,
        "reconciliation_required": True,
        "evidence": {},
    }
    fields.update(changes)
    return ledger_module.LedgerSnapshot(**fields)


def test_an_earned_snapshot_rebuilds_into_an_earned_identity() -> None:
    from altegio_bot.campaigns.easyweek_voucher_delivery.runner import _identity_from_snapshot
    from altegio_bot.tests.easyweek_voucher_delivery_fixtures import BOOKING_UUID

    identity = _identity_from_snapshot(_snapshot_with())

    assert identity is not None
    assert identity.recipient_basis == VOUCHER_DELIVERY_BASIS_EARNED
    assert identity.source_booking_uuid == str(BOOKING_UUID)


def test_a_test_snapshot_rebuilds_with_a_genuinely_null_booking() -> None:
    """Not an empty string. The contract says NULL, and NULL is what it gets."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.runner import _identity_from_snapshot

    identity = _identity_from_snapshot(
        _snapshot_with(recipient_basis=VOUCHER_DELIVERY_BASIS_TEST, source_booking_uuid=None)
    )

    assert identity is not None
    assert identity.recipient_basis == VOUCHER_DELIVERY_BASIS_TEST
    assert identity.source_booking_uuid is None


@pytest.mark.parametrize(
    "changes, why",
    [
        pytest.param({"recipient_basis": None}, "no basis at all", id="basis_missing"),
        pytest.param({"recipient_basis": ""}, "an empty basis", id="basis_empty"),
        pytest.param({"recipient_basis": "something_else"}, "a word nobody wrote", id="basis_unknown"),
        pytest.param({"source_booking_uuid": None}, "earned with no booking", id="earned_without_booking"),
        pytest.param({"source_booking_uuid": ""}, "earned with an empty booking", id="earned_empty_booking"),
        pytest.param(
            {"recipient_basis": VOUCHER_DELIVERY_BASIS_TEST},
            "a test row carrying a booking",
            id="test_with_booking",
        ),
        pytest.param({"company_id": None}, "no company", id="company_missing"),
        pytest.param({"campaign_recipient_id": None}, "no recipient", id="recipient_missing"),
        pytest.param({"easyweek_customer_uuid": None}, "no customer", id="customer_missing"),
        pytest.param({"reconciliation_marker": "  "}, "a blank marker", id="marker_blank"),
        pytest.param({"voucher_template_uuid": "not-a-uuid"}, "a template that is not a uuid", id="template_bad"),
    ],
)
def test_an_incomplete_snapshot_rebuilds_into_nothing(changes: dict[str, Any], why: str) -> None:
    """Filling the gaps with `or 0` and `or ""` is what this replaces."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.runner import _identity_from_snapshot

    assert _identity_from_snapshot(_snapshot_with(**changes)) is None, why


@pytest.mark.asyncio
async def test_an_unknown_create_over_an_open_order_recovers_on_the_earned_basis(session_maker, fences) -> None:
    """The blocker: this branch raised TypeError and could never reconcile."""
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
    from altegio_bot.tests import test_easyweek_voucher_delivery_runner as delivery_tests

    request, reader = await delivery_tests._ready(session_maker)
    mutator = delivery_tests.FakeMutator(
        reader,
        marker=request.marker,
        create_error=delivery_tests.EasyWeekVoucherMutationUnknown("lost"),
    )
    await delivery_tests._create(session_maker, request, reader, mutator)
    open_order = await delivery_tests.marker_order(session_maker, marker=request.marker)
    reader.order_pages = [delivery_tests.orders_page([open_order])]
    reader.order = open_order
    calls_before = list(mutator.calls)

    report = await delivery_tests._reconcile(session_maker, request, reader)

    # Whatever it concludes, it concluded it — no TypeError, no mutation.
    assert mutator.calls == calls_before
    assert report.outcome in (
        runner_module_outcomes().OUTCOME_PROVEN,
        runner_module_outcomes().OUTCOME_UNKNOWN,
    )
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.recipient_basis == VOUCHER_DELIVERY_BASIS_EARNED


def runner_module_outcomes():
    from altegio_bot.campaigns.easyweek_voucher_delivery import runner as voucher_runner

    return voucher_runner


@pytest.mark.asyncio
async def test_an_unknown_create_over_an_open_order_recovers_on_the_test_basis(session_maker, fences) -> None:
    """The same recovery for a canary whose recipient has no booking at all."""
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
    from altegio_bot.campaigns.easyweek_voucher_delivery import runner as voucher_runner

    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    await _attach_canary(session_maker, run_id, added.recipient_id)
    await ledger_module.record_outcome(
        session_maker,
        status="create_unknown",
        expected_statuses=frozenset({"planned"}),
        reason_code="x",
        reconciliation_required=True,
        manual_cleanup_required=True,
    )
    snapshot = await ledger_module.load(session_maker)
    assert snapshot.recipient_basis == VOUCHER_DELIVERY_BASIS_TEST
    assert snapshot.source_booking_uuid is None

    identity = voucher_runner._identity_from_snapshot(snapshot)

    assert identity is not None
    assert identity.recipient_basis == VOUCHER_DELIVERY_BASIS_TEST
    assert identity.source_booking_uuid is None
    # And it still matches the row it was rebuilt from.
    assert identity.matches(snapshot) is True


# ---------------------------------------------------------------------------
# What the reports say, in refusal as well as in success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_empty_ledger_claims_no_first_visit_proof(session_maker) -> None:
    """The blocker: no row at all reported `first_visit_proof: earned`."""
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
    from altegio_bot.campaigns.easyweek_voucher_delivery import runner as voucher_runner

    snapshot = await ledger_module.load(session_maker)
    safe = snapshot.as_safe_dict()

    assert safe["ledger_row_exists"] is False
    assert safe["recipient_basis"] is None
    assert safe["first_visit_proof"] == "not_available"

    status = await voucher_runner.run_status(session_maker)
    assert status.ledger["first_visit_proof"] == "not_available"


@pytest.mark.asyncio
async def test_a_proven_test_canary_reports_not_applicable(session_maker, fences) -> None:
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module

    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)
    await _attach_canary(session_maker, run_id, added.recipient_id)

    safe = (await ledger_module.load(session_maker)).as_safe_dict()

    assert safe["recipient_basis"] == VOUCHER_DELIVERY_BASIS_TEST
    assert safe["first_visit_proof"] == "not_applicable"


@pytest.mark.asyncio
async def test_an_earned_canary_reports_earned(session_maker, fences) -> None:
    from altegio_bot.campaigns.easyweek_voucher_delivery import ledger as ledger_module
    from altegio_bot.tests import test_easyweek_voucher_delivery_runner as delivery_tests

    request, reader = await delivery_tests._ready(session_maker)
    mutator = delivery_tests.FakeMutator(reader, marker=request.marker)
    await delivery_tests._create(session_maker, request, reader, mutator)

    safe = (await ledger_module.load(session_maker)).as_safe_dict()

    assert safe["recipient_basis"] == VOUCHER_DELIVERY_BASIS_EARNED
    assert safe["first_visit_proof"] == "earned"


@pytest.mark.asyncio
@pytest.mark.parametrize("break_it", ["opt_out", "phone_change", "no_client", "malformed_binding"])
async def test_a_refused_test_recipient_is_never_reported_as_earned(session_maker, fences, break_it: str) -> None:
    """The blocker: early refusals fell back to the dataclass default.

    A refusal that misdescribes what it is refusing is worse than no report:
    it says the canary was about an entitlement somebody earned.
    """
    run_id, _ = await _empty_preview(session_maker)
    added = await _add(session_maker, run_id)

    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, added.recipient_id)
            client = (await session.execute(select(Client).where(Client.phone_e164 == TEST_PHONE))).scalar_one()
            if break_it == "opt_out":
                client.wa_opted_out = True
            elif break_it == "phone_change":
                client.phone_e164 = "+4915100007777"
            elif break_it == "no_client":
                row.client_id = None
            elif break_it == "malformed_binding":
                row.easyweek_test_customer_uuid = None
                row.status = "skipped"

    proof = await _prove(session_maker, run_id, added.recipient_id)
    safe = proof.as_safe_dict()

    assert proof.proven is False
    if break_it == "malformed_binding":
        # No binding left, so this is no longer a test row — but it must not be
        # reported as a PROVEN earned one either.
        assert safe["first_visit_proof"] != "not_applicable"
    else:
        assert proof.recipient_basis == VOUCHER_DELIVERY_BASIS_TEST
        assert safe["recipient_basis"] == VOUCHER_DELIVERY_BASIS_TEST
        assert safe["first_visit_proof"] == "not_applicable"
    for secret in (TEST_CUSTOMER_UUID, TEST_PHONE, "Synthetic Fixture"):
        assert secret not in repr(safe)


@pytest.mark.asyncio
async def test_a_proof_without_a_recipient_names_no_basis(session_maker, fences) -> None:
    """Nothing was read, so nothing may be claimed about it."""
    proof = await _prove(session_maker, 999_999, 999_998)

    assert proof.proven is False
    assert proof.recipient_basis is None
    assert proof.as_safe_dict()["first_visit_proof"] == "not_available"


@pytest.mark.asyncio
async def test_an_earned_recipient_reports_the_earned_basis_even_when_refused(session_maker, fences) -> None:
    run_id, recipient_id = await seed_recipient(session_maker)

    proof = await _prove(session_maker, run_id, recipient_id)

    assert proof.proven is False
    assert proof.recipient_basis == VOUCHER_DELIVERY_BASIS_EARNED
    assert proof.as_safe_dict()["first_visit_proof"] == "earned"
