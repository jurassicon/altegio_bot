"""The operator-editable EasyWeek preview (§37.1).

EasyWeek only started on 1 September, so the transitional August list has no
EasyWeek data to be proven from. An operator assembles it by typing phone
numbers — and that is a decision, not a proof. What these tests guard is that
the decision can never be mistaken for a proof, and that a screen which can add
a recipient cannot become a way to name any customer at all.

So: identity is resolved on the server, twice; the browser may say a phone
number and nothing else; the row carries `operator_manual_selection` and is
forbidden the earned source-proof columns; every existing-row shape has one
defined answer rather than a default; and the edit and its counters commit
together or not at all.

Every identity here is synthetic.
"""

from __future__ import annotations

import uuid as uuid_module
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import altegio_bot.campaigns.easyweek_manual_recipient as manual_module
import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.campaigns.easyweek_manual_recipient import (
    ACTION_CREATED,
    ACTION_INCLUDED,
    ACTION_REACTIVATED,
    ACTION_UNCHANGED,
    add_manual_recipient,
)
from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekError, EasyWeekRetryableError
from altegio_bot.main import app
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    Client,
)
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (  # noqa: F401 - fixtures
    COMPANY_ID,
    PERIOD_END,
    PERIOD_START,
    seed_recipient,
)

CUSTOMER_UUID = "aaaa1111-2222-4333-8444-bbbbbbbbbbbb"
OTHER_UUID = "cccc1111-2222-4333-8444-dddddddddddd"
PHONE = "+4915100000042"
FIRST_NAME = "Synthetic Person"


class _Reader:
    """The two reads §37.1 makes, and nothing else. Records every call."""

    def __init__(
        self,
        *,
        pages: list[dict[str, Any]] | None = None,
        card: dict[str, Any] | None = None,
        list_error: Exception | None = None,
        get_error: Exception | None = None,
    ) -> None:
        self.pages = pages if pages is not None else [_page([_row()])]
        self.card = card if card is not None else _row()
        self.list_error = list_error
        self.get_error = get_error
        self.calls: list[str] = []

    async def __aenter__(self) -> "_Reader":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        page = int(params.get("page", 1))
        self.calls.append(f"list:{page}")
        if self.list_error is not None:
            raise self.list_error
        index = page - 1
        if index >= len(self.pages):
            return _page([], page=page, last_page=len(self.pages))
        return self.pages[index]

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.calls.append("get")
        if self.get_error is not None:
            raise self.get_error
        return self.card

    # Nothing else exists on this path. A write would be a person invented in a
    # production workspace on the strength of a typo.
    async def create_customer(self, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("§37.1 must never create an EasyWeek customer")


def _row(**changes: Any) -> dict[str, Any]:
    row = {"uuid": CUSTOMER_UUID, "phone": PHONE, "first_name": FIRST_NAME}
    row.update(changes)
    return row


def _page(rows: list[dict[str, Any]], *, page: int = 1, last_page: int = 1, total: int | None = None) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {
            "current_page": page,
            "last_page": last_page,
            "per_page": 100,
            "total": len(rows) if total is None else total,
        },
    }


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncClient:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def _preview(session_maker, *, phone: str = PHONE, keep_recipient: bool = False) -> int:
    """A completed EasyWeek preview and one local client for `phone`."""
    run_id, recipient_id = await seed_recipient(session_maker, phone=phone, client_phone=phone)
    if not keep_recipient:
        async with session_maker() as session:
            async with session.begin():
                await session.delete(await session.get(CampaignRecipient, recipient_id))
    return run_id


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


async def _add(session_maker, run_id: int, reader: _Reader | None = None, **kwargs: Any):
    return await add_manual_recipient(
        session_maker,
        run_id=run_id,
        phone=kwargs.pop("phone", PHONE),
        reader=reader or _Reader(),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# The happy path, and what the row is allowed to claim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_operator_can_add_a_recipient_by_phone(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is True
    assert outcome.action == ACTION_CREATED
    # Resolved by listing, then re-read directly. Both, in that order.
    assert reader.calls == ["list:1", "get"]
    [row] = await _rows(session_maker, run_id)
    assert row.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert row.easyweek_customer_uuid == uuid_module.UUID(CUSTOMER_UUID)
    assert row.status == "candidate"


@pytest.mark.asyncio
async def test_a_manual_row_carries_no_earned_proof(session_maker, configuration) -> None:
    """A decision must never be able to look like a proven first visit."""
    run_id = await _preview(session_maker)

    await _add(session_maker, run_id)

    [row] = await _rows(session_maker, run_id)
    assert row.source_easyweek_event_id is None
    assert row.source_record_id is None
    assert row.source_booking_uuid is None
    assert row.source_visits_total is None
    assert row.source_visits_total_updated_at is None
    assert row.altegio_client_id is None


@pytest.mark.asyncio
async def test_the_earned_proof_columns_are_forbidden_on_a_manual_row(session_maker, configuration) -> None:
    """Not "does not happen to have them" — cannot have them."""
    run_id = await _preview(session_maker)
    added = await _add(session_maker, run_id)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                row = await session.get(CampaignRecipient, added.recipient_id)
                row.source_visits_total = 1


@pytest.mark.asyncio
async def test_the_browser_may_not_name_the_customer(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader()

    by_uuid = await _add(session_maker, run_id, reader, customer_uuid=CUSTOMER_UUID)
    by_client = await _add(session_maker, run_id, reader, altegio_client_id=7)

    assert by_uuid.reason == manual_module.IDENTITY_NOT_ACCEPTED
    assert by_client.reason == manual_module.IDENTITY_NOT_ACCEPTED
    assert reader.calls == []
    assert await _rows(session_maker, run_id) == []


# ---------------------------------------------------------------------------
# Resolving the customer: two reads, and every way they can fail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_every_claimed_page_of_the_phone_lookup_is_read(session_maker, configuration) -> None:
    """An unfinished read is not an absence: the unseen row may be the customer."""
    run_id = await _preview(session_maker)
    reader = _Reader(
        pages=[
            _page([], page=1, last_page=3, total=1),
            _page([], page=2, last_page=3, total=1),
            _page([_row()], page=3, last_page=3, total=1),
        ]
    )

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is True
    assert reader.calls == ["list:1", "list:2", "list:3", "get"]


@pytest.mark.asyncio
async def test_an_incomplete_page_walk_is_never_read_as_an_absence(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    # The server counted a row it never handed over.
    reader = _Reader(pages=[_page([], page=1, last_page=1, total=4)])

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is False
    assert outcome.reason == manual_module.CUSTOMER_UNPROVEN
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_an_absent_customer_is_refused_and_never_created(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader(pages=[_page([])])

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == manual_module.CUSTOMER_ABSENT
    assert "get" not in reader.calls
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_two_customers_on_one_number_are_refused_without_picking(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader(pages=[_page([_row(), _row(uuid=OTHER_UUID)])])

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == manual_module.CUSTOMER_AMBIGUOUS
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "card, get_error, reason",
    [
        pytest.param(_row(uuid=OTHER_UUID), None, manual_module.CUSTOMER_UNPROVEN, id="uuid_mismatch"),
        pytest.param(_row(phone="+4915100009999"), None, manual_module.CUSTOMER_UNPROVEN, id="phone_mismatch"),
        pytest.param({"nonsense": True}, None, manual_module.CUSTOMER_UNPROVEN, id="malformed"),
        pytest.param(_row(first_name="   "), None, manual_module.CUSTOMER_NAME_MISSING, id="blank_name"),
        pytest.param(None, EasyWeekError("boom"), manual_module.CUSTOMER_UNPROVEN, id="transport"),
        pytest.param(None, TimeoutError("slow"), manual_module.CUSTOMER_UNPROVEN, id="timeout"),
    ],
)
async def test_a_direct_read_that_does_not_prove_the_customer_writes_nothing(
    session_maker, configuration, card, get_error, reason
) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader(card=card, get_error=get_error)

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.ok is False
    assert outcome.reason == reason
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        pytest.param(EasyWeekAuthError("denied", status_code=401), id="auth"),
        pytest.param(EasyWeekRetryableError("rate limited", operation="list", attempts=3), id="rate_limited"),
        pytest.param(EasyWeekRetryableError("server error", operation="list", attempts=3), id="server_error"),
        pytest.param(EasyWeekError("protocol"), id="transport"),
    ],
)
async def test_a_lookup_that_never_read_the_workspace_writes_nothing(session_maker, configuration, error) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader(list_error=error)

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == manual_module.CUSTOMER_UNPROVEN
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_a_customer_whose_name_is_missing_is_refused(session_maker, configuration) -> None:
    """The delivery template has a name slot; a blank would reach a customer."""
    run_id = await _preview(session_maker)
    reader = _Reader(pages=[_page([_row(first_name="")])], card=_row(first_name=""))

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == manual_module.CUSTOMER_NAME_MISSING
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("phone", ["", "   ", "nonsense", "not-a-number"])
async def test_an_unusable_phone_reads_nothing(session_maker, configuration, phone: str) -> None:
    run_id = await _preview(session_maker)
    reader = _Reader()

    outcome = await _add(session_maker, run_id, reader, phone=phone)

    assert outcome.reason == manual_module.PHONE_UNUSABLE
    assert reader.calls == []


# ---------------------------------------------------------------------------
# The local client half
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_number_with_no_local_client_is_refused(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == PHONE))).scalar_one()
            client.phone_e164 = "+4915100008888"
    reader = _Reader()

    outcome = await _add(session_maker, run_id, reader)

    assert outcome.reason == manual_module.CLIENT_ABSENT
    assert reader.calls == []


@pytest.mark.asyncio
async def test_two_local_clients_for_one_number_are_refused(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_client_id=990011,
                    phone_e164=PHONE,
                    display_name="Second Synthetic",
                    raw={},
                )
            )

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.CLIENT_AMBIGUOUS
    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_a_client_from_another_company_does_not_count(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == PHONE))).scalar_one()
            client.company_id = 315607

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.CLIENT_ABSENT


@pytest.mark.asyncio
async def test_an_opted_out_client_is_never_added(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == PHONE))).scalar_one()
            client.wa_opted_out = True

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.CLIENT_OPTED_OUT
    assert await _rows(session_maker, run_id) == []


# ---------------------------------------------------------------------------
# Which runs may be edited at all
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_missing_run_is_refused(session_maker, configuration) -> None:
    outcome = await _add(session_maker, 999_999)

    assert outcome.reason == manual_module.RUN_NOT_FOUND


@pytest.mark.asyncio
async def test_an_altegio_run_is_refused(session_maker, configuration) -> None:
    """The Altegio path has its own contract and keeps it."""
    run_id, _ = await seed_recipient(session_maker, provider=PROVIDER_ALTEGIO, phone=PHONE, client_phone=PHONE)

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.RUN_NOT_EASYWEEK


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "failed", "discarded", "deleted"])
async def test_a_run_that_is_not_a_completed_preview_is_refused(session_maker, configuration, status: str) -> None:
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            run.status = status

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.RUN_NOT_EDITABLE


@pytest.mark.asyncio
async def test_a_branch_outside_the_registry_is_refused(session_maker, configuration) -> None:
    """The browser sends a company id; it does not define what one means."""
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            run.company_ids = [424242]

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.RUN_BRANCH_UNKNOWN


@pytest.mark.asyncio
async def test_a_preview_already_used_for_send_real_is_refused(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                CampaignRun(
                    provider=PROVIDER_EASYWEEK,
                    campaign_code="new_clients_monthly",
                    mode="send-real",
                    company_ids=[COMPANY_ID],
                    source_preview_run_id=run_id,
                    period_start=PERIOD_START,
                    period_end=PERIOD_END,
                    status="completed",
                )
            )

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.RUN_NOT_EDITABLE


# ---------------------------------------------------------------------------
# What happens when the person is already in the snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adding_the_same_person_twice_creates_one_row(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)

    first = await _add(session_maker, run_id)
    second = await _add(session_maker, run_id)

    assert second.ok is True
    assert second.action == ACTION_UNCHANGED
    assert second.recipient_id == first.recipient_id
    assert len(await _rows(session_maker, run_id)) == 1


@pytest.mark.asyncio
async def test_an_active_earned_candidate_stays_earned(session_maker, configuration) -> None:
    """Asking for somebody the segmenter proved does not downgrade the proof."""
    run_id = await _preview(session_maker, keep_recipient=True)

    outcome = await _add(session_maker, run_id)

    assert outcome.ok is True
    assert outcome.action == ACTION_UNCHANGED
    [row] = await _rows(session_maker, run_id)
    assert row.recipient_basis == RECIPIENT_BASIS_EARNED
    assert row.source_booking_uuid is not None


@pytest.mark.asyncio
async def test_a_removed_earned_candidate_comes_back_as_earned(session_maker, configuration) -> None:
    """Restored as what it was — not manufactured into a decision."""
    run_id = await _preview(session_maker, keep_recipient=True)
    [row] = await _rows(session_maker, run_id)
    await runner_module.remove_recipient_from_preview(run_id, row.id)

    outcome = await _add(session_maker, run_id)

    assert outcome.action == ACTION_REACTIVATED
    [restored] = await _rows(session_maker, run_id)
    assert restored.recipient_basis == RECIPIENT_BASIS_EARNED
    assert restored.status == "candidate"
    assert restored.excluded_reason is None
    assert restored.source_booking_uuid is not None


@pytest.mark.asyncio
async def test_a_removed_manual_candidate_comes_back_as_manual(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    added = await _add(session_maker, run_id)
    await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)

    outcome = await _add(session_maker, run_id)

    assert outcome.action == ACTION_REACTIVATED
    assert outcome.recipient_id == added.recipient_id
    [restored] = await _rows(session_maker, run_id)
    assert restored.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert restored.status == "candidate"


@pytest.mark.asyncio
async def test_including_an_automatically_excluded_row_keeps_the_original_reason(session_maker, configuration) -> None:
    """An override that erases what it overrode leaves nobody able to say why."""
    run_id = await _preview(session_maker, keep_recipient=True)
    [row] = await _rows(session_maker, run_id)
    async with session_maker() as session:
        async with session.begin():
            stored = await session.get(CampaignRecipient, row.id)
            stored.status = "skipped"
            stored.excluded_reason = "has_records_before_period"

    outcome = await _add(session_maker, run_id)

    assert outcome.ok is True
    assert outcome.action == ACTION_INCLUDED
    assert outcome.overrode_auto_reason == "has_records_before_period"
    [included] = await _rows(session_maker, run_id)
    assert included.status == "candidate"
    assert included.excluded_reason is None
    assert included.auto_excluded_reason == "has_records_before_period"
    # Still earned: the segmenter's proof is intact, the operator only overrode
    # its verdict about whether to send.
    assert included.recipient_basis == RECIPIENT_BASIS_EARNED


@pytest.mark.asyncio
async def test_two_rows_for_one_person_are_refused_without_picking(session_maker, configuration) -> None:
    run_id = await _preview(session_maker, keep_recipient=True)
    [earned] = await _rows(session_maker, run_id)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                CampaignRecipient(
                    provider=PROVIDER_EASYWEEK,
                    campaign_run_id=run_id,
                    company_id=COMPANY_ID,
                    client_id=earned.client_id,
                    phone_e164=PHONE,
                    status="skipped",
                    excluded_reason="manual_removed",
                    recipient_basis=RECIPIENT_BASIS_MANUAL,
                    easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUID),
                )
            )

    outcome = await _add(session_maker, run_id)

    assert outcome.reason == manual_module.ROWS_AMBIGUOUS
    assert len(await _rows(session_maker, run_id)) == 2


@pytest.mark.asyncio
async def test_one_active_row_per_customer_is_a_database_rule(session_maker, configuration) -> None:
    """Not merely remembered by the add path: enforced."""
    run_id = await _preview(session_maker)
    await _add(session_maker, run_id)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                session.add(
                    CampaignRecipient(
                        provider=PROVIDER_EASYWEEK,
                        campaign_run_id=run_id,
                        company_id=COMPANY_ID,
                        phone_e164=PHONE,
                        status="candidate",
                        recipient_basis=RECIPIENT_BASIS_MANUAL,
                        easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUID),
                    )
                )


# ---------------------------------------------------------------------------
# Remove, and the transaction the edit lives in
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_is_soft_idempotent_and_keeps_the_basis(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    added = await _add(session_maker, run_id)

    first = await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)
    second = await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)

    assert first.status == "skipped"
    assert first.excluded_reason == "manual_removed"
    assert second.status == "skipped"
    [row] = await _rows(session_maker, run_id)
    # The audit survives the removal: what it was, and who it was.
    assert row.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert row.easyweek_customer_uuid is not None


@pytest.mark.asyncio
async def test_the_counters_follow_every_edit(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    added = await _add(session_maker, run_id)

    async with session_maker() as session:
        after_add = await session.get(CampaignRun, run_id)
        added_count = after_add.candidates_count
    await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)
    async with session_maker() as session:
        after_remove = await session.get(CampaignRun, run_id)

    assert added_count == 1
    assert after_remove.candidates_count == 0
    assert after_remove.total_clients_seen == 1


@pytest.mark.asyncio
async def test_a_counter_failure_rolls_the_whole_add_back(session_maker, configuration, monkeypatch) -> None:
    run_id = await _preview(session_maker)

    async def boom(session, run):
        raise RuntimeError("counter recompute failed")

    monkeypatch.setattr(runner_module, "recompute_snapshot_counters", boom)

    with pytest.raises(RuntimeError):
        await _add(session_maker, run_id)

    assert await _rows(session_maker, run_id) == []


@pytest.mark.asyncio
async def test_a_frozen_preview_refuses_add_and_remove(session_maker, configuration) -> None:
    """Once the canary holds the preview, the snapshot stops being editable."""
    from altegio_bot.tests.test_easyweek_test_recipient import _attach_canary

    run_id = await _preview(session_maker)
    added = await _add(session_maker, run_id)
    await _attach_canary(session_maker, run_id, added.recipient_id)

    blocked = await _add(session_maker, run_id, _Reader(), phone="+4915100000043")

    assert blocked.ok is False
    assert blocked.reason == manual_module.PREVIEW_FROZEN
    with pytest.raises(ValueError, match="canary"):
        await runner_module.remove_recipient_from_preview(run_id, added.recipient_id)


# ---------------------------------------------------------------------------
# The Ops surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_endpoint_adds_a_recipient_and_reports_the_new_count(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader())

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add-manual", json={"phone": PHONE})

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["recipient_basis"] == RECIPIENT_BASIS_MANUAL
    assert body["candidates_count"] == 1
    assert body["campaign_send_authorized"] is False
    assert body["global_ready_for_send"] is False
    assert body["recipient"]["recipient_basis"] == RECIPIENT_BASIS_MANUAL
    # Presence only — the UUID itself is never serialised.
    assert body["recipient"]["easyweek_customer_recorded"] is True
    assert CUSTOMER_UUID not in resp.text


@pytest.mark.asyncio
async def test_a_refusal_carries_a_code_and_no_personal_data(
    session_maker, configuration, http_client, monkeypatch, caplog
) -> None:
    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader(pages=[_page([])]))

    with caplog.at_level("INFO"):
        resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add-manual", json={"phone": PHONE})

    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == manual_module.CUSTOMER_ABSENT
    for surface in (resp.text, caplog.text):
        for secret in (PHONE, CUSTOMER_UUID, FIRST_NAME):
            assert secret not in surface


@pytest.mark.asyncio
async def test_the_endpoint_never_accepts_an_identity_from_the_browser(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader())

    resp = await http_client.post(
        f"/ops/campaigns/runs/{run_id}/recipients/add-manual",
        json={"phone": PHONE, "customer_uuid": CUSTOMER_UUID, "altegio_client_id": 9},
    )

    # Extra fields are not a way in: the model does not carry them, so they are
    # ignored rather than trusted, and the row is still resolved server-side.
    assert resp.status_code == 201
    [row] = await _rows(session_maker, run_id)
    assert row.easyweek_customer_uuid == uuid_module.UUID(CUSTOMER_UUID)
    assert row.altegio_client_id is None


@pytest.mark.asyncio
async def test_the_canary_endpoint_is_untouched_by_this_phase(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    """§36.11 still adds only the configured account, and is still fenced."""
    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader())

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add", json={"phone": PHONE})

    assert resp.status_code == 422
    assert resp.json()["detail"]["reason"] == "voucher_delivery_test_recipient_disabled"
    assert await _rows(session_maker, run_id) == []


# ---------------------------------------------------------------------------
# The preview form itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_form_offers_easyweek_and_its_registry_branches(http_client, monkeypatch) -> None:
    """The branch list is the server's registry, rendered — not a hardcoded one."""
    import json

    from altegio_bot.settings import settings

    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": 308697,
                    "location_uuid": "11111111-1111-4111-8111-111111111111",
                    "meta_template_prefix": "du",
                    "booking_page_url": "https://durlach.example.invalid/",
                },
                "rastatt": {
                    "location_id": 315607,
                    "location_uuid": "22222222-2222-4222-8222-222222222222",
                    "meta_template_prefix": "ra",
                    "booking_page_url": "https://rastatt.example.invalid/",
                },
                "karlsruhe": {
                    "location_id": 322579,
                    "location_uuid": "8395fab6-7ee8-4702-88d9-fd78f92539c1",
                    "meta_template_prefix": "ka",
                    "booking_page_url": "https://karlsruhe.example.invalid/",
                },
            }
        ),
        raising=False,
    )

    page = (await http_client.get("/ops/campaigns/new-clients")).text

    assert '<option value="easyweek">easyweek</option>' in page
    for company_id in (308697, 315607, 322579):
        assert f'value="{company_id}"' in page
    # And says plainly that this phase does not send.
    assert "§37.1" in page and "закрыт" in page


@pytest.mark.asyncio
async def test_the_form_hides_the_altegio_only_controls_for_easyweek(http_client, configuration) -> None:
    """Not removed for Altegio — hidden when they do not apply."""
    page = (await http_client.get("/ops/campaigns/new-clients")).text

    assert "altegio-only" in page and "easyweek-only" in page
    assert "onProviderChange()" in page
    # The Altegio controls still exist for Altegio.
    assert 'id="f-card-type"' in page
    assert 'id="f-followup-enabled"' in page


@pytest.mark.asyncio
async def test_an_easyweek_preview_request_needs_no_location_id(http_client, configuration, session_maker) -> None:
    resp = await http_client.post(
        "/ops/campaigns/new-clients/preview",
        json={
            "provider": "easyweek",
            "company_id": COMPANY_ID,
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-31T23:59:59Z",
        },
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["provider"] == PROVIDER_EASYWEEK


@pytest.mark.asyncio
async def test_an_easyweek_preview_for_an_unknown_branch_is_refused(http_client, configuration) -> None:
    resp = await http_client.post(
        "/ops/campaigns/new-clients/preview",
        json={
            "provider": "easyweek",
            "company_id": 424242,
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-31T23:59:59Z",
        },
    )

    assert resp.status_code == 422
    assert "registry" in resp.text


@pytest.mark.asyncio
async def test_an_altegio_preview_still_requires_its_location_id(http_client, configuration) -> None:
    """The Altegio contract is unchanged, including what it refuses."""
    resp = await http_client.post(
        "/ops/campaigns/new-clients/preview",
        json={
            "provider": "altegio",
            "company_id": 758285,
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-31T23:59:59Z",
        },
    )

    assert resp.status_code == 422
    assert "location_id" in resp.text


def test_no_external_mutation_is_reachable_from_this_module() -> None:
    """Structural: the whole surface is two reads."""
    import inspect

    source = inspect.getsource(manual_module)

    # It may READ the voucher ledger — that is how a frozen preview is
    # recognised — but nothing here may create, pay, send or enqueue.
    for forbidden in (
        "create_customer(",
        ".post(",
        "MessageJob",
        "OutboxMessage",
        "send_message",
        "run_create",
        "run_pay",
        "run_deliver",
    ):
        assert forbidden not in source, forbidden
