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


async def _auto_excluded(session_maker, run_id: int, *, reason: str = "has_records_before_period") -> int:
    """A row shaped the way the segmenter actually produces an excluded one.

    Not an eligible candidate forced to `skipped`: the segmenter attaches the
    five source-proof columns EXACTLY when a row is eligible, so a row it
    excluded carries none of them. Building the unrealistic shape and calling it
    realistic is how a test ends up proving something the product never does.
    """
    async with session_maker() as session:
        async with session.begin():
            client = (await session.execute(select(Client).where(Client.phone_e164 == PHONE))).scalar_one()
            row = CampaignRecipient(
                provider=PROVIDER_EASYWEEK,
                campaign_run_id=run_id,
                company_id=COMPANY_ID,
                client_id=client.id,
                phone_e164=PHONE,
                display_name=client.display_name,
                local_client_found=True,
                status="skipped",
                excluded_reason=reason,
            )
            session.add(row)
            await session.flush()
            return row.id


@pytest.mark.asyncio
async def test_including_an_automatically_excluded_row_makes_it_a_manual_selection(
    session_maker, configuration
) -> None:
    """The operator's decision becomes the basis — and keeps what it overrode.

    The segmenter refused to call this person eligible. Including them anyway
    does not make the refusal into a proof: the row becomes an explicit manual
    selection, and the original verdict is kept as audit rather than erased.
    """
    run_id = await _preview(session_maker)
    row_id = await _auto_excluded(session_maker, run_id)

    outcome = await _add(session_maker, run_id)

    assert outcome.ok is True
    assert outcome.action == ACTION_INCLUDED
    assert outcome.recipient_id == row_id
    assert outcome.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert outcome.overrode_auto_reason == "has_records_before_period"
    [included] = await _rows(session_maker, run_id)
    assert included.status == "candidate"
    assert included.excluded_reason is None
    assert included.auto_excluded_reason == "has_records_before_period"
    assert included.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert included.easyweek_customer_uuid == uuid_module.UUID(CUSTOMER_UUID)
    assert included.phone_e164 == PHONE
    assert included.display_name == FIRST_NAME
    # And it carries no first-visit evidence at all.
    assert included.source_easyweek_event_id is None
    assert included.source_booking_uuid is None
    assert included.source_visits_total is None


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


# ---------------------------------------------------------------------------
# The production client, end to end through the endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_endpoint_works_with_the_real_client_over_a_mock_transport(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    """Not a hand-written fake: the class the deployment actually constructs."""
    import httpx

    from altegio_bot.easyweek_client import EasyWeekClient

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.method} {request.url.path}")
        assert request.method == "GET", "the manual add path must never write"
        if request.url.path.endswith(f"/customers/{CUSTOMER_UUID}"):
            return httpx.Response(200, json=_row())
        if request.url.path.endswith("/customers"):
            assert request.url.params["phone"] == PHONE
            return httpx.Response(200, json=_page([_row()]))
        raise AssertionError(f"unexpected path {request.url.path}")

    async def _no_sleep(_seconds: float) -> None:
        return None

    def build_client() -> EasyWeekClient:
        return EasyWeekClient(
            api_key="SYNTHETIC-API-KEY",
            workspace_slug="synthetic-workspace",
            base_url="https://my.easyweek.io/api/public/v2",
            transport=httpx.MockTransport(handler),
            sleep=_no_sleep,
            max_attempts=2,
        )

    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", build_client)

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add-manual", json={"phone": PHONE})

    assert resp.status_code == 201, resp.text
    assert [call.split()[0] for call in seen] == ["GET", "GET"]
    [row] = await _rows(session_maker, run_id)
    assert row.recipient_basis == RECIPIENT_BASIS_MANUAL


# ---------------------------------------------------------------------------
# The basis the answer reports is the basis the row has
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_top_level_basis_never_contradicts_the_recipient(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    """Adding somebody the segmenter proved leaves them earned — and says so."""
    run_id = await _preview(session_maker, keep_recipient=True)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader())

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add-manual", json={"phone": PHONE})

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["requested_basis"] == RECIPIENT_BASIS_MANUAL
    assert body["recipient_basis"] == RECIPIENT_BASIS_EARNED
    assert body["recipient"]["recipient_basis"] == RECIPIENT_BASIS_EARNED
    assert body["action"] == ACTION_UNCHANGED


@pytest.mark.asyncio
async def test_a_refusal_claims_no_resulting_basis(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)

    outcome = await _add(session_maker, run_id, _Reader(pages=[_page([])]))

    assert outcome.ok is False
    assert outcome.recipient_basis is None
    safe = outcome.as_safe_dict()
    assert safe["requested_basis"] == RECIPIENT_BASIS_MANUAL
    assert safe["recipient_basis"] is None


# ---------------------------------------------------------------------------
# What the database refuses to hold
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_active_earned_easyweek_candidate_must_carry_its_proof(session_maker, configuration) -> None:
    """`earned` is a claim about evidence, and the evidence is those columns."""
    run_id = await _preview(session_maker, keep_recipient=True)
    [row] = await _rows(session_maker, run_id)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                stored = await session.get(CampaignRecipient, row.id)
                stored.source_booking_uuid = None


@pytest.mark.asyncio
async def test_an_auto_excluded_reason_belongs_only_to_a_manual_row(session_maker, configuration) -> None:
    run_id = await _preview(session_maker, keep_recipient=True)
    [row] = await _rows(session_maker, run_id)

    with pytest.raises(IntegrityError):
        async with session_maker() as session:
            async with session.begin():
                stored = await session.get(CampaignRecipient, row.id)
                stored.auto_excluded_reason = "has_records_before_period"


# ---------------------------------------------------------------------------
# Remove
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_refuses_an_automatically_excluded_row(session_maker, configuration) -> None:
    """Overwriting the segmenter's reason would destroy why it excluded them."""
    run_id = await _preview(session_maker)
    row_id = await _auto_excluded(session_maker, run_id)

    with pytest.raises(ValueError, match="автоматически"):
        await runner_module.remove_recipient_from_preview(run_id, row_id)

    async with session_maker() as session:
        row = await session.get(CampaignRecipient, row_id)
        run = await session.get(CampaignRun, run_id)
    assert row.excluded_reason == "has_records_before_period"
    assert run.candidates_count == 0


@pytest.mark.asyncio
async def test_remove_keeps_the_basis_of_an_earned_candidate(session_maker, configuration) -> None:
    run_id = await _preview(session_maker, keep_recipient=True)
    [row] = await _rows(session_maker, run_id)

    await runner_module.remove_recipient_from_preview(run_id, row.id)

    [removed] = await _rows(session_maker, run_id)
    assert removed.recipient_basis == RECIPIENT_BASIS_EARNED
    assert removed.source_booking_uuid is not None
    assert removed.excluded_reason == "manual_removed"


@pytest.mark.asyncio
async def test_remove_keeps_the_override_audit_of_a_manual_row(session_maker, configuration) -> None:
    run_id = await _preview(session_maker)
    row_id = await _auto_excluded(session_maker, run_id)
    await _add(session_maker, run_id)

    await runner_module.remove_recipient_from_preview(run_id, row_id)

    [removed] = await _rows(session_maker, run_id)
    assert removed.excluded_reason == "manual_removed"
    assert removed.auto_excluded_reason == "has_records_before_period"
    assert removed.recipient_basis == RECIPIENT_BASIS_MANUAL


# ---------------------------------------------------------------------------
# The canary will not deliver to a manual selection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_canary_refuses_a_manual_recipient_before_touching_anything(
    session_maker, configuration, monkeypatch
) -> None:
    """Honest about the basis, and refused before any live read or ledger row."""
    from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import prove_recipient
    from altegio_bot.campaigns.easyweek_voucher_delivery.identity import RECIPIENT_BASIS_UNSUPPORTED
    from altegio_bot.utils import utcnow

    run_id = await _preview(session_maker)
    added = await _add(session_maker, run_id)

    class _Forbidden:
        async def get_booking(self, booking_uuid: str):  # pragma: no cover - must not run
            raise AssertionError("a manual recipient must be refused before any live read")

        async def get_customer(self, customer_uuid: str):  # pragma: no cover - must not run
            raise AssertionError("a manual recipient must be refused before any live read")

        async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100):
            raise AssertionError("a manual recipient must be refused before any live read")

    async with session_maker() as session:
        proof = await prove_recipient(
            session,
            preview_run_id=run_id,
            campaign_recipient_id=added.recipient_id,
            expected_company_id=COMPANY_ID,
            client_reader=_Forbidden(),
            now=utcnow(),
        )

    assert proof.proven is False
    assert proof.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert RECIPIENT_BASIS_UNSUPPORTED in proof.reasons
    safe = proof.as_safe_dict()
    assert safe["recipient_basis"] == RECIPIENT_BASIS_MANUAL
    assert safe["first_visit_proof"] != "earned"
    for secret in (PHONE, CUSTOMER_UUID, FIRST_NAME):
        assert secret not in repr(safe)


@pytest.mark.asyncio
async def test_the_test_endpoint_refuses_a_manual_row_without_touching_it(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    """No basis conversion, and no IntegrityError surfacing as a 500."""
    from altegio_bot.settings import settings

    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader())
    added = await _add(session_maker, run_id)

    # Open the §36.11 fences so the refusal comes from the row, not the fence.
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_canary_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_recipient_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_test_customer_uuid", CUSTOMER_UUID, raising=False)

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add", json={"phone": PHONE})

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["reason"] == "test_recipient_rows_ambiguous"
    [row] = await _rows(session_maker, run_id)
    assert row.id == added.recipient_id
    assert row.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert row.easyweek_test_customer_uuid is None


# ---------------------------------------------------------------------------
# Reports and the provider-aware UI
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_report_counts_bases_and_overrides(session_maker, configuration, http_client) -> None:
    run_id = await _preview(session_maker)
    await _auto_excluded(session_maker, run_id, reason="has_records_before_period")
    await _add(session_maker, run_id)

    report = (await http_client.get(f"/ops/campaigns/runs/{run_id}/report")).json()

    assert report["recipient_basis"]["by_basis"] == {RECIPIENT_BASIS_MANUAL: 1}
    assert report["recipient_basis"]["manual_overrides_by_auto_reason"] == {"has_records_before_period": 1}
    assert report["recipient_basis"]["manual_overrides_total"] == 1
    # The long-standing block is untouched.
    assert "by_reason" in report["excluded"]
    # Every row has exactly one basis, so the bases account for the whole
    # snapshot: what the run reports as seen, and what it reports as eligible.
    assert sum(report["recipient_basis"]["by_basis"].values()) == report["total_found"]
    assert report["eligible"] == 1


@pytest.mark.asyncio
async def test_the_easyweek_detail_page_shows_real_reasons_not_altegio_zeros(
    session_maker, configuration, http_client
) -> None:
    run_id = await _preview(session_maker)
    await _auto_excluded(session_maker, run_id, reason="easyweek_specific_future_reason")

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "Excluded (EasyWeek)" in page
    # A reason this page has never heard of still appears.
    assert "easyweek_specific_future_reason" in page
    # And the Altegio-only vocabulary does not.
    assert "CRM unavailable" not in page


@pytest.mark.asyncio
async def test_the_preview_page_keeps_the_card_cleanup_altegio_only(http_client, configuration) -> None:
    page = (await http_client.get("/ops/campaigns/new-clients")).text

    # The destructive call checks the provider itself, not only the button's
    # visibility, and a stale in-flight read cannot repaint the panel.
    assert "OUTSTANDING_GENERATION" in page
    assert "if (isEasyWeek()) {" in page
    assert 'provider: "altegio"' in page
    assert "provider=altegio" in page


@pytest.mark.asyncio
async def test_the_card_endpoints_refuse_easyweek_without_building_a_client(
    http_client, configuration, monkeypatch
) -> None:
    """Backend fails closed even if a stale click somehow reached it."""
    import altegio_bot.campaigns.loyalty_cleanup as loyalty_cleanup

    def forbidden(*args: Any, **kwargs: Any):  # pragma: no cover - must not run
        raise AssertionError("EasyWeek must never construct an Altegio loyalty client")

    monkeypatch.setattr(loyalty_cleanup, "AltegioLoyaltyClient", forbidden, raising=False)

    listed = await http_client.get(
        "/ops/campaigns/outstanding-cards",
        params={"campaign_code": "new_clients_monthly", "company_id": COMPANY_ID, "provider": "easyweek"},
    )
    deleted = await http_client.post(
        "/ops/campaigns/bulk-delete-cards",
        json={"provider": "easyweek", "campaign_code": "new_clients_monthly", "company_id": COMPANY_ID},
    )

    assert listed.status_code == 409
    assert deleted.status_code == 409


@pytest.mark.asyncio
async def test_the_preview_page_offers_both_easyweek_add_actions(http_client, configuration, session_maker) -> None:
    run_id = await _preview(session_maker)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "showAddRecipientForm('manual')" in page
    assert "showAddRecipientForm('test')" in page
    assert "Add test recipient" in page
    assert "recipients/add-manual" in page
    assert "§37.1" in page and "§36.11" in page


@pytest.mark.asyncio
async def test_easyweek_preview_is_discoverable_from_the_campaigns_page(
    http_client, configuration, session_maker
) -> None:
    run_id = await _preview(session_maker)

    general = await http_client.get("/ops/campaigns")
    assert general.status_code == 200
    assert 'href="/ops/campaigns?provider=easyweek&amp;mode=preview"' in general.text
    # The Altegio default-list contract still hides previews; the new link is
    # a provider-scoped, one-click way into the EasyWeek editor.
    assert f'href="/ops/campaigns/{run_id}"' not in general.text

    previews = await http_client.get("/ops/campaigns", params={"provider": "easyweek", "mode": "preview"})
    assert previews.status_code == 200
    assert f'href="/ops/campaigns/{run_id}"' in previews.text
    assert f"runFromPreview({run_id})" not in previews.text


@pytest.mark.asyncio
async def test_easyweek_detail_shows_the_active_snapshot_before_progress(
    http_client, configuration, session_maker, monkeypatch
) -> None:
    run_id = await _preview(session_maker)
    monkeypatch.setattr(campaigns_api_module, "EasyWeekClient", lambda: _Reader())
    added = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/add-manual", json={"phone": PHONE})
    assert added.status_code == 201, added.text

    async with session_maker() as session:
        async with session.begin():
            active = await session.get(CampaignRecipient, added.json()["recipient_id"])
            active.display_name = "<b>Active Fixture</b>"
            session.add(
                CampaignRecipient(
                    provider=PROVIDER_EASYWEEK,
                    campaign_run_id=run_id,
                    company_id=COMPANY_ID,
                    phone_e164="+4915100000043",
                    display_name="Excluded Fixture",
                    status="skipped",
                    excluded_reason="has_records_before_period",
                )
            )

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text
    snapshot_start = page.index('id="easyweek-active-snapshot"')
    progress_start = page.index("📈 Progress")
    assert snapshot_start < progress_start
    snapshot = page[snapshot_start:progress_start]
    assert "Активные получатели EasyWeek snapshot (1)" in snapshot
    assert "&lt;b&gt;Active Fixture&lt;/b&gt;" in snapshot
    assert "<b>Active Fixture</b>" not in snapshot
    assert PHONE in snapshot
    assert ">manual<" in snapshot
    assert "Excluded Fixture" not in snapshot
    assert f'href="/ops/campaigns/{run_id}/recipients?status=candidate"' in snapshot
    assert CUSTOMER_UUID not in page
    assert "EasyWeek send-real по §37.1 остаётся закрытым" in snapshot


# ---------------------------------------------------------------------------
# A fresh preview reports the reasons it actually recorded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_fresh_easyweek_preview_returns_its_real_exclusion_reasons(
    session_maker, configuration, http_client, monkeypatch
) -> None:
    """The blocker: the POST answered without `by_reason` at all.

    The page reads that key for EasyWeek, so an absent one rendered as
    "nothing was excluded" while the totals said otherwise.
    """
    import altegio_bot.campaigns.runner as runner

    run_id = await _preview(session_maker)
    # A reason this page has never heard of.
    await _auto_excluded(session_maker, run_id, reason="easyweek_brand_new_reason_code")
    async with session_maker() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            await runner.recompute_snapshot_counters(session, run)

    # What the POST answers for a freshly built preview: the `excluded` block
    # carries `by_reason`, which is the key the page reads for EasyWeek.
    fresh = await http_client.post(
        "/ops/campaigns/new-clients/preview",
        json={
            "provider": "easyweek",
            "company_id": COMPANY_ID,
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-31T23:59:59Z",
        },
    )
    assert fresh.status_code == 200, fresh.text
    assert "by_reason" in fresh.json()["excluded"]

    # And the run that HAS the excluded row reports it, through the same
    # aggregator the report uses.

    report = (await http_client.get(f"/ops/campaigns/runs/{run_id}/report")).json()
    assert report["excluded"]["by_reason"] == {"easyweek_brand_new_reason_code": 1}
    # The HTML detail page, not the JSON one: this is what an operator reads.
    detail = (await http_client.get(f"/ops/campaigns/{run_id}")).text
    assert "easyweek_brand_new_reason_code" in detail
    assert "Исключённых получателей нет" not in detail


@pytest.mark.asyncio
async def test_the_preview_summary_and_the_report_agree_about_exclusions(
    session_maker, configuration, http_client
) -> None:
    """One aggregator, so the two surfaces cannot drift apart."""
    from altegio_bot.campaigns.reports import excluded_reason_counts

    run_id = await _preview(session_maker)
    await _auto_excluded(session_maker, run_id, reason="has_records_before_period")
    await _auto_excluded(session_maker, run_id, reason="opted_out")

    report = (await http_client.get(f"/ops/campaigns/runs/{run_id}/report")).json()
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        direct = await excluded_reason_counts(session, run)

    assert report["excluded"]["by_reason"] == direct
    assert sum(direct.values()) == 2


@pytest.mark.asyncio
async def test_the_altegio_preview_contract_is_unchanged(http_client, configuration, monkeypatch) -> None:
    """The long-standing Altegio counters stay exactly where they were."""
    import altegio_bot.campaigns.runner as runner

    async def no_candidates(**kwargs: Any):
        return []

    monkeypatch.setattr(runner, "_find_candidates", no_candidates)

    resp = await http_client.post(
        "/ops/campaigns/new-clients/preview",
        json={
            "provider": "altegio",
            "company_id": 758285,
            "location_id": 758285,
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-31T23:59:59Z",
        },
    )

    assert resp.status_code == 200, resp.text
    excluded = resp.json()["excluded"]
    for legacy in ("opted_out", "no_phone", "invalid_phone", "multiple_records_in_period"):
        assert legacy in excluded


# ---------------------------------------------------------------------------
# The inline editor table, per provider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_inline_table_has_an_easyweek_branch_with_basis_columns(http_client, configuration) -> None:
    page = (await http_client.get("/ops/campaigns/new-clients")).text

    table_fn = page[page.find("function renderRecipientsTable") :][:3000]
    # Two branches, chosen by the provider of the RUN being drawn rather than by
    # the dropdown — which may have moved since the answer was requested.
    assert 'const easyweek = (provider || schemaProvider(PREVIEW_CONTEXT)) === "easyweek";' in table_fn
    assert "isEasyWeek()" not in table_fn
    # The EasyWeek columns.
    for column in ("Основание", "Было исключено автоматически", "EasyWeek customer"):
        assert column in table_fn
    # The three bases, with short labels.
    for basis in ("earned_first_visit", "operator_manual_selection", "owner_test_account"):
        assert basis in table_fn
    for label in (">auto<", ">manual<", ">test<"):
        assert label in table_fn
    # Altegio keeps its own columns.
    for column in ("Ресничных", "Подтверждённых лаш", "До периода (CRM)"):
        assert column in table_fn
    # And no customer UUID is ever rendered — presence only.
    assert "easyweek_customer_recorded" in table_fn
    assert "easyweek_customer_uuid" not in table_fn


@pytest.mark.asyncio
async def test_the_recipients_json_carries_everything_the_table_needs(
    session_maker, configuration, http_client
) -> None:
    """Behaviour, not markup: the three bases and an override, as served."""
    run_id = await _preview(session_maker, keep_recipient=True)

    earned = (await http_client.get(f"/ops/campaigns/runs/{run_id}/recipients")).json()

    [earned_row] = earned["items"]
    assert earned_row["recipient_basis"] == RECIPIENT_BASIS_EARNED
    assert earned_row["auto_excluded_reason"] is None
    assert earned_row["easyweek_customer_recorded"] is False


@pytest.mark.asyncio
async def test_the_recipients_json_shows_a_manual_override(session_maker, configuration, http_client) -> None:
    """The override, as the table receives it: manual basis plus what it overrode."""
    manual_run = await _preview(session_maker)
    overridden = await _auto_excluded(session_maker, manual_run, reason="has_records_before_period")
    await _add(session_maker, manual_run)

    manual = (await http_client.get(f"/ops/campaigns/runs/{manual_run}/recipients")).json()

    [manual_row] = [row for row in manual["items"] if row["id"] == overridden]
    assert manual_row["recipient_basis"] == RECIPIENT_BASIS_MANUAL
    assert manual_row["auto_excluded_reason"] == "has_records_before_period"
    assert manual_row["easyweek_customer_recorded"] is True
    # Presence only, in the payload as on the screen.
    assert CUSTOMER_UUID not in repr(manual)


# ---------------------------------------------------------------------------
# The template surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_easyweek_template_block_never_shows_the_altegio_newsletter(http_client, configuration) -> None:
    page = (await http_client.get("/ops/campaigns/new-clients")).text

    ew_block = page[page.find("Шаблон EasyWeek") : page.find("БЛОК ШАБЛОНА: ALTEGIO")]
    assert "new_client_voucher" in ew_block
    assert "<code>easyweek</code>" in ew_block
    # The approved Meta name and the language are filled in only after THIS
    # branch's row has been proven — Karlsruhe's approval is not a default for
    # Durlach or Rastatt, so the markup asserts neither.
    assert 'id="ew-template-name"' in ew_block
    assert 'id="ew-template-language"' in ew_block
    assert "kitilash_ka_new_client_voucher_v1" not in ew_block
    for altegio_only in ("newsletter_new_clients_monthly", "newsletter_new_clients_followup"):
        assert altegio_only not in ew_block
    # And it says what §37.1 does not open.
    assert "закрыт" in ew_block
    # The Altegio block still exists, for Altegio.
    assert "БЛОК ШАБЛОНА: ALTEGIO" in page
    assert "altegio-only" in page


@pytest.mark.asyncio
async def test_the_template_endpoint_never_answers_easyweek_with_an_altegio_row(
    session_maker, configuration, http_client
) -> None:
    """No cross-provider fallback: a missing EasyWeek row is missing."""
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            session.add(
                MessageTemplate(
                    provider="altegio",
                    company_id=COMPANY_ID,
                    code="new_client_voucher",
                    language="de",
                    body="Altegio body that must never be served to EasyWeek",
                    meta_template_name="kitilash_ka_new_client_voucher_v1",
                    is_active=True,
                )
            )

    resp = await http_client.get(
        "/ops/campaigns/new-clients/template-text",
        params={
            "provider": "easyweek",
            "template_name": "kitilash_ka_new_client_voucher_v1",
            "company_id": COMPANY_ID,
        },
    )

    assert resp.status_code == 404
    assert "Altegio body" not in resp.text


# ---------------------------------------------------------------------------
# The outstanding-cards race, deterministically
# ---------------------------------------------------------------------------


def _js(page: str, name: str) -> str:
    start = page.find(f"async function {name}")
    assert start != -1, name
    return page[start : start + 6000]


@pytest.mark.asyncio
async def test_a_late_answer_for_another_branch_cannot_repaint_or_be_deleted(http_client, configuration) -> None:
    """A → B → B answers → A answers. A must not paint, and Delete must refuse.

    Checked on the shipped code rather than a re-implementation: every guard the
    ordering depends on has to be present, and the load must take a NEW token
    each time rather than reusing one per provider.
    """
    page = (await http_client.get("/ops/campaigns/new-clients")).text
    load_fn = _js(page, "loadOutstandingCards")
    delete_fn = _js(page, "deleteOutstandingCards")

    # A new token per load — not one per provider change.
    assert "const generation = ++OUTSTANDING_GENERATION;" in load_fn
    # The answer is discarded unless it is the newest AND the branch still matches.
    assert "generation !== OUTSTANDING_GENERATION" in load_fn
    assert "currentCompany !== scope.companyId" in load_fn
    # The painted table records what it is showing.
    assert "OUTSTANDING_SCOPE = scope;" in load_fn
    # Delete refuses when the screen and the loaded scope disagree.
    assert "OUTSTANDING_SCOPE.companyId !== String(companyId)" in delete_fn
    assert 'OUTSTANDING_SCOPE.provider !== "altegio"' in delete_fn
    assert "Загрузите список заново" in delete_fn
    # Switching branch or provider drops both.
    assert page.count("OUTSTANDING_SCOPE = null;") >= 3


def test_the_outstanding_card_race_rules_hold_when_answers_land_out_of_order() -> None:
    """The ordering itself, executed rather than described.

    A faithful transcription of the guards above: load A, load B, let B answer,
    then let A answer late. A must neither paint nor leave a scope Delete could
    act on.
    """
    state = {"generation": 0, "scope": None, "painted": None, "selected": "A"}

    def begin_load(company: str) -> dict[str, object]:
        state["generation"] += 1
        state["scope"] = None
        return {"generation": state["generation"], "company": company}

    def finish_load(request: dict[str, object]) -> None:
        if request["generation"] != state["generation"]:
            return
        if request["company"] != state["selected"]:
            return
        state["painted"] = request["company"]
        state["scope"] = {"provider": "altegio", "company": request["company"]}

    def delete_allowed() -> bool:
        scope = state["scope"]
        return bool(scope and scope["provider"] == "altegio" and scope["company"] == state["selected"])

    request_a = begin_load("A")
    state["selected"] = "B"
    request_b = begin_load("B")

    finish_load(request_b)
    assert state["painted"] == "B"
    assert delete_allowed() is True

    # A answers late, for a branch nobody is looking at any more.
    finish_load(request_a)

    assert state["painted"] == "B", "a stale answer repainted the screen"
    assert state["scope"] == {"provider": "altegio", "company": "B"}
    assert delete_allowed() is True

    # And if the operator moves again without reloading, Delete stops.
    state["selected"] = "A"
    assert delete_allowed() is False


@pytest.mark.asyncio
async def test_the_easyweek_detail_page_marks_altegio_only_sections_closed(
    session_maker, configuration, http_client
) -> None:
    """Zeros under Delivery and Loyalty read as a working feature. Say it plainly."""
    run_id = await _preview(session_maker, keep_recipient=True)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "Delivery — закрыто (§37.1)" in page
    assert "Loyalty — не применяется" in page
    assert "Follow-up — закрыто (§37.1)" in page
    # The Altegio follow-up schedule fields are not rendered for EasyWeek.
    assert "Auto status (run.meta)" not in page
    assert "Follow-up due at" not in page
    # What §37.1 DOES open stays visible: the basis, the exclusions, the editor.
    assert "Excluded (EasyWeek)" in page
    assert "Add recipient" in page


@pytest.mark.asyncio
async def test_the_altegio_detail_page_keeps_all_three_sections(session_maker, configuration, http_client) -> None:
    """The Altegio page is untouched by any of this."""
    run_id, _ = await seed_recipient(session_maker, provider=PROVIDER_ALTEGIO, phone=PHONE, client_phone=PHONE)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "📨 Delivery" in page and "закрыто" not in page.split("📨 Delivery")[1][:80]
    assert "🎁 Loyalty" in page
    assert "Follow-up schedule / auto-run" in page
    assert "Auto status (run.meta)" in page
