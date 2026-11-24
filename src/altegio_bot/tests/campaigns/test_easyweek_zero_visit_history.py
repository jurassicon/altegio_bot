"""The operator's check of one zero-counter row, and the decision after it.

Three things are proven here, in the order an operator meets them.

1. The check itself: the number the delivery carried beside the number the API
   answers, the exact source booking, anything booked ahead, and the moment it
   was read. Plus every way it must refuse: an unproven identity, a reassigned
   booking, an incomplete history, an API that is down. An unread history is
   never reported as zero visits.
2. The same scenario through the shipped Ops API, with a real session cookie,
   this session's CSRF token and a same-origin request — because "the operator
   can see it" is a statement about the interface, not about a function.
3. The decision: the existing single Add turns that excluded row into
   ``operator_manual_selection``, keeps the original verdict as audit, does not
   duplicate the row, and is refused for an opted-out person and for a frozen
   preview.

Everything is synthetic, and nothing here sends, queues, issues or pays.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, text

import altegio_bot.campaigns.easyweek_segment as segment
import altegio_bot.campaigns.runner as runner
import altegio_bot.ops.voucher_mailing as voucher_mailing_module
from altegio_bot.campaigns.easyweek_customer_history import (
    API_RETRYABLE_UNCERTAINTY,
    CUSTOMER_UUID_MISMATCH,
    HISTORY_PAGINATION_INCOMPLETE,
)
from altegio_bot.campaigns.easyweek_manual_recipient import (
    ACTION_INCLUDED,
    ACTION_UNCHANGED,
    CLIENT_OPTED_OUT,
    PREVIEW_FROZEN,
    RUN_NOT_EDITABLE,
    ProvenCustomer,
    add_manual_recipient,
)
from altegio_bot.campaigns.easyweek_zero_visit import ZERO_VISITS_REPORTED, ZERO_VISITS_REPORTED_TEXT
from altegio_bot.campaigns.easyweek_zero_visit_history import (
    BOOKING_REASSIGNED,
    CUSTOMER_IDENTITY_AMBIGUOUS,
    CUSTOMER_IDENTITY_UNPROVEN,
    HISTORY_UNVERIFIED,
    IDENTITY_VIA_CAPTURED_CONTACT,
    IDENTITY_VIA_STORED_UUID,
    RECIPIENT_NOT_FOUND,
    ROW_ALREADY_DECIDED,
    ROW_NOT_ZERO_DIAGNOSTIC,
    RUN_NOT_FOUND,
    SOURCE_EVENT_AMBIGUOUS,
    SOURCE_EVENT_UNPROVEN,
    SOURCE_NOT_ESTABLISHED,
    SOURCE_STATE_ABSENT,
    SOURCE_STATE_CANCELED,
    SOURCE_STATE_COMPLETED,
    SOURCE_STATE_NOT_COMPLETED,
    TEMPORARILY_UNAVAILABLE,
    ZeroVisitHistoryReport,
    check_zero_visit_history,
)
from altegio_bot.campaigns.runner import RunParams
from altegio_bot.easyweek_client import EasyWeekNotFoundError, EasyWeekRetryableError
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.tests.easyweek_zero_visit_fixtures import (
    COMPANY_ID,
    LOCATION_UUID,
    NOW,
    OTHER_COMPANY_ID,
    OTHER_LOCATION_UUID,
    PERIOD_END,
    PERIOD_START,
    START,
    Person,
    record_raw,
    registry,
    seed_person,
    succeeded_payload,
)

CUSTOMER_UUID = uuid.UUID("c0ffee00-1111-4222-8333-444444444444")
OTHER_CUSTOMER_UUID = uuid.UUID("deadbeef-5555-4666-8777-888888888888")
HISTORY_URL = "/ops/voucher-mailings/api/recipients/zero-visit-history"


@pytest.fixture(autouse=True)
def _configuration(easyweek_zero_visit_configuration: None) -> None:
    """The synthetic branch registry, for the server and the check alike."""


# ===========================================================================
# A fake EasyWeek: three reads, each independently steerable
# ===========================================================================


def _history_row(
    *,
    booking_uuid: uuid.UUID,
    customer_uuid: uuid.UUID = CUSTOMER_UUID,
    completed: bool = True,
    canceled: bool = False,
    start_time: datetime = START,
) -> dict[str, Any]:
    return {
        "uuid": str(booking_uuid),
        "customer": {"uuid": str(customer_uuid)},
        "is_canceled": canceled,
        "is_completed": completed,
        "start_time": start_time.isoformat(),
    }


def _history_page(rows: list[dict[str, Any]], *, current: int = 1, last: int = 1, total: int | None = None):
    return {
        "data": rows,
        "meta": {
            "current_page": current,
            "last_page": last,
            "per_page": 100,
            "total": len(rows) if total is None else total,
        },
    }


def _booking_response(
    *,
    booking_uuid: uuid.UUID,
    customer_uuid: uuid.UUID = CUSTOMER_UUID,
    location_uuid: str = LOCATION_UUID,
) -> dict[str, Any]:
    return {
        "uuid": str(booking_uuid),
        "location_uuid": location_uuid,
        "customer": {"uuid": str(customer_uuid)},
        "is_canceled": False,
        "is_completed": True,
        "start_time": START.isoformat(),
        "ordered_services": [{}],
    }


class FakeEasyWeek:
    """Read-only EasyWeek. Records every call so a test can prove what was read."""

    def __init__(
        self,
        *,
        booking: object | Exception,
        customer: object | Exception | None = None,
        pages: dict[int, object | Exception] | None = None,
        customers: object | Exception | None = None,
        on_booking: Any = None,
    ) -> None:
        self.booking = booking
        self.customer = {"uuid": str(CUSTOMER_UUID)} if customer is None else customer
        self.pages: dict[int, object | Exception] = pages or {1: _history_page([])}
        # The workspace-wide phone listing, used only by the §44.2 branch that
        # proves an identity from the contact a delivery captured.
        self.customers = customers
        self.calls: list[str] = []
        # Which booking uuids were actually asked for, so a test can prove that
        # the SOURCE the preview recorded is the one read.
        self.booking_requests: list[str] = []
        self.on_booking = on_booking

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append("customers")
        if isinstance(self.customers, Exception):
            raise self.customers
        if self.customers is None:
            raise AssertionError("this test did not expect a workspace lookup")
        return self.customers  # type: ignore[return-value]

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append("booking")
        self.booking_requests.append(booking_uuid)
        if self.on_booking is not None:
            await self.on_booking()
        if isinstance(self.booking, Exception):
            raise self.booking
        return self.booking  # type: ignore[return-value]

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.calls.append("customer")
        if isinstance(self.customer, Exception):
            raise self.customer
        return self.customer  # type: ignore[return-value]

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        self.calls.append(f"history:{page}")
        value = self.pages[page]
        if isinstance(value, Exception):
            raise value
        return value  # type: ignore[return-value]


class FakeCustomerDirectory:
    """The two reads the existing single Add path uses, nothing else."""

    def __init__(self, *, phone: str, name: str, customer_uuid: uuid.UUID = CUSTOMER_UUID) -> None:
        self.phone = phone
        self.name = name
        self.customer_uuid = customer_uuid

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        row = {"uuid": str(self.customer_uuid), "phone": self.phone, "first_name": self.name}
        return {"data": [row], "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": 1}}

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        return {"uuid": str(self.customer_uuid), "phone": self.phone, "first_name": self.name}


# ===========================================================================
# Shared setup: one preview holding exactly one zero-counter row
# ===========================================================================


async def _preview_with_zero_row(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    *,
    customer_uuid: uuid.UUID | None = CUSTOMER_UUID,
    opted_out: bool = False,
    deliveries: int = 1,
) -> tuple[int, int, Person]:
    person = await seed_person(
        session_maker,
        1,
        customer_uuid=customer_uuid,
        opted_out=opted_out,
        deliveries=deliveries,
    )
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    async with session_maker() as session:
        rows = list(
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].excluded_reason == ZERO_VISITS_REPORTED
    return run.id, rows[0].id, person


async def _check(session_maker, *, run_id: int, recipient_id: int, reader: Any, now: datetime = NOW):
    async def _no_pause(_seconds: float) -> None:
        return None

    return await check_zero_visit_history(
        session_maker,
        run_id=run_id,
        recipient_id=recipient_id,
        reader=reader,
        now=now,
        registry=registry(),
        pause=_no_pause,
        pause_sec=0.0,
    )


async def _row(session_maker, recipient_id: int) -> CampaignRecipient:
    async with session_maker() as session:
        row = await session.get(CampaignRecipient, recipient_id)
        assert row is not None
        return row


# ===========================================================================
# 1. The confirmed scenario: the webhook said 0, the API says 1
# ===========================================================================


async def test_webhook_zero_and_api_one_is_shown_as_both_numbers(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_confirmed is True
    assert report.history_verified is True
    # The two numbers, side by side. The event's own counter is never rewritten.
    assert report.event_visits_total == 0
    assert report.completed_visit_count == 1
    # Two facts, not one: it is in the history, and there it is a completed
    # non-canceled visit.
    assert report.source_booking_in_history is True
    assert report.source_booking_state == SOURCE_STATE_COMPLETED
    assert report.active_future_booking is False
    assert report.checked_at == NOW.isoformat()
    assert report.pages_read == 1

    safe = report.as_safe_dict()
    assert safe["voucher_entitlement_granted"] is False
    assert safe["campaign_send_authorized"] is False
    assert safe["recipient_changed"] is False
    assert safe["mode"] == "read-only"
    # Nothing identifying in the answer at all.
    rendered = json.dumps(safe)
    assert str(CUSTOMER_UUID) not in rendered
    assert str(person.booking_uuid) not in rendered
    assert person.phone not in rendered
    assert person.name not in rendered


async def test_the_check_changes_nothing_at_all(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    before = await _row(session_maker, recipient_id)
    snapshot = (
        before.status,
        before.excluded_reason,
        before.auto_excluded_reason,
        before.recipient_basis,
        before.source_easyweek_event_id,
        before.easyweek_customer_uuid,
        before.is_opted_out,
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)
    assert report.ok is True

    after = await _row(session_maker, recipient_id)
    assert (
        after.status,
        after.excluded_reason,
        after.auto_excluded_reason,
        after.recipient_basis,
        after.source_easyweek_event_id,
        after.easyweek_customer_uuid,
        after.is_opted_out,
    ) == snapshot
    async with session_maker() as session:
        client = await session.get(Client, person.client_id)
        assert client is not None
        # No counter was written, and certainly not a 1.
        assert client.easyweek_visits_total is None
        run = await session.get(CampaignRun, run_id)
        assert run is not None
        assert run.candidates_count == 0
        assert int(await session.scalar(select(func.count()).select_from(MessageJob)) or 0) == 0
        assert int(await session.scalar(select(func.count()).select_from(OutboxMessage)) or 0) == 0


async def test_a_second_completed_visit_is_reported_as_two(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    other_booking = uuid.UUID(int=0x3300_0001, version=4)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={
            1: _history_page(
                [
                    _history_row(booking_uuid=person.booking_uuid),
                    _history_row(booking_uuid=other_booking, start_time=START + timedelta(days=20)),
                ]
            )
        },
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.completed_visit_count == 2
    assert report.source_booking_state == SOURCE_STATE_COMPLETED
    # Still excluded: more visits is a fact, not an entitlement.
    assert (await _row(session_maker, recipient_id)).excluded_reason == ZERO_VISITS_REPORTED


async def test_an_active_future_booking_is_reported(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    future_booking = uuid.UUID(int=0x3300_0002, version=4)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={
            1: _history_page(
                [
                    _history_row(booking_uuid=person.booking_uuid),
                    _history_row(
                        booking_uuid=future_booking,
                        completed=False,
                        start_time=NOW + timedelta(days=4),
                    ),
                ]
            )
        },
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.active_future_booking is True
    assert report.completed_visit_count == 1


async def test_the_source_booking_missing_from_history_is_said_so(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    unrelated = uuid.UUID(int=0x3300_0003, version=4)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=unrelated)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.source_booking_in_history is False
    assert report.source_booking_state == SOURCE_STATE_ABSENT
    assert report.completed_visit_count == 1


async def test_an_empty_but_complete_history_is_reported_as_zero_visits(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control that distinguishes a real zero from an unread history."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([], total=0)},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.history_verified is True
    assert report.completed_visit_count == 0
    assert report.source_booking_in_history is False
    assert report.source_booking_state == SOURCE_STATE_ABSENT


# ===========================================================================
# 2. Unknown is never zero
# ===========================================================================


async def test_incomplete_pagination_is_not_zero_visits(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        # Claims three rows, returns one.
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)], total=3)},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == HISTORY_UNVERIFIED
    assert report.detail_reason == HISTORY_PAGINATION_INCOMPLETE
    assert report.identity_confirmed is True
    assert report.history_verified is False
    # The three facts stay unknown. Not 0, not False.
    assert report.completed_visit_count is None
    assert report.source_booking_in_history is None
    assert report.source_booking_state is None
    assert report.active_future_booking is None
    # Nothing is wrong with the connection here, so this is not "try again".
    assert report.retryable is False
    assert report.checked_at == NOW.isoformat()


@pytest.mark.parametrize(
    ("failure", "reason", "detail", "retryable"),
    [
        pytest.param(
            EasyWeekRetryableError("429"), TEMPORARILY_UNAVAILABLE, API_RETRYABLE_UNCERTAINTY, True, id="retryable"
        ),
        pytest.param(EasyWeekNotFoundError("404"), HISTORY_UNVERIFIED, None, False, id="not-found"),
    ],
)
async def test_a_failing_history_read_is_unverified(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    reason: str,
    detail: str | None,
    retryable: bool,
) -> None:
    """A 429 is a temporary inability to check; a 404 is not, and never will be."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: failure},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == reason
    assert report.retryable is retryable
    assert report.completed_visit_count is None
    if detail is not None:
        assert report.detail_reason == detail


async def test_an_unreadable_source_booking_is_temporarily_unavailable(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=EasyWeekRetryableError("502"))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    # A 502 reading the booking is an outage, not a doubt about the person.
    assert report.reason == TEMPORARILY_UNAVAILABLE
    assert report.detail_reason == API_RETRYABLE_UNCERTAINTY
    assert report.retryable is True
    assert report.identity_confirmed is False
    assert report.completed_visit_count is None
    # The history was never asked for, so no budget was spent on it.
    assert reader.calls == ["booking"]


async def test_a_booking_answered_for_another_branch_is_unverified(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid, location_uuid=OTHER_LOCATION_UUID)
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == HISTORY_UNVERIFIED
    assert reader.calls == ["booking"]


async def test_a_customer_card_that_resolves_to_someone_else_is_not_confirmed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer={"uuid": str(OTHER_CUSTOMER_UUID)},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == CUSTOMER_IDENTITY_UNPROVEN
    assert report.detail_reason == CUSTOMER_UUID_MISMATCH
    assert report.completed_visit_count is None


# ===========================================================================
# 3. Identity: a reassigned booking and an unlinked card are refused
# ===========================================================================


async def test_a_reassigned_booking_is_not_confirmed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The current card of a mutable booking does not prove who earned it."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid, customer_uuid=OTHER_CUSTOMER_UUID),
        # A history that would look perfect if it were followed.
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == BOOKING_REASSIGNED
    assert report.completed_visit_count is None
    # The other person's history was never read.
    assert reader.calls == ["booking"]


async def test_a_card_without_a_stored_customer_uuid_is_not_confirmed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch, customer_uuid=None)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == CUSTOMER_IDENTITY_UNPROVEN
    # Nothing was read at all: there was no identity to read a history for.
    assert reader.calls == []


async def test_two_zero_counter_bookings_for_one_card_are_ambiguous(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    person = await seed_person(session_maker, 1, customer_uuid=CUSTOMER_UUID)
    second_uuid = uuid.UUID(int=0x4400_0001, version=4)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                Record(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_record_id=person.booking_id + 1,
                    easyweek_booking_uuid=second_uuid,
                    client_id=person.client_id,
                    altegio_client_id=person.customer_id,
                    starts_at=START + timedelta(days=2),
                    is_deleted=False,
                    raw=record_raw(),
                )
            )
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-succeeded",
                    body_truncated=False,
                    payload=succeeded_payload(
                        booking_uuid=second_uuid,
                        booking_id=person.booking_id + 1,
                        customer_id=person.customer_id,
                        visits_total=0,
                    ),
                    payload_hash=f"event-{uuid.uuid4()}",
                )
            )

    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    row = (await _rows_of(session_maker, run.id))[0]
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run.id, recipient_id=row.id, reader=reader)

    assert report.ok is False
    assert report.reason == SOURCE_EVENT_AMBIGUOUS
    assert reader.calls == []


async def _rows_of(session_maker, run_id: int) -> list[CampaignRecipient]:
    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(CampaignRecipient)
                    .where(CampaignRecipient.campaign_run_id == run_id)
                    .order_by(CampaignRecipient.id.asc())
                )
            )
            .scalars()
            .all()
        )


async def test_a_source_event_that_no_longer_attributes_is_not_confirmed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The booking was reassigned in OUR rows after the preview was made."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            other = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                altegio_client_id=person.customer_id + 10_000,
                phone_e164=f"{person.phone}7",
                display_name="Synthetic later",
                raw={},
                wa_opted_out=False,
            )
            session.add(other)
            await session.flush()
            record = await session.get(
                __import__("altegio_bot.models.models", fromlist=["Record"]).Record, person.record_id
            )
            assert record is not None
            record.client_id = other.id
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == SOURCE_EVENT_UNPROVEN
    assert reader.calls == []


# ===========================================================================
# 4. Only this row, in this preview
# ===========================================================================


async def test_an_unknown_run_is_refused(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id + 9999, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, RUN_NOT_FOUND)
    assert reader.calls == []


async def test_a_recipient_of_another_run_is_refused(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id + 9999, reader=reader)

    assert (report.ok, report.reason) == (False, RECIPIENT_NOT_FOUND)
    assert reader.calls == []


async def test_a_row_with_another_reason_is_refused(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            assert row is not None
            row.excluded_reason = "easyweek_campaign_opted_out"
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, ROW_NOT_ZERO_DIAGNOSTIC)
    assert reader.calls == []


# ===========================================================================
# 5. The same scenario through the shipped Ops interface
# ===========================================================================


def _install_reader(monkeypatch: pytest.MonkeyPatch, reader: Any) -> None:
    @contextlib.asynccontextmanager
    async def _client():
        yield reader

    monkeypatch.setattr(voucher_mailing_module, "EasyWeekClient", lambda *a, **k: _client())


async def test_ops_api_shows_the_webhook_zero_against_the_api_one(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(
        monkeypatch,
        FakeEasyWeek(
            booking=_booking_response(booking_uuid=person.booking_uuid),
            pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        ),
    )

    response = await ui_client.post(HISTORY_URL, json={"preview_run_id": run_id, "campaign_recipient_id": recipient_id})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["event_visits_total"] == 0
    assert body["completed_visit_count"] == 1
    assert body["source_booking_in_history"] is True
    assert body["source_booking_state"] == SOURCE_STATE_COMPLETED
    assert body["active_future_booking"] is False
    assert body["checked_at"]
    assert body["voucher_entitlement_granted"] is False
    assert response.headers["cache-control"] == "no-store"
    # Still excluded after the operator looked.
    assert (await _row(session_maker, recipient_id)).excluded_reason == ZERO_VISITS_REPORTED


async def test_ops_api_reports_an_unavailable_history_without_a_count(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(
        monkeypatch,
        FakeEasyWeek(
            booking=_booking_response(booking_uuid=person.booking_uuid),
            pages={1: EasyWeekRetryableError("429")},
        ),
    )

    response = await ui_client.post(HISTORY_URL, json={"preview_run_id": run_id, "campaign_recipient_id": recipient_id})

    assert response.status_code == 409
    body = response.json()
    assert body["ok"] is False
    assert body["reason"] == TEMPORARILY_UNAVAILABLE
    assert body["retryable"] is True
    assert body["completed_visit_count"] is None


async def test_ops_api_refuses_a_body_that_names_a_customer(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(monkeypatch, FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid)))

    response = await ui_client.post(
        HISTORY_URL,
        json={
            "preview_run_id": run_id,
            "campaign_recipient_id": recipient_id,
            "customer_uuid": str(CUSTOMER_UUID),
        },
    )

    assert response.status_code == 400
    assert response.json()["reason"] == "manual_batch_input_invalid"


async def test_ops_api_needs_the_session_and_the_csrf_token(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client, anon_client
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(monkeypatch, FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid)))
    payload = {"preview_run_id": run_id, "campaign_recipient_id": recipient_id}

    anonymous = await anon_client.post(HISTORY_URL, json=payload)
    assert anonymous.status_code in (401, 403)

    without_csrf = await ui_client.post(HISTORY_URL, json=payload, headers={"X-Ops-CSRF": ""})
    assert without_csrf.status_code == 403

    cross_origin = await ui_client.post(HISTORY_URL, json=payload, headers={"Origin": "http://evil.invalid"})
    assert cross_origin.status_code == 403


async def test_ops_recipients_page_shows_the_reason_and_the_check(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch)

    page = await ui_client.get(f"/ops/campaigns/{run_id}/recipients")

    assert page.status_code == 200
    html = page.text
    # The operator reads a sentence, not a code.
    assert ZERO_VISITS_REPORTED_TEXT in html
    assert "Проверить историю" in html
    assert f"checkZeroVisitHistory({run_id},{recipient_id})" in html
    # The page never renders the customer UUID of the row it offers a check for.
    assert str(CUSTOMER_UUID) not in html


# ===========================================================================
# 6. The decision afterwards: the existing single Add
# ===========================================================================


async def _add(session_maker, *, run_id: int, person: Person, reader: Any):
    return await add_manual_recipient(
        session_maker,
        run_id=run_id,
        phone=person.phone,
        reader=reader,
        assign_karlsruhe=True,
    )


async def test_manual_add_turns_the_excluded_row_into_a_recorded_decision(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    directory = FakeCustomerDirectory(phone=person.phone, name="Synthetic")

    outcome = await _add(session_maker, run_id=run_id, person=person, reader=directory)

    assert outcome.ok is True
    assert outcome.action == ACTION_INCLUDED
    assert outcome.recipient_id == recipient_id
    assert outcome.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert outcome.overrode_auto_reason == ZERO_VISITS_REPORTED

    row = await _row(session_maker, recipient_id)
    assert row.status == "candidate"
    assert row.excluded_reason is None
    # The original automatic verdict is kept, not erased.
    assert row.auto_excluded_reason == ZERO_VISITS_REPORTED
    assert row.recipient_basis == RECIPIENT_BASIS_MANUAL
    assert row.easyweek_customer_uuid == CUSTOMER_UUID
    # A decision never becomes a proof.
    assert (row.source_easyweek_event_id, row.source_visits_total) == (None, None)

    async with session_maker() as session:
        total = int(
            await session.scalar(
                select(func.count()).select_from(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
            )
            or 0
        )
        assert total == 1
        assert int(await session.scalar(select(func.count()).select_from(MessageJob)) or 0) == 0
        assert int(await session.scalar(select(func.count()).select_from(OutboxMessage)) or 0) == 0


async def test_adding_the_same_person_twice_does_not_duplicate_the_row(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    directory = FakeCustomerDirectory(phone=person.phone, name="Synthetic")

    first = await _add(session_maker, run_id=run_id, person=person, reader=directory)
    second = await _add(session_maker, run_id=run_id, person=person, reader=directory)

    assert first.action == ACTION_INCLUDED
    assert second.ok is True
    assert second.action == ACTION_UNCHANGED
    assert second.recipient_id == recipient_id
    async with session_maker() as session:
        total = int(
            await session.scalar(
                select(func.count()).select_from(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id)
            )
            or 0
        )
    assert total == 1
    row = await _row(session_maker, recipient_id)
    assert row.auto_excluded_reason == ZERO_VISITS_REPORTED


async def test_an_opted_out_person_is_refused(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """A STOP stands: an operator's decision does not override it."""
    person = await seed_person(session_maker, 1, customer_uuid=CUSTOMER_UUID, opted_out=True)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    directory = FakeCustomerDirectory(phone=person.phone, name="Synthetic")

    outcome = await _add(session_maker, run_id=run.id, person=person, reader=directory)

    assert outcome.ok is False
    assert outcome.reason == CLIENT_OPTED_OUT
    rows = await _rows_of(session_maker, run.id)
    assert all(row.excluded_reason is not None for row in rows)
    assert all(row.recipient_basis != RECIPIENT_BASIS_MANUAL for row in rows)


async def test_a_frozen_preview_refuses_the_decision(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    directory = FakeCustomerDirectory(phone=person.phone, name="Synthetic")
    import altegio_bot.campaigns.easyweek_manual_recipient as manual_module

    async def _frozen(_session, run_id: int) -> bool:
        return True

    monkeypatch.setattr(manual_module, "_frozen", _frozen)

    outcome = await _add(session_maker, run_id=run_id, person=person, reader=directory)

    assert outcome.ok is False
    assert outcome.reason == PREVIEW_FROZEN
    row = await _row(session_maker, recipient_id)
    assert row.excluded_reason == ZERO_VISITS_REPORTED
    assert row.recipient_basis != RECIPIENT_BASIS_MANUAL


async def test_a_preview_already_used_for_a_send_is_not_editable(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fixed composition, proven by real state rather than by a patched seam."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                CampaignRun(
                    provider=PROVIDER_EASYWEEK,
                    company_ids=[COMPANY_ID],
                    location_id=COMPANY_ID,
                    period_start=PERIOD_START,
                    period_end=PERIOD_END,
                    mode="send-real",
                    status="completed",
                    campaign_code="new_clients_monthly",
                    source_preview_run_id=run_id,
                )
            )
    directory = FakeCustomerDirectory(phone=person.phone, name="Synthetic")

    outcome = await _add(session_maker, run_id=run_id, person=person, reader=directory)

    assert outcome.ok is False
    assert outcome.reason == RUN_NOT_EDITABLE
    row = await _row(session_maker, recipient_id)
    assert row.excluded_reason == ZERO_VISITS_REPORTED
    assert row.recipient_basis != RECIPIENT_BASIS_MANUAL


async def test_the_check_is_refused_once_the_row_became_a_decision(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After the decision the row is no longer the open question it answers."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    directory = FakeCustomerDirectory(phone=person.phone, name="Synthetic")
    assert (await _add(session_maker, run_id=run_id, person=person, reader=directory)).ok

    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))
    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    # Its own refusal, distinct from "this is not a zero row": a tab that still
    # shows the button must be able to say "already decided, reload" rather than
    # claiming the person is still excluded.
    assert (report.ok, report.reason) == (False, ROW_ALREADY_DECIDED)
    assert reader.calls == []


def test_proven_customer_shape_is_unchanged() -> None:
    """The decision path's own contract is reused, not re-declared."""
    proven = ProvenCustomer(uuid=str(CUSTOMER_UUID), phone="+4915100000001", first_name="Synthetic")
    assert proven.uuid == str(CUSTOMER_UUID)


# ===========================================================================
# 7. The source is the delivery the PREVIEW recorded — review round 1, item 1
# ===========================================================================


def _customers_page(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": len(rows)},
    }


def _customer_row(*, phone: str, customer_uuid: uuid.UUID = CUSTOMER_UUID, name: str = "Synthetic") -> dict[str, Any]:
    return {"uuid": str(customer_uuid), "phone": phone, "first_name": name}


async def _add_later_zero_delivery(
    session_maker,
    person: Person,
    *,
    booking_uuid: uuid.UUID,
    booking_id: int,
    starts_at: datetime,
) -> tuple[int, int]:
    """A SECOND zero-counter booking for the same card, created afterwards."""
    async with session_maker() as session:
        async with session.begin():
            record = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                altegio_record_id=booking_id,
                easyweek_booking_uuid=booking_uuid,
                client_id=person.client_id,
                altegio_client_id=person.customer_id,
                starts_at=starts_at,
                is_deleted=False,
                raw=record_raw(),
            )
            session.add(record)
            event = EasyWeekEvent(
                status="processed",
                event_hint="booking-succeeded",
                body_truncated=False,
                payload=succeeded_payload(
                    booking_uuid=booking_uuid,
                    booking_id=booking_id,
                    customer_id=person.customer_id,
                    visits_total=0,
                ),
                payload_hash=f"event-{uuid.uuid4()}",
            )
            session.add(event)
            await session.flush()
            return event.id, record.id


async def test_a_later_delivery_cannot_become_an_earlier_rows_source(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A -> B. The row was created from A, so A is what gets re-proven.

    This is the defect the first review round named: the source used to be
    re-derived at open time by scanning the archive, so a zero-counter booking
    that arrived AFTER the preview was saved could silently become the source of
    a row that was written about a different visit — or make the row ambiguous
    and unopenable. The binding the preview recorded settles it.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    later_booking = uuid.UUID(int=0x5100_0001, version=4)
    await _add_later_zero_delivery(
        session_maker,
        person,
        booking_uuid=later_booking,
        booking_id=person.booking_id + 1,
        starts_at=START + timedelta(days=3),
    )

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )
    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    # The booking actually read is A's, never B's.
    assert reader.booking_requests == [str(person.booking_uuid)]
    assert str(later_booking) not in reader.booking_requests
    assert report.source_booking_state == SOURCE_STATE_COMPLETED


async def test_the_preview_records_which_delivery_the_row_came_from(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The binding is ids and a version — never a payload, phone or UUID."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    stored = (row.meta or {})["easyweek_zero_visit_source"]

    assert stored["version"] == 1
    assert stored["event_id"] == person.event_ids[0]
    assert stored["record_id"] == person.record_id
    assert stored["booking_uuid"] == str(person.booking_uuid)
    assert stored["booking_id"] == person.booking_id
    assert stored["customer_id"] == person.customer_id
    assert stored["distinct_sources"] == 1
    assert stored["reported_visits_total"] == 0
    # The binding is not proof, and the database constraints keep it that way.
    assert row.source_easyweek_event_id is None
    assert row.source_visits_total is None
    blob = json.dumps(row.meta or {})
    assert person.phone not in blob
    assert str(CUSTOMER_UUID) not in blob
    assert 'visits_total": 1' not in blob
    assert run_id == row.campaign_run_id


async def test_a_row_without_a_stored_binding_says_the_source_is_not_established(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A preview written before this path existed has no binding to re-prove."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            assert row is not None
            row.meta = {}
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_NOT_ESTABLISHED)
    # Nothing is read, so no provider budget is spent guessing.
    assert reader.calls == []
    assert report.run_id == run_id


@pytest.mark.parametrize(
    "corruption",
    [
        pytest.param({"version": 2}, id="unknown-version"),
        pytest.param({"event_id": 0}, id="non-positive-event"),
        pytest.param({"booking_uuid": "not-a-uuid"}, id="unparsable-uuid"),
        pytest.param({"reported_visits_total": 1}, id="counter-not-zero"),
        pytest.param({"record_id": None}, id="missing-record"),
    ],
)
async def test_an_unreadable_binding_is_never_guessed_at(
    session_maker, monkeypatch: pytest.MonkeyPatch, corruption: dict[str, Any]
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            assert row is not None
            stored = dict((row.meta or {})["easyweek_zero_visit_source"])
            stored.update(corruption)
            row.meta = {"easyweek_zero_visit_source": stored}
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_NOT_ESTABLISHED)
    assert reader.calls == []


async def test_a_deleted_source_record_is_an_unproven_source(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, person.record_id)
            assert record is not None
            record.is_deleted = True
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert reader.calls == []


async def test_a_source_record_moved_out_of_the_period_is_unproven(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, person.record_id)
            assert record is not None
            record.starts_at = PERIOD_END + timedelta(days=1)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert reader.calls == []


async def test_a_source_record_reassigned_locally_is_unproven(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The stored record now belongs to somebody else's card."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            other = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                altegio_client_id=person.customer_id + 7,
                phone_e164=f"{person.phone}7",
                display_name="Synthetic other",
                raw={},
            )
            session.add(other)
            await session.flush()
            record = await session.get(Record, person.record_id)
            assert record is not None
            record.client_id = other.id
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert reader.calls == []


async def test_a_second_preview_does_not_rewrite_the_first_ones_rows(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing is backfilled or re-bound behind an existing snapshot."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            assert row is not None
            row.meta = {}
    await _add_later_zero_delivery(
        session_maker,
        person,
        booking_uuid=uuid.UUID(int=0x5100_0002, version=4),
        booking_id=person.booking_id + 2,
        starts_at=START + timedelta(days=4),
    )

    second = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )

    assert second.id != run_id
    # The historical row is untouched: still empty, still not re-bound.
    assert (await _row(session_maker, recipient_id)).meta == {}


# ===========================================================================
# 8. Both §44.2 identity branches — review round 1, item 2
# ===========================================================================


async def _preview_without_stored_uuid(
    session_maker, monkeypatch: pytest.MonkeyPatch, **kwargs: Any
) -> tuple[int, int, Person]:
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True, **kwargs)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    async with session_maker() as session:
        rows = list(
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].excluded_reason == ZERO_VISITS_REPORTED
    return run.id, rows[0].id, person


async def test_a_captured_contact_proves_the_customer_without_a_stored_uuid(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§44.2 branch two, in full: workspace lookup, uniqueness, direct card.

    No numeric-to-UUID correspondence exists on the card, so the identity comes
    from the contact THIS delivery captured — the same number ingestion stored
    on the source client — resolved across the whole workspace to exactly one
    customer and then read back directly.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.identity_confirmed is True
    assert report.event_visits_total == 0
    assert report.completed_visit_count == 1
    assert report.source_booking_state == SOURCE_STATE_COMPLETED
    # The listing was walked, the card was read directly, and only then the
    # booking and the history.
    assert reader.calls[0] == "customers"
    assert "booking" in reader.calls
    # Nothing was written: the proven UUID is not adopted onto the card.
    async with session_maker() as session:
        client = await session.get(Client, person.client_id)
        assert client is not None
        assert client.easyweek_customer_uuid is None
    body = json.dumps(report.as_safe_dict())
    assert person.phone not in body
    assert str(CUSTOMER_UUID) not in body


async def test_a_stored_correspondence_needs_no_workspace_lookup(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Branch one spends nothing on resolving an identity it already holds."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_STORED_UUID
    assert "customers" not in reader.calls


async def test_an_ambiguous_captured_contact_shows_nobodys_history(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One number, two customers. Picking one would pick a person."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=_customers_page(
            [
                _customer_row(phone=person.phone),
                _customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID, name="Synthetic two"),
            ]
        ),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_AMBIGUOUS)
    assert report.retryable is False
    # Neither person's booking or history was touched.
    assert reader.calls == ["customers"]
    assert report.completed_visit_count is None


async def test_a_503_reading_the_customer_card_is_temporary(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """A 503 is an outage, never a doubt about who this is."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        customer=EasyWeekRetryableError("503", status_code=503),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, TEMPORARILY_UNAVAILABLE)
    assert report.retryable is True
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.completed_visit_count is None
    assert "history:1" not in reader.calls


async def test_a_503_listing_the_workspace_is_temporary(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=EasyWeekRetryableError("503", status_code=503),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, TEMPORARILY_UNAVAILABLE)
    assert report.retryable is True
    assert reader.calls == ["customers"]


async def test_a_503_on_the_stored_branch_card_read_is_temporary(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Branch one reads the card inside the history proof; a 503 there too."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=EasyWeekRetryableError("503", status_code=503),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, TEMPORARILY_UNAVAILABLE)
    assert report.retryable is True
    assert report.completed_visit_count is None


async def test_a_captured_contact_that_is_not_the_cards_number_proves_nothing(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A number that is not what ingestion stored is not evidence about this card."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(
        session_maker, monkeypatch, card_phone="+4915100099999"
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=_customers_page([_customer_row(phone=person.phone)]),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    # Refused locally: no workspace read at all.
    assert reader.calls == []


async def test_a_contact_shared_by_two_local_cards_is_ambiguous(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_client_id=person.customer_id + 11,
                    phone_e164=person.phone,
                    display_name="Synthetic twin",
                    raw={},
                )
            )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=_customers_page([_customer_row(phone=person.phone)]),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_AMBIGUOUS)
    assert reader.calls == []


async def test_a_proven_uuid_another_local_card_holds_is_ambiguous(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The number resolves to a customer a different card already owns."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_client_id=person.customer_id + 12,
                    easyweek_customer_uuid=CUSTOMER_UUID,
                    easyweek_identity_assigned_at=NOW,
                    phone_e164=f"{person.phone}8",
                    display_name="Synthetic owner",
                    raw={},
                )
            )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_AMBIGUOUS)
    assert "history:1" not in reader.calls


async def test_the_current_booking_owner_never_substitutes_the_identity(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live booking names a customer; that is not a proof of who this was.

    With no stored correspondence and no captured contact, the booking's current
    owner is the only candidate on offer — and §44.2 forbids using it.
    """
    run_id, recipient_id, person = await seed_and_preview_without_contact(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert reader.calls == []


async def seed_and_preview_without_contact(session_maker, monkeypatch: pytest.MonkeyPatch) -> tuple[int, int, Person]:
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    async with session_maker() as session:
        rows = list(
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    return run.id, rows[0].id, person


# ===========================================================================
# 9. One click costs the same whatever the archive holds — item 3
# ===========================================================================


async def _seed_archive(session_maker, *, count: int) -> None:
    """Zero-counter deliveries for other branches and other people."""
    async with session_maker() as session:
        async with session.begin():
            for index in range(count):
                session.add(
                    EasyWeekEvent(
                        status="processed",
                        event_hint="booking-succeeded",
                        body_truncated=False,
                        payload=succeeded_payload(
                            booking_uuid=uuid.UUID(int=0x6100_0000 + index, version=4),
                            booking_id=700_000 + index,
                            customer_id=600_000 + index,
                            visits_total=0,
                            location_id=OTHER_COMPANY_ID if index % 2 else COMPANY_ID,
                            location_uuid=OTHER_LOCATION_UUID if index % 2 else LOCATION_UUID,
                        ),
                        payload_hash=f"archive-{uuid.uuid4()}",
                    )
                )


async def _rows_fetched_by_one_check(session_maker, engine, *, run_id: int, recipient_id: int, person: Person) -> int:
    """How many rows the whole check pulled out of the database."""
    from sqlalchemy import event as sa_event

    counted = {"rows": 0}

    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if cursor.rowcount and cursor.rowcount > 0 and statement.lstrip().upper().startswith("SELECT"):
            counted["rows"] += cursor.rowcount

    sync_engine = engine.sync_engine
    sa_event.listen(sync_engine, "after_cursor_execute", after_cursor_execute)
    try:
        reader = FakeEasyWeek(
            booking=_booking_response(booking_uuid=person.booking_uuid),
            pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        )
        report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)
        assert report.ok is True
    finally:
        sa_event.remove(sync_engine, "after_cursor_execute", after_cursor_execute)
    return counted["rows"]


async def test_a_large_archive_does_not_grow_one_operator_click(
    session_maker, engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The check reads the run, the row, the card, one event and one record.

    Before this, opening a row scanned every processed ``booking-succeeded``
    delivery and resolved each one against the database, so an operator's click
    grew with the whole archive — including other branches it can never be
    about.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    baseline = await _rows_fetched_by_one_check(
        session_maker, engine, run_id=run_id, recipient_id=recipient_id, person=person
    )

    await _seed_archive(session_maker, count=120)
    after = await _rows_fetched_by_one_check(
        session_maker, engine, run_id=run_id, recipient_id=recipient_id, person=person
    )

    assert baseline > 0
    assert after == baseline


# ===========================================================================
# 10. The deadline covers the local half too — item 3
# ===========================================================================


async def test_a_slow_local_phase_is_bounded_by_the_same_deadline(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The timeout used to start only once the network phase did."""
    import altegio_bot.campaigns.easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    real_local = history_module._local_facts

    async def slow_local(*args: Any, **kwargs: Any):
        await asyncio.sleep(5)
        return await real_local(*args, **kwargs)

    monkeypatch.setattr(history_module, "_local_facts", slow_local)
    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.05)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, TEMPORARILY_UNAVAILABLE)
    assert report.retryable is True
    assert report.checked_at == NOW.isoformat()
    assert reader.calls == []


async def test_no_row_lock_is_held_while_the_api_is_read(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """A slow EasyWeek must not be able to block the row it is about."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    locked: dict[str, Any] = {"ok": False, "error": None}

    async def probe() -> None:
        try:
            async with session_maker() as session:
                async with session.begin():
                    await session.execute(text("SET LOCAL lock_timeout = '500ms'"))
                    await session.execute(
                        select(CampaignRecipient)
                        .where(CampaignRecipient.id == recipient_id)
                        .with_for_update(nowait=True)
                    )
            locked["ok"] = True
        except Exception as exc:  # noqa: BLE001 - the failure IS the finding
            locked["error"] = repr(exc)

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_booking=probe,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert locked["error"] is None
    assert locked["ok"] is True


# ===========================================================================
# 11. Present in the history is not the same as counted — item 6
# ===========================================================================


@pytest.mark.parametrize(
    ("completed", "canceled", "state", "counted"),
    [
        pytest.param(True, False, SOURCE_STATE_COMPLETED, 1, id="completed"),
        pytest.param(True, True, SOURCE_STATE_CANCELED, 0, id="canceled"),
        pytest.param(False, False, SOURCE_STATE_NOT_COMPLETED, 0, id="not-completed"),
    ],
)
async def test_the_source_bookings_state_is_reported_separately_from_the_count(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    completed: bool,
    canceled: bool,
    state: str,
    counted: int,
) -> None:
    """A canceled or unfinished source booking is present without being counted."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={
            1: _history_page([_history_row(booking_uuid=person.booking_uuid, completed=completed, canceled=canceled)])
        },
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.source_booking_in_history is True
    assert report.source_booking_state == state
    assert report.completed_visit_count == counted


# ===========================================================================
# 12. One operator, one check at a time — review round 1, item 4
# ===========================================================================


async def test_two_concurrent_checks_leave_only_one_running(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """The second click is refused while the first is still in flight.

    The guard used to be taken AFTER an ``await`` that read the request body, so
    two requests both passed the check and both ran — and then the first one to
    finish released the slot the second was still occupying. The barrier here is
    explicit: no sleeps, no timing assumptions.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(monkeypatch, FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid)))
    started = asyncio.Event()
    release = asyncio.Event()
    entered = {"count": 0}

    async def barrier(*args: Any, **kwargs: Any):
        entered["count"] += 1
        started.set()
        await release.wait()
        return ZeroVisitHistoryReport(False, run_id, recipient_id, reason=HISTORY_UNVERIFIED)

    monkeypatch.setattr(voucher_mailing_module, "check_zero_visit_history", barrier)
    body = {"preview_run_id": run_id, "campaign_recipient_id": recipient_id}
    first = asyncio.create_task(ui_client.post(HISTORY_URL, json=body))
    await asyncio.wait_for(started.wait(), timeout=5)

    second = await ui_client.post(HISTORY_URL, json=body)

    assert second.status_code == 429
    assert second.json()["reason"] == "manual_batch_rate_limit"
    release.set()
    assert (await asyncio.wait_for(first, timeout=5)).status_code == 409
    # Exactly one request ever reached the check.
    assert entered["count"] == 1
    # And the slot is free again afterwards.
    assert voucher_mailing_module._MANUAL_BATCH_IN_FLIGHT == set()


async def test_a_failing_check_releases_the_slot_it_took(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """An exception must not strand the operator's single slot."""
    from sqlalchemy.exc import SQLAlchemyError

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(monkeypatch, FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid)))

    async def boom(*args: Any, **kwargs: Any):
        raise SQLAlchemyError("synthetic")

    monkeypatch.setattr(voucher_mailing_module, "check_zero_visit_history", boom)
    body = {"preview_run_id": run_id, "campaign_recipient_id": recipient_id}

    failed = await ui_client.post(HISTORY_URL, json=body)

    assert failed.status_code == 409
    assert failed.json()["reason"] == "manual_batch_database_unavailable"
    assert voucher_mailing_module._MANUAL_BATCH_IN_FLIGHT == set()

    # The next click works, which is what "released" has to mean in practice.
    monkeypatch.setattr(
        voucher_mailing_module,
        "check_zero_visit_history",
        lambda *a, **k: _ok_report(run_id, recipient_id),
    )
    again = await ui_client.post(HISTORY_URL, json=body)
    assert again.status_code == 200


async def _ok_report(run_id: int, recipient_id: int) -> ZeroVisitHistoryReport:
    return ZeroVisitHistoryReport(
        True,
        run_id,
        recipient_id,
        identity_confirmed=True,
        history_verified=True,
        event_visits_total=0,
        completed_visit_count=1,
        source_booking_in_history=True,
        source_booking_state=SOURCE_STATE_COMPLETED,
        active_future_booking=False,
        checked_at=NOW.isoformat(),
    )


async def test_a_refused_overlapping_check_does_not_release_the_other_slot(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """Only the request that took the slot may give it back."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install_reader(monkeypatch, FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid)))
    started = asyncio.Event()
    release = asyncio.Event()

    async def barrier(*args: Any, **kwargs: Any):
        started.set()
        await release.wait()
        return await _ok_report(run_id, recipient_id)

    monkeypatch.setattr(voucher_mailing_module, "check_zero_visit_history", barrier)
    body = {"preview_run_id": run_id, "campaign_recipient_id": recipient_id}
    first = asyncio.create_task(ui_client.post(HISTORY_URL, json=body))
    await asyncio.wait_for(started.wait(), timeout=5)

    assert (await ui_client.post(HISTORY_URL, json=body)).status_code == 429
    # The refusal above must not have freed the slot the first request holds.
    assert (await ui_client.post(HISTORY_URL, json=body)).status_code == 429

    release.set()
    assert (await asyncio.wait_for(first, timeout=5)).status_code == 200
    assert voucher_mailing_module._MANUAL_BATCH_IN_FLIGHT == set()


# ===========================================================================
# 13. Nothing claims a proven first visit it has not got — item 5
# ===========================================================================


async def test_the_recipients_json_marks_the_zero_row_as_unproven(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """The basis alone is not a proof, so the API states the proof separately."""
    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch)

    response = await ui_client.get(f"/ops/campaigns/runs/{run_id}/recipients?limit=100")

    assert response.status_code == 200
    rows = response.json()["items"]
    row = next(item for item in rows if item["id"] == recipient_id)
    assert row["recipient_basis"] == "earned_first_visit"
    assert row["first_visit_proof_recorded"] is False
    # The reason is visible, and it is the zero-counter one.
    assert row["excluded_reason"] == ZERO_VISITS_REPORTED
    # No confirmed in-category visit is claimed: none was ever checked.
    assert row["segment"]["lash_records_in_period"] == 0
    assert row["segment"]["confirmed_lash_records_in_period"] == 0
    assert row["segment"]["confirmed_records_in_period"] == 0
    assert row["segment"]["service_titles_in_period"] == []


async def test_the_recipients_page_shows_the_reason_and_no_proof_claim(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    page = (await ui_client.get(f"/ops/campaigns/{run_id}/recipients")).text

    assert ZERO_VISITS_REPORTED_TEXT in page
    assert "первый визит не доказан" in page
    assert "первый визит доказан" not in page.replace("первый визит не доказан", "")
    assert f"checkZeroVisitHistory({run_id},{recipient_id})" in page
    assert str(CUSTOMER_UUID) not in page
    assert person.phone in page  # the operator's own list legitimately shows it


async def test_the_all_recipients_table_picks_its_label_from_the_proof(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """The "Все" mode renders the sub-label from the recorded proof, not the basis."""
    page = (await ui_client.get("/ops/campaigns/new-clients")).text

    table_fn = page[page.find("function renderRecipientsTable") :][:4000]
    assert "AUTO_PROOF_LABELS" in page
    assert "r.first_visit_proof_recorded" in table_fn
    assert '"первый визит не доказан", "первый визит доказан сегментатором"' in page
    # The basis map no longer carries the claim on its own.
    assert '"earned_first_visit": [\'<span class="badge bg-secondary">auto</span>\', ""]' in page


async def test_a_proven_candidate_still_reports_its_proof(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """The ordinary earned row is unchanged by any of this."""
    from altegio_bot.tests.easyweek_zero_visit_fixtures import STAMP

    await seed_person(session_maker, 2, visits_total=1, local_counter=1, local_stamp=STAMP)
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )

    rows = (await ui_client.get(f"/ops/campaigns/runs/{run.id}/recipients?limit=100")).json()["items"]

    assert len(rows) == 1
    assert rows[0]["first_visit_proof_recorded"] is True
    assert rows[0]["excluded_reason"] is None
    assert rows[0]["segment"]["confirmed_lash_records_in_period"] == 1


# ===========================================================================
# 14. Nothing contactable reaches a log or the row — item 2 and item 7
# ===========================================================================


async def test_the_captured_contact_never_reaches_a_log(session_maker, monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    """The branch that handles a phone number must not write it anywhere."""
    import logging

    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    with caplog.at_level(logging.DEBUG):
        report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert person.phone not in logged
    assert str(CUSTOMER_UUID) not in logged
    assert str(person.booking_uuid) not in logged


async def test_the_check_writes_nothing_to_the_card_or_the_row(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """No UUID adoption, no counter, no consent change, no row movement."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    async def snapshot() -> tuple[Any, ...]:
        async with session_maker() as session:
            client = await session.get(Client, person.client_id)
            row = await session.get(CampaignRecipient, recipient_id)
            assert client is not None and row is not None
            return (
                client.easyweek_customer_uuid,
                client.easyweek_identity_assigned_at,
                client.easyweek_visits_total,
                client.easyweek_visits_total_updated_at,
                client.wa_opted_out,
                row.status,
                row.excluded_reason,
                row.recipient_basis,
                row.auto_excluded_reason,
                row.source_easyweek_event_id,
                json.dumps(row.meta or {}, sort_keys=True),
            )

    before = await snapshot()
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert await snapshot() == before
    assert run_id > 0
