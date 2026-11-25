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
    CUSTOMER_RESPONSE_MALFORMED,
    CUSTOMER_UUID_MISMATCH,
    HISTORY_PAGINATION_INCOMPLETE,
    HISTORY_UNREADABLE,
    SOURCE_BOOKING_MALFORMED,
    read_customer_bookings,
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
from altegio_bot.campaigns.easyweek_zero_visit import (
    ZERO_VISIT_SOURCE_META_KEY,
    ZERO_VISITS_REPORTED,
    ZERO_VISITS_REPORTED_TEXT,
)
from altegio_bot.campaigns.easyweek_zero_visit_history import (
    API_NOT_CONFIGURED,
    API_REFUSED,
    API_RESPONSE_MALFORMED,
    BOOKING_REASSIGNED,
    CONTACT_EVIDENCE_MISSING,
    CONTACT_OWNER_CONFLICT,
    CUSTOMER_IDENTITY_AMBIGUOUS,
    CUSTOMER_IDENTITY_UNPROVEN,
    DEADLINE_AFTER_READ,
    DEADLINE_AFTER_READ_DETAIL,
    HISTORY_UNVERIFIED,
    IDENTITY_UUID_CONFLICT,
    IDENTITY_VIA_CAPTURED_CONTACT,
    IDENTITY_VIA_STORED_UUID,
    LOCAL_DEADLINE,
    LOCAL_DEADLINE_DETAIL,
    NUMERIC_OWNER_CONFLICT,
    RECIPIENT_NOT_FOUND,
    ROW_ALREADY_DECIDED,
    ROW_NOT_ZERO_DIAGNOSTIC,
    RUN_NOT_FOUND,
    SOURCE_EVENT_AMBIGUOUS,
    SOURCE_EVENT_UNPROVEN,
    SOURCE_NOT_ESTABLISHED,
    SOURCE_RECORD_CHANGED,
    SOURCE_STATE_ABSENT,
    SOURCE_STATE_CANCELED,
    SOURCE_STATE_COMPLETED,
    SOURCE_STATE_NOT_COMPLETED,
    TEMPORARILY_UNAVAILABLE,
    ZeroVisitHistoryReport,
    check_zero_visit_history,
)
from altegio_bot.campaigns.easyweek_zero_visit_lookup import (
    LOOKUP_CARD_MALFORMED,
    LOOKUP_CARD_UUID_MISMATCH,
    LOOKUP_PAGE_COUNT_UNEXPECTED,
    LOOKUP_PAGE_EMPTY,
    LOOKUP_PAGE_OVERFILLED,
    LOOKUP_PAGE_SIZE_UNSTABLE,
    LOOKUP_PAGE_UNDERFILLED,
    LOOKUP_ROWS_NOT_DISTINCT,
    LOOKUP_TOTAL_REACHED_EARLY,
    DiagnosticReader,
)
from altegio_bot.campaigns.runner import RunParams
from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_migration.customer_api import LOOKUP_FOUND, lookup_customer_by_phone
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
        pytest.param(EasyWeekNotFoundError("404"), API_REFUSED, HISTORY_UNREADABLE, False, id="not-found"),
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
    # A 200 that answers for another branch is a malformed answer, told apart
    # from an unreadable history and from a temporary failure.
    assert report.reason == API_RESPONSE_MALFORMED
    assert report.detail_reason == SOURCE_BOOKING_MALFORMED
    assert report.retryable is False
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
    # The card read back names a different customer than the UUID it was read
    # for. That is a conflict about identity, not a gap in what we could read,
    # and it is never offered as "try again later".
    assert report.reason == IDENTITY_UUID_CONFLICT
    assert report.detail_reason == CUSTOMER_UUID_MISMATCH
    assert report.retryable is False
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
                    booking_uuid=second_uuid,
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
                booking_uuid=booking_uuid,
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
                        booking_uuid=uuid.UUID(int=0x6100_0000 + index, version=4),
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

    # EasyWeek was never asked, so the answer must not say EasyWeek failed.
    assert (report.ok, report.reason) == (False, LOCAL_DEADLINE)
    assert report.detail_reason == LOCAL_DEADLINE_DETAIL
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


# ===========================================================================
# 15. The correspondence is the WORKSPACE's, not one branch's — round 2, item 1
# ===========================================================================
# §44.2: one customer UUID may legitimately hold cards in several proven
# branches, and the numeric-to-UUID correspondence a webhook proved on any of
# them identifies the same person everywhere. Reading only the Karlsruhe card
# treated a known person as unknown and sent the check down the path that has to
# prove an identity from a contact — where a phone that now resolves to somebody
# else was accepted as the answer.


async def _card_in_other_branch(
    session_maker,
    *,
    customer_id: int,
    customer_uuid: uuid.UUID | None,
    phone: str,
) -> int:
    """One EasyWeek card for the same person in the second configured branch."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=OTHER_COMPANY_ID,
                altegio_client_id=customer_id,
                easyweek_customer_uuid=customer_uuid,
                easyweek_identity_assigned_at=NOW if customer_uuid is not None else None,
                phone_e164=phone,
                display_name="Synthetic durlach",
                raw={},
                wa_opted_out=False,
            )
            session.add(client)
            await session.flush()
            return int(client.id)


async def test_a_correspondence_in_another_branch_refuses_a_different_live_customer(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, item 1: another branch already says N -> A; everything live says B.

    The Karlsruhe card carries no UUID, so the old code proved the identity from
    the captured contact and read B's history against A's row. The stored
    correspondence is the workspace's, so A is who this numeric customer is, and
    a live booking naming B stops the check — before a single page of anybody's
    history is read.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    await _card_in_other_branch(
        session_maker,
        customer_id=person.customer_id,
        customer_uuid=CUSTOMER_UUID,
        phone=person.phone,
    )
    reader = FakeEasyWeek(
        # The booking's CURRENT customer, and the one the number resolves to.
        booking=_booking_response(booking_uuid=person.booking_uuid, customer_uuid=OTHER_CUSTOMER_UUID),
        customer=_customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID),
        customers=_customers_page([_customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == BOOKING_REASSIGNED
    # The stored correspondence identified the person, so no lookup was needed.
    assert report.identity_source == IDENTITY_VIA_STORED_UUID
    assert report.retryable is False
    # Nobody's history was read.
    assert not any(call.startswith("history") for call in reader.calls)
    assert report.completed_visit_count is None


async def test_two_correspondences_for_one_numeric_customer_are_a_conflict(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contradictory bindings cannot be preferred, so nothing is read at all."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    await _card_in_other_branch(
        session_maker,
        customer_id=person.customer_id,
        customer_uuid=CUSTOMER_UUID,
        phone=person.phone,
    )
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID + 1,
                    altegio_client_id=person.customer_id,
                    easyweek_customer_uuid=OTHER_CUSTOMER_UUID,
                    easyweek_identity_assigned_at=NOW,
                    phone_e164=f"{person.phone}7",
                    display_name="Synthetic third",
                    raw={},
                    wa_opted_out=False,
                )
            )
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.retryable is False
    assert reader.calls == []


async def test_a_correspondence_written_during_the_lookup_refuses_the_answer(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, item 1: the webhook proves N -> A while the lookup is in flight.

    Every local fact the check read at the start was true when it read it: no
    correspondence existed anywhere, so the identity had to come from the
    captured contact. Mid-lookup an ingesting delivery proves and stores A on
    this very card, and the lookup answers B. Only re-reading the local facts —
    the Karlsruhe card included — catches it.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    class RacingEasyWeek(FakeEasyWeek):
        async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
            # The webhook bridge's write, landing while this call is in flight.
            async with session_maker() as session:
                async with session.begin():
                    client = await session.get(Client, person.client_id)
                    assert client is not None
                    client.easyweek_customer_uuid = CUSTOMER_UUID
                    client.easyweek_identity_assigned_at = NOW
            return await super().list_customers(params=params)

    reader = RacingEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid, customer_uuid=OTHER_CUSTOMER_UUID),
        customer=_customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID),
        customers=_customers_page([_customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.retryable is False
    assert not any(call.startswith("history") for call in reader.calls)
    assert report.completed_visit_count is None


async def test_one_consistent_identity_across_two_branches_is_checked(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The positive case §44.2 allows: same person, two proven branches.

    The Karlsruhe card has no UUID of its own; the other branch's card for the
    same numeric customer does, and the live booking agrees with it. That is a
    stored correspondence, so the check proceeds — and spends nothing looking
    the person up.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    await _card_in_other_branch(
        session_maker,
        customer_id=person.customer_id,
        customer_uuid=CUSTOMER_UUID,
        phone=person.phone,
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_STORED_UUID
    assert report.completed_visit_count == 1
    assert report.source_booking_state == SOURCE_STATE_COMPLETED
    assert "customers" not in reader.calls
    # Still read-only: the other branch's card is not copied onto this one.
    async with session_maker() as session:
        client = await session.get(Client, person.client_id)
        assert client is not None
        assert client.easyweek_customer_uuid is None


async def test_a_second_card_holding_the_proven_uuid_refuses_after_the_lookup(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An owner conflict appearing during the lookup stops the check too."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    class RacingEasyWeek(FakeEasyWeek):
        async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
            async with session_maker() as session:
                async with session.begin():
                    session.add(
                        Client(
                            provider=PROVIDER_EASYWEEK,
                            company_id=COMPANY_ID,
                            altegio_client_id=person.customer_id + 4242,
                            easyweek_customer_uuid=CUSTOMER_UUID,
                            easyweek_identity_assigned_at=NOW,
                            phone_e164=f"{person.phone}5",
                            display_name="Synthetic twin",
                            raw={},
                            wa_opted_out=False,
                        )
                    )
            return await super().get_customer(customer_uuid)

    reader = RacingEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_AMBIGUOUS)
    assert not any(call.startswith("history") for call in reader.calls)


# ===========================================================================
# 16. The contact may come from a sibling delivery — round 2, item 2
# ===========================================================================
# A zero-counter delivery does not always carry a contact, and §44.2 forbids
# borrowing the booking's current owner's details to fill the gap. What it does
# allow is a contact THIS workspace captured for THIS booking — a repeat of the
# same delivery, or the booking's create/update delivery. Which delivery that
# was is decided once, by the preview, and recorded apart from the zero-counter
# source; the number itself is never stored.


async def _run_preview(session_maker, monkeypatch: pytest.MonkeyPatch) -> tuple[int, int]:
    """Build the preview over whatever has been seeded, and return its one row."""
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
    return run.id, rows[0].id


async def _sibling_delivery(
    session_maker,
    person: Person,
    *,
    event_hint: str = "booking-created",
    phone: str | None = None,
    booking_uuid: uuid.UUID | None = None,
    booking_id: int | None = None,
    customer_id: int | None = None,
    status: str = "processed",
    location_id: int = COMPANY_ID,
    location_uuid: str = LOCATION_UUID,
) -> int:
    """One more processed delivery about (nominally) the same booking."""
    identity = booking_uuid or person.booking_uuid
    payload: dict[str, Any] = {
        "uid": str(identity),
        "id": person.booking_id if booking_id is None else booking_id,
        "location_id": location_id,
        "location_uuid": location_uuid,
        "customer_id": person.customer_id if customer_id is None else customer_id,
    }
    if phone is not None:
        payload["customer_phone"] = phone
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                status=status,
                event_hint=event_hint,
                booking_uuid=identity,
                body_truncated=False,
                payload=payload,
                payload_hash=f"sibling-{uuid.uuid4()}",
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def test_a_sibling_delivery_supplies_the_contact_the_zero_delivery_lacked(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, item 2: the create delivery carried the number, the succeeded one did not."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    contact_event_id = await _sibling_delivery(session_maker, person, phone=person.phone)
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    stored = (row.meta or {})[ZERO_VISIT_SOURCE_META_KEY]
    # Recorded SEPARATELY from the zero-counter source, and pointing at the
    # delivery that carried the contact rather than the one that carried the 0.
    assert stored["event_id"] == person.event_ids[0]
    assert stored["contact_event_id"] == contact_event_id
    assert stored["contact_sources"] == 1
    # The number itself is not in the snapshot, in any shape.
    assert person.phone not in json.dumps(row.meta or {})

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.completed_visit_count == 1
    # The source binding is still booking A's, not the sibling's.
    assert reader.booking_requests == [str(person.booking_uuid)]


async def test_contradictory_captured_contacts_choose_nobody(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two deliveries of one booking, two different numbers: no owner follows."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    await _sibling_delivery(session_maker, person, phone=person.phone)
    await _sibling_delivery(session_maker, person, event_hint="booking-updated", phone=f"{person.phone}9")
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    stored = (row.meta or {})[ZERO_VISIT_SOURCE_META_KEY]
    assert stored["contact_sources"] == 2
    assert "contact_event_id" not in stored

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.completed_visit_count is None
    # Not even the workspace was asked: there is no number to ask about.
    assert reader.calls == []


@pytest.mark.parametrize(
    ("kwargs", "label"),
    [
        pytest.param({"booking_uuid": uuid.UUID(int=0x7700_0001, version=4)}, "another booking", id="foreign-uuid"),
        pytest.param({"booking_id": 999_111}, "another numeric booking id", id="foreign-booking-id"),
        pytest.param({"customer_id": 777_222}, "another customer", id="foreign-customer"),
        pytest.param(
            {"location_id": OTHER_COMPANY_ID, "location_uuid": OTHER_LOCATION_UUID},
            "another branch",
            id="foreign-branch",
        ),
        pytest.param({"status": "captured"}, "an unprocessed delivery", id="unprocessed"),
    ],
)
async def test_a_contact_that_is_not_this_bookings_is_not_evidence(
    session_maker, monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, Any], label: str
) -> None:
    """Every identity field must line up before a captured number is used."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    await _sibling_delivery(session_maker, person, phone=person.phone, **kwargs)
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    stored = (row.meta or {})[ZERO_VISIT_SOURCE_META_KEY]
    assert stored["contact_sources"] == 0, label
    assert "contact_event_id" not in stored

    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))
    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert reader.calls == []


async def test_a_delivery_that_arrives_after_the_preview_supplies_nothing(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The binding is decided when the row is written, not when it is opened."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)
    # Only now does a delivery carrying a contact arrive.
    await _sibling_delivery(session_maker, person, event_hint="booking-updated", phone=person.phone)

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert reader.calls == []


@pytest.mark.parametrize("created_first", [True, False], ids=["create-first", "succeeded-first"])
async def test_repeats_of_one_number_answer_the_same_whatever_arrived_first(
    session_maker, monkeypatch: pytest.MonkeyPatch, created_first: bool
) -> None:
    """Delivery order changes which row carries the contact, never the answer."""
    if created_first:
        person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
        await _sibling_delivery(session_maker, person, phone=person.phone)
        await _sibling_delivery(session_maker, person, event_hint="booking-updated", phone=person.phone)
    else:
        person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True)
        await _sibling_delivery(session_maker, person, phone=person.phone)
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    assert (row.meta or {})[ZERO_VISIT_SOURCE_META_KEY]["contact_sources"] == 1

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.completed_visit_count == 1


async def test_a_contact_from_a_sibling_keeps_the_click_bounded(
    session_maker, engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reading the contact delivery is one more read by primary key, not a scan."""
    from sqlalchemy import event as sa_event

    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    await _sibling_delivery(session_maker, person, phone=person.phone)
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    async def one_click() -> int:
        counted = {"rows": 0}

        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            if cursor.rowcount and cursor.rowcount > 0 and statement.lstrip().upper().startswith("SELECT"):
                counted["rows"] += cursor.rowcount

        sa_event.listen(engine.sync_engine, "after_cursor_execute", after_cursor_execute)
        try:
            reader = FakeEasyWeek(
                booking=_booking_response(booking_uuid=person.booking_uuid),
                customer=_customer_row(phone=person.phone),
                customers=_customers_page([_customer_row(phone=person.phone)]),
                pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
            )
            report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)
            assert report.ok is True
        finally:
            sa_event.remove(engine.sync_engine, "after_cursor_execute", after_cursor_execute)
        return counted["rows"]

    baseline = await one_click()
    # Forty more deliveries that carry this very number but belong to other
    # bookings. None of them is this row's contact and none may be read.
    for index in range(40):
        await _sibling_delivery(
            session_maker,
            person,
            event_hint="booking-updated",
            phone=person.phone,
            booking_uuid=uuid.UUID(int=0x7900_0000 + index, version=4),
            booking_id=880_000 + index,
        )

    assert baseline > 0
    assert await one_click() == baseline


# ===========================================================================
# 17. The lookup must prove it saw the whole listing — round 2, item 3
# ===========================================================================
# The shared migration lookup treats one UUID listed twice as one customer, by
# design and on production evidence. That is an allowance about ROWS; the
# diagnostic needs a guarantee about COVERAGE, and the two came apart in the
# reported case: `total: 2`, the same UUID on both pages, and `per_page`
# changing between the responses. Two rows seen, two promised — and yet, if the
# window really moved, one promised row was never shown, and the row never shown
# may be a second person on that number.


def _page(rows: list[dict[str, Any]], *, page: int, last: int, total: int, per_page: int = 100) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": page, "last_page": last, "per_page": per_page, "total": total},
    }


class PagedDirectory(FakeEasyWeek):
    """A workspace listing served page by page, as the API would serve it."""

    def __init__(self, *, listing: dict[int, dict[str, Any]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.listing = listing

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(f"customers:{params.get('page')}")
        return self.listing[int(params["page"])]


async def test_a_repeated_uuid_inside_a_declared_total_is_indeterminate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, item 3: total=2, one UUID on both pages, per_page changing."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={
            1: _page([row], page=1, last=2, total=2, per_page=1),
            2: _page([row], page=2, last=2, total=2, per_page=2),
        },
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == CUSTOMER_IDENTITY_UNPROVEN
    # Our own strictness, named as such — never reported as an EasyWeek outage.
    assert report.detail_reason == LOOKUP_PAGE_SIZE_UNSTABLE
    assert report.retryable is False
    assert report.completed_visit_count is None
    # No card read, no booking read, no history read.
    assert reader.calls == ["customers:1", "customers:2"]


async def test_a_repeated_uuid_at_a_stable_page_size_is_also_indeterminate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row spent twice on one customer leaves the declared total unaccounted."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={1: _page([row, dict(row)], page=1, last=1, total=2, per_page=100)},
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.detail_reason == LOOKUP_ROWS_NOT_DISTINCT
    assert reader.calls == ["customers:1"]


async def test_a_short_page_before_the_last_one_is_indeterminate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A window that shrank mid-walk never handed over what fell between."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={
            1: _page([row], page=1, last=2, total=3, per_page=2),
            2: _page(
                [_customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID)],
                page=2,
                last=2,
                total=3,
                per_page=2,
            ),
        },
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.detail_reason == LOOKUP_PAGE_UNDERFILLED
    assert reader.calls == ["customers:1"]


async def test_an_ordinary_complete_listing_still_proves_the_customer(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stricter contract must not refuse a well-formed multi-page answer."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    mine = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={1: _page([mine], page=1, last=1, total=1, per_page=100)},
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=mine,
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.completed_visit_count == 1


async def test_two_different_cards_on_one_number_show_nobodys_history(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A shared number is an ambiguity, told apart from a coverage problem."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = PagedDirectory(
        listing={
            1: _page(
                [
                    _customer_row(phone=person.phone),
                    _customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID),
                ],
                page=1,
                last=1,
                total=2,
            )
        },
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_AMBIGUOUS)
    assert report.completed_visit_count is None
    assert not any(call.startswith("history") for call in reader.calls)


async def test_the_migration_lookup_keeps_its_duplicate_uuid_contract() -> None:
    """The shared contract is untouched: one UUID listed twice is one customer.

    The diagnostic is stricter; the migration read and the manual Add are not,
    and must not become so — their allowance rests on production evidence and
    on a different question.
    """
    row = {"uuid": str(CUSTOMER_UUID), "phone": "+4915100012345", "first_name": "Synthetic"}

    class Directory:
        async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
            return _page([row, dict(row)], page=1, last=1, total=2)

        async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
            return row

    lookup = await lookup_customer_by_phone(Directory(), "+4915100012345")

    assert lookup.outcome == LOOKUP_FOUND
    assert lookup.match_count == 1
    assert lookup.uuid == str(CUSTOMER_UUID)


async def test_manual_add_still_accepts_the_duplicate_uuid_listing(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The existing single Add behaves exactly as before on the same listing."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    row = {"uuid": str(CUSTOMER_UUID), "phone": person.phone, "first_name": "Synthetic"}

    class Directory:
        async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
            return _page([row, dict(row)], page=1, last=1, total=2)

        async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
            return row

    outcome = await add_manual_recipient(session_maker, run_id=run_id, phone=person.phone, reader=Directory())

    assert outcome.ok is True
    assert outcome.recipient_id == recipient_id


# ===========================================================================
# 18. Four failure families, never collapsed — round 2, item 5
# ===========================================================================


@pytest.mark.parametrize(
    ("failure", "reason", "retryable"),
    [
        pytest.param(EasyWeekRetryableError("429"), TEMPORARILY_UNAVAILABLE, True, id="429"),
        pytest.param(EasyWeekRetryableError("503"), TEMPORARILY_UNAVAILABLE, True, id="503"),
        pytest.param(EasyWeekAuthError("403"), API_REFUSED, False, id="403"),
        pytest.param(EasyWeekNotFoundError("404"), API_REFUSED, False, id="404"),
        pytest.param(EasyWeekPermanentError("422"), API_REFUSED, False, id="422"),
        pytest.param(EasyWeekProtocolError("shape"), API_RESPONSE_MALFORMED, False, id="protocol"),
    ],
)
async def test_a_typed_failure_listing_the_workspace_keeps_its_family(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    reason: str,
    retryable: bool,
) -> None:
    """Round 2, item 5: temporary, refused and malformed stay three answers.

    All four families end the check the same way — the number of visits stays
    unknown — and say three different things about what to do next. Collapsing
    them told an operator to retry a 403 forever, or to treat a 429 as a reason
    to doubt whose row they were looking at.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=failure,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    assert report.reason == reason
    assert report.retryable is retryable
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    # Unknown, in every family. An unread workspace is never zero visits.
    assert report.completed_visit_count is None
    assert report.history_verified is False
    assert reader.calls == ["customers"]


@pytest.mark.parametrize(
    ("failure", "reason", "retryable"),
    [
        pytest.param(EasyWeekRetryableError("503"), TEMPORARILY_UNAVAILABLE, True, id="503"),
        pytest.param(EasyWeekAuthError("401"), API_REFUSED, False, id="401"),
        pytest.param(EasyWeekProtocolError("shape"), API_RESPONSE_MALFORMED, False, id="protocol"),
    ],
)
async def test_a_typed_failure_reading_the_booking_keeps_its_family(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    reason: str,
    retryable: bool,
) -> None:
    """The same three families on the source-booking read."""
    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=failure)

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason, report.retryable) == (False, reason, retryable)
    assert report.completed_visit_count is None


# ===========================================================================
# 19. `total_records_in_period` means what it says — round 2, item 4
# ===========================================================================
# The field is the number of this client's records starting inside the
# campaign's half-open period. The diagnostic row used to hard-code 1, which was
# a count of neither records nor deliveries: a person with two different proven
# bookings in the period was reported as having one, and the figure would have
# become a count of zero-counter events had it followed the deliveries instead.
# It is now counted in SQL over distinct records.


async def _period_count(ui_client, *, run_id: int, recipient_id: int) -> int:
    """The number the shipped API actually serves for that row."""
    response = await ui_client.get(f"/ops/campaigns/runs/{run_id}/recipients?limit=100")
    assert response.status_code == 200
    row = next(item for item in response.json()["items"] if item["id"] == recipient_id)
    return int(row["segment"]["total_records_in_period"])


async def test_two_different_bookings_in_the_period_are_not_counted_as_one(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """Round 2, item 4: two proven records in the period is 2, never 1."""
    person = await seed_person(session_maker, 1, customer_uuid=CUSTOMER_UUID)
    await _add_later_zero_delivery(
        session_maker,
        person,
        booking_uuid=uuid.UUID(int=0x7A00_0001, version=4),
        booking_id=person.booking_id + 11,
        starts_at=START + timedelta(days=3),
    )
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    assert row.total_records_in_period == 2
    assert await _period_count(ui_client, run_id=run_id, recipient_id=recipient_id) == 2
    # Still no claim of a confirmed in-category visit, and still no proof.
    assert row.confirmed_records_in_period == 0
    assert row.source_easyweek_event_id is None


async def test_two_deliveries_of_one_booking_are_counted_once(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """A replayed webhook is one more delivery and not one more record."""
    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch, deliveries=2)

    row = await _row(session_maker, recipient_id)
    assert row.total_records_in_period == 1
    assert await _period_count(ui_client, run_id=run_id, recipient_id=recipient_id) == 1


async def test_a_record_outside_the_period_is_not_counted(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """The half-open period is the same one the rest of this path uses."""
    person = await seed_person(session_maker, 1, customer_uuid=CUSTOMER_UUID)
    await _add_later_zero_delivery(
        session_maker,
        person,
        booking_uuid=uuid.UUID(int=0x7A00_0002, version=4),
        booking_id=person.booking_id + 12,
        # Exactly the period's exclusive end, plus one outside it entirely.
        starts_at=PERIOD_END,
    )
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)

    row = await _row(session_maker, recipient_id)
    assert row.total_records_in_period == 1
    assert await _period_count(ui_client, run_id=run_id, recipient_id=recipient_id) == 1


async def test_a_proven_candidate_keeps_its_own_period_count(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The earned path is untouched: one proven source booking, counted as one."""
    from altegio_bot.tests.easyweek_zero_visit_fixtures import STAMP

    await seed_person(session_maker, 3, visits_total=1, local_counter=1, local_stamp=STAMP)
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
    assert rows[0].excluded_reason is None
    assert rows[0].total_records_in_period == 1
    assert rows[0].confirmed_records_in_period == 1


# ===========================================================================
# 20. The correspondence is checked in BOTH directions — round 3, item 1
# ===========================================================================
# One numeric customer with two UUIDs was already refused. The other direction
# was not: one UUID carrying two different numeric customer ids is the same
# contradiction read the other way round, and the webhook bridge has always
# refused it. The diagnostic now asks the same question, workspace-wide, on both
# §44.2 branches — and, for the branch that proves an identity from a contact,
# also asks whether a local card claims that number for somebody else.


async def _card_in_branch(
    session_maker,
    *,
    company_id: int,
    customer_id: int | None,
    customer_uuid: uuid.UUID | None,
    phone: str,
) -> int:
    """One more EasyWeek card, exactly as an import or a webhook would leave it."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                altegio_client_id=customer_id,
                easyweek_customer_uuid=customer_uuid,
                easyweek_identity_assigned_at=NOW if customer_uuid is not None else None,
                phone_e164=phone,
                display_name="Synthetic other card",
                raw={},
                wa_opted_out=False,
            )
            session.add(client)
            await session.flush()
            return int(client.id)


async def test_one_uuid_carrying_two_numeric_customers_is_a_conflict(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 1: the stored branch, read the other way round.

    The Karlsruhe card says ``N -> U``. Another branch's card says ``M -> U``
    for a different numeric customer. One UUID may hold cards in several proven
    branches, but not two numeric identities: the webhook bridge refuses exactly
    this, and reading one of the two people's history against the other's row is
    the mistake this path exists to prevent.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    await _card_in_branch(
        session_maker,
        company_id=OTHER_COMPANY_ID,
        customer_id=person.customer_id + 501,
        customer_uuid=CUSTOMER_UUID,
        phone=f"{person.phone}3",
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.detail_reason == NUMERIC_OWNER_CONFLICT
    assert report.retryable is False
    assert report.completed_visit_count is None
    # Refused before anything was asked of EasyWeek.
    assert reader.calls == []


async def test_a_uuid_owned_elsewhere_refuses_the_contact_branch_too(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same question on the branch that proves the identity from a contact."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    await _card_in_branch(
        session_maker,
        company_id=OTHER_COMPANY_ID,
        customer_id=person.customer_id + 502,
        customer_uuid=CUSTOMER_UUID,
        phone=f"{person.phone}4",
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.identity_source == IDENTITY_VIA_CAPTURED_CONTACT
    assert report.retryable is False
    assert not any(call.startswith("history") for call in reader.calls)
    assert report.completed_visit_count is None


async def test_a_card_claiming_this_number_for_another_customer_refuses(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 1: a local card proves the number's owner is in doubt.

    Another branch holds a card on this very number for a DIFFERENT numeric
    customer. The contact the delivery captured therefore does not identify one
    person, and a workspace lookup on it answers a question nobody can act on.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    await _card_in_branch(
        session_maker,
        company_id=OTHER_COMPANY_ID,
        customer_id=person.customer_id + 503,
        customer_uuid=None,
        phone=person.phone,
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.detail_reason == CONTACT_OWNER_CONFLICT
    # Refused locally: the number is never sent anywhere.
    assert reader.calls == []


async def test_the_same_numeric_and_uuid_in_two_branches_stays_allowed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§44.2's positive case: one person, one UUID, one numeric id, two branches."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    await _card_in_branch(
        session_maker,
        company_id=OTHER_COMPANY_ID,
        customer_id=person.customer_id,
        customer_uuid=CUSTOMER_UUID,
        phone=person.phone,
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_STORED_UUID
    assert report.completed_visit_count == 1
    assert "customers" not in reader.calls


async def test_a_numeric_only_card_in_another_branch_is_not_a_conflict(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A card with no UUID yet binds nothing, so it contradicts nothing."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    await _card_in_branch(
        session_maker,
        company_id=OTHER_COMPANY_ID,
        customer_id=person.customer_id,
        customer_uuid=None,
        phone=person.phone,
    )
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.completed_visit_count == 1


async def test_a_recipient_of_another_branch_is_not_this_preview_s_row(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The row and its source record must belong to the preview's own branch."""
    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch)
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            assert row is not None
            row.company_id = OTHER_COMPANY_ID
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=uuid.uuid4()))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, RECIPIENT_NOT_FOUND)
    assert reader.calls == []


# ===========================================================================
# 21. The owner can still change after the lookup — round 3, item 2
# ===========================================================================
# The identity recheck closed the race against the lookup. It did not close the
# window after it: the source booking read is another network call, and a
# webhook storing a different UUID for this client while it is in flight leaves
# the check about to read one person's history for another's row. And a conflict
# that appears during the history read itself must not be served as a
# confirmation that was already known to be out of date.


class _WritingDuringHistory(FakeEasyWeek):
    """A reader that lets a webhook's write land during one chosen read."""

    def __init__(self, *, on_history: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.on_history = on_history

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        await self.on_history()
        return await super().list_customer_bookings(customer_uuid, page=page, per_page=per_page)


async def _store_uuid_on_card(session_maker, *, client_id: int, customer_uuid: uuid.UUID) -> None:
    """Exactly what the webhook bridge's transaction leaves behind."""
    async with session_maker() as session:
        async with session.begin():
            client = await session.get(Client, client_id)
            assert client is not None
            client.easyweek_customer_uuid = customer_uuid
            client.easyweek_identity_assigned_at = NOW


async def test_a_conflict_written_during_the_booking_read_stops_the_history(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 2: the lookup proved U, then the webhook stored another UUID.

    The barrier is the booking read itself: the write lands while that call is in
    flight, so every local fact the lookup was checked against is already stale
    by the time the history would be asked for. The history must not be asked
    for at all.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    async def race() -> None:
        await _store_uuid_on_card(session_maker, client_id=person.client_id or 0, customer_uuid=OTHER_CUSTOMER_UUID)

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_booking=race,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.retryable is False
    assert report.completed_visit_count is None
    # The booking was read; the history was not.
    assert "booking" in reader.calls
    assert not any(call.startswith("history") for call in reader.calls)


async def test_a_conflict_written_during_the_booking_read_stops_the_stored_branch(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same window on the branch that needed no lookup at all."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _card_in_branch(
            session_maker,
            company_id=OTHER_COMPANY_ID,
            customer_id=person.customer_id,
            customer_uuid=OTHER_CUSTOMER_UUID,
            phone=f"{person.phone}6",
        )

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_booking=race,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert not any(call.startswith("history") for call in reader.calls)


async def test_a_conflict_during_the_history_read_is_not_served_as_a_confirmation(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 2: an answer already known to be out of date is not shown.

    The history read is the last network call, and a webhook can prove a
    different UUID for this client while its pages are being walked. The pages
    were read for a customer this card no longer belongs to, so the count must
    not be reported — neither as a confirmation nor as a number.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _card_in_branch(
            session_maker,
            company_id=OTHER_COMPANY_ID,
            customer_id=person.customer_id,
            customer_uuid=OTHER_CUSTOMER_UUID,
            phone=f"{person.phone}7",
        )

    reader = _WritingDuringHistory(
        on_history=race,
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.completed_visit_count is None
    assert report.history_verified is False
    # It did read the history — and then refused to present it.
    assert "history:1" in reader.calls


async def test_an_undisturbed_check_rechecks_and_still_confirms(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The extra rechecks must not cost a correct answer."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.completed_visit_count) == (True, 1)
    assert report.history_verified is True


async def test_the_rechecks_hold_no_lock_through_the_api(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Short reads, no transaction and no row lock across a network call."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    probe: dict[str, Any] = {"ok": False, "error": None}

    async def check_lock() -> None:
        try:
            async with session_maker() as session:
                async with session.begin():
                    await session.execute(text("SET LOCAL lock_timeout = '500ms'"))
                    await session.execute(
                        select(Client).where(Client.id == person.client_id).with_for_update(nowait=True)
                    )
            probe["ok"] = True
        except Exception as exc:  # noqa: BLE001 - the failure IS the finding
            probe["error"] = repr(exc)

    reader = _WritingDuringHistory(
        on_history=check_lock,
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert (probe["ok"], probe["error"]) == (True, None)


# ===========================================================================
# 22. The listing must add up, page by page — round 3, item 3
# ===========================================================================
# The coverage contract checked stability and distinctness. It did not check
# that the declared page size, page count and total describe a listing that can
# exist: an empty first page followed by one card at ``total: 1`` was accepted,
# and so was an extra empty page after the rows had already added up. Both are
# answers whose completeness cannot be proven, and the card never shown may be a
# second person on that number.


def _loose_page(
    rows: list[dict[str, Any]],
    *,
    page: int,
    last: int,
    total: int,
    per_page: object = None,
) -> dict[str, Any]:
    """A page whose declared size is a string, or absent altogether."""
    meta: dict[str, Any] = {"current_page": page, "last_page": last, "total": total}
    if per_page is not None:
        meta["per_page"] = per_page
    return {"data": rows, "meta": meta}


@pytest.mark.parametrize("per_page", [pytest.param("100", id="string-size"), pytest.param(None, id="no-size")])
async def test_an_empty_first_page_before_a_single_card_is_indeterminate(
    session_maker, monkeypatch: pytest.MonkeyPatch, per_page: object
) -> None:
    """Round 3, item 3: nothing on page one, one card on page two, total 1."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={
            1: _loose_page([], page=1, last=2, total=1, per_page=per_page),
            2: _loose_page([row], page=2, last=2, total=1, per_page=per_page),
        },
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.detail_reason == LOOKUP_PAGE_EMPTY
    assert report.retryable is False
    assert report.completed_visit_count is None
    # Refused on the first page: no card read, no booking read, no history read.
    assert reader.calls == ["customers:1"]


async def test_an_extra_empty_last_page_is_indeterminate(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Round 3, item 3: one card, total 1, and a second page that cannot exist."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={
            1: _page([row], page=1, last=2, total=1, per_page=100),
            2: _page([], page=2, last=2, total=1, per_page=100),
        },
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.detail_reason == LOOKUP_PAGE_COUNT_UNEXPECTED
    assert reader.calls == ["customers:1"]


async def test_an_extra_page_after_the_total_was_reached_is_indeterminate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same shape with no declared size: the rows added up too early."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    reader = PagedDirectory(
        listing={
            1: _loose_page([row], page=1, last=2, total=1),
            2: _loose_page([], page=2, last=2, total=1),
        },
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.detail_reason == LOOKUP_TOTAL_REACHED_EARLY
    assert reader.calls == ["customers:1"]


async def test_a_page_holding_more_rows_than_its_size_is_indeterminate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two rows on a page that declares room for one describes no listing."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    row = _customer_row(phone=person.phone)
    other = _customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID)
    reader = PagedDirectory(
        listing={1: _page([row, other], page=1, last=2, total=2, per_page=1)},
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=row,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    assert report.detail_reason == LOOKUP_PAGE_OVERFILLED
    assert reader.calls == ["customers:1"]


async def test_a_correct_empty_listing_is_an_answer_not_a_refusal(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 3: nobody on that number, read completely, is a fact.

    The stricter contract must not turn a legitimately empty workspace answer
    into a coverage complaint: the listing is complete, the customer is absent,
    and the check refuses for that reason and not for ours.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = PagedDirectory(
        listing={1: _page([], page=1, last=1, total=0, per_page=100)},
        booking=_booking_response(booking_uuid=person.booking_uuid),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_UNPROVEN)
    # Not one of OUR coverage refusals: the listing was fine.
    assert report.detail_reason is None
    assert reader.calls == ["customers:1"]
    assert report.completed_visit_count is None


async def test_the_migration_lookup_still_accepts_the_empty_first_page() -> None:
    """The shared contracts keep their own answer to the same listing.

    The migration read and the manual Add resolve this listing exactly as they
    did before; the stricter coverage rule lives in the diagnostic alone.
    """
    row = {"uuid": str(CUSTOMER_UUID), "phone": "+4915100012345", "first_name": "Synthetic"}

    class Directory:
        async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
            page = int(params["page"])
            rows = [] if page == 1 else [row]
            return {
                "data": rows,
                "meta": {"current_page": page, "last_page": 2, "per_page": "100", "total": 1},
            }

        async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
            return row

    lookup = await lookup_customer_by_phone(Directory(), "+4915100012345")

    assert lookup.outcome == LOOKUP_FOUND
    assert lookup.uuid == str(CUSTOMER_UUID)


# ===========================================================================
# 23. A row written before the contact count — round 3, item 5
# ===========================================================================
# Rows created by the previous revision hold a binding without the proof that
# exactly one contact was captured for the booking. The card's number alone does
# not make up for it: it may be the one number a single delivery captured, or
# one of several the deliveries disagreed about, and the row cannot say which.
# Such a row is refused with a reason of its own — unless a stored numeric↔UUID
# correspondence identifies the person without any contact at all. Old rows are
# never rewritten and never rebuilt from a later delivery.


async def _strip_contact_proof(session_maker, recipient_id: int) -> dict[str, Any]:
    """Rewrite the row's binding into the exact shape commit c3cb4f1 stored."""
    async with session_maker() as session:
        async with session.begin():
            row = await session.get(CampaignRecipient, recipient_id)
            assert row is not None
            meta = dict(row.meta or {})
            stored = dict(meta[ZERO_VISIT_SOURCE_META_KEY])
            stored.pop("contact_sources", None)
            stored.pop("contact_event_id", None)
            meta[ZERO_VISIT_SOURCE_META_KEY] = stored
            row.meta = meta
            return stored


async def test_a_row_without_the_contact_proof_is_refused_with_its_own_reason(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 5: two contradictory contacts, and an old row that cannot tell.

    The archive holds two deliveries of this booking carrying two different
    numbers, so no contact identifies the customer. A row from before the count
    existed looks exactly like a row from a booking with one contact, so it is
    refused rather than resolved — and the lookup is never spent on it.
    """
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True)
    await _sibling_delivery(session_maker, person, phone="+4915100077777")
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)
    stored = await _strip_contact_proof(session_maker, recipient_id)
    assert "contact_sources" not in stored
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CONTACT_EVIDENCE_MISSING)
    assert report.retryable is False
    assert report.completed_visit_count is None
    assert reader.calls == []
    # The stored row is left exactly as it was found.
    row = await _row(session_maker, recipient_id)
    assert ZERO_VISIT_SOURCE_META_KEY in (row.meta or {})
    assert "contact_sources" not in (row.meta or {})[ZERO_VISIT_SOURCE_META_KEY]


async def test_an_old_row_with_a_stored_correspondence_is_still_checked(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 5: a consistent stored UUID remains a basis of its own."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    await _strip_contact_proof(session_maker, recipient_id)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.identity_source == IDENTITY_VIA_STORED_UUID
    assert report.completed_visit_count == 1
    assert "customers" not in reader.calls


async def test_an_old_row_is_not_rebuilt_from_a_later_delivery(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """A delivery that arrived after the preview supplies nothing, as before."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    run_id, recipient_id = await _run_preview(session_maker, monkeypatch)
    await _strip_contact_proof(session_maker, recipient_id)
    # Only now does a delivery carrying the number arrive.
    await _sibling_delivery(session_maker, person, phone=person.phone)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CONTACT_EVIDENCE_MISSING)
    assert reader.calls == []


# ===========================================================================
# 24. Which failure the operator is told about — round 3, item 6
# ===========================================================================
# Four more distinctions an operator has to be able to act on: our own database
# running out of time after EasyWeek had already answered is not EasyWeek
# failing; an unusable integration is not an unreachable branch; and on the
# direct card read, a body that is not a card is not the same as a card for
# somebody else.


async def test_a_database_timeout_after_the_api_answered_is_not_blamed_on_easyweek(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 6: EasyWeek answered, our own phase ran out of time."""
    import altegio_bot.campaigns.easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    real_recheck = history_module._recheck_local_identity

    async def slow_recheck(*args: Any, **kwargs: Any):
        await asyncio.sleep(5)
        return await real_recheck(*args, **kwargs)

    monkeypatch.setattr(history_module, "_recheck_local_identity", slow_recheck)
    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.2)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is False
    # Neutral: our side. Never "EasyWeek did not answer" — it did.
    assert report.reason == DEADLINE_AFTER_READ
    assert report.detail_reason == DEADLINE_AFTER_READ_DETAIL
    assert report.retryable is True
    assert report.completed_visit_count is None
    # It had already answered the lookup and the card read.
    assert "customers" in reader.calls
    assert not any(call.startswith("history") for call in reader.calls)


async def test_a_deadline_inside_an_api_call_still_says_easyweek_did_not_answer(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The temporary family is untouched where it is the right answer."""
    import altegio_bot.campaigns.easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def hang() -> None:
        await asyncio.sleep(5)

    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.2)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid), on_booking=hang)

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, TEMPORARILY_UNAVAILABLE)
    assert report.detail_reason == API_RETRYABLE_UNCERTAINTY
    assert report.retryable is True


async def test_an_unusable_integration_is_not_an_unreachable_branch(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 6: a configuration error has its own answer."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=EasyWeekConfigError("EASYWEEK_API_KEY is not configured"),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, API_NOT_CONFIGURED)
    # Not temporary: no retry can configure a workspace slug.
    assert report.retryable is False
    assert report.completed_visit_count is None


async def test_the_ops_api_says_the_integration_is_not_configured(
    session_maker, monkeypatch: pytest.MonkeyPatch, ui_client
) -> None:
    """The same distinction where the client is built, before any request."""
    run_id, recipient_id, _person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    class Unconfigured:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise EasyWeekConfigError("EASYWEEK_WORKSPACE_SLUG is not configured")

    monkeypatch.setattr(voucher_mailing_module, "EasyWeekClient", Unconfigured)
    response = await ui_client.post(
        HISTORY_URL,
        json={"preview_run_id": run_id, "campaign_recipient_id": recipient_id},
    )

    assert response.status_code == 409
    body = response.json()
    assert body["ok"] is False
    assert body["reason"] == API_NOT_CONFIGURED
    assert body.get("completed_visit_count") is None


async def test_a_card_body_that_is_not_a_card_is_a_format_problem(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 6: a 200 that is not a readable card."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer={"unexpected": "shape"},
        customers=_customers_page([_customer_row(phone=person.phone)]),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, API_RESPONSE_MALFORMED)
    assert report.detail_reason == LOOKUP_CARD_MALFORMED
    assert report.retryable is False
    assert report.completed_visit_count is None


async def test_a_card_read_answering_about_another_customer_is_a_conflict(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A well-formed card for a different UUID is an identity answer, not a format one."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID),
        customers=_customers_page([_customer_row(phone=person.phone)]),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.detail_reason == LOOKUP_CARD_UUID_MISMATCH
    assert report.retryable is False
    assert not any(call.startswith("history") for call in reader.calls)


@pytest.mark.parametrize(
    ("failure", "reason", "retryable"),
    [
        pytest.param(EasyWeekRetryableError("429"), TEMPORARILY_UNAVAILABLE, True, id="429"),
        pytest.param(EasyWeekRetryableError("503"), TEMPORARILY_UNAVAILABLE, True, id="503"),
        pytest.param(EasyWeekPermanentError("422"), API_REFUSED, False, id="422"),
    ],
)
async def test_the_three_preserved_families_still_answer_as_before(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    reason: str,
    retryable: bool,
) -> None:
    """429, 503 and a permanent 422 keep exactly the answers they had."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: failure},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason, report.retryable) == (False, reason, retryable)
    assert report.completed_visit_count is None


async def test_a_second_card_on_the_number_appearing_mid_check_stops_it(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 2: the recheck applies the rule the first read applied.

    A second local card on the proof's number, created while the booking read
    was in flight, is exactly what the first read refuses as ambiguous. The
    window must not be a way past it.
    """
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    async def race() -> None:
        await _card_in_branch(
            session_maker,
            company_id=COMPANY_ID,
            customer_id=person.customer_id + 777,
            customer_uuid=None,
            phone=person.phone,
        )

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_booking=race,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, CUSTOMER_IDENTITY_AMBIGUOUS)
    assert report.completed_visit_count is None
    assert not any(call.startswith("history") for call in reader.calls)


# ===========================================================================
# 25. The source record is re-proven too — round 4, item 1
# ===========================================================================
# The rechecks after each network call read the cards and their UUIDs. They did
# not read the RECORD the preview bound the row to, so a webhook that moved the
# source booking to another client — leaving both cards and both UUIDs exactly
# as they were — left the check reading the proven customer's history and
# showing it against a row that was no longer about that visit. The whole chain
# is re-proven now, through the project's own rule, against the exact stored
# record; no substitute record is ever looked for.


def _history_rows(count: int, *, first: int = 0) -> list[dict[str, Any]]:
    """Filler history rows with distinct booking uuids, none of them completed."""
    return [
        _history_row(booking_uuid=uuid.UUID(int=0x9000_0000 + first + index, version=4), completed=False)
        for index in range(count)
    ]


def _two_page_history(person: Person) -> dict[int, dict[str, Any]]:
    """101 bookings over two pages, the source booking among them.

    The shared reader insists every page before the last is exactly full, so a
    second page needs a first page of a hundred rows. The alternative — asserting
    page-by-page behaviour against a one-page history — would assert nothing.
    """
    page_one = [_history_row(booking_uuid=person.booking_uuid)] + _history_rows(99)
    return {
        1: _history_page(page_one, current=1, last=2, total=101),
        2: _history_page(_history_rows(1, first=500), current=2, last=2, total=101),
    }


class HookedEasyWeek(FakeEasyWeek):
    """A reader that lets a webhook's write land before one chosen read.

    ``on_customer`` fires before the direct card read, ``on_history_page``
    before the page it names, and ``customers_in_sequence`` answers successive
    card reads with different bodies — which is how the repeat read inside the
    history reader can be made to fail while the lookup's own read succeeded.
    """

    def __init__(
        self,
        *,
        on_customer: Any = None,
        on_history_page: dict[int, Any] | None = None,
        customers_in_sequence: list[Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.on_customer = on_customer
        self.on_history_page = on_history_page or {}
        self.customers_in_sequence = list(customers_in_sequence or [])

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        if self.on_customer is not None:
            await self.on_customer()
        if self.customers_in_sequence:
            self.calls.append("customer")
            value = self.customers_in_sequence.pop(0)
            if isinstance(value, Exception):
                raise value
            return value  # type: ignore[return-value]
        return await super().get_customer(customer_uuid)

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        hook = self.on_history_page.get(page)
        if hook is not None:
            await hook()
        return await super().list_customer_bookings(customer_uuid, page=page, per_page=per_page)


async def _move_record(
    session_maker,
    record_id: int | None,
    *,
    client_id: int | None = None,
    customer_id: int | None = None,
    deleted: bool | None = None,
    starts_at: datetime | None = None,
) -> None:
    """Exactly what an ingesting webhook leaves behind on the source record."""
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, record_id)
            assert record is not None
            if client_id is not None:
                record.client_id = client_id
            if customer_id is not None:
                record.altegio_client_id = customer_id
            if deleted is not None:
                record.is_deleted = deleted
            if starts_at is not None:
                record.starts_at = starts_at


async def test_a_record_reassigned_during_the_booking_read_stops_the_history(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 1: the booking moves to another client mid-check.

    Both cards keep their numbers and their UUIDs, so every identity recheck
    passes; what changed is the record this row was written about. Reading the
    proven customer's history now and showing it against this row would be
    showing one visit's numbers under another visit's heading.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    other_client = await _card_in_branch(
        session_maker,
        company_id=COMPANY_ID,
        customer_id=person.customer_id + 820,
        customer_uuid=OTHER_CUSTOMER_UUID,
        phone=f"{person.phone}2",
    )

    async def race() -> None:
        await _move_record(
            session_maker,
            person.record_id,
            client_id=other_client,
            customer_id=person.customer_id + 820,
        )

    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_booking=race,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert report.detail_reason == SOURCE_RECORD_CHANGED
    assert report.retryable is False
    assert report.completed_visit_count is None
    # The booking was read; no page of anybody's history was.
    assert "booking" in reader.calls
    assert not any(call.startswith("history") for call in reader.calls)


async def test_a_source_record_deleted_during_the_history_read_is_not_confirmed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 1: the record is deleted while the pages are being walked."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _move_record(session_maker, person.record_id, deleted=True)

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages=_two_page_history(person),
        on_history_page={1: race},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert report.detail_reason == SOURCE_RECORD_CHANGED
    assert report.completed_visit_count is None
    assert report.history_verified is False
    # It stopped where the conflict became visible: before the second page.
    assert "history:1" in reader.calls
    assert "history:2" not in reader.calls


async def test_a_source_record_moved_out_of_the_period_is_not_confirmed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The half-open period is part of the chain, so leaving it ends the check."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        # Exactly the period's exclusive end: outside it.
        await _move_record(session_maker, person.record_id, starts_at=PERIOD_END)

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages=_two_page_history(person),
        on_history_page={1: race},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert report.detail_reason == SOURCE_RECORD_CHANGED
    assert report.completed_visit_count is None
    assert "history:2" not in reader.calls


async def test_a_record_that_changes_during_the_last_page_is_still_caught(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A one-page history has no "next page", so the final check is what catches it."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _move_record(session_maker, person.record_id, deleted=True)

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_history_page={1: race},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, SOURCE_EVENT_UNPROVEN)
    assert report.completed_visit_count is None
    assert report.history_verified is False
    assert reader.calls.count("history:1") == 1


async def test_an_unchanged_record_still_confirms_the_history(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The re-proof must not cost a correct answer, over two pages either."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages=_two_page_history(person),
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert report.completed_visit_count == 1
    assert report.source_booking_state == SOURCE_STATE_COMPLETED
    assert report.pages_read == 2
    assert [call for call in reader.calls if call.startswith("history")] == ["history:1", "history:2"]


async def test_no_lock_is_held_on_the_source_record_while_the_api_is_read(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The record re-proof must not pin the row a webhook needs to write."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    probe: dict[str, Any] = {"ok": False, "error": None}

    async def check_lock() -> None:
        try:
            async with session_maker() as session:
                async with session.begin():
                    await session.execute(text("SET LOCAL lock_timeout = '500ms'"))
                    await session.execute(
                        select(Record).where(Record.id == person.record_id).with_for_update(nowait=True)
                    )
            probe["ok"] = True
        except Exception as exc:  # noqa: BLE001 - the failure IS the finding
            probe["error"] = repr(exc)

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages=_two_page_history(person),
        on_history_page={2: check_lock},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert report.ok is True
    assert (probe["ok"], probe["error"]) == (True, None)


# ===========================================================================
# 26. The window inside the history read — round 4, item 2
# ===========================================================================
# The recheck before the history read is not the last moment a conflict can
# appear before the first page: the shared reader proves the card with a GET of
# its own first, and a webhook can write during THAT. The diagnostic now hands
# the shared reader a guard it calls before every page request, so a conflict
# that is already visible means the page is never asked for. Other consumers of
# the shared module pass no guard and behave exactly as before.


async def test_a_conflict_during_the_card_read_stops_before_the_first_page(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 2: the write lands during the history reader's own card GET."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _card_in_branch(
            session_maker,
            company_id=OTHER_COMPANY_ID,
            customer_id=person.customer_id,
            customer_uuid=OTHER_CUSTOMER_UUID,
            phone=f"{person.phone}8",
        )

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_customer=race,
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.completed_visit_count is None
    # The card was read, and then no page was requested at all.
    assert "customer" in reader.calls
    assert [call for call in reader.calls if call.startswith("history")] == []
    assert report.pages_read == 0


async def test_a_conflict_before_the_next_page_ends_the_walk(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Round 4, item 2: page one is read, the conflict appears, page two is not."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _card_in_branch(
            session_maker,
            company_id=OTHER_COMPANY_ID,
            customer_id=person.customer_id,
            customer_uuid=OTHER_CUSTOMER_UUID,
            phone=f"{person.phone}9",
        )

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages=_two_page_history(person),
        on_history_page={1: race},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.completed_visit_count is None
    assert [call for call in reader.calls if call.startswith("history")] == ["history:1"]


async def test_a_conflict_during_the_last_page_is_caught_by_the_final_check(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard cannot see it, so the check after the read must."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)

    async def race() -> None:
        await _card_in_branch(
            session_maker,
            company_id=OTHER_COMPANY_ID,
            customer_id=person.customer_id,
            customer_uuid=OTHER_CUSTOMER_UUID,
            phone=f"{person.phone}4",
        )

    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
        on_history_page={1: race},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.completed_visit_count is None
    assert report.history_verified is False
    assert [call for call in reader.calls if call.startswith("history")] == ["history:1"]


async def test_the_shared_history_reader_without_a_guard_is_unchanged(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 2: other consumers of the shared module keep their behaviour.

    No guard, so no local read, no extra call and no early stop: the card is
    proven and every page the answer declares is walked.
    """
    person = await seed_person(session_maker, 1, customer_uuid=CUSTOMER_UUID)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        pages=_two_page_history(person),
    )

    async def _no_pause(_seconds: float) -> None:
        return None

    result = await read_customer_bookings(reader, customer_uuid=CUSTOMER_UUID, pause=_no_pause, pause_sec=0.0)

    assert (result.complete, result.identity_current) == (True, True)
    assert result.pages_read == 2
    assert len(result.bookings) == 101
    assert [call for call in reader.calls if call.startswith("history")] == ["history:1", "history:2"]


# ===========================================================================
# 27. The listing is counted like every other read — round 4, item 3
# ===========================================================================
# ``list_customers`` kept its own counter and did not mark the call as in
# flight, so a deadline that expired while the workspace listing was unanswered
# looked like our own phase running out of time afterwards — and told the
# operator so.


async def test_a_directory_that_never_answers_is_a_provider_timeout(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 3: the deadline expires inside the listing call."""
    import altegio_bot.campaigns.easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)

    class SilentDirectory(FakeEasyWeek):
        async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
            self.calls.append("customers")
            await asyncio.sleep(5)
            raise AssertionError("the deadline should have expired")

    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.2)
    reader = SilentDirectory(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, TEMPORARILY_UNAVAILABLE)
    assert report.detail_reason == API_RETRYABLE_UNCERTAINTY
    assert report.retryable is True
    assert report.completed_visit_count is None


async def test_one_listing_call_is_counted_once() -> None:
    """Round 4, item 3: one read, one count, through the shared wrapper."""

    class Directory:
        async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
            return _page([_customer_row(phone="+4915100012345")], page=1, last=1, total=1)

        async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused
            raise AssertionError("not part of this test")

        async def get_customer(self, customer_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused
            raise AssertionError("not part of this test")

        async def list_customer_bookings(
            self, customer_uuid: str, page: int, per_page: int = 100
        ) -> dict[str, Any]:  # pragma: no cover - unused
            raise AssertionError("not part of this test")

    tracked = DiagnosticReader(Directory(), phone="+4915100012345")

    await tracked.list_customers(params={"phone": "+4915100012345", "page": 1})

    assert tracked.external_calls == 1
    assert tracked.awaiting is False


async def test_a_local_timeout_after_the_listing_answered_stays_on_our_side(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 3: the listing answered, then our own read ran out of time."""
    import altegio_bot.campaigns.easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    real_recheck = history_module._recheck_local_identity

    async def slow_recheck(*args: Any, **kwargs: Any):
        await asyncio.sleep(5)
        return await real_recheck(*args, **kwargs)

    monkeypatch.setattr(history_module, "_recheck_local_identity", slow_recheck)
    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.3)
    reader = FakeEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customer=_customer_row(phone=person.phone),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, DEADLINE_AFTER_READ)
    assert report.detail_reason == DEADLINE_AFTER_READ_DETAIL
    assert report.retryable is True
    assert "customers" in reader.calls


async def test_a_deadline_before_the_first_listing_call_blames_nobody(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 3: and the case where nothing external happened at all."""
    import altegio_bot.campaigns.easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    real_local = history_module._local_facts

    async def slow_local(*args: Any, **kwargs: Any):
        await asyncio.sleep(5)
        return await real_local(*args, **kwargs)

    monkeypatch.setattr(history_module, "_local_facts", slow_local)
    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.05)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid))

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, LOCAL_DEADLINE)
    assert report.detail_reason == LOCAL_DEADLINE_DETAIL
    assert reader.calls == []


# ===========================================================================
# 28. The direct card read keeps its exact reason — round 4, item 4
# ===========================================================================
# An HTTP 200 carrying ``{}`` is a body that does not keep the endpoint's
# contract, and it reached the operator as "личность не доказана" — a statement
# about the person rather than about the answer. A readable card for a different
# customer is the other thing, and the two must not share a sentence.


async def test_an_empty_card_body_on_the_stored_branch_is_a_format_error(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 4: ``{}`` where the stored correspondence needed no lookup."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid), customer={})

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, API_RESPONSE_MALFORMED)
    assert report.detail_reason == CUSTOMER_RESPONSE_MALFORMED
    assert report.retryable is False
    assert report.completed_visit_count is None
    assert [call for call in reader.calls if call.startswith("history")] == []


async def test_an_empty_card_body_on_the_repeat_read_is_a_format_error(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 4: the lookup's card read was fine, the repeat one is ``{}``."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        customers_in_sequence=[_customer_row(phone=person.phone), {}],
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, API_RESPONSE_MALFORMED)
    assert report.detail_reason == CUSTOMER_RESPONSE_MALFORMED
    assert report.completed_visit_count is None
    assert reader.calls.count("customer") == 2
    assert [call for call in reader.calls if call.startswith("history")] == []


async def test_a_valid_card_for_another_customer_on_the_repeat_read_is_a_conflict(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 4, item 4: the other failure of the same read, kept apart from it."""
    run_id, recipient_id, person = await _preview_without_stored_uuid(session_maker, monkeypatch)
    reader = HookedEasyWeek(
        booking=_booking_response(booking_uuid=person.booking_uuid),
        customers=_customers_page([_customer_row(phone=person.phone)]),
        customers_in_sequence=[
            _customer_row(phone=person.phone),
            _customer_row(phone=person.phone, customer_uuid=OTHER_CUSTOMER_UUID),
        ],
        pages={1: _history_page([_history_row(booking_uuid=person.booking_uuid)])},
    )

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason) == (False, IDENTITY_UUID_CONFLICT)
    assert report.detail_reason == CUSTOMER_UUID_MISMATCH
    assert report.completed_visit_count is None
    assert [call for call in reader.calls if call.startswith("history")] == []


@pytest.mark.parametrize(
    ("failure", "reason", "retryable"),
    [
        pytest.param(EasyWeekRetryableError("429"), TEMPORARILY_UNAVAILABLE, True, id="429"),
        pytest.param(EasyWeekRetryableError("503"), TEMPORARILY_UNAVAILABLE, True, id="503"),
        pytest.param(EasyWeekPermanentError("422"), API_REFUSED, False, id="422"),
        pytest.param(EasyWeekConfigError("no key"), API_NOT_CONFIGURED, False, id="unconfigured"),
    ],
)
async def test_the_card_read_keeps_the_other_families_too(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    reason: str,
    retryable: bool,
) -> None:
    """Round 4, item 4: 429, 503, a permanent 422 and an unusable integration."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    reader = FakeEasyWeek(booking=_booking_response(booking_uuid=person.booking_uuid), customer=failure)

    report = await _check(session_maker, run_id=run_id, recipient_id=recipient_id, reader=reader)

    assert (report.ok, report.reason, report.retryable) == (False, reason, retryable)
    assert report.completed_visit_count is None
