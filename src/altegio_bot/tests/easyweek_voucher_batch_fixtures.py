"""Synthetic payloads and fakes for the §41 voucher snapshot batch tests.

Every identity here is obviously fabricated, and the voucher codes are
sentinels whose whole job is to be searched for: no production customer, phone
number, order, message id or artifact value belongs in this repository.

One fixture builds N people
---------------------------
The whole point of this phase is that there is more than one recipient, so
almost everything here is indexed by slot. ``seed_batch_preview`` creates a
completed preview holding *count* manually selected candidates, each with its
own customer UUID, phone and local client, and ``FakeReader`` answers the
workspace-wide phone lookup for all of them.
"""

from __future__ import annotations

import itertools
import json
import uuid as uuid_module
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    KARLSRUHE_COMPANY_ID,
    NEW_CLIENT_CAMPAIGN_CODE,
    UNIT_PRICE_MINOR,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_batch.runner import BatchRequest
from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    DELIVERY_UNKNOWN,
    DeliveryOutcome,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    RECIPIENT_BASIS_TEST,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekVoucherSnapshotBatchItem,
    MessageTemplate,
    WhatsAppSender,
)
from altegio_bot.settings import settings

COMPANY_ID = KARLSRUHE_COMPANY_ID
STAFFER_UUID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
ACCOUNT_UUID = "cccccccc-3333-4333-8333-cccccccccccc"
SENDER_PHONE_NUMBER_ID = "SYNTHETIC_BATCH_PHONE_NUMBER_ID"

# The maximum a test ever needs to address by hand. Five is the batch ceiling;
# the sixth exists so a test can try to exceed it.
_MAX_FIXTURE_PEOPLE = 8

# One obviously synthetic identity per person, generated from the index so a
# test can talk about "person 3" without a table of magic strings.
CUSTOMER_UUIDS = tuple(
    str(uuid_module.UUID(f"a{index}a1a1a1-b2b2-4c3c-8d4d-e5e5e5e5e5e{index}"))
    for index in range(1, _MAX_FIXTURE_PEOPLE + 1)
)
ORDER_UUIDS = tuple(
    str(uuid_module.UUID(f"0d{index}d0d0d-1111-4222-8333-44444444444{index}"))
    for index in range(1, _MAX_FIXTURE_PEOPLE + 1)
)
PHONES = tuple(f"+49151123456{70 + index}" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))
CUSTOMER_NAMES = tuple(f"Synthetic Fixture {index}" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))
PROVIDER_MESSAGE_IDS = tuple(f"wamid.SYNTHETICBATCH{index:04d}" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))

# The strings every secrecy test hunts for, one per slot.
VOUCHER_CODE_SENTINELS = tuple(f"SENTINEL-BATCH-CODE-{index}qq777" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))

_LOCAL_CLIENT_IDS = itertools.count(940042)


def _next_local_client_id() -> int:
    """A fresh synthetic local id per NEW client row."""
    return next(_LOCAL_CLIENT_IDS)


# The August wave: over, in the past, and the same whenever the suite runs.
NOW = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
PERIOD_START = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 8, 31, 23, 59, 59, tzinfo=timezone.utc)

BOOKING_LINK = "https://karlsruhe.example.invalid/"


def location_map(*, booking_link: str = BOOKING_LINK) -> str:
    """The server-side registry, as the settings string the loader reads."""
    return json.dumps(
        {
            "karlsruhe": {
                "location_id": COMPANY_ID,
                "location_uuid": KARLSRUHE_LOCATION_UUID,
                "meta_template_prefix": "ka",
                "booking_page_url": booking_link,
            }
        }
    )


@pytest.fixture
def batch_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fence open and every identity configured — the acting case."""
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_staffer_uuid", STAFFER_UUID, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_snapshot_batch_account_uuid", ACCOUNT_UUID, raising=False)


def batch_request(*, run_id: int) -> BatchRequest:
    return BatchRequest(
        preview_run_id=run_id,
        sender_code="default",
        staffer_uuid=STAFFER_UUID,
        payment_account_uuid=ACCOUNT_UUID,
    )


def customer_payload(index: int, **changes: Any) -> dict[str, Any]:
    """What ``GET /customers/{uuid}`` answers for synthetic person *index*."""
    payload: dict[str, Any] = {
        "uuid": CUSTOMER_UUIDS[index],
        "phone": PHONES[index],
        "first_name": CUSTOMER_NAMES[index],
    }
    payload.update(changes)
    return payload


def customers_page(
    rows: list[dict[str, Any]],
    *,
    current: int = 1,
    last: int = 1,
    total: int | None = None,
) -> dict[str, Any]:
    """One page of ``GET /customers?phone=`` as the strict reader expects it."""
    return {
        "data": rows,
        "meta": {
            "current_page": current,
            "last_page": last,
            "total": len(rows) if total is None else total,
        },
    }


def template_payload(*, services: int = 42, all_services: int = 42, **changes: Any) -> dict[str, Any]:
    """The voucher template as the 42/42 baseline expects to read it."""
    payload: dict[str, Any] = {
        "uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "is_enabled": True,
        "is_online": False,
        "is_single_charge": True,
        "cost": UNIT_PRICE_MINOR,
        "value": UNIT_PRICE_MINOR,
        "validity": None,
        "forces_activation": True,
        "activate_after": 0,
        "activate_at": None,
        "is_connected_all_branches": True,
        "branches_count": 3,
        "all_branches_count": 3,
        "is_connected_all_services": True,
        "services_count": services,
        "all_services_count": all_services,
        "goods_count": 0,
        "vouchers_count": 0,
        "activated_vouchers_count": 0,
    }
    payload.update(changes)
    return payload


def issued_voucher(index: int, **changes: Any) -> dict[str, Any]:
    """The singleton artifact shape production actually returned: no quantity."""
    voucher: dict[str, Any] = {
        "code": VOUCHER_CODE_SENTINELS[index],
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "value": UNIT_PRICE_MINOR,
        "price": UNIT_PRICE_MINOR,
    }
    voucher.update(changes)
    return voucher


def voucher_order(
    index: int,
    *,
    marker: str,
    status: str = "open",
    order_uuid: str | None = None,
    vouchers: list[dict[str, Any]] | None = None,
    **changes: Any,
) -> dict[str, Any]:
    order: dict[str, Any] = {
        "uuid": order_uuid or ORDER_UUIDS[index],
        "comment": marker,
        "customer": {"uuid": CUSTOMER_UUIDS[index]},
        "status": status,
        "is_paid": status in ("paid", "refunded"),
        "is_reverted": status == "refunded",
        "total": UNIT_PRICE_MINOR,
        "subtotal": UNIT_PRICE_MINOR,
        "services": [],
        "goods": [],
        "vouchers": [issued_voucher(index)] if vouchers is None else vouchers,
        "created_at": NOW.isoformat(),
    }
    order.update(changes)
    return order


def orders_page(
    rows: list[dict[str, Any]] | None = None,
    *,
    current: int = 1,
    last: int = 1,
) -> dict[str, Any]:
    """One page of ``GET /orders`` as the §35 page walker expects it."""
    return {
        "data": rows or [],
        "meta": {"current_page": current, "last_page": last, "per_page": 100},
    }


class FakeReader:
    """The EasyWeek read surface this phase uses, and nothing else.

    The phone listing is keyed by number so one reader can answer for a whole
    batch; ``get_order`` and ``get_voucher_template`` answer from dictionaries
    the test controls, so an out-of-order or failing read is expressed by what
    the test puts in them rather than by patching the runner.
    """

    def __init__(
        self,
        *,
        count: int = 1,
        customers: dict[str, Any] | None = None,
        customer_pages: dict[str, list[dict[str, Any]] | Exception] | None = None,
        orders: dict[str, Any] | None = None,
        template: dict[str, Any] | Exception | None = None,
        order_pages: list[dict[str, Any]] | Exception | None = None,
    ) -> None:
        # One card per person, addressable by UUID.
        self.customers: dict[str, Any] = customers or {
            CUSTOMER_UUIDS[index]: customer_payload(index) for index in range(count)
        }
        # One single-row page per number.
        self.customer_pages: dict[str, list[dict[str, Any]] | Exception] = customer_pages or {
            PHONES[index]: [customers_page([customer_payload(index)])] for index in range(count)
        }
        self.orders: dict[str, Any] = orders or {}
        self.template = template if template is not None else template_payload()
        self.order_pages = order_pages if order_pages is not None else [orders_page()]
        self.customer_calls: list[str] = []
        self.order_calls: list[str] = []
        self.listing_calls: list[tuple[str, int]] = []
        self.template_calls = 0

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        phone = str(params.get("phone"))
        page = int(params.get("page", 1))
        self.listing_calls.append((phone, page))
        pages = self.customer_pages.get(phone)
        if isinstance(pages, Exception):
            raise pages
        if pages is None:
            return customers_page([])
        if page > len(pages):
            raise AssertionError(f"the walk asked for page {page} beyond the fixture")
        return pages[page - 1]

    async def list_location_customer_orders(
        self, *, location_uuid: str, customer_uuid: str, page: int = 1
    ) -> dict[str, Any]:
        if isinstance(self.order_pages, Exception):
            raise self.order_pages
        if page > len(self.order_pages):
            raise AssertionError(f"the walk asked for page {page} beyond the fixture")
        return self.order_pages[page - 1]

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.customer_calls.append(customer_uuid)
        answer = self.customers.get(customer_uuid)
        if isinstance(answer, Exception):
            raise answer
        if answer is None:
            raise KeyError(customer_uuid)
        return answer

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        self.order_calls.append(order_uuid)
        answer = self.orders.get(order_uuid)
        if isinstance(answer, Exception):
            raise answer
        if answer is None:
            raise KeyError(order_uuid)
        return answer

    async def get_voucher_template(self, template_uuid: str) -> dict[str, Any]:
        self.template_calls += 1
        if isinstance(self.template, Exception):
            raise self.template
        return self.template

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused here
        raise AssertionError("the batch never reads a booking")

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        # A manual basis proves no history, so nothing may walk one.
        raise AssertionError("the batch never walks a customer's history")


class FakeMutator:
    """The three EasyWeek writes, recorded rather than performed.

    Answers can be given per call, so a test can say "the third CREATE times
    out" without patching anything.
    """

    def __init__(
        self,
        *,
        create: Any = None,
        pay: Any = None,
        refund: Any = None,
        create_sequence: list[Any] | None = None,
        pay_sequence: list[Any] | None = None,
        reader: FakeReader | None = None,
        settles: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.create = create
        self.pay = pay
        self.refund = refund
        self.create_sequence = list(create_sequence) if create_sequence is not None else None
        self.pay_sequence = list(pay_sequence) if pay_sequence is not None else None
        # A successful pay or refund changes what the NEXT readback sees.
        # Modelled here so a test does not have to flip the reader by hand at
        # exactly the right instant — which is an instant the runner controls.
        self.reader = reader
        self.settles = settles or {}
        self.calls: list[str] = []
        self.create_calls: list[dict[str, Any]] = []
        self.pay_calls: list[dict[str, Any]] = []

    @staticmethod
    def _answer(value: Any) -> Any:
        if isinstance(value, Exception):
            raise value
        return value

    def _settle(self, order_uuid: str | None) -> None:
        if self.reader is None or order_uuid is None:
            return
        settled = self.settles.get(order_uuid)
        if settled is not None:
            self.reader.orders[order_uuid] = settled

    async def create_voucher_order(self, **kwargs: Any) -> Any:
        self.calls.append("create")
        self.create_calls.append(kwargs)
        if self.create_sequence is not None:
            return self._answer(self.create_sequence[len(self.create_calls) - 1])
        return self._answer(self.create)

    async def pay_voucher_order(self, **kwargs: Any) -> Any:
        self.calls.append("pay")
        self.pay_calls.append(kwargs)
        answer = self.pay_sequence[len(self.pay_calls) - 1] if self.pay_sequence is not None else self.pay
        result = self._answer(answer)
        self._settle(kwargs.get("order_uuid"))
        return result

    async def refund_voucher_order(self, **kwargs: Any) -> Any:
        self.calls.append("refund")
        result = self._answer(self.refund)
        self._settle(kwargs.get("order_uuid"))
        return result


class FakeSender:
    """The Meta calls, recorded. Records that params ARRIVED, never stores them."""

    def __init__(self, outcomes: list[DeliveryOutcome] | None = None) -> None:
        self.outcomes = outcomes
        self.calls = 0
        self.saw_codes: list[bool] = []
        self.destinations: list[str] = []

    async def send_voucher_template(self, *, params: list[str], to_e164: str, **kwargs: Any) -> DeliveryOutcome:
        index = self.calls
        self.calls += 1
        self.saw_codes.append(any(code in params for code in VOUCHER_CODE_SENTINELS))
        self.destinations.append(to_e164)
        if self.outcomes is not None:
            return self.outcomes[index]
        return DeliveryOutcome(outcome=DELIVERY_ACCEPTED, provider_message_id=PROVIDER_MESSAGE_IDS[index])


def unknown_outcome(reason: str = "timeout") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=reason)


def rejected_outcome(reason: str = "invalid_parameter") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_REJECTED, reason=reason, http_status=400)


def accepted_outcome(index: int) -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_ACCEPTED, provider_message_id=PROVIDER_MESSAGE_IDS[index])


async def seed_batch_preview(
    session_maker,
    *,
    count: int = 3,
    company_id: int = COMPANY_ID,
    campaign_code: str = NEW_CLIENT_CAMPAIGN_CODE,
    run_status: str = "completed",
    run_mode: str = "preview",
    recipient_status: str = "candidate",
    bases: list[str] | None = None,
    customer_uuids: list[str | None] | None = None,
    opted_out: list[bool] | None = None,
    period_start: datetime = PERIOD_START,
    period_end: datetime = PERIOD_END,
    provider: str = PROVIDER_EASYWEEK,
) -> tuple[int, list[int]]:
    """One completed preview with *count* manually selected candidates.

    Returns the run id and the recipient ids in creation order, which is also
    the slot order the batch derives.
    """
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider=provider,
                campaign_code=campaign_code,
                mode=run_mode,
                company_ids=[company_id],
                period_start=period_start,
                period_end=period_end,
                status=run_status,
            )
            session.add(run)
            await session.flush()

            recipient_ids: list[int] = []
            for index in range(count):
                basis = bases[index] if bases is not None else RECIPIENT_BASIS_MANUAL
                customer = customer_uuids[index] if customer_uuids is not None else CUSTOMER_UUIDS[index]
                if provider != PROVIDER_EASYWEEK:
                    # The shared CHECK forbids the manual basis outside EasyWeek
                    # entirely, so an Altegio run is seeded the only way the
                    # schema allows one. That is the point of such a fixture:
                    # the batch must refuse it, and it must be a real row.
                    basis = RECIPIENT_BASIS_EARNED
                    customer = None
                out = opted_out[index] if opted_out is not None else False
                number = PHONES[index]
                # The same human being can appear in two previews — that is
                # exactly the mistake the entitlement rule exists to catch — so
                # an existing local client is REUSED rather than duplicated.
                client = (
                    await session.execute(
                        select(Client)
                        .where(Client.provider == provider)
                        .where(Client.company_id == company_id)
                        .where(Client.phone_e164 == number)
                    )
                ).scalar_one_or_none()
                if client is None:
                    client = Client(
                        provider=provider,
                        company_id=company_id,
                        # Not-null in the shared table. An EasyWeek client
                        # carries a synthetic local id here; this phase never
                        # reads it.
                        altegio_client_id=_next_local_client_id(),
                        phone_e164=number,
                        display_name=CUSTOMER_NAMES[index],
                        raw={},
                        wa_opted_out=out,
                    )
                    session.add(client)
                else:
                    client.wa_opted_out = out
                await session.flush()
                recipient = CampaignRecipient(
                    campaign_run_id=run.id,
                    provider=provider,
                    company_id=company_id,
                    client_id=client.id,
                    phone_e164=number,
                    display_name=CUSTOMER_NAMES[index],
                    status=recipient_status,
                    recipient_basis=basis,
                    easyweek_customer_uuid=uuid_module.UUID(customer) if customer else None,
                    # The owner-test basis carries its identity in its own typed
                    # column, and the shared CHECK constraints require exactly
                    # that. A test row here is only ever a foreign basis for the
                    # batch to refuse.
                    easyweek_test_customer_uuid=(
                        uuid_module.UUID(CUSTOMER_UUIDS[index]) if basis == RECIPIENT_BASIS_TEST else None
                    ),
                    is_opted_out=out,
                )
                session.add(recipient)
                await session.flush()
                recipient_ids.append(recipient.id)
            return run.id, recipient_ids


async def seed_template_and_sender(session_maker, *, company_id: int = COMPANY_ID) -> None:
    """The approved Meta template row and an active sender line.

    Idempotent: a test that seeds more than one preview in one database would
    otherwise collide on the sender's provider/company/code uniqueness, which is
    a fact about the fixture rather than about anything under test.
    """
    async with session_maker() as session:
        # One explicit transaction around the read AND the writes: the SELECT
        # autobegins, and opening a second transaction on top of it is the very
        # error §36.10 hit in production.
        async with session.begin():
            existing = await session.scalar(
                select(WhatsAppSender.id)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.company_id == company_id)
                .where(WhatsAppSender.sender_code == "default")
            )
            if existing is not None:
                return
            session.add(
                MessageTemplate(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    code=VOUCHER_TEMPLATE_CODE,
                    language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
                    meta_template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
                    body=template_contract.VOUCHER_TEMPLATE_BODY,
                    is_active=True,
                )
            )
            session.add(
                WhatsAppSender(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    sender_code="default",
                    phone_number_id=SENDER_PHONE_NUMBER_ID,
                    is_active=True,
                )
            )


async def markers_for(session_maker) -> dict[int, str]:
    """The reconciliation marker the frozen batch actually wrote, per slot."""
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherSnapshotBatchItem).order_by(EasyWeekVoucherSnapshotBatchItem.slot.asc())
                )
            )
            .scalars()
            .all()
        )
        return {int(row.slot): row.reconciliation_marker for row in rows}


async def marker_orders(session_maker, *, count: int, **changes: Any) -> dict[str, dict[str, Any]]:
    """One open order per slot, dated inside the create window the ledger wrote.

    Built from the window the ledger actually recorded rather than from a fixed
    constant: the window is anchored on the real clock at claim time, and a row
    dated from a constant would fall outside it and be correctly ignored.
    """
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherSnapshotBatchItem).order_by(EasyWeekVoucherSnapshotBatchItem.slot.asc())
                )
            )
            .scalars()
            .all()
        )
    orders: dict[str, dict[str, Any]] = {}
    for row in rows[:count]:
        index = int(row.slot) - 1
        start, end = row.create_window_start, row.create_window_end
        created = NOW if start is None or end is None else start + (end - start) / 2
        orders[ORDER_UUIDS[index]] = voucher_order(
            index,
            marker=row.reconciliation_marker,
            created_at=created.isoformat(),
            **changes,
        )
    return orders


__all__ = [
    "ACCOUNT_UUID",
    "BOOKING_LINK",
    "COMPANY_ID",
    "CUSTOMER_NAMES",
    "CUSTOMER_UUIDS",
    "NOW",
    "ORDER_UUIDS",
    "PERIOD_END",
    "PERIOD_START",
    "PHONES",
    "PROVIDER_MESSAGE_IDS",
    "SENDER_PHONE_NUMBER_ID",
    "STAFFER_UUID",
    "VOUCHER_CODE_SENTINELS",
    "FakeMutator",
    "FakeReader",
    "FakeSender",
    "accepted_outcome",
    "batch_configuration",
    "batch_request",
    "customer_payload",
    "customers_page",
    "issued_voucher",
    "location_map",
    "marker_orders",
    "markers_for",
    "orders_page",
    "rejected_outcome",
    "seed_batch_preview",
    "seed_template_and_sender",
    "template_payload",
    "unknown_outcome",
    "voucher_order",
]
