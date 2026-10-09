"""Synthetic payloads and fakes for the §42 production voucher mailing tests.

Every identity here is obviously fabricated, and the voucher codes are
sentinels whose whole job is to be searched for: no production customer, phone
number, order, message id or artifact value belongs in this repository.

One fixture builds N people, and N is not five
----------------------------------------------
§41's fixtures stopped at eight because its schema stopped at five. This phase
has no recipient ceiling, so the fixtures have to be able to build a mailing
big enough that a quadratic cost would show up and a "first five" bug would be
visible — hence :data:`_MAX_FIXTURE_PEOPLE`.

``seed_production_preview`` creates a completed preview holding *count*
manually selected candidates, each with its own customer UUID, phone and local
client, and ``FakeReader`` answers the workspace-wide phone lookup for all of
them.

Two independent previews, on purpose
------------------------------------
Several tests need two mailings at once — to prove slot numbers may repeat
across batches, that one batch's approval cannot authorise another's stage, and
that a halt in one does not touch the other. ``seed_production_preview`` can be
called more than once in a test and allocates disjoint people through
``offset``, so the two previews never accidentally share a customer.
"""

from __future__ import annotations

import itertools
import json
import uuid as uuid_module
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    DELIVERY_UNKNOWN,
    DeliveryOutcome,
)
from altegio_bot.campaigns.easyweek_voucher_production import issuer as issuer_module
from altegio_bot.campaigns.easyweek_voucher_production.composition import BatchApproval
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    KARLSRUHE_COMPANY_ID,
    NEW_CLIENT_CAMPAIGN_CODE,
    UNIT_PRICE_MINOR,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_production.runner import ProductionRequest
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
    EasyWeekVoucherProductionBatchItem,
    MessageTemplate,
    WhatsAppSender,
)
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

COMPANY_ID = KARLSRUHE_COMPANY_ID
# The ONE approved issuer of every production voucher, synthetically (§43.9).
# Obviously fabricated, like every other identity here: the real staffer UUID is
# deployment configuration and the owner's local evidence, and putting it in a
# fixture would publish it in the repository forever.
STAFFER_UUID = "dddddddd-4444-4444-8444-dddddddddddd"
# A second perfectly valid staffer of the same branch, for the case the pin
# exists to catch: seven of the eight people at Karlsruhe have a UUID that passes
# every other check in this phase and is still the wrong answer.
OTHER_STAFFER_UUID = "dddddddd-4444-4444-8444-dddddddddd99"
ACCOUNT_UUID = "eeeeeeee-5555-4555-8555-eeeeeeeeeeee"
# The free gift certificate settles through a different till, so the fixtures need a
# second synthetic account: one that is NOT the paid one, which is the whole point.
GIFT_ACCOUNT_UUID = "eeeeeeee-6666-4666-8666-eeeeeeeeeeee"
SENDER_PHONE_NUMBER_ID = "SYNTHETIC_PRODUCTION_PHONE_NUMBER_ID"

# The fingerprint the synthetic issuer above has, computed the same way runtime
# computes it. Tests replace what `expected_issuer_fingerprint()` ANSWERS, which
# is the single narrow seam the issuer module exposes — never a bypass flag and
# never an environment variable. There is no value this can be set to that lets
# an arbitrary UUID through: the comparison stays exact, it is only told which
# fingerprint is the approved one in this synthetic world.
SYNTHETIC_ISSUER_FINGERPRINT = issuer_module.issuer_fingerprint(STAFFER_UUID)


def pin_synthetic_issuer(monkeypatch: pytest.MonkeyPatch, *, fingerprint: str | None = None) -> None:
    """Make `STAFFER_UUID` the approved issuer for the duration of one test."""
    monkeypatch.setattr(
        issuer_module,
        "expected_issuer_fingerprint",
        lambda: fingerprint if fingerprint is not None else SYNTHETIC_ISSUER_FINGERPRINT,
    )


def staffers_page(
    uuids: list[str],
    *,
    current: int = 1,
    last: int = 1,
) -> dict[str, Any]:
    """One page of ``GET /locations/{uuid}/staffers`` as the strict walk expects.

    The walk proves completeness from ``meta.last_page``, so a fixture that wants
    an INCOMPLETE walk simply omits or contradicts the metadata.
    """
    return {
        "data": [{"uuid": value} for value in uuids],
        "meta": {"current_page": current, "last_page": last, "per_page": 100},
    }


# Big enough for the sizes these tests actually exercise: 1, 2, 6 (the number
# §41 could not hold), 12 (a mailing where a per-item quadratic would bite), and
# two independent previews side by side without sharing a person.
_MAX_FIXTURE_PEOPLE = 40

# One obviously synthetic identity per person, generated from the index so a
# test can talk about "person 3" without a table of magic strings.
CUSTOMER_UUIDS = tuple(
    str(uuid_module.UUID(int=(0xC0FFEE << 88) | (index << 8) | 0x42)) for index in range(1, _MAX_FIXTURE_PEOPLE + 1)
)
ORDER_UUIDS = tuple(
    str(uuid_module.UUID(int=(0xD0D0 << 96) | (index << 8) | 0x11)) for index in range(1, _MAX_FIXTURE_PEOPLE + 1)
)
PHONES = tuple(f"+4915177{100 + index:04d}" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))
CUSTOMER_NAMES = tuple(f"Synthetic Production {index}" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))
PROVIDER_MESSAGE_IDS = tuple(f"wamid.SYNTHETICPROD{index:04d}" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))

# The strings every secrecy test hunts for, one per person.
VOUCHER_CODE_SENTINELS = tuple(f"SENTINEL-PROD-CODE-{index}zz991" for index in range(1, _MAX_FIXTURE_PEOPLE + 1))

_LOCAL_CLIENT_IDS = itertools.count(770042)


def _next_local_client_id() -> int:
    """A fresh synthetic local id per NEW client row."""
    return next(_LOCAL_CLIENT_IDS)


# The August wave: over, in the past, and the same whenever the suite runs.
# This is the entitlement period, deliberately not "now": the first real task of
# this phase is an August audience mailed in October, and the fixtures model
# that rather than the comfortable case.
NOW = datetime(2026, 10, 12, 9, 0, tzinfo=timezone.utc)
PERIOD_START = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 8, 31, 23, 59, 59, tzinfo=timezone.utc)

# A second, different wave, for the tests about one person and two periods.
SEPTEMBER_START = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)
SEPTEMBER_END = datetime(2026, 9, 30, 23, 59, 59, tzinfo=timezone.utc)

BOOKING_LINK = "https://karlsruhe-production.example.invalid/"


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
def production_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fence open and every identity configured — the acting case.

    Includes the §43.9 issuer pin, because a correctly configured deployment has
    one: the configured staffer IS the approved issuer. Tests about the pin
    failing re-point the seam or the setting themselves.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", STAFFER_UUID, raising=False)
    # The till a correctly configured deployment points at TODAY, which is the
    # one the current new-mailing product settles through. §43/§44 requests name
    # their own historical account explicitly and are unaffected.
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_account_uuid", GIFT_ACCOUNT_UUID, raising=False)
    pin_synthetic_issuer(monkeypatch)
    # The same synthetic deployment also backs explicit §45 tests. Historical
    # requests never consult this new-product account proof.
    from altegio_bot.campaigns.easyweek_voucher_production import account

    monkeypatch.setattr(
        account,
        "expected_account_fingerprint",
        # Synthetic, and per contract: the till pin is now a property of the
        # product, so the stub has to take the contract the real one takes.
        lambda contract=None: account.account_fingerprint(
            GIFT_ACCOUNT_UUID if contract is not None and contract.free_issue else ACCOUNT_UUID
        ),
    )


def production_request(*, run_id: int, batch_id: int | None = None) -> ProductionRequest:
    return ProductionRequest(
        preview_run_id=run_id,
        sender_code="default",
        staffer_uuid=STAFFER_UUID,
        payment_account_uuid=ACCOUNT_UUID,
        batch_id=batch_id,
    )


def approval_for(count: int) -> BatchApproval:
    """The approval a correct operator would type for a snapshot of *count*."""
    return BatchApproval(expected_recipient_count=count, approved_exposure_minor=count * UNIT_PRICE_MINOR)


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


def template_payload(*, services: int = 43, all_services: int = 43, **changes: Any) -> dict[str, Any]:
    """The voucher template as the §42 baseline expects to read it.

    43/43, the owner-approved live catalogue of 27.09.2026 that
    ``PRODUCTION_BASELINE_VERSION`` pins. The historical §37.2 manual canary
    keeps its own 42/42 baseline and §41 its own constant; this one is this
    phase's, and a reading of 42 here is a refusal rather than a fallback.
    """
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
    mailing; ``get_order`` and ``get_voucher_template`` answer from dictionaries
    the test controls, so an out-of-order or failing read is expressed by what
    the test puts in them rather than by patching the runner.

    ``indices`` names WHICH synthetic people this reader knows about, which is
    what lets two previews be served by one reader without either of them
    answering for the other's customers.
    """

    def __init__(
        self,
        *,
        count: int = 1,
        indices: list[int] | None = None,
        customers: dict[str, Any] | None = None,
        customer_pages: dict[str, list[dict[str, Any]] | Exception] | None = None,
        orders: dict[str, Any] | None = None,
        template: dict[str, Any] | Exception | None = None,
        order_pages: list[dict[str, Any]] | Exception | None = None,
        staffer_pages: list[dict[str, Any]] | Exception | None = None,
    ) -> None:
        known = list(range(count)) if indices is None else list(indices)
        # One card per person, addressable by UUID.
        self.customers: dict[str, Any] = (
            customers if customers is not None else {CUSTOMER_UUIDS[index]: customer_payload(index) for index in known}
        )
        # One single-row page per number.
        self.customer_pages: dict[str, list[dict[str, Any]] | Exception] = (
            customer_pages
            if customer_pages is not None
            else {PHONES[index]: [customers_page([customer_payload(index)])] for index in known}
        )
        self.orders: dict[str, Any] = orders or {}
        self.template = template if template is not None else template_payload()
        self.order_pages = order_pages if order_pages is not None else [orders_page()]
        # The location's staffer catalogue, for the §43.9 membership proof. By
        # default the branch holds the approved issuer plus one other employee,
        # which is the shape production actually has: one right answer among
        # several valid UUIDs.
        self.staffer_pages = (
            staffer_pages if staffer_pages is not None else [staffers_page([OTHER_STAFFER_UUID, STAFFER_UUID])]
        )
        self.customer_calls: list[str] = []
        self.order_calls: list[str] = []
        self.listing_calls: list[tuple[str, int]] = []
        self.staffer_calls: list[tuple[str, int]] = []
        self.template_calls = 0

    def teach(self, indices: list[int]) -> None:
        """Also answer for these synthetic people, for a second preview."""
        for index in indices:
            self.customers[CUSTOMER_UUIDS[index]] = customer_payload(index)
            self.customer_pages[PHONES[index]] = [customers_page([customer_payload(index)])]

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

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        """One page of the location's staffer catalogue.

        Recorded per call, so a test can prove the walk happens ONCE per stage
        plan rather than once per recipient.
        """
        self.staffer_calls.append((location_uuid, page))
        if isinstance(self.staffer_pages, Exception):
            raise self.staffer_pages
        if page > len(self.staffer_pages):
            raise AssertionError(f"the staffer walk asked for page {page} beyond the fixture")
        return self.staffer_pages[page - 1]

    async def get_voucher_template(self, template_uuid: str) -> dict[str, Any]:
        self.template_calls += 1
        if isinstance(self.template, Exception):
            raise self.template
        return self.template

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused here
        raise AssertionError("the production mailing never reads a booking")

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        # A manual basis proves no history, so nothing may walk one.
        raise AssertionError("the production mailing never walks a customer's history")


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
        self.refund_calls: list[dict[str, Any]] = []

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
        self.refund_calls.append(kwargs)
        result = self._answer(self.refund)
        self._settle(kwargs.get("order_uuid"))
        return result


class FakeSender:
    """The Meta calls, recorded. Records that params ARRIVED, never stores them."""

    def __init__(self, outcomes: list[DeliveryOutcome] | None = None, *, first_index: int = 0) -> None:
        self.outcomes = outcomes
        self.first_index = first_index
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
        return DeliveryOutcome(
            outcome=DELIVERY_ACCEPTED,
            provider_message_id=PROVIDER_MESSAGE_IDS[self.first_index + index],
        )


def unknown_outcome(reason: str = "timeout") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=reason)


def rejected_outcome(reason: str = "invalid_parameter") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_REJECTED, reason=reason, http_status=400)


def accepted_outcome(index: int) -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_ACCEPTED, provider_message_id=PROVIDER_MESSAGE_IDS[index])


async def seed_production_preview(
    session_maker,
    *,
    count: int = 3,
    offset: int = 0,
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
    the slot order the mailing derives.

    ``offset`` shifts which synthetic people are used, so two previews seeded in
    one test hold genuinely different customers unless a test deliberately
    overlaps them.
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
            for position in range(count):
                index = offset + position
                basis = bases[position] if bases is not None else RECIPIENT_BASIS_MANUAL
                customer = customer_uuids[position] if customer_uuids is not None else CUSTOMER_UUIDS[index]
                if provider != PROVIDER_EASYWEEK:
                    # The shared CHECK forbids the manual basis outside EasyWeek
                    # entirely, so an Altegio run is seeded the only way the
                    # schema allows one. That is the point of such a fixture:
                    # the mailing must refuse it, and it must be a real row.
                    basis = RECIPIENT_BASIS_EARNED
                    customer = None
                if basis != RECIPIENT_BASIS_MANUAL:
                    # The shared CHECK is an equivalence:
                    # ``recipient_basis = manual`` IFF ``easyweek_customer_uuid``
                    # is set. A foreign-basis row therefore carries no customer
                    # UUID in that column at all — an earned row identifies its
                    # person through its booking and a test row through its own
                    # typed column. Seeding it any other way would be seeding a
                    # row production cannot hold.
                    customer = None
                out = opted_out[position] if opted_out is not None else False
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
                    # mailing to refuse.
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


async def markers_for(session_maker, *, batch_id: int) -> dict[int, str]:
    """The reconciliation marker one frozen batch actually wrote, per slot."""
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                    .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
                )
            )
            .scalars()
            .all()
        )
        return {int(row.slot): row.reconciliation_marker for row in rows}


async def marker_orders(
    session_maker, *, batch_id: int, count: int | None = None, offset: int = 0, **changes: Any
) -> dict[str, dict[str, Any]]:
    """One open order per slot of ONE batch, dated inside its create window.

    Built from the window the ledger actually recorded rather than from a fixed
    constant: the window is anchored on the real clock at claim time, and a row
    dated from a constant would fall outside it and be correctly ignored.
    """
    async with session_maker() as session:
        rows = list(
            (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem)
                    .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                    .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
                )
            )
            .scalars()
            .all()
        )
    orders: dict[str, dict[str, Any]] = {}
    for row in rows if count is None else rows[:count]:
        index = offset + int(row.slot) - 1
        start, end = row.create_window_start, row.create_window_end
        # Inside the window the ledger actually recorded, when there is one.
        #
        # The fallback is the REAL clock, not :data:`NOW`. The create window is
        # anchored on ``utcnow()`` at claim time, and ``NOW`` is deliberately in
        # October 2026 to model an August audience mailed late — so dating an
        # order from it would put the row weeks outside the window and the
        # marker search would correctly find nothing. That is a trap about the
        # fixture rather than a fact about the code, so it does not exist.
        created = utcnow() if start is None or end is None else start + (end - start) / 2
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
    "SEPTEMBER_END",
    "SEPTEMBER_START",
    "STAFFER_UUID",
    "VOUCHER_CODE_SENTINELS",
    "FakeMutator",
    "FakeReader",
    "FakeSender",
    "accepted_outcome",
    "approval_for",
    "customer_payload",
    "customers_page",
    "issued_voucher",
    "location_map",
    "marker_orders",
    "markers_for",
    "orders_page",
    "production_configuration",
    "production_request",
    "rejected_outcome",
    "seed_production_preview",
    "seed_template_and_sender",
    "template_payload",
    "unknown_outcome",
    "voucher_order",
]
